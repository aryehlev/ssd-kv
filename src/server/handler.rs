//! Request dispatch and handling with performance optimizations.

use std::io;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use tracing::{error, trace};

use crate::engine::index::Index;
use crate::engine::index_entry::hash_key;
use crate::perf::prefetch::{BloomFilter, LockFreeBloomFilter};
use crate::perf::simd::{batch_bloom_check, batch_hash_keys_4, group_by_shard};
use crate::io::async_reader::AsyncReader;
use crate::io::aligned_buf::AlignedBuffer;
use crate::storage::eviction::EvictionPolicy;
use crate::storage::file_manager::{FileManager, ParallelFileManager, FILE_HEADER_SIZE};
use crate::storage::record::Record;
use crate::storage::wal::WriteAheadLog;
use crate::storage::wblock_cache::WblockCache;
use crate::storage::write_buffer::{WriteBuffer, WBLOCK_SIZE};

/// Metadata about a stored record, returned by get_with_meta.
pub struct RecordMeta {
    pub value: Vec<u8>,
    pub timestamp_micros: u64,
    pub ttl_secs: u32,
}

/// Statistics for the handler.
#[derive(Debug, Default)]
pub struct HandlerStats {
    pub gets: AtomicU64,
    pub puts: AtomicU64,
    pub deletes: AtomicU64,
    pub get_hits: AtomicU64,
    pub get_misses: AtomicU64,
    pub cache_hits: AtomicU64,
    pub errors: AtomicU64,
    /// Per-op latency histograms. Recording is a handful of relaxed
    /// atomics in the hot path; percentiles are computed lazily when
    /// `INFO latencystats` runs.
    pub set_latency: crate::perf::latency_hist::LatencyHistogram,
    pub get_latency: crate::perf::latency_hist::LatencyHistogram,
    pub del_latency: crate::perf::latency_hist::LatencyHistogram,
}

impl HandlerStats {
    pub fn to_json(&self) -> String {
        format!(
            r#"{{"gets":{},"puts":{},"deletes":{},"hits":{},"misses":{},"cache_hits":{},"errors":{}}}"#,
            self.gets.load(Ordering::Relaxed),
            self.puts.load(Ordering::Relaxed),
            self.deletes.load(Ordering::Relaxed),
            self.get_hits.load(Ordering::Relaxed),
            self.get_misses.load(Ordering::Relaxed),
            self.cache_hits.load(Ordering::Relaxed),
            self.errors.load(Ordering::Relaxed),
        )
    }
}

/// The request handler that processes KV operations.
pub struct Handler {
    index: Arc<Index>,
    file_manager: Arc<FileManager>,
    write_buffer: Arc<WriteBuffer>,
    wblock_cache: Option<Arc<WblockCache>>,
    /// Optional io_uring reader for cache-miss reads. When set, GETs that
    /// fall through to disk go through io_uring (SQPOLL amortizes the
    /// submission syscall) instead of a blocking pread.
    async_reader: Option<Arc<AsyncReader>>,
    /// Parallel WAL shards. Each reactor pins to one shard; all writes
    /// from that reactor go into that shard. When empty, writes live in
    /// RAM until a WBlock fills (tests that don't care about
    /// durability). When length is 1, behaves exactly like the previous
    /// single-WAL code. When length > 1, each shard has its own commit
    /// thread and fsync pipeline — the architectural move that lets
    /// total throughput scale past a single device's IOPS ceiling.
    wal_shards: Vec<Arc<WriteAheadLog>>,
    /// Highest record generation that is durably in a data file. Bumped
    /// by `flush()` after a successful fsync. The periodic WAL-trim path
    /// uses this to decide when old WAL files are redundant.
    durable_gen: AtomicU64,
    /// Global bloom filter for membership hints. Lock-free — every
    /// put_nowait CAS-adds without taking a mutex, so hot-key
    /// workloads don't serialize here.
    bloom_filter: Arc<LockFreeBloomFilter>,
    next_generation: AtomicU64,
    stats: Arc<HandlerStats>,
    eviction_policy: EvictionPolicy,
    max_entries: u64,
    max_data_bytes: u64,
}

impl Handler {
    /// Creates a new handler.
    pub fn new(
        index: Arc<Index>,
        file_manager: Arc<FileManager>,
        write_buffer: Arc<WriteBuffer>,
    ) -> Self {
        Self {
            index,
            file_manager,
            write_buffer,
            wblock_cache: None,
            async_reader: None,
            wal_shards: Vec::new(),
            durable_gen: AtomicU64::new(0),
            bloom_filter: Arc::new(LockFreeBloomFilter::new(10_000_000, 0.01)),
            next_generation: AtomicU64::new(1),
            stats: Arc::new(HandlerStats::default()),
            eviction_policy: EvictionPolicy::NoEviction,
            max_entries: 0,
            max_data_bytes: 0,
        }
    }

    /// Highest generation that is durable in a data file (not just WAL).
    /// The WAL trim path uses this to decide which log files are redundant.
    pub fn durable_generation(&self) -> u64 {
        self.durable_gen.load(Ordering::Acquire)
    }

    fn bump_durable_gen(&self, seen: u64) {
        let mut prev = self.durable_gen.load(Ordering::Relaxed);
        while seen > prev {
            match self.durable_gen.compare_exchange_weak(
                prev,
                seen,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => prev = actual,
            }
        }
    }

    /// Installs an io_uring async reader for cache-miss reads.
    pub fn set_async_reader(&mut self, reader: Arc<AsyncReader>) {
        self.async_reader = Some(reader);
    }

    /// Installs a shared wblock cache (shared across databases so total
    /// memory is bounded by `--wblock-cache-mb`).
    pub fn set_wblock_cache(&mut self, cache: Arc<WblockCache>) {
        self.wblock_cache = Some(cache);
    }

    pub fn wblock_cache(&self) -> Option<&Arc<WblockCache>> {
        self.wblock_cache.as_ref()
    }

    /// Configured cap on live entries (0 = unlimited). Used by CONFIG GET.
    pub fn max_entries(&self) -> u64 {
        self.max_entries
    }

    /// Configured cap on total data bytes (0 = unlimited). Used by CONFIG GET.
    pub fn max_data_bytes(&self) -> u64 {
        self.max_data_bytes
    }

    /// Current eviction policy (read-only for CONFIG GET).
    pub fn eviction_policy(&self) -> EvictionPolicy {
        self.eviction_policy
    }

    /// Installs a single write-ahead log. Back-compat shim for the
    /// pre-sharded API: equivalent to `set_wal_shards(vec![wal])`.
    /// New code should use `set_wal_shards` directly with N shards
    /// sized to the reactor-thread count.
    pub fn set_wal(&mut self, wal: Arc<WriteAheadLog>) {
        self.wal_shards = vec![wal];
    }

    /// Install N parallel WAL shards. Reactor `i` will write into
    /// shard `i`. The length of this vec sets how many reactor threads
    /// get independent fsync pipelines; the common case is
    /// `shards.len() == --reactor-threads`.
    pub fn set_wal_shards(&mut self, shards: Vec<Arc<WriteAheadLog>>) {
        self.wal_shards = shards;
    }

    /// Returns the first WAL shard if any exist. Back-compat accessor
    /// for callers that only care "does this handler have durable
    /// writes enabled"; do not use for shard-aware hot-path logic.
    pub fn wal(&self) -> Option<&Arc<WriteAheadLog>> {
        self.wal_shards.first()
    }

    /// Read-only view of all WAL shards. Reactors use this to pick
    /// their assigned shard.
    pub fn wal_shards(&self) -> &[Arc<WriteAheadLog>] {
        &self.wal_shards
    }

    /// Pick the WAL shard a write should land in, given a hint
    /// (typically the reactor's own index). Returns None if no WAL is
    /// configured (memory-only). Clamps out-of-range hints rather than
    /// panicking — extra reactors past the shard count simply share
    /// the last shard, which is a reasonable degradation vs crashing.
    #[inline]
    fn wal_for(&self, hint: usize) -> Option<&Arc<WriteAheadLog>> {
        if self.wal_shards.is_empty() {
            None
        } else {
            let idx = hint.min(self.wal_shards.len() - 1);
            Some(&self.wal_shards[idx])
        }
    }

    /// Sets the eviction policy and capacity limits.
    pub fn set_eviction_config(&mut self, policy: EvictionPolicy, max_entries: u64, max_data_mb: u64) {
        self.eviction_policy = policy;
        self.max_entries = max_entries;
        self.max_data_bytes = max_data_mb * 1024 * 1024;
    }

    /// Returns a reference to the index.
    pub fn index(&self) -> &Arc<Index> {
        &self.index
    }

    /// Returns a reference to the file manager.
    pub fn file_manager(&self) -> &Arc<FileManager> {
        &self.file_manager
    }

    /// Returns the handler statistics.
    pub fn stats(&self) -> Arc<HandlerStats> {
        Arc::clone(&self.stats)
    }

    /// Synchronous PUT for Redis compatibility. Blocks until the record
    /// is durably in the WAL (if one is installed). Lands on WAL shard
    /// 0 — non-reactor callers (tests, eviction thread) don't care
    /// about sharding, so funnelling them all to the first shard keeps
    /// the API simple.
    pub fn put_sync(&self, key: &[u8], value: &[u8], ttl: u32) -> io::Result<()> {
        let start = std::time::Instant::now();
        let wal_pos = self.put_nowait_on(0, key, value, ttl)?;
        if let (Some(pos), Some(wal)) = (wal_pos, self.wal_for(0)) {
            wal.wait_for_durable(pos);
        }
        self.stats.set_latency.record(start.elapsed().as_micros() as u64);
        Ok(())
    }

    /// Non-blocking PUT on WAL shard 0 — back-compat wrapper for
    /// callers that don't know or care about sharding. Reactor code
    /// should use `put_nowait_on` with its own shard index.
    #[inline]
    pub fn put_nowait(&self, key: &[u8], value: &[u8], ttl: u32) -> io::Result<Option<u64>> {
        self.put_nowait_on(0, key, value, ttl)
    }

    /// Non-blocking PUT with explicit shard routing. Updates the shard's
    /// WAL, the write_buffer, and the index, then returns immediately.
    /// Returns the WAL position within that shard at which the record
    /// ends (so the reactor can poll
    /// `wal_shards[shard_id].durable_position()` and send the +OK once
    /// it's covered). Returns `Ok(None)` if there's no WAL configured.
    ///
    /// The write_buffer and index remain global — sharding is only
    /// about parallel fsync pipelines, not data partitioning.
    ///
    /// ## Visibility (NOT strictly Redis-like)
    ///
    /// The index is updated BEFORE the WAL byte is fsynced, so a concurrent
    /// GET from a different connection can observe the new value before
    /// the writer has received its `+OK`. If the process crashes in the
    /// window between "index updated" and "WAL durable", the record is
    /// lost on restart — WAL replay never picks it up — and any reader
    /// that saw the value during that window saw data that the system
    /// subsequently forgot.
    ///
    /// In Redis this window doesn't exist because Redis only exposes a
    /// value after the command has completed on the single command
    /// thread. Here we trade that guarantee for ~10x write throughput:
    /// reactors pipeline many PUTs per fsync, so fsync latency doesn't
    /// block index publication.
    ///
    /// Callers that need strict "visible ⇒ durable" semantics must use
    /// `put_sync`, which blocks until WAL durability before returning.
    pub fn put_nowait_on(
        &self,
        shard_hint: usize,
        key: &[u8],
        value: &[u8],
        ttl: u32,
    ) -> io::Result<Option<u64>> {
        // Capacity check: reject writes if NoEviction and at capacity
        if matches!(self.eviction_policy, EvictionPolicy::NoEviction) {
            if self.max_entries > 0 {
                let stats = self.index.stats();
                if stats.live_entries >= self.max_entries {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "OOM command not allowed when used memory > 'maxmemory'",
                    ));
                }
            }
            if self.max_data_bytes > 0 {
                if self.index.total_data_bytes() >= self.max_data_bytes {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "OOM command not allowed when used memory > 'maxmemory'",
                    ));
                }
            }
        }

        let key_hash = hash_key(key);
        let generation = self.next_generation.fetch_add(1, Ordering::SeqCst);

        // Stage into the shard's WAL (BufWriter memcpy, no fsync yet).
        // That shard's commit thread picks it up on the next tick.
        // `wal_for` clamps out-of-range hints to the last shard so extra
        // reactors degrade gracefully.
        let wal_pos = if let Some(wal) = self.wal_for(shard_hint) {
            Some(wal.append_put_nowait(key, value, generation, ttl)?)
        } else {
            None
        };

        // Create record + append to write buffer + index. Readers on the
        // SAME reactor see the new value immediately via the write_buffer
        // fast-path; clients on other connections see it once the reactor
        // sends the +OK (after durability).
        let mut record = Record::new(key.to_vec(), value.to_vec(), generation, ttl)?;
        let location = self.write_buffer.append(&mut record)?;
        self.index
            .insert(key, location, generation, record.header.value_len);
        self.bloom_filter.add(key_hash);

        self.stats.puts.fetch_add(1, Ordering::Relaxed);
        Ok(wal_pos)
    }

    /// Write path used by recovery / WAL replay. Uses an explicit generation
    /// from the WAL entry, skips the WAL append, and skips capacity checks
    /// (crash recovery shouldn't be blocked by eviction).
    pub(crate) fn put_from_wal(
        &self,
        key: &[u8],
        value: &[u8],
        generation: u64,
        ttl: u32,
    ) -> io::Result<()> {
        let key_hash = hash_key(key);
        let mut record = Record::new(key.to_vec(), value.to_vec(), generation, ttl)?;
        let location = self.write_buffer.append(&mut record)?;
        self.index
            .insert(key, location, generation, record.header.value_len);
        self.bloom_filter.add(key_hash);
        Ok(())
    }

    /// Delete path used by recovery / WAL replay.
    pub(crate) fn delete_from_wal(&self, key: &[u8], generation: u64) -> io::Result<()> {
        let mut record = Record::tombstone(key.to_vec(), generation)?;
        self.write_buffer.append(&mut record)?;
        self.index.delete(key, generation);
        Ok(())
    }

    /// Advance the generation counter past a value observed during recovery
    /// so subsequent writes don't conflict with replayed records.
    pub(crate) fn bump_generation_past(&self, seen: u64) {
        let next = seen.saturating_add(1);
        // Load-max-store loop; only increases.
        loop {
            let cur = self.next_generation.load(Ordering::Acquire);
            if next <= cur {
                return;
            }
            if self
                .next_generation
                .compare_exchange(cur, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Synchronous DELETE for Redis compatibility. Blocks until durable.
    /// Lands on WAL shard 0 like `put_sync` — non-reactor callers don't
    /// need shard-aware routing.
    pub fn delete_sync(&self, key: &[u8]) -> io::Result<bool> {
        let start = std::time::Instant::now();
        let (deleted, wal_pos) = self.delete_nowait_on(0, key)?;
        if let (Some(pos), Some(wal)) = (wal_pos, self.wal_for(0)) {
            wal.wait_for_durable(pos);
        }
        self.stats.del_latency.record(start.elapsed().as_micros() as u64);
        Ok(deleted)
    }

    /// Back-compat wrapper for non-sharded callers.
    #[inline]
    pub fn delete_nowait(&self, key: &[u8]) -> io::Result<(bool, Option<u64>)> {
        self.delete_nowait_on(0, key)
    }

    /// Non-blocking DELETE with explicit shard routing. Returns
    /// `(was_live, Option<wal_pos>)`. The position is within the chosen
    /// shard's WAL — the reactor polls that shard's `durable_position()`
    /// before sending the `:N\r\n` reply.
    pub fn delete_nowait_on(
        &self,
        shard_hint: usize,
        key: &[u8],
    ) -> io::Result<(bool, Option<u64>)> {
        let generation = self.next_generation.fetch_add(1, Ordering::SeqCst);

        let wal_pos = if let Some(wal) = self.wal_for(shard_hint) {
            Some(wal.append_delete_nowait(key, generation)?)
        } else {
            None
        };

        let mut record = Record::tombstone(key.to_vec(), generation)?;
        self.write_buffer.append(&mut record)?;

        let deleted = self.index.delete(key, generation);
        self.stats.deletes.fetch_add(1, Ordering::Relaxed);
        Ok((deleted, wal_pos))
    }

    /// Fast synchronous GET for UDP/Redis - returns value if found.
    /// Priority: write buffer -> wblock cache -> disk
    #[inline]
    pub fn get_value(&self, key: &[u8]) -> Option<Vec<u8>> {
        let start = std::time::Instant::now();
        let result = self.get_value_inner(key);
        self.stats.get_latency.record(start.elapsed().as_micros() as u64);
        result
    }

    #[inline]
    fn get_value_inner(&self, key: &[u8]) -> Option<Vec<u8>> {
        // Fast path 1: Check index
        let entry = self.index.get(key)?;

        // Fast path 2: Write buffer (not yet flushed)
        if let Some(value) = self.write_buffer.read_unflushed(&entry.location, key) {
            return Some(value);
        }

        let file_id = entry.location.file_id;
        let wblock_id = entry.location.wblock_id as u32;
        let offset = entry.location.offset as usize;

        // Fast path 3: wblock cache. Avoid a 1 MB pread on repeat reads of
        // any key in the same wblock.
        if let Some(cache) = &self.wblock_cache {
            if let Some(buf) = cache.get(file_id, wblock_id) {
                self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
                if offset >= buf.len() {
                    return None;
                }
                let record = Record::from_bytes(&buf.as_ref()[offset..]).ok()?;
                if record.header.is_deleted() || record.header.is_expired() {
                    return None;
                }
                return Some(record.value);
            }
        }

        // Slow path: if we know the exact record size (populated by the
        // write path or recovery scan), do a sub-WBlock partial pread;
        // otherwise fall back to a full-WBlock read and populate the
        // cache for future reads in the same block.
        if entry.location.supports_partial_read() {
            let (buf, start) = self.read_record_for_get(&entry.location).ok()?;
            if start >= buf.len() {
                return None;
            }
            let record = Record::from_bytes(&buf[start..]).ok()?;
            if record.header.is_deleted() || record.header.is_expired() {
                return None;
            }
            return Some(record.value);
        }

        let wblock_data = self.read_wblock_for_get(file_id, wblock_id).ok()?;

        if offset >= wblock_data.len() {
            return None;
        }

        let record = Record::from_bytes(&wblock_data[offset..]).ok()?;
        let result = if record.header.is_deleted() || record.header.is_expired() {
            None
        } else {
            Some(record.value)
        };

        // Populate cache for future hits. We wrap the freshly-read buffer
        // in an Arc; subsequent readers share it without re-reading disk.
        if let Some(cache) = &self.wblock_cache {
            cache.insert(file_id, wblock_id, Arc::new(wblock_data));
        }

        result
    }

    /// Read a wblock for the GET hot path. Prefers io_uring when an
    /// AsyncReader is installed, falls back to the blocking pread path
    /// otherwise. Used only for request-path reads — compaction / scans
    /// deliberately go through FileManager directly so they don't pollute
    /// this code path.
    fn read_wblock_for_get(
        &self,
        file_id: u32,
        wblock_id: u32,
    ) -> io::Result<AlignedBuffer> {
        if let Some(reader) = &self.async_reader {
            let file = self.file_manager.get_file(file_id).ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "file not found")
            })?;
            let file_guard = file.lock();
            let fd = file_guard.raw_fd();
            drop(file_guard); // release before we block waiting for io_uring

            let offset = FILE_HEADER_SIZE as u64 + (wblock_id as u64 * WBLOCK_SIZE as u64);
            match reader.pread_blocking(fd, offset, WBLOCK_SIZE) {
                Ok(buf) => return Ok(buf),
                Err(e) if e.kind() == io::ErrorKind::TimedOut => {
                    // io_uring is misbehaving in this environment; fall
                    // through to the blocking pread path for this op so
                    // the request doesn't hang. Callers higher up decide
                    // whether to log at every miss; we don't log here so
                    // a systemic io_uring failure doesn't spam.
                }
                Err(e) => return Err(e),
            }
        }

        let file = self
            .file_manager
            .get_file(file_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))?;
        let file_guard = file.lock();
        file_guard.read_wblock(wblock_id)
    }

    /// Read just enough bytes off disk to parse the target record.
    /// Skips the WBlock cache intentionally: partial reads are used for
    /// small values where the 1 MiB full-block read would be wasteful,
    /// and keeping partial buffers in the cache would defeat its
    /// "amortize many keys in one block" purpose.
    ///
    /// Falls back to `read_wblock_for_get` (full WBlock) when the
    /// record size isn't known — which is the case for anything
    /// written through the legacy `DiskLocation::new` path or a
    /// pre-existing data file that hasn't been re-scanned.
    fn read_record_for_get(
        &self,
        location: &crate::storage::write_buffer::DiskLocation,
    ) -> io::Result<(AlignedBuffer, usize)> {
        if location.supports_partial_read() {
            let file = self
                .file_manager
                .get_file(location.file_id)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))?;
            let file_guard = file.lock();
            let buf = file_guard.read_partial_wblock(
                location.wblock_id as u32,
                location.offset,
                location.record_size,
            )?;
            // The partial read aligns `offset` down to a 4 KiB
            // boundary. Compute how far into the returned buffer the
            // record actually starts.
            let absolute = FILE_HEADER_SIZE as u64
                + (location.wblock_id as u64 * WBLOCK_SIZE as u64)
                + location.offset as u64;
            let aligned = absolute & !(crate::io::aligned_buf::ALIGNMENT as u64 - 1);
            let start_in_buf = (absolute - aligned) as usize;
            Ok((buf, start_in_buf))
        } else {
            let buf = self
                .read_wblock_for_get(location.file_id, location.wblock_id as u32)?;
            Ok((buf, location.offset as usize))
        }
    }

    /// Returns the full record metadata for a key (value, timestamp, ttl).
    /// Same read path as get_value but returns the full record info.
    pub fn get_with_meta(&self, key: &[u8]) -> Option<RecordMeta> {
        let entry = self.index.get(key)?;

        // Check write buffer first - read_unflushed_record returns full Record
        if let Some(meta) = self.write_buffer.read_unflushed_meta(&entry.location, key) {
            return Some(meta);
        }

        let file_id = entry.location.file_id;
        let wblock_id = entry.location.wblock_id as u32;
        let offset = entry.location.offset as usize;

        if let Some(cache) = &self.wblock_cache {
            if let Some(buf) = cache.get(file_id, wblock_id) {
                self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
                if offset >= buf.len() {
                    return None;
                }
                let record = Record::from_bytes(&buf.as_ref()[offset..]).ok()?;
                if record.header.is_deleted() || record.header.is_expired() {
                    return None;
                }
                return Some(RecordMeta {
                    value: record.value,
                    timestamp_micros: record.header.timestamp,
                    ttl_secs: record.header.ttl,
                });
            }
        }

        // Partial-read fast path for small records. See `get_value_inner`
        // for the rationale — keeps the common small-value GET off the
        // 1 MiB full-block read.
        if entry.location.supports_partial_read() {
            let (buf, start) = self.read_record_for_get(&entry.location).ok()?;
            if start >= buf.len() {
                return None;
            }
            let record = Record::from_bytes(&buf[start..]).ok()?;
            if record.header.is_deleted() || record.header.is_expired() {
                return None;
            }
            return Some(RecordMeta {
                value: record.value,
                timestamp_micros: record.header.timestamp,
                ttl_secs: record.header.ttl,
            });
        }

        // Disk read (io_uring if wired, else pread)
        let wblock_data = self.read_wblock_for_get(file_id, wblock_id).ok()?;

        if offset >= wblock_data.len() {
            return None;
        }

        let record = Record::from_bytes(&wblock_data[offset..]).ok()?;
        let meta = if record.header.is_deleted() || record.header.is_expired() {
            None
        } else {
            Some(RecordMeta {
                value: record.value,
                timestamp_micros: record.header.timestamp,
                ttl_secs: record.header.ttl,
            })
        };

        if let Some(cache) = &self.wblock_cache {
            cache.insert(file_id, wblock_id, Arc::new(wblock_data));
        }

        meta
    }

    /// Updates the TTL for an existing key. Returns false if key doesn't exist.
    pub fn update_ttl(&self, key: &[u8], new_ttl: u32) -> io::Result<bool> {
        let value = match self.get_value(key) {
            Some(v) => v,
            None => return Ok(false),
        };
        self.put_sync(key, &value, new_ttl)?;
        Ok(true)
    }

    /// Tries to flush pending writes to disk (non-blocking, best effort).
    fn try_flush(&self) -> io::Result<()> {
        // Flush all pending WBlocks
        for mut wblock in self.write_buffer.take_pending() {
            let file = self.file_manager.get_or_create_file(wblock.file_id)?;
            let file_guard = file.lock();
            file_guard.write_wblock(&mut wblock)?;
        }
        Ok(())
    }

    /// Flushes pending writes to disk. After each wblock is fsynced we
    /// advance `durable_gen` so the periodic WAL-trim path knows which
    /// WAL files are now redundant.
    pub async fn flush(&self) -> io::Result<()> {
        // Force flush current WBlock
        if let Some(mut wblock) = self.write_buffer.force_flush() {
            let max_gen = wblock.max_generation;
            let file = self.file_manager.get_or_create_file(wblock.file_id)?;
            let file_guard = file.lock();
            file_guard.write_wblock(&mut wblock)?;
            file_guard.sync()?;
            drop(file_guard);
            self.bump_durable_gen(max_gen);
        }

        // Flush all pending WBlocks
        for mut wblock in self.write_buffer.take_pending() {
            let max_gen = wblock.max_generation;
            let file = self.file_manager.get_or_create_file(wblock.file_id)?;
            let file_guard = file.lock();
            file_guard.write_wblock(&mut wblock)?;
            file_guard.sync()?;
            drop(file_guard);
            self.bump_durable_gen(max_gen);
        }

        Ok(())
    }
}

/// High-performance handler optimized for 1M+ RPS.
///
/// Key optimizations over base Handler:
/// - Lock-free bloom filter (no write contention)
/// - Parallel file manager for key distribution
/// - Partial WBlock reads for small records
pub struct OptimizedHandler {
    index: Arc<Index>,
    parallel_fm: Arc<ParallelFileManager>,
    write_buffers: Vec<Arc<WriteBuffer>>,
    bloom_filter: Arc<LockFreeBloomFilter>,
    next_generation: AtomicU64,
    stats: Arc<HandlerStats>,
}

impl OptimizedHandler {
    /// Creates a new optimized handler.
    pub fn new(
        index: Arc<Index>,
        parallel_fm: Arc<ParallelFileManager>,
    ) -> Self {
        let num_partitions = parallel_fm.num_partitions();

        // Create write buffer per partition
        let mut write_buffers = Vec::with_capacity(num_partitions);
        for _ in 0..num_partitions {
            write_buffers.push(Arc::new(WriteBuffer::new(0, 1023)));
        }

        Self {
            index,
            parallel_fm,
            write_buffers,
            bloom_filter: Arc::new(LockFreeBloomFilter::new(10_000_000, 0.01)),
            next_generation: AtomicU64::new(1),
            stats: Arc::new(HandlerStats::default()),
        }
    }

    /// Returns the handler statistics.
    pub fn stats(&self) -> Arc<HandlerStats> {
        Arc::clone(&self.stats)
    }

    /// Returns the partition for a key hash.
    #[inline]
    fn partition_for(&self, key_hash: u64) -> usize {
        self.parallel_fm.partition_for(key_hash)
    }

    /// Synchronous PUT optimized for high throughput.
    pub fn put_sync(&self, key: &[u8], value: &[u8], ttl: u32) -> io::Result<()> {
        let key_hash = hash_key(key);
        let partition = self.partition_for(key_hash);
        let generation = self.next_generation.fetch_add(1, Ordering::SeqCst);

        // Create record
        let mut record = Record::new(key.to_vec(), value.to_vec(), generation, ttl)?;
        let record_size = record.serialized_size();

        // Append to partition's write buffer
        let mut location = self.write_buffers[partition].append(&mut record)?;
        location.record_size = record_size as u32;

        // Update index (values always on disk, only keys in memory)
        self.index.insert(key, location, generation, record.header.value_len);

        // Update lock-free bloom filter (no write lock needed!)
        self.bloom_filter.add(key_hash);

        self.stats.puts.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Synchronous DELETE optimized for high throughput.
    pub fn delete_sync(&self, key: &[u8]) -> io::Result<bool> {
        let key_hash = hash_key(key);
        let partition = self.partition_for(key_hash);
        let generation = self.next_generation.fetch_add(1, Ordering::SeqCst);

        // Create tombstone record
        let mut record = Record::tombstone(key.to_vec(), generation)?;

        // Append tombstone to partition's write buffer
        self.write_buffers[partition].append(&mut record)?;

        // Update index
        let deleted = self.index.delete(key, generation);

        self.stats.deletes.fetch_add(1, Ordering::Relaxed);
        Ok(deleted)
    }

    /// Fast synchronous GET with all optimizations.
    /// Priority: bloom filter -> write buffer -> disk
    #[inline]
    pub fn get_value(&self, key: &[u8]) -> Option<Vec<u8>> {
        let key_hash = hash_key(key);

        // Fast path 1: Check lock-free bloom filter for early rejection
        if !self.bloom_filter.may_contain(key_hash) {
            self.stats.get_misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        // Fast path 2: Check index
        let entry = match self.index.get(key) {
            Some(e) => e,
            None => {
                self.stats.get_misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };

        // Fast path 3: Check partition's write buffer (not yet flushed)
        let partition = self.partition_for(key_hash);
        if let Some(value) = self.write_buffers[partition].read_unflushed(&entry.location, key) {
            self.stats.get_hits.fetch_add(1, Ordering::Relaxed);
            return Some(value);
        }

        // Slow path: Read from disk using parallel file manager
        let file = self.parallel_fm.get_file(partition, entry.location.file_id)?;
        let file_guard = file.lock();

        // Use partial read if record size is known
        let wblock_data = if entry.location.supports_partial_read() {
            file_guard
                .read_partial_wblock(
                    entry.location.wblock_id as u32,
                    entry.location.offset,
                    entry.location.record_size,
                )
                .ok()?
        } else {
            file_guard.read_wblock(entry.location.wblock_id as u32).ok()?
        };

        let offset = entry.location.offset as usize;
        if offset >= wblock_data.len() {
            return None;
        }

        let record = Record::from_bytes(&wblock_data[offset..]).ok()?;
        if record.header.is_deleted() || record.header.is_expired() {
            return None;
        }

        self.stats.get_hits.fetch_add(1, Ordering::Relaxed);
        Some(record.value)
    }

    /// Batch GET for exactly 4 keys using SIMD-accelerated operations.
    /// Returns results in the same order as input keys.
    /// This is 2-3x faster than 4 individual get_value() calls.
    #[inline]
    pub fn batch_get_4(&self, keys: [&[u8]; 4]) -> [Option<Vec<u8>>; 4] {
        // SIMD: Compute all hashes in one go
        let hashes = batch_hash_keys_4(keys);

        // SIMD: Batch bloom filter check - rejects definite misses early
        let bloom_result = batch_bloom_check(&self.bloom_filter, &hashes);

        let mut results: [Option<Vec<u8>>; 4] = [None, None, None, None];

        // Process each key that passed bloom filter
        for i in 0..4 {
            if !bloom_result.may_contain(i) {
                self.stats.get_misses.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            // Index lookup
            let entry = match self.index.get(keys[i]) {
                Some(e) => e,
                None => {
                    self.stats.get_misses.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };

            // Fast path: write buffer
            let partition = self.partition_for(hashes[i]);
            if let Some(value) = self.write_buffers[partition].read_unflushed(&entry.location, keys[i]) {
                self.stats.get_hits.fetch_add(1, Ordering::Relaxed);
                results[i] = Some(value);
                continue;
            }

            // Slow path: disk read
            if let Some(value) = self.read_from_disk(partition, &entry) {
                self.stats.get_hits.fetch_add(1, Ordering::Relaxed);
                results[i] = Some(value);
            }
        }

        results
    }

    /// Batch GET for variable number of keys using SIMD acceleration.
    /// More efficient than individual gets due to:
    /// - Batched bloom filter checks
    /// - Better CPU cache utilization
    /// - Reduced function call overhead
    pub fn batch_get(&self, keys: &[&[u8]]) -> Vec<Option<Vec<u8>>> {
        let n = keys.len();
        let mut results = vec![None; n];

        // Compute all hashes first (better cache behavior)
        let hashes: Vec<u64> = keys.iter().map(|k| hash_key(k)).collect();

        // Batch bloom filter check
        let bloom_result = batch_bloom_check(&self.bloom_filter, &hashes);

        for (i, (&key, &hash)) in keys.iter().zip(hashes.iter()).enumerate() {
            // Skip definite misses from bloom filter
            if !bloom_result.may_contain(i) {
                self.stats.get_misses.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            // Index lookup
            let entry = match self.index.get(key) {
                Some(e) => e,
                None => {
                    self.stats.get_misses.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };

            // Fast path: write buffer
            let partition = self.partition_for(hash);
            if let Some(value) = self.write_buffers[partition].read_unflushed(&entry.location, key) {
                self.stats.get_hits.fetch_add(1, Ordering::Relaxed);
                results[i] = Some(value);
                continue;
            }

            // Slow path: disk read
            if let Some(value) = self.read_from_disk(partition, &entry) {
                self.stats.get_hits.fetch_add(1, Ordering::Relaxed);
                results[i] = Some(value);
            }
        }

        results
    }

    /// Batch GET optimized for cache locality by grouping keys by shard.
    /// This minimizes lock contention and improves CPU cache hit rates.
    /// Best for large batches (>8 keys).
    pub fn batch_get_by_shard(&self, keys: &[&[u8]]) -> Vec<Option<Vec<u8>>> {
        use crate::engine::index::NUM_SHARDS;

        let n = keys.len();
        let mut results = vec![None; n];

        // Compute all hashes
        let hashes: Vec<u64> = keys.iter().map(|k| hash_key(k)).collect();

        // Batch bloom filter check
        let bloom_result = batch_bloom_check(&self.bloom_filter, &hashes);

        // Group keys by shard for cache-friendly access
        let shard_groups = group_by_shard(&hashes, NUM_SHARDS);

        // Process each shard group - better cache locality
        for (shard_idx, key_indices) in shard_groups {
            for &i in &key_indices {
                // Skip bloom filter misses
                if !bloom_result.may_contain(i) {
                    self.stats.get_misses.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                let key = keys[i];
                let hash = hashes[i];

                // Index lookup
                let entry = match self.index.get(key) {
                    Some(e) => e,
                    None => {
                        self.stats.get_misses.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };

                // Fast path: write buffer
                let partition = self.partition_for(hash);
                if let Some(value) = self.write_buffers[partition].read_unflushed(&entry.location, key) {
                    self.stats.get_hits.fetch_add(1, Ordering::Relaxed);
                    results[i] = Some(value);
                    continue;
                }

                // Slow path: disk read
                if let Some(value) = self.read_from_disk(partition, &entry) {
                    self.stats.get_hits.fetch_add(1, Ordering::Relaxed);
                    results[i] = Some(value);
                }
            }
        }

        results
    }

    /// Helper: read value from disk for an index entry.
    #[inline]
    fn read_from_disk(&self, partition: usize, entry: &crate::engine::index_entry::IndexEntry) -> Option<Vec<u8>> {
        let file = self.parallel_fm.get_file(partition, entry.location.file_id)?;
        let file_guard = file.lock();

        let wblock_data = if entry.location.supports_partial_read() {
            file_guard
                .read_partial_wblock(
                    entry.location.wblock_id as u32,
                    entry.location.offset,
                    entry.location.record_size,
                )
                .ok()?
        } else {
            file_guard.read_wblock(entry.location.wblock_id as u32).ok()?
        };

        let offset = entry.location.offset as usize;
        if offset >= wblock_data.len() {
            return None;
        }

        let record = Record::from_bytes(&wblock_data[offset..]).ok()?;
        if record.header.is_deleted() || record.header.is_expired() {
            return None;
        }

        Some(record.value)
    }

    /// Flushes all pending writes across all partitions.
    pub fn flush_all(&self) -> io::Result<()> {
        for (partition, write_buffer) in self.write_buffers.iter().enumerate() {
            // Flush current block
            if let Some(mut wblock) = write_buffer.force_flush() {
                let file = self.parallel_fm.get_or_create_file(partition, wblock.file_id)?;
                let file_guard = file.lock();
                file_guard.write_wblock(&mut wblock)?;
                file_guard.sync()?;
            }

            // Flush pending blocks
            for mut wblock in write_buffer.take_pending() {
                let file = self.parallel_fm.get_or_create_file(partition, wblock.file_id)?;
                let file_guard = file.lock();
                file_guard.write_wblock(&mut wblock)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::file_manager::WBLOCKS_PER_FILE;
    use tempfile::tempdir;

    fn create_test_handler() -> (Handler, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let file_manager = Arc::new(FileManager::new(dir.path()).unwrap());
        let index = Arc::new(Index::new());
        let write_buffer = Arc::new(WriteBuffer::new(0, WBLOCKS_PER_FILE));

        // Create initial file
        file_manager.create_file().unwrap();

        (Handler::new(index, file_manager, write_buffer), dir)
    }

    #[test]
    fn test_put_get() {
        let (handler, _dir) = create_test_handler();

        // PUT
        handler.put_sync(b"key1", b"value1", 0).unwrap();

        // GET
        let value = handler.get_value(b"key1");
        assert_eq!(value, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_get_not_found() {
        let (handler, _dir) = create_test_handler();

        let value = handler.get_value(b"nonexistent");
        assert_eq!(value, None);
    }

    #[test]
    fn test_delete() {
        let (handler, _dir) = create_test_handler();

        // PUT
        handler.put_sync(b"del_key", b"del_value", 0).unwrap();

        // DELETE
        let deleted = handler.delete_sync(b"del_key").unwrap();
        assert!(deleted);

        // GET should return not found
        let value = handler.get_value(b"del_key");
        assert_eq!(value, None);
    }

    #[test]
    fn test_optimized_handler_basic() {
        let dir = tempdir().unwrap();
        let parallel_fm = Arc::new(ParallelFileManager::new(dir.path(), 4).unwrap());
        let index = Arc::new(Index::new());

        // Create initial file in each partition
        for i in 0..4 {
            parallel_fm.create_file(i).unwrap();
        }

        let handler = OptimizedHandler::new(index, parallel_fm);

        // PUT
        handler.put_sync(b"key1", b"value1", 0).unwrap();

        // GET (should hit hot cache)
        let value = handler.get_value(b"key1");
        assert_eq!(value, Some(b"value1".to_vec()));

        // Check stats
        assert_eq!(handler.stats().puts.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_optimized_handler_partitioning() {
        let dir = tempdir().unwrap();
        let parallel_fm = Arc::new(ParallelFileManager::new(dir.path(), 8).unwrap());
        let index = Arc::new(Index::new());

        for i in 0..8 {
            parallel_fm.create_file(i).unwrap();
        }

        let handler = OptimizedHandler::new(index, parallel_fm);

        // Insert multiple keys (should distribute across partitions)
        for i in 0..100 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            handler.put_sync(key.as_bytes(), value.as_bytes(), 0).unwrap();
        }

        // Verify all keys are retrievable
        for i in 0..100 {
            let key = format!("key_{}", i);
            let expected_value = format!("value_{}", i);
            let value = handler.get_value(key.as_bytes());
            assert_eq!(value, Some(expected_value.into_bytes()));
        }
    }

    #[test]
    fn test_batch_get_4() {
        let dir = tempdir().unwrap();
        let parallel_fm = Arc::new(ParallelFileManager::new(dir.path(), 4).unwrap());
        let index = Arc::new(Index::new());

        for i in 0..4 {
            parallel_fm.create_file(i).unwrap();
        }

        let handler = OptimizedHandler::new(index, parallel_fm);

        // Insert test keys
        handler.put_sync(b"key_0", b"value_0", 0).unwrap();
        handler.put_sync(b"key_1", b"value_1", 0).unwrap();
        handler.put_sync(b"key_2", b"value_2", 0).unwrap();
        // key_3 not inserted

        // Batch GET with SIMD
        let keys: [&[u8]; 4] = [b"key_0", b"key_1", b"key_2", b"key_3"];
        let results = handler.batch_get_4(keys);

        assert_eq!(results[0], Some(b"value_0".to_vec()));
        assert_eq!(results[1], Some(b"value_1".to_vec()));
        assert_eq!(results[2], Some(b"value_2".to_vec()));
        assert_eq!(results[3], None); // Not found
    }

    #[test]
    fn test_batch_get_variable() {
        let dir = tempdir().unwrap();
        let parallel_fm = Arc::new(ParallelFileManager::new(dir.path(), 4).unwrap());
        let index = Arc::new(Index::new());

        for i in 0..4 {
            parallel_fm.create_file(i).unwrap();
        }

        let handler = OptimizedHandler::new(index, parallel_fm);

        // Insert 10 keys
        for i in 0..10 {
            let key = format!("batch_key_{}", i);
            let value = format!("batch_value_{}", i);
            handler.put_sync(key.as_bytes(), value.as_bytes(), 0).unwrap();
        }

        // Batch GET - mix of existing and non-existing keys
        let keys_owned: Vec<Vec<u8>> = vec![
            b"batch_key_0".to_vec(),
            b"batch_key_5".to_vec(),
            b"nonexistent".to_vec(),
            b"batch_key_9".to_vec(),
            b"also_missing".to_vec(),
        ];
        let keys: Vec<&[u8]> = keys_owned.iter().map(|k| k.as_slice()).collect();

        let results = handler.batch_get(&keys);

        assert_eq!(results.len(), 5);
        assert_eq!(results[0], Some(b"batch_value_0".to_vec()));
        assert_eq!(results[1], Some(b"batch_value_5".to_vec()));
        assert_eq!(results[2], None);
        assert_eq!(results[3], Some(b"batch_value_9".to_vec()));
        assert_eq!(results[4], None);
    }

    #[test]
    fn test_batch_get_by_shard() {
        let dir = tempdir().unwrap();
        let parallel_fm = Arc::new(ParallelFileManager::new(dir.path(), 4).unwrap());
        let index = Arc::new(Index::new());

        for i in 0..4 {
            parallel_fm.create_file(i).unwrap();
        }

        let handler = OptimizedHandler::new(index, parallel_fm);

        // Insert 20 keys (will distribute across shards)
        for i in 0..20 {
            let key = format!("shard_key_{}", i);
            let value = format!("shard_value_{}", i);
            handler.put_sync(key.as_bytes(), value.as_bytes(), 0).unwrap();
        }

        // Batch GET by shard - should give same results but with better cache locality
        let keys_owned: Vec<Vec<u8>> = (0..20)
            .map(|i| format!("shard_key_{}", i).into_bytes())
            .collect();
        let keys: Vec<&[u8]> = keys_owned.iter().map(|k| k.as_slice()).collect();

        let results = handler.batch_get_by_shard(&keys);

        assert_eq!(results.len(), 20);
        for i in 0..20 {
            let expected = format!("shard_value_{}", i);
            assert_eq!(results[i], Some(expected.into_bytes()), "Key {} mismatch", i);
        }
    }

    #[test]
    fn test_get_with_meta_returns_ttl() {
        let (handler, _dir) = create_test_handler();
        handler.put_sync(b"ttl_key", b"ttl_val", 3600).unwrap();

        let meta = handler.get_with_meta(b"ttl_key").unwrap();
        assert_eq!(meta.value, b"ttl_val");
        assert_eq!(meta.ttl_secs, 3600);
        assert!(meta.timestamp_micros > 0);
    }

    #[test]
    fn test_get_with_meta_no_ttl() {
        let (handler, _dir) = create_test_handler();
        handler.put_sync(b"no_ttl_key", b"no_ttl_val", 0).unwrap();

        let meta = handler.get_with_meta(b"no_ttl_key").unwrap();
        assert_eq!(meta.value, b"no_ttl_val");
        assert_eq!(meta.ttl_secs, 0);
    }

    #[test]
    fn test_get_with_meta_nonexistent() {
        let (handler, _dir) = create_test_handler();
        assert!(handler.get_with_meta(b"missing").is_none());
    }

    #[test]
    fn test_update_ttl() {
        let (handler, _dir) = create_test_handler();
        handler.put_sync(b"upd_key", b"upd_val", 0).unwrap();

        // Update TTL
        assert!(handler.update_ttl(b"upd_key", 7200).unwrap());

        // Verify new TTL
        let meta = handler.get_with_meta(b"upd_key").unwrap();
        assert_eq!(meta.ttl_secs, 7200);
        assert_eq!(meta.value, b"upd_val");
    }

    #[test]
    fn test_update_ttl_nonexistent() {
        let (handler, _dir) = create_test_handler();
        assert!(!handler.update_ttl(b"missing", 100).unwrap());
    }
}
