//! Request dispatch: bridges WAL + SegmentManager for the RESP server.

use std::io;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use tracing::error;

use crate::engine::index::Index;
use crate::perf::prefetch::LockFreeBloomFilter;
use crate::io::async_reader::AsyncReader;
use crate::storage::eviction::EvictionPolicy;
use crate::storage::segment_manager::{SegmentManager, now_micros};
use crate::storage::wal::WriteAheadLog;
use crate::storage::write_buffer::WriteBuffer;

/// Metadata about a stored record, returned by `get_with_meta`.
pub struct RecordMeta {
    pub value: Vec<u8>,
    /// Creation timestamp in microseconds (for TTL eviction).
    pub timestamp_micros: u64,
    pub ttl_secs: u32,
}

/// Per-handler operation statistics.
#[derive(Debug, Default)]
pub struct HandlerStats {
    pub gets: AtomicU64,
    pub puts: AtomicU64,
    pub deletes: AtomicU64,
    pub get_hits: AtomicU64,
    pub get_misses: AtomicU64,
    pub cache_hits: AtomicU64,
    pub errors: AtomicU64,
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

// ─── Async GET result ─────────────────────────────────────────────────────────

/// Return value from [`Handler::try_get_async`].
pub enum GetResult {
    /// Served from memory; RESP response already written into caller's buffer.
    Immediate,
    /// Key found but data is on disk — caller must issue an async pread.
    NeedsDisk {
        fd: std::os::unix::io::RawFd,
        offset: u64,
        size: usize,
        loc: crate::engine::index_entry::RecordLocation,
    },
    /// Key not found.
    Miss,
}

// ─── Handler ─────────────────────────────────────────────────────────────────

pub struct Handler {
    index: Arc<Index>,
    sm: Arc<SegmentManager>,
    wal_shards: Vec<Arc<WriteAheadLog>>,
    durable_gen: AtomicU32,
    bloom_filter: Arc<LockFreeBloomFilter>,
    stats: Arc<HandlerStats>,
    eviction_policy: EvictionPolicy,
    max_entries: u64,
    max_data_bytes: u64,
}

impl Handler {
    /// Construct from the three objects the test harness provides.
    /// Uses the passed `index` for all in-memory lookups.
    pub fn new(
        index: Arc<Index>,
        file_manager: Arc<SegmentManager>,
        _write_buffer: Arc<WriteBuffer>,
    ) -> Self {
        Self {
            index,
            sm: file_manager,
            wal_shards: Vec::new(),
            durable_gen: AtomicU32::new(0),
            bloom_filter: Arc::new(LockFreeBloomFilter::new(10_000_000, 0.01)),
            stats: Arc::new(HandlerStats::default()),
            eviction_policy: EvictionPolicy::NoEviction,
            max_entries: 0,
            max_data_bytes: 0,
        }
    }

    // ─── Accessors ─────────────────────────────────────────────────────────

    pub fn index(&self) -> &Arc<Index> { &self.index }

    pub fn file_manager(&self) -> &Arc<SegmentManager> { &self.sm }

    pub fn stats(&self) -> Arc<HandlerStats> { Arc::clone(&self.stats) }

    pub fn durable_generation(&self) -> u32 { self.durable_gen.load(Ordering::Acquire) }

    pub fn max_entries(&self) -> u64 { self.max_entries }

    pub fn max_data_bytes(&self) -> u64 { self.max_data_bytes }

    pub fn eviction_policy(&self) -> EvictionPolicy { self.eviction_policy }

    // ─── WAL installation ──────────────────────────────────────────────────

    pub fn set_wal(&mut self, wal: Arc<WriteAheadLog>) {
        self.wal_shards = vec![wal];
    }

    pub fn set_wal_shards(&mut self, shards: Vec<Arc<WriteAheadLog>>) {
        self.wal_shards = shards;
    }

    pub fn wal(&self) -> Option<&Arc<WriteAheadLog>> { self.wal_shards.first() }

    pub fn wal_shards(&self) -> &[Arc<WriteAheadLog>] { &self.wal_shards }

    #[inline]
    fn wal_for(&self, hint: usize) -> Option<&Arc<WriteAheadLog>> {
        if self.wal_shards.is_empty() { None }
        else { Some(&self.wal_shards[hint.min(self.wal_shards.len() - 1)]) }
    }

    // ─── Optional subsystems (no-ops in new design) ────────────────────────

    pub fn set_async_reader(&mut self, _reader: Arc<AsyncReader>) {}

    pub fn set_wblock_cache<T: Send + Sync + 'static>(&mut self, _cache: Arc<T>) {}

    pub fn wblock_cache(&self) -> Option<()> { None }

    pub fn set_eviction_config(&mut self, policy: EvictionPolicy, max_entries: u64, max_data_mb: u64) {
        self.eviction_policy = policy;
        self.max_entries = max_entries;
        self.max_data_bytes = max_data_mb * 1024 * 1024;
    }

    // ─── KV operations ─────────────────────────────────────────────────────

    /// Synchronous PUT: WAL + segment write + index update. Blocks until durable.
    pub fn put_sync(&self, key: &[u8], value: &[u8], ttl: u32) -> io::Result<()> {
        let start = std::time::Instant::now();
        let wal_pos = self.put_nowait_on(0, key, value, ttl)?;
        if let (Some(pos), Some(wal)) = (wal_pos, self.wal_for(0)) {
            wal.wait_for_durable(pos);
        }
        self.stats.set_latency.record(start.elapsed().as_micros() as u64);
        Ok(())
    }

    /// Non-blocking PUT via WAL shard 0 (back-compat shim).
    #[inline]
    pub fn put_nowait(&self, key: &[u8], value: &[u8], ttl: u32) -> io::Result<Option<u64>> {
        self.put_nowait_on(0, key, value, ttl)
    }

    /// Non-blocking PUT with shard routing.
    pub fn put_nowait_on(
        &self, shard_hint: usize, key: &[u8], value: &[u8], ttl: u32,
    ) -> io::Result<Option<u64>> {
        // Capacity gate.
        if matches!(self.eviction_policy, EvictionPolicy::NoEviction) {
            if self.max_entries > 0 && self.index.stats().live_entries >= self.max_entries {
                return Err(io::Error::new(io::ErrorKind::Other,
                    "OOM command not allowed when used memory > 'maxmemory'"));
            }
            if self.max_data_bytes > 0 && self.index.total_data_bytes() >= self.max_data_bytes {
                return Err(io::Error::new(io::ErrorKind::Other,
                    "OOM command not allowed when used memory > 'maxmemory'"));
            }
        }

        let generation = self.sm.next_generation();
        let ts = now_micros();

        let wal_pos = if let Some(wal) = self.wal_for(shard_hint) {
            Some(wal.append_put_nowait(key, value, generation, ttl)?)
        } else {
            None
        };

        let loc = self.sm.write_entry(key, value, ts, ttl, generation, false)?;
        self.index.insert(key, loc, generation, value.len() as u32);
        self.stats.puts.fetch_add(1, Ordering::Relaxed);
        Ok(wal_pos)
    }

    /// Write path for WAL replay (skips WAL append, uses the replayed generation).
    pub fn put_from_wal(&self, key: &[u8], value: &[u8], generation: u32, ttl: u32) -> io::Result<()> {
        let ts = now_micros();
        let loc = self.sm.write_entry(key, value, ts, ttl, generation, false)?;
        self.index.insert(key, loc, generation, value.len() as u32);
        Ok(())
    }

    /// Delete path for WAL replay.
    pub fn delete_from_wal(&self, key: &[u8], generation: u32) -> io::Result<()> {
        let ts = now_micros();
        self.sm.write_entry(key, b"", ts, 0, generation, true)?;
        self.index.delete(key, generation);
        Ok(())
    }

    /// Advance the generation counter past a WAL-replayed value.
    pub fn bump_generation_past(&self, seen: u32) {
        self.sm.bump_generation_past(seen);
    }

    /// Synchronous DELETE. Blocks until durable.
    pub fn delete_sync(&self, key: &[u8]) -> io::Result<bool> {
        let start = std::time::Instant::now();
        let (deleted, wal_pos) = self.delete_nowait_on(0, key)?;
        if let (Some(pos), Some(wal)) = (wal_pos, self.wal_for(0)) {
            wal.wait_for_durable(pos);
        }
        self.stats.del_latency.record(start.elapsed().as_micros() as u64);
        Ok(deleted)
    }

    #[inline]
    pub fn delete_nowait(&self, key: &[u8]) -> io::Result<(bool, Option<u64>)> {
        self.delete_nowait_on(0, key)
    }

    pub fn delete_nowait_on(
        &self, shard_hint: usize, key: &[u8],
    ) -> io::Result<(bool, Option<u64>)> {
        let existed = self.index.get(key).map(|e| e.is_live()).unwrap_or(false);
        if !existed {
            return Ok((false, None));
        }

        let generation = self.sm.next_generation();
        let ts = now_micros();

        let wal_pos = if let Some(wal) = self.wal_for(shard_hint) {
            Some(wal.append_delete_nowait(key, generation)?)
        } else {
            None
        };

        self.sm.write_entry(key, b"", ts, 0, generation, true)?;
        self.index.delete(key, generation);
        self.stats.deletes.fetch_add(1, Ordering::Relaxed);
        Ok((true, wal_pos))
    }

    /// Try to serve a GET from RAM. If the page is in the staging buffer or
    /// the active in-memory ipage the value is encoded directly into `out`
    /// and `Immediate` is returned. If the page is on disk, `NeedsDisk`
    /// is returned with the coordinates needed for an async pread — the
    /// reactor submits the read and delivers the response later.
    ///
    /// Only works for SSD-backed databases (checks the index + SegmentManager).
    pub fn try_get_async(&self, key: &[u8], out: &mut Vec<u8>) -> GetResult {
        let entry = match self.index.get(key) {
            None => return GetResult::Miss,
            Some(e) => e,
        };
        if entry.is_deleted() { return GetResult::Miss; }

        // Fast path: in-memory staging or active ipage.
        if let Some(pe) = self.sm.try_read_staged(entry.location) {
            if pe.is_deleted || pe.is_expired() { return GetResult::Miss; }
            // Encode RESP bulk string into caller's buffer.
            let vlen = pe.value.len();
            out.push(b'$');
            out.extend_from_slice(itoa::Buffer::new().format(vlen).as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(&pe.value);
            out.extend_from_slice(b"\r\n");
            return GetResult::Immediate;
        }

        // Slow path: caller must read from disk asynchronously.
        match self.sm.disk_read_coords(entry.location) {
            Ok((fd, offset, size)) => GetResult::NeedsDisk { fd, offset, size, loc: entry.location },
            Err(_) => GetResult::Miss,
        }
    }

    /// Synchronous GET — returns raw value bytes.
    pub fn get_value(&self, key: &[u8]) -> Option<Vec<u8>> {
        let start = std::time::Instant::now();
        let result = self.get_value_inner(key);
        self.stats.get_latency.record(start.elapsed().as_micros() as u64);
        if result.is_some() {
            self.stats.get_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats.get_misses.fetch_add(1, Ordering::Relaxed);
        }
        self.stats.gets.fetch_add(1, Ordering::Relaxed);
        result
    }

    fn get_value_inner(&self, key: &[u8]) -> Option<Vec<u8>> {
        let entry = self.index.get(key)?;
        if entry.is_deleted() { return None; }
        let pe = self.sm.read_at(entry.location).ok()?;
        if pe.is_deleted || pe.is_expired() { return None; }
        Some(pe.value)
    }

    /// Returns value + metadata for eviction and TTL commands.
    pub fn get_with_meta(&self, key: &[u8]) -> Option<RecordMeta> {
        let entry = self.index.get(key)?;
        if entry.is_deleted() { return None; }
        let pe = self.sm.read_at(entry.location).ok()?;
        if pe.is_deleted || pe.is_expired() { return None; }
        Some(RecordMeta {
            value: pe.value,
            timestamp_micros: pe.ts,
            ttl_secs: pe.ttl,
        })
    }

    /// Update the TTL of an existing key.
    pub fn update_ttl(&self, key: &[u8], new_ttl: u32) -> io::Result<bool> {
        let meta = match self.get_with_meta(key) {
            None => return Ok(false),
            Some(m) => m,
        };
        self.put_nowait_on(0, key, &meta.value, new_ttl)?;
        Ok(true)
    }

    /// Flush pending data to durable storage.
    pub async fn flush(&self) -> io::Result<()> {
        self.sm.flush()
    }
}

// ─── OptimizedHandler (thin wrapper kept for API compat) ─────────────────────

pub struct OptimizedHandler {
    inner: Arc<Handler>,
    stats: Arc<HandlerStats>,
}

impl OptimizedHandler {
    pub fn new(
        index: Arc<Index>,
        sm: Arc<SegmentManager>,
    ) -> Self {
        let inner = Arc::new(Handler::new(index, sm, Arc::new(WriteBuffer::new(0u32, 0usize))));
        let stats = inner.stats();
        Self { inner, stats }
    }

    pub fn stats(&self) -> Arc<HandlerStats> { Arc::clone(&self.stats) }

    pub fn put_sync(&self, key: &[u8], value: &[u8], ttl: u32) -> io::Result<()> {
        self.inner.put_sync(key, value, ttl)
    }

    pub fn get_value(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.inner.get_value(key)
    }

    pub fn delete_sync(&self, key: &[u8]) -> io::Result<bool> {
        self.inner.delete_sync(key)
    }
}
