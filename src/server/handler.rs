//! Request dispatch and handling with performance optimizations.

use std::io;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use tracing::{error, trace};

use crate::engine::index::Index;
use crate::engine::index_entry::hash_key;
use crate::perf::prefetch::{BloomFilter, LockFreeBloomFilter};
use crate::perf::simd::{batch_bloom_check, batch_hash_keys_4, group_by_shard};
use crate::storage::eviction::EvictionPolicy;
use crate::storage::file_manager::{FileManager, ParallelFileManager};
use crate::storage::record::Record;
use crate::storage::write_buffer::{WriteBuffer, WBLOCK_SIZE};

/// Metadata about a stored record, returned by get_with_meta.
pub struct RecordMeta {
    pub value: Vec<u8>,
    pub timestamp_micros: u64,
    pub ttl_secs: u32,
    pub value_type: u8,
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
    bloom_filter: parking_lot::RwLock<BloomFilter>,
    next_generation: AtomicU32,
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
            bloom_filter: parking_lot::RwLock::new(BloomFilter::new(1_000_000, 0.01)),
            next_generation: AtomicU32::new(1),
            stats: Arc::new(HandlerStats::default()),
            eviction_policy: EvictionPolicy::NoEviction,
            max_entries: 0,
            max_data_bytes: 0,
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

    /// Synchronous PUT for Redis compatibility
    pub fn put_sync(&self, key: &[u8], value: &[u8], ttl: u32) -> io::Result<()> {
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

        // Create record
        let mut record = Record::new(key.to_vec(), value.to_vec(), generation, ttl)?;

        // Append to write buffer
        let location = self.write_buffer.append(&mut record)?;

        // Update index (values always on disk, only keys in memory)
        self.index.insert(key, location, generation, record.header.value_len);

        // Update bloom filter
        self.bloom_filter.write().add(key_hash);

        self.stats.puts.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Synchronous DELETE for Redis compatibility
    pub fn delete_sync(&self, key: &[u8]) -> io::Result<bool> {
        let generation = self.next_generation.fetch_add(1, Ordering::SeqCst);

        // Create tombstone record
        let mut record = Record::tombstone(key.to_vec(), generation)?;

        // Append tombstone to write buffer
        self.write_buffer.append(&mut record)?;

        // Update index
        let deleted = self.index.delete(key, generation);

        self.stats.deletes.fetch_add(1, Ordering::Relaxed);
        Ok(deleted)
    }

    /// Fast synchronous GET for UDP/Redis - returns value if found.
    /// Priority: write buffer -> disk
    #[inline]
    pub fn get_value(&self, key: &[u8]) -> Option<Vec<u8>> {
        // Fast path 1: Check index
        let entry = self.index.get(key)?;

        // Fast path 2: Write buffer (not yet flushed)
        if let Some(value) = self.write_buffer.read_unflushed(&entry.location, key) {
            return Some(value);
        }

        // Slow path: Read from disk (blocking)
        let file = self.file_manager.get_file(entry.location.file_id)?;
        let file_guard = file.lock();
        let wblock_data = file_guard.read_wblock(entry.location.wblock_id as u32).ok()?;

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

    /// Returns the full record metadata for a key (value, timestamp, ttl).
    /// Same read path as get_value but returns the full record info.
    pub fn get_with_meta(&self, key: &[u8]) -> Option<RecordMeta> {
        let entry = self.index.get(key)?;

        // Check write buffer first - read_unflushed_record returns full Record
        if let Some(meta) = self.write_buffer.read_unflushed_meta(&entry.location, key) {
            return Some(meta);
        }

        // Disk read
        let file = self.file_manager.get_file(entry.location.file_id)?;
        let file_guard = file.lock();
        let wblock_data = file_guard.read_wblock(entry.location.wblock_id as u32).ok()?;

        let offset = entry.location.offset as usize;
        if offset >= wblock_data.len() {
            return None;
        }

        let record = Record::from_bytes(&wblock_data[offset..]).ok()?;
        if record.header.is_deleted() || record.header.is_expired() {
            return None;
        }

        Some(RecordMeta {
            value: record.value,
            timestamp_micros: record.header.timestamp,
            ttl_secs: record.header.ttl,
            value_type: record.header.reserved[0],
        })
    }

    /// Synchronous PUT that also sets the value type byte in reserved[0].
    pub fn put_sync_typed(&self, key: &[u8], value: &[u8], ttl: u32, value_type: u8) -> io::Result<()> {
        // Capacity check (same as put_sync)
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

        let mut record = Record::new(key.to_vec(), value.to_vec(), generation, ttl)?;
        record.header.reserved[0] = value_type;

        let location = self.write_buffer.append(&mut record)?;
        self.index.insert(key, location, generation, record.header.value_len);
        self.bloom_filter.write().add(key_hash);
        self.stats.puts.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Returns (value, value_type) for a key.
    pub fn get_value_with_type(&self, key: &[u8]) -> Option<(Vec<u8>, u8)> {
        let entry = self.index.get(key)?;

        // Check write buffer first
        if let Some(meta) = self.write_buffer.read_unflushed_meta(&entry.location, key) {
            return Some((meta.value, meta.value_type));
        }

        // Disk read
        let file = self.file_manager.get_file(entry.location.file_id)?;
        let file_guard = file.lock();
        let wblock_data = file_guard.read_wblock(entry.location.wblock_id as u32).ok()?;

        let offset = entry.location.offset as usize;
        if offset >= wblock_data.len() {
            return None;
        }

        let record = Record::from_bytes(&wblock_data[offset..]).ok()?;
        if record.header.is_deleted() || record.header.is_expired() {
            return None;
        }

        Some((record.value, record.header.reserved[0]))
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

    /// Flushes pending writes to disk.
    pub async fn flush(&self) -> io::Result<()> {
        // Force flush current WBlock
        if let Some(mut wblock) = self.write_buffer.force_flush() {
            let file = self.file_manager.get_or_create_file(wblock.file_id)?;
            let file_guard = file.lock();
            file_guard.write_wblock(&mut wblock)?;
            file_guard.sync()?;
        }

        // Flush all pending WBlocks
        for mut wblock in self.write_buffer.take_pending() {
            let file = self.file_manager.get_or_create_file(wblock.file_id)?;
            let file_guard = file.lock();
            file_guard.write_wblock(&mut wblock)?;
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
    next_generation: AtomicU32,
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
            next_generation: AtomicU32::new(1),
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
