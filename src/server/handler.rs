//! Request dispatch and handling with performance optimizations.

use std::io;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use tracing::{error, trace};

use crate::engine::index::Index;
use crate::engine::index_entry::hash_key;
use crate::perf::prefetch::{prefetch_read, BloomFilter, LockFreeBloomFilter};
use crate::server::protocol::{Opcode, Request, Response};
use crate::storage::file_manager::{FileManager, ParallelFileManager, FILE_HEADER_SIZE};
use crate::storage::record::Record;
use crate::storage::write_buffer::{DiskLocation, WriteBuffer, WBLOCK_SIZE};

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
        }
    }

    /// Returns the handler statistics.
    pub fn stats(&self) -> Arc<HandlerStats> {
        Arc::clone(&self.stats)
    }

    /// Synchronous PUT for Redis compatibility
    pub fn put_sync(&self, key: &[u8], value: &[u8], ttl: u32) -> io::Result<()> {
        use crate::engine::index_entry::MAX_INLINE_VALUE_SIZE;

        let key_hash = hash_key(key);
        let generation = self.next_generation.fetch_add(1, Ordering::SeqCst);

        // Create record
        let mut record = Record::new(key.to_vec(), value.to_vec(), generation, ttl)?;

        // Append to write buffer
        let location = self.write_buffer.append(&mut record)?;

        // Update index (with inline value for small values)
        if value.len() <= MAX_INLINE_VALUE_SIZE {
            self.index.insert_with_value(key, location, generation, value);
        } else {
            self.index.insert(key, location, generation, record.header.value_len);
        }

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
    /// Priority: inline value -> write buffer -> disk
    #[inline]
    pub fn get_value(&self, key: &[u8]) -> Option<Vec<u8>> {
        // Fast path 1: Check index
        let entry = self.index.get(key)?;

        // Fast path 2: Inline value (no disk!)
        if let Some(value) = entry.get_inline_value() {
            return Some(value.to_vec());
        }

        // Fast path 3: Write buffer (not yet flushed)
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

    /// Handles a request and returns a response.
    #[inline]
    pub async fn handle(&self, request: Request) -> Response {
        let request_id = request.header.request_id;

        match request.header.opcode {
            Opcode::Get => self.handle_get(request_id, &request).await,
            Opcode::Put => self.handle_put(request_id, &request).await,
            Opcode::Delete => self.handle_delete(request_id, &request).await,
            Opcode::Ping => Response::pong(request_id),
            Opcode::Stats => self.handle_stats(request_id),
            _ => {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                Response::invalid(request_id, "Unknown opcode")
            }
        }
    }

    /// Handles a GET request.
    #[inline]
    async fn handle_get(&self, request_id: u32, request: &Request) -> Response {
        self.stats.gets.fetch_add(1, Ordering::Relaxed);

        let key = match request.parse_get() {
            Ok(k) => k,
            Err(e) => {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                return Response::invalid(request_id, &format!("Invalid GET: {}", e));
            }
        };

        let key_hash = hash_key(&key);

        // Fast path 1: Check bloom filter for early rejection
        if !self.bloom_filter.read().may_contain(key_hash) {
            self.stats.get_misses.fetch_add(1, Ordering::Relaxed);
            return Response::not_found(request_id);
        }

        // Look up in index
        let entry = match self.index.get(&key) {
            Some(e) => e,
            None => {
                self.stats.get_misses.fetch_add(1, Ordering::Relaxed);
                return Response::not_found(request_id);
            }
        };

        // Fast path 2: Check for inline value (NO DISK READ!)
        if let Some(value) = entry.get_inline_value() {
            self.stats.get_hits.fetch_add(1, Ordering::Relaxed);
            return Response::success(request_id, value);
        }

        // Fast path 3: Check write buffer for unflushed data
        if let Some(value) = self.write_buffer.read_unflushed(&entry.location, &key) {
            self.stats.get_hits.fetch_add(1, Ordering::Relaxed);
            return Response::success(request_id, &value);
        }

        // Prefetch the disk location data
        prefetch_read(&entry.location as *const DiskLocation);

        // Slow path: Read value from disk (only for large values)
        match self.read_value(&entry.location, entry.value_len).await {
            Ok(value) => {
                self.stats.get_hits.fetch_add(1, Ordering::Relaxed);
                Response::success(request_id, &value)
            }
            Err(e) => {
                error!("Failed to read value: {}", e);
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                Response::error(request_id, &format!("Read error: {}", e))
            }
        }
    }

    /// Handles a PUT request.
    #[inline]
    async fn handle_put(&self, request_id: u32, request: &Request) -> Response {
        self.stats.puts.fetch_add(1, Ordering::Relaxed);

        let (key, value, ttl) = match request.parse_put() {
            Ok(parsed) => parsed,
            Err(e) => {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                return Response::invalid(request_id, &format!("Invalid PUT: {}", e));
            }
        };

        let key_hash = hash_key(&key);
        let generation = self.next_generation.fetch_add(1, Ordering::SeqCst);

        trace!(
            "PUT key={:?} value_len={} ttl={}",
            String::from_utf8_lossy(&key),
            value.len(),
            ttl
        );

        // Create record
        let mut record = match Record::new(key.clone(), value.clone(), generation, ttl) {
            Ok(r) => r,
            Err(e) => {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                return Response::error(request_id, &format!("Record creation failed: {}", e));
            }
        };

        // Append to write buffer
        let location = match self.write_buffer.append(&mut record) {
            Ok(loc) => loc,
            Err(e) => {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                return Response::error(request_id, &format!("Write failed: {}", e));
            }
        };

        // Update index (with inline value for small values)
        use crate::engine::index_entry::MAX_INLINE_VALUE_SIZE;
        if value.len() <= MAX_INLINE_VALUE_SIZE {
            self.index.insert_with_value(&key, location, generation, &value);
        } else {
            self.index.insert(&key, location, generation, record.header.value_len);
        }

        // Update bloom filter
        self.bloom_filter.write().add(key_hash);

        // Periodically flush (every 1000 operations)
        if generation % 1000 == 0 {
            let _ = self.try_flush();
        }

        Response::success(request_id, &[])
    }

    /// Handles a DELETE request.
    #[inline]
    async fn handle_delete(&self, request_id: u32, request: &Request) -> Response {
        self.stats.deletes.fetch_add(1, Ordering::Relaxed);

        let key = match request.parse_delete() {
            Ok(k) => k,
            Err(e) => {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                return Response::invalid(request_id, &format!("Invalid DELETE: {}", e));
            }
        };

        trace!("DELETE key={:?}", String::from_utf8_lossy(&key));

        let generation = self.next_generation.fetch_add(1, Ordering::SeqCst);

        // Create tombstone record
        let mut record = match Record::tombstone(key.clone(), generation) {
            Ok(r) => r,
            Err(e) => {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                return Response::error(request_id, &format!("Tombstone creation failed: {}", e));
            }
        };

        // Append tombstone to write buffer
        if let Err(e) = self.write_buffer.append(&mut record) {
            self.stats.errors.fetch_add(1, Ordering::Relaxed);
            return Response::error(request_id, &format!("Write failed: {}", e));
        }

        // Update index
        self.index.delete(&key, generation);

        Response::success(request_id, &[])
    }

    /// Handles a STATS request.
    fn handle_stats(&self, request_id: u32) -> Response {
        let index_stats = self.index.stats();
        let json = format!(
            r#"{{"index":{{"entries":{},"live":{},"tombstones":{}}},"ops":{}}}"#,
            index_stats.total_entries,
            index_stats.live_entries,
            index_stats.tombstones,
            self.stats.to_json(),
        );
        Response::stats(request_id, &json)
    }

    /// Reads a value from disk.
    /// Uses partial reads when record_size is known to avoid reading full 1MB WBlocks.
    #[inline]
    async fn read_value(&self, location: &DiskLocation, value_len: u32) -> io::Result<Vec<u8>> {
        let file = self
            .file_manager
            .get_file(location.file_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Data file not found"))?;

        let file_guard = file.lock();

        // Use partial read if record size is known (optimization for small records)
        let wblock_data = if location.supports_partial_read() {
            file_guard.read_partial_wblock(
                location.wblock_id as u32,
                location.offset,
                location.record_size,
            )?
        } else {
            // Fall back to reading entire WBlock
            file_guard.read_wblock(location.wblock_id as u32)?
        };

        // Parse record from the correct offset within the buffer
        // For partial reads, offset adjustment is handled in read_partial_wblock
        let data_offset = if location.supports_partial_read() {
            // Partial read: data starts at the alignment boundary adjustment
            let abs_offset = (FILE_HEADER_SIZE as u64
                + location.wblock_id as u64 * WBLOCK_SIZE as u64
                + location.offset as u64) as usize;
            let aligned_offset = abs_offset & !(4096 - 1);
            abs_offset - aligned_offset
        } else {
            location.offset as usize
        };

        if data_offset >= wblock_data.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid record offset"));
        }

        let record = Record::from_bytes(&wblock_data[data_offset..])?;

        if record.header.is_deleted() || record.header.is_expired() {
            return Err(io::Error::new(io::ErrorKind::NotFound, "Record deleted or expired"));
        }

        Ok(record.value)
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
        use crate::engine::index_entry::MAX_INLINE_VALUE_SIZE;

        let key_hash = hash_key(key);
        let partition = self.partition_for(key_hash);
        let generation = self.next_generation.fetch_add(1, Ordering::SeqCst);

        // Create record
        let mut record = Record::new(key.to_vec(), value.to_vec(), generation, ttl)?;
        let record_size = record.serialized_size();

        // Append to partition's write buffer
        let mut location = self.write_buffers[partition].append(&mut record)?;
        location.record_size = record_size as u32;

        // Update index (with inline value for small values)
        if value.len() <= MAX_INLINE_VALUE_SIZE {
            self.index.insert_with_value(key, location, generation, value);
        } else {
            self.index.insert(key, location, generation, record.header.value_len);
        }

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
    /// Priority: bloom filter -> inline value -> write buffer -> disk
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

        // Fast path 3: Inline value (no disk!)
        if let Some(value) = entry.get_inline_value() {
            self.stats.get_hits.fetch_add(1, Ordering::Relaxed);
            return Some(value.to_vec());
        }

        // Fast path 4: Check partition's write buffer (not yet flushed)
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

    #[tokio::test]
    async fn test_put_get() {
        let (handler, _dir) = create_test_handler();

        // PUT
        let put_req = Request::put(1, b"key1", b"value1", 0);
        let put_resp = handler.handle(put_req).await;
        assert_eq!(put_resp.header.opcode, Opcode::Success);

        // Flush to disk
        handler.flush().await.unwrap();

        // GET (should use inline value from index)
        let get_req = Request::get(2, b"key1");
        let get_resp = handler.handle(get_req).await;
        assert_eq!(get_resp.header.opcode, Opcode::Success);
        assert_eq!(get_resp.payload, b"value1");

        // Check hit
        assert!(handler.stats().get_hits.load(Ordering::Relaxed) > 0);
    }

    #[tokio::test]
    async fn test_get_not_found() {
        let (handler, _dir) = create_test_handler();

        let get_req = Request::get(1, b"nonexistent");
        let get_resp = handler.handle(get_req).await;
        assert_eq!(get_resp.header.opcode, Opcode::NotFound);
    }

    #[tokio::test]
    async fn test_delete() {
        let (handler, _dir) = create_test_handler();

        // PUT
        let put_req = Request::put(1, b"del_key", b"del_value", 0);
        handler.handle(put_req).await;
        handler.flush().await.unwrap();

        // DELETE
        let del_req = Request::delete(2, b"del_key");
        let del_resp = handler.handle(del_req).await;
        assert_eq!(del_resp.header.opcode, Opcode::Success);

        // GET should return not found
        let get_req = Request::get(3, b"del_key");
        let get_resp = handler.handle(get_req).await;
        assert_eq!(get_resp.header.opcode, Opcode::NotFound);
    }

    #[tokio::test]
    async fn test_ping() {
        let (handler, _dir) = create_test_handler();

        let ping_req = Request::ping(42);
        let pong_resp = handler.handle(ping_req).await;
        assert_eq!(pong_resp.header.opcode, Opcode::Success);
        assert_eq!(pong_resp.header.request_id, 42);
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
}
