//! Request dispatch and handling with performance optimizations.

use std::io;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use tracing::{error, trace};

use crate::engine::index::Index;
use crate::engine::index_entry::hash_key;
use crate::perf::hot_cache::HotCache;
use crate::perf::prefetch::{prefetch_read, BloomFilter};
use crate::server::protocol::{Opcode, Request, Response};
use crate::storage::file_manager::{FileManager, FILE_HEADER_SIZE};
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
    hot_cache: Arc<HotCache>,
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
            hot_cache: Arc::new(HotCache::new()),
            bloom_filter: parking_lot::RwLock::new(BloomFilter::new(1_000_000, 0.01)),
            next_generation: AtomicU32::new(1),
            stats: Arc::new(HandlerStats::default()),
        }
    }

    /// Creates a handler with a custom hot cache.
    pub fn with_hot_cache(
        index: Arc<Index>,
        file_manager: Arc<FileManager>,
        write_buffer: Arc<WriteBuffer>,
        hot_cache: Arc<HotCache>,
    ) -> Self {
        Self {
            index,
            file_manager,
            write_buffer,
            hot_cache,
            bloom_filter: parking_lot::RwLock::new(BloomFilter::new(1_000_000, 0.01)),
            next_generation: AtomicU32::new(1),
            stats: Arc::new(HandlerStats::default()),
        }
    }

    /// Returns the handler statistics.
    pub fn stats(&self) -> Arc<HandlerStats> {
        Arc::clone(&self.stats)
    }

    /// Returns the hot cache.
    pub fn hot_cache(&self) -> Arc<HotCache> {
        Arc::clone(&self.hot_cache)
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

        // Update hot cache
        self.hot_cache.put(key, key_hash, value, generation);

        self.stats.puts.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Synchronous DELETE for Redis compatibility
    pub fn delete_sync(&self, key: &[u8]) -> io::Result<bool> {
        let key_hash = hash_key(key);
        let generation = self.next_generation.fetch_add(1, Ordering::SeqCst);

        // Create tombstone record
        let mut record = Record::tombstone(key.to_vec(), generation)?;

        // Append tombstone to write buffer
        self.write_buffer.append(&mut record)?;

        // Update index
        let deleted = self.index.delete(key, generation);

        // Invalidate hot cache
        self.hot_cache.invalidate(key_hash);

        self.stats.deletes.fetch_add(1, Ordering::Relaxed);
        Ok(deleted)
    }

    /// Fast synchronous GET for UDP/Redis - returns value if found.
    /// Priority: hot cache -> inline value -> write buffer -> disk
    #[inline]
    pub fn get_value(&self, key: &[u8]) -> Option<Vec<u8>> {
        let key_hash = hash_key(key);

        // Fast path 1: Check hot cache
        if let Some(value) = self.hot_cache.get(key, key_hash) {
            return Some(value);
        }

        // Fast path 2: Check index
        let entry = self.index.get(key)?;

        // Fast path 3: Inline value (no disk!)
        if let Some(value) = entry.get_inline_value() {
            self.hot_cache.put(key, key_hash, value, entry.generation);
            return Some(value.to_vec());
        }

        // Fast path 4: Write buffer (not yet flushed)
        if let Some(value) = self.write_buffer.read_unflushed(&entry.location, key) {
            self.hot_cache.put(key, key_hash, &value, entry.generation);
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

        self.hot_cache.put(key, key_hash, &record.value, entry.generation);
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

    /// Handles a GET request with hot cache acceleration.
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

        // Fast path 2: Check hot cache (lock-free read)
        if let Some(value) = self.hot_cache.get(&key, key_hash) {
            self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            self.stats.get_hits.fetch_add(1, Ordering::Relaxed);
            return Response::success(request_id, &value);
        }

        trace!("GET key={:?} (cache miss)", String::from_utf8_lossy(&key));

        // Slow path: Look up in index
        let entry = match self.index.get(&key) {
            Some(e) => e,
            None => {
                self.stats.get_misses.fetch_add(1, Ordering::Relaxed);
                return Response::not_found(request_id);
            }
        };

        // Fast path 3: Check for inline value (NO DISK READ!)
        if let Some(value) = entry.get_inline_value() {
            self.stats.get_hits.fetch_add(1, Ordering::Relaxed);
            self.hot_cache.put(&key, key_hash, value, entry.generation);
            return Response::success(request_id, value);
        }

        // Fast path 4: Check write buffer for unflushed data
        if let Some(value) = self.write_buffer.read_unflushed(&entry.location, &key) {
            self.stats.get_hits.fetch_add(1, Ordering::Relaxed);
            // Update hot cache
            self.hot_cache.put(&key, key_hash, &value, entry.generation);
            return Response::success(request_id, &value);
        }

        // Prefetch the disk location data
        prefetch_read(&entry.location as *const DiskLocation);

        // Slow path: Read value from disk (only for large values)
        match self.read_value(&entry.location, entry.value_len).await {
            Ok(value) => {
                self.stats.get_hits.fetch_add(1, Ordering::Relaxed);

                // Update hot cache for next access
                self.hot_cache.put(&key, key_hash, &value, entry.generation);

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

        // Update hot cache
        self.hot_cache.put(&key, key_hash, &value, generation);

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

        let key_hash = hash_key(&key);

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

        // Invalidate hot cache
        self.hot_cache.invalidate(key_hash);

        Response::success(request_id, &[])
    }

    /// Handles a STATS request.
    fn handle_stats(&self, request_id: u32) -> Response {
        let index_stats = self.index.stats();
        let cache_stats = self.hot_cache.stats();
        let json = format!(
            r#"{{"index":{{"entries":{},"live":{},"tombstones":{}}},"cache":{{"capacity":{},"used":{}}},"ops":{}}}"#,
            index_stats.total_entries,
            index_stats.live_entries,
            index_stats.tombstones,
            cache_stats.capacity,
            cache_stats.used,
            self.stats.to_json(),
        );
        Response::stats(request_id, &json)
    }

    /// Reads a value from disk.
    #[inline]
    async fn read_value(&self, location: &DiskLocation, value_len: u32) -> io::Result<Vec<u8>> {
        let file = self
            .file_manager
            .get_file(location.file_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Data file not found"))?;

        let file_guard = file.lock();

        // Read the entire WBlock (records are within WBlocks)
        let wblock_data = file_guard.read_wblock(location.wblock_id as u32)?;

        // Parse record from the correct offset within the WBlock
        let offset = location.offset as usize;
        if offset >= wblock_data.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid record offset"));
        }

        let record = Record::from_bytes(&wblock_data[offset..])?;

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

        // GET (should hit hot cache)
        let get_req = Request::get(2, b"key1");
        let get_resp = handler.handle(get_req).await;
        assert_eq!(get_resp.header.opcode, Opcode::Success);
        assert_eq!(get_resp.payload, b"value1");

        // Check cache hit
        assert!(handler.stats().cache_hits.load(Ordering::Relaxed) > 0);
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
}
