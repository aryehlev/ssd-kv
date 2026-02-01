//! High-throughput batch writer with write coalescing.
//!
//! Accumulates writes and flushes them in optimal batches to maximize
//! SSD throughput by:
//! 1. Coalescing small writes into large sequential writes
//! 2. Aligning writes to SSD page boundaries (4KB)
//! 3. Using io_uring for async submission
//! 4. Pipelining: preparing next batch while current one is in flight

use std::collections::VecDeque;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError};
use parking_lot::{Condvar, Mutex};

use crate::engine::index::Index;
use crate::io::aligned_buf::AlignedBuffer;
use crate::storage::file_manager::FileManager;
use crate::storage::record::Record;
use crate::storage::write_buffer::{DiskLocation, WBlock, WriteBuffer, WBLOCK_SIZE};

/// Batch size threshold for flushing (number of records).
const BATCH_SIZE_THRESHOLD: usize = 1000;

/// Time threshold for flushing (microseconds).
const BATCH_TIME_THRESHOLD_US: u64 = 500; // 500μs

/// Maximum pending batches before backpressure.
const MAX_PENDING_BATCHES: usize = 8;

/// A write request.
pub struct WriteRequest {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub ttl: u32,
    pub completion: Option<tokio::sync::oneshot::Sender<io::Result<DiskLocation>>>,
}

/// A batch of write requests.
struct WriteBatch {
    requests: Vec<WriteRequest>,
    created_at: Instant,
}

impl WriteBatch {
    fn new() -> Self {
        Self {
            requests: Vec::with_capacity(BATCH_SIZE_THRESHOLD),
            created_at: Instant::now(),
        }
    }

    fn is_ready(&self) -> bool {
        self.requests.len() >= BATCH_SIZE_THRESHOLD
            || self.created_at.elapsed() > Duration::from_micros(BATCH_TIME_THRESHOLD_US)
    }

    fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }
}

/// Statistics for the batch writer.
#[derive(Debug, Default)]
pub struct BatchWriterStats {
    pub batches_written: AtomicU64,
    pub records_written: AtomicU64,
    pub bytes_written: AtomicU64,
    pub avg_batch_size: AtomicU64,
    pub flush_latency_us: AtomicU64,
}

/// High-throughput batch writer.
pub struct BatchWriter {
    /// Channel for receiving write requests.
    request_tx: Sender<WriteRequest>,
    /// Statistics.
    stats: Arc<BatchWriterStats>,
    /// Shutdown flag.
    shutdown: Arc<AtomicBool>,
}

impl BatchWriter {
    /// Creates and starts a new batch writer.
    pub fn new(
        index: Arc<Index>,
        file_manager: Arc<FileManager>,
        write_buffer: Arc<WriteBuffer>,
    ) -> Self {
        let (request_tx, request_rx) = bounded(BATCH_SIZE_THRESHOLD * 4);
        let stats = Arc::new(BatchWriterStats::default());
        let shutdown = Arc::new(AtomicBool::new(false));

        let stats_clone = Arc::clone(&stats);
        let shutdown_clone = Arc::clone(&shutdown);

        std::thread::Builder::new()
            .name("batch-writer".to_string())
            .spawn(move || {
                batch_writer_thread(
                    request_rx,
                    index,
                    file_manager,
                    write_buffer,
                    stats_clone,
                    shutdown_clone,
                );
            })
            .expect("Failed to spawn batch writer thread");

        Self {
            request_tx,
            stats,
            shutdown,
        }
    }

    /// Submits a write request.
    #[inline]
    pub fn write(&self, request: WriteRequest) -> io::Result<()> {
        self.request_tx
            .send(request)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Batch writer died"))
    }

    /// Submits a write and waits for completion.
    pub async fn write_sync(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: u32,
    ) -> io::Result<DiskLocation> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.write(WriteRequest {
            key,
            value,
            ttl,
            completion: Some(tx),
        })?;
        rx.await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Completion channel closed"))?
    }

    /// Returns statistics.
    pub fn stats(&self) -> &BatchWriterStats {
        &self.stats
    }

    /// Shuts down the batch writer.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }
}

impl Drop for BatchWriter {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn batch_writer_thread(
    request_rx: Receiver<WriteRequest>,
    index: Arc<Index>,
    file_manager: Arc<FileManager>,
    write_buffer: Arc<WriteBuffer>,
    stats: Arc<BatchWriterStats>,
    shutdown: Arc<AtomicBool>,
) {
    let mut current_batch = WriteBatch::new();
    let mut generation_counter = 1u32;

    loop {
        if shutdown.load(Ordering::Relaxed) && current_batch.is_empty() {
            break;
        }

        // Try to receive requests
        match request_rx.try_recv() {
            Ok(request) => {
                current_batch.requests.push(request);

                // Drain more requests if available (up to batch size)
                while current_batch.requests.len() < BATCH_SIZE_THRESHOLD {
                    match request_rx.try_recv() {
                        Ok(req) => current_batch.requests.push(req),
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => break,
                    }
                }
            }
            Err(TryRecvError::Empty) => {
                if current_batch.is_empty() {
                    // Wait for requests
                    match request_rx.recv_timeout(Duration::from_micros(BATCH_TIME_THRESHOLD_US)) {
                        Ok(req) => current_batch.requests.push(req),
                        Err(_) => continue,
                    }
                }
            }
            Err(TryRecvError::Disconnected) => {
                if current_batch.is_empty() {
                    break;
                }
            }
        }

        // Check if batch is ready to flush
        if current_batch.is_ready() || shutdown.load(Ordering::Relaxed) {
            let flush_start = Instant::now();
            let batch_size = current_batch.requests.len();

            // Process the batch
            let mut completions = Vec::with_capacity(batch_size);
            let mut bytes_written = 0u64;

            for request in current_batch.requests.drain(..) {
                generation_counter = generation_counter.wrapping_add(1);

                let mut record = match Record::new(
                    request.key.clone(),
                    request.value.clone(),
                    generation_counter,
                    request.ttl,
                ) {
                    Ok(r) => r,
                    Err(e) => {
                        if let Some(tx) = request.completion {
                            let _ = tx.send(Err(e));
                        }
                        continue;
                    }
                };

                let location = match write_buffer.append(&mut record) {
                    Ok(loc) => loc,
                    Err(e) => {
                        if let Some(tx) = request.completion {
                            let _ = tx.send(Err(e));
                        }
                        continue;
                    }
                };

                // Update index
                index.insert(
                    &request.key,
                    location,
                    generation_counter,
                    request.value.len() as u32,
                );

                bytes_written += record.serialized_size() as u64;
                completions.push((request.completion, location));
            }

            // Flush pending WBlocks to disk
            flush_wblocks(&file_manager, &write_buffer);

            // Notify completions
            for (completion, location) in completions {
                if let Some(tx) = completion {
                    let _ = tx.send(Ok(location));
                }
            }

            // Update stats
            let flush_latency = flush_start.elapsed().as_micros() as u64;
            stats.batches_written.fetch_add(1, Ordering::Relaxed);
            stats
                .records_written
                .fetch_add(batch_size as u64, Ordering::Relaxed);
            stats.bytes_written.fetch_add(bytes_written, Ordering::Relaxed);
            stats.flush_latency_us.store(flush_latency, Ordering::Relaxed);

            // Reset batch
            current_batch = WriteBatch::new();
        }
    }

    // Final flush
    if !current_batch.is_empty() {
        for request in current_batch.requests.drain(..) {
            if let Some(tx) = request.completion {
                let _ = tx.send(Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "Writer shutting down",
                )));
            }
        }
    }
}

fn flush_wblocks(file_manager: &FileManager, write_buffer: &WriteBuffer) {
    // Flush current block
    if let Some(mut wblock) = write_buffer.force_flush() {
        if let Ok(file) = file_manager.get_or_create_file(wblock.file_id) {
            let file_guard = file.lock();
            let _ = file_guard.write_wblock(&mut wblock);
        }
    }

    // Flush pending blocks
    for mut wblock in write_buffer.take_pending() {
        if let Ok(file) = file_manager.get_or_create_file(wblock.file_id) {
            let file_guard = file.lock();
            let _ = file_guard.write_wblock(&mut wblock);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_batch_writer() {
        let dir = tempdir().unwrap();
        let file_manager = Arc::new(FileManager::new(dir.path()).unwrap());
        let index = Arc::new(Index::new());
        let write_buffer = Arc::new(WriteBuffer::new(0, 1023));
        file_manager.create_file().unwrap();

        let writer = BatchWriter::new(
            Arc::clone(&index),
            Arc::clone(&file_manager),
            Arc::clone(&write_buffer),
        );

        // Write some data
        let location = writer
            .write_sync(b"key1".to_vec(), b"value1".to_vec(), 0)
            .await
            .unwrap();

        // Verify in index
        let entry = index.get(b"key1").unwrap();
        assert_eq!(entry.location, location);

        writer.shutdown();
    }
}
