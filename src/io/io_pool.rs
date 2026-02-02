//! Dedicated I/O thread pool for parallel disk operations.
//!
//! Provides a pool of I/O threads, each with its own io_uring instance,
//! for maximum parallel disk throughput.

use std::io;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError};

use crate::io::aligned_buf::AlignedBuffer;
use crate::io::uring::{IoOperation, IoResult, UringManager};

/// Default batch size for io_uring submissions.
pub const DEFAULT_BATCH_SIZE: usize = 64;

/// Request for an I/O operation.
#[derive(Debug)]
pub struct IoPoolRequest {
    pub fd: RawFd,
    pub offset: u64,
    pub len: u32,
    pub buffer: Option<AlignedBuffer>,
    pub operation: IoOperation,
    pub user_data: u64,
}

/// Statistics for the I/O pool.
#[derive(Debug, Default)]
pub struct IoPoolStats {
    pub reads_submitted: AtomicU64,
    pub writes_submitted: AtomicU64,
    pub reads_completed: AtomicU64,
    pub writes_completed: AtomicU64,
    pub bytes_read: AtomicU64,
    pub bytes_written: AtomicU64,
}

/// A single I/O worker thread.
struct IoWorker {
    handle: JoinHandle<()>,
    request_tx: Sender<IoPoolRequest>,
}

/// Pool of I/O threads for parallel disk operations.
pub struct IoPool {
    workers: Vec<IoWorker>,
    result_rx: Receiver<IoResult>,
    stats: Arc<IoPoolStats>,
    shutdown: Arc<AtomicBool>,
    next_id: AtomicU64,
}

impl IoPool {
    /// Creates a new I/O pool with the specified number of workers.
    /// Each worker gets its own io_uring instance and handles requests
    /// for a specific partition.
    pub fn new(num_workers: usize, queue_depth: u32) -> io::Result<Self> {
        let num_workers = num_workers.max(1);
        let (result_tx, result_rx) = bounded(queue_depth as usize * num_workers);
        let shutdown = Arc::new(AtomicBool::new(false));
        let stats = Arc::new(IoPoolStats::default());

        let mut workers = Vec::with_capacity(num_workers);

        for worker_id in 0..num_workers {
            let (request_tx, request_rx) = bounded(queue_depth as usize);
            let result_tx = result_tx.clone();
            let shutdown = Arc::clone(&shutdown);
            let stats = Arc::clone(&stats);

            let handle = thread::Builder::new()
                .name(format!("io-worker-{}", worker_id))
                .spawn(move || {
                    io_worker_loop(worker_id, request_rx, result_tx, shutdown, stats, queue_depth);
                })?;

            workers.push(IoWorker { handle, request_tx });
        }

        Ok(Self {
            workers,
            result_rx,
            stats,
            shutdown,
            next_id: AtomicU64::new(1),
        })
    }

    /// Returns the number of workers in the pool.
    pub fn num_workers(&self) -> usize {
        self.workers.len()
    }

    /// Generates a unique request ID.
    pub fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Submits a read request to a specific partition/worker.
    pub fn submit_read(
        &self,
        partition: usize,
        fd: RawFd,
        offset: u64,
        len: u32,
        user_data: u64,
    ) -> io::Result<()> {
        let worker_idx = partition % self.workers.len();
        let buffer = AlignedBuffer::new(len as usize);

        self.workers[worker_idx]
            .request_tx
            .send(IoPoolRequest {
                fd,
                offset,
                len,
                buffer: Some(buffer),
                operation: IoOperation::Read,
                user_data,
            })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "I/O worker died"))
    }

    /// Submits a write request to a specific partition/worker.
    pub fn submit_write(
        &self,
        partition: usize,
        fd: RawFd,
        offset: u64,
        buffer: AlignedBuffer,
        user_data: u64,
    ) -> io::Result<()> {
        let worker_idx = partition % self.workers.len();

        self.workers[worker_idx]
            .request_tx
            .send(IoPoolRequest {
                fd,
                offset,
                len: buffer.len() as u32,
                buffer: Some(buffer),
                operation: IoOperation::Write,
                user_data,
            })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "I/O worker died"))
    }

    /// Submits an fsync request to a specific partition/worker.
    pub fn submit_fsync(&self, partition: usize, fd: RawFd, user_data: u64) -> io::Result<()> {
        let worker_idx = partition % self.workers.len();

        self.workers[worker_idx]
            .request_tx
            .send(IoPoolRequest {
                fd,
                offset: 0,
                len: 0,
                buffer: None,
                operation: IoOperation::Fsync,
                user_data,
            })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "I/O worker died"))
    }

    /// Receives a completed I/O result (blocking).
    pub fn recv(&self) -> Option<IoResult> {
        self.result_rx.recv().ok()
    }

    /// Tries to receive a completed I/O result (non-blocking).
    pub fn try_recv(&self) -> Option<IoResult> {
        self.result_rx.try_recv().ok()
    }

    /// Receives all available completed I/O results (non-blocking).
    pub fn recv_all(&self) -> Vec<IoResult> {
        let mut results = Vec::new();
        loop {
            match self.result_rx.try_recv() {
                Ok(result) => results.push(result),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break,
            }
        }
        results
    }

    /// Returns the pool statistics.
    pub fn stats(&self) -> &Arc<IoPoolStats> {
        &self.stats
    }

    /// Shuts down the I/O pool.
    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);

        // Drop all senders to unblock workers
        for worker in self.workers.drain(..) {
            drop(worker.request_tx);
            let _ = worker.handle.join();
        }
    }
}

impl Drop for IoPool {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// I/O worker thread main loop.
fn io_worker_loop(
    worker_id: usize,
    request_rx: Receiver<IoPoolRequest>,
    result_tx: Sender<IoResult>,
    shutdown: Arc<AtomicBool>,
    stats: Arc<IoPoolStats>,
    queue_depth: u32,
) {
    // Create io_uring instance for this worker
    let mut manager = UringManager::new(queue_depth)
        .or_else(|_| UringManager::new_simple(queue_depth))
        .expect("Failed to create I/O manager");

    let mut pending_count = 0usize;

    loop {
        // Check for shutdown
        if shutdown.load(Ordering::Relaxed) && pending_count == 0 {
            break;
        }

        // Collect incoming requests (batching)
        let mut batch_count = 0;
        while batch_count < DEFAULT_BATCH_SIZE {
            match request_rx.try_recv() {
                Ok(req) => {
                    let result = match req.operation {
                        IoOperation::Read => {
                            stats.reads_submitted.fetch_add(1, Ordering::Relaxed);
                            let buffer = req.buffer.unwrap_or_else(|| AlignedBuffer::new(req.len as usize));
                            manager.submit_read(req.fd, buffer, req.offset, req.len)
                        }
                        IoOperation::Write => {
                            stats.writes_submitted.fetch_add(1, Ordering::Relaxed);
                            stats.bytes_written.fetch_add(req.len as u64, Ordering::Relaxed);
                            let buffer = req.buffer.expect("Write requires buffer");
                            manager.submit_write(req.fd, buffer, req.offset)
                        }
                        IoOperation::Fsync => {
                            manager.submit_fsync(req.fd)
                        }
                    };

                    if result.is_ok() {
                        pending_count += 1;
                        batch_count += 1;
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    if pending_count == 0 {
                        return;
                    }
                    break;
                }
            }
        }

        // Submit batch to kernel (or process synchronously on fallback)
        if pending_count > 0 {
            if let Err(e) = manager.submit_and_wait(1) {
                tracing::error!("Worker {} I/O submit failed: {}", worker_id, e);
                continue;
            }

            // Collect completions and send results
            for result in manager.collect_completions() {
                pending_count -= 1;

                // Update stats
                match result.operation {
                    IoOperation::Read => {
                        stats.reads_completed.fetch_add(1, Ordering::Relaxed);
                        if result.result > 0 {
                            stats.bytes_read.fetch_add(result.result as u64, Ordering::Relaxed);
                        }
                    }
                    IoOperation::Write => {
                        stats.writes_completed.fetch_add(1, Ordering::Relaxed);
                    }
                    IoOperation::Fsync => {}
                }

                if result_tx.send(result).is_err() {
                    return; // Main thread died
                }
            }
        } else {
            // No pending ops, wait for new requests
            match request_rx.recv() {
                Ok(req) => {
                    let result = match req.operation {
                        IoOperation::Read => {
                            stats.reads_submitted.fetch_add(1, Ordering::Relaxed);
                            let buffer = req.buffer.unwrap_or_else(|| AlignedBuffer::new(req.len as usize));
                            manager.submit_read(req.fd, buffer, req.offset, req.len)
                        }
                        IoOperation::Write => {
                            stats.writes_submitted.fetch_add(1, Ordering::Relaxed);
                            stats.bytes_written.fetch_add(req.len as u64, Ordering::Relaxed);
                            let buffer = req.buffer.expect("Write requires buffer");
                            manager.submit_write(req.fd, buffer, req.offset)
                        }
                        IoOperation::Fsync => {
                            manager.submit_fsync(req.fd)
                        }
                    };

                    if result.is_ok() {
                        pending_count += 1;
                    }
                }
                Err(_) => return, // Channel closed
            }
        }
    }
}

/// Batched reader that collects pending reads and submits them in batches.
pub struct BatchedReader {
    pending: Vec<PendingRead>,
    io_pool: Arc<IoPool>,
    max_batch_size: usize,
}

struct PendingRead {
    fd: RawFd,
    offset: u64,
    len: u32,
    partition: usize,
    user_data: u64,
}

impl BatchedReader {
    /// Creates a new batched reader.
    pub fn new(io_pool: Arc<IoPool>, max_batch_size: usize) -> Self {
        Self {
            pending: Vec::with_capacity(max_batch_size),
            io_pool,
            max_batch_size,
        }
    }

    /// Adds a read to the pending batch.
    pub fn add_read(
        &mut self,
        partition: usize,
        fd: RawFd,
        offset: u64,
        len: u32,
        user_data: u64,
    ) {
        self.pending.push(PendingRead {
            fd,
            offset,
            len,
            partition,
            user_data,
        });

        // Auto-flush if batch is full
        if self.pending.len() >= self.max_batch_size {
            self.flush();
        }
    }

    /// Submits all pending reads to the I/O pool.
    pub fn flush(&mut self) {
        for read in self.pending.drain(..) {
            let _ = self.io_pool.submit_read(
                read.partition,
                read.fd,
                read.offset,
                read.len,
                read.user_data,
            );
        }
    }

    /// Returns the number of pending reads.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_pool_creation() {
        let pool = IoPool::new(2, 32);
        assert!(pool.is_ok());
        let pool = pool.unwrap();
        assert_eq!(pool.num_workers(), 2);
    }

    #[test]
    fn test_io_pool_id_generation() {
        let pool = IoPool::new(1, 32).unwrap();
        let id1 = pool.next_id();
        let id2 = pool.next_id();
        assert_ne!(id1, id2);
    }
}
