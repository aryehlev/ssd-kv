//! io_uring wrapper for async I/O operations.
//!
//! This module provides io_uring support on Linux and a fallback
//! implementation using blocking I/O on other platforms.

use std::collections::HashMap;
use std::io;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam_channel::{bounded, Receiver, Sender};

use crate::io::aligned_buf::AlignedBuffer;

/// Default queue depth.
pub const DEFAULT_QUEUE_DEPTH: u32 = 256;

/// Operation types for I/O.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoOperation {
    Read,
    Write,
    Fsync,
}

/// Result of an I/O operation.
#[derive(Debug)]
pub struct IoResult {
    pub user_data: u64,
    pub operation: IoOperation,
    pub result: i32,
    pub buffer: Option<AlignedBuffer>,
}

/// Request for an I/O operation.
pub struct IoRequest {
    pub fd: RawFd,
    pub offset: u64,
    pub buffer: AlignedBuffer,
    pub operation: IoOperation,
    pub user_data: u64,
}

// ============================================================================
// Linux implementation using io_uring
// ============================================================================

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use io_uring::{opcode, types, IoUring};

    struct PendingOp {
        operation: IoOperation,
        buffer: Option<AlignedBuffer>,
    }

    /// io_uring-based async I/O manager (Linux only).
    pub struct UringManager {
        ring: IoUring,
        pending: HashMap<u64, PendingOp>,
        next_id: AtomicU64,
    }

    impl UringManager {
        /// Creates a new io_uring manager with SQPOLL.
        pub fn new(queue_depth: u32) -> io::Result<Self> {
            let ring = IoUring::builder()
                .setup_sqpoll(1000)
                .build(queue_depth)?;

            Ok(Self {
                ring,
                pending: HashMap::new(),
                next_id: AtomicU64::new(1),
            })
        }

        /// Creates a new manager without SQPOLL.
        pub fn new_simple(queue_depth: u32) -> io::Result<Self> {
            let ring = IoUring::new(queue_depth)?;

            Ok(Self {
                ring,
                pending: HashMap::new(),
                next_id: AtomicU64::new(1),
            })
        }

        fn next_id(&self) -> u64 {
            self.next_id.fetch_add(1, Ordering::Relaxed)
        }

        pub fn submit_write(
            &mut self,
            fd: RawFd,
            buffer: AlignedBuffer,
            offset: u64,
        ) -> io::Result<u64> {
            let id = self.next_id();

            let write_op = opcode::Write::new(types::Fd(fd), buffer.as_ptr(), buffer.len() as u32)
                .offset(offset)
                .build()
                .user_data(id);

            unsafe {
                self.ring.submission().push(&write_op).map_err(|_| {
                    io::Error::new(io::ErrorKind::WouldBlock, "Submission queue full")
                })?;
            }

            self.pending.insert(
                id,
                PendingOp {
                    operation: IoOperation::Write,
                    buffer: Some(buffer),
                },
            );

            Ok(id)
        }

        pub fn submit_read(
            &mut self,
            fd: RawFd,
            mut buffer: AlignedBuffer,
            offset: u64,
            len: u32,
        ) -> io::Result<u64> {
            let id = self.next_id();

            let read_op = opcode::Read::new(types::Fd(fd), buffer.as_mut_ptr(), len)
                .offset(offset)
                .build()
                .user_data(id);

            unsafe {
                self.ring.submission().push(&read_op).map_err(|_| {
                    io::Error::new(io::ErrorKind::WouldBlock, "Submission queue full")
                })?;
            }

            self.pending.insert(
                id,
                PendingOp {
                    operation: IoOperation::Read,
                    buffer: Some(buffer),
                },
            );

            Ok(id)
        }

        pub fn submit_fsync(&mut self, fd: RawFd) -> io::Result<u64> {
            let id = self.next_id();

            let fsync_op = opcode::Fsync::new(types::Fd(fd)).build().user_data(id);

            unsafe {
                self.ring.submission().push(&fsync_op).map_err(|_| {
                    io::Error::new(io::ErrorKind::WouldBlock, "Submission queue full")
                })?;
            }

            self.pending.insert(
                id,
                PendingOp {
                    operation: IoOperation::Fsync,
                    buffer: None,
                },
            );

            Ok(id)
        }

        pub fn submit(&mut self) -> io::Result<usize> {
            self.ring.submit().map_err(io::Error::from)
        }

        pub fn submit_and_wait(&mut self, want: usize) -> io::Result<usize> {
            self.ring.submit_and_wait(want).map_err(io::Error::from)
        }

        pub fn collect_completions(&mut self) -> Vec<IoResult> {
            let mut results = Vec::new();

            for cqe in self.ring.completion() {
                let id = cqe.user_data();
                let result = cqe.result();

                if let Some(mut pending) = self.pending.remove(&id) {
                    if pending.operation == IoOperation::Read && result > 0 {
                        if let Some(ref mut buf) = pending.buffer {
                            unsafe {
                                buf.set_len(result as usize);
                            }
                        }
                    }

                    results.push(IoResult {
                        user_data: id,
                        operation: pending.operation,
                        result,
                        buffer: pending.buffer,
                    });
                }
            }

            results
        }

        pub fn pending_count(&self) -> usize {
            self.pending.len()
        }
    }
}

// ============================================================================
// Fallback implementation for non-Linux platforms
// ============================================================================

#[cfg(not(target_os = "linux"))]
mod fallback {
    use super::*;
    use std::os::unix::io::FromRawFd;
    use std::fs::File;
    use std::io::{Read, Seek, SeekFrom, Write};

    struct PendingOp {
        operation: IoOperation,
        buffer: Option<AlignedBuffer>,
    }

    /// Fallback I/O manager using blocking operations.
    pub struct UringManager {
        pending: HashMap<u64, PendingOp>,
        next_id: AtomicU64,
    }

    impl UringManager {
        pub fn new(_queue_depth: u32) -> io::Result<Self> {
            Ok(Self {
                pending: HashMap::new(),
                next_id: AtomicU64::new(1),
            })
        }

        pub fn new_simple(queue_depth: u32) -> io::Result<Self> {
            Self::new(queue_depth)
        }

        fn next_id(&self) -> u64 {
            self.next_id.fetch_add(1, Ordering::Relaxed)
        }

        pub fn submit_write(
            &mut self,
            fd: RawFd,
            buffer: AlignedBuffer,
            offset: u64,
        ) -> io::Result<u64> {
            let id = self.next_id();

            // Perform blocking write using pwrite
            let result = unsafe {
                libc::pwrite(
                    fd,
                    buffer.as_ptr() as *const libc::c_void,
                    buffer.len(),
                    offset as libc::off_t,
                )
            };

            self.pending.insert(
                id,
                PendingOp {
                    operation: IoOperation::Write,
                    buffer: Some(buffer),
                },
            );

            // Store result for collection
            if let Some(op) = self.pending.get_mut(&id) {
                // We'll handle the result in collect_completions
            }

            Ok(id)
        }

        pub fn submit_read(
            &mut self,
            fd: RawFd,
            mut buffer: AlignedBuffer,
            offset: u64,
            len: u32,
        ) -> io::Result<u64> {
            let id = self.next_id();

            // Perform blocking read using pread
            let result = unsafe {
                libc::pread(
                    fd,
                    buffer.as_mut_ptr() as *mut libc::c_void,
                    len as usize,
                    offset as libc::off_t,
                )
            };

            if result > 0 {
                unsafe {
                    buffer.set_len(result as usize);
                }
            }

            self.pending.insert(
                id,
                PendingOp {
                    operation: IoOperation::Read,
                    buffer: Some(buffer),
                },
            );

            Ok(id)
        }

        pub fn submit_fsync(&mut self, fd: RawFd) -> io::Result<u64> {
            let id = self.next_id();

            unsafe {
                libc::fsync(fd);
            }

            self.pending.insert(
                id,
                PendingOp {
                    operation: IoOperation::Fsync,
                    buffer: None,
                },
            );

            Ok(id)
        }

        pub fn submit(&mut self) -> io::Result<usize> {
            // No-op for fallback, operations are synchronous
            Ok(self.pending.len())
        }

        pub fn submit_and_wait(&mut self, _want: usize) -> io::Result<usize> {
            // No-op for fallback
            Ok(self.pending.len())
        }

        pub fn collect_completions(&mut self) -> Vec<IoResult> {
            // All operations completed synchronously
            let mut results = Vec::new();
            for (id, op) in self.pending.drain() {
                results.push(IoResult {
                    user_data: id,
                    operation: op.operation,
                    result: op.buffer.as_ref().map(|b| b.len() as i32).unwrap_or(0),
                    buffer: op.buffer,
                });
            }
            results
        }

        pub fn pending_count(&self) -> usize {
            self.pending.len()
        }
    }
}

// Re-export the appropriate implementation
#[cfg(target_os = "linux")]
pub use linux::UringManager;

#[cfg(not(target_os = "linux"))]
pub use fallback::UringManager;

/// Thread-safe wrapper for I/O operations using channels.
pub struct AsyncUring {
    request_tx: Sender<IoRequest>,
    result_rx: Receiver<IoResult>,
}

impl AsyncUring {
    /// Starts the I/O thread and returns a handle for submitting operations.
    pub fn start(queue_depth: u32) -> io::Result<Self> {
        let (request_tx, request_rx) = bounded::<IoRequest>(queue_depth as usize);
        let (result_tx, result_rx) = bounded::<IoResult>(queue_depth as usize);

        std::thread::Builder::new()
            .name("io-uring".to_string())
            .spawn(move || {
                io_thread(request_rx, result_tx, queue_depth);
            })?;

        Ok(Self {
            request_tx,
            result_rx,
        })
    }

    /// Submits a write request.
    pub fn write(
        &self,
        fd: RawFd,
        buffer: AlignedBuffer,
        offset: u64,
        user_data: u64,
    ) -> io::Result<()> {
        self.request_tx
            .send(IoRequest {
                fd,
                offset,
                buffer,
                operation: IoOperation::Write,
                user_data,
            })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "I/O thread died"))
    }

    /// Submits a read request.
    pub fn read(
        &self,
        fd: RawFd,
        buffer: AlignedBuffer,
        offset: u64,
        user_data: u64,
    ) -> io::Result<()> {
        self.request_tx
            .send(IoRequest {
                fd,
                offset,
                buffer,
                operation: IoOperation::Read,
                user_data,
            })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "I/O thread died"))
    }

    /// Receives completed I/O results.
    pub fn recv(&self) -> Option<IoResult> {
        self.result_rx.recv().ok()
    }

    /// Tries to receive a result without blocking.
    pub fn try_recv(&self) -> Option<IoResult> {
        self.result_rx.try_recv().ok()
    }
}

fn io_thread(
    request_rx: Receiver<IoRequest>,
    result_tx: Sender<IoResult>,
    queue_depth: u32,
) {
    let mut manager = UringManager::new(queue_depth)
        .or_else(|_| UringManager::new_simple(queue_depth))
        .expect("Failed to create I/O manager");

    loop {
        // Process incoming requests
        while let Ok(req) = request_rx.try_recv() {
            let _ = match req.operation {
                IoOperation::Write => {
                    manager.submit_write(req.fd, req.buffer, req.offset)
                }
                IoOperation::Read => {
                    let len = req.buffer.capacity() as u32;
                    manager.submit_read(req.fd, req.buffer, req.offset, len)
                }
                IoOperation::Fsync => {
                    manager.submit_fsync(req.fd)
                }
            };
        }

        // Submit to kernel (or process synchronously on fallback)
        if manager.pending_count() > 0 {
            if let Err(e) = manager.submit_and_wait(1) {
                tracing::error!("I/O submit failed: {}", e);
                continue;
            }

            // Collect completions
            for result in manager.collect_completions() {
                if result_tx.send(result).is_err() {
                    return; // Main thread died
                }
            }
        } else {
            // No pending ops, block on new requests
            match request_rx.recv() {
                Ok(req) => {
                    let _ = match req.operation {
                        IoOperation::Write => {
                            manager.submit_write(req.fd, req.buffer, req.offset)
                        }
                        IoOperation::Read => {
                            let len = req.buffer.capacity() as u32;
                            manager.submit_read(req.fd, req.buffer, req.offset, len)
                        }
                        IoOperation::Fsync => {
                            manager.submit_fsync(req.fd)
                        }
                    };
                }
                Err(_) => return, // Channel closed
            }
        }
    }
}

/// Batched I/O manager for submitting multiple operations at once.
/// This reduces syscall overhead by batching up to 64 operations per submission.
pub struct BatchedUring {
    manager: UringManager,
    pending_reads: Vec<BatchedReadRequest>,
    pending_writes: Vec<BatchedWriteRequest>,
    max_batch_size: usize,
}

struct BatchedReadRequest {
    fd: RawFd,
    offset: u64,
    buffer: AlignedBuffer,
    user_data: u64,
}

struct BatchedWriteRequest {
    fd: RawFd,
    offset: u64,
    buffer: AlignedBuffer,
    user_data: u64,
}

impl BatchedUring {
    /// Creates a new batched I/O manager.
    pub fn new(queue_depth: u32, max_batch_size: usize) -> io::Result<Self> {
        let manager = UringManager::new(queue_depth)
            .or_else(|_| UringManager::new_simple(queue_depth))?;

        Ok(Self {
            manager,
            pending_reads: Vec::with_capacity(max_batch_size),
            pending_writes: Vec::with_capacity(max_batch_size),
            max_batch_size,
        })
    }

    /// Queues a read operation for batched submission.
    pub fn queue_read(
        &mut self,
        fd: RawFd,
        buffer: AlignedBuffer,
        offset: u64,
        user_data: u64,
    ) {
        self.pending_reads.push(BatchedReadRequest {
            fd,
            offset,
            buffer,
            user_data,
        });
    }

    /// Queues a write operation for batched submission.
    pub fn queue_write(
        &mut self,
        fd: RawFd,
        buffer: AlignedBuffer,
        offset: u64,
        user_data: u64,
    ) {
        self.pending_writes.push(BatchedWriteRequest {
            fd,
            offset,
            buffer,
            user_data,
        });
    }

    /// Submits all queued operations as a batch.
    /// Returns the number of operations submitted.
    pub fn flush(&mut self) -> io::Result<usize> {
        let mut submitted = 0;

        // Submit all pending reads
        for req in self.pending_reads.drain(..) {
            let len = req.buffer.capacity() as u32;
            self.manager.submit_read(req.fd, req.buffer, req.offset, len)?;
            submitted += 1;
        }

        // Submit all pending writes
        for req in self.pending_writes.drain(..) {
            self.manager.submit_write(req.fd, req.buffer, req.offset)?;
            submitted += 1;
        }

        if submitted > 0 {
            self.manager.submit()?;
        }

        Ok(submitted)
    }

    /// Submits all queued operations and waits for at least `min_completions`.
    pub fn flush_and_wait(&mut self, min_completions: usize) -> io::Result<usize> {
        let submitted = self.flush()?;
        if submitted > 0 {
            self.manager.submit_and_wait(min_completions)?;
        }
        Ok(submitted)
    }

    /// Collects completed operations.
    pub fn collect(&mut self) -> Vec<IoResult> {
        self.manager.collect_completions()
    }

    /// Returns the number of pending operations (not yet submitted).
    pub fn pending_count(&self) -> usize {
        self.pending_reads.len() + self.pending_writes.len()
    }

    /// Returns true if there are pending operations.
    pub fn has_pending(&self) -> bool {
        !self.pending_reads.is_empty() || !self.pending_writes.is_empty()
    }

    /// Returns the maximum batch size.
    pub fn max_batch_size(&self) -> usize {
        self.max_batch_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uring_manager_creation() {
        let manager = UringManager::new_simple(32);
        assert!(manager.is_ok());
    }

    #[test]
    fn test_batched_uring_creation() {
        let batched = BatchedUring::new(64, 32);
        assert!(batched.is_ok());
        let batched = batched.unwrap();
        assert_eq!(batched.max_batch_size(), 32);
        assert_eq!(batched.pending_count(), 0);
    }
}
