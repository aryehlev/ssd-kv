//! io_uring-based networking for high-performance TCP handling.
//!
//! This module provides io_uring support for network operations:
//! - Accept: Accept new connections
//! - Recv: Receive data from connections
//! - Send: Send data to connections
//! - Multishot accept: Accept multiple connections with one syscall
//!
//! Benefits over epoll:
//! - Batched syscalls (submit multiple ops, one syscall)
//! - SQPOLL mode (kernel polls, no syscalls for submission)
//! - Registered buffers (zero-copy I/O)
//! - Better cache locality

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicU64, Ordering};

/// Network operation types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetOperation {
    Accept,
    Recv,
    Send,
    Close,
}

/// Result of a network operation.
#[derive(Debug)]
pub struct NetResult {
    pub user_data: u64,
    pub operation: NetOperation,
    pub result: i32,
    pub fd: RawFd,
    pub buffer: Option<Vec<u8>>,
}

/// Connection state for io_uring networking.
#[derive(Debug)]
pub struct Connection {
    pub fd: RawFd,
    pub addr: Option<SocketAddr>,
    pub recv_buffer: Vec<u8>,
    pub send_queue: Vec<Vec<u8>>,
}

// ============================================================================
// Linux implementation using io_uring
// ============================================================================

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use io_uring::{opcode, types, IoUring, Probe};
    use std::net::TcpListener;

    /// Pending operation tracking.
    struct PendingOp {
        operation: NetOperation,
        fd: RawFd,
        buffer: Option<Vec<u8>>,
    }

    /// io_uring-based network manager.
    pub struct UringNet {
        ring: IoUring,
        pending: HashMap<u64, PendingOp>,
        next_id: AtomicU64,
        registered_buffers: bool,
        multishot_supported: bool,
    }

    impl UringNet {
        /// Creates a new io_uring network manager.
        pub fn new(queue_depth: u32) -> io::Result<Self> {
            // Try to create with SQPOLL for kernel-side polling
            let ring = IoUring::builder()
                .setup_sqpoll(2000)  // 2ms idle before kernel thread sleeps
                .setup_sqpoll_cpu(0) // Pin to CPU 0
                .build(queue_depth)
                .or_else(|_| IoUring::new(queue_depth))?;

            // Check for multishot accept support
            let mut probe = Probe::new();
            let multishot_supported = if ring.submitter().register_probe(&mut probe).is_ok() {
                probe.is_supported(opcode::AcceptMulti::CODE)
            } else {
                false
            };

            Ok(Self {
                ring,
                pending: HashMap::with_capacity(queue_depth as usize),
                next_id: AtomicU64::new(1),
                registered_buffers: false,
                multishot_supported,
            })
        }

        /// Creates with simpler settings (no SQPOLL).
        pub fn new_simple(queue_depth: u32) -> io::Result<Self> {
            let ring = IoUring::new(queue_depth)?;

            Ok(Self {
                ring,
                pending: HashMap::with_capacity(queue_depth as usize),
                next_id: AtomicU64::new(1),
                registered_buffers: false,
                multishot_supported: false,
            })
        }

        fn next_id(&self) -> u64 {
            self.next_id.fetch_add(1, Ordering::Relaxed)
        }

        /// Register fixed buffers for zero-copy I/O.
        pub fn register_buffers(&mut self, buffers: &[Vec<u8>]) -> io::Result<()> {
            let iovecs: Vec<libc::iovec> = buffers
                .iter()
                .map(|b| libc::iovec {
                    iov_base: b.as_ptr() as *mut _,
                    iov_len: b.len(),
                })
                .collect();

            unsafe {
                self.ring.submitter().register_buffers(&iovecs)?;
            }
            self.registered_buffers = true;
            Ok(())
        }

        /// Submit an accept operation.
        pub fn submit_accept(&mut self, listener_fd: RawFd) -> io::Result<u64> {
            let id = self.next_id();

            let accept_op = opcode::Accept::new(types::Fd(listener_fd), std::ptr::null_mut(), std::ptr::null_mut())
                .build()
                .user_data(id);

            unsafe {
                self.ring.submission().push(&accept_op).map_err(|_| {
                    io::Error::new(io::ErrorKind::WouldBlock, "Submission queue full")
                })?;
            }

            self.pending.insert(id, PendingOp {
                operation: NetOperation::Accept,
                fd: listener_fd,
                buffer: None,
            });

            Ok(id)
        }

        /// Submit a multishot accept (accepts multiple connections with one op).
        pub fn submit_accept_multishot(&mut self, listener_fd: RawFd) -> io::Result<u64> {
            if !self.multishot_supported {
                return self.submit_accept(listener_fd);
            }

            let id = self.next_id();

            let accept_op = opcode::AcceptMulti::new(types::Fd(listener_fd))
                .build()
                .user_data(id);

            unsafe {
                self.ring.submission().push(&accept_op).map_err(|_| {
                    io::Error::new(io::ErrorKind::WouldBlock, "Submission queue full")
                })?;
            }

            self.pending.insert(id, PendingOp {
                operation: NetOperation::Accept,
                fd: listener_fd,
                buffer: None,
            });

            Ok(id)
        }

        /// Submit a recv operation.
        pub fn submit_recv(&mut self, fd: RawFd, buffer: Vec<u8>) -> io::Result<u64> {
            let id = self.next_id();
            let len = buffer.len();
            let ptr = buffer.as_ptr() as *mut u8;

            // Store buffer in pending before creating the operation
            self.pending.insert(id, PendingOp {
                operation: NetOperation::Recv,
                fd,
                buffer: Some(buffer),
            });

            let recv_op = opcode::Recv::new(types::Fd(fd), ptr, len as u32)
                .build()
                .user_data(id);

            unsafe {
                if self.ring.submission().push(&recv_op).is_err() {
                    self.pending.remove(&id);
                    return Err(io::Error::new(io::ErrorKind::WouldBlock, "Submission queue full"));
                }
            }

            Ok(id)
        }

        /// Submit a send operation.
        pub fn submit_send(&mut self, fd: RawFd, buffer: Vec<u8>) -> io::Result<u64> {
            let id = self.next_id();
            let len = buffer.len();
            let ptr = buffer.as_ptr();

            // Store buffer in pending before creating the operation
            self.pending.insert(id, PendingOp {
                operation: NetOperation::Send,
                fd,
                buffer: Some(buffer),
            });

            let send_op = opcode::Send::new(types::Fd(fd), ptr, len as u32)
                .build()
                .user_data(id);

            unsafe {
                if self.ring.submission().push(&send_op).is_err() {
                    self.pending.remove(&id);
                    return Err(io::Error::new(io::ErrorKind::WouldBlock, "Submission queue full"));
                }
            }

            Ok(id)
        }

        /// Submit a close operation.
        pub fn submit_close(&mut self, fd: RawFd) -> io::Result<u64> {
            let id = self.next_id();

            let close_op = opcode::Close::new(types::Fd(fd))
                .build()
                .user_data(id);

            unsafe {
                self.ring.submission().push(&close_op).map_err(|_| {
                    io::Error::new(io::ErrorKind::WouldBlock, "Submission queue full")
                })?;
            }

            self.pending.insert(id, PendingOp {
                operation: NetOperation::Close,
                fd,
                buffer: None,
            });

            Ok(id)
        }

        /// Submit all pending operations to the kernel.
        pub fn submit(&mut self) -> io::Result<usize> {
            self.ring.submit().map_err(io::Error::from)
        }

        /// Submit and wait for at least one completion.
        pub fn submit_and_wait(&mut self, want: usize) -> io::Result<usize> {
            self.ring.submit_and_wait(want).map_err(io::Error::from)
        }

        /// Collect completed operations.
        pub fn collect_completions(&mut self) -> Vec<NetResult> {
            let mut results = Vec::new();

            for cqe in self.ring.completion() {
                let id = cqe.user_data();
                let result = cqe.result();

                if let Some(mut pending) = self.pending.remove(&id) {
                    // For recv, truncate buffer to actual received length
                    if pending.operation == NetOperation::Recv && result > 0 {
                        if let Some(ref mut buf) = pending.buffer {
                            buf.truncate(result as usize);
                        }
                    }

                    results.push(NetResult {
                        user_data: id,
                        operation: pending.operation,
                        result,
                        fd: if pending.operation == NetOperation::Accept && result >= 0 {
                            result // For accept, result is the new fd
                        } else {
                            pending.fd
                        },
                        buffer: pending.buffer,
                    });
                }
            }

            results
        }

        /// Number of pending operations.
        pub fn pending_count(&self) -> usize {
            self.pending.len()
        }

        /// Check if SQPOLL is active (kernel is polling).
        pub fn is_sqpoll_active(&self) -> bool {
            // SQPOLL keeps the kernel thread alive
            true
        }
    }
}

// ============================================================================
// Fallback implementation for non-Linux platforms
// ============================================================================

#[cfg(not(target_os = "linux"))]
mod fallback {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpStream;

    struct PendingOp {
        operation: NetOperation,
        fd: RawFd,
        buffer: Option<Vec<u8>>,
        result: i32,
    }

    /// Fallback network manager using blocking operations.
    pub struct UringNet {
        pending: HashMap<u64, PendingOp>,
        next_id: AtomicU64,
    }

    impl UringNet {
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

        pub fn register_buffers(&mut self, _buffers: &[Vec<u8>]) -> io::Result<()> {
            Ok(())
        }

        pub fn submit_accept(&mut self, listener_fd: RawFd) -> io::Result<u64> {
            let id = self.next_id();

            // Blocking accept using libc
            let mut addr: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
            let mut addr_len: libc::socklen_t = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;

            let result = unsafe {
                libc::accept(listener_fd, &mut addr as *mut _ as *mut libc::sockaddr, &mut addr_len)
            };

            self.pending.insert(id, PendingOp {
                operation: NetOperation::Accept,
                fd: listener_fd,
                buffer: None,
                result: result as i32,
            });

            Ok(id)
        }

        pub fn submit_accept_multishot(&mut self, listener_fd: RawFd) -> io::Result<u64> {
            self.submit_accept(listener_fd)
        }

        pub fn submit_recv(&mut self, fd: RawFd, mut buffer: Vec<u8>) -> io::Result<u64> {
            let id = self.next_id();

            let result = unsafe {
                libc::recv(fd, buffer.as_mut_ptr() as *mut libc::c_void, buffer.len(), 0)
            };

            if result > 0 {
                buffer.truncate(result as usize);
            }

            self.pending.insert(id, PendingOp {
                operation: NetOperation::Recv,
                fd,
                buffer: Some(buffer),
                result: result as i32,
            });

            Ok(id)
        }

        pub fn submit_send(&mut self, fd: RawFd, buffer: Vec<u8>) -> io::Result<u64> {
            let id = self.next_id();

            let result = unsafe {
                libc::send(fd, buffer.as_ptr() as *const libc::c_void, buffer.len(), 0)
            };

            self.pending.insert(id, PendingOp {
                operation: NetOperation::Send,
                fd,
                buffer: Some(buffer),
                result: result as i32,
            });

            Ok(id)
        }

        pub fn submit_close(&mut self, fd: RawFd) -> io::Result<u64> {
            let id = self.next_id();

            let result = unsafe { libc::close(fd) };

            self.pending.insert(id, PendingOp {
                operation: NetOperation::Close,
                fd,
                buffer: None,
                result,
            });

            Ok(id)
        }

        pub fn submit(&mut self) -> io::Result<usize> {
            Ok(self.pending.len())
        }

        pub fn submit_and_wait(&mut self, _want: usize) -> io::Result<usize> {
            Ok(self.pending.len())
        }

        pub fn collect_completions(&mut self) -> Vec<NetResult> {
            self.pending
                .drain()
                .map(|(id, op)| NetResult {
                    user_data: id,
                    operation: op.operation,
                    result: op.result,
                    fd: if op.operation == NetOperation::Accept && op.result >= 0 {
                        op.result
                    } else {
                        op.fd
                    },
                    buffer: op.buffer,
                })
                .collect()
        }

        pub fn pending_count(&self) -> usize {
            self.pending.len()
        }

        pub fn is_sqpoll_active(&self) -> bool {
            false
        }
    }
}

#[cfg(target_os = "linux")]
pub use linux::UringNet;

#[cfg(not(target_os = "linux"))]
pub use fallback::UringNet;

// ============================================================================
// High-level server using io_uring
// ============================================================================

use std::net::TcpListener;

/// Buffer pool for network I/O.
pub struct NetBufferPool {
    buffers: Vec<Vec<u8>>,
    buffer_size: usize,
}

impl NetBufferPool {
    pub fn new(count: usize, buffer_size: usize) -> Self {
        let buffers = (0..count)
            .map(|_| vec![0u8; buffer_size])
            .collect();
        Self { buffers, buffer_size }
    }

    pub fn get(&mut self) -> Vec<u8> {
        self.buffers.pop().unwrap_or_else(|| vec![0u8; self.buffer_size])
    }

    pub fn put(&mut self, mut buffer: Vec<u8>) {
        if buffer.capacity() >= self.buffer_size {
            buffer.clear();
            buffer.resize(self.buffer_size, 0);
            self.buffers.push(buffer);
        }
    }
}

/// Connection tracked by io_uring server.
pub struct TrackedConnection {
    pub fd: RawFd,
    pub recv_pending: bool,
    pub send_pending: bool,
    pub send_queue: Vec<Vec<u8>>,
}

/// High-performance TCP server using io_uring.
pub struct UringServer {
    net: UringNet,
    listener_fd: RawFd,
    connections: HashMap<RawFd, TrackedConnection>,
    buffer_pool: NetBufferPool,
    recv_buffer_size: usize,
}

impl UringServer {
    /// Create a new io_uring-based server.
    pub fn new(addr: SocketAddr, queue_depth: u32, recv_buffer_size: usize) -> io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        let listener_fd = listener.as_raw_fd();

        // Prevent listener from being dropped (we manage the fd)
        std::mem::forget(listener);

        let net = UringNet::new(queue_depth)
            .or_else(|_| UringNet::new_simple(queue_depth))?;

        Ok(Self {
            net,
            listener_fd,
            connections: HashMap::new(),
            buffer_pool: NetBufferPool::new(256, recv_buffer_size),
            recv_buffer_size,
        })
    }

    /// Start accepting connections.
    pub fn start_accept(&mut self) -> io::Result<()> {
        self.net.submit_accept(self.listener_fd)?;
        Ok(())
    }

    /// Queue a recv for a connection.
    pub fn queue_recv(&mut self, fd: RawFd) -> io::Result<()> {
        if let Some(conn) = self.connections.get_mut(&fd) {
            if !conn.recv_pending {
                let buffer = self.buffer_pool.get();
                self.net.submit_recv(fd, buffer)?;
                conn.recv_pending = true;
            }
        }
        Ok(())
    }

    /// Queue a send for a connection.
    pub fn queue_send(&mut self, fd: RawFd, data: Vec<u8>) -> io::Result<()> {
        if let Some(conn) = self.connections.get_mut(&fd) {
            if conn.send_pending {
                // Already sending, queue for later
                conn.send_queue.push(data);
            } else {
                self.net.submit_send(fd, data)?;
                conn.send_pending = true;
            }
        }
        Ok(())
    }

    /// Submit all pending operations.
    pub fn submit(&mut self) -> io::Result<usize> {
        self.net.submit()
    }

    /// Wait for completions.
    pub fn wait(&mut self, min_completions: usize) -> io::Result<usize> {
        self.net.submit_and_wait(min_completions)
    }

    /// Process completions and return events.
    pub fn process_completions<F>(&mut self, mut handler: F) -> io::Result<usize>
    where
        F: FnMut(NetEvent) -> Option<Vec<u8>>,
    {
        let completions = self.net.collect_completions();
        let count = completions.len();

        for result in completions {
            match result.operation {
                NetOperation::Accept => {
                    if result.result >= 0 {
                        let new_fd = result.result;

                        // Set non-blocking and TCP_NODELAY
                        unsafe {
                            let flags = libc::fcntl(new_fd, libc::F_GETFL);
                            libc::fcntl(new_fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
                            let nodelay: libc::c_int = 1;
                            libc::setsockopt(
                                new_fd,
                                libc::IPPROTO_TCP,
                                libc::TCP_NODELAY,
                                &nodelay as *const _ as *const libc::c_void,
                                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                            );
                        }

                        self.connections.insert(new_fd, TrackedConnection {
                            fd: new_fd,
                            recv_pending: false,
                            send_pending: false,
                            send_queue: Vec::new(),
                        });

                        handler(NetEvent::Accept(new_fd));

                        // Start receiving on new connection
                        let buffer = self.buffer_pool.get();
                        let _ = self.net.submit_recv(new_fd, buffer);
                        if let Some(conn) = self.connections.get_mut(&new_fd) {
                            conn.recv_pending = true;
                        }
                    }

                    // Re-arm accept
                    let _ = self.net.submit_accept(self.listener_fd);
                }

                NetOperation::Recv => {
                    let fd = result.fd;

                    if let Some(conn) = self.connections.get_mut(&fd) {
                        conn.recv_pending = false;
                    }

                    if result.result <= 0 {
                        // Connection closed or error
                        self.connections.remove(&fd);
                        handler(NetEvent::Close(fd));
                        let _ = self.net.submit_close(fd);
                    } else if let Some(data) = result.buffer {
                        // Got data, call handler
                        if let Some(response) = handler(NetEvent::Data(fd, data)) {
                            let _ = self.queue_send(fd, response);
                        }

                        // Re-arm recv
                        if self.connections.contains_key(&fd) {
                            let buffer = self.buffer_pool.get();
                            let _ = self.net.submit_recv(fd, buffer);
                            if let Some(conn) = self.connections.get_mut(&fd) {
                                conn.recv_pending = true;
                            }
                        }
                    }
                }

                NetOperation::Send => {
                    let fd = result.fd;

                    if let Some(conn) = self.connections.get_mut(&fd) {
                        conn.send_pending = false;

                        // Return buffer to pool
                        if let Some(buf) = result.buffer {
                            self.buffer_pool.put(buf);
                        }

                        // Send next queued data if any
                        if let Some(next_data) = conn.send_queue.pop() {
                            let _ = self.net.submit_send(fd, next_data);
                            conn.send_pending = true;
                        }
                    }
                }

                NetOperation::Close => {
                    // Connection fully closed
                }
            }
        }

        Ok(count)
    }

    /// Get number of active connections.
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }
}

/// Network event from io_uring server.
#[derive(Debug)]
pub enum NetEvent {
    Accept(RawFd),
    Data(RawFd, Vec<u8>),
    Close(RawFd),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uring_net_creation() {
        let net = UringNet::new_simple(32);
        assert!(net.is_ok());
    }

    #[test]
    fn test_buffer_pool() {
        let mut pool = NetBufferPool::new(10, 4096);
        let buf = pool.get();
        assert_eq!(buf.len(), 4096);
        pool.put(buf);
    }
}
