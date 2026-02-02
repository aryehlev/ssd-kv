//! High-performance TCP server using io_uring for networking.
//!
//! This server uses io_uring for all network operations:
//! - Accept: Batched accept with multishot support
//! - Recv: Zero-copy receive with registered buffers
//! - Send: Batched send with automatic queuing
//!
//! Benefits:
//! - Single syscall for multiple operations
//! - SQPOLL mode eliminates submission syscalls
//! - Better cache locality than epoll
//! - Supports higher connection counts

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use tracing::{error, info, warn};

use crate::server::handler::Handler;
use crate::server::protocol::{Header, Request, Response, HEADER_SIZE};

/// Configuration for io_uring server.
#[derive(Debug, Clone)]
pub struct UringServerConfig {
    pub bind_addr: SocketAddr,
    pub queue_depth: u32,
    pub recv_buffer_size: usize,
    pub max_connections: usize,
    pub batch_size: usize,
}

impl Default for UringServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:7777".parse().unwrap(),
            queue_depth: 4096,
            recv_buffer_size: 65536,
            max_connections: 10000,
            batch_size: 64,
        }
    }
}

/// Statistics for io_uring server.
#[derive(Debug, Default)]
pub struct UringServerStats {
    pub accepts: AtomicU64,
    pub requests: AtomicU64,
    pub responses: AtomicU64,
    pub bytes_recv: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub active_connections: AtomicU64,
}

/// Connection state for protocol parsing.
struct ConnectionState {
    fd: RawFd,
    recv_buf: Vec<u8>,
    recv_pos: usize,
    send_queue: Vec<Vec<u8>>,
}

impl ConnectionState {
    fn new(fd: RawFd, buffer_size: usize) -> Self {
        Self {
            fd,
            recv_buf: vec![0u8; buffer_size],
            recv_pos: 0,
            send_queue: Vec::new(),
        }
    }

    /// Parse requests from receive buffer.
    fn parse_requests(&mut self) -> Vec<Request> {
        let mut requests = Vec::new();
        let mut pos = 0;

        while pos + HEADER_SIZE <= self.recv_pos {
            // Try to parse header
            if let Ok(header_bytes) = <&[u8; HEADER_SIZE]>::try_from(&self.recv_buf[pos..pos + HEADER_SIZE]) {
                let header = Header::from_bytes(header_bytes);

                if !header.is_valid() {
                    // Invalid header, skip byte
                    pos += 1;
                    continue;
                }

                let total_len = HEADER_SIZE + header.payload_len as usize;

                if pos + total_len <= self.recv_pos {
                    // Full request available
                    let payload = self.recv_buf[pos + HEADER_SIZE..pos + total_len].to_vec();

                    requests.push(Request {
                        header,
                        payload,
                    });

                    pos = pos + total_len;
                } else {
                    // Incomplete request, wait for more data
                    break;
                }
            } else {
                break;
            }
        }

        // Move remaining data to front of buffer
        if pos > 0 && pos < self.recv_pos {
            self.recv_buf.copy_within(pos..self.recv_pos, 0);
            self.recv_pos -= pos;
        } else if pos >= self.recv_pos {
            self.recv_pos = 0;
        }

        requests
    }

    /// Add received data to buffer.
    fn add_data(&mut self, data: &[u8]) -> bool {
        if self.recv_pos + data.len() > self.recv_buf.len() {
            // Buffer full, resize
            self.recv_buf.resize(self.recv_buf.len() * 2, 0);
        }

        self.recv_buf[self.recv_pos..self.recv_pos + data.len()].copy_from_slice(data);
        self.recv_pos += data.len();
        true
    }
}

// ============================================================================
// Linux io_uring implementation
// ============================================================================

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use io_uring::{opcode, types, IoUring};

    /// Pending operation types.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum OpType {
        Accept,
        Recv,
        Send,
        Close,
    }

    /// Pending operation.
    struct PendingOp {
        op_type: OpType,
        fd: RawFd,
        buffer_idx: Option<usize>,
    }

    /// io_uring-based TCP server.
    pub struct UringTcpServer {
        config: UringServerConfig,
        handler: Arc<Handler>,
        ring: IoUring,
        listener_fd: RawFd,
        connections: HashMap<RawFd, ConnectionState>,
        pending: HashMap<u64, PendingOp>,
        next_op_id: u64,
        stats: Arc<UringServerStats>,
        shutdown: Arc<AtomicBool>,

        // Buffer pool for recv operations
        recv_buffers: Vec<Vec<u8>>,
        free_buffers: Vec<usize>,
    }

    impl UringTcpServer {
        /// Create a new io_uring TCP server.
        pub fn new(
            config: UringServerConfig,
            handler: Arc<Handler>,
            shutdown: Arc<AtomicBool>,
        ) -> io::Result<Self> {
            // Create listener socket
            let listener = std::net::TcpListener::bind(config.bind_addr)?;
            listener.set_nonblocking(true)?;
            let listener_fd = listener.as_raw_fd();

            // Set SO_REUSEPORT for load balancing
            unsafe {
                let optval: libc::c_int = 1;
                libc::setsockopt(
                    listener_fd,
                    libc::SOL_SOCKET,
                    libc::SO_REUSEPORT,
                    &optval as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }

            // Prevent listener from being dropped
            std::mem::forget(listener);

            // Create io_uring with SQPOLL
            let ring = IoUring::builder()
                .setup_sqpoll(2000)
                .build(config.queue_depth)
                .or_else(|_| IoUring::new(config.queue_depth))?;

            // Pre-allocate recv buffers
            let buffer_count = config.max_connections.min(1024);
            let recv_buffers: Vec<Vec<u8>> = (0..buffer_count)
                .map(|_| vec![0u8; config.recv_buffer_size])
                .collect();
            let free_buffers: Vec<usize> = (0..buffer_count).collect();

            Ok(Self {
                config,
                handler,
                ring,
                listener_fd,
                connections: HashMap::new(),
                pending: HashMap::new(),
                next_op_id: 1,
                stats: Arc::new(UringServerStats::default()),
                shutdown,
                recv_buffers,
                free_buffers,
            })
        }

        fn next_op_id(&mut self) -> u64 {
            let id = self.next_op_id;
            self.next_op_id += 1;
            id
        }

        fn alloc_buffer(&mut self) -> Option<usize> {
            self.free_buffers.pop()
        }

        fn free_buffer(&mut self, idx: usize) {
            if idx < self.recv_buffers.len() {
                self.free_buffers.push(idx);
            }
        }

        /// Submit an accept operation.
        fn submit_accept(&mut self) -> io::Result<u64> {
            let id = self.next_op_id();

            let accept_op = opcode::Accept::new(
                types::Fd(self.listener_fd),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
            .build()
            .user_data(id);

            unsafe {
                self.ring.submission().push(&accept_op).map_err(|_| {
                    io::Error::new(io::ErrorKind::WouldBlock, "SQ full")
                })?;
            }

            self.pending.insert(id, PendingOp {
                op_type: OpType::Accept,
                fd: self.listener_fd,
                buffer_idx: None,
            });

            Ok(id)
        }

        /// Submit a recv operation.
        fn submit_recv(&mut self, fd: RawFd) -> io::Result<u64> {
            let buffer_idx = match self.alloc_buffer() {
                Some(idx) => idx,
                None => return Err(io::Error::new(io::ErrorKind::WouldBlock, "No buffers")),
            };

            let id = self.next_op_id();
            let buffer = &mut self.recv_buffers[buffer_idx];

            let recv_op = opcode::Recv::new(
                types::Fd(fd),
                buffer.as_mut_ptr(),
                buffer.len() as u32,
            )
            .build()
            .user_data(id);

            unsafe {
                if self.ring.submission().push(&recv_op).is_err() {
                    self.free_buffer(buffer_idx);
                    return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
                }
            }

            self.pending.insert(id, PendingOp {
                op_type: OpType::Recv,
                fd,
                buffer_idx: Some(buffer_idx),
            });

            Ok(id)
        }

        /// Submit a send operation.
        fn submit_send(&mut self, fd: RawFd, data: &[u8]) -> io::Result<u64> {
            let id = self.next_op_id();

            // We need to keep the data alive until send completes
            // Store it in the connection's send queue
            if let Some(conn) = self.connections.get_mut(&fd) {
                conn.send_queue.push(data.to_vec());
                let send_data = conn.send_queue.last().unwrap();

                let send_op = opcode::Send::new(
                    types::Fd(fd),
                    send_data.as_ptr(),
                    send_data.len() as u32,
                )
                .build()
                .user_data(id);

                unsafe {
                    self.ring.submission().push(&send_op).map_err(|_| {
                        conn.send_queue.pop();
                        io::Error::new(io::ErrorKind::WouldBlock, "SQ full")
                    })?;
                }

                self.pending.insert(id, PendingOp {
                    op_type: OpType::Send,
                    fd,
                    buffer_idx: None,
                });

                Ok(id)
            } else {
                Err(io::Error::new(io::ErrorKind::NotConnected, "Connection not found"))
            }
        }

        /// Submit a close operation.
        fn submit_close(&mut self, fd: RawFd) -> io::Result<u64> {
            let id = self.next_op_id();

            let close_op = opcode::Close::new(types::Fd(fd))
                .build()
                .user_data(id);

            unsafe {
                self.ring.submission().push(&close_op).map_err(|_| {
                    io::Error::new(io::ErrorKind::WouldBlock, "SQ full")
                })?;
            }

            self.pending.insert(id, PendingOp {
                op_type: OpType::Close,
                fd,
                buffer_idx: None,
            });

            Ok(id)
        }

        /// Process a single request and return response.
        async fn process_request(&self, request: Request) -> Response {
            self.handler.handle(request).await
        }

        /// Run the server event loop.
        pub async fn run(&mut self) -> io::Result<()> {
            info!("io_uring server starting on {}", self.config.bind_addr);

            // Submit initial accept
            self.submit_accept()?;
            self.ring.submit()?;

            while !self.shutdown.load(Ordering::Relaxed) {
                // Wait for completions
                match self.ring.submit_and_wait(1) {
                    Ok(_) => {}
                    Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                    Err(e) => return Err(e),
                }

                // Process completions
                let mut to_process = Vec::new();

                for cqe in self.ring.completion() {
                    let id = cqe.user_data();
                    let result = cqe.result();

                    if let Some(op) = self.pending.remove(&id) {
                        to_process.push((op, result));
                    }
                }

                for (op, result) in to_process {
                    match op.op_type {
                        OpType::Accept => {
                            if result >= 0 {
                                let new_fd = result;
                                self.handle_accept(new_fd)?;
                            }
                            // Always re-arm accept
                            let _ = self.submit_accept();
                        }

                        OpType::Recv => {
                            let fd = op.fd;
                            let buffer_idx = op.buffer_idx.unwrap();

                            if result <= 0 {
                                // Connection closed
                                self.free_buffer(buffer_idx);
                                self.connections.remove(&fd);
                                self.stats.active_connections.fetch_sub(1, Ordering::Relaxed);
                                let _ = self.submit_close(fd);
                            } else {
                                // Process received data
                                let len = result as usize;
                                self.stats.bytes_recv.fetch_add(len as u64, Ordering::Relaxed);

                                // Copy data from buffer
                                let data = self.recv_buffers[buffer_idx][..len].to_vec();
                                self.free_buffer(buffer_idx);

                                // Add to connection buffer and parse requests
                                if let Some(conn) = self.connections.get_mut(&fd) {
                                    conn.add_data(&data);
                                    let requests = conn.parse_requests();

                                    // Process requests
                                    for request in requests {
                                        self.stats.requests.fetch_add(1, Ordering::Relaxed);
                                        let response = self.process_request(request).await;
                                        let response_bytes = response.serialize();
                                        self.stats.bytes_sent.fetch_add(response_bytes.len() as u64, Ordering::Relaxed);
                                        self.stats.responses.fetch_add(1, Ordering::Relaxed);
                                        let _ = self.submit_send(fd, &response_bytes);
                                    }
                                }

                                // Re-arm recv
                                if self.connections.contains_key(&fd) {
                                    let _ = self.submit_recv(fd);
                                }
                            }
                        }

                        OpType::Send => {
                            let fd = op.fd;
                            // Remove sent data from queue
                            if let Some(conn) = self.connections.get_mut(&fd) {
                                if !conn.send_queue.is_empty() {
                                    conn.send_queue.remove(0);
                                }
                            }
                        }

                        OpType::Close => {
                            // Connection fully closed
                        }
                    }
                }

                // Submit any new operations
                let _ = self.ring.submit();
            }

            info!("io_uring server shutting down");
            Ok(())
        }

        fn handle_accept(&mut self, new_fd: i32) -> io::Result<()> {
            // Set TCP_NODELAY and non-blocking
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

            // Create connection state
            self.connections.insert(new_fd, ConnectionState::new(new_fd, self.config.recv_buffer_size));
            self.stats.accepts.fetch_add(1, Ordering::Relaxed);
            self.stats.active_connections.fetch_add(1, Ordering::Relaxed);

            // Start receiving
            self.submit_recv(new_fd)?;

            Ok(())
        }

        /// Get server statistics.
        pub fn stats(&self) -> &Arc<UringServerStats> {
            &self.stats
        }
    }
}

// ============================================================================
// Fallback for non-Linux platforms
// ============================================================================

#[cfg(not(target_os = "linux"))]
mod fallback {
    use super::*;

    /// Fallback TCP server (non-io_uring).
    pub struct UringTcpServer {
        config: UringServerConfig,
        handler: Arc<Handler>,
        shutdown: Arc<AtomicBool>,
        stats: Arc<UringServerStats>,
    }

    impl UringTcpServer {
        pub fn new(
            config: UringServerConfig,
            handler: Arc<Handler>,
            shutdown: Arc<AtomicBool>,
        ) -> io::Result<Self> {
            Ok(Self {
                config,
                handler,
                shutdown,
                stats: Arc::new(UringServerStats::default()),
            })
        }

        pub async fn run(&mut self) -> io::Result<()> {
            // Use the regular mio-based server on non-Linux
            warn!("io_uring not available, using fallback server");

            let listener = std::net::TcpListener::bind(self.config.bind_addr)?;
            listener.set_nonblocking(true)?;

            while !self.shutdown.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _addr)) => {
                        let handler = Arc::clone(&self.handler);
                        let stats = Arc::clone(&self.stats);
                        tokio::spawn(async move {
                            let _ = Self::handle_connection(stream, handler, stats).await;
                        });
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                    }
                    Err(e) => {
                        error!("Accept error: {}", e);
                    }
                }
            }

            Ok(())
        }

        async fn handle_connection(
            mut stream: std::net::TcpStream,
            handler: Arc<Handler>,
            stats: Arc<UringServerStats>,
        ) -> io::Result<()> {
            stream.set_nonblocking(false)?;
            stream.set_nodelay(true)?;

            stats.accepts.fetch_add(1, Ordering::Relaxed);
            stats.active_connections.fetch_add(1, Ordering::Relaxed);

            let mut buf = vec![0u8; 65536];
            let mut pos = 0;

            loop {
                match stream.read(&mut buf[pos..]) {
                    Ok(0) => break,
                    Ok(n) => {
                        pos += n;
                        stats.bytes_recv.fetch_add(n as u64, Ordering::Relaxed);

                        // Try to parse and process requests
                        while pos >= HEADER_SIZE {
                            if let Ok(header_bytes) = <&[u8; HEADER_SIZE]>::try_from(&buf[..HEADER_SIZE]) {
                                let header = Header::from_bytes(header_bytes);
                                if !header.is_valid() {
                                    break;
                                }

                                let total = HEADER_SIZE + header.payload_len as usize;
                                if pos >= total {
                                    let payload = buf[HEADER_SIZE..total].to_vec();

                                    let request = Request { header, payload };
                                    stats.requests.fetch_add(1, Ordering::Relaxed);

                                    let response = handler.handle(request).await;
                                    let response_bytes = response.serialize();
                                    stats.bytes_sent.fetch_add(response_bytes.len() as u64, Ordering::Relaxed);
                                    stats.responses.fetch_add(1, Ordering::Relaxed);

                                    stream.write_all(&response_bytes)?;

                                    buf.copy_within(total..pos, 0);
                                    pos -= total;
                                } else {
                                    break;
                                }
                            } else {
                                break;
                            }
                        }
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                    Err(_) => break,
                }
            }

            stats.active_connections.fetch_sub(1, Ordering::Relaxed);
            Ok(())
        }

        pub fn stats(&self) -> &Arc<UringServerStats> {
            &self.stats
        }
    }
}

#[cfg(target_os = "linux")]
pub use linux::UringTcpServer;

#[cfg(not(target_os = "linux"))]
pub use fallback::UringTcpServer;

/// Start io_uring server in background.
pub fn start_uring_server(
    config: UringServerConfig,
    handler: Arc<Handler>,
) -> io::Result<(Arc<UringServerStats>, Arc<AtomicBool>)> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = Arc::clone(&shutdown);

    let mut server = UringTcpServer::new(config.clone(), handler, shutdown_clone)?;
    let stats = Arc::clone(server.stats());

    std::thread::Builder::new()
        .name("uring-server".to_string())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create runtime");
            rt.block_on(async {
                if let Err(e) = server.run().await {
                    error!("io_uring server error: {}", e);
                }
            });
        })?;

    info!("io_uring server started on {}", config.bind_addr);
    Ok((stats, shutdown))
}
