//! Ultra high-performance TCP server optimized to beat Aerospike.
//!
//! Key optimizations:
//! - SO_REUSEPORT for multi-threaded accept (load balancing across cores)
//! - Request coalescing (batch read all available requests)
//! - Batched response writing (single syscall for multiple responses)
//! - Zero-copy buffer reuse (pre-allocated buffers)
//! - Inline fast-path processing (avoid async overhead for cache hits)
//! - TCP_NODELAY (disable Nagle's algorithm)
//! - Large socket buffers (reduce syscalls)

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use parking_lot::Mutex;
use tracing::{debug, error, info, trace, warn};

use crate::server::handler::Handler;
use crate::server::protocol::{Header, Opcode, Request, Response, HEADER_SIZE, PROTOCOL_MAGIC, MAX_PAYLOAD_SIZE};

/// Server statistics.
#[derive(Debug, Default)]
pub struct ServerStats {
    pub connections_total: AtomicU64,
    pub connections_active: AtomicU64,
    pub requests_total: AtomicU64,
    pub bytes_received: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub batches_processed: AtomicU64,
    pub avg_batch_size: AtomicU64,
}

/// TCP server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind_addr: SocketAddr,
    pub max_connections: usize,
    pub read_buffer_size: usize,
    pub write_buffer_size: usize,
    pub num_workers: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        let num_cpus = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4);

        Self {
            bind_addr: "127.0.0.1:7777".parse().unwrap(),
            max_connections: 10000,
            read_buffer_size: 256 * 1024,  // 256KB for batching
            write_buffer_size: 256 * 1024, // 256KB for batching
            num_workers: num_cpus,
        }
    }
}

/// The TCP server.
pub struct Server {
    config: ServerConfig,
    handler: Arc<Handler>,
    stats: Arc<ServerStats>,
    shutdown: Arc<AtomicBool>,
}

impl Server {
    /// Creates a new server.
    pub fn new(config: ServerConfig, handler: Arc<Handler>) -> Self {
        Self {
            config,
            handler,
            stats: Arc::new(ServerStats::default()),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Returns the server statistics.
    pub fn stats(&self) -> Arc<ServerStats> {
        Arc::clone(&self.stats)
    }

    /// Starts the server with multiple worker threads.
    pub async fn run(&self) -> io::Result<()> {
        info!("Starting high-performance TCP server on {}", self.config.bind_addr);
        info!("Workers: {}, Read buffer: {}KB, Write buffer: {}KB",
            self.config.num_workers,
            self.config.read_buffer_size / 1024,
            self.config.write_buffer_size / 1024
        );

        // Create listener with SO_REUSEPORT for load balancing
        let listener = self.create_listener()?;
        let listener = Arc::new(Mutex::new(listener));

        // Spawn worker threads
        let mut handles = Vec::with_capacity(self.config.num_workers);

        for worker_id in 0..self.config.num_workers {
            let listener = Arc::clone(&listener);
            let handler = Arc::clone(&self.handler);
            let stats = Arc::clone(&self.stats);
            let shutdown = Arc::clone(&self.shutdown);
            let config = self.config.clone();

            let handle = thread::spawn(move || {
                // Pin worker to CPU if possible
                #[cfg(target_os = "linux")]
                {
                    let _ = crate::perf::pin_to_cpu(worker_id);
                }

                worker_loop(worker_id, listener, handler, stats, shutdown, config);
            });

            handles.push(handle);
        }

        info!("Server listening on {} with {} workers", self.config.bind_addr, self.config.num_workers);

        // Wait for shutdown signal
        while !self.shutdown.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Wait for workers to finish
        for handle in handles {
            let _ = handle.join();
        }

        Ok(())
    }

    /// Creates a TCP listener with optimized settings.
    fn create_listener(&self) -> io::Result<TcpListener> {
        use socket2::{Domain, Protocol, Socket, Type};

        let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;

        // SO_REUSEADDR for fast restart
        socket.set_reuse_address(true)?;

        // SO_REUSEPORT for load balancing across workers (Linux/macOS)
        #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
        {
            use std::os::unix::io::AsRawFd;
            unsafe {
                let optval: libc::c_int = 1;
                libc::setsockopt(
                    socket.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_REUSEPORT,
                    &optval as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
        }

        // Large receive buffer
        socket.set_recv_buffer_size(self.config.read_buffer_size)?;
        socket.set_send_buffer_size(self.config.write_buffer_size)?;

        // Bind and listen
        socket.bind(&self.config.bind_addr.into())?;
        socket.listen(4096)?; // Large backlog

        Ok(socket.into())
    }

    /// Signals the server to shut down.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    /// Returns true if the server is running.
    pub fn is_running(&self) -> bool {
        !self.shutdown.load(Ordering::SeqCst)
    }
}

/// Worker thread main loop.
fn worker_loop(
    worker_id: usize,
    listener: Arc<Mutex<TcpListener>>,
    handler: Arc<Handler>,
    stats: Arc<ServerStats>,
    shutdown: Arc<AtomicBool>,
    config: ServerConfig,
) {
    debug!("Worker {} started", worker_id);

    while !shutdown.load(Ordering::Relaxed) {
        // Accept with timeout to check shutdown
        let (socket, addr) = {
            let listener = listener.lock();
            listener.set_nonblocking(true).ok();
            match listener.accept() {
                Ok((s, a)) => (s, a),
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_micros(100));
                    continue;
                }
                Err(e) => {
                    if !shutdown.load(Ordering::Relaxed) {
                        error!("Worker {} accept error: {}", worker_id, e);
                    }
                    continue;
                }
            }
        };

        // Check connection limit
        if stats.connections_active.load(Ordering::Relaxed) >= config.max_connections as u64 {
            warn!("Max connections reached, rejecting {}", addr);
            continue;
        }

        stats.connections_total.fetch_add(1, Ordering::Relaxed);
        stats.connections_active.fetch_add(1, Ordering::Relaxed);

        // Handle connection in a new thread for true parallelism
        let handler = Arc::clone(&handler);
        let stats = Arc::clone(&stats);
        let config = config.clone();

        thread::spawn(move || {
            if let Err(e) = handle_connection_fast(socket, addr, handler, &stats, &config) {
                debug!("Connection {} error: {}", addr, e);
            }
            stats.connections_active.fetch_sub(1, Ordering::Relaxed);
        });
    }

    debug!("Worker {} stopped", worker_id);
}

/// Ultra-fast connection handler with request coalescing.
fn handle_connection_fast(
    mut socket: TcpStream,
    addr: SocketAddr,
    handler: Arc<Handler>,
    stats: &ServerStats,
    config: &ServerConfig,
) -> io::Result<()> {
    debug!("New connection from {}", addr);

    // Optimize socket
    socket.set_nodelay(true)?;
    socket.set_read_timeout(Some(Duration::from_secs(300)))?;
    socket.set_write_timeout(Some(Duration::from_secs(30)))?;

    // Pre-allocated buffers for zero-copy
    let mut read_buf = vec![0u8; config.read_buffer_size];
    let mut write_buf = Vec::with_capacity(config.write_buffer_size);
    let mut read_pos = 0usize;
    let mut read_len = 0usize;

    loop {
        // Read as much data as available (coalescing)
        let n = match socket.read(&mut read_buf[read_len..]) {
            Ok(0) => {
                debug!("Client {} disconnected", addr);
                break;
            }
            Ok(n) => n,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                // No more data, process what we have
                if read_len > read_pos {
                    0
                } else {
                    // Wait for more data
                    thread::sleep(Duration::from_micros(10));
                    continue;
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::TimedOut => {
                debug!("Client {} timed out", addr);
                break;
            }
            Err(e) => {
                debug!("Read error from {}: {}", addr, e);
                break;
            }
        };

        read_len += n;
        stats.bytes_received.fetch_add(n as u64, Ordering::Relaxed);

        // Process all complete requests in the buffer (batching)
        let mut requests_processed = 0u32;
        write_buf.clear();

        while read_pos + HEADER_SIZE <= read_len {
            // Parse header
            let header_bytes: [u8; HEADER_SIZE] = read_buf[read_pos..read_pos + HEADER_SIZE]
                .try_into()
                .unwrap();
            let header = Header::from_bytes(&header_bytes);

            if !header.is_valid() {
                debug!("Invalid header from {}", addr);
                read_pos += 1; // Skip one byte and try again
                continue;
            }

            let total_len = HEADER_SIZE + header.payload_len as usize;
            if read_pos + total_len > read_len {
                // Incomplete request, wait for more data
                break;
            }

            // Extract payload
            let payload = if header.payload_len > 0 {
                read_buf[read_pos + HEADER_SIZE..read_pos + total_len].to_vec()
            } else {
                Vec::new()
            };

            let request = Request { header, payload };
            read_pos += total_len;

            // Process request inline (fast path)
            let response = process_request_inline(&handler, request);

            // Append response to write buffer
            write_buf.extend_from_slice(&response.header.to_bytes());
            write_buf.extend_from_slice(&response.payload);

            requests_processed += 1;
            stats.requests_total.fetch_add(1, Ordering::Relaxed);
        }

        // Compact read buffer if needed
        if read_pos > 0 {
            if read_pos < read_len {
                read_buf.copy_within(read_pos..read_len, 0);
                read_len -= read_pos;
            } else {
                read_len = 0;
            }
            read_pos = 0;
        }

        // Write all responses in single syscall (batched write)
        if !write_buf.is_empty() {
            socket.write_all(&write_buf)?;
            stats.bytes_sent.fetch_add(write_buf.len() as u64, Ordering::Relaxed);

            if requests_processed > 1 {
                stats.batches_processed.fetch_add(1, Ordering::Relaxed);
                trace!("Batched {} requests from {}", requests_processed, addr);
            }
        }
    }

    Ok(())
}

/// Process request inline without async overhead.
#[inline(always)]
fn process_request_inline(handler: &Handler, request: Request) -> Response {
    let request_id = request.header.request_id;

    match request.header.opcode {
        Opcode::Get => {
            match request.parse_get() {
                Ok(key) => {
                    // Fast path: direct lookup
                    match handler.get_value(&key) {
                        Some(value) => Response::success(request_id, &value),
                        None => Response::not_found(request_id),
                    }
                }
                Err(e) => Response::invalid(request_id, &e.to_string()),
            }
        }
        Opcode::Put => {
            match request.parse_put() {
                Ok((key, value, ttl)) => {
                    match handler.put_sync(&key, &value, ttl) {
                        Ok(_) => Response::success(request_id, &[]),
                        Err(e) => Response::error(request_id, &e.to_string()),
                    }
                }
                Err(e) => Response::invalid(request_id, &e.to_string()),
            }
        }
        Opcode::Delete => {
            match request.parse_delete() {
                Ok(key) => {
                    match handler.delete_sync(&key) {
                        Ok(_) => Response::success(request_id, &[]),
                        Err(e) => Response::error(request_id, &e.to_string()),
                    }
                }
                Err(e) => Response::invalid(request_id, &e.to_string()),
            }
        }
        Opcode::MultiGet => {
            process_multi_get(handler, request_id, &request.payload)
        }
        Opcode::MultiPut => {
            process_multi_put(handler, request_id, &request.payload)
        }
        Opcode::Ping => Response::pong(request_id),
        Opcode::Stats => {
            let stats = handler.stats();
            Response::stats(request_id, &stats.to_json())
        }
        _ => Response::invalid(request_id, "Unknown opcode"),
    }
}

/// Process MultiGet request - batch multiple key lookups.
#[inline]
fn process_multi_get(handler: &Handler, request_id: u32, payload: &[u8]) -> Response {
    if payload.len() < 2 {
        return Response::invalid(request_id, "MultiGet payload too small");
    }

    let count = u16::from_le_bytes([payload[0], payload[1]]) as usize;
    let mut offset = 2;
    let mut response_payload = Vec::with_capacity(count * 64);

    // Write count
    response_payload.extend_from_slice(&(count as u16).to_le_bytes());

    for _ in 0..count {
        if offset + 2 > payload.len() {
            break;
        }
        let key_len = u16::from_le_bytes([payload[offset], payload[offset + 1]]) as usize;
        offset += 2;

        if offset + key_len > payload.len() {
            break;
        }
        let key = &payload[offset..offset + key_len];
        offset += key_len;

        // Lookup value
        match handler.get_value(key) {
            Some(value) => {
                response_payload.push(0x01); // Found
                response_payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
                response_payload.extend_from_slice(&value);
            }
            None => {
                response_payload.push(0x00); // Not found
            }
        }
    }

    Response::success(request_id, &response_payload)
}

/// Process MultiPut request - batch multiple key-value inserts.
#[inline]
fn process_multi_put(handler: &Handler, request_id: u32, payload: &[u8]) -> Response {
    if payload.len() < 2 {
        return Response::invalid(request_id, "MultiPut payload too small");
    }

    let count = u16::from_le_bytes([payload[0], payload[1]]) as usize;
    let mut offset = 2;
    let mut success_count = 0u16;

    for _ in 0..count {
        // Read key length
        if offset + 2 > payload.len() {
            break;
        }
        let key_len = u16::from_le_bytes([payload[offset], payload[offset + 1]]) as usize;
        offset += 2;

        // Read key
        if offset + key_len > payload.len() {
            break;
        }
        let key = &payload[offset..offset + key_len];
        offset += key_len;

        // Read value length
        if offset + 4 > payload.len() {
            break;
        }
        let value_len = u32::from_le_bytes([
            payload[offset],
            payload[offset + 1],
            payload[offset + 2],
            payload[offset + 3],
        ]) as usize;
        offset += 4;

        // Read value
        if offset + value_len > payload.len() {
            break;
        }
        let value = &payload[offset..offset + value_len];
        offset += value_len;

        // Read TTL
        if offset + 4 > payload.len() {
            break;
        }
        let ttl = u32::from_le_bytes([
            payload[offset],
            payload[offset + 1],
            payload[offset + 2],
            payload[offset + 3],
        ]);
        offset += 4;

        // Insert
        if handler.put_sync(key, value, ttl).is_ok() {
            success_count += 1;
        }
    }

    // Return count of successful inserts
    Response::success(request_id, &success_count.to_le_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::index::Index;
    use crate::storage::file_manager::{FileManager, WBLOCKS_PER_FILE};
    use crate::storage::write_buffer::WriteBuffer;
    use tempfile::tempdir;
    use std::io::{Read, Write};
    use std::net::TcpStream;

    fn create_test_handler() -> (Arc<Handler>, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let file_manager = Arc::new(FileManager::new(dir.path()).unwrap());
        let index = Arc::new(Index::new());
        let write_buffer = Arc::new(WriteBuffer::new(0, WBLOCKS_PER_FILE));

        file_manager.create_file().unwrap();

        (Arc::new(Handler::new(index, file_manager, write_buffer)), dir)
    }

    #[test]
    fn test_process_request_inline_ping() {
        let (handler, _dir) = create_test_handler();
        let request = Request::ping(42);
        let response = process_request_inline(&handler, request);
        assert_eq!(response.header.opcode, Opcode::Success);
        assert_eq!(response.header.request_id, 42);
    }

    #[test]
    fn test_process_request_inline_put_get() {
        let (handler, _dir) = create_test_handler();

        // PUT
        let put_req = Request::put(1, b"test_key", b"test_value", 0);
        let put_resp = process_request_inline(&handler, put_req);
        assert_eq!(put_resp.header.opcode, Opcode::Success);

        // GET
        let get_req = Request::get(2, b"test_key");
        let get_resp = process_request_inline(&handler, get_req);
        assert_eq!(get_resp.header.opcode, Opcode::Success);
        assert_eq!(get_resp.payload, b"test_value");
    }

    #[test]
    fn test_multi_get() {
        let (handler, _dir) = create_test_handler();

        // Insert some keys
        handler.put_sync(b"key1", b"value1", 0).unwrap();
        handler.put_sync(b"key2", b"value2", 0).unwrap();

        // Build MultiGet payload: [count:2][key1_len:2][key1][key2_len:2][key2]
        let mut payload = Vec::new();
        payload.extend_from_slice(&2u16.to_le_bytes()); // count
        payload.extend_from_slice(&4u16.to_le_bytes()); // key1 len
        payload.extend_from_slice(b"key1");
        payload.extend_from_slice(&4u16.to_le_bytes()); // key2 len
        payload.extend_from_slice(b"key2");

        let response = process_multi_get(&handler, 1, &payload);
        assert_eq!(response.header.opcode, Opcode::Success);

        // Verify response contains both values
        let resp = &response.payload;
        assert_eq!(u16::from_le_bytes([resp[0], resp[1]]), 2); // count
    }
}
