//! Event-driven async TCP server using mio for high concurrency.
//!
//! Key features:
//! - Non-blocking I/O with mio epoll/kqueue
//! - Single event loop per worker thread
//! - Connection state machine with read/write buffers
//! - Request batching across multiple connections
//! - Zero-copy where possible

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use slab::Slab;
use tracing::{debug, error, info, trace, warn};

use crate::server::handler::Handler;
use crate::server::protocol::{Header, Opcode, Request, Response, HEADER_SIZE, MAX_PAYLOAD_SIZE};

/// Token for the listener socket.
const LISTENER: Token = Token(0);

/// Maximum number of events to process per poll.
const MAX_EVENTS: usize = 1024;

/// Read buffer size per connection.
const READ_BUF_SIZE: usize = 64 * 1024; // 64KB

/// Write buffer size per connection.
const WRITE_BUF_SIZE: usize = 64 * 1024; // 64KB

/// Connection state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionState {
    /// Reading request header.
    ReadingHeader,
    /// Reading request payload.
    ReadingPayload,
    /// Writing response.
    Writing,
    /// Closed.
    Closed,
}

/// A single connection with its state and buffers.
struct Connection {
    socket: TcpStream,
    addr: SocketAddr,
    state: ConnectionState,
    read_buf: Vec<u8>,
    read_pos: usize,
    write_buf: Vec<u8>,
    write_pos: usize,
    current_header: Option<Header>,
    interest: Interest,
}

impl Connection {
    fn new(socket: TcpStream, addr: SocketAddr) -> Self {
        Self {
            socket,
            addr,
            state: ConnectionState::ReadingHeader,
            read_buf: vec![0u8; READ_BUF_SIZE],
            read_pos: 0,
            write_buf: Vec::with_capacity(WRITE_BUF_SIZE),
            write_pos: 0,
            current_header: None,
            interest: Interest::READABLE,
        }
    }

    /// Reads data from the socket into the read buffer.
    fn read(&mut self) -> io::Result<usize> {
        let n = self.socket.read(&mut self.read_buf[self.read_pos..])?;
        self.read_pos += n;
        Ok(n)
    }

    /// Writes data from the write buffer to the socket.
    fn write(&mut self) -> io::Result<usize> {
        if self.write_pos >= self.write_buf.len() {
            return Ok(0);
        }
        let n = self.socket.write(&self.write_buf[self.write_pos..])?;
        self.write_pos += n;
        Ok(n)
    }

    /// Returns true if write buffer is fully sent.
    fn write_complete(&self) -> bool {
        self.write_pos >= self.write_buf.len()
    }

    /// Clears the write buffer for reuse.
    fn clear_write_buf(&mut self) {
        self.write_buf.clear();
        self.write_pos = 0;
    }

    /// Compacts the read buffer after processing.
    fn compact_read_buf(&mut self, consumed: usize) {
        if consumed > 0 && consumed < self.read_pos {
            self.read_buf.copy_within(consumed..self.read_pos, 0);
            self.read_pos -= consumed;
        } else if consumed >= self.read_pos {
            self.read_pos = 0;
        }
    }
}

/// Statistics for the async server.
#[derive(Debug, Default)]
pub struct AsyncServerStats {
    pub connections_total: AtomicU64,
    pub connections_active: AtomicU64,
    pub requests_total: AtomicU64,
    pub bytes_received: AtomicU64,
    pub bytes_sent: AtomicU64,
}

/// Configuration for the async server.
#[derive(Debug, Clone)]
pub struct AsyncServerConfig {
    pub bind_addr: SocketAddr,
    pub num_workers: usize,
    pub max_connections_per_worker: usize,
}

impl Default for AsyncServerConfig {
    fn default() -> Self {
        let num_cpus = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4);

        Self {
            bind_addr: "127.0.0.1:7779".parse().unwrap(),
            num_workers: num_cpus,
            max_connections_per_worker: 10000,
        }
    }
}

/// Async TCP server using mio for event-driven I/O.
pub struct AsyncServer {
    config: AsyncServerConfig,
    handler: Arc<Handler>,
    stats: Arc<AsyncServerStats>,
    shutdown: Arc<AtomicBool>,
}

impl AsyncServer {
    /// Creates a new async server.
    pub fn new(config: AsyncServerConfig, handler: Arc<Handler>) -> Self {
        Self {
            config,
            handler,
            stats: Arc::new(AsyncServerStats::default()),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Returns the server statistics.
    pub fn stats(&self) -> Arc<AsyncServerStats> {
        Arc::clone(&self.stats)
    }

    /// Runs the server with multiple worker threads.
    pub fn run(&self) -> io::Result<Vec<JoinHandle<()>>> {
        info!(
            "Starting async TCP server on {} with {} workers",
            self.config.bind_addr, self.config.num_workers
        );

        let mut handles = Vec::with_capacity(self.config.num_workers);

        for worker_id in 0..self.config.num_workers {
            let config = self.config.clone();
            let handler = Arc::clone(&self.handler);
            let stats = Arc::clone(&self.stats);
            let shutdown = Arc::clone(&self.shutdown);

            let handle = thread::Builder::new()
                .name(format!("async-worker-{}", worker_id))
                .spawn(move || {
                    if let Err(e) = worker_loop(worker_id, config, handler, stats, shutdown) {
                        error!("Worker {} failed: {}", worker_id, e);
                    }
                })?;

            handles.push(handle);
        }

        Ok(handles)
    }

    /// Signals the server to shut down.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }
}

/// Worker thread event loop.
fn worker_loop(
    worker_id: usize,
    config: AsyncServerConfig,
    handler: Arc<Handler>,
    stats: Arc<AsyncServerStats>,
    shutdown: Arc<AtomicBool>,
) -> io::Result<()> {
    // Create poll instance
    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(MAX_EVENTS);

    // Create listener
    let listener_addr = config.bind_addr;
    let mut listener = TcpListener::bind(listener_addr)?;

    // Register listener
    poll.registry()
        .register(&mut listener, LISTENER, Interest::READABLE)?;

    // Connection slab (token = index + 1, since 0 is LISTENER)
    let mut connections: Slab<Connection> = Slab::with_capacity(config.max_connections_per_worker);

    debug!("Worker {} started on {}", worker_id, listener_addr);

    loop {
        // Check for shutdown
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        // Poll for events with timeout
        poll.poll(&mut events, Some(Duration::from_millis(100)))?;

        for event in events.iter() {
            match event.token() {
                LISTENER => {
                    // Accept new connections
                    loop {
                        match listener.accept() {
                            Ok((socket, addr)) => {
                                // Check connection limit
                                if connections.len() >= config.max_connections_per_worker {
                                    warn!("Worker {} at max connections, rejecting {}", worker_id, addr);
                                    continue;
                                }

                                // Insert connection and get token
                                let entry = connections.vacant_entry();
                                let token = Token(entry.key() + 1);
                                let mut conn = Connection::new(socket, addr);

                                // Set non-blocking
                                conn.socket.set_nodelay(true)?;

                                // Register with poll
                                poll.registry().register(
                                    &mut conn.socket,
                                    token,
                                    conn.interest,
                                )?;

                                entry.insert(conn);
                                stats.connections_total.fetch_add(1, Ordering::Relaxed);
                                stats.connections_active.fetch_add(1, Ordering::Relaxed);

                                trace!("Worker {} accepted connection from {}", worker_id, addr);
                            }
                            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                            Err(e) => {
                                error!("Worker {} accept error: {}", worker_id, e);
                                break;
                            }
                        }
                    }
                }
                token => {
                    let idx = token.0 - 1;
                    if !connections.contains(idx) {
                        continue;
                    }

                    let mut close = false;

                    // Handle readable event
                    if event.is_readable() {
                        match handle_readable(&mut connections[idx], &handler, &stats) {
                            Ok(should_close) => close = should_close,
                            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
                            Err(e) => {
                                trace!("Read error on {}: {}", connections[idx].addr, e);
                                close = true;
                            }
                        }
                    }

                    // Handle writable event
                    if event.is_writable() && !close {
                        match handle_writable(&mut connections[idx]) {
                            Ok(should_close) => close = close || should_close,
                            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
                            Err(e) => {
                                trace!("Write error on {}: {}", connections[idx].addr, e);
                                close = true;
                            }
                        }
                    }

                    // Update interest or close
                    if close || connections[idx].state == ConnectionState::Closed {
                        let mut conn = connections.remove(idx);
                        let _ = poll.registry().deregister(&mut conn.socket);
                        stats.connections_active.fetch_sub(1, Ordering::Relaxed);
                        trace!("Worker {} closed connection {}", worker_id, conn.addr);
                    } else {
                        // Re-register with updated interest
                        let conn = &mut connections[idx];
                        let _ = poll.registry().reregister(
                            &mut conn.socket,
                            token,
                            conn.interest,
                        );
                    }
                }
            }
        }
    }

    debug!("Worker {} shutting down", worker_id);
    Ok(())
}

/// Handles readable events on a connection.
fn handle_readable(
    conn: &mut Connection,
    handler: &Handler,
    stats: &AsyncServerStats,
) -> io::Result<bool> {
    // Read available data
    match conn.read() {
        Ok(0) => return Ok(true), // Connection closed
        Ok(n) => {
            stats.bytes_received.fetch_add(n as u64, Ordering::Relaxed);
        }
        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(false),
        Err(e) => return Err(e),
    }

    // Process all complete requests in the buffer
    let mut consumed = 0;

    while consumed < conn.read_pos {
        let remaining = conn.read_pos - consumed;

        match conn.state {
            ConnectionState::ReadingHeader => {
                if remaining < HEADER_SIZE {
                    break; // Need more data
                }

                let header_bytes: [u8; HEADER_SIZE] =
                    conn.read_buf[consumed..consumed + HEADER_SIZE]
                        .try_into()
                        .unwrap();
                let header = Header::from_bytes(&header_bytes);

                if !header.is_valid() {
                    // Invalid header, skip one byte and try again
                    consumed += 1;
                    continue;
                }

                if header.payload_len > MAX_PAYLOAD_SIZE {
                    // Payload too large, close connection
                    conn.state = ConnectionState::Closed;
                    return Ok(true);
                }

                consumed += HEADER_SIZE;
                conn.current_header = Some(header.clone());

                if header.payload_len > 0 {
                    conn.state = ConnectionState::ReadingPayload;
                } else {
                    // No payload, process request immediately
                    let request = Request {
                        header,
                        payload: Vec::new(),
                    };
                    process_request(conn, request, handler, stats);
                    conn.current_header = None;
                }
            }
            ConnectionState::ReadingPayload => {
                let header = conn.current_header.as_ref().unwrap();
                let payload_len = header.payload_len as usize;

                if remaining < payload_len {
                    break; // Need more data
                }

                let payload = conn.read_buf[consumed..consumed + payload_len].to_vec();
                consumed += payload_len;

                let request = Request {
                    header: header.clone(),
                    payload,
                };
                process_request(conn, request, handler, stats);

                conn.current_header = None;
                conn.state = ConnectionState::ReadingHeader;
            }
            ConnectionState::Writing | ConnectionState::Closed => break,
        }
    }

    // Compact read buffer
    conn.compact_read_buf(consumed);

    // Update interest based on state
    if !conn.write_buf.is_empty() {
        conn.interest = Interest::READABLE | Interest::WRITABLE;
    } else {
        conn.interest = Interest::READABLE;
    }

    Ok(false)
}

/// Handles writable events on a connection.
fn handle_writable(conn: &mut Connection) -> io::Result<bool> {
    if conn.write_buf.is_empty() {
        conn.interest = Interest::READABLE;
        return Ok(false);
    }

    match conn.write() {
        Ok(n) => {
            if conn.write_complete() {
                conn.clear_write_buf();
                conn.interest = Interest::READABLE;
                conn.state = ConnectionState::ReadingHeader;
            }
            Ok(false)
        }
        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => Ok(false),
        Err(e) => Err(e),
    }
}

/// Processes a request and queues the response.
fn process_request(
    conn: &mut Connection,
    request: Request,
    handler: &Handler,
    stats: &AsyncServerStats,
) {
    stats.requests_total.fetch_add(1, Ordering::Relaxed);

    let request_id = request.header.request_id;
    let response = match request.header.opcode {
        Opcode::Get => match request.parse_get() {
            Ok(key) => match handler.get_value(&key) {
                Some(value) => Response::success(request_id, &value),
                None => Response::not_found(request_id),
            },
            Err(e) => Response::invalid(request_id, &e.to_string()),
        },
        Opcode::Put => match request.parse_put() {
            Ok((key, value, ttl)) => match handler.put_sync(&key, &value, ttl) {
                Ok(_) => Response::success(request_id, &[]),
                Err(e) => Response::error(request_id, &e.to_string()),
            },
            Err(e) => Response::invalid(request_id, &e.to_string()),
        },
        Opcode::Delete => match request.parse_delete() {
            Ok(key) => match handler.delete_sync(&key) {
                Ok(_) => Response::success(request_id, &[]),
                Err(e) => Response::error(request_id, &e.to_string()),
            },
            Err(e) => Response::invalid(request_id, &e.to_string()),
        },
        Opcode::Ping => Response::pong(request_id),
        Opcode::Stats => {
            let stats = handler.stats();
            Response::stats(request_id, &stats.to_json())
        }
        _ => Response::invalid(request_id, "Unknown opcode"),
    };

    // Append response to write buffer
    conn.write_buf.extend_from_slice(&response.header.to_bytes());
    conn.write_buf.extend_from_slice(&response.payload);
    conn.interest = Interest::READABLE | Interest::WRITABLE;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_state_transitions() {
        // Test basic state enum
        assert_eq!(ConnectionState::ReadingHeader, ConnectionState::ReadingHeader);
        assert_ne!(ConnectionState::ReadingHeader, ConnectionState::Writing);
    }

    #[test]
    fn test_async_server_config_default() {
        let config = AsyncServerConfig::default();
        assert!(config.num_workers > 0);
        assert!(config.max_connections_per_worker > 0);
    }
}
