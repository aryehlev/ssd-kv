//! TCP server using tokio.

use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tracing::{debug, error, info, trace, warn};

use crate::server::handler::Handler;
use crate::server::protocol::{Request, Response};

/// Server statistics.
#[derive(Debug, Default)]
pub struct ServerStats {
    pub connections_total: AtomicU64,
    pub connections_active: AtomicU64,
    pub requests_total: AtomicU64,
    pub bytes_received: AtomicU64,
    pub bytes_sent: AtomicU64,
}

/// TCP server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind_addr: SocketAddr,
    pub max_connections: usize,
    pub read_buffer_size: usize,
    pub write_buffer_size: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:7777".parse().unwrap(),
            max_connections: 10000,
            read_buffer_size: 64 * 1024,
            write_buffer_size: 64 * 1024,
        }
    }
}

/// The TCP server.
pub struct Server {
    config: ServerConfig,
    handler: Arc<Handler>,
    stats: Arc<ServerStats>,
    shutdown: broadcast::Sender<()>,
    running: AtomicBool,
}

impl Server {
    /// Creates a new server.
    pub fn new(config: ServerConfig, handler: Arc<Handler>) -> Self {
        let (shutdown, _) = broadcast::channel(1);

        Self {
            config,
            handler,
            stats: Arc::new(ServerStats::default()),
            shutdown,
            running: AtomicBool::new(false),
        }
    }

    /// Returns the server statistics.
    pub fn stats(&self) -> Arc<ServerStats> {
        Arc::clone(&self.stats)
    }

    /// Starts the server.
    pub async fn run(&self) -> io::Result<()> {
        let listener = TcpListener::bind(self.config.bind_addr).await?;
        info!("Server listening on {}", self.config.bind_addr);

        self.running.store(true, Ordering::SeqCst);

        let mut shutdown_rx = self.shutdown.subscribe();

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((socket, addr)) => {
                            if self.stats.connections_active.load(Ordering::Relaxed)
                                >= self.config.max_connections as u64
                            {
                                warn!("Max connections reached, rejecting {}", addr);
                                continue;
                            }

                            self.stats.connections_total.fetch_add(1, Ordering::Relaxed);
                            self.stats.connections_active.fetch_add(1, Ordering::Relaxed);

                            let handler = Arc::clone(&self.handler);
                            let stats = Arc::clone(&self.stats);
                            let config = self.config.clone();

                            tokio::spawn(async move {
                                if let Err(e) = handle_connection(socket, addr, handler, stats.clone(), config).await {
                                    debug!("Connection {} error: {}", addr, e);
                                }
                                stats.connections_active.fetch_sub(1, Ordering::Relaxed);
                            });
                        }
                        Err(e) => {
                            error!("Accept error: {}", e);
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Server shutting down");
                    break;
                }
            }
        }

        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }

    /// Signals the server to shut down.
    pub fn shutdown(&self) {
        let _ = self.shutdown.send(());
    }

    /// Returns true if the server is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

/// Handles a single client connection.
async fn handle_connection(
    socket: TcpStream,
    addr: SocketAddr,
    handler: Arc<Handler>,
    stats: Arc<ServerStats>,
    config: ServerConfig,
) -> io::Result<()> {
    debug!("New connection from {}", addr);

    socket.set_nodelay(true)?;

    let (reader, writer) = socket.into_split();
    let mut reader = BufReader::with_capacity(config.read_buffer_size, reader);
    let mut writer = BufWriter::with_capacity(config.write_buffer_size, writer);

    loop {
        // Read request
        let request = match Request::read_async(&mut reader).await {
            Ok(req) => {
                stats.bytes_received.fetch_add(
                    (16 + req.payload.len()) as u64,
                    Ordering::Relaxed,
                );
                req
            }
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                debug!("Client {} disconnected", addr);
                break;
            }
            Err(e) => {
                debug!("Read error from {}: {}", addr, e);
                break;
            }
        };

        stats.requests_total.fetch_add(1, Ordering::Relaxed);
        trace!("Request from {}: {:?}", addr, request.header.opcode);

        // Handle request
        let response = handler.handle(request).await;

        // Write response
        let response_size = 16 + response.payload.len();
        if let Err(e) = response.write_async(&mut writer).await {
            debug!("Write error to {}: {}", addr, e);
            break;
        }

        if let Err(e) = writer.flush().await {
            debug!("Flush error to {}: {}", addr, e);
            break;
        }

        stats.bytes_sent.fetch_add(response_size as u64, Ordering::Relaxed);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::index::Index;
    use crate::storage::file_manager::{FileManager, WBLOCKS_PER_FILE};
    use crate::storage::write_buffer::WriteBuffer;
    use tempfile::tempdir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    async fn create_test_server() -> (Server, SocketAddr) {
        let dir = tempdir().unwrap();
        let file_manager = Arc::new(FileManager::new(dir.path()).unwrap());
        let index = Arc::new(Index::new());
        let write_buffer = Arc::new(WriteBuffer::new(0, WBLOCKS_PER_FILE));

        file_manager.create_file().unwrap();

        let handler = Arc::new(Handler::new(index, file_manager, write_buffer));

        // Use port 0 to get a random available port
        let config = ServerConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..Default::default()
        };

        let server = Server::new(config, handler);
        let addr = server.config.bind_addr;
        (server, addr)
    }

    #[tokio::test]
    async fn test_server_ping() {
        let dir = tempdir().unwrap();
        let file_manager = Arc::new(FileManager::new(dir.path()).unwrap());
        let index = Arc::new(Index::new());
        let write_buffer = Arc::new(WriteBuffer::new(0, WBLOCKS_PER_FILE));
        file_manager.create_file().unwrap();
        let handler = Arc::new(Handler::new(index, file_manager, write_buffer));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handler_clone = Arc::clone(&handler);
        let server_task = tokio::spawn(async move {
            let (socket, client_addr) = listener.accept().await.unwrap();
            let stats = Arc::new(ServerStats::default());
            let config = ServerConfig::default();
            handle_connection(socket, client_addr, handler_clone, stats, config)
                .await
                .unwrap();
        });

        // Connect and send ping
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let req = Request::ping(42);
        req.write_async(&mut stream).await.unwrap();

        // Read response
        let resp = Response::read_async(&mut stream).await.unwrap();
        assert_eq!(resp.header.request_id, 42);

        drop(stream);
        let _ = server_task.await;
    }
}
