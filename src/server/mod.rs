//! Server layer: protocol, TCP listener, and request handling.

pub mod async_tcp;
pub mod handler;
pub mod pipeline;
pub mod protocol;
pub mod redis;
pub mod tcp;
pub mod uring_server;

pub use async_tcp::{AsyncServer, AsyncServerConfig, AsyncServerStats};
pub use handler::{Handler, HandlerStats, OptimizedHandler};
pub use protocol::{Header, Opcode, Request, Response, PROTOCOL_MAGIC, PROTOCOL_VERSION};
pub use redis::start_redis_server;
pub use tcp::{Server, ServerConfig, ServerStats};
pub use uring_server::{UringServerConfig, UringServerStats, UringTcpServer, start_uring_server};
