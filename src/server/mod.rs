//! Server layer: protocol, TCP listener, and request handling.

pub mod protocol;
pub mod handler;
pub mod tcp;
pub mod redis;
pub mod pipeline;

pub use protocol::{Header, Opcode, Request, Response, PROTOCOL_MAGIC, PROTOCOL_VERSION};
pub use handler::{Handler, HandlerStats};
pub use tcp::{Server, ServerConfig, ServerStats};
pub use redis::start_redis_server;
