//! Server layer: Redis protocol and request handling.

pub mod handler;
pub mod redis;

pub use handler::{Handler, HandlerStats, OptimizedHandler};
pub use redis::start_redis_server;
