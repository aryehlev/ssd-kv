//! Listeners (DESIGN §7.1, §6.7).
//!
//! Each worker binds its own listener with `SO_REUSEPORT` on every `bind` address and `port`;
//! accepted sockets get `TCP_NODELAY` and `SO_KEEPALIVE` with `tcp-keepalive` seconds; accepts
//! are multishot. The optional `unixsocket` listener lives on worker 0 (M1.3). `maxclients` is
//! enforced at accept: the new connection receives `-ERR max number of clients reached` and is
//! closed.

use std::net::SocketAddr;

/// One bound listening socket.
pub struct Listener {
    pub fd: i32,
    pub addr: SocketAddr,
}

/// Binds this worker's listeners.
pub fn bind_all(bind: &[String], port: u16, backlog: u32) -> std::io::Result<Vec<Listener>> {
    todo!("DESIGN §7.1: listeners")
}
