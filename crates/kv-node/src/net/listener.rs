//! Listeners (DESIGN §7.1, §6.7).
//!
//! Each worker binds its own listener with `SO_REUSEPORT` on every `bind` address and `port`;
//! accepted sockets get `TCP_NODELAY` and `SO_KEEPALIVE` with `tcp-keepalive` seconds; accepts
//! are multishot. The optional `unixsocket` listener lives on worker 0 (M1.3). `maxclients` is
//! enforced at accept: the new connection receives `-ERR max number of clients reached` and is
//! closed.

use std::net::SocketAddr;

use crate::runtime::ring::sys;

/// The reply to a connection refused by `maxclients` (§6.7).
pub const MAX_CLIENTS_REPLY: &[u8] = b"-ERR max number of clients reached\r\n";

/// One bound listening socket.
pub struct Listener {
    pub fd: i32,
    pub addr: SocketAddr,
}

impl Drop for Listener {
    fn drop(&mut self) {
        sys::close(self.fd);
    }
}

/// Parses one `bind` entry (`-` prefix: optional, Redis 7 style).
fn parse_bind(b: &str, port: u16) -> Option<(SocketAddr, bool)> {
    let (optional, b) = match b.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, b),
    };
    let ip: std::net::IpAddr = b.parse().ok()?;
    Some((SocketAddr::new(ip, port), optional))
}

/// Binds this worker's listeners.
pub fn bind_all(bind: &[String], port: u16, backlog: u32) -> std::io::Result<Vec<Listener>> {
    let mut out = Vec::with_capacity(bind.len());
    for b in bind {
        let Some((addr, optional)) = parse_bind(b, port) else {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("bad bind address {b}")));
        };
        match sys::tcp_listener(addr, backlog) {
            Ok(fd) => out.push(Listener { fd, addr: sys::local_addr(fd).unwrap_or(addr) }),
            // an optional address that is not available (e.g. no IPv6) is skipped
            Err(e) if optional && matches!(e.raw_os_error(), Some(libc::EADDRNOTAVAIL) | Some(libc::EAFNOSUPPORT)) => {}
            Err(e) => return Err(std::io::Error::new(e.kind(), format!("cannot listen on {addr}: {e}"))),
        }
    }
    if out.is_empty() {
        return Err(std::io::Error::new(std::io::ErrorKind::AddrNotAvailable, "no bind address could be used"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reuseport_lets_every_worker_bind_the_same_port() {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);
        let a = bind_all(&["127.0.0.1".to_owned()], port, 16).unwrap();
        let b = bind_all(&["127.0.0.1".to_owned(), "-::1".to_owned()], port, 16).unwrap();
        assert_eq!(a[0].addr.port(), port);
        assert_eq!(b[0].addr.port(), port);
        assert!(std::net::TcpStream::connect(("127.0.0.1", port)).is_ok());
        assert!(bind_all(&["not-an-ip".to_owned()], port, 16).is_err());
    }
}
