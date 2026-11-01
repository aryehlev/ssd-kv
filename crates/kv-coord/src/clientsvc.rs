//! Client topology service on `client-port` (DESIGN §23.5).
//!
//! CLUSTER SLOTS/SHARDS/NODES/INFO rendered from the committed state (cached per version);
//! CLUSTER MYID / MYSHARDID / KEYSLOT; protocol-compatibility replies (HELLO with `mode:
//! cluster`, PING, ECHO, AUTH, SELECT 0, QUIT, RESET, READONLY, READWRITE, ASKING, COMMAND*,
//! CLIENT*, INFO with `server_kind:coordinator`); keyed commands answered with MOVED to the
//! slot's primary, CLUSTERDOWN or CROSSSLOT (bulk arguments are read and discarded); MULTI is
//! accepted and EXEC aborts. Followers serve topology from their committed state while they
//! heard from a leader within 2 s, else `-CLUSTERDOWN The cluster is down`.
//!
//! Keyless commands that clients send to "all masters" are not in this service's table with
//! coordinator line style R1. With style R2 they get the constant-literal replies below: no
//! keyspace abstraction and no stored state exist here (LC-38).

use std::sync::Arc;

use crate::wsr::Wsr;

/// R2 stub: DBSIZE
pub const STUB_DBSIZE: &[u8] = b":0\r\n";
/// R2 stub: KEYS
pub const STUB_KEYS: &[u8] = b"*0\r\n";
/// R2 stub: SCAN
pub const STUB_SCAN: &[u8] = b"*2\r\n$1\r\n0\r\n*0\r\n";
/// R2 stub: RANDOMKEY (RESP2)
pub const STUB_RANDOMKEY_RESP2: &[u8] = b"$-1\r\n";
/// R2 stub: RANDOMKEY (RESP3)
pub const STUB_RANDOMKEY_RESP3: &[u8] = b"_\r\n";
/// R2 stub: FLUSHALL / FLUSHDB (no-op)
pub const STUB_FLUSH: &[u8] = b"+OK\r\n";
/// R2 stub: WAIT
pub const STUB_WAIT: &[u8] = b":0\r\n";
/// CLUSTER COUNTKEYSINSLOT / GETKEYSINSLOT
pub const ERR_NO_KEYS: &[u8] = b"-ERR the coordinator stores no keys\r\n";
/// Follower without a recent leader.
pub const ERR_CLUSTER_DOWN: &[u8] = b"-CLUSTERDOWN The cluster is down\r\n";

pub async fn serve_clients(l: tokio::net::TcpListener, wsr: Arc<Wsr>) -> std::io::Result<()> {
    todo!("DESIGN §23.5: client service")
}
