//! Command handlers and their registry (DESIGN §8, §24.11).
//!
//! Every command id maps to one [`Handler`]: `Keyed` handlers run inside a partition's `exec`
//! against a `Keyspace` (re-runnable: they mutate only through staged `Keyspace` calls); `Conn`
//! handlers run on the connection's home worker for keyless and connection commands; `Scan` and
//! `Txn` are the SCAN/KEYS step machinery and MULTI/EXEC; `Cluster` handles CLUSTER and related
//! commands. Handlers never read `Ownership` fields; they call `gate()` and `slot_filter()`
//! (LC-24). Error texts are Redis 7.0.15's (§8.1).

pub mod cluster_cmds;
pub mod config_cmd;
pub mod conn_cmds;
pub mod expire;
pub mod hash;
pub mod info;
pub mod keys;
pub mod scan;
pub mod server;
pub mod strings;
pub mod txn;

use crate::net::conn::Conn;
use crate::runtime::xmsg::ExecEnv;
use crate::shared::NodeShared;

pub type KeyedFn = fn(
    &mut kv_storage::keyspace::Keyspace<'_>,
    &Args<'_>,
    &mut kv_resp::ReplyWriter<'_>,
    &ExecEnv,
) -> Result<(), kv_storage::keyspace::StoreError>;

/// keyless/connection commands on the home worker
pub type ConnFn = fn(&mut ConnCtx<'_>, &Args<'_>, &mut kv_resp::ReplyWriter<'_>) -> ConnOutcome;

/// Arguments of one command (index 0 = command name).
pub struct Args<'a> {
    pub argv: smallvec::SmallVec<[&'a [u8]; 8]>,
}

/// What a connection-level handler wants next.
pub enum ConnOutcome {
    Done,
    /// Close after the reply is flushed (QUIT).
    Close,
    FanOut(FanOutSplit),
    /// The reply arrives later (gathers, barriers, SHUTDOWN).
    Async,
}

/// fixed per-command split: one sub-request per distinct partition of the keys, key bytes only
/// (LC-37)
pub struct FanOutSplit {
    /// `(partition, argument indexes of its keys)`, one entry per distinct partition.
    pub parts: smallvec::SmallVec<[(u16, smallvec::SmallVec<[u16; 4]>); 4]>,
}

/// Context of a connection-level handler.
pub struct ConnCtx<'a> {
    pub conn: &'a mut Conn,
    pub shared: &'a NodeShared,
    pub worker: u16,
}

#[derive(Clone, Copy)]
pub enum Handler {
    Keyed(KeyedFn),
    Conn(ConnFn),
    Scan,
    Txn,
    Cluster(ConnFn),
}

/// The handler registered for a command id.
pub fn handler(id: kv_cmd::CmdId) -> Handler {
    todo!("DESIGN §8: handler registry")
}
