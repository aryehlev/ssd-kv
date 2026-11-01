//! Standalone cross-partition transactions (DESIGN §6.8, §14.6, LC-37).
//!
//! 1. The coordinating worker (the one that received the command) takes the node-global
//!    `XTxnGate` (one XTxn at a time; a conflict lock, not load control). The txn credit was
//!    reserved before the command was consumed (§6.7).
//! 2. `Lock { keys }` (key bytes only) to each involved partition: it stops admitting new
//!    requests (queued FIFO), lets already-ready requests finish, prefetches the keys, replies
//!    `Locked`.
//! 3. `Read`: values and metas the command needs (MSETNX existence, RENAME source value).
//! 4. A txn id is taken and `Apply { txn_id, ops }` is sent; partitions stage TXN-flagged
//!    records and report when their writes are durable.
//! 5. When every part is durable the commit is requested from the meta thread; `Commit` is sent,
//!    partitions unlock (queued requests resume in FIFO order) and the reply is released.
//!
//! Cluster mode never uses XTxn (cross-slot -> `-CROSSSLOT`). No message carries a key hash.

use smallvec::SmallVec;

use kv_storage::effect::Op;

/// The node-global conflict gate.
pub struct XTxnGate(pub parking_lot::Mutex<()>);

pub enum XTxnMsg {
    Lock { xtxn: u64, from: u16, partition: u16, db: u8, keys: SmallVec<[(Box<[u8]>, kv_cmd::Access); 4]> },
    Locked { xtxn: u64, partition: u16 },
    Read { xtxn: u64, partition: u16 },
    /// Per locked key: `(exists, value if requested)`.
    ReadReply { xtxn: u64, partition: u16, values: Vec<(bool, Option<Vec<u8>>)> },
    Apply { xtxn: u64, txn_id: u64, partition: u16, ops: Vec<Op> },
    /// The partition's TXN-flagged writes are durable (or failed).
    Applied { xtxn: u64, partition: u16, ok: bool },
    Commit { xtxn: u64, txn_id: u64 },
    /// Unlock without applying (the command failed before staging).
    Unlock { xtxn: u64 },
}
