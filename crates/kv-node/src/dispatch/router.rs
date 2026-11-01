//! Routing of one parsed command (DESIGN §6.2, §7.5, §23.6).
//!
//! Unknown command -> `-ERR unknown command 'x', with args beginning with: ...` (Redis 7.0
//! format); arity error -> `-ERR wrong number of arguments for 'x' command`; NOAUTH before AUTH;
//! inside MULTI, `+QUEUED` (queue-time errors abort EXEC). Cluster mode: `Ownership::gate()`
//! decides Serve / MOVED to a coordinator / CROSSSLOT / NOREPLICAS / TRYAGAIN / READONLY /
//! CLUSTERDOWN before execution (and again at queue time inside MULTI). Then
//! `slot = key_slot(key)`, `partition = slot % P`, `worker = partition % W`.

use crate::cluster::ownership::Gate;
use crate::runtime::xmsg::ExecEnv;

/// Where a command goes.
pub enum Route {
    /// Execute on a partition of this worker.
    Local { partition: u16 },
    /// Send to the owner worker.
    Remote { worker: u16, partition: u16 },
    /// Split across partitions (standalone multi-key reads/deletes).
    FanOut,
    /// Cross-partition transaction (standalone).
    XTxn,
    /// Keyless command handled on the home worker.
    Home,
    /// Answered by the gate without execution.
    Refused(Gate),
}

/// Partition of a slot.
pub fn partition_of(slot: u16, partitions: u16) -> u16 {
    slot % partitions
}

/// Worker owning a partition.
pub fn worker_of(partition: u16, workers: u16) -> u16 {
    partition % workers
}

/// Routes one command.
pub fn route(id: kv_cmd::CmdId, argv: &[&[u8]], env: &ExecEnv) -> Route {
    todo!("DESIGN §23.6: routing")
}
