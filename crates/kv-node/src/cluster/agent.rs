//! The agent thread (DESIGN §6.1, §23.4, §23.9, §23.10, §23.12).
//!
//! A tokio current-thread runtime. It accepts the coordinator leader's control connection on
//! `cluster-control-port` (data nodes never dial anyone for topology), answers `Hello` with
//! identity and readiness only, applies `Assign` in `(coord_term, version)` order after
//! persisting role, shard, fence term and version in node meta, answers older assignments with
//! `Error(StaleAssignment)`, sends `Renew` every `renew_ms` and sets the lease cell from each
//! `LeaseGrant` (`t_send + lease_ms * 0.95 - 10 ms`), persists the fence term before
//! acknowledging a `Fence`, runs graceful handoffs, and serves `/metrics`. It talks to
//! partitions only through `XMsg::Ctrl`.

use kv_ctrl::{MigPhase, NodeAssignment, NodeId, ShardId};

/// Control-plane events delivered to workers.
pub enum CtrlEvent {
    /// A newly applied assignment (each worker rebuilds what it needs).
    Assign(Box<NodeAssignment>),
    /// Fence to `new_term`: close streams of older terms, report applied positions.
    Fence { req: u64, shard: ShardId, new_term: u32 },
    /// Graceful handoff to `to`: refuse new writes with TRYAGAIN, drain, report last positions.
    Handoff { req: u64, shard: ShardId, to: NodeId },
    MigrationPhase { id: u64, phase: MigPhase },
}

/// Runs the agent until shutdown.
pub fn run(shared: std::sync::Arc<crate::shared::NodeShared>) -> std::io::Result<()> {
    todo!("DESIGN §23.4: agent thread")
}
