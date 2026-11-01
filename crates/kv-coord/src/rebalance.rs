//! `kvadm rebalance plan` (DESIGN §23.12).
//!
//! `--by slots` only: a deterministic greedy list of migrate commands, computed from the
//! committed `ClusterState` alone, that evens slot counts per shard. `rebalance apply` runs them
//! one at a time, each started only when the previous one is Done, and stops at the first
//! failover or health change without re-planning.

use kv_ctrl::{ShardId, SlotRanges};
use kv_topo::ClusterState;

/// One planned `kvadm migrate <ranges> --to <shard>`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MigrateCmd {
    pub slots: SlotRanges,
    pub to: ShardId,
}

/// --by slots only; deterministic greedy over ClusterState
pub fn rebalance_plan(s: &ClusterState) -> Vec<MigrateCmd> {
    todo!("DESIGN §23.12: rebalance plan")
}
