//! Cluster state transitions (DESIGN §23.2, §23.9, §23.12, §23.13, LC-25).
//!
//! `Admin(AdminOp)` is the only kind that changes `nodes`, member lists, the assignment of
//! unassigned slots, migration start/resume/abort and params; it comes only from operator
//! commands. The others record health flags, promotions decided by the static failover rule, ISR
//! changes, member states and migration phases. Nothing re-replicates, rebalances or migrates on
//! its own.

use std::net::SocketAddr;

use kv_ctrl::{MemberState, MigPhase, NodeId, ShardId, SlotRanges, TermEnd};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub enum Transition {
    Admin(AdminOp),
    Health { node: NodeId, fail: bool },
    Promote { shard: ShardId, primary: NodeId, term: u32, isr: Vec<NodeId>, term_end: TermEnd },
    Isr { shard: ShardId, term: u32, node: NodeId, add: bool },
    MemberState { shard: ShardId, node: NodeId, state: MemberState },
    Migration { id: u64, phase: MigPhase, slots_to_dst: bool },
}

/// Operator commands (§23.12).
#[derive(Clone, Serialize, Deserialize)]
pub enum AdminOp {
    ParamSet { key: String, value: String },
    NodeAdd { id: NodeId, ctrl: SocketAddr, client: SocketAddr, repl: SocketAddr, zone: String, priority: u8, partitions: u16 },
    NodeRemove { id: NodeId, force: bool },
    NodeMaint { id: NodeId, on: bool },
    ShardCreate { id: ShardId, primary: NodeId, min_isr: u8 },
    ShardDelete { id: ShardId },
    SlotsAssign { slots: SlotRanges, shard: ShardId },
    ReplicaAdd { shard: ShardId, node: NodeId },
    ReplicaRemove { shard: ShardId, node: NodeId },
    ReplicaResync { node: NodeId },
    MigrateStart { id: u64, slots: SlotRanges, dst: ShardId, rate_mbps: Option<u32>, barriers: u8 },
    MigratePause { id: u64 },
    MigrateResume { id: u64 },
    MigrateAbort { id: u64 },
}

/// Why a batch was refused.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Reject {
    UnknownNode,
    UnknownShard,
    UnknownMigration,
    /// A §23.12 precondition does not hold.
    Precondition(String),
    StaleTerm,
    StateTooLarge,
    Malformed,
}
