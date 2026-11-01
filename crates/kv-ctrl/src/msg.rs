//! Control messages (DESIGN §23.4, §23.9, §23.10, §23.12, §23.13).
//!
//! Handshake: `CoordToNode::Hello` -> `NodeToCoord::HelloAck` (identity and readiness only).
//! Leases: the node sends `Renew` every `renew_ms`; the leader answers `LeaseGrant` while its
//! own check-quorum lease is valid. Fences go to one chosen node at a time (§23.9). Requests carry
//! a `req` id that replies echo. There are no prepare/commit-vote message types (LC-26).

use serde::{Deserialize, Serialize};

use crate::assignment::{MigPhase, NodeAssignment};
use crate::ids::{CoordId, NodeId, ShardId};

#[derive(Clone, Serialize, Deserialize)]
pub enum CoordToNode {
    /// `auth` is the cluster token: no `Debug` on this type.
    Hello { cluster_id: [u8; 16], coord: CoordId, coord_term: u64, auth: Vec<u8> },
    Assign(NodeAssignment),
    LeaseGrant { seq: u64, lease_ms: u32 },
    /// to one node only, §23.9
    Fence { req: u64, shard: ShardId, new_term: u32 },
    Handoff { req: u64, shard: ShardId, to: NodeId },
    MigrationPhase { id: u64, phase: MigPhase },
    Ping,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum NodeToCoord {
    // replies to coordinator requests: identity, acks and positions only; no
    // role/shard/slot/key-count/term field (LC-26)
    HelloAck { node: Option<NodeId>, incarnation: u64, set_uuid: [u8; 16], partitions: u16, ready: bool },
    AssignAck { version: u64 },
    FenceAck { req: u64, applied: Vec<u64> },
    HandoffDone { req: u64, last: Vec<u64> },
    Error(NodeError),
    // node-initiated messages
    Renew { seq: u64, incarnation: u64, version_seen: u64, health: Health },
    IsrShrink { shard: ShardId, term: u32, member: NodeId },
    IsrExpand { shard: ShardId, term: u32, member: NodeId },
    MemberReady { shard: ShardId, term: u32, member: NodeId },
    MigrationReady { id: u64 },
    SealDone { id: u64 },
}

/// no contents
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum NodeError {
    StaleAssignment,
    BadAuth,
    Malformed,
}

/// Node health carried by `Renew` (no storage-maintenance field, LC-30).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Health {
    pub ready: bool,
    pub failed: bool,
    pub read_only_partitions: u16,
    pub corrupt_pages: u32,
}
