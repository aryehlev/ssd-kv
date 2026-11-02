//! Control messages (DESIGN §23.4, §23.9, §23.10, §23.12, §23.13).
//!
//! Handshake: `CoordToNode::Hello` -> `NodeToCoord::HelloAck` (identity and readiness only).
//! Leases: the node sends `Renew` every `renew_ms`; the leader answers `LeaseGrant` while its
//! own check-quorum lease is valid. Fences go to one chosen node at a time (§23.9). Requests carry
//! a `req` id that replies echo. There are no two-phase or ballot message types (LC-26).

use core::fmt;

use serde::{Deserialize, Serialize};

use crate::assignment::{MigPhase, NodeAssignment};
use crate::ids::{CoordId, NodeId, ShardId};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CoordToNode {
    /// `auth` is the cluster token: no `Debug` on this type.
    Hello {
        cluster_id: [u8; 16],
        coord: CoordId,
        coord_term: u64,
        auth: Vec<u8>,
    },
    Assign(NodeAssignment),
    LeaseGrant {
        seq: u64,
        lease_ms: u32,
    },
    /// to one node only, §23.9
    Fence {
        req: u64,
        shard: ShardId,
        new_term: u32,
    },
    Handoff {
        req: u64,
        shard: ShardId,
        to: NodeId,
    },
    MigrationPhase {
        id: u64,
        phase: MigPhase,
    },
    Ping,
}

// `Hello` carries the cluster token and `Assign` the client password.
static_assertions::assert_not_impl_any!(CoordToNode: fmt::Debug);

impl CoordToNode {
    /// The request id a reply must echo (`Fence`, `Handoff`), if any.
    pub fn req(&self) -> Option<u64> {
        match self {
            CoordToNode::Fence { req, .. } | CoordToNode::Handoff { req, .. } => Some(*req),
            _ => None,
        }
    }

    /// Short variant name for diagnostics (never the contents).
    pub fn kind(&self) -> &'static str {
        match self {
            CoordToNode::Hello { .. } => "hello",
            CoordToNode::Assign(_) => "assign",
            CoordToNode::LeaseGrant { .. } => "lease-grant",
            CoordToNode::Fence { .. } => "fence",
            CoordToNode::Handoff { .. } => "handoff",
            CoordToNode::MigrationPhase { .. } => "migration-phase",
            CoordToNode::Ping => "ping",
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum NodeToCoord {
    // replies to coordinator requests: identity, acks and positions only; no
    // role/shard/slot/key-count/term field (LC-26)
    HelloAck {
        node: Option<NodeId>,
        incarnation: u64,
        set_uuid: [u8; 16],
        partitions: u16,
        ready: bool,
    },
    AssignAck {
        version: u64,
    },
    FenceAck {
        req: u64,
        applied: Vec<u64>,
    },
    HandoffDone {
        req: u64,
        last: Vec<u64>,
    },
    Error(NodeError),
    // node-initiated messages
    Renew {
        seq: u64,
        incarnation: u64,
        version_seen: u64,
        health: Health,
    },
    IsrShrink {
        shard: ShardId,
        term: u32,
        member: NodeId,
    },
    IsrExpand {
        shard: ShardId,
        term: u32,
        member: NodeId,
    },
    MemberReady {
        shard: ShardId,
        term: u32,
        member: NodeId,
    },
    MigrationReady {
        id: u64,
    },
    SealDone {
        id: u64,
    },
    /// Primary: `member` cannot catch up by Continue or Repair (its handshake was refused with
    /// `NeedOperatorResync`): only `replica resync` can bring it back (§23.7 case 4). The
    /// coordinator records the member state NeedsFullSync; nothing starts a sync by itself.
    MemberNeedsResync {
        shard: ShardId,
        term: u32,
        member: NodeId,
    },
    /// Primary: the node is shutting down (SIGTERM) and asks for a graceful handoff of
    /// `shard` first (§23.12; the target is chosen by the coordinator's static rule).
    HandoffRequest {
        shard: ShardId,
        term: u32,
    },
}

impl NodeToCoord {
    /// True for the replies to coordinator requests (`HelloAck`, `AssignAck`, `FenceAck`,
    /// `HandoffDone`, `Error`); false for node-initiated messages.
    pub fn is_reply(&self) -> bool {
        matches!(
            self,
            NodeToCoord::HelloAck { .. }
                | NodeToCoord::AssignAck { .. }
                | NodeToCoord::FenceAck { .. }
                | NodeToCoord::HandoffDone { .. }
                | NodeToCoord::Error(_)
        )
    }

    /// The request id this reply echoes (`FenceAck`, `HandoffDone`), if any.
    pub fn req(&self) -> Option<u64> {
        match self {
            NodeToCoord::FenceAck { req, .. } | NodeToCoord::HandoffDone { req, .. } => Some(*req),
            _ => None,
        }
    }

    /// Short variant name for diagnostics.
    pub fn kind(&self) -> &'static str {
        match self {
            NodeToCoord::HelloAck { .. } => "hello-ack",
            NodeToCoord::AssignAck { .. } => "assign-ack",
            NodeToCoord::FenceAck { .. } => "fence-ack",
            NodeToCoord::HandoffDone { .. } => "handoff-done",
            NodeToCoord::Error(_) => "error",
            NodeToCoord::Renew { .. } => "renew",
            NodeToCoord::IsrShrink { .. } => "isr-shrink",
            NodeToCoord::IsrExpand { .. } => "isr-expand",
            NodeToCoord::MemberReady { .. } => "member-ready",
            NodeToCoord::MigrationReady { .. } => "migration-ready",
            NodeToCoord::SealDone { .. } => "seal-done",
            NodeToCoord::MemberNeedsResync { .. } => "member-needs-resync",
            NodeToCoord::HandoffRequest { .. } => "handoff-request",
        }
    }
}

/// no contents
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum NodeError {
    StaleAssignment,
    BadAuth,
    Malformed,
}

/// Node health carried by `Renew` (no storage-maintenance field, LC-30).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
pub struct Health {
    pub ready: bool,
    pub failed: bool,
    pub read_only_partitions: u16,
    pub corrupt_pages: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reply_classification() {
        assert!(NodeToCoord::AssignAck { version: 1 }.is_reply());
        assert!(NodeToCoord::Error(NodeError::Malformed).is_reply());
        assert!(!NodeToCoord::SealDone { id: 1 }.is_reply());
        assert_eq!(
            NodeToCoord::FenceAck {
                req: 5,
                applied: vec![]
            }
            .req(),
            Some(5)
        );
        assert_eq!(
            CoordToNode::Handoff {
                req: 6,
                shard: ShardId(1),
                to: NodeId([1; 20])
            }
            .req(),
            Some(6)
        );
        assert_eq!(CoordToNode::Ping.req(), None);
        assert_eq!(CoordToNode::Ping.kind(), "ping");
    }
}
