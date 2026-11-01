//! A node's projection of the committed cluster state (DESIGN §23.4, §23.13).
//!
//! `Assign(NodeAssignment)` is pushed on every committed change of the node's projection; the
//! node applies assignments in `(coord_term, version)` order and persists role, shard, fence and
//! version in node meta before acknowledging. An assignment names the node's own role in its
//! own shard only; no full slot map type exists in this crate or in kv-node (LC-24, LC-26). The
//! coordinator list is rendered from the coordinators' static operator-set peer list, in list
//! order (MOVED targets, §23.6).

use std::net::SocketAddr;

use kv_common::SlotSet;
use serde::{Deserialize, Serialize};

use crate::ids::{CoordId, NodeId, ShardId};

#[derive(Clone, Serialize, Deserialize)]
pub struct NodeAssignment {
    pub version: u64,
    pub coord_term: u64,
    pub node_id: NodeId,
    /// client addrs for MOVED, rendered from the coordinators' static operator-set peer list
    /// (§23.3), in list order
    pub coordinators: Vec<CoordEndpoint>,
    pub role: Option<ShardRole>,
    /// Pushed `requirepass` (a cluster parameter); no `Debug` on this type.
    pub client_password: Option<String>,
    pub params: NodeParams,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ShardRole {
    pub shard: ShardId,
    pub term: u32,
    pub is_primary: bool,
    /// 2 KiB bitmap
    #[serde(with = "slotset_serde")]
    pub owned_slots: SlotSet,
    /// replicas: whom to dial
    pub primary_repl: Option<PeerAddr>,
    /// primary: its peers (no addresses needed: they dial in); `(node, state, in_isr)`
    pub members: Vec<(NodeId, MemberState, bool)>,
    pub min_isr: u8,
    pub history: Vec<TermEnd>,
    /// Out{id, slots, phase} | In{id, slots, src_repl, phase} (the destination dials the source)
    pub migrations: Vec<MigrationTask>,
}

/// Replication state of a shard member (§23.2).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub enum MemberState {
    InSync,
    Reconciling,
    Syncing,
    CatchingUp,
    Failed,
    NeedsFullSync,
}

/// Takeover point of a past term (§23.2, §23.9).
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct TermEnd {
    pub term: u32,
    /// per partition: last seq of `term` kept by the new primary
    pub takeover: Vec<u64>,
}

/// Phase of an operator-initiated slot migration (§23.13).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub enum MigPhase {
    Copy,
    Ready,
    Cutover,
    Done,
    Paused,
}

/// Inclusive slot ranges `(first, last)`, ascending.
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
pub struct SlotRanges(pub Vec<(u16, u16)>);

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum MigrationTask {
    Out { id: u64, slots: SlotRanges, phase: MigPhase },
    In { id: u64, slots: SlotRanges, src_repl: PeerAddr, phase: MigPhase },
}

/// A coordinator as listed in assignments (MOVED target) and CLUSTER NODES.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct CoordEndpoint {
    pub id: CoordId,
    pub client: SocketAddr,
}

/// A replication endpoint.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct PeerAddr(pub SocketAddr);

/// Cluster parameters a node needs (§23.2, §23.4).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct NodeParams {
    pub partitions: u16,
    pub lease_ms: u32,
    pub renew_ms: u32,
    pub repl_ack_timeout_ms: u32,
}

/// `SlotSet` as a sequence of 256 bitmap words.
mod slotset_serde {
    use kv_common::SlotSet;
    use serde::de::Error;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(s: &SlotSet, ser: S) -> Result<S::Ok, S::Error> {
        s.words()[..].serialize(ser)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<SlotSet, D::Error> {
        let v = Vec::<u64>::deserialize(de)?;
        let words: [u64; 256] = v.try_into().map_err(|_| D::Error::custom("slot set must have 256 words"))?;
        Ok(SlotSet::from_words(words))
    }
}
