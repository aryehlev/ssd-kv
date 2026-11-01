//! The slot map object (DESIGN §23.2).
//!
//! `version` is +1 per committed batch of transitions (the only map version); `cluster_id` is
//! drawn once at `kvc init`. Collections are `Vec`s in operator order; the canonical encoding is
//! postcard of the struct with Vecs in stored order, the slot array run-length encoded. Clients
//! never receive terms or seqs (LC-28). Encoded state larger than 8 MiB - 64 KiB is rejected
//! (`StateTooLarge`, §23.3).

use std::net::SocketAddr;

pub use kv_ctrl::{MemberState, MigPhase, SlotRanges, TermEnd};
use kv_ctrl::{CoordEndpoint, NodeAssignment, NodeId, ShardId};
use serde::{Deserialize, Serialize};

use crate::transition::{Reject, Transition};

/// `slots[s]` value of an unassigned slot.
pub const SLOT_UNASSIGNED: u16 = 0xFFFF;

#[derive(Clone, Serialize, Deserialize)]
pub struct ClusterState {
    /// +1 per committed batch of transitions; the only map version
    pub version: u64,
    /// random once at `kvc init`; never per transition
    pub cluster_id: [u8; 16],
    pub params: ClusterParams,
    /// operator order
    pub nodes: Vec<NodeRecord>,
    pub shards: Vec<ShardRecord>,
    /// ShardId or 0xFFFF; run-length encoded on the wire
    #[serde(with = "slots_rle")]
    pub slots: Box<[u16; 16384]>,
    pub migrations: Vec<Migration>,
}

/// Cluster parameters (no coordinator list: peer lists live in each coord.meta, §23.3).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterParams {
    /// P, cluster-wide constant
    pub partitions: u16,
    pub lease_ms: u32,
    pub renew_ms: u32,
    pub fail_grace_ms: u32,
    pub repl_ack_timeout_ms: u32,
    /// Pushed to nodes as `requirepass`; no `Debug` on this type.
    pub client_password: Option<String>,
}

impl Default for ClusterParams {
    fn default() -> Self {
        ClusterParams {
            partitions: 64,
            lease_ms: 1500,
            renew_ms: 100,
            fail_grace_ms: 200,
            repl_ack_timeout_ms: 250,
            client_password: None,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
pub struct NodeFlags {
    pub fail: bool,
    pub maint: bool,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct NodeRecord {
    pub id: NodeId,
    pub ctrl: SocketAddr,
    pub client: SocketAddr,
    pub repl: SocketAddr,
    /// Operator-assigned availability zone label.
    pub zone: String,
    /// Static operator priority, used only by the coordinator's static failover choice.
    pub priority: u8,
    pub flags: NodeFlags,
    pub partitions: u16,
    pub added_version: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum ShardStatus {
    Serving,
    NoPrimary,
    Promoting,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ShardRecord {
    pub id: ShardId,
    pub primary: Option<NodeId>,
    /// operator order
    pub members: Vec<Member>,
    pub term: u32,
    pub min_isr: u8,
    /// last 4
    pub history: Vec<TermEnd>,
    pub status: ShardStatus,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Member {
    pub node: NodeId,
    pub state: MemberState,
    pub in_isr: bool,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Migration {
    pub id: u64,
    pub slots: SlotRanges,
    pub src: ShardId,
    pub dst: ShardId,
    pub phase: MigPhase,
    /// a static token-bucket rate, never a priority
    pub rate_mbps: Option<u32>,
    /// Further position barriers after the copy (`--barriers`, default 2).
    pub barriers: u8,
}

impl ClusterState {
    pub fn initial(params: ClusterParams, cluster_id: [u8; 16]) -> Self {
        todo!("DESIGN §23.12: initial state")
    }

    /// pure; version + 1
    pub fn apply(&self, batch: &[Transition]) -> Result<ClusterState, Reject> {
        todo!("DESIGN §23.2: apply transitions")
    }

    pub fn encode(&self) -> Vec<u8> {
        todo!("DESIGN §23.2: canonical encoding")
    }

    pub fn decode(b: &[u8]) -> Result<Self, Reject> {
        todo!("DESIGN §23.2: decoding")
    }

    pub fn assignment_for(
        &self,
        node: &NodeId,
        coord_term: u64,
        coordinators: &[CoordEndpoint], /* static peer list order */
    ) -> NodeAssignment {
        todo!("DESIGN §23.4: node projection")
    }

    pub fn shard_of_slot(&self, slot: u16) -> Option<&ShardRecord> {
        todo!("DESIGN §23.5: slot owner")
    }
}

/// The slot array as `(first_slot, run_len, value)` runs.
mod slots_rle {
    use serde::de::Error;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(slots: &[u16; 16384], ser: S) -> Result<S::Ok, S::Error> {
        let mut runs: Vec<(u16, u16, u16)> = Vec::new();
        let mut i = 0usize;
        while i < slots.len() {
            let v = slots[i];
            let start = i;
            while i < slots.len() && slots[i] == v {
                i += 1;
            }
            runs.push((start as u16, (i - start - 1) as u16, v));
        }
        runs.serialize(ser)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<Box<[u16; 16384]>, D::Error> {
        let runs = Vec::<(u16, u16, u16)>::deserialize(de)?;
        let mut slots = Box::new([0u16; 16384]);
        let mut next = 0usize;
        for (start, len_minus_1, v) in runs {
            let (start, end) = (usize::from(start), usize::from(start) + usize::from(len_minus_1) + 1);
            if start != next || end > slots.len() {
                return Err(D::Error::custom("slot runs must tile 0..16384"));
            }
            slots[start..end].fill(v);
            next = end;
        }
        if next != slots.len() {
            return Err(D::Error::custom("slot runs must tile 0..16384"));
        }
        Ok(slots)
    }
}
