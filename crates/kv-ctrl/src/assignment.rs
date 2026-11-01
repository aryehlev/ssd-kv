//! A node's projection of the committed cluster state (DESIGN §23.4, §23.13).
//!
//! `Assign(NodeAssignment)` is pushed on every committed change of the node's projection; the
//! node applies assignments in `(coord_term, version)` order and persists role, shard, fence and
//! version in node meta before acknowledging. An assignment names the node's own role in its
//! own shard only; no full slot map type exists in this crate or in kv-node (LC-24, LC-26). The
//! coordinator list is rendered from the coordinators' static operator-set peer list, in list
//! order (MOVED targets, §23.6).

use core::fmt;
use std::net::SocketAddr;

use kv_common::{SLOTS, SlotSet};
use serde::{Deserialize, Serialize};

use crate::ids::{CoordId, NodeId, ShardId};

/// Highest slot number.
const MAX_SLOT: u16 = (SLOTS - 1) as u16;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
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

// The assignment carries the client password: it never gets a `Debug` rendering.
static_assertions::assert_not_impl_any!(NodeAssignment: fmt::Debug);

impl NodeAssignment {
    /// `(coord_term, version)`: the order in which a node applies assignments (§23.4). An
    /// assignment whose key is not greater than the last applied one is stale.
    pub fn order_key(&self) -> (u64, u64) {
        (self.coord_term, self.version)
    }

    /// True when `self` and `other` give the node the same projection (everything except the
    /// version and the coordinator term that stamped it). The coordinator pushes an assignment
    /// only when the projection changed.
    pub fn same_projection(&self, other: &NodeAssignment) -> bool {
        self.node_id == other.node_id
            && self.coordinators == other.coordinators
            && self.role == other.role
            && self.client_password == other.client_password
            && self.params == other.params
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
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

impl MemberState {
    /// Lowercase name used by `kvadm status`.
    pub fn as_str(self) -> &'static str {
        match self {
            MemberState::InSync => "in-sync",
            MemberState::Reconciling => "reconciling",
            MemberState::Syncing => "syncing",
            MemberState::CatchingUp => "catching-up",
            MemberState::Failed => "failed",
            MemberState::NeedsFullSync => "needs-full-sync",
        }
    }
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

impl MigPhase {
    /// Copy, Ready and Cutover: the migration is running (its slots are in flight).
    pub fn is_active(self) -> bool {
        matches!(self, MigPhase::Copy | MigPhase::Ready | MigPhase::Cutover)
    }

    /// Lowercase name used by `kvadm migrations`.
    pub fn as_str(self) -> &'static str {
        match self {
            MigPhase::Copy => "copy",
            MigPhase::Ready => "ready",
            MigPhase::Cutover => "cutover",
            MigPhase::Done => "done",
            MigPhase::Paused => "paused",
        }
    }
}

/// Inclusive slot ranges `(first, last)`, ascending.
///
/// A valid value ([`SlotRanges::is_valid`]) has `first <= last <= 16383` in every range and
/// strictly ascending, non-overlapping ranges. [`SlotRanges::normalized`] additionally merges
/// adjacent ranges; that form is the one stored in the cluster state.
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
pub struct SlotRanges(pub Vec<(u16, u16)>);

impl SlotRanges {
    /// Maximal runs of the slots in `set`, ascending (the normalized form).
    pub fn from_slot_set(set: &SlotSet) -> Self {
        let mut out = Vec::new();
        let mut run: Option<(u16, u16)> = None;
        for (wi, &w) in set.words().iter().enumerate() {
            if w == 0 {
                if let Some(r) = run.take() {
                    out.push(r);
                }
                continue;
            }
            for bit in 0..64u16 {
                // wi < 256 and bit < 64, so the slot fits in u16.
                let s = (wi as u16) * 64 + bit;
                if w & (1u64 << bit) != 0 {
                    run = match run {
                        Some((a, b)) if b + 1 == s => Some((a, s)),
                        Some(r) => {
                            out.push(r);
                            Some((s, s))
                        }
                        None => Some((s, s)),
                    };
                } else if let Some(r) = run.take() {
                    out.push(r);
                }
            }
        }
        if let Some(r) = run {
            out.push(r);
        }
        SlotRanges(out)
    }

    /// The slots as a bitmap. Ranges outside `0..=16383` are clipped.
    pub fn to_slot_set(&self) -> SlotSet {
        let mut set = SlotSet::new();
        for &(a, b) in &self.0 {
            for s in a..=b.min(MAX_SLOT) {
                set.insert(s);
            }
        }
        set
    }

    /// Every range has `first <= last <= 16383`; ranges are strictly ascending and disjoint.
    pub fn is_valid(&self) -> bool {
        let mut prev: Option<u16> = None;
        for &(a, b) in &self.0 {
            if a > b || b > MAX_SLOT {
                return false;
            }
            if let Some(p) = prev
                && a <= p
            {
                return false;
            }
            prev = Some(b);
        }
        true
    }

    /// Valid ranges with adjacent ones merged (`None` when not valid).
    pub fn normalized(&self) -> Option<SlotRanges> {
        if !self.is_valid() {
            return None;
        }
        let mut out: Vec<(u16, u16)> = Vec::with_capacity(self.0.len());
        for &(a, b) in &self.0 {
            match out.last_mut() {
                Some(last) if last.1 + 1 == a => last.1 = b,
                _ => out.push((a, b)),
            }
        }
        Some(SlotRanges(out))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Number of slots covered (ranges assumed disjoint).
    pub fn slot_count(&self) -> usize {
        self.0
            .iter()
            .map(|&(a, b)| usize::from(b.saturating_sub(a)) + usize::from(a <= b))
            .sum()
    }

    pub fn contains(&self, slot: u16) -> bool {
        self.0.iter().any(|&(a, b)| a <= slot && slot <= b)
    }

    /// Every slot, ascending (for valid ranges).
    pub fn iter_slots(&self) -> impl Iterator<Item = u16> + '_ {
        self.0.iter().flat_map(|&(a, b)| a..=b)
    }

    /// Parses the operator syntax `a-b,c,d-e` (whitespace around items ignored). The result is
    /// validated and normalized; `None` on any syntax or range error.
    pub fn parse(s: &str) -> Option<SlotRanges> {
        let mut v = Vec::new();
        for item in s.split(',') {
            let item = item.trim();
            let (a, b) = match item.split_once('-') {
                Some((a, b)) => (a.trim(), b.trim()),
                None => (item, item),
            };
            let a = parse_slot(a)?;
            let b = parse_slot(b)?;
            v.push((a, b));
        }
        SlotRanges(v).normalized()
    }
}

fn parse_slot(s: &str) -> Option<u16> {
    if s.is_empty() || !s.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let v: u16 = s.parse().ok()?;
    (v <= MAX_SLOT).then_some(v)
}

impl fmt::Display for SlotRanges {
    /// `a-b,c` (a single-slot range prints as one number), the syntax [`SlotRanges::parse`]
    /// reads.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, &(a, b)) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            if a == b {
                write!(f, "{a}")?;
            } else {
                write!(f, "{a}-{b}")?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum MigrationTask {
    Out {
        id: u64,
        slots: SlotRanges,
        phase: MigPhase,
    },
    In {
        id: u64,
        slots: SlotRanges,
        src_repl: PeerAddr,
        phase: MigPhase,
    },
}

impl MigrationTask {
    pub fn id(&self) -> u64 {
        match self {
            MigrationTask::Out { id, .. } | MigrationTask::In { id, .. } => *id,
        }
    }

    pub fn slots(&self) -> &SlotRanges {
        match self {
            MigrationTask::Out { slots, .. } | MigrationTask::In { slots, .. } => slots,
        }
    }

    pub fn phase(&self) -> MigPhase {
        match self {
            MigrationTask::Out { phase, .. } | MigrationTask::In { phase, .. } => *phase,
        }
    }
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

impl Default for NodeParams {
    /// The §23.2 defaults: P = 64, lease 1500 ms, renew 100 ms, ack timeout 250 ms.
    fn default() -> Self {
        NodeParams {
            partitions: 64,
            lease_ms: 1500,
            renew_ms: 100,
            repl_ack_timeout_ms: 250,
        }
    }
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
        let words: [u64; 256] = v
            .try_into()
            .map_err(|_| D::Error::custom("slot set must have 256 words"))?;
        Ok(SlotSet::from_words(words))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges_from_set() {
        let mut set = SlotSet::new();
        for s in [0u16, 1, 2, 63, 64, 65, 100, 16383] {
            set.insert(s);
        }
        let r = SlotRanges::from_slot_set(&set);
        assert_eq!(r.0, vec![(0, 2), (63, 65), (100, 100), (16383, 16383)]);
        assert!(r.is_valid());
        assert_eq!(r.slot_count(), 8);
        assert!(r.to_slot_set() == set);
        assert_eq!(SlotRanges::from_slot_set(&SlotSet::new()).0, vec![]);

        let mut all = SlotSet::new();
        for s in 0..16384u16 {
            all.insert(s);
        }
        assert_eq!(SlotRanges::from_slot_set(&all).0, vec![(0, 16383)]);
    }

    #[test]
    fn ranges_validity_and_normalization() {
        assert!(SlotRanges(vec![(0, 10), (11, 20)]).is_valid());
        assert!(!SlotRanges(vec![(0, 10), (10, 20)]).is_valid());
        assert!(!SlotRanges(vec![(5, 4)]).is_valid());
        assert!(!SlotRanges(vec![(0, 16384)]).is_valid());
        assert!(!SlotRanges(vec![(20, 30), (0, 10)]).is_valid());
        assert_eq!(
            SlotRanges(vec![(0, 10), (11, 20), (30, 30)])
                .normalized()
                .unwrap()
                .0,
            vec![(0, 20), (30, 30)]
        );
        assert!(SlotRanges(vec![(3, 1)]).normalized().is_none());
    }

    #[test]
    fn ranges_parse_and_display() {
        let r = SlotRanges::parse("0-5460, 5461 ,10000-10001").unwrap();
        assert_eq!(r.0, vec![(0, 5461), (10000, 10001)]);
        assert_eq!(r.to_string(), "0-5461,10000-10001");
        assert_eq!(SlotRanges::parse("7").unwrap().to_string(), "7");
        for bad in [
            "", "a", "1-", "-1", "5-3", "16384", "1,1", "+1", "0-16384", "3-5,4",
        ] {
            assert!(SlotRanges::parse(bad).is_none(), "{bad}");
        }
        assert!(r.contains(5461) && !r.contains(5462));
        assert_eq!(r.iter_slots().count(), r.slot_count());
    }

    #[test]
    fn migration_task_accessors() {
        let t = MigrationTask::In {
            id: 9,
            slots: SlotRanges(vec![(1, 2)]),
            src_repl: PeerAddr("127.0.0.1:1".parse().unwrap()),
            phase: MigPhase::Cutover,
        };
        assert_eq!(t.id(), 9);
        assert_eq!(t.phase(), MigPhase::Cutover);
        assert_eq!(t.slots().slot_count(), 2);
        assert!(
            MigPhase::Copy.is_active()
                && !MigPhase::Done.is_active()
                && !MigPhase::Paused.is_active()
        );
    }
}
