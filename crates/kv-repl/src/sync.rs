//! The per-partition sync decision (DESIGN §23.7, §23.9, LC-25, LC-39).
//!
//! 1. `last_term == term` and the backlog covers `applied + 1` -> `Continue { from: applied + 1 }`
//!    (current-term frames only).
//! 2. `last_term < term`, not a learner, and the needed key sets are known -> `Repair { at }`:
//!    keys of the member's own groups after its term's takeover point (its received ring, or its
//!    post-scan divergent set sent as `RepairKeys`), keys of `(applied, takeover]` from the
//!    primary's received ring of that term, keys of whole intermediate terms and of the current
//!    term up to `at` from the primary's backlog; then one image per key stamped `at`.
//! 3. Learner (`replica add`) or operator `replica resync` -> `Full { at }` (in-place merge).
//! 4. Otherwise (including `SyncInterrupted`) -> `Reject(NeedOperatorResync)`. There is no
//!    automatic path to a full sync.
//!
//! The decision reads only the handshake, the shard's committed history and the primary's two
//! RAM rings; it never compares a lag or a ring size against a constant (LC-36): coverage is a
//! position test (`seq >= floor`). A range whose groups hold a partition-wide op (`FlushDb`,
//! `FlushSlot`) cannot be described by keys and counts as not covered.

use crate::Seq;
use crate::backlog::Backlog;
use crate::codec::KeySet;
use crate::frame::{Hello, Recovered, RejectReason};

/// What the primary knows when a member says Hello.
pub struct PrimaryView<'a> {
    pub term: u32,
    /// `(term, takeover seq per partition)` of past terms.
    pub history: &'a [(u32, Vec<u64>)],
    pub backlog: &'a Backlog,
    pub received: &'a Backlog,
    pub is_member: bool,
    /// replica add/resync only
    pub operator_full_sync: bool,
    pub last_executed: Seq,
}

pub enum SyncDecision {
    Continue { from: Seq },
    Repair { at: Seq, primary_keys: Vec<(u8, Box<[u8]>)>, want_member_keys: bool },
    Full { at: Seq },
    Reject(RejectReason),
}

impl SyncDecision {
    /// Short name for diagnostics.
    pub fn kind(&self) -> &'static str {
        match self {
            SyncDecision::Continue { .. } => "continue",
            SyncDecision::Repair { .. } => "repair",
            SyncDecision::Full { .. } => "full",
            SyncDecision::Reject(_) => "reject",
        }
    }
}

/// The takeover seq of `term` for partition `p`, if the history still records that term.
pub fn takeover(history: &[(u32, Vec<u64>)], term: u32, p: u16) -> Option<Seq> {
    history.iter().find(|(t, _)| *t == term).and_then(|(_, v)| v.get(usize::from(p)).copied()).map(Seq)
}

/// The seq the next group after `s` carries at the earliest (`from` of a `Continue`).
fn after(s: Seq) -> Seq {
    Seq(s.0.saturating_add(1))
}

pub fn decide(h: &Hello, v: &PrimaryView<'_>) -> SyncDecision {
    // Both sides reject peers whose term is below their fence: a member that saw a newer term
    // than ours talks to a primary that has been superseded.
    if h.fence_term > v.term || h.last_term > v.term {
        return SyncDecision::Reject(RejectReason::StaleTerm);
    }
    if !v.is_member {
        return SyncDecision::Reject(RejectReason::NotMember);
    }
    // the only start of a full sync: the operator's replica add or replica resync (LC-25)
    let start = Seq::new(v.term, 0);
    let at = v.last_executed.max(start);
    if v.operator_full_sync {
        return SyncDecision::Full { at };
    }
    if h.recovered == Recovered::SyncInterrupted {
        return SyncDecision::Reject(RejectReason::NeedOperatorResync);
    }
    if h.last_term == v.term {
        // frames of the current term only, i.e. produced by this primary
        if h.applied <= at && v.backlog.covers_after(h.applied) {
            return SyncDecision::Continue { from: after(h.applied) };
        }
        return SyncDecision::Reject(RejectReason::NeedOperatorResync);
    }
    if h.learner {
        return SyncDecision::Reject(RejectReason::NeedOperatorResync);
    }
    let mut keys = KeySet::default();
    // (b) + (c): groups of earlier terms the member may lack, up to the end of the previous
    // term as this primary took it over; they are in this primary's received ring
    let Some(prev_end) = v.term.checked_sub(1).and_then(|t| takeover(v.history, t, h.partition)) else {
        return SyncDecision::Reject(RejectReason::NeedOperatorResync);
    };
    if h.applied < prev_end {
        match v.received.keys_between_into(h.applied, prev_end, &mut keys) {
            Some(false) => {}
            // not covered, or a partition-wide op a key set cannot describe
            _ => return SyncDecision::Reject(RejectReason::NeedOperatorResync),
        }
    }
    // (d): the current term's groups up to `at`, from this primary's own backlog
    if at > start {
        match v.backlog.keys_between_into(start, at, &mut keys) {
            Some(false) => {}
            _ => return SyncDecision::Reject(RejectReason::NeedOperatorResync),
        }
    }
    // (a) comes from the member itself (its ring, or its post-recovery divergent set)
    SyncDecision::Repair { at, primary_keys: keys.into_vec(), want_member_keys: true }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::encode_group;
    use crate::{Group, Op};

    fn put(seq: Seq, key: &[u8]) -> Vec<u8> {
        let mut b = Vec::new();
        encode_group(&Group { seq, ops: vec![Op::Put { db: 0, vtype: 1, expire_at: 0, key: key.into(), value: b"v".to_vec() }] }, &mut b);
        b
    }

    fn hello(last_term: u32, applied: Seq) -> Hello {
        Hello {
            cluster_id: [1; 16],
            auth: b"t".to_vec(),
            node_id: [2; 20],
            shard: 1,
            partition: 3,
            fence_term: last_term,
            last_term,
            applied,
            recovered: Recovered::Clean,
            learner: false,
            migration: None,
        }
    }

    struct Rings {
        backlog: Backlog,
        received: Backlog,
        history: Vec<(u32, Vec<u64>)>,
    }

    /// A primary of term 3 that was a member in term 2 (received (2,1)..=(2,10)), took over
    /// at (2, 8), and produced (3,1)..=(3,4).
    fn rings() -> Rings {
        let mut received = Backlog::new(1 << 16);
        received.start_after(Seq::new(2, 0));
        for i in 1..=8 {
            let s = Seq::new(2, i);
            received.push(s, &put(s, format!("old{i}").as_bytes()));
        }
        let mut backlog = Backlog::new(1 << 16);
        backlog.start_after(Seq::new(3, 0));
        for i in 1..=4 {
            let s = Seq::new(3, i);
            backlog.push(s, &put(s, format!("new{i}").as_bytes()));
        }
        let mut takeover = vec![0u64; 16];
        takeover[3] = Seq::new(2, 8).0;
        Rings { backlog, received, history: vec![(2, takeover)] }
    }

    fn view<'a>(r: &'a Rings) -> PrimaryView<'a> {
        PrimaryView {
            term: 3,
            history: &r.history,
            backlog: &r.backlog,
            received: &r.received,
            is_member: true,
            operator_full_sync: false,
            last_executed: Seq::new(3, 4),
        }
    }

    fn keys(d: &SyncDecision) -> Vec<String> {
        match d {
            SyncDecision::Repair { primary_keys, .. } => {
                primary_keys.iter().map(|(_, k)| String::from_utf8_lossy(k).into_owned()).collect()
            }
            _ => panic!("expected repair, got {}", d.kind()),
        }
    }

    #[test]
    fn same_term_continues_from_the_backlog() {
        let r = rings();
        let d = decide(&hello(3, Seq::new(3, 2)), &view(&r));
        assert!(matches!(d, SyncDecision::Continue { from } if from == Seq::new(3, 3)));
        // the very start of the term is covered too
        let d = decide(&hello(3, Seq::new(3, 0)), &view(&r));
        assert!(matches!(d, SyncDecision::Continue { from } if from == Seq::new(3, 1)));
        // a member claiming more than this primary executed is not continued
        let d = decide(&hello(3, Seq::new(3, 9)), &view(&r));
        assert!(matches!(d, SyncDecision::Reject(RejectReason::NeedOperatorResync)));
    }

    #[test]
    fn older_terms_repair_with_the_keys_of_every_needed_range() {
        let r = rings();
        // behind in term 2: (2,5]..(2,8] from the received ring, plus the current term
        let d = decide(&hello(2, Seq::new(2, 5)), &view(&r));
        assert_eq!(keys(&d), ["old6", "old7", "old8", "new1", "new2", "new3", "new4"]);
        let SyncDecision::Repair { at, want_member_keys, .. } = d else { unreachable!() };
        assert_eq!(at, Seq::new(3, 4));
        assert!(want_member_keys, "the member's own groups after the takeover come from it");
        // ahead of the takeover point (an old primary): only the current term's keys here
        let d = decide(&hello(2, Seq::new(2, 30)), &view(&r));
        assert_eq!(keys(&d), ["new1", "new2", "new3", "new4"]);
    }

    #[test]
    fn uncovered_ranges_need_the_operator() {
        let r = rings();
        // applied before the received ring's start
        let d = decide(&hello(1, Seq::new(1, 7)), &view(&r));
        assert!(matches!(d, SyncDecision::Reject(RejectReason::NeedOperatorResync)));
        // a flush inside a needed range cannot be described by keys
        let mut r2 = rings();
        let mut b = Vec::new();
        encode_group(&Group { seq: Seq::new(3, 5), ops: vec![Op::FlushDb { db: None }] }, &mut b);
        r2.backlog.push(Seq::new(3, 5), &b);
        let v = PrimaryView { last_executed: Seq::new(3, 5), ..view(&r2) };
        assert!(matches!(decide(&hello(2, Seq::new(2, 8)), &v), SyncDecision::Reject(RejectReason::NeedOperatorResync)));
        // no history entry for the previous term
        let r3 = Rings { history: Vec::new(), ..rings() };
        assert!(matches!(decide(&hello(2, Seq::new(2, 8)), &view(&r3)), SyncDecision::Reject(RejectReason::NeedOperatorResync)));
        // interrupted full sync
        let mut h = hello(3, Seq::new(3, 2));
        h.recovered = Recovered::SyncInterrupted;
        assert!(matches!(decide(&h, &view(&r)), SyncDecision::Reject(RejectReason::NeedOperatorResync)));
        // a learner without the operator's full sync
        let mut h = hello(0, Seq::ZERO);
        h.learner = true;
        assert!(matches!(decide(&h, &view(&r)), SyncDecision::Reject(RejectReason::NeedOperatorResync)));
    }

    #[test]
    fn full_sync_only_for_operator_members_and_stale_peers_are_refused() {
        let r = rings();
        let v = PrimaryView { operator_full_sync: true, ..view(&r) };
        assert!(matches!(decide(&hello(0, Seq::ZERO), &v), SyncDecision::Full { at } if at == Seq::new(3, 4)));
        let v = PrimaryView { is_member: false, ..view(&r) };
        assert!(matches!(decide(&hello(3, Seq::new(3, 1)), &v), SyncDecision::Reject(RejectReason::NotMember)));
        let mut h = hello(3, Seq::new(3, 1));
        h.fence_term = 4;
        assert!(matches!(decide(&h, &view(&r)), SyncDecision::Reject(RejectReason::StaleTerm)));
        assert!(matches!(decide(&hello(5, Seq::new(5, 1)), &view(&r)), SyncDecision::Reject(RejectReason::StaleTerm)));
        // a fresh term without writes yet: the decision position is the term's start
        let mut fresh = rings();
        fresh.backlog = Backlog::new(4096);
        fresh.backlog.start_after(Seq::new(3, 0));
        let v = PrimaryView { last_executed: Seq::new(2, 8), ..view(&fresh) };
        assert!(matches!(decide(&hello(2, Seq::new(2, 8)), &v), SyncDecision::Repair { at, .. } if at == Seq::new(3, 0)));
        assert_eq!(takeover(&r.history, 2, 3), Some(Seq::new(2, 8)));
        assert_eq!(takeover(&r.history, 1, 3), None);
    }
}
