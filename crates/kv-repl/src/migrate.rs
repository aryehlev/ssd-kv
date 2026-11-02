//! Slot migration endpoints (DESIGN §23.13, LC-36, LC-37).
//!
//! Source A, per partition holding migrating slots: streams `SnapBegin/SnapRec/SnapEnd` in slot
//! order (page-reference walk, paced by static rates) interleaved with live groups for those
//! slots, forwarding a group only for keys below the single scalar walk position. When the walk
//! completes it records `copy_end` = its last executed seq, then a fixed number `k` of further
//! barriers, each fixed when the previous one completed. Readiness is reported once the
//! destination's committed acknowledgements reach the last recorded barrier: a `Seq >= Seq` test
//! against positions recorded in the past, never a count of outstanding work. After cutover A
//! sends `Seal { id, last_src_seq }` after all earlier frames.
//!
//! Destination B applies the copy as its own writes, acks A's seqs once committed on B's ISR,
//! and is sealed when everything up to `last_src_seq` is committed.

use crate::Seq;
use crate::walk::{self, WalkPos};

/// walk position, barriers, seal tracking
pub struct MigrationSource {
    id: u64,
    /// Migrating slots of this partition, in walk (ascending) order.
    slots: Vec<u16>,
    pos: WalkPos,
    /// Further barriers after `copy_end` (`--barriers k`).
    barriers_total: u8,
    /// Barrier positions recorded so far (`copy_end`, then each further barrier), each fixed
    /// when recorded.
    recorded: Vec<Seq>,
    walk_complete: bool,
    ready: bool,
}

/// The destination side of one partition's migration stream.
pub struct MigrationDest {
    id: u64,
    /// Highest source position whose effects are committed on the destination's ISR.
    committed_src: Seq,
    /// `last_src_seq` of the source's `Seal`, once received.
    seal_at: Option<Seq>,
}

impl MigrationSource {
    pub fn new(id: u64, slots: &[u16], barriers: u8) -> Self {
        let mut slots = slots.to_vec();
        slots.sort_unstable();
        slots.dedup();
        MigrationSource {
            id,
            slots,
            pos: WalkPos(0),
            barriers_total: barriers,
            recorded: Vec::new(),
            walk_complete: false,
            ready: false,
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    /// The migrating slots of this partition, in walk order.
    pub fn slots(&self) -> &[u16] {
        &self.slots
    }

    pub fn pos(&self) -> WalkPos {
        self.pos
    }

    /// The walk position at the start of the slot with this ordinal.
    pub fn slot_start(ordinal: u16) -> WalkPos {
        WalkPos(u64::from(ordinal) << 32)
    }

    /// The walk position right after `fp` in the slot with this ordinal (`fp_next` is the next
    /// fingerprint to visit, `2^32` once the slot is done).
    pub fn position(ordinal: u16, fp_next: u64) -> WalkPos {
        WalkPos((u64::from(ordinal) << 32).saturating_add(fp_next))
    }

    /// The walk never moves back.
    pub fn advance(&mut self, to: WalkPos) {
        self.pos = self.pos.max(to);
    }

    /// The walk visited every migrating slot: `copy_end` is fixed at `last_executed`.
    pub fn walk_done(&mut self, last_executed: Seq) {
        if self.walk_complete {
            return;
        }
        self.walk_complete = true;
        self.pos = WalkPos(u64::MAX);
        self.recorded.push(last_executed);
    }

    pub fn is_walk_done(&self) -> bool {
        self.walk_complete
    }

    /// Is `slot` one of this partition's migrating slots?
    pub fn migrates(&self, slot: u16) -> bool {
        self.slots.binary_search(&slot).is_ok()
    }

    /// Has the walk passed the key with fingerprint `fp` in `slot`? (`false` for slots that do
    /// not migrate: their groups are never forwarded.) One scalar comparison.
    pub fn passed(&self, slot: u16, fp: u32) -> bool {
        match self.slots.binary_search(&slot) {
            Ok(i) => walk::passed(self.pos, i as u16, fp),
            Err(_) => false,
        }
    }

    /// `copy_end` and the further barriers recorded so far.
    pub fn barriers(&self) -> &[Seq] {
        &self.recorded
    }

    /// position barriers only: copy_end_i fixed when recorded; true once acked >= the last
    /// barrier (LC-36)
    ///
    /// `acked`: the source position the destination committed on its ISR; `last_executed`:
    /// the source partition's last executed seq now (the position the next barrier is fixed
    /// at when the current one completes).
    pub fn on_dest_committed(&mut self, acked: Seq, last_executed: Seq) -> bool /* ready */ {
        if !self.walk_complete {
            return false;
        }
        while !self.ready {
            let Some(&current) = self.recorded.last() else {
                return false;
            };
            if acked < current {
                break;
            }
            // barrier i - 1 completed
            if self.recorded.len() > usize::from(self.barriers_total) {
                self.ready = true;
            } else {
                self.recorded.push(last_executed.max(current));
            }
        }
        self.ready
    }

    pub fn is_ready(&self) -> bool {
        self.ready
    }
}

impl MigrationDest {
    pub fn new(id: u64) -> Self {
        MigrationDest {
            id,
            committed_src: Seq(0),
            seal_at: None,
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    /// Everything the source sent up to its position `s` is committed on this shard's ISR.
    pub fn on_src_seq_committed(&mut self, s: Seq) {
        self.committed_src = self.committed_src.max(s);
    }

    /// The highest committed source position.
    pub fn committed(&self) -> Seq {
        self.committed_src
    }

    /// The source's `Seal { last_src_seq }` arrived.
    pub fn on_seal(&mut self, last: Seq) {
        self.seal_at = Some(self.seal_at.map_or(last, |s| s.max(last)));
    }

    pub fn seal_at(&self) -> Option<Seq> {
        self.seal_at
    }

    pub fn sealed(&self) -> bool {
        self.seal_at.is_some_and(|s| self.committed_src >= s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn walk_position_forwards_only_passed_keys_of_migrating_slots() {
        let mut m = MigrationSource::new(9, &[40, 7, 7, 12], 2);
        assert_eq!(m.slots(), &[7, 12, 40]);
        assert!(!m.passed(7, 0), "nothing passed yet");
        m.advance(MigrationSource::position(0, 1000));
        assert!(m.passed(7, 999) && !m.passed(7, 1000));
        assert!(
            !m.passed(12, 0) && !m.passed(5, 0),
            "later slots and other slots"
        );
        m.advance(MigrationSource::slot_start(0));
        assert!(m.passed(7, 999), "the walk never moves back");
        m.advance(MigrationSource::position(1, 1 << 32));
        assert!(m.passed(7, u32::MAX) && m.passed(12, u32::MAX) && !m.passed(40, 0));
        m.walk_done(Seq::new(1, 50));
        assert!(m.passed(40, u32::MAX) && !m.passed(41, 0));
        assert!(m.migrates(12) && !m.migrates(13));
    }

    #[test]
    fn barriers_complete_in_order_and_are_fixed_when_recorded() {
        let mut m = MigrationSource::new(1, &[3], 2);
        assert!(
            !m.on_dest_committed(Seq::new(1, 1000), Seq::new(1, 1000)),
            "walk not done"
        );
        m.walk_done(Seq::new(1, 100));
        assert_eq!(m.barriers(), &[Seq::new(1, 100)]);
        // the destination is behind copy_end: nothing moves however far ahead the source is
        assert!(!m.on_dest_committed(Seq::new(1, 99), Seq::new(1, 500)));
        assert_eq!(m.barriers(), &[Seq::new(1, 100)]);
        // copy_end committed: barrier 1 is fixed at the source's position now
        assert!(!m.on_dest_committed(Seq::new(1, 100), Seq::new(1, 140)));
        assert_eq!(m.barriers(), &[Seq::new(1, 100), Seq::new(1, 140)]);
        // later source progress does not move a recorded barrier
        assert!(!m.on_dest_committed(Seq::new(1, 139), Seq::new(1, 900)));
        assert_eq!(m.barriers()[1], Seq::new(1, 140));
        assert!(!m.on_dest_committed(Seq::new(1, 140), Seq::new(1, 150)));
        assert_eq!(
            m.barriers(),
            &[Seq::new(1, 100), Seq::new(1, 140), Seq::new(1, 150)]
        );
        assert!(m.on_dest_committed(Seq::new(1, 150), Seq::new(1, 151)));
        assert!(m.is_ready());
        assert_eq!(m.barriers().len(), 3, "1 + k barriers");
        // steady state: a destination already at the source's position completes every barrier
        let mut m = MigrationSource::new(2, &[3], 2);
        m.walk_done(Seq::new(4, 10));
        assert!(m.on_dest_committed(Seq::new(4, 10), Seq::new(4, 10)));
        // k = 0: copy_end alone
        let mut m = MigrationSource::new(3, &[3], 0);
        m.walk_done(Seq::new(4, 10));
        assert!(m.on_dest_committed(Seq::new(4, 10), Seq::new(4, 99)));
    }

    /// LC-36: a destination held artificially 10^5 groups behind still becomes ready exactly
    /// when it commits the recorded barriers; readiness never depends on how far behind it is.
    #[test]
    fn migration_ready_is_position_barrier() {
        let mut m = MigrationSource::new(7, &[1, 2, 3], 2);
        let mut source = Seq::new(5, 1);
        // the walk ends with the source 10^5 groups ahead of the destination
        let behind = 100_000u64;
        for _ in 0..behind {
            source = source.next().unwrap();
        }
        m.walk_done(source);
        let copy_end = source;
        let mut dest = Seq::new(5, 1);
        let mut checks = 0u64;
        // the destination catches up one group at a time while the source keeps executing
        while !m.is_ready() {
            dest = dest.next().unwrap();
            source = source.next().unwrap();
            let ready = m.on_dest_committed(dest, source);
            checks += 1;
            let last = *m.barriers().last().unwrap();
            if ready {
                assert_eq!(dest, last, "ready exactly at the last recorded barrier");
            } else {
                assert!(dest < last, "never ready before the barrier");
            }
            assert!(checks < 10 * behind, "the barriers are finite");
        }
        let b = m.barriers();
        assert_eq!(b[0], copy_end);
        assert_eq!(b.len(), 3);
        // each further barrier was fixed at the source position when the previous completed
        assert!(b[1] > b[0] && b[2] > b[1]);
        assert_eq!(dest, b[2]);
    }

    #[test]
    fn destination_seals_once_the_seal_position_is_committed() {
        let mut d = MigrationDest::new(4);
        d.on_src_seq_committed(Seq::new(2, 10));
        assert!(!d.sealed(), "no seal yet");
        d.on_seal(Seq::new(2, 12));
        assert!(!d.sealed());
        d.on_src_seq_committed(Seq::new(2, 11));
        assert!(!d.sealed());
        d.on_src_seq_committed(Seq::new(2, 12));
        assert!(d.sealed());
        d.on_src_seq_committed(Seq::new(2, 3));
        assert_eq!(d.committed(), Seq::new(2, 12), "monotonic");
        assert_eq!(d.id(), 4);
    }
}
