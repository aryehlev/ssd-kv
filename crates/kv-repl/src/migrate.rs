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
use crate::walk::WalkPos;

/// walk position, barriers, seal tracking
pub struct MigrationSource {
    id: u64,
    slots: Vec<u16>,
    pos: WalkPos,
    barriers_total: u8,
    /// Barrier positions recorded so far (`copy_end`, then each further barrier).
    recorded: Vec<Seq>,
    walk_complete: bool,
}

pub struct MigrationDest {
    id: u64,
    committed_src: Seq,
    seal_at: Option<Seq>,
}

impl MigrationSource {
    pub fn new(id: u64, slots: &[u16], barriers: u8) -> Self {
        MigrationSource {
            id,
            slots: slots.to_vec(),
            pos: WalkPos(0),
            barriers_total: barriers,
            recorded: Vec::new(),
            walk_complete: false,
        }
    }

    pub fn pos(&self) -> WalkPos {
        self.pos
    }

    pub fn advance(&mut self, to: WalkPos) {
        todo!("DESIGN §23.13: walk advance")
    }

    pub fn walk_done(&mut self, last_executed: Seq) {
        todo!("DESIGN §23.13: copy_end barrier")
    }

    /// position barriers only: copy_end_i fixed when recorded; true once acked >= the last
    /// barrier (LC-36)
    pub fn on_dest_committed(&mut self, acked: Seq, last_executed: Seq) -> bool /* ready */ {
        todo!("DESIGN §23.13: barrier progress")
    }
}

impl MigrationDest {
    pub fn new(id: u64) -> Self {
        MigrationDest { id, committed_src: Seq(0), seal_at: None }
    }

    pub fn on_src_seq_committed(&mut self, s: Seq) {
        todo!("DESIGN §23.13: destination commit")
    }

    pub fn sealed(&self) -> bool {
        todo!("DESIGN §23.13: seal")
    }
}
