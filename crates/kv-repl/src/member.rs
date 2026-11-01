//! Member side of a partition stream (DESIGN §23.7, §23.9).
//!
//! `MemberStream` turns received frames into partition actions: groups are applied in seq order
//! with `Origin::Replicated` (ops skipped when the stored seq is >= the op's seq), repair images
//! with `ApplyMode::Image`, full-sync records with `ApplyMode::SnapshotMerge` (identical ->
//! mark only). `SyncBegin` sets the durable SYNCING flag and resets nothing; `SyncDone` prunes the
//! unmarked entries of the synced slots with ordinary deletes. The member acks its contiguous
//! durable prefix (coalesced, at most one ack per ms) and keeps a ring of received groups.

use crate::backlog::Backlog;
use crate::frame::{Frame, FrameError, ImageRec, SnapRec};
use crate::{Group, Seq};

pub enum MemberAction {
    Apply(Group),
    Image(ImageRec, Seq),
    /// ApplyMode::SnapshotMerge
    Snap(SnapRec, Seq),
    /// set SYNCING; nothing is reset
    SyncBegin(Seq),
    Commit(Seq),
    /// prune unmarked entries
    SyncDone(Seq),
    SendRepairKeys,
    Nothing,
}

pub struct MemberStream {
    partition: u16,
    received: Backlog,
    applied: Seq,
    durable: Seq,
    last_ack_ns: u64,
}

impl MemberStream {
    pub fn new(partition: u16, backlog_bytes: usize) -> Self {
        todo!("DESIGN §23.7: member stream")
    }

    pub fn on_frame(&mut self, f: Frame) -> Result<MemberAction, FrameError> {
        todo!("DESIGN §23.7: member frame handling")
    }

    pub fn note_durable(&mut self, s: Seq) {
        todo!("DESIGN §23.7: member durability")
    }

    pub fn ack_due(&mut self, now_ns: u64) -> Option<Frame> {
        todo!("DESIGN §23.7: coalesced ack")
    }

    pub fn received(&self) -> &Backlog {
        &self.received
    }
}
