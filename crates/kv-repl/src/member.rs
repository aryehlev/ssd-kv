//! Member side of a partition stream (DESIGN §23.7, §23.9).
//!
//! `MemberStream` turns received frames into partition actions: groups are applied in seq order
//! with `Origin::Replicated` (ops skipped when the stored seq is >= the op's seq), repair images
//! with `ApplyMode::Image`, full-sync records with `ApplyMode::SnapshotMerge` (identical ->
//! mark only). `SyncBegin` sets the durable SYNCING flag and resets nothing; `SyncDone` prunes the
//! unmarked entries of the synced slots with ordinary deletes. The member acks its contiguous
//! durable prefix (coalesced, at most one ack per ms) and keeps a ring of received groups.
//!
//! Stream shapes after `Accept`:
//! * `Continue { from }`: `GROUPS`/`COMMIT`/`HEARTBEAT` frames.
//! * `Repair { at }`: the member answers with `REPAIR_KEYS` ([`MemberAction::SendRepairKeys`]:
//!   the keys of its own groups after its term's takeover point); then `IMAGE` frames (applied
//!   unconditionally, stamped `at`) interleaved, in the primary's execution order, with every
//!   group the primary executes meanwhile; `SYNC_DONE { end }` (the images are complete:
//!   [`MemberAction::RepairDone`]); then groups after `end`.
//! * `Full { at }`: per slot `SNAP_BEGIN`, `SNAP_REC`s, `SNAP_END`, interleaved with the groups of
//!   keys the walk has passed, then `SYNC_DONE { end }` and groups after `end`.
//! * Migration destination (§23.13): like `Full`; after `SYNC_DONE { copy_end }` the source's
//!   `COMMIT { position }` frames are position markers, and `SEAL` ends the stream.
//!
//! Snapshot records are stamped `max(at, seq of the last FlushDb received during the walk)`, so
//! a flush cutoff never hides a record written after it. Accepting a `Repair` or `Full` stream
//! resets the received-groups ring to start after `at` (the ring then covers only groups this
//! member really received; images and snapshot records are not groups).

use std::collections::VecDeque;

use crate::backlog::Backlog;
use crate::codec;
use crate::frame::{AcceptMode, Frame, FrameError, ImageRec, Reassembly, RejectReason, SnapRec};
use crate::{Group, Op, Seq};

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
    /// Full sync: the records of `slot` follow.
    SnapBegin(u16),
    /// Full sync: every record of `slot` was sent (`count` of them); keys of the slot that the
    /// walk did not confirm may now be pruned.
    SnapEnd { slot: u16, count: u64 },
    /// Repair: every image was sent; the member's state equals the primary's at `at` once they
    /// are durable.
    RepairDone(Seq),
    /// Migration destination: the source sealed the migration at its position `last`.
    Seal { mig: u64, last: Seq },
}

pub struct MemberStream {
    partition: u16,
    received: Backlog,
    /// Last group seq handed out for apply.
    applied: Seq,
    /// Contiguous durable prefix (as noted by the caller).
    durable: Seq,
    last_ack_ns: u64,
    /// `(durable, applied)` of the last ack sent.
    acked: (Seq, Seq),
    term: u32,
    mode: Option<AcceptMode>,
    rejected: Option<RejectReason>,
    /// Stamp of snapshot records (full sync).
    snap_seq: Seq,
    commit: Seq,
    queued: VecDeque<MemberAction>,
    parts: Reassembly,
}

impl MemberStream {
    pub fn new(partition: u16, backlog_bytes: usize) -> Self {
        Self::with_backlog(partition, Backlog::new(backlog_bytes))
    }

    /// A stream over an already allocated received-groups ring.
    pub fn with_backlog(partition: u16, received: Backlog) -> Self {
        MemberStream {
            partition,
            received,
            applied: Seq::ZERO,
            durable: Seq::ZERO,
            last_ack_ns: 0,
            acked: (Seq::ZERO, Seq::ZERO),
            term: 0,
            mode: None,
            rejected: None,
            snap_seq: Seq::ZERO,
            commit: Seq::ZERO,
            queued: VecDeque::new(),
            parts: Reassembly::new(),
        }
    }

    pub fn partition(&self) -> u16 {
        self.partition
    }

    /// Starts a new stream (a new handshake) keeping the received-groups ring: `applied` and
    /// `durable` are the member's positions (its `Hello`).
    pub fn restart(&mut self, applied: Seq, durable: Seq) {
        self.applied = applied;
        self.durable = durable;
        self.acked = (Seq::ZERO, Seq::ZERO);
        self.last_ack_ns = 0;
        self.term = 0;
        self.mode = None;
        self.rejected = None;
        self.snap_seq = Seq::ZERO;
        self.commit = Seq::ZERO;
        self.queued.clear();
        self.parts = Reassembly::new();
    }

    /// The accepted term and mode (`None` before `Accept`).
    pub fn accepted(&self) -> Option<(u32, AcceptMode)> {
        self.mode.map(|m| (self.term, m))
    }

    pub fn rejected(&self) -> Option<RejectReason> {
        self.rejected
    }

    pub fn applied(&self) -> Seq {
        self.applied
    }

    pub fn durable(&self) -> Seq {
        self.durable
    }

    /// Last commit point the primary announced.
    pub fn commit(&self) -> Seq {
        self.commit
    }

    /// Handles one frame; further actions it produced are returned by
    /// [`MemberStream::next_action`].
    pub fn on_frame(&mut self, f: Frame) -> Result<MemberAction, FrameError> {
        self.handle(f)?;
        Ok(self.queued.pop_front().unwrap_or(MemberAction::Nothing))
    }

    /// The next queued action of the last frame.
    pub fn next_action(&mut self) -> Option<MemberAction> {
        self.queued.pop_front()
    }

    fn push(&mut self, a: MemberAction) {
        self.queued.push_back(a);
    }

    fn handle(&mut self, f: Frame) -> Result<(), FrameError> {
        match f {
            Frame::Accept { term, mode } => {
                if self.mode.is_some() {
                    return Err(FrameError::Malformed);
                }
                self.term = term;
                self.mode = Some(mode);
                match mode {
                    AcceptMode::Continue { from } => {
                        // groups from `from` follow; the member applied everything before it
                        self.applied = Seq(from.0.saturating_sub(1));
                    }
                    AcceptMode::Repair { at } => {
                        // groups after `at` follow (interleaved with the images)
                        self.applied = at;
                        self.push(MemberAction::SendRepairKeys);
                    }
                    AcceptMode::Full { at } => {
                        self.applied = at;
                        self.snap_seq = at;
                        self.received.clear();
                        self.received.start_after(at);
                        self.push(MemberAction::SyncBegin(at));
                    }
                }
            }
            Frame::Reject(r) => self.rejected = Some(r),
            _ if self.mode.is_none() => return Err(FrameError::Malformed),
            Frame::Groups(b) => {
                for (seq, g) in codec::iter_groups(&b) {
                    if seq <= self.applied {
                        continue;
                    }
                    let (group, _) = codec::decode_group(g).map_err(|_| FrameError::Malformed)?;
                    self.received.push(seq, g);
                    self.applied = seq;
                    if matches!(self.mode, Some(AcceptMode::Full { .. }))
                        && group.ops.iter().any(|o| matches!(o, Op::FlushDb { .. }))
                    {
                        self.snap_seq = self.snap_seq.max(seq);
                    }
                    self.push(MemberAction::Apply(group));
                }
            }
            Frame::Commit(s) | Frame::Heartbeat(s) => {
                if s > self.commit {
                    self.commit = s;
                    self.push(MemberAction::Commit(s));
                }
            }
            Frame::SnapBegin { slot } => self.push(MemberAction::SnapBegin(slot)),
            Frame::SnapRec(r) => {
                if !matches!(self.mode, Some(AcceptMode::Full { .. })) {
                    return Err(FrameError::Malformed);
                }
                let s = self.snap_seq;
                self.push(MemberAction::Snap(r, s));
            }
            Frame::SnapEnd { slot, count } => self.push(MemberAction::SnapEnd { slot, count }),
            Frame::SyncDone { at } => match self.mode {
                Some(AcceptMode::Repair { at: a }) => {
                    self.applied = self.applied.max(a);
                    self.push(MemberAction::RepairDone(a));
                }
                Some(AcceptMode::Full { .. }) => {
                    self.applied = self.applied.max(at);
                    self.push(MemberAction::SyncDone(at));
                }
                _ => return Err(FrameError::Malformed),
            },
            Frame::Image(i) => match self.mode {
                Some(AcceptMode::Repair { at }) => self.push(MemberAction::Image(i, at)),
                _ => return Err(FrameError::Malformed),
            },
            Frame::PutPart { offset, total, data, .. } => {
                if let Some((p, _term, inner)) = self.parts.push(offset, total, &data)? {
                    if p != self.partition {
                        return Err(FrameError::Malformed);
                    }
                    self.handle(inner)?;
                }
            }
            Frame::Seal { mig, last } => self.push(MemberAction::Seal { mig, last }),
            Frame::Hello(_) | Frame::Ack { .. } | Frame::RepairKeys(_) => return Err(FrameError::Malformed),
        }
        Ok(())
    }

    pub fn note_durable(&mut self, s: Seq) {
        self.durable = self.durable.max(s);
    }

    pub fn ack_due(&mut self, now_ns: u64) -> Option<Frame> {
        let cur = (self.durable, self.applied);
        if cur == self.acked || now_ns.saturating_sub(self.last_ack_ns) < 1_000_000 {
            return None;
        }
        self.acked = cur;
        self.last_ack_ns = now_ns;
        Some(Frame::Ack { durable: cur.0, applied: cur.1 })
    }

    pub fn received(&self) -> &Backlog {
        &self.received
    }

    /// After the member computed its `REPAIR_KEYS` from the ring: the ring restarts after `at`
    /// (groups at or below `at` are now represented by images, not by groups).
    pub fn reset_received(&mut self, at: Seq) {
        self.received.clear();
        self.received.start_after(at);
    }

    /// Takes the received-groups ring (a member promoted to primary keeps it for Repair key
    /// sets of members of the term it took over, §23.7 (b)).
    pub fn into_received(self) -> Backlog {
        self.received
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::encode_group;

    fn groups(seqs: &[Seq]) -> Frame {
        let mut b = Vec::new();
        for &s in seqs {
            encode_group(&Group { seq: s, ops: vec![Op::Del { db: 0, key: b"k".as_slice().into() }] }, &mut b);
        }
        Frame::Groups(b)
    }

    fn drain(m: &mut MemberStream, first: MemberAction) -> Vec<MemberAction> {
        let mut v = vec![first];
        while let Some(a) = m.next_action() {
            v.push(a);
        }
        v
    }

    #[test]
    fn continue_applies_groups_in_order_and_acks_durable_prefix() {
        let mut m = MemberStream::new(4, 1 << 16);
        assert!(m.on_frame(groups(&[Seq::new(1, 1)])).is_err(), "groups before accept");
        m.restart(Seq::new(1, 2), Seq::new(1, 2));
        assert!(matches!(m.on_frame(Frame::Accept { term: 1, mode: AcceptMode::Continue { from: Seq::new(1, 3) } }).unwrap(), MemberAction::Nothing));
        let a = m.on_frame(groups(&[Seq::new(1, 2), Seq::new(1, 3), Seq::new(1, 4)])).unwrap();
        let acts = drain(&mut m, a);
        let seqs: Vec<Seq> = acts.iter().filter_map(|a| if let MemberAction::Apply(g) = a { Some(g.seq) } else { None }).collect();
        assert_eq!(seqs, vec![Seq::new(1, 3), Seq::new(1, 4)], "an already applied group is skipped");
        assert_eq!(m.applied(), Seq::new(1, 4));
        assert_eq!(m.received().last(), Some(Seq::new(1, 4)));
        m.note_durable(Seq::new(1, 3));
        let Some(Frame::Ack { durable, applied }) = m.ack_due(10_000_000) else { panic!("ack due") };
        assert_eq!((durable, applied), (Seq::new(1, 3), Seq::new(1, 4)));
        assert!(m.ack_due(10_500_000).is_none(), "nothing changed");
        m.note_durable(Seq::new(1, 4));
        assert!(m.ack_due(10_500_000).is_none(), "coalesced: at most one per ms");
        assert!(m.ack_due(11_100_000).is_some());
        assert!(matches!(m.on_frame(Frame::Commit(Seq::new(1, 4))).unwrap(), MemberAction::Commit(_)));
        assert!(matches!(m.on_frame(Frame::Heartbeat(Seq::new(1, 4))).unwrap(), MemberAction::Nothing));
    }

    #[test]
    fn repair_and_full_sync_shapes() {
        let mut m = MemberStream::new(0, 1 << 16);
        assert!(matches!(m.on_frame(Frame::Accept { term: 3, mode: AcceptMode::Repair { at: Seq::new(3, 9) } }).unwrap(), MemberAction::SendRepairKeys));
        assert_eq!(m.applied(), Seq::new(3, 9));
        let img = ImageRec { db: 0, key: b"a".as_slice().into(), image: None };
        assert!(matches!(m.on_frame(Frame::Image(img)).unwrap(), MemberAction::Image(_, s) if s == Seq::new(3, 9)));
        assert!(matches!(m.on_frame(Frame::SyncDone { at: Seq::new(3, 9) }).unwrap(), MemberAction::RepairDone(_)));
        assert!(m.on_frame(Frame::SnapRec(SnapRec { db: 0, vtype: 1, expire_at: 0, key: b"x".as_slice().into(), value: vec![] })).is_err());

        let mut m = MemberStream::new(0, 1 << 16);
        assert!(matches!(m.on_frame(Frame::Accept { term: 2, mode: AcceptMode::Full { at: Seq::new(2, 5) } }).unwrap(), MemberAction::SyncBegin(_)));
        assert!(matches!(m.on_frame(Frame::SnapBegin { slot: 7 }).unwrap(), MemberAction::SnapBegin(7)));
        let rec = || SnapRec { db: 0, vtype: 1, expire_at: 0, key: b"x".as_slice().into(), value: b"v".to_vec() };
        assert!(matches!(m.on_frame(Frame::SnapRec(rec())).unwrap(), MemberAction::Snap(_, s) if s == Seq::new(2, 5)));
        // a flush during the walk raises the stamp of later records
        let mut b = Vec::new();
        encode_group(&Group { seq: Seq::new(2, 8), ops: vec![Op::FlushDb { db: None }] }, &mut b);
        assert!(matches!(m.on_frame(Frame::Groups(b)).unwrap(), MemberAction::Apply(_)));
        assert!(matches!(m.on_frame(Frame::SnapRec(rec())).unwrap(), MemberAction::Snap(_, s) if s == Seq::new(2, 8)));
        assert!(matches!(m.on_frame(Frame::SnapEnd { slot: 7, count: 2 }).unwrap(), MemberAction::SnapEnd { slot: 7, count: 2 }));
        assert!(matches!(m.on_frame(Frame::SyncDone { at: Seq::new(2, 9) }).unwrap(), MemberAction::SyncDone(s) if s == Seq::new(2, 9)));
        assert_eq!(m.applied(), Seq::new(2, 9));
        assert!(matches!(m.on_frame(Frame::Seal { mig: 5, last: Seq::new(2, 9) }).unwrap(), MemberAction::Seal { mig: 5, .. }));
        // the ring restarted after `at`: it holds only the group received since
        assert_eq!(m.received().first(), Some(Seq::new(2, 8)));
        assert!(!m.received().covers_after(Seq::new(2, 4)));
    }

    #[test]
    fn rejects_are_recorded() {
        let mut m = MemberStream::new(0, 4096);
        m.on_frame(Frame::Reject(RejectReason::NeedOperatorResync)).unwrap();
        assert_eq!(m.rejected(), Some(RejectReason::NeedOperatorResync));
        assert!(m.accepted().is_none());
    }
}
