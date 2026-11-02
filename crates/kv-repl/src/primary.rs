//! Primary side of a partition stream (DESIGN §23.7, §23.8).
//!
//! `ReplSender` receives every encoded group the partition produces (`on_group`), keeps it in
//! the backlog and fills per-member send buffers (frames of the current term only).
//! `commit_seq = min(local_durable, min over ISR members m != primary of acked_durable(m))`;
//! replies are released when the command's ticket satisfies the durability mode, `commit_seq >=
//! seq` and the lease is valid. A member whose oldest unacknowledged group is older than
//! `repl-ack-timeout-ms` is reported by `lagging` (an age test, failure detection), which leads
//! to an `IsrShrink` request to the coordinator; the primary keeps waiting for that member until
//! the committed assignment drops it (§23.8).
//!
//! Members are attached when their handshake completes: `Continue { from }` and
//! `Repair { at }` stream groups from the backlog (from `from`, respectively `at + 1`, after the
//! repair images); `Full { at }` attaches the member without streaming (the full-sync walk
//! feeds it) until [`ReplSender::resume`] starts the backlog stream after the walk.

use std::collections::VecDeque;

use smallvec::SmallVec;

use crate::Seq;
use crate::backlog::Backlog;
use crate::frame::{self, AcceptMode, Frame};

/// Index of a member within the shard's member list.
pub type MemberId = u16;

/// Bytes of encoded groups per `GROUPS` frame (one group may exceed it; it then travels alone).
pub const GROUPS_FRAME_BYTES: usize = 256 << 10;

/// Per-member stream state.
struct MemberSlot {
    id: MemberId,
    /// A stream to the member is open.
    attached: bool,
    /// Groups flow from the backlog (false during a full-sync walk).
    streaming: bool,
    /// Last group seq sent.
    sent: Seq,
    acked_durable: Seq,
    /// Last commit point told to the member.
    commit_sent: Seq,
    /// Boot-clock time of the last frame sent.
    last_send_ns: u64,
    /// The backlog dropped a group this member still needed: its stream must restart.
    overrun: bool,
}

impl MemberSlot {
    fn new(id: MemberId) -> Self {
        MemberSlot {
            id,
            attached: false,
            streaming: false,
            sent: Seq::ZERO,
            acked_durable: Seq::ZERO,
            commit_sent: Seq::ZERO,
            last_send_ns: 0,
            overrun: false,
        }
    }
}

pub struct ReplSender {
    partition: u16,
    term: u32,
    backlog: Backlog,
    members: Vec<MemberSlot>,
    /// ISR members (without the primary) whose acks count toward the commit point.
    isr: SmallVec<[MemberId; 4]>,
    local_durable: Seq,
    /// Last group pushed (`Seq::new(term, 0)` before the first).
    last: Seq,
    /// `(seq, boot ns)` of groups not yet acknowledged by every counted member (the age test of
    /// `lagging`).
    produced: VecDeque<(Seq, u64)>,
    heartbeat_ns: u64,
}

impl ReplSender {
    pub fn new(partition: u16, term: u32, backlog_bytes: usize) -> Self {
        Self::with_backlog(partition, term, Backlog::new(backlog_bytes))
    }

    /// A sender over an already allocated backlog (allocation failures are reported by
    /// [`Backlog::try_new`]).
    pub fn with_backlog(partition: u16, term: u32, mut backlog: Backlog) -> Self {
        let start = Seq::new(term, 0);
        backlog.start_after(start);
        ReplSender {
            partition,
            term,
            backlog,
            members: Vec::new(),
            isr: SmallVec::new(),
            local_durable: start,
            last: start,
            produced: VecDeque::new(),
            heartbeat_ns: 10_000_000,
        }
    }

    pub fn partition(&self) -> u16 {
        self.partition
    }

    pub fn term(&self) -> u32 {
        self.term
    }

    /// Idle time after which a `HEARTBEAT` is due (`repl-heartbeat-ms`).
    pub fn set_heartbeat_ns(&mut self, ns: u64) {
        self.heartbeat_ns = ns.max(1_000_000);
    }

    pub fn backlog(&self) -> &Backlog {
        &self.backlog
    }

    /// Last group seq produced.
    pub fn last(&self) -> Seq {
        self.last
    }

    fn slot(&self, m: MemberId) -> Option<&MemberSlot> {
        self.members.iter().find(|s| s.id == m)
    }

    fn slot_mut(&mut self, m: MemberId) -> &mut MemberSlot {
        match self.members.iter().position(|s| s.id == m) {
            Some(i) => &mut self.members[i],
            None => {
                self.members.push(MemberSlot::new(m));
                self.members.last_mut().expect("just pushed")
            }
        }
    }

    pub fn on_group(&mut self, encoded: &[u8], seq: Seq) {
        self.on_group_at(encoded, seq, kv_common::clock::boot_ns());
    }

    /// [`ReplSender::on_group`] with the caller's clock (boot ns).
    pub fn on_group_at(&mut self, encoded: &[u8], seq: Seq, now_ns: u64) {
        if seq <= self.last {
            // replayed or out of order: never shipped twice
            return;
        }
        self.backlog.push(seq, encoded);
        self.last = seq;
        if !self.isr.is_empty() {
            self.produced.push_back((seq, now_ns));
        }
    }

    pub fn attach(&mut self, m: MemberId, mode: &AcceptMode) {
        let s = self.slot_mut(m);
        s.attached = true;
        s.overrun = false;
        s.commit_sent = Seq::ZERO;
        match *mode {
            AcceptMode::Continue { from } => {
                let before = Seq(from.0.saturating_sub(1));
                s.streaming = true;
                s.sent = before;
                // the member's handshake position is durable there (a persisted lower bound)
                s.acked_durable = s.acked_durable.max(before);
            }
            AcceptMode::Repair { at } => {
                // the images bring the member to `at`; groups follow from `at + 1`
                s.streaming = true;
                s.sent = at;
            }
            AcceptMode::Full { at } => {
                s.streaming = false;
                s.sent = at;
            }
        }
    }

    /// Starts streaming backlog groups after `after` to a member attached with `Full` (the
    /// walk is complete).
    pub fn resume(&mut self, m: MemberId, after: Seq) {
        let s = self.slot_mut(m);
        s.streaming = true;
        s.sent = after;
    }

    pub fn detach(&mut self, m: MemberId) {
        let counted = self.isr.contains(&m);
        if counted {
            // an ISR member keeps its acknowledged position until the ISR drops it
            let s = self.slot_mut(m);
            s.attached = false;
            s.streaming = false;
        } else {
            self.members.retain(|s| s.id != m);
        }
    }

    /// Members whose acks count toward `commit_seq` (the committed ISR minus the primary).
    pub fn set_isr(&mut self, counted: &[MemberId]) {
        self.isr = counted.iter().copied().collect();
        for &m in counted {
            self.slot_mut(m);
        }
        let isr = self.isr.clone();
        self.members.retain(|s| s.attached || isr.contains(&s.id));
        self.trim_produced();
    }

    /// The counted members.
    pub fn isr(&self) -> &[MemberId] {
        &self.isr
    }

    pub fn on_ack(&mut self, m: MemberId, durable: Seq, now_ns: u64) {
        let _ = now_ns;
        let durable = durable.min(self.last);
        let s = self.slot_mut(m);
        s.acked_durable = s.acked_durable.max(durable);
        self.trim_produced();
    }

    fn trim_produced(&mut self) {
        let min = self.isr.iter().map(|&m| self.slot(m).map_or(Seq::ZERO, |s| s.acked_durable)).min();
        match min {
            None => self.produced.clear(),
            Some(min) => {
                while self.produced.front().is_some_and(|&(s, _)| s <= min) {
                    self.produced.pop_front();
                }
            }
        }
    }

    pub fn set_local_durable(&mut self, s: Seq) {
        self.local_durable = self.local_durable.max(s.min(self.last));
    }

    pub fn local_durable(&self) -> Seq {
        self.local_durable
    }

    /// The smallest acknowledged durable position over the counted members (`None`: no member
    /// counts).
    pub fn members_durable(&self) -> Option<Seq> {
        self.isr.iter().map(|&m| self.slot(m).map_or(Seq::ZERO, |s| s.acked_durable)).min()
    }

    pub fn commit_seq(&self) -> Seq {
        match self.members_durable() {
            None => self.local_durable,
            Some(m) => self.local_durable.min(m),
        }
    }

    /// The member's acknowledged durable position.
    pub fn acked(&self, m: MemberId) -> Option<Seq> {
        self.slot(m).map(|s| s.acked_durable)
    }

    pub fn is_attached(&self, m: MemberId) -> bool {
        self.slot(m).is_some_and(|s| s.attached)
    }

    /// The member streams from the backlog and has acknowledged everything up to the commit
    /// point (§23.8 step 5).
    pub fn caught_up(&self, m: MemberId) -> bool {
        self.slot(m).is_some_and(|s| s.attached && s.streaming && s.acked_durable >= self.commit_seq())
    }

    /// The backlog no longer holds a group the member still needs: its stream must restart.
    pub fn overrun(&self, m: MemberId) -> bool {
        self.slot(m).is_some_and(|s| s.overrun)
    }

    /// frames due for m
    pub fn fill_send(&mut self, m: MemberId, out: &mut Vec<u8>, max: usize) -> bool {
        self.fill_send_at(m, out, max, kv_common::clock::boot_ns())
    }

    /// [`ReplSender::fill_send`] with the caller's clock: groups after the member's last sent
    /// seq (up to about `max` bytes, at least one group), then `COMMIT` when the commit point
    /// moved, or a `HEARTBEAT` after `repl-heartbeat-ms` without any frame. Returns whether
    /// anything was appended.
    pub fn fill_send_at(&mut self, m: MemberId, out: &mut Vec<u8>, max: usize, now_ns: u64) -> bool {
        let commit = self.commit_seq();
        let (partition, term, last, heartbeat_ns) = (self.partition, self.term, self.last, self.heartbeat_ns);
        let Some(i) = self.members.iter().position(|s| s.id == m) else { return false };
        if !self.members[i].attached || self.members[i].overrun {
            return false;
        }
        let start = out.len();
        if self.members[i].streaming && self.members[i].sent < last {
            let sent = self.members[i].sent;
            if !self.backlog.covers_after(sent) {
                self.members[i].overrun = true;
                return false;
            }
            let mut groups = Vec::new();
            let mut budget = max.max(1);
            let mut at = sent;
            while budget > 0 && at < last {
                groups.clear();
                let Some(l) = self.backlog.read_after(at, budget.min(GROUPS_FRAME_BYTES), &mut groups) else { break };
                budget = budget.saturating_sub(groups.len());
                frame::encode(&Frame::Groups(std::mem::take(&mut groups)), partition, term, out);
                at = l;
            }
            self.members[i].sent = at;
        }
        let s = &mut self.members[i];
        if s.streaming && commit > s.commit_sent {
            frame::encode(&Frame::Commit(commit), partition, term, out);
            s.commit_sent = commit;
        } else if out.len() == start && now_ns.saturating_sub(s.last_send_ns) >= heartbeat_ns {
            frame::encode(&Frame::Heartbeat(if s.streaming { commit } else { s.commit_sent }), partition, term, out);
        }
        if out.len() > start {
            s.last_send_ns = now_ns;
            true
        } else {
            false
        }
    }

    /// Members whose oldest unacknowledged group is older than `timeout_ns`.
    pub fn lagging(&self, now_ns: u64, timeout_ns: u64) -> SmallVec<[MemberId; 2]> {
        let mut out = SmallVec::new();
        for &m in &self.isr {
            let acked = self.slot(m).map_or(Seq::ZERO, |s| s.acked_durable);
            // the produced list is in seq order: the first entry above `acked` is the oldest
            // group this member has not acknowledged
            let i = self.produced.partition_point(|&(s, _)| s <= acked);
            if let Some(&(_, t)) = self.produced.get(i)
                && now_ns.saturating_sub(t) > timeout_ns
            {
                out.push(m);
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::encode_group;
    use crate::frame::FrameDecoder;
    use crate::{Group, Op};

    fn enc(seq: Seq) -> Vec<u8> {
        let mut b = Vec::new();
        encode_group(&Group { seq, ops: vec![Op::Del { db: 0, key: b"k".as_slice().into() }] }, &mut b);
        b
    }

    fn frames(b: &[u8]) -> Vec<Frame> {
        let mut d = FrameDecoder::new();
        let mut at = 0;
        let mut out = Vec::new();
        while at < b.len() {
            let (_, _, f, n) = d.feed(&b[at..]).unwrap().unwrap();
            at += n;
            out.push(f);
        }
        out
    }

    #[test]
    fn commit_is_the_minimum_of_local_and_isr_acks() {
        let mut r = ReplSender::new(3, 2, 1 << 16);
        assert_eq!(r.commit_seq(), Seq::new(2, 0));
        for i in 1..=10 {
            r.on_group_at(&enc(Seq::new(2, i)), Seq::new(2, i), i * 1000);
        }
        r.set_local_durable(Seq::new(2, 8));
        assert_eq!(r.commit_seq(), Seq::new(2, 8), "no ISR member: local durability alone");
        r.set_isr(&[1, 2]);
        assert_eq!(r.commit_seq(), Seq::ZERO, "ISR members have acknowledged nothing in this term");
        r.attach(1, &AcceptMode::Continue { from: Seq::new(2, 1) });
        r.attach(2, &AcceptMode::Continue { from: Seq::new(2, 1) });
        r.on_ack(1, Seq::new(2, 9), 0);
        r.on_ack(2, Seq::new(2, 5), 0);
        assert_eq!(r.commit_seq(), Seq::new(2, 5));
        r.on_ack(2, Seq::new(2, 10), 0);
        assert_eq!(r.commit_seq(), Seq::new(2, 8), "bounded by local durability");
        // a detached ISR member keeps its position; a removed one stops counting
        r.detach(1);
        assert_eq!(r.commit_seq(), Seq::new(2, 8));
        r.set_local_durable(Seq::new(2, 10));
        assert_eq!(r.commit_seq(), Seq::new(2, 9));
        r.set_isr(&[2]);
        assert_eq!(r.commit_seq(), Seq::new(2, 10));
    }

    #[test]
    fn streams_groups_then_commit_and_heartbeats() {
        let mut r = ReplSender::new(0, 1, 1 << 16);
        r.set_heartbeat_ns(5_000_000);
        for i in 1..=5 {
            r.on_group_at(&enc(Seq::new(1, i)), Seq::new(1, i), 0);
        }
        r.attach(7, &AcceptMode::Continue { from: Seq::new(1, 3) });
        r.set_local_durable(Seq::new(1, 4));
        let mut out = Vec::new();
        assert!(r.fill_send_at(7, &mut out, 1 << 20, 100));
        let fs = frames(&out);
        let Frame::Groups(g) = &fs[0] else { panic!("groups first") };
        let seqs: Vec<Seq> = crate::codec::iter_groups(g).map(|(s, _)| s).collect();
        assert_eq!(seqs, vec![Seq::new(1, 3), Seq::new(1, 4), Seq::new(1, 5)]);
        assert!(matches!(fs[1], Frame::Commit(s) if s == Seq::new(1, 4)));
        out.clear();
        assert!(!r.fill_send_at(7, &mut out, 1 << 20, 200), "nothing new");
        assert!(r.fill_send_at(7, &mut out, 1 << 20, 10_000_000));
        assert!(matches!(frames(&out)[0], Frame::Heartbeat(_)));
        // a full-sync member gets nothing from the backlog until resumed
        r.attach(8, &AcceptMode::Full { at: Seq::new(1, 5) });
        r.on_group_at(&enc(Seq::new(1, 6)), Seq::new(1, 6), 0);
        out.clear();
        r.fill_send_at(8, &mut out, 1 << 20, 0);
        assert!(frames(&out).iter().all(|f| !matches!(f, Frame::Groups(_))));
        r.resume(8, Seq::new(1, 5));
        out.clear();
        r.fill_send_at(8, &mut out, 1 << 20, 0);
        assert!(matches!(&frames(&out)[0], Frame::Groups(g) if crate::codec::peek_seq(g) == Some(Seq::new(1, 6))));
    }

    #[test]
    fn lagging_is_an_age_test_and_overrun_restarts_the_stream() {
        let mut r = ReplSender::new(0, 1, 512);
        r.set_isr(&[1]);
        r.attach(1, &AcceptMode::Continue { from: Seq::new(1, 1) });
        r.on_group_at(&enc(Seq::new(1, 1)), Seq::new(1, 1), 1_000);
        r.on_group_at(&enc(Seq::new(1, 2)), Seq::new(1, 2), 2_000);
        assert!(r.lagging(100_000, 250_000).is_empty());
        assert_eq!(r.lagging(300_000, 250_000).as_slice(), &[1]);
        r.on_ack(1, Seq::new(1, 1), 0);
        assert!(r.lagging(251_500, 250_000).is_empty(), "the oldest unacknowledged group is now (1, 2)");
        r.on_ack(1, Seq::new(1, 2), 0);
        assert!(r.lagging(u64::MAX, 250_000).is_empty());
        // a tiny backlog: a member that falls behind it is told to restart
        r.attach(2, &AcceptMode::Continue { from: Seq::new(1, 1) });
        for i in 3..=40 {
            r.on_group_at(&enc(Seq::new(1, i)), Seq::new(1, i), 0);
        }
        assert!(!r.fill_send_at(2, &mut Vec::new(), 1 << 20, 0));
        assert!(r.overrun(2));
    }
}
