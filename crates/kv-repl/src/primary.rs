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
//! The age test judges the member, never the primary: a group's age runs from when the
//! primary's own durability covered it (a member that acknowledged everything the primary has
//! made durable is not what holds the commit back), and the caller's clock is a
//! [`StallClock`], which leaves out the time the primary itself did not run (a stopped or
//! descheduled process, a receive path that did not deliver the acks waiting for it). A
//! primary that stalls for longer than the timeout therefore never reports its healthy
//! members on waking up, which would drop them from the ISR and leave no promotable member if
//! it then crashed.
//!
//! ISR expand (§23.8 step 5) has two position tests (LC-36: positions recorded in the past,
//! never a lag compared with a constant):
//! 1. [`ReplSender::caught_up`]: the member acknowledged a fixed chain of barriers, the commit
//!    point when its backlog stream started, then the commit point when it reached that one,
//!    and so on, [`EXPAND_BARRIERS`] times; the caller then starts counting its acks. An
//!    instantaneous `acked >= commit` test is never met by a member that stays a few
//!    milliseconds behind under a steady write load, so it would never rejoin.
//! 2. [`ReplSender::holds_committed`]: once counted, the member acknowledged the commit point
//!    recorded when counting started. From then on it holds every write ever committed (the
//!    later ones needed its ack), so only then may the caller ask for its `IsrExpand`.
//!
//! Members are attached when their handshake completes: `Continue { from }` streams groups from
//! the backlog from `from`. `Repair { at }` and `Full { at }` attach the member without backlog
//! streaming: during the repair images or the full-sync walk the caller writes every frame in
//! the partition's execution order (images or snapshot records as they are read, groups as they
//! are executed), so that nothing a later frame already contains is applied twice;
//! [`ReplSender::resume`] then starts the backlog stream after the position the direct frames
//! reached.

use std::collections::VecDeque;

use smallvec::SmallVec;

use crate::Seq;
use crate::backlog::Backlog;
use crate::frame::{self, AcceptMode, Frame};

/// Index of a member within the shard's member list.
pub type MemberId = u16;

/// Bytes of encoded groups per `GROUPS` frame (one group may exceed it; it then travels alone).
pub const GROUPS_FRAME_BYTES: usize = 256 << 10;

/// Position barriers a member outside the ISR acknowledges after the first one before it is
/// caught up (a fixed number, LC-36).
pub const EXPAND_BARRIERS: u8 = 2;

/// Largest step a [`StallClock`] takes between two observations.
pub const STALL_STEP_NS: u64 = 50_000_000;

/// A clock for the age test of [`ReplSender::lagging`] that leaves out the time the caller did
/// not run. The caller observes it once per tick with the boot clock; a gap between two
/// observations counts at most [`STALL_STEP_NS`] (a primary ticks every few milliseconds while
/// it has members, so a longer gap is a stopped or descheduled process), and nothing while
/// `held` (the receive path that delivers the members' acks has not run recently, so a missing
/// ack is no evidence against the member). Its readings start at the first observation's boot
/// time and never go backwards.
#[derive(Clone, Copy, Debug, Default)]
pub struct StallClock {
    run_ns: u64,
    real_ns: u64,
    started: bool,
}

impl StallClock {
    pub const fn new() -> Self {
        StallClock {
            run_ns: 0,
            real_ns: 0,
            started: false,
        }
    }

    /// One observation at boot time `real_ns`; returns the clock's reading.
    pub fn advance(&mut self, real_ns: u64, held: bool) -> u64 {
        if !self.started {
            self.started = true;
            self.real_ns = real_ns;
            self.run_ns = real_ns;
            return real_ns;
        }
        let gap = real_ns.saturating_sub(self.real_ns);
        self.real_ns = self.real_ns.max(real_ns);
        if !held {
            self.run_ns = self.run_ns.saturating_add(gap.min(STALL_STEP_NS));
        }
        self.run_ns
    }

    /// The reading an observation at `real_ns` would give if the receive path is running (for
    /// stamps taken between observations; never below the last reading).
    pub fn peek(&self, real_ns: u64) -> u64 {
        if !self.started {
            return real_ns;
        }
        self.run_ns
            .saturating_add(real_ns.saturating_sub(self.real_ns).min(STALL_STEP_NS))
    }
}

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
    /// The catch-up barrier chain of a streaming member: `(position, barriers left after it)`.
    barrier: Option<(Seq, u8)>,
    /// While counted: the commit point when counting started (every write committed before it
    /// did not need this member's ack).
    counted_from: Option<Seq>,
    /// While counted: boot-clock time counting started (the age of groups produced while
    /// nobody was counted runs from here, see `lagging`).
    counted_since_ns: u64,
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
            barrier: None,
            counted_from: None,
            counted_since_ns: 0,
        }
    }

    /// Moves the barrier chain on while the member's acknowledged position covers it; every
    /// next barrier is the commit point at the moment the previous one was reached.
    fn advance_barrier(&mut self, commit: Seq) {
        while let Some((b, left)) = self.barrier {
            if self.acked_durable < b || left == 0 {
                break;
            }
            self.barrier = Some((commit.max(b), left - 1));
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
    /// `(seq, produced at, covered by the primary's own durability at (0: not yet))` of groups
    /// not yet acknowledged by every counted member, in the caller's [`StallClock`] time (the
    /// age test of `lagging`). The entries covered by local durability are a prefix.
    produced: VecDeque<(Seq, u64, u64)>,
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

    /// The partition already executed groups of this term up to `s` before this sender
    /// existed (a primary resuming its own recovered state): the stream and the backlog start
    /// after `s`, so no member is ever continued over groups the backlog never held.
    pub fn start_at(&mut self, s: Seq) {
        if s > self.last {
            self.last = s;
            self.local_durable = self.local_durable.max(s);
            self.backlog.start_after(s);
        }
    }

    /// The backlog of this term's groups (kept by a demoted primary for its Repair key set).
    pub fn into_backlog(self) -> Backlog {
        self.backlog
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
            self.produced.push_back((seq, now_ns, 0));
        }
    }

    pub fn attach(&mut self, m: MemberId, mode: &AcceptMode) {
        let commit = self.commit_seq();
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
                s.barrier = Some((commit, EXPAND_BARRIERS));
                s.advance_barrier(commit);
            }
            AcceptMode::Repair { at } | AcceptMode::Full { at } => {
                // fed directly by the caller until `resume`
                s.streaming = false;
                s.sent = at;
                s.barrier = None;
            }
        }
    }

    /// Starts streaming backlog groups after `after` to a member attached with `Full` (the
    /// walk is complete).
    pub fn resume(&mut self, m: MemberId, after: Seq) {
        let commit = self.commit_seq();
        let s = self.slot_mut(m);
        s.streaming = true;
        s.sent = after;
        // its catch-up barriers start at the end of the direct feed
        s.barrier = Some((commit.max(after), EXPAND_BARRIERS));
        s.advance_barrier(commit);
    }

    pub fn detach(&mut self, m: MemberId) {
        let counted = self.isr.contains(&m);
        if counted {
            // an ISR member keeps its acknowledged position until the ISR drops it
            let s = self.slot_mut(m);
            s.attached = false;
            s.streaming = false;
            s.barrier = None;
        } else {
            self.members.retain(|s| s.id != m);
        }
    }

    /// Members whose acks count toward `commit_seq` (the committed ISR minus the primary).
    pub fn set_isr(&mut self, counted: &[MemberId]) {
        self.set_isr_at(counted, kv_common::clock::boot_ns());
    }

    /// [`ReplSender::set_isr`] with the caller's clock (boot ns).
    pub fn set_isr_at(&mut self, counted: &[MemberId], now_ns: u64) {
        let dropped: SmallVec<[MemberId; 4]> = self
            .isr
            .iter()
            .copied()
            .filter(|m| !counted.contains(m))
            .collect();
        let added: SmallVec<[MemberId; 4]> = counted
            .iter()
            .copied()
            .filter(|m| !self.isr.contains(m))
            .collect();
        // the commit point before the new members count: what they must hold before their
        // IsrExpand (see `holds_committed`)
        let before = self.commit_seq();
        self.isr = counted.iter().copied().collect();
        for &m in counted {
            self.slot_mut(m);
        }
        for m in added {
            let s = self.slot_mut(m);
            s.counted_from = Some(before);
            s.counted_since_ns = now_ns;
        }
        for &m in &dropped {
            if let Some(s) = self.members.iter_mut().find(|s| s.id == m) {
                s.counted_from = None;
                s.counted_since_ns = 0;
            }
        }
        // a member that stops counting (it lagged, or the ISR dropped it) catches up again
        // through a fresh chain of barriers before it may count again
        let commit = self.commit_seq();
        for m in dropped {
            if let Some(s) = self.members.iter_mut().find(|s| s.id == m)
                && s.attached
                && s.streaming
            {
                s.barrier = Some((commit, EXPAND_BARRIERS));
                s.advance_barrier(commit);
            }
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
        let commit = self.commit_seq();
        self.slot_mut(m).advance_barrier(commit);
        self.trim_produced();
    }

    fn trim_produced(&mut self) {
        let min = self
            .isr
            .iter()
            .map(|&m| self.slot(m).map_or(Seq::ZERO, |s| s.acked_durable))
            .min();
        match min {
            None => self.produced.clear(),
            Some(min) => {
                while self.produced.front().is_some_and(|&(s, _, _)| s <= min) {
                    self.produced.pop_front();
                }
            }
        }
    }

    pub fn set_local_durable(&mut self, s: Seq) {
        self.set_local_durable_at(s, kv_common::clock::boot_ns());
    }

    /// [`ReplSender::set_local_durable`] with the caller's clock (the [`StallClock`] reading
    /// of `lagging`): groups the primary's own durability covers from now on are timed from
    /// here.
    pub fn set_local_durable_at(&mut self, s: Seq, now_ns: u64) {
        self.local_durable = self.local_durable.max(s.min(self.last));
        let local = self.local_durable;
        let from = self.produced.partition_point(|&(_, _, d)| d != 0);
        for e in self.produced.iter_mut().skip(from) {
            if e.0 > local {
                break;
            }
            e.2 = now_ns.max(1);
        }
    }

    pub fn local_durable(&self) -> Seq {
        self.local_durable
    }

    /// The smallest acknowledged durable position over the counted members (`None`: no member
    /// counts).
    pub fn members_durable(&self) -> Option<Seq> {
        self.isr
            .iter()
            .map(|&m| self.slot(m).map_or(Seq::ZERO, |s| s.acked_durable))
            .min()
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

    /// The member streams from the backlog and has acknowledged its whole chain of catch-up
    /// barriers (§23.8 step 5; see the module notes).
    pub fn caught_up(&self, m: MemberId) -> bool {
        self.slot(m).is_some_and(|s| {
            s.attached
                && s.streaming
                && s.barrier
                    .is_some_and(|(b, left)| left == 0 && s.acked_durable >= b)
        })
    }

    /// The member counts toward the commit point and acknowledged the commit point recorded
    /// when it started to count: it holds every write ever committed, so it may join the
    /// committed ISR (`IsrExpand`).
    pub fn holds_committed(&self, m: MemberId) -> bool {
        self.isr.contains(&m)
            && self
                .slot(m)
                .is_some_and(|s| s.counted_from.is_some_and(|c| s.acked_durable >= c))
    }

    /// The member is attached and fed directly (repair images or a full-sync walk).
    pub fn direct(&self, m: MemberId) -> bool {
        self.slot(m).is_some_and(|s| s.attached && !s.streaming)
    }

    /// Members currently attached.
    pub fn attached(&self) -> SmallVec<[MemberId; 4]> {
        self.members
            .iter()
            .filter(|s| s.attached)
            .map(|s| s.id)
            .collect()
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
    /// anything was written.
    pub fn fill_send_at(
        &mut self,
        m: MemberId,
        out: &mut Vec<u8>,
        max: usize,
        now_ns: u64,
    ) -> bool {
        let commit = self.commit_seq();
        let (partition, term, last, heartbeat_ns) =
            (self.partition, self.term, self.last, self.heartbeat_ns);
        let Some(i) = self.members.iter().position(|s| s.id == m) else {
            return false;
        };
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
                let Some(l) =
                    self.backlog
                        .read_after(at, budget.min(GROUPS_FRAME_BYTES), &mut groups)
                else {
                    break;
                };
                budget = budget.saturating_sub(groups.len());
                frame::encode(
                    &Frame::Groups(std::mem::take(&mut groups)),
                    partition,
                    term,
                    out,
                );
                at = l;
            }
            self.members[i].sent = at;
        }
        let s = &mut self.members[i];
        if s.streaming && commit > s.commit_sent {
            frame::encode(&Frame::Commit(commit), partition, term, out);
            s.commit_sent = commit;
        } else if out.len() == start && now_ns.saturating_sub(s.last_send_ns) >= heartbeat_ns {
            frame::encode(
                &Frame::Heartbeat(if s.streaming { commit } else { s.commit_sent }),
                partition,
                term,
                out,
            );
        }
        if out.len() > start {
            s.last_send_ns = now_ns;
            true
        } else {
            false
        }
    }

    /// Counted members that hold the commit point back for longer than `timeout_ns`: a member
    /// is reported when the oldest group it has not acknowledged was covered by the primary's
    /// own durability more than `timeout_ns` ago (a member that acknowledged everything the
    /// primary has made durable is never reported). Groups produced while nobody was counted
    /// have no time of their own: for a member that started counting below them (its commit
    /// point then moved back to its acknowledged position) their age runs from when it started
    /// counting, so it is reported like any other. `now_ns` is the reading of the caller's
    /// [`StallClock`], the clock every stamp of this sender's age test was taken with.
    pub fn lagging(&self, now_ns: u64, timeout_ns: u64) -> SmallVec<[MemberId; 2]> {
        let mut out = SmallVec::new();
        for &m in &self.isr {
            let (acked, since) = self
                .slot(m)
                .map_or((Seq::ZERO, 0), |s| (s.acked_durable, s.counted_since_ns));
            if acked >= self.local_durable {
                continue;
            }
            // the produced list is in seq order: the first entry above `acked` is the oldest
            // group this member has not acknowledged that was produced while it counted; when
            // the primary's durability does not cover it yet, the groups it lacks below the
            // local durable position were produced alone (no entry: timed from `since`)
            let i = self.produced.partition_point(|&(s, _, _)| s <= acked);
            let t = match self.produced.get(i) {
                Some(&(_, _, d)) if d != 0 => d.max(since),
                _ => since,
            };
            if now_ns.saturating_sub(t) > timeout_ns {
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
        encode_group(
            &Group {
                seq,
                ops: vec![Op::Del {
                    db: 0,
                    key: b"k".as_slice().into(),
                }],
            },
            &mut b,
        );
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
        assert_eq!(
            r.commit_seq(),
            Seq::new(2, 8),
            "no ISR member: local durability alone"
        );
        r.set_isr(&[1, 2]);
        assert_eq!(
            r.commit_seq(),
            Seq::ZERO,
            "ISR members have acknowledged nothing in this term"
        );
        r.attach(
            1,
            &AcceptMode::Continue {
                from: Seq::new(2, 1),
            },
        );
        r.attach(
            2,
            &AcceptMode::Continue {
                from: Seq::new(2, 1),
            },
        );
        r.on_ack(1, Seq::new(2, 9), 0);
        r.on_ack(2, Seq::new(2, 5), 0);
        assert_eq!(r.commit_seq(), Seq::new(2, 5));
        r.on_ack(2, Seq::new(2, 10), 0);
        assert_eq!(
            r.commit_seq(),
            Seq::new(2, 8),
            "bounded by local durability"
        );
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
        r.attach(
            7,
            &AcceptMode::Continue {
                from: Seq::new(1, 3),
            },
        );
        r.set_local_durable(Seq::new(1, 4));
        let mut out = Vec::new();
        assert!(r.fill_send_at(7, &mut out, 1 << 20, 100));
        let fs = frames(&out);
        let Frame::Groups(g) = &fs[0] else {
            panic!("groups first")
        };
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
        assert!(
            matches!(&frames(&out)[0], Frame::Groups(g) if crate::codec::peek_seq(g) == Some(Seq::new(1, 6)))
        );
    }

    /// A member that stays a few groups behind under a steady write load is caught up once it
    /// acknowledged the fixed chain of barriers (positions recorded in the past); an
    /// instantaneous `acked >= commit` test would never hold for it.
    #[test]
    fn catch_up_is_a_fixed_chain_of_position_barriers() {
        let mut r = ReplSender::new(0, 1, 1 << 16);
        fn produce(r: &mut ReplSender, seq: &mut u64, n: u64) {
            for _ in 0..n {
                *seq += 1;
                r.on_group_at(&enc(Seq::new(1, *seq)), Seq::new(1, *seq), 0);
            }
            r.set_local_durable(Seq::new(1, *seq));
        }
        let mut seq = 0u64;
        produce(&mut r, &mut seq, 100);
        r.attach(
            5,
            &AcceptMode::Continue {
                from: Seq::new(1, 1),
            },
        );
        assert!(!r.caught_up(5));
        // the member acknowledges, always 3 groups behind the head, while writes continue
        let mut rounds = 0;
        while !r.caught_up(5) {
            rounds += 1;
            assert!(rounds < 10, "caught up after a fixed number of barriers");
            r.on_ack(5, Seq::new(1, seq - 3), 0);
            produce(&mut r, &mut seq, 5);
            assert!(r.acked(5).unwrap() < r.local_durable(), "always behind");
        }
        assert!(
            rounds <= usize::from(EXPAND_BARRIERS) + 2,
            "{rounds} rounds"
        );
        // counting starts: the member may join the committed ISR only once it acknowledged
        // the commit point of that moment (it misses no write committed before it counted)
        let before = r.commit_seq();
        r.set_isr(&[5]);
        assert!(!r.holds_committed(5) || r.acked(5).unwrap() >= before);
        produce(&mut r, &mut seq, 5);
        assert!(
            r.commit_seq() <= r.acked(5).unwrap(),
            "now nothing commits without it"
        );
        r.on_ack(5, before, 0);
        assert!(r.holds_committed(5));
        r.set_isr(&[]);
        assert!(!r.holds_committed(5), "not counted any more");
        // an idle shard: caught up at once
        let mut idle = ReplSender::new(1, 1, 1 << 16);
        idle.on_group_at(&enc(Seq::new(1, 1)), Seq::new(1, 1), 0);
        idle.set_local_durable(Seq::new(1, 1));
        idle.attach(
            2,
            &AcceptMode::Continue {
                from: Seq::new(1, 2),
            },
        );
        assert!(idle.caught_up(2));
        // a repaired member's chain starts at the end of its direct feed
        idle.attach(3, &AcceptMode::Repair { at: Seq::new(1, 1) });
        assert!(!idle.caught_up(3));
        idle.resume(3, Seq::new(1, 1));
        assert!(!idle.caught_up(3), "nothing acknowledged yet");
        idle.on_ack(3, Seq::new(1, 1), 0);
        assert!(idle.caught_up(3));
    }

    #[test]
    fn lagging_is_an_age_test_and_overrun_restarts_the_stream() {
        let mut r = ReplSender::new(0, 1, 512);
        r.set_isr_at(&[1], 0);
        r.attach(
            1,
            &AcceptMode::Continue {
                from: Seq::new(1, 1),
            },
        );
        r.on_group_at(&enc(Seq::new(1, 1)), Seq::new(1, 1), 1_000);
        r.set_local_durable_at(Seq::new(1, 1), 1_000);
        r.on_group_at(&enc(Seq::new(1, 2)), Seq::new(1, 2), 2_000);
        r.set_local_durable_at(Seq::new(1, 2), 2_000);
        assert!(r.lagging(100_000, 250_000).is_empty());
        assert_eq!(r.lagging(300_000, 250_000).as_slice(), &[1]);
        r.on_ack(1, Seq::new(1, 1), 0);
        assert!(
            r.lagging(251_500, 250_000).is_empty(),
            "the oldest unacknowledged group is now (1, 2)"
        );
        r.on_ack(1, Seq::new(1, 2), 0);
        assert!(r.lagging(u64::MAX, 250_000).is_empty());
        // a tiny backlog: a member that falls behind it is told to restart
        r.attach(
            2,
            &AcceptMode::Continue {
                from: Seq::new(1, 1),
            },
        );
        for i in 3..=40 {
            r.on_group_at(&enc(Seq::new(1, i)), Seq::new(1, i), 0);
        }
        assert!(!r.fill_send_at(2, &mut Vec::new(), 1 << 20, 0));
        assert!(r.overrun(2));
    }

    #[test]
    fn a_member_counted_below_groups_produced_alone_is_reported_when_it_stops_acking() {
        // the primary served alone (nobody counted: no group has an age entry), then a
        // member that acknowledged up to (1, 3) starts counting: the commit point moves back
        // to (1, 3) and the replies of (1, 4) and (1, 5) wait for it
        let mut r = ReplSender::new(0, 1, 1 << 16);
        r.attach(
            1,
            &AcceptMode::Continue {
                from: Seq::new(1, 1),
            },
        );
        for i in 1..=5 {
            r.on_group_at(&enc(Seq::new(1, i)), Seq::new(1, i), 1_000);
        }
        r.set_local_durable_at(Seq::new(1, 5), 1_000);
        r.on_ack(1, Seq::new(1, 3), 2_000);
        r.set_isr_at(&[1], 10_000);
        assert_eq!(r.commit_seq(), Seq::new(1, 3));
        assert!(r.lagging(200_000, 250_000).is_empty(), "within the timeout");
        // it never acknowledges (1, 4) and no other group is produced: reported all the same,
        // the age running from when it started counting
        assert_eq!(r.lagging(260_001, 250_000).as_slice(), &[1]);
        r.on_ack(1, Seq::new(1, 5), 270_000);
        assert!(
            r.lagging(u64::MAX, 250_000).is_empty(),
            "everything acknowledged"
        );
        // dropped and counted again later: the age restarts then
        r.on_group_at(&enc(Seq::new(1, 6)), Seq::new(1, 6), 280_000);
        r.set_isr_at(&[], 290_000);
        r.on_group_at(&enc(Seq::new(1, 7)), Seq::new(1, 7), 300_000);
        r.set_local_durable_at(Seq::new(1, 7), 300_000);
        r.set_isr_at(&[1], 1_000_000);
        assert!(r.lagging(1_200_000, 250_000).is_empty());
        assert_eq!(r.lagging(1_250_001, 250_000).as_slice(), &[1]);
    }

    const T: u64 = 250_000_000;

    #[test]
    fn a_primary_stall_is_not_member_lag() {
        // a primary with one counted member stops for 400 ms (SIGSTOP, or descheduled) right
        // after producing a group; the member acknowledged it at once, but the ack waits in the
        // socket until the primary's receive path runs again
        let base = 7_000_000_000u64;
        let mut c = StallClock::new();
        let mut r = ReplSender::new(0, 1, 1 << 16);
        r.set_isr_at(&[1], c.advance(base, false));
        r.attach(
            1,
            &AcceptMode::Continue {
                from: Seq::new(1, 1),
            },
        );
        r.on_group_at(&enc(Seq::new(1, 1)), Seq::new(1, 1), c.peek(base + 1_000_000));
        r.set_local_durable_at(Seq::new(1, 1), c.advance(base + 2_000_000, false));
        // first tick after waking: the receive path has not run yet
        let now = c.advance(base + 402_000_000, true);
        assert!(r.lagging(now, T).is_empty(), "the stall is not the member's");
        // the receive path runs again; the ack is one more tick away
        let now = c.advance(base + 403_000_000, false);
        assert!(r.lagging(now, T).is_empty());
        r.on_ack(1, Seq::new(1, 1), 0);
        assert!(r.lagging(now, T).is_empty());
        // a gap counts at most one step even when the receive path seems to run
        let mut d = StallClock::new();
        let t0 = d.advance(base, false);
        assert_eq!(d.advance(base + 400_000_000, false) - t0, STALL_STEP_NS);
        assert_eq!(d.peek(base + 401_000_000) - t0, STALL_STEP_NS + 1_000_000);
        // a member that really stops acknowledging is still reported once the primary ran
        // for the timeout
        r.on_group_at(&enc(Seq::new(1, 2)), Seq::new(1, 2), c.peek(base + 404_000_000));
        let mut now = c.advance(base + 405_000_000, false);
        r.set_local_durable_at(Seq::new(1, 2), now);
        let start = now;
        let mut real = base + 405_000_000;
        while now.saturating_sub(start) <= T {
            assert!(r.lagging(now, T).is_empty());
            real += 5_000_000;
            now = c.advance(real, false);
        }
        assert_eq!(r.lagging(now, T).as_slice(), &[1]);
    }

    #[test]
    fn a_member_is_timed_from_the_primary_durability() {
        let mut r = ReplSender::new(0, 1, 1 << 16);
        r.set_isr_at(&[1], 0);
        r.attach(
            1,
            &AcceptMode::Continue {
                from: Seq::new(1, 1),
            },
        );
        r.on_group_at(&enc(Seq::new(1, 1)), Seq::new(1, 1), 1_000);
        // the primary's own disk is slow: the member is not what holds the commit back
        assert!(r.lagging(10 * T, T).is_empty());
        r.on_ack(1, Seq::new(1, 1), 0);
        r.on_group_at(&enc(Seq::new(1, 2)), Seq::new(1, 2), 11 * T);
        r.set_local_durable_at(Seq::new(1, 2), 12 * T);
        assert!(r.lagging(13 * T, T).is_empty(), "within the timeout");
        assert_eq!(r.lagging(13 * T + 1, T).as_slice(), &[1]);
        r.on_ack(1, Seq::new(1, 2), 0);
        assert!(r.lagging(u64::MAX, T).is_empty());
    }
}
