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

use smallvec::SmallVec;

use crate::Seq;
use crate::backlog::Backlog;
use crate::frame::AcceptMode;

/// Index of a member within the shard's member list.
pub type MemberId = u16;

/// Per-member stream state.
struct MemberSlot {
    id: MemberId,
    /// Next seq to send.
    next: Seq,
    acked_durable: Seq,
    /// Boot-clock time of the oldest unacknowledged send.
    oldest_unacked_ns: Option<u64>,
}

pub struct ReplSender {
    partition: u16,
    term: u32,
    backlog: Backlog,
    members: Vec<MemberSlot>,
    isr: SmallVec<[MemberId; 4]>,
    local_durable: Seq,
}

impl ReplSender {
    pub fn new(partition: u16, term: u32, backlog_bytes: usize) -> Self {
        todo!("DESIGN §23.7: replication sender")
    }

    pub fn on_group(&mut self, encoded: &[u8], seq: Seq) {
        todo!("DESIGN §23.8: ship a group")
    }

    pub fn attach(&mut self, m: MemberId, mode: &AcceptMode) {
        todo!("DESIGN §23.7: attach member")
    }

    pub fn detach(&mut self, m: MemberId) {
        todo!("DESIGN §23.7: detach member")
    }

    /// Members whose acks count toward `commit_seq` (the committed ISR minus the primary).
    pub fn set_isr(&mut self, counted: &[MemberId]) {
        todo!("DESIGN §23.8: ISR")
    }

    pub fn on_ack(&mut self, m: MemberId, durable: Seq, now_ns: u64) {
        todo!("DESIGN §23.8: member ack")
    }

    pub fn set_local_durable(&mut self, s: Seq) {
        todo!("DESIGN §23.8: local durability")
    }

    pub fn commit_seq(&self) -> Seq {
        todo!("DESIGN §23.8: commit point")
    }

    /// frames due for m
    pub fn fill_send(&mut self, m: MemberId, out: &mut Vec<u8>, max: usize) -> bool {
        todo!("DESIGN §23.7: member send buffer")
    }

    /// Members whose oldest unacknowledged group is older than `timeout_ns`.
    pub fn lagging(&self, now_ns: u64, timeout_ns: u64) -> SmallVec<[MemberId; 2]> {
        todo!("DESIGN §23.8: ack timeout")
    }
}
