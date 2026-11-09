//! Connection state (DESIGN §7.1, §7.4-§7.6, §6.7).
//!
//! `read_buf` starts at 16 KiB, grows to fit the current command and shrinks back when idle;
//! `out` plus shared iovec bodies for values >= 64 KiB, one sendmsg per tick. Every parsed
//! command gets a `req_seq`; its reply slot is `Ready(bytes)`, `Waiting{partition, ticket,
//! commit_seq}`, `Remote` or `Gather{n_parts, parts}`, and slots are released strictly in
//! `req_seq` order. A connection stops re-arming recv while any single limit of §6.7 holds and
//! resumes when it no longer holds. With `requirepass` set, commands other than AUTH, HELLO,
//! QUIT and RESET reply `-NOAUTH Authentication required.` until authenticated.
//!
//! Layout of the reply path: a reply produced while no earlier reply is outstanding goes
//! straight to the output (`out`); otherwise it waits in the reorder buffer (`slots`, one
//! entry per outstanding `req_seq`, oldest first) until every earlier reply is out. Waiting
//! for a durability ticket or a remote worker is kept by the worker as a `Pending` slot here
//! (the ticket bookkeeping lives with the partition's owner, which sends the bytes once the
//! ticket allows, §14.7). Shared value bodies are queued as separate output segments and sent
//! as iovecs, never copied.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::net::SocketAddr;

use kv_cmd::CmdId;
use kv_resp::{Limits, Parser, Proto};

use crate::cmd::FanOutSplit;
use crate::runtime::ring::OutSeg;
use crate::runtime::xmsg::{OwnedCmd, ReplyBody};

/// Initial (and idle) size of `read_buf`.
pub const READ_BUF_INITIAL: usize = 16 * 1024;
/// Unreplied commands a connection may have before its first reply ([`Conn::reply_window`]).
pub const INITIAL_WINDOW: u64 = 4;
/// What a command of a connection that has had no reply yet is expected to reply, for the
/// node's in-flight estimate ([`node_reply_bytes`]).
const FIRST_REPLY_EST: u64 = 64 * 1024;
/// Reply bytes held by all connections of the node (produced, not sent yet) above which a
/// connection reads its next command only once it has no reply unproduced or unsent
/// ([`Conn::reply_window`]), and above half of which a new connection starts with a window of
/// one. A connection whose replies were all sent always reads its next command, so every
/// connection that reads its replies makes progress.
pub const NODE_REPLY_BUDGET: u64 = 1 << 30;

/// Reply bytes held by every connection of the process (`Conn::held`), plus the estimated
/// replies of their commands in flight (`Conn::inflight_est`).
static NODE_HELD: AtomicU64 = AtomicU64::new(0);
/// Connections closed for holding more than [`Conn::over_hard_limit`] allows.
static EVICTED: AtomicU64 = AtomicU64::new(0);

/// Reply bytes held by the node's connections and expected for their commands in flight (INFO
/// `client_reply_bytes_held`).
pub fn node_reply_bytes() -> u64 {
    NODE_HELD.load(AtomicOrdering::Relaxed)
}

/// Connections closed because their unsent replies passed the hard limit (INFO
/// `client_output_limit_disconnections`).
pub fn evicted() -> u64 {
    EVICTED.load(AtomicOrdering::Relaxed)
}

/// Counts a connection closed by [`Conn::over_hard_limit`].
pub fn note_evicted() {
    EVICTED.fetch_add(1, AtomicOrdering::Relaxed);
}
/// Replies at least this long are queued as their own output segment (no copy into `out`).
const SEG_MIN: usize = 64 * 1024;
/// An idle `read_buf` larger than this shrinks back to [`READ_BUF_INITIAL`].
pub const READ_BUF_SHRINK: usize = 1024 * 1024;

/// MULTI state: queued commands and whether a queue-time error occurred (EXECABORT).
pub struct MultiState {
    pub queued: Vec<OwnedCmd>,
    pub errored: bool,
}

/// A WATCHed key (exact bytes; no `Debug`, LC-32).
pub struct WatchKey {
    pub db: u8,
    pub key: Box<[u8]>,
}

/// Connection flags.
pub mod flags {
    pub const CLOSE_AFTER_REPLY: u32 = 1;
    pub const BACKPRESSURED: u32 = 2;
    pub const NO_EVICT: u32 = 4;
    pub const NO_TOUCH: u32 = 8;
    pub const WATCH_DIRTY: u32 = 16;
    /// Closed by CLIENT KILL (or a protocol error): no further command is parsed.
    pub const KILLED: u32 = 32;
}

/// Why a connection stopped reading (§6.7; each a single limit).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Hold {
    /// Unreplied commands >= `max-pending-per-conn`.
    Pending,
    /// Queued output >= `client-output-buffer-limit`.
    Output,
    /// The destination XMsg queue is full.
    Queue,
    /// The large-value soft limit was reached before this large command was consumed.
    LargeValue,
    /// No txn credit for this multi-key write.
    TxnCredit,
    /// The worker's staged-write memory is at its share of `max-dirty-bytes`.
    Dirty,
    /// An ordering wait, not a limit: the connection's cross-partition transaction waits for
    /// the transaction gate, and later commands must not reach its partitions before its locks
    /// (§6.6, §6.8). Lifted when the locks are sent.
    Order,
    /// A cluster write the routing gate would answer TRYAGAIN (a handoff or term raise of
    /// this primary, a migration seal not committed yet): it waits for the state to pass, up
    /// to `GATE_HOLD_NS` from `Conn::gate_hold_since`, instead of failing at once (clients do
    /// not retry a TRYAGAIN inside a transaction pipeline).
    Gate,
}

/// A fan-out reply being gathered (§6.8).
pub struct GatherSlot {
    pub id: CmdId,
    pub split: FanOutSplit,
    pub argc: usize,
    pub parts: Vec<Option<ReplyBody>>,
    pub remaining: usize,
}

/// One outstanding reply in the reorder buffer.
pub enum Slot {
    /// Not produced yet (local execution, durability wait or a remote worker).
    Pending,
    Ready(ReplyBody),
    Gather(Box<GatherSlot>),
}

pub struct Conn {
    pub id: u64,
    pub fd: i32,
    pub proto: Proto,
    pub db: u8,
    pub name: Option<Box<[u8]>>,
    pub lib_name: Option<Box<[u8]>>,
    pub lib_ver: Option<Box<[u8]>>,
    pub authed: bool,
    pub readonly: bool,
    pub multi: Option<MultiState>,
    pub watch: Vec<WatchKey>,
    /// The connection's WATCH session (`runtime::watch`; 0: none).
    pub watch_token: u64,
    /// Boot-clock ns of creation / of the last command.
    pub created: u64,
    pub last_cmd: u64,
    pub flags: u32,
    /// Receive buffer, fully initialized: `read_buf[..filled]` holds received bytes.
    pub read_buf: Vec<u8>,
    pub out: Vec<u8>,
    pub next_req_seq: u64,
    /// Remote and local addresses (CLIENT LIST `addr=` / `laddr=`).
    pub addr: Option<SocketAddr>,
    pub laddr: Option<SocketAddr>,
    /// The last command executed (CLIENT LIST `cmd=`).
    pub last_cmd_id: Option<CmdId>,
    /// Incremental RESP parser state.
    pub parser: Parser,
    /// Slot generation in the worker's connection table.
    pub generation: u32,
    /// Bytes of `read_buf` received.
    pub(crate) filled: usize,
    /// Bytes of `read_buf` already consumed by parsed commands.
    pub(crate) consumed: usize,
    /// Reorder buffer: `slots[i]` is the reply of `req_seq == base_seq + i`.
    pub(crate) slots: VecDeque<Slot>,
    pub(crate) base_seq: u64,
    /// Output queued before `out` (shared bodies and the bytes around them), oldest first.
    pub(crate) out_segs: VecDeque<OutSeg>,
    /// Bytes queued for sending (in flight included), for `client-output-buffer-limit`.
    pub(crate) out_queued: u64,
    pub(crate) recv_inflight: bool,
    pub(crate) send_inflight: bool,
    /// The command being received passed the large-value check (§6.9).
    pub(crate) lv_admitted: bool,
    /// Why reading stopped, if it did.
    pub(crate) hold: Option<Hold>,
    /// When the command at the parse position was first held on the routing gate
    /// (`Hold::Gate`, boot-clock ns).
    pub(crate) gate_hold_since: Option<u64>,
    /// When the command at the parse position was first held for a txn credit
    /// (`Hold::TxnCredit`, boot-clock ns).
    pub(crate) credit_hold_since: Option<u64>,
    /// The connection is on the worker's "has bytes to parse" list.
    pub(crate) queued_for_parse: bool,
    /// The connection is on the worker's "has output to send" list.
    pub(crate) queued_for_send: bool,
    /// Running estimate of this connection's reply size in bytes (1/8 weight to each new
    /// reply), for [`Conn::reply_window`]; `0` until the first reply.
    pub(crate) reply_est: u64,
    /// Reply bytes this connection holds: produced (ready in the reorder buffer, gathered
    /// parts, queued output) and not sent yet; counted in [`node_reply_bytes`].
    pub(crate) held: u64,
    /// Estimated reply bytes of the commands in flight (`unproduced` of them), counted in
    /// [`node_reply_bytes`] until their replies arrive.
    pub(crate) inflight_est: u64,
    pub(crate) unproduced: u64,
    /// The largest reply this connection was sent (for [`Conn::over_hard_limit`]).
    pub(crate) reply_max: u64,
}

impl Drop for Conn {
    fn drop(&mut self) {
        NODE_HELD.fetch_sub(self.held + self.inflight_est, AtomicOrdering::Relaxed);
    }
}

impl Conn {
    /// A new connection on socket `fd` (`now` = boot-clock ns).
    pub fn new(id: u64, fd: i32, now: u64, limits: Limits) -> Self {
        Conn {
            id,
            fd,
            proto: Proto::Resp2,
            db: 0,
            name: None,
            lib_name: None,
            lib_ver: None,
            authed: false,
            readonly: false,
            multi: None,
            watch: Vec::new(),
            watch_token: 0,
            created: now,
            last_cmd: now,
            flags: 0,
            read_buf: vec![0; READ_BUF_INITIAL],
            out: Vec::new(),
            next_req_seq: 0,
            addr: None,
            laddr: None,
            last_cmd_id: None,
            parser: Parser::new(limits),
            generation: 0,
            filled: 0,
            consumed: 0,
            slots: VecDeque::new(),
            base_seq: 0,
            out_segs: VecDeque::new(),
            out_queued: 0,
            recv_inflight: false,
            send_inflight: false,
            lv_admitted: false,
            hold: None,
            gate_hold_since: None,
            credit_hold_since: None,
            queued_for_parse: false,
            queued_for_send: false,
            reply_est: 0,
            held: 0,
            inflight_est: 0,
            unproduced: 0,
            reply_max: 0,
        }
    }

    /// One command's reply was produced: its share of the in-flight estimate is dropped.
    fn produced(&mut self) {
        if self.unproduced == 0 {
            return;
        }
        let d = if self.unproduced == 1 { self.inflight_est } else { self.inflight_est / self.unproduced };
        self.unproduced -= 1;
        self.inflight_est -= d;
        NODE_HELD.fetch_sub(d, AtomicOrdering::Relaxed);
    }

    /// Whether the connection's unsent replies are past the hard limit: more than four times
    /// the output limit beyond its largest reply. Its reply window keeps a connection that
    /// reads its replies well below that; a client that pipelines commands whose replies grow
    /// (small GETs, then thousands of GETs of large values) and does not read them is closed,
    /// as Redis closes a client past its hard `client-output-buffer-limit`.
    pub fn over_hard_limit(&self, output_limit: u64) -> bool {
        self.held > output_limit.saturating_mul(4).saturating_add(self.reply_max)
    }

    fn hold_add(&mut self, n: usize) {
        self.held += n as u64;
        NODE_HELD.fetch_add(n as u64, AtomicOrdering::Relaxed);
    }

    fn hold_sub(&mut self, n: u64) {
        let n = n.min(self.held);
        self.held -= n;
        NODE_HELD.fetch_sub(n, AtomicOrdering::Relaxed);
    }

    /// Unreplied commands (§6.7 `max-pending-per-conn`).
    #[inline]
    pub fn pending(&self) -> usize {
        self.slots.len()
    }

    /// Bytes queued for output (§6.7 `client-output-buffer-limit`).
    #[inline]
    pub fn output_bytes(&self) -> u64 {
        self.out_queued
    }

    /// How many unreplied commands the connection may have before reading stops: the replies
    /// of commands already consumed are produced whatever their size (a consumed command never
    /// waits, LC-15), so the output limit has to apply before consumption. With a window of
    /// `max_pending` alone, one client pipelining 4096 GETs of a 4 MiB value without reading
    /// the replies made the server hold 16 GiB of replies and abort on a failed allocation.
    /// The window is `output_limit` over the connection's estimated reply size, between 1 and
    /// `max_pending`; before the first reply it is [`INITIAL_WINDOW`].
    ///
    /// The estimate lags a change of reply size (a connection that saw small replies may have
    /// thousands of GETs of large values in flight), so the node also bounds what connections
    /// hold together: above [`NODE_REPLY_BUDGET`] held reply bytes the window is one, and zero
    /// while the connection holds unsent replies (it reads its next command once its earlier
    /// replies are produced and sent); above half of it a new connection's window is one. 150
    /// connections pipelining 20 GETs each of distinct 2 MB values made the server hold 4.8 GB.
    pub fn reply_window(&self, output_limit: u64, max_pending: usize) -> usize {
        let node = node_reply_bytes();
        if node >= NODE_REPLY_BUDGET {
            // one command at a time, and only once everything before it was sent: a client that
            // does not read keeps at most one reply
            return usize::from(self.held == 0);
        }
        let initial = if node >= NODE_REPLY_BUDGET / 2 { 1 } else { INITIAL_WINDOW };
        let est = if self.reply_est == 0 { output_limit / initial } else { self.reply_est };
        let w = output_limit / est.max(1);
        (w.min(max_pending as u64) as usize).max(1)
    }

    /// A reply of `n` bytes went to the output: the estimate follows with 1/8 weight down and at
    /// once up to half of a larger reply (a connection whose replies grow gets a small window
    /// after its first large reply).
    fn note_reply(&mut self, n: u64) {
        self.reply_max = self.reply_max.max(n);
        self.reply_est = if self.reply_est == 0 { n.max(1) } else { (self.reply_est - self.reply_est / 8 + n / 8).max(n / 2).max(1) };
    }

    /// Unparsed input bytes (CLIENT LIST `qbuf=`).
    #[inline]
    pub fn query_buffer_len(&self) -> usize {
        self.filled - self.consumed
    }

    /// Whether a close was requested (QUIT, protocol error, CLIENT KILL).
    #[inline]
    pub fn closing(&self) -> bool {
        self.flags & (flags::CLOSE_AFTER_REPLY | flags::KILLED) != 0
    }

    /// The unparsed input.
    #[inline]
    pub fn unparsed(&self) -> &[u8] {
        &self.read_buf[self.consumed..self.filled]
    }

    /// Received bytes (`read_buf[..filled]`).
    #[inline]
    pub fn filled(&self) -> usize {
        self.filled
    }

    /// Records `n` bytes received at `read_buf[filled..]`.
    #[inline]
    pub fn received(&mut self, n: usize) {
        self.filled = (self.filled + n).min(self.read_buf.len());
    }

    /// Reserves the next request sequence number with a `Pending` slot (its reply comes later).
    pub fn reserve_slot(&mut self) -> u64 {
        let seq = self.next_req_seq;
        self.next_req_seq += 1;
        if self.slots.is_empty() {
            self.base_seq = seq;
        }
        self.slots.push_back(Slot::Pending);
        let e = if self.reply_est == 0 { FIRST_REPLY_EST } else { self.reply_est };
        self.unproduced += 1;
        self.inflight_est += e;
        NODE_HELD.fetch_add(e, AtomicOrdering::Relaxed);
        seq
    }

    /// A reply produced at once for a newly parsed command: straight to the output when no
    /// earlier reply is outstanding, else into the reorder buffer.
    pub fn reply_now(&mut self, body: ReplyBody) {
        let seq = self.next_req_seq;
        self.next_req_seq += 1;
        self.hold_add(body.len());
        if self.slots.is_empty() {
            self.base_seq = seq + 1;
            self.push_output(body);
        } else {
            self.slots.push_back(Slot::Ready(body));
        }
    }

    /// Direct writes for a command answered at once (writer over `out`), valid only when no
    /// earlier reply is outstanding; returns `None` otherwise.
    pub fn direct_out(&mut self) -> Option<&mut Vec<u8>> {
        if self.slots.is_empty() && self.out_segs.is_empty() { Some(&mut self.out) } else { None }
    }

    /// Accounts for a reply written through [`Conn::direct_out`] (`n` bytes).
    pub fn direct_done(&mut self, n: usize) {
        self.next_req_seq += 1;
        self.base_seq = self.next_req_seq;
        self.out_queued += n as u64;
        self.note_reply(n as u64);
        self.hold_add(n);
    }

    /// Delivers the reply of `req_seq`; returns whether output became ready.
    pub fn deliver(&mut self, req_seq: u64, body: ReplyBody) -> bool {
        let Some(i) = req_seq.checked_sub(self.base_seq) else { return false };
        let n = body.len();
        match self.slots.get_mut(i as usize) {
            Some(s @ Slot::Pending) => *s = Slot::Ready(body),
            _ => return false,
        }
        self.produced();
        self.hold_add(n);
        self.release()
    }

    /// Installs a gather in the slot of `req_seq`.
    pub fn set_gather(&mut self, req_seq: u64, g: GatherSlot) {
        if let Some(i) = req_seq.checked_sub(self.base_seq)
            && let Some(s) = self.slots.get_mut(i as usize)
        {
            *s = Slot::Gather(Box::new(g));
        }
    }

    /// Delivers part `part` of a gathered reply; when the last part arrives the parts are
    /// merged by `merge` and the slot becomes ready. Returns whether output became ready.
    pub fn deliver_part(&mut self, req_seq: u64, part: u16, body: ReplyBody, merge: impl FnOnce(&GatherSlot) -> Vec<u8>) -> bool {
        let Some(i) = req_seq.checked_sub(self.base_seq) else { return false };
        let Some(slot) = self.slots.get_mut(i as usize) else { return false };
        let Slot::Gather(g) = slot else { return false };
        let Some(p) = g.parts.get_mut(usize::from(part)) else { return false };
        if p.is_some() {
            return false;
        }
        let n = body.len();
        *p = Some(body);
        g.remaining -= 1;
        if g.remaining > 0 {
            self.hold_add(n);
            return false;
        }
        let merged = merge(g);
        let parts: usize = g.parts.iter().flatten().map(ReplyBody::len).sum::<usize>() - n;
        let m = merged.len();
        *slot = Slot::Ready(ReplyBody::Bytes(merged));
        self.hold_sub(parts as u64);
        self.hold_add(m);
        self.produced();
        self.release()
    }

    /// Moves every leading ready reply to the output; returns whether any did.
    fn release(&mut self) -> bool {
        let mut any = false;
        while matches!(self.slots.front(), Some(Slot::Ready(_))) {
            let Some(Slot::Ready(body)) = self.slots.pop_front() else { break };
            self.base_seq += 1;
            self.push_output(body);
            any = true;
        }
        any
    }

    /// Appends reply bytes to the output.
    pub fn push_output(&mut self, body: ReplyBody) {
        self.out_queued += body.len() as u64;
        self.note_reply(body.len() as u64);
        match body {
            ReplyBody::Bytes(b) => {
                if self.out.is_empty() && self.out.capacity() < b.capacity() {
                    self.out = b;
                } else if b.len() >= SEG_MIN {
                    // a large reply goes out as its own segment: copied into `out`, it doubled
                    // `out`'s allocation (a 1 GiB allocation for a few hundred MiB of replies)
                    if !self.out.is_empty() {
                        let o = std::mem::take(&mut self.out);
                        self.out_segs.push_back(OutSeg::Bytes(o, 0));
                    }
                    self.out_segs.push_back(OutSeg::Bytes(b, 0));
                } else {
                    self.out.extend_from_slice(&b);
                }
            }
            ReplyBody::Shared(head, body) => {
                self.out.extend_from_slice(&head);
                if !self.out.is_empty() {
                    let o = std::mem::take(&mut self.out);
                    self.out_segs.push_back(OutSeg::Bytes(o, 0));
                }
                self.out_segs.push_back(OutSeg::Shared(body, 0));
                // the bulk terminator after the shared body (counted in `body.len()`)
                self.out.extend_from_slice(b"\r\n");
            }
            ReplyBody::Ext(head, parts) => {
                self.out.extend_from_slice(&head);
                let o = std::mem::take(&mut self.out);
                self.out_segs.push_back(OutSeg::Bytes(o, 0));
                for p in parts {
                    self.out_segs.push_back(OutSeg::Ext(p, 0));
                }
                self.out.extend_from_slice(b"\r\n");
            }
        }
    }

    /// Whether there is output to send.
    #[inline]
    pub fn has_output(&self) -> bool {
        !self.out.is_empty() || !self.out_segs.is_empty()
    }

    /// Takes everything queued for one sendmsg (oldest first); `spare` is a recycled buffer
    /// that becomes the new `out`.
    pub fn take_output(&mut self, spare: Vec<u8>) -> Vec<OutSeg> {
        let mut segs: Vec<OutSeg> = self.out_segs.drain(..).collect();
        if !self.out.is_empty() {
            let o = std::mem::replace(&mut self.out, spare);
            segs.push(OutSeg::Bytes(o, 0));
        }
        segs
    }

    /// A send completed: `sent` bytes went out; `rest` (unsent remainder of the in-flight
    /// segments) goes back in front of newer output.
    pub fn sent(&mut self, sent: usize, rest: Vec<OutSeg>) {
        self.out_queued = self.out_queued.saturating_sub(sent as u64);
        self.hold_sub(sent as u64);
        for seg in rest.into_iter().rev() {
            self.out_segs.push_front(seg);
        }
    }

    /// Compacts consumed input; makes room for at least `want` more bytes; shrinks an idle,
    /// oversized buffer.
    pub fn prepare_read(&mut self, want: usize) {
        if self.consumed > 0 {
            self.read_buf.copy_within(self.consumed..self.filled, 0);
            self.filled -= self.consumed;
            self.consumed = 0;
        }
        if self.filled == 0 && self.read_buf.len() > READ_BUF_SHRINK {
            self.read_buf = vec![0; READ_BUF_INITIAL];
        }
        let want = want.max(READ_BUF_INITIAL / 4);
        if self.read_buf.len() - self.filled < want {
            // zero-filled once per growth (amortized), never per receive
            let new_len = (self.filled + want).next_power_of_two().max(READ_BUF_INITIAL);
            self.read_buf.resize(new_len, 0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kv_common::shared_buf::SharedBuf;

    fn conn() -> Conn {
        Conn::new(1, -1, 0, Limits::default())
    }

    fn bytes(s: &[u8]) -> ReplyBody {
        ReplyBody::Bytes(s.to_vec())
    }

    /// The output limit applies before commands are consumed: a client pipelining GETs of a
    /// 4 MiB value gets about `limit / 4 MiB` of them in flight, not `max-pending-per-conn`.
    #[test]
    fn reply_window_follows_the_reply_size() {
        let limit = 64 << 20;
        let mut c = conn();
        assert_eq!(c.reply_window(limit, 4096), INITIAL_WINDOW as usize);
        c.push_output(ReplyBody::Bytes(vec![0; 4 << 20]));
        assert_eq!(c.reply_window(limit, 4096), 16);
        c.push_output(ReplyBody::Bytes(vec![0; 8 << 20]));
        assert_eq!(c.reply_window(limit, 4096), 14);
        c.push_output(ReplyBody::Bytes(vec![0; 512 << 20]));
        assert_eq!(c.reply_window(limit, 4096), 1, "never below one");
        for _ in 0..400 {
            c.push_output(bytes(b"+OK\r\n"));
        }
        assert_eq!(c.reply_window(limit, 4096), 4096, "small replies: max-pending-per-conn");
    }

    /// Held reply bytes and the in-flight estimate return to zero once every reply is sent, and
    /// a connection holding far more than its output limit is past the hard limit.
    #[test]
    fn held_reply_bytes_follow_replies_until_sent() {
        let limit = 1 << 20;
        let mut c = conn();
        let a = c.reserve_slot();
        let b = c.reserve_slot();
        assert_eq!((c.unproduced, c.inflight_est), (2, 2 * FIRST_REPLY_EST));
        c.reply_now(bytes(b"+3\r\n"));
        assert!(!c.deliver(b, ReplyBody::Bytes(vec![b'x'; 1000])));
        assert_eq!(c.held, 1004);
        assert!(c.deliver(a, bytes(b"+1\r\n")));
        assert_eq!((c.unproduced, c.inflight_est), (0, 0));
        let out = drain(&mut c);
        assert_eq!(out.len(), 1008);
        assert_eq!(c.held, 0);
        assert!(!c.over_hard_limit(limit));
        for _ in 0..5 {
            let s = c.reserve_slot();
            c.deliver(s, ReplyBody::Bytes(vec![b'y'; 1 << 20]));
        }
        assert!(!c.over_hard_limit(limit), "four times the limit beyond the largest reply");
        let s = c.reserve_slot();
        c.deliver(s, ReplyBody::Bytes(vec![b'y'; 1 << 20]));
        assert!(c.over_hard_limit(limit));
        drain(&mut c);
        assert_eq!(c.held, 0);
    }

    fn drain(c: &mut Conn) -> Vec<u8> {
        let segs = c.take_output(Vec::new());
        let mut v = Vec::new();
        for s in &segs {
            v.extend_from_slice(s.remaining());
        }
        let n = v.len();
        c.sent(n, Vec::new());
        v
    }

    #[test]
    fn replies_leave_in_request_order() {
        let mut c = conn();
        let a = c.reserve_slot();
        let b = c.reserve_slot();
        c.reply_now(bytes(b"+3\r\n"));
        let d = c.reserve_slot();
        assert_eq!(c.pending(), 4);
        assert!(!c.deliver(b, bytes(b"+2\r\n")), "an earlier reply is outstanding");
        assert!(!c.has_output());
        assert!(c.deliver(a, bytes(b"+1\r\n")));
        assert_eq!(drain(&mut c), b"+1\r\n+2\r\n+3\r\n");
        assert_eq!(c.pending(), 1);
        assert!(c.deliver(d, bytes(b"+4\r\n")));
        assert_eq!(drain(&mut c), b"+4\r\n");
        assert_eq!(c.pending(), 0);
        c.reply_now(bytes(b"+5\r\n"));
        assert_eq!(drain(&mut c), b"+5\r\n");
        assert!(!c.deliver(0, bytes(b"stale")), "stale sequence numbers are ignored");
        assert_eq!(c.output_bytes(), 0);
    }

    #[test]
    fn direct_output_only_when_nothing_is_outstanding() {
        let mut c = conn();
        let out = c.direct_out().unwrap();
        out.extend_from_slice(b"+PONG\r\n");
        c.direct_done(7);
        let s = c.reserve_slot();
        assert!(c.direct_out().is_none());
        assert!(c.deliver(s, bytes(b":1\r\n")));
        assert_eq!(drain(&mut c), b"+PONG\r\n:1\r\n");
    }

    #[test]
    fn gathers_merge_when_complete() {
        let mut c = conn();
        let s = c.reserve_slot();
        let split = FanOutSplit { parts: smallvec::smallvec![(1, smallvec::smallvec![1]), (2, smallvec::smallvec![2])] };
        c.set_gather(s, GatherSlot { id: CmdId::Del, split, argc: 3, parts: vec![None, None], remaining: 2 });
        let merge = |g: &GatherSlot| {
            let n: usize = g.parts.iter().map(|p| p.as_ref().unwrap().len()).sum();
            format!(":{n}\r\n").into_bytes()
        };
        assert!(!c.deliver_part(s, 1, bytes(b":1\r\n"), merge));
        assert!(!c.deliver_part(s, 1, bytes(b":1\r\n"), merge), "duplicate parts are ignored");
        assert!(c.deliver_part(s, 0, bytes(b":0\r\n"), merge));
        assert_eq!(drain(&mut c), b":8\r\n");
    }

    #[test]
    fn shared_bodies_become_iovec_segments_in_order() {
        let mut c = conn();
        c.reply_now(bytes(b"+A\r\n"));
        c.reply_now(ReplyBody::Shared(b"$3\r\n".to_vec(), SharedBuf::new(b"xyz".to_vec())));
        c.reply_now(bytes(b"+B\r\n"));
        let segs = c.take_output(Vec::new());
        let shapes: Vec<_> = segs.iter().map(|s| matches!(s, OutSeg::Shared(..))).collect();
        assert_eq!(shapes, vec![false, true, false]);
        let all: Vec<u8> = segs.iter().flat_map(|s| s.remaining().to_vec()).collect();
        assert_eq!(all, b"+A\r\n$3\r\nxyz\r\n+B\r\n");
        assert_eq!(c.output_bytes(), all.len() as u64);
        // a partial send puts the remainder back in front of newer output
        let mut segs = segs;
        crate::runtime::ring::advance_segments(&mut segs, 9, &mut |_| {});
        c.reply_now(bytes(b"+C\r\n"));
        c.sent(9, segs);
        assert_eq!(drain(&mut c), b"yz\r\n+B\r\n+C\r\n");
    }

    #[test]
    fn read_buffer_compacts_grows_and_shrinks() {
        let mut c = conn();
        let input = b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPI";
        c.read_buf[..input.len()].copy_from_slice(input);
        c.received(input.len());
        c.consumed = 14;
        c.prepare_read(100);
        assert_eq!(c.unparsed(), b"*1\r\n$4\r\nPI");
        assert_eq!(c.consumed, 0);
        c.prepare_read(4 * 1024 * 1024);
        assert!(c.read_buf.len() - c.filled() >= 4 * 1024 * 1024);
        c.consumed = c.filled();
        c.prepare_read(0);
        assert!(c.read_buf.len() <= READ_BUF_SHRINK, "an idle large buffer shrinks");
    }

}
