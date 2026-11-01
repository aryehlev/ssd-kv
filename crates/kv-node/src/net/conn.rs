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
use std::net::SocketAddr;

use kv_cmd::CmdId;
use kv_resp::{Limits, Parser, Proto};

use crate::cmd::FanOutSplit;
use crate::runtime::ring::OutSeg;
use crate::runtime::xmsg::{OwnedCmd, ReplyBody};

/// Initial (and idle) size of `read_buf`.
pub const READ_BUF_INITIAL: usize = 16 * 1024;
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
    /// The connection is on the worker's "has bytes to parse" list.
    pub(crate) queued_for_parse: bool,
    /// The connection is on the worker's "has output to send" list.
    pub(crate) queued_for_send: bool,
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
            queued_for_parse: false,
            queued_for_send: false,
        }
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
        seq
    }

    /// A reply produced at once for a newly parsed command: straight to the output when no
    /// earlier reply is outstanding, else into the reorder buffer.
    pub fn reply_now(&mut self, body: ReplyBody) {
        let seq = self.next_req_seq;
        self.next_req_seq += 1;
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
    }

    /// Delivers the reply of `req_seq`; returns whether output became ready.
    pub fn deliver(&mut self, req_seq: u64, body: ReplyBody) -> bool {
        let Some(i) = req_seq.checked_sub(self.base_seq) else { return false };
        match self.slots.get_mut(i as usize) {
            Some(s @ Slot::Pending) => *s = Slot::Ready(body),
            _ => return false,
        }
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
        *p = Some(body);
        g.remaining -= 1;
        if g.remaining > 0 {
            return false;
        }
        let merged = merge(g);
        *slot = Slot::Ready(ReplyBody::Bytes(merged));
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
        match body {
            ReplyBody::Bytes(b) => {
                if self.out.is_empty() && self.out.capacity() < b.capacity() {
                    self.out = b;
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
