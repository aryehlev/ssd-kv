//! Cross-worker messages (DESIGN §6.3, §6.8, §16.5, LC-37).
//!
//! One bounded `ArrayQueue<XMsg>` (capacity 4096) per ordered worker pair, used as SPSC.
//! Messages are owned values: a command's key and argument bytes are copied into one
//! `Box<[u8]>`. Doorbell: push, then `if dst.parked { write(eventfd, 1) }`; the receiver sets
//! `parked` and re-checks every inbound queue before waiting. A full destination queue keeps the
//! command in the connection's pending list and stops parsing that connection (§6.7); messages
//! are never dropped. Messages carry key bytes only: the owner partition computes key hashes
//! (LC-37).
//!
//! Threads that are not workers (meta thread reply hooks, agent, service thread) use the
//! worker's unbounded side queue ([`WorkerHandle::send_side`]) instead, so they never block and
//! never drop a message.

use std::sync::atomic::{Ordering, fence};

use kv_common::shared_buf::SharedBuf;
use kv_storage::keyspace::StoreError;
use kv_storage::metastore::MetaDone;
use smallvec::SmallVec;

use crate::cluster::agent::CtrlEvent;
use crate::dispatch::scan::{ScanOutcome, ScanReq};
use crate::dispatch::xtxn::XTxnMsg;
use crate::runtime::ring::sys;
use crate::shared::WorkerHandle;

/// Capacity of each worker-to-worker queue.
pub const XMSG_QUEUE_CAP: usize = 4096;

/// A connection on its home worker.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ConnRef {
    pub worker: u16,
    pub slot: u32,
    /// Generation of the connection slot (stale replies to a reused slot are dropped).
    pub generation: u32,
}

/// A pending gather (FLUSH*, multi-partition fan-out) on its home worker.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct GatherRef {
    pub worker: u16,
    pub id: u32,
}

pub enum XMsg {
    /// A keyed command for `partition` (owned by the destination worker). `part` is set for a
    /// sub-request of a fan-out (§6.8): its reply comes back as [`XMsg::Part`]. `credit`: the
    /// command carries the txn credit taken before it was consumed (§14.6 step 0); `lv`: the
    /// large-value reservation of its buffer (§6.9).
    Cmd {
        from: u16,
        conn: ConnRef,
        req_seq: u64,
        partition: u16,
        cmd: OwnedCmd,
        env: ExecEnv,
        part: Option<u16>,
        credit: bool,
        lv: crate::net::budget::Reservation,
    },
    Reply { conn: ConnRef, req_seq: u64, reply: ReplyBody },
    Part { conn: ConnRef, req_seq: u64, part: u16, reply: ReplyBody },
    /// FLUSHDB (`Some(db)`) / FLUSHALL (`None`) of every partition the destination owns.
    Flush { db: Option<u8>, gather: GatherRef },
    XTxn(XTxnMsg),
    Meta { partition: u16, token: u64, done: MetaDone },
    Ctrl(CtrlEvent),
    ReplSocket { fd: i32, partition: u16, hello: Box<kv_repl::frame::Hello> },
    /// A replication transport event for a partition of the destination (§23.7).
    Repl(crate::cluster::net::ReplEvent),
    Shutdown,
    /// One SCAN/KEYS/RANDOMKEY step on a partition of the destination (§16.6, LC-18).
    Scan { from: u16, conn: ConnRef, req_seq: u64, req: ScanReq },
    /// The result of a [`XMsg::Scan`] step.
    ScanDone { conn: ConnRef, req_seq: u64, out: ScanOutcome },
    /// Durability barrier (SAVE, BGSAVE, SHUTDOWN drain) over every partition the destination
    /// owns: answered when every effect executed so far is durable.
    Barrier { gather: GatherRef },
    /// One worker's answer to a [`XMsg::Flush`] or [`XMsg::Barrier`].
    GatherDone { gather: GatherRef, from: u16, res: Result<(), StoreError> },
    /// An EXEC body whose keys share `partition` (§8.5), run as one partition request.
    Exec { from: u16, conn: ConnRef, req_seq: u64, partition: u16, cmds: Vec<OwnedCmd>, env: ExecEnv, credit: bool },
    /// CLIENT LIST / CLIENT KILL over the destination's connections (`caller` = the asking
    /// client's id).
    Clients { gather: GatherRef, op: std::sync::Arc<crate::cmd::NodeOp>, caller: u64 },
    /// One worker's answer to [`XMsg::Clients`]: CLIENT LIST lines, connections killed.
    ClientsDone { gather: GatherRef, lines: Vec<u8>, killed: u64 },
}

/// A parsed command moved to its owner worker: argument `(offset, len)` pairs into `buf`.
pub struct OwnedCmd {
    pub id: kv_cmd::CmdId,
    pub buf: Box<[u8]>,
    pub args: smallvec::SmallVec<[(u32, u32); 8]>,
}

impl OwnedCmd {
    /// Copies `argv` into one buffer (one allocation; the argument vector stays inline for
    /// up to 8 arguments).
    pub fn new(id: kv_cmd::CmdId, argv: &[&[u8]]) -> Self {
        let total: usize = argv.iter().map(|a| a.len()).sum();
        let mut buf = Vec::with_capacity(total);
        let mut args = SmallVec::with_capacity(argv.len());
        for a in argv {
            args.push((buf.len() as u32, a.len() as u32));
            buf.extend_from_slice(a);
        }
        OwnedCmd { id, buf: buf.into_boxed_slice(), args }
    }

    /// The argument slices (index 0 = command name).
    #[inline]
    pub fn argv(&self) -> SmallVec<[&[u8]; 8]> {
        self.args.iter().map(|&(o, l)| &self.buf[o as usize..(o + l) as usize]).collect()
    }

    /// Argument `i`.
    #[inline]
    pub fn arg(&self, i: usize) -> &[u8] {
        let (o, l) = self.args[i];
        &self.buf[o as usize..(o + l) as usize]
    }

    #[inline]
    pub fn argc(&self) -> usize {
        self.args.len()
    }
}

/// Encoded reply bytes; large value bodies travel as shared buffers sent as iovecs.
pub enum ReplyBody {
    Bytes(Vec<u8>),
    /// header bytes, then the shared body: a bulk string whose `$<len>\r\n` header (and any
    /// bytes before it) is in the first field; the terminating CRLF after the body is implied.
    Shared(Vec<u8>, SharedBuf),
}

impl ReplyBody {
    /// Total encoded length.
    pub fn len(&self) -> usize {
        match self {
            ReplyBody::Bytes(b) => b.len(),
            ReplyBody::Shared(h, b) => h.len() + b.len() + 2,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// A contiguous view (copies a shared body; used for gathers of small part replies).
    pub fn to_vec(&self) -> Vec<u8> {
        match self {
            ReplyBody::Bytes(b) => b.clone(),
            ReplyBody::Shared(h, b) => {
                let mut v = h.clone();
                v.extend_from_slice(b.as_bytes());
                v.extend_from_slice(b"\r\n");
                v
            }
        }
    }
}

/// Connection state a command executes with.
#[derive(Clone, Copy)]
pub struct ExecEnv {
    pub proto: kv_resp::Proto,
    pub db: u8,
    pub now_ms: u64,
    pub readonly_conn: bool,
    pub conn_id: u64,
}

impl WorkerHandle {
    /// Sends from worker `from` over its SPSC queue to this worker; gives the message back when
    /// the queue is full (the sender keeps it and stops parsing that connection, §6.3).
    #[inline]
    #[allow(clippy::result_large_err)] // the message itself comes back; nothing is dropped
    pub fn try_send(&self, from: u16, msg: XMsg) -> Result<(), XMsg> {
        self.inbox[usize::from(from)].push(msg)?;
        self.wake();
        Ok(())
    }

    /// Sends from a thread that is not a worker (never blocks, never drops).
    pub fn send_side(&self, msg: XMsg) {
        self.side.push(msg);
        self.wake();
    }

    /// Rings the doorbell if the worker is (about to be) parked.
    #[inline]
    pub fn wake(&self) {
        // Pairs with the receiver's `parked` store + queue re-check: either the receiver sees
        // the message, or this load sees `parked == true` and the eventfd wakes it.
        fence(Ordering::SeqCst);
        if self.parked.load(Ordering::SeqCst) {
            sys::eventfd_signal(self.eventfd);
        }
    }

    /// Receiver side, before a waiting enter: announces parking and reports whether any inbound
    /// queue already holds a message (then the caller must not wait).
    pub fn park(&self) -> bool {
        self.parked.store(true, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        let pending = !self.side.is_empty() || self.inbox.iter().any(|q| !q.is_empty());
        if pending {
            self.parked.store(false, Ordering::Release);
        }
        !pending
    }

    /// Receiver side, after waking.
    #[inline]
    pub fn unpark(&self) {
        self.parked.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::shared::NodeShared;
    use std::sync::Arc;

    fn reply(n: u8) -> XMsg {
        XMsg::Reply { conn: ConnRef { worker: 0, slot: 0, generation: 0 }, req_seq: u64::from(n), reply: ReplyBody::Bytes(vec![n]) }
    }

    /// Every queue slot holds one `XMsg` (W^2 queues of 4096 slots are allocated up front):
    /// keep the message small, larger payloads go behind a box.
    #[test]
    fn messages_stay_small() {
        assert!(std::mem::size_of::<XMsg>() <= 176, "XMsg is {} bytes", std::mem::size_of::<XMsg>());
        assert!(std::mem::size_of::<OwnedCmd>() <= 96);
    }

    #[test]
    fn owned_cmd_copies_arguments_once() {
        let c = OwnedCmd::new(kv_cmd::CmdId::Set, &[b"SET", b"k", b"", b"value"]);
        assert_eq!(c.argc(), 4);
        assert_eq!(c.argv().as_slice(), &[&b"SET"[..], b"k", b"", b"value"]);
        assert_eq!(c.arg(3), b"value");
        assert_eq!(c.buf.len(), 9);
    }

    #[test]
    fn queues_are_bounded_and_fifo() {
        let s = NodeShared::new(Config::default(), 4, 2).unwrap();
        let w1 = &s.workers[1];
        for i in 0..XMSG_QUEUE_CAP {
            assert!(w1.try_send(0, reply((i % 250) as u8)).is_ok());
        }
        assert!(w1.try_send(0, reply(1)).is_err(), "a full queue gives the message back");
        for i in 0..XMSG_QUEUE_CAP {
            match w1.inbox[0].pop() {
                Some(XMsg::Reply { req_seq, .. }) => assert_eq!(req_seq, (i % 250) as u64),
                _ => panic!("FIFO order"),
            }
        }
    }

    #[test]
    fn park_sees_pending_messages_and_doorbell_wakes() {
        let s = Arc::new(NodeShared::new(Config::default(), 4, 2).unwrap());
        let w0 = &s.workers[0];
        assert!(w0.park(), "nothing pending: may wait");
        w0.unpark();
        w0.send_side(XMsg::Shutdown);
        assert!(!w0.park(), "a pending message forbids waiting");
        let _ = w0.side.pop();

        // a parked receiver blocked in the ring is woken by a sender on another thread
        let mut ring = crate::runtime::ring::Ring::new().unwrap();
        ring.arm_doorbell(w0.eventfd).unwrap();
        assert!(w0.park());
        let s2 = s.clone();
        let t = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            s2.workers[0].try_send(1, reply(7)).ok();
        });
        let mut out = Vec::new();
        while !out.iter().any(|c| matches!(c, crate::runtime::ring::Completion::Doorbell)) {
            ring.submit_and_wait(1).unwrap();
            ring.reap(&mut out);
        }
        w0.unpark();
        t.join().unwrap();
        assert!(matches!(w0.inbox[1].pop(), Some(XMsg::Reply { req_seq: 7, .. })));
    }
}
