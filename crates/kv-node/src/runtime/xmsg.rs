//! Cross-worker messages (DESIGN §6.3, §6.8, §16.5, LC-37).
//!
//! One bounded `ArrayQueue<XMsg>` (capacity 4096) per ordered worker pair, used as SPSC.
//! Messages are owned values: a command's key and argument bytes are copied into one
//! `Box<[u8]>`. Doorbell: push, then `if dst.parked { write(eventfd, 1) }`; the receiver sets
//! `parked` and re-checks every inbound queue before waiting. A full destination queue keeps the
//! command in the connection's pending list and stops parsing that connection (§6.7); messages
//! are never dropped. Messages carry key bytes only: the owner partition computes key hashes
//! (LC-37).

use kv_common::shared_buf::SharedBuf;
use kv_storage::metastore::MetaDone;

use crate::cluster::agent::CtrlEvent;
use crate::dispatch::xtxn::XTxnMsg;

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
    Cmd { from: u16, conn: ConnRef, req_seq: u64, partition: u16, cmd: OwnedCmd, env: ExecEnv },
    Reply { conn: ConnRef, req_seq: u64, reply: ReplyBody },
    Part { conn: ConnRef, req_seq: u64, part: u16, reply: ReplyBody },
    Flush { db: Option<u8>, gather: GatherRef },
    XTxn(XTxnMsg),
    Meta { partition: u16, token: u64, done: MetaDone },
    Ctrl(CtrlEvent),
    ReplSocket { fd: i32, partition: u16, hello: Box<kv_repl::frame::Hello> },
    Shutdown,
}

/// A parsed command moved to its owner worker: argument `(offset, len)` pairs into `buf`.
pub struct OwnedCmd {
    pub id: kv_cmd::CmdId,
    pub buf: Box<[u8]>,
    pub args: smallvec::SmallVec<[(u32, u32); 8]>,
}

/// Encoded reply bytes; large value bodies travel as shared buffers sent as iovecs.
pub enum ReplyBody {
    Bytes(Vec<u8>),
    /// header bytes, then the shared body
    Shared(Vec<u8>, SharedBuf),
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
