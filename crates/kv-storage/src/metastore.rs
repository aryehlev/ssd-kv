//! The meta thread: fixed-location A/B metadata and the AG pool (DESIGN §6.1, §10.2-§10.7,
//! §11.1, §14.6, §17.6).
//!
//! All superblock, AG table, partition meta, node meta and intent table I/O happens on this one
//! thread, never touching the index or pages. Requests come from workers through
//! [`MetaClient`]; replies go back through per-worker hooks (`XMsg::Meta` in kv-node). AG pool:
//! a free-AG bitmap per device built from the AG tables; `AcquireAg` picks the device with the
//! most free AGs (ties by a keyed draw) and a uniformly drawn free AG on it, never the lowest
//! index (LC-01), and replies only after the AG table block is durable (E11). Commit batching
//! (§14.6): every `CommitTxn` that arrived since the previous slot write is written into the
//! chosen slot(s) in one flush and answered with `Committed`; `resolved_w` advances when every
//! txn id up to it is resolved and is answered with `Resolved { credits }` (E10). Clock floor and
//! replication positions are persisted on their own cadence (§19).

use crate::agtable::AgKind;
use crate::io::BlockDevice;
use crate::layout::{AgId, Seq};
use crate::pmeta::{NodeMeta, PartitionMeta};
use crate::superblock::SbUpdate;

pub enum MetaReq {
    AcquireAg { partition: u16, kind: AgKind },
    ReleaseAg { ag: AgId },
    PartitionMeta { m: PartitionMeta },
    NodeMeta { m: NodeMeta },
    ReserveSeq { partition: u16, floor: Seq },
    /// batched, §14.6
    CommitTxn { txn: u64 },
    /// records zeroed
    TxnAborted { txn: u64 },
    Superblock(SbUpdate),
    Barrier,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MetaDone {
    Ag(Option<AgId>),
    Ok,
    Committed { txn: u64 },
    /// W advanced
    Resolved { credits: u32 },
    Failed(i32),
}

/// One queued request.
pub(crate) struct MetaMsg {
    pub(crate) partition: u16,
    pub(crate) token: u64,
    pub(crate) req: MetaReq,
}

/// Sending side of the meta thread's request queue (one clone per worker).
#[derive(Clone)]
pub struct MetaClient {
    tx: std::sync::mpsc::Sender<MetaMsg>,
}

impl MetaClient {
    /// Queues `r`; the reply arrives through the requesting worker's hook with `(partition, token)`.
    pub fn request(&self, partition: u16, token: u64, r: MetaReq) {
        todo!("DESIGN §6.1: meta request")
    }
}

pub struct MetaStore;

impl MetaStore {
    /// Starts the meta thread over the node's devices; `reply[w]` delivers completions to
    /// worker `w`.
    pub fn start(
        devs: Vec<Box<dyn BlockDevice + Send>>,
        reply: Vec<Box<dyn Fn(u16, u64, MetaDone) + Send>>,
    ) -> (MetaClient, std::thread::JoinHandle<()>) {
        todo!("DESIGN §6.1: meta thread")
    }
}
