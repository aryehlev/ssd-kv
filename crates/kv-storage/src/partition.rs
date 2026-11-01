//! The partition state machine (DESIGN §6.5, §14, §15, §16, §19).
//!
//! One `PartitionStore` per partition, single-threaded on its owner worker. Pipeline:
//! `submit(Request)` computes key hashes and index candidates and joins per-key FIFO queues
//! (commands on the same key execute in arrival order, LC-15); needed pages and extents are
//! prefetched; `take_ready` yields requests at the head of all their key queues with every page
//! present; `exec` runs the command's closure synchronously with no I/O against a [`Keyspace`];
//! effects are staged into page states, the index and (cluster) a replication [`Group`];
//! `tick` builds and submits page images (including foreground scrubs of commands executed this
//! tick) and then runs background tasks under their static rates. Tickets report when a
//! command's effects are written / durable.
//!
//! A consumed command waits only for its own I/O and for earlier commands on its keys, never
//! for load, a memory budget or a count (LC-14, LC-15).

use kv_common::{Now, RecordCrc};
use kv_index::{PartitionIndex, VType};
use smallvec::SmallVec;

use crate::cache::PageCache;
use crate::effect::Group;
use crate::flush::{FlushTracker, WriteId};
use crate::io::{IoDone, IoReq, PageUpdateMode};
use crate::keyspace::{Keyspace, StoreError};
use crate::layout::Seq;
use crate::metastore::{MetaClient, MetaDone};
use crate::recovery::{RecoveryMode, RecoveryProgress};
use crate::snapshot::SnapshotWriter;
use crate::ticket::{TicketId, TicketState};

/// When replies are released (§14.7).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Durability {
    Async,
    Written,
    Flushed,
}

/// Write granularity of page images (§13.6).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WriteGranularity {
    Page,
    Lba,
}

/// Per-partition shares of the node-total background rates (§19, §20.2).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct BgRates {
    pub scrub_iops: u64,
    pub warm_iops: u64,
    pub expire_reads: u64,
    pub reclaim_iops: u64,
    pub purge_iops: u64,
    pub sync_mbps: u64,
    pub sync_iops: u64,
}

#[derive(Clone, Copy)]
pub struct PartitionCfg {
    pub id: u16,
    pub partitions: u16,
    pub databases: u8,
    pub durability: Durability,
    pub granularity: WriteGranularity,
    pub page_update: PageUpdateMode,
    pub page_slack_cap: u16,
    pub page_slack_atomic: u16,
    pub hash_seed: u64,
    pub boot_gen: u32,
    pub crc: RecordCrc,
    pub lba: u32,
    /// Produce replication groups (cluster primary).
    pub produce_groups: bool,
    pub cluster: bool,
    pub rates: BgRates,
}

/// A submitted request.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ReqId(pub u32);

/// Where a request comes from.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Origin {
    Client,
    Replicated(Seq),
    Image(Seq),
    Snapshot(Seq),
    Migration { src_seq: Seq },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReqKind {
    Keyed,
    Txn { txn_id: u64 },
    ScanStep { db: u8, cursor: u64, count: u32, vtype: Option<VType> },
    RandomKey { db: u8, r: u64 },
    FlushDb { db: Option<u8> },
    /// sync/migration source walk via scan_pages (page refs)
    SlotIter { slot: u16, fp_from: u32, max_entries: u32 },
    /// merge-sync final deletes of unmarked entries
    SyncPrune { slot: u16, fp_from: u32 },
    Barrier,
}

/// A partition request: key bytes and access modes only (the partition computes key hashes).
pub struct Request {
    pub db: u8,
    pub keys: SmallVec<[(Box<[u8]>, kv_common::Access); 2]>,
    pub kind: ReqKind,
    pub origin: Origin,
}

/// Side outputs of an executed request.
pub struct ExecOut {
    pub ticket: Option<TicketId>,
    pub group: Option<Group>,
    pub seq: Option<Seq>,
}

pub enum ExecStatus<R> {
    Done(R, ExecOut),
    /// Needs pages that were not prefetched; re-queued until they are loaded.
    Requeued,
    Failed(StoreError),
}

/// Worker resources lent to a partition call.
pub struct StorageCtx<'a> {
    pub cache: &'a mut PageCache,
    pub io: &'a mut Vec<IoReq>,
    /// One flush tracker per device.
    pub flush: &'a mut [FlushTracker],
    pub meta: &'a MetaClient,
    pub now: Now,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PartState {
    Loading,
    Ready,
    ReadOnly,
}

/// Plain per-partition counters, mirrored into atomics by the worker every tick (§21).
#[derive(Clone, Debug, Default)]
pub struct PartitionCounters {
    /// Keys per db (DBSIZE, INFO keyspace).
    pub keys: [u64; 16],
    /// Keys with a TTL per db.
    pub expires: [u64; 16],
    pub keyspace_hits: u64,
    pub keyspace_misses: u64,
    pub expired_keys: u64,
    pub page_reads: u64,
    pub page_writes_update: u64,
    pub page_writes_insert: u64,
    pub page_writes_delete: u64,
    pub page_writes_move: u64,
    pub page_writes_scrub: u64,
    pub page_writes_txn: u64,
    pub extent_writes: u64,
    pub host_bytes_written: u64,
    pub user_bytes_written: u64,
    pub sector_writes: u64,
    pub full_page_writes: u64,
    pub moves: u64,
    pub fp_false_positive_reads: u64,
    pub stale_copies: u64,
    pub foreground_scrub_writes: u64,
    pub txn_markers: u64,
    pub flush_reclaim_pending_pages: u64,
    pub corrupt_pages: u64,
    pub unexpected_orphans: u64,
    pub index_bytes: u64,
    pub alloc_state_bytes: u64,
    pub dirty_bytes: u64,
}

pub struct PartitionStore {
    cfg: PartitionCfg,
    state: PartState,
    index: PartitionIndex,
    next_seq: Seq,
    counters: PartitionCounters,
}

impl PartitionStore {
    pub fn new(cfg: PartitionCfg) -> Self {
        todo!("DESIGN §6.5: partition construction")
    }

    pub fn state(&self) -> PartState {
        self.state
    }

    pub fn submit(&mut self, cx: &mut StorageCtx<'_>, req: Request) -> ReqId {
        todo!("DESIGN §6.5: submit")
    }

    pub fn take_ready(&mut self, out: &mut Vec<ReqId>) {
        todo!("DESIGN §6.5: ready requests")
    }

    pub fn exec<R>(
        &mut self,
        cx: &mut StorageCtx<'_>,
        id: ReqId,
        f: &mut dyn FnMut(&mut Keyspace<'_>) -> Result<R, StoreError>,
    ) -> ExecStatus<R> {
        todo!("DESIGN §6.5: exec")
    }

    /// SCAN/KEYS step (§16.6): keys of the step's pages that pass the span, liveness, expiry,
    /// TYPE and `matcher` (MATCH) filters; returns the next cursor.
    pub fn scan_keys(
        &mut self,
        cx: &mut StorageCtx<'_>,
        id: ReqId,
        matcher: &dyn Fn(&[u8]) -> bool,
        out: &mut Vec<Box<[u8]>>,
    ) -> ExecStatus<u64 /*next cursor*/> {
        todo!("DESIGN §16.6: scan step")
    }

    pub fn on_io_done(&mut self, cx: &mut StorageCtx<'_>, d: IoDone) {
        todo!("DESIGN §6.4: I/O completion")
    }

    pub fn on_durable(&mut self, writes: &[WriteId]) {
        todo!("DESIGN §13.4: durable images")
    }

    pub fn on_meta_done(&mut self, cx: &mut StorageCtx<'_>, m: MetaDone) {
        todo!("DESIGN §6.4: meta completion")
    }

    /// build/submit images, background tasks
    pub fn tick(&mut self, cx: &mut StorageCtx<'_>) {
        todo!("DESIGN §6.4: partition tick")
    }

    pub fn take_tickets(&mut self, out: &mut Vec<(TicketId, TicketState)>) {
        todo!("DESIGN §13.9: ticket updates")
    }

    pub fn next_seq(&self) -> Seq {
        self.next_seq
    }

    /// Starts a new term: the counter restarts at 1 (§10.11, §23.9).
    pub fn set_term(&mut self, term: u32) {
        todo!("DESIGN §23.9: new term")
    }

    /// driven to completion by the worker
    pub fn recover(&mut self, cx: &mut StorageCtx<'_>, mode: RecoveryMode) -> RecoveryProgress {
        todo!("DESIGN §18: recovery")
    }

    /// true when done
    pub fn snapshot_section(&mut self, cx: &mut StorageCtx<'_>, w: &mut SnapshotWriter) -> bool {
        todo!("DESIGN §10.10: snapshot section")
    }

    pub fn counters(&self) -> &PartitionCounters {
        &self.counters
    }
}

