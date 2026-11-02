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
//! for load, a memory budget or a count (LC-14, LC-15). Every wait is registered where its event
//! is delivered and every waiter names that event: see [`crate::progress`] for the table of
//! waits and wakeups, the structural check ([`PartitionStore::check_progress`]), the quiescence
//! check and the stall counters (`requests_waiting_over_1s`, `oldest_wait_ms`).
//!
//! Worker contract (kv-node, [`crate::engine::LocalEngine`], the simulator):
//! * route every [`IoDone`] whose tag names this partition to [`PartitionStore::on_io_done`];
//!   completions of flush requests (tag partition [`crate::flush::FLUSH_TAG_PARTITION`]) go to
//!   the device's [`FlushTracker`], whose durable [`WriteId`]s go to
//!   [`PartitionStore::on_durable`] (writes of other partitions are ignored);
//! * meta replies go to [`PartitionStore::on_meta_done`] in the order the meta thread sends them
//!   (it answers one partition's requests in request order; `Resolved` and the `Committed` of a
//!   cross-partition txn part are recognised without consuming a request);
//! * call [`PartitionStore::tick`] once per loop iteration after executing ready requests, then
//!   [`FlushTracker::poll`] and [`PartitionStore::take_tickets`];
//! * a closure whose `exec` returned [`ExecStatus::Requeued`] is run again from the start once
//!   the request is ready again: discard everything it produced (reply bytes).

use std::collections::VecDeque;
use std::sync::Arc;

use hashbrown::{HashMap, HashSet};
use kv_common::pace::TokenBucket;
use kv_common::{Access, KeyHash, Now, RecordCrc, key_slot};
use kv_index::{Entry, Kind, PageRef, PartitionIndex, Pos, ScanFilter, VType, cursor};
use smallvec::SmallVec;

use crate::agtable::AgKind;
use crate::alloc::{Alloc, ExtPlace, KeyedPick, PAGE_ROOM, SmallPlace};
use crate::cache::PageCache;
use crate::effect::{Group, Op};
use crate::engine::Boot;
use crate::extent::{
    self, CHUNK_MAX_PAYLOAD, ChunkRef, EXT_FLAG_HAS_EXPIRE, EXT_FLAG_TXN, EXT_KIND_CHUNK, EXT_KIND_CHUNKMAP,
    EXT_KIND_VALUE, EXT_VTYPE_HASH, EXT_VTYPE_STRING, ExtHeader,
};
use crate::flush::{FlushTracker, WriteId};
use crate::image::{self, BuildEnv, EncodedRecord, Granularity, Inflight, InflightState, PageState, Pending, RecDesc, RecStatus};
use crate::io::{AlignedBuf, IoDone, IoReq, IoTag, PageUpdateMode};
use crate::keyspace::{Change, ChunkFetch, Cur, ExtFetch, Keyspace, Need, Staged, StoreError};
use crate::layout::{AgId, EXT_HDR, PAGE, PageId, SMALL_REC_MAX, Seq, class_for, class_pages, pages_for};
use crate::metastore::{MetaClient, MetaDone, MetaReq};
use crate::page::{PageParse, parse_small_page};
use crate::pmeta::PartitionMeta;
use crate::record::{
    REC_FLAG_HAS_EXPIRE, REC_FLAG_TXN, REC_KIND_HASH, REC_KIND_STRING, REC_KIND_TXN_DELETE_MARKER, RecHeader, encode_small,
    small_len,
};
use crate::recovery::{RecoveryMode, RecoveryProgress};
use crate::snapshot::SnapshotWriter;
use crate::stale::{CopyState, StaleCopies, StaleRef};
use crate::ticket::{TICKET_HAS_DELETE, TICKET_HAS_META, TicketId, TicketState, Tickets};

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

impl BgRates {
    /// Per-partition shares of the §20.2 node totals for `partitions` partitions.
    pub fn defaults(partitions: u16) -> Self {
        let p = u64::from(partitions.max(1));
        BgRates {
            scrub_iops: (4000 / p).max(1),
            warm_iops: (8000 / p).max(1),
            expire_reads: (20_000 / p).max(1),
            reclaim_iops: (4000 / p).max(1),
            purge_iops: (4000 / p).max(1),
            sync_mbps: 200,
            sync_iops: 5000,
        }
    }
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

impl ExecOut {
    fn none() -> Self {
        ExecOut { ticket: None, group: None, seq: None }
    }
}

pub enum ExecStatus<R> {
    Done(R, ExecOut),
    /// Needs pages that were not prefetched (or waits for space or a sequence reservation);
    /// re-queued until it can run.
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
    /// Tracked sources released by the page writer's newest-durable-copy check rather than by
    /// the durability event of their newer copy (expected 0; a lost hand-off, liveness I15).
    pub missed_handoffs: u64,
    /// Live requests and tickets older than one second (a stall; expected 0).
    pub requests_waiting_over_1s: u64,
    /// Age of the oldest live request or ticket, in milliseconds.
    pub oldest_wait_ms: u64,
    /// Requests and tickets that ever stayed live for more than one second.
    pub stalled_total: u64,
    /// Work of the active-expiry RAM sweep (hint positions and table visits), paced by time.
    pub expire_sweep_work: u64,
    pub index_bytes: u64,
    pub alloc_state_bytes: u64,
    pub dirty_bytes: u64,
}

/// Seqs reserved per `seq_floor` write (§10.5).
pub(crate) const SEQ_RESERVE: u64 = 1 << 24;
/// Positions of the RAM expiry sweep per tick at most (§16.4); the sweep is paced by time
/// (`expire::SWEEP_PASS_NS`).
const EXPIRY_BUDGET: usize = 64 * 1024;
/// Empty tables a SCAN step walks past at most (§16.6).
const SCAN_EMPTY_TABLES: u16 = 4096;
/// Hint rebase period (§9.5).
const REBASE_PERIOD_S: u64 = 3600;
/// Records the page writer moves to another page per tick (§14.3); more wait for the next.
const RELOCATIONS_PER_TICK: u32 = 64;
/// Slot tables the expiry sweep visits per tick (empty ones cost little but are bounded too).
const SWEEP_TABLES_PER_TICK: u32 = 256;

// -------------------------------------------------------------------------------------------
// Requests

pub(crate) struct KeyInfo {
    pub(crate) kh: KeyHash,
    pub(crate) slot: u16,
}

pub(crate) struct Req {
    pub(crate) req: Request,
    pub(crate) keys: SmallVec<[KeyInfo; 2]>,
    pub(crate) order: u64,
    pub(crate) generation: u32,
    /// Loads and reads outstanding for this request.
    pub(crate) io: u32,
    /// Key queues where it is not at the head.
    pub(crate) waits: u32,
    pub(crate) barrier: bool,
    pub(crate) blocked: bool,
    pub(crate) queued: bool,
    /// Parked until an AG, a seq reservation or a txn id arrives.
    pub(crate) parked: bool,
    pub(crate) pins: SmallVec<[PageId; 2]>,
    pub(crate) fetched: SmallVec<[ExtFetch; 1]>,
    pub(crate) failed: Option<StoreError>,
    /// Monotonic submit time (age tracking, stall counters).
    pub(crate) born_ns: u64,
    /// Counted once in the stall counter.
    pub(crate) stalled: bool,
    /// Reported once by the stall trace (`KVSSD_STALL_TRACE_MS`).
    pub(crate) traced: bool,
}

#[derive(Default)]
pub(crate) struct Requests {
    pub(crate) slots: Vec<Option<Req>>,
    pub(crate) gens: Vec<u32>,
    pub(crate) free: Vec<u32>,
    pub(crate) busy: HashMap<(u8, u64), VecDeque<ReqId>, foldhash::fast::FixedState>,
    pub(crate) ready: VecDeque<ReqId>,
    pub(crate) order: VecDeque<(u64, ReqId)>,
    pub(crate) barriers: VecDeque<(u64, ReqId)>,
    /// Requests held behind a barrier (re-checked when it finishes).
    pub(crate) blocked: Vec<ReqId>,
    /// Executed local multi-key txns that keep their key queues until the commit is durable
    /// (a later command on one of their keys waits for it, LC-15): `txn -> request`.
    pub(crate) held: HashMap<u64, ReqId, foldhash::fast::FixedState>,
    pub(crate) next_order: u64,
    pub(crate) live: usize,
}

impl Requests {
    pub(crate) fn get(&self, id: ReqId) -> Option<&Req> {
        self.slots.get(id.0 as usize).and_then(Option::as_ref)
    }

    fn get_mut(&mut self, id: ReqId) -> Option<&mut Req> {
        self.slots.get_mut(id.0 as usize).and_then(Option::as_mut)
    }

    fn insert(&mut self, mut r: Req) -> ReqId {
        let i = match self.free.pop() {
            Some(i) => i,
            None => {
                self.slots.push(None);
                self.gens.push(0);
                (self.slots.len() - 1) as u32
            }
        };
        self.gens[i as usize] = self.gens[i as usize].wrapping_add(1);
        r.generation = self.gens[i as usize];
        self.slots[i as usize] = Some(r);
        self.live += 1;
        ReqId(i)
    }

    fn live_gen(&self, id: ReqId, generation: u32) -> bool {
        self.get(id).is_some_and(|r| r.generation == generation)
    }

    /// The oldest live request's order number.
    fn oldest(&mut self) -> Option<u64> {
        while let Some(&(o, id)) = self.order.front() {
            if self.get(id).is_some_and(|r| r.order == o) {
                return Some(o);
            }
            self.order.pop_front();
        }
        None
    }

    fn check_ready(&mut self, id: ReqId) {
        let oldest = self.oldest();
        let first_barrier = self.barriers.front().map(|&(o, _)| o);
        let Some(r) = self.get_mut(id) else { return };
        let blocked = !r.barrier && first_barrier.is_some_and(|o| o < r.order);
        let newly_blocked = blocked && !r.blocked;
        r.blocked = blocked;
        let barrier_ok = !r.barrier || oldest == Some(r.order);
        if !r.queued && !r.parked && r.io == 0 && r.waits == 0 && !blocked && barrier_ok {
            r.queued = true;
            self.ready.push_back(id);
        }
        if newly_blocked {
            self.blocked.push(id);
        }
    }
}

// -------------------------------------------------------------------------------------------
// I/O bookkeeping

/// What an outstanding request of this partition is for.
#[derive(Clone, Copy)]
pub(crate) enum IoKind {
    PageRead { page: PageId, retried: bool },
    PageWrite { page: PageId },
    ExtRead { req: ReqId, generation: u32, idx: usize, chunk: Option<usize> },
    ExtWrite { start: PageId },
    ExtZero { start: PageId },
    /// One-page header read of a background task.
    HeaderRead { start: PageId, why: HeaderWhy },
    Recovery { id: u64 },
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum HeaderWhy {
    /// Active expiry verification of an extent entry (§16.4).
    Expire { db: u8, slot: u16, fp32: u32 },
    /// FLUSHDB reclaim walk (§16.5).
    Reclaim,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Durable {
    Page(PageId),
    Ext(PageId),
    Zero(PageId),
}

/// Why a page is being loaded.
#[derive(Default)]
pub(crate) struct Load {
    pub(crate) waiters: SmallVec<[(ReqId, u32); 2]>,
    pub(crate) scrub: bool,
    /// Active expiry candidates of this page: `(db, slot, fp32, x8)`.
    pub(crate) expire: SmallVec<[(u8, u16, u32, u8); 2]>,
}

// -------------------------------------------------------------------------------------------
// Extents

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum ExtState {
    Pending,
    Writing,
    AwaitFlush,
}

/// A VALUE, CHUNKMAP or CHUNK extent write (staged or in flight).
pub(crate) struct ExtWrite {
    pub(crate) hdr: ExtHeader,
    pub(crate) db: u8,
    pub(crate) key: Box<[u8]>,
    /// The whole extent image; `None` while the device holds it.
    pub(crate) buf: Option<AlignedBuf>,
    pub(crate) tickets: SmallVec<[TicketId; 1]>,
    pub(crate) state: ExtState,
    /// CHUNKMAP: chunk writes not durable yet (E3).
    pub(crate) wait_chunks: u32,
    /// CHUNK: the chunkmap it belongs to.
    pub(crate) parent: Option<PageId>,
    /// Atomic in-place rewrite (§14.4 step 1).
    pub(crate) atomic: bool,
    /// Requests waiting for this write to complete (reads of a chunked or in-flight value).
    pub(crate) waiters: SmallVec<[(ReqId, u32); 1]>,
    wid: u64,
    /// The device failed the write (errno): the media never held it.
    pub(crate) failed: Option<i32>,
}

/// Why an extent's first sector is zeroed.
pub(crate) enum ZeroWhy {
    /// A tracked older copy (stale); clears the registry entry when durable.
    Stale { db: u8, key: Box<[u8]> },
    /// The key's newest copy is deleted: waits until no other copy is tracked (D1, E9).
    Delete { db: u8, key: Box<[u8]>, tickets: SmallVec<[TicketId; 1]> },
    /// Dead content (flush cutoff, uncommitted txn): only frees the run.
    Dead,
}

pub(crate) struct ExtZero {
    pub(crate) n_pages: u32,
    pub(crate) why: ZeroWhy,
    pub(crate) state: ExtState,
    /// Tickets besides the delete's that wait for this zeroing (a barrier).
    pub(crate) waiters: SmallVec<[TicketId; 1]>,
    wid: u64,
}

impl ExtZero {
    /// A zeroing waiting to be issued.
    pub(crate) fn pending(n_pages: u32, why: ZeroWhy) -> Self {
        ExtZero { n_pages, why, state: ExtState::Pending, waiters: SmallVec::new(), wid: 0 }
    }
}

#[derive(Default)]
pub(crate) struct ExtTables {
    pub(crate) writes: HashMap<PageId, ExtWrite, foldhash::fast::FixedState>,
    pub(crate) zeros: HashMap<PageId, ExtZero, foldhash::fast::FixedState>,
    /// CHUNKMAP start -> its chunks `(start, n_pages)`.
    pub(crate) chunkmaps: HashMap<PageId, SmallVec<[(PageId, u32); 4]>, foldhash::fast::FixedState>,
    /// CHUNK start -> chunkmaps on media or in flight referencing it.
    pub(crate) chunk_rc: HashMap<PageId, u32, foldhash::fast::FixedState>,
}

// -------------------------------------------------------------------------------------------
// Txns (§14.6)

pub(crate) struct OpenTxn {
    /// The command's ticket.
    pub(crate) ticket: Option<TicketId>,
    /// The partition requests the commit itself (single-partition txn); XTxn parts are
    /// committed by the coordinating worker.
    pub(crate) local: bool,
    pub(crate) requested: bool,
    pub(crate) keys: SmallVec<[(u8, Box<[u8]>); 2]>,
    /// The seq every record of the txn carries (one seq per command).
    pub(crate) seq: Seq,
    /// Tickets waiting for this txn's commit (one metadata dependency each): barriers, and
    /// the tickets of staged writes outside the txn that an effect of the txn superseded
    /// (`PartitionStore::inherit_into_txn`).
    pub(crate) barriers: SmallVec<[TicketId; 1]>,
    /// `(db, key, page, seq)` of the txn delete markers.
    pub(crate) markers: SmallVec<[MarkerAt; 1]>,
    pub(crate) pages: SmallVec<[PageId; 2]>,
}

/// A txn delete marker: `(db, key, page, seq)`.
type MarkerAt = (u8, Box<[u8]>, PageId, Seq);

/// A delete waiting for older copies: `(db, key, seq of the delete, tickets)`. It waits only
/// for tracked copies older than the delete (D1), and ends early when a later write of the key
/// is durable (D2); copies that later writes create never extend it.
pub(crate) type GoneWait = (u8, Box<[u8]>, Seq, SmallVec<[TicketId; 1]>);

/// A newest copy that stands for "absent" (a committed txn marker, or an expired record that
/// still has older copies) and is dropped once it is the key's only copy (D1, §16.2, §18.3).
pub(crate) struct RetireItem {
    pub(crate) db: u8,
    pub(crate) key: Box<[u8]>,
    pub(crate) page: PageId,
    pub(crate) seq: Seq,
}

/// A meta request whose reply is pending (FIFO, §6.1).
#[derive(Debug)]
pub(crate) enum MetaWait {
    Ag(AgKind),
    Seq(Seq),
    Cutoff { cutoff: u64, dbs: u16, ticket: TicketId, all: bool },
    Commit { txn: u64 },
    Plain,
}

/// Background pacing (§19).
pub(crate) struct Buckets {
    pub(crate) scrub: TokenBucket,
    pub(crate) warm: TokenBucket,
    pub(crate) expire: TokenBucket,
    pub(crate) reclaim: TokenBucket,
    pub(crate) release: TokenBucket,
}

// -------------------------------------------------------------------------------------------
// The partition core (everything `Keyspace` borrows)

pub(crate) struct Core {
    pub(crate) cfg: PartitionCfg,
    pub(crate) boot: Arc<Boot>,
    pub(crate) state: PartState,
    pub(crate) index: PartitionIndex,
    pub(crate) alloc: Alloc,
    pub(crate) stale: StaleCopies,
    pub(crate) tickets: Tickets,
    pub(crate) next_seq: Seq,
    /// Seqs below this are covered by a durable reservation (E12).
    pub(crate) seq_limit: Seq,
    pub(crate) seq_asked: bool,
    pub(crate) pmeta: PartitionMeta,
    pub(crate) cutoff: [u64; 16],
    pub(crate) cutoff_durable: [u64; 16],
    pub(crate) dirty: Vec<PageId>,
    pub(crate) ext: ExtTables,
    pub(crate) io: HashMap<u64, IoKind, foldhash::fast::FixedState>,
    pub(crate) next_io: u64,
    pub(crate) durable_wait: HashMap<u64, Durable, foldhash::fast::FixedState>,
    pub(crate) durable_q: Vec<Durable>,
    pub(crate) next_wid: u64,
    pub(crate) loads: HashMap<PageId, Load, foldhash::fast::FixedState>,
    pub(crate) meta_wait: VecDeque<MetaWait>,
    pub(crate) next_token: u64,
    pub(crate) counters: PartitionCounters,
    pub(crate) bufs: Vec<AlignedBuf>,
    pub(crate) expiry: crate::expire::ExpirySweep,
    pub(crate) buckets: Buckets,
    /// Pages whose tracked copies are zeroed by paced scrubs.
    pub(crate) scrub_queue: VecDeque<PageId>,
    /// Keys whose older copies a command waits for (foreground, this tick).
    pub(crate) fg_keys: Vec<(u8, Box<[u8]>)>,
    /// Pages holding effects deferred until other copies are zeroed (re-dirtied on change).
    pub(crate) deferred: HashSet<PageId, foldhash::fast::FixedState>,
    /// Pages to build in the next tick (their build could not run in this one).
    pub(crate) retry: Vec<PageId>,
    /// Pages whose read completed when no cache frame could be had: read again next tick.
    pub(crate) load_retry: Vec<PageId>,
    pub(crate) txns: HashMap<u64, OpenTxn, foldhash::fast::FixedState>,
    /// Txns committed this boot whose ids may still be above the durable `resolved_w`.
    pub(crate) committed: HashSet<u64, foldhash::fast::FixedState>,
    /// The durable `resolved_w` last seen: every txn id at or below it is valid (§10.7).
    pub(crate) resolved_w: u64,
    /// Ids of txns from before this boot known to be committed (recovery view).
    pub(crate) prior: Arc<crate::intent::CommittedSet>,
    /// FLUSHALL: AGs to release once no write is in flight and the cutoffs are durable.
    pub(crate) retiring_ags: Vec<AgId>,
    pub(crate) flushall_wait: bool,
    /// FLUSHDB reclaim walk (paced): SMALL pages to load, extent starts to check.
    pub(crate) reclaim_pages: VecDeque<PageId>,
    pub(crate) reclaim_ext: VecDeque<PageId>,
    pub(crate) space_wait: Vec<ReqId>,
    pub(crate) seq_wait: Vec<ReqId>,
    pub(crate) now: Now,
    pub(crate) failed: Option<i32>,
    pub(crate) writes_in_flight: u32,
    pub(crate) recovered_bytes: u64,
    pub(crate) recovery_mode: Option<RecoveryMode>,
    /// Deletes whose newest copy never reached media: waiting until no older copy is left
    /// (D2): `(db, key, tickets)`.
    pub(crate) gone_wait: Vec<GoneWait>,
    /// Absent newest copies waiting for their older copies to be zeroed.
    pub(crate) retire: Vec<RetireItem>,
    /// Pre-boot uncommitted txn records still on media (pages / extents being zeroed, E10);
    /// `None` once reported to the meta thread.
    pub(crate) prior_txn_zero: Option<u32>,
    /// Pages with pre-boot uncommitted txn records whose zeroing counts towards it.
    pub(crate) prior_txn_pages: HashSet<PageId, foldhash::fast::FixedState>,
    /// The clean-shutdown section was handed to the snapshot writer.
    pub(crate) snapshot_done: bool,
    /// Ticks run (cheap periodic work).
    pub(crate) ticks: u64,
    /// Monotonic time the stall counters were last refreshed.
    pub(crate) stall_checked_ns: u64,
    /// Relocations in the current tick.
    pub(crate) relocations: u32,
    /// The local txn the last commit opened (its request keeps its key queues, see `hold`).
    pub(crate) last_local_txn: Option<u64>,
    /// Tracked locations that also hold an older copy of their key: `(page, db, key)`.
    pub(crate) older_pages: Vec<(PageId, u8, Box<[u8]>)>,
    /// Tickets inherited by an effect of a txn whose `OpenTxn` the executing commit has not
    /// registered yet: `(txn, ticket)`, moved to its commit waiters at the end of the commit.
    pub(crate) txn_inherit: Vec<(u64, TicketId)>,
    /// Scratch vectors (no per-tick allocation).
    pub(crate) scratch_pos: Vec<Pos>,
    pub(crate) scratch_txn: Vec<u64>,
    /// Keys watched by clients (WATCH) and the watched keys changed since the last
    /// `PartitionStore::take_touched`.
    pub(crate) watch: WatchSet,
}

/// Watched keys of a partition (WATCH, §16): `[db] ++ key -> watchers`. Every committed change
/// of a watched key (a write, a delete, an expiry with or without a write, a FLUSHDB/FLUSHALL
/// of its db) reports the key once through [`PartitionStore::take_touched`]. Costs one emptiness
/// test per change while nothing is watched.
#[derive(Default)]
pub(crate) struct WatchSet {
    keys: HashMap<Box<[u8]>, u32, foldhash::fast::FixedState>,
    touched: Vec<(u8, Box<[u8]>)>,
}

impl WatchSet {
    fn tag(db: u8, key: &[u8]) -> Box<[u8]> {
        let mut t = Vec::with_capacity(key.len() + 1);
        t.push(db);
        t.extend_from_slice(key);
        t.into_boxed_slice()
    }

    fn watch(&mut self, db: u8, key: &[u8]) {
        *self.keys.entry(Self::tag(db, key)).or_insert(0) += 1;
    }

    fn unwatch(&mut self, db: u8, key: &[u8]) {
        let t = Self::tag(db, key);
        if let Some(n) = self.keys.get_mut(&t) {
            *n -= 1;
            if *n == 0 {
                self.keys.remove(&t);
            }
        }
    }

    /// A committed change of `(db, key)`.
    pub(crate) fn touch(&mut self, db: u8, key: &[u8]) {
        if self.keys.is_empty() {
            return;
        }
        if self.keys.contains_key(&Self::tag(db, key)) {
            self.touched.push((db, key.into()));
        }
    }

    /// Every watched key of the dbs in `mask` (bit i = db i) changed (FLUSHDB / FLUSHALL).
    fn touch_dbs(&mut self, mask: u16) {
        for t in self.keys.keys() {
            if mask & (1 << (t[0] & 15)) != 0 {
                self.touched.push((t[0], t[1..].into()));
            }
        }
    }
}

impl Core {
    pub(crate) fn crc(&self) -> RecordCrc {
        self.cfg.crc
    }

    pub(crate) fn take_buf(&mut self, len: usize) -> AlignedBuf {
        if len <= PAGE
            && let Some(mut b) = self.bufs.pop()
        {
            b.set_visible_len(len);
            return b;
        }
        AlignedBuf::zeroed(len)
    }

    pub(crate) fn give_buf(&mut self, mut b: AlignedBuf) {
        if b.capacity() == PAGE && self.bufs.len() < 64 {
            b.set_visible_len(PAGE);
            self.bufs.push(b);
        }
    }

    pub(crate) fn tag(&mut self, k: IoKind) -> IoTag {
        let id = self.next_io;
        self.next_io += 1;
        self.io.insert(id, k);
        IoTag { partition: self.cfg.id, id }
    }

    pub(crate) fn dev_lba(&self, dev: u8) -> u32 {
        self.boot.devs.get(usize::from(dev)).map_or(self.cfg.lba, |d| d.lba).max(512)
    }

    pub(crate) fn dev_mode(&self, dev: u8) -> PageUpdateMode {
        self.boot.devs.get(usize::from(dev)).map_or(self.cfg.page_update, |d| d.page_update)
    }

    fn dsync(&self, dev: u8) -> bool {
        self.boot.devs.get(usize::from(dev)).is_some_and(|d| d.dsync)
    }

    /// True when a record is dead: below a durable flush cutoff, or of a txn that never
    /// committed (§16.2).
    pub(crate) fn dead(&self, db: u8, seq: Seq, txn: u64) -> bool {
        if self.cutoff_durable.get(usize::from(db)).is_some_and(|&c| seq.0 < c) {
            return true;
        }
        txn != 0 && txn > self.resolved_w && !self.txns.contains_key(&txn) && !self.committed.contains(&txn) && !self.prior.is_committed(txn)
    }

    fn txn_open(&self, txn: u64) -> bool {
        self.txns.contains_key(&txn)
    }

    pub(crate) fn meta_req(&mut self, meta: &MetaClient, r: MetaReq, w: MetaWait) {
        let t = self.next_token;
        self.next_token += 1;
        self.meta_wait.push_back(w);
        meta.request(self.cfg.id, t, r);
    }

    pub(crate) fn mark_dirty(&mut self, cache: &mut PageCache, page: PageId) {
        let Some(f) = cache.peek(page) else { return };
        let Some(st) = cache.state_mut(f) else { return };
        let pin = !st.work_pin;
        st.work_pin = true;
        if !st.dirty {
            st.dirty = true;
            if st.inflight.is_none() {
                self.dirty.push(page);
            }
        }
        if pin {
            cache.pin(f);
        }
    }

    /// Releases the work pin of a page with nothing staged, in flight or wanted.
    pub(crate) fn maybe_unpin(cache: &mut PageCache, page: PageId) {
        let Some(f) = cache.peek(page) else { return };
        let Some(st) = cache.state_mut(f) else { return };
        if st.work_pin && st.is_clean() {
            st.work_pin = false;
            cache.unpin(f);
        }
    }

    pub(crate) fn note_page_space(&mut self, cache: &PageCache, page: PageId) {
        let Some(st) = cache.page_state(page) else { return };
        if st.corrupt {
            self.alloc.note_cached(page, 0, PAGE_ROOM, 0);
            return;
        }
        if st.fresh && st.pending.is_empty() && st.inflight.is_none() {
            return;
        }
        let used = st.used_bytes();
        let largest = st.largest();
        // a record needs one gap; the slack rule counts every free byte (§11.3)
        let free = PAGE_ROOM.saturating_sub(used);
        self.alloc.note_cached(page, free, largest, st.largest_gap());
    }

    // ---------------------------------------------------------------------------------------
    // Index helpers

    /// The highest seq among the copies of `(db, key)` that the index does not name: tracked
    /// older copies and an absent newest copy (`Seq::ZERO` when there is none).
    pub(crate) fn tracked_copy_seq(&self, db: u8, key: &[u8]) -> Seq {
        let tracked = self.stale.get(db, key).iter().map(|e| e.seq).max().unwrap_or(Seq::ZERO);
        tracked.max(self.stale.absent_of(db, key).map_or(Seq::ZERO, |a| a.seq))
    }

    /// True when a txn that is not committed yet wrote or deleted `(db, key)`.
    pub(crate) fn in_open_txn(&self, db: u8, key: &[u8]) -> bool {
        !self.txns.is_empty() && self.txns.values().any(|o| o.keys.iter().any(|(d, k)| *d == db && **k == *key))
    }

    /// The position of the entry of `(db, slot, kh)` naming `page36`.
    pub(crate) fn find_entry(&self, db: u8, slot: u16, kh: KeyHash, page36: u64) -> Option<Pos> {
        let t = self.index.table(db, slot)?;
        let mut c = SmallVec::new();
        t.candidates(kh, &mut c);
        c.iter().find(|c| c.entry.page36 == page36).map(|c| c.pos)
    }

    /// True when `(db, slot, kh)` has an entry of `kind` naming `page36`.
    pub(crate) fn indexed_at(&self, db: u8, slot: u16, kh: KeyHash, page36: u64) -> bool {
        self.find_entry(db, slot, kh, page36).is_some()
    }

    pub(crate) fn hint(&self, expire_at: Option<u64>) -> u16 {
        kv_index::hint::encode(expire_at, self.expiry.hint_base_s)
    }

    pub(crate) fn index_set_at(&mut self, db: u8, slot: u16, kh: KeyHash, cur36: Option<u64>, e: Entry, expire_at: Option<u64>) {
        let hint = self.hint(expire_at);
        let pos = cur36.and_then(|p| self.find_entry(db, slot, kh, p));
        let t = self.index.table_mut(db, slot);
        match pos {
            Some(pos) => {
                let mut e = e;
                e.fp32 = kh.fp32();
                if e.kind == Kind::Small {
                    e.x8 = kh.x8();
                }
                t.update(pos, e, hint);
            }
            None => {
                if t.try_insert(kh, e, hint).is_err() {
                    // more than 8 328 keys of one slot sharing 19 fingerprint bits; the key
                    // stays reachable through its record only after a restart
                    self.counters.unexpected_orphans += 1;
                }
            }
        }
    }

    fn index_set(&mut self, s: &Staged, cur36: Option<u64>, e: Entry, expire_at: Option<u64>) {
        self.index_set_at(s.db, s.slot, s.kh, cur36, e, expire_at);
    }

    pub(crate) fn index_remove_at(&mut self, db: u8, slot: u16, kh: KeyHash, cur36: u64) {
        if let Some(pos) = self.find_entry(db, slot, kh, cur36) {
            self.index.table_mut(db, slot).remove(pos);
        }
    }

    fn index_remove(&mut self, s: &Staged, cur36: u64) {
        self.index_remove_at(s.db, s.slot, s.kh, cur36);
    }

    /// Registers a durable-write dependency and returns true when it is durable already.
    fn register_durable(&mut self, flush: &mut [FlushTracker], dev: u8, wid: u64, what: Durable) {
        if self.dsync(dev) {
            self.durable_q.push(what);
            return;
        }
        match flush.get_mut(usize::from(dev)) {
            Some(ft) => match ft.on_write_done(WriteId::new(self.cfg.id, wid)) {
                Some(_) => self.durable_q.push(what),
                None => {
                    self.durable_wait.insert(wid, what);
                }
            },
            None => self.durable_q.push(what),
        }
    }

    fn new_wid(&mut self) -> u64 {
        let w = self.next_wid;
        self.next_wid += 1;
        w
    }
}

/// Seq of the version at `c` (`Seq::ZERO` when absent or unknown).
fn cur_seq(c: &Cur) -> Seq {
    match c {
        Cur::Absent => Seq::ZERO,
        Cur::Small { seq, .. } | Cur::Extent { seq, .. } => *seq,
    }
}

fn cur36(c: &Cur) -> Option<u64> {
    match c {
        Cur::Absent => None,
        Cur::Small { page, .. } => Some(page.to36()),
        Cur::Extent { start, .. } => Some(start.to36()),
    }
}

/// [`BuildEnv`] over the partition core for one page.
struct Env<'a> {
    core: &'a Core,
    page: PageId,
}

impl BuildEnv for Env<'_> {
    fn mode(&self) -> PageUpdateMode {
        self.core.dev_mode(self.page.dev())
    }
    fn crc(&self) -> RecordCrc {
        self.core.cfg.crc
    }
    fn partition(&self) -> u16 {
        self.core.cfg.id
    }
    fn lba(&self) -> u32 {
        self.core.dev_lba(self.page.dev())
    }
    fn granularity(&self) -> Granularity {
        match self.core.cfg.granularity {
            WriteGranularity::Page => Granularity::Page,
            WriteGranularity::Lba => Granularity::Lba,
        }
    }
    fn atomic_min(&self) -> u32 {
        let d = self.core.boot.devs.get(usize::from(self.page.dev()));
        let unit_min = d.and_then(|d| d.atomic_unit).map_or(0, |u| u.0);
        self.lba().max(unit_min)
    }
    fn others(&self, db: u8, key: &[u8]) -> usize {
        self.core.stale.count_outside(db, key, self.page)
    }
    fn others_older(&self, db: u8, key: &[u8], seq: Seq) -> usize {
        self.core.stale.count_older_outside(db, key, self.page, seq)
    }
    fn tracked_here(&self, db: u8, key: &[u8]) -> Option<CopyState> {
        match self.core.stale.state_at(db, key, self.page) {
            // a deleted key's newest copy stays until its older copies are gone (D1)
            Some(CopyState::Stale) if self.core.stale.fenced_by_older(db, key, self.page) => Some(CopyState::Source),
            s => s,
        }
    }
    fn tracked_keys(&self) -> SmallVec<[(u8, Box<[u8]>); 2]> {
        self.core.stale.keys_at(self.page)
    }
    fn dead(&self, r: &RecDesc) -> bool {
        self.core.dead(r.db, r.seq, r.txn)
    }
    fn txn_open(&self, t: u64) -> bool {
        self.core.txn_open(t)
    }
}

pub struct PartitionStore {
    pub(crate) core: Core,
    pub(crate) reqs: Requests,
    pub(crate) recovery: Option<Box<crate::recovery::Scan>>,
}

impl PartitionStore {
    pub fn new(cfg: PartitionCfg) -> Self {
        Self::with_boot(cfg, Arc::new(Boot::empty(cfg.partitions, cfg.databases)))
    }

    /// A partition over the devices described by `boot` (engine open, §18.1). It starts
    /// `Loading`; [`PartitionStore::recover`] makes it Ready.
    pub fn with_boot(cfg: PartitionCfg, boot: Arc<Boot>) -> Self {
        let pmeta = boot.pmeta.get(usize::from(cfg.id)).cloned().unwrap_or_else(|| PartitionMeta::initial(cfg.id));
        let pick = KeyedPick::new(cfg.hash_seed, cfg.boot_gen);
        let alloc = Alloc::new(cfg.id, boot.layouts.clone(), pick, cfg.page_update, cfg.page_slack_cap, cfg.page_slack_atomic);
        let r = cfg.rates;
        let bucket = |rate: u64| TokenBucket::new(rate, rate.clamp(1, 64));
        let now = Now::read();
        let mut cutoff = [0u64; 16];
        cutoff.copy_from_slice(&pmeta.flush_cutoff);
        let prior = Arc::new(boot.committed.clone());
        let core = Core {
            index: PartitionIndex::new(cfg.id, cfg.partitions, cfg.databases.max(1)),
            alloc,
            stale: StaleCopies::new(),
            tickets: Tickets::new(),
            next_seq: pmeta.seq_floor.max(Seq::new(0, 1)),
            seq_limit: Seq::ZERO,
            seq_asked: false,
            cutoff,
            cutoff_durable: cutoff,
            pmeta,
            dirty: Vec::new(),
            ext: ExtTables::default(),
            io: HashMap::with_hasher(Default::default()),
            next_io: 1,
            durable_wait: HashMap::with_hasher(Default::default()),
            durable_q: Vec::new(),
            next_wid: 1,
            loads: HashMap::with_hasher(Default::default()),
            meta_wait: VecDeque::new(),
            next_token: 1,
            counters: PartitionCounters::default(),
            bufs: Vec::new(),
            expiry: crate::expire::ExpirySweep::new(now.unix_ms),
            buckets: Buckets {
                scrub: bucket(r.scrub_iops),
                warm: bucket(r.warm_iops),
                expire: bucket(r.expire_reads),
                reclaim: bucket(r.reclaim_iops),
                release: TokenBucket::new(2, 2),
            },
            scrub_queue: VecDeque::new(),
            fg_keys: Vec::new(),
            deferred: HashSet::with_hasher(Default::default()),
            retry: Vec::new(),
            load_retry: Vec::new(),
            txns: HashMap::with_hasher(Default::default()),
            committed: HashSet::with_hasher(Default::default()),
            resolved_w: boot.committed.resolved_w,
            prior,
            retiring_ags: Vec::new(),
            flushall_wait: false,
            reclaim_pages: VecDeque::new(),
            reclaim_ext: VecDeque::new(),
            space_wait: Vec::new(),
            seq_wait: Vec::new(),
            now,
            failed: None,
            writes_in_flight: 0,
            recovered_bytes: 0,
            recovery_mode: None,
            gone_wait: Vec::new(),
            retire: Vec::new(),
            prior_txn_zero: Some(0),
            prior_txn_pages: HashSet::with_hasher(Default::default()),
            snapshot_done: false,
            ticks: 0,
            stall_checked_ns: 0,
            relocations: 0,
            last_local_txn: None,
            older_pages: Vec::new(),
            txn_inherit: Vec::new(),
            scratch_pos: Vec::new(),
            scratch_txn: Vec::new(),
            watch: WatchSet { keys: HashMap::with_hasher(Default::default()), touched: Vec::new() },
            boot,
            state: PartState::Loading,
            cfg,
        };
        PartitionStore { core, reqs: Requests::default(), recovery: None }
    }

    pub fn state(&self) -> PartState {
        self.core.state
    }

    pub fn id(&self) -> u16 {
        self.core.cfg.id
    }

    pub fn cfg(&self) -> &PartitionCfg {
        &self.core.cfg
    }

    /// The partition's index (DBSIZE, INFO, tests).
    pub fn index(&self) -> &PartitionIndex {
        &self.core.index
    }

    /// Requests submitted and not finished.
    pub fn live_requests(&self) -> usize {
        self.reqs.live
    }

    /// True when nothing is staged, in flight or waiting for durability (drain, shutdown).
    pub fn is_quiescent(&self) -> bool {
        self.core.dirty.is_empty()
            && self.core.retry.is_empty()
            && self.core.deferred.is_empty()
            && self.core.writes_in_flight == 0
            && self.core.ext.writes.is_empty()
            && self.core.ext.zeros.is_empty()
            && self.core.durable_wait.is_empty()
            && self.core.durable_q.is_empty()
            && self.core.txns.is_empty()
            && self.core.loads.is_empty()
            && self.core.meta_wait.is_empty()
            && self.core.gone_wait.is_empty()
            && self.reqs.live == 0
    }

    /// A one-line summary of the partition's queues and waits (diagnostics; no user bytes).
    pub fn diag(&self) -> String {
        let c = &self.core;
        format!(
            "p{} {:?} live={} tickets={} oldest_ms={} ready={} space_wait={} seq_wait={} meta_wait={} txns={} next_seq={:#x} seq_limit={:#x} seq_asked={} dirty={} writes={} ext_w={} ext_z={} stale={} absent={} deferred={} loads={} durable_wait={} gone_wait={} retire={} missed_handoffs={}",
            c.cfg.id,
            c.state,
            self.reqs.live,
            c.tickets.len(),
            c.counters.oldest_wait_ms,
            self.reqs.ready.len(),
            c.space_wait.len(),
            c.seq_wait.len(),
            c.meta_wait.len(),
            c.txns.len(),
            c.next_seq.0,
            c.seq_limit.0,
            c.seq_asked,
            c.dirty.len(),
            c.writes_in_flight,
            c.ext.writes.len(),
            c.ext.zeros.len(),
            c.stale.len(),
            c.stale.absent_len(),
            c.deferred.len(),
            c.loads.len(),
            c.durable_wait.len(),
            c.gone_wait.len(),
            c.retire.len(),
            c.counters.missed_handoffs,
        )
    }

    /// [`PartitionStore::diag`] plus the deferred pages' staged effects and the tracked copies
    /// (diagnostics; key lengths only).
    pub fn diag_deep(&self, cache: &PageCache) -> String {
        let mut out = self.diag();
        for p in &self.core.deferred {
            if let Some(st) = cache.page_state(*p) {
                out.push_str(&format!("\n  deferred {:#x}: recs=[", p.0));
                for r in st.recs.iter() {
                    out.push_str(&format!("({:?} k={:04x} seq={:#x} txn={} kind={} klen={}) ", r.status, r.h32 & 0xffff, r.seq.0, r.txn, r.kind, r.key_len));
                }
                out.push_str("] pending=[");
                for e in st.pending.iter() {
                    let kind = match e {
                        Pending::Put(r) => format!("put(seq={:#x} txn={})", r.desc.seq.0, r.desc.txn),
                        Pending::Remove { .. } => "remove".to_string(),
                        Pending::Retire { seq, .. } => format!("retire({:#x})", seq.0),
                    };
                    out.push_str(&format!("{kind} k={:04x} klen={} tickets={} others={}; ", e.h32() & 0xffff, e.key().len(), e.tickets().len(), self.core.stale.count(e.db(), e.key())));
                }
                out.push(']');
            }
        }
        for (db, k, e) in self.core.stale.iter() {
            let kh = KeyHash::of(self.core.cfg.hash_seed, k);
            let h = kh.h32() & 0xffff;
            out.push_str(&format!("\n  stale db={db} k={h:04x} klen={} at={:#x} {:?} seq={:#x} older={} fg={}", k.len(), e.at.page().0, e.state, e.seq.0, e.older, e.foreground));
            // the page behind a tracked small copy: what its writer would do with the copy
            if let StaleRef::Small { page } = e.at {
                match cache.page_state(page) {
                    None => out.push_str(&format!(" page=uncached load={}", self.core.loads.contains_key(&page))),
                    Some(st) => {
                        let fmt = |recs: &[RecDesc], img: &[u8; PAGE]| {
                            recs.iter().filter(|r| r.db == db && r.h32 == kh.h32() && r.key(img) == k).map(|r| format!("{:?}@{:#x}/txn{}", r.status, r.seq.0, r.txn)).collect::<Vec<_>>().join(",")
                        };
                        let m = fmt(&st.recs, cache.frame(st.m_frame));
                        let n = st.inflight.as_ref().map(|i| fmt(&i.recs, cache.frame(i.n_frame)));
                        out.push_str(&format!(
                            " page: m=[{m}] inflight={:?} pending={} dirty={} scrub={} queued={} retry={} deferred={} corrupt={}",
                            n,
                            st.pending.len(),
                            st.dirty,
                            st.scrub,
                            self.core.dirty.contains(&page),
                            self.core.retry.contains(&page),
                            self.core.deferred.contains(&page),
                            st.corrupt
                        ));
                    }
                }
            }
        }
        for (t, o) in &self.core.txns {
            out.push_str(&format!("\n  txn {t} keys={} pages={} requested={} local={}", o.keys.len(), o.pages.len(), o.requested, o.local));
        }
        for it in &self.core.retire {
            let h = KeyHash::of(self.core.cfg.hash_seed, &it.key).h32() & 0xffff;
            out.push_str(&format!("\n  retire k={h:04x} at={:#x} seq={:#x}", it.page.0, it.seq.0));
        }
        out
    }

    /// Where the current version of `(db, key)` lives (see [`crate::engine::Located`]).
    pub fn locate(&self, cache: &PageCache, db: u8, key: &[u8]) -> Option<crate::engine::Located> {
        let kh = KeyHash::of(self.core.cfg.hash_seed, key);
        let slot = key_slot(key);
        if slot % self.core.cfg.partitions != self.core.cfg.id || db >= self.core.cfg.databases {
            return None;
        }
        let t = self.core.index.table(db, slot)?;
        let mut c = SmallVec::new();
        t.candidates(kh, &mut c);
        for c in c.iter() {
            let page = PageId::from36(c.entry.page36);
            match c.entry.kind {
                Kind::Small => {
                    let Some(st) = cache.page_state(page) else {
                        return Some(crate::engine::Located { kind: Kind::Small, page, off: None, n_pages: 1 });
                    };
                    if st.pending.iter().any(|p| p.is_change() && p.is_key(db, kh.h32(), key)) {
                        return Some(crate::engine::Located { kind: Kind::Small, page, off: None, n_pages: 1 });
                    }
                    let (recs, fr) = st.image();
                    let img = cache.frame(fr);
                    if let Some(r) = recs.iter().filter(|r| r.status == RecStatus::Live && r.db == db && r.key(img) == key).max_by_key(|r| r.seq) {
                        return Some(crate::engine::Located { kind: Kind::Small, page, off: Some(r.off), n_pages: 1 });
                    }
                }
                k => return Some(crate::engine::Located { kind: k, page, off: None, n_pages: class_pages(c.entry.x8) }),
            }
        }
        None
    }

    /// Tracked stale copies left (a snapshot needs none, §8.2 SHUTDOWN).
    pub fn stale_copies(&self) -> usize {
        self.core.stale.len()
    }

    /// True while background work is pending (the worker keeps its 1 ms timer armed, §6.4).
    pub fn busy(&self) -> bool {
        !self.is_quiescent()
            || !self.core.scrub_queue.is_empty()
            || !self.core.stale.is_empty()
            || !self.core.reclaim_pages.is_empty()
            || !self.core.reclaim_ext.is_empty()
            || !self.core.retire.is_empty()
            || (0..self.core.cfg.databases).any(|d| self.core.index.expires(d) > 0)
    }

    pub fn counters(&self) -> &PartitionCounters {
        &self.core.counters
    }

    pub fn next_seq(&self) -> Seq {
        self.core.next_seq
    }

    /// Produce replication groups (`ExecOut::group`) from now on: the partition became a
    /// cluster primary (§23.7). Members stop producing them and stop lazy expiry (§16.4).
    /// Registers one watcher of `(db, key)` (WATCH). Changes of the key committed from now on
    /// are reported by [`PartitionStore::take_touched`] until the matching
    /// [`PartitionStore::unwatch_key`].
    pub fn watch_key(&mut self, db: u8, key: &[u8]) {
        self.core.watch.watch(db, key);
    }

    /// Removes one watcher of `(db, key)` (UNWATCH, EXEC, DISCARD, connection close).
    pub fn unwatch_key(&mut self, db: u8, key: &[u8]) {
        self.core.watch.unwatch(db, key);
    }

    /// Moves the watched keys changed since the last call into `out` (a key changed several
    /// times may appear several times): writes, deletes, expiries (lazy, active, replicated) and
    /// FLUSHDB/FLUSHALL of its db, in commit order. The caller flags the connections watching
    /// them (`WATCH_DIRTY`).
    pub fn take_touched(&mut self, out: &mut Vec<(u8, Box<[u8]>)>) {
        out.append(&mut self.core.watch.touched);
    }

    pub fn set_produce_groups(&mut self, on: bool) {
        self.core.cfg.produce_groups = on;
    }

    /// Starts a new term (cluster promotion): the counter restarts at 1 (§10.11).
    pub fn set_term(&mut self, term: u32) {
        if term > self.core.next_seq.term() {
            self.core.next_seq = Seq::new(term, 1);
            self.core.pmeta.term = term;
            self.core.seq_limit = Seq::ZERO;
        }
    }

    /// Drains ticket state changes since the last call.
    pub fn take_tickets(&mut self, out: &mut Vec<(TicketId, TicketState)>) {
        self.core.tickets.take(out);
    }

    // ---------------------------------------------------------------------------------------
    // Submission

    pub fn submit(&mut self, cx: &mut StorageCtx<'_>, req: Request) -> ReqId {
        self.core.now = cx.now;
        self.core.tickets.now_ns = cx.now.mono_ns;
        let seed = self.core.cfg.hash_seed;
        let keys: SmallVec<[KeyInfo; 2]> =
            req.keys.iter().map(|(k, _)| KeyInfo { kh: KeyHash::of(seed, k), slot: key_slot(k) }).collect();
        let barrier = matches!(req.kind, ReqKind::FlushDb { .. } | ReqKind::Barrier);
        let order = self.reqs.next_order;
        self.reqs.next_order += 1;
        let db = req.db;
        let r = Req {
            req,
            keys,
            order,
            generation: 0,
            io: 0,
            waits: 0,
            barrier,
            blocked: false,
            queued: false,
            parked: false,
            pins: SmallVec::new(),
            fetched: SmallVec::new(),
            failed: None,
            born_ns: cx.now.mono_ns,
            stalled: false,
            traced: false,
        };
        let id = self.reqs.insert(r);
        self.reqs.order.push_back((order, id));
        if barrier {
            self.reqs.barriers.push_back((order, id));
        }
        // per-key FIFO queues (LC-15)
        let mut seen: SmallVec<[(u8, u64); 2]> = SmallVec::new();
        let n = self.reqs.get(id).map_or(0, |r| r.keys.len());
        for i in 0..n {
            let raw = self.reqs.get(id).expect("live").keys[i].kh.raw();
            let k = (db, raw);
            if seen.contains(&k) {
                continue;
            }
            seen.push(k);
            let q = self.reqs.busy.entry(k).or_default();
            q.push_back(id);
            if q.len() > 1 {
                self.reqs.get_mut(id).expect("live").waits += 1;
            }
        }
        if self.core.state != PartState::Loading {
            self.prefetch(cx, id);
        }
        self.reqs.check_ready(id);
        id
    }

    /// Prefetches the pages and extents a request will read (§14.1, §15).
    fn prefetch(&mut self, cx: &mut StorageCtx<'_>, id: ReqId) {
        let Some(r) = self.reqs.get(id) else { return };
        let kind = r.req.kind;
        let db = r.req.db;
        let mut needs: SmallVec<[Need; 4]> = SmallVec::new();
        match kind {
            ReqKind::ScanStep { db, cursor, count, vtype } => {
                let plan = self.scan_plan(db, cursor, count, vtype);
                for pr in &plan.refs {
                    needs.push(page_ref_need(pr));
                }
            }
            ReqKind::RandomKey { db, r: rv } => {
                if let Some(n) = self.random_need(db, rv) {
                    needs.push(n);
                }
            }
            ReqKind::SlotIter { slot, fp_from, max_entries } => {
                if let Some(t) = self.core.index.table(db, slot) {
                    let mut refs = Vec::new();
                    t.scan_pages(fp_from, max_entries as usize, ScanFilter::ALL, &mut refs);
                    for pr in refs {
                        needs.push(match pr.kind {
                            Kind::Small => Need::Page(PageId::from36(pr.page36)),
                            _ => Need::Extent { start: PageId::from36(pr.page36), pages: class_pages(pr.class), chunks: true },
                        });
                    }
                }
            }
            ReqKind::FlushDb { .. } | ReqKind::Barrier | ReqKind::SyncPrune { .. } => {}
            ReqKind::Keyed | ReqKind::Txn { .. } => {
                for i in 0..r.keys.len() {
                    let (kh, slot) = (r.keys[i].kh, r.keys[i].slot);
                    let access = r.req.keys[i].1;
                    if slot % self.core.cfg.partitions != self.core.cfg.id {
                        continue;
                    }
                    let Some(t) = self.core.index.table(db, slot) else { continue };
                    let mut c = SmallVec::new();
                    t.candidates(kh, &mut c);
                    // the first candidate only: a false positive costs a NeedMore re-run (§14.1)
                    if let Some(c) = c.first() {
                        let start = PageId::from36(c.entry.page36);
                        match c.entry.kind {
                            Kind::Small => needs.push(Need::Page(start)),
                            Kind::Extent | Kind::ChunkMap => {
                                if self.core.ext.writes.contains_key(&start) {
                                    continue;
                                }
                                let full = matches!(access, Access::Read | Access::ReadWrite);
                                let pages = if full { class_pages(c.entry.x8) } else { 1 };
                                needs.push(Need::Extent { start, pages, chunks: full && c.entry.kind == Kind::ChunkMap });
                            }
                        }
                    }
                }
            }
        }
        for n in needs {
            self.need(cx, id, n);
        }
    }

    /// Starts satisfying `n` for request `id`.
    fn need(&mut self, cx: &mut StorageCtx<'_>, id: ReqId, n: Need) {
        let generation = match self.reqs.get(id) {
            Some(r) => r.generation,
            None => return,
        };
        match n {
            Need::Page(page) => {
                if let Some(f) = cx.cache.get(page)
                    && cx.cache.state(f).is_some()
                {
                    let r = self.reqs.get_mut(id).expect("live");
                    if !r.pins.contains(&page) {
                        r.pins.push(page);
                        cx.cache.pin(f);
                    }
                    return;
                }
                self.load_page(cx, page, Some((id, generation)), false);
            }
            Need::Extent { start, pages, chunks } => {
                let r = self.reqs.get_mut(id).expect("live");
                if r.fetched.iter().any(|f| f.start == start && f.pages >= pages && (!chunks || f.chunks_asked) && f.err.is_none()) {
                    return;
                }
                // reads in flight name their fetch by index: an entry is replaced in place,
                // never removed, so that the other entries keep their indexes
                let early = r.waits > 0 || r.blocked;
                let fresh = ExtFetch { start, pages, buf: None, err: None, chunks: None, want_chunks: chunks, chunks_asked: chunks, early };
                let idx = match r.fetched.iter().position(|f| f.start == start) {
                    Some(i) => {
                        r.fetched[i] = fresh;
                        i
                    }
                    None => {
                        r.fetched.push(fresh);
                        r.fetched.len() - 1
                    }
                };
                r.io += 1;
                let buf = AlignedBuf::zeroed(pages as usize * PAGE);
                let tag = self.core.tag(IoKind::ExtRead { req: id, generation, idx, chunk: None });
                cx.io.push(IoReq::read(tag, start.dev(), start.byte_offset(), buf));
                self.core.counters.page_reads += u64::from(pages);
            }
            Need::ExtWrite(start) => {
                if let Some(e) = self.core.failed
                    && self.core.state == PartState::ReadOnly
                {
                    // a read-only partition issues no write any more: the value never arrives
                    self.reqs.get_mut(id).expect("live").failed = Some(StoreError::Io(e));
                    return;
                }
                // only a write still to complete wakes its waiters (at `ext_write_done`)
                if let Some(w) = self.core.ext.writes.get_mut(&start)
                    && w.state != ExtState::AwaitFlush
                {
                    w.waiters.push((id, generation));
                    self.reqs.get_mut(id).expect("live").io += 1;
                }
            }
        }
    }

    /// Issues (or joins) the read of `page`.
    pub(crate) fn load_page(&mut self, cx: &mut StorageCtx<'_>, page: PageId, waiter: Option<(ReqId, u32)>, scrub: bool) {
        if let Some(w) = waiter
            && let Some(r) = self.reqs.get_mut(w.0)
        {
            r.io += 1;
        }
        if let Some(l) = self.core.loads.get_mut(&page) {
            l.waiters.extend(waiter);
            l.scrub |= scrub;
            return;
        }
        let mut l = Load { scrub, ..Load::default() };
        l.waiters.extend(waiter);
        self.core.loads.insert(page, l);
        let buf = self.core.take_buf(PAGE);
        let tag = self.core.tag(IoKind::PageRead { page, retried: false });
        cx.io.push(IoReq::read(tag, page.dev(), page.byte_offset(), buf));
        self.core.counters.page_reads += 1;
    }

    pub fn take_ready(&mut self, out: &mut Vec<ReqId>) {
        while let Some(id) = self.reqs.ready.pop_front() {
            if let Some(r) = self.reqs.get_mut(id)
                && r.queued
            {
                out.push(id);
            }
        }
    }

    /// Removes a finished request and advances its key queues.
    fn finish(&mut self, cache: &mut PageCache, id: ReqId, r: Req) {
        for p in &r.pins {
            if let Some(f) = cache.peek(*p) {
                cache.unpin(f);
            }
        }
        let db = r.req.db;
        let mut seen: SmallVec<[(u8, u64); 2]> = SmallVec::new();
        let mut wake: SmallVec<[ReqId; 4]> = SmallVec::new();
        for k in &r.keys {
            let key = (db, k.kh.raw());
            if seen.contains(&key) {
                continue;
            }
            seen.push(key);
            if let Some(q) = self.reqs.busy.get_mut(&key) {
                if q.front() == Some(&id) {
                    q.pop_front();
                    if let Some(&next) = q.front() {
                        wake.push(next);
                    }
                } else {
                    q.retain(|x| *x != id);
                }
                if q.is_empty() {
                    self.reqs.busy.remove(&key);
                }
            }
        }
        self.reqs.slots[id.0 as usize] = None;
        self.reqs.free.push(id.0);
        self.reqs.live -= 1;
        if r.barrier {
            self.reqs.barriers.retain(|&(_, b)| b != id);
        }
        for w in wake {
            if let Some(n) = self.reqs.get_mut(w) {
                n.waits = n.waits.saturating_sub(1);
            }
            self.reqs.check_ready(w);
        }
        // the oldest request finished: the front barrier may run now
        if let Some(&(_, b)) = self.reqs.barriers.front() {
            self.reqs.check_ready(b);
        }
        if r.barrier {
            let held = core::mem::take(&mut self.reqs.blocked);
            for i in held {
                if let Some(x) = self.reqs.get_mut(i) {
                    x.blocked = false;
                }
                self.reqs.check_ready(i);
            }
        }
    }

    /// Keeps an executed local txn's key queues until its commit is durable.
    fn hold(&mut self, cache: &mut PageCache, id: ReqId, mut r: Req, txn: u64) {
        for p in r.pins.drain(..) {
            if let Some(f) = cache.peek(p) {
                cache.unpin(f);
            }
        }
        r.fetched.clear();
        r.queued = false;
        self.reqs.slots[id.0 as usize] = Some(r);
        self.reqs.held.insert(txn, id);
    }

    /// Releases the key queues of a held txn request.
    fn release_held(&mut self, cache: &mut PageCache, txn: u64) {
        if let Some(id) = self.reqs.held.remove(&txn)
            && let Some(r) = self.take_req(id)
        {
            self.finish(cache, id, r);
        }
    }

    fn requeue(&mut self, id: ReqId, r: Req) {
        self.reqs.slots[id.0 as usize] = Some(r);
        self.reqs.check_ready(id);
    }

    fn take_req(&mut self, id: ReqId) -> Option<Req> {
        let mut r = self.reqs.slots.get_mut(id.0 as usize).and_then(Option::take)?;
        r.queued = false;
        Some(r)
    }

    // ---------------------------------------------------------------------------------------
    // Execution

    pub fn exec<R>(
        &mut self,
        cx: &mut StorageCtx<'_>,
        id: ReqId,
        f: &mut dyn FnMut(&mut Keyspace<'_>) -> Result<R, StoreError>,
    ) -> ExecStatus<R> {
        self.core.now = cx.now;
        self.core.tickets.now_ns = cx.now.mono_ns;
        let Some(mut r) = self.take_req(id) else {
            return ExecStatus::Failed(StoreError::Io(-libc::ENOENT));
        };
        if let Some(e) = r.failed {
            self.finish(cx.cache, id, r);
            return ExecStatus::Failed(e);
        }
        if self.core.state == PartState::Loading {
            self.finish(cx.cache, id, r);
            return ExecStatus::Failed(StoreError::Loading);
        }
        if matches!(r.req.kind, ReqKind::FlushDb { .. } | ReqKind::Barrier) {
            // partition-wide requests: the closure only produces the reply (it sees an empty
            // keyspace view and must not stage anything)
            let v = {
                let mut fetched = SmallVec::new();
                let mut ks = Keyspace::new(&mut self.core, cx.cache, &mut fetched, r.req.db, cx.now.unix_ms);
                f(&mut ks)
            };
            let out = match (v.is_ok(), r.req.kind) {
                (false, _) => Ok(ExecOut::none()),
                (true, ReqKind::FlushDb { db }) => self.exec_flush(cx, db, r.req.origin),
                // a read-only partition writes nothing more: a barrier could never complete
                (true, _) if self.core.state == PartState::ReadOnly => Err(StoreError::ReadOnly),
                (true, _) => Ok(self.exec_barrier(cx.cache)),
            };
            self.finish(cx.cache, id, r);
            return match (v, out) {
                (Ok(v), Ok(out)) => ExecStatus::Done(v, out),
                (Err(e), _) | (_, Err(e)) => ExecStatus::Failed(e),
            };
        }
        let now_ms = cx.now.unix_ms;
        let db = r.req.db;
        // extents read before the request reached the head of its key queues may hold a
        // version that the requests ahead of it replaced since: read them again (NeedMore)
        r.fetched.retain(|f| !f.early);
        let (result, staged, needs, seqo) = {
            let mut ks = Keyspace::new(&mut self.core, cx.cache, &mut r.fetched, db, now_ms);
            match r.req.origin {
                Origin::Replicated(s) | Origin::Image(s) | Origin::Snapshot(s) => {
                    ks.seq = Some(s);
                    ks.no_lazy_expire = true;
                }
                Origin::Migration { src_seq } => ks.seq = Some(src_seq),
                Origin::Client => {}
            }
            if let ReqKind::RandomKey { r: rv, .. } = r.req.kind {
                ks.rand = rv;
            }
            let res = f(&mut ks);
            (res, core::mem::take(&mut ks.staged), core::mem::take(&mut ks.needs), ks.seq)
        };
        match result {
            Ok(v) => {
                if self.core.state == PartState::ReadOnly && staged.iter().any(|s| !matches!(s.change, Change::Expired)) {
                    self.finish(cx.cache, id, r);
                    return ExecStatus::Failed(StoreError::ReadOnly);
                }
                let txn = match r.req.kind {
                    ReqKind::Txn { txn_id } => Some(txn_id),
                    _ => None,
                };
                match self.commit(cx, staged, txn, seqo) {
                    Commit::Done(out) => {
                        match self.core.last_local_txn.take() {
                            Some(t) => self.hold(cx.cache, id, r, t),
                            None => self.finish(cx.cache, id, r),
                        }
                        ExecStatus::Done(v, out)
                    }
                    Commit::Need(needs) => {
                        // the needs are registered before the readiness check: a request is
                        // never ready while its reads are outstanding
                        self.reqs.slots[id.0 as usize] = Some(r);
                        for n in needs {
                            self.need(cx, id, n);
                        }
                        self.reqs.check_ready(id);
                        ExecStatus::Requeued
                    }
                    Commit::Park => {
                        r.parked = true;
                        self.core.space_wait.push(id);
                        self.requeue(id, r);
                        ExecStatus::Requeued
                    }
                    Commit::Seq => {
                        r.parked = true;
                        self.core.seq_wait.push(id);
                        self.requeue(id, r);
                        ExecStatus::Requeued
                    }
                    Commit::Err(e) => {
                        self.finish(cx.cache, id, r);
                        ExecStatus::Failed(e)
                    }
                }
            }
            Err(StoreError::NeedMore) if !needs.is_empty() => {
                self.core.counters.fp_false_positive_reads += 1;
                self.reqs.slots[id.0 as usize] = Some(r);
                for n in needs {
                    self.need(cx, id, n);
                }
                self.reqs.check_ready(id);
                ExecStatus::Requeued
            }
            Err(e) => {
                self.finish(cx.cache, id, r);
                ExecStatus::Failed(e)
            }
        }
    }
}

fn page_ref_need(pr: &PageRef) -> Need {
    match pr.kind {
        Kind::Small => Need::Page(PageId::from36(pr.page36)),
        _ => Need::Extent { start: PageId::from36(pr.page36), pages: 1, chunks: false },
    }
}

/// Outcome of a commit attempt.
enum Commit {
    Done(ExecOut),
    /// Pages or extents to load before the request is run again.
    Need(SmallVec<[Need; 2]>),
    /// Waiting for an AG from the meta thread.
    Park,
    /// Waiting for a seq reservation (E12) or a txn id.
    Seq,
    Err(StoreError),
}

/// Where a staged change goes (planned before anything is applied).
#[derive(Clone)]
enum Plan {
    None,
    Small { page: PageId, fresh: bool },
    Ext { start: PageId, n_pages: u32, inplace: bool },
    Chunked { map: PageId, map_pages: u32, chunks: SmallVec<[(PageId, u32); 4]> },
}

/// Allocations made while planning, released when the commit cannot complete.
#[derive(Default)]
struct Undo {
    fresh: SmallVec<[PageId; 2]>,
    ext: SmallVec<[(PageId, u32); 2]>,
}

impl PartitionStore {
    // ---------------------------------------------------------------------------------------
    // Commit (§14.1-§14.6, §16.1)

    fn commit(&mut self, cx: &mut StorageCtx<'_>, staged: SmallVec<[Staged; 2]>, txn: Option<u64>, seqo: Option<Seq>) -> Commit {
        if staged.is_empty() {
            return Commit::Done(ExecOut::none());
        }
        let writes = staged.iter().filter(|s| matches!(s.change, Change::Put { .. } | Change::Delete)).count();
        let seq = if writes == 0 {
            Seq::ZERO
        } else if let Some(s) = seqo {
            s
        } else {
            if self.core.next_seq >= self.core.seq_limit {
                self.ask_seq(cx.meta);
                return Commit::Seq;
            }
            self.core.next_seq
        };
        // a versioned write (replicated, image, snapshot, migration) never goes below a copy of
        // its key at the same or a later version: recovery would prefer that copy to the write,
        // and the copy, protected until a durable copy with a higher seq exists, would never be
        // released, so the write's effect would wait behind it forever. `apply_replicated`
        // skips such ops; anything else is refused here.
        if seqo.is_some() {
            for s in staged.iter().filter(|s| matches!(s.change, Change::Put { .. } | Change::Delete)) {
                let cur = match s.cur {
                    Cur::Absent => Seq::ZERO,
                    Cur::Small { seq, .. } | Cur::Extent { seq, .. } => seq,
                };
                if cur.max(self.core.tracked_copy_seq(s.db, &s.key)) >= seq {
                    return Commit::Err(StoreError::Io(-libc::ESTALE));
                }
            }
        }
        // a cross-partition part carries its txn id; a local multi-key write takes one (§14.6)
        let is_txn = txn.is_some() || writes >= 2;
        // 1. plan every placement; nothing changes unless all succeed
        let mut undo = Undo::default();
        let mut plans: SmallVec<[Plan; 2]> = SmallVec::new();
        for s in staged.iter() {
            let p = match &s.change {
                Change::Put { value, .. } => self.plan_put(cx, s, s.key.len(), value.len() as u64, is_txn, txn.is_some(), &mut undo),
                Change::Delete if is_txn && !matches!(s.cur, Cur::Absent) => self.plan_put(cx, s, s.key.len(), 0, true, txn.is_some(), &mut undo),
                _ => Ok(Plan::None),
            };
            match p {
                Ok(p) => plans.push(p),
                Err(c) => {
                    self.undo(undo);
                    return c;
                }
            }
        }
        let (txn_id, local_txn) = match txn {
            Some(t) => (t, false),
            None if is_txn => match cx.meta.next_txn_id() {
                0 => {
                    // the id range is being extended (§14.6 step 1): wait like a missing credit
                    self.undo(undo);
                    return Commit::Seq;
                }
                t => (t, true),
            },
            None => (0, false),
        };
        // 2. apply (infallible)
        let mut flags = 0;
        if staged.iter().any(|s| matches!(s.change, Change::Delete)) {
            flags |= TICKET_HAS_DELETE;
        }
        let ticket = (writes > 0).then(|| self.core.tickets.create(flags));
        let fx = (txn_id != 0 && local_txn).then(|| self.core.tickets.create_txn(txn_id));
        let mut ops: Vec<Op> = Vec::new();
        let mut otx = (txn_id != 0).then(|| OpenTxn {
            ticket,
            local: local_txn,
            requested: false,
            keys: SmallVec::new(),
            seq,
            barriers: SmallVec::new(),
            markers: SmallVec::new(),
            pages: SmallVec::new(),
        });
        if otx.is_some() {
            self.core.counters.page_writes_txn += 1;
        }
        for (s, plan) in staged.into_iter().zip(plans) {
            if let Some(op) = &s.op {
                ops.push(op.clone());
            }
            self.apply(cx, s, plan, seq, txn_id, ticket, fx, otx.as_mut());
        }
        if let Some(mut o) = otx {
            if let (Some(t), true) = (ticket, o.local) {
                // the reply waits for the durable COMMITTED listing (E6)
                self.core.tickets.add_meta(t);
            }
            if o.local {
                self.core.last_local_txn = Some(txn_id);
            }
            for (x, t) in core::mem::take(&mut self.core.txn_inherit) {
                debug_assert_eq!(x, txn_id);
                o.barriers.push(t);
            }
            self.core.txns.insert(txn_id, o);
        }
        if writes > 0 {
            match seqo {
                Some(s) if s >= self.core.next_seq => self.core.next_seq = s.next().unwrap_or(s),
                Some(_) => {}
                None => self.core.next_seq = seq.next().unwrap_or(seq),
            }
        }
        if let Some(t) = fx {
            self.core.tickets.seal(t);
        }
        if let Some(t) = ticket {
            self.core.tickets.seal(t);
        }
        let group = (self.core.cfg.produce_groups && !ops.is_empty()).then_some(Group { seq, ops });
        Commit::Done(ExecOut { ticket, group, seq: (writes > 0).then_some(seq) })
    }

    fn undo(&mut self, u: Undo) {
        for p in u.fresh {
            self.core.alloc.unplace_fresh(p);
        }
        for (s, n) in u.ext {
            self.core.alloc.free_extent(s, n);
        }
    }

    pub(crate) fn ask_seq(&mut self, meta: &MetaClient) {
        if self.core.seq_asked {
            return;
        }
        let floor = Seq(self.core.next_seq.0.saturating_add(SEQ_RESERVE));
        self.core.seq_asked = true;
        self.core.pmeta.seq_floor = self.core.pmeta.seq_floor.max(floor);
        let id = self.core.cfg.id;
        self.core.meta_req(meta, MetaReq::ReserveSeq { partition: id, floor }, MetaWait::Seq(floor));
    }

    /// Chooses where a put of `val_len` value bytes goes. `reserve`: the put is part of a
    /// cross-partition txn whose other parts may already be applied; it may use the space
    /// reserve above the fill limit (like an accepted move, §14.3), and fails with `-OOM` only
    /// once the reserve refused too. A part that fails leaves its siblings' versions to the
    /// coordinator to resolve, so failing for space that the reserve still has is avoided.
    #[allow(clippy::too_many_arguments)]
    fn plan_put(&mut self, cx: &mut StorageCtx<'_>, s: &Staged, key_len: usize, val_len: u64, txn: bool, reserve: bool, undo: &mut Undo) -> Result<Plan, Commit> {
        let r = self.plan_put_at(cx, s, key_len, val_len, txn, undo);
        if reserve && matches!(r, Err(Commit::Err(StoreError::OutOfSpace))) {
            let small = key_len <= u16::MAX as usize && val_len <= u16::MAX as u64 && small_len(key_len, val_len as usize, txn) <= SMALL_REC_MAX;
            let kind = if small { AgKind::Small } else { AgKind::Extent };
            if !self.core.alloc.reserve_refused(kind, cx.now.mono_ns) && self.core.alloc.want_reserved(kind) {
                return Err(Commit::Park);
            }
        }
        r
    }

    fn plan_put_at(&mut self, cx: &mut StorageCtx<'_>, s: &Staged, key_len: usize, val_len: u64, txn: bool, undo: &mut Undo) -> Result<Plan, Commit> {
        let now_ns = cx.now.mono_ns;
        let small = small_len(key_len, val_len as usize, txn);
        if key_len <= u16::MAX as usize && val_len <= u16::MAX as u64 && small <= SMALL_REC_MAX {
            let need = small as u16;
            if let Cur::Small { page, .. } = s.cur
                && self.fits(cx.cache, page, need, s)
            {
                return Ok(Plan::Small { page, fresh: false });
            }
            // never stage a key onto a page holding its tracked copy, or onto a corrupt page
            let stale = &self.core.stale;
            let cache: &PageCache = cx.cache;
            let absent = stale.absent_of(s.db, &s.key).map(|a| a.page);
            let exclude = |q: PageId| stale.state_at(s.db, &s.key, q).is_some() || absent == Some(q) || cache.page_state(q).is_some_and(|st| st.corrupt);
            return match self.core.alloc.place_small(need, cache, now_ns, &exclude) {
                SmallPlace::Cached(q) => Ok(Plan::Small { page: q, fresh: false }),
                SmallPlace::Fresh(q) => {
                    undo.fresh.push(q);
                    // the frame is taken now: applying the plan cannot fail any more
                    self.init_fresh(cx.cache, q);
                    if cx.cache.page_state(q).is_none() {
                        return Err(Commit::Err(StoreError::Io(-libc::ENOMEM)));
                    }
                    Ok(Plan::Small { page: q, fresh: true })
                }
                SmallPlace::Uncached(q) => {
                    let mut n = SmallVec::new();
                    n.push(Need::Page(q));
                    Err(Commit::Need(n))
                }
                SmallPlace::NeedAg => Err(Commit::Park),
                SmallPlace::Full => Err(Commit::Err(StoreError::OutOfSpace)),
            };
        }
        if key_len > u32::MAX as usize {
            return Err(Commit::Err(StoreError::TooLarge));
        }
        if extent::fits_value_extent(key_len, val_len) {
            let pages = extent::value_pages(key_len, val_len) as u32;
            let n = class_pages(class_for(pages).ok_or(Commit::Err(StoreError::TooLarge))?);
            if let Cur::Extent { start, n_pages, kind: Kind::Extent, .. } = s.cur
                && n_pages == n
                && !txn
                && self.atomic_extent(start, n)
            {
                return Ok(Plan::Ext { start, n_pages: n, inplace: true });
            }
            let twin = match s.cur {
                Cur::Extent { twin, .. } => twin,
                _ => None,
            };
            return match self.core.alloc.alloc_extent(n, twin, now_ns, None) {
                ExtPlace::Run(start) => {
                    undo.ext.push((start, n));
                    Ok(Plan::Ext { start, n_pages: n, inplace: false })
                }
                ExtPlace::NeedAg => Err(Commit::Park),
                ExtPlace::Full => Err(Commit::Err(StoreError::OutOfSpace)),
            };
        }
        // CHUNKMAP + CHUNKs (§14.5)
        let n_chunks = val_len.div_ceil(CHUNK_MAX_PAYLOAD as u64).max(1) as usize;
        let map_bytes = (EXT_HDR + extent::chunkmap_body_len(key_len, n_chunks)) as u64;
        let map_pages = class_pages(class_for(pages_for(map_bytes) as u32).ok_or(Commit::Err(StoreError::TooLarge))?);
        let twin = match s.cur {
            Cur::Extent { twin, kind: Kind::ChunkMap, .. } => twin,
            _ => None,
        };
        let map = match self.core.alloc.alloc_extent(map_pages, twin, now_ns, None) {
            ExtPlace::Run(p) => p,
            ExtPlace::NeedAg => return Err(Commit::Park),
            ExtPlace::Full => return Err(Commit::Err(StoreError::OutOfSpace)),
        };
        undo.ext.push((map, map_pages));
        let mut chunks = SmallVec::new();
        let mut left = val_len;
        while left > 0 {
            let len = left.min(CHUNK_MAX_PAYLOAD as u64);
            left -= len;
            let n = class_pages(class_for(pages_for(EXT_HDR as u64 + len) as u32).ok_or(Commit::Err(StoreError::TooLarge))?);
            // every chunk on the chunkmap's device (LC-08)
            match self.core.alloc.alloc_extent(n, None, now_ns, Some(map.dev())) {
                ExtPlace::Run(p) => {
                    undo.ext.push((p, n));
                    chunks.push((p, n));
                }
                ExtPlace::NeedAg => return Err(Commit::Park),
                ExtPlace::Full => return Err(Commit::Err(StoreError::OutOfSpace)),
            }
        }
        Ok(Plan::Chunked { map, map_pages, chunks })
    }

    /// True when an extent of `n` pages at `start` can be rewritten in place atomically: one
    /// `RWF_ATOMIC` write, whose length must be a power of two inside the device's atomic unit
    /// and whose offset must be a multiple of that length (devices refuse anything else with
    /// `EINVAL`).
    fn atomic_extent(&self, start: PageId, n: u32) -> bool {
        let Some(d) = self.core.boot.devs.get(usize::from(start.dev())) else { return false };
        d.page_update == PageUpdateMode::Atomic
            && d.atomic_unit.is_some_and(|(min, max)| {
                let len = n * PAGE as u32;
                len >= min && len <= max && len.is_power_of_two() && start.byte_offset().is_multiple_of(u64::from(len))
            })
            && !self.core.ext.writes.contains_key(&start)
            && !self.core.ext.zeros.contains_key(&start)
    }

    /// True when a new version of `need` bytes of the key can stay in its page.
    fn fits(&self, cache: &PageCache, page: PageId, need: u16, s: &Staged) -> bool {
        let Some(st) = cache.page_state(page) else { return false };
        if st.corrupt {
            return false;
        }
        let h32 = s.kh.h32();
        let staged_same: u16 = st
            .pending
            .iter()
            .filter_map(|p| match p {
                Pending::Put(r) if p.is_key(s.db, h32, &s.key) => Some(r.desc.len),
                _ => None,
            })
            .sum();
        let free = PAGE_ROOM.saturating_sub(st.used_bytes()) + staged_same;
        if self.core.dev_mode(page.dev()) == PageUpdateMode::Atomic {
            let (recs, fr) = st.image();
            let img = cache.frame(fr);
            let old = recs
                .iter()
                .filter(|r| r.status == RecStatus::Live && r.db == s.db && r.h32 == h32 && r.key(img) == &*s.key)
                .map(|r| r.len)
                .fold(0, u16::max);
            if need <= old {
                return true;
            }
        }
        need <= free
    }

    /// Applies one planned change.
    #[allow(clippy::too_many_arguments)]
    fn apply(
        &mut self,
        cx: &mut StorageCtx<'_>,
        s: Staged,
        plan: Plan,
        seq: Seq,
        txn: u64,
        ticket: Option<TicketId>,
        fx: Option<TicketId>,
        otx: Option<&mut OpenTxn>,
    ) {
        let seq = s.seq.unwrap_or(seq);
        let mut tickets: SmallVec<[TicketId; 1]> = SmallVec::new();
        if matches!(s.change, Change::Put { .. } | Change::Delete) {
            tickets.extend(ticket);
            tickets.extend(fx);
            self.core.watch.touch(s.db, &s.key);
        }
        match s.change {
            Change::Confirm => {
                if let Some(c) = cur36(&s.cur)
                    && let Some(pos) = self.core.find_entry(s.db, s.slot, s.kh, c)
                {
                    self.core.index.table_mut(s.db, s.slot).set_sync_mark(pos, true);
                }
            }
            Change::Expired => self.apply_expired(cx, &s),
            Change::Put { vtype, expire_at, ref value } => {
                if matches!(s.cur, Cur::Absent) {
                    self.absent_superseded(s.db, &s.key);
                }
                let (rec_kind, ext_vtype) = match vtype {
                    VType::Hash => (REC_KIND_HASH, EXT_VTYPE_HASH),
                    VType::String => (REC_KIND_STRING, EXT_VTYPE_STRING),
                };
                match plan {
                    Plan::Small { page, fresh } => {
                        let rec = self.encode(&s, rec_kind, seq, expire_at, value, txn, tickets);
                        self.place_small_version(cx, &s, page, fresh, rec, vtype, expire_at, otx);
                    }
                    Plan::Ext { start, n_pages, inplace } => {
                        let class = class_for(n_pages).expect("extent class");
                        let twin = match s.cur {
                            Cur::Extent { start: old, .. } if !inplace => Some(old),
                            Cur::Extent { twin, .. } => twin,
                            _ => None,
                        };
                        let hdr = ext_header(EXT_KIND_VALUE, ext_vtype, s.db, start, seq, expire_at, n_pages, s.key.len(), value.len() as u64, txn, s.kh.h32(), twin);
                        let mut buf = AlignedBuf::zeroed(n_pages as usize * PAGE);
                        extent::encode_extent(&hdr, &s.key, &[value], self.core.crc(), &mut buf);
                        // tickets of a cancelled staged write move over with their count (D2)
                        let inherited = self.move_away(cx, &s, Some(start), inplace);
                        self.add_ext_write(start, hdr, s.db, s.key.clone(), buf, tickets, inherited, None, inplace);
                        let e = Entry { fp32: s.kh.fp32(), x8: class, page36: start.to36(), kind: Kind::Extent, vtype, sync_mark: false };
                        self.core.index_set(&s, cur36(&s.cur), e, expire_at);
                        if let Some(o) = otx {
                            o.keys.push((s.db, s.key.clone()));
                        }
                    }
                    Plan::Chunked { map, map_pages, chunks } => {
                        let class = class_for(map_pages).expect("chunkmap class");
                        let mut refs: SmallVec<[ChunkRef; 4]> = SmallVec::new();
                        let mut at = 0usize;
                        for &(cp, cn) in &chunks {
                            let len = (value.len() - at).min(CHUNK_MAX_PAYLOAD);
                            let part = &value[at..at + len];
                            at += len;
                            let ch = ext_header(EXT_KIND_CHUNK, 0, 0, cp, seq, None, cn, 0, len as u64, 0, 0, None);
                            let mut b = AlignedBuf::zeroed(cn as usize * PAGE);
                            extent::encode_extent(&ch, &[], &[part], self.core.crc(), &mut b);
                            let crc = extent::stored_crc(&b).unwrap_or(0);
                            refs.push(ChunkRef { page: cp, n_pages: cn, len: len as u32, crc });
                            *self.core.ext.chunk_rc.entry(cp).or_insert(0) += 1;
                            self.add_ext_write(cp, ch, 0, Box::default(), b, SmallVec::new(), SmallVec::new(), Some(map), false);
                        }
                        let mut body = vec![0u8; extent::CHUNKMAP_PREFIX + refs.len() * extent::CHUNK_REF_BYTES];
                        extent::encode_chunk_refs(&refs, &mut body);
                        let twin = match s.cur {
                            Cur::Extent { start: old, kind: Kind::ChunkMap, .. } => Some(old),
                            _ => None,
                        };
                        let hdr = ext_header(EXT_KIND_CHUNKMAP, ext_vtype, s.db, map, seq, expire_at, map_pages, s.key.len(), value.len() as u64, txn, s.kh.h32(), twin);
                        let mut buf = AlignedBuf::zeroed(map_pages as usize * PAGE);
                        extent::encode_extent(&hdr, &s.key, &[&body], self.core.crc(), &mut buf);
                        let inherited = self.move_away(cx, &s, Some(map), false);
                        self.core.ext.chunkmaps.insert(map, chunks.iter().copied().collect());
                        self.add_ext_write(map, hdr, s.db, s.key.clone(), buf, tickets, inherited, None, false);
                        if let Some(w) = self.core.ext.writes.get_mut(&map) {
                            w.wait_chunks = chunks.len() as u32;
                        }
                        let e = Entry { fp32: s.kh.fp32(), x8: class, page36: map.to36(), kind: Kind::ChunkMap, vtype, sync_mark: false };
                        self.core.index_set(&s, cur36(&s.cur), e, expire_at);
                        if let Some(o) = otx {
                            o.keys.push((s.db, s.key.clone()));
                        }
                    }
                    Plan::None => debug_assert!(false, "put without a plan"),
                }
            }
            Change::Delete => {
                if txn != 0 {
                    // a txn delete stages a TXN_DELETE_MARKER instead of a Remove (§14.6)
                    if let Plan::Small { page, fresh } = plan {
                        let rec = self.encode(&s, REC_KIND_TXN_DELETE_MARKER, seq, None, &[], txn, tickets);
                        self.place_small_version(cx, &s, page, fresh, rec, VType::String, None, otx);
                        self.core.counters.txn_markers += 1;
                    } else {
                        // nothing on media to delete
                        for t in tickets {
                            self.core.tickets.add_effect(t);
                            self.core.tickets.effect_resolved(t);
                        }
                    }
                    return;
                }
                self.apply_delete(cx, &s, seq, tickets);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn encode(&self, s: &Staged, kind: u8, seq: Seq, expire_at: Option<u64>, value: &[u8], txn: u64, tickets: SmallVec<[TicketId; 1]>) -> EncodedRecord {
        let txn_opt = (txn != 0).then_some(txn);
        let mut flags = 0;
        if expire_at.is_some() {
            flags |= REC_FLAG_HAS_EXPIRE;
        }
        if txn != 0 {
            flags |= REC_FLAG_TXN;
        }
        let h = RecHeader {
            kind,
            db: s.db,
            flags,
            seq,
            expire_at_ms: expire_at.unwrap_or(0),
            key_len: s.key.len() as u16,
            val_len: value.len() as u16,
            h32: s.kh.h32(),
            txn: txn_opt,
        };
        let len = small_len(s.key.len(), value.len(), txn != 0);
        let mut b = vec![0u8; len].into_boxed_slice();
        // sealed for its real location when placed (image.rs)
        encode_small(&h, &s.key, value, PageId(0), crate::layout::SMALL_HDR as u16, self.core.crc(), &mut b);
        EncodedRecord {
            bytes: b,
            desc: RecDesc {
                off: 0,
                len: len as u16,
                h32: h.h32,
                db: s.db,
                kind,
                flags,
                seq,
                txn,
                status: RecStatus::Live,
                expire_at_ms: expire_at.unwrap_or(0),
                key_len: h.key_len,
                val_len: h.val_len,
            },
            tickets,
        }
    }

    /// Stages a new small version of `s`'s key on `page` (moving the key away from its
    /// previous location when that is another page or an extent) and updates the index.
    #[allow(clippy::too_many_arguments)]
    fn place_small_version(
        &mut self,
        cx: &mut StorageCtx<'_>,
        s: &Staged,
        page: PageId,
        fresh: bool,
        mut rec: EncodedRecord,
        vtype: VType,
        expire_at: Option<u64>,
        otx: Option<&mut OpenTxn>,
    ) {
        if fresh {
            self.init_fresh(cx.cache, page);
        }
        let marker = rec.desc.kind == REC_KIND_TXN_DELETE_MARKER;
        let same_page = matches!(s.cur, Cur::Small { page: p, .. } if p == page);
        // this command's tickets gain one dependency: the new record
        for &t in &rec.tickets {
            self.core.tickets.add_effect(t);
        }
        if !same_page {
            // tickets of a staged effect this version cancels move over with the dependency
            // they already count (D2); counting them again would leave them unwritten forever
            let inherited = self.move_away(cx, s, None, false);
            self.inherit_into_txn(rec.desc.txn, &inherited);
            rec.tickets.extend(inherited);
            if !matches!(s.cur, Cur::Absent) {
                self.core.counters.moves += 1;
            }
        } else if rec.desc.txn != 0 {
            // a txn version never replaces the copy it supersedes before the commit (13.3 c)
            self.protect_in_page(cx.cache, page, s.db, s.kh.h32(), &s.key);
        }
        for &t in &rec.tickets {
            self.core.tickets.note_page(t, page, 0);
        }
        let seq = rec.desc.seq;
        let txn = rec.desc.txn;
        self.stage_put(cx.cache, page, rec);
        if !marker {
            let e = Entry { fp32: s.kh.fp32(), x8: s.kh.x8(), page36: page.to36(), kind: Kind::Small, vtype, sync_mark: false };
            self.core.index_set(s, cur36(&s.cur), e, expire_at);
        } else {
            if let Some(c) = cur36(&s.cur) {
                // a marker means "absent": the key leaves the index
                self.core.index_remove(s, c);
            }
            // the index no longer names the key, but the marker is its newest copy until it
            // is retired: a later write of the key must see it (E14)
            self.core.stale.note_absent(s.db, &s.key, page, seq, txn);
        }
        if let Some(o) = otx {
            o.keys.push((s.db, s.key.clone()));
            if !o.pages.contains(&page) {
                o.pages.push(page);
            }
            if marker {
                o.markers.push((s.db, s.key.clone(), page, seq));
            }
        }
    }

    /// A write creates the key again while its absent newest copy (a txn delete marker not
    /// retired yet) is still on media. The index does not name that copy, but it is a copy of
    /// the key: from now on it is an older copy, tracked like a moved-away version (E2, E14),
    /// zeroed once the new version is durable, and its retirement is moot. The placement of
    /// the new version already avoided its page (`plan_put`). Untracked, it counted as a kept
    /// in-page copy for a later write of the key placed on its page, and that write's
    /// third-copy wait needed a hand-off only the waiting write itself could bring.
    fn absent_superseded(&mut self, db: u8, key: &[u8]) {
        let Some(a) = self.core.stale.take_absent(db, key) else { return };
        self.core.retire.retain(|r| !(r.db == db && r.page == a.page && r.seq == a.seq && *r.key == *key));
        if self.core.dead(db, a.seq, a.txn) {
            return;
        }
        self.core.stale.add(db, key, StaleRef::Small { page: a.page }, CopyState::Source, a.seq);
    }

    pub(crate) fn init_fresh(&mut self, cache: &mut PageCache, page: PageId) {
        if cache.contains(page) {
            if let Some(st) = cache.page_state_mut(page) {
                st.fresh = st.recs.is_empty() && st.pending.is_empty() && st.inflight.is_none();
            }
            if cache.page_state(page).is_some() {
                return;
            }
        }
        let Some(f) = cache.alloc(page) else { return };
        cache.frame_mut(f).fill(0);
        cache.put_state(f, Box::new(PageState::fresh(page, f)));
    }

    /// Marks the key's copies in `page` protected (not live): a newer version lives elsewhere
    /// or is a txn version that is not committed. Returns the seq of the newest copy the page
    /// holds (M or the image in flight), `None` when it holds none.
    ///
    /// Records of the key that a FLUSHDB/FLUSHALL executed since (seq below the db's cutoff,
    /// durable or not) or of a txn that never committed are not copies of the key's current
    /// value: they are left alone and never become a tracked source. A source stays protected
    /// until a copy with a higher seq becomes durable; a flushed record registered as one when
    /// the key's post-flush copy was already durable waited for a hand-off that only the
    /// write which registered it could bring, and that write waited for it (E14).
    fn protect_in_page(&mut self, cache: &mut PageCache, page: PageId, db: u8, h32: u32, key: &[u8]) -> Option<Seq> {
        let f = cache.peek(page)?;
        let mut st = cache.take_state(f)?;
        let mut newest: Option<Seq> = None;
        let core = &self.core;
        let gone = |r: &RecDesc| core.cutoff.get(usize::from(r.db)).is_some_and(|&c| r.seq.0 < c) || core.dead(r.db, r.seq, r.txn);
        let mut mark = |recs: &mut [RecDesc], img: &[u8; PAGE]| {
            for r in recs.iter_mut() {
                if r.db == db && r.h32 == h32 && matches!(r.status, RecStatus::Live | RecStatus::Protected) && !gone(r) && r.key(img) == key {
                    r.status = RecStatus::Protected;
                    newest = Some(newest.map_or(r.seq, |n| n.max(r.seq)));
                }
            }
        };
        mark(&mut st.recs, cache.frame(st.m_frame));
        if let Some(inf) = st.inflight.as_mut() {
            mark(&mut inf.recs, cache.frame(inf.n_frame));
        }
        cache.put_state(f, st);
        newest
    }

    /// The key's version at `s.cur` stops being current (a newer one goes to `new_ext` or a
    /// page): copies on media become tracked sources (E2), staged effects there are cancelled
    /// and their tickets returned for the new effect (D2).
    fn move_away(&mut self, cx: &mut StorageCtx<'_>, s: &Staged, new_ext: Option<PageId>, inplace: bool) -> SmallVec<[TicketId; 1]> {
        let mut out = SmallVec::new();
        match s.cur {
            Cur::Absent => {}
            Cur::Small { page, .. } => {
                let h32 = s.kh.h32();
                if let Some(st) = cx.cache.page_state_mut(page)
                    && let Some(i) = st.pending.iter().position(|p| p.is_change() && p.is_key(s.db, h32, &s.key))
                {
                    let mut eff = st.pending.remove(i);
                    out.extend(eff.tickets_mut().drain(..));
                }
                if let Some(seq) = self.protect_in_page(cx.cache, page, s.db, h32, &s.key) {
                    self.core.stale.add(s.db, &s.key, StaleRef::Small { page }, CopyState::Source, seq);
                    self.note_older_copy(cx.cache, page, s.db, h32, &s.key);
                }
                self.core.note_page_space(cx.cache, page);
                Core::maybe_unpin(cx.cache, page);
            }
            Cur::Extent { start, n_pages, .. } => {
                if inplace || Some(start) == new_ext {
                    // rewritten in place: a staged write of the old image is replaced
                    if let Some(w) = self.core.ext.writes.get(&start)
                        && w.state == ExtState::Pending
                    {
                        let w = self.core.ext.writes.remove(&start).expect("present");
                        out.extend(w.tickets);
                        self.wake(w.waiters);
                    }
                    return out;
                }
                match self.core.ext.writes.get(&start).map(|w| (w.state, w.atomic)) {
                    Some((ExtState::Pending, false)) => {
                        // never written: the run goes back at once
                        let w = self.core.ext.writes.remove(&start).expect("present");
                        out.extend(w.tickets);
                        self.wake(w.waiters);
                        self.core.alloc.free_extent(start, w.hdr.n_pages);
                        self.cancel_chunks(start);
                    }
                    Some((ExtState::Pending, true)) => {
                        // an in-place rewrite never issued: the run still holds the version it
                        // was to replace, an older copy like any other (freeing the run left
                        // that version on media, untracked: a later delete of the key did not
                        // wait for it, and a crash could bring it back)
                        let w = self.core.ext.writes.remove(&start).expect("present");
                        out.extend(w.tickets);
                        self.wake(w.waiters);
                        self.core.stale.add(s.db, &s.key, StaleRef::Extent { page: start, n_pages }, CopyState::Source, cur_seq(&s.cur));
                    }
                    _ => self.core.stale.add(s.db, &s.key, StaleRef::Extent { page: start, n_pages }, CopyState::Source, cur_seq(&s.cur)),
                }
            }
        }
        out
    }

    /// An effect of txn `txn` supersedes a staged write that is not part of it and inherits
    /// that write's tickets (D2). The superseded write never reaches media, and the txn's
    /// version of the key is valid only once the txn's commit is durable (E6, §17.5): until
    /// then a crash brings back the key's state from before both. Those tickets therefore also
    /// wait for the commit (one metadata dependency each, released with the txn's barriers).
    /// Without it, a write acknowledged as written or durable was lost when the txn that
    /// superseded it did not commit before a crash, and the key showed the txn's effect while
    /// its other keys did not.
    fn inherit_into_txn(&mut self, txn: u64, inherited: &[TicketId]) {
        if txn == 0 {
            return;
        }
        for &t in inherited {
            if self.core.tickets.is_internal(t) {
                continue;
            }
            self.core.tickets.add_meta(t);
            match self.core.txns.get_mut(&txn) {
                Some(o) => o.barriers.push(t),
                // the executing commit registers its txn after applying its changes
                None => self.core.txn_inherit.push((txn, t)),
            }
        }
    }

    /// A key moves away from `page`, which still holds an older superseded copy of it next to
    /// the current one: that copy is zeroed by a foreground scrub, and until then it counts as
    /// one more copy, so the moved version waits (third-copy rule, E14, I11).
    fn note_older_copy(&mut self, cache: &mut PageCache, page: PageId, db: u8, h32: u32, key: &[u8]) {
        let copies = cache.page_state(page).map_or(0, |st| {
            let (recs, fr) = st.image();
            let img = cache.frame(fr);
            recs.iter().filter(|r| r.db == db && r.h32 == h32 && r.status != RecStatus::Droppable && r.key(img) == key).count()
        });
        if copies > 1 {
            self.core.stale.set_older(db, key, page, true);
            self.core.older_pages.push((page, db, key.into()));
            if let Some(st) = cache.page_state_mut(page) {
                st.scrub = true;
            }
            self.core.mark_dirty(cache, page);
        }
    }

    /// Cancels the staged chunk writes of a chunkmap that was never written.
    fn cancel_chunks(&mut self, map: PageId) {
        let Some(chunks) = self.core.ext.chunkmaps.remove(&map) else { return };
        for (c, n) in chunks {
            let rc = self.core.ext.chunk_rc.entry(c).or_insert(1);
            *rc = rc.saturating_sub(1);
            if *rc == 0 {
                self.core.ext.chunk_rc.remove(&c);
                match self.core.ext.writes.get(&c).map(|w| w.state) {
                    Some(ExtState::Pending) => {
                        self.core.ext.writes.remove(&c);
                        self.core.alloc.free_extent(c, n);
                    }
                    Some(_) => {
                        // in flight: freed once its write is durable
                        if let Some(w) = self.core.ext.writes.get_mut(&c) {
                            w.parent = None;
                        }
                    }
                    None => self.core.alloc.free_extent(c, n),
                }
            }
        }
    }

    /// Releases the chunks of a chunkmap whose zeroing is durable.
    fn release_chunks(&mut self, map: PageId) {
        let Some(chunks) = self.core.ext.chunkmaps.remove(&map) else { return };
        for (c, n) in chunks {
            let rc = self.core.ext.chunk_rc.entry(c).or_insert(1);
            *rc = rc.saturating_sub(1);
            if *rc == 0 {
                self.core.ext.chunk_rc.remove(&c);
                if !self.core.ext.writes.contains_key(&c) {
                    self.core.alloc.free_extent(c, n);
                }
            }
        }
    }

    fn wake(&mut self, waiters: SmallVec<[(ReqId, u32); 1]>) {
        for (id, generation) in waiters {
            if self.reqs.live_gen(id, generation) {
                let r = self.reqs.get_mut(id).expect("live");
                r.io = r.io.saturating_sub(1);
                self.reqs.check_ready(id);
            }
        }
    }

    /// Stages an extent write. `tickets` (the executing command's) gain one dependency each;
    /// `inherited` come from a staged effect this write cancels (D2) and keep the dependency
    /// they already count.
    #[allow(clippy::too_many_arguments)]
    fn add_ext_write(
        &mut self,
        start: PageId,
        hdr: ExtHeader,
        db: u8,
        key: Box<[u8]>,
        buf: AlignedBuf,
        mut tickets: SmallVec<[TicketId; 1]>,
        inherited: SmallVec<[TicketId; 1]>,
        parent: Option<PageId>,
        atomic: bool,
    ) {
        for &t in &tickets {
            self.core.tickets.add_effect(t);
        }
        self.inherit_into_txn(hdr.txn, &inherited);
        tickets.extend(inherited);
        let mut waiters = SmallVec::new();
        if let Some(old) = self.core.ext.writes.remove(&start) {
            // a staged image of the same run never reached media (callers cancel it through
            // `move_away` first); whatever still waits on it moves to the new image
            debug_assert!(old.state == ExtState::Pending);
            if old.hdr.txn != hdr.txn {
                self.inherit_into_txn(hdr.txn, &old.tickets);
            }
            tickets.extend(old.tickets);
            waiters = old.waiters;
        }
        for &t in &tickets {
            self.core.tickets.note_extent(t, start);
        }
        // header reads of this run in flight may return the previous header (§16.4, §16.5)
        self.core.alloc.note_run_written(start);
        self.core.ext.writes.insert(
            start,
            ExtWrite { hdr, db, key, buf: Some(buf), tickets, state: ExtState::Pending, wait_chunks: 0, parent, atomic, waiters, wid: 0, failed: None },
        );
    }

    fn apply_delete(&mut self, cx: &mut StorageCtx<'_>, s: &Staged, seq: Seq, tickets: SmallVec<[TicketId; 1]>) {
        for &t in &tickets {
            self.core.tickets.add_effect(t);
        }
        // every older copy must be gone before the newest is dropped (D1): foreground now
        if self.core.stale.count(s.db, &s.key) > 0 {
            self.core.stale.require(s.db, &s.key, true);
            self.core.fg_keys.push((s.db, s.key.clone()));
        }
        match s.cur {
            Cur::Absent => {
                for t in tickets {
                    self.core.tickets.effect_resolved(t);
                }
            }
            Cur::Small { page, .. } => {
                self.core.index_remove(s, page.to36());
                self.stage_remove(cx.cache, page, s.db, s.kh.h32(), s.key.clone(), seq, tickets);
                // the deleted copy counts towards the key's copies until it is gone (E14): a
                // write of the key elsewhere meanwhile waits like a third copy
                if let Some(seq) = self.protect_in_page(cx.cache, page, s.db, s.kh.h32(), &s.key) {
                    self.core.stale.add(s.db, &s.key, StaleRef::Small { page }, CopyState::Source, seq);
                }
            }
            Cur::Extent { start, n_pages, .. } => {
                self.core.index_remove(s, start.to36());
                let mut tickets = tickets;
                if self.core.ext.writes.get(&start).is_some_and(|w| w.state == ExtState::Pending && w.atomic) {
                    // an in-place rewrite never issued: the run still holds the version it was
                    // to replace; that version is zeroed below like any copy on media, and the
                    // rewrite's tickets wait for that zeroing
                    let w = self.core.ext.writes.remove(&start).expect("present");
                    tickets.extend(w.tickets);
                    self.wake(w.waiters);
                }
                match self.core.ext.writes.get(&start).map(|w| w.state) {
                    Some(ExtState::Pending) => {
                        // never written: nothing at `start` to drop; the delete completes when
                        // the older copies are gone (D2)
                        let w = self.core.ext.writes.remove(&start).expect("present");
                        let mut all = tickets;
                        all.extend(w.tickets);
                        self.wake(w.waiters);
                        self.core.alloc.free_extent(start, w.hdr.n_pages);
                        self.cancel_chunks(start);
                        if self.core.stale.count_older(s.db, &s.key, seq) == 0 {
                            for t in all {
                                self.core.tickets.effect_resolved(t);
                            }
                        } else {
                            self.core.gone_wait.push((s.db, s.key.clone(), seq, all));
                        }
                    }
                    // on media or in flight (then the zeroing is issued after it is durable)
                    _ => {
                        self.core.ext.zeros.insert(
                            start,
                            ExtZero::pending(n_pages, ZeroWhy::Delete { db: s.db, key: s.key.clone(), tickets }),
                        );
                        self.core.stale.add(s.db, &s.key, StaleRef::Extent { page: start, n_pages }, CopyState::Source, cur_seq(&s.cur));
                    }
                }
            }
        }
    }

    /// Lazy expiry (§16.4): the entry goes; the record stays as free space (no write) when it
    /// is the key's only copy. A key with older copies, or whose current version is still
    /// staged, is removed like a delete (its older copies first, D1, v1.2).
    fn apply_expired(&mut self, cx: &mut StorageCtx<'_>, s: &Staged) {
        if self.core.in_open_txn(s.db, &s.key) {
            // the version that expired belongs to a txn not committed yet (the active sweep
            // and key-less commands such as RANDOMKEY reach it outside its key queue): removed
            // now, its older copies were zeroed although recovery, finding the txn
            // uncommitted, falls back to them, while the txn's other keys revert (I4). It is
            // left for a sweep or a command after the commit.
            return;
        }
        self.core.counters.expired_keys += 1;
        self.core.watch.touch(s.db, &s.key);
        let others = self.core.stale.count(s.db, &s.key) > 0;
        if others {
            self.core.stale.require(s.db, &s.key, true);
            self.core.fg_keys.push((s.db, s.key.clone()));
        }
        match s.cur {
            Cur::Absent => {}
            Cur::Small { page, .. } => {
                self.core.index_remove(s, page.to36());
                let h32 = s.kh.h32();
                let Some(f) = cx.cache.peek(page) else { return };
                let staged = cx.cache.state(f).is_some_and(|st| st.pending.iter().any(|p| p.is_change() && p.is_key(s.db, h32, &s.key)));
                let mut older_here = false;
                if let Some(st) = cx.cache.state(f) {
                    let (recs, fr) = st.image();
                    let img = cx.cache.frame(fr);
                    older_here = recs.iter().filter(|r| r.db == s.db && r.h32 == h32 && r.status != RecStatus::Droppable && r.key(img) == &*s.key).count() > 1;
                }
                if others || staged || older_here {
                    // later writes of the key get seqs from `next_seq` on
                    let seq = self.core.next_seq;
                    self.stage_remove(cx.cache, page, s.db, h32, s.key.clone(), seq, SmallVec::new());
                    if let Some(seq) = self.protect_in_page(cx.cache, page, s.db, h32, &s.key) {
                        self.core.stale.add(s.db, &s.key, StaleRef::Small { page }, CopyState::Source, seq);
                    }
                    return;
                }
                if let Some(mut st) = cx.cache.take_state(f) {
                    // both images: the record list of an image in flight becomes M's when it
                    // is durable, and a copy left Live there would come back as the key's
                    // current version although the index no longer names it (it then counted
                    // as a third copy of the next write of the key, which waited forever on a
                    // copy whose hand-off only that write could bring)
                    let mark = |recs: &mut [RecDesc], img: &[u8; PAGE]| {
                        for r in recs.iter_mut() {
                            if r.status == RecStatus::Live && r.db == s.db && r.h32 == h32 && r.key(img) == &*s.key {
                                r.status = RecStatus::Droppable;
                            }
                        }
                    };
                    mark(&mut st.recs, cx.cache.frame(st.m_frame));
                    if let Some(inf) = st.inflight.as_mut() {
                        mark(&mut inf.recs, cx.cache.frame(inf.n_frame));
                    }
                    cx.cache.put_state(f, st);
                }
                self.core.note_page_space(cx.cache, page);
            }
            Cur::Extent { start, n_pages, .. } => {
                self.core.index_remove(s, start.to36());
                if self.core.ext.writes.get(&start).is_some_and(|w| w.state == ExtState::Pending && w.atomic) {
                    // an in-place rewrite never issued: the run still holds the version it was
                    // to replace, removed like a delete (its tickets wait for the zeroing)
                    let w = self.core.ext.writes.remove(&start).expect("present");
                    self.wake(w.waiters);
                    self.core.ext.zeros.insert(start, ExtZero::pending(n_pages, ZeroWhy::Delete { db: s.db, key: s.key.clone(), tickets: w.tickets }));
                    self.core.stale.add(s.db, &s.key, StaleRef::Extent { page: start, n_pages }, CopyState::Source, cur_seq(&s.cur));
                    return;
                }
                if matches!(self.core.ext.writes.get(&start).map(|w| w.state), Some(ExtState::Pending)) {
                    // the expired version never reaches media: its writer's tickets complete
                    // once the older copies it was to shadow are gone (D1, D2), like a delete
                    // of a version never written. Resolved at once, an older copy still on
                    // media came back after a crash although the write was acknowledged
                    let w = self.core.ext.writes.remove(&start).expect("present");
                    let seq = w.hdr.seq;
                    self.wake(w.waiters);
                    self.core.alloc.free_extent(start, w.hdr.n_pages);
                    self.cancel_chunks(start);
                    if self.core.stale.count_older(s.db, &s.key, seq) == 0 {
                        for t in w.tickets {
                            self.core.tickets.effect_resolved(t);
                        }
                    } else {
                        self.core.gone_wait.push((s.db, s.key.clone(), seq, w.tickets));
                    }
                    return;
                }
                if !self.core.ext.zeros.contains_key(&start) {
                    self.core.ext.zeros.insert(start, ExtZero::pending(n_pages, ZeroWhy::Delete { db: s.db, key: s.key.clone(), tickets: SmallVec::new() }));
                    self.core.stale.add(s.db, &s.key, StaleRef::Extent { page: start, n_pages }, CopyState::Source, cur_seq(&s.cur));
                }
            }
        }
    }

    pub(crate) fn stage_put(&mut self, cache: &mut PageCache, page: PageId, mut rec: EncodedRecord) {
        if let Some(st) = cache.page_state_mut(page) {
            let (db, h32) = (rec.desc.db, rec.desc.h32);
            let mut inherited: SmallVec<[TicketId; 1]> = SmallVec::new();
            if let Some(i) = st.pending.iter().position(|p| p.is_change() && p.is_key(db, h32, rec.key())) {
                // D2: a superseded staged effect hands its tickets to the survivor
                let mut old = st.pending.remove(i);
                let same_txn = matches!(&old, Pending::Put(o) if o.desc.txn == rec.desc.txn);
                if !same_txn {
                    inherited.extend(old.tickets_mut().iter().copied());
                }
                rec.tickets.extend(old.tickets_mut().drain(..));
            }
            let txn = rec.desc.txn;
            st.pending.push(Pending::Put(rec));
            self.inherit_into_txn(txn, &inherited);
        }
        self.core.mark_dirty(cache, page);
        self.core.note_page_space(cache, page);
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn stage_remove(&mut self, cache: &mut PageCache, page: PageId, db: u8, h32: u32, key: Box<[u8]>, seq: Seq, mut tickets: SmallVec<[TicketId; 1]>) {
        if let Some(st) = cache.page_state_mut(page) {
            if let Some(i) = st.pending.iter().position(|p| p.is_change() && p.is_key(db, h32, &key)) {
                let mut old = st.pending.remove(i);
                tickets.extend(old.tickets_mut().drain(..));
            }
            st.pending.push(Pending::Remove { db, h32, key, seq, tickets });
        } else {
            for t in tickets {
                self.core.tickets.effect_resolved(t);
            }
            return;
        }
        self.core.mark_dirty(cache, page);
        self.core.note_page_space(cache, page);
    }

    /// Stages the retirement of an absent newest copy (`Pending::Retire`).
    fn stage_retire(&mut self, cache: &mut PageCache, page: PageId, db: u8, key: Box<[u8]>, seq: Seq) {
        let h32 = KeyHash::of(self.core.cfg.hash_seed, &key).h32();
        let Some(st) = cache.page_state_mut(page) else { return };
        if st.pending.iter().any(|p| matches!(p, Pending::Retire { seq: s, .. } if *s == seq) && p.is_key(db, h32, &key)) {
            return;
        }
        st.pending.push(Pending::Retire { db, h32, key, seq, tickets: SmallVec::new() });
        self.core.mark_dirty(cache, page);
    }
}

/// An extent header for a write.
#[allow(clippy::too_many_arguments)]
fn ext_header(
    kind: u8,
    vtype: u8,
    db: u8,
    page: PageId,
    seq: Seq,
    expire_at: Option<u64>,
    n_pages: u32,
    key_len: usize,
    val_len: u64,
    txn: u64,
    h32: u32,
    twin: Option<PageId>,
) -> ExtHeader {
    let mut flags = 0;
    if expire_at.is_some() {
        flags |= EXT_FLAG_HAS_EXPIRE;
    }
    if txn != 0 {
        flags |= EXT_FLAG_TXN;
    }
    ExtHeader { kind, vtype, db, flags, page, seq, expire_at_ms: expire_at.unwrap_or(0), n_pages, key_len: key_len as u32, val_len, txn, h32, twin }
}

/// A SCAN step's walk over the partition's tables (§16.6): the spans walked and the
/// de-duplicated page references (nothing per entry, LC-18).
pub(crate) struct ScanPlan {
    /// `(slot, fp_lo, fp_hi_excl)` of every table span of the step.
    pub(crate) spans: SmallVec<[(u16, u32, u64); 4]>,
    pub(crate) refs: Vec<PageRef>,
    pub(crate) next: u64,
}

impl PartitionStore {
    // ---------------------------------------------------------------------------------------
    // FLUSHDB / FLUSHALL (§16.5) and barriers

    fn exec_flush(&mut self, cx: &mut StorageCtx<'_>, db: Option<u8>, origin: Origin) -> Result<ExecOut, StoreError> {
        if self.core.state == PartState::ReadOnly {
            return Err(StoreError::ReadOnly);
        }
        // every existing record has a smaller seq (replicas apply the primary's cutoff, §16.5)
        let cutoff = match origin {
            Origin::Replicated(s) | Origin::Image(s) | Origin::Snapshot(s) => s,
            _ => self.core.next_seq,
        };
        if cutoff >= self.core.next_seq {
            self.core.next_seq = cutoff.next().unwrap_or(cutoff);
        }
        let dbs = self.core.cfg.databases.clamp(1, 16);
        let mut mask = 0u16;
        for d in 0..dbs {
            if db.is_some_and(|x| x != d) {
                continue;
            }
            mask |= 1 << d;
            let c = &mut self.core.pmeta.flush_cutoff[usize::from(d)];
            *c = (*c).max(cutoff.0);
            self.core.cutoff[usize::from(d)] = *c;
            self.core.index.drop_db(d);
            self.core.stale.drop_db(d);
        }
        self.core.watch.touch_dbs(mask);
        // effects deferred behind copies of the flushed dbs go ahead (those copies are dead
        // by the cutoff; nothing else would wake them)
        let deferred: Vec<PageId> = self.core.deferred.drain().collect();
        for p in deferred {
            self.core.mark_dirty(cx.cache, p);
        }
        self.core.fg_keys.retain(|(d, _)| mask & (1 << d) == 0);
        self.core.retire.retain(|r| mask & (1 << r.db) == 0);
        let t = self.core.tickets.create(TICKET_HAS_META | TICKET_HAS_DELETE);
        self.core.tickets.add_meta(t);
        self.core.tickets.seal(t);
        let all = db.is_none();
        let m = self.core.pmeta.clone();
        self.core.meta_req(cx.meta, MetaReq::PartitionMeta { m }, MetaWait::Cutoff { cutoff: cutoff.0, dbs: mask, ticket: t, all });
        if all {
            // the partition allocator restarts from keyed draws (§16.5 step 4); the old AGs are
            // released once their writes are done and the cutoffs durable
            self.core.retiring_ags.extend(self.core.alloc.release_all());
            self.core.ext.chunkmaps.clear();
            self.core.ext.chunk_rc.clear();
            self.core.reclaim_pages.clear();
            self.core.reclaim_ext.clear();
            self.core.alloc.want_spare(AgKind::Small);
        } else {
            self.start_reclaim(cx.cache);
        }
        let group = self.core.cfg.produce_groups.then(|| Group { seq: cutoff, ops: vec![Op::FlushDb { db }] });
        Ok(ExecOut { ticket: Some(t), group, seq: Some(cutoff) })
    }

    /// Queues the paced FLUSHDB reclaim walk over every owned page and extent (§16.5).
    fn start_reclaim(&mut self, cache: &PageCache) {
        self.core.reclaim_pages.clear();
        self.core.reclaim_ext.clear();
        let pages: Vec<PageId> = self.core.alloc.owned_small_pages().filter(|p| !cache.contains(*p)).collect();
        self.core.reclaim_pages.extend(pages);
        let starts: Vec<PageId> = self.core.alloc.extent_starts().collect();
        // a run written or freed before its header is read is no dead extent of this walk
        self.core.alloc.unwatch_idle();
        for &s in &starts {
            self.core.alloc.watch_header(s);
        }
        self.core.reclaim_ext.extend(starts);
        self.core.counters.flush_reclaim_pending_pages = (self.core.reclaim_pages.len() + self.core.reclaim_ext.len()) as u64;
    }

    /// A barrier: its ticket is durable once everything staged before it is durable, every txn
    /// open at that moment is committed and every older copy tracked at that moment is zeroed
    /// (SHUTDOWN's drain, SAVE, §16.2). It depends on exactly those effects (one dependency
    /// each, handed over like any ticket), never on work that arrives later, so a barrier under
    /// sustained load completes in bounded time.
    fn exec_barrier(&mut self, cache: &mut PageCache) -> ExecOut {
        let t = self.core.tickets.create(TICKET_HAS_META);
        let pages: Vec<PageId> = self.own_pages(cache).filter(|(_, st)| !st.pending.is_empty() || st.inflight.is_some()).map(|(p, _)| p).collect();
        // `n` effects still to be written, `w` written ones waiting for their flush
        let (mut n, mut w) = (0u32, 0u32);
        for p in pages {
            let Some(st) = cache.page_state_mut(p) else { continue };
            for e in st.pending.iter_mut() {
                e.tickets_mut().push(t);
                n += 1;
            }
            if let Some(inf) = st.inflight.as_mut() {
                for e in inf.applied.iter_mut() {
                    e.tickets_mut().push(t);
                    match inf.state {
                        InflightState::Writing => n += 1,
                        InflightState::AwaitFlush => w += 1,
                    }
                }
            }
        }
        let c = &mut self.core;
        for x in c.ext.writes.values_mut() {
            x.tickets.push(t);
            match x.state {
                ExtState::AwaitFlush => w += 1,
                _ => n += 1,
            }
        }
        for z in c.ext.zeros.values_mut() {
            z.waiters.push(t);
            match z.state {
                ExtState::AwaitFlush => w += 1,
                _ => n += 1,
            }
        }
        for g in c.gone_wait.iter_mut() {
            g.3.push(t);
            n += 1;
        }
        n += c.stale.watch_all(t);
        let mut meta = 0;
        for o in c.txns.values_mut() {
            o.barriers.push(t);
            meta += 1;
        }
        for _ in 0..n {
            c.tickets.add_effect(t);
        }
        for _ in 0..w {
            c.tickets.add_durable_only(t);
        }
        for _ in 0..meta {
            c.tickets.add_meta(t);
        }
        c.tickets.seal(t);
        // the tracked copies go by foreground writes (the barrier waits for them)
        let keys: Vec<(u8, Box<[u8]>)> = c.stale.iter().map(|(db, k, _)| (db, Box::<[u8]>::from(k))).collect();
        for (db, k) in keys {
            self.core.stale.require(db, &k, false);
            self.core.fg_keys.push((db, k));
        }
        ExecOut { ticket: Some(t), group: None, seq: None }
    }

    // ---------------------------------------------------------------------------------------
    // SCAN and RANDOMKEY (§16.6)

    /// The walk of one SCAN step from `cursor`.
    pub(crate) fn scan_plan(&self, db: u8, cur: u64, count: u32, vtype: Option<VType>) -> ScanPlan {
        let id = self.core.cfg.id;
        let parts = self.core.cfg.partitions;
        let after_partition = cursor::encode(id.saturating_add(1), 0, 0);
        let mut plan = ScanPlan { spans: SmallVec::new(), refs: Vec::new(), next: after_partition };
        let (p, mut sl, mut fp) = cursor::decode(cur);
        if p != id || db >= self.core.cfg.databases {
            return plan;
        }
        let local = self.core.index.local_slots();
        let filter = ScanFilter { vtype, unmarked_only: false };
        let mut left = count.max(1) as usize;
        let mut empty = 0u16;
        loop {
            if sl >= local {
                plan.next = if id + 1 >= parts { cursor::encode(parts, 0, 0) } else { after_partition };
                return plan;
            }
            let slot = self.core.index.slot_of(sl);
            let Some(t) = self.core.index.table(db, slot) else {
                empty += 1;
                sl += 1;
                fp = 0;
                if empty >= SCAN_EMPTY_TABLES {
                    plan.next = if sl >= local { after_partition } else { cursor::encode(id, sl, 0) };
                    return plan;
                }
                continue;
            };
            let span = t.scan_pages(fp, left, filter, &mut plan.refs);
            plan.spans.push((slot, span.fp_lo, span.fp_hi_excl));
            match span.next {
                Some(nfp) => {
                    plan.next = cursor::encode(id, sl, nfp);
                    return plan;
                }
                None => {
                    left = left.saturating_sub(t.len());
                    sl += 1;
                    fp = 0;
                    if left == 0 || plan.refs.len() >= kv_index::table::SCAN_STEP_MAX_PAGES {
                        plan.next = if sl >= local { if id + 1 >= parts { cursor::encode(parts, 0, 0) } else { after_partition } } else { cursor::encode(id, sl, 0) };
                        return plan;
                    }
                }
            }
        }
    }

    /// The page the first RANDOMKEY draw reads (prefetch; the same draw `Keyspace::random_key`
    /// makes first).
    fn random_need(&self, db: u8, rv: u64) -> Option<Need> {
        if db >= self.core.cfg.databases {
            return None;
        }
        let r = crate::keyspace::splitmix(rv | 1);
        let slot = self.core.index.random_table(db, r)?;
        let t = self.core.index.table(db, slot)?;
        let (_, e) = t.random_entry(crate::keyspace::splitmix(r ^ 0x5151))?;
        let page = PageId::from36(e.page36);
        Some(match e.kind {
            Kind::Small => Need::Page(page),
            _ => Need::Extent { start: page, pages: 1, chunks: false },
        })
    }

    /// Runs a SCAN step or a RANDOMKEY attempt (§16.6): keys only, never values (LC-17).
    /// Returns the next cursor (SCAN) or 0 (RANDOMKEY, the key, if any, is in `out`).
    pub fn scan_keys(&mut self, cx: &mut StorageCtx<'_>, id: ReqId, matcher: &dyn Fn(&[u8]) -> bool, out: &mut Vec<Box<[u8]>>) -> ExecStatus<u64> {
        self.core.now = cx.now;
        let kind = match self.reqs.get(id) {
            Some(r) => r.req.kind,
            None => return ExecStatus::Failed(StoreError::Io(-libc::ENOENT)),
        };
        match kind {
            ReqKind::RandomKey { .. } => {
                let mut key = None;
                let st = self.exec(cx, id, &mut |ks| {
                    key = ks.random_key()?;
                    Ok(())
                });
                match st {
                    ExecStatus::Done((), o) => {
                        out.extend(key);
                        ExecStatus::Done(0, o)
                    }
                    ExecStatus::Requeued => ExecStatus::Requeued,
                    ExecStatus::Failed(e) => ExecStatus::Failed(e),
                }
            }
            ReqKind::ScanStep { db, cursor: cur, count, vtype } => {
                let plan = self.scan_plan(db, cur, count, vtype);
                self.scan_step(cx, id, db, plan, vtype, matcher, out)
            }
            ReqKind::SlotIter { slot, fp_from, max_entries } => {
                let db = self.reqs.get(id).map_or(0, |r| r.req.db);
                let plan = self.slot_plan(db, slot, fp_from, max_entries);
                self.scan_step(cx, id, db, plan, None, matcher, out)
            }
            // merge-sync pruning belongs to replica full sync (M2.2, §23.7), not to M1
            ReqKind::SyncPrune { .. } => {
                if let Some(r) = self.take_req(id) {
                    self.finish(cx.cache, id, r);
                }
                ExecStatus::Failed(StoreError::Io(-libc::ENOSYS))
            }
            _ => ExecStatus::Failed(StoreError::Io(-libc::EINVAL)),
        }
    }

    /// The walk of one table of a sync or migration source (§23.7): the keys of `(db, slot)`
    /// from `fp_from`; the result's next value is the span's end (`2^32` once the slot is
    /// done).
    fn slot_plan(&self, db: u8, slot: u16, fp_from: u32, max_entries: u32) -> ScanPlan {
        let mut plan = ScanPlan { spans: SmallVec::new(), refs: Vec::new(), next: 1 << 32 };
        if db >= self.core.cfg.databases || slot % self.core.cfg.partitions != self.core.cfg.id {
            return plan;
        }
        if let Some(t) = self.core.index.table(db, slot) {
            let span = t.scan_pages(fp_from, max_entries as usize, ScanFilter::ALL, &mut plan.refs);
            plan.spans.push((slot, span.fp_lo, span.fp_hi_excl));
            plan.next = span.fp_hi_excl;
        }
        plan
    }

    #[allow(clippy::too_many_arguments)]
    fn scan_step(
        &mut self,
        cx: &mut StorageCtx<'_>,
        id: ReqId,
        db: u8,
        plan: ScanPlan,
        vtype: Option<VType>,
        matcher: &dyn Fn(&[u8]) -> bool,
        out: &mut Vec<Box<[u8]>>,
    ) -> ExecStatus<u64> {
        let Some(mut r) = self.take_req(id) else { return ExecStatus::Failed(StoreError::Io(-libc::ENOENT)) };
        if self.core.state == PartState::Loading {
            self.finish(cx.cache, id, r);
            return ExecStatus::Failed(StoreError::Loading);
        }
        if let Some(e) = r.failed {
            self.finish(cx.cache, id, r);
            return ExecStatus::Failed(e);
        }
        // every referenced page must be present (the index may have changed since submit)
        let mut needs: SmallVec<[Need; 4]> = SmallVec::new();
        for pr in &plan.refs {
            let page = PageId::from36(pr.page36);
            match pr.kind {
                Kind::Small => {
                    if cx.cache.page_state(page).is_none() {
                        needs.push(Need::Page(page));
                    }
                }
                _ => {
                    let have = self.core.ext.writes.contains_key(&page) || r.fetched.iter().any(|f| f.start == page && f.complete());
                    if !have {
                        needs.push(Need::Extent { start: page, pages: 1, chunks: false });
                    }
                }
            }
        }
        if !needs.is_empty() {
            self.reqs.slots[id.0 as usize] = Some(r);
            for n in needs {
                self.need(cx, id, n);
            }
            self.reqs.check_ready(id);
            return ExecStatus::Requeued;
        }
        let now_ms = cx.now.unix_ms;
        let seed = self.core.cfg.hash_seed;
        let parts = self.core.cfg.partitions;
        let in_span = |key: &[u8], kh: KeyHash| -> bool {
            let slot = key_slot(key);
            slot % parts == self.core.cfg.id
                && plan.spans.iter().any(|&(s, lo, hi)| s == slot && kh.fp32() >= lo && u64::from(kh.fp32()) < hi)
        };
        let live = |exp: Option<u64>| exp.is_none_or(|t| t > now_ms);
        let vt_ok = |k: u8| match vtype {
            None => true,
            Some(VType::Hash) => k == REC_KIND_HASH,
            Some(VType::String) => k == REC_KIND_STRING,
        };
        let mut extra_needs: SmallVec<[Need; 2]> = SmallVec::new();
        for pr in &plan.refs {
            let page = PageId::from36(pr.page36);
            match pr.kind {
                Kind::Small => {
                    let Some(st) = cx.cache.page_state(page) else { continue };
                    if st.corrupt {
                        continue;
                    }
                    let mut seen: SmallVec<[u32; 8]> = SmallVec::new();
                    for p in st.pending.iter().rev() {
                        let Pending::Put(rec) = p else {
                            seen.push(p.h32());
                            continue;
                        };
                        if rec.desc.db != db || rec.desc.kind == REC_KIND_TXN_DELETE_MARKER {
                            seen.push(rec.desc.h32);
                            continue;
                        }
                        let key = rec.key();
                        let kh = KeyHash::of(seed, key);
                        seen.push(rec.desc.h32);
                        if in_span(key, kh)
                            && self.core.indexed_at(db, key_slot(key), kh, page.to36())
                            && live(rec.desc.expire_at())
                            && vt_ok(rec.desc.kind)
                            && matcher(key)
                        {
                            out.push(key.into());
                        }
                    }
                    let (recs, fr) = st.image();
                    let img = cx.cache.frame(fr);
                    for rd in recs {
                        if rd.status != RecStatus::Live || rd.db != db || rd.is_marker() {
                            continue;
                        }
                        let key = rd.key(img);
                        if seen.contains(&rd.h32) && st.pending.iter().any(|p| p.is_key(db, rd.h32, key)) {
                            continue; // the staged effect is the key's current version
                        }
                        let kh = KeyHash::of(seed, key);
                        if in_span(key, kh)
                            && self.core.indexed_at(db, key_slot(key), kh, page.to36())
                            && live(rd.expire_at())
                            && vt_ok(rd.kind)
                            && matcher(key)
                        {
                            out.push(key.into());
                        }
                    }
                }
                _ => {
                    let (hdr, key): (ExtHeader, &[u8]) = if let Some(w) = self.core.ext.writes.get(&page) {
                        (w.hdr, &w.key)
                    } else if let Some(fe) = r.fetched.iter().find(|f| f.start == page && f.complete()) {
                        let b = fe.buf.as_ref().map_or(&[][..], |b| b.as_slice());
                        let Some(h) = extent::parse_extent_header(b, page) else { continue };
                        let kl = h.key_len as usize;
                        if EXT_HDR + kl > b.len() {
                            extra_needs.push(Need::Extent { start: page, pages: (EXT_HDR + kl).div_ceil(PAGE) as u32, chunks: false });
                            continue;
                        }
                        (h, &b[EXT_HDR..EXT_HDR + kl])
                    } else {
                        continue;
                    };
                    if hdr.db != db || hdr.kind == EXT_KIND_CHUNK {
                        continue;
                    }
                    let kh = KeyHash::of(seed, key);
                    let vk = if hdr.vtype == EXT_VTYPE_HASH { REC_KIND_HASH } else { REC_KIND_STRING };
                    if in_span(key, kh) && self.core.indexed_at(db, key_slot(key), kh, page.to36()) && live(hdr.expire_at()) && vt_ok(vk) && matcher(key) {
                        out.push(key.into());
                    }
                }
            }
        }
        if !extra_needs.is_empty() {
            out.clear();
            self.reqs.slots[id.0 as usize] = Some(r);
            for n in extra_needs {
                self.need(cx, id, n);
            }
            self.reqs.check_ready(id);
            return ExecStatus::Requeued;
        }
        r.fetched.clear();
        self.finish(cx.cache, id, r);
        ExecStatus::Done(plan.next, ExecOut::none())
    }

    // ---------------------------------------------------------------------------------------
    // I/O completions

    /// Routes a completion of this partition's request.
    pub fn on_io_done(&mut self, cx: &mut StorageCtx<'_>, d: IoDone) {
        self.core.now = cx.now;
        if d.tag.partition != self.core.cfg.id {
            return;
        }
        let Some(kind) = self.core.io.remove(&d.tag.id) else {
            if let Some(b) = d.buf {
                self.core.give_buf(b);
            }
            return;
        };
        match kind {
            IoKind::PageRead { page, retried } => self.page_read_done(cx, page, retried, d),
            IoKind::PageWrite { page } => self.page_write_done(cx, page, d),
            IoKind::ExtRead { req, generation, idx, chunk } => self.ext_read_done(cx, req, generation, idx, chunk, d),
            IoKind::ExtWrite { start } => self.ext_write_done(cx, start, d),
            IoKind::ExtZero { start } => self.ext_zero_done(cx, start, d),
            IoKind::HeaderRead { start, why } => self.header_read_done(cx, start, why, d),
            IoKind::Recovery { id } => crate::recovery::on_read_done(self, cx, id, d),
        }
        self.process_durable(cx);
    }

    /// Writes made durable by a flush (any partition's ids; others are ignored).
    pub fn on_durable(&mut self, writes: &[WriteId]) {
        let me = self.core.cfg.id;
        for w in writes {
            if w.partition() == me
                && let Some(d) = self.core.durable_wait.remove(&w.local())
            {
                self.core.durable_q.push(d);
            }
        }
    }

    /// A flush failed: the writes it covered may be lost (§22): the partition goes read-only.
    pub fn on_flush_failed(&mut self, writes: &[WriteId], errno: i32) {
        if writes.iter().any(|w| w.partition() == self.core.cfg.id) {
            self.write_failed(errno);
        }
    }

    fn write_failed(&mut self, errno: i32) {
        if self.core.failed.is_none() {
            self.core.failed = Some(errno);
        }
        if self.core.state == PartState::Ready {
            self.core.state = PartState::ReadOnly;
        }
        self.core.tickets.fail_all(errno);
        // no write is issued any more: readers waiting for a staged or in-flight extent write
        // fail now instead of waiting for it
        let mut waiting: Vec<(ReqId, u32)> = Vec::new();
        for w in self.core.ext.writes.values_mut() {
            waiting.extend(w.waiters.drain(..));
        }
        for (id, generation) in waiting {
            if self.reqs.live_gen(id, generation) {
                let r = self.reqs.get_mut(id).expect("live");
                r.io = r.io.saturating_sub(1);
                r.failed = Some(StoreError::Io(errno));
                self.reqs.check_ready(id);
            }
        }
    }

    fn page_read_done(&mut self, cx: &mut StorageCtx<'_>, page: PageId, retried: bool, d: IoDone) {
        if let Err(e) = d.res {
            if let Some(b) = d.buf {
                self.core.give_buf(b);
            }
            if !retried && e != -libc::EINVAL {
                let buf = self.core.take_buf(PAGE);
                let tag = self.core.tag(IoKind::PageRead { page, retried: true });
                cx.io.push(IoReq::read(tag, page.dev(), page.byte_offset(), buf));
                return;
            }
            let Some(l) = self.core.loads.remove(&page) else { return };
            for (id, generation) in l.waiters {
                if self.reqs.live_gen(id, generation) {
                    let r = self.reqs.get_mut(id).expect("live");
                    r.io = r.io.saturating_sub(1);
                    r.failed = Some(StoreError::Io(e));
                    self.reqs.check_ready(id);
                }
            }
            if l.scrub || self.core.stale.at_page(page) > 0 {
                // a tracked copy that cannot be read cannot be zeroed: the deletes and writes
                // waiting for it could never complete, so they fail now (read-only, §22)
                self.write_failed(e);
            }
            return;
        }
        let Some(l) = self.core.loads.remove(&page) else {
            if let Some(b) = d.buf {
                self.core.give_buf(b);
            }
            return;
        };
        let Some(buf) = d.buf else {
            // a read reported without its buffer: read again (the waiters stay registered)
            self.core.loads.insert(page, l);
            self.core.load_retry.push(page);
            return;
        };
        if cx.cache.page_state(page).is_none() {
            let Some(f) = cx.cache.alloc(page) else {
                // no frame (the mapping of a new cache chunk failed): the waiters stay
                // registered and the page is read again at the next tick
                self.core.give_buf(buf);
                self.core.loads.insert(page, l);
                self.core.load_retry.push(page);
                return;
            };
            cx.cache.frame_mut(f).copy_from_slice(buf.page(0));
            let st = self.classify_loaded(cx, page, f);
            cx.cache.put_state(f, Box::new(st));
            self.core.note_page_space(cx.cache, page);
        }
        self.core.give_buf(buf);
        let Some(f) = cx.cache.peek(page) else { return };
        for (id, generation) in l.waiters {
            if self.reqs.live_gen(id, generation) {
                let r = self.reqs.get_mut(id).expect("live");
                r.io = r.io.saturating_sub(1);
                if !r.pins.contains(&page) {
                    r.pins.push(page);
                    cx.cache.pin(f);
                }
                self.reqs.check_ready(id);
            }
        }
        if l.scrub {
            if let Some(st) = cx.cache.state_mut(f) {
                st.scrub = true;
            }
            self.core.mark_dirty(cx.cache, page);
        }
        for (db, slot, fp32, x8) in l.expire {
            self.expire_check_small(cx, page, db, slot, fp32, x8);
        }
        // reclaim walk / warming: recompute the page's free space and drop dead space
        let dead = cx.cache.page_state(page).is_some_and(|st| st.recs.iter().any(|r| r.status == RecStatus::Droppable));
        if dead {
            self.core.note_page_space(cx.cache, page);
        }
        Core::maybe_unpin(cx.cache, page);
    }

    /// The in-RAM state of a page just read (§13.3, last bullet): Live is derived from the
    /// index; lower-seq copies are Superseded; absent newest copies with older copies are kept
    /// and retired later (D1); orphans are tracked or dropped.
    fn classify_loaded(&mut self, cx: &mut StorageCtx<'_>, page: PageId, f: crate::cache::FrameIdx) -> PageState {
        let crc = self.core.crc();
        let part = self.core.cfg.id;
        let seed = self.core.cfg.hash_seed;
        let parts = self.core.cfg.partitions;
        let mut views = Vec::new();
        let img: &[u8; PAGE] = cx.cache.frame(f);
        let parse = parse_small_page(img, page, part, crc, &mut views);
        if parse != PageParse::Ok {
            let mut st = PageState::loaded(page, f, SmallVec::new());
            // a page the index names cannot lose its header: corruption (§15)
            if self.core.alloc.page_free(page).is_some_and(|v| v != crate::alloc::FRESH) {
                st.corrupt = true;
                self.core.counters.corrupt_pages += 1;
            } else {
                st.fresh = true;
            }
            return st;
        }
        let mut recs: SmallVec<[RecDesc; 16]> = views
            .iter()
            .map(|v| RecDesc {
                off: v.off,
                len: v.len,
                h32: v.hdr.h32,
                db: v.hdr.db,
                kind: v.hdr.kind,
                flags: v.hdr.flags,
                seq: v.hdr.seq,
                txn: v.hdr.txn.unwrap_or(0),
                status: RecStatus::Live,
                expire_at_ms: v.hdr.expire_at_ms,
                key_len: v.hdr.key_len,
                val_len: v.hdr.val_len,
            })
            .collect();
        let n = recs.len();
        let mut retire: SmallVec<[RetireItem; 1]> = SmallVec::new();
        let mut done = vec![false; n];
        for i in 0..n {
            if done[i] {
                continue;
            }
            let (db, h32) = (recs[i].db, recs[i].h32);
            let key = views[i].key;
            // the key's records in this page (not dead), newest last
            let mut group: SmallVec<[usize; 2]> = SmallVec::new();
            for j in i..n {
                if !done[j] && recs[j].db == db && recs[j].h32 == h32 && views[j].key == key {
                    done[j] = true;
                    if self.core.dead(recs[j].db, recs[j].seq, recs[j].txn) {
                        recs[j].status = RecStatus::Droppable;
                    } else {
                        group.push(j);
                    }
                }
            }
            if group.is_empty() {
                continue;
            }
            group.sort_by_key(|&j| recs[j].seq);
            let newest = *group.last().expect("non-empty");
            let older = group.len() > 1;
            for &j in &group[..group.len() - 1] {
                recs[j].status = RecStatus::Superseded;
            }
            let slot = key_slot(key);
            if slot % parts != part || usize::from(db) >= usize::from(self.core.cfg.databases) {
                recs[newest].status = RecStatus::Droppable;
                continue;
            }
            let kh = KeyHash::of(seed, key);
            let rn = recs[newest];
            let open_txn = rn.txn != 0 && self.core.txn_open(rn.txn);
            if open_txn {
                for &j in &group[..group.len() - 1] {
                    recs[j].status = RecStatus::Protected;
                }
            }
            let here = self.core.indexed_at(db, slot, kh, page.to36());
            if here && !rn.is_marker() {
                recs[newest].status = RecStatus::Live;
                continue;
            }
            if rn.is_marker() || !here {
                let tracked = self.core.stale.state_at(db, key, page).is_some();
                let indexed_elsewhere = !rn.is_marker() && self.core.index.table(db, slot).is_some_and(|t| {
                    let mut c = SmallVec::new();
                    t.candidates(kh, &mut c);
                    !c.is_empty()
                });
                if tracked {
                    recs[newest].status = RecStatus::Live;
                } else if indexed_elsewhere {
                    // a copy of a key indexed elsewhere that nobody tracks: zeroed as a stale
                    // copy. An expired one is the expected leftover of an expiry without write
                    // (§16.4: the index entry went, the bytes stayed) followed by a new write of
                    // the key; anything else is unexpected (defensive, §13.3)
                    if rn.expire_at().is_none_or(|t| t > cx.now.unix_ms) {
                        self.core.counters.unexpected_orphans += 1;
                    }
                    self.core.stale.add(db, key, StaleRef::Small { page }, CopyState::Stale, rn.seq);
                    recs[newest].status = RecStatus::Live;
                    // nothing else schedules its zeroing: a paced scrub, like recovery's copies
                    // (§18.5); left without one, the entry outlived the run and a later write or
                    // delete of the key counted a copy nothing would remove
                    if !self.core.scrub_queue.contains(&page) {
                        self.core.scrub_queue.push_back(page);
                    }
                } else if open_txn || older || self.core.stale.count(db, key) > 0 {
                    // an absent newest copy (marker or expired) with older copies: kept until
                    // they are zeroed (D1, v1.2)
                    recs[newest].status = RecStatus::Protected;
                    self.core.stale.note_absent(db, key, page, rn.seq, rn.txn);
                    if !open_txn {
                        retire.push(RetireItem { db, key: key.into(), page, seq: rn.seq });
                    }
                } else {
                    if !rn.is_marker() && rn.expire_at().is_none_or(|t| t > cx.now.unix_ms) {
                        self.core.counters.unexpected_orphans += 1;
                    }
                    recs[newest].status = RecStatus::Droppable;
                }
            }
        }
        self.core.retire.extend(retire);
        PageState::loaded(page, f, recs)
    }

    fn page_write_done(&mut self, cx: &mut StorageCtx<'_>, page: PageId, d: IoDone) {
        if let Some(b) = d.buf {
            self.core.give_buf(b);
        }
        let Some(f) = cx.cache.peek(page) else { return };
        let Some(st) = cx.cache.state_mut(f) else { return };
        let Some(inf) = st.inflight.as_mut() else { return };
        if let Err(e) = d.res {
            inf.failed.get_or_insert(e);
        }
        inf.sub_ios = inf.sub_ios.saturating_sub(1);
        if inf.sub_ios > 0 {
            return;
        }
        let failed = inf.failed;
        let wid = inf.wid;
        let tickets: SmallVec<[TicketId; 4]> = inf.applied.iter().flat_map(|e| e.tickets().iter().copied()).collect();
        inf.state = InflightState::AwaitFlush;
        self.core.writes_in_flight = self.core.writes_in_flight.saturating_sub(1);
        if let Some(e) = failed {
            self.write_failed(e);
            return;
        }
        for t in tickets {
            self.core.tickets.effect_written(t);
        }
        self.core.register_durable(cx.flush, page.dev(), wid, Durable::Page(page));
    }

    fn ext_read_done(&mut self, cx: &mut StorageCtx<'_>, req: ReqId, generation: u32, idx: usize, chunk: Option<usize>, d: IoDone) {
        if !self.reqs.live_gen(req, generation) {
            return;
        }
        let crc = self.core.crc();
        let r = self.reqs.get_mut(req).expect("live");
        let Some(fe) = r.fetched.get_mut(idx) else { return };
        let mut next_chunk: Option<(PageId, u32, usize)> = None;
        match chunk {
            None => {
                match d.res {
                    Ok(_) => fe.buf = d.buf,
                    Err(e) => fe.err = Some(e),
                }
                if fe.err.is_none() && fe.want_chunks {
                    let b = fe.buf.as_ref().map_or(&[][..], |b| b.as_slice());
                    let refs = extent::parse_extent_header(b, fe.start)
                        .filter(|h| h.kind == EXT_KIND_CHUNKMAP && extent::verify_extent(b, h, crc))
                        .and_then(|h| extent::chunkmap_parts(b, &h).and_then(|(_, body)| extent::decode_chunkmap(body)));
                    match refs {
                        Some(refs) if !refs.is_empty() => {
                            let cs: SmallVec<[ChunkFetch; 4]> =
                                refs.iter().map(|c| ChunkFetch { page: c.page, n_pages: c.n_pages, crc: c.crc, buf: None }).collect();
                            next_chunk = Some((cs[0].page, cs[0].n_pages, 0));
                            fe.chunks = Some(cs);
                        }
                        // not a chunkmap any more (or corrupt): the exec reports it
                        _ => fe.want_chunks = false,
                    }
                }
            }
            Some(ci) => {
                let cs = fe.chunks.as_mut().expect("chunk read of a chunkmap");
                match d.res {
                    Ok(_) => cs[ci].buf = d.buf,
                    Err(e) => fe.err = Some(e),
                }
                if fe.err.is_none() && ci + 1 < cs.len() {
                    next_chunk = Some((cs[ci + 1].page, cs[ci + 1].n_pages, ci + 1));
                }
            }
        }
        match next_chunk {
            // chunks are read one at a time, in order (§14.5)
            Some((page, n, ci)) => {
                let tag = self.core.tag(IoKind::ExtRead { req, generation, idx, chunk: Some(ci) });
                cx.io.push(IoReq::read(tag, page.dev(), page.byte_offset(), AlignedBuf::zeroed(n as usize * PAGE)));
                self.core.counters.page_reads += u64::from(n);
            }
            None => {
                let r = self.reqs.get_mut(req).expect("live");
                r.io = r.io.saturating_sub(1);
                self.reqs.check_ready(req);
            }
        }
    }

    fn ext_write_done(&mut self, cx: &mut StorageCtx<'_>, start: PageId, d: IoDone) {
        self.core.writes_in_flight = self.core.writes_in_flight.saturating_sub(1);
        let Some(w) = self.core.ext.writes.get_mut(&start) else { return };
        if let Err(e) = d.res {
            // the write will never complete: readers waiting for its buffer get it back (the
            // value stays readable from RAM), the partition goes read-only and its tickets fail
            w.buf = d.buf;
            w.state = ExtState::AwaitFlush;
            w.failed = Some(e);
            let waiters = core::mem::take(&mut w.waiters);
            self.wake(waiters);
            self.write_failed(e);
            return;
        }
        w.buf = d.buf;
        w.state = ExtState::AwaitFlush;
        let tickets = w.tickets.clone();
        let waiters = core::mem::take(&mut w.waiters);
        let wid = w.wid;
        for t in tickets {
            self.core.tickets.effect_written(t);
        }
        self.wake(waiters);
        self.core.register_durable(cx.flush, start.dev(), wid, Durable::Ext(start));
    }

    fn ext_zero_done(&mut self, cx: &mut StorageCtx<'_>, start: PageId, d: IoDone) {
        self.core.writes_in_flight = self.core.writes_in_flight.saturating_sub(1);
        let Some(z) = self.core.ext.zeros.get_mut(&start) else { return };
        if let Err(e) = d.res {
            self.write_failed(e);
            return;
        }
        z.state = ExtState::AwaitFlush;
        let wid = z.wid;
        let mut written: SmallVec<[TicketId; 2]> = z.waiters.iter().copied().collect();
        if let ZeroWhy::Delete { tickets, .. } = &z.why {
            written.extend(tickets.iter().copied());
        }
        for t in written {
            self.core.tickets.effect_written(t);
        }
        self.core.register_durable(cx.flush, start.dev(), wid, Durable::Zero(start));
    }

    fn header_read_done(&mut self, cx: &mut StorageCtx<'_>, start: PageId, why: HeaderWhy, d: IoDone) {
        // a write, zeroing or release of the run while the header was read voids the read: it
        // may have returned the previous occupant's header (an in-place rewrite keeps the
        // location; a freed run is given out again), and the index checks below cannot tell
        let overtaken = !self.core.alloc.end_header_read(start) || self.core.ext.writes.contains_key(&start) || self.core.ext.zeros.contains_key(&start);
        let Some(buf) = d.buf else {
            if why == HeaderWhy::Reclaim {
                self.reclaim_progress();
            }
            return;
        };
        if d.res.is_err() || overtaken {
            if why == HeaderWhy::Reclaim {
                self.reclaim_progress();
            }
            return;
        }
        let Some(h) = extent::parse_extent_header(&buf, start) else {
            if why == HeaderWhy::Reclaim {
                self.reclaim_progress();
            }
            return;
        };
        match why {
            HeaderWhy::Reclaim => {
                // a dead VALUE/CHUNKMAP extent of a flushed db is free space (§16.5)
                let dead = h.kind != EXT_KIND_CHUNK && self.core.dead(h.db, h.seq, h.txn);
                if dead && !self.core.ext.writes.contains_key(&start) && !self.core.ext.zeros.contains_key(&start) {
                    self.core.ext.zeros.insert(start, ExtZero::pending(h.n_pages, ZeroWhy::Dead));
                }
                self.reclaim_progress();
            }
            HeaderWhy::Expire { db, slot, fp32 } => {
                let kl = h.key_len as usize;
                if h.kind == EXT_KIND_CHUNK || h.db != db || EXT_HDR + kl > buf.len() {
                    return;
                }

                let key: Box<[u8]> = buf[EXT_HDR..EXT_HDR + kl].into();
                let kh = KeyHash::of(self.core.cfg.hash_seed, &key);
                if kh.fp32() != fp32 || key_slot(&key) != slot || !self.core.indexed_at(db, slot, kh, start.to36()) {
                    return;
                }
                self.expire_or_rehint(cx, db, &key, kh, slot, start, h.expire_at(), h.seq);
            }
        }
    }

    fn reclaim_progress(&mut self) {
        self.core.counters.flush_reclaim_pending_pages = self.core.counters.flush_reclaim_pending_pages.saturating_sub(1);
    }

    // ---------------------------------------------------------------------------------------
    // Durability

    fn process_durable(&mut self, cx: &mut StorageCtx<'_>) {
        while let Some(d) = self.core.durable_q.pop() {
            match d {
                Durable::Page(p) => self.image_durable(cx, p),
                Durable::Ext(s) => self.ext_write_durable(cx, s),
                Durable::Zero(s) => self.ext_zero_durable(cx, s),
            }
        }
    }

    /// N became durable (§13.4 step 7): it is the page's M now.
    fn image_durable(&mut self, cx: &mut StorageCtx<'_>, page: PageId) {
        let Some(f_old) = cx.cache.peek(page) else { return };
        let Some(mut st) = cx.cache.take_state(f_old) else { return };
        let Some(inf) = st.inflight.take() else {
            cx.cache.put_state(f_old, st);
            return;
        };
        cx.cache.promote(page, inf.n_frame);
        st.m_frame = inf.n_frame;
        st.recs = inf.recs;
        image::settle(&mut st.recs, cx.cache.frame(inf.n_frame));
        st.fresh = false;
        st.r#gen = st.r#gen.wrapping_add(1);
        let mut supersede: SmallVec<[image::KeyAt; 2]> = SmallVec::new();
        for eff in inf.applied.iter() {
            for &t in eff.tickets() {
                self.core.tickets.effect_durable(t);
            }
            match eff {
                Pending::Put(rec) if rec.desc.txn == 0 => supersede.push((rec.desc.db, rec.key().into(), rec.desc.seq)),
                // the retired marker is off the media
                Pending::Retire { db, key, seq, .. } => self.core.stale.absent_gone(*db, key, page, *seq),
                _ => {}
            }
        }
        let empty = st.recs.is_empty() && st.pending.is_empty();
        if empty {
            st.fresh = true;
        }
        let redo = st.dirty || !st.pending.is_empty();
        if self.core.prior_txn_pages.remove(&page)
            && let Some(n) = self.core.prior_txn_zero.as_mut()
        {
            *n = n.saturating_sub(1);
        }
        cx.cache.put_state(inf.n_frame, st);
        if empty {
            self.core.alloc.note_fresh(page);
        } else {
            self.core.note_page_space(cx.cache, page);
        }
        for (db, key) in inf.cleared {
            if self.core.stale.remove_at(db, &key, page) {
                self.stale_gone(cx, db, &key);
            }
        }
        if self.core.older_pages.iter().any(|o| o.0 == page) {
            // older copies zeroed by this image no longer count (E14)
            let list = core::mem::take(&mut self.core.older_pages);
            for (p, db, key) in list {
                if p != page {
                    self.core.older_pages.push((p, db, key));
                    continue;
                }
                let h32 = KeyHash::of(self.core.cfg.hash_seed, &key).h32();
                let left = cx.cache.page_state(page).map_or(0, |st| {
                    let img = cx.cache.frame(st.m_frame);
                    st.recs.iter().filter(|r| r.db == db && r.h32 == h32 && r.status != RecStatus::Droppable && r.key(img) == &*key).count()
                });
                if left > 1 {
                    self.core.older_pages.push((p, db, key));
                } else {
                    self.core.stale.set_older(db, &key, page, false);
                    self.stale_gone(cx, db, &key);
                }
            }
        }
        for (db, key, seq) in supersede {
            self.sources_superseded(cx, db, &key, seq, false);
        }
        if redo {
            if let Some(st) = cx.cache.page_state_mut(page) {
                st.dirty = true;
            }
            self.core.dirty.push(page);
        }
        Core::maybe_unpin(cx.cache, page);
    }

    fn ext_write_durable(&mut self, cx: &mut StorageCtx<'_>, start: PageId) {
        let Some(mut w) = self.core.ext.writes.remove(&start) else { return };
        let waiters = core::mem::take(&mut w.waiters);
        self.wake(waiters);
        for &t in &w.tickets {
            self.core.tickets.effect_durable(t);
        }
        match w.hdr.kind {
            EXT_KIND_CHUNK => match w.parent {
                Some(p) => {
                    if let Some(m) = self.core.ext.writes.get_mut(&p) {
                        m.wait_chunks = m.wait_chunks.saturating_sub(1);
                    }
                }
                // the chunkmap was cancelled while this chunk was in flight
                None => {
                    if !self.core.ext.chunk_rc.contains_key(&start) {
                        self.core.alloc.free_extent(start, w.hdr.n_pages);
                    }
                }
            },
            _ if w.hdr.txn == 0 => {
                self.sources_superseded(cx, w.db, &w.key, w.hdr.seq, true);
            }
            _ => {}
        }
    }

    fn ext_zero_durable(&mut self, cx: &mut StorageCtx<'_>, start: PageId) {
        let Some(z) = self.core.ext.zeros.remove(&start) else { return };
        for &t in &z.waiters {
            self.core.tickets.effect_durable(t);
        }
        self.core.alloc.free_extent(start, z.n_pages);
        self.release_chunks(start);
        if self.core.prior_txn_pages.remove(&start)
            && let Some(n) = self.core.prior_txn_zero.as_mut()
        {
            *n = n.saturating_sub(1);
        }
        match z.why {
            ZeroWhy::Delete { db, key, tickets } => {
                for t in tickets {
                    self.core.tickets.effect_durable(t);
                }
                if self.core.stale.remove_at(db, &key, start) {
                    self.stale_gone(cx, db, &key);
                }
            }
            ZeroWhy::Stale { db, key } => {
                if self.core.stale.remove_at(db, &key, start) {
                    self.stale_gone(cx, db, &key);
                }
            }
            ZeroWhy::Dead => {}
        }
        self.core.alloc.clear_refusal();
    }

    /// A copy of the key with seq `newer` is durable and committed: the tracked sources it
    /// shadows become stale and are zeroed (extents at once, §14.4 step 3; pages by the paced
    /// scrub unless a command waits, or at once when `now`). Returns how many sources changed.
    fn sources_superseded(&mut self, cx: &mut StorageCtx<'_>, db: u8, key: &[u8], newer: Seq, now: bool) -> usize {
        self.resolve_gone(db, key, newer);
        let (entries, changed) = self.core.stale.sources_superseded(db, key, newer);
        for e in entries {
            match e.at {
                StaleRef::Extent { page, n_pages } => {
                    self.core.ext.zeros.entry(page).or_insert_with(|| ExtZero::pending(n_pages, ZeroWhy::Stale { db, key: key.into() }));
                }
                StaleRef::Small { page } => {
                    if now || e.foreground {
                        self.ensure_scrub(cx, page);
                    } else {
                        self.core.scrub_queue.push_back(page);
                    }
                }
            }
        }
        changed
    }

    /// Deletes of `(db, key)` waiting for older copies end: those with no tracked copy older
    /// than themselves left (D1), and those a durable copy with seq `newer` supersedes (D2).
    fn resolve_gone(&mut self, db: u8, key: &[u8], newer: Seq) {
        let mut i = 0;
        while i < self.core.gone_wait.len() {
            let g = &self.core.gone_wait[i];
            if g.0 == db && *g.1 == *key && (g.2 < newer || self.core.stale.count_older(db, key, g.2) == 0) {
                let (_, _, _, ts) = self.core.gone_wait.swap_remove(i);
                for t in ts {
                    self.core.tickets.effect_resolved(t);
                }
            } else {
                i += 1;
            }
        }
    }

    /// A tracked copy of `(db, key)` is durably gone.
    fn stale_gone(&mut self, cx: &mut StorageCtx<'_>, db: u8, key: &[u8]) {
        self.resolve_gone(db, key, Seq::ZERO);
        // the key's fenced copies (a delete's newest ones) may go now that an older one went
        let fenced: SmallVec<[PageId; 1]> =
            self.core.stale.get(db, key).iter().filter_map(|e| match e.at { StaleRef::Small { page } if e.fenced => Some(page), _ => None }).collect();
        for page in fenced {
            if !self.core.stale.fenced_by_older(db, key, page) {
                self.ensure_scrub(cx, page);
            }
        }
        // effects deferred behind older copies are tried again
        let pages: Vec<PageId> = self.core.deferred.drain().collect();
        for p in pages {
            self.core.mark_dirty(cx.cache, p);
        }
    }

    /// Makes sure `page` is rebuilt to zero its tracked copies (loads it when needed).
    fn ensure_scrub(&mut self, cx: &mut StorageCtx<'_>, page: PageId) {
        if let Some(st) = cx.cache.page_state_mut(page) {
            st.scrub = true;
            self.core.mark_dirty(cx.cache, page);
            self.core.counters.foreground_scrub_writes += 1;
            return;
        }
        self.load_page(cx, page, None, true);
    }

    // ---------------------------------------------------------------------------------------
    // Meta replies

    pub fn on_meta_done(&mut self, cx: &mut StorageCtx<'_>, m: MetaDone) {
        self.core.now = cx.now;
        match m {
            MetaDone::Resolved { .. } => {
                // ids at or below the durable `resolved_w` stay valid through it (E10)
                let w = cx.meta.resolved_w().max(self.core.resolved_w);
                self.core.resolved_w = w;
                self.core.committed.retain(|&t| t > w);
                return;
            }
            MetaDone::Committed { txn } if !matches!(self.core.meta_wait.front(), Some(MetaWait::Commit { txn: t }) if *t == txn) => {
                // the commit of a cross-partition part, requested by the coordinating worker
                self.txn_committed(cx, txn);
                return;
            }
            _ => {}
        }
        let Some(w) = self.core.meta_wait.pop_front() else { return };
        let now_ns = cx.now.mono_ns;
        match (w, m) {
            (MetaWait::Ag(kind), MetaDone::Ag(ag)) => {
                self.core.alloc.ag_granted(kind, ag, now_ns);
                self.wake_parked(true);
            }
            (MetaWait::Ag(kind), _) => {
                self.core.alloc.ag_granted(kind, None, now_ns);
                self.wake_parked(true);
            }
            (MetaWait::Seq(floor), MetaDone::Ok) => {
                self.core.seq_limit = self.core.seq_limit.max(floor);
                self.core.seq_asked = false;
                self.wake_parked(false);
            }
            (MetaWait::Seq(_), _) => {
                self.core.seq_asked = false;
                self.write_failed(-libc::EIO);
                self.wake_parked(false);
            }
            (MetaWait::Cutoff { cutoff, dbs, ticket, all }, MetaDone::Ok) => {
                for d in 0..16u8 {
                    if dbs & (1 << d) != 0 {
                        let c = &mut self.core.cutoff_durable[usize::from(d)];
                        *c = (*c).max(cutoff);
                    }
                }
                self.core.tickets.meta_done(ticket);
                if all {
                    self.core.flushall_wait = true;
                }
                // effects deferred behind in-page copies of the flushed dbs go ahead: the page
                // writer counts those copies until the cutoff is durable (a pre-flush copy is
                // still the key's state if the flush is lost) and dead ones after it; nothing
                // else rebuilds a page whose only wait was such a copy
                if !self.core.deferred.is_empty() {
                    let deferred: Vec<PageId> = self.core.deferred.drain().collect();
                    for p in deferred {
                        self.core.mark_dirty(cx.cache, p);
                    }
                }
                // deletes waiting for copies of flushed keys: those copies are dead now (E5)
                let mut i = 0;
                while i < self.core.gone_wait.len() {
                    if dbs & (1 << self.core.gone_wait[i].0) != 0 {
                        let (_, _, _, ts) = self.core.gone_wait.swap_remove(i);
                        for t in ts {
                            self.core.tickets.effect_resolved(t);
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            (MetaWait::Cutoff { ticket, .. }, MetaDone::Failed(e)) => {
                self.core.tickets.fail(ticket, e);
                self.write_failed(e);
            }
            (MetaWait::Cutoff { ticket, .. }, _) => self.core.tickets.meta_done(ticket),
            (MetaWait::Commit { txn }, MetaDone::Committed { .. }) => self.txn_committed(cx, txn),
            (MetaWait::Commit { txn }, MetaDone::Failed(e)) => {
                if let Some(o) = self.core.txns.get(&txn)
                    && let Some(t) = o.ticket
                {
                    self.core.tickets.fail(t, e);
                }
                self.write_failed(e);
            }
            (MetaWait::Commit { txn }, _) => self.txn_committed(cx, txn),
            (MetaWait::Plain, MetaDone::Failed(e)) => self.write_failed(e),
            (MetaWait::Plain, _) => {}
        }
    }

    fn wake_parked(&mut self, space: bool) {
        let ids = if space { core::mem::take(&mut self.core.space_wait) } else { core::mem::take(&mut self.core.seq_wait) };
        for id in ids {
            if let Some(r) = self.reqs.get_mut(id) {
                r.parked = false;
            }
            self.reqs.check_ready(id);
        }
    }

    /// `COMMITTED(txn)` is durable (§14.6 steps 3-4): the reply may go; phase 2 starts.
    fn txn_committed(&mut self, cx: &mut StorageCtx<'_>, txn: u64) {
        self.core.committed.insert(txn);
        self.release_held(cx.cache, txn);
        let Some(o) = self.core.txns.remove(&txn) else { return };
        if o.local
            && let Some(t) = o.ticket
        {
            self.core.tickets.meta_done(t);
        }
        for &t in &o.barriers {
            self.core.tickets.meta_done(t);
        }
        for (db, key) in &o.keys {
            self.sources_superseded(cx, *db, key, o.seq, true);
        }
        for p in &o.pages {
            self.core.mark_dirty(cx.cache, *p);
        }
        for (db, key, page, seq) in o.markers {
            self.core.retire.push(RetireItem { db, key, page, seq });
        }
    }

    fn request_commit(&mut self, meta: &MetaClient, txn: u64) {
        let Some(o) = self.core.txns.get_mut(&txn) else { return };
        if o.requested || !o.local {
            return;
        }
        o.requested = true;
        self.core.meta_req(meta, MetaReq::CommitTxn { txn }, MetaWait::Commit { txn });
    }

    // ---------------------------------------------------------------------------------------
    // The tick: page writer, extent writes, background tasks

    pub fn tick(&mut self, cx: &mut StorageCtx<'_>) {
        self.core.now = cx.now;
        self.core.tickets.now_ns = cx.now.mono_ns;
        if self.core.state == PartState::Loading {
            return;
        }
        self.process_durable(cx);
        for page in core::mem::take(&mut self.core.load_retry) {
            if self.core.loads.contains_key(&page) {
                let buf = self.core.take_buf(PAGE);
                let tag = self.core.tag(IoKind::PageRead { page, retried: true });
                cx.io.push(IoReq::read(tag, page.dev(), page.byte_offset(), buf));
            }
        }
        let retry = core::mem::take(&mut self.core.retry);
        for p in retry {
            self.core.mark_dirty(cx.cache, p);
            if cx.cache.page_state(p).is_some_and(|st| st.inflight.is_none()) && !self.core.dirty.contains(&p) {
                self.core.dirty.push(p);
            }
        }
        if self.core.state == PartState::ReadOnly && !self.reqs.held.is_empty() {
            // a txn that cannot commit any more releases its keys (its ticket failed)
            let held: Vec<u64> = self.reqs.held.keys().copied().collect();
            for t in held {
                self.release_held(cx.cache, t);
            }
        }
        self.txn_progress(cx);
        self.retire_ready(cx);
        self.foreground_scrubs(cx);
        if self.core.state == PartState::Ready {
            // builds may stage relocations onto other pages: run until no page is left
            self.core.relocations = 0;
            while let Some(page) = self.core.dirty.pop() {
                self.build_page(cx, page);
            }
            self.submit_extents(cx);
        } else {
            self.core.dirty.clear();
        }
        self.meta_requests(cx);
        self.background(cx);
        self.finish_barriers();
        self.update_counters(cx);
        if crate::progress::self_check()
            && let Err(e) = self.check_progress(cx.cache)
        {
            panic!("{e}");
        }
    }

    fn txn_progress(&mut self, cx: &mut StorageCtx<'_>) {
        let mut done = core::mem::take(&mut self.core.scratch_txn);
        self.core.tickets.take_txns_durable(&mut done);
        for txn in done.drain(..) {
            self.request_commit(cx.meta, txn);
        }
        self.core.scratch_txn = done;
        let mut failed = Vec::new();
        self.core.tickets.take_txns_failed(&mut failed);
        for (txn, e) in failed {
            if let Some(o) = self.core.txns.get(&txn)
                && let Some(t) = o.ticket
            {
                self.core.tickets.fail(t, e);
            }
        }
        // ids handed out while the range was being extended: parked requests try again
        if !self.core.seq_wait.is_empty() && !self.core.seq_asked && self.core.next_seq < self.core.seq_limit {
            self.wake_parked(false);
        }
    }

    /// Absent newest copies whose older copies are gone are dropped (D1).
    fn retire_ready(&mut self, cx: &mut StorageCtx<'_>) {
        if self.core.retire.is_empty() {
            return;
        }
        let items = core::mem::take(&mut self.core.retire);
        for it in items {
            if self.core.stale.count(it.db, &it.key) > 0 {
                self.core.retire.push(it);
                continue;
            }
            if cx.cache.page_state(it.page).is_some() {
                self.stage_retire(cx.cache, it.page, it.db, it.key, it.seq);
            } else {
                self.load_page(cx, it.page, None, false);
                self.core.retire.push(it);
            }
        }
    }

    /// Zeroings a received command waits for are built in this tick, never paced (§16.2).
    fn foreground_scrubs(&mut self, cx: &mut StorageCtx<'_>) {
        if self.core.fg_keys.is_empty() {
            return;
        }
        let keys = core::mem::take(&mut self.core.fg_keys);
        for (db, key) in keys {
            let entries = self.core.stale.require(db, &key, false);
            for e in entries {
                if e.state != CopyState::Stale {
                    continue;
                }
                match e.at {
                    StaleRef::Small { page } => self.ensure_scrub(cx, page),
                    StaleRef::Extent { page, n_pages } => {
                        self.core.ext.zeros.entry(page).or_insert_with(|| ExtZero::pending(n_pages, ZeroWhy::Stale { db, key: key.clone() }));
                    }
                }
            }
        }
    }

    /// Builds and submits the next image of `page` (§13.4).
    fn build_page(&mut self, cx: &mut StorageCtx<'_>, page: PageId) {
        let Some(f) = cx.cache.peek(page) else { return };
        let Some(mut st) = cx.cache.take_state(f) else { return };
        if st.inflight.is_some() {
            // R5: re-queued when the image in flight is durable
            cx.cache.put_state(f, st);
            return;
        }
        st.dirty = false;
        if st.corrupt {
            st.pending.clear();
            st.scrub = false;
            cx.cache.put_state(f, st);
            Core::maybe_unpin(cx.cache, page);
            return;
        }
        let Some(n) = cx.cache.alloc_scratch() else {
            st.dirty = true;
            cx.cache.put_state(f, st);
            self.core.retry.push(page);
            return;
        };
        let built = {
            let env = Env { core: &self.core, page };
            let (m, nb) = cx.cache.pair(st.m_frame, n);
            image::build(&mut st, m, nb, &env)
        };
        let scrubbed = st.scrub;
        st.scrub = false;
        let image::Built { spans, atomic, recs, applied, relocate, foreground, sources_durable, cleared, zeroed, placed, user_bytes } = built;
        self.core.fg_keys.extend(foreground);
        let mut gone: SmallVec<[(u8, Box<[u8]>); 1]> = SmallVec::new();
        if !spans.is_empty() {
            let wid = self.core.new_wid();
            let dev = page.dev();
            let dsync = self.core.dsync(dev);
            for &(off, len) in &spans {
                let mut buf = self.core.take_buf(PAGE);
                buf.as_mut_slice().copy_from_slice(cx.cache.frame(n));
                let tag = self.core.tag(IoKind::PageWrite { page });
                cx.io.push(IoReq::write_span(tag, dev, page.byte_offset() + u64::from(off), buf, u32::from(off), u32::from(len), dsync, atomic));
                self.core.counters.host_bytes_written += u64::from(len);
                if usize::from(len) == PAGE {
                    self.core.counters.full_page_writes += 1;
                } else {
                    self.core.counters.sector_writes += 1;
                }
            }
            let c = &mut self.core.counters;
            c.user_bytes_written += u64::from(user_bytes);
            if scrubbed && placed == 0 {
                c.page_writes_scrub += 1;
            } else if zeroed > 0 && placed == 0 {
                c.page_writes_delete += 1;
            } else if st.fresh || zeroed == 0 {
                c.page_writes_insert += 1;
            } else {
                c.page_writes_update += 1;
            }
            for &t in applied.iter().flat_map(|e| e.tickets().iter()) {
                self.core.tickets.note_page(t, page, st.r#gen.wrapping_add(1));
            }
            st.inflight = Some(Inflight { n_frame: n, recs, sub_ios: spans.len() as u8, state: InflightState::Writing, applied, cleared, wid, failed: None });
            self.core.writes_in_flight += 1;
            if !st.pending.is_empty() {
                // the rest goes into the next image once this one is durable (R5)
                st.dirty = true;
            }
        } else {
            cx.cache.free_scratch(n);
            st.recs = recs;
            for &t in applied.iter().flat_map(|e| e.tickets().iter()) {
                self.core.tickets.effect_resolved(t);
            }
            for e in applied.iter() {
                if let Pending::Retire { db, key, seq, .. } = e {
                    self.core.stale.absent_gone(*db, key, page, *seq);
                }
            }
            for (db, key) in cleared {
                if self.core.stale.remove_at(db, &key, page) {
                    // the copy is gone without a write: whoever waits for it is told below,
                    // once this page's state is back in the cache
                    gone.push((db, key));
                }
            }
            if !st.pending.is_empty() {
                self.core.deferred.insert(page);
            }
            if scrubbed
                && self.core.prior_txn_pages.remove(&page)
                && let Some(n) = self.core.prior_txn_zero.as_mut()
            {
                // nothing left to zero on this page
                *n = n.saturating_sub(1);
            }
        }
        cx.cache.put_state(f, st);
        for (db, key) in gone {
            self.stale_gone(cx, db, &key);
        }
        for (db, key, seq) in sources_durable {
            if self.sources_superseded(cx, db, &key, seq, true) > 0 {
                // a source the event that made this copy durable should have released
                self.core.counters.missed_handoffs += 1;
            }
        }
        for rec in relocate {
            self.relocate(cx, page, rec);
        }
        self.core.note_page_space(cx.cache, page);
        Core::maybe_unpin(cx.cache, page);
    }

    /// A staged record that does not fit its page moves to another one (§14.3). At most
    /// `RELOCATIONS_PER_TICK` moves happen per tick; the rest wait for the next one.
    fn relocate(&mut self, cx: &mut StorageCtx<'_>, from: PageId, rec: EncodedRecord) {
        if self.core.cutoff.get(usize::from(rec.desc.db)).is_some_and(|&c| rec.desc.seq.0 < c) {
            // staged before a FLUSHDB of its db: dead by the cutoff, nothing to move (moved,
            // it was put back into the index the flush had dropped, and the flushed key was
            // visible again)
            for t in rec.tickets {
                self.core.tickets.effect_resolved(t);
            }
            return;
        }
        self.core.relocations += 1;
        if self.core.relocations > RELOCATIONS_PER_TICK {
            if let Some(st) = cx.cache.page_state_mut(from) {
                st.pending.push(Pending::Put(rec));
            }
            self.core.retry.push(from);
            return;
        }
        let key: Box<[u8]> = rec.key().into();
        let db = rec.desc.db;
        let kh = KeyHash::of(self.core.cfg.hash_seed, &key);
        let slot = key_slot(&key);
        let need = rec.desc.len;
        let now_ns = cx.now.mono_ns;
        let place = {
            let stale = &self.core.stale;
            let cache: &PageCache = cx.cache;
            let absent = stale.absent_of(db, &key).map(|a| a.page);
            let exclude = |q: PageId| q == from || stale.state_at(db, &key, q).is_some() || absent == Some(q) || cache.page_state(q).is_some_and(|st| st.corrupt);
            self.core.alloc.place_small(need, cache, now_ns, &exclude)
        };
        let target = match place {
            SmallPlace::Cached(q) => Some(q),
            SmallPlace::Fresh(q) => {
                self.init_fresh(cx.cache, q);
                if cx.cache.page_state(q).is_some() {
                    Some(q)
                } else {
                    self.core.alloc.unplace_fresh(q);
                    None
                }
            }
            SmallPlace::Uncached(q) => {
                self.load_page(cx, q, None, false);
                None
            }
            SmallPlace::NeedAg | SmallPlace::Full => {
                // the write was accepted: its space comes from above the fill limit
                let _ = self.core.alloc.want_reserved(AgKind::Small);
                None
            }
        };
        let Some(q) = target else {
            // no room yet (an AG or a page read is on its way): try again next tick
            if let Some(st) = cx.cache.page_state_mut(from) {
                st.pending.push(Pending::Put(rec));
            }
            self.core.retry.push(from);
            return;
        };
        let marker = rec.desc.kind == REC_KIND_TXN_DELETE_MARKER;
        let txn = rec.desc.txn;
        if let Some(seq) = self.protect_in_page(cx.cache, from, db, kh.h32(), &key) {
            self.core.stale.add(db, &key, StaleRef::Small { page: from }, CopyState::Source, seq);
        }
        let vtype = if rec.desc.kind == REC_KIND_HASH { VType::Hash } else { VType::String };
        let expire = rec.desc.expire_at();
        let seq = rec.desc.seq;
        for &t in &rec.tickets {
            self.core.tickets.note_page(t, q, 0);
        }
        self.stage_put(cx.cache, q, rec);
        if !marker {
            // the entry names `from` (a staged version leaves its page only through this move)
            if self.core.find_entry(db, slot, kh, from.to36()).is_some() {
                let e = Entry { fp32: kh.fp32(), x8: kh.x8(), page36: q.to36(), kind: Kind::Small, vtype, sync_mark: false };
                self.core.index_set_at(db, slot, kh, Some(from.to36()), e, expire);
            }
        } else {
            self.core.stale.note_absent(db, &key, q, seq, txn);
        }
        if txn != 0
            && let Some(o) = self.core.txns.get_mut(&txn)
        {
            if !o.pages.contains(&q) {
                o.pages.push(q);
            }
            for m in o.markers.iter_mut() {
                if m.0 == db && *m.1 == *key && m.3 == seq {
                    m.2 = q;
                }
            }
        }
        self.core.counters.moves += 1;
        self.core.counters.page_writes_move += 1;
        Core::maybe_unpin(cx.cache, from);
    }

    /// Submits staged extent writes and zeroings whose ordering edges hold.
    fn submit_extents(&mut self, cx: &mut StorageCtx<'_>) {
        if !self.core.ext.writes.is_empty() {
            let starts: SmallVec<[PageId; 8]> =
                self.core.ext.writes.iter().filter(|(_, w)| w.state == ExtState::Pending && w.wait_chunks == 0).map(|(s, _)| *s).collect();
            for start in starts {
                let (db, kind) = {
                    let w = &self.core.ext.writes[&start];
                    (w.db, w.hdr.kind)
                };
                if kind != EXT_KIND_CHUNK {
                    // E14: a further copy waits until the key's older tracked copy is zeroed
                    let key = self.core.ext.writes[&start].key.clone();
                    let older = self.core.stale.get(db, &key).iter().any(|e| e.at.page() != start && (e.state == CopyState::Stale || e.older));
                    if older {
                        self.core.fg_keys.push((db, key));
                        continue;
                    }
                }
                let wid = self.core.new_wid();
                let dsync = self.core.dsync(start.dev());
                let tag = self.core.tag(IoKind::ExtWrite { start });
                let w = self.core.ext.writes.get_mut(&start).expect("present");
                let Some(buf) = w.buf.take() else { continue };
                let len = buf.len() as u32;
                let atomic = w.atomic;
                w.state = ExtState::Writing;
                w.wid = wid;
                cx.io.push(IoReq::write_span(tag, start.dev(), start.byte_offset(), buf, 0, len, dsync, atomic));
                self.core.writes_in_flight += 1;
                self.core.counters.extent_writes += 1;
                self.core.counters.host_bytes_written += u64::from(len);
            }
        }
        if !self.core.ext.zeros.is_empty() {
            let starts: SmallVec<[PageId; 8]> = self.core.ext.zeros.iter().filter(|(_, z)| z.state == ExtState::Pending).map(|(s, _)| *s).collect();
            for start in starts {
                // never while a write of the same run is staged or in flight (E4)
                if self.core.ext.writes.contains_key(&start) {
                    continue;
                }
                if let ZeroWhy::Delete { db, key, .. } | ZeroWhy::Stale { db, key } = &self.core.ext.zeros[&start].why
                    && self.core.stale.older_than_source_at(*db, key, start)
                {
                    // D1: the newest copy goes after the older ones
                    let k = (*db, key.clone());
                    self.core.fg_keys.push(k);
                    continue;
                }
                let lba = self.core.dev_lba(start.dev());
                let wid = self.core.new_wid();
                let dsync = self.core.dsync(start.dev());
                let tag = self.core.tag(IoKind::ExtZero { start });
                let z = self.core.ext.zeros.get_mut(&start).expect("present");
                z.state = ExtState::Writing;
                z.wid = wid;
                cx.io.push(IoReq::write_span(tag, start.dev(), start.byte_offset(), AlignedBuf::zeroed(lba as usize), 0, lba, dsync, false));
                self.core.writes_in_flight += 1;
                self.core.counters.host_bytes_written += u64::from(lba);
            }
        }
    }

    fn meta_requests(&mut self, cx: &mut StorageCtx<'_>) {
        let mut kinds: SmallVec<[crate::alloc::AgRequest; 2]> = SmallVec::new();
        self.core.alloc.take_ag_requests(&mut kinds);
        for (kind, dev, class) in kinds {
            let partition = self.core.cfg.id;
            let r = match (dev, class) {
                (Some(dev), _) => MetaReq::AcquireAgOn { partition, kind, dev },
                (None, crate::alloc::AgClass::Spare) => MetaReq::AcquireSpare { partition, kind },
                (None, crate::alloc::AgClass::Need) => MetaReq::AcquireAg { partition, kind },
                (None, crate::alloc::AgClass::Reserved) => MetaReq::AcquireReserved { partition, kind },
            };
            self.core.meta_req(cx.meta, r, MetaWait::Ag(kind));
        }
        // keep a reservation ahead of the counter (E12)
        if !self.core.seq_asked
            && self.core.seq_limit > Seq::ZERO
            && self.core.seq_limit.0.saturating_sub(self.core.next_seq.0) < SEQ_RESERVE / 2
        {
            self.ask_seq(cx.meta);
        }
        // FLUSHALL: release the old AGs when nothing is staged, in flight or waiting for a
        // flush in them any more (forgetting a page whose image awaits its flush would lose
        // the image's tickets: its durability event finds no page state)
        if self.core.flushall_wait
            && self.core.writes_in_flight == 0
            && self.core.dirty.is_empty()
            && self.core.retry.is_empty()
            && self.core.deferred.is_empty()
            && self.core.durable_wait.is_empty()
            && self.core.durable_q.is_empty()
            && self.core.ext.writes.is_empty()
            && self.core.ext.zeros.is_empty()
            && self.core.loads.is_empty()
        {
            self.core.flushall_wait = false;
            let ags = core::mem::take(&mut self.core.retiring_ags);
            for ag in ags {
                self.forget_ag(cx.cache, ag);
                self.core.meta_req(cx.meta, MetaReq::ReleaseAg { ag }, MetaWait::Plain);
            }
        }
        // pre-boot uncommitted txn records are zeroed: `resolved_w` may pass them (E10)
        if self.core.prior_txn_zero == Some(0) {
            self.core.prior_txn_zero = None;
            let partition = self.core.cfg.id;
            self.core.meta_req(cx.meta, MetaReq::PriorTxnsZeroed { partition }, MetaWait::Plain);
        }
    }

    /// Drops the cached pages of a released AG.
    fn forget_ag(&mut self, cache: &mut PageCache, ag: AgId) {
        let Some(first) = self.core.alloc.ag_first(ag) else { return };
        for i in 0..crate::layout::AG_PAGES {
            let p = self.core.alloc.page_of_first(ag.dev, first, i);
            if cache.contains(p) {
                cache.forget(p);
            }
        }
    }

    fn background(&mut self, cx: &mut StorageCtx<'_>) {
        let now_ns = cx.now.mono_ns;
        // paced scrubs of copies no command waits for (§19)
        while !self.core.scrub_queue.is_empty() && self.core.buckets.scrub.try_take(now_ns, 1) {
            let Some(page) = self.core.scrub_queue.pop_front() else { break };
            if self.core.stale.at_page(page) > 0 {
                self.ensure_scrub(cx, page);
            }
        }
        if self.core.state != PartState::Ready {
            return;
        }
        // AG pressure (§11.1): a write of some partition was refused an AG; an idle spare goes
        // back to the pool (never one a request of this partition waits for)
        if cx.meta.ag_pressure()
            && self.core.space_wait.is_empty()
            && self.reqs.ready.is_empty()
            && let Some(ag) = self.core.alloc.take_idle_spare()
        {
            self.forget_ag(cx.cache, ag);
            self.core.meta_req(cx.meta, MetaReq::ReleaseAg { ag }, MetaWait::Plain);
        }
        // AG release (§11.7)
        if self.core.buckets.release.try_take(now_ns, 1) {
            let busy = |p: PageId| -> bool {
                cx.cache.page_state(p).is_some_and(|st| !st.is_clean()) || self.core.stale.at_page(p) > 0 || self.core.loads.contains_key(&p)
            };
            if let Some(ag) = self.core.alloc.take_releasable(&busy) {
                self.forget_ag(cx.cache, ag);
                self.core.meta_req(cx.meta, MetaReq::ReleaseAg { ag }, MetaWait::Plain);
            }
        }
        // FLUSHDB reclaim walk (§16.5)
        while (!self.core.reclaim_pages.is_empty() || !self.core.reclaim_ext.is_empty()) && self.core.buckets.reclaim.try_take(now_ns, 1) {
            if let Some(p) = self.core.reclaim_pages.pop_front() {
                if cx.cache.page_state(p).is_none() && !self.core.loads.contains_key(&p) {
                    self.load_page(cx, p, None, false);
                }
                self.reclaim_progress();
            } else if let Some(s) = self.core.reclaim_ext.pop_front() {
                if !self.core.alloc.begin_header_read(s) {
                    // written or freed since the walk started: not a dead extent to reclaim
                    self.reclaim_progress();
                    continue;
                }
                let tag = self.core.tag(IoKind::HeaderRead { start: s, why: HeaderWhy::Reclaim });
                cx.io.push(IoReq::read(tag, s.dev(), s.byte_offset(), AlignedBuf::zeroed(PAGE)));
            }
        }
        // hole warming (§11.3): keep a few cached pages with room
        if self.core.alloc.cached_pages() < 8 && cx.cache.used() + 64 < cx.cache.capacity() && self.core.buckets.warm.try_take(now_ns, 1)
            && let Some(p) = self.core.alloc.warm_candidate(cx.cache)
            && !self.core.loads.contains_key(&p)
        {
            self.load_page(cx, p, None, false);
        }
        self.expiry_sweep(cx);
    }

    /// Active expiry (§16.4): RAM sweep of hint planes, then verification of candidates by
    /// reading their record (paced); replicas never expire keys on their own.
    fn expiry_sweep(&mut self, cx: &mut StorageCtx<'_>) {
        if self.core.cfg.cluster && !self.core.cfg.produce_groups {
            return;
        }
        let now_ms = cx.now.unix_ms;
        let now_s = now_ms / 1000;
        if now_s >= self.core.expiry.hint_base_s + REBASE_PERIOD_S + 60 {
            self.rebase_hints(now_s.saturating_sub(60));
        }
        let dbs = self.core.cfg.databases;
        let local = self.core.index.local_slots();
        // paced by time (one pass per SWEEP_PASS_NS), never by the tick rate
        let (ttl_keys, ttl_dbs) = (0..dbs).fold((0u64, 0u64), |(k, d), db| {
            let n = self.core.index.expires(db);
            (k + n, d + u64::from(n > 0))
        });
        if ttl_keys == 0 {
            self.core.expiry.last_ns = cx.now.mono_ns;
            self.core.expiry.credit = 0;
            return;
        }
        let pass_cost = ttl_keys + ttl_dbs * u64::from(local);
        let mut budget = self.core.expiry.earn(cx.now.mono_ns, pass_cost, EXPIRY_BUDGET as u64) as usize;
        if budget == 0 {
            return;
        }
        let earned = budget;
        let mut cands = core::mem::take(&mut self.core.scratch_pos);
        let mut found: SmallVec<[(u8, u16, Entry); 8]> = SmallVec::new();
        // tables visited per tick; dbs without TTL keys are skipped whole
        let mut tables = 0u32;
        while budget > 0 && tables < SWEEP_TABLES_PER_TICK {
            let sw = &mut self.core.expiry;
            if sw.db >= dbs {
                sw.db = 0;
                sw.slot_local = 0;
            }
            let db = sw.db;
            if self.core.index.expires(db) == 0 {
                let sw = &mut self.core.expiry;
                sw.db += 1;
                sw.slot_local = 0;
                sw.pos = Pos::START;
                tables += 1;
                if (0..dbs).all(|d| self.core.index.expires(d) == 0) {
                    break;
                }
                continue;
            }
            let slot = self.core.index.slot_of(sw.slot_local);
            let now_lb = kv_index::hint::now_code(now_ms, sw.hint_base_s);
            let resume = match self.core.index.table(db, slot) {
                Some(t) if t.ttl_len() > 0 => {
                    cands.clear();
                    let step = budget.min(4096);
                    let from_start = sw.pos == Pos::START;
                    let resume = t.expiry_candidates(sw.pos, step, now_lb, &mut cands);
                    // a whole small table costs about its TTL entries, a slice its budget
                    let cost = if from_start && resume.is_end() { step.min(t.ttl_len() + 1) } else { step };
                    budget = budget.saturating_sub(cost);
                    for &p in &cands {
                        if let Some((e, _)) = t.get(p) {
                            found.push((db, slot, e));
                        }
                    }
                    resume
                }
                _ => {
                    budget = budget.saturating_sub(1);
                    Pos::END
                }
            };
            let sw = &mut self.core.expiry;
            if resume.is_end() {
                sw.pos = Pos::START;
                sw.slot_local += 1;
                if sw.slot_local >= local {
                    sw.slot_local = 0;
                    sw.db += 1;
                }
                tables += 1;
            } else {
                sw.pos = resume;
            }
            if found.len() >= 8 {
                break;
            }
        }
        self.core.scratch_pos = cands;
        self.core.expiry.credit = budget as u64;
        self.core.counters.expire_sweep_work += (earned - budget) as u64;
        let now_ns = cx.now.mono_ns;
        for (db, slot, e) in found {
            let page = PageId::from36(e.page36);
            match e.kind {
                Kind::Small => {
                    if cx.cache.page_state(page).is_some() {
                        self.expire_check_small(cx, page, db, slot, e.fp32, e.x8);
                    } else if self.core.buckets.expire.try_take(now_ns, 1) {
                        self.load_page(cx, page, None, false);
                        if let Some(l) = self.core.loads.get_mut(&page)
                            && l.expire.len() < 16
                        {
                            l.expire.push((db, slot, e.fp32, e.x8));
                        }
                    }
                }
                _ => {
                    if let Some(w) = self.core.ext.writes.get(&page) {
                        let (key, exp, seq) = (w.key.clone(), w.hdr.expire_at(), w.hdr.seq);
                        let kh = KeyHash::of(self.core.cfg.hash_seed, &key);
                        if kh.fp32() == e.fp32 {
                            self.expire_or_rehint(cx, db, &key, kh, slot, page, exp, seq);
                        }
                    } else if self.core.buckets.expire.try_take(now_ns, 1) {
                        self.core.alloc.begin_header_read(page);
                        let tag = self.core.tag(IoKind::HeaderRead { start: page, why: HeaderWhy::Expire { db, slot, fp32: e.fp32 } });
                        cx.io.push(IoReq::read(tag, page.dev(), page.byte_offset(), AlignedBuf::zeroed(PAGE)));
                    }
                }
            }
        }
    }

    /// Re-encodes every hint against `new_base` (the hourly rebase, §9.5).
    fn rebase_hints(&mut self, new_base: u64) {
        let old = self.core.expiry.hint_base_s;
        if new_base <= old {
            return;
        }
        let local = self.core.index.local_slots();
        for db in 0..self.core.cfg.databases {
            for sl in 0..local {
                let slot = self.core.index.slot_of(sl);
                if let Some(t) = self.core.index.existing_mut(db, slot) {
                    t.rehint(&mut |h| kv_index::hint::rebase(h, old, new_base));
                }
            }
        }
        self.core.expiry.hint_base_s = new_base;
    }

    /// Verifies active-expiry candidates of a cached page: `(fp32, x8)` of `(db, slot)`.
    fn expire_check_small(&mut self, cx: &mut StorageCtx<'_>, page: PageId, db: u8, slot: u16, fp32: u32, x8: u8) {
        let seed = self.core.cfg.hash_seed;
        let found: Option<(Box<[u8]>, Option<u64>, Seq)> = {
            let Some(st) = cx.cache.page_state(page) else { return };
            let staged = st.pending.iter().rev().find_map(|p| match p {
                Pending::Put(r) if r.desc.db == db => {
                    let h = KeyHash::of(seed, r.key());
                    (h.fp32() == fp32 && h.x8() == x8).then(|| (Box::<[u8]>::from(r.key()), r.desc.expire_at(), r.desc.seq))
                }
                _ => None,
            });
            staged.or_else(|| {
                let (recs, fr) = st.image();
                let img = cx.cache.frame(fr);
                recs.iter().filter(|r| r.status == RecStatus::Live && r.db == db && !r.is_marker()).find_map(|r| {
                    let k = r.key(img);
                    let h = KeyHash::of(seed, k);
                    (h.fp32() == fp32 && h.x8() == x8).then(|| (Box::<[u8]>::from(k), r.expire_at(), r.seq))
                })
            })
        };
        let Some((key, exp, seq)) = found else { return };
        let kh = KeyHash::of(seed, &key);
        if key_slot(&key) != slot || !self.core.indexed_at(db, slot, kh, page.to36()) {
            return;
        }
        self.expire_or_rehint(cx, db, &key, kh, slot, page, exp, seq);
    }

    /// Expires a verified key without a write, or re-encodes its hint (§16.4).
    #[allow(clippy::too_many_arguments)]
    fn expire_or_rehint(&mut self, cx: &mut StorageCtx<'_>, db: u8, key: &[u8], kh: KeyHash, slot: u16, at: PageId, exp: Option<u64>, seq: Seq) {
        let now_ms = cx.now.unix_ms;
        match exp {
            Some(t) if t <= now_ms => {
                // the same staged removal as a lazy expiry found by a command
                let kind = self.core.index.table(db, slot).and_then(|t| {
                    let mut c = SmallVec::new();
                    t.candidates(kh, &mut c);
                    c.iter().find(|c| c.entry.page36 == at.to36()).map(|c| c.entry.kind)
                });
                let cur = match kind {
                    Some(Kind::Small) => Cur::Small { page: at, seq },
                    Some(k) => {
                        let n_pages = self.core.ext.writes.get(&at).map_or_else(
                            || self.core.index.table(db, slot).map_or(1, |t| {
                                let mut c = SmallVec::new();
                                t.candidates(kh, &mut c);
                                c.iter().find(|c| c.entry.page36 == at.to36()).map_or(1, |c| class_pages(c.entry.x8))
                            }),
                            |w| w.hdr.n_pages,
                        );
                        Cur::Extent { start: at, n_pages, kind: k, twin: None, seq }
                    }
                    None => return,
                };
                let s = Staged { db, key: key.into(), kh, slot, cur, change: Change::Expired, op: None, seq: None };
                self.apply_expired(cx, &s);
            }
            _ => {
                let hint = self.core.hint(exp);
                if let Some(pos) = self.core.find_entry(db, slot, kh, at.to36())
                    && let Some(t) = self.core.index.existing_mut(db, slot)
                    && let Some((e, _)) = t.get(pos)
                {
                    t.update(pos, e, hint);
                }
            }
        }
    }

    /// Tickets watching tracked copies (barriers) whose copy is gone.
    fn finish_barriers(&mut self) {
        let mut done = Vec::new();
        self.core.stale.take_released(&mut done);
        for t in done {
            self.core.tickets.effect_resolved(t);
        }
    }

    fn update_counters(&mut self, cx: &StorageCtx<'_>) {
        let dbs = usize::from(self.core.cfg.databases.min(16));
        for d in 0..dbs {
            self.core.counters.keys[d] = self.core.index.keys(d as u8);
            self.core.counters.expires[d] = self.core.index.expires(d as u8);
        }
        self.core.counters.stale_copies = self.core.stale.len() as u64;
        self.core.counters.dirty_bytes = self.staged_bytes(cx.cache) as u64;
        // ages of live requests and tickets: refreshed at most every 10 ms
        if cx.now.mono_ns.saturating_sub(self.core.stall_checked_ns) >= 10_000_000 {
            self.core.stall_checked_ns = cx.now.mono_ns;
            self.update_stall_counters();
            self.trace_stalls_deep(cx.cache);
        }
        // memory figures walk every table: refreshed every 1024 ticks
        self.core.ticks = self.core.ticks.wrapping_add(1);
        if self.core.ticks % 1024 == 1 {
            self.core.counters.index_bytes = self.core.index.memory_bytes() as u64;
            self.core.counters.alloc_state_bytes = self.core.alloc.state_bytes() as u64;
        }
    }

    /// Bytes of staged records not in any image yet (`max-dirty-bytes`, §6.7).
    pub fn staged_bytes(&self, cache: &PageCache) -> usize {
        self.core.dirty.iter().filter_map(|p| cache.page_state(*p)).map(PageState::staged_bytes).sum::<usize>()
            + self.core.ext.writes.values().filter(|w| w.state == ExtState::Pending).map(|w| w.buf.as_ref().map_or(0, AlignedBuf::len)).sum::<usize>()
    }

    // ---------------------------------------------------------------------------------------
    // Recovery and snapshot glue

    /// Drives recovery to completion (§18); call again after I/O completions while `Running`.
    pub fn recover(&mut self, cx: &mut StorageCtx<'_>, mode: RecoveryMode) -> RecoveryProgress {
        crate::recovery::drive(self, cx, mode)
    }

    /// Hands this partition's clean-shutdown section to `w` (§10.10); `false` while the
    /// partition still has work in flight (keep ticking), `true` once handed over.
    pub fn snapshot_section(&mut self, cx: &mut StorageCtx<'_>, w: &mut SnapshotWriter) -> bool {
        let _ = &cx;
        // a partition that failed a write (read-only) holds RAM state its media may not have:
        // it hands in no section, so the snapshot stays incomplete and the next start takes
        // the full scan (a snapshot of it described flushed or written keys whose writes or
        // cutoffs never became durable). Done as far as the caller's drain is concerned.
        if self.core.failed.is_some() || self.core.state != PartState::Ready {
            return true;
        }
        if !self.is_quiescent() || !self.core.stale.is_empty() {
            return false;
        }
        crate::snapshot::deposit(self, w);
        true
    }

    /// The partition finished recovery: Ready, with a seq reservation requested (E12).
    pub(crate) fn become_ready(&mut self, cx: &mut StorageCtx<'_>) {
        self.core.state = PartState::Ready;
        self.core.alloc.finish_rebuild();
        self.core.alloc.want_spare(AgKind::Small);
        self.ask_seq(cx.meta);
        self.update_counters(cx);
    }
}

impl Drop for PartitionStore {
    /// A partition dropped while drained hands its section to the engine's snapshot writer,
    /// so that a clean shutdown can write the snapshot (§10.10); one that is not drained hands
    /// in nothing and the next start takes the full scan.
    fn drop(&mut self) {
        if self.core.state == PartState::Ready && !self.core.snapshot_done && self.is_quiescent() && self.core.stale.is_empty() {
            let mut w = self.core.boot.snapshot_writer();
            crate::snapshot::deposit(self, &mut w);
        }
    }
}
