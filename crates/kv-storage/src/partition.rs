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
//!
//! Worker contract (kv-node, the local driver of `engine.rs`, the simulator):
//! * route every [`IoDone`] whose tag names this partition to [`PartitionStore::on_io_done`];
//!   completions of flush requests (tag partition [`crate::flush::FLUSH_TAG_PARTITION`]) go to
//!   the device's [`FlushTracker`], whose durable [`WriteId`]s go to
//!   [`PartitionStore::on_durable`] of [`WriteId::partition`];
//! * meta replies go to [`PartitionStore::on_meta_done`] in the order the meta thread sends them
//!   (it answers one partition's requests in request order; `Resolved` carries txn credits for
//!   the worker and may be passed on or not);
//! * call [`PartitionStore::tick`] once per loop iteration after executing ready requests, then
//!   [`FlushTracker::poll`] and [`PartitionStore::take_tickets`];
//! * a closure whose `exec` returned [`ExecStatus::Requeued`] is run again from the start once
//!   the request is ready again: discard everything it produced (reply bytes).

use std::collections::VecDeque;
use std::sync::Arc;

use hashbrown::{HashMap, HashSet};
use kv_common::pace::TokenBucket;
use kv_common::{Access, KeyHash, Now, RecordCrc, key_slot};
use kv_index::{Entry, Kind, PartitionIndex, Pos, VType};
use smallvec::SmallVec;

use crate::agtable::AgKind;
use crate::alloc::{Alloc, ExtPlace, KeyedPick, PAGE_ROOM, SmallPlace};
use crate::cache::PageCache;
use crate::effect::{Group, Op};
use crate::engine::Boot;
use crate::extent::{
    self, CHUNK_MAX_PAYLOAD, ChunkRef, EXT_FLAG_HAS_EXPIRE, EXT_FLAG_TXN, EXT_HDR, EXT_KIND_CHUNK, EXT_KIND_CHUNKMAP,
    EXT_KIND_VALUE, EXT_VTYPE_HASH, EXT_VTYPE_STRING, ExtHeader,
};
use crate::flush::{FlushTracker, WriteId};
use crate::image::{self, BuildEnv, EncodedRecord, Granularity, Inflight, InflightState, PageState, Pending, RecDesc, RecStatus};
use crate::io::{AlignedBuf, IoDone, IoReq, IoTag, PageUpdateMode};
use crate::keyspace::{Change, ChunkFetch, Cur, ExtFetch, Keyspace, Need, Staged, StoreError};
use crate::layout::{AgId, PAGE, PageId, SMALL_REC_MAX, Seq, class_for, class_pages, pages_for};
use crate::metastore::{MetaClient, MetaDone, MetaReq};
use crate::page::{PageParse, SmallView, parse_small_page};
use crate::pmeta::PartitionMeta;
use crate::record::{
    REC_FLAG_HAS_EXPIRE, REC_FLAG_TXN, REC_KIND_HASH, REC_KIND_STRING, REC_KIND_TXN_DELETE_MARKER, RecHeader, encode_small,
    small_len,
};
use crate::recovery::{RecoveryMode, RecoveryProgress};
use crate::snapshot::SnapshotWriter;
use crate::stale::{CopyState, StaleCopies, StaleRef};
use crate::ticket::{TICKET_HAS_DELETE, TicketId, TicketState, Tickets};

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

/// Seqs reserved per `seq_floor` write (§10.5).
pub(crate) const SEQ_RESERVE: u64 = 1 << 24;
/// Positions of the RAM expiry sweep per tick (§16.4).
const EXPIRY_BUDGET: usize = 64 * 1024;
/// Empty tables a SCAN step walks past at most (§16.6).
const SCAN_EMPTY_TABLES: u16 = 4096;

// -------------------------------------------------------------------------------------------
// Requests

struct KeyInfo {
    kh: KeyHash,
    slot: u16,
}

struct Req {
    req: Request,
    keys: SmallVec<[KeyInfo; 2]>,
    order: u64,
    gen: u32,
    /// Loads and reads outstanding for this request.
    io: u32,
    /// Key queues where it is not at the head.
    waits: u8,
    barrier: bool,
    blocked: bool,
    queued: bool,
    /// Parked until an AG or a seq reservation arrives.
    parked: bool,
    pins: SmallVec<[PageId; 2]>,
    fetched: SmallVec<[ExtFetch; 1]>,
    failed: Option<StoreError>,
}

#[derive(Default)]
struct Requests {
    slots: Vec<Option<Req>>,
    gens: Vec<u32>,
    free: Vec<u32>,
    busy: HashMap<(u8, u64), VecDeque<ReqId>, foldhash::fast::FixedState>,
    ready: VecDeque<ReqId>,
    order: VecDeque<(u64, ReqId)>,
    barriers: VecDeque<(u64, ReqId)>,
    next_order: u64,
    live: usize,
}

impl Requests {
    fn get(&self, id: ReqId) -> Option<&Req> {
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
        r.gen = self.gens[i as usize];
        self.slots[i as usize] = Some(r);
        self.live += 1;
        ReqId(i)
    }

    fn live_gen(&self, id: ReqId, gen: u32) -> bool {
        self.get(id).is_some_and(|r| r.gen == gen)
    }

    fn first_barrier_before(&self, order: u64) -> bool {
        self.barriers.front().is_some_and(|&(o, _)| o < order)
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
        let blocked_by = |rs: &Requests, r: &Req| !r.barrier && rs.first_barrier_before(r.order);
        let Some(r) = self.get(id) else { return };
        let bl = blocked_by(self, r);
        let r = self.get_mut(id).expect("live request");
        r.blocked = bl;
        let barrier_ok = !r.barrier || oldest == Some(r.order);
        if !r.queued && !r.parked && r.io == 0 && r.waits == 0 && !r.blocked && barrier_ok {
            r.queued = true;
            self.ready.push_back(id);
        }
    }
}

// -------------------------------------------------------------------------------------------
// I/O bookkeeping

#[derive(Clone, Copy)]
enum IoKind {
    PageRead { page: PageId, retried: bool },
    PageWrite { page: PageId },
    ExtRead { req: ReqId, gen: u32, idx: usize, chunk: Option<usize> },
    ExtWrite { start: PageId },
    ExtZero { start: PageId },
    Reclaim { start: PageId },
    Recovery { id: u64 },
    SnapRead { id: u64 },
    SnapWrite,
}

#[derive(Clone, Copy)]
enum Durable {
    Page(PageId),
    Ext(PageId),
    Zero(PageId),
}

/// Why a page is being loaded.
#[derive(Default)]
struct Load {
    waiters: SmallVec<[(ReqId, u32); 2]>,
    scrub: bool,
    expire: bool,
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
    /// The previous location the new copy supersedes is registered as a source.
    pub(crate) supersedes: bool,
}

/// Why an extent's first sector is zeroed.
pub(crate) enum ZeroWhy {
    /// A tracked older copy (stale); clears the registry entry when durable.
    Stale { db: u8, key: Box<[u8]> },
    /// The key's newest copy is deleted: waits until no other copy is tracked (D1, E9).
    Delete { db: u8, key: Box<[u8]>, tickets: SmallVec<[TicketId; 1]> },
}

pub(crate) struct ExtZero {
    pub(crate) n_pages: u32,
    pub(crate) why: ZeroWhy,
    pub(crate) state: ExtState,
    wid: u64,
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
    /// Effects (page puts, markers, extent writes) not durable yet.
    left: u32,
    ticket: Option<TicketId>,
    /// The partition requests the commit itself (single-partition txn); XTxn parts are
    /// committed by the coordinating worker.
    local: bool,
    requested: bool,
    keys: SmallVec<[(u8, Box<[u8]>); 2]>,
    /// `(db, key, page)` of the txn delete markers.
    markers: SmallVec<[(u8, Box<[u8]>, PageId); 1]>,
    pages: SmallVec<[PageId; 2]>,
}

/// A meta request whose reply is pending (FIFO, §6.1).
enum MetaWait {
    Ag(AgKind),
    Seq(Seq),
    Cutoff { cutoff: u64, dbs: u32, ticket: TicketId, all: bool },
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
    seq_asked: bool,
    pub(crate) pmeta: PartitionMeta,
    pub(crate) cutoff: [u64; 16],
    pub(crate) cutoff_durable: [u64; 16],
    pub(crate) dirty: Vec<PageId>,
    pub(crate) ext: ExtTables,
    io: HashMap<u64, IoKind, foldhash::fast::FixedState>,
    next_io: u64,
    durable_wait: HashMap<u64, Durable, foldhash::fast::FixedState>,
    durable_q: Vec<Durable>,
    next_wid: u64,
    loads: HashMap<PageId, Load, foldhash::fast::FixedState>,
    meta_wait: VecDeque<MetaWait>,
    next_token: u64,
    pub(crate) counters: PartitionCounters,
    bufs: Vec<AlignedBuf>,
    pub(crate) expiry: crate::expire::ExpirySweep,
    pub(crate) buckets: Buckets,
    /// Pages whose tracked copies are zeroed by paced scrubs.
    scrub_queue: VecDeque<PageId>,
    /// Keys whose older copies a command waits for (foreground, this tick).
    fg_keys: Vec<(u8, Box<[u8]>)>,
    /// Pages holding effects deferred until other copies are zeroed (re-dirtied on change).
    deferred: HashSet<PageId, foldhash::fast::FixedState>,
    /// Extent zeroings blocked by D1.
    zero_blocked: Vec<PageId>,
    pub(crate) txns: HashMap<u64, OpenTxn, foldhash::fast::FixedState>,
    /// Txns committed this boot whose ids may still be above the durable `resolved_w`.
    committed: HashSet<u64, foldhash::fast::FixedState>,
    /// Ids of txns from before this boot known to be committed (recovery view).
    pub(crate) prior: Arc<crate::intent::CommittedSet>,
    /// FLUSHALL: AGs to release once no write is in flight.
    release_after_flush: Vec<AgId>,
    flushall_pending: bool,
    /// FLUSHDB reclaim walk (paced): SMALL pages to load, extent starts to check.
    reclaim_pages: VecDeque<PageId>,
    reclaim_ext: VecDeque<PageId>,
    space_wait: Vec<ReqId>,
    seq_wait: Vec<ReqId>,
    pub(crate) now: Now,
    pub(crate) failed: Option<i32>,
    pub(crate) writes_in_flight: u32,
    pub(crate) recovered_bytes: u64,
    pub(crate) recovery_mode: Option<RecoveryMode>,
    /// Uncommitted txn records found by recovery that must be zeroed before `resolved_w`
    /// passes them (E10); reported once zeroed.
    pub(crate) txn_zero_pending: u32,
    pub(crate) txn_zero_reported: bool,
}

impl Core {
    pub(crate) fn crc(&self) -> RecordCrc {
        self.cfg.crc
    }

    fn take_buf(&mut self, len: usize) -> AlignedBuf {
        if len <= PAGE
            && let Some(mut b) = self.bufs.pop()
        {
            b.set_visible_len(len);
            return b;
        }
        AlignedBuf::zeroed(len)
    }

    fn give_buf(&mut self, mut b: AlignedBuf) {
        if b.capacity() == PAGE && self.bufs.len() < 64 {
            b.set_visible_len(PAGE);
            self.bufs.push(b);
        }
    }

    fn tag(&mut self, k: IoKind) -> IoTag {
        let id = self.next_io;
        self.next_io += 1;
        self.io.insert(id, k);
        IoTag { partition: self.cfg.id, id }
    }

    fn dev_lba(&self, dev: u8) -> u32 {
        self.boot.devs.get(usize::from(dev)).map_or(self.cfg.lba, |d| d.lba).max(512)
    }

    fn dev_mode(&self, dev: u8) -> PageUpdateMode {
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
        txn != 0 && !self.txns.contains_key(&txn) && !self.committed.contains(&txn) && !self.prior.is_committed(txn)
    }

    fn txn_open(&self, txn: u64) -> bool {
        self.txns.contains_key(&txn)
    }

    fn meta_req(&mut self, meta: &MetaClient, r: MetaReq, w: MetaWait) {
        let t = self.next_token;
        self.next_token += 1;
        self.meta_wait.push_back(w);
        meta.request(self.cfg.id, t, r);
    }

    fn mark_dirty(&mut self, cache: &mut PageCache, page: PageId) {
        let Some(f) = cache.peek(page) else { return };
        let Some(st) = cache.state_mut(f) else { return };
        let pin = !st.work_pin;
        st.work_pin = true;
        if !st.dirty {
            st.dirty = true;
            self.dirty.push(page);
        }
        if pin {
            cache.pin(f);
        }
    }

    /// Releases the work pin of a page with nothing staged, in flight or wanted.
    fn maybe_unpin(cache: &mut PageCache, page: PageId) {
        let Some(f) = cache.peek(page) else { return };
        let Some(st) = cache.state_mut(f) else { return };
        if st.work_pin && st.is_clean() {
            st.work_pin = false;
            cache.unpin(f);
        }
    }

    fn note_page_space(&mut self, cache: &PageCache, page: PageId) {
        let Some(st) = cache.page_state(page) else { return };
        if st.fresh && st.pending.is_empty() && st.inflight.is_none() {
            return;
        }
        let used = st.used_bytes();
        let largest = st.largest();
        self.alloc.note_cached(page, PAGE_ROOM.saturating_sub(used), largest);
    }

    // ---------------------------------------------------------------------------------------
    // Index helpers

    /// The position of `(db, key)`'s entry naming `page36` with `kind`.
    fn find_entry(&self, db: u8, slot: u16, kh: KeyHash, page36: u64) -> Option<Pos> {
        let t = self.index.table(db, slot)?;
        let mut c = SmallVec::new();
        t.candidates(kh, &mut c);
        c.iter().find(|c| c.entry.page36 == page36).map(|c| c.pos)
    }

    fn hint(&self, expire_at: Option<u64>) -> u16 {
        kv_index::hint::encode(expire_at, self.expiry.hint_base_s)
    }

    fn index_set(&mut self, s: &Staged, cur36: Option<u64>, e: Entry, expire_at: Option<u64>) {
        let hint = self.hint(expire_at);
        let pos = cur36.and_then(|p| self.find_entry(s.db, s.slot, s.kh, p));
        let t = self.index.table_mut(s.db, s.slot);
        match pos {
            Some(pos) => {
                let mut e = e;
                e.fp32 = s.kh.fp32();
                t.update(pos, e, hint);
            }
            None => {
                if t.try_insert(s.kh, e, hint).is_err() {
                    debug_assert!(false, "index table full");
                }
            }
        }
    }

    fn index_remove(&mut self, s: &Staged, cur36: u64) {
        if let Some(pos) = self.find_entry(s.db, s.slot, s.kh, cur36) {
            self.index.table_mut(s.db, s.slot).remove(pos);
        }
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
        self.core.stale.get(db, key).iter().filter(|e| e.at.page() != self.page).count()
    }
    fn tracked_here(&self, db: u8, key: &[u8]) -> Option<CopyState> {
        self.core.stale.state_at(db, key, self.page)
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
    reqs: Requests,
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
            zero_blocked: Vec::new(),
            txns: HashMap::with_hasher(Default::default()),
            committed: HashSet::with_hasher(Default::default()),
            prior,
            release_after_flush: Vec::new(),
            flushall_pending: false,
            reclaim_pages: VecDeque::new(),
            reclaim_ext: VecDeque::new(),
            space_wait: Vec::new(),
            seq_wait: Vec::new(),
            now,
            failed: None,
            writes_in_flight: 0,
            recovered_bytes: 0,
            recovery_mode: None,
            txn_zero_pending: 0,
            txn_zero_reported: false,
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
            && self.core.writes_in_flight == 0
            && self.core.ext.writes.is_empty()
            && self.core.ext.zeros.is_empty()
            && self.core.durable_wait.is_empty()
            && self.core.durable_q.is_empty()
            && self.core.txns.is_empty()
            && self.core.loads.is_empty()
            && self.core.meta_wait.is_empty()
    }

    /// Tracked stale copies left (a snapshot needs none, §8.2 SHUTDOWN).
    pub fn stale_copies(&self) -> usize {
        self.core.stale.len()
    }

    // ---------------------------------------------------------------------------------------
    // Submission

    pub fn submit(&mut self, cx: &mut StorageCtx<'_>, req: Request) -> ReqId {
        self.core.now = cx.now;
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
            gen: 0,
            io: 0,
            waits: 0,
            barrier,
            blocked: false,
            queued: false,
            parked: false,
            pins: SmallVec::new(),
            fetched: SmallVec::new(),
            failed: None,
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
        if self.core.state == PartState::Ready {
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
                    needs.push(match pr.kind {
                        Kind::Small => Need::Page(PageId::from36(pr.page36)),
                        _ => Need::Extent { start: PageId::from36(pr.page36), pages: 1, chunks: false },
                    });
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
                    t.scan_pages(fp_from, max_entries as usize, kv_index::ScanFilter::ALL, &mut refs);
                    for pr in refs {
                        needs.push(match pr.kind {
                            Kind::Small => Need::Page(PageId::from36(pr.page36)),
                            _ => Need::Extent { start: PageId::from36(pr.page36), pages: class_pages(pr.class), chunks: true },
                        });
                    }
                }
            }
            _ => {
                for i in 0..r.keys.len() {
                    let (kh, slot) = (r.keys[i].kh, r.keys[i].slot);
                    let access = r.req.keys[i].1;
                    let Some(t) = self.core.index.table(db, slot) else { continue };
                    let mut c = SmallVec::new();
                    t.candidates(kh, &mut c);
                    for c in c.iter() {
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
        let gen = match self.reqs.get(id) {
            Some(r) => r.gen,
            None => return,
        };
        match n {
            Need::Page(page) => {
                if let Some(f) = cx.cache.get(page) {
                    if cx.cache.state(f).is_some() {
                        let r = self.reqs.get_mut(id).expect("live");
                        if !r.pins.contains(&page) {
                            r.pins.push(page);
                            cx.cache.pin(f);
                        }
                        return;
                    }
                }
                self.load_page(cx, page, Some((id, gen)), false);
            }
            Need::Extent { start, pages, chunks } => {
                let r = self.reqs.get_mut(id).expect("live");
                if r.fetched.iter().any(|f| f.start == start && f.pages >= pages && (!chunks || f.want_chunks) && f.err.is_none()) {
                    return;
                }
                r.fetched.retain(|f| f.start != start);
                r.fetched.push(ExtFetch { start, pages, buf: None, err: None, chunks: None, want_chunks: chunks });
                let idx = r.fetched.len() - 1;
                r.io += 1;
                let buf = AlignedBuf::zeroed(pages as usize * PAGE);
                let tag = self.core.tag(IoKind::ExtRead { req: id, gen, idx, chunk: None });
                cx.io.push(IoReq::read(tag, start.dev(), start.byte_offset(), buf));
                self.core.counters.page_reads += u64::from(pages);
            }
            Need::ExtWrite(start) => {
                if let Some(w) = self.core.ext.writes.get_mut(&start) {
                    w.waiters.push((id, gen));
                    self.reqs.get_mut(id).expect("live").io += 1;
                }
            }
        }
    }

    /// Issues (or joins) the read of `page`.
    fn load_page(&mut self, cx: &mut StorageCtx<'_>, page: PageId, waiter: Option<(ReqId, u32)>, scrub: bool) {
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
        // barriers and requests blocked behind them
        if let Some(&(_, b)) = self.reqs.barriers.front() {
            self.reqs.check_ready(b);
        }
        if r.barrier || self.reqs.barriers.is_empty() {
            let ids: Vec<ReqId> = self
                .reqs
                .slots
                .iter()
                .enumerate()
                .filter_map(|(i, s)| s.as_ref().filter(|r| r.blocked).map(|_| ReqId(i as u32)))
                .collect();
            for i in ids {
                self.reqs.check_ready(i);
            }
        }
    }

    fn requeue(&mut self, id: ReqId, r: Req) {
        self.reqs.slots[id.0 as usize] = Some(r);
        self.reqs.check_ready(id);
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
        let Some(mut r) = self.reqs.slots.get_mut(id.0 as usize).and_then(Option::take) else {
            return ExecStatus::Failed(StoreError::Io(-libc::ENOENT));
        };
        r.queued = false;
        if let Some(e) = r.failed {
            self.finish(cx.cache, id, r);
            return ExecStatus::Failed(e);
        }
        match self.core.state {
            PartState::Loading => {
                self.finish(cx.cache, id, r);
                return ExecStatus::Failed(StoreError::Loading);
            }
            PartState::ReadOnly | PartState::Ready => {}
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
                (true, ReqKind::FlushDb { db }) => self.exec_flush(cx, db),
                (true, _) => Ok(ExecOut { ticket: Some(self.exec_barrier()), group: None, seq: None }),
            };
            self.finish(cx.cache, id, r);
            return match (v, out) {
                (Ok(v), Ok(out)) => ExecStatus::Done(v, out),
                (Err(e), _) | (_, Err(e)) => ExecStatus::Failed(e),
            };
        }
        let now_ms = cx.now.unix_ms;
        let db = r.req.db;
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
                    ReqKind::Txn { txn_id } => Some((txn_id, false)),
                    _ => None,
                };
                match self.commit(cx, staged, txn, seqo) {
                    Commit::Done(out) => {
                        self.count_hits(&r, &out);
                        self.finish(cx.cache, id, r);
                        ExecStatus::Done(v, out)
                    }
                    Commit::Need(needs) => {
                        self.requeue(id, r);
                        for n in needs {
                            self.need(cx, id, n);
                        }
                        self.reqs.check_ready(id);
                        ExecStatus::Requeued
                    }
                    Commit::Space => {
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
                self.requeue(id, r);
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

    fn count_hits(&mut self, _r: &Req, out: &ExecOut) {
        if out.seq.is_some() {
            self.core.counters.keyspace_hits += 0;
        }
    }
}

/// Outcome of a commit attempt.
enum Commit {
    Done(ExecOut),
    /// Pages or extents to load before the request is run again.
    Need(SmallVec<[Need; 2]>),
    /// Waiting for an AG from the meta thread.
    Space,
    /// Waiting for a seq reservation (E12).
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

    fn commit(&mut self, cx: &mut StorageCtx<'_>, staged: SmallVec<[Staged; 2]>, txn: Option<(u64, bool)>, seqo: Option<Seq>) -> Commit {
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
        // txn id: an XTxn part carries it; a local multi-key write takes one (§14.6)
        let (txn_id, local_txn) = match txn {
            Some((t, _)) => (t, false),
            None if writes >= 2 => match cx.meta.next_txn_id() {
                0 => return Commit::Err(StoreError::Io(-libc::EAGAIN)),
                t => (t, true),
            },
            None => (0, false),
        };
        let is_txn = txn_id != 0;
        // 1. plan every placement; nothing changes unless all succeed
        let mut undo = Undo::default();
        let mut plans: SmallVec<[Plan; 2]> = SmallVec::new();
        for s in staged.iter() {
            let p = match &s.change {
                Change::Put { value, .. } => self.plan_put(cx, s, s.key.len(), value.len() as u64, is_txn, &mut undo),
                Change::Delete if is_txn => self.plan_put(cx, s, s.key.len(), 0, true, &mut undo),
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
        // 2. apply (infallible)
        let mut flags = 0;
        if staged.iter().any(|s| matches!(s.change, Change::Delete)) {
            flags |= TICKET_HAS_DELETE;
        }
        let ticket = (writes > 0).then(|| self.core.tickets.create(flags));
        let mut ops: Vec<Op> = Vec::new();
        let mut otx = is_txn.then(|| OpenTxn {
            left: 0,
            ticket,
            local: local_txn,
            requested: false,
            keys: SmallVec::new(),
            markers: SmallVec::new(),
            pages: SmallVec::new(),
        });
        for (s, plan) in staged.into_iter().zip(plans) {
            if let Some(op) = &s.op {
                ops.push(op.clone());
            }
            self.apply(cx, s, plan, seq, txn_id, ticket, otx.as_mut());
        }
        if let (Some(t), Some(o)) = (ticket, otx.as_ref())
            && o.local
        {
            self.core.tickets.add_meta(t);
        }
        if let Some(o) = otx {
            if o.left == 0 && o.local {
                // nothing to write (cannot happen for a txn with writes): commit at once
                self.core.txns.insert(txn_id, o);
                self.request_commit(cx.meta, txn_id);
            } else {
                self.core.txns.insert(txn_id, o);
            }
        }
        if writes > 0 {
            match seqo {
                Some(s) if s >= self.core.next_seq => self.core.next_seq = s.next().unwrap_or(s),
                Some(_) => {}
                None => self.core.next_seq = seq.next().unwrap_or(seq),
            }
        }
        if let Some(t) = ticket {
            self.core.tickets.seal(t);
        }
        let group = (self.core.cfg.produce_groups && !ops.is_empty()).then(|| Group { seq, ops });
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

    fn ask_seq(&mut self, meta: &MetaClient) {
        if self.core.seq_asked {
            return;
        }
        let floor = Seq(self.core.next_seq.0.saturating_add(SEQ_RESERVE));
        self.core.seq_asked = true;
        self.core.pmeta.seq_floor = floor;
        let id = self.core.cfg.id;
        self.core.meta_req(meta, MetaReq::ReserveSeq { partition: id, floor }, MetaWait::Seq(floor));
    }

    /// Chooses where a put of `val_len` value bytes goes.
    fn plan_put(&mut self, cx: &mut StorageCtx<'_>, s: &Staged, key_len: usize, val_len: u64, txn: bool, undo: &mut Undo) -> Result<Plan, Commit> {
        let now_ns = cx.now.mono_ns;
        let small = small_len(key_len, val_len as usize, txn);
        if key_len <= u16::MAX as usize && val_len <= u16::MAX as u64 && small <= SMALL_REC_MAX {
            let need = small as u16;
            if let Cur::Small { page, .. } = s.cur
                && self.fits(cx.cache, page, need, s)
            {
                return Ok(Plan::Small { page, fresh: false });
            }
            for _ in 0..2 {
                match self.core.alloc.place_small(need, cx.cache, now_ns) {
                    SmallPlace::Cached(q) => {
                        if self.core.stale.state_at(s.db, &s.key, q).is_some() {
                            continue; // never stage a key onto a page holding its tracked copy
                        }
                        return Ok(Plan::Small { page: q, fresh: false });
                    }
                    SmallPlace::Fresh(q) => {
                        undo.fresh.push(q);
                        return Ok(Plan::Small { page: q, fresh: true });
                    }
                    SmallPlace::Uncached(q) => {
                        let mut n = SmallVec::new();
                        n.push(Need::Page(q));
                        return Err(Commit::Need(n));
                    }
                    SmallPlace::NeedAg => return Err(Commit::Space),
                    SmallPlace::Full => return Err(Commit::Err(StoreError::OutOfSpace)),
                }
            }
            return Err(Commit::Space);
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
            return match self.core.alloc.alloc_extent(n, twin, now_ns) {
                ExtPlace::Run(start) => {
                    undo.ext.push((start, n));
                    Ok(Plan::Ext { start, n_pages: n, inplace: false })
                }
                ExtPlace::NeedAg => Err(Commit::Space),
                ExtPlace::Full => Err(Commit::Err(StoreError::OutOfSpace)),
            };
        }
        // CHUNKMAP + CHUNKs (§14.5)
        let n_chunks = val_len.div_ceil(CHUNK_MAX_PAYLOAD as u64).max(1) as usize;
        let map_pages = class_pages(
            class_for(pages_for((EXT_HDR + extent::chunkmap_body_len(key_len, n_chunks)) as u64) as u32)
                .ok_or(Commit::Err(StoreError::TooLarge))?,
        );
        let twin = match s.cur {
            Cur::Extent { twin, kind: Kind::ChunkMap, .. } => twin,
            _ => None,
        };
        let map = match self.core.alloc.alloc_extent(map_pages, twin, now_ns) {
            ExtPlace::Run(p) => p,
            ExtPlace::NeedAg => return Err(Commit::Space),
            ExtPlace::Full => return Err(Commit::Err(StoreError::OutOfSpace)),
        };
        undo.ext.push((map, map_pages));
        let mut chunks = SmallVec::new();
        let mut left = val_len;
        while left > 0 {
            let len = left.min(CHUNK_MAX_PAYLOAD as u64);
            left -= len;
            let n = class_pages(class_for(pages_for(EXT_HDR as u64 + len) as u32).expect("chunk fits a class"));
            match self.core.alloc.alloc_extent(n, None, now_ns) {
                ExtPlace::Run(p) => {
                    undo.ext.push((p, n));
                    chunks.push((p, n));
                }
                ExtPlace::NeedAg => return Err(Commit::Space),
                ExtPlace::Full => return Err(Commit::Err(StoreError::OutOfSpace)),
            }
        }
        Ok(Plan::Chunked { map, map_pages, chunks })
    }

    /// True when an extent of `n` pages at `start` can be rewritten in place atomically.
    fn atomic_extent(&self, start: PageId, n: u32) -> bool {
        let Some(d) = self.core.boot.devs.get(usize::from(start.dev())) else { return false };
        d.page_update == PageUpdateMode::Atomic
            && d.atomic_unit.is_some_and(|(min, max)| {
                let len = n * PAGE as u32;
                len >= min && len <= max && len.is_power_of_two()
            })
            && !self.core.ext.writes.contains_key(&start)
            && self.core.ext.zeros.is_empty()
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
    fn apply(&mut self, cx: &mut StorageCtx<'_>, s: Staged, plan: Plan, seq: Seq, txn: u64, ticket: Option<TicketId>, otx: Option<&mut OpenTxn>) {
        let seq = s.seq.unwrap_or(seq);
        let mut tickets: SmallVec<[TicketId; 1]> = SmallVec::new();
        if let Some(t) = ticket
            && matches!(s.change, Change::Put { .. } | Change::Delete)
        {
            tickets.push(t);
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
                            _ => None,
                        };
                        let hdr = ext_header(EXT_KIND_VALUE, ext_vtype, s.db, start, seq, expire_at, n_pages, s.key.len(), value.len() as u64, txn, s.kh.h32(), twin);
                        let mut buf = AlignedBuf::zeroed(n_pages as usize * PAGE);
                        extent::encode_extent(&hdr, &s.key, &[value], self.core.crc(), &mut buf);
                        let tk = self.move_away(cx, &s, Some(start), inplace);
                        let mut tks = tickets;
                        tks.extend(tk);
                        self.add_ext_write(start, hdr, s.db, s.key.clone(), buf, tks, None, inplace, !inplace && s.cur != Cur::Absent);
                        let e = Entry { fp32: s.kh.fp32(), x8: class, page36: start.to36(), kind: Kind::Extent, vtype, sync_mark: false };
                        self.core.index_set(&s, cur36(&s.cur), e, expire_at);
                        if let Some(o) = otx {
                            o.left += 1;
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
                            self.add_ext_write(cp, ch, 0, Box::default(), b, SmallVec::new(), Some(map), false, false);
                        }
                        let mut body = vec![0u8; extent::CHUNKMAP_PREFIX + refs.len() * extent::CHUNK_REF_BYTES];
                        extent::encode_chunk_refs(&refs, &mut body);
                        let twin = match s.cur {
                            Cur::Extent { start: old, kind: Kind::ChunkMap, .. } => Some(old),
                            _ => None,
                        };
                        let mut hdr = ext_header(EXT_KIND_CHUNKMAP, ext_vtype, s.db, map, seq, expire_at, map_pages, s.key.len(), value.len() as u64, txn, s.kh.h32(), twin);
                        hdr.val_len = value.len() as u64;
                        let mut buf = AlignedBuf::zeroed(map_pages as usize * PAGE);
                        extent::encode_extent(&hdr, &s.key, &[&body], self.core.crc(), &mut buf);
                        let tk = self.move_away(cx, &s, Some(map), false);
                        let mut tks = tickets;
                        tks.extend(tk);
                        self.core.ext.chunkmaps.insert(map, chunks.iter().copied().collect());
                        self.add_ext_write(map, hdr, s.db, s.key.clone(), buf, tks, None, false, s.cur != Cur::Absent);
                        if let Some(w) = self.core.ext.writes.get_mut(&map) {
                            w.wait_chunks = chunks.len() as u32;
                        }
                        let e = Entry { fp32: s.kh.fp32(), x8: class, page36: map.to36(), kind: Kind::ChunkMap, vtype, sync_mark: false };
                        self.core.index_set(&s, cur36(&s.cur), e, expire_at);
                        if let Some(o) = otx {
                            o.left += 1;
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
                        if let Some(c) = cur36(&Cur::Small { page, seq }) {
                            // the marker's page holds no index entry: the key is absent
                            let _ = c;
                        }
                        let pos = self.core.find_entry(s.db, s.slot, s.kh, page.to36());
                        if let Some(pos) = pos {
                            self.core.index.table_mut(s.db, s.slot).remove(pos);
                        }
                        self.core.counters.txn_markers += 1;
                    }
                    return;
                }
                self.apply_delete(cx, &s, tickets);
            }
        }
    }

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
        if !same_page {
            let tk = self.move_away(cx, s, None, false);
            rec.tickets.extend(tk);
            if !matches!(s.cur, Cur::Absent) {
                self.core.counters.moves += 1;
            }
        } else if rec.desc.txn != 0 {
            // a txn version never replaces the copy it supersedes before the commit (13.3 c)
            self.protect_in_page(cx.cache, page, s);
        }
        let txn = rec.desc.txn;
        for t in rec.tickets.clone() {
            self.core.tickets.add_effect(t);
            self.core.tickets.note_page(t, page, 0);
        }
        self.stage_put(cx.cache, page, rec);
        if !marker {
            let e = Entry { fp32: s.kh.fp32(), x8: s.kh.x8(), page36: page.to36(), kind: Kind::Small, vtype, sync_mark: false };
            self.core.index_set(s, cur36(&s.cur), e, expire_at);
        } else if let Some(c) = cur36(&s.cur)
            && !matches!(s.cur, Cur::Absent)
        {
            self.core.index_remove(s, c);
        }
        if let Some(o) = otx {
            o.left += 1;
            o.keys.push((s.db, s.key.clone()));
            if !o.pages.contains(&page) {
                o.pages.push(page);
            }
            if marker {
                o.markers.push((s.db, s.key.clone(), page));
            }
        }
        let _ = txn;
    }

    fn init_fresh(&mut self, cache: &mut PageCache, page: PageId) {
        if cache.contains(page) {
            if let Some(st) = cache.page_state_mut(page) {
                st.fresh = st.recs.is_empty() && st.pending.is_empty() && st.inflight.is_none();
            }
            return;
        }
        let Some(f) = cache.alloc(page) else { return };
        cache.frame_mut(f).fill(0);
        cache.put_state(f, Box::new(PageState::fresh(page, f)));
    }

    /// Marks the key's copies in `page` protected (not live): a newer version lives elsewhere
    /// or is a txn version that is not committed.
    fn protect_in_page(&mut self, cache: &mut PageCache, page: PageId, s: &Staged) -> bool {
        let Some(f) = cache.peek(page) else { return false };
        let Some(mut st) = cache.take_state(f) else { return false };
        let h32 = s.kh.h32();
        let mut has = false;
        {
            let img = cache.frame(st.m_frame);
            for r in st.recs.iter_mut() {
                if r.db == s.db && r.h32 == h32 && r.key(img) == &*s.key && matches!(r.status, RecStatus::Live | RecStatus::Protected) {
                    r.status = RecStatus::Protected;
                    has = true;
                }
            }
        }
        if let Some(inf) = st.inflight.as_mut() {
            let img = cache.frame(inf.n_frame);
            for r in inf.recs.iter_mut() {
                if r.db == s.db && r.h32 == h32 && r.key(img) == &*s.key && matches!(r.status, RecStatus::Live | RecStatus::Protected) {
                    r.status = RecStatus::Protected;
                    has = true;
                }
            }
        }
        cache.put_state(f, st);
        has
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
                    && let Some(i) = st.pending.iter().position(|p| p.is_key(s.db, h32, &s.key))
                {
                    let mut eff = st.pending.remove(i);
                    out.extend(eff.tickets_mut().drain(..));
                }
                if self.protect_in_page(cx.cache, page, s) {
                    self.core.stale.add(s.db, &s.key, StaleRef::Small { page }, CopyState::Source);
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
                match self.core.ext.writes.get(&start).map(|w| w.state) {
                    Some(ExtState::Pending) => {
                        let w = self.core.ext.writes.remove(&start).expect("present");
                        out.extend(w.tickets);
                        self.wake(w.waiters);
                        self.core.alloc.free_extent(start, w.hdr.n_pages);
                        self.cancel_chunks(start);
                    }
                    Some(_) => self.core.stale.add(s.db, &s.key, StaleRef::Extent { page: start, n_pages }, CopyState::Source),
                    None => self.core.stale.add(s.db, &s.key, StaleRef::Extent { page: start, n_pages }, CopyState::Source),
                }
            }
        }
        out
    }

    /// Cancels the staged chunk writes of a chunkmap that was never written.
    fn cancel_chunks(&mut self, map: PageId) {
        let Some(chunks) = self.core.ext.chunkmaps.remove(&map) else { return };
        for (c, n) in chunks {
            let rc = self.core.ext.chunk_rc.entry(c).or_insert(1);
            *rc -= 1;
            if *rc == 0 {
                self.core.ext.chunk_rc.remove(&c);
                if self.core.ext.writes.get(&c).is_some_and(|w| w.state == ExtState::Pending) {
                    self.core.ext.writes.remove(&c);
                    self.core.alloc.free_extent(c, n);
                }
            }
        }
    }

    fn wake(&mut self, waiters: SmallVec<[(ReqId, u32); 1]>) {
        for (id, gen) in waiters {
            if self.reqs.live_gen(id, gen) {
                let r = self.reqs.get_mut(id).expect("live");
                r.io = r.io.saturating_sub(1);
                self.reqs.check_ready(id);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn add_ext_write(
        &mut self,
        start: PageId,
        hdr: ExtHeader,
        db: u8,
        key: Box<[u8]>,
        buf: AlignedBuf,
        tickets: SmallVec<[TicketId; 1]>,
        parent: Option<PageId>,
        atomic: bool,
        supersedes: bool,
    ) {
        for &t in &tickets {
            self.core.tickets.add_effect(t);
            self.core.tickets.note_extent(t, start);
        }
        if let Some(old) = self.core.ext.writes.remove(&start) {
            // atomic in-place rewrite over a staged image (the old one never reached media)
            debug_assert!(old.state == ExtState::Pending);
        }
        self.core.ext.writes.insert(
            start,
            ExtWrite {
                hdr,
                db,
                key,
                buf: Some(buf),
                tickets,
                state: ExtState::Pending,
                wait_chunks: 0,
                parent,
                atomic,
                waiters: SmallVec::new(),
                wid: 0,
                supersedes,
            },
        );
    }

    fn apply_delete(&mut self, cx: &mut StorageCtx<'_>, s: &Staged, tickets: SmallVec<[TicketId; 1]>) {
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
                self.stage_remove(cx.cache, page, s.db, s.kh.h32(), s.key.clone(), tickets);
            }
            Cur::Extent { start, n_pages, .. } => {
                self.core.index_remove(s, start.to36());
                let mut tickets = tickets;
                match self.core.ext.writes.get(&start).map(|w| w.state) {
                    Some(ExtState::Pending) => {
                        // never written: nothing at `start` to drop
                        let w = self.core.ext.writes.remove(&start).expect("present");
                        for t in w.tickets {
                            self.core.tickets.effect_resolved(t);
                        }
                        self.wake(w.waiters);
                        self.core.alloc.free_extent(start, w.hdr.n_pages);
                        self.cancel_chunks(start);
                        self.core.gone_wait.push((s.db, s.key.clone(), core::mem::take(&mut tickets)));
                    }
                    _ => {
                        self.core.ext.zeros.insert(
                            start,
                            ExtZero { n_pages, why: ZeroWhy::Delete { db: s.db, key: s.key.clone(), tickets }, state: ExtState::Pending, wid: 0 },
                        );
                    }
                }
            }
        }
    }

    /// Lazy expiry without a write (§16.4): the entry goes; the record stays as free space
    /// unless older copies of the key exist (then it is kept until they are zeroed, D1).
    fn apply_expired(&mut self, cx: &mut StorageCtx<'_>, s: &Staged) {
        self.core.counters.expired_keys += 1;
        match s.cur {
            Cur::Absent => {}
            Cur::Small { page, .. } => {
                self.core.index_remove(s, page.to36());
                let others = self.core.stale.count(s.db, &s.key) > 0;
                let h32 = s.kh.h32();
                if let Some(f) = cx.cache.peek(page)
                    && let Some(mut st) = cx.cache.take_state(f)
                {
                    let img = cx.cache.frame(st.m_frame);
                    let mut copies = 0;
                    for r in st.recs.iter() {
                        if r.db == s.db && r.h32 == h32 && r.key(img) == &*s.key && r.status != RecStatus::Droppable {
                            copies += 1;
                        }
                    }
                    let keep = others || copies > 1;
                    for r in st.recs.iter_mut() {
                        if r.status == RecStatus::Live && r.db == s.db && r.h32 == h32 && r.key(img) == &*s.key {
                            r.status = if keep { RecStatus::Protected } else { RecStatus::Droppable };
                        }
                    }
                    cx.cache.put_state(f, st);
                }
                self.core.note_page_space(cx.cache, page);
            }
            Cur::Extent { start, n_pages, .. } => {
                self.core.index_remove(s, start.to36());
                self.core.ext.zeros.entry(start).or_insert(ExtZero {
                    n_pages,
                    why: ZeroWhy::Delete { db: s.db, key: s.key.clone(), tickets: SmallVec::new() },
                    state: ExtState::Pending,
                    wid: 0,
                });
            }
        }
    }

    fn stage_put(&mut self, cache: &mut PageCache, page: PageId, mut rec: EncodedRecord) {
        if let Some(st) = cache.page_state_mut(page) {
            let (db, h32) = (rec.desc.db, rec.desc.h32);
            if let Some(i) = st.pending.iter().position(|p| p.is_key(db, h32, rec.key())) {
                // D2: a superseded staged effect hands its tickets to the survivor
                let mut old = st.pending.remove(i);
                rec.tickets.extend(old.tickets_mut().drain(..));
            }
            st.pending.push(Pending::Put(rec));
        }
        self.core.mark_dirty(cache, page);
        self.core.note_page_space(cache, page);
    }

    fn stage_remove(&mut self, cache: &mut PageCache, page: PageId, db: u8, h32: u32, key: Box<[u8]>, mut tickets: SmallVec<[TicketId; 1]>) {
        if let Some(st) = cache.page_state_mut(page) {
            if let Some(i) = st.pending.iter().position(|p| p.is_key(db, h32, &key)) {
                let mut old = st.pending.remove(i);
                tickets.extend(old.tickets_mut().drain(..));
            }
            st.pending.push(Pending::Remove { db, h32, key, tickets });
        } else {
            for t in tickets {
                self.core.tickets.effect_resolved(t);
            }
            return;
        }
        self.core.mark_dirty(cache, page);
        self.core.note_page_space(cache, page);
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
    ExtHeader {
        kind,
        vtype,
        db,
        flags,
        page,
        seq,
        expire_at_ms: expire_at.unwrap_or(0),
        n_pages,
        key_len: key_len as u32,
        val_len,
        txn,
        h32,
        twin,
    }
}
