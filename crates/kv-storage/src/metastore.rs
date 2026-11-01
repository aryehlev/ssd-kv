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
//!
//! The logic is [`MetaCore`], a synchronous state machine over a [`MetaIo`]: the thread of
//! [`MetaStore::start`] runs it over devices it owns; single-threaded drivers (tools, tests, the
//! simulator) run it inline. One processing cycle takes every queued request, updates the
//! in-RAM blocks, writes every changed block once into its older copy (never both copies of a
//! block in one batch, §10.2), flushes each touched device once, then replies in request order
//! (so every partition sees its replies in the order it asked).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::time::Duration;

use hashbrown::{HashMap, HashSet};
use kv_common::abblock::AbCopy;
use parking_lot::Mutex;
use smallvec::SmallVec;

use crate::agtable::{AgKind, AgTableBlock};
use crate::alloc::KeyedPick;
use crate::engine::{flush_sync, read_sync, write_sync};
use crate::intent::{CommittedSet, INTENT_SLOTS, IntentHeader, SlotPair, TxnIds, plan_commit};
use crate::io::{AlignedBuf, BlockDevice};
use crate::layout::{AgId, DeviceLayout, PAGE, SB_PAGE, Seq};
use crate::pmeta::{NodeMeta, PartitionMeta};
use crate::superblock::{SbUpdate, Superblock};

pub enum MetaReq {
    AcquireAg { partition: u16, kind: AgKind },
    /// An AG on device `dev` (the chunks of a large value stay on their chunkmap's device,
    /// LC-08); refused when that device has no free AG.
    AcquireAgOn { partition: u16, kind: AgKind, dev: u8 },
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
    /// The partition's uncommitted txn records of earlier boots are durably zeroed, so
    /// `resolved_w` may pass their ids (E10, §18.5).
    PriorTxnsZeroed { partition: u16 },
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
pub struct MetaMsg {
    pub(crate) partition: u16,
    pub(crate) token: u64,
    pub(crate) req: MetaReq,
}

impl MetaMsg {
    pub fn new(partition: u16, token: u64, req: MetaReq) -> Self {
        MetaMsg { partition, token, req }
    }
}

/// State shared by every client and the meta logic.
pub struct MetaShared {
    txn_ids: Arc<TxnIds>,
    resolved_w: AtomicU64,
}

impl MetaShared {
    fn new(txn_ids: Arc<TxnIds>, resolved_w: u64) -> Self {
        MetaShared { txn_ids, resolved_w: AtomicU64::new(resolved_w) }
    }
}

/// Sending side of the meta thread's request queue (one clone per worker).
#[derive(Clone)]
pub struct MetaClient {
    tx: Sender<MetaMsg>,
    shared: Arc<MetaShared>,
}

/// Receiving side of a meta queue (inline drivers).
pub struct MetaInbox {
    rx: Receiver<MetaMsg>,
}

impl MetaInbox {
    /// Every queued request, in arrival order.
    pub fn drain(&self, out: &mut Vec<MetaMsg>) {
        while let Ok(m) = self.rx.try_recv() {
            out.push(m);
        }
    }
}

impl MetaClient {
    /// Queues `r`; the reply arrives through the requesting worker's hook with `(partition, token)`.
    pub fn request(&self, partition: u16, token: u64, r: MetaReq) {
        // a stopped meta thread drops the request: the engine is shutting down
        let _ = self.tx.send(MetaMsg { partition, token, req: r });
    }

    /// A client whose requests go nowhere (device-less partitions, tests).
    pub fn disconnected() -> Self {
        let (tx, _rx) = std::sync::mpsc::channel();
        MetaClient { tx, shared: Arc::new(MetaShared::new(Arc::new(TxnIds::new(1, 1)), 0)) }
    }

    /// The next node-global txn id, 0 while the reserved range is being extended (§14.6).
    pub fn next_txn_id(&self) -> u64 {
        self.shared.txn_ids.next()
    }

    /// The txn id source (cross-partition transactions take their ids here).
    pub fn txn_ids(&self) -> Arc<TxnIds> {
        self.shared.txn_ids.clone()
    }

    /// The durable `resolved_w` (§10.7).
    pub fn resolved_w(&self) -> u64 {
        self.shared.resolved_w.load(Ordering::Acquire)
    }
}

/// The I/O the meta logic needs: synchronous whole-block reads, writes and flushes.
pub trait MetaIo {
    fn read(&mut self, dev: u8, offset: u64, len: usize) -> std::io::Result<AlignedBuf>;
    fn write(&mut self, dev: u8, offset: u64, buf: AlignedBuf) -> std::io::Result<()>;
    fn flush(&mut self, dev: u8) -> std::io::Result<()>;
    fn devices(&self) -> usize;
    fn lba(&self, dev: u8) -> u32;
}

/// Devices owned by the meta thread.
pub struct OwnedDevs(pub Vec<Box<dyn BlockDevice + Send>>);

impl MetaIo for OwnedDevs {
    fn read(&mut self, dev: u8, offset: u64, len: usize) -> std::io::Result<AlignedBuf> {
        read_sync(&mut *self.0[usize::from(dev)], offset, len)
    }
    fn write(&mut self, dev: u8, offset: u64, buf: AlignedBuf) -> std::io::Result<()> {
        write_sync(&mut *self.0[usize::from(dev)], offset, buf)
    }
    fn flush(&mut self, dev: u8) -> std::io::Result<()> {
        let d = &mut *self.0[usize::from(dev)];
        if d.info().write_through {
            return Ok(());
        }
        flush_sync(d)
    }
    fn devices(&self) -> usize {
        self.0.len()
    }
    fn lba(&self, dev: u8) -> u32 {
        self.0[usize::from(dev)].info().lba.max(512)
    }
}

// ---------------------------------------------------------------------------------------------
// Superblock link: the meta thread and the engine's shutdown share device 0's superblock state

/// Device 0's superblock, its newest block seq and copy; `closed` once the engine wrote the
/// final (shutdown) superblock, after which the meta thread writes no superblock.
pub(crate) struct SbCell {
    pub(crate) sb: Superblock,
    pub(crate) seq: u64,
    pub(crate) copy: AbCopy,
    pub(crate) closed: bool,
}

pub(crate) struct SbLink {
    pub(crate) cell: Mutex<SbCell>,
}

static SB_LINKS: Mutex<Vec<([u8; 16], std::sync::Weak<SbLink>)>> = parking_lot::const_mutex(Vec::new());

impl SbLink {
    pub(crate) fn new(sb: Superblock, seq: u64, copy: AbCopy) -> Arc<SbLink> {
        Arc::new(SbLink { cell: Mutex::new(SbCell { sb, seq, copy, closed: false }) })
    }

    /// Makes `link` the shared superblock state of its set (engine open).
    pub(crate) fn register(link: &Arc<SbLink>) {
        let uuid = link.cell.lock().sb.set_uuid;
        let mut v = SB_LINKS.lock();
        v.retain(|(u, w)| w.strong_count() > 0 && *u != uuid);
        v.push((uuid, Arc::downgrade(link)));
    }

    pub(crate) fn find(uuid: &[u8; 16]) -> Option<Arc<SbLink>> {
        let v = SB_LINKS.lock();
        v.iter().find(|(u, _)| u == uuid).and_then(|(_, w)| w.upgrade())
    }

    /// Writes `sb` (with the next block seq, into the older copy) through `io` and flushes.
    pub(crate) fn write(cell: &mut SbCell, io: &mut dyn MetaIo, sb: Superblock) -> std::io::Result<()> {
        let seq = cell.seq + 1;
        let copy = cell.copy.other();
        let mut buf = AlignedBuf::zeroed(PAGE);
        sb.encode(seq, buf.page_mut(0));
        io.write(0, SB_PAGE[copy.index()] * PAGE as u64, buf)?;
        io.flush(0)?;
        cell.sb = sb;
        cell.seq = seq;
        cell.copy = copy;
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------
// The meta logic

/// One AbBlock in RAM with its newest block seq and the copy holding it.
struct Ab<T> {
    v: T,
    seq: u64,
    copy: AbCopy,
    dirty: bool,
}

impl<T> Ab<T> {
    fn new(v: T, seq: u64, copy: AbCopy) -> Self {
        Ab { v, seq, copy, dirty: false }
    }
}

/// Draw domains of the AG pool (separate from the partitions' domains).
const DOMAIN_AG_DEVICE: u64 = 0x1_0000;
const DOMAIN_AG_PICK: u64 = 0x2_0000;
/// Clock floor cadence (`clock-floor-interval-ms`, §19).
pub const CLOCK_FLOOR_INTERVAL_MS: u64 = 1000;

/// The meta thread's state (§6.1).
pub struct MetaCore {
    partitions: u16,
    layouts: Vec<DeviceLayout>,
    link: Arc<SbLink>,
    agt: Vec<Vec<Ab<AgTableBlock>>>,
    /// Free-AG bitmap per device (bit = AG index).
    free: Vec<Vec<u64>>,
    allocated: u64,
    total_ags: u64,
    max_fill: f32,
    pmeta: Vec<Ab<PartitionMeta>>,
    nmeta: Ab<NodeMeta>,
    header: Ab<IntentHeader>,
    slots: Vec<SlotPair>,
    pick: KeyedPick,
    shared: Arc<MetaShared>,
    /// Durable `resolved_w`.
    w: u64,
    /// `txn_floor` at startup: ids below it belong to earlier boots.
    boot_floor: u64,
    /// Committed ids of earlier boots (listed in slots at startup).
    prior: CommittedSet,
    prior_zeroed: HashSet<u16, foldhash::fast::FixedState>,
    /// Resolved ids above `w` (committed durably or aborted).
    resolved: HashSet<u64, foldhash::fast::FixedState>,
    /// Who asked to commit or abort an id (credits go back there).
    owners: HashMap<u64, (u16, u64), foldhash::fast::FixedState>,
    last_clock_ms: u64,
    failed: Option<i32>,
}

/// Replies of one cycle: `(partition, token, done)` in request order.
pub type MetaReplies = Vec<(u16, u64, MetaDone)>;

impl MetaCore {
    /// Reads every fixed metadata block from the devices and makes the txn id range usable
    /// (writes the intent header with an extended `txn_floor`, §10.7).
    pub fn load(io: &mut dyn MetaIo, now_ms: u64) -> std::io::Result<MetaCore> {
        use std::io::{Error, ErrorKind};
        let bad = |what: String| Error::new(ErrorKind::InvalidData, what);
        let n = io.devices();
        if n == 0 {
            return Err(Error::new(ErrorKind::InvalidInput, "no devices"));
        }
        let mut layouts = Vec::with_capacity(n);
        let mut sb0 = None;
        for d in 0..n as u8 {
            let a = io.read(d, SB_PAGE[0] * PAGE as u64, PAGE)?;
            let b = io.read(d, SB_PAGE[1] * PAGE as u64, PAGE)?;
            let (copy, seq, sb) = Superblock::pick(&a, &b).ok_or_else(|| bad(format!("device {d}: no valid superblock")))?;
            layouts.push(sb.layout());
            if d == 0 {
                sb0 = Some((copy, seq, sb));
            }
        }
        let (copy, seq, sb) = sb0.expect("device 0");
        let partitions = sb.partitions;
        let link = match SbLink::find(&sb.set_uuid) {
            Some(l) => l,
            None => SbLink::new(sb.clone(), seq, copy),
        };
        let mut agt = Vec::with_capacity(n);
        let mut free = Vec::with_capacity(n);
        let mut allocated = 0u64;
        let mut total = 0u64;
        for (d, l) in layouts.iter().enumerate() {
            let mut blocks = Vec::with_capacity(l.ag_table_blocks as usize);
            let mut bits = vec![0u64; (l.n_ags as usize).div_ceil(64)];
            for blk in 0..l.ag_table_blocks {
                let a = io.read(d as u8, DeviceLayout::ag_table_copy_page(blk, AbCopy::A) * PAGE as u64, PAGE)?;
                let b = io.read(d as u8, DeviceLayout::ag_table_copy_page(blk, AbCopy::B) * PAGE as u64, PAGE)?;
                let (c, s, t) = AgTableBlock::pick(blk, &a, &b).ok_or_else(|| bad(format!("device {d}: AG table block {blk}")))?;
                for (i, &(kind, _)) in t.entries.iter().enumerate() {
                    let ag = t.ag_of(i);
                    if ag >= l.n_ags {
                        continue;
                    }
                    if kind == AgKind::Free {
                        bits[(ag / 64) as usize] |= 1 << (ag % 64);
                    } else {
                        allocated += 1;
                    }
                }
                blocks.push(Ab::new(t, s, c));
            }
            total += u64::from(l.n_ags);
            agt.push(blocks);
            free.push(bits);
        }
        let l0 = layouts[0];
        let mut pmeta = Vec::with_capacity(usize::from(partitions));
        for p in 0..partitions {
            let a = io.read(0, l0.pmeta_copy_page(p, AbCopy::A) * PAGE as u64, PAGE)?;
            let b = io.read(0, l0.pmeta_copy_page(p, AbCopy::B) * PAGE as u64, PAGE)?;
            let (c, s, m) = PartitionMeta::pick(p, partitions, &a, &b).ok_or_else(|| bad(format!("partition meta {p}")))?;
            pmeta.push(Ab::new(m, s, c));
        }
        let a = io.read(0, l0.nmeta_copy_page(AbCopy::A) * PAGE as u64, PAGE)?;
        let b = io.read(0, l0.nmeta_copy_page(AbCopy::B) * PAGE as u64, PAGE)?;
        let (c, s, nm) = NodeMeta::pick(&a, &b).ok_or_else(|| bad("node meta".into()))?;
        let nmeta = Ab::new(nm, s, c);
        let a = io.read(0, l0.intent_header_copy_page(AbCopy::A) * PAGE as u64, PAGE)?;
        let b = io.read(0, l0.intent_header_copy_page(AbCopy::B) * PAGE as u64, PAGE)?;
        let (c, s, ih) = IntentHeader::pick(&a, &b).ok_or_else(|| bad("intent header".into()))?;
        let lba0 = io.lba(0);
        let slots = read_slots(io, &l0, lba0)?;
        let prior = CommittedSet::new(ih.resolved_w, &slots);
        let boot_floor = ih.txn_floor.max(ih.resolved_w + 1).max(1);
        let txn_ids = Arc::new(TxnIds::new(boot_floor, boot_floor));
        let shared = Arc::new(MetaShared::new(txn_ids, ih.resolved_w));
        let mut core = MetaCore {
            partitions,
            layouts,
            link,
            agt,
            free,
            allocated,
            total_ags: total,
            max_fill: 0.95,
            pmeta,
            nmeta,
            header: Ab::new(ih, s, c),
            slots,
            pick: KeyedPick::new(sb.hash_seed, sb.boot_gen),
            shared,
            w: ih.resolved_w,
            boot_floor,
            prior,
            prior_zeroed: HashSet::with_hasher(Default::default()),
            resolved: HashSet::with_hasher(Default::default()),
            owners: HashMap::with_hasher(Default::default()),
            last_clock_ms: now_ms,
            failed: None,
        };
        // a reserved id range before any multi-key write (§10.7)
        core.header.v.txn_floor = core.shared.txn_ids.next_floor();
        core.header.dirty = true;
        core.write_dirty(io)?;
        core.shared.txn_ids.extend_floor(core.header.v.txn_floor);
        Ok(core)
    }

    /// The fill limit (`max-storage-fill`, §11.6).
    pub fn set_max_fill(&mut self, f: f32) {
        self.max_fill = if f.is_finite() { f.clamp(0.0, 1.0) } else { 0.95 };
    }

    /// A client connected to a new queue for this core (`(client, inbox)`).
    pub fn client(&self) -> (MetaClient, MetaInbox) {
        let (tx, rx) = std::sync::mpsc::channel();
        (MetaClient { tx, shared: self.shared.clone() }, MetaInbox { rx })
    }

    /// The durable `resolved_w`.
    pub fn resolved_w(&self) -> u64 {
        self.w
    }

    /// The txn id source.
    pub fn txn_ids(&self) -> Arc<TxnIds> {
        self.shared.txn_ids.clone()
    }

    /// AGs per state: `(allocated, total)`.
    pub fn ag_usage(&self) -> (u64, u64) {
        (self.allocated, self.total_ags)
    }

    /// Runs one cycle over `msgs` and returns the replies (in request order) after everything
    /// they depend on is durable. `now_ms` drives the clock floor (E13).
    pub fn process(&mut self, io: &mut dyn MetaIo, msgs: Vec<MetaMsg>, now_ms: u64, out: &mut MetaReplies) {
        let mut pending: Vec<(u16, u64, MetaDone)> = Vec::with_capacity(msgs.len());
        let mut commits: SmallVec<[(usize, u64); 8]> = SmallVec::new();
        let mut sb_updates: SmallVec<[SbUpdate; 1]> = SmallVec::new();
        for m in msgs {
            let done = match m.req {
                MetaReq::AcquireAg { partition, kind } => MetaDone::Ag(self.acquire(partition, kind, None)),
                MetaReq::AcquireAgOn { partition, kind, dev } => MetaDone::Ag(self.acquire(partition, kind, Some(dev))),
                MetaReq::ReleaseAg { ag } => {
                    self.release(ag);
                    MetaDone::Ok
                }
                MetaReq::PartitionMeta { m: pm } => {
                    if let Some(b) = self.pmeta.get_mut(usize::from(pm.partition)) {
                        let floor = b.v.seq_floor.max(pm.seq_floor);
                        let mut cut = b.v.flush_cutoff;
                        for (c, n) in cut.iter_mut().zip(pm.flush_cutoff.iter()) {
                            if *n > *c {
                                *c = *n;
                            }
                        }
                        b.v = pm;
                        b.v.seq_floor = floor;
                        b.v.flush_cutoff = cut;
                        b.dirty = true;
                    }
                    MetaDone::Ok
                }
                MetaReq::NodeMeta { m: nm } => {
                    self.nmeta.v = nm;
                    self.nmeta.dirty = true;
                    MetaDone::Ok
                }
                MetaReq::ReserveSeq { partition, floor } => {
                    if let Some(b) = self.pmeta.get_mut(usize::from(partition))
                        && floor > b.v.seq_floor
                    {
                        b.v.seq_floor = floor;
                        b.dirty = true;
                    }
                    MetaDone::Ok
                }
                MetaReq::CommitTxn { txn } => {
                    self.owners.insert(txn, (m.partition, m.token));
                    commits.push((pending.len(), txn));
                    MetaDone::Committed { txn }
                }
                MetaReq::TxnAborted { txn } => {
                    if txn > self.w {
                        self.resolved.insert(txn);
                        self.owners.insert(txn, (m.partition, m.token));
                    }
                    MetaDone::Ok
                }
                MetaReq::Superblock(u) => {
                    sb_updates.push(u);
                    MetaDone::Ok
                }
                MetaReq::Barrier => MetaDone::Ok,
                MetaReq::PriorTxnsZeroed { partition } => {
                    self.prior_zeroed.insert(partition);
                    MetaDone::Ok
                }
            };
            pending.push((m.partition, m.token, done));
        }
        // commit batch: the lowest-index slots with room (§10.7)
        if !commits.is_empty() {
            let batch: SmallVec<[u64; 8]> = commits.iter().map(|&(_, t)| t).collect();
            if let Err(e) = self.write_commits(io, &batch) {
                for &(i, _) in &commits {
                    pending[i].2 = MetaDone::Failed(neg(&e));
                }
            } else {
                for &t in &batch {
                    if t > self.w {
                        self.resolved.insert(t);
                    }
                }
            }
        }
        if now_ms >= self.last_clock_ms + CLOCK_FLOOR_INTERVAL_MS {
            self.last_clock_ms = now_ms;
            sb_updates.push(SbUpdate::ClockFloor { unix_ms: now_ms });
        }
        if self.shared.txn_ids.wants_extension() {
            self.header.v.txn_floor = self.shared.txn_ids.next_floor();
            self.header.dirty = true;
        }
        let w_target = self.w_target();
        if w_target > self.w {
            self.header.v.resolved_w = w_target;
            self.header.dirty = true;
        }
        let res = self.write_dirty(io).and_then(|()| self.write_superblock(io, &sb_updates));
        match res {
            Ok(()) => {
                self.shared.txn_ids.extend_floor(self.header.v.txn_floor);
                if w_target > self.w {
                    self.advance_w(w_target, &mut pending);
                }
            }
            Err(e) => {
                let errno = neg(&e);
                self.failed = Some(errno);
                for p in pending.iter_mut() {
                    if !matches!(p.2, MetaDone::Failed(_)) {
                        p.2 = MetaDone::Failed(errno);
                    }
                }
            }
        }
        out.append(&mut pending);
    }

    /// The highest `t` such that every txn id `<= t` is resolved (E10).
    fn w_target(&self) -> u64 {
        let mut w = self.w;
        // ids of earlier boots: the listed ones are committed; every other one is dead and
        // resolved once each partition reported its records zeroed (§18.5)
        if self.prior_zeroed.len() >= usize::from(self.partitions) {
            if self.boot_floor > w + 1 {
                w = self.boot_floor - 1;
            }
        } else {
            for &t in self.prior.listed() {
                if t == w + 1 && t < self.boot_floor {
                    w = t;
                } else if t > w + 1 {
                    break;
                }
            }
        }
        if w + 1 < self.boot_floor {
            return w;
        }
        // ids of this boot: only ids already handed out can be resolved
        let ids = &self.shared.txn_ids;
        let next = ids.floor().saturating_sub(ids.available());
        while w + 1 < next && self.resolved.contains(&(w + 1)) {
            w += 1;
        }
        w
    }

    /// `resolved_w = w` is durable: forget resolved ids, return their credits (E10).
    fn advance_w(&mut self, w: u64, out: &mut Vec<(u16, u64, MetaDone)>) {
        let old = self.w;
        self.w = w;
        self.shared.resolved_w.store(w, Ordering::Release);
        let mut credits: HashMap<(u16, u64), u32, foldhash::fast::FixedState> = HashMap::with_hasher(Default::default());
        self.resolved.retain(|&t| t > w);
        let owners = core::mem::take(&mut self.owners);
        for (t, o) in owners {
            if t <= w && t > old {
                *credits.entry(o).or_insert(0) += 1;
            } else if t > w {
                self.owners.insert(t, o);
            }
        }
        for ((p, token), n) in credits {
            out.push((p, token, MetaDone::Resolved { credits: n }));
        }
    }

    fn acquire(&mut self, partition: u16, kind: AgKind, want_dev: Option<u8>) -> Option<AgId> {
        if kind == AgKind::Free || partition >= self.partitions {
            return None;
        }
        // fill limit (§11.6): an immediate refusal, never a wait
        if (self.allocated + 1) as f64 > f64::from(self.max_fill) * self.total_ags as f64 {
            return None;
        }
        // the device with the most free AGs; ties by a keyed draw (LC-01, LC-08)
        let counts: SmallVec<[u64; 4]> = self.free.iter().map(|b| b.iter().map(|w| u64::from(w.count_ones())).sum()).collect();
        let mut most = 0u64;
        for &c in &counts {
            if c > most {
                most = c;
            }
        }
        if most == 0 {
            return None;
        }
        let mut tied = [0u64; 1];
        for (d, &c) in counts.iter().enumerate() {
            if c == most {
                tied[0] |= 1 << d;
            }
        }
        if let Some(d) = want_dev {
            if counts.get(usize::from(d)).is_none_or(|&c| c == 0) {
                return None;
            }
            tied[0] = 1 << d;
        }
        let dev = self.pick.pick(DOMAIN_AG_DEVICE | u64::from(partition), &tied)?;
        let idx = self.pick.pick(DOMAIN_AG_PICK | u64::from(partition), &self.free[dev])? as u32;
        self.free[dev][(idx / 64) as usize] &= !(1 << (idx % 64));
        self.allocated += 1;
        let (blk, e) = AgTableBlock::locate(idx);
        let b = &mut self.agt[dev][blk as usize];
        b.v.entries[e] = (kind, partition as u8);
        b.dirty = true;
        Some(AgId { dev: dev as u8, idx })
    }

    fn release(&mut self, ag: AgId) {
        let Some(blocks) = self.agt.get_mut(usize::from(ag.dev)) else { return };
        let (blk, e) = AgTableBlock::locate(ag.idx);
        let Some(b) = blocks.get_mut(blk as usize) else { return };
        if e >= b.v.entries.len() || b.v.entries[e].0 == AgKind::Free {
            return;
        }
        b.v.entries[e] = (AgKind::Free, crate::agtable::OWNER_NONE);
        b.dirty = true;
        self.free[usize::from(ag.dev)][(ag.idx / 64) as usize] |= 1 << (ag.idx % 64);
        self.allocated = self.allocated.saturating_sub(1);
    }

    /// Writes the commit batch into the lowest-index slots with room and flushes (E6).
    fn write_commits(&mut self, io: &mut dyn MetaIo, batch: &[u64]) -> std::io::Result<()> {
        let mut open = [0usize; INTENT_SLOTS];
        for (s, p) in self.slots.iter().enumerate() {
            open[s] = p.open_count(self.w);
        }
        let mut plan = SmallVec::new();
        if !plan_commit(&open, batch.len(), &mut plan) {
            return Err(std::io::Error::other("intent table full"));
        }
        let lba = io.lba(0);
        let l0 = self.layouts[0];
        let mut at = 0usize;
        let mut writes: SmallVec<[(usize, AbCopy, crate::intent::IntentSlot); 2]> = SmallVec::new();
        for &(slot, n) in plan.iter() {
            let ids = &batch[at..at + usize::from(n)];
            at += usize::from(n);
            let pair = &self.slots[usize::from(slot)];
            let (copy, s) = pair.next_copy(self.w, ids).ok_or_else(|| std::io::Error::other("intent slot overflow"))?;
            let mut buf = AlignedBuf::zeroed(lba as usize);
            s.encode_sector(buf.as_mut_slice());
            io.write(0, l0.intent_slot_copy_offset(lba, u32::from(slot), copy), buf)?;
            writes.push((usize::from(slot), copy, s));
        }
        io.flush(0)?;
        for (slot, copy, s) in writes {
            self.slots[slot].store(copy, s);
        }
        Ok(())
    }

    /// Writes every changed block once into its older copy, then flushes each touched device.
    fn write_dirty(&mut self, io: &mut dyn MetaIo) -> std::io::Result<()> {
        let mut touched = [false; crate::layout::MAX_DEVICES];
        let mut page = AlignedBuf::zeroed(PAGE);
        for (d, blocks) in self.agt.iter_mut().enumerate() {
            for (i, b) in blocks.iter_mut().enumerate() {
                if !b.dirty {
                    continue;
                }
                let copy = b.copy.other();
                b.v.encode(b.seq + 1, page.page_mut(0));
                io.write(d as u8, DeviceLayout::ag_table_copy_page(i as u32, copy) * PAGE as u64, page.clone())?;
                b.seq += 1;
                b.copy = copy;
                b.dirty = false;
                touched[d] = true;
            }
        }
        let l0 = self.layouts[0];
        let parts = self.partitions;
        for (p, b) in self.pmeta.iter_mut().enumerate() {
            if !b.dirty {
                continue;
            }
            let copy = b.copy.other();
            b.v.encode(parts, b.seq + 1, page.page_mut(0));
            io.write(0, l0.pmeta_copy_page(p as u16, copy) * PAGE as u64, page.clone())?;
            b.seq += 1;
            b.copy = copy;
            b.dirty = false;
            touched[0] = true;
        }
        if self.nmeta.dirty {
            let copy = self.nmeta.copy.other();
            self.nmeta.v.encode(self.nmeta.seq + 1, page.page_mut(0));
            io.write(0, l0.nmeta_copy_page(copy) * PAGE as u64, page.clone())?;
            self.nmeta.seq += 1;
            self.nmeta.copy = copy;
            self.nmeta.dirty = false;
            touched[0] = true;
        }
        if self.header.dirty {
            let copy = self.header.copy.other();
            self.header.v.encode(self.header.seq + 1, page.page_mut(0));
            io.write(0, l0.intent_header_copy_page(copy) * PAGE as u64, page.clone())?;
            self.header.seq += 1;
            self.header.copy = copy;
            self.header.dirty = false;
            touched[0] = true;
        }
        for (d, t) in touched.iter().enumerate().take(io.devices()) {
            if *t {
                io.flush(d as u8)?;
            }
        }
        Ok(())
    }

    fn write_superblock(&mut self, io: &mut dyn MetaIo, updates: &[SbUpdate]) -> std::io::Result<()> {
        if updates.is_empty() {
            return Ok(());
        }
        let link = self.link.clone();
        let mut cell = link.cell.lock();
        if cell.closed {
            return Ok(());
        }
        let mut sb = cell.sb.clone();
        for u in updates {
            sb.apply(u);
        }
        if sb == cell.sb {
            return Ok(());
        }
        SbLink::write(&mut cell, io, sb)
    }

    /// The device-0 superblock as last written.
    pub fn superblock(&self) -> Superblock {
        self.link.cell.lock().sb.clone()
    }

    /// The partition metas as last written (tests, tools).
    pub fn partition_meta(&self, p: u16) -> Option<PartitionMeta> {
        self.pmeta.get(usize::from(p)).map(|b| b.v.clone())
    }

    /// The first write or flush error, if any (the meta state is then read-only).
    pub fn failed(&self) -> Option<i32> {
        self.failed
    }
}

fn read_slots(io: &mut dyn MetaIo, l0: &DeviceLayout, lba: u32) -> std::io::Result<Vec<SlotPair>> {
    let first = l0.intent_slot_copy_offset(lba, 0, AbCopy::A);
    let last = l0.intent_slot_copy_offset(lba, (INTENT_SLOTS - 1) as u32, AbCopy::B) + u64::from(lba);
    let bytes = io.read(0, first, (last - first) as usize)?;
    Ok(decode_slots(&bytes, first, l0, lba))
}

/// The slot pairs of an intent slot area read from byte `first` of device 0 (§10.7).
pub(crate) fn decode_slots(bytes: &[u8], first: u64, l0: &DeviceLayout, lba: u32) -> Vec<SlotPair> {
    use crate::intent::INTENT_SLOT_COPY_BYTES;
    (0..INTENT_SLOTS as u32)
        .map(|s| {
            let a = (l0.intent_slot_copy_offset(lba, s, AbCopy::A) - first) as usize;
            let b = (l0.intent_slot_copy_offset(lba, s, AbCopy::B) - first) as usize;
            SlotPair::decode(&bytes[a..a + INTENT_SLOT_COPY_BYTES], &bytes[b..b + INTENT_SLOT_COPY_BYTES])
        })
        .collect()
}

fn neg(e: &std::io::Error) -> i32 {
    -e.raw_os_error().unwrap_or(libc::EIO)
}

// ---------------------------------------------------------------------------------------------
// The thread

/// Delivers a meta reply `(partition, token, done)` to one worker.
pub type ReplyHook = Box<dyn Fn(u16, u64, MetaDone) + Send>;

pub struct MetaStore;

impl MetaStore {
    /// Starts the meta thread over the node's devices; `reply[w]` delivers completions to
    /// worker `w` (the worker of partition `p` is `p % reply.len()`).
    pub fn start(devs: Vec<Box<dyn BlockDevice + Send>>, reply: Vec<ReplyHook>) -> (MetaClient, std::thread::JoinHandle<()>) {
        Self::start_with_fill(devs, reply, 0.95)
    }

    /// [`MetaStore::start`] with a fill limit other than the default 0.95 (§11.6).
    pub fn start_with_fill(devs: Vec<Box<dyn BlockDevice + Send>>, reply: Vec<ReplyHook>, max_fill: f32) -> (MetaClient, std::thread::JoinHandle<()>) {
        let mut io = OwnedDevs(devs);
        let now = kv_common::Now::read().unix_ms;
        let core = MetaCore::load(&mut io, now);
        let (tx, rx) = std::sync::mpsc::channel::<MetaMsg>();
        let shared = match &core {
            Ok(c) => c.shared.clone(),
            Err(_) => Arc::new(MetaShared::new(Arc::new(TxnIds::new(1, 1)), 0)),
        };
        let client = MetaClient { tx, shared };
        let handle = std::thread::Builder::new()
            .name("kv-meta".into())
            .spawn(move || {
                let mut core = core;
                if let Ok(c) = core.as_mut() {
                    c.set_max_fill(max_fill);
                }
                let deliver = |out: &mut MetaReplies| {
                    for (p, t, d) in out.drain(..) {
                        if reply.is_empty() {
                            continue;
                        }
                        let w = usize::from(p) % reply.len();
                        (reply[w])(p, t, d);
                    }
                };
                let mut out = Vec::new();
                loop {
                    let first = match rx.recv_timeout(Duration::from_millis(CLOCK_FLOOR_INTERVAL_MS)) {
                        Ok(m) => Some(m),
                        Err(RecvTimeoutError::Timeout) => None,
                        Err(RecvTimeoutError::Disconnected) => break,
                    };
                    let mut msgs: Vec<MetaMsg> = first.into_iter().collect();
                    while let Ok(m) = rx.try_recv() {
                        msgs.push(m);
                    }
                    let now = kv_common::Now::read().unix_ms;
                    match core.as_mut() {
                        Ok(c) => c.process(&mut io, msgs, now, &mut out),
                        Err(e) => {
                            let errno = neg(e);
                            out.extend(msgs.into_iter().map(|m| (m.partition, m.token, MetaDone::Failed(errno))));
                        }
                    }
                    deliver(&mut out);
                }
            })
            .expect("spawn the meta thread");
        (client, handle)
    }
}
