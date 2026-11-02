//! Recovery and restart (DESIGN §18).
//!
//! Clean path (§18.2): when the superblock has CLEAN and SNAPSHOT_VALID and the snapshot header
//! crc matches, each partition reads its section from the snapshot region the superblock names,
//! verifies its crc, bulk-builds its slot tables and hint planes and restores allocator state;
//! any section error makes the whole node take the full scan.
//!
//! Full scan (§18.3, the crash path, correct by construction): per partition, over every AG the
//! AG table assigns to it, in 1 MiB reads: SMALL pages are header-checked and tiling-parsed with
//! crc; dead records are filtered (flush cutoff, uncommitted txn ids above W; expiry is not a
//! dead filter); same-key records of a page keep the higher seq (a marker or an expired record
//! competes like any other); extent headers are validated and crc-checked; fingerprint
//! conflicts are resolved by re-reading and grouping by actual key, losers going to
//! `stale_copies`; expired and marker survivors leave the index, and stay reserved while older
//! copies remain (v1.2); chunkmaps are checked against their chunks; allocator state, hint
//! planes and counters are rebuilt; `next_seq = max(seq_floor, max_seq_seen + 1)`. This is the
//! only module that reads whole AGs (LC-12).
//!
//! Post-recovery (§18.5): stale copies are scrubbed by the paced scrub task (extents: their
//! first sector at once); pages and extents holding records of txns that never committed are
//! zeroed unpaced and then reported to the meta thread so `resolved_w` may pass their ids (E10);
//! absent newest copies are retired once their older copies are gone.

use hashbrown::HashMap;
use kv_common::{KeyHash, key_slot};
use kv_index::{Entry, Kind, SlotTable, VType};
use smallvec::SmallVec;

use crate::agtable::AgKind;
use crate::extent::{self, EXT_KIND_CHUNK, EXT_KIND_CHUNKMAP, EXT_VTYPE_HASH};
use crate::layout::EXT_HDR;
use crate::io::{AlignedBuf, IoDone, IoReq};
use crate::layout::{AG_PAGES, AgId, PAGE, PageId, Seq, class_exact, class_pages};
use crate::page::{PageParse, parse_small_page};
use crate::partition::{IoKind, PartState, PartitionStore, RetireItem, StorageCtx, ZeroWhy};
use crate::record::{REC_KIND_HASH, REC_KIND_TXN_DELETE_MARKER};
use crate::stale::{CopyState, StaleRef};

/// Which restart path a partition takes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RecoveryMode {
    /// Clean-shutdown snapshot (§18.2).
    Snapshot,
    /// Full scan of allocated AGs (§18.3).
    FullScan,
}

/// Progress of `PartitionStore::recover`, driven to completion by the worker.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RecoveryProgress {
    /// I/O outstanding; call again after completions.
    Running,
    /// The partition is Ready.
    Done { bytes_scanned: u64 },
    /// A snapshot section failed verification: the node must restart every partition with
    /// `RecoveryMode::FullScan`.
    SnapshotInvalid,
}

/// Pages per scan read (1 MiB, §18.3).
const CHUNK_PAGES: u32 = 256;
/// Scan reads in flight per partition (`recovery-qd 16` per worker shared by its partitions).
const SCAN_QD: u32 = 4;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Phase {
    SnapRead,
    SnapChunkmaps,
    Scanning,
    Conflicts,
    Finish,
    Done,
    Invalid,
}

/// What a recovery read is for.
enum ReadFor {
    /// Chunk `chunk` of AG number `ag` (index into `ags`).
    Chunk { ag: usize, chunk: u32 },
    /// A conflict candidate's location.
    Conflict { at: PageId, kind: Kind },
    /// A piece of this partition's snapshot section (logical offset inside the section).
    Section { at: u64 },
    /// A chunkmap read for the refcount rebuild after a snapshot restore.
    Chunkmap { start: PageId },
}

/// A survivor that stands for "absent": an expired record or a committed txn marker.
struct Absent {
    db: u8,
    slot: u16,
    kh: KeyHash,
    at: PageId,
    /// Older copies exist: its bytes stay until they are zeroed (retired later).
    reserved: bool,
}

/// One record found for a conflict group.
struct Found {
    key: Box<[u8]>,
    seq: Seq,
    absent: bool,
    at: PageId,
    kind: Kind,
    n_pages: u32,
}

/// State of one partition's recovery.
pub(crate) struct Scan {
    mode: RecoveryMode,
    phase: Phase,
    ags: Vec<(AgId, AgKind)>,
    next_ag: usize,
    next_chunk: u32,
    inflight: u32,
    reads: HashMap<u64, ReadFor, foldhash::fast::FixedState>,
    next_id: u64,
    /// Chunks of EXTENT AGs being assembled (`ag -> chunks`).
    ext_chunks: HashMap<usize, Vec<Option<AlignedBuf>>, foldhash::fast::FixedState>,
    /// `(db, slot, KeyHash bits, KeyHash)` of fingerprint conflicts.
    conflicts: Vec<(u8, u16, u64, KeyHash)>,
    absent: Vec<Absent>,
    chunks: HashMap<PageId, (u32, u32), foldhash::fast::FixedState>,
    /// Chunkmaps found: `(start, refs, db, slot, KeyHash)`.
    chunkmaps: Vec<(PageId, Vec<extent::ChunkRef>, u8, u16, KeyHash)>,
    max_seq: Seq,
    bytes: u64,
    /// Records are expired when `expire_at <= dead_ms` (`max(now, clock_floor)`, §16.4).
    dead_ms: u64,
    /// Conflict resolution: the current group and what its reads found.
    group: usize,
    found: Vec<Found>,
    /// Extents of uncommitted txns to zero after recovery.
    txn_extents: Vec<(PageId, u32)>,
    /// Snapshot path: the section being assembled.
    section: Vec<u8>,
    section_left: u32,
    /// Snapshot path: chunkmaps whose chunk references are read back `(start, pages)`.
    snap_chunkmaps: Vec<(PageId, u32)>,
}

impl Scan {
    fn new(mode: RecoveryMode, ags: Vec<(AgId, AgKind)>, dead_ms: u64) -> Self {
        Scan {
            mode,
            phase: if mode == RecoveryMode::Snapshot { Phase::SnapRead } else { Phase::Scanning },
            ags,
            next_ag: 0,
            next_chunk: 0,
            inflight: 0,
            reads: HashMap::with_hasher(Default::default()),
            next_id: 1,
            ext_chunks: HashMap::with_hasher(Default::default()),
            conflicts: Vec::new(),
            absent: Vec::new(),
            chunks: HashMap::with_hasher(Default::default()),
            chunkmaps: Vec::new(),
            max_seq: Seq::ZERO,
            bytes: 0,
            dead_ms,
            group: 0,
            found: Vec::new(),
            txn_extents: Vec::new(),
            section: Vec::new(),
            section_left: 0,
            snap_chunkmaps: Vec::new(),
        }
    }
}

/// Issues a read for the scan.
fn read(p: &mut PartitionStore, cx: &mut StorageCtx<'_>, scan: &mut Scan, what: ReadFor, dev: u8, offset: u64, len: usize) {
    let id = scan.next_id;
    scan.next_id += 1;
    scan.reads.insert(id, what);
    scan.inflight += 1;
    let tag = p.core.tag(IoKind::Recovery { id });
    cx.io.push(IoReq::read(tag, dev, offset, AlignedBuf::zeroed(len)));
}

/// Drives recovery of `p` (called repeatedly by the worker while it returns `Running`).
pub(crate) fn drive(p: &mut PartitionStore, cx: &mut StorageCtx<'_>, mode: RecoveryMode) -> RecoveryProgress {
    if p.core.state != PartState::Loading {
        return RecoveryProgress::Done { bytes_scanned: p.core.recovered_bytes };
    }
    let mut scan = match p.recovery.take() {
        Some(s) if s.mode == mode => s,
        _ => {
            reset(p);
            let ags = p.core.boot.owned.get(usize::from(p.core.cfg.id)).cloned().unwrap_or_default();
            let dead_ms = crate::expire::recovery_now(cx.now.unix_ms, p.core.boot.clock_floor_ms);
            p.core.recovery_mode = Some(mode);
            let mut s = Box::new(Scan::new(mode, ags, dead_ms));
            if mode == RecoveryMode::FullScan {
                for &(ag, kind) in &s.ags {
                    match kind {
                        AgKind::Small => {
                            p.core.alloc.add_small_ag(ag);
                        }
                        AgKind::Extent => {
                            p.core.alloc.add_extent_ag(ag);
                        }
                        AgKind::Free => {}
                    }
                }
            } else if !start_snapshot(p, cx, &mut s) {
                s.phase = Phase::Invalid;
            }
            s
        }
    };
    let res = advance(p, cx, &mut scan);
    if matches!(res, RecoveryProgress::Running) {
        p.recovery = Some(scan);
    }
    res
}

/// Clears everything a failed snapshot attempt may have restored.
fn reset(p: &mut PartitionStore) {
    let cfg = p.core.cfg;
    p.core.index = kv_index::PartitionIndex::new(cfg.id, cfg.partitions, cfg.databases.max(1));
    p.core.alloc = crate::alloc::Alloc::new(
        cfg.id,
        p.core.boot.layouts.clone(),
        crate::alloc::KeyedPick::new(cfg.hash_seed, cfg.boot_gen),
        cfg.page_update,
        cfg.page_slack_cap,
        cfg.page_slack_atomic,
    );
    p.core.stale.clear();
    p.core.ext = Default::default();
    p.core.retire.clear();
    p.core.prior_txn_pages.clear();
    p.core.prior_txn_zero = Some(0);
}

fn advance(p: &mut PartitionStore, cx: &mut StorageCtx<'_>, scan: &mut Scan) -> RecoveryProgress {
    loop {
        match scan.phase {
            Phase::Invalid => return RecoveryProgress::SnapshotInvalid,
            Phase::Done => return RecoveryProgress::Done { bytes_scanned: scan.bytes },
            Phase::SnapRead => {
                if scan.section_left > 0 {
                    return RecoveryProgress::Running;
                }
                if !finish_snapshot(p, cx, scan) {
                    scan.phase = Phase::Invalid;
                    continue;
                }
                scan.phase = Phase::SnapChunkmaps;
            }
            Phase::SnapChunkmaps => {
                if scan.inflight > 0 {
                    return RecoveryProgress::Running;
                }
                if let Some((start, n)) = scan.snap_chunkmaps.pop() {
                    read(p, cx, scan, ReadFor::Chunkmap { start }, start.dev(), start.byte_offset(), n as usize * PAGE);
                    return RecoveryProgress::Running;
                }
                scan.phase = Phase::Finish;
            }
            Phase::Scanning => {
                issue_scan_reads(p, cx, scan);
                if scan.inflight > 0 || scan.next_ag < scan.ags.len() || !scan.ext_chunks.is_empty() {
                    return RecoveryProgress::Running;
                }
                scan.conflicts.sort_unstable_by_key(|c| (c.0, c.1, c.2));
                scan.conflicts.dedup_by_key(|c| (c.0, c.1, c.2));
                scan.group = 0;
                scan.phase = Phase::Conflicts;
                if !start_group(p, cx, scan) {
                    continue;
                }
                return RecoveryProgress::Running;
            }
            Phase::Conflicts => {
                if scan.inflight > 0 {
                    return RecoveryProgress::Running;
                }
                if scan.group < scan.conflicts.len() {
                    resolve_group(p, scan);
                    scan.group += 1;
                }
                if scan.group < scan.conflicts.len() {
                    start_group(p, cx, scan);
                    continue;
                }
                finish_scan(p, cx, scan);
                scan.phase = Phase::Finish;
            }
            Phase::Finish => {
                post_recovery(p, cx, scan);
                p.core.recovered_bytes = scan.bytes;
                p.become_ready(cx);
                scan.phase = Phase::Done;
            }
        }
    }
}

/// A read of the scan completed.
pub(crate) fn on_read_done(p: &mut PartitionStore, cx: &mut StorageCtx<'_>, id: u64, d: IoDone) {
    let Some(mut scan) = p.recovery.take() else { return };
    if let Some(what) = scan.reads.remove(&id) {
        scan.inflight = scan.inflight.saturating_sub(1);
        match d.res {
            Err(_) => {
                // an unreadable chunk holds nothing usable; a snapshot read fails the snapshot
                match what {
                    ReadFor::Section { .. } | ReadFor::Chunkmap { .. } => scan.phase = Phase::Invalid,
                    ReadFor::Chunk { ag, .. } => {
                        if let Some(v) = scan.ext_chunks.get_mut(&ag) {
                            v.clear();
                            scan.ext_chunks.remove(&ag);
                        }
                    }
                    _ => {}
                }
            }
            Ok(_) => {
                if let Some(buf) = d.buf {
                    scan.bytes += buf.len() as u64;
                    on_data(p, cx, &mut scan, what, buf);
                }
            }
        }
    }
    p.recovery = Some(scan);
}

fn on_data(p: &mut PartitionStore, cx: &mut StorageCtx<'_>, scan: &mut Scan, what: ReadFor, buf: AlignedBuf) {
    match what {
        ReadFor::Chunk { ag, chunk } => {
            let (agid, kind) = scan.ags[ag];
            match kind {
                AgKind::Small => {
                    for i in 0..CHUNK_PAGES {
                        let pi = chunk * CHUNK_PAGES + i;
                        let Some(page) = p.core.alloc.ag_page(agid, pi) else { continue };
                        scan_small_page(p, scan, page, buf.page(i as usize));
                    }
                }
                AgKind::Extent => {
                    let n = (AG_PAGES / CHUNK_PAGES) as usize;
                    let v = scan.ext_chunks.entry(ag).or_insert_with(|| (0..n).map(|_| None).collect());
                    if v.len() == n {
                        v[chunk as usize] = Some(buf);
                    }
                    if v.len() == n && v.iter().all(Option::is_some) {
                        let v = scan.ext_chunks.remove(&ag).expect("present");
                        scan_extent_ag(p, cx, scan, agid, &v);
                    }
                }
                AgKind::Free => {}
            }
        }
        ReadFor::Conflict { at, kind } => collect_found(p, scan, at, kind, &buf),
        ReadFor::Section { at } => {
            let at = at as usize;
            let end = (at + buf.len()).min(scan.section.len());
            if at < end {
                scan.section[at..end].copy_from_slice(&buf[..end - at]);
            }
            scan.section_left = scan.section_left.saturating_sub(1);
        }
        ReadFor::Chunkmap { start } => {
            let crc = p.core.crc();
            let refs = extent::parse_extent_header(&buf, start)
                .filter(|h| h.kind == EXT_KIND_CHUNKMAP && extent::verify_extent(&buf, h, crc))
                .and_then(|h| extent::chunkmap_parts(&buf, &h).and_then(|(_, body)| extent::decode_chunkmap(body)));
            match refs {
                Some(refs) => {
                    let list: SmallVec<[(PageId, u32); 4]> = refs.iter().map(|c| (c.page, c.n_pages)).collect();
                    for &(c, _) in &list {
                        *p.core.ext.chunk_rc.entry(c).or_insert(0) += 1;
                    }
                    p.core.ext.chunkmaps.insert(start, list);
                }
                None => p.core.counters.corrupt_pages += 1,
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Full scan

fn issue_scan_reads(p: &mut PartitionStore, cx: &mut StorageCtx<'_>, scan: &mut Scan) {
    while scan.inflight < SCAN_QD && scan.next_ag < scan.ags.len() {
        let (ag, _) = scan.ags[scan.next_ag];
        let Some(first) = p.core.alloc.ag_page(ag, scan.next_chunk * CHUNK_PAGES) else {
            scan.next_ag += 1;
            scan.next_chunk = 0;
            continue;
        };
        let what = ReadFor::Chunk { ag: scan.next_ag, chunk: scan.next_chunk };
        read(p, cx, scan, what, first.dev(), first.byte_offset(), (CHUNK_PAGES as usize) * PAGE);
        scan.next_chunk += 1;
        if scan.next_chunk * CHUNK_PAGES >= AG_PAGES {
            scan.next_ag += 1;
            scan.next_chunk = 0;
        }
    }
}

/// True when the record is dead for recovery: below its db's flush cutoff, or of a txn that
/// never committed (§18.3 step 2; expiry is not part of this filter).
fn dead(p: &PartitionStore, db: u8, seq: Seq, txn: u64) -> bool {
    if p.core.cutoff.get(usize::from(db)).is_some_and(|&c| seq.0 < c) {
        return true;
    }
    txn != 0 && !p.core.prior.is_committed(txn)
}

/// Inserts a survivor into its table, noting fingerprint conflicts.
#[allow(clippy::too_many_arguments)]
fn insert_survivor(p: &mut PartitionStore, scan: &mut Scan, db: u8, slot: u16, kh: KeyHash, e: Entry, expire_at: Option<u64>, absent: bool) {
    let t = p.core.index.table_mut(db, slot);
    let mut c = SmallVec::new();
    t.candidates(kh, &mut c);
    if !c.is_empty() {
        scan.conflicts.push((db, slot, kh.raw(), kh));
    }
    let hint = kv_index::hint::encode(expire_at, p.core.expiry.hint_base_s);
    if t.try_insert(kh, e, hint).is_err() {
        p.core.counters.unexpected_orphans += 1;
        return;
    }
    if absent {
        scan.absent.push(Absent { db, slot, kh, at: PageId::from36(e.page36), reserved: false });
    }
}

fn scan_small_page(p: &mut PartitionStore, scan: &mut Scan, page: PageId, buf: &[u8; PAGE]) {
    let part = p.core.cfg.id;
    let parts = p.core.cfg.partitions;
    let dbs = p.core.cfg.databases;
    let crc = p.core.crc();
    let seed = p.core.cfg.hash_seed;
    let mut views = Vec::new();
    if parse_small_page(buf, page, part, crc, &mut views) != PageParse::Ok {
        // foreign or never written: holds nothing for this partition
        p.core.alloc.recovered_page(page, 0);
        return;
    }
    let mut occupied = 0u32;
    let mut done = vec![false; views.len()];
    for i in 0..views.len() {
        if done[i] {
            continue;
        }
        let (db, h32, key) = (views[i].hdr.db, views[i].hdr.h32, views[i].key);
        let mut best: Option<usize> = None;
        // valid records of the key in this page (an older one sits next to the newest after a
        // two-image update whose follow-up image was not written)
        let mut copies = 0u32;
        for j in i..views.len() {
            let v = &views[j];
            if done[j] || v.hdr.db != db || v.hdr.h32 != h32 || v.key != key {
                continue;
            }
            done[j] = true;
            if dead(p, v.hdr.db, v.hdr.seq, v.hdr.txn.unwrap_or(0)) {
                if v.hdr.txn.is_some() {
                    // an uncommitted txn's record: zeroed before `resolved_w` passes it (E10)
                    p.core.prior_txn_pages.insert(page);
                }
                continue;
            }
            occupied += u32::from(v.len);
            copies += 1;
            if best.is_none_or(|b| v.hdr.seq > views[b].hdr.seq) {
                best = Some(j);
            }
        }
        let Some(b) = best else { continue };
        let v = &views[b];
        let slot = key_slot(key);
        if slot % parts != part || db >= dbs {
            continue;
        }
        let kh = KeyHash::of(seed, key);
        if v.hdr.seq > scan.max_seq {
            scan.max_seq = v.hdr.seq;
        }
        let marker = v.hdr.kind == REC_KIND_TXN_DELETE_MARKER;
        let expired = crate::expire::is_expired(v.hdr.expire_at(), scan.dead_ms);
        let vtype = if v.hdr.kind == REC_KIND_HASH { VType::Hash } else { VType::String };
        let e = Entry { fp32: kh.fp32(), x8: kh.x8(), page36: page.to36(), kind: Kind::Small, vtype, sync_mark: false };
        let absent = marker || expired;
        insert_survivor(p, scan, db, slot, kh, e, v.hdr.expire_at(), absent);
        if absent && copies > 1 {
            // an absent newest copy (expired, or a committed marker) with an older valid copy
            // of the key next to it: the key's newest copy on media until the older one is
            // zeroed (D1, v1.2), as for older copies elsewhere (`resolve_group`). Left
            // unrecorded, nothing zeroed the older copy: a later write of the key made a third
            // copy (E14 never counted the two here) and the page kept both for good
            p.core.stale.note_absent(db, key, page, v.hdr.seq, 0);
            p.core.retire.push(RetireItem { db, key: key.into(), page, seq: v.hdr.seq });
            if let Some(a) = scan.absent.last_mut()
                && a.at == page
                && a.db == db
                && a.kh == kh
            {
                a.reserved = true;
            }
        }
    }
    p.core.alloc.recovered_page(page, occupied.min(u32::from(crate::alloc::PAGE_ROOM)) as u16);
}

fn scan_extent_ag(p: &mut PartitionStore, cx: &mut StorageCtx<'_>, scan: &mut Scan, ag: AgId, chunks: &[Option<AlignedBuf>]) {
    let crc = p.core.crc();
    let page_bytes = |i: u32| -> &[u8] {
        let c = chunks[(i / CHUNK_PAGES) as usize].as_ref().expect("chunk present");
        let o = (i % CHUNK_PAGES) as usize * PAGE;
        &c[o..]
    };
    let mut i = 0u32;
    while i < AG_PAGES {
        let Some(start) = p.core.alloc.ag_page(ag, i) else { break };
        let first = page_bytes(i);
        let Some(h) = extent::parse_extent_header(&first[..PAGE], start) else {
            i += 1;
            continue;
        };
        let n = h.n_pages;
        if i + n > AG_PAGES {
            i += 1;
            continue;
        }
        let ci = i / CHUNK_PAGES;
        let fits = (i + n).div_ceil(CHUNK_PAGES) - 1 == ci;
        if fits {
            let bytes = &first[..n as usize * PAGE];
            if extent::verify_extent(bytes, &h, crc) {
                accept_extent(p, scan, start, &h, bytes);
                // only the pages the checksum covers: the run's tail may hold a later write
                // if this extent is dead (its run was freed and partly reused)
                i += extent::covered_pages(bytes, &h);
                continue;
            }
            i += 1;
            continue;
        }
        // spans chunks: assemble a copy
        let mut whole = AlignedBuf::zeroed(n as usize * PAGE);
        for k in 0..n {
            whole.page_mut(k as usize).copy_from_slice(&page_bytes(i + k)[..PAGE]);
        }
        if extent::verify_extent(&whole, &h, crc) {
            accept_extent(p, scan, start, &h, &whole);
            i += extent::covered_pages(&whole, &h);
        } else {
            i += 1;
        }
    }
    let _ = cx;
}

/// A verified extent found by the scan.
fn accept_extent(p: &mut PartitionStore, scan: &mut Scan, start: PageId, h: &extent::ExtHeader, buf: &[u8]) {
    let n = h.n_pages;
    if h.kind == EXT_KIND_CHUNK {
        if let Some(c) = extent::stored_crc(buf) {
            scan.chunks.insert(start, (n, c));
        }
        return;
    }
    let kl = h.key_len as usize;
    let Some(key) = buf.get(EXT_HDR..EXT_HDR + kl) else { return };
    if dead(p, h.db, h.seq, h.txn) {
        if h.txn != 0 {
            // the run stays allocated until its first sector is zeroed (E10)
            p.core.alloc.mark_extent(start, n);
            scan.txn_extents.push((start, n));
        }
        return;
    }
    let slot = key_slot(key);
    if slot % p.core.cfg.partitions != p.core.cfg.id || h.db >= p.core.cfg.databases {
        return;
    }
    let seed = p.core.cfg.hash_seed;
    let kh = KeyHash::of(seed, key);
    if h.seq > scan.max_seq {
        scan.max_seq = h.seq;
    }
    p.core.alloc.mark_extent(start, n);
    let kind = if h.kind == EXT_KIND_CHUNKMAP { Kind::ChunkMap } else { Kind::Extent };
    if kind == Kind::ChunkMap
        && let Some(refs) = extent::chunkmap_parts(buf, h).and_then(|(_, body)| extent::decode_chunkmap(body))
    {
        scan.chunkmaps.push((start, refs, h.db, slot, kh));
    }
    let vtype = if h.vtype == EXT_VTYPE_HASH { VType::Hash } else { VType::String };
    let class = class_exact(n).unwrap_or(0);
    let e = Entry { fp32: kh.fp32(), x8: class, page36: start.to36(), kind, vtype, sync_mark: false };
    let expired = crate::expire::is_expired(h.expire_at(), scan.dead_ms);
    insert_survivor(p, scan, h.db, slot, kh, e, h.expire_at(), expired);
}

// ---------------------------------------------------------------------------------------------
// Conflicts (§18.3 step 4)

/// Issues the reads of conflict group `scan.group`; false when it needs none.
fn start_group(p: &mut PartitionStore, cx: &mut StorageCtx<'_>, scan: &mut Scan) -> bool {
    scan.found.clear();
    let Some(&(db, slot, _, kh)) = scan.conflicts.get(scan.group) else { return false };
    let Some(t) = p.core.index.table(db, slot) else { return false };
    let mut c = SmallVec::new();
    t.candidates(kh, &mut c);
    let locs: SmallVec<[(PageId, Kind, u8); 4]> = c.iter().map(|c| (PageId::from36(c.entry.page36), c.entry.kind, c.entry.x8)).collect();
    let mut issued = false;
    for (at, kind, x8) in locs {
        let pages = if kind == Kind::Small { 1 } else { class_pages(x8) };
        read(p, cx, scan, ReadFor::Conflict { at, kind }, at.dev(), at.byte_offset(), pages as usize * PAGE);
        issued = true;
    }
    issued
}

/// Collects the records of the current conflict group found at `at`.
fn collect_found(p: &mut PartitionStore, scan: &mut Scan, at: PageId, kind: Kind, buf: &[u8]) {
    let Some(&(db, _, _, kh0)) = scan.conflicts.get(scan.group) else { return };
    let seed = p.core.cfg.hash_seed;
    let crc = p.core.crc();
    match kind {
        Kind::Small => {
            let Ok(page): Result<&[u8; PAGE], _> = buf[..PAGE].try_into() else { return };
            let mut views = Vec::new();
            if parse_small_page(page, at, p.core.cfg.id, crc, &mut views) != PageParse::Ok {
                return;
            }
            for v in &views {
                if v.hdr.db != db || dead(p, v.hdr.db, v.hdr.seq, v.hdr.txn.unwrap_or(0)) {
                    continue;
                }
                let kh = KeyHash::of(seed, v.key);
                if kh.fp32() != kh0.fp32() || kh.x8() != kh0.x8() {
                    continue;
                }
                let absent = v.hdr.kind == REC_KIND_TXN_DELETE_MARKER || crate::expire::is_expired(v.hdr.expire_at(), scan.dead_ms);
                let f = Found { key: v.key.into(), seq: v.hdr.seq, absent, at, kind, n_pages: 1 };
                // keep the newest record of a key in this page
                match scan.found.iter_mut().find(|x| x.at == at && x.key == f.key) {
                    Some(x) if x.seq < f.seq => *x = f,
                    Some(_) => {}
                    None => scan.found.push(f),
                }
            }
        }
        _ => {
            let Some(h) = extent::parse_extent_header(buf, at) else { return };
            if h.kind == EXT_KIND_CHUNK || h.db != db || dead(p, h.db, h.seq, h.txn) || !extent::verify_extent(buf, &h, crc) {
                return;
            }
            let Some(key) = buf.get(EXT_HDR..EXT_HDR + h.key_len as usize) else { return };
            let kh = KeyHash::of(seed, key);
            if kh.fp32() != kh0.fp32() {
                return;
            }
            let absent = crate::expire::is_expired(h.expire_at(), scan.dead_ms);
            scan.found.push(Found { key: key.into(), seq: h.seq, absent, at, kind, n_pages: h.n_pages });
        }
    }
}

/// Groups the current conflict group's records by actual key: the max seq wins, the losers
/// leave the index and become tracked stale copies; an absent winner with losers stays
/// reserved until they are zeroed (v1.2).
fn resolve_group(p: &mut PartitionStore, scan: &mut Scan) {
    let Some(&(db, slot, _, _)) = scan.conflicts.get(scan.group) else { return };
    let seed = p.core.cfg.hash_seed;
    let mut found = core::mem::take(&mut scan.found);
    found.sort_by(|a, b| a.key.cmp(&b.key).then(b.seq.cmp(&a.seq)));
    let mut i = 0;
    while i < found.len() {
        let mut j = i + 1;
        while j < found.len() && found[j].key == found[i].key {
            j += 1;
        }
        let kh = KeyHash::of(seed, &found[i].key);
        let winner = &found[i];
        for loser in &found[i + 1..j] {
            p.core.index_remove_at(db, slot, kh, loser.at.to36());
            let at = match loser.kind {
                Kind::Small => StaleRef::Small { page: loser.at },
                _ => StaleRef::Extent { page: loser.at, n_pages: loser.n_pages },
            };
            p.core.stale.add(db, &found[i].key, at, CopyState::Stale, loser.seq);
        }
        if winner.absent && j > i + 1 {
            // the key is absent: its entry goes in step 4a; its bytes stay until the losers
            // are durably zeroed
            match winner.kind {
                Kind::Small => {
                    p.core.stale.note_absent(db, &winner.key, winner.at, winner.seq, 0);
                    p.core.retire.push(RetireItem { db, key: winner.key.clone(), page: winner.at, seq: winner.seq });
                }
                _ => {
                    p.core.ext.zeros.insert(
                        winner.at,
                        crate::partition::ExtZero::pending(winner.n_pages, ZeroWhy::Delete { db, key: winner.key.clone(), tickets: SmallVec::new() }),
                    );
                }
            }
            for a in scan.absent.iter_mut() {
                if a.at == winner.at && a.db == db && a.kh == kh {
                    a.reserved = true;
                }
            }
        }
        i = j;
    }
}

/// Step 4a and 5: absent survivors leave the index; chunkmaps take their chunks.
fn finish_scan(p: &mut PartitionStore, cx: &mut StorageCtx<'_>, scan: &mut Scan) {
    for a in core::mem::take(&mut scan.absent) {
        let present = p.core.indexed_at(a.db, a.slot, a.kh, a.at.to36());
        if present {
            let kind = p.core.index.table(a.db, a.slot).and_then(|t| {
                let mut c = SmallVec::new();
                t.candidates(a.kh, &mut c);
                c.iter().find(|c| c.entry.page36 == a.at.to36()).map(|c| (c.entry.kind, c.entry.x8))
            });
            p.core.index_remove_at(a.db, a.slot, a.kh, a.at.to36());
            // an absent (expired) extent with no older copy: its run is free once its first
            // sector is zeroed. Freed with its header intact, a later recovery found it again:
            // as a loser of the key's next version it was marked over whatever had been
            // written into its run's tail meanwhile, and freed with it once zeroed
            if let Some((Kind::Extent | Kind::ChunkMap, x8)) = kind
                && !a.reserved
                && !p.core.ext.zeros.contains_key(&a.at)
            {
                p.core.ext.zeros.insert(a.at, crate::partition::ExtZero::pending(class_pages(x8), ZeroWhy::Dead));
            }
        }
    }
    // step 5: the indexed (current) chunkmap of a key takes its chunks; a missing chunk is
    // corruption. A chunkmap that lost to a newer copy, or an absent one kept until its older
    // copies are zeroed, is never current again (it loses to the winner, and D1 keeps the winner
    // until it is gone): its chunks are free space. They must not be taken: their runs may have
    // been free and reused since that chunkmap was written (a crash before its commit or
    // zeroing), and taking them marked, and once the chunkmap was zeroed freed, the run of a
    // live extent written there (it was then given out again while still in use)
    for (start, refs, db, slot, kh) in core::mem::take(&mut scan.chunkmaps) {
        if !p.core.indexed_at(db, slot, kh, start.to36()) {
            continue;
        }
        let mut list: SmallVec<[(PageId, u32); 4]> = SmallVec::new();
        for c in &refs {
            match scan.chunks.get(&c.page) {
                Some(&(n, crc)) if n == c.n_pages && crc == c.crc => {}
                _ => {
                    // not on media as written: nothing of it to hold (its run may be in use)
                    p.core.counters.corrupt_pages += 1;
                    continue;
                }
            }
            if !p.core.alloc.extent_allocated(c.page, c.n_pages) {
                p.core.alloc.mark_extent(c.page, c.n_pages);
            }
            *p.core.ext.chunk_rc.entry(c.page).or_insert(0) += 1;
            list.push((c.page, c.n_pages));
        }
        p.core.ext.chunkmaps.insert(start, list);
    }
    let _ = cx;
}

/// §18.5: seq, scrubs of stale copies, zeroing of uncommitted txn records, retirements.
fn post_recovery(p: &mut PartitionStore, cx: &mut StorageCtx<'_>, scan: &mut Scan) {
    let floor = p.core.pmeta.seq_floor;
    let next = scan.max_seq.next().unwrap_or(scan.max_seq);
    p.core.next_seq = floor.max(next).max(Seq::new(0, 1));
    if scan.mode == RecoveryMode::FullScan {
        let entries: Vec<(u8, Box<[u8]>, StaleRef)> = p.core.stale.iter().map(|(db, k, e)| (db, Box::<[u8]>::from(k), e.at)).collect();
        for (db, key, at) in entries {
            match at {
                StaleRef::Small { page } => p.core.scrub_queue.push_back(page),
                StaleRef::Extent { page, n_pages } => {
                    p.core.ext.zeros.entry(page).or_insert(crate::partition::ExtZero::pending(n_pages, ZeroWhy::Stale { db, key }));
                }
            }
        }
    }
    // records of txns that never committed: zeroed unpaced, then reported (E10)
    let pages: Vec<PageId> = p.core.prior_txn_pages.iter().copied().collect();
    let mut pending = pages.len() as u32;
    for page in pages {
        p.load_page(cx, page, None, true);
    }
    for (start, n) in core::mem::take(&mut scan.txn_extents) {
        p.core.prior_txn_pages.insert(start);
        p.core.ext.zeros.entry(start).or_insert(crate::partition::ExtZero::pending(n, ZeroWhy::Dead));
        pending += 1;
    }
    p.core.prior_txn_zero = Some(pending);
}

// ---------------------------------------------------------------------------------------------
// Clean path (§18.2)

fn start_snapshot(p: &mut PartitionStore, cx: &mut StorageCtx<'_>, scan: &mut Scan) -> bool {
    let Some(dir) = p.core.boot.snapshot.clone() else { return false };
    let Some((off, len, _)) = dir.section(p.core.cfg.id) else { return false };
    if len == 0 || len > (1 << 40) {
        return false;
    }
    scan.section = vec![0u8; len as usize];
    let aligned = len.next_multiple_of(PAGE as u64);
    let spans = crate::snapshot::region_spans(&p.core.boot.layouts, dir.region, off, aligned);
    let mut total = 0u64;
    for (dev, dev_off, span_len, at) in spans {
        let mut done = 0u64;
        while done < span_len {
            let n = (span_len - done).min(1 << 20);
            read(p, cx, scan, ReadFor::Section { at: at + done }, dev, dev_off + done, n as usize);
            scan.section_left += 1;
            done += n;
        }
        total += span_len;
    }
    total == aligned
}

fn finish_snapshot(p: &mut PartitionStore, cx: &mut StorageCtx<'_>, scan: &mut Scan) -> bool {
    let Some(dir) = p.core.boot.snapshot.clone() else { return false };
    let Some((_, len, crc)) = dir.section(p.core.cfg.id) else { return false };
    if scan.section.len() as u64 != len || kv_common::crc::crc32c(&scan.section) != crc {
        return false;
    }
    let Some(sec) = crate::snapshot::decode_section(&scan.section, p.core.cfg.id, p.core.cfg.databases) else { return false };
    scan.bytes = len;
    scan.section = Vec::new();
    // the section's AGs must be exactly the ones the AG table assigns (spares may be extra)
    let owned = scan.ags.clone();
    for (ag, _) in &sec.small {
        if !owned.iter().any(|(a, k)| a == ag && *k == AgKind::Small) {
            return false;
        }
    }
    for (ag, _, _) in &sec.extent {
        if !owned.iter().any(|(a, k)| a == ag && *k == AgKind::Extent) {
            return false;
        }
    }
    for &(ag, kind) in &owned {
        match kind {
            AgKind::Small => {
                p.core.alloc.add_small_ag(ag);
            }
            AgKind::Extent => {
                p.core.alloc.add_extent_ag(ag);
            }
            AgKind::Free => {}
        }
    }
    for (ag, pf) in &sec.small {
        p.core.alloc.set_small_free(*ag, pf);
    }
    for (ag, a, s) in sec.extent {
        p.core.alloc.set_extent_bits(ag, a, s);
    }
    p.core.expiry.hint_base_s = sec.hint_base_s;
    for (db, slot, entries) in sec.tables {
        if slot % p.core.cfg.partitions != p.core.cfg.id {
            return false;
        }
        for (e, _) in &entries {
            if e.kind == Kind::ChunkMap {
                scan.snap_chunkmaps.push((PageId::from36(e.page36), class_pages(e.x8)));
            }
        }
        let Ok(t) = SlotTable::try_bulk_build(entries) else { return false };
        *p.core.index.table_mut(db, slot) = t;
    }
    scan.max_seq = sec.next_seq.counter().checked_sub(1).map_or(Seq::ZERO, |c| Seq::new(sec.next_seq.term(), c));
    let _ = (cx, sec.partition);
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_geometry() {
        assert_eq!(AG_PAGES % CHUNK_PAGES, 0);
        assert_eq!(CHUNK_PAGES as usize * PAGE, 1 << 20);
    }
}
