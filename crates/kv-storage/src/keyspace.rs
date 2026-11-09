//! The command-facing storage API (DESIGN §6.5, §14, §15, §16, §23.7).
//!
//! A [`Keyspace`] is handed to a command's `exec` closure for one partition request. Every
//! method is staged and committed only if `exec` returns `Ok`, so commands are re-runnable when
//! `exec` needs a page that was not prefetched (`StoreError::NeedMore`, §6.5). Lookups verify the
//! full key against the bytes read from the page or extent (LC-07); a key whose
//! `expire_at <= now_ms` is treated as absent (lazy expiry, §16.4). Members apply replicated
//! effects through [`Keyspace::apply_replicated`]: `Ordered` skips an op whose key already has a
//! stored seq >= the op's seq; `Image` installs a repair image; `SnapshotMerge` confirms an
//! identical record (sets its `sync_mark`, writes nothing) or writes the differing one (§23.7).
//!
//! Re-runs: when `exec` returns [`crate::partition::ExecStatus::Requeued`] the closure ran
//! against a state it could not finish with (a page or extent was missing, or the commit has to
//! wait for space or a sequence reservation); everything it staged was discarded and anything it
//! produced on the side (reply bytes) must be discarded by the caller, which runs it again once
//! the request is ready.

use kv_common::{KeyHash, key_slot};
use kv_index::{Kind, VType};
use std::sync::Arc;

use smallvec::SmallVec;

use crate::cache::{FrameIdx, PageCache};
use crate::effect::Op;
use crate::extent::{self, EXT_KIND_CHUNKMAP, EXT_KIND_VALUE, EXT_VTYPE_HASH, EXT_VTYPE_STRING};
use crate::layout::EXT_HDR;
use crate::image::{Pending, RecDesc, RecStatus};
use crate::io::AlignedBuf;
use crate::layout::{PAGE, PageId, Seq, class_pages};
use crate::partition::Core;
use crate::record::{REC_KIND_HASH, REC_KIND_TXN_DELETE_MARKER};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StoreError {
    /// A page that was not prefetched is needed; the partition loads it and re-runs `exec`.
    NeedMore,
    /// `-OOM` at the fill limit (§11.6).
    OutOfSpace,
    /// Value beyond 512 MiB (or `hash-max-bytes`).
    TooLarge,
    WrongType,
    /// `-ERR storage corruption detected` (§15); names the page only.
    Corrupt(PageId),
    /// `-ERR storage I/O error` (§22).
    Io(i32),
    /// The partition is read-only after a write error (§22).
    ReadOnly,
    /// `-LOADING` (§18.1).
    Loading,
}

impl core::fmt::Display for StoreError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            StoreError::NeedMore => f.write_str("data not loaded"),
            StoreError::OutOfSpace => f.write_str("storage full"),
            StoreError::TooLarge => f.write_str("value too large"),
            StoreError::WrongType => f.write_str("wrong type"),
            StoreError::Corrupt(p) => write!(f, "storage corruption detected (page {:#x})", p.0),
            StoreError::Io(e) => write!(f, "storage I/O error ({e})"),
            StoreError::ReadOnly => f.write_str("partition is read-only"),
            StoreError::Loading => f.write_str("loading"),
        }
    }
}

impl std::error::Error for StoreError {}

/// Largest value (`proto-max-bulk-len` default, §1.3).
pub const MAX_VALUE: u64 = 512 << 20;

/// Header-level facts about a key.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct KeyMeta {
    pub vtype: VType,
    pub expire_at_ms: Option<u64>,
    pub len: u64,
}

/// A value: contiguous (small record or VALUE extent) or chunked (CHUNKMAP). Holds user bytes:
/// no `Debug` (LC-32).
pub enum Value<'a> {
    Contig(&'a [u8]),
    Chunks(SmallVec<[&'a [u8]; 4]>),
}

impl<'a> Value<'a> {
    pub fn len(&self) -> u64 {
        match self {
            Value::Contig(b) => b.len() as u64,
            Value::Chunks(cs) => cs.iter().map(|c| c.len() as u64).sum(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn to_vec(&self) -> Vec<u8> {
        match self {
            Value::Contig(b) => b.to_vec(),
            Value::Chunks(cs) => cs.concat(),
        }
    }

    /// Bytes `[start, end)` of the value (clamped by the caller to the value length).
    pub fn slice(&self, start: u64, end: u64) -> Vec<u8> {
        let end = end.min(self.len());
        if start >= end {
            return Vec::new();
        }
        match self {
            Value::Contig(b) => b[start as usize..end as usize].to_vec(),
            Value::Chunks(cs) => {
                let mut out = Vec::with_capacity((end - start) as usize);
                let mut at = 0u64;
                for c in cs {
                    let (cs, ce) = (at, at + c.len() as u64);
                    if ce > start && cs < end {
                        let a = start.max(cs) - cs;
                        let b = end.min(ce) - cs;
                        out.extend_from_slice(&c[a as usize..b as usize]);
                    }
                    at = ce;
                    if at >= end {
                        break;
                    }
                }
                out
            }
        }
    }
}

/// How a replicated op is applied on a member.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ApplyMode {
    /// skip if stored seq >= op seq
    Ordered,
    Image,
    /// §23.7: identical -> mark only
    SnapshotMerge,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ApplyOutcome {
    Applied,
    Skipped,
    /// identical record kept, sync_mark set, nothing written
    Confirmed,
}

// -------------------------------------------------------------------------------------------
// Staging

/// Where a key's current version lives (as found by the lookup that preceded a change).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Cur {
    Absent,
    Small { page: PageId, seq: Seq },
    Extent { start: PageId, n_pages: u32, kind: Kind, twin: Option<PageId>, seq: Seq },
}

/// A staged change of one key.
pub(crate) enum Change {
    Put { vtype: VType, expire_at: Option<u64>, value: Vec<u8> },
    Delete,
    /// Lazy expiry without a write (§16.4).
    Expired,
    /// Merge sync: the stored record is identical; only the entry's sync mark is set.
    Confirm,
}

pub(crate) struct Staged {
    pub(crate) db: u8,
    pub(crate) key: Box<[u8]>,
    pub(crate) kh: KeyHash,
    pub(crate) slot: u16,
    pub(crate) cur: Cur,
    pub(crate) change: Change,
    /// The replication-visible op (cluster primaries).
    pub(crate) op: Option<Op>,
    /// Explicit version (replicated ops carry the primary's seq).
    pub(crate) seq: Option<Seq>,
    /// The change does not make the key's stored data grow: a TTL change of an existing
    /// version (same value) or a delete. On a full device it may use the space reserve above
    /// the fill limit (like an accepted move, §14.3): the copy it replaces is freed once it is
    /// durable, and EXPIRE / PERSIST / DEL stay available to make room (Redis never refuses
    /// them for memory).
    pub(crate) no_growth: bool,
    /// Key plus value bytes of the stored version this change replaces (0 when the key was
    /// absent): a request whose puts are no larger than what it replaces does not grow the
    /// stored data either (RENAME, an overwrite of the same size).
    pub(crate) old_len: u64,
}

/// Something `exec` needed that was not present.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Need {
    Page(PageId),
    /// Read `pages` pages of the extent at `start`; `chunks` also reads a chunkmap's chunks.
    Extent { start: PageId, pages: u32, chunks: bool },
    /// Wait until the extent write at `start` completes.
    ExtWrite(PageId),
}

/// One chunk of a chunkmap read for a request.
pub(crate) struct ChunkFetch {
    pub(crate) page: PageId,
    pub(crate) n_pages: u32,
    pub(crate) crc: u32,
    /// Shared with the replies that carry the chunk's bytes ([`Keyspace::get_shared`]).
    pub(crate) buf: Option<Arc<AlignedBuf>>,
}

/// An extent read for one request (prefetch or `NeedMore`).
pub(crate) struct ExtFetch {
    pub(crate) start: PageId,
    pub(crate) pages: u32,
    /// Shared with the replies that carry the value's bytes ([`Keyspace::get_shared`]).
    pub(crate) buf: Option<Arc<AlignedBuf>>,
    pub(crate) err: Option<i32>,
    /// Chunkmap reads: the chunks, read one at a time in order.
    pub(crate) chunks: Option<SmallVec<[ChunkFetch; 4]>>,
    pub(crate) want_chunks: bool,
    /// The read was asked with its chunks (a value read of a chunkmap). `want_chunks` drops to
    /// false when the extent read turns out not to be a valid chunkmap; this stays set, so that
    /// the exec reports the corruption instead of asking for the same read again.
    pub(crate) chunks_asked: bool,
    /// Read while earlier requests on the request's keys were still queued ahead of it (the
    /// prefetch at submit): those requests may rewrite the extent in place or reuse its run
    /// for a newer version before this one runs, so the buffer is dropped when it runs.
    pub(crate) early: bool,
}

impl ExtFetch {
    pub(crate) fn complete(&self) -> bool {
        self.err.is_some()
            || (self.buf.is_some()
                && (!self.want_chunks || self.chunks.as_ref().is_some_and(|c| c.iter().all(|c| c.buf.is_some()))))
    }
}

/// Bytes of a value in an extent read buffer, shared with the reply that carries them
/// ([`Keyspace::get_shared`]): a GET of a large value sends the buffer it was read into instead
/// of a copy, and every queued GET of the same version shares that one buffer (the read is handed
/// down the key's queue). The buffer is freed when the last holder drops it.
#[derive(Clone)]
pub struct SharedValue {
    buf: Arc<AlignedBuf>,
    off: usize,
    len: usize,
}

impl SharedValue {
    /// `part` must lie inside `buf`.
    fn of(buf: &Arc<AlignedBuf>, part: &[u8]) -> Self {
        let off = part.as_ptr() as usize - buf.as_slice().as_ptr() as usize;
        debug_assert!(off + part.len() <= buf.as_slice().len());
        SharedValue { buf: Arc::clone(buf), off, len: part.len() }
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf.as_slice()[self.off..self.off + self.len]
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl AsRef<[u8]> for SharedValue {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

/// Where a found value's bytes are.
#[derive(Clone, Copy)]
enum Src {
    Staged(usize),
    Pending { frame: FrameIdx, idx: usize },
    Image { frame: FrameIdx, rec: RecDesc },
    ExtWrite(PageId),
    Fetched(usize),
    Chunks(usize),
    /// Header only (meta access of an extent).
    NotLoaded,
}

#[derive(Clone, Copy)]
struct Hit {
    vtype: VType,
    expire_at: Option<u64>,
    len: u64,
    seq: Seq,
    src: Src,
}

/// Staged access to one partition for one request.
pub struct Keyspace<'p> {
    pub(crate) core: &'p mut Core,
    pub(crate) cache: &'p mut PageCache,
    pub(crate) fetched: &'p mut SmallVec<[ExtFetch; 1]>,
    db: u8,
    now_ms: u64,
    pub(crate) staged: SmallVec<[Staged; 2]>,
    pub(crate) needs: SmallVec<[Need; 2]>,
    /// Explicit version for every change staged (replicated requests).
    pub(crate) seq: Option<Seq>,
    /// Seed of `random_key` draws (RANDOMKEY requests).
    pub(crate) rand: u64,
    /// A replica hides expired keys but never expires them (§16.4).
    pub(crate) no_lazy_expire: bool,
}

fn vtype_of_rec(kind: u8) -> VType {
    if kind == REC_KIND_HASH { VType::Hash } else { VType::String }
}

impl<'p> Keyspace<'p> {
    pub(crate) fn new(
        core: &'p mut Core,
        cache: &'p mut PageCache,
        fetched: &'p mut SmallVec<[ExtFetch; 1]>,
        db: u8,
        now_ms: u64,
    ) -> Self {
        let no_lazy_expire = core.cfg.cluster && !core.cfg.produce_groups;
        Keyspace {
            core,
            cache,
            fetched,
            db,
            now_ms,
            staged: SmallVec::new(),
            needs: SmallVec::new(),
            seq: None,
            rand: 0,
            no_lazy_expire,
        }
    }

    pub fn db(&self) -> u8 {
        self.db
    }

    pub fn select(&mut self, db: u8) {
        self.db = db;
    }

    /// Wall-clock time of the request (TTL decisions).
    pub fn now_ms(&self) -> u64 {
        self.now_ms
    }

    fn need(&mut self, n: Need) {
        if !self.needs.contains(&n) {
            self.needs.push(n);
        }
    }

    fn staged_idx(&self, db: u8, key: &[u8]) -> Option<usize> {
        self.staged.iter().rposition(|s| s.db == db && *s.key == *key)
    }

    /// Finds the current version of `(db, key)`: its location and, unless it is absent or
    /// expired, the facts and the bytes source. `value` asks for the whole value to be present.
    fn find(&mut self, db: u8, key: &[u8], value: bool) -> Result<(Cur, Option<Hit>), StoreError> {
        if let Some(i) = self.staged_idx(db, key) {
            let s = &self.staged[i];
            return Ok(match &s.change {
                Change::Put { vtype, expire_at, value } => (
                    s.cur,
                    Some(Hit {
                        vtype: *vtype,
                        expire_at: *expire_at,
                        len: value.len() as u64,
                        seq: s.seq.unwrap_or(Seq::ZERO),
                        src: Src::Staged(i),
                    }),
                ),
                Change::Delete | Change::Expired => (s.cur, None),
                Change::Confirm => (s.cur, None),
            });
        }
        let kh = KeyHash::of(self.core.cfg.hash_seed, key);
        let slot = key_slot(key);
        if slot % self.core.cfg.partitions != self.core.cfg.id {
            debug_assert!(false, "key of another partition");
            return Err(StoreError::Io(-libc::EXDEV));
        }
        let mut cands = SmallVec::new();
        match self.core.index.table(db, slot) {
            Some(t) => t.candidates(kh, &mut cands),
            None => return Ok((Cur::Absent, None)),
        }
        let mut missing = false;
        for c in cands.iter() {
            let start = PageId::from36(c.entry.page36);
            let found = match c.entry.kind {
                Kind::Small => self.find_small(start, db, kh, key, &mut missing)?,
                Kind::Extent | Kind::ChunkMap => self.find_extent(start, c.entry.x8, c.entry.kind, db, key, value, &mut missing)?,
            };
            if let Some((cur, hit)) = found {
                return Ok(self.expiry(db, key, kh, slot, cur, hit));
            }
        }
        if missing {
            return Err(StoreError::NeedMore);
        }
        Ok((Cur::Absent, None))
    }

    /// Lazy expiry: an expired version is absent; stage its removal from the index (§16.4).
    fn expiry(&mut self, db: u8, key: &[u8], kh: KeyHash, slot: u16, cur: Cur, hit: Option<Hit>) -> (Cur, Option<Hit>) {
        let Some(h) = hit else { return (cur, None) };
        match h.expire_at {
            Some(t) if t <= self.now_ms => {
                if !self.no_lazy_expire {
                    self.staged.push(Staged {
                        db,
                        key: key.into(),
                        kh,
                        slot,
                        cur,
                        change: Change::Expired,
                        op: Some(Op::Expired { db, key: key.into() }),
                        seq: self.seq,
                        no_growth: true,
                        old_len: h.len.saturating_add(key.len() as u64),
                    });
                }
                (cur, None)
            }
            _ => (cur, Some(h)),
        }
    }

    fn find_small(
        &mut self,
        page: PageId,
        db: u8,
        kh: KeyHash,
        key: &[u8],
        missing: &mut bool,
    ) -> Result<Option<(Cur, Option<Hit>)>, StoreError> {
        let Some(f) = self.cache.peek(page) else {
            self.need(Need::Page(page));
            *missing = true;
            return Ok(None);
        };
        let Some(st) = self.cache.state(f) else {
            self.need(Need::Page(page));
            *missing = true;
            return Ok(None);
        };
        if st.corrupt {
            return Err(StoreError::Corrupt(page));
        }
        let h32 = kh.h32();
        for (i, p) in st.pending.iter().enumerate().rev() {
            if p.is_change() && p.is_key(db, h32, key) {
                return Ok(match p {
                    Pending::Put(r) => {
                        let cur = Cur::Small { page, seq: r.desc.seq };
                        if r.desc.kind == REC_KIND_TXN_DELETE_MARKER {
                            Some((cur, None))
                        } else {
                            Some((
                                cur,
                                Some(Hit {
                                    vtype: vtype_of_rec(r.desc.kind),
                                    expire_at: r.desc.expire_at(),
                                    len: u64::from(r.desc.val_len),
                                    seq: r.desc.seq,
                                    src: Src::Pending { frame: f, idx: i },
                                }),
                            ))
                        }
                    }
                    // removed from this page: the key may live elsewhere (another candidate)
                    Pending::Remove { .. } | Pending::Retire { .. } => None,
                });
            }
        }
        let (recs, fr) = st.image();
        let img = self.cache.frame(fr);
        let mut best: Option<RecDesc> = None;
        for r in recs {
            if r.status == RecStatus::Live
                && r.db == db
                && r.h32 == h32
                && best.is_none_or(|b| r.seq > b.seq)
                && r.key(img) == key
            {
                best = Some(*r);
            }
        }
        Ok(best.map(|r| {
            let cur = Cur::Small { page, seq: r.seq };
            if r.is_marker() {
                (cur, None)
            } else {
                (
                    cur,
                    Some(Hit {
                        vtype: vtype_of_rec(r.kind),
                        expire_at: r.expire_at(),
                        len: u64::from(r.val_len),
                        seq: r.seq,
                        src: Src::Image { frame: fr, rec: r },
                    }),
                )
            }
        }))
    }

    #[allow(clippy::too_many_arguments)]
    fn find_extent(
        &mut self,
        start: PageId,
        class: u8,
        kind: Kind,
        db: u8,
        key: &[u8],
        value: bool,
        missing: &mut bool,
    ) -> Result<Option<(Cur, Option<Hit>)>, StoreError> {
        let n_pages = class_pages(class.min((crate::layout::N_CLASSES - 1) as u8));
        // a write of this extent is staged or in flight: the header and value are in RAM (a
        // chunked value whose chunkmap write completed is read from the device instead: no
        // event would wake a reader waiting for that write any more)
        if let Some(w) = self.core.ext.writes.get(&start)
            && let Some(e) = w.failed
            && value
            && w.db == db
            && *w.key == *key
            && (w.hdr.kind != EXT_KIND_VALUE || w.buf.is_none())
        {
            // the device failed this write and its value is not in RAM (a chunked value: the
            // chunks hold it); the media never held it, so the error is what a read reports
            return Err(StoreError::Io(e));
        }
        if let Some(w) = self.core.ext.writes.get(&start)
            && !(value && w.state == crate::partition::ExtState::AwaitFlush && w.failed.is_none() && (w.buf.is_none() || w.hdr.kind != EXT_KIND_VALUE))
        {
            if w.db != db || *w.key != *key {
                return Ok(None);
            }
            let h = w.hdr;
            let cur = Cur::Extent { start, n_pages: h.n_pages, kind, twin: h.twin, seq: h.seq };
            let hit = Hit {
                vtype: if h.vtype == EXT_VTYPE_HASH { VType::Hash } else { VType::String },
                expire_at: h.expire_at(),
                len: h.val_len,
                seq: h.seq,
                src: if !value { Src::NotLoaded } else { Src::ExtWrite(start) },
            };
            if value && (w.buf.is_none() || h.kind != EXT_KIND_VALUE) {
                // in flight (buffer handed to the device) or chunked: wait for the write
                self.need(Need::ExtWrite(start));
                *missing = true;
                return Ok(None);
            }
            return Ok(Some((cur, Some(hit))));
        }
        let want_pages = if value { n_pages } else { 1 };
        // the value of a chunkmap is in its chunks: a read of the chunkmap pages alone (a
        // prefetch for a write, a metadata read) does not have it
        let want_chunks = value && kind == Kind::ChunkMap;
        let fi = self.fetched.iter().position(|e| e.start == start && e.pages >= want_pages && (!want_chunks || e.chunks_asked) && e.complete());
        let Some(fi) = fi else {
            self.need(Need::Extent { start, pages: want_pages, chunks: value && kind == Kind::ChunkMap });
            *missing = true;
            return Ok(None);
        };
        let fe = &self.fetched[fi];
        if let Some(e) = fe.err {
            return Err(StoreError::Io(e));
        }
        let buf = fe.buf.as_ref().map_or(&[][..], |b| b.as_slice());
        let Some(h) = extent::parse_extent_header(buf, start) else {
            // the entry names this extent: anything else there is corruption (§15)
            return Err(StoreError::Corrupt(start));
        };
        let k = h.key_len as usize;
        if EXT_HDR + k > buf.len() {
            // a key longer than the pages read: read more of the extent
            let pages = (EXT_HDR + k).div_ceil(PAGE) as u32;
            self.need(Need::Extent { start, pages: pages.max(want_pages), chunks: value && kind == Kind::ChunkMap });
            *missing = true;
            return Ok(None);
        }
        if h.db != db || &buf[EXT_HDR..EXT_HDR + k] != key {
            return Ok(None);
        }
        let cur = Cur::Extent { start, n_pages: h.n_pages, kind, twin: h.twin, seq: h.seq };
        let mut src = Src::NotLoaded;
        if value {
            if !extent::verify_extent(buf, &h, self.core.crc()) {
                return Err(StoreError::Corrupt(start));
            }
            src = if h.kind == EXT_KIND_CHUNKMAP { Src::Chunks(fi) } else { Src::Fetched(fi) };
            if h.kind == EXT_KIND_CHUNKMAP {
                let chunks_ok = fe.chunks.as_ref().is_some_and(|cs| {
                    cs.iter().all(|c| {
                        c.buf.as_ref().is_some_and(|b| {
                            extent::parse_extent_header(b, c.page)
                                .is_some_and(|ch| extent::stored_crc(b) == Some(c.crc) && extent::verify_extent(b, &ch, self.core.crc()))
                        })
                    })
                });
                if !chunks_ok {
                    return Err(StoreError::Corrupt(start));
                }
            }
        }
        Ok(Some((
            cur,
            Some(Hit {
                vtype: if h.vtype == EXT_VTYPE_HASH { VType::Hash } else { VType::String },
                expire_at: h.expire_at(),
                len: h.val_len,
                seq: h.seq,
                src,
            }),
        )))
    }

    /// The value bytes of a hit.
    fn value_of(&self, h: &Hit) -> Value<'_> {
        match h.src {
            Src::Staged(i) => match &self.staged[i].change {
                Change::Put { value, .. } => Value::Contig(value),
                _ => Value::Contig(&[]),
            },
            Src::Pending { frame, idx } => match self.cache.state(frame).and_then(|s| s.pending.get(idx)) {
                Some(Pending::Put(r)) => Value::Contig(r.val()),
                _ => Value::Contig(&[]),
            },
            Src::Image { frame, rec } => Value::Contig(rec.val(self.cache.frame(frame))),
            Src::ExtWrite(start) => match self.core.ext.writes.get(&start).and_then(|w| w.buf.as_ref().map(|b| (w, b))) {
                Some((w, b)) => {
                    let s = EXT_HDR + w.key.len();
                    Value::Contig(&b.as_slice()[s..s + h.len as usize])
                }
                None => Value::Contig(&[]),
            },
            Src::Fetched(fi) => {
                let b = self.fetched[fi].buf.as_ref().map_or(&[][..], |b| b.as_slice());
                match extent::parse_extent_header(b, self.fetched[fi].start).and_then(|eh| extent::value_parts(b, &eh)) {
                    Some((_, v)) => Value::Contig(v),
                    None => Value::Contig(&[]),
                }
            }
            Src::Chunks(fi) => {
                let mut parts = SmallVec::new();
                if let Some(cs) = &self.fetched[fi].chunks {
                    for c in cs {
                        if let Some(b) = &c.buf
                            && let Some(ch) = extent::parse_extent_header(b, c.page)
                            && let Some(p) = extent::chunk_payload(b, &ch)
                        {
                            parts.push(p);
                        }
                    }
                }
                Value::Chunks(parts)
            }
            Src::NotLoaded => Value::Contig(&[]),
        }
    }

    fn count_lookup(&mut self, found: bool) {
        let c = &mut self.core.counters;
        if found {
            c.keyspace_hits += 1;
        } else {
            c.keyspace_misses += 1;
        }
    }

    pub fn meta(&mut self, key: &[u8]) -> Result<Option<KeyMeta>, StoreError> {
        let db = self.db;
        let (_, hit) = self.find(db, key, false)?;
        self.count_lookup(hit.is_some());
        Ok(hit.map(|h| KeyMeta { vtype: h.vtype, expire_at_ms: h.expire_at, len: h.len }))
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<(KeyMeta, Value<'_>)>, StoreError> {
        let db = self.db;
        let (_, hit) = self.find(db, key, true)?;
        self.count_lookup(hit.is_some());
        Ok(hit.map(|h| (KeyMeta { vtype: h.vtype, expire_at_ms: h.expire_at, len: h.len }, self.value_of(&h))))
    }

    /// [`Keyspace::get`] that also gives the value's bytes as shared extent read buffers when it
    /// was read from extents (empty otherwise: the value is in a page or staged). The parts are in
    /// value order and together are the whole value.
    pub fn get_shared(&mut self, key: &[u8]) -> Result<Option<(KeyMeta, Value<'_>, SmallVec<[SharedValue; 4]>)>, StoreError> {
        let db = self.db;
        let (_, hit) = self.find(db, key, true)?;
        self.count_lookup(hit.is_some());
        Ok(hit.map(|h| {
            let shared = self.shared_of(&h);
            (KeyMeta { vtype: h.vtype, expire_at_ms: h.expire_at, len: h.len }, self.value_of(&h), shared)
        }))
    }

    fn shared_of(&self, h: &Hit) -> SmallVec<[SharedValue; 4]> {
        let mut out: SmallVec<[SharedValue; 4]> = SmallVec::new();
        match h.src {
            Src::Fetched(fi) => {
                if let Some(b) = &self.fetched[fi].buf
                    && let Some(eh) = extent::parse_extent_header(b, self.fetched[fi].start)
                    && let Some((_, v)) = extent::value_parts(b, &eh)
                {
                    out.push(SharedValue::of(b, v));
                }
            }
            Src::Chunks(fi) => {
                for c in self.fetched[fi].chunks.iter().flatten() {
                    match &c.buf {
                        Some(b) => match extent::parse_extent_header(b, c.page).and_then(|ch| extent::chunk_payload(b, &ch)) {
                            Some(p) => out.push(SharedValue::of(b, p)),
                            None => return SmallVec::new(),
                        },
                        None => return SmallVec::new(),
                    }
                }
            }
            _ => {}
        }
        if out.iter().map(|p| p.len() as u64).sum::<u64>() != h.len {
            // not the whole value: the caller copies it from `value_of`
            out.clear();
        }
        out
    }

    fn stage(&mut self, db: u8, key: &[u8], cur: Cur, old: Option<&Hit>, change: Change, op: Option<Op>) {
        self.stage_as(db, key, cur, old, change, op, false);
    }

    /// [`Keyspace::stage`] of a change that does not make the key grow (`Staged::no_growth`).
    #[allow(clippy::too_many_arguments)]
    fn stage_as(&mut self, db: u8, key: &[u8], cur: Cur, old: Option<&Hit>, change: Change, op: Option<Op>, no_growth: bool) {
        let old_len = old.map_or(0, |h| h.len.saturating_add(key.len() as u64));
        let op = if self.core.cfg.produce_groups { op } else { None };
        if let Some(i) = self.staged_idx(db, key) {
            // one change per key: the newest wins, the location found first stays
            let s = &mut self.staged[i];
            s.change = change;
            // a request that also wrote the key otherwise may grow it
            s.no_growth &= no_growth;
            if op.is_some() {
                // the group carries one op per key, so a key changed twice by one request
                // replicates as its final state: a relative op (APPEND, SETRANGE, EXPIRE) alone
                // would apply to the member's version from before the request's first change
                s.op = match &s.change {
                    Change::Put { vtype, expire_at, value } => Some(Op::Put {
                        db,
                        vtype: if *vtype == VType::Hash { EXT_VTYPE_HASH } else { EXT_VTYPE_STRING },
                        expire_at: expire_at.unwrap_or(0),
                        key: key.into(),
                        value: value.clone(),
                    }),
                    Change::Delete | Change::Expired => Some(Op::Del { db, key: key.into() }),
                    Change::Confirm => s.op.take(),
                };
            }
            return;
        }
        self.staged.push(Staged {
            db,
            key: key.into(),
            kh: KeyHash::of(self.core.cfg.hash_seed, key),
            slot: key_slot(key),
            cur,
            change,
            op,
            seq: self.seq,
            no_growth,
            old_len,
        });
    }

    pub fn put(&mut self, key: &[u8], value: &[u8], vtype: VType, expire_at_ms: Option<u64>) -> Result<(), StoreError> {
        if value.len() as u64 > MAX_VALUE {
            return Err(StoreError::TooLarge);
        }
        let db = self.db;
        let (cur, hit) = self.find(db, key, false)?;
        let op = Op::Put {
            db,
            vtype: if vtype == VType::Hash { 2 } else { 1 },
            expire_at: expire_at_ms.unwrap_or(0),
            key: key.into(),
            value: value.to_vec(),
        };
        self.stage(db, key, cur, hit.as_ref(), Change::Put { vtype, expire_at: expire_at_ms, value: value.to_vec() }, Some(op));
        Ok(())
    }

    /// Returns the new length.
    pub fn append(&mut self, key: &[u8], data: &[u8]) -> Result<u64, StoreError> {
        let db = self.db;
        let (cur, hit) = self.find(db, key, true)?;
        let (mut v, expire) = match &hit {
            Some(h) if h.vtype != VType::String => return Err(StoreError::WrongType),
            Some(h) => (self.value_of(h).to_vec(), h.expire_at),
            None => (Vec::new(), None),
        };
        if v.len() as u64 + data.len() as u64 > MAX_VALUE {
            return Err(StoreError::TooLarge);
        }
        v.extend_from_slice(data);
        let len = v.len() as u64;
        let op = Op::Append { db, key: key.into(), data: data.to_vec() };
        self.stage(db, key, cur, hit.as_ref(), Change::Put { vtype: VType::String, expire_at: expire, value: v }, Some(op));
        Ok(len)
    }

    /// Returns the new length.
    pub fn set_range(&mut self, key: &[u8], off: u64, data: &[u8]) -> Result<u64, StoreError> {
        let db = self.db;
        let (cur, hit) = self.find(db, key, true)?;
        let (mut v, expire) = match &hit {
            Some(h) if h.vtype != VType::String => return Err(StoreError::WrongType),
            Some(h) => (self.value_of(h).to_vec(), h.expire_at),
            None if data.is_empty() => return Ok(0),
            None => (Vec::new(), None),
        };
        if data.is_empty() {
            return Ok(v.len() as u64);
        }
        let end = off.checked_add(data.len() as u64).ok_or(StoreError::TooLarge)?;
        if end > MAX_VALUE {
            return Err(StoreError::TooLarge);
        }
        if (v.len() as u64) < end {
            v.resize(end as usize, 0);
        }
        v[off as usize..end as usize].copy_from_slice(data);
        let len = v.len() as u64;
        let op = Op::SetRange { db, key: key.into(), offset: off, data: data.to_vec() };
        self.stage(db, key, cur, hit.as_ref(), Change::Put { vtype: VType::String, expire_at: expire, value: v }, Some(op));
        Ok(len)
    }

    /// Sets (`Some`) or removes (`None`) the expiry; returns whether the key exists.
    pub fn set_expire(&mut self, key: &[u8], expire_at_ms: Option<u64>) -> Result<bool, StoreError> {
        let db = self.db;
        let (cur, hit) = self.find(db, key, true)?;
        let Some(h) = hit else { return Ok(false) };
        let v = self.value_of(&h).to_vec();
        let op = Op::Expire { db, key: key.into(), expire_at: expire_at_ms.unwrap_or(0) };
        self.stage_as(db, key, cur, Some(&h), Change::Put { vtype: h.vtype, expire_at: expire_at_ms, value: v }, Some(op), true);
        Ok(true)
    }

    /// Returns whether the key existed.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool, StoreError> {
        let db = self.db;
        let (cur, hit) = self.find(db, key, false)?;
        if hit.is_none() {
            return Ok(false);
        }
        self.stage_as(db, key, cur, hit.as_ref(), Change::Delete, Some(Op::Del { db, key: key.into() }), true);
        Ok(true)
    }

    /// The highest seq of any copy of `key` in the selected db that the partition holds: the
    /// current version (staged, in flight or durable), tracked older copies and an absent newest
    /// copy (an unretired txn delete marker).
    fn newest_copy_seq(&mut self, key: &[u8]) -> Result<Seq, StoreError> {
        let db = self.db;
        let stored = self.stored_seq(key)?;
        Ok(stored.max(self.core.tracked_copy_seq(db, key)))
    }

    /// The stored version of a key (`Seq::ZERO` when absent), for ordered replication.
    pub fn stored_seq(&mut self, key: &[u8]) -> Result<Seq, StoreError> {
        let db = self.db;
        let (_, hit) = self.find(db, key, false)?;
        Ok(hit.map_or(Seq::ZERO, |h| h.seq))
    }

    /// A uniformly drawn live key of the selected db (RANDOMKEY, §16.6): a random non-empty
    /// table, a random entry, the key read from its record; up to 16 tries on expired keys and
    /// fingerprint mismatches.
    pub fn random_key(&mut self) -> Result<Option<Box<[u8]>>, StoreError> {
        let db = self.db;
        let mut r = self.rand | 1;
        for _ in 0..16 {
            r = splitmix(r);
            let Some(slot) = self.core.index.random_table(db, r) else { return Ok(None) };
            let Some(t) = self.core.index.table(db, slot) else { continue };
            let Some((_, e)) = t.random_entry(splitmix(r ^ 0x5151)) else { continue };
            let page = PageId::from36(e.page36);
            let key: Option<Box<[u8]>> = match e.kind {
                Kind::Small => {
                    let Some(f) = self.cache.peek(page) else {
                        self.need(Need::Page(page));
                        return Err(StoreError::NeedMore);
                    };
                    self.small_key_for(f, db, e.fp32, e.x8)
                }
                Kind::Extent | Kind::ChunkMap => {
                    if let Some(w) = self.core.ext.writes.get(&page) {
                        Some(w.key.clone())
                    } else if let Some(fe) = self.fetched.iter().find(|f| f.start == page && f.complete()) {
                        let b = fe.buf.as_ref().map_or(&[][..], |b| b.as_slice());
                        extent::parse_extent_header(b, page)
                            .filter(|h| EXT_HDR + h.key_len as usize <= b.len())
                            .map(|h| Box::<[u8]>::from(&b[EXT_HDR..EXT_HDR + h.key_len as usize]))
                    } else {
                        self.need(Need::Extent { start: page, pages: 1, chunks: false });
                        return Err(StoreError::NeedMore);
                    }
                }
            };
            let Some(key) = key else { continue };
            if self.meta(&key)?.is_some() {
                return Ok(Some(key));
            }
        }
        Ok(None)
    }

    /// The live key of page frame `f` whose hash has fingerprint `(fp32, x8)`.
    fn small_key_for(&self, f: FrameIdx, db: u8, fp32: u32, x8: u8) -> Option<Box<[u8]>> {
        let st = self.cache.state(f)?;
        let seed = self.core.cfg.hash_seed;
        let matches = |k: &[u8]| {
            let h = KeyHash::of(seed, k);
            h.fp32() == fp32 && h.x8() == x8
        };
        for p in st.pending.iter().rev() {
            if let Pending::Put(r) = p
                && r.desc.db == db
                && matches(r.key())
            {
                return Some(r.key().into());
            }
        }
        let (recs, fr) = st.image();
        let img = self.cache.frame(fr);
        recs.iter()
            .filter(|r| r.status == RecStatus::Live && r.db == db && !r.is_marker())
            .map(|r| r.key(img))
            .find(|k| matches(k))
            .map(Into::into)
    }

    /// member / image / snapshot
    pub fn apply_replicated(&mut self, op: &Op, seq: Seq, mode: ApplyMode) -> Result<ApplyOutcome, StoreError> {
        let saved = (self.db, self.seq, self.no_lazy_expire);
        self.seq = Some(seq);
        self.no_lazy_expire = true;
        let r = self.apply_op(op, seq, mode);
        (self.db, self.seq, self.no_lazy_expire) = saved;
        r
    }

    fn apply_op(&mut self, op: &Op, seq: Seq, mode: ApplyMode) -> Result<ApplyOutcome, StoreError> {
        let key: &[u8] = match op {
            Op::Put { key, .. }
            | Op::Del { key, .. }
            | Op::Expired { key, .. }
            | Op::Expire { key, .. }
            | Op::Append { key, .. }
            | Op::SetRange { key, .. } => key,
            Op::Noop => return Ok(ApplyOutcome::Applied),
            // partition-wide effects are requests of their own (ReqKind::FlushDb, slot purge)
            Op::FlushDb { .. } | Op::FlushSlot { .. } => return Err(StoreError::Io(-libc::EINVAL)),
        };
        let db = match op {
            Op::Put { db, .. }
            | Op::Del { db, .. }
            | Op::Expired { db, .. }
            | Op::Expire { db, .. }
            | Op::Append { db, .. }
            | Op::SetRange { db, .. } => *db,
            _ => self.db,
        };
        self.db = db;
        // a version never replaces a copy of its key at the same or a later version, in any mode:
        // recovery would prefer that copy to the write, and the copy (protected until a durable
        // copy with a higher seq exists) would never be released, so the write's effect would
        // wait forever behind it. The stored copy is already as new as the op.
        let newest = self.newest_copy_seq(key)?;
        if mode == ApplyMode::Ordered && newest >= seq {
            return Ok(ApplyOutcome::Skipped);
        }
        match op {
            Op::Put { vtype, expire_at, value, .. } => {
                let vt = if *vtype == EXT_VTYPE_HASH { VType::Hash } else { VType::String };
                let exp = (*expire_at != 0).then_some(*expire_at);
                let identical = mode == ApplyMode::SnapshotMerge
                    && match self.get(key)? {
                        Some((m, v)) => m.vtype == vt && m.expire_at_ms == exp && v.len() == value.len() as u64 && v.to_vec() == *value,
                        None => false,
                    };
                if identical {
                    let (cur, _) = self.find(db, key, false)?;
                    self.stage(db, key, cur, None, Change::Confirm, None);
                    return Ok(ApplyOutcome::Confirmed);
                }
                if newest >= seq {
                    return Ok(ApplyOutcome::Skipped);
                }
                self.put(key, value, vt, exp)?;
            }
            _ if newest >= seq => return Ok(ApplyOutcome::Skipped),
            Op::Del { .. } | Op::Expired { .. } => {
                self.delete(key)?;
            }
            Op::Expire { expire_at, .. } => {
                self.set_expire(key, (*expire_at != 0).then_some(*expire_at))?;
            }
            Op::Append { data, .. } => {
                self.append(key, data)?;
            }
            Op::SetRange { offset, data, .. } => {
                self.set_range(key, *offset, data)?;
            }
            Op::Noop | Op::FlushDb { .. } | Op::FlushSlot { .. } => {}
        }
        Ok(ApplyOutcome::Applied)
    }
}

pub(crate) fn splitmix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_slices() {
        let a = [1u8, 2, 3];
        let b = [4u8, 5];
        let v = Value::Chunks(SmallVec::from_slice(&[&a[..], &b[..]]));
        assert_eq!(v.len(), 5);
        assert_eq!(v.to_vec(), vec![1, 2, 3, 4, 5]);
        assert_eq!(v.slice(2, 4), vec![3, 4]);
        assert_eq!(v.slice(0, 99), vec![1, 2, 3, 4, 5]);
        assert_eq!(v.slice(4, 2), Vec::<u8>::new());
        let c = Value::Contig(&a);
        assert_eq!(c.slice(1, 3), vec![2, 3]);
        assert!(!c.is_empty());
    }
}
