//! Slot tables (DESIGN §9.1, §9.4, §9.8, §16.6).
//!
//! A [`SlotTable`] holds every key of one `(db, slot)`: an extendible-hashing directory of
//! `2^D + 1` u32 segment ids (the last is a `u32::MAX` sentinel; `seg = dir[fp >> (32 - D)]`,
//! `D = 0 -> dir[0]`) over ordered linear-probing segments (see `segment.rs`). Lookups return
//! fingerprint candidates; the caller verifies the key against the SSD bytes (LC-07).
//!
//! Iteration (LC-18): SCAN, KEYS, replica full sync and migration walk a table only through
//! [`SlotTable::scan_pages`], which consumes whole equal-fp32 groups in fingerprint order,
//! applies the filter to each entry before anything is emitted, and outputs only
//! de-duplicated [`PageRef`]s plus one scalar [`ScanSpan`]; nothing carrying a per-entry
//! fingerprint, hash or entry leaves the walk. A step stops after `max_entries` entries or
//! [`SCAN_STEP_MAX_PAGES`] distinct pages, a compile-time constant tied to the COUNT batch.
//! [`SlotTable::snapshot_iter`] is reserved to the clean-shutdown snapshot writer
//! (`kv-storage/src/snapshot.rs`).
//!
//! Directory at `D = 0` (LC-09, scaffold open point 6): `(2^0 + 1) * 4 = 8` bytes would be a
//! power of two, so a table with a single segment keeps its directory inline (no allocation;
//! the segment is `segs[0]`). From `D = 1` on the directory is one `seg_alloc` allocation of
//! `(2^D + 1) * 4` bytes, which is `4 * odd` and never a power of two ([`dir_bytes`]).
//!
//! Growth and shrinking (§9.4) are local to one segment and run inline: a segment over the grow
//! threshold (or with no EMPTY left after the insert position) is rebuilt at the next capacity
//! of the sequence; one at [`SEG_MAX`] splits on fp bit `31 - d` into two children whose
//! capacity leaves them at load <= 0.8 (the load a 9/8 growth step leaves); the directory
//! doubles when a child's depth exceeds `D`. A segment below a quarter full is rebuilt at the
//! smallest capacity with load <= 0.8. Segments are never merged: the directory never shrinks,
//! and a table that becomes empty releases every segment and its directory.
//!
//! Fingerprint order is the only order: splitting, growing and shrinking never reorder entries
//! of different fp32, which is what makes the scalar SCAN cursor stable (§16.6).

use core::fmt;
use core::ops::Range;

use kv_common::KeyHash;
use smallvec::SmallVec;

use crate::entry::{
    Cand, Entry, Kind, Pos, Raw, VType, raw_fp32, raw_is_small, raw_set_sync_mark, raw_sync_mark,
    raw_vtype,
};
use crate::segment::{
    Load, SEG_MAX, SEG_MIN_CAP, Segment, below_shrink_threshold, cap_for_load, next_cap,
    within_grow_threshold,
};
use crate::sysbuf::{SysBuf, seg_alloc};

/// A page (or extent) referenced by entries of a scanned span.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct PageRef {
    pub page36: u64,
    pub kind: Kind,
    pub class: u8,
}

/// The fingerprint range covered by one `scan_pages` step.
#[derive(Clone, Copy, Debug)]
pub struct ScanSpan {
    pub fp_lo: u32,
    /// 2^32 = end of table
    pub fp_hi_excl: u64,
    pub next: Option<u32>,
}

/// Built-in key-only filters applied inside the walk (LC-17).
#[derive(Clone, Copy, Debug)]
pub struct ScanFilter {
    pub vtype: Option<VType>,
    /// Merge-sync prune walk: only entries whose `sync_mark` is unset.
    pub unmarked_only: bool,
}

impl ScanFilter {
    /// No filtering.
    pub const ALL: ScanFilter = ScanFilter {
        vtype: None,
        unmarked_only: false,
    };

    #[inline]
    fn admits(&self, raw: &Raw) -> bool {
        if self.unmarked_only && raw_sync_mark(raw) {
            return false;
        }
        self.vtype.is_none_or(|v| raw_vtype(raw) == v)
    }
}

/// compile-time constant, never configurable (LC-18)
pub const SCAN_STEP_MAX_PAGES: usize = 32;

/// Largest global (and local) directory depth: `2^19 + 1` directory entries (§9.4).
pub const MAX_DIR_DEPTH: u8 = 19;

/// Byte size of the directory allocation at global depth `depth`: 0 at `D = 0` (kept inline),
/// otherwise `(2^D + 1) * 4`, never a power of two (LC-09).
pub const fn dir_bytes(depth: u8) -> usize {
    if depth == 0 {
        0
    } else {
        ((1usize << depth) + 1) * 4
    }
}

/// The table cannot place another entry: the target segment is at [`SEG_MAX`] and
/// [`MAX_DIR_DEPTH`] with no free position. Unreachable with keyed-hash fingerprints (it needs
/// more than 8 328 keys sharing their top 19 fingerprint bits in one slot).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct IndexFull;

impl fmt::Display for IndexFull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("slot table capacity exhausted")
    }
}

impl std::error::Error for IndexFull {}

/// Children of a split and segments of a bulk build target load <= 0.8.
const REBUILD_LOAD: Load = Load { num: 8, den: 10 };
/// Bulk build places segments at load <= 0.85 (§9.8).
const BULK_LOAD: Load = Load { num: 85, den: 100 };
/// Bulk build: target entries per segment, `0.85 * SEG_MAX`.
const BULK_PER_SEG: usize = SEG_MAX as usize * 85 / 100;
/// Random draws tried inside a segment before `random_entry` falls back to counting.
const RANDOM_PROBES: usize = 32;

/// The index of one `(db, slot)`.
pub struct SlotTable {
    /// `None` at `D = 0` (inline, the only segment is `segs[0]`); else `2^D + 1` u32 segment
    /// ids (little endian), last = sentinel `u32::MAX`.
    dir: Option<SysBuf>,
    depth: u8,
    segs: Vec<Segment>,
    len: usize,
    ttl_len: usize,
}

impl Default for SlotTable {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for SlotTable {
    /// Counts only (no fingerprints, LC-32).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlotTable")
            .field("len", &self.len)
            .field("ttl_len", &self.ttl_len)
            .field("depth", &self.depth)
            .field("segments", &self.segs.len())
            .field("memory_bytes", &self.memory_bytes())
            .finish()
    }
}

/// A sorted run of entries that can be laid out as one or more segments.
trait Run: Sized {
    fn count(&self) -> usize;
    fn entries(&self) -> impl Iterator<Item = (Raw, u16)>;
    /// Splits on fp bit `31 - depth`: (bit clear, bit set).
    fn split(&self, depth: u8) -> (Self, Self);
}

#[inline]
fn fp_bit(fp: u32, depth: u8) -> bool {
    (fp >> (31 - u32::from(depth))) & 1 == 1
}

/// Positions `range` of an existing segment.
struct SegRun<'a> {
    seg: &'a Segment,
    range: Range<usize>,
}

impl Run for SegRun<'_> {
    fn count(&self) -> usize {
        self.seg.ents()[self.range.clone()]
            .iter()
            .filter(|r| !Entry::is_empty_raw(r))
            .count()
    }
    fn entries(&self) -> impl Iterator<Item = (Raw, u16)> {
        self.seg.iter_range(self.range.clone())
    }
    fn split(&self, depth: u8) -> (Self, Self) {
        let mid = self.seg.ents()[self.range.clone()]
            .iter()
            .position(|r| !Entry::is_empty_raw(r) && fp_bit(raw_fp32(r), depth))
            .map_or(self.range.end, |k| self.range.start + k);
        (
            SegRun {
                seg: self.seg,
                range: self.range.start..mid,
            },
            SegRun {
                seg: self.seg,
                range: mid..self.range.end,
            },
        )
    }
}

/// A slice of decoded entries sorted by fp32 (bulk build).
struct SliceRun<'a>(&'a [(Entry, u16)]);

impl Run for SliceRun<'_> {
    fn count(&self) -> usize {
        self.0.len()
    }
    fn entries(&self) -> impl Iterator<Item = (Raw, u16)> {
        self.0.iter().map(|(e, h)| (e.to_raw(), *h))
    }
    fn split(&self, depth: u8) -> (Self, Self) {
        let mid = self.0.partition_point(|(e, _)| !fp_bit(e.fp32, depth));
        let (a, b) = self.0.split_at(mid);
        (SliceRun(a), SliceRun(b))
    }
}

/// A laid-out segment with its fp prefix (the top `seg.depth` bits).
struct Placed {
    prefix: u32,
    seg: Segment,
}

/// Lays out `run` (all entries share the top `depth` bits `prefix`) as one segment at the
/// smallest capacity with load <= `load` that the placement fits, or, if it does not fit in
/// one segment within the grow threshold, as the segments of a split on the next fp bit.
fn fit(
    run: impl Run,
    depth: u8,
    prefix: u32,
    load: Load,
    out: &mut Vec<Placed>,
) -> Result<(), IndexFull> {
    let n = u32::try_from(run.count()).unwrap_or(u32::MAX);
    if within_grow_threshold(n, SEG_MAX) {
        let mut cap = cap_for_load(n, load);
        loop {
            if let Some(seg) = Segment::build(cap, depth, run.entries()) {
                out.push(Placed { prefix, seg });
                return Ok(());
            }
            match next_cap(cap) {
                Some(c) => cap = c,
                None => break,
            }
        }
    }
    if depth >= MAX_DIR_DEPTH {
        // Cannot split further: use every position SEG_MAX offers, threshold or not.
        let seg = Segment::build(SEG_MAX, depth, run.entries()).ok_or(IndexFull)?;
        out.push(Placed { prefix, seg });
        return Ok(());
    }
    let (lo, hi) = run.split(depth);
    fit(lo, depth + 1, prefix << 1, load, out)?;
    fit(hi, depth + 1, (prefix << 1) | 1, load, out)
}

/// `floor(r * n / 2^64)`: a uniform draw in `0..n` from 64 random bits.
#[inline]
fn pick(r: u64, n: usize) -> usize {
    ((u128::from(r) * n as u128) >> 64) as usize
}

/// SplitMix64 step, deriving further draws from one caller-supplied random value.
#[inline]
fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[inline]
fn dir_read(d: &SysBuf, i: usize) -> u32 {
    u32::from_le_bytes(d.as_slice().as_chunks::<4>().0[i])
}

impl SlotTable {
    pub fn new() -> Self {
        SlotTable {
            dir: None,
            depth: 0,
            segs: Vec::new(),
            len: 0,
            ttl_len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Number of entries that carry a TTL hint.
    pub fn ttl_len(&self) -> usize {
        self.ttl_len
    }

    /// Global directory depth `D`.
    pub fn depth(&self) -> u8 {
        self.depth
    }

    /// Number of segments.
    pub fn segments(&self) -> usize {
        self.segs.len()
    }

    /// Bytes of segments, hint planes and the directory (§9.6), plus the table and segment
    /// headers. Mapped allocations count whole pages.
    pub fn memory_bytes(&self) -> usize {
        core::mem::size_of::<Self>()
            + self.segs.capacity() * core::mem::size_of::<Segment>()
            + self.dir.as_ref().map_or(0, SysBuf::alloc_bytes)
            + self.segs.iter().map(Segment::mem_bytes).sum::<usize>()
    }

    /// Segment id for `fp`. Requires at least one segment.
    #[inline(always)]
    fn seg_index(&self, fp: u32) -> usize {
        match &self.dir {
            None => 0,
            Some(d) => dir_read(d, (fp >> (32 - u32::from(self.depth))) as usize) as usize,
        }
    }

    /// Fingerprint candidates for `h` (§9.4): equal `fp32` and, for small entries, equal `x8`.
    /// `out` is cleared first.
    #[inline]
    pub fn candidates(&self, h: KeyHash, out: &mut SmallVec<[Cand; 2]>) {
        out.clear();
        self.candidates_fp(h.fp32(), h.x8(), out);
    }

    /// [`SlotTable::candidates`] by fingerprint bits; pushes onto `out` (not cleared).
    #[inline]
    pub(crate) fn candidates_fp(&self, fp: u32, x8: u8, out: &mut SmallVec<[Cand; 2]>) {
        if self.segs.is_empty() {
            return;
        }
        let sid = self.seg_index(fp);
        let seg = &self.segs[sid];
        let ents = seg.ents();
        let mut i = seg.home(fp);
        while let Some(raw) = ents.get(i) {
            if Entry::is_empty_raw(raw) {
                break;
            }
            let f = raw_fp32(raw);
            if f > fp {
                break;
            }
            if f == fp && (!raw_is_small(raw) || raw[4] == x8) {
                out.push(Cand {
                    pos: Pos {
                        seg: sid as u32,
                        idx: i as u32,
                    },
                    entry: Entry::decode(raw),
                    hint: seg.hint_at(i),
                });
            }
            i += 1;
        }
    }

    /// The entry and hint at `pos`, `None` if the position is EMPTY or out of range.
    pub fn get(&self, pos: Pos) -> Option<(Entry, u16)> {
        let seg = self.segs.get(pos.seg as usize)?;
        let raw = seg.ents().get(pos.idx as usize)?;
        if Entry::is_empty_raw(raw) {
            return None;
        }
        Some((Entry::decode(raw), seg.hint_at(pos.idx as usize)))
    }

    /// Inserts a new entry. `fp32` (and `x8` for small entries) are taken from `h`.
    ///
    /// Panics with [`IndexFull`] in the case [`SlotTable::try_insert`] reports; that needs more
    /// than 8 328 keys of one slot sharing their top 19 fingerprint bits.
    pub fn insert(&mut self, h: KeyHash, e: Entry, hint: u16) {
        if let Err(err) = self.try_insert(h, e, hint) {
            panic!("{err}");
        }
    }

    /// [`SlotTable::insert`], reporting a table that cannot place the entry.
    pub fn try_insert(&mut self, h: KeyHash, mut e: Entry, hint: u16) -> Result<(), IndexFull> {
        e.fp32 = h.fp32();
        if e.kind == Kind::Small {
            e.x8 = h.x8();
        }
        self.insert_entry(e, hint)
    }

    /// Inserts `e` as given (fingerprint bits included).
    pub(crate) fn insert_entry(&mut self, e: Entry, hint: u16) -> Result<(), IndexFull> {
        assert!(
            !e.is_empty(),
            "index entries need a page (page36 0 means EMPTY)"
        );
        let raw = e.to_raw();
        if self.segs.is_empty() {
            self.segs.push(Segment::new(SEG_MIN_CAP, 0));
        }
        loop {
            let sid = self.seg_index(e.fp32);
            let seg = &mut self.segs[sid];
            if within_grow_threshold(seg.len + 1, seg.cap) && seg.try_insert(&raw, hint) {
                break;
            }
            if self.grow(sid, e.fp32).is_err() {
                // No larger layout exists: use any free position the segment still has.
                if !self.segs[sid].try_insert(&raw, hint) {
                    return Err(IndexFull);
                }
                break;
            }
        }
        self.len += 1;
        if hint != 0 {
            self.ttl_len += 1;
        }
        Ok(())
    }

    /// Rebuilds segment `sid` (which holds `fp`) at the next capacity that fits, or splits it.
    fn grow(&mut self, sid: usize, fp: u32) -> Result<(), IndexFull> {
        let seg = &self.segs[sid];
        let mut cap = seg.cap;
        while let Some(c) = next_cap(cap) {
            cap = c;
            if let Some(new) = Segment::build(c, seg.depth, seg.iter_range(0..seg.total())) {
                self.segs[sid] = new;
                return Ok(());
            }
        }
        self.split(sid, fp)
    }

    /// Splits segment `sid` (which holds `fp`) on fp bit `31 - d` (§9.4).
    fn split(&mut self, sid: usize, fp: u32) -> Result<(), IndexFull> {
        let seg = &self.segs[sid];
        let d = seg.depth;
        if d >= MAX_DIR_DEPTH {
            return Err(IndexFull);
        }
        let prefix = (u64::from(fp) >> (32 - u32::from(d))) as u32;
        let (lo, hi) = SegRun {
            seg,
            range: 0..seg.total(),
        }
        .split(d);
        let mut placed = Vec::with_capacity(2);
        fit(lo, d + 1, prefix << 1, REBUILD_LOAD, &mut placed)?;
        fit(hi, d + 1, (prefix << 1) | 1, REBUILD_LOAD, &mut placed)?;
        self.install(Some(sid), placed);
        Ok(())
    }

    /// Puts `placed` segments into the table; the first replaces segment `reuse` if given.
    /// Doubles the directory as needed and points each segment's directory range at it.
    fn install(&mut self, reuse: Option<usize>, placed: Vec<Placed>) {
        let need = placed
            .iter()
            .map(|p| p.seg.depth)
            .max()
            .unwrap_or(0)
            .max(self.depth);
        if need > self.depth {
            self.expand_dir(need);
        }
        let gd = u32::from(self.depth);
        for (k, p) in placed.into_iter().enumerate() {
            let shift = gd - u32::from(p.seg.depth);
            let id = match (k, reuse) {
                (0, Some(sid)) => {
                    self.segs[sid] = p.seg;
                    sid
                }
                _ => {
                    self.segs.push(p.seg);
                    self.segs.len() - 1
                }
            };
            if let Some(d) = &mut self.dir {
                let start = (p.prefix as usize) << shift;
                let ids =
                    &mut d.as_mut_slice().as_chunks_mut::<4>().0[start..start + (1usize << shift)];
                ids.fill((id as u32).to_le_bytes());
            }
        }
    }

    /// Doubles the directory to global depth `new_depth` (entry `i` of the old directory
    /// becomes entries `i << k .. (i + 1) << k`).
    fn expand_dir(&mut self, new_depth: u8) {
        debug_assert!(new_depth > self.depth && new_depth <= MAX_DIR_DEPTH);
        let k = u32::from(new_depth - self.depth);
        let mut nd = seg_alloc(dir_bytes(new_depth));
        let n = 1usize << new_depth;
        {
            let ids = nd.as_mut_slice().as_chunks_mut::<4>().0;
            for (i, slot) in ids[..n].iter_mut().enumerate() {
                let id = self.dir.as_ref().map_or(0, |d| dir_read(d, i >> k));
                *slot = id.to_le_bytes();
            }
            ids[n] = u32::MAX.to_le_bytes();
        }
        self.dir = Some(nd);
        self.depth = new_depth;
    }

    /// Rewrites the entry at `pos` in place (§9.4): `page36`, `kind`, `vtype`, `x8` and the
    /// hint. `fp32` must not change (the stored one is kept). A set `sync_mark` is kept (only
    /// [`SlotTable::set_sync_mark`] and [`SlotTable::clear_sync_marks`] clear it).
    pub fn update(&mut self, pos: Pos, e: Entry, hint: u16) {
        let i = pos.idx as usize;
        let Some(seg) = self.segs.get_mut(pos.seg as usize) else {
            debug_assert!(false, "update of an invalid position");
            return;
        };
        let Some(old) = seg
            .ents()
            .get(i)
            .copied()
            .filter(|r| !Entry::is_empty_raw(r))
        else {
            debug_assert!(false, "update of an EMPTY or invalid position");
            return;
        };
        debug_assert_eq!(e.fp32, raw_fp32(&old), "update must not change fp32");
        if e.is_empty() {
            debug_assert!(false, "update to page36 0; use remove");
            self.remove(pos);
            return;
        }
        let mut e = e;
        e.fp32 = raw_fp32(&old);
        e.sync_mark |= raw_sync_mark(&old);
        let old_hint = seg.replace_at(i, &e.to_raw(), hint);
        match (old_hint != 0, hint != 0) {
            (false, true) => self.ttl_len += 1,
            (true, false) => self.ttl_len -= 1,
            _ => {}
        }
    }

    /// Removes the entry at `pos` (backward shift, §9.4); shrinks its segment when it falls
    /// below a quarter of its capacity; releases everything when the table becomes empty.
    pub fn remove(&mut self, pos: Pos) {
        let sid = pos.seg as usize;
        let Some(seg) = self.segs.get_mut(sid) else {
            debug_assert!(false, "remove of an invalid position");
            return;
        };
        let Some(old_hint) = seg.remove_at(pos.idx as usize) else {
            debug_assert!(false, "remove of an EMPTY or invalid position");
            return;
        };
        self.len -= 1;
        if old_hint != 0 {
            self.ttl_len -= 1;
        }
        if self.len == 0 {
            *self = SlotTable::new();
        } else if below_shrink_threshold(seg.len, seg.cap) {
            self.shrink(sid);
        }
    }

    /// Rebuilds segment `sid` at the smallest capacity below its current one that the layout
    /// fits, aiming at load <= 0.8.
    fn shrink(&mut self, sid: usize) {
        let seg = &self.segs[sid];
        let mut cap = cap_for_load(seg.len, REBUILD_LOAD);
        while cap < seg.cap {
            if let Some(new) = Segment::build(cap, seg.depth, seg.iter_range(0..seg.total())) {
                self.segs[sid] = new;
                return;
            }
            match next_cap(cap) {
                Some(c) => cap = c,
                None => return,
            }
        }
    }

    /// A walk over the non-EMPTY entries with `fp32 >= fp_from`, in fingerprint order.
    fn walk_from(&self, fp_from: u32) -> Walk<'_> {
        let mut w = Walk {
            t: self,
            dir_i: 0,
            sid: 0,
            pos: 0,
        };
        if self.segs.is_empty() {
            w.dir_i = usize::MAX;
            return w;
        }
        if self.depth > 0 {
            let d = self.depth;
            let i = (fp_from >> (32 - u32::from(d))) as usize;
            w.sid = self.seg_index(fp_from);
            let local = self.segs[w.sid].depth;
            // first directory index of this segment's range
            w.dir_i = i & !((1usize << (d - local)) - 1);
        }
        let seg = &self.segs[w.sid];
        let ents = seg.ents();
        let mut p = seg.home(fp_from);
        while p < ents.len() && (Entry::is_empty_raw(&ents[p]) || raw_fp32(&ents[p]) < fp_from) {
            p += 1;
        }
        w.pos = p;
        w.settle();
        w
    }

    /// SCAN/KEYS/sync/migration walk: consumes whole equal-fp groups from `fp_from` until
    /// `max_entries` entries or SCAN_STEP_MAX_PAGES distinct pages; filter applied before
    /// emitting; emits de-duplicated page refs only.
    ///
    /// * `out` is extended (not cleared) and stays de-duplicated as a whole; the page bound
    ///   applies to its total length, so one SCAN call may walk several tables into one `out`.
    /// * `max_entries` counts entries visited (filtered or not); 0 is treated as 1.
    /// * The span covers every fp32 in `fp_lo..fp_hi_excl`; `next` is the fp32 of the first
    ///   group not consumed (`fp_hi_excl` as `u32`), or `None` (`fp_hi_excl = 2^32`) when no
    ///   entry with a larger fp32 remains.
    /// * A group that would push `out` past [`SCAN_STEP_MAX_PAGES`] ends the step before it,
    ///   unless `out` was empty on entry and it is the first group: then it is consumed anyway
    ///   (only possible with more than 32 keys of one fp32 on distinct pages), so every step
    ///   started with an empty `out` makes progress.
    /// * For small entries `class` is 0; for extents and chunkmaps it is the size class.
    pub fn scan_pages(
        &self,
        fp_from: u32,
        max_entries: usize,
        f: ScanFilter,
        out: &mut Vec<PageRef>,
    ) -> ScanSpan {
        let max_entries = max_entries.max(1);
        let started_empty = out.is_empty();
        let mut visited = 0usize;
        let mut w = self.walk_from(fp_from);
        let mut consumed_any = false;
        loop {
            let Some(fp) = w.current().map(raw_fp32) else {
                return ScanSpan {
                    fp_lo: fp_from,
                    fp_hi_excl: 1 << 32,
                    next: None,
                };
            };
            if visited >= max_entries {
                return ScanSpan {
                    fp_lo: fp_from,
                    fp_hi_excl: u64::from(fp),
                    next: Some(fp),
                };
            }
            let mark = out.len();
            let mut group = 0usize;
            while let Some(raw) = w.current().filter(|r| raw_fp32(r) == fp) {
                if f.admits(raw) {
                    let pr = page_ref(raw);
                    if !out.contains(&pr) {
                        out.push(pr);
                    }
                }
                group += 1;
                w.step();
            }
            if out.len() > SCAN_STEP_MAX_PAGES && (consumed_any || !started_empty) {
                out.truncate(mark);
                return ScanSpan {
                    fp_lo: fp_from,
                    fp_hi_excl: u64::from(fp),
                    next: Some(fp),
                };
            }
            consumed_any = true;
            visited += group;
        }
    }

    /// Snapshot writer only (kv-storage/src/snapshot.rs; lint LC-18). Entries in fingerprint
    /// order with their hints; `sync_mark` is always reported as 0 (§9.2).
    pub fn snapshot_iter(&self) -> impl Iterator<Item = (Entry, u16)> + '_ {
        let mut w = self.walk_from(0);
        core::iter::from_fn(move || {
            let (sid, pos) = (w.sid, w.pos);
            let raw = *w.current()?;
            w.step();
            let mut e = Entry::decode(&raw);
            e.sync_mark = false;
            Some((e, self.segs[sid].hint_at(pos)))
        })
    }

    /// Sets or clears the merge-sync mark of the entry at `pos` (§23.7).
    pub fn set_sync_mark(&mut self, pos: Pos, on: bool) {
        let Some(raw) = self
            .segs
            .get_mut(pos.seg as usize)
            .and_then(|s| s.ents_mut().get_mut(pos.idx as usize))
        else {
            debug_assert!(false, "sync mark of an invalid position");
            return;
        };
        if Entry::is_empty_raw(raw) {
            debug_assert!(false, "sync mark of an EMPTY position");
            return;
        }
        raw_set_sync_mark(raw, on);
    }

    /// Clears every merge-sync mark of the table (§23.7).
    pub fn clear_sync_marks(&mut self) {
        for seg in &mut self.segs {
            seg.clear_sync_marks();
        }
    }

    /// Builds a table from entries sorted by fp32 (clean restart, §9.8, §18.2).
    ///
    /// Panics with [`IndexFull`] where [`SlotTable::try_bulk_build`] reports it.
    pub fn bulk_build(sorted: Vec<(Entry, u16)>) -> Self {
        match Self::try_bulk_build(sorted) {
            Ok(t) => t,
            Err(err) => panic!("{err}"),
        }
    }

    /// [`SlotTable::bulk_build`], reporting input that no table can hold (more than 8 328
    /// entries sharing their top 19 fingerprint bits). EMPTY entries are dropped; input not
    /// sorted by fp32 is sorted first. Segments: the next power of two
    /// `>= n / (0.85 * SEG_MAX)`, `D` = its exponent, linear placement at load <= 0.85.
    pub fn try_bulk_build(mut sorted: Vec<(Entry, u16)>) -> Result<Self, IndexFull> {
        sorted.retain(|(e, _)| !e.is_empty());
        if !sorted.is_sorted_by_key(|(e, _)| e.fp32) {
            sorted.sort_by_key(|(e, _)| e.fp32);
        }
        let mut t = SlotTable::new();
        if sorted.is_empty() {
            return Ok(t);
        }
        let d0 = sorted
            .len()
            .div_ceil(BULK_PER_SEG)
            .next_power_of_two()
            .trailing_zeros();
        let d0 = d0.min(u32::from(MAX_DIR_DEPTH)) as u8;
        let mut placed = Vec::with_capacity(1 << d0);
        let mut rest = &sorted[..];
        for prefix in 0..(1u32 << d0) {
            let cut = if d0 == 0 {
                rest.len()
            } else {
                rest.partition_point(|(e, _)| e.fp32 >> (32 - u32::from(d0)) <= prefix)
            };
            let (run, tail) = rest.split_at(cut);
            rest = tail;
            fit(SliceRun(run), d0, prefix, BULK_LOAD, &mut placed)?;
        }
        t.segs.reserve_exact(placed.len());
        t.install(None, placed);
        t.len = t.segs.iter().map(|s| s.len as usize).sum();
        t.ttl_len = t.segs.iter().map(|s| s.ttl_count as usize).sum();
        Ok(t)
    }

    /// Active expiry RAM sweep (§16.4): pushes positions whose hint lower bound is `<= now_lb`,
    /// visiting at most `budget` positions from `start`; returns the resume position.
    ///
    /// `now_lb` is [`crate::hint::now_code`] of the current time: a position qualifies when its
    /// hint `h` satisfies `h != 0 && h <= now_lb`. Positions are visited segment by segment
    /// (storage order, not fp order); a segment without TTL keys costs one unit of `budget`.
    /// Start with [`Pos::START`]; [`Pos::END`] is returned once the table has been swept. A
    /// stale `start` (the table changed since it was returned) is clamped, so a sweep may then
    /// skip or revisit some positions; the next round covers them.
    pub fn expiry_candidates(
        &self,
        start: Pos,
        budget: usize,
        now_lb: u16,
        out: &mut Vec<Pos>,
    ) -> Pos {
        let mut budget = budget.max(1);
        let mut sid = start.seg as usize;
        let mut i = start.idx as usize;
        while sid < self.segs.len() {
            let seg = &self.segs[sid];
            match seg.hints().filter(|_| seg.ttl_count > 0) {
                None => budget -= 1,
                Some(h) => {
                    let end = h.len().min(i.saturating_add(budget));
                    if i < end {
                        push_due(&h[i..end], i, sid as u32, now_lb, out);
                        budget -= end - i;
                    }
                    if end < h.len() {
                        return Pos {
                            seg: sid as u32,
                            idx: end as u32,
                        };
                    }
                }
            }
            sid += 1;
            i = 0;
            if budget == 0 {
                break;
            }
        }
        if sid < self.segs.len() {
            Pos {
                seg: sid as u32,
                idx: 0,
            }
        } else {
            Pos::END
        }
    }

    /// rebase pass (sliced by caller)
    ///
    /// Maps every non-zero hint through `f` (for example [`crate::hint::rebase`]). A result of
    /// 0 removes the key from the TTL count.
    pub fn rehint(&mut self, f: &mut dyn FnMut(u16) -> u16) {
        for seg in &mut self.segs {
            if seg.ttl_count == 0 {
                continue;
            }
            let Some(h) = &mut seg.hints else { continue };
            let mut cleared = 0u32;
            for slot in h.as_mut_slice().as_chunks_mut::<2>().0 {
                let old = u16::from_le_bytes(*slot);
                if old != 0 {
                    let new = f(old);
                    if new == 0 {
                        cleared += 1;
                    }
                    *slot = new.to_le_bytes();
                }
            }
            seg.ttl_count -= cleared;
            self.ttl_len -= cleared as usize;
        }
    }

    /// A uniformly drawn entry for RANDOMKEY (§16.6), from 64 random bits `r`.
    pub fn random_entry(&self, r: u64) -> Option<(Pos, Entry)> {
        if self.len == 0 {
            return None;
        }
        let mut state = r;
        let mut k = pick(splitmix(&mut state), self.len);
        // the segment holding the k-th entry (in storage order)
        let (sid, seg) = self.segs.iter().enumerate().find(|(_, s)| {
            let n = s.len as usize;
            if k < n {
                true
            } else {
                k -= n;
                false
            }
        })?;
        let ents = seg.ents();
        // rejection sampling over positions is uniform among the segment's entries
        for _ in 0..RANDOM_PROBES {
            let i = pick(splitmix(&mut state), ents.len());
            if !Entry::is_empty_raw(&ents[i]) {
                return Some((
                    Pos {
                        seg: sid as u32,
                        idx: i as u32,
                    },
                    Entry::decode(&ents[i]),
                ));
            }
        }
        // sparse segment: take its k-th entry
        let (i, raw) = ents
            .iter()
            .enumerate()
            .filter(|(_, r)| !Entry::is_empty_raw(r))
            .nth(k)?;
        Some((
            Pos {
                seg: sid as u32,
                idx: i as u32,
            },
            Entry::decode(raw),
        ))
    }
}

/// Pushes the positions of `hints` (starting at position `base`) with `1 <= h <= now_lb`.
/// Processes 16 hints per step as a bit mask so the compare vectorizes.
#[inline]
fn push_due(hints: &[[u8; 2]], base: usize, sid: u32, now_lb: u16, out: &mut Vec<Pos>) {
    let (chunks, rest) = hints.as_chunks::<16>();
    let mut at = base;
    for c in chunks {
        let mut m = 0u32;
        for (j, h) in c.iter().enumerate() {
            m |= u32::from(u16::from_le_bytes(*h).wrapping_sub(1) < now_lb) << j;
        }
        while m != 0 {
            out.push(Pos {
                seg: sid,
                idx: (at + m.trailing_zeros() as usize) as u32,
            });
            m &= m - 1;
        }
        at += 16;
    }
    for (j, h) in rest.iter().enumerate() {
        if u16::from_le_bytes(*h).wrapping_sub(1) < now_lb {
            out.push(Pos {
                seg: sid,
                idx: (at + j) as u32,
            });
        }
    }
}

#[inline]
fn page_ref(raw: &Raw) -> PageRef {
    let e = Entry::decode(raw);
    PageRef {
        page36: e.page36,
        kind: e.kind,
        class: if e.kind == Kind::Small { 0 } else { e.x8 },
    }
}

/// Cursor over the non-EMPTY positions of a table in fingerprint order.
struct Walk<'a> {
    t: &'a SlotTable,
    /// First directory index covered by segment `sid`; `usize::MAX` once exhausted.
    dir_i: usize,
    sid: usize,
    pos: usize,
}

impl<'a> Walk<'a> {
    /// The entry at the cursor, `None` at the end.
    #[inline]
    fn current(&self) -> Option<&'a Raw> {
        if self.dir_i == usize::MAX {
            return None;
        }
        self.t.segs[self.sid].ents().get(self.pos)
    }

    #[inline]
    fn step(&mut self) {
        self.pos += 1;
        self.settle();
    }

    /// Moves the cursor forward to the next non-EMPTY position (across segments).
    fn settle(&mut self) {
        let t = self.t;
        while self.dir_i != usize::MAX {
            let seg = &t.segs[self.sid];
            let ents = seg.ents();
            while self.pos < ents.len() && Entry::is_empty_raw(&ents[self.pos]) {
                self.pos += 1;
            }
            if self.pos < ents.len() {
                return;
            }
            let next = self.dir_i + (1usize << (t.depth - seg.depth));
            if next >= (1usize << t.depth) {
                self.dir_i = usize::MAX;
                return;
            }
            self.dir_i = next;
            self.sid = match &t.dir {
                None => 0,
                Some(d) => dir_read(d, next) as usize,
            };
            self.pos = 0;
        }
    }
}

#[cfg(test)]
impl SlotTable {
    /// Checks the directory, every segment (I-a, I-b, I-c), prefixes, counts and LC-09 sizes.
    pub(crate) fn check(&self) -> Result<(), String> {
        if self.segs.is_empty() {
            if self.len != 0 || self.ttl_len != 0 || self.dir.is_some() || self.depth != 0 {
                return Err("empty table with state".into());
            }
            return Ok(());
        }
        let n = 1usize << self.depth;
        let id_at =
            |i: usize| -> usize { self.dir.as_ref().map_or(0, |d| dir_read(d, i) as usize) };
        match &self.dir {
            None if self.depth != 0 || self.segs.len() != 1 => {
                return Err("inline directory with D > 0".into());
            }
            None => {}
            Some(d) => {
                if d.len() != dir_bytes(self.depth) || d.len().is_power_of_two() {
                    return Err("LC-09: directory size".into());
                }
                if dir_read(d, n) != u32::MAX {
                    return Err("directory sentinel".into());
                }
            }
        }
        let mut seen = vec![false; self.segs.len()];
        let mut i = 0usize;
        while i < n {
            let sid = id_at(i);
            let seg = self.segs.get(sid).ok_or("directory id out of range")?;
            if seen[sid] {
                return Err(format!("segment {sid} referenced twice"));
            }
            seen[sid] = true;
            if seg.depth > self.depth {
                return Err("local depth above global depth".into());
            }
            let span = 1usize << (self.depth - seg.depth);
            if !i.is_multiple_of(span) || (i..i + span).any(|j| id_at(j) != sid) {
                return Err(format!("segment {sid} directory range not aligned"));
            }
            let prefix = (i >> (self.depth - seg.depth)) as u64;
            seg.check().map_err(|e| format!("segment {sid}: {e}"))?;
            for (raw, _) in seg.iter_range(0..seg.total()) {
                let fp = u64::from(raw_fp32(&raw));
                if seg.depth > 0 && fp >> (32 - u32::from(seg.depth)) != prefix {
                    return Err(format!("segment {sid}: entry outside its prefix"));
                }
            }
            i += span;
        }
        if seen.iter().any(|s| !s) {
            return Err("unreferenced segment".into());
        }
        let len: usize = self.segs.iter().map(|s| s.len as usize).sum();
        let ttl: usize = self.segs.iter().map(|s| s.ttl_count as usize).sum();
        if len != self.len || ttl != self.ttl_len {
            return Err(format!(
                "table counts: {} / {} vs {len} / {ttl}",
                self.len, self.ttl_len
            ));
        }
        Ok(())
    }

    /// `(segments, sum of capacities, depth)`: changes exactly when the layout is restructured.
    pub(crate) fn shape(&self) -> (usize, u64, u8) {
        (
            self.segs.len(),
            self.segs.iter().map(|s| u64::from(s.cap)).sum(),
            self.depth,
        )
    }

    /// Checks the cluster around `pos` (cheap per-operation check for large tables).
    pub(crate) fn check_near(&self, pos: Pos) -> Result<(), String> {
        match self.segs.get(pos.seg as usize) {
            None => Ok(()),
            Some(s) => s.check_cluster(pos.idx as usize),
        }
    }

    /// Fingerprint lookup returning positions of every entry with this fp32 (any x8).
    pub(crate) fn positions_of_fp(&self, fp: u32) -> SmallVec<[Pos; 2]> {
        let mut out = SmallVec::new();
        if self.segs.is_empty() {
            return out;
        }
        let sid = self.seg_index(fp);
        let seg = &self.segs[sid];
        let ents = seg.ents();
        let mut i = seg.home(fp);
        while let Some(raw) = ents.get(i) {
            if Entry::is_empty_raw(raw) || raw_fp32(raw) > fp {
                break;
            }
            if raw_fp32(raw) == fp {
                out.push(Pos {
                    seg: sid as u32,
                    idx: i as u32,
                });
            }
            i += 1;
        }
        out
    }
}

#[cfg(test)]
mod tests;
