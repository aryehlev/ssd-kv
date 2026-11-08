//! Free-space management (DESIGN §11).
//!
//! Per partition: `page_free` (one byte per owned SMALL page: free bytes / 16, 255 = fresh) and,
//! per EXTENT AG, `alloc`/`start` bitmaps plus the free runs sorted by `(len, start)`. This is
//! allocator state, not index (reported as `alloc_state_bytes`, LC-09). Placement of a small
//! record (§11.3): best fit among cached qualifying pages, else a fresh page of the owned SMALL
//! AG with the fewest fresh pages left, else best fit among uncached pages by hint, else the
//! spare SMALL AG, else `-OOM` at the fill limit (§11.6). Extents (§11.4): the key's twin run if
//! free, else the best-fit hole, carving at a drawn `n`-aligned offset of a longer run, else the
//! spare EXTENT AG. Freed space is reusable at once by any key of the partition; empty AGs are
//! released to the node pool (§11.7). Live records are never moved to reclaim space.
//!
//! Every choice among free pages, AGs, tie-breaks and carve offsets goes through
//! [`KeyedPick`] (LC-01): `r = xxh3(domain || boot_gen || draws, seed)` selects the
//! `(r mod n)`-th candidate, so the acquisition sequence is neither monotonic nor periodic and
//! placement is uncorrelated with time and device address.

use std::sync::Arc;

use hashbrown::HashMap;
use kv_common::KeyHash;
use smallvec::SmallVec;

use crate::agtable::AgKind;
use crate::cache::PageCache;
use crate::io::PageUpdateMode;
use crate::layout::{AG_PAGES, AgId, DeviceLayout, PAGE, PageId, SMALL_HDR};

/// Keyed pseudo-random selection among equal candidates.
pub struct KeyedPick {
    seed: u64,
    boot_gen: u32,
    draws: u64,
}

impl KeyedPick {
    pub fn new(hash_seed: u64, boot_gen: u32) -> Self {
        KeyedPick { seed: hash_seed, boot_gen, draws: 0 }
    }

    /// index of the (r mod n)-th set bit, r = xxh3(domain || boot_gen || draws, seed); None if
    /// no bit is set
    pub fn pick(&mut self, domain: u64, candidates: &[u64]) -> Option<usize> {
        let n: u64 = candidates.iter().map(|w| u64::from(w.count_ones())).sum();
        if n == 0 {
            return None;
        }
        let k = self.draw(domain) % n;
        Some(select_bit(candidates, k))
    }

    /// among n equal candidates
    pub fn pick_index(&mut self, domain: u64, n: usize) -> Option<usize> {
        if n == 0 {
            return None;
        }
        Some((self.draw(domain) % n as u64) as usize)
    }

    /// Draws taken so far (an in-RAM count; it selects nothing by itself).
    pub fn draws(&self) -> u64 {
        self.draws
    }

    /// The next draw value `xxh3(domain || boot_gen || draws, seed)`.
    fn draw(&mut self, domain: u64) -> u64 {
        let mut msg = [0u8; 20];
        msg[..8].copy_from_slice(&domain.to_le_bytes());
        msg[8..12].copy_from_slice(&self.boot_gen.to_le_bytes());
        msg[12..].copy_from_slice(&self.draws.to_le_bytes());
        self.draws += 1;
        KeyHash::of(self.seed, &msg).raw()
    }
}

/// Position of the `k`-th set bit (0-based) of `words`; the caller guarantees it exists.
fn select_bit(words: &[u64], mut k: u64) -> usize {
    for (i, &w) in words.iter().enumerate() {
        let c = u64::from(w.count_ones());
        if k < c {
            let mut w = w;
            for _ in 0..k {
                w &= w - 1;
            }
            return i * 64 + w.trailing_zeros() as usize;
        }
        k -= c;
    }
    unreachable!("select_bit: rank beyond the population count")
}

/// Draw domains (combined with the partition id so that partitions draw independently).
pub(crate) mod domain {
    pub(crate) const CACHED: u64 = 1;
    pub(crate) const FRESH_AG: u64 = 2;
    pub(crate) const FRESH_PAGE: u64 = 3;
    pub(crate) const UNCACHED_AG: u64 = 4;
    pub(crate) const UNCACHED_PAGE: u64 = 5;
    pub(crate) const EXTENT_TIE: u64 = 6;
    pub(crate) const CARVE: u64 = 7;
    pub(crate) const WARM: u64 = 10;
}

/// A 4096-bit set (one bit per page of an AG). 512 bytes, one allocation.
#[derive(Clone)]
pub(crate) struct Bits4096(Box<[u64; 64]>);

impl Bits4096 {
    pub(crate) fn new() -> Self {
        Bits4096(Box::new([0u64; 64]))
    }

    pub(crate) fn full() -> Self {
        Bits4096(Box::new([u64::MAX; 64]))
    }

    #[inline]
    pub(crate) fn get(&self, i: u32) -> bool {
        self.0[(i / 64) as usize] >> (i % 64) & 1 == 1
    }

    #[inline]
    pub(crate) fn set(&mut self, i: u32) {
        self.0[(i / 64) as usize] |= 1 << (i % 64);
    }

    #[inline]
    pub(crate) fn clear(&mut self, i: u32) {
        self.0[(i / 64) as usize] &= !(1 << (i % 64));
    }

    pub(crate) fn set_range(&mut self, a: u32, n: u32) {
        for i in a..a + n {
            self.set(i);
        }
    }

    pub(crate) fn clear_range(&mut self, a: u32, n: u32) {
        for i in a..a + n {
            self.clear(i);
        }
    }

    /// True when every bit of `[a, a + n)` is clear.
    pub(crate) fn all_clear(&self, a: u32, n: u32) -> bool {
        a + n <= AG_PAGES && (a..a + n).all(|i| !self.get(i))
    }

    pub(crate) fn words(&self) -> &[u64; 64] {
        &self.0
    }

    /// Bytes `0..512` little endian (snapshot section encoding).
    pub(crate) fn to_bytes(&self, out: &mut Vec<u8>) {
        for w in self.0.iter() {
            out.extend_from_slice(&w.to_le_bytes());
        }
    }

    pub(crate) fn from_bytes(b: &[u8]) -> Option<Self> {
        if b.len() != 512 {
            return None;
        }
        let mut words = Box::new([0u64; 64]);
        for (i, w) in words.iter_mut().enumerate() {
            *w = u64::from_le_bytes(b[8 * i..8 * i + 8].try_into().ok()?);
        }
        Some(Bits4096(words))
    }
}

/// `page_free` value of a fresh page (never parsed; next write is a full-page write).
pub(crate) const FRESH: u8 = 255;
/// Record bytes of a SMALL page (`[0x20, 0x1000)`).
pub(crate) const PAGE_ROOM: u16 = (PAGE - SMALL_HDR) as u16;
/// Cached pages are grouped by free bytes / 128.
const CACHED_CLASSES: usize = 32;
/// Draws tried in one free class before moving to the next larger one.
const DRAWS_PER_CLASS: usize = 4;
/// Owned SMALL AGs examined by one uncached best-fit search.
const UNCACHED_AGS_PER_SEARCH: usize = 4;

/// `page_free` byte for `free` bytes.
#[inline]
pub(crate) fn free_code(free: u16) -> u8 {
    (free / 16).min(254) as u8
}

/// One owned SMALL AG (§11.2).
pub(crate) struct SmallAg {
    pub(crate) ag: AgId,
    /// First device page of the AG.
    pub(crate) first: u32,
    /// Offset of this AG's 4096 bytes in `Alloc.page_free`.
    pub(crate) base: u32,
    /// Pages with `page_free == 255`.
    pub(crate) fresh: u16,
    fresh_bits: Bits4096,
    /// Upper bound of `page_free` over the AG's non-fresh pages (uncached search).
    max_hint: u8,
}

/// One owned EXTENT AG (§11.2).
pub(crate) struct ExtentAg {
    pub(crate) ag: AgId,
    pub(crate) first: u32,
    pub(crate) alloc: Bits4096,
    pub(crate) start: Bits4096,
    /// Free runs `(len, start)`, sorted.
    runs: Vec<(u16, u16)>,
}

impl ExtentAg {
    fn new(ag: AgId, first: u32) -> Self {
        ExtentAg { ag, first, alloc: Bits4096::new(), start: Bits4096::new(), runs: vec![(AG_PAGES as u16, 0)] }
    }

    /// Recomputes the free runs from the `alloc` bits.
    fn rebuild_runs(&mut self) {
        self.runs.clear();
        let mut i = 0u32;
        while i < AG_PAGES {
            if self.alloc.get(i) {
                i += 1;
                continue;
            }
            let s = i;
            while i < AG_PAGES && !self.alloc.get(i) {
                i += 1;
            }
            insert_run(&mut self.runs, ((i - s) as u16, s as u16));
        }
    }

    fn take(&mut self, off: u16, n: u16) {
        let ri = self.runs.iter().position(|&(l, s)| s <= off && off + n <= s + l);
        debug_assert!(ri.is_some(), "extent taken from a range that is not free");
        if let Some(ri) = ri {
            let (l, s) = self.runs.remove(ri);
            if off > s {
                insert_run(&mut self.runs, (off - s, s));
            }
            let (end, rend) = (off + n, s + l);
            if rend > end {
                insert_run(&mut self.runs, (rend - end, end));
            }
        }
        self.alloc.set_range(u32::from(off), u32::from(n));
        self.start.set(u32::from(off));
    }

    fn give_back(&mut self, off: u16, n: u16) {
        self.alloc.clear_range(u32::from(off), u32::from(n));
        self.start.clear(u32::from(off));
        let mut s = u32::from(off);
        while s > 0 && !self.alloc.get(s - 1) {
            s -= 1;
        }
        let mut e = u32::from(off + n);
        while e < AG_PAGES && !self.alloc.get(e) {
            e += 1;
        }
        let (s, e) = (s as u16, e as u16);
        self.runs.retain(|&(l, rs)| !(rs >= s && rs + l <= e));
        insert_run(&mut self.runs, (e - s, s));
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.runs.len() == 1 && self.runs[0] == (AG_PAGES as u16, 0)
    }
}

fn insert_run(runs: &mut Vec<(u16, u16)>, r: (u16, u16)) {
    let i = runs.partition_point(|x| *x < r);
    runs.insert(i, r);
}

/// Cached SMALL pages by free class, with exact free bytes and largest record length.
#[derive(Default)]
struct CachedSpace {
    classes: [Vec<PageId>; CACHED_CLASSES],
    at: HashMap<PageId, (u8, u32), foldhash::fast::FixedState>,
    /// `(free bytes, largest record, largest gap)`.
    info: HashMap<PageId, (u16, u16, u16), foldhash::fast::FixedState>,
}

impl CachedSpace {
    fn set(&mut self, p: PageId, free: u16, largest: u16, gap: u16) {
        let class = ((free / 128) as usize).min(CACHED_CLASSES - 1) as u8;
        if let Some(&(c, _)) = self.at.get(&p) {
            if c == class {
                self.info.insert(p, (free, largest, gap));
                return;
            }
            self.remove(p);
        }
        let v = &mut self.classes[usize::from(class)];
        self.at.insert(p, (class, v.len() as u32));
        v.push(p);
        self.info.insert(p, (free, largest, gap));
    }

    fn remove(&mut self, p: PageId) {
        let Some((c, i)) = self.at.remove(&p) else { return };
        self.info.remove(&p);
        let v = &mut self.classes[usize::from(c)];
        v.swap_remove(i as usize);
        if let Some(&moved) = v.get(i as usize) {
            self.at.insert(moved, (c, i));
        }
    }

    fn len(&self) -> usize {
        self.at.len()
    }
}

/// Outcome of a small-record placement (§11.3).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum SmallPlace {
    /// A cached page with room (step 1).
    Cached(PageId),
    /// A fresh page (steps 2 and 4): not read, first write is a full-page write.
    Fresh(PageId),
    /// An uncached page whose hint qualifies (step 3): the command waits for its read.
    Uncached(PageId),
    /// No space until a spare AG arrives from the meta thread.
    NeedAg,
    /// The fill limit is reached (§11.6): `-OOM`.
    Full,
}

/// Outcome of an extent allocation (§11.4).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum ExtPlace {
    Run(PageId),
    NeedAg,
    Full,
}

/// Spare and acquisition state of one AG kind.
#[derive(Clone, Copy, Default)]
struct SpareSlot {
    spare: Option<AgId>,
    /// An `AcquireAg` should be sent.
    want: bool,
    /// Why the wanted AG is wanted (the most urgent reason since the last request).
    class: AgClass,
    /// The AG is wanted on this device (the chunks of a large value, LC-08).
    want_dev: Option<u8>,
    /// An `AcquireAg` is in flight.
    asked: bool,
    /// The last acquisition was refused (fill limit); retried after `RETRY_NS`.
    refused_at_ns: Option<u64>,
    /// Class of the request in flight.
    asked_class: AgClass,
    /// The request in flight is served from the reserve.
    asked_class_was_reserved: bool,
    /// The last request served from the reserve was refused too (the reserve is used up).
    reserve_refused_at_ns: Option<u64>,
    /// The partition released AGs while the request in flight was being answered: a refusal
    /// of it predates the release and is not latched (the request is sent again).
    stale_refusal: bool,
    /// Stale refusals asked again since the last grant (`MAX_STALE_RETRIES`).
    stale_retries: u8,
    /// Answers so far (stall trace): grants, spare denials, stale asks again, give-back asks
    /// again, latched refusals.
    answers: [u32; 5],
}

/// Refusals that predate a release of this partition, asked again before one is latched. On
/// a busy device at the fill limit the AGs a partition released went to other partitions'
/// requests in the same meta cycle, the partition released more meanwhile, and each answer
/// was stale again: its writes parked through twenty round trips (several seconds) instead
/// of being answered `-OOM`.
const MAX_STALE_RETRIES: u8 = 2;

/// Why an AG is requested, least urgent first.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(crate) enum AgClass {
    /// A spare kept ahead (§11.1): granted only from headroom, a denial is no refusal.
    #[default]
    Spare,
    /// Space a command's placement needs now: refused at the fill limit (`-OOM`, §11.6).
    Need,
    /// Space an already accepted write needs to complete (a record its page could not hold
    /// moves, §14.3): served from the headroom above the fill limit, so that a write the node
    /// acknowledged as accepted never waits for space that the limit keeps back.
    Reserved,
}

/// An AG request to send: `(kind, wanted device, class)`.
pub(crate) type AgRequest = (AgKind, Option<u8>, AgClass);

const RETRY_NS: u64 = 1_000_000_000;

/// The per-partition allocator (§11.2).
pub(crate) struct Alloc {
    partition: u16,
    pick: KeyedPick,
    layouts: Arc<[DeviceLayout]>,
    mode: PageUpdateMode,
    slack_cap: u16,
    slack_atomic: u16,
    pub(crate) page_free: Vec<u8>,
    pub(crate) small: Vec<SmallAg>,
    small_of: HashMap<AgId, u32, foldhash::fast::FixedState>,
    pub(crate) extent: Vec<ExtentAg>,
    extent_of: HashMap<AgId, u32, foldhash::fast::FixedState>,
    cached: CachedSpace,
    slots: [SpareSlot; 2],
    /// Extent starts whose header is read asynchronously (FLUSHDB reclaim walk, active
    /// expiry): start -> (reads in flight, run written or freed since it was registered). A
    /// read concurrent with a write of the run, or with its release and reuse, may return the
    /// previous header; what such a read decides is void (`Alloc::end_header_read`).
    hdr_watch: HashMap<PageId, (u32, bool), foldhash::fast::FixedState>,
    /// An idle spare given back to the pool (taken by the partition, which releases it).
    give_back: Option<AgId>,
    /// See [`Alloc::refusals`].
    refusals: u64,
    /// The pool had AGs left under the fill limit when last looked at
    /// ([`Alloc::set_pool_room`]): a latched refusal no longer holds.
    pool_room: bool,
}

fn kind_slot(k: AgKind) -> usize {
    if k == AgKind::Extent { 1 } else { 0 }
}

impl Alloc {
    pub(crate) fn new(
        partition: u16,
        layouts: Arc<[DeviceLayout]>,
        pick: KeyedPick,
        mode: PageUpdateMode,
        slack_cap: u16,
        slack_atomic: u16,
    ) -> Self {
        Alloc {
            partition,
            pick,
            layouts,
            mode,
            slack_cap: slack_cap.min(PAGE_ROOM),
            slack_atomic: slack_atomic.min(PAGE_ROOM),
            page_free: Vec::new(),
            small: Vec::new(),
            small_of: HashMap::with_hasher(Default::default()),
            extent: Vec::new(),
            extent_of: HashMap::with_hasher(Default::default()),
            hdr_watch: HashMap::with_hasher(Default::default()),
            give_back: None,
            pool_room: false,
            refusals: 0,
            cached: CachedSpace::default(),
            slots: [SpareSlot::default(); 2],
        }
    }

    fn dom(&self, d: u64) -> u64 {
        (d << 16) | u64::from(self.partition)
    }

    /// A keyed draw among `n` equal candidates (shared by the partition's other choices).
    pub(crate) fn pick_index(&mut self, d: u64, n: usize) -> Option<usize> {
        let d = self.dom(d);
        self.pick.pick_index(d, n)
    }

    /// First device page of `ag`.
    pub(crate) fn ag_first(&self, ag: AgId) -> Option<u32> {
        let l = self.layouts.get(usize::from(ag.dev))?;
        (ag.idx < l.n_ags).then(|| l.ag_first_page(ag.idx) as u32)
    }

    /// Page `i` of `ag` (the only place outside format and recovery that names device pages).
    pub(crate) fn page_of(&self, ag: AgId, i: u32) -> Option<PageId> {
        Some(PageId::new(ag.dev, self.ag_first(ag)? + i))
    }

    /// The AG holding `page`.
    pub(crate) fn ag_of(&self, page: PageId) -> Option<(AgId, u32)> {
        let l = self.layouts.get(usize::from(page.dev()))?;
        let idx = l.ag_of_page(u64::from(page.page()))?;
        Some((AgId { dev: page.dev(), idx }, page.page() - l.ag_first_page(idx) as u32))
    }

    // ---------------------------------------------------------------------------------------
    // Ownership

    /// Adds an owned SMALL AG; every page starts fresh.
    pub(crate) fn add_small_ag(&mut self, ag: AgId) -> bool {
        let Some(first) = self.ag_first(ag) else { return false };
        if self.small_of.contains_key(&ag) {
            return true;
        }
        let base = self.page_free.len() as u32;
        self.page_free.resize(self.page_free.len() + AG_PAGES as usize, FRESH);
        self.small_of.insert(ag, self.small.len() as u32);
        self.small.push(SmallAg { ag, first, base, fresh: AG_PAGES as u16, fresh_bits: Bits4096::full(), max_hint: 0 });
        true
    }

    /// Adds an owned EXTENT AG with every page free.
    pub(crate) fn add_extent_ag(&mut self, ag: AgId) -> bool {
        let Some(first) = self.ag_first(ag) else { return false };
        if self.extent_of.contains_key(&ag) {
            return true;
        }
        self.extent_of.insert(ag, self.extent.len() as u32);
        self.extent.push(ExtentAg::new(ag, first));
        true
    }

    pub(crate) fn owns_small(&self, page: PageId) -> bool {
        self.small_slot(page).is_some()
    }

    fn small_slot(&self, page: PageId) -> Option<(usize, u32)> {
        let (ag, i) = self.ag_of(page)?;
        let si = *self.small_of.get(&ag)? as usize;
        Some((si, i))
    }

    fn extent_slot(&self, page: PageId) -> Option<(usize, u16)> {
        let (ag, i) = self.ag_of(page)?;
        let ei = *self.extent_of.get(&ag)? as usize;
        Some((ei, i as u16))
    }

    /// Every owned AG with its kind (snapshot sections, FLUSHALL).
    pub(crate) fn owned(&self) -> impl Iterator<Item = (AgId, AgKind)> + '_ {
        self.small.iter().map(|s| (s.ag, AgKind::Small)).chain(self.extent.iter().map(|e| (e.ag, AgKind::Extent)))
    }



    /// Bytes of allocator state (`alloc_state_bytes`, reported separately from the index).
    pub(crate) fn state_bytes(&self) -> usize {
        self.page_free.capacity()
            + self.small.capacity() * (core::mem::size_of::<SmallAg>() + 512)
            + self.extent.iter().map(|e| core::mem::size_of::<ExtentAg>() + 1024 + e.runs.capacity() * 4).sum::<usize>()
            + self.cached.len() * 40
    }

    // ---------------------------------------------------------------------------------------
    // page_free maintenance

    pub(crate) fn page_free(&self, page: PageId) -> Option<u8> {
        let (si, i) = self.small_slot(page)?;
        Some(self.page_free[(self.small[si].base + i) as usize])
    }

    /// Sets the `page_free` byte of an owned page (recovery, snapshot restore, builds).
    pub(crate) fn set_page_free(&mut self, page: PageId, v: u8) {
        let Some((si, i)) = self.small_slot(page) else { return };
        let ag = &mut self.small[si];
        let slot = &mut self.page_free[(ag.base + i) as usize];
        let was_fresh = *slot == FRESH;
        *slot = v;
        match (was_fresh, v == FRESH) {
            (true, false) => {
                ag.fresh -= 1;
                ag.fresh_bits.clear(i);
            }
            (false, true) => {
                ag.fresh += 1;
                ag.fresh_bits.set(i);
            }
            _ => {}
        }
        if v != FRESH && v > ag.max_hint {
            ag.max_hint = v;
        }
    }

    /// Exact free bytes, largest record and largest contiguous gap of a cached page (§11.3
    /// step 1 and the slack rule; a record is placed into one gap, R2).
    pub(crate) fn note_cached(&mut self, page: PageId, free: u16, largest: u16, gap: u16) {
        if !self.owns_small(page) {
            return;
        }
        self.set_page_free(page, free_code(free));
        self.cached.set(page, free, largest, gap);
    }

    /// A cached page holds nothing any more: it is fresh again (its next write is a full page).
    pub(crate) fn note_fresh(&mut self, page: PageId) {
        self.cached.remove(page);
        self.set_page_free(page, FRESH);
    }


    /// Slack a page must keep after placing `need` bytes (§11.3).
    fn slack(&self, need: u16, largest: u16) -> u16 {
        match self.mode {
            PageUpdateMode::TwoImage => need.max(largest).min(self.slack_cap),
            PageUpdateMode::Atomic => need.min(self.slack_atomic),
        }
    }

    fn slack_min(&self, need: u16) -> u16 {
        match self.mode {
            PageUpdateMode::TwoImage => need.min(self.slack_cap),
            PageUpdateMode::Atomic => need.min(self.slack_atomic),
        }
    }

    /// True when a cached page with `free` bytes and largest record `largest` can take `need`.
    pub(crate) fn qualifies(&self, need: u16, free: u16, largest: u16) -> bool {
        free >= need && free - need >= self.slack(need, largest)
    }

    // ---------------------------------------------------------------------------------------
    // Small placement (§11.3)

    /// Chooses a page for a new small record of `need` bytes. A `Fresh` result is already
    /// marked used (undo with [`Alloc::unplace_fresh`]); other results change nothing.
    ///
    /// `exclude` names pages the record must not go to (a page holding a tracked copy of the
    /// same key, a corrupt page, the page a record is moving away from); they are skipped by
    /// the draws.
    pub(crate) fn place_small(&mut self, need: u16, cache: &PageCache, now_ns: u64, exclude: &dyn Fn(PageId) -> bool) -> SmallPlace {
        if let Some(p) = self.best_cached(need, cache, exclude) {
            return SmallPlace::Cached(p);
        }
        if let Some(p) = self.take_fresh(need) {
            return SmallPlace::Fresh(p);
        }
        if let Some(p) = self.best_uncached(need, cache, exclude) {
            return SmallPlace::Uncached(p);
        }
        let slot = &mut self.slots[0];
        if let Some(ag) = slot.spare.take() {
            slot.want = true;
            self.add_small_ag(ag);
            if let Some(p) = self.take_fresh(need) {
                return SmallPlace::Fresh(p);
            }
        }
        self.need_ag(AgKind::Small, now_ns, None)
    }

    fn need_ag(&mut self, kind: AgKind, now_ns: u64, dev: Option<u8>) -> SmallPlace {
        let slot = &mut self.slots[kind_slot(kind)];
        if let Some(t) = slot.refused_at_ns {
            // a refusal holds while the pool has no AG left under the fill limit: other
            // partitions' AGs given back since (a FLUSHDB or FLUSHALL of every partition releases
            // them one partition after another) lift it. Held for the whole retry period, the
            // extent writes of a partition whose request was answered before the others had
            // released anything got `-OOM` for a second after a FLUSHDB had emptied the device
            if now_ns.saturating_sub(t) < RETRY_NS && !self.pool_room {
                return SmallPlace::Full;
            }
            slot.refused_at_ns = None;
        }
        slot.want = true;
        slot.class = slot.class.max(AgClass::Need);
        if dev.is_some() {
            slot.want_dev = dev;
        }
        SmallPlace::NeedAg
    }

    fn best_cached(&mut self, need: u16, cache: &PageCache, exclude: &dyn Fn(PageId) -> bool) -> Option<PageId> {
        let min = need.saturating_add(self.slack_min(need));
        let c0 = usize::from(min / 128).min(CACHED_CLASSES - 1);
        for c in c0..CACHED_CLASSES {
            for _ in 0..DRAWS_PER_CLASS {
                let n = self.cached.classes[c].len();
                if n == 0 {
                    break;
                }
                let d = self.dom(domain::CACHED);
                let k = self.pick.pick_index(d, n)?;
                let p = self.cached.classes[c][k];
                if !cache.contains(p) {
                    self.cached.remove(p);
                    continue;
                }
                let (free, largest, gap) = self.cached.info.get(&p).copied().unwrap_or((0, 0, 0));
                if gap >= need && self.qualifies(need, free, largest) && !exclude(p) {
                    return Some(p);
                }
            }
        }
        None
    }

    /// A fresh page of the owned SMALL AG with the fewest fresh pages left (ties drawn), the
    /// page itself drawn among the AG's fresh pages; marked used.
    fn take_fresh(&mut self, need: u16) -> Option<PageId> {
        let mut fewest = u16::MAX;
        let mut ties: SmallVec<[usize; 4]> = SmallVec::new();
        for (i, a) in self.small.iter().enumerate() {
            if a.fresh == 0 {
                continue;
            }
            if a.fresh < fewest {
                fewest = a.fresh;
                ties.clear();
            }
            if a.fresh == fewest {
                ties.push(i);
            }
        }
        let ai = ties[self.pick_index(domain::FRESH_AG, ties.len())?];
        let d = self.dom(domain::FRESH_PAGE);
        let i = self.pick.pick(d, self.small[ai].fresh_bits.words())? as u32;
        let page = PageId::new(self.small[ai].ag.dev, self.small[ai].first + i);
        self.set_page_free(page, free_code(PAGE_ROOM.saturating_sub(need)));
        Some(page)
    }

    /// Undoes a `Fresh` placement that was not used.
    pub(crate) fn unplace_fresh(&mut self, page: PageId) {
        self.set_page_free(page, FRESH);
    }

    /// Best fit among uncached pages by hint (§11.3 step 3).
    fn best_uncached(&mut self, need: u16, cache: &PageCache, exclude: &dyn Fn(PageId) -> bool) -> Option<PageId> {
        let req = need.saturating_add(self.slack_min(need)).div_ceil(16);
        if req > 254 {
            return None;
        }
        let req = req as u8;
        for _ in 0..UNCACHED_AGS_PER_SEARCH {
            // the AG with the smallest qualifying upper bound (ties drawn)
            let mut best = u8::MAX;
            let mut ties: SmallVec<[usize; 4]> = SmallVec::new();
            for (i, a) in self.small.iter().enumerate() {
                if a.max_hint < req || a.fresh as u32 == AG_PAGES {
                    continue;
                }
                if a.max_hint < best {
                    best = a.max_hint;
                    ties.clear();
                }
                if a.max_hint == best {
                    ties.push(i);
                }
            }
            let ai = ties[self.pick_index(domain::UNCACHED_AG, ties.len())?];
            let (base, first, dev) = (self.small[ai].base as usize, self.small[ai].first, self.small[ai].ag.dev);
            let mut fit = u8::MAX;
            let mut cands: SmallVec<[u32; 8]> = SmallVec::new();
            let mut top = 0u8;
            for i in 0..AG_PAGES {
                let v = self.page_free[base + i as usize];
                if v == FRESH {
                    continue;
                }
                if v > top {
                    top = v;
                }
                if v < req || v > fit {
                    continue;
                }
                let p = PageId::new(dev, first + i);
                if cache.contains(p) || exclude(p) {
                    continue;
                }
                if v < fit {
                    fit = v;
                    cands.clear();
                }
                if cands.len() < 8 {
                    cands.push(i);
                }
            }
            if !cands.is_empty() {
                let k = self.pick_index(domain::UNCACHED_PAGE, cands.len())?;
                return Some(PageId::new(dev, first + cands[k]));
            }
            // the bound was stale: tighten it and look at the next AG
            self.small[ai].max_hint = if top >= req { req.saturating_sub(1) } else { top };
        }
        None
    }

    /// An uncached page with the most free bytes per its hint (hole warming, §11.3).
    pub(crate) fn warm_candidate(&mut self, cache: &PageCache) -> Option<PageId> {
        let mut best = 0u8;
        let mut ties: SmallVec<[usize; 4]> = SmallVec::new();
        for (i, a) in self.small.iter().enumerate() {
            if a.max_hint == 0 || a.fresh as u32 == AG_PAGES {
                continue;
            }
            if a.max_hint > best {
                best = a.max_hint;
                ties.clear();
            }
            if a.max_hint == best {
                ties.push(i);
            }
        }
        if best < 16 {
            return None;
        }
        let ai = ties[self.pick_index(domain::WARM, ties.len())?];
        let (base, first, dev) = (self.small[ai].base as usize, self.small[ai].first, self.small[ai].ag.dev);
        let mut top = 0u8;
        let mut found = None;
        for i in 0..AG_PAGES {
            let v = self.page_free[base + i as usize];
            if v == FRESH || v <= top {
                continue;
            }
            let p = PageId::new(dev, first + i);
            if !cache.contains(p) {
                top = v;
                found = Some(p);
            }
        }
        self.small[ai].max_hint = top;
        found
    }

    // ---------------------------------------------------------------------------------------
    // Extents (§11.4)

    /// Allocates `n` pages (`n` a size class, <= 2048): the twin run when free, else the best
    /// fit hole (ties drawn), carved at a drawn `n`-aligned offset when longer, else the spare
    /// EXTENT AG. `dev` restricts the run to one device (the chunks of a large value live on
    /// their chunkmap's device, LC-08).
    pub(crate) fn alloc_extent(&mut self, n: u32, twin: Option<PageId>, now_ns: u64, dev: Option<u8>) -> ExtPlace {
        debug_assert!((1..=crate::layout::EXT_MAX_PAGES).contains(&n));
        let n16 = n as u16;
        if let Some(t) = twin
            && dev.is_none_or(|d| t.dev() == d)
            && let Some((ei, off)) = self.extent_slot(t)
            && self.extent[ei].alloc.all_clear(u32::from(off), n)
        {
            self.extent[ei].take(off, n16);
            return ExtPlace::Run(t);
        }
        if let Some(p) = self.best_fit(n16, dev) {
            return ExtPlace::Run(p);
        }
        let slot = &mut self.slots[1];
        if let Some(ag) = slot.spare
            && dev.is_none_or(|d| ag.dev == d)
        {
            slot.spare = None;
            slot.want = true;
            self.add_extent_ag(ag);
            if let Some(p) = self.best_fit(n16, dev) {
                return ExtPlace::Run(p);
            }
        }
        match self.need_ag(AgKind::Extent, now_ns, dev) {
            SmallPlace::Full => ExtPlace::Full,
            _ => ExtPlace::NeedAg,
        }
    }

    /// Runs of at least this many times the request are open space: the carve position is a
    /// keyed draw over every `n`-aligned position of all of them, weighted by length, rather
    /// than inside the smallest one. Runs too short for two requests are holes and are filled
    /// first (best fit). Filling the smallest of several open runs first would lay extents down
    /// in clusters that follow allocation order (LC-01 `prop_placement_uncorrelated`).
    const OPEN_SPACE: u16 = 2;

    fn best_fit(&mut self, n: u16, dev: Option<u8>) -> Option<PageId> {
        let mut best = u16::MAX;
        let mut ties: SmallVec<[(usize, usize); 4]> = SmallVec::new();
        for (ai, a) in self.extent.iter().enumerate() {
            if dev.is_some_and(|d| a.ag.dev != d) {
                continue;
            }
            let i = a.runs.partition_point(|&(l, _)| l < n);
            let Some(&(l, _)) = a.runs.get(i) else { continue };
            if l < best {
                best = l;
                ties.clear();
            }
            if l == best {
                let mut j = i;
                while j < a.runs.len() && a.runs[j].0 == l && ties.len() < 16 {
                    ties.push((ai, j));
                    j += 1;
                }
            }
        }
        if ties.is_empty() {
            return None;
        }
        if best >= n.saturating_mul(Self::OPEN_SPACE) {
            return self.carve_open(n, dev);
        }
        let (ai, ri) = ties[self.pick_index(domain::EXTENT_TIE, ties.len())?];
        let (l, s) = self.extent[ai].runs[ri];
        let off = if l == n {
            s
        } else {
            let k = self.pick_index(domain::CARVE, usize::from(l / n))? as u16;
            s + k * n
        };
        self.extent[ai].take(off, n);
        Some(PageId::new(self.extent[ai].ag.dev, self.extent[ai].first + u32::from(off)))
    }

    /// Carves `n` pages at a keyed draw over every `n`-aligned position of the open-space
    /// runs (no small hole fits the request) that keeps a run of the largest extent class
    /// (2048 pages, half an AG) wherever one was: a run that can hold one offers only the
    /// positions leaving a 2048-page piece before or after the carve, and any position is drawn
    /// only when no run offers one. A value just over 8 MiB is a 2048-page chunk plus one-page
    /// pieces; the pieces, drawn anywhere in an AG, left no 2048-page run in it, so most such
    /// values took an AG of their own and a device refused them with much of its space free.
    fn carve_open(&mut self, n: u16, dev: Option<u8>) -> Option<PageId> {
        let min = n.saturating_mul(Self::OPEN_SPACE);
        let big = crate::layout::EXT_MAX_PAGES as u16;
        // the n-aligned slots of a run (relative index ranges) under the policy above
        let slots = |l: u16, spare_big: bool| -> (usize, usize, usize) {
            let all = usize::from(l / n);
            if !spare_big || n >= big || l < big {
                return (all, 0, 0);
            }
            // a carve at slot k leaves k*n pages before it and l - (k+1)*n after it
            let before_end = if l >= big + n { usize::from((l - big) / n) } else { 0 };
            let after_start = usize::from(big.div_ceil(n));
            let after = all.saturating_sub(after_start);
            (before_end.min(after_start), after_start, after)
        };
        let count = |l: u16, spare_big: bool| -> usize {
            let (a, _, c) = slots(l, spare_big);
            a + c
        };
        let mut spare_big = true;
        let mut total = 0usize;
        for pass in 0..2 {
            spare_big = pass == 0;
            total = 0;
            for a in &self.extent {
                if dev.is_some_and(|d| a.ag.dev != d) {
                    continue;
                }
                total += a.runs.iter().filter(|r| r.0 >= min).map(|r| count(r.0, spare_big)).sum::<usize>();
            }
            if total > 0 {
                break;
            }
        }
        let mut k = self.pick_index(domain::CARVE, total)?;
        for ai in 0..self.extent.len() {
            if dev.is_some_and(|d| self.extent[ai].ag.dev != d) {
                continue;
            }
            for ri in 0..self.extent[ai].runs.len() {
                let (l, s) = self.extent[ai].runs[ri];
                if l < min {
                    continue;
                }
                let (a, c0, c) = slots(l, spare_big);
                if k < a + c {
                    let slot = if k < a { k } else { c0 + (k - a) };
                    let off = s + slot as u16 * n;
                    self.extent[ai].take(off, n);
                    return Some(PageId::new(self.extent[ai].ag.dev, self.extent[ai].first + u32::from(off)));
                }
                k -= a + c;
            }
        }
        None
    }

    /// Frees an extent run (its first-sector zeroing is durable, or it is dead).
    /// Registers the run at `start` for a header read issued later (the reclaim walk's
    /// candidates, recorded when the walk starts): a write or release of the run before the
    /// read is issued voids it too.
    pub(crate) fn watch_header(&mut self, start: PageId) {
        self.hdr_watch.entry(start).or_insert((0, false));
    }

    /// Forgets every watched run without a read in flight (a new reclaim walk replaces the old).
    pub(crate) fn unwatch_idle(&mut self) {
        self.hdr_watch.retain(|_, e| e.0 > 0);
    }

    /// A header read of the run at `start` is issued. False when the run was written or freed
    /// since it was registered (the read would decide nothing).
    pub(crate) fn begin_header_read(&mut self, start: PageId) -> bool {
        let e = self.hdr_watch.entry(start).or_insert((0, false));
        if e.1 && e.0 == 0 {
            self.hdr_watch.remove(&start);
            return false;
        }
        e.0 += 1;
        true
    }

    /// A header read of the run at `start` completed: true when nothing wrote or freed the run
    /// since it was registered, so the header read is the run's current one.
    pub(crate) fn end_header_read(&mut self, start: PageId) -> bool {
        let Some(e) = self.hdr_watch.get_mut(&start) else { return false };
        e.0 = e.0.saturating_sub(1);
        let valid = !e.1;
        if e.0 == 0 {
            self.hdr_watch.remove(&start);
        }
        valid
    }

    /// A write of the run at `start` is staged: header reads of it in flight are void.
    pub(crate) fn note_run_written(&mut self, start: PageId) {
        if let Some(e) = self.hdr_watch.get_mut(&start) {
            e.1 = true;
        }
    }

    pub(crate) fn free_extent(&mut self, start: PageId, n: u32) {
        self.note_run_written(start);
        if let Some((ei, off)) = self.extent_slot(start)
            && u32::from(off) + n <= AG_PAGES
        {
            self.extent[ei].give_back(off, n as u16);
        }
    }

    /// Marks a run allocated (recovery, snapshot restore); runs are rebuilt by
    /// [`Alloc::finish_rebuild`].
    pub(crate) fn mark_extent(&mut self, start: PageId, n: u32) -> bool {
        let Some((ei, off)) = self.extent_slot(start) else { return false };
        if u32::from(off) + n > AG_PAGES {
            return false;
        }
        let a = &mut self.extent[ei];
        a.alloc.set_range(u32::from(off), n);
        a.start.set(u32::from(off));
        true
    }

    /// Replaces the bitmaps of an owned EXTENT AG (snapshot restore).
    pub(crate) fn set_extent_bits(&mut self, ag: AgId, alloc: Bits4096, start: Bits4096) {
        if let Some(&ei) = self.extent_of.get(&ag) {
            let a = &mut self.extent[ei as usize];
            a.alloc = alloc;
            a.start = start;
        }
    }

    /// True when every page of the run is allocated.
    pub(crate) fn extent_allocated(&self, start: PageId, n: u32) -> bool {
        self.extent_slot(start).is_some_and(|(ei, off)| {
            let a = &self.extent[ei];
            u32::from(off) + n <= AG_PAGES && (u32::from(off)..u32::from(off) + n).all(|i| a.alloc.get(i))
        })
    }


    /// Recomputes extent runs, fresh counts and hint bounds after recovery or snapshot restore.
    pub(crate) fn finish_rebuild(&mut self) {
        for a in &mut self.extent {
            a.rebuild_runs();
        }
        for a in &mut self.small {
            let pf = &self.page_free[a.base as usize..(a.base + AG_PAGES) as usize];
            a.fresh = 0;
            a.fresh_bits = Bits4096::new();
            a.max_hint = 0;
            for (i, &v) in pf.iter().enumerate() {
                if v == FRESH {
                    a.fresh += 1;
                    a.fresh_bits.set(i as u32);
                } else if v > a.max_hint {
                    a.max_hint = v;
                }
            }
        }
    }

    // ---------------------------------------------------------------------------------------
    // AG acquisition and release (§11.1, §11.7)

    /// The acquisition state of both AG kinds (stall trace): wanted, asked (and its class),
    /// refused how long ago, spare held.
    pub(crate) fn diag(&self, now_ns: u64) -> String {
        let ms = |t: Option<u64>| t.map(|t| (now_ns.saturating_sub(t) / 1_000_000) as i64).unwrap_or(-1);
        let mut out = String::new();
        for (name, s) in [("small", &self.slots[0]), ("ext", &self.slots[1])] {
            out.push_str(&format!(
                "{name}: want={} class={:?} asked={} asked_class={:?} refused_ms={} reserve_refused_ms={} spare={} answers={:?} ",
                s.want,
                s.class,
                s.asked,
                s.asked_class,
                ms(s.refused_at_ns),
                ms(s.reserve_refused_at_ns),
                s.spare.is_some(),
                s.answers
            ));
        }
        out.push_str(&format!("ags small={} ext={}", self.small.len(), self.extent.len()));
        out
    }

    /// AG requests for space a placement needed that the meta thread refused so far (stale
    /// ones included): a request parked on space since before one of them and for longer
    /// than `SPACE_WAIT_NS` gets `-OOM` (`PartitionStore::exec`).
    pub(crate) fn refusals(&self) -> u64 {
        self.refusals
    }

    /// Asks for a spare of `kind` (at startup and when a spare was consumed).
    pub(crate) fn want_spare(&mut self, kind: AgKind) {
        let s = &mut self.slots[kind_slot(kind)];
        if s.spare.is_none() {
            s.want = true;
        }
    }

    /// AG requests to send now: `(kind, wanted device, spare)`. A spare request (`spare`) only
    /// keeps an AG ahead and may be denied without it counting as the fill limit; a request for
    /// space a placement needs now is a plain `AcquireAg`. A spare on another device than the
    /// one wanted does not stop a request for the wanted device.
    pub(crate) fn take_ag_requests(&mut self, out: &mut SmallVec<[AgRequest; 2]>) {
        for (i, kind) in [(0, AgKind::Small), (1, AgKind::Extent)] {
            let s = &mut self.slots[i];
            let spare_ok = match (s.spare, s.want_dev) {
                (None, _) => true,
                (Some(ag), Some(d)) => ag.dev != d,
                (Some(_), None) => false,
            };
            if s.want && !s.asked && spare_ok && (s.refused_at_ns.is_none() || s.class == AgClass::Reserved) {
                s.want = false;
                s.asked = true;
                let class = if s.want_dev.is_some() { s.class.max(AgClass::Need) } else { s.class };
                s.asked_class = class;
                s.asked_class_was_reserved = class == AgClass::Reserved;
                s.class = AgClass::Spare;
                out.push((kind, s.want_dev.take(), class));
            }
        }
    }

    /// The meta thread answered an AG request of `kind`. A denied spare request is no
    /// refusal: the next placement that needs space asks for an AG of its own.
    pub(crate) fn ag_granted(&mut self, kind: AgKind, ag: Option<AgId>, now_ns: u64) {
        let ki = kind_slot(kind);
        if ag.is_none() && self.slots[ki].asked_class != AgClass::Spare {
            self.refusals += 1;
        }
        self.slots[ki].asked = false;
        let stale = core::mem::take(&mut self.slots[ki].stale_refusal);
        let spare = core::mem::take(&mut self.slots[ki].asked_class) == AgClass::Spare;
        match ag {
            Some(ag) => {
                self.slots[ki].answers[0] += 1;
                // a spare already held (on another device) becomes an owned AG
                if let Some(old) = self.slots[ki].spare.replace(ag) {
                    match kind {
                        AgKind::Small => {
                            self.add_small_ag(old);
                        }
                        _ => {
                            self.add_extent_ag(old);
                        }
                    }
                }
                self.slots[ki].refused_at_ns = None;
                self.slots[ki].stale_retries = 0;
            }
            None if spare => self.slots[ki].answers[1] += 1,
            None if stale && self.slots[ki].stale_retries < MAX_STALE_RETRIES => {
                // answered before AGs this partition released were back in the pool: ask
                // again (behind the releases) instead of failing writes for the retry period
                self.slots[ki].stale_retries += 1;
                self.slots[ki].answers[2] += 1;
                self.slots[ki].want = true;
                self.slots[ki].class = self.slots[ki].class.max(AgClass::Need);
            }
            None if self.slots[1 - ki].spare.is_some() && !self.slots[ki].asked_class_was_reserved => {
                // refused while this partition holds an idle spare of the other kind: that
                // spare goes back to the pool and the request is sent again (behind the
                // release). Latched as a refusal, the write failed with -OOM for the retry
                // period while the partition sat on a whole free AG (after a FLUSHALL every
                // partition takes a SMALL spare first, so its first EXTENT AG was refused)
                self.slots[ki].answers[3] += 1;
                self.give_back = self.slots[1 - ki].spare.take();
                self.slots[1 - ki].want = false;
                self.slots[ki].want = true;
                self.slots[ki].class = self.slots[ki].class.max(AgClass::Need);
            }
            None => {
                self.slots[ki].answers[4] += 1;
                self.slots[ki].stale_retries = 0;
                self.slots[ki].refused_at_ns = Some(now_ns.max(1));
                if self.slots[ki].asked_class_was_reserved {
                    self.slots[ki].reserve_refused_at_ns = Some(now_ns.max(1));
                }
            }
        }
        self.slots[ki].asked_class_was_reserved = false;
        if ag.is_some() {
            self.slots[ki].reserve_refused_at_ns = None;
        }
    }

    /// A spare AG this partition gives back to the pool (see [`Alloc::ag_granted`]).
    pub(crate) fn take_give_back(&mut self) -> Option<AgId> {
        self.give_back.take()
    }

    /// True when the reserve above the fill limit refused an AG of `kind` within the retry
    /// period: a write that may use the reserve fails with `-OOM` instead of waiting.
    pub(crate) fn reserve_refused(&self, kind: AgKind, now_ns: u64) -> bool {
        self.slots[kind_slot(kind)].reserve_refused_at_ns.is_some_and(|t| now_ns.saturating_sub(t) < RETRY_NS)
    }

    /// True when an AG request of this partition was refused within the retry period: its
    /// writes get `-OOM` meanwhile, so work that gives space back is no longer background.
    pub(crate) fn refused_recently(&self, now_ns: u64) -> bool {
        self.slots.iter().any(|s| {
            [s.refused_at_ns, s.reserve_refused_at_ns].iter().flatten().any(|&t| now_ns.saturating_sub(t) < RETRY_NS)
        })
    }

    /// Whether the pool has AGs left under the fill limit ([`crate::metastore::MetaClient::pool_has_room`]),
    /// looked at before each placement.
    pub(crate) fn set_pool_room(&mut self, room: bool) {
        self.pool_room = room;
    }

    /// Clears the refusal so that the next need asks again (space was released).
    pub(crate) fn clear_refusal(&mut self) {
        for s in &mut self.slots {
            s.refused_at_ns = None;
            s.reserve_refused_at_ns = None;
            s.stale_refusal = s.asked;
        }
    }

    /// True while some AG is wanted or requested and not answered: a request parked on space
    /// is woken by that answer (liveness).
    pub(crate) fn ag_outstanding(&self) -> bool {
        self.slots.iter().any(|s| s.asked || (s.want && (s.refused_at_ns.is_none() || s.class == AgClass::Reserved)))
    }

    /// An accepted write needs an AG of `kind` to complete (a staged record moves, §14.3): the
    /// request is served above the fill limit and is sent even while a refusal is latched.
    pub(crate) fn want_reserved(&mut self, kind: AgKind) -> bool {
        let s = &mut self.slots[kind_slot(kind)];
        if s.spare.is_none() {
            s.want = true;
            s.class = AgClass::Reserved;
            return true;
        }
        false
    }

    /// True while an AG of `kind` is requested and not answered.
    #[cfg(test)]
    pub(crate) fn ag_pending(&self, kind: AgKind) -> bool {
        let s = &self.slots[kind_slot(kind)];
        s.asked || s.want
    }

    /// An idle spare AG to give back under AG pressure (§11.1): only one whose kind the
    /// partition already writes in (its first AG of a kind is never given back) and that no
    /// request is waiting for. The slot stops wanting a spare until a placement needs one.
    pub(crate) fn take_idle_spare(&mut self) -> Option<AgId> {
        for ki in 0..2 {
            let owned = if ki == 0 { !self.small.is_empty() } else { !self.extent.is_empty() };
            let s = &mut self.slots[ki];
            if owned && !s.asked && s.class != AgClass::Need && s.class != AgClass::Reserved {
                if let Some(ag) = s.spare.take() {
                    s.want = false;
                    return Some(ag);
                }
            }
        }
        None
    }

    /// The spare AGs (owned per the AG table but holding nothing).
    pub(crate) fn spares(&self) -> impl Iterator<Item = (AgId, AgKind)> + '_ {
        self.slots[0]
            .spare
            .map(|a| (a, AgKind::Small))
            .into_iter()
            .chain(self.slots[1].spare.map(|a| (a, AgKind::Extent)))
    }

    /// An owned AG that holds nothing and can be released, per kind: SMALL with every page
    /// fresh and none of its pages passing `busy`, EXTENT with no allocated page. An empty AG
    /// first fills a missing spare slot instead of being released.
    pub(crate) fn take_releasable(&mut self, busy: &dyn Fn(PageId) -> bool) -> Option<AgId> {
        if let Some(si) = self.small.iter().position(|a| u32::from(a.fresh) == AG_PAGES) {
            let a = &self.small[si];
            let (dev, first) = (a.ag.dev, a.first);
            if (0..AG_PAGES).any(|i| busy(PageId::new(dev, first + i))) {
                return None;
            }
            let ag = self.remove_small(si);
            if self.small.is_empty() || self.slots[0].spare.is_none() {
                // keep it: as the spare (or as the only AG) it saves a meta round trip
                if self.slots[0].spare.is_none() && !self.slots[0].asked {
                    self.slots[0].spare = Some(ag);
                    self.slots[0].want = false;
                    return None;
                }
                self.add_small_ag(ag);
                return None;
            }
            return Some(ag);
        }
        if let Some(ei) = self.extent.iter().position(ExtentAg::is_empty) {
            let ag = self.remove_extent(ei);
            if self.slots[1].spare.is_none() && !self.slots[1].asked {
                self.slots[1].spare = Some(ag);
                self.slots[1].want = false;
                return None;
            }
            return Some(ag);
        }
        None
    }

    fn remove_small(&mut self, si: usize) -> AgId {
        let last = self.small.len() - 1;
        let base = self.small[si].base as usize;
        if si != last {
            // swap-remove: the last AG's 4096 bytes move into the hole (O(4 KiB), §11.2)
            let lb = self.small[last].base as usize;
            self.page_free.copy_within(lb..lb + AG_PAGES as usize, base);
            self.small[last].base = base as u32;
        }
        self.page_free.truncate(self.page_free.len() - AG_PAGES as usize);
        let removed = self.small.swap_remove(si);
        self.small_of.remove(&removed.ag);
        if si < self.small.len() {
            self.small_of.insert(self.small[si].ag, si as u32);
        }
        let (dev, first) = (removed.ag.dev, removed.first);
        for i in 0..AG_PAGES {
            self.cached.remove(PageId::new(dev, first + i));
        }
        removed.ag
    }

    fn remove_extent(&mut self, ei: usize) -> AgId {
        let removed = self.extent.swap_remove(ei);
        self.extent_of.remove(&removed.ag);
        if ei < self.extent.len() {
            self.extent_of.insert(self.extent[ei].ag, ei as u32);
        }
        removed.ag
    }

    /// FLUSHALL reset (§16.5): every owned AG and spare is given up; returns them for release.
    pub(crate) fn release_all(&mut self) -> Vec<AgId> {
        let mut out: Vec<AgId> = self.owned().map(|(a, _)| a).collect();
        out.extend(self.spares().map(|(a, _)| a));
        self.page_free.clear();
        self.small.clear();
        self.small_of.clear();
        self.extent.clear();
        self.extent_of.clear();
        self.cached = CachedSpace::default();
        // every run is released: header reads in flight decide nothing any more
        self.hdr_watch.clear();
        for s in &mut self.slots {
            s.spare = None;
            s.want = false;
            s.refused_at_ns = None;
            // a refusal of a request in flight was answered before these AGs come back
            s.stale_refusal = s.asked;
        }
        out
    }

    /// Fresh pages left in owned SMALL AGs (tests).
    #[cfg(test)]
    pub(crate) fn fresh_pages(&self) -> u64 {
        self.small.iter().map(|a| u64::from(a.fresh)).sum()
    }

    /// Owned SMALL pages that are not fresh (FLUSHDB reclaim walk, §16.5).
    pub(crate) fn owned_small_pages(&self) -> impl Iterator<Item = PageId> + '_ {
        self.small.iter().flat_map(move |a| {
            (0..AG_PAGES).filter(move |&i| self.page_free[(a.base + i) as usize] != FRESH).map(move |i| PageId::new(a.ag.dev, a.first + i))
        })
    }

    /// First pages of every allocated extent run (FLUSHDB reclaim walk, snapshot checks).
    pub(crate) fn extent_starts(&self) -> impl Iterator<Item = PageId> + '_ {
        self.extent.iter().flat_map(|a| (0..AG_PAGES).filter(move |&i| a.start.get(i)).map(move |i| PageId::new(a.ag.dev, a.first + i)))
    }

    /// Cached SMALL pages known to the best-fit search.
    pub(crate) fn cached_pages(&self) -> usize {
        self.cached.len()
    }

    /// Page `i` of the AG whose first page is `first` on `dev` (AG release).
    pub(crate) fn page_of_first(&self, dev: u8, first: u32, i: u32) -> PageId {
        PageId::new(dev, first + i)
    }

    /// Recovery: the occupied bytes of page `page` of an owned SMALL AG (0 = no valid
    /// record: the page is fresh again).
    pub(crate) fn recovered_page(&mut self, page: PageId, occupied: u16) {
        if occupied == 0 {
            self.set_page_free(page, FRESH);
        } else {
            self.set_page_free(page, free_code(PAGE_ROOM.saturating_sub(occupied)));
        }
    }

    /// Page `i` of a recovered AG (recovery walks AGs by index).
    pub(crate) fn ag_page(&self, ag: AgId, i: u32) -> Option<PageId> {
        self.page_of(ag, i)
    }

    /// Owned EXTENT AGs (snapshot sections): `(ag, alloc bits, start bits)`.
    pub(crate) fn extent_ags(&self) -> impl Iterator<Item = (AgId, &Bits4096, &Bits4096)> + '_ {
        self.extent.iter().map(|e| (e.ag, &e.alloc, &e.start))
    }

    /// Owned SMALL AGs with their `page_free` bytes (snapshot sections).
    pub(crate) fn small_ags(&self) -> impl Iterator<Item = (AgId, &[u8])> + '_ {
        self.small.iter().map(|a| (a.ag, &self.page_free[a.base as usize..(a.base + AG_PAGES) as usize]))
    }

    /// Replaces the `page_free` bytes of an owned SMALL AG (snapshot restore).
    pub(crate) fn set_small_free(&mut self, ag: AgId, bytes: &[u8]) {
        if bytes.len() != AG_PAGES as usize {
            return;
        }
        if let Some(&si) = self.small_of.get(&ag) {
            let b = self.small[si as usize].base as usize;
            self.page_free[b..b + AG_PAGES as usize].copy_from_slice(bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layouts() -> Arc<[DeviceLayout]> {
        let l = DeviceLayout::compute(64 * 4096, true, 0).unwrap();
        Arc::from(vec![l])
    }

    fn alloc(mode: PageUpdateMode) -> Alloc {
        Alloc::new(3, layouts(), KeyedPick::new(42, 1), mode, 1024, 128)
    }

    #[test]
    fn keyed_pick_selects_set_bits_only() {
        let mut k = KeyedPick::new(7, 3);
        let words = [0u64, 1 << 5 | 1 << 63, 0, 1];
        let mut seen = [0u32; 4];
        for _ in 0..400 {
            let i = k.pick(1, &words).unwrap();
            let slot = match i {
                69 => 0,
                127 => 1,
                192 => 2,
                other => panic!("picked unset bit {other}"),
            };
            seen[slot] += 1;
        }
        assert!(seen[..3].iter().all(|&c| c > 80), "{seen:?}");
        assert_eq!(k.pick(1, &[0, 0]), None);
        assert_eq!(k.pick_index(1, 0), None);
        assert!(k.pick_index(1, 5).unwrap() < 5);
        assert_eq!(k.draws(), 401);
    }

    #[test]
    fn keyed_pick_is_deterministic_per_seed_and_boot() {
        let run = |seed, boot| {
            let mut k = KeyedPick::new(seed, boot);
            (0..32).map(|_| k.pick_index(9, 1000).unwrap()).collect::<Vec<_>>()
        };
        assert_eq!(run(1, 1), run(1, 1));
        assert_ne!(run(1, 1), run(1, 2));
        assert_ne!(run(1, 1), run(2, 1));
    }

    #[test]
    fn fresh_pages_are_drawn_not_sequential() {
        let mut a = alloc(PageUpdateMode::TwoImage);
        let cache = PageCache::new(1 << 20, false);
        assert!(a.add_small_ag(AgId { dev: 0, idx: 0 }));
        let mut pages = Vec::new();
        for _ in 0..64 {
            match a.place_small(352, &cache, 0, &|_| false) {
                SmallPlace::Fresh(p) => pages.push(p.page()),
                other => panic!("{other:?}"),
            }
        }
        let mut sorted = pages.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), 64, "no page handed out twice");
        assert_ne!(pages, sorted, "fresh pages are not handed out in address order");
        assert_eq!(a.fresh_pages(), 4096 - 64);
    }

    #[test]
    fn spare_and_refusal() {
        let mut a = alloc(PageUpdateMode::TwoImage);
        let cache = PageCache::new(1 << 20, false);
        assert_eq!(a.place_small(100, &cache, 0, &|_| false), SmallPlace::NeedAg);
        let mut req = SmallVec::new();
        a.take_ag_requests(&mut req);
        assert_eq!(req.as_slice(), &[(AgKind::Small, None, AgClass::Need)], "space needed now");
        req.clear();
        a.take_ag_requests(&mut req);
        assert!(req.is_empty(), "one request in flight");
        assert!(a.ag_outstanding());
        a.ag_granted(AgKind::Small, Some(AgId { dev: 0, idx: 2 }), 0);
        assert!(matches!(a.place_small(100, &cache, 0, &|_| false), SmallPlace::Fresh(_)));
        a.take_ag_requests(&mut req);
        assert_eq!(req.as_slice(), &[(AgKind::Small, None, AgClass::Spare)], "consumed spare is replaced by a spare request");
        // a denied spare is no fill-limit refusal: the next need asks again
        a.ag_granted(AgKind::Small, None, 10);
        assert!(!a.ag_pending(AgKind::Small));
        for _ in 0..4095 {
            assert!(matches!(a.place_small(4000, &cache, 20, &|_| false), SmallPlace::Fresh(_)));
        }
        assert_eq!(a.place_small(4000, &cache, 20, &|_| false), SmallPlace::NeedAg);
        req.clear();
        a.take_ag_requests(&mut req);
        assert_eq!(req.as_slice(), &[(AgKind::Small, None, AgClass::Need)]);
        // a refused need is the fill limit: -OOM until the retry interval passed
        a.ag_granted(AgKind::Small, None, 30);
        assert_eq!(a.place_small(4000, &cache, 40, &|_| false), SmallPlace::Full);
        req.clear();
        a.take_ag_requests(&mut req);
        assert!(req.is_empty(), "the refusal is latched");
        // an accepted write that must move still asks, above the fill limit
        a.want_reserved(AgKind::Small);
        assert!(a.ag_outstanding());
        a.take_ag_requests(&mut req);
        assert_eq!(req.as_slice(), &[(AgKind::Small, None, AgClass::Reserved)]);
    }

    /// A refusal answered while the partition released AGs is asked again, but at most
    /// `MAX_STALE_RETRIES` times in a row: a partition that keeps releasing (overwrites on a
    /// full device) latched no refusal at all, and its writes parked for seconds.
    #[test]
    fn stale_refusals_are_asked_again_a_bounded_number_of_times() {
        let mut a = alloc(PageUpdateMode::TwoImage);
        let cache = PageCache::new(1 << 20, false);
        let mut req = SmallVec::new();
        let mut asks = 0;
        let mut now = 0;
        loop {
            now += 1;
            match a.place_small(100, &cache, now, &|_| false) {
                SmallPlace::NeedAg => {}
                SmallPlace::Full => break,
                other => panic!("{other:?}"),
            }
            req.clear();
            a.take_ag_requests(&mut req);
            assert_eq!(req.len(), 1, "one request");
            asks += 1;
            // the partition releases an AG while the request is in flight; the answer is a
            // refusal that predates the release
            a.clear_refusal();
            let before = a.refusals();
            a.ag_granted(AgKind::Small, None, now);
            assert_eq!(a.refusals(), before + 1);
            assert!(asks <= usize::from(MAX_STALE_RETRIES) + 1, "asked {asks} times");
        }
        assert_eq!(asks, usize::from(MAX_STALE_RETRIES) + 1);
    }

    #[test]
    fn extent_twin_best_fit_and_carving() {
        let mut a = alloc(PageUpdateMode::TwoImage);
        assert_eq!(a.alloc_extent(4, None, 0, None), ExtPlace::NeedAg);
        a.ag_granted(AgKind::Extent, Some(AgId { dev: 0, idx: 1 }), 0);
        let ExtPlace::Run(e1) = a.alloc_extent(4, None, 0, None) else { panic!() };
        let first = a.ag_first(AgId { dev: 0, idx: 1 }).unwrap();
        assert_eq!((e1.page() - first) % 4, 0, "carved at an n-aligned offset");
        assert!(a.extent_allocated(e1, 4));
        let ExtPlace::Run(e2) = a.alloc_extent(4, None, 0, None) else { panic!() };
        assert_ne!(e1, e2);
        // free e1, then the twin preference takes exactly that run
        a.free_extent(e1, 4);
        assert!(!a.extent_allocated(e1, 4));
        assert_eq!(a.alloc_extent(4, Some(e1), 0, None), ExtPlace::Run(e1));
        // free everything: the AG is one run again and becomes releasable (kept as spare)
        a.free_extent(e1, 4);
        a.free_extent(e2, 4);
        assert!(a.extent[0].is_empty());
        assert_eq!(a.take_releasable(&|_| false), None);
        assert_eq!(a.spares().count(), 1);
    }

    #[test]
    fn extent_runs_merge_on_free() {
        let mut e = ExtentAg::new(AgId { dev: 0, idx: 0 }, 0);
        e.take(0, 10);
        e.take(10, 10);
        e.take(20, 10);
        e.give_back(10, 10);
        assert!(e.runs.contains(&(10, 10)));
        e.give_back(0, 10);
        assert!(e.runs.contains(&(20, 0)));
        e.give_back(20, 10);
        assert!(e.is_empty());
        e.alloc.set_range(5, 3);
        e.rebuild_runs();
        assert_eq!(e.runs, vec![(5, 0), (4088, 8)]);
    }

    #[test]
    fn cached_best_fit_respects_slack() {
        let mut a = alloc(PageUpdateMode::TwoImage);
        let mut cache = PageCache::new(1 << 20, false);
        a.add_small_ag(AgId { dev: 0, idx: 0 });
        let p = a.page_of(AgId { dev: 0, idx: 0 }, 7).unwrap();
        cache.alloc(p).unwrap();
        a.note_cached(p, 900, 400, 900);
        // 900 - 352 = 548 >= min(max(352, 400), 1024) = 400
        assert_eq!(a.place_small(352, &cache, 0, &|_| false), SmallPlace::Cached(p));
        // 900 - 500 = 400 < 500: no
        assert!(matches!(a.place_small(500, &cache, 0, &|_| false), SmallPlace::Fresh(_)));
        assert!(a.qualifies(100, 300, 100));
        assert!(!a.qualifies(100, 150, 100));
    }

    #[test]
    fn uncached_hint_search_and_release() {
        let mut a = alloc(PageUpdateMode::Atomic);
        let cache = PageCache::new(1 << 20, false);
        let ag = AgId { dev: 0, idx: 0 };
        a.add_small_ag(ag);
        for i in 0..AG_PAGES {
            let p = a.page_of(ag, i).unwrap();
            a.set_page_free(p, if i == 77 { 40 } else { 2 });
        }
        a.finish_rebuild();
        let want = a.page_of(ag, 77).unwrap();
        assert_eq!(a.place_small(352, &cache, 0, &|_| false), SmallPlace::Uncached(want));
        for i in 0..AG_PAGES {
            a.note_fresh(a.page_of(ag, i).unwrap());
        }
        // the only AG is kept (spare slot is empty)
        assert_eq!(a.take_releasable(&|_| false), None);
    }
}
