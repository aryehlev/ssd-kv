//! Page cache (DESIGN §12).
//!
//! One `PageCache` per worker, shared by its partitions. Frames are 4096-byte aligned, carved
//! from 2 MiB huge-page mappings (fallback: anonymous mappings with transparent huge pages),
//! `cache-size / W` bytes per worker, marked `MADV_DONTDUMP` (LC-32), optionally registered
//! as fixed io_uring buffers. Map: `PageId -> FrameIdx` (hashbrown + foldhash). Eviction is
//! SIEVE: a hand walks the frames, a visited frame is cleared and skipped; a frame is evictable
//! only if unpinned, with no pending change, no write in flight, no blocked drop and no tracked
//! stale copy. Concurrent loads of one page share the loading frame's FIFO waiter list. A page
//! write copies the built image into a separate frame, which becomes the page's durable image
//! when the write is durable.
//!
//! Implementation notes: mappings are made lazily, one 2 MiB chunk at a time, up to the
//! configured size. Partitions pin every frame that must stay (dirty pages, pages with a write in
//! flight, pages prefetched for a request that has not executed yet, image frames being
//! written), so the SIEVE hand only ever evicts clean, unpinned pages. When every frame is pinned
//! the cache maps one more chunk beyond its nominal size instead of failing: a consumed command
//! never waits for cache space (LC-15); the overshoot is reported by [`PageCache::overshoot`].
//! The in-RAM page state of a cached page ([`PageState`]) lives in its frame's metadata and is
//! dropped with the frame.
//!
//! This is one of the files of this crate allowed to contain `unsafe` (§24); every block carries
//! a `SAFETY:` comment.
#![allow(unsafe_code)]

use core::ptr::NonNull;

use hashbrown::HashMap;

use crate::image::PageState;
use crate::layout::{PAGE, PageId};

/// Index of a frame in its worker's cache.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct FrameIdx(pub u32);

/// Frames per mapping chunk (2 MiB).
const CHUNK_FRAMES: usize = 512;
const CHUNK_BYTES: usize = CHUNK_FRAMES * PAGE;

/// Per-frame metadata.
pub(crate) struct FrameMeta {
    pub(crate) page: Option<PageId>,
    pub(crate) pin: u16,
    pub(crate) visited: bool,
    pub(crate) page_state: Option<Box<PageState>>,
    /// The frame holds a page or a scratch image (not on the free list).
    in_use: bool,
}

/// One private anonymous mapping of `CHUNK_BYTES`.
struct Chunk {
    ptr: NonNull<u8>,
    huge: bool,
}

// SAFETY: a chunk exclusively owns its mapping; the cache that holds it hands out frame slices
// only through `&self` / `&mut self` borrows, so moving it to another thread is sound.
unsafe impl Send for Chunk {}

impl Chunk {
    fn map(huge: bool) -> Option<Chunk> {
        let prot = libc::PROT_READ | libc::PROT_WRITE;
        let flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS;
        let mut got_huge = false;
        let mut raw = libc::MAP_FAILED;
        if huge {
            // SAFETY: anonymous private mapping with no address hint; the result is checked.
            raw = unsafe { libc::mmap(core::ptr::null_mut(), CHUNK_BYTES, prot, flags | libc::MAP_HUGETLB, -1, 0) };
            got_huge = raw != libc::MAP_FAILED;
        }
        if raw == libc::MAP_FAILED {
            // SAFETY: as above, without huge pages.
            raw = unsafe { libc::mmap(core::ptr::null_mut(), CHUNK_BYTES, prot, flags, -1, 0) };
        }
        if raw == libc::MAP_FAILED {
            return None;
        }
        // SAFETY: `raw` is a mapping of CHUNK_BYTES we own; madvise only changes paging and
        // dump behavior (failures are harmless and ignored).
        unsafe {
            if !got_huge {
                libc::madvise(raw, CHUNK_BYTES, libc::MADV_HUGEPAGE);
            }
            libc::madvise(raw, CHUNK_BYTES, libc::MADV_DONTDUMP);
        }
        NonNull::new(raw.cast::<u8>()).map(|ptr| Chunk { ptr, huge: got_huge })
    }

    #[inline]
    fn frame_ptr(&self, i: usize) -> *mut u8 {
        debug_assert!(i < CHUNK_FRAMES);
        // SAFETY: `i < CHUNK_FRAMES`, so the offset stays inside the mapping.
        unsafe { self.ptr.as_ptr().add(i * PAGE) }
    }
}

impl Drop for Chunk {
    fn drop(&mut self) {
        // SAFETY: the mapping was created by `mmap` with CHUNK_BYTES and is unmapped once.
        unsafe {
            libc::munmap(self.ptr.as_ptr().cast(), CHUNK_BYTES);
        }
    }
}

pub struct PageCache {
    map: hashbrown::HashMap<PageId, FrameIdx, foldhash::fast::RandomState>,
    meta: Vec<FrameMeta>,
    chunks: Vec<Chunk>,
    free: Vec<u32>,
    /// Nominal size in frames.
    budget: usize,
    hand: usize,
    used: usize,
    huge: bool,
    hits: u64,
    misses: u64,
}

impl PageCache {
    /// A cache of `bytes` (at least 64 frames); `huge` asks for `MAP_HUGETLB` mappings.
    pub fn new(bytes: usize, huge: bool) -> Self {
        PageCache {
            map: HashMap::with_hasher(foldhash::fast::RandomState::default()),
            meta: Vec::new(),
            chunks: Vec::new(),
            free: Vec::new(),
            budget: (bytes / PAGE).max(64),
            hand: 0,
            used: 0,
            huge,
            hits: 0,
            misses: 0,
        }
    }

    /// Looks `p` up and marks it visited (SIEVE).
    pub fn get(&mut self, p: PageId) -> Option<FrameIdx> {
        match self.map.get(&p) {
            Some(&f) => {
                self.meta[f.0 as usize].visited = true;
                self.hits += 1;
                Some(f)
            }
            None => {
                self.misses += 1;
                None
            }
        }
    }

    /// Lookup without touching the SIEVE state or the hit counters.
    pub fn peek(&self, p: PageId) -> Option<FrameIdx> {
        self.map.get(&p).copied()
    }

    pub fn contains(&self, p: PageId) -> bool {
        self.map.contains_key(&p)
    }

    /// A free or evicted frame for `p` (the existing frame if `p` is cached already). The
    /// frame's bytes are unspecified; its page state is empty.
    pub fn alloc(&mut self, p: PageId) -> Option<FrameIdx> {
        if let Some(&f) = self.map.get(&p) {
            return Some(f);
        }
        let f = self.take_frame()?;
        let m = &mut self.meta[f.0 as usize];
        m.page = Some(p);
        m.visited = false;
        m.pin = 0;
        m.page_state = None;
        self.map.insert(p, f);
        Some(f)
    }

    /// A pinned frame mapped to no page (an image being written).
    pub fn alloc_scratch(&mut self) -> Option<FrameIdx> {
        let f = self.take_frame()?;
        let m = &mut self.meta[f.0 as usize];
        m.page = None;
        m.pin = 1;
        m.visited = false;
        m.page_state = None;
        Some(f)
    }

    /// Returns a scratch frame (or an unmapped frame) to the free list.
    pub fn free_scratch(&mut self, f: FrameIdx) {
        let m = &mut self.meta[f.0 as usize];
        debug_assert!(m.page.is_none(), "free_scratch of a mapped frame");
        if m.in_use {
            m.in_use = false;
            m.pin = 0;
            m.page_state = None;
            self.free.push(f.0);
            self.used -= 1;
        }
    }

    /// Makes scratch frame `n` the frame of `p` (its image became durable, §12): the page
    /// state, pins and visited bit move with it; the old frame is freed.
    pub fn promote(&mut self, p: PageId, n: FrameIdx) {
        let Some(old) = self.map.insert(p, n) else {
            let m = &mut self.meta[n.0 as usize];
            m.page = Some(p);
            m.pin = 0;
            return;
        };
        let (state, pin, visited) = {
            let o = &mut self.meta[old.0 as usize];
            (o.page_state.take(), o.pin, o.visited)
        };
        let m = &mut self.meta[n.0 as usize];
        m.page = Some(p);
        m.page_state = state;
        m.pin = pin;
        m.visited = visited;
        let o = &mut self.meta[old.0 as usize];
        o.page = None;
        o.pin = 0;
        o.in_use = false;
        self.free.push(old.0);
        self.used -= 1;
    }

    /// Drops `p` from the cache whatever its state (AG release, FLUSHALL reset).
    pub fn forget(&mut self, p: PageId) {
        if let Some(f) = self.map.remove(&p) {
            let m = &mut self.meta[f.0 as usize];
            m.page = None;
            m.page_state = None;
            m.pin = 0;
            m.in_use = false;
            self.free.push(f.0);
            self.used -= 1;
        }
    }

    fn take_frame(&mut self) -> Option<FrameIdx> {
        if self.free.is_empty() {
            if self.meta.len() >= self.budget {
                self.evict_for_space();
            }
            if self.free.is_empty() {
                // below the nominal size, or every frame is pinned
                self.grow()?;
            }
        }
        let i = self.free.pop()?;
        self.meta[i as usize].in_use = true;
        self.used += 1;
        Some(FrameIdx(i))
    }

    /// One SIEVE eviction of any unpinned page; true when a frame was freed.
    fn evict_for_space(&mut self) -> bool {
        self.evict_one(&|_| true).is_some()
    }

    fn grow(&mut self) -> Option<()> {
        let c = Chunk::map(self.huge)?;
        let base = self.meta.len() as u32;
        self.chunks.push(c);
        for i in 0..CHUNK_FRAMES as u32 {
            self.meta.push(FrameMeta { page: None, pin: 0, visited: false, page_state: None, in_use: false });
            self.free.push(base + CHUNK_FRAMES as u32 - 1 - i);
        }
        Some(())
    }

    #[inline]
    fn ptr(&self, f: FrameIdx) -> *mut u8 {
        let i = f.0 as usize;
        self.chunks[i / CHUNK_FRAMES].frame_ptr(i % CHUNK_FRAMES)
    }

    pub fn frame(&self, f: FrameIdx) -> &[u8; PAGE] {
        assert!((f.0 as usize) < self.meta.len(), "frame index out of range");
        // SAFETY: the frame lies inside a live mapping owned by `self`; the shared borrow of
        // `self` prevents a concurrent `&mut` view of any frame for the returned lifetime.
        unsafe { &*(self.ptr(f) as *const [u8; PAGE]) }
    }

    pub fn frame_mut(&mut self, f: FrameIdx) -> &mut [u8; PAGE] {
        assert!((f.0 as usize) < self.meta.len(), "frame index out of range");
        // SAFETY: as in `frame`; the exclusive borrow of `self` makes the view unique.
        unsafe { &mut *(self.ptr(f) as *mut [u8; PAGE]) }
    }

    /// Frame `src` for reading and the distinct frame `dst` for writing at once.
    pub fn pair(&mut self, src: FrameIdx, dst: FrameIdx) -> (&[u8; PAGE], &mut [u8; PAGE]) {
        assert!(src != dst, "pair of one frame");
        assert!((src.0 as usize) < self.meta.len() && (dst.0 as usize) < self.meta.len(), "frame index out of range");
        // SAFETY: distinct frames are disjoint 4096-byte ranges of live mappings owned by
        // `self`; the exclusive borrow of `self` prevents any other view for the lifetime.
        unsafe { (&*(self.ptr(src) as *const [u8; PAGE]), &mut *(self.ptr(dst) as *mut [u8; PAGE])) }
    }

    pub fn pin(&mut self, f: FrameIdx) {
        let m = &mut self.meta[f.0 as usize];
        m.pin = m.pin.saturating_add(1);
    }

    pub fn unpin(&mut self, f: FrameIdx) {
        let m = &mut self.meta[f.0 as usize];
        debug_assert!(m.pin > 0, "unpin of an unpinned frame");
        m.pin = m.pin.saturating_sub(1);
    }

    pub fn pins(&self, f: FrameIdx) -> u16 {
        self.meta[f.0 as usize].pin
    }

    /// SIEVE eviction of one frame whose page passes `can`.
    pub fn evict_one(&mut self, can: &dyn Fn(PageId) -> bool) -> Option<PageId> {
        let n = self.meta.len();
        if n == 0 {
            return None;
        }
        for _ in 0..2 * n {
            let i = self.hand;
            self.hand = (self.hand + 1) % n;
            let m = &mut self.meta[i];
            let Some(p) = m.page else { continue };
            if !m.in_use || m.pin > 0 {
                continue;
            }
            if m.visited {
                m.visited = false;
                continue;
            }
            if !can(p) {
                continue;
            }
            self.forget(p);
            return Some(p);
        }
        None
    }

    /// Frames in use.
    pub fn used(&self) -> usize {
        self.used
    }

    /// Nominal capacity in frames.
    pub fn capacity(&self) -> usize {
        self.budget
    }

    /// Frames mapped beyond the nominal size because every frame was pinned.
    pub fn overshoot(&self) -> usize {
        self.meta.len().saturating_sub(self.budget.next_multiple_of(CHUNK_FRAMES))
    }

    /// True when at least one chunk is backed by explicit huge pages.
    pub fn huge_backed(&self) -> bool {
        self.chunks.iter().any(|c| c.huge)
    }

    /// `(hits, misses)` of [`PageCache::get`].
    pub fn hit_counts(&self) -> (u64, u64) {
        (self.hits, self.misses)
    }

    pub(crate) fn state(&self, f: FrameIdx) -> Option<&PageState> {
        self.meta[f.0 as usize].page_state.as_deref()
    }

    pub(crate) fn state_mut(&mut self, f: FrameIdx) -> Option<&mut PageState> {
        self.meta[f.0 as usize].page_state.as_deref_mut()
    }

    pub(crate) fn take_state(&mut self, f: FrameIdx) -> Option<Box<PageState>> {
        self.meta[f.0 as usize].page_state.take()
    }

    pub(crate) fn put_state(&mut self, f: FrameIdx, s: Box<PageState>) {
        self.meta[f.0 as usize].page_state = Some(s);
    }

    /// The page state of cached page `p`.
    pub(crate) fn page_state(&self, p: PageId) -> Option<&PageState> {
        self.peek(p).and_then(|f| self.state(f))
    }

    pub(crate) fn page_state_mut(&mut self, p: PageId) -> Option<&mut PageState> {
        let f = self.peek(p)?;
        self.state_mut(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pid(n: u32) -> PageId {
        PageId(u64::from(n) + 100)
    }

    #[test]
    fn alloc_get_frames_are_aligned_and_distinct() {
        let mut c = PageCache::new(64 * PAGE, false);
        let a = c.alloc(pid(1)).unwrap();
        let b = c.alloc(pid(2)).unwrap();
        assert_ne!(a, b);
        assert_eq!(c.alloc(pid(1)), Some(a), "existing frame returned");
        c.frame_mut(a).fill(7);
        c.frame_mut(b).fill(9);
        assert!(c.frame(a).iter().all(|&x| x == 7));
        assert_eq!(c.frame(a).as_ptr() as usize % PAGE, 0);
        assert_eq!(c.get(pid(1)), Some(a));
        assert_eq!(c.get(pid(3)), None);
        assert_eq!(c.used(), 2);
        let (s, d) = c.pair(a, b);
        d.copy_from_slice(s);
        assert!(c.frame(b).iter().all(|&x| x == 7));
    }

    #[test]
    fn sieve_skips_visited_and_pinned() {
        let mut c = PageCache::new(64 * PAGE, false);
        let fa = c.alloc(pid(1)).unwrap();
        let _fb = c.alloc(pid(2)).unwrap();
        let _fc = c.alloc(pid(3)).unwrap();
        c.pin(fa);
        c.get(pid(2)); // visited
        let ev = c.evict_one(&|_| true).unwrap();
        assert_eq!(ev, pid(3), "unvisited unpinned page goes first");
        let ev = c.evict_one(&|_| true).unwrap();
        assert_eq!(ev, pid(2), "visited bit was cleared by the first pass");
        assert_eq!(c.evict_one(&|_| true), None, "pinned page stays");
        c.unpin(fa);
        assert_eq!(c.evict_one(&|p| p != pid(1)), None, "predicate respected");
        assert_eq!(c.evict_one(&|_| true), Some(pid(1)));
        assert_eq!(c.used(), 0);
    }

    #[test]
    fn full_cache_evicts_then_grows_when_pinned() {
        let mut c = PageCache::new(64 * PAGE, false);
        let budget = c.capacity();
        for i in 0..budget as u32 {
            let f = c.alloc(pid(i)).unwrap();
            if i % 2 == 0 {
                c.pin(f);
            }
        }
        // beyond the mapped chunk: evictions keep the cache within one chunk
        for i in 0..400u32 {
            c.alloc(pid(10_000 + i)).unwrap();
        }
        assert!(c.used() <= CHUNK_FRAMES);
        // pin everything: allocation still succeeds by mapping another chunk
        for i in 0..CHUNK_FRAMES as u32 {
            if let Some(f) = c.peek(pid(10_000 + i)) {
                c.pin(f);
            }
        }
        for i in 0..CHUNK_FRAMES as u32 {
            if let Some(f) = c.peek(pid(i)) {
                if c.pins(f) == 0 {
                    c.pin(f);
                }
            }
        }
        for i in 0..CHUNK_FRAMES as u32 {
            let f = c.alloc(pid(50_000 + i)).unwrap();
            c.pin(f);
        }
        assert!(c.overshoot() > 0);
    }

    #[test]
    fn scratch_promote_moves_state() {
        let mut c = PageCache::new(64 * PAGE, false);
        let m = c.alloc(pid(5)).unwrap();
        c.pin(m);
        let n = c.alloc_scratch().unwrap();
        assert!(!c.contains(PageId(0)));
        c.frame_mut(n).fill(3);
        c.promote(pid(5), n);
        assert_eq!(c.peek(pid(5)), Some(n));
        assert_eq!(c.pins(n), 1);
        assert!(c.frame(n).iter().all(|&x| x == 3));
        assert_eq!(c.used(), 1);
        let s = c.alloc_scratch().unwrap();
        c.free_scratch(s);
        assert_eq!(c.used(), 1);
        c.forget(pid(5));
        assert_eq!(c.used(), 0);
    }
}
