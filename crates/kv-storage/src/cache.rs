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
//! This is one of the files of this crate allowed to contain `unsafe` (§24); every block carries
//! a `SAFETY:` comment.
#![allow(unsafe_code)]

use crate::layout::{PAGE, PageId};

/// Index of a frame in its worker's cache.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct FrameIdx(pub u32);

/// Per-frame metadata.
pub(crate) struct FrameMeta {
    pub(crate) page: Option<PageId>,
    pub(crate) pin: u16,
    pub(crate) visited: bool,
    pub(crate) page_state: Option<Box<crate::image::PageState>>,
}

pub struct PageCache {
    map: hashbrown::HashMap<PageId, FrameIdx, foldhash::fast::RandomState>,
    meta: Vec<FrameMeta>,
    hand: usize,
    used: usize,
    huge: bool,
}

impl PageCache {
    pub fn new(bytes: usize, huge: bool) -> Self {
        todo!("DESIGN §12: page cache mapping")
    }

    pub fn get(&mut self, p: PageId) -> Option<FrameIdx> {
        todo!("DESIGN §12: lookup (sets visited)")
    }

    /// A free or evicted frame for `p`.
    pub fn alloc(&mut self, p: PageId) -> Option<FrameIdx> {
        todo!("DESIGN §12: frame allocation")
    }

    pub fn frame(&self, f: FrameIdx) -> &[u8; PAGE] {
        todo!("DESIGN §12: frame view")
    }

    pub fn frame_mut(&mut self, f: FrameIdx) -> &mut [u8; PAGE] {
        todo!("DESIGN §12: frame view")
    }

    pub fn pin(&mut self, f: FrameIdx) {
        todo!("DESIGN §12: pin")
    }

    pub fn unpin(&mut self, f: FrameIdx) {
        todo!("DESIGN §12: unpin")
    }

    /// SIEVE eviction of one frame whose page passes `can`.
    pub fn evict_one(&mut self, can: &dyn Fn(PageId) -> bool) -> Option<PageId> {
        todo!("DESIGN §12: SIEVE eviction")
    }

    /// Frames in use.
    pub fn used(&self) -> usize {
        self.used
    }
}
