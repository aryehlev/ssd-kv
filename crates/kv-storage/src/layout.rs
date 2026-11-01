//! Geometry, identifiers and size classes (DESIGN §10.1, §10.9, §10.11).
//!
//! Page = 4096 B; AG = 4096 pages = 16 MiB. `PageId = dev << 32 | page_in_dev` (byte offset
//! `page_in_dev * 4096` on device `dev`; at most 16 devices so that a page id fits the index's
//! 36-bit `page36`). Device layout (every device): superblock A/B at pages 0/1, the AG table
//! from page 4 (block i copy A at `4 + 2i`, copy B at `4 + 2i + 1`), then on device 0 the
//! partition metas (2 x 128 pages), node meta A/B, intent header A/B and 128 intent-slot pages;
//! then on every device the two fixed snapshot regions and the AG area, all on 16 MiB
//! boundaries.
//!
//! `Seq` = `term:24 << 40 | counter:40`, per partition, strictly increasing (§10.11).
//! Extent size classes (entry `x8`): `CLASS_PAGES[0..32] = 1..=32`, then
//! `p[i] = max(p[i-1] + 1, ceil(p[i-1] * 1.025))` until 2048 pages (8 MiB).

pub const PAGE: usize = 4096;
pub const AG_PAGES: u32 = 4096;
pub const SMALL_HDR: usize = 32;
pub const SMALL_REC_MAX: usize = 2048;
pub const EXT_HDR: usize = 80;
pub const EXT_MAX_PAGES: u32 = 2048;
/// Most devices per node (the device id has 4 bits in `page36`).
pub const MAX_DEVICES: usize = 16;

/// `dev << 32 | page_in_dev`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct PageId(pub u64);

impl PageId {
    /// Only called by the allocator, the formatter and recovery (LC-08 lint).
    pub fn new(dev: u8, page: u32) -> Self {
        debug_assert!(usize::from(dev) < MAX_DEVICES);
        PageId((u64::from(dev) << 32) | u64::from(page))
    }

    pub fn dev(self) -> u8 {
        (self.0 >> 32) as u8
    }

    pub fn page(self) -> u32 {
        self.0 as u32
    }

    /// The index's 36-bit page reference (same bit layout).
    pub fn to36(self) -> u64 {
        self.0 & ((1u64 << 36) - 1)
    }

    pub fn from36(v: u64) -> Self {
        PageId(v & ((1u64 << 36) - 1))
    }

    /// Byte offset on its device.
    pub fn byte_offset(self) -> u64 {
        u64::from(self.page()) * PAGE as u64
    }
}

/// An allocation group: device and AG index inside that device's AG area.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct AgId {
    pub dev: u8,
    pub idx: u32,
}

/// Record version: `term << 40 | counter`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct Seq(pub u64);

impl Seq {
    /// Bits of the counter part.
    pub const COUNTER_BITS: u32 = 40;

    pub fn new(term: u32, counter: u64) -> Self {
        debug_assert!(counter < (1u64 << Self::COUNTER_BITS));
        debug_assert!(term < (1u32 << 24));
        Seq((u64::from(term) << Self::COUNTER_BITS) | counter)
    }

    pub fn term(self) -> u32 {
        (self.0 >> Self::COUNTER_BITS) as u32
    }

    pub fn counter(self) -> u64 {
        self.0 & ((1u64 << Self::COUNTER_BITS) - 1)
    }
}

/// Pages of extent size class `class`.
pub fn class_pages(class: u8) -> u32 {
    todo!("DESIGN §10.9: size class table")
}

/// Smallest size class holding `pages` pages, `None` above 2048 pages.
pub fn class_for(pages: u32) -> Option<u8> {
    todo!("DESIGN §10.9: size class lookup")
}
