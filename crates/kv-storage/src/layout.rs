//! Geometry, identifiers and size classes (DESIGN §10.1, §10.9, §10.11).
//!
//! Page = 4096 B; AG = 4096 pages = 16 MiB. `PageId = dev << 32 | page_in_dev` (byte offset
//! `page_in_dev * 4096` on device `dev`; at most 16 devices so that a page id fits the index's
//! 36-bit `page36`). Device layout (every device): superblock A/B at pages 0/1, the AG table
//! from page 4 (block i copy A at `4 + 2i`, copy B at `4 + 2i + 1`), then on device 0 the
//! partition metas (2 x 128 pages), node meta A/B, intent header A/B and 128 intent-slot pages;
//! then on every device the two fixed snapshot regions and the AG area, all on 16 MiB
//! boundaries ([`DeviceLayout`]).
//!
//! `Seq` = `term:24 << 40 | counter:40`, per partition, strictly increasing (§10.11).
//! Extent size classes (entry `x8`): `CLASS_PAGES[0..32] = 1..=32`, then
//! `p[i] = max(p[i-1] + 1, ceil(p[i-1] * 1.025))` until 2048 pages (8 MiB).

use kv_common::abblock::AbCopy;

pub const PAGE: usize = 4096;
pub const AG_PAGES: u32 = 4096;
pub const SMALL_HDR: usize = 32;
pub const SMALL_REC_MAX: usize = 2048;
pub const EXT_HDR: usize = 80;
pub const EXT_MAX_PAGES: u32 = 2048;
/// Most devices per node (the device id has 4 bits in `page36`).
pub const MAX_DEVICES: usize = 16;
/// Most partitions per node (P is 16, 32, 64 or 128; the partition meta area holds 128 pairs).
pub const MAX_PARTITIONS: u16 = 128;
/// Bytes of one AG.
pub const AG_BYTES: u64 = AG_PAGES as u64 * PAGE as u64;
/// Byte length of every fixed-location A/B metadata block (§10.2).
pub const AB_BLOCK: usize = kv_common::abblock::AB_BLOCK;
/// Offset of the payload inside an A/B block (§10.2).
pub const AB_PAYLOAD_OFF: usize = kv_common::abblock::AB_PAYLOAD_OFF;

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

    /// The page `n` pages after this one on the same device.
    pub fn advance(self, n: u32) -> Self {
        debug_assert!(self.page().checked_add(n).is_some());
        PageId(self.0 + u64::from(n))
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
    /// Bits of the term part (§10.11: 24 used).
    pub const TERM_BITS: u32 = 24;
    /// Largest counter value.
    pub const MAX_COUNTER: u64 = (1u64 << Self::COUNTER_BITS) - 1;
    /// Largest term value.
    pub const MAX_TERM: u32 = (1u32 << Self::TERM_BITS) - 1;
    /// "No version" (never assigned to a record: counters start at 1).
    pub const ZERO: Seq = Seq(0);

    pub fn new(term: u32, counter: u64) -> Self {
        debug_assert!(counter <= Self::MAX_COUNTER);
        debug_assert!(term <= Self::MAX_TERM);
        Seq((u64::from(term) << Self::COUNTER_BITS) | (counter & Self::MAX_COUNTER))
    }

    pub fn term(self) -> u32 {
        (self.0 >> Self::COUNTER_BITS) as u32
    }

    pub fn counter(self) -> u64 {
        self.0 & Self::MAX_COUNTER
    }

    /// The next version in the same term, `None` when the counter is exhausted.
    pub fn next(self) -> Option<Seq> {
        (self.counter() < Self::MAX_COUNTER).then(|| Seq(self.0 + 1))
    }
}

/// Number of extent size classes (the class id fits the index entry's `x8`).
pub const N_CLASSES: usize = class_table_len();

/// Pages of every size class, strictly increasing, `CLASS_PAGES[N_CLASSES - 1] == 2048`.
pub const CLASS_PAGES: [u16; N_CLASSES] = class_table();

/// `ceil(p * 1.025)` in exact integer arithmetic (`p * 41 / 40` rounded up).
const fn grow(p: u32) -> u32 {
    let g = (p * 41).div_ceil(40);
    let n = if g > p + 1 { g } else { p + 1 };
    if n > EXT_MAX_PAGES { EXT_MAX_PAGES } else { n }
}

const fn class_table_len() -> usize {
    let mut p = 32u32;
    let mut n = 32usize;
    while p < EXT_MAX_PAGES {
        p = grow(p);
        n += 1;
    }
    n
}

const fn class_table() -> [u16; N_CLASSES] {
    let mut t = [0u16; N_CLASSES];
    let mut i = 0;
    while i < 32 {
        t[i] = (i + 1) as u16;
        i += 1;
    }
    while i < N_CLASSES {
        t[i] = grow(t[i - 1] as u32) as u16;
        i += 1;
    }
    t
}

const _: () = assert!(N_CLASSES <= 256 && CLASS_PAGES[N_CLASSES - 1] as u32 == EXT_MAX_PAGES);

/// Pages of extent size class `class`.
///
/// Panics if `class >= N_CLASSES` (class ids come only from [`class_for`]).
pub fn class_pages(class: u8) -> u32 {
    u32::from(CLASS_PAGES[usize::from(class)])
}

/// Smallest size class holding `pages` pages, `None` above 2048 pages (0 is treated as 1).
pub fn class_for(pages: u32) -> Option<u8> {
    if pages <= 32 {
        return Some(pages.saturating_sub(1) as u8);
    }
    if pages > EXT_MAX_PAGES {
        return None;
    }
    let i = CLASS_PAGES.partition_point(|&p| u32::from(p) < pages);
    Some(i as u8)
}

/// The class whose size is exactly `pages`, if any (headers read from media must match one).
pub fn class_exact(pages: u32) -> Option<u8> {
    class_for(pages).filter(|&c| pages != 0 && class_pages(c) == pages)
}

/// Pages needed by an extent of `bytes` bytes (header included), at least one.
pub fn pages_for(bytes: u64) -> u64 {
    bytes.div_ceil(PAGE as u64).max(1)
}

// ---------------------------------------------------------------------------------------------
// Device layout (§10.1)

/// Superblock copies A and B.
pub const SB_PAGE: [u64; 2] = [0, 1];
/// First AG table page (block i copy c at `AG_TABLE_PAGE + 2i + c`).
pub const AG_TABLE_PAGE: u64 = 4;
/// Entries per AG table block (§10.4).
pub const AG_TABLE_ENTRIES: u32 = 2032;
/// Pages reserved for partition metas (`2 x MAX_PARTITIONS`).
pub const PMETA_PAGES: u64 = 2 * MAX_PARTITIONS as u64;
/// Intent slots and the pages reserved for them (one `lba` sector per slot copy).
pub const INTENT_SLOTS: u32 = 64;
pub const INTENT_SLOT_PAGES: u64 = 128;
/// Pages of the device-0 metadata area that follows the AG table:
/// partition metas, node meta A/B, intent header A/B, intent slots.
pub const DEV0_META_PAGES: u64 = PMETA_PAGES + 2 + 2 + INTENT_SLOT_PAGES;

/// Where every fixed structure of one device lives (all values in pages of that device).
///
/// Computed once at format and stored in the device's superblock; readers use the stored
/// values ([`DeviceLayout::from_superblock`]) and [`DeviceLayout::check`] them against the rules.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct DeviceLayout {
    pub device_pages: u64,
    pub n_ags: u32,
    /// `ceil(n_ags / 2032)`.
    pub ag_table_blocks: u32,
    /// Device 0 only (0 elsewhere).
    pub pmeta_page: u64,
    pub nmeta_page: u64,
    pub intent_page: u64,
    pub intent_slot_page: u64,
    /// Regions A and B (0 when `snap_region_pages == 0`).
    pub snap_region_page: [u64; 2],
    /// Pages per snapshot region (multiple of 4096; 0 = no regions).
    pub snap_region_pages: u64,
    pub ag_area_page: u64,
}

/// Why a device geometry cannot be laid out or is inconsistent.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LayoutError {
    /// The device has no room for one AG after its metadata and snapshot regions.
    TooSmall,
    /// The snapshot region size is not a multiple of 16 MiB.
    SnapshotRegionUnaligned,
    /// The device is larger than a 32-bit page index can address.
    TooLarge,
    /// Stored positions disagree with the format rules.
    Inconsistent(&'static str),
}

impl core::fmt::Display for LayoutError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            LayoutError::TooSmall => f.write_str("device too small for one allocation group"),
            LayoutError::SnapshotRegionUnaligned => f.write_str("snapshot region size is not a multiple of 16 MiB"),
            LayoutError::TooLarge => f.write_str("device larger than 2^32 pages"),
            LayoutError::Inconsistent(what) => write!(f, "inconsistent device layout: {what}"),
        }
    }
}

impl std::error::Error for LayoutError {}

impl DeviceLayout {
    /// Lays out a device of `device_pages` pages. `dev0` adds the device-0 metadata area;
    /// `snap_region_pages` (a multiple of 4096, or 0) is the size of each snapshot region.
    pub fn compute(device_pages: u64, dev0: bool, snap_region_pages: u64) -> Result<Self, LayoutError> {
        if device_pages > u64::from(u32::MAX) + 1 {
            return Err(LayoutError::TooLarge);
        }
        if !snap_region_pages.is_multiple_of(u64::from(AG_PAGES)) {
            return Err(LayoutError::SnapshotRegionUnaligned);
        }
        // The AG area starts at a 16 MiB boundary after the metadata and both regions, so the
        // number of AGs depends on the AG table size and vice versa. The AG count is
        // non-increasing in the table size and changes by at most one AG per table block, so the
        // smallest table size `t` that covers its own AG count exists and is found by a scan.
        let upper = device_pages / u64::from(AG_PAGES);
        let mut t = 1u64;
        loop {
            let l = Self::with_table(device_pages, dev0, snap_region_pages, t);
            let n = l.map_or(0, |l| u64::from(l.n_ags));
            if n.div_ceil(u64::from(AG_TABLE_ENTRIES)) <= t {
                let l = l.ok_or(LayoutError::TooSmall)?;
                if l.n_ags == 0 {
                    return Err(LayoutError::TooSmall);
                }
                return Ok(l);
            }
            t += 1;
            if t > upper.div_ceil(u64::from(AG_TABLE_ENTRIES)) + 1 {
                return Err(LayoutError::TooSmall);
            }
        }
    }

    /// Layout with `t` AG table blocks reserved (the stored block count is derived from the
    /// resulting AG count and is at most `t`).
    fn with_table(device_pages: u64, dev0: bool, snap_region_pages: u64, t: u64) -> Option<Self> {
        let table_end = AG_TABLE_PAGE + 2 * t;
        let (pmeta, nmeta, intent, slots, meta_end) = if dev0 {
            let pmeta = table_end.next_multiple_of(64);
            let nmeta = pmeta + PMETA_PAGES;
            let intent = nmeta + 2;
            let slots = intent + 2;
            (pmeta, nmeta, intent, slots, slots + INTENT_SLOT_PAGES)
        } else {
            (0, 0, 0, 0, table_end)
        };
        let first = meta_end.next_multiple_of(u64::from(AG_PAGES));
        let (regions, ag_area) = if snap_region_pages == 0 {
            ([0, 0], first)
        } else {
            ([first, first + snap_region_pages], first + 2 * snap_region_pages)
        };
        if ag_area > device_pages {
            return None;
        }
        let n_ags = (device_pages - ag_area) / u64::from(AG_PAGES);
        let n_ags = u32::try_from(n_ags).ok()?;
        Some(DeviceLayout {
            device_pages,
            n_ags,
            ag_table_blocks: n_ags.div_ceil(AG_TABLE_ENTRIES),
            pmeta_page: pmeta,
            nmeta_page: nmeta,
            intent_page: intent,
            intent_slot_page: slots,
            snap_region_page: regions,
            snap_region_pages,
            ag_area_page: ag_area,
        })
    }

    /// Checks stored positions against the format rules (regions and AG area on 16 MiB
    /// boundaries, no overlaps, everything inside the device).
    pub fn check(&self, dev0: bool) -> Result<(), LayoutError> {
        let ag = u64::from(AG_PAGES);
        let table_end = AG_TABLE_PAGE + 2 * u64::from(self.ag_table_blocks);
        if self.ag_table_blocks != self.n_ags.div_ceil(AG_TABLE_ENTRIES) || self.n_ags == 0 {
            return Err(LayoutError::Inconsistent("AG table size"));
        }
        let meta_end = if dev0 {
            if self.pmeta_page < table_end
                || !self.pmeta_page.is_multiple_of(64)
                || self.nmeta_page != self.pmeta_page + PMETA_PAGES
                || self.intent_page != self.nmeta_page + 2
                || self.intent_slot_page != self.intent_page + 2
            {
                return Err(LayoutError::Inconsistent("device-0 metadata area"));
            }
            self.intent_slot_page + INTENT_SLOT_PAGES
        } else {
            if self.pmeta_page | self.nmeta_page | self.intent_page | self.intent_slot_page != 0 {
                return Err(LayoutError::Inconsistent("metadata pages on a device other than 0"));
            }
            table_end
        };
        if !self.snap_region_pages.is_multiple_of(ag) || !self.ag_area_page.is_multiple_of(ag) {
            return Err(LayoutError::Inconsistent("16 MiB alignment"));
        }
        let regions_end = if self.snap_region_pages == 0 {
            if self.snap_region_page != [0, 0] {
                return Err(LayoutError::Inconsistent("snapshot region pages without regions"));
            }
            meta_end
        } else {
            let [a, b] = self.snap_region_page;
            if a < meta_end || !a.is_multiple_of(ag) || b != a + self.snap_region_pages {
                return Err(LayoutError::Inconsistent("snapshot regions"));
            }
            b + self.snap_region_pages
        };
        if self.ag_area_page < regions_end {
            return Err(LayoutError::Inconsistent("AG area overlaps metadata"));
        }
        let end = self.ag_area_page + u64::from(self.n_ags) * ag;
        if end > self.device_pages || self.device_pages > u64::from(u32::MAX) + 1 {
            return Err(LayoutError::Inconsistent("AG area beyond the device"));
        }
        Ok(())
    }

    /// First page of AG `idx` on this device.
    pub fn ag_first_page(&self, idx: u32) -> u64 {
        self.ag_area_page + u64::from(idx) * u64::from(AG_PAGES)
    }

    /// The AG holding device page `page`, if it is inside the AG area.
    pub fn ag_of_page(&self, page: u64) -> Option<u32> {
        let rel = page.checked_sub(self.ag_area_page)?;
        let idx = rel / u64::from(AG_PAGES);
        (idx < u64::from(self.n_ags)).then_some(idx as u32)
    }

    /// Page of copy `c` of AG table block `block`.
    pub fn ag_table_copy_page(block: u32, c: AbCopy) -> u64 {
        AG_TABLE_PAGE + 2 * u64::from(block) + c.index() as u64
    }

    /// Page of copy `c` of partition `p`'s meta block (device 0).
    pub fn pmeta_copy_page(&self, p: u16, c: AbCopy) -> u64 {
        self.pmeta_page + 2 * u64::from(p) + c.index() as u64
    }

    /// Page of copy `c` of the node meta block (device 0).
    pub fn nmeta_copy_page(&self, c: AbCopy) -> u64 {
        self.nmeta_page + c.index() as u64
    }

    /// Page of copy `c` of the intent table header (device 0).
    pub fn intent_header_copy_page(&self, c: AbCopy) -> u64 {
        self.intent_page + c.index() as u64
    }

    /// Byte offset on device 0 of copy `c` of intent slot `slot`: `(2s + c) * lba` past
    /// `intent_slot_page` (§10.7).
    pub fn intent_slot_copy_offset(&self, lba: u32, slot: u32, c: AbCopy) -> u64 {
        debug_assert!(slot < INTENT_SLOTS);
        self.intent_slot_page * PAGE as u64 + (2 * u64::from(slot) + c.index() as u64) * u64::from(lba)
    }
}

/// Picks the valid copy with the higher `block_seq` among two decoded copies (ties: A).
///
/// The writer of an A/B block always writes the copy this does *not* return, with
/// `block_seq + 1`, so a torn write damages only the copy that held the older state (§10.2).
pub fn ab_select<T>(a: Option<(u64, T)>, b: Option<(u64, T)>) -> Option<(AbCopy, u64, T)> {
    match (a, b) {
        (Some((sa, ta)), Some((sb, tb))) => {
            if sb > sa {
                Some((AbCopy::B, sb, tb))
            } else {
                Some((AbCopy::A, sa, ta))
            }
        }
        (Some((sa, ta)), None) => Some((AbCopy::A, sa, ta)),
        (None, Some((sb, tb))) => Some((AbCopy::B, sb, tb)),
        (None, None) => None,
    }
}

// ---------------------------------------------------------------------------------------------
// Little-endian field access shared by the codecs of §10 (bounds are the caller's contract).

pub(crate) mod le {
    #[inline]
    pub(crate) fn u16_at(b: &[u8], off: usize) -> u16 {
        u16::from_le_bytes([b[off], b[off + 1]])
    }

    #[inline]
    pub(crate) fn u32_at(b: &[u8], off: usize) -> u32 {
        u32::from_le_bytes(b[off..off + 4].try_into().expect("4-byte field"))
    }

    #[inline]
    pub(crate) fn u64_at(b: &[u8], off: usize) -> u64 {
        u64::from_le_bytes(b[off..off + 8].try_into().expect("8-byte field"))
    }

    #[inline]
    pub(crate) fn put_u16(b: &mut [u8], off: usize, v: u16) {
        b[off..off + 2].copy_from_slice(&v.to_le_bytes());
    }

    #[inline]
    pub(crate) fn put_u32(b: &mut [u8], off: usize, v: u32) {
        b[off..off + 4].copy_from_slice(&v.to_le_bytes());
    }

    #[inline]
    pub(crate) fn put_u64(b: &mut [u8], off: usize, v: u64) {
        b[off..off + 8].copy_from_slice(&v.to_le_bytes());
    }

    #[inline]
    pub(crate) fn bytes<const N: usize>(b: &[u8], off: usize) -> [u8; N] {
        b[off..off + N].try_into().expect("fixed-size field")
    }

    /// True when every byte of `b[range]` is zero.
    #[inline]
    pub(crate) fn zero(b: &[u8], range: core::ops::Range<usize>) -> bool {
        b[range].iter().all(|&x| x == 0)
    }
}

/// Encodes an A/B block whose payload is built by `fill` at *block* offsets (`0x018..0x1000`),
/// which is how §10 documents every field.
pub(crate) fn ab_build(magic: u64, block_seq: u64, out: &mut [u8; PAGE], fill: impl FnOnce(&mut [u8; PAGE])) {
    let mut tmp = [0u8; PAGE];
    fill(&mut tmp);
    kv_common::abblock::ab_encode(magic, block_seq, &tmp[AB_PAYLOAD_OFF..], out);
}

/// Validates an A/B block and hands the whole block (fields at block offsets) to `parse`.
pub(crate) fn ab_parse<T>(magic: u64, b: &[u8], parse: impl FnOnce(&[u8]) -> Option<T>) -> Option<(u64, T)> {
    if b.len() < PAGE {
        return None;
    }
    let (seq, _payload) = kv_common::abblock::ab_decode(magic, &b[..PAGE])?;
    parse(&b[..PAGE]).map(|t| (seq, t))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_id_packing() {
        let p = PageId((7u64 << 32) | 123_456);
        assert_eq!(p.dev(), 7);
        assert_eq!(p.page(), 123_456);
        assert_eq!(PageId::from36(p.to36()), p);
        assert_eq!(p.byte_offset(), 123_456 * 4096);
        assert_eq!(p.advance(10).page(), 123_466);
        // 36 bits: 4 device bits above the 32-bit page index.
        let max = PageId((15u64 << 32) | u64::from(u32::MAX));
        assert_eq!(max.to36(), (1u64 << 36) - 1);
        assert_eq!(PageId::from36(max.to36()), max);
        assert_eq!(PageId::from36(u64::MAX), max);
    }

    #[test]
    fn seq_packing() {
        let s = Seq::new(5, 77);
        assert_eq!(s.0, (5u64 << 40) | 77);
        assert_eq!(s.term(), 5);
        assert_eq!(s.counter(), 77);
        let top = Seq::new(Seq::MAX_TERM, Seq::MAX_COUNTER);
        assert_eq!(top.0, u64::MAX);
        assert_eq!(top.term(), Seq::MAX_TERM);
        assert_eq!(top.counter(), Seq::MAX_COUNTER);
        assert_eq!(top.next(), None);
        assert_eq!(Seq::new(1, 1).next(), Some(Seq::new(1, 2)));
        // Order: by term first, then counter.
        assert!(Seq::new(2, 1) > Seq::new(1, Seq::MAX_COUNTER));
        assert!(Seq::new(1, 2) > Seq::new(1, 1));
    }

    #[test]
    fn size_class_table() {
        assert_eq!(N_CLASSES, 184);
        for (i, &p) in CLASS_PAGES.iter().enumerate().take(32) {
            assert_eq!(usize::from(p), i + 1);
        }
        for i in 32..N_CLASSES {
            let prev = u32::from(CLASS_PAGES[i - 1]);
            let want = (prev + 1).max((f64::from(prev) * 1.025).ceil() as u32).min(EXT_MAX_PAGES);
            assert_eq!(u32::from(CLASS_PAGES[i]), want, "class {i}");
        }
        assert_eq!(CLASS_PAGES[N_CLASSES - 1], 2048);
        assert!(CLASS_PAGES.windows(2).all(|w| w[0] < w[1]));
        // Spot values of the sequence.
        assert_eq!(&CLASS_PAGES[40..46], &[41, 43, 45, 47, 49, 51]);
        assert_eq!(&CLASS_PAGES[N_CLASSES - 3..], &[1987, 2037, 2048]);
        // Waste <= 2.5% above 128 KiB (32 pages).
        for w in CLASS_PAGES.windows(2).skip(31) {
            let (a, b) = (f64::from(w[0]), f64::from(w[1]));
            assert!((b - (a + 1.0)) / (a + 1.0) <= 0.025 + 1e-9, "{a} -> {b}");
        }
    }

    #[test]
    fn class_lookup() {
        assert_eq!(class_for(0), Some(0));
        assert_eq!(class_for(1), Some(0));
        assert_eq!(class_for(32), Some(31));
        assert_eq!(class_for(33), Some(32));
        assert_eq!(class_for(42), Some(41)); // 43 pages
        assert_eq!(class_pages(41), 43);
        assert_eq!(class_for(2048), Some((N_CLASSES - 1) as u8));
        assert_eq!(class_for(2049), None);
        for pages in 1..=2048u32 {
            let c = class_for(pages).unwrap();
            assert!(class_pages(c) >= pages);
            if c > 0 {
                assert!(class_pages(c - 1) < pages);
            }
            assert_eq!(class_exact(pages).is_some(), class_pages(c) == pages);
        }
        assert_eq!(class_exact(0), None);
        assert_eq!(class_exact(42), None);
        assert_eq!(class_exact(43), Some(41));
        assert_eq!(pages_for(0), 1);
        assert_eq!(pages_for(4096), 1);
        assert_eq!(pages_for(4097), 2);
    }

    #[test]
    fn ab_select_rules() {
        assert_eq!(ab_select::<()>(None, None), None);
        assert_eq!(ab_select(Some((3, 'a')), None), Some((AbCopy::A, 3, 'a')));
        assert_eq!(ab_select(None, Some((3, 'b'))), Some((AbCopy::B, 3, 'b')));
        assert_eq!(ab_select(Some((3, 'a')), Some((4, 'b'))), Some((AbCopy::B, 4, 'b')));
        assert_eq!(ab_select(Some((5, 'a')), Some((4, 'b'))), Some((AbCopy::A, 5, 'a')));
        assert_eq!(ab_select(Some((4, 'a')), Some((4, 'b'))), Some((AbCopy::A, 4, 'a')));
    }

    #[test]
    fn layout_small_device0() {
        // 1 GiB device, regions of 16 MiB each.
        let pages = (1u64 << 30) / 4096;
        let l = DeviceLayout::compute(pages, true, 4096).unwrap();
        assert_eq!(l.ag_table_blocks, 1);
        assert_eq!(l.pmeta_page, 64); // after 4 + 2 pages, rounded up to 64
        assert_eq!(l.nmeta_page, 64 + 256);
        assert_eq!(l.intent_page, 64 + 258);
        assert_eq!(l.intent_slot_page, 64 + 260);
        assert_eq!(l.snap_region_page, [4096, 8192]);
        assert_eq!(l.ag_area_page, 12288);
        assert_eq!(l.n_ags, 64 - 3);
        l.check(true).unwrap();
        assert_eq!(l.ag_first_page(0), 12288);
        assert_eq!(l.ag_of_page(12288 + 4095), Some(0));
        assert_eq!(l.ag_of_page(12288 + 4096), Some(1));
        assert_eq!(l.ag_of_page(12287), None);
        assert_eq!(l.ag_of_page(pages), None);
        assert_eq!(DeviceLayout::ag_table_copy_page(3, AbCopy::B), 11);
        assert_eq!(l.pmeta_copy_page(5, AbCopy::A), 74);
        assert_eq!(l.intent_slot_copy_offset(512, 3, AbCopy::B), (64 + 260) * 4096 + 7 * 512);
        assert_eq!(l.intent_slot_copy_offset(4096, 63, AbCopy::B), (64 + 260 + 127) * 4096);
    }

    #[test]
    fn layout_other_device_no_regions() {
        let pages = 20 * 4096 + 17;
        let l = DeviceLayout::compute(pages, false, 0).unwrap();
        assert_eq!((l.pmeta_page, l.nmeta_page, l.intent_page, l.intent_slot_page), (0, 0, 0, 0));
        assert_eq!(l.snap_region_page, [0, 0]);
        assert_eq!(l.ag_area_page, 4096);
        assert_eq!(l.n_ags, 19);
        l.check(false).unwrap();
        assert!(l.check(true).is_err());
    }

    #[test]
    fn layout_errors() {
        assert_eq!(DeviceLayout::compute(4096, true, 0), Err(LayoutError::TooSmall));
        assert_eq!(DeviceLayout::compute(3 * 4096, true, 4096), Err(LayoutError::TooSmall));
        assert_eq!(DeviceLayout::compute(1 << 20, true, 100), Err(LayoutError::SnapshotRegionUnaligned));
        assert_eq!(DeviceLayout::compute((1u64 << 32) + 2, false, 0), Err(LayoutError::TooLarge));
        let mut l = DeviceLayout::compute(1 << 20, true, 4096).unwrap();
        l.n_ags += 1;
        assert!(l.check(true).is_err());
    }

    #[test]
    fn layout_table_fixed_point() {
        // Every size: the stored table covers the AG count, which uses the space exactly.
        for ags in [1u64, 2, 2031, 2032, 2033, 4064, 4065, 100_000] {
            for extra in [0u64, 1, 4095] {
                for dev0 in [false, true] {
                    let pages = (ags + 2) * 4096 + extra;
                    let l = DeviceLayout::compute(pages, dev0, 0).unwrap();
                    l.check(dev0).unwrap();
                    assert!(l.device_pages - l.ag_first_page(l.n_ags) < 4096);
                    assert_eq!(l.ag_table_blocks, l.n_ags.div_ceil(2032));
                }
            }
        }
        // Largest device: 2^32 pages = 16 TiB.
        let l = DeviceLayout::compute(1u64 << 32, true, 4096 * 10).unwrap();
        l.check(true).unwrap();
        assert_eq!(l.ag_table_blocks, l.n_ags.div_ceil(2032));
    }
}
