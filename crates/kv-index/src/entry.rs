//! Index entries (DESIGN §9.2).
//!
//! 10 bytes, little endian, unaligned, `Copy`:
//! ```text
//! bytes 0..4   fp32 = KeyHash bits 63..32           (sort key and position source)
//! byte  4      x8   = Small: KeyHash bits 31..24;   Extent/ChunkMap: extent size class
//! bytes 5..10  w (40-bit little-endian integer):
//!              w[0..36]  page36 = dev(4) << 32 | page_in_dev(32)   (first page for extents)
//!              w[36..38] kind   0 Small, 1 Extent, 2 ChunkMap, 3 reserved
//!              w[38]     vtype  0 string, 1 hash
//!              w[39]     sync_mark (RAM only, merge full sync §23.7; always 0 in snapshots)
//! EMPTY <=> page36 == 0   (page 0 of device 0 is superblock copy A, never data)
//! ```
//! An entry holds no pointer, no seq, no expiry, no key bytes and no digest (LC-13, LC-20).
//!
//! Inside a segment every EMPTY position is stored as ten zero bytes, so an empty position can
//! be written, moved and compared like any other entry. The crate-internal `raw_*` helpers read
//! single fields straight from the packed bytes for the probe loops.

/// Where the record lives. (`Hash` is needed by `PageRef`'s derive.)
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Kind {
    Small = 0,
    Extent = 1,
    ChunkMap = 2,
}

impl Kind {
    /// The 2-bit code of `w[36..38]`; `None` for the reserved code 3 (or anything wider).
    pub const fn from_code(code: u8) -> Option<Kind> {
        match code {
            0 => Some(Kind::Small),
            1 => Some(Kind::Extent),
            2 => Some(Kind::ChunkMap),
            _ => None,
        }
    }
}

/// Value type of the key.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum VType {
    String = 0,
    Hash = 1,
}

/// Decoded index entry.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Entry {
    pub fp32: u32,
    pub x8: u8,
    pub page36: u64,
    pub kind: Kind,
    pub vtype: VType,
    /// bit 39, RAM-only merge-sync mark, §9.2
    pub sync_mark: bool,
}

/// The packed 10-byte image of an entry as stored in a segment.
pub(crate) type Raw = [u8; Entry::BYTES];

/// The all-zero EMPTY image.
pub(crate) const RAW_EMPTY: Raw = [0; Entry::BYTES];

/// Mask of the 36-bit page reference inside `w`.
pub const PAGE36_MASK: u64 = (1 << 36) - 1;

const KIND_SHIFT: u32 = 36;
const VTYPE_BIT: u64 = 1 << 38;
const SYNC_BIT: u64 = 1 << 39;
/// Bit 7 of byte 9 is `w[39]`.
const SYNC_BYTE: usize = 9;
const SYNC_BYTE_MASK: u8 = 0x80;

impl Entry {
    pub const BYTES: usize = 10;

    /// Packs the entry. `page36` is truncated to 36 bits (debug builds assert it fits).
    #[inline]
    pub fn encode(&self, out: &mut [u8; 10]) {
        debug_assert!(self.page36 <= PAGE36_MASK, "page36 wider than 36 bits");
        let w = (self.page36 & PAGE36_MASK)
            | ((self.kind as u64) << KIND_SHIFT)
            | if self.vtype == VType::Hash {
                VTYPE_BIT
            } else {
                0
            }
            | if self.sync_mark { SYNC_BIT } else { 0 };
        out[0..4].copy_from_slice(&self.fp32.to_le_bytes());
        out[4] = self.x8;
        out[5..10].copy_from_slice(&w.to_le_bytes()[..5]);
    }

    /// Unpacks bytes produced by [`Entry::encode`]. The reserved kind code 3 never occurs in
    /// RAM; it decodes as `ChunkMap` here (debug builds assert). Untrusted bytes (for example a
    /// snapshot section being loaded) go through [`Entry::try_decode`].
    #[inline]
    pub fn decode(b: &[u8; 10]) -> Self {
        let w = raw_w(b);
        let code = ((w >> KIND_SHIFT) & 3) as u8;
        debug_assert!(code != 3, "reserved entry kind");
        let kind = Kind::from_code(code).unwrap_or(Kind::ChunkMap);
        Self::from_parts(b, w, kind)
    }

    /// Like [`Entry::decode`], but `None` for the reserved kind code.
    pub fn try_decode(b: &[u8; 10]) -> Option<Self> {
        let w = raw_w(b);
        let kind = Kind::from_code(((w >> KIND_SHIFT) & 3) as u8)?;
        Some(Self::from_parts(b, w, kind))
    }

    #[inline]
    fn from_parts(b: &[u8; 10], w: u64, kind: Kind) -> Self {
        Entry {
            fp32: raw_fp32(b),
            x8: b[4],
            page36: w & PAGE36_MASK,
            kind,
            vtype: if w & VTYPE_BIT != 0 {
                VType::Hash
            } else {
                VType::String
            },
            sync_mark: w & SYNC_BIT != 0,
        }
    }

    /// `page36 == 0` on the raw bytes.
    #[inline(always)]
    pub fn is_empty_raw(b: &[u8; 10]) -> bool {
        // one unaligned 8-byte load of bytes 2..10: page36 occupies its bits 24..60
        let [_, _, rest @ ..] = *b;
        u64::from_le_bytes(rest) & (PAGE36_MASK << 24) == 0
    }

    /// `true` for the EMPTY entry (`page36 == 0`).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.page36 == 0
    }

    /// The packed image.
    #[inline]
    pub(crate) fn to_raw(self) -> Raw {
        let mut r = RAW_EMPTY;
        self.encode(&mut r);
        r
    }
}

/// The 40-bit `w` field.
#[inline(always)]
fn raw_w(b: &Raw) -> u64 {
    u64::from_le_bytes([b[5], b[6], b[7], b[8], b[9], 0, 0, 0])
}

/// `fp32` of a packed entry.
#[inline(always)]
pub(crate) fn raw_fp32(b: &Raw) -> u32 {
    u32::from_le_bytes([b[0], b[1], b[2], b[3]])
}

/// `true` if the packed entry is of kind Small (kind code bits `w[36..38]` == 0).
#[inline(always)]
pub(crate) fn raw_is_small(b: &Raw) -> bool {
    b[9] & 0x30 == 0
}

/// The value type bit (`w[38]`) of a packed entry.
#[inline(always)]
pub(crate) fn raw_vtype(b: &Raw) -> VType {
    if b[9] & 0x40 != 0 {
        VType::Hash
    } else {
        VType::String
    }
}

#[inline(always)]
pub(crate) fn raw_sync_mark(b: &Raw) -> bool {
    b[SYNC_BYTE] & SYNC_BYTE_MASK != 0
}

#[inline(always)]
pub(crate) fn raw_set_sync_mark(b: &mut Raw, on: bool) {
    if on {
        b[SYNC_BYTE] |= SYNC_BYTE_MASK;
    } else {
        b[SYNC_BYTE] &= !SYNC_BYTE_MASK;
    }
}

/// Position of an entry; valid only until the next mutation of its table (§9.4).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Pos {
    pub seg: u32,
    pub idx: u32,
}

impl Pos {
    /// Where a sweep over a table starts ([`crate::SlotTable::expiry_candidates`]).
    pub const START: Pos = Pos { seg: 0, idx: 0 };
    /// Returned by a sweep that reached the end of its table.
    pub const END: Pos = Pos {
        seg: u32::MAX,
        idx: u32::MAX,
    };

    /// `true` for [`Pos::END`].
    #[inline]
    pub fn is_end(self) -> bool {
        self == Pos::END
    }
}

/// A fingerprint candidate returned by `SlotTable::candidates`.
#[derive(Clone, Copy, Debug)]
pub struct Cand {
    pub pos: Pos,
    pub entry: Entry,
    pub hint: u16,
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn entry(fp32: u32, x8: u8, page36: u64, kind: Kind, vtype: VType, sync_mark: bool) -> Entry {
        Entry {
            fp32,
            x8,
            page36,
            kind,
            vtype,
            sync_mark,
        }
    }

    /// Golden byte vectors: this layout is part of the snapshot format (§9.8, §10.10).
    #[test]
    fn golden_vectors() {
        let cases: [(Entry, [u8; 10]); 5] = [
            (
                entry(0x1122_3344, 0x55, 0x1, Kind::Small, VType::String, false),
                [0x44, 0x33, 0x22, 0x11, 0x55, 0x01, 0x00, 0x00, 0x00, 0x00],
            ),
            (
                // dev 0xA, page_in_dev 0xDEADBEEF, extent, hash
                entry(
                    0xFFFF_FFFF,
                    0x07,
                    0xA_DEAD_BEEF,
                    Kind::Extent,
                    VType::Hash,
                    false,
                ),
                [0xFF, 0xFF, 0xFF, 0xFF, 0x07, 0xEF, 0xBE, 0xAD, 0xDE, 0x5A],
            ),
            (
                entry(0, 0xFF, PAGE36_MASK, Kind::ChunkMap, VType::String, true),
                [0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xAF],
            ),
            (
                entry(
                    0x0000_0100,
                    0x00,
                    0x1_0000_0000,
                    Kind::Small,
                    VType::Hash,
                    true,
                ),
                [0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC1],
            ),
            (
                entry(
                    0xCAFE_F00D,
                    0x80,
                    0x0_0000_1000,
                    Kind::Extent,
                    VType::String,
                    false,
                ),
                [0x0D, 0xF0, 0xFE, 0xCA, 0x80, 0x00, 0x10, 0x00, 0x00, 0x10],
            ),
        ];
        for (e, bytes) in cases {
            let mut out = [0u8; 10];
            e.encode(&mut out);
            assert_eq!(out, bytes, "{e:?}");
            assert_eq!(Entry::decode(&bytes), e);
            assert_eq!(Entry::try_decode(&bytes), Some(e));
            assert!(!Entry::is_empty_raw(&bytes));
            assert_eq!(raw_fp32(&bytes), e.fp32);
            assert_eq!(raw_is_small(&bytes), e.kind == Kind::Small);
            assert_eq!(raw_sync_mark(&bytes), e.sync_mark);
        }
    }

    #[test]
    fn empty_and_reserved() {
        assert!(Entry::is_empty_raw(&RAW_EMPTY));
        // fp, x8 and the flag bits do not make an entry non-empty: only page36 does.
        let b = [0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x00, 0x00, 0x00, 0x00, 0xF0];
        assert!(Entry::is_empty_raw(&b));
        // kind code 3 is reserved
        let r = [0, 0, 0, 0, 0, 1, 0, 0, 0, 0x30];
        assert_eq!(Entry::try_decode(&r), None);
        assert_eq!(Kind::from_code(3), None);
        assert_eq!(std::mem::size_of::<Raw>(), Entry::BYTES);
    }

    #[test]
    fn sync_mark_bit() {
        let e = entry(7, 9, 42, Kind::Small, VType::Hash, false);
        let mut r = e.to_raw();
        raw_set_sync_mark(&mut r, true);
        assert_eq!(
            Entry::decode(&r),
            Entry {
                sync_mark: true,
                ..e
            }
        );
        raw_set_sync_mark(&mut r, false);
        assert_eq!(Entry::decode(&r), e);
    }

    fn arb_entry() -> impl Strategy<Value = Entry> {
        (
            any::<u32>(),
            any::<u8>(),
            0..=PAGE36_MASK,
            0u8..3,
            any::<bool>(),
            any::<bool>(),
        )
            .prop_map(|(fp32, x8, page36, k, h, sync_mark)| Entry {
                fp32,
                x8,
                page36,
                kind: Kind::from_code(k).unwrap(),
                vtype: if h { VType::Hash } else { VType::String },
                sync_mark,
            })
    }

    proptest! {
        #[test]
        fn roundtrip(e in arb_entry()) {
            let r = e.to_raw();
            prop_assert_eq!(Entry::decode(&r), e);
            prop_assert_eq!(Entry::is_empty_raw(&r), e.page36 == 0);
            prop_assert_eq!(raw_fp32(&r), e.fp32);
            prop_assert_eq!(raw_is_small(&r), e.kind == Kind::Small);
        }
    }
}
