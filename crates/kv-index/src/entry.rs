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

/// Where the record lives. (`Hash` is needed by `PageRef`'s derive.)
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Kind {
    Small = 0,
    Extent = 1,
    ChunkMap = 2,
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

impl Entry {
    pub const BYTES: usize = 10;

    pub fn encode(&self, out: &mut [u8; 10]) {
        todo!("DESIGN §9.2: entry encode")
    }

    pub fn decode(b: &[u8; 10]) -> Self {
        todo!("DESIGN §9.2: entry decode")
    }

    /// `page36 == 0` on the raw bytes.
    pub fn is_empty_raw(b: &[u8; 10]) -> bool {
        todo!("DESIGN §9.2: EMPTY test")
    }
}

/// Position of an entry; valid only until the next mutation of its table (§9.4).
#[derive(Clone, Copy, Debug)]
pub struct Pos {
    pub seg: u32,
    pub idx: u32,
}

/// A fingerprint candidate returned by `SlotTable::candidates`.
#[derive(Clone, Copy, Debug)]
pub struct Cand {
    pub pos: Pos,
    pub entry: Entry,
    pub hint: u16,
}
