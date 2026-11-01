//! AG table codec (DESIGN §10.4; AbBlock magic `KVAGTBL1`).
//!
//! Payload: `0x018 u32 block_index`, `0x01C u16 n_entries (<= 2032)`, `0x01E u16 zero`,
//! `0x020 entries[2032] x {u8 kind (0 FREE, 1 SMALL, 2 EXTENT, 3 reserved), u8 owner_partition
//! (0xFF none)}`. Only the changed block is rewritten (A/B) when an AG is acquired, released or
//! changes kind; an AG changes kind or owner only when it holds no live record and no tracked
//! stale copy. The entry is durable before the first write into the AG (E11).

use crate::layout::PAGE;

/// `KVAGTBL1`
pub const AGT_MAGIC: u64 = kv_common::abblock::magic(b"KVAGTBL1");
/// Entries per AG table block.
pub const AG_TABLE_ENTRIES: usize = 2032;
/// `owner_partition` of a FREE AG.
pub const OWNER_NONE: u8 = 0xFF;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AgKind {
    Free,
    Small,
    Extent,
}

/// One AG table block: `(kind, owner_partition)` per AG.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AgTableBlock {
    pub index: u32,
    pub entries: Vec<(AgKind, u8)>,
}

impl AgTableBlock {
    pub fn encode(&self, seq: u64, out: &mut [u8; PAGE]) {
        todo!("DESIGN §10.4: AG table encode")
    }

    /// Validates the AbBlock and returns `(block_seq, block)`.
    pub fn decode(b: &[u8]) -> Option<(u64, Self)> {
        todo!("DESIGN §10.4: AG table decode")
    }
}
