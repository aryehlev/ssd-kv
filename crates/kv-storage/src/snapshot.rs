//! Clean-shutdown index snapshot into fixed regions (DESIGN §10.10, §17.6, §18.2).
//!
//! Two fixed regions A and B on every device, reserved at format; region X is the concatenation
//! of every device's region X in device-index order. A snapshot overwrites, whole and from its
//! first byte, the region the superblock does not name; then the superblock flips to name it
//! (E8). If the encoded snapshot is larger than one region, nothing is written: SNAPSHOT_VALID
//! stays unset and the next start takes the full scan (INFO `snapshot_skipped_too_large:1`).
//! Writes target only `snap_region_page[..]` ranges (LC-02).
//!
//! Logical page 0 of a region is the header (AbBlock magic `KVSNAPH1`: snap_seq, boot_gen,
//! partitions, region, total_bytes, then per-partition `{offset, len, crc32c}`); sections follow
//! from logical page 1. Section: `"SNP1"`, partition, databases, next_seq, hint_base_s, tables
//! (`{db, flags, slot, n, n x 10 B entries in fp order (sync bit 0), [n x u16 hints]}`), SMALL
//! AGs with their `page_free`, EXTENT AGs with their `alloc`/`start` bitmaps, crc32c. This
//! module is the only caller of `SlotTable::snapshot_iter` (LC-18).

use kv_common::abblock::AbCopy;

/// `KVSNAPH1`
pub const SNAP_HEADER_MAGIC: u64 = kv_common::abblock::magic(b"KVSNAPH1");
/// `"SNP1"`
pub const SNAP_SECTION_MAGIC: u32 = u32::from_le_bytes(*b"SNP1");

/// Streams partition sections into the target snapshot region.
pub struct SnapshotWriter {
    region: AbCopy,
    /// Logical byte offset of the next section inside the region.
    offset: u64,
    /// Capacity of one region (all devices).
    region_bytes: u64,
    /// `(offset, len, crc32c)` per partition, for the header.
    sections: Vec<(u64, u64, u32)>,
    too_large: bool,
}

impl SnapshotWriter {
    pub fn new(region: AbCopy, region_bytes: u64, partitions: u16) -> Self {
        todo!("DESIGN §10.10: snapshot writer")
    }

    /// The encoded snapshot did not fit its region.
    pub fn too_large(&self) -> bool {
        self.too_large
    }
}
