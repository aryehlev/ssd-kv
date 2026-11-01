//! Superblock codec (DESIGN §10.3; AbBlock magic `KVSSDSB1`).
//!
//! One A/B pair at pages 0/1 of every device. Device 0's copy is authoritative for set-wide
//! fields (seed, salt, boot_gen, clock floor, flags, snapshot). Device 0's superblock is
//! rewritten at startup (`boot_gen += 1`, CLEAN and SNAPSHOT_VALID cleared, before any data
//! write: edge E7), every `clock-floor-interval` with the new clock floor, and at clean shutdown
//! (snapshot fields, CLEAN set; E8). `hash_seed` and `record_crc_salt` are secrets: no `Debug`.

use crate::layout::PAGE;

/// `KVSSDSB1`
pub const SB_MAGIC: u64 = kv_common::abblock::magic(b"KVSSDSB1");
/// flags b0
pub const SB_CLEAN: u16 = 1;
/// flags b1
pub const SB_CLUSTER_MODE: u16 = 2;
/// flags b2
pub const SB_SNAPSHOT_VALID: u16 = 4;

/// Every field of §10.3 (offsets in the comments).
#[derive(Clone, PartialEq, Eq)]
pub struct Superblock {
    /// 0x018
    pub set_uuid: [u8; 16],
    /// 0x028
    pub device_uuid: [u8; 16],
    /// 0x038
    pub device_index: u16,
    /// 0x03A
    pub device_count: u16,
    /// 0x03C, P
    pub partitions: u16,
    /// 0x03E: SB_CLEAN | SB_CLUSTER_MODE | SB_SNAPSHOT_VALID
    pub flags: u16,
    /// 0x040, 4096
    pub page_size: u32,
    /// 0x044, 4096
    pub ag_pages: u32,
    /// 0x048
    pub lba: u32,
    /// 0x04C
    pub n_ags: u32,
    /// 0x050
    pub device_pages: u64,
    /// 0x058, 4
    pub ag_table_page: u64,
    /// 0x060
    pub ag_table_blocks: u32,
    /// 0x068
    pub ag_area_page: u64,
    /// 0x070 (device 0 only)
    pub pmeta_page: u64,
    /// 0x078 (device 0 only)
    pub nmeta_page: u64,
    /// 0x080 (device 0 only; 0 elsewhere)
    pub intent_page: u64,
    /// 0x088 (device 0 only; 0 elsewhere)
    pub intent_slot_page: u64,
    /// 0x090
    pub hash_seed: u64,
    /// 0x098
    pub record_crc_salt: u32,
    /// 0x09C
    pub boot_gen: u32,
    /// 0x0A0
    pub clock_floor_unix_ms: u64,
    /// 0x0A8
    pub created_unix_ms: u64,
    /// 0x0B0
    pub snap_seq: u64,
    /// 0x0B8
    pub snap_header_crc: u32,
    /// 0x0BC: 0 = A, 1 = B (region of the last snapshot written)
    pub snap_region: u8,
    /// 0x0C0
    pub snap_bytes: u64,
    /// 0x0C8: 16 standalone, 1 cluster
    pub databases: u16,
    /// 0x0D0: this device's regions A and B (fixed at format)
    pub snap_region_page: [u64; 2],
    /// 0x0E0: pages per region on this device (multiple of 4096; 0 = no regions)
    pub snap_region_pages: u64,
    /// 0x100: member device uuids by device_index (device 0 only)
    pub member_uuids: [[u8; 16]; 16],
}

impl Superblock {
    pub fn encode(&self, seq: u64, out: &mut [u8; PAGE]) {
        todo!("DESIGN §10.3: superblock encode")
    }

    /// Validates the AbBlock and returns `(block_seq, superblock)`.
    pub fn decode(b: &[u8]) -> Option<(u64, Self)> {
        todo!("DESIGN §10.3: superblock decode")
    }
}

/// A superblock rewrite requested from the meta thread (`MetaReq::Superblock`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SbUpdate {
    /// Startup: `boot_gen += 1`, CLEAN and SNAPSHOT_VALID cleared (E7).
    Startup,
    /// Periodic clock floor (E13).
    ClockFloor { unix_ms: u64 },
    /// Clean shutdown: snapshot fields, CLEAN set, SNAPSHOT_VALID when a snapshot was written
    /// (E8).
    CleanShutdown { snapshot: Option<SnapshotMark> },
}

/// Superblock fields naming a completed snapshot.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SnapshotMark {
    pub snap_seq: u64,
    pub snap_header_crc: u32,
    pub snap_region: u8,
    pub snap_bytes: u64,
}
