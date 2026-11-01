//! Superblock codec (DESIGN §10.3; AbBlock magic `KVSSDSB1`).
//!
//! One A/B pair at pages 0/1 of every device. Device 0's copy is authoritative for set-wide
//! fields (seed, salt, boot_gen, clock floor, flags, snapshot). Device 0's superblock is
//! rewritten at startup (`boot_gen += 1`, CLEAN and SNAPSHOT_VALID cleared, before any data
//! write: edge E7), every `clock-floor-interval` with the new clock floor, and at clean shutdown
//! (snapshot fields, CLEAN set; E8). `hash_seed` and `record_crc_salt` are secrets: no `Debug`.
//!
//! ```text
//! 0x018 [16] set_uuid              0x028 [16] device_uuid
//! 0x038 u16 device_index           0x03A u16 device_count       0x03C u16 partitions (P)
//! 0x03E u16 flags: b0 CLEAN, b1 CLUSTER_MODE, b2 SNAPSHOT_VALID
//! 0x040 u32 page_size = 4096       0x044 u32 ag_pages = 4096    0x048 u32 lba     0x04C u32 n_ags
//! 0x050 u64 device_pages           0x058 u64 ag_table_page = 4  0x060 u32 ag_table_blocks   0x064 u32 zero
//! 0x068 u64 ag_area_page           0x070 u64 pmeta_page         0x078 u64 nmeta_page
//! 0x080 u64 intent_page            0x088 u64 intent_slot_page   (device 0 only; 0 elsewhere)
//! 0x090 u64 hash_seed              0x098 u32 record_crc_salt    0x09C u32 boot_gen
//! 0x0A0 u64 clock_floor_unix_ms    0x0A8 u64 created_unix_ms
//! 0x0B0 u64 snap_seq               0x0B8 u32 snap_header_crc    0x0BC u8 snap_region   0x0BD [3] zero
//! 0x0C0 u64 snap_bytes             0x0C8 u16 databases          0x0CA [6] zero
//! 0x0D0 u64 snap_region_page[2]    0x0E0 u64 snap_region_pages  0x0E8 [24] zero
//! 0x100 [16 x 16] member device uuids by device_index (device 0 only)
//! rest zero
//! ```

use kv_common::abblock::AbCopy;

use crate::layout::{
    AG_PAGES, AG_TABLE_PAGE, DeviceLayout, LayoutError, MAX_DEVICES, PAGE, ab_build, ab_parse, ab_select, le,
};

/// `KVSSDSB1`
pub const SB_MAGIC: u64 = kv_common::abblock::magic(b"KVSSDSB1");
/// flags b0
pub const SB_CLEAN: u16 = 1;
/// flags b1
pub const SB_CLUSTER_MODE: u16 = 2;
/// flags b2
pub const SB_SNAPSHOT_VALID: u16 = 4;
/// Every defined flag bit.
pub const SB_FLAGS_KNOWN: u16 = SB_CLEAN | SB_CLUSTER_MODE | SB_SNAPSHOT_VALID;

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

/// Why a decoded superblock cannot be used.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SuperblockError {
    /// `page_size` / `ag_pages` other than 4096.
    Geometry,
    /// `lba` other than 512 or 4096.
    Lba(u32),
    /// Device index or count out of range.
    DeviceIndex,
    /// Partition count other than 16, 32, 64 or 128.
    Partitions(u16),
    /// Database count other than 16 (standalone) or 1 (cluster).
    Databases(u16),
    /// Unknown flag bits or `snap_region > 1`.
    Flags,
    /// Positions disagree with the format rules.
    Layout(LayoutError),
}

impl core::fmt::Display for SuperblockError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            SuperblockError::Geometry => f.write_str("unsupported page or AG size"),
            SuperblockError::Lba(l) => write!(f, "unsupported logical block size {l}"),
            SuperblockError::DeviceIndex => f.write_str("device index out of range"),
            SuperblockError::Partitions(p) => write!(f, "unsupported partition count {p}"),
            SuperblockError::Databases(d) => write!(f, "unsupported database count {d}"),
            SuperblockError::Flags => f.write_str("unknown superblock flags"),
            SuperblockError::Layout(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for SuperblockError {}

impl Superblock {
    pub fn encode(&self, seq: u64, out: &mut [u8; PAGE]) {
        ab_build(SB_MAGIC, seq, out, |b| {
            b[0x018..0x028].copy_from_slice(&self.set_uuid);
            b[0x028..0x038].copy_from_slice(&self.device_uuid);
            le::put_u16(b, 0x038, self.device_index);
            le::put_u16(b, 0x03A, self.device_count);
            le::put_u16(b, 0x03C, self.partitions);
            le::put_u16(b, 0x03E, self.flags);
            le::put_u32(b, 0x040, self.page_size);
            le::put_u32(b, 0x044, self.ag_pages);
            le::put_u32(b, 0x048, self.lba);
            le::put_u32(b, 0x04C, self.n_ags);
            le::put_u64(b, 0x050, self.device_pages);
            le::put_u64(b, 0x058, self.ag_table_page);
            le::put_u32(b, 0x060, self.ag_table_blocks);
            le::put_u64(b, 0x068, self.ag_area_page);
            le::put_u64(b, 0x070, self.pmeta_page);
            le::put_u64(b, 0x078, self.nmeta_page);
            le::put_u64(b, 0x080, self.intent_page);
            le::put_u64(b, 0x088, self.intent_slot_page);
            le::put_u64(b, 0x090, self.hash_seed);
            le::put_u32(b, 0x098, self.record_crc_salt);
            le::put_u32(b, 0x09C, self.boot_gen);
            le::put_u64(b, 0x0A0, self.clock_floor_unix_ms);
            le::put_u64(b, 0x0A8, self.created_unix_ms);
            le::put_u64(b, 0x0B0, self.snap_seq);
            le::put_u32(b, 0x0B8, self.snap_header_crc);
            b[0x0BC] = self.snap_region;
            le::put_u64(b, 0x0C0, self.snap_bytes);
            le::put_u16(b, 0x0C8, self.databases);
            le::put_u64(b, 0x0D0, self.snap_region_page[0]);
            le::put_u64(b, 0x0D8, self.snap_region_page[1]);
            le::put_u64(b, 0x0E0, self.snap_region_pages);
            for (i, u) in self.member_uuids.iter().enumerate() {
                b[0x100 + 16 * i..0x110 + 16 * i].copy_from_slice(u);
            }
        });
    }

    /// Validates the AbBlock and returns `(block_seq, superblock)`.
    ///
    /// Only the block structure is checked here; [`Superblock::validate`] checks the values.
    pub fn decode(b: &[u8]) -> Option<(u64, Self)> {
        ab_parse(SB_MAGIC, b, |b| {
            let mut member_uuids = [[0u8; 16]; 16];
            for (i, u) in member_uuids.iter_mut().enumerate() {
                *u = le::bytes(b, 0x100 + 16 * i);
            }
            Some(Superblock {
                set_uuid: le::bytes(b, 0x018),
                device_uuid: le::bytes(b, 0x028),
                device_index: le::u16_at(b, 0x038),
                device_count: le::u16_at(b, 0x03A),
                partitions: le::u16_at(b, 0x03C),
                flags: le::u16_at(b, 0x03E),
                page_size: le::u32_at(b, 0x040),
                ag_pages: le::u32_at(b, 0x044),
                lba: le::u32_at(b, 0x048),
                n_ags: le::u32_at(b, 0x04C),
                device_pages: le::u64_at(b, 0x050),
                ag_table_page: le::u64_at(b, 0x058),
                ag_table_blocks: le::u32_at(b, 0x060),
                ag_area_page: le::u64_at(b, 0x068),
                pmeta_page: le::u64_at(b, 0x070),
                nmeta_page: le::u64_at(b, 0x078),
                intent_page: le::u64_at(b, 0x080),
                intent_slot_page: le::u64_at(b, 0x088),
                hash_seed: le::u64_at(b, 0x090),
                record_crc_salt: le::u32_at(b, 0x098),
                boot_gen: le::u32_at(b, 0x09C),
                clock_floor_unix_ms: le::u64_at(b, 0x0A0),
                created_unix_ms: le::u64_at(b, 0x0A8),
                snap_seq: le::u64_at(b, 0x0B0),
                snap_header_crc: le::u32_at(b, 0x0B8),
                snap_region: b[0x0BC],
                snap_bytes: le::u64_at(b, 0x0C0),
                databases: le::u16_at(b, 0x0C8),
                snap_region_page: [le::u64_at(b, 0x0D0), le::u64_at(b, 0x0D8)],
                snap_region_pages: le::u64_at(b, 0x0E0),
                member_uuids,
            })
        })
    }

    /// Reads both copies and returns the valid one with the higher `block_seq` (§10.2): a torn
    /// write damages only the copy being written.
    pub fn pick(a: &[u8], b: &[u8]) -> Option<(AbCopy, u64, Self)> {
        ab_select(Self::decode(a), Self::decode(b))
    }

    /// Checks the decoded values against the format rules: geometry, `lba`, device index,
    /// partition and database counts, flags and the stored layout.
    pub fn validate(&self) -> Result<(), SuperblockError> {
        if self.page_size != PAGE as u32 || self.ag_pages != AG_PAGES || self.ag_table_page != AG_TABLE_PAGE {
            return Err(SuperblockError::Geometry);
        }
        if self.lba != 512 && self.lba != 4096 {
            return Err(SuperblockError::Lba(self.lba));
        }
        if self.device_count == 0
            || usize::from(self.device_count) > MAX_DEVICES
            || self.device_index >= self.device_count
        {
            return Err(SuperblockError::DeviceIndex);
        }
        if !matches!(self.partitions, 16 | 32 | 64 | 128) {
            return Err(SuperblockError::Partitions(self.partitions));
        }
        let want_db = if self.cluster_mode() { 1 } else { 16 };
        if self.databases != want_db {
            return Err(SuperblockError::Databases(self.databases));
        }
        if self.flags & !SB_FLAGS_KNOWN != 0 || self.snap_region > 1 {
            return Err(SuperblockError::Flags);
        }
        self.layout().check(self.device_index == 0).map_err(SuperblockError::Layout)
    }

    /// The stored device layout.
    pub fn layout(&self) -> DeviceLayout {
        DeviceLayout {
            device_pages: self.device_pages,
            n_ags: self.n_ags,
            ag_table_blocks: self.ag_table_blocks,
            pmeta_page: self.pmeta_page,
            nmeta_page: self.nmeta_page,
            intent_page: self.intent_page,
            intent_slot_page: self.intent_slot_page,
            snap_region_page: self.snap_region_page,
            snap_region_pages: self.snap_region_pages,
            ag_area_page: self.ag_area_page,
        }
    }

    /// Copies the positions of `l` into the layout fields.
    pub fn set_layout(&mut self, l: &DeviceLayout) {
        self.device_pages = l.device_pages;
        self.n_ags = l.n_ags;
        self.ag_table_page = AG_TABLE_PAGE;
        self.ag_table_blocks = l.ag_table_blocks;
        self.pmeta_page = l.pmeta_page;
        self.nmeta_page = l.nmeta_page;
        self.intent_page = l.intent_page;
        self.intent_slot_page = l.intent_slot_page;
        self.snap_region_page = l.snap_region_page;
        self.snap_region_pages = l.snap_region_pages;
        self.ag_area_page = l.ag_area_page;
    }

    pub fn is_clean(&self) -> bool {
        self.flags & SB_CLEAN != 0
    }

    pub fn cluster_mode(&self) -> bool {
        self.flags & SB_CLUSTER_MODE != 0
    }

    pub fn snapshot_valid(&self) -> bool {
        self.flags & SB_SNAPSHOT_VALID != 0
    }

    /// The snapshot fields, when SNAPSHOT_VALID is set.
    pub fn snapshot(&self) -> Option<SnapshotMark> {
        self.snapshot_valid().then_some(SnapshotMark {
            snap_seq: self.snap_seq,
            snap_header_crc: self.snap_header_crc,
            snap_region: self.snap_region,
            snap_bytes: self.snap_bytes,
        })
    }

    /// True when `other` belongs to the same device set and geometry family (set uuid,
    /// device count, partitions, databases, cluster flag).
    pub fn same_set(&self, other: &Superblock) -> bool {
        self.set_uuid == other.set_uuid
            && self.device_count == other.device_count
            && self.partitions == other.partitions
            && self.databases == other.databases
            && self.cluster_mode() == other.cluster_mode()
    }

    /// Applies a rewrite request of the meta thread (§10.3) to device 0's superblock.
    ///
    /// * `Startup`: `boot_gen += 1`, CLEAN and SNAPSHOT_VALID cleared (E7).
    /// * `ClockFloor`: the clock floor moves to `unix_ms` but never backwards (E13: a floor
    ///   that was durable may already have been used to treat records as dead).
    /// * `CleanShutdown`: CLEAN set; the snapshot fields and SNAPSHOT_VALID when a snapshot was
    ///   written (E8), SNAPSHOT_VALID cleared otherwise.
    pub fn apply(&mut self, u: &SbUpdate) {
        match *u {
            SbUpdate::Startup => {
                self.boot_gen = self.boot_gen.wrapping_add(1);
                self.flags &= !(SB_CLEAN | SB_SNAPSHOT_VALID);
            }
            SbUpdate::ClockFloor { unix_ms } => {
                self.clock_floor_unix_ms = self.clock_floor_unix_ms.max(unix_ms);
            }
            SbUpdate::CleanShutdown { snapshot } => {
                self.flags |= SB_CLEAN;
                match snapshot {
                    Some(m) => {
                        self.snap_seq = m.snap_seq;
                        self.snap_header_crc = m.snap_header_crc;
                        self.snap_region = m.snap_region;
                        self.snap_bytes = m.snap_bytes;
                        self.flags |= SB_SNAPSHOT_VALID;
                    }
                    None => self.flags &= !SB_SNAPSHOT_VALID,
                }
            }
        }
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

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use proptest::prelude::*;

    /// A fully populated device-0 superblock used by the codec and golden tests.
    pub(crate) fn sample() -> Superblock {
        let l = DeviceLayout::compute(65_536, true, 4096).unwrap();
        let mut sb = Superblock {
            set_uuid: *b"set-uuid-0123456",
            device_uuid: *b"dev-uuid-0000000",
            device_index: 0,
            device_count: 2,
            partitions: 64,
            flags: SB_CLEAN,
            page_size: 4096,
            ag_pages: 4096,
            lba: 512,
            n_ags: 0,
            device_pages: 0,
            ag_table_page: 0,
            ag_table_blocks: 0,
            ag_area_page: 0,
            pmeta_page: 0,
            nmeta_page: 0,
            intent_page: 0,
            intent_slot_page: 0,
            hash_seed: 0x0123_4567_89AB_CDEF,
            record_crc_salt: 0xDEAD_BEEF,
            boot_gen: 7,
            clock_floor_unix_ms: 1_793_000_000_000,
            created_unix_ms: 1_792_000_000_000,
            snap_seq: 99,
            snap_header_crc: 0x1234_5678,
            snap_region: 1,
            snap_bytes: 1 << 20,
            databases: 16,
            snap_region_page: [0, 0],
            snap_region_pages: 0,
            member_uuids: [[0; 16]; 16],
        };
        sb.set_layout(&l);
        sb.member_uuids[0] = sb.device_uuid;
        sb.member_uuids[1] = *b"dev-uuid-1111111";
        sb
    }

    #[test]
    fn round_trip_and_offsets() {
        let sb = sample();
        sb.validate().unwrap();
        let mut b = [0u8; PAGE];
        sb.encode(42, &mut b);
        assert_eq!(&b[0..8], b"KVSSDSB1");
        assert_eq!(le::u32_at(&b, 0x0C), 1); // AbBlock version
        assert_eq!(le::u64_at(&b, 0x10), 42);
        assert_eq!(&b[0x18..0x28], b"set-uuid-0123456");
        assert_eq!(le::u16_at(&b, 0x3C), 64);
        assert_eq!(le::u32_at(&b, 0x40), 4096);
        assert_eq!(le::u64_at(&b, 0x58), 4);
        assert_eq!(le::u64_at(&b, 0x90), 0x0123_4567_89AB_CDEF);
        assert_eq!(b[0xBC], 1);
        assert_eq!(le::u16_at(&b, 0xC8), 16);
        assert_eq!(le::u64_at(&b, 0xD0), 4096);
        assert_eq!(le::u64_at(&b, 0xE0), 4096);
        assert_eq!(&b[0x110..0x120], b"dev-uuid-1111111");
        assert!(le::zero(&b, 0x064..0x068));
        assert!(le::zero(&b, 0x0BD..0x0C0));
        assert!(le::zero(&b, 0x0CA..0x0D0));
        assert!(le::zero(&b, 0x0E8..0x100));
        assert!(le::zero(&b, 0x200..PAGE));
        let (seq, d) = Superblock::decode(&b).unwrap();
        assert_eq!(seq, 42);
        assert!(d == sb);
    }

    #[test]
    fn decode_rejects_damage() {
        let sb = sample();
        let mut b = [0u8; PAGE];
        sb.encode(1, &mut b);
        for off in [0usize, 8, 0x0C, 0x10, 0x18, 0x90, 0x200, 0xFFF] {
            let mut d = b;
            d[off] ^= 0x40;
            assert!(Superblock::decode(&d).is_none(), "flip at {off:#x}");
        }
        assert!(Superblock::decode(&b[..100]).is_none());
        assert!(Superblock::decode(&[0u8; PAGE]).is_none());
    }

    #[test]
    fn validate_rules() {
        let ok = sample();
        let mut s = ok.clone();
        s.lba = 1024;
        assert_eq!(s.validate(), Err(SuperblockError::Lba(1024)));
        let mut s = ok.clone();
        s.partitions = 48;
        assert_eq!(s.validate(), Err(SuperblockError::Partitions(48)));
        let mut s = ok.clone();
        s.flags |= SB_CLUSTER_MODE;
        assert_eq!(s.validate(), Err(SuperblockError::Databases(16)));
        s.databases = 1;
        s.validate().unwrap();
        let mut s = ok.clone();
        s.flags |= 0x80;
        assert_eq!(s.validate(), Err(SuperblockError::Flags));
        let mut s = ok.clone();
        s.device_index = 2;
        assert_eq!(s.validate(), Err(SuperblockError::DeviceIndex));
        let mut s = ok.clone();
        s.page_size = 8192;
        assert_eq!(s.validate(), Err(SuperblockError::Geometry));
        let mut s = ok.clone();
        s.ag_area_page += 4096;
        assert!(matches!(s.validate(), Err(SuperblockError::Layout(_))));
    }

    #[test]
    fn updates() {
        let mut s = sample();
        s.flags = SB_CLEAN | SB_SNAPSHOT_VALID;
        s.apply(&SbUpdate::Startup);
        assert_eq!(s.boot_gen, 8);
        assert!(!s.is_clean() && !s.snapshot_valid());
        s.apply(&SbUpdate::ClockFloor { unix_ms: 1_793_000_000_500 });
        assert_eq!(s.clock_floor_unix_ms, 1_793_000_000_500);
        s.apply(&SbUpdate::ClockFloor { unix_ms: 5 });
        assert_eq!(s.clock_floor_unix_ms, 1_793_000_000_500);
        let m = SnapshotMark { snap_seq: 5, snap_header_crc: 9, snap_region: 0, snap_bytes: 77 };
        s.apply(&SbUpdate::CleanShutdown { snapshot: Some(m) });
        assert!(s.is_clean() && s.snapshot_valid());
        assert_eq!(s.snapshot(), Some(m));
        s.apply(&SbUpdate::CleanShutdown { snapshot: None });
        assert!(s.is_clean() && !s.snapshot_valid());
        assert_eq!(s.snapshot(), None);
        assert!(s.same_set(&sample()));
    }

    /// Test-only wrapper: proptest prints failing inputs, the superblock itself has no `Debug`.
    #[derive(Clone)]
    struct ArbSb(Superblock);

    impl core::fmt::Debug for ArbSb {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            write!(f, "Superblock(device {}/{}, P {}, flags {:#x})", self.0.device_index, self.0.device_count, self.0.partitions, self.0.flags)
        }
    }

    fn arb_sb() -> impl Strategy<Value = ArbSb> {
        (
            (any::<[u8; 16]>(), any::<[u8; 16]>(), any::<u16>(), any::<u16>(), any::<u16>(), any::<u16>()),
            (any::<u32>(), any::<u32>(), any::<u32>(), any::<u32>(), any::<u64>(), any::<u64>(), any::<u32>()),
            (any::<u64>(), any::<u64>(), any::<u64>(), any::<u64>(), any::<u64>(), any::<u64>(), any::<u32>(), any::<u32>()),
            (any::<u64>(), any::<u64>(), any::<u64>(), any::<u32>(), any::<u8>(), any::<u64>(), any::<u16>()),
            (any::<[u64; 2]>(), any::<u64>(), any::<[[u8; 16]; 16]>()),
        )
            .prop_map(|(a, b, c, d, e)| ArbSb(Superblock {
                set_uuid: a.0,
                device_uuid: a.1,
                device_index: a.2,
                device_count: a.3,
                partitions: a.4,
                flags: a.5,
                page_size: b.0,
                ag_pages: b.1,
                lba: b.2,
                n_ags: b.3,
                device_pages: b.4,
                ag_table_page: b.5,
                ag_table_blocks: b.6,
                ag_area_page: c.0,
                pmeta_page: c.1,
                nmeta_page: c.2,
                intent_page: c.3,
                intent_slot_page: c.4,
                hash_seed: c.5,
                record_crc_salt: c.6,
                boot_gen: c.7,
                clock_floor_unix_ms: d.0,
                created_unix_ms: d.1,
                snap_seq: d.2,
                snap_header_crc: d.3,
                snap_region: d.4,
                snap_bytes: d.5,
                databases: d.6,
                snap_region_page: e.0,
                snap_region_pages: e.1,
                member_uuids: e.2,
            }))
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 256, failure_persistence: None, ..ProptestConfig::default() })]

        #[test]
        fn prop_round_trip(sb in arb_sb(), seq in any::<u64>()) {
            let sb = sb.0;
            let mut b = [0u8; PAGE];
            sb.encode(seq, &mut b);
            let (s, d) = Superblock::decode(&b).unwrap();
            prop_assert_eq!(s, seq);
            prop_assert!(d == sb);
        }

        /// A/B selection with torn copies (§10.2, §17.6): the writer overwrites the copy that
        /// does not hold the newest state; whatever byte mix of its old and new image a power
        /// cut leaves, the pick is the new state (only if the copy is whole) or the previous one.
        #[test]
        fn prop_torn_copy_selection(
            boots in (any::<u32>(), any::<u32>(), any::<u32>()),
            seq in 1u64..u64::MAX - 2,
            write_b in any::<bool>(),
            mix in proptest::collection::vec(any::<u8>(), PAGE),
            mix_kind in 0u8..3,
        ) {
            let state = |boot| { let mut s = sample(); s.boot_gen = boot; s };
            let (older, newest, next) = (state(boots.0), state(boots.1), state(boots.2));
            let (mut img_old, mut img_newest, mut img_next) = ([0u8; PAGE], [0u8; PAGE], [0u8; PAGE]);
            older.encode(seq, &mut img_old);
            newest.encode(seq + 1, &mut img_newest);
            next.encode(seq + 2, &mut img_next);
            // Torn image of the copy being overwritten: per-byte, per-512-sector or prefix mix.
            let mut torn = img_old;
            for i in 0..PAGE {
                let take_new = match mix_kind {
                    0 => mix[i] & 1 == 1,
                    1 => mix[i / 512 * 512] & 1 == 1,
                    _ => i < usize::from(mix[0]) * 16,
                };
                if take_new { torn[i] = img_next[i]; }
            }
            let (a, b) = if write_b { (img_newest, torn) } else { (torn, img_newest) };
            let (copy, s, got) = Superblock::pick(&a, &b).unwrap();
            if torn == img_next {
                prop_assert_eq!(s, seq + 2);
                prop_assert!(got == next);
                prop_assert_eq!(copy, if write_b { AbCopy::B } else { AbCopy::A });
            } else {
                prop_assert_eq!(s, seq + 1);
                prop_assert!(got == newest);
                prop_assert_eq!(copy, if write_b { AbCopy::A } else { AbCopy::B });
            }
        }

        #[test]
        fn prop_random_blocks_never_decode(b in proptest::collection::vec(any::<u8>(), PAGE)) {
            prop_assert!(Superblock::decode(&b).is_none());
        }
    }

    #[test]
    fn pick_prefers_newer_valid_copy() {
        let mut old = sample();
        old.boot_gen = 1;
        let mut new = sample();
        new.boot_gen = 2;
        let (mut a, mut b) = ([0u8; PAGE], [0u8; PAGE]);
        old.encode(10, &mut a);
        new.encode(11, &mut b);
        let (c, seq, s) = Superblock::pick(&a, &b).unwrap();
        assert_eq!((c, seq, s.boot_gen), (AbCopy::B, 11, 2));
        // B torn (its new image half written over an older B): A wins.
        let mut torn = b;
        torn[2048..].fill(0x5A);
        let (c, seq, s) = Superblock::pick(&a, &torn).unwrap();
        assert_eq!((c, seq, s.boot_gen), (AbCopy::A, 10, 1));
        assert!(Superblock::pick(&torn, &torn).is_none());
    }
}
