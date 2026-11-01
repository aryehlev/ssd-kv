//! `kvd format` (DESIGN §10.1, §10.10, §20.1, LC-34).
//!
//! Probes every device (refusing substrates outside LC-34), writes the superblocks, AG tables,
//! device-0 metadata blocks and reserves the two fixed snapshot regions per device (default 3% of
//! the device's capacity each, rounded up to 16 MiB; 0 disables them). Data files are
//! pre-allocated and always zero-filled, whatever their size, so no unwritten-extent conversion
//! ever happens while serving; they are never extended afterwards (LC-02). Raw devices may be
//! discarded first. This is the only module that pre-allocates file space, and every
//! pre-allocation is followed by the zero-fill pass (LC-34).
//!
//! Space is pre-allocated by writing it: a file is sized once, then every byte is written with
//! zeros through `O_DIRECT` and made durable, so every extent is a written extent before the
//! first start. Block devices keep their content except for the metadata area, which is zeroed
//! (the AG area may hold garbage: the keyed checksums never accept it, §10.1).
//!
//! Write order: zero fill, AG tables and device-0 blocks (both copies), flush, then the
//! superblocks (devices 1.. first, device 0 last), flush. A format interrupted before its last
//! flush leaves no valid superblock pair naming the new set.
//!
//! Refusals carry the [`SubstrateRefusal`] as the `io::Error`'s inner error
//! (`ErrorKind::Unsupported`), so `kvd format` can exit with code 1 and its one-line reason.

use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};

use crate::agtable::AgTableBlock;
use crate::devprobe::{self, SubstrateRefusal};
use crate::intent::IntentHeader;
use crate::io::{AlignedBuf, DevInfo, SyncFileDevice, SyncOpen};
use crate::layout::{AG_BYTES, AG_PAGES, DeviceLayout, MAX_DEVICES, PAGE, SB_PAGE};
use crate::pmeta::{NodeMeta, PartitionMeta};
use crate::superblock::{SB_CLEAN, SB_CLUSTER_MODE, Superblock};

/// Size of each of the two snapshot regions per device.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum SnapshotRegionSize {
    Bytes(u64),
    Percent(f32),
}

impl Default for SnapshotRegionSize {
    fn default() -> Self {
        SnapshotRegionSize::Percent(3.0)
    }
}

impl SnapshotRegionSize {
    /// Pages of each region on a device of `device_bytes` bytes: the requested size rounded up
    /// to whole 16 MiB units; 0 = no regions.
    pub fn region_pages(&self, device_bytes: u64) -> std::io::Result<u64> {
        let bytes = match *self {
            SnapshotRegionSize::Bytes(b) => b,
            SnapshotRegionSize::Percent(p) => {
                if !p.is_finite() || !(0.0..50.0).contains(&p) {
                    return Err(Error::new(ErrorKind::InvalidInput, "snapshot region percent must be in [0, 50)"));
                }
                (device_bytes as f64 * f64::from(p) / 100.0).ceil() as u64
            }
        };
        Ok(bytes.div_ceil(AG_BYTES) * u64::from(AG_PAGES))
    }

    /// Parses `--snapshot-region`: `N%`, a byte count with an optional `k`/`m`/`g`/`t` suffix
    /// (powers of 1024, optional trailing `b`), or `0`.
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim().to_ascii_lowercase();
        if let Some(p) = s.strip_suffix('%') {
            let p: f32 = p.trim().parse().ok()?;
            return (p.is_finite() && (0.0..50.0).contains(&p)).then_some(SnapshotRegionSize::Percent(p));
        }
        let s = s.strip_suffix('b').unwrap_or(&s);
        let (num, mult) = match s.chars().last()? {
            'k' => (&s[..s.len() - 1], 1u64 << 10),
            'm' => (&s[..s.len() - 1], 1 << 20),
            'g' => (&s[..s.len() - 1], 1 << 30),
            't' => (&s[..s.len() - 1], 1 << 40),
            _ => (s, 1),
        };
        num.trim().parse::<u64>().ok()?.checked_mul(mult).map(SnapshotRegionSize::Bytes)
    }
}

/// files are always zero-filled
#[derive(Clone, Debug)]
pub struct FormatOpts {
    /// `(path, size)`; the size is required for files that do not exist yet.
    pub devices: Vec<(PathBuf, Option<u64>)>,
    /// P: 16, 32, 64 or 128.
    pub partitions: u16,
    pub cluster: bool,
    /// `Bytes(u64) | Percent(f32)`, default 3%
    pub snapshot_region: SnapshotRegionSize,
}

/// Secret and identifying values chosen once per format.
#[derive(Clone)]
pub(crate) struct Secrets {
    pub(crate) set_uuid: [u8; 16],
    pub(crate) device_uuids: Vec<[u8; 16]>,
    pub(crate) hash_seed: u64,
    pub(crate) record_crc_salt: u32,
}

impl Secrets {
    /// Fresh values from the kernel's random source.
    fn random(devices: usize) -> std::io::Result<Self> {
        use std::io::Read;
        let mut f = std::fs::File::open("/dev/urandom")?;
        let mut b = vec![0u8; 16 + 16 * devices + 12];
        f.read_exact(&mut b)?;
        let mut device_uuids = Vec::with_capacity(devices);
        for i in 0..devices {
            device_uuids.push(uuid_v4(b[16 + 16 * i..32 + 16 * i].try_into().expect("16 bytes")));
        }
        let tail = &b[16 + 16 * devices..];
        Ok(Secrets {
            set_uuid: uuid_v4(b[..16].try_into().expect("16 bytes")),
            device_uuids,
            hash_seed: u64::from_le_bytes(tail[..8].try_into().expect("8 bytes")),
            record_crc_salt: u32::from_le_bytes(tail[8..12].try_into().expect("4 bytes")),
        })
    }
}

/// RFC 4122 version 4 / variant 1 bits on 16 random bytes.
fn uuid_v4(mut b: [u8; 16]) -> [u8; 16] {
    b[6] = (b[6] & 0x0F) | 0x40;
    b[8] = (b[8] & 0x3F) | 0x80;
    b
}

pub fn format(opts: &FormatOpts) -> std::io::Result<()> {
    let secrets = Secrets::random(opts.devices.len())?;
    format_with(opts, kv_common::Now::read().unix_ms, &secrets)
}

/// One device being formatted.
struct Target {
    path: PathBuf,
    created: bool,
    block: bool,
    bytes: u64,
    lba: u32,
    layout: DeviceLayout,
}

fn invalid(msg: &'static str) -> Error {
    Error::new(ErrorKind::InvalidInput, msg)
}

fn refused(r: SubstrateRefusal) -> Error {
    Error::new(ErrorKind::Unsupported, r)
}

/// Formats with given secrets and creation time (deterministic; used by `format` and tests).
pub(crate) fn format_with(opts: &FormatOpts, now_ms: u64, secrets: &Secrets) -> std::io::Result<()> {
    if opts.devices.is_empty() || opts.devices.len() > MAX_DEVICES {
        return Err(invalid("between 1 and 16 devices are required"));
    }
    if !matches!(opts.partitions, 16 | 32 | 64 | 128) {
        return Err(invalid("partitions must be 16, 32, 64 or 128"));
    }
    if secrets.device_uuids.len() != opts.devices.len() {
        return Err(invalid("one device uuid per device"));
    }
    let mut targets: Vec<Target> = Vec::with_capacity(opts.devices.len());
    for (i, (path, size)) in opts.devices.iter().enumerate() {
        let made = ensure(path, *size).and_then(|(created, block)| {
            prepare(i, path, *size, created, block, opts.snapshot_region).inspect_err(|_| {
                if created {
                    let _ = std::fs::remove_file(path);
                }
            })
        });
        match made {
            Ok(t) => targets.push(t),
            Err(e) => {
                cleanup(&targets);
                return Err(e);
            }
        }
    }
    let result = write_all(opts, now_ms, secrets, &targets);
    if result.is_err() {
        cleanup(&targets);
    }
    result
}

/// Files this format created are removed again when it fails.
fn cleanup(targets: &[Target]) {
    for t in targets.iter().filter(|t| t.created) {
        let _ = std::fs::remove_file(&t.path);
    }
}

/// Classifies `path`, creating a missing data file (which then needs a size): `(created, block)`.
fn ensure(path: &Path, size: Option<u64>) -> std::io::Result<(bool, bool)> {
    use std::os::unix::fs::FileTypeExt;
    match std::fs::metadata(path) {
        Ok(md) if md.file_type().is_block_device() => Ok((false, true)),
        Ok(md) if md.is_file() => Ok((false, false)),
        Ok(_) => Err(refused(SubstrateRefusal::NotFileOrBlock)),
        Err(e) if e.kind() == ErrorKind::NotFound => {
            if size.is_none() {
                return Err(invalid("a size is required to create a data file"));
            }
            std::fs::OpenOptions::new().write(true).create_new(true).open(path)?;
            Ok((true, false))
        }
        Err(e) => Err(e),
    }
}

/// Probes, sizes and lays out device `i`.
fn prepare(i: usize, path: &Path, size: Option<u64>, created: bool, block: bool, region: SnapshotRegionSize) -> std::io::Result<Target> {
    let probe = devprobe::probe(path).map_err(refused)?;
    let bytes = if block {
        let have = crate::io::device_size(&std::fs::File::open(path)?)?;
        match size {
            Some(s) if s > have => return Err(invalid("requested size exceeds the block device")),
            Some(s) => s,
            None => have,
        }
    } else {
        let want = match size {
            Some(s) => s,
            None => std::fs::metadata(path)?.len(),
        };
        let want = want / PAGE as u64 * PAGE as u64;
        // The one place a data file is ever sized (LC-02); the zero fill follows.
        std::fs::OpenOptions::new().write(true).open(path)?.set_len(want)?;
        want
    };
    let pages = bytes / PAGE as u64;
    let region_pages = region.region_pages(pages * PAGE as u64)?;
    let layout = DeviceLayout::compute(pages, i == 0, region_pages).map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
    Ok(Target { path: path.to_path_buf(), created, block, bytes: pages * PAGE as u64, lba: probe.lba, layout })
}

/// Zero chunk for the fill pass.
const FILL_CHUNK: usize = 8 << 20;

fn write_all(opts: &FormatOpts, now_ms: u64, secrets: &Secrets, targets: &[Target]) -> std::io::Result<()> {
    let mut devs = Vec::with_capacity(targets.len());
    for t in targets {
        let info = DevInfo::new(t.layout.device_pages, t.lba);
        devs.push(open_direct(&t.path, info)?);
    }
    let zero = AlignedBuf::zeroed(FILL_CHUNK);
    for (t, d) in targets.iter().zip(devs.iter_mut()) {
        // Files: every byte. Block devices: the metadata area before the snapshot regions / AGs.
        let end = if t.block {
            let l = &t.layout;
            let first = if l.snap_region_pages == 0 { l.ag_area_page } else { l.snap_region_page[0] };
            first * PAGE as u64
        } else {
            t.bytes
        };
        zero_fill(d, &zero, end)?;
    }
    drop(zero);

    let p = opts.partitions;
    let mut page = AlignedBuf::zeroed(PAGE);
    for (i, (t, d)) in targets.iter().zip(devs.iter_mut()).enumerate() {
        let l = &t.layout;
        for b in 0..l.ag_table_blocks {
            let blk = AgTableBlock::free(b, l.n_ags);
            for (c, seq) in COPIES {
                blk.encode(seq, page.page_mut(0));
                write_page(d, &page, DeviceLayout::ag_table_copy_page(b, c))?;
            }
        }
        if i == 0 {
            for part in 0..p {
                let m = PartitionMeta::initial(part);
                for (c, seq) in COPIES {
                    m.encode(p, seq, page.page_mut(0));
                    write_page(d, &page, l.pmeta_copy_page(part, c))?;
                }
            }
            let n = NodeMeta::initial();
            let h = IntentHeader::initial();
            for (c, seq) in COPIES {
                n.encode(seq, page.page_mut(0));
                write_page(d, &page, l.nmeta_copy_page(c))?;
                h.encode(seq, page.page_mut(0));
                write_page(d, &page, l.intent_header_copy_page(c))?;
            }
            // Intent slots stay zero (no valid copy = no committed id): the fill covered them.
        }
        d.sync()?;
    }

    let sbs: Vec<Superblock> = targets.iter().enumerate().map(|(i, t)| superblock(opts, now_ms, secrets, targets, i, t)).collect();
    // Devices 1.. first, device 0 (authoritative) last.
    for i in (1..targets.len()).chain(core::iter::once(0)) {
        for (c, seq) in COPIES {
            sbs[i].encode(seq, page.page_mut(0));
            write_page(&mut devs[i], &page, SB_PAGE[c.index()])?;
        }
        devs[i].sync()?;
    }
    Ok(())
}

/// Both copies are written at format, A with `block_seq` 1 and B with 2; the first rewrite
/// then goes to A with 3.
const COPIES: [(kv_common::abblock::AbCopy, u64); 2] = [(kv_common::abblock::AbCopy::A, 1), (kv_common::abblock::AbCopy::B, 2)];

fn superblock(opts: &FormatOpts, now_ms: u64, s: &Secrets, targets: &[Target], i: usize, t: &Target) -> Superblock {
    let mut member_uuids = [[0u8; 16]; 16];
    if i == 0 {
        member_uuids[..s.device_uuids.len()].copy_from_slice(&s.device_uuids);
    }
    let mut sb = Superblock {
        set_uuid: s.set_uuid,
        device_uuid: s.device_uuids[i],
        device_index: i as u16,
        device_count: targets.len() as u16,
        partitions: opts.partitions,
        flags: SB_CLEAN | if opts.cluster { SB_CLUSTER_MODE } else { 0 },
        page_size: PAGE as u32,
        ag_pages: AG_PAGES,
        lba: t.lba,
        n_ags: 0,
        device_pages: 0,
        ag_table_page: 0,
        ag_table_blocks: 0,
        ag_area_page: 0,
        pmeta_page: 0,
        nmeta_page: 0,
        intent_page: 0,
        intent_slot_page: 0,
        hash_seed: s.hash_seed,
        record_crc_salt: s.record_crc_salt,
        boot_gen: 0,
        clock_floor_unix_ms: now_ms,
        created_unix_ms: now_ms,
        snap_seq: 0,
        snap_header_crc: 0,
        snap_region: 0,
        snap_bytes: 0,
        databases: if opts.cluster { 1 } else { 16 },
        snap_region_page: [0, 0],
        snap_region_pages: 0,
        member_uuids,
    };
    sb.set_layout(&t.layout);
    sb
}

fn open_direct(path: &Path, info: DevInfo) -> std::io::Result<SyncFileDevice> {
    SyncFileDevice::open_with(path, info, SyncOpen { direct: true, exclusive: true, read_only: false })
}

fn zero_fill(d: &mut SyncFileDevice, zero: &AlignedBuf, end: u64) -> std::io::Result<()> {
    let mut off = 0u64;
    while off < end {
        let len = (end - off).min(zero.len() as u64) as usize;
        d.write_at(off, &zero[..len])?;
        off += len as u64;
    }
    d.sync()
}

fn write_page(d: &mut SyncFileDevice, page: &AlignedBuf, at: u64) -> std::io::Result<()> {
    d.write_at(at * PAGE as u64, &page[..PAGE])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent::{INTENT_SLOTS, IntentSlot};
    use kv_common::abblock::AbCopy;

    fn secrets(n: usize) -> Secrets {
        Secrets {
            set_uuid: [7; 16],
            device_uuids: (0..n).map(|i| [i as u8 + 1; 16]).collect(),
            hash_seed: 0xFEED,
            record_crc_salt: 0xBEEF,
        }
    }

    /// True when the test directory is on an allowed substrate with `O_DIRECT` (as on the CI
    /// box); format tests are skipped elsewhere.
    fn substrate_ok(dir: &Path) -> bool {
        let p = dir.join("probe.img");
        std::fs::write(&p, vec![0u8; PAGE]).unwrap();
        let ok = devprobe::probe(&p).is_ok()
            && SyncFileDevice::open_with(&p, DevInfo::new(0, 512), SyncOpen { direct: true, exclusive: false, read_only: true }).is_ok();
        std::fs::remove_file(&p).unwrap();
        if !ok {
            eprintln!("skipping: test directory is not on XFS/ext4 with O_DIRECT");
        }
        ok
    }

    fn read_page(path: &Path, page: u64) -> Vec<u8> {
        use std::os::unix::fs::FileExt;
        let f = std::fs::File::open(path).unwrap();
        let mut b = vec![0u8; PAGE];
        f.read_exact_at(&mut b, page * PAGE as u64).unwrap();
        b
    }

    #[test]
    fn region_sizes() {
        let gib = 1u64 << 30;
        assert_eq!(SnapshotRegionSize::Percent(3.0).region_pages(100 * gib).unwrap(), 192 * 4096); // 3.07 GB -> 192 x 16 MiB
        assert_eq!(SnapshotRegionSize::Percent(3.0).region_pages(gib).unwrap(), 2 * 4096);
        assert_eq!(SnapshotRegionSize::Percent(0.0).region_pages(gib).unwrap(), 0);
        assert_eq!(SnapshotRegionSize::Bytes(0).region_pages(gib).unwrap(), 0);
        assert_eq!(SnapshotRegionSize::Bytes(1).region_pages(gib).unwrap(), 4096);
        assert_eq!(SnapshotRegionSize::Bytes(16 << 20).region_pages(gib).unwrap(), 4096);
        assert!(SnapshotRegionSize::Percent(60.0).region_pages(gib).is_err());
        assert!(SnapshotRegionSize::Percent(f32::NAN).region_pages(gib).is_err());
        assert_eq!(SnapshotRegionSize::default(), SnapshotRegionSize::Percent(3.0));
        assert_eq!(SnapshotRegionSize::parse("3%"), Some(SnapshotRegionSize::Percent(3.0)));
        assert_eq!(SnapshotRegionSize::parse("0"), Some(SnapshotRegionSize::Bytes(0)));
        assert_eq!(SnapshotRegionSize::parse("64m"), Some(SnapshotRegionSize::Bytes(64 << 20)));
        assert_eq!(SnapshotRegionSize::parse("2GB"), Some(SnapshotRegionSize::Bytes(2 << 30)));
        assert_eq!(SnapshotRegionSize::parse("x"), None);
        assert_eq!(SnapshotRegionSize::parse("80%"), None);
    }

    #[test]
    fn formats_two_files() {
        let dir = tempfile::tempdir().unwrap();
        let d0 = dir.path().join("d0.img");
        let d1 = dir.path().join("d1.img");
        std::fs::write(&d1, vec![0xAAu8; 64 << 20]).unwrap(); // existing garbage is zeroed
        let opts = FormatOpts {
            devices: vec![(d0.clone(), Some(96 << 20)), (d1.clone(), None)],
            partitions: 16,
            cluster: false,
            snapshot_region: SnapshotRegionSize::Percent(3.0),
        };
        if !substrate_ok(dir.path()) {
            return;
        }
        format_with(&opts, 1_790_000_000_000, &secrets(2)).unwrap();
        assert_eq!(std::fs::metadata(&d0).unwrap().len(), 96 << 20);
        assert_eq!(std::fs::metadata(&d1).unwrap().len(), 64 << 20);

        let mut sbs = Vec::new();
        for (i, p) in [&d0, &d1].into_iter().enumerate() {
            let a = read_page(p, 0);
            let b = read_page(p, 1);
            let (sa, x) = Superblock::decode(&a).unwrap();
            let (sb, y) = Superblock::decode(&b).unwrap();
            assert_eq!((sa, sb), (1, 2));
            assert!(x == y);
            x.validate().unwrap();
            assert_eq!(x.device_index, i as u16);
            assert_eq!(x.device_count, 2);
            assert_eq!(x.set_uuid, [7; 16]);
            assert_eq!(x.device_uuid, [i as u8 + 1; 16]);
            assert_eq!((x.hash_seed, x.record_crc_salt, x.boot_gen), (0xFEED, 0xBEEF, 0));
            assert_eq!((x.created_unix_ms, x.clock_floor_unix_ms), (1_790_000_000_000, 1_790_000_000_000));
            assert!(x.is_clean() && !x.snapshot_valid() && !x.cluster_mode());
            assert_eq!(x.databases, 16);
            assert_eq!(x.snap_region_pages, 4096);
            assert_eq!(x.snap_region_page, [4096, 8192]);
            assert_eq!(x.ag_area_page, 12288);
            sbs.push(x);
        }
        assert_eq!((sbs[0].n_ags, sbs[1].n_ags), (3, 1));
        assert_eq!(&sbs[0].member_uuids[..2], &[[1u8; 16], [2u8; 16]]);
        assert_eq!(sbs[1].member_uuids, [[0u8; 16]; 16]);
        assert!(sbs[0].same_set(&sbs[1]));
        assert_eq!(sbs[1].pmeta_page, 0);

        // AG tables: every AG free, both copies.
        for (p, sb) in [(&d0, &sbs[0]), (&d1, &sbs[1])] {
            for blk in 0..sb.ag_table_blocks {
                let a = read_page(p, DeviceLayout::ag_table_copy_page(blk, AbCopy::A));
                let b = read_page(p, DeviceLayout::ag_table_copy_page(blk, AbCopy::B));
                let (c, seq, t) = AgTableBlock::pick(blk, &a, &b).unwrap();
                assert_eq!((c, seq), (AbCopy::B, 2));
                assert_eq!(t, AgTableBlock::free(blk, sb.n_ags));
            }
        }
        // Device-0 blocks.
        let l = sbs[0].layout();
        for part in 0..16 {
            let a = read_page(&d0, l.pmeta_copy_page(part, AbCopy::A));
            let b = read_page(&d0, l.pmeta_copy_page(part, AbCopy::B));
            let (_, _, m) = PartitionMeta::pick(part, 16, &a, &b).unwrap();
            assert_eq!(m, PartitionMeta::initial(part));
        }
        assert!(read_page(&d0, l.pmeta_copy_page(16, AbCopy::A)).iter().all(|&x| x == 0));
        let n = NodeMeta::pick(&read_page(&d0, l.nmeta_copy_page(AbCopy::A)), &read_page(&d0, l.nmeta_copy_page(AbCopy::B)));
        assert_eq!(n.map(|(_, _, m)| m), Some(NodeMeta::initial()));
        let h = IntentHeader::pick(&read_page(&d0, l.intent_header_copy_page(AbCopy::A)), &read_page(&d0, l.intent_header_copy_page(AbCopy::B)));
        assert_eq!(h.map(|(_, _, h)| h), Some(IntentHeader::initial()));
        for s in 0..INTENT_SLOTS as u32 {
            for c in [AbCopy::A, AbCopy::B] {
                let off = l.intent_slot_copy_offset(sbs[0].lba, s, c);
                let pg = read_page(&d0, off / PAGE as u64);
                let at = (off % PAGE as u64) as usize;
                assert!(IntentSlot::decode(&pg[at..]).is_none());
            }
        }
        // Everything else is zero, including the AG area and the regions of the old garbage file.
        let data = std::fs::read(&d1).unwrap();
        let meta_end = 4 + 2 * sbs[1].ag_table_blocks as usize;
        assert!(data[meta_end * PAGE..].iter().all(|&x| x == 0));
        assert!(data[2 * PAGE..4 * PAGE].iter().all(|&x| x == 0));
    }

    #[test]
    fn cluster_mode_and_no_regions() {
        let dir = tempfile::tempdir().unwrap();
        let d0 = dir.path().join("c.img");
        let opts = FormatOpts {
            devices: vec![(d0.clone(), Some(40 << 20))],
            partitions: 128,
            cluster: true,
            snapshot_region: SnapshotRegionSize::Bytes(0),
        };
        if !substrate_ok(dir.path()) {
            return;
        }
        format_with(&opts, 5, &secrets(1)).unwrap();
        let (_, sb) = Superblock::decode(&read_page(&d0, 0)).unwrap();
        sb.validate().unwrap();
        assert!(sb.cluster_mode());
        assert_eq!((sb.databases, sb.partitions, sb.snap_region_pages, sb.snap_region_page), (1, 128, 0, [0, 0]));
        assert_eq!(sb.ag_area_page, 4096);
        assert_eq!(sb.n_ags, 1);
        let l = sb.layout();
        let (_, _, m) =
            PartitionMeta::pick(127, 128, &read_page(&d0, l.pmeta_copy_page(127, AbCopy::A)), &read_page(&d0, l.pmeta_copy_page(127, AbCopy::B)))
                .unwrap();
        assert_eq!(m.partition, 127);
    }

    #[test]
    fn reformat_replaces_secrets_and_clears_slots() {
        let dir = tempfile::tempdir().unwrap();
        let d0 = dir.path().join("r.img");
        let opts = FormatOpts {
            devices: vec![(d0.clone(), Some(48 << 20))],
            partitions: 16,
            cluster: false,
            snapshot_region: SnapshotRegionSize::Bytes(0),
        };
        if !substrate_ok(dir.path()) {
            return;
        }
        format_with(&opts, 5, &secrets(1)).unwrap();
        // Plant a valid slot copy as an earlier set would have left it.
        let (_, sb) = Superblock::decode(&read_page(&d0, 0)).unwrap();
        let off = sb.layout().intent_slot_copy_offset(sb.lba, 0, AbCopy::A);
        let mut slot = [0u8; 512];
        IntentSlot { copy_seq: 9, txn_ids: smallvec::smallvec![42] }.encode(&mut slot);
        {
            use std::os::unix::fs::FileExt;
            std::fs::OpenOptions::new().write(true).open(&d0).unwrap().write_all_at(&slot, off).unwrap();
        }
        let mut s2 = secrets(1);
        s2.record_crc_salt = 0x1234;
        format_with(&opts, 6, &s2).unwrap();
        let (_, sb) = Superblock::decode(&read_page(&d0, 0)).unwrap();
        assert_eq!(sb.record_crc_salt, 0x1234);
        let pg = read_page(&d0, off / PAGE as u64);
        assert!(IntentSlot::decode(&pg[(off % PAGE as u64) as usize..]).is_none());
    }

    #[test]
    fn rejects_bad_options() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("x.img");
        let mk = |devices: Vec<(PathBuf, Option<u64>)>, partitions| FormatOpts {
            devices,
            partitions,
            cluster: false,
            snapshot_region: SnapshotRegionSize::default(),
        };
        let e = format_with(&mk(vec![(p.clone(), Some(64 << 20))], 48), 0, &secrets(1)).unwrap_err();
        assert_eq!(e.kind(), ErrorKind::InvalidInput);
        assert!(format_with(&mk(vec![], 16), 0, &secrets(0)).is_err());
        let many: Vec<_> = (0..17).map(|i| (dir.path().join(format!("{i}")), Some(64u64 << 20))).collect();
        assert!(format_with(&mk(many, 16), 0, &secrets(17)).is_err());
        // a new file needs a size
        assert_eq!(format_with(&mk(vec![(p.clone(), None)], 16), 0, &secrets(1)).unwrap_err().kind(), ErrorKind::InvalidInput);
        assert!(!p.exists());
        // too small for one AG after the metadata and regions: refused, created file removed
        let e = format_with(&mk(vec![(p.clone(), Some(20 << 20))], 16), 0, &secrets(1)).unwrap_err();
        assert!(matches!(e.kind(), ErrorKind::InvalidInput | ErrorKind::Unsupported), "{e}");
        assert!(!p.exists());
        // a directory is neither a file nor a block device
        let e = format_with(&mk(vec![(dir.path().to_path_buf(), None)], 16), 0, &secrets(1)).unwrap_err();
        assert_eq!(e.kind(), ErrorKind::Unsupported);
        let inner = e.get_ref().and_then(|r| r.downcast_ref::<SubstrateRefusal>());
        assert_eq!(inner, Some(&SubstrateRefusal::NotFileOrBlock));
    }

    #[test]
    fn uuid_bits() {
        let u = uuid_v4([0xFF; 16]);
        assert_eq!(u[6] >> 4, 4);
        assert_eq!(u[8] >> 6, 2);
        let s = Secrets::random(3).unwrap();
        assert_eq!(s.device_uuids.len(), 3);
        assert_ne!(s.device_uuids[0], s.device_uuids[1]);
    }
}
