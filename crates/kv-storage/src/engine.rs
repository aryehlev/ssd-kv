//! The storage engine entry point (DESIGN §18.1, LC-03).
//!
//! [`StorageEngine`] has exactly one implementation, [`PageStore`] (LC-03): `open` probes and
//! opens the devices (substrate rules LC-34, page-update and cache modes), reads superblocks, AG
//! tables, metas and the intent table, writes the startup superblock (E7) and returns one
//! [`PartitionStore`] per partition for the workers to recover and serve; `shutdown` writes the
//! clean-shutdown snapshot when every partition handed in its section and it fits its region,
//! then sets CLEAN (§8.2 SHUTDOWN, §10.10, E8).
//!
//! The engine-wide facts every partition needs (device geometry and modes, the partition
//! metas, the committed-txn view, AG ownership, the snapshot directory) are read once into a
//! shared [`Boot`]. [`LocalEngine`] drives a whole engine (partitions, page cache, flush
//! trackers and the meta logic) synchronously in one thread over any [`BlockDevice`]s: tools,
//! tests and the deterministic simulator use it.

use std::io::{Error, ErrorKind};
use std::path::PathBuf;
use std::sync::Arc;

use kv_common::Now;
use kv_common::abblock::AbCopy;

use crate::agtable::{AgKind, AgTableBlock};
use crate::cache::PageCache;
use crate::devprobe;
use crate::flush::{FlushTracker, flush_tag_dev};
use crate::intent::{CommittedSet, INTENT_SLOTS, IntentHeader, SlotPair};
use crate::io::{AlignedBuf, BlockDevice, DevInfo, IoDone, IoReq, IoTag, PageUpdateMode, SyncFileDevice, SyncOpen};
use crate::layout::{AgId, DeviceLayout, PAGE, SB_PAGE};
use crate::metastore::{MetaClient, MetaCore, MetaInbox, MetaIo, SbLink};
use crate::partition::{BgRates, Durability, PartitionCfg, PartitionStore, StorageCtx, WriteGranularity};
use crate::pmeta::{NodeMeta, PartitionMeta};
use crate::recovery::{RecoveryMode, RecoveryProgress};
use crate::snapshot::{SnapshotDir, SnapshotWriter};
use crate::superblock::{SbUpdate, Superblock};

/// `page-update` setting (§13.1, §20.2).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PageUpdatePolicy {
    Auto,
    TwoImage,
    /// Refuses to start unless the device verifies atomic 4 KiB writes.
    AtomicOverwrite,
}

/// `device-cache` setting (§13.8).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DeviceCachePolicy {
    Auto,
    WriteThrough,
    WriteBack,
}

/// `flush-method` setting (§13.8).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FlushMethod {
    Auto,
    Fdatasync,
    Dsync,
    None,
}

/// `write-granularity` setting (§13.6).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum GranularityPolicy {
    Auto,
    Page,
    Lba,
}

/// Engine configuration derived from the node configuration (§20.2).
#[derive(Clone, Debug)]
pub struct EngineCfg {
    /// Data devices or files, in device-index order.
    pub devices: Vec<PathBuf>,
    pub workers: u16,
    pub cluster: bool,
    pub durability: Durability,
    pub page_update: PageUpdatePolicy,
    pub device_cache: DeviceCachePolicy,
    pub flush_method: FlushMethod,
    pub write_granularity: GranularityPolicy,
    pub page_slack_cap: u16,
    pub page_slack_atomic: u16,
    pub max_storage_fill: f32,
    pub cache_bytes: usize,
    pub recovery_qd: u32,
    pub rates: BgRates,
}

impl EngineCfg {
    /// Defaults of §20.2 for `devices` (tests and tools).
    pub fn defaults(devices: Vec<PathBuf>) -> Self {
        EngineCfg {
            devices,
            workers: 1,
            cluster: false,
            durability: Durability::Written,
            page_update: PageUpdatePolicy::Auto,
            device_cache: DeviceCachePolicy::Auto,
            flush_method: FlushMethod::Auto,
            write_granularity: GranularityPolicy::Auto,
            page_slack_cap: 1024,
            page_slack_atomic: 128,
            max_storage_fill: 0.95,
            cache_bytes: 64 << 20,
            recovery_qd: 16,
            rates: BgRates::defaults(16),
        }
    }
}

pub trait StorageEngine: Sized {
    type Partition;
    fn open(cfg: &EngineCfg) -> std::io::Result<(Self, Vec<Self::Partition>)>;
    fn shutdown(&mut self, clean: bool) -> std::io::Result<()>;
}

// ---------------------------------------------------------------------------------------------
// Boot: engine-wide facts shared by every partition

/// Per-device facts established at open.
#[derive(Clone, Copy, Debug)]
pub(crate) struct DevBoot {
    pub(crate) lba: u32,
    pub(crate) page_update: PageUpdateMode,
    pub(crate) atomic_unit: Option<(u32, u32)>,
    /// Writes carry `RWF_DSYNC` (`flush-method dsync`): durable at completion.
    pub(crate) dsync: bool,
}

impl DevBoot {
    fn from_info(i: &DevInfo, dsync: bool) -> Self {
        DevBoot { lba: i.lba.max(512), page_update: i.page_update, atomic_unit: i.atomic_unit, dsync }
    }
}

/// What every partition of an opened engine shares (read once at open, §18.1).
pub struct Boot {
    pub(crate) partitions: u16,
    pub(crate) databases: u8,
    pub(crate) cluster: bool,
    pub(crate) hash_seed: u64,
    pub(crate) salt: u32,
    /// The generation of this start (after the startup superblock, E7).
    pub(crate) boot_gen: u32,
    /// The durable clock floor read at startup (E13).
    pub(crate) clock_floor_ms: u64,
    pub(crate) layouts: Arc<[DeviceLayout]>,
    pub(crate) devs: Vec<DevBoot>,
    pub(crate) pmeta: Vec<PartitionMeta>,
    /// Committed txn ids of earlier boots (§10.7).
    pub(crate) committed: CommittedSet,
    /// AGs per owner partition with their kind (§10.4).
    pub(crate) owned: Vec<Vec<(AgId, AgKind)>>,
    /// The snapshot the superblock names, when CLEAN and SNAPSHOT_VALID (§18.1 step 3).
    pub(crate) snapshot: Option<SnapshotDir>,
    /// Sections handed in by partitions for the clean-shutdown snapshot (§10.10).
    pub(crate) collector: SnapshotWriter,
    /// Superblock of device 0 at open (after the startup rewrite), its block seq and the copy
    /// holding it.
    pub(crate) sb0: Superblock,
    pub(crate) sb0_seq: u64,
    pub(crate) sb0_copy: AbCopy,
}

impl Boot {
    /// A device-less engine view (partitions built by [`PartitionStore::new`]).
    pub fn empty(partitions: u16, databases: u8) -> Self {
        Boot {
            partitions: partitions.max(1),
            databases: databases.max(1),
            cluster: false,
            hash_seed: 0,
            salt: 0,
            boot_gen: 1,
            clock_floor_ms: 0,
            layouts: Arc::from(Vec::new()),
            devs: Vec::new(),
            pmeta: (0..partitions).map(PartitionMeta::initial).collect(),
            committed: CommittedSet::default(),
            owned: vec![Vec::new(); usize::from(partitions.max(1))],
            snapshot: None,
            collector: SnapshotWriter::detached(partitions.max(1)),
            sb0: blank_superblock(),
            sb0_seq: 0,
            sb0_copy: AbCopy::B,
        }
    }

    pub fn partitions(&self) -> u16 {
        self.partitions
    }

    pub fn databases(&self) -> u8 {
        self.databases
    }

    pub fn cluster(&self) -> bool {
        self.cluster
    }

    pub fn boot_gen(&self) -> u32 {
        self.boot_gen
    }

    /// The restart path the superblock allows (§18.1 step 3).
    pub fn recovery_mode(&self) -> RecoveryMode {
        if self.snapshot.is_some() { RecoveryMode::Snapshot } else { RecoveryMode::FullScan }
    }

    /// The seed of key hashes (`KeyHash::of(hash_seed, key)`, §6.2).
    pub fn hash_seed(&self) -> u64 {
        self.hash_seed
    }

    /// The keyed record checksum of this engine (tools: `kvd inspect`, tests).
    pub fn record_crc(&self) -> kv_common::RecordCrc {
        kv_common::RecordCrc::new(self.salt)
    }

    /// The geometry of every device.
    pub fn layouts(&self) -> &[DeviceLayout] {
        &self.layouts
    }

    /// The committed-txn view of earlier boots (§10.7).
    pub fn committed(&self) -> &CommittedSet {
        &self.committed
    }

    /// The device-0 superblock as written at startup.
    pub fn superblock(&self) -> &Superblock {
        &self.sb0
    }

    /// Handle that partitions hand their clean-shutdown sections to.
    pub fn snapshot_writer(&self) -> SnapshotWriter {
        self.collector.clone()
    }

    /// Reads every fixed structure of an engine from its devices (§18.1 step 1).
    ///
    /// `devs[i]` must be the device with index `i`. Checks the set (uuid, indexes, geometry);
    /// does not write anything.
    pub fn load(devs: &mut [&mut dyn BlockDevice], dsync: bool) -> std::io::Result<Boot> {
        if devs.is_empty() {
            return Err(Error::new(ErrorKind::InvalidInput, "no data devices"));
        }
        let mut sbs: Vec<(u64, Superblock)> = Vec::with_capacity(devs.len());
        let mut sb0_copy = AbCopy::A;
        for (i, d) in devs.iter_mut().enumerate() {
            let a = read_sync(&mut **d, SB_PAGE[0] * PAGE as u64, PAGE)?;
            let b = read_sync(&mut **d, SB_PAGE[1] * PAGE as u64, PAGE)?;
            let Some((copy, seq, sb)) = Superblock::pick(&a, &b) else {
                return Err(Error::new(ErrorKind::InvalidData, format!("device {i} holds no valid superblock (run `kvd format`)")));
            };
            sb.validate().map_err(|e| Error::new(ErrorKind::InvalidData, format!("device {i}: {e}")))?;
            if usize::from(sb.device_index) != i {
                return Err(Error::new(ErrorKind::InvalidData, format!("device {i} is device {} of its set", sb.device_index)));
            }
            if i == 0 {
                sb0_copy = copy;
            }
            sbs.push((seq, sb));
        }
        let (sb0_seq, sb0) = sbs[0].clone();
        if usize::from(sb0.device_count) != devs.len() {
            return Err(Error::new(ErrorKind::InvalidData, format!("the set has {} devices, {} given", sb0.device_count, devs.len())));
        }
        for (i, (_, sb)) in sbs.iter().enumerate() {
            if !sb.same_set(&sb0) || sb0.member_uuids[i] != sb.device_uuid {
                return Err(Error::new(ErrorKind::InvalidData, format!("device {i} belongs to another set")));
            }
        }
        let layouts: Vec<DeviceLayout> = sbs.iter().map(|(_, sb)| sb.layout()).collect();
        for (i, l) in layouts.iter().enumerate() {
            l.check(i == 0).map_err(|e| Error::new(ErrorKind::InvalidData, format!("device {i}: {e}")))?;
        }
        let partitions = sb0.partitions;
        // AG tables (§10.4): ownership per partition
        let mut owned: Vec<Vec<(AgId, AgKind)>> = vec![Vec::new(); usize::from(partitions)];
        for (dev, (d, l)) in devs.iter_mut().zip(layouts.iter()).enumerate() {
            for blk in 0..l.ag_table_blocks {
                let a = read_sync(&mut **d, DeviceLayout::ag_table_copy_page(blk, AbCopy::A) * PAGE as u64, PAGE)?;
                let b = read_sync(&mut **d, DeviceLayout::ag_table_copy_page(blk, AbCopy::B) * PAGE as u64, PAGE)?;
                let Some((_, _, t)) = AgTableBlock::pick(blk, &a, &b) else {
                    return Err(Error::new(ErrorKind::InvalidData, format!("device {dev}: AG table block {blk} unreadable")));
                };
                for (i, &(kind, owner)) in t.entries.iter().enumerate() {
                    let idx = t.ag_of(i);
                    if kind == AgKind::Free || idx >= l.n_ags {
                        continue;
                    }
                    if let Some(v) = owned.get_mut(usize::from(owner)) {
                        v.push((AgId { dev: dev as u8, idx }, kind));
                    }
                }
            }
        }
        // device 0: partition metas, intent table
        let l0 = layouts[0];
        let d0 = &mut *devs[0];
        let mut pmeta = Vec::with_capacity(usize::from(partitions));
        for p in 0..partitions {
            let a = read_sync(d0, l0.pmeta_copy_page(p, AbCopy::A) * PAGE as u64, PAGE)?;
            let b = read_sync(d0, l0.pmeta_copy_page(p, AbCopy::B) * PAGE as u64, PAGE)?;
            let m = PartitionMeta::pick(p, partitions, &a, &b).map(|(_, _, m)| m).ok_or_else(|| {
                Error::new(ErrorKind::InvalidData, format!("partition meta {p} unreadable"))
            })?;
            pmeta.push(m);
        }
        let a = read_sync(d0, l0.intent_header_copy_page(AbCopy::A) * PAGE as u64, PAGE)?;
        let b = read_sync(d0, l0.intent_header_copy_page(AbCopy::B) * PAGE as u64, PAGE)?;
        let (_, _, ih) = IntentHeader::pick(&a, &b).ok_or_else(|| Error::new(ErrorKind::InvalidData, "intent header unreadable"))?;
        let slots = read_intent_slots(d0, &l0, sbs[0].1.lba.max(512))?;
        let committed = CommittedSet::new(ih.resolved_w, &slots);
        let devboot: Vec<DevBoot> = devs.iter().map(|d| DevBoot::from_info(&d.info(), dsync)).collect();
        let layouts: Arc<[DeviceLayout]> = Arc::from(layouts);
        let snapshot = SnapshotDir::read(devs, &layouts, &sb0)?;
        Ok(Boot {
            partitions,
            databases: sb0.databases.clamp(1, 16) as u8,
            cluster: sb0.cluster_mode(),
            hash_seed: sb0.hash_seed,
            salt: sb0.record_crc_salt,
            boot_gen: sb0.boot_gen,
            clock_floor_ms: sb0.clock_floor_unix_ms,
            layouts,
            devs: devboot,
            pmeta,
            committed,
            owned,
            snapshot,
            collector: SnapshotWriter::detached(partitions),
            sb0,
            sb0_seq,
            sb0_copy,
        })
    }

    /// Writes the startup superblock of device 0 (`boot_gen + 1`, CLEAN and SNAPSHOT_VALID
    /// cleared) durably before any data write (E7), and records it in `self`.
    pub(crate) fn write_startup(&mut self, d0: &mut dyn BlockDevice) -> std::io::Result<()> {
        let mut sb = self.sb0.clone();
        sb.apply(&SbUpdate::Startup);
        let seq = self.sb0_seq + 1;
        let copy = self.sb0_copy.other();
        write_superblock(d0, &sb, seq, copy)?;
        self.boot_gen = sb.boot_gen;
        self.sb0 = sb;
        self.sb0_seq = seq;
        self.sb0_copy = copy;
        self.collector = SnapshotWriter::new(self);
        Ok(())
    }

    /// Refuses devices too small for the partition count (see [`check_capacity`]).
    pub fn check_capacity(&self, max_fill: f32) -> std::io::Result<()> {
        check_capacity(&self.layouts, self.partitions, max_fill)
    }

    /// The partition configuration of partition `p` under `cfg`.
    pub(crate) fn partition_cfg(&self, p: u16, cfg: &EngineCfg) -> PartitionCfg {
        let lba = self.devs.first().map_or(4096, |d| d.lba);
        let granularity = match cfg.write_granularity {
            GranularityPolicy::Page => WriteGranularity::Page,
            GranularityPolicy::Lba => WriteGranularity::Lba,
            GranularityPolicy::Auto if lba == 512 => WriteGranularity::Lba,
            GranularityPolicy::Auto => WriteGranularity::Page,
        };
        let page_update = self.devs.first().map_or(PageUpdateMode::TwoImage, |d| d.page_update);
        PartitionCfg {
            id: p,
            partitions: self.partitions,
            databases: self.databases,
            durability: cfg.durability,
            granularity,
            page_update,
            page_slack_cap: cfg.page_slack_cap,
            page_slack_atomic: cfg.page_slack_atomic,
            hash_seed: self.hash_seed,
            boot_gen: self.boot_gen,
            crc: kv_common::RecordCrc::new(self.salt),
            lba,
            produce_groups: false,
            cluster: self.cluster,
            rates: cfg.rates,
        }
    }
}

/// The device's slot pairs of the intent table (§10.7).
pub(crate) fn read_intent_slots(d0: &mut dyn BlockDevice, l0: &DeviceLayout, lba: u32) -> std::io::Result<Vec<SlotPair>> {
    let first = l0.intent_slot_copy_offset(lba, 0, AbCopy::A);
    let last = l0.intent_slot_copy_offset(lba, (INTENT_SLOTS - 1) as u32, AbCopy::B) + u64::from(lba);
    let bytes = read_sync(d0, first, (last - first) as usize)?;
    Ok(crate::metastore::decode_slots(&bytes, first, l0, lba))
}

/// Writes superblock `sb` with `block_seq = seq` into `copy` (the copy that does not hold the
/// newest valid block) and flushes (§10.2: the other copy stays valid).
pub(crate) fn write_superblock(d: &mut dyn BlockDevice, sb: &Superblock, seq: u64, copy: AbCopy) -> std::io::Result<()> {
    let mut buf = AlignedBuf::zeroed(PAGE);
    sb.encode(seq, buf.page_mut(0));
    write_sync(d, SB_PAGE[copy.index()] * PAGE as u64, buf)?;
    flush_sync(d)
}

/// A superblock with every field zero (placeholder of a device-less engine).
pub(crate) fn blank_superblock() -> Superblock {
    Superblock {
        set_uuid: [0; 16],
        device_uuid: [0; 16],
        device_index: 0,
        device_count: 0,
        partitions: 0,
        flags: 0,
        page_size: PAGE as u32,
        ag_pages: crate::layout::AG_PAGES,
        lba: 4096,
        n_ags: 0,
        device_pages: 0,
        ag_table_page: 0,
        ag_table_blocks: 0,
        ag_area_page: 0,
        pmeta_page: 0,
        nmeta_page: 0,
        intent_page: 0,
        intent_slot_page: 0,
        hash_seed: 0,
        record_crc_salt: 0,
        boot_gen: 0,
        clock_floor_unix_ms: 0,
        created_unix_ms: 0,
        snap_seq: 0,
        snap_header_crc: 0,
        snap_region: 0,
        snap_bytes: 0,
        databases: 1,
        snap_region_page: [0, 0],
        snap_region_pages: 0,
        member_uuids: [[0; 16]; 16],
    }
}

// ---------------------------------------------------------------------------------------------
// Synchronous I/O over a BlockDevice (open, shutdown, tools)

/// Tag partition of the engine's own synchronous requests.
pub(crate) const SYNC_TAG_PARTITION: u16 = 0xFFFD;

fn wait_one(d: &mut dyn BlockDevice, id: u64) -> std::io::Result<IoDone> {
    let mut done = Vec::new();
    for _ in 0..1_000_000 {
        d.poll(&mut done, true);
        if let Some(i) = done.iter().position(|x| x.tag.partition == SYNC_TAG_PARTITION && x.tag.id == id) {
            return Ok(done.swap_remove(i));
        }
    }
    Err(Error::new(ErrorKind::TimedOut, "device did not complete a request"))
}

fn sync_tag() -> IoTag {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    IoTag { partition: SYNC_TAG_PARTITION, id: NEXT.fetch_add(1, Ordering::Relaxed) }
}

/// Reads `len` bytes at `offset` (both `lba` aligned).
pub(crate) fn read_sync(d: &mut dyn BlockDevice, offset: u64, len: usize) -> std::io::Result<AlignedBuf> {
    let tag = sync_tag();
    d.submit(IoReq::read(tag, 0, offset, AlignedBuf::zeroed(len)));
    let done = wait_one(d, tag.id)?;
    match done.res {
        Ok(_) => done.buf.ok_or_else(|| Error::other("read completed without its buffer")),
        Err(e) => Err(Error::from_raw_os_error(-e)),
    }
}

/// Writes the whole of `buf` at `offset`.
pub(crate) fn write_sync(d: &mut dyn BlockDevice, offset: u64, buf: AlignedBuf) -> std::io::Result<()> {
    let tag = sync_tag();
    d.submit(IoReq::write(tag, 0, offset, buf));
    match wait_one(d, tag.id)?.res {
        Ok(_) => Ok(()),
        Err(e) => Err(Error::from_raw_os_error(-e)),
    }
}

/// Flushes every device of a set before anything of it is read at open. A crashed process
/// left completed writes in the devices' volatile caches; recovery takes what it reads as the
/// state on media and zeroes the older copies of every key whose newer copy it finds. Read
/// unflushed, a newer copy on one device (say the chunks of a large value) could still be lost
/// to a power cut after recovery had durably zeroed its older copy on another (device 0's
/// startup superblock flush covers device 0 only): the key was lost although a durable version
/// of it existed.
pub(crate) fn flush_all(devs: &mut [&mut dyn BlockDevice]) -> std::io::Result<()> {
    for d in devs.iter_mut() {
        flush_sync(&mut **d)?;
    }
    Ok(())
}

/// Makes every completed write durable (no-op request on write-through devices completes too).
pub(crate) fn flush_sync(d: &mut dyn BlockDevice) -> std::io::Result<()> {
    let tag = sync_tag();
    d.submit(IoReq::flush(tag, 0));
    match wait_one(d, tag.id)?.res {
        Ok(_) => Ok(()),
        Err(e) => Err(Error::from_raw_os_error(-e)),
    }
}

// ---------------------------------------------------------------------------------------------
// PageStore: the engine over device paths

/// The page store engine.
pub struct PageStore {
    cfg: EngineCfg,
    devs: Vec<SyncFileDevice>,
    boot: Arc<Boot>,
    link: Arc<SbLink>,
    closed: bool,
}

/// Opens one data device or file with the substrate rules (LC-34) and the configured modes.
pub(crate) fn open_device(path: &std::path::Path, cfg: &EngineCfg) -> std::io::Result<SyncFileDevice> {
    let probe = devprobe::probe(path).map_err(|r| Error::new(ErrorKind::Unsupported, r))?;
    let bytes = crate::io::device_size(&std::fs::File::open(path)?)?;
    let atomic_ok = probe.atomic_unit.is_some_and(|(min, max)| min as usize <= PAGE && PAGE <= max as usize);
    let page_update = match cfg.page_update {
        PageUpdatePolicy::Auto if atomic_ok => PageUpdateMode::Atomic,
        PageUpdatePolicy::Auto | PageUpdatePolicy::TwoImage => PageUpdateMode::TwoImage,
        PageUpdatePolicy::AtomicOverwrite if atomic_ok => PageUpdateMode::Atomic,
        PageUpdatePolicy::AtomicOverwrite => {
            return Err(Error::new(ErrorKind::Unsupported, format!("{}: atomic 4 KiB writes are not verified", path.display())));
        }
    };
    let write_through = cfg.flush_method == FlushMethod::None
        || match cfg.device_cache {
            DeviceCachePolicy::WriteThrough => true,
            DeviceCachePolicy::WriteBack => false,
            DeviceCachePolicy::Auto => !probe.write_cache,
        };
    let info = DevInfo { pages: bytes / PAGE as u64, lba: probe.lba, write_through, discard: probe.discard, page_update, atomic_unit: probe.atomic_unit };
    SyncFileDevice::open_with(path, info, SyncOpen { direct: true, exclusive: true, read_only: false })
}

impl PageStore {
    /// The engine-wide facts the partitions share.
    pub fn boot(&self) -> &Arc<Boot> {
        &self.boot
    }

    /// The configuration the engine was opened with.
    pub fn cfg(&self) -> &EngineCfg {
        &self.cfg
    }

    /// Handle that partitions hand their clean-shutdown sections to (§10.10).
    pub fn snapshot_writer(&self) -> SnapshotWriter {
        self.boot.snapshot_writer()
    }
}

// the only impl (LC-03)
impl StorageEngine for PageStore {
    type Partition = PartitionStore;

    fn open(cfg: &EngineCfg) -> std::io::Result<(Self, Vec<Self::Partition>)> {
        let mut devs = Vec::with_capacity(cfg.devices.len());
        for path in &cfg.devices {
            devs.push(open_device(path, cfg)?);
        }
        let dsync = cfg.flush_method == FlushMethod::Dsync;
        let mut boot = {
            let mut refs: Vec<&mut dyn BlockDevice> = devs.iter_mut().map(|d| d as &mut dyn BlockDevice).collect();
            flush_all(&mut refs)?;
            Boot::load(&mut refs, dsync)?
        };
        if boot.cluster != cfg.cluster {
            return Err(Error::new(ErrorKind::InvalidInput, "the devices were formatted for the other cluster mode"));
        }
        boot.write_startup(&mut devs[0])?;
        let link = SbLink::new(boot.sb0.clone(), boot.sb0_seq, boot.sb0_copy);
        SbLink::register(&link);
        let boot = Arc::new(boot);
        let parts = (0..boot.partitions).map(|p| PartitionStore::with_boot(boot.partition_cfg(p, cfg), boot.clone())).collect();
        Ok((PageStore { cfg: cfg.clone(), devs, boot, link, closed: false }, parts))
    }

    fn shutdown(&mut self, clean: bool) -> std::io::Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        let mut refs: Vec<&mut dyn BlockDevice> = self.devs.iter_mut().map(|d| d as &mut dyn BlockDevice).collect();
        shutdown_devices(&mut refs, &self.boot, &self.link, clean)
    }
}

/// Writes the clean-shutdown snapshot (when every partition handed in its section and it fits)
/// and the CLEAN superblock (E8); the meta thread writes no superblock afterwards.
fn shutdown_devices(devs: &mut [&mut dyn BlockDevice], boot: &Boot, link: &Arc<SbLink>, clean: bool) -> std::io::Result<()> {
    let mut cell = link.cell.lock();
    if cell.closed {
        return Ok(());
    }
    cell.closed = true;
    let w = boot.snapshot_writer();
    if !clean || !w.complete() || cell.failed {
        // not drained: the next start takes the full scan (CLEAN stays unset)
        return Ok(());
    }
    let mark = w.write_region(devs, &boot.layouts, &cell.sb)?;
    let mut sb = cell.sb.clone();
    sb.apply(&SbUpdate::CleanShutdown { snapshot: mark });
    SbLink::write(&mut cell, &mut DevRefs(devs), sb)
}

/// [`MetaIo`] over borrowed devices that complete requests synchronously (engine open and
/// shutdown).
struct DevRefs<'a, 'b>(&'a mut [&'b mut dyn BlockDevice]);

impl MetaIo for DevRefs<'_, '_> {
    fn read(&mut self, dev: u8, offset: u64, len: usize) -> std::io::Result<AlignedBuf> {
        read_sync(&mut *self.0[usize::from(dev)], offset, len)
    }
    fn write(&mut self, dev: u8, offset: u64, buf: AlignedBuf) -> std::io::Result<()> {
        write_sync(&mut *self.0[usize::from(dev)], offset, buf)
    }
    fn flush(&mut self, dev: u8) -> std::io::Result<()> {
        flush_sync(&mut *self.0[usize::from(dev)])
    }
    fn devices(&self) -> usize {
        self.0.len()
    }
    fn lba(&self, dev: u8) -> u32 {
        self.0[usize::from(dev)].info().lba.max(512)
    }
}

// ---------------------------------------------------------------------------------------------
// LocalEngine: the whole engine driven synchronously in one thread

/// A device plus the completions its synchronous meta requests polled on behalf of others.
struct EngineDev<D: BlockDevice> {
    dev: D,
    stash: Vec<IoDone>,
}

impl<D: BlockDevice> EngineDev<D> {
    fn sync(&mut self, r: IoReq) -> std::io::Result<IoDone> {
        let tag = r.tag;
        self.dev.submit(r);
        let mut got = Vec::new();
        for _ in 0..1_000_000 {
            self.dev.poll(&mut got, true);
            if let Some(i) = got.iter().position(|x| x.tag == tag) {
                let d = got.swap_remove(i);
                self.stash.append(&mut got);
                return Ok(d);
            }
            self.stash.append(&mut got);
        }
        Err(Error::new(ErrorKind::TimedOut, "device did not complete a request"))
    }
}

struct LocalIo<'a, D: BlockDevice>(&'a mut [EngineDev<D>]);

impl<D: BlockDevice> MetaIo for LocalIo<'_, D> {
    fn read(&mut self, dev: u8, offset: u64, len: usize) -> std::io::Result<AlignedBuf> {
        let d = self.0[usize::from(dev)].sync(IoReq::read(sync_tag(), dev, offset, AlignedBuf::zeroed(len)))?;
        match d.res {
            Ok(_) => d.buf.ok_or_else(|| Error::other("read without buffer")),
            Err(e) => Err(Error::from_raw_os_error(-e)),
        }
    }
    fn write(&mut self, dev: u8, offset: u64, buf: AlignedBuf) -> std::io::Result<()> {
        match self.0[usize::from(dev)].sync(IoReq::write(sync_tag(), dev, offset, buf))?.res {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::from_raw_os_error(-e)),
        }
    }
    fn flush(&mut self, dev: u8) -> std::io::Result<()> {
        if self.0[usize::from(dev)].dev.info().write_through {
            return Ok(());
        }
        match self.0[usize::from(dev)].sync(IoReq::flush(sync_tag(), dev))?.res {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::from_raw_os_error(-e)),
        }
    }
    fn devices(&self) -> usize {
        self.0.len()
    }
    fn lba(&self, dev: u8) -> u32 {
        self.0[usize::from(dev)].dev.info().lba.max(512)
    }
}

/// Synchronous plumbing between devices and partitions shared by the driver loop.
struct Plumbing<D: BlockDevice> {
    devs: Vec<EngineDev<D>>,
    flush: Vec<FlushTracker>,
    io: Vec<IoReq>,
}

/// The whole engine in one thread: partitions, page cache, flush trackers and the meta logic
/// over any [`BlockDevice`]s (tools, tests and the deterministic simulator; LC-33: it records
/// nothing about individual writes).
pub struct LocalEngine<D: BlockDevice> {
    plumb: Plumbing<D>,
    boot: Arc<Boot>,
    link: Arc<SbLink>,
    meta: MetaCore,
    client: MetaClient,
    inbox: MetaInbox,
    parts: Vec<PartitionStore>,
    cache: PageCache,
    replies: crate::metastore::MetaReplies,
    /// Fixed clock (simulator); `None` reads the system clock.
    clock: Option<Now>,
    tickets: hashbrown::HashMap<(u16, u64), crate::ticket::TicketState, foldhash::fast::FixedState>,
    closed: bool,
}

impl<D: BlockDevice> LocalEngine<D> {
    /// Opens an engine over formatted devices (index order), writes the startup superblock
    /// (E7) and loads the meta state; partitions are `Loading` until [`LocalEngine::recover`].
    pub fn open(devs: Vec<D>, cfg: &EngineCfg) -> std::io::Result<Self> {
        let dsync = cfg.flush_method == FlushMethod::Dsync;
        let mut devs: Vec<EngineDev<D>> = devs.into_iter().map(|dev| EngineDev { dev, stash: Vec::new() }).collect();
        let mut boot = {
            let mut refs: Vec<&mut dyn BlockDevice> = devs.iter_mut().map(|d| &mut d.dev as &mut dyn BlockDevice).collect();
            flush_all(&mut refs)?;
            Boot::load(&mut refs, dsync)?
        };
        boot.write_startup(&mut devs[0].dev)?;
        let link = SbLink::new(boot.sb0.clone(), boot.sb0_seq, boot.sb0_copy);
        SbLink::register(&link);
        let now = Now::read();
        let mut meta = MetaCore::load(&mut LocalIo(&mut devs), now.unix_ms)?;
        meta.set_max_fill(cfg.max_storage_fill);
        let (client, inbox) = meta.client();
        let flush = devs.iter().enumerate().map(|(i, d)| FlushTracker::new(i as u8, d.dev.info().write_through)).collect();
        let boot = Arc::new(boot);
        let parts = (0..boot.partitions).map(|p| PartitionStore::with_boot(boot.partition_cfg(p, cfg), boot.clone())).collect();
        Ok(LocalEngine {
            plumb: Plumbing { devs, flush, io: Vec::new() },
            boot,
            link,
            meta,
            client,
            inbox,
            parts,
            cache: PageCache::new(cfg.cache_bytes, false),
            replies: Vec::new(),
            clock: None,
            tickets: hashbrown::HashMap::with_hasher(Default::default()),
            closed: false,
        })
    }

    /// Uses a fixed clock (advance it with [`LocalEngine::set_now`]).
    pub fn set_now(&mut self, now: Now) {
        self.clock = Some(now);
    }

    fn now(&self) -> Now {
        self.clock.unwrap_or_else(Now::read)
    }

    pub fn boot(&self) -> &Arc<Boot> {
        &self.boot
    }

    pub fn partitions(&self) -> u16 {
        self.boot.partitions
    }

    /// The partition owning `key` (`slot % P`, §6.2).
    pub fn partition_of(&self, key: &[u8]) -> u16 {
        kv_common::key_slot(key) % self.boot.partitions
    }

    pub fn partition(&self, p: u16) -> &PartitionStore {
        &self.parts[usize::from(p)]
    }

    pub fn meta(&self) -> &MetaCore {
        &self.meta
    }

    /// Device `i` (tests read simulated media through it).
    pub fn device(&self, i: usize) -> &D {
        &self.plumb.devs[i].dev
    }

    pub fn cache(&self) -> &PageCache {
        &self.cache
    }

    /// Where the current version of `(db, key)` lives: the index entry's kind and page, the
    /// record offset when the page is cached, and the extent's size in pages.
    pub fn locate(&self, db: u8, key: &[u8]) -> Option<Located> {
        let p = self.partition_of(key);
        self.parts[usize::from(p)].locate(&self.cache, db, key)
    }

    /// Recovers every partition (§18): the clean snapshot path when the superblock allows it,
    /// falling back to the full scan for every partition when a section fails.
    pub fn recover(&mut self) -> std::io::Result<RecoveryMode> {
        let mut mode = self.boot.recovery_mode();
        'again: loop {
            for p in 0..self.parts.len() {
                for _ in 0..100_000_000u64 {
                    let now = self.now();
                    let progress = {
                        let (part, mut cx) = split(&mut self.parts, &mut self.plumb, &mut self.cache, &self.client, p, now);
                        part.recover(&mut cx, mode)
                    };
                    match progress {
                        RecoveryProgress::Done { .. } => break,
                        RecoveryProgress::SnapshotInvalid if mode == RecoveryMode::Snapshot => {
                            mode = RecoveryMode::FullScan;
                            continue 'again;
                        }
                        RecoveryProgress::SnapshotInvalid => return Err(Error::new(ErrorKind::InvalidData, "recovery failed")),
                        RecoveryProgress::Running => self.pump_io(),
                    }
                }
            }
            break;
        }
        self.pump();
        Ok(mode)
    }

    /// Submits a request to partition `p`.
    pub fn submit(&mut self, p: u16, req: crate::partition::Request) -> crate::partition::ReqId {
        let now = self.now();
        let (part, mut cx) = split(&mut self.parts, &mut self.plumb, &mut self.cache, &self.client, usize::from(p), now);
        part.submit(&mut cx, req)
    }

    /// Ready requests of every partition.
    pub fn ready(&mut self, out: &mut Vec<(u16, crate::partition::ReqId)>) {
        let mut ids = Vec::new();
        for (p, part) in self.parts.iter_mut().enumerate() {
            ids.clear();
            part.take_ready(&mut ids);
            out.extend(ids.iter().map(|&i| (p as u16, i)));
        }
    }

    /// Executes a ready request.
    pub fn exec<R>(
        &mut self,
        p: u16,
        id: crate::partition::ReqId,
        f: &mut dyn FnMut(&mut crate::keyspace::Keyspace<'_>) -> Result<R, crate::keyspace::StoreError>,
    ) -> crate::partition::ExecStatus<R> {
        let now = self.now();
        let (part, mut cx) = split(&mut self.parts, &mut self.plumb, &mut self.cache, &self.client, usize::from(p), now);
        part.exec(&mut cx, id, f)
    }

    /// Runs a SCAN step or RANDOMKEY attempt that is ready.
    pub fn scan(
        &mut self,
        p: u16,
        id: crate::partition::ReqId,
        matcher: &dyn Fn(&[u8]) -> bool,
        out: &mut Vec<Box<[u8]>>,
    ) -> crate::partition::ExecStatus<u64> {
        let now = self.now();
        let (part, mut cx) = split(&mut self.parts, &mut self.plumb, &mut self.cache, &self.client, usize::from(p), now);
        part.scan_keys(&mut cx, id, matcher, out)
    }

    /// One driver iteration: device completions, meta requests, page writer and background
    /// work of every partition, flush submission (§6.4).
    pub fn pump(&mut self) {
        // a fixed clock moves 1 ms per iteration while an AG request waits for the pool
        // (`metastore::ACQUIRE_WAIT_MS`), so that its refusal comes in bounded iterations
        if self.meta.has_held_acquires()
            && let Some(c) = self.clock.as_mut()
        {
            c.mono_ns += 1_000_000;
            c.boot_ns += 1_000_000;
            c.unix_ms += 1;
        }
        self.pump_io();
        let now = self.now();
        for p in 0..self.parts.len() {
            let (part, mut cx) = split(&mut self.parts, &mut self.plumb, &mut self.cache, &self.client, p, now);
            part.tick(&mut cx);
        }
        for f in &mut self.plumb.flush {
            f.poll(&mut self.plumb.io);
        }
        self.submit_io();
        self.collect_tickets();
    }

    fn submit_io(&mut self) {
        for r in self.plumb.io.drain(..) {
            let d = usize::from(r.dev);
            match self.plumb.devs.get_mut(d) {
                Some(dev) => dev.dev.submit(r),
                None => {
                    let tag = r.tag;
                    self.plumb.devs[0].stash.push(IoDone { tag, res: Err(-libc::ENODEV), buf: r.into_buf() });
                }
            }
        }
    }

    /// Device completions and meta replies only.
    fn pump_io(&mut self) {
        self.submit_io();
        let now = self.now();
        let mut done = Vec::new();
        for d in &mut self.plumb.devs {
            done.append(&mut d.stash);
            d.dev.poll(&mut done, false);
        }
        let mut durable = Vec::new();
        for c in done {
            if let Some(dev) = flush_tag_dev(c.tag) {
                let Some(ft) = self.plumb.flush.get_mut(usize::from(dev)) else { continue };
                match c.res {
                    Ok(_) => {
                        durable.clear();
                        ft.on_flush_done_into(&mut durable);
                        for part in &mut self.parts {
                            part.on_durable(&durable);
                        }
                    }
                    Err(e) => {
                        let lost = ft.on_flush_failed();
                        for part in &mut self.parts {
                            part.on_flush_failed(&lost, e);
                        }
                    }
                }
                continue;
            }
            let p = usize::from(c.tag.partition);
            if p < self.parts.len() {
                let (part, mut cx) = split(&mut self.parts, &mut self.plumb, &mut self.cache, &self.client, p, now);
                part.on_io_done(&mut cx, c);
            }
        }
        let mut msgs = Vec::new();
        self.inbox.drain(&mut msgs);
        if !msgs.is_empty() || self.meta.has_deferred_commits() {
            self.meta.process(&mut LocalIo(&mut self.plumb.devs), msgs, now.unix_ms, &mut self.replies);
        }
        let replies = core::mem::take(&mut self.replies);
        for (p, _token, d) in replies {
            let p = usize::from(p);
            if p < self.parts.len() {
                let (part, mut cx) = split(&mut self.parts, &mut self.plumb, &mut self.cache, &self.client, p, now);
                part.on_meta_done(&mut cx, d);
            }
        }
        self.submit_io();
    }

    fn collect_tickets(&mut self) {
        let mut v = Vec::new();
        for (p, part) in self.parts.iter_mut().enumerate() {
            v.clear();
            part.take_tickets(&mut v);
            for &(t, s) in &v {
                self.tickets.insert((p as u16, t.0), s);
            }
        }
    }

    /// [`PartitionStore::abort_txn`] on partition `p` (a cross-partition txn its coordinator
    /// gave up).
    pub fn abort_txn(&mut self, p: u16, txn: u64) -> Option<crate::ticket::TicketId> {
        let now = self.now();
        let (part, mut cx) = split(&mut self.parts, &mut self.plumb, &mut self.cache, &self.client, usize::from(p), now);
        let t = part.abort_txn(&mut cx, txn);
        self.submit_io();
        t
    }

    /// [`PartitionStore::txn_commit_failed`] on partition `p`.
    pub fn txn_commit_failed(&mut self, p: u16, txn: u64, errno: i32) {
        self.parts[usize::from(p)].txn_commit_failed(txn, errno);
    }

    /// The last reported state of ticket `t` of partition `p` (`None` = not reported yet).
    pub fn ticket_state(&self, p: u16, t: crate::ticket::TicketId) -> Option<crate::ticket::TicketState> {
        self.tickets.get(&(p, t.0)).copied()
    }

    /// Pumps until ticket `t` reached `want` (Written or Durable) or failed; returns its state.
    ///
    /// Stops early (returning the state reached) when a partition fails
    /// [`PartitionStore::check_progress`]: the ticket can then never reach `want`.
    pub fn wait_ticket(&mut self, p: u16, t: crate::ticket::TicketId, want: crate::ticket::TicketState) -> crate::ticket::TicketState {
        use crate::ticket::TicketState as S;
        for i in 0..1_000_000u32 {
            match self.ticket_state(p, t) {
                Some(S::Failed(e)) => return S::Failed(e),
                Some(S::Durable) => return S::Durable,
                Some(S::Written) if want == S::Written => return S::Written,
                _ => {}
            }
            if i % 256 == 255 && self.check_progress().is_err() {
                break;
            }
            self.pump();
        }
        self.ticket_state(p, t).unwrap_or(S::Executed)
    }

    /// Submits `req` to `p`, waits until it is ready and executes `f` (re-running it after
    /// `NeedMore`); returns its result and side outputs.
    pub fn run<R>(
        &mut self,
        p: u16,
        req: crate::partition::Request,
        f: &mut dyn FnMut(&mut crate::keyspace::Keyspace<'_>) -> Result<R, crate::keyspace::StoreError>,
    ) -> Result<(R, crate::partition::ExecOut), crate::keyspace::StoreError> {
        use crate::partition::ExecStatus;
        let id = self.submit(p, req);
        let mut ready = Vec::new();
        for _ in 0..1_000_000 {
            ready.clear();
            self.ready(&mut ready);
            for &(rp, rid) in &ready {
                if rp == p && rid == id {
                    match self.exec(p, id, f) {
                        ExecStatus::Done(r, o) => return Ok((r, o)),
                        ExecStatus::Failed(e) => return Err(e),
                        ExecStatus::Requeued => {}
                    }
                }
            }
            self.pump();
        }
        Err(crate::keyspace::StoreError::Io(-libc::ETIMEDOUT))
    }

    /// Like [`LocalEngine::run`] for a SCAN step or RANDOMKEY attempt: the next cursor and
    /// the keys found.
    pub fn run_scan(&mut self, p: u16, req: crate::partition::Request, matcher: &dyn Fn(&[u8]) -> bool) -> Result<ScanResult, crate::keyspace::StoreError> {
        use crate::partition::ExecStatus;
        let id = self.submit(p, req);
        let mut ready = Vec::new();
        let mut out = Vec::new();
        for _ in 0..1_000_000 {
            ready.clear();
            self.ready(&mut ready);
            for &(rp, rid) in &ready {
                if rp == p && rid == id {
                    out.clear();
                    match self.scan(p, id, matcher, &mut out) {
                        ExecStatus::Done(n, _) => return Ok((n, core::mem::take(&mut out))),
                        ExecStatus::Failed(e) => return Err(e),
                        ExecStatus::Requeued => {}
                    }
                }
            }
            self.pump();
        }
        Err(crate::keyspace::StoreError::Io(-libc::ETIMEDOUT))
    }

    /// Pumps until every partition is quiescent (or `limit` iterations passed); true when it
    /// is.
    pub fn settle(&mut self, limit: usize) -> bool {
        for _ in 0..limit {
            // a fixed clock moves 1 ms per idle iteration so that paced work progresses
            if let Some(c) = self.clock.as_mut() {
                c.mono_ns += 1_000_000;
                c.boot_ns += 1_000_000;
            }
            self.pump();
            if self.parts.iter().all(|p| p.is_quiescent() && p.stale_copies() == 0) && self.plumb.devs.iter().all(|d| d.stash.is_empty()) {
                return true;
            }
        }
        false
    }

    /// Structural liveness check of every partition ([`PartitionStore::check_progress`]).
    pub fn check_progress(&self) -> Result<(), String> {
        self.parts.iter().try_for_each(|p| p.check_progress(&self.cache))
    }

    /// Drives the engine with no new input until every partition passes
    /// [`PartitionStore::check_quiescent_complete`]: each iteration completes the submitted
    /// device requests and meta requests, ticks every partition and submits flushes (a fixed
    /// clock moves 1 ms per iteration, so paced work progresses). Every iteration also checks
    /// [`PartitionStore::check_progress`], so a lost wakeup fails at once rather than at the
    /// bound. Returns the iterations taken, or the report of what is still waiting after
    /// `max_iters`.
    pub fn drive_to_quiescence(&mut self, max_iters: usize) -> Result<usize, String> {
        for i in 0..max_iters {
            if let Some(c) = self.clock.as_mut() {
                c.mono_ns += 1_000_000;
                c.boot_ns += 1_000_000;
            }
            self.pump();
            self.check_progress()?;
            if self.plumb.devs.iter().all(|d| d.stash.is_empty()) && self.parts.iter().all(|p| p.check_quiescent_complete(&self.cache).is_ok()) {
                return Ok(i + 1);
            }
        }
        let mut report = format!("not quiescent after {max_iters} iterations");
        for p in &self.parts {
            if let Err(e) = p.check_quiescent_complete(&self.cache) {
                report.push('\n');
                report.push_str(&e);
            }
        }
        Err(report)
    }

    /// Panics with the waiting items and state dumps unless
    /// [`LocalEngine::drive_to_quiescence`] reaches quiescence within `max_iters`.
    pub fn assert_quiescent_complete(&mut self, max_iters: usize) -> usize {
        match self.drive_to_quiescence(max_iters) {
            Ok(n) => n,
            Err(e) => panic!("{e}"),
        }
    }

    /// Clean shutdown: drains, hands every partition's section to the snapshot writer, writes
    /// the snapshot and the CLEAN superblock (§8.2 SHUTDOWN, E8); returns the devices.
    pub fn shutdown(mut self, clean: bool) -> std::io::Result<Vec<D>> {
        if clean {
            self.settle(1_000_000);
            let mut w = self.boot.snapshot_writer();
            let now = self.now();
            for p in 0..self.parts.len() {
                let (part, mut cx) = split(&mut self.parts, &mut self.plumb, &mut self.cache, &self.client, p, now);
                part.snapshot_section(&mut cx, &mut w);
            }
        }
        {
            let mut refs: Vec<&mut dyn BlockDevice> = self.plumb.devs.iter_mut().map(|d| &mut d.dev as &mut dyn BlockDevice).collect();
            shutdown_devices(&mut refs, &self.boot, &self.link, clean)?;
        }
        self.closed = true;
        Ok(self.into_devices())
    }

    /// Stops without any further write (a crash as seen by the engine) and returns the
    /// devices; requests already submitted to them stay there.
    pub fn into_devices(mut self) -> Vec<D> {
        self.link.cell.lock().closed = true;
        // drained partitions must not hand in sections for a snapshot nobody writes
        for p in &mut self.parts {
            p.core.snapshot_done = true;
        }
        let parts = core::mem::take(&mut self.parts);
        drop(parts);
        let plumb = core::mem::replace(&mut self.plumb, Plumbing { devs: Vec::new(), flush: Vec::new(), io: Vec::new() });
        plumb.devs.into_iter().map(|d| d.dev).collect()
    }
}

/// AGs every partition needs at least: one for small records and one for extents (§11.2).
pub const MIN_AGS_PER_PARTITION: u64 = 2;

/// Refuses a device set whose AG area cannot give every partition its first SMALL and EXTENT AG
/// under the fill limit (§11.6): such a node would answer `-OOM` with the devices almost empty.
/// Spare AGs (§11.1) only come out of the headroom above this minimum. Nothing in the engine
/// enforces it (the simulator and format tests use nearly full and tiny devices on purpose); a
/// node calls it, or [`Boot::check_capacity`], to refuse such a device set at startup.
pub fn check_capacity(layouts: &[DeviceLayout], partitions: u16, max_fill: f32) -> std::io::Result<()> {
    let total: u64 = layouts.iter().map(|l| u64::from(l.n_ags)).sum();
    let fill = if max_fill.is_finite() { max_fill.clamp(0.0, 1.0) } else { 0.95 };
    let usable = (f64::from(fill) * total as f64).floor() as u64;
    let need = u64::from(partitions) * MIN_AGS_PER_PARTITION;
    if usable >= need {
        return Ok(());
    }
    Err(Error::new(
        ErrorKind::InvalidInput,
        format!(
            "data devices too small for {partitions} partitions: {total} allocation groups of 16 MiB, {usable} under max-storage-fill {fill}; \
             every partition needs at least {MIN_AGS_PER_PARTITION} (one for small records, one for extents), {need} in all: \
             format with fewer partitions or larger devices"
        ),
    ))
}

/// A SCAN step's next cursor and the keys it found.
pub type ScanResult = (u64, Vec<Box<[u8]>>);

/// Where a key's current version lives (tests and tools).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Located {
    pub kind: kv_index::Kind,
    pub page: crate::layout::PageId,
    /// Offset of the small record in its page (cached pages only).
    pub off: Option<u16>,
    /// Pages of the extent (1 for small records).
    pub n_pages: u32,
}

/// One partition and the context lent to it.
fn split<'a, D: BlockDevice>(
    parts: &'a mut [PartitionStore],
    plumb: &'a mut Plumbing<D>,
    cache: &'a mut PageCache,
    meta: &'a MetaClient,
    p: usize,
    now: Now,
) -> (&'a mut PartitionStore, StorageCtx<'a>) {
    (&mut parts[p], StorageCtx { cache, io: &mut plumb.io, flush: &mut plumb.flush, meta, now })
}

// ---------------------------------------------------------------------------------------------
// In-memory format (simulator and tests)

/// Formats `devs` (any [`BlockDevice`]s, e.g. `SimDevice`s) like `kvd format` formats files
/// (§10.1): superblocks, AG tables, device-0 metadata, fixed snapshot regions of
/// `snap_region_pages` pages each (a multiple of 4096, or 0). `seed` makes the secrets
/// deterministic.
pub fn format_devices(devs: &mut [&mut dyn BlockDevice], partitions: u16, cluster: bool, snap_region_pages: u64, seed: u64) -> std::io::Result<()> {
    use kv_common::KeyHash;
    if devs.is_empty() || devs.len() > crate::layout::MAX_DEVICES || !matches!(partitions, 16 | 32 | 64 | 128) {
        return Err(Error::new(ErrorKind::InvalidInput, "1-16 devices and 16, 32, 64 or 128 partitions"));
    }
    let r = |i: u64| KeyHash::of(seed, &i.to_le_bytes()).raw();
    let uuid = |i: u64| {
        let mut b = [0u8; 16];
        b[..8].copy_from_slice(&r(2 * i).to_le_bytes());
        b[8..].copy_from_slice(&r(2 * i + 1).to_le_bytes());
        b[6] = (b[6] & 0x0F) | 0x40;
        b[8] = (b[8] & 0x3F) | 0x80;
        b
    };
    let set_uuid = uuid(1000);
    let n = devs.len();
    let mut layouts = Vec::with_capacity(n);
    for (i, d) in devs.iter().enumerate() {
        let l = DeviceLayout::compute(d.info().pages, i == 0, snap_region_pages).map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
        layouts.push(l);
    }
    let mut member_uuids = [[0u8; 16]; 16];
    for (i, m) in member_uuids.iter_mut().enumerate().take(n) {
        *m = uuid(i as u64);
    }
    let created = 1_700_000_000_000u64;
    let mut page = AlignedBuf::zeroed(PAGE);
    let copies = [(AbCopy::A, 1u64), (AbCopy::B, 2u64)];
    for (i, (d, l)) in devs.iter_mut().zip(layouts.iter()).enumerate() {
        // metadata area zeroed (intent slots: no valid copy = nothing committed)
        let end = if l.snap_region_pages == 0 { l.ag_area_page } else { l.snap_region_page[0] };
        let zero = AlignedBuf::zeroed((1 << 20).min(end as usize * PAGE));
        let mut off = 0u64;
        while off < end * PAGE as u64 {
            let len = (end * PAGE as u64 - off).min(zero.len() as u64) as usize;
            write_sync(&mut **d, off, AlignedBuf::from_slice(&zero[..len]))?;
            off += len as u64;
        }
        for b in 0..l.ag_table_blocks {
            let blk = AgTableBlock::free(b, l.n_ags);
            for (c, seq) in copies {
                blk.encode(seq, page.page_mut(0));
                write_sync(&mut **d, DeviceLayout::ag_table_copy_page(b, c) * PAGE as u64, page.clone())?;
            }
        }
        if i == 0 {
            for p in 0..partitions {
                let m = PartitionMeta::initial(p);
                for (c, seq) in copies {
                    m.encode(partitions, seq, page.page_mut(0));
                    write_sync(&mut **d, l.pmeta_copy_page(p, c) * PAGE as u64, page.clone())?;
                }
            }
            for (c, seq) in copies {
                NodeMeta::initial().encode(seq, page.page_mut(0));
                write_sync(&mut **d, l.nmeta_copy_page(c) * PAGE as u64, page.clone())?;
                IntentHeader::initial().encode(seq, page.page_mut(0));
                write_sync(&mut **d, l.intent_header_copy_page(c) * PAGE as u64, page.clone())?;
            }
        }
        flush_sync(&mut **d)?;
    }
    for i in (1..n).chain(core::iter::once(0)) {
        let l = &layouts[i];
        let d = &mut *devs[i];
        let mut sb = blank_superblock();
        sb.set_uuid = set_uuid;
        sb.device_uuid = member_uuids[i];
        sb.device_index = i as u16;
        sb.device_count = n as u16;
        sb.partitions = partitions;
        sb.flags = crate::superblock::SB_CLEAN | if cluster { crate::superblock::SB_CLUSTER_MODE } else { 0 };
        sb.lba = d.info().lba;
        sb.hash_seed = r(77);
        sb.record_crc_salt = r(78) as u32;
        sb.clock_floor_unix_ms = created;
        sb.created_unix_ms = created;
        sb.databases = if cluster { 1 } else { 16 };
        if i == 0 {
            sb.member_uuids = member_uuids;
        }
        sb.set_layout(l);
        for (c, seq) in copies {
            sb.encode(seq, page.page_mut(0));
            write_sync(d, SB_PAGE[c.index()] * PAGE as u64, page.clone())?;
        }
        flush_sync(d)?;
    }
    Ok(())
}

/// An in-memory device for the crate's unit tests: requests complete at the next poll.
#[cfg(test)]
pub(crate) mod testdev {
    use crate::io::{BlockDevice, DevInfo, IoDone, IoOp, IoReq};

    pub(crate) struct MemDev {
        pub(crate) data: Vec<u8>,
        lba: u32,
        done: Vec<IoDone>,
    }

    impl MemDev {
        pub(crate) fn new(pages: u64, lba: u32) -> Self {
            MemDev { data: vec![0u8; pages as usize * 4096], lba, done: Vec::new() }
        }
    }

    impl BlockDevice for MemDev {
        fn info(&self) -> DevInfo {
            let mut i = DevInfo::new((self.data.len() / 4096) as u64, self.lba);
            i.write_through = true;
            i
        }
        fn submit(&mut self, mut r: IoReq) {
            let res = r.check(self.lba, self.data.len() as u64).map(|()| {
                let (off, len) = (r.offset as usize, r.len as usize);
                match &mut r.op {
                    IoOp::Read { buf } => buf.as_mut_slice()[..len].copy_from_slice(&self.data[off..off + len]),
                    IoOp::Write { buf, buf_off, .. } => {
                        let b = *buf_off as usize;
                        self.data[off..off + len].copy_from_slice(&buf.as_slice()[b..b + len]);
                    }
                    IoOp::Flush | IoOp::Discard => {}
                }
                r.len
            });
            let tag = r.tag;
            self.done.push(IoDone { tag, res, buf: r.into_buf() });
        }
        fn poll(&mut self, out: &mut Vec<IoDone>, _wait: bool) {
            out.append(&mut self.done);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::effect::Op;
    use crate::partition::{Origin, ReqKind, Request};
    use crate::ticket::TicketState;
    use kv_common::Access;
    use kv_index::VType;

    const T0: u64 = 1_800_000_000_000;

    fn engine() -> LocalEngine<testdev::MemDev> {
        let mut d = testdev::MemDev::new(64 * 4096, 512);
        format_devices(&mut [&mut d as &mut dyn BlockDevice], 16, false, 0, 1).unwrap();
        let mut c = EngineCfg::defaults(Vec::new());
        c.cache_bytes = 8 << 20;
        let mut eng = LocalEngine::open(vec![d], &c).unwrap();
        eng.recover().unwrap();
        for p in eng.parts.iter_mut() {
            p.set_produce_groups(true);
        }
        eng
    }

    fn at(eng: &mut LocalEngine<testdev::MemDev>, ms: u64) {
        eng.set_now(Now { mono_ns: ms * 1_000_000, boot_ns: ms * 1_000_000, unix_ms: ms });
    }

    fn req(k: &[u8]) -> Request {
        Request { db: 0, keys: smallvec::smallvec![(Box::<[u8]>::from(k), Access::ReadWrite)], kind: ReqKind::Keyed, origin: Origin::Client }
    }

    /// A request that changes one key twice (MULTI: SET then APPEND; an expired key appended
    /// to) replicates the key's final state: the group carries one op per key, and the second
    /// change's relative op alone would apply to the member's version from before the request.
    #[test]
    fn a_key_changed_twice_in_one_request_replicates_its_final_state() {
        let mut eng = engine();
        at(&mut eng, T0);
        let k: &[u8] = b"twice";
        let p = eng.partition_of(k);
        let (_, out) = eng
            .run(p, req(k), &mut |ks| {
                ks.put(k, b"a", VType::String, None)?;
                ks.append(k, b"b")?;
                ks.set_expire(k, Some(T0 + 50_000)).map(|_| ())
            })
            .unwrap();
        let g = out.group.expect("group");
        assert_eq!(g.ops.len(), 1);
        assert!(matches!(&g.ops[0], Op::Put { value, expire_at, .. } if value == b"ab" && *expire_at == T0 + 50_000));
        assert_eq!(eng.wait_ticket(p, out.ticket.unwrap(), TicketState::Durable), TicketState::Durable);
        // the key expires; an APPEND starts from nothing on the primary (lazy expiry first)
        at(&mut eng, T0 + 60_000);
        let (_, out) = eng.run(p, req(k), &mut |ks| ks.append(k, b"c")).unwrap();
        let g = out.group.expect("group");
        assert_eq!(g.ops.len(), 1);
        assert!(matches!(&g.ops[0], Op::Put { value, expire_at: 0, .. } if value == b"c"), "an append to an expired key replicates as a put");
        assert_eq!(eng.wait_ticket(p, out.ticket.unwrap(), TicketState::Durable), TicketState::Durable);
        // one change per key: the op is the change itself
        let (_, out) = eng.run(p, req(k), &mut |ks| ks.append(k, b"d")).unwrap();
        assert!(matches!(&out.group.expect("group").ops[0], Op::Append { data, .. } if data == b"d"));
        eng.assert_quiescent_complete(100_000);
    }

    /// WATCH: every committed change of a watched key is reported once, whatever made it (a
    /// write, a delete, a lazy or active expiry, FLUSHDB); unwatched keys and reads are not.
    #[test]
    fn watched_keys_report_every_committed_change() {
        let mut eng = engine();
        at(&mut eng, T0);
        let (k, other): (&[u8], &[u8]) = (b"{w}k", b"{w}other");
        let p = eng.partition_of(k);
        let mut got = Vec::new();
        let mut touched = |eng: &mut LocalEngine<testdev::MemDev>| {
            got.clear();
            eng.parts[usize::from(p)].take_touched(&mut got);
            got.iter().filter(|(d, x)| *d == 0 && **x == *k).count()
        };
        eng.run(p, req(k), &mut |ks| ks.put(k, b"v", VType::String, None)).unwrap();
        assert_eq!(touched(&mut eng), 0, "not watched yet");
        eng.parts[usize::from(p)].watch_key(0, k);
        eng.run(p, req(k), &mut |ks| ks.get(k).map(|_| ())).unwrap();
        eng.run(p, req(other), &mut |ks| ks.put(other, b"x", VType::String, None)).unwrap();
        assert_eq!(touched(&mut eng), 0, "reads and other keys");
        eng.run(p, req(k), &mut |ks| ks.put(k, b"v2", VType::String, None)).unwrap();
        assert_eq!(touched(&mut eng), 1, "write");
        eng.run(p, req(k), &mut |ks| ks.delete(k).map(|_| ())).unwrap();
        assert_eq!(touched(&mut eng), 1, "delete");
        // lazy expiry (a read finds it expired)
        eng.run(p, req(k), &mut |ks| ks.put(k, b"v3", VType::String, Some(T0 + 10))).unwrap();
        assert_eq!(touched(&mut eng), 1);
        at(&mut eng, T0 + 20);
        eng.run(p, req(k), &mut |ks| ks.get(k).map(|_| ())).unwrap();
        assert_eq!(touched(&mut eng), 1, "lazy expiry");
        // active expiry (the sweep removes it without a command)
        eng.run(p, req(k), &mut |ks| ks.put(k, b"v4", VType::String, Some(T0 + 30))).unwrap();
        assert_eq!(touched(&mut eng), 1);
        for i in 0..2_000u64 {
            at(&mut eng, T0 + 40 + i * 5);
            eng.pump();
            if touched(&mut eng) > 0 {
                break;
            }
            assert!(i < 1_999, "active expiry never reported the key");
        }
        // FLUSHDB of its db, then unwatch
        eng.run(p, req(k), &mut |ks| ks.put(k, b"v5", VType::String, None)).unwrap();
        assert_eq!(touched(&mut eng), 1);
        let flush = Request { db: 0, keys: smallvec::SmallVec::new(), kind: ReqKind::FlushDb { db: Some(0) }, origin: Origin::Client };
        eng.run(p, flush, &mut |_| Ok(())).unwrap();
        assert_eq!(touched(&mut eng), 1, "flushdb");
        eng.parts[usize::from(p)].unwatch_key(0, k);
        eng.run(p, req(k), &mut |ks| ks.put(k, b"v6", VType::String, None)).unwrap();
        assert_eq!(touched(&mut eng), 0, "unwatched");
        eng.assert_quiescent_complete(100_000);
    }
}
