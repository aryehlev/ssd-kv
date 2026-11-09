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
//! from logical page 1, each starting on a page boundary. Section: `"SNP1"`, partition,
//! databases, next_seq, hint_base_s, tables (`{db, flags, slot, n, n x 10 B entries in fp order
//! (sync bit 0), [n x u16 hints]}`), SMALL AGs with their `page_free`, EXTENT AGs with their
//! `alloc`/`start` bitmaps, crc32c. An AG is named by `dev << 24 | index` (a device holds fewer
//! than 2^20 AGs). This module is the only caller of `SlotTable::snapshot_iter` (LC-18).
//!
//! Partitions hand their encoded sections to a shared [`SnapshotWriter`] when they are drained
//! (explicitly through `PartitionStore::snapshot_section`, or when a drained partition is
//! dropped); the engine's shutdown writes the region, the header, flushes, and only then the
//! superblock naming it.

use std::sync::Arc;

use kv_common::abblock::AbCopy;
use kv_common::crc::crc32c;
use kv_index::{Entry, SlotTable};
use parking_lot::Mutex;

use crate::alloc::Bits4096;
use crate::engine::Boot;
use crate::io::{AlignedBuf, BlockDevice};
use crate::layout::{AG_PAGES, AgId, DeviceLayout, PAGE, Seq, ab_build, ab_parse, le};
use crate::partition::PartitionStore;
use crate::superblock::{SnapshotMark, Superblock};

/// `KVSNAPH1`
pub const SNAP_HEADER_MAGIC: u64 = kv_common::abblock::magic(b"KVSNAPH1");
/// `"SNP1"`
pub const SNAP_SECTION_MAGIC: u32 = u32::from_le_bytes(*b"SNP1");

/// Offset of the section directory in the header block.
const DIR_OFF: usize = 0x040;
/// Bytes per directory entry.
const DIR_ENTRY: usize = 24;
/// Section table flag: hints follow the entries.
const TABLE_HINTS: u8 = 1;

/// Encodes `dev`/`idx` as the section's AG number.
fn ag_code(ag: AgId) -> u32 {
    (u32::from(ag.dev) << 24) | (ag.idx & 0x00FF_FFFF)
}

fn ag_decode(v: u32) -> AgId {
    AgId { dev: (v >> 24) as u8, idx: v & 0x00FF_FFFF }
}

// ---------------------------------------------------------------------------------------------
// Sections

/// Encodes partition `p`'s section (index tables, allocator state, next seq).
pub(crate) fn encode_section(p: &PartitionStore) -> Vec<u8> {
    let core = &p.core;
    let mut out = Vec::with_capacity(4096);
    out.extend_from_slice(&SNAP_SECTION_MAGIC.to_le_bytes());
    out.extend_from_slice(&core.cfg.id.to_le_bytes());
    out.extend_from_slice(&u16::from(core.cfg.databases).to_le_bytes());
    out.extend_from_slice(&core.next_seq.0.to_le_bytes());
    out.extend_from_slice(&core.expiry.hint_base_s.to_le_bytes());
    let n_tables_at = out.len();
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    let mut n_tables = 0u32;
    let local = core.index.local_slots();
    let mut raw = [0u8; Entry::BYTES];
    for db in 0..core.cfg.databases {
        for sl in 0..local {
            let slot = core.index.slot_of(sl);
            let Some(t) = core.index.table(db, slot) else { continue };
            let hints = t.ttl_len() > 0;
            out.push(db);
            out.push(if hints { TABLE_HINTS } else { 0 });
            out.extend_from_slice(&slot.to_le_bytes());
            out.extend_from_slice(&(t.len() as u32).to_le_bytes());
            let mut hs: Vec<u16> = Vec::new();
            for (e, h) in SlotTable::snapshot_iter(t) {
                e.encode(&mut raw);
                out.extend_from_slice(&raw);
                if hints {
                    hs.push(h);
                }
            }
            for h in hs {
                out.extend_from_slice(&h.to_le_bytes());
            }
            n_tables += 1;
        }
    }
    out[n_tables_at..n_tables_at + 4].copy_from_slice(&n_tables.to_le_bytes());
    let smalls: Vec<(AgId, &[u8])> = core.alloc.small_ags().collect();
    out.extend_from_slice(&(smalls.len() as u32).to_le_bytes());
    for (ag, pf) in smalls {
        out.extend_from_slice(&ag_code(ag).to_le_bytes());
        out.extend_from_slice(pf);
    }
    let exts: Vec<(AgId, &Bits4096, &Bits4096)> = core.alloc.extent_ags().collect();
    out.extend_from_slice(&(exts.len() as u32).to_le_bytes());
    for (ag, a, s) in exts {
        out.extend_from_slice(&ag_code(ag).to_le_bytes());
        a.to_bytes(&mut out);
        s.to_bytes(&mut out);
    }
    let crc = crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// One table of a section: `(db, slot, entries with hints in fp order)`.
pub(crate) type SectionTable = (u8, u16, Vec<(Entry, u16)>);

/// A decoded section.
pub(crate) struct Section {
    pub(crate) partition: u16,
    pub(crate) next_seq: Seq,
    pub(crate) hint_base_s: u64,
    /// `(db, slot, entries with hints in fp order)`.
    pub(crate) tables: Vec<SectionTable>,
    pub(crate) small: Vec<(AgId, Vec<u8>)>,
    pub(crate) extent: Vec<(AgId, Bits4096, Bits4096)>,
}

/// Decodes and verifies a section (`None` on any inconsistency, §18.2: the node then takes the
/// full scan).
pub(crate) fn decode_section(b: &[u8], partition: u16, databases: u8) -> Option<Section> {
    if b.len() < 32 + 4 {
        return None;
    }
    let body = &b[..b.len() - 4];
    if crc32c(body) != le::u32_at(b, b.len() - 4) || le::u32_at(b, 0) != SNAP_SECTION_MAGIC {
        return None;
    }
    if le::u16_at(b, 4) != partition || le::u16_at(b, 6) != u16::from(databases) || le::u32_at(b, 28) != 0 {
        return None;
    }
    let next_seq = Seq(le::u64_at(b, 8));
    let hint_base_s = le::u64_at(b, 16);
    let n_tables = le::u32_at(b, 24) as usize;
    let mut at = 32usize;
    let need = |at: usize, n: usize| (at.checked_add(n)? <= body.len()).then_some(());
    let mut tables = Vec::with_capacity(n_tables);
    for _ in 0..n_tables {
        need(at, 8)?;
        let db = body[at];
        let flags = body[at + 1];
        let slot = le::u16_at(body, at + 2);
        let n = le::u32_at(body, at + 4) as usize;
        at += 8;
        if db >= databases || flags & !TABLE_HINTS != 0 || usize::from(slot) >= kv_common::slot::SLOTS {
            return None;
        }
        need(at, n.checked_mul(Entry::BYTES)?)?;
        let mut v = Vec::with_capacity(n);
        let mut prev = 0u32;
        for i in 0..n {
            let raw: [u8; Entry::BYTES] = body[at + i * Entry::BYTES..at + (i + 1) * Entry::BYTES].try_into().ok()?;
            let e = Entry::try_decode(&raw)?;
            if e.is_empty() || e.sync_mark || e.fp32 < prev {
                return None;
            }
            prev = e.fp32;
            v.push((e, 0u16));
        }
        at += n * Entry::BYTES;
        if flags & TABLE_HINTS != 0 {
            need(at, n * 2)?;
            for (i, x) in v.iter_mut().enumerate() {
                x.1 = le::u16_at(body, at + 2 * i);
            }
            at += n * 2;
        }
        tables.push((db, slot, v));
    }
    need(at, 4)?;
    let n_small = le::u32_at(body, at) as usize;
    at += 4;
    let mut small = Vec::with_capacity(n_small);
    for _ in 0..n_small {
        need(at, 4 + AG_PAGES as usize)?;
        let ag = ag_decode(le::u32_at(body, at));
        small.push((ag, body[at + 4..at + 4 + AG_PAGES as usize].to_vec()));
        at += 4 + AG_PAGES as usize;
    }
    need(at, 4)?;
    let n_ext = le::u32_at(body, at) as usize;
    at += 4;
    let mut extent = Vec::with_capacity(n_ext);
    for _ in 0..n_ext {
        need(at, 4 + 1024)?;
        let ag = ag_decode(le::u32_at(body, at));
        let a = Bits4096::from_bytes(&body[at + 4..at + 516])?;
        let s = Bits4096::from_bytes(&body[at + 516..at + 1028])?;
        extent.push((ag, a, s));
        at += 4 + 1024;
    }
    (at == body.len()).then_some(Section { partition, next_seq, hint_base_s, tables, small, extent })
}

/// Hands partition `p`'s section to `w` (once).
pub(crate) fn deposit(p: &mut PartitionStore, w: &mut SnapshotWriter) {
    if p.core.snapshot_done {
        return;
    }
    p.core.snapshot_done = true;
    let id = p.core.cfg.id;
    let bytes = encode_section(p);
    w.put(id, bytes);
}

// ---------------------------------------------------------------------------------------------
// The writer (sections collected from every partition, written by the engine at shutdown)

struct Collected {
    sections: Vec<Option<Vec<u8>>>,
}

/// Collects the sections of a clean-shutdown snapshot (cheap to clone; shared by every
/// partition of an engine).
#[derive(Clone)]
pub struct SnapshotWriter {
    inner: Arc<Mutex<Collected>>,
    /// Target region (the one the superblock does not name).
    region: AbCopy,
    /// Capacity of one region over all devices (0 = no regions).
    region_bytes: u64,
}

impl SnapshotWriter {
    /// A writer for an engine described by `boot`: it targets the region the superblock does
    /// not name (`1 - snap_region`, §10.10).
    pub(crate) fn new(boot: &Boot) -> Self {
        let region_bytes = boot.layouts.iter().map(|l| l.snap_region_pages * PAGE as u64).sum();
        let region = if boot.sb0.snap_region == 0 { AbCopy::B } else { AbCopy::A };
        SnapshotWriter { inner: Arc::new(Mutex::new(Collected { sections: vec![None; usize::from(boot.partitions)] })), region, region_bytes }
    }

    /// A writer that collects nothing useful (device-less engines).
    pub(crate) fn detached(partitions: u16) -> Self {
        SnapshotWriter { inner: Arc::new(Mutex::new(Collected { sections: vec![None; usize::from(partitions)] })), region: AbCopy::A, region_bytes: 0 }
    }

    fn put(&mut self, partition: u16, bytes: Vec<u8>) {
        let mut c = self.inner.lock();
        if let Some(s) = c.sections.get_mut(usize::from(partition)) {
            *s = Some(bytes);
        }
    }

    /// True when every partition handed in its section.
    pub fn complete(&self) -> bool {
        self.inner.lock().sections.iter().all(Option::is_some)
    }

    /// Bytes the snapshot needs (header page plus page-aligned sections).
    pub fn encoded_bytes(&self) -> u64 {
        let c = self.inner.lock();
        PAGE as u64 + c.sections.iter().flatten().map(|s| (s.len() as u64).next_multiple_of(PAGE as u64)).sum::<u64>()
    }

    /// The encoded snapshot did not fit its region.
    pub fn too_large(&self) -> bool {
        self.region_bytes == 0 || self.encoded_bytes() > self.region_bytes
    }

    /// Writes the snapshot into its region (sections, then the header), flushes every
    /// device, and returns the superblock fields naming it (E8: the caller writes the
    /// superblock only after this returns). `None` when it is incomplete or too large.
    pub(crate) fn write_region(&self, devs: &mut [&mut dyn BlockDevice], layouts: &[DeviceLayout], sb0: &Superblock) -> std::io::Result<Option<SnapshotMark>> {
        if !self.complete() || self.too_large() {
            return Ok(None);
        }
        let c = self.inner.lock();
        let partitions = c.sections.len();
        let region = self.region;
        let mut dir = Vec::with_capacity(partitions);
        let mut off = PAGE as u64;
        for s in c.sections.iter().flatten() {
            let len = s.len() as u64;
            dir.push((off, len, crc32c(s)));
            let mut buf = AlignedBuf::zeroed(len.next_multiple_of(PAGE as u64) as usize);
            buf.as_mut_slice()[..s.len()].copy_from_slice(s);
            write_logical(devs, layouts, region, off, &buf)?;
            off += buf.len() as u64;
        }
        let total = off;
        let snap_seq = sb0.snap_seq + 1;
        let mut hdr = AlignedBuf::zeroed(PAGE);
        encode_header(snap_seq, sb0.boot_gen, region, total, &dir, hdr.page_mut(0));
        let header_crc = crc32c(hdr.as_slice());
        write_logical(devs, layouts, region, 0, &hdr)?;
        for d in devs.iter_mut() {
            crate::engine::flush_sync(&mut **d)?;
        }
        Ok(Some(SnapshotMark { snap_seq, snap_header_crc: header_crc, snap_region: region.index() as u8, snap_bytes: total }))
    }
}

/// Encodes the snapshot header block (§10.10): `dir[p] = (offset, len, crc32c)` of partition
/// `p`'s section.
pub(crate) fn encode_header(snap_seq: u64, boot_gen: u32, region: AbCopy, total: u64, dir: &[(u64, u64, u32)], out: &mut [u8; PAGE]) {
    ab_build(SNAP_HEADER_MAGIC, snap_seq, out, |b| {
        le::put_u64(b, 0x018, snap_seq);
        le::put_u32(b, 0x020, boot_gen);
        le::put_u16(b, 0x024, dir.len() as u16);
        b[0x026] = region.index() as u8;
        le::put_u64(b, 0x028, total);
        for (i, &(o, l, crc)) in dir.iter().enumerate() {
            let at = DIR_OFF + DIR_ENTRY * i;
            le::put_u64(b, at, o);
            le::put_u64(b, at + 8, l);
            le::put_u32(b, at + 16, crc);
        }
    });
}

/// Device spans of logical bytes `[off, off + len)` of region `region`: `(dev, device byte
/// offset, span length, offset in the logical range)`.
pub(crate) fn region_spans(layouts: &[DeviceLayout], region: AbCopy, off: u64, len: u64) -> Vec<(u8, u64, u64, u64)> {
    let mut out = Vec::new();
    let mut base = 0u64;
    let end = off + len;
    for (d, l) in layouts.iter().enumerate() {
        let size = l.snap_region_pages * PAGE as u64;
        let (s, e) = (base, base + size);
        if size > 0 && end > s && off < e {
            let a = off.max(s);
            let b = end.min(e);
            out.push((d as u8, l.snap_region_page[region.index()] * PAGE as u64 + (a - s), b - a, a - off));
        }
        base = e;
    }
    out
}

fn write_logical(devs: &mut [&mut dyn BlockDevice], layouts: &[DeviceLayout], region: AbCopy, off: u64, buf: &AlignedBuf) -> std::io::Result<()> {
    const CHUNK: u64 = 1 << 20;
    for (d, dev_off, len, at) in region_spans(layouts, region, off, buf.len() as u64) {
        let mut done = 0u64;
        while done < len {
            let n = (len - done).min(CHUNK);
            let s = (at + done) as usize;
            let part = AlignedBuf::from_slice(&buf.as_slice()[s..s + n as usize]);
            crate::engine::write_sync(&mut *devs[usize::from(d)], dev_off + done, part)?;
            done += n;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// The directory of the snapshot a clean start restores

/// The snapshot named by the superblock: region and per-partition `(offset, len, crc32c)`.
#[derive(Clone, Debug)]
pub struct SnapshotDir {
    pub(crate) region: AbCopy,
    pub(crate) sections: Vec<(u64, u64, u32)>,
}

impl SnapshotDir {
    /// Reads and verifies the header the superblock names (CLEAN and SNAPSHOT_VALID, header crc,
    /// partitions); `None` when there is no usable snapshot (§18.1 step 3).
    pub(crate) fn read(devs: &mut [&mut dyn BlockDevice], layouts: &[DeviceLayout], sb0: &Superblock) -> std::io::Result<Option<SnapshotDir>> {
        if !sb0.is_clean() || !sb0.snapshot_valid() {
            return Ok(None);
        }
        let region = if sb0.snap_region == 0 { AbCopy::A } else { AbCopy::B };
        let spans = region_spans(layouts, region, 0, PAGE as u64);
        let Some(&(d, dev_off, len, _)) = spans.first() else { return Ok(None) };
        if len != PAGE as u64 {
            return Ok(None);
        }
        let hdr = crate::engine::read_sync(&mut *devs[usize::from(d)], dev_off, PAGE)?;
        if crc32c(hdr.as_slice()) != sb0.snap_header_crc {
            return Ok(None);
        }
        let parsed = ab_parse(SNAP_HEADER_MAGIC, hdr.as_slice(), |b| {
            let seq = le::u64_at(b, 0x018);
            let parts = le::u16_at(b, 0x024);
            if seq != sb0.snap_seq || parts != sb0.partitions || b[0x026] as usize != region.index() || le::u64_at(b, 0x028) != sb0.snap_bytes {
                return None;
            }
            let mut sections = Vec::with_capacity(usize::from(parts));
            for i in 0..usize::from(parts) {
                let at = DIR_OFF + DIR_ENTRY * i;
                sections.push((le::u64_at(b, at), le::u64_at(b, at + 8), le::u32_at(b, at + 16)));
            }
            Some(sections)
        });
        Ok(parsed.map(|(_, sections)| SnapshotDir { region, sections }))
    }

    /// The `(offset, len, crc)` of partition `p`'s section.
    pub(crate) fn section(&self, p: u16) -> Option<(u64, u64, u32)> {
        self.sections.get(usize::from(p)).copied()
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    use crate::partition::{BgRates, Durability, PartitionCfg, PartitionStore, WriteGranularity};
    use kv_common::KeyHash;
    use kv_index::{Kind, VType};

    fn part() -> PartitionStore {
        PartitionStore::new(PartitionCfg {
            id: 3,
            partitions: 16,
            databases: 16,
            durability: Durability::Written,
            granularity: WriteGranularity::Lba,
            page_update: crate::io::PageUpdateMode::TwoImage,
            page_slack_cap: 1024,
            page_slack_atomic: 128,
            hash_seed: 9,
            boot_gen: 1,
            crc: kv_common::RecordCrc::new(1),
            lba: 512,
            produce_groups: false,
            cluster: false,
            rates: BgRates::defaults(16),
        })
    }

    /// Keys of partition 3 (of 16).
    fn keys(n: usize) -> Vec<Vec<u8>> {
        (0..).map(|i: u64| format!("k{i}").into_bytes()).filter(|k| kv_common::key_slot(k) % 16 == 3).take(n).collect()
    }

    #[test]
    fn section_round_trip_and_corruption() {
        let mut p = part();
        let ks = keys(300);
        for (i, k) in ks.iter().enumerate() {
            let kh = KeyHash::of(9, k);
            let slot = kv_common::key_slot(k);
            let db = (i % 3) as u8;
            let e = Entry { fp32: kh.fp32(), x8: kh.x8(), page36: 0x1000 + i as u64, kind: Kind::Small, vtype: VType::String, sync_mark: i % 5 == 0 };
            let hint = if i % 4 == 0 { 7 } else { 0 };
            p.core.index.table_mut(db, slot).insert(kh, e, hint);
        }
        p.core.next_seq = Seq::new(2, 77);
        let bytes = encode_section(&p);
        let sec = decode_section(&bytes, 3, 16).expect("valid section");
        assert_eq!(sec.next_seq, Seq::new(2, 77));
        let n: usize = sec.tables.iter().map(|t| t.2.len()).sum();
        assert_eq!(n, 300);
        for (db, slot, entries) in &sec.tables {
            let t = p.core.index.table(*db, *slot).unwrap();
            let want: Vec<(Entry, u16)> = SlotTable::snapshot_iter(t).collect();
            assert_eq!(&want, entries, "entries in fp order with hints, sync marks cleared");
            assert!(entries.iter().all(|(e, _)| !e.sync_mark));
        }
        // any flipped byte, a wrong partition or a truncation is refused
        for i in [0usize, 5, 40, bytes.len() / 2, bytes.len() - 1] {
            let mut b = bytes.clone();
            b[i] ^= 0x10;
            assert!(decode_section(&b, 3, 16).is_none(), "flip at {i} accepted");
        }
        assert!(decode_section(&bytes, 4, 16).is_none());
        assert!(decode_section(&bytes[..bytes.len() - 8], 3, 16).is_none());
    }

    #[test]
    fn header_golden_vector() {
        let dir = [(4096u64, 100u64, 0xDEAD_BEEFu32), (8192, 4096, 0x0102_0304)];
        let mut b = [0u8; PAGE];
        encode_header(5, 9, AbCopy::B, 12_288, &dir, &mut b);
        // AbBlock framing
        assert_eq!(&b[..8], b"KVSNAPH1");
        assert_eq!(le::u32_at(&b, 0x0C), 1, "AbBlock version");
        assert_eq!(le::u64_at(&b, 0x10), 5, "block_seq = snap_seq");
        // fields of §10.10 at their offsets
        assert_eq!(le::u64_at(&b, 0x18), 5);
        assert_eq!(le::u32_at(&b, 0x20), 9);
        assert_eq!(le::u16_at(&b, 0x24), 2);
        assert_eq!(b[0x26], 1);
        assert_eq!(b[0x27], 0);
        assert_eq!(le::u64_at(&b, 0x28), 12_288);
        assert!(b[0x30..0x40].iter().all(|&x| x == 0));
        assert_eq!(le::u64_at(&b, 0x40), 4096);
        assert_eq!(le::u64_at(&b, 0x48), 100);
        assert_eq!(le::u32_at(&b, 0x50), 0xDEAD_BEEF);
        assert_eq!(le::u32_at(&b, 0x54), 0);
        assert_eq!(le::u64_at(&b, 0x58), 8192);
        assert_eq!(le::u64_at(&b, 0x60), 4096);
        assert_eq!(le::u32_at(&b, 0x68), 0x0102_0304);
        assert!(b[0x70..].iter().all(|&x| x == 0));
        // the AbBlock crc covers [0x0C, 0x1000)
        assert_eq!(le::u32_at(&b, 0x08), crc32c(&b[0x0C..]));
        assert!(kv_common::abblock::ab_decode(SNAP_HEADER_MAGIC, &b).is_some());
    }

    #[test]
    fn ag_codes_round_trip() {
        for ag in [AgId { dev: 0, idx: 0 }, AgId { dev: 15, idx: (1 << 20) - 1 }, AgId { dev: 3, idx: 77 }] {
            assert_eq!(ag_decode(ag_code(ag)), ag);
        }
    }

    #[test]
    fn region_spans_concatenate_devices() {
        let mut l0 = DeviceLayout::compute(16 * 4096, true, 4096).unwrap();
        let l1 = DeviceLayout::compute(16 * 4096, false, 4096).unwrap();
        let r = l0.snap_region_pages * PAGE as u64;
        let spans = region_spans(&[l0, l1], AbCopy::B, r - 4096, 8192);
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0], (0, l0.snap_region_page[1] * PAGE as u64 + r - 4096, 4096, 0));
        assert_eq!(spans[1], (1, l1.snap_region_page[1] * PAGE as u64, 4096, 4096));
        l0.snap_region_pages = 0;
        assert!(region_spans(&[l0], AbCopy::A, 0, 10).is_empty());
    }
}
