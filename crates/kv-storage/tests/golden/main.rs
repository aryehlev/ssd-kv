//! Golden byte vectors of the on-SSD formats (DESIGN §10, §25.1).
//!
//! Every format of §10 is encoded from a fixed sample through the public API and compared with
//! the vector checked in next to this file; each vector is also decoded back and compared with
//! the sample. A change to any byte of any format fails here.
//!
//! Vector files: `# comment` lines, `len N`, then `OFFSET: HEX` lines of 32 bytes; lines whose
//! 32 bytes are all zero are omitted. Regenerate after an intended format change with
//! `KVSSD_BLESS=1 cargo test -p kv-storage --test golden` and review the diff.

use std::path::PathBuf;

use kv_common::RecordCrc;
use kv_common::abblock::AbCopy;
use kv_storage::agtable::{AgKind, AgTableBlock};
use kv_storage::extent::{
    ChunkRef, EXT_FLAG_HAS_EXPIRE, EXT_FLAG_TXN, EXT_KIND_CHUNK, EXT_KIND_CHUNKMAP, EXT_KIND_VALUE, EXT_VTYPE_HASH,
    EXT_VTYPE_STRING, ExtHeader, chunk_payload, chunkmap_parts, decode_chunkmap, encode_chunk_refs, encode_extent,
    parse_extent_header, stored_crc, value_parts, verify_extent,
};
use kv_storage::intent::{INTENT_SLOT_COPY_BYTES, IntentHeader, IntentSlot};
use kv_storage::layout::{DeviceLayout, PAGE, PageId, Seq};
use kv_storage::page::{PageParse, parse_small_page, write_small_header};
use kv_storage::pmeta::{NodeMeta, PMETA_SYNCING, PartitionMeta, ROLE_PRIMARY};
use kv_storage::record::{
    REC_FLAG_HAS_EXPIRE, REC_FLAG_TXN, REC_KIND_HASH, REC_KIND_STRING, REC_KIND_TXN_DELETE_MARKER, RecHeader, encode_small,
    small_len,
};
use kv_storage::superblock::{SB_CLEAN, SB_SNAPSHOT_VALID, Superblock};

const SALT: u32 = 0x5A17_C0DE;

fn dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests").join("golden")
}

fn to_hex(title: &str, bytes: &[u8]) -> String {
    let mut s = format!("# kvssd golden vector: {title}\nlen {}\n", bytes.len());
    for (i, row) in bytes.chunks(32).enumerate() {
        if row.iter().all(|&b| b == 0) {
            continue;
        }
        s.push_str(&format!("{:04x}:", i * 32));
        for b in row {
            s.push_str(&format!(" {b:02x}"));
        }
        s.push('\n');
    }
    s
}

fn from_hex(text: &str) -> Vec<u8> {
    let mut out = Vec::new();
    for line in text.lines().map(str::trim).filter(|l| !l.is_empty() && !l.starts_with('#')) {
        if let Some(n) = line.strip_prefix("len ") {
            out = vec![0u8; n.trim().parse().expect("vector length")];
            continue;
        }
        let (off, hex) = line.split_once(':').expect("OFFSET: HEX line");
        let at = usize::from_str_radix(off.trim(), 16).expect("hex offset");
        for (i, b) in hex.split_whitespace().enumerate() {
            out[at + i] = u8::from_str_radix(b, 16).expect("hex byte");
        }
    }
    out
}

/// Compares `bytes` with vector `name` (or writes it in bless mode) and returns the vector.
fn golden(name: &str, title: &str, bytes: &[u8]) -> Vec<u8> {
    let path = dir().join(format!("{name}.hex"));
    if std::env::var_os("KVSSD_BLESS").is_some() {
        std::fs::write(&path, to_hex(title, bytes)).expect("write vector");
        return bytes.to_vec();
    }
    let text = std::fs::read_to_string(&path).unwrap_or_else(|_| panic!("missing vector {name}.hex (bless with KVSSD_BLESS=1)"));
    let want = from_hex(&text);
    assert_eq!(want.len(), bytes.len(), "{name}: length");
    if let Some(i) = (0..want.len()).find(|&i| want[i] != bytes[i]) {
        panic!("{name}: first difference at byte {i:#x}: vector {:#04x}, encoded {:#04x}", want[i], bytes[i]);
    }
    want
}

fn page_id(dev: u64, page: u64) -> PageId {
    PageId((dev << 32) | page)
}

fn superblock(dev: u16) -> Superblock {
    let l = DeviceLayout::compute(1 << 18, dev == 0, 4096).expect("layout");
    let mut sb = Superblock {
        set_uuid: [0x11; 16],
        device_uuid: [0x20 + dev as u8; 16],
        device_index: dev,
        device_count: 2,
        partitions: 64,
        flags: SB_CLEAN | SB_SNAPSHOT_VALID,
        page_size: 4096,
        ag_pages: 4096,
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
        hash_seed: 0x0011_2233_4455_6677,
        record_crc_salt: SALT,
        boot_gen: 41,
        clock_floor_unix_ms: 1_793_520_000_000,
        created_unix_ms: 1_793_000_000_000,
        snap_seq: 0x0000_0001_0000_0abc,
        snap_header_crc: 0xC0FF_EE00,
        snap_region: 1,
        snap_bytes: 123_456_789,
        databases: 16,
        snap_region_page: [0, 0],
        snap_region_pages: 0,
        member_uuids: [[0; 16]; 16],
    };
    sb.set_layout(&l);
    if dev == 0 {
        sb.member_uuids[0] = [0x20; 16];
        sb.member_uuids[1] = [0x21; 16];
    }
    sb
}

#[test]
fn superblocks() {
    for dev in [0u16, 1] {
        let sb = superblock(dev);
        sb.validate().expect("valid sample");
        let mut b = [0u8; PAGE];
        sb.encode(1000 + u64::from(dev), &mut b);
        let v = golden(&format!("superblock_dev{dev}"), &format!("superblock of device {dev} (DESIGN §10.3)"), &b);
        let (seq, d) = Superblock::decode(&v).expect("decodes");
        assert_eq!(seq, 1000 + u64::from(dev));
        assert!(d == sb);
    }
}

#[test]
fn ag_table() {
    let mut t = AgTableBlock::free(0, 70);
    t.entries[0] = (AgKind::Small, 0);
    t.entries[1] = (AgKind::Extent, 0);
    t.entries[2] = (AgKind::Small, 63);
    t.entries[69] = (AgKind::Extent, 17);
    let mut b = [0u8; PAGE];
    t.encode(7, &mut b);
    let v = golden("agtable", "AG table block (DESIGN §10.4)", &b);
    assert_eq!(AgTableBlock::decode(&v), Some((7, t)));
}

#[test]
fn partition_and_node_meta() {
    let mut m = PartitionMeta::initial(37);
    m.flags = PMETA_SYNCING;
    m.seq_floor = Seq::new(4, 5 << 24);
    m.term = 4;
    m.flush_cutoff[1] = Seq::new(3, 77).0;
    m.flush_cutoff[15] = Seq::new(4, 1).0;
    m.repl_durable = Seq::new(4, 1000);
    m.repl_commit = Seq::new(4, 990);
    m.history = [(4, 0), (3, Seq::new(3, 500).0), (1, Seq::new(1, 20).0), (0, 0)];
    let mut b = [0u8; PAGE];
    m.encode(64, 3, &mut b);
    let v = golden("pmeta", "partition meta (DESIGN §10.5)", &b);
    assert_eq!(PartitionMeta::decode_with_partitions(&v), Some((3, 64, m)));

    let n = NodeMeta {
        cluster_id: *b"cluster-0000-abc",
        node_id: *b"node-00000000000042a",
        shard: 3,
        role: ROLE_PRIMARY,
        fence_term: 9,
        assignment_version: 1234,
    };
    n.encode(2, &mut b);
    let v = golden("nmeta", "node meta (DESIGN §10.6)", &b);
    assert_eq!(NodeMeta::decode(&v), Some((2, n)));
}

#[test]
fn intent_table() {
    let h = IntentHeader { txn_floor: 3 << 20, resolved_w: (3 << 20) - 100 };
    let mut b = [0u8; PAGE];
    h.encode(12, &mut b);
    let v = golden("intent_header", "intent table header (DESIGN §10.7)", &b);
    assert_eq!(IntentHeader::decode(&v), Some((12, h)));

    let s = IntentSlot { copy_seq: 31, txn_ids: smallvec::smallvec![(3 << 20) - 99, (3 << 20) - 98, (3 << 20) + 5] };
    let mut c = [0u8; INTENT_SLOT_COPY_BYTES];
    s.encode(&mut c);
    let v = golden("intent_slot", "intent slot copy (DESIGN §10.7)", &c);
    assert_eq!(IntentSlot::decode(&v), Some(s));
}

fn rec(kind: u8, db: u8, seq: u64, key: &[u8], val: &[u8], txn: Option<u64>, expire: Option<u64>) -> RecHeader {
    RecHeader {
        kind,
        db,
        flags: if expire.is_some() { REC_FLAG_HAS_EXPIRE } else { 0 } | if txn.is_some() { REC_FLAG_TXN } else { 0 },
        seq: Seq(seq),
        expire_at_ms: expire.unwrap_or(0),
        key_len: key.len() as u16,
        val_len: val.len() as u16,
        h32: (seq as u32).wrapping_mul(0x9E37_79B9),
        txn,
    }
}

#[test]
fn small_page() {
    let page = page_id(1, 0x3000);
    let crc = RecordCrc::new(SALT);
    let mut b = [0u8; PAGE];
    write_small_header(&mut b, page, 37);
    let long_val = vec![b'x'; 300];
    let recs: [(RecHeader, &[u8], &[u8], u16); 4] = [
        (rec(REC_KIND_STRING, 0, Seq::new(4, 10).0, b"user:1000", b"alice", None, Some(1_800_000_000_000)), b"user:1000", b"alice", 0x20),
        (rec(REC_KIND_HASH, 3, Seq::new(4, 11).0, b"h", b"\x01\x02\x03", Some(3 << 20), None), b"h", b"\x01\x02\x03", 0x60),
        (rec(REC_KIND_TXN_DELETE_MARKER, 0, Seq::new(4, 12).0, b"gone", b"", Some(3 << 20), None), b"gone", b"", 0xA0),
        (rec(REC_KIND_STRING, 15, Seq::new(4, 13).0, b"k300", &long_val, None, None), b"k300", &long_val, 0x400),
    ];
    for (h, k, v, off) in &recs {
        let mut h = *h;
        h.val_len = v.len() as u16;
        encode_small(&h, k, v, page, *off, crc, &mut b[usize::from(*off)..]);
    }
    let v = golden("small_page", "SMALL page with records (DESIGN §10.8)", &b);
    let img: [u8; PAGE] = v.try_into().expect("page");
    let mut out = Vec::new();
    assert_eq!(parse_small_page(&img, page, 37, crc, &mut out), PageParse::Ok);
    assert_eq!(out.len(), 4);
    for (view, (h, k, val, off)) in out.iter().zip(&recs) {
        assert_eq!(view.off, *off);
        assert_eq!(usize::from(view.len), small_len(k.len(), val.len(), h.txn.is_some()));
        assert_eq!((view.key, view.val), (*k, *val));
        assert_eq!((view.hdr.kind, view.hdr.db, view.hdr.seq, view.hdr.txn), (h.kind, h.db, h.seq, h.txn));
    }
    // The vector is location bound: as another page, nothing validates.
    let mut moved = img;
    write_small_header(&mut moved, page_id(1, 0x3001), 37);
    assert_eq!(parse_small_page(&moved, page_id(1, 0x3001), 37, crc, &mut out), PageParse::Ok);
    assert!(out.is_empty());
}

#[test]
fn extents() {
    let crc = RecordCrc::new(SALT);
    // VALUE, one page, with expiry and a twin run.
    let vpage = page_id(0, 0x10_0000);
    let key = b"big:value";
    let val: Vec<u8> = (0..3000u32).map(|i| (i * 31 + 7) as u8).collect();
    let vh = ExtHeader {
        kind: EXT_KIND_VALUE,
        vtype: EXT_VTYPE_STRING,
        db: 2,
        flags: EXT_FLAG_HAS_EXPIRE,
        page: vpage,
        seq: Seq::new(4, 20),
        expire_at_ms: 1_800_000_000_123,
        n_pages: 1,
        key_len: key.len() as u32,
        val_len: val.len() as u64,
        txn: 0,
        h32: 0xA1B2_C3D4,
        twin: Some(page_id(0, 0x10_0040)),
    };
    let mut b = vec![0u8; PAGE];
    encode_extent(&vh, key, &[&val], crc, &mut b);
    let v = golden("extent_value", "VALUE extent (DESIGN §10.9)", &b);
    let h = parse_extent_header(&v, vpage).expect("header");
    assert_eq!(h, vh);
    assert!(verify_extent(&v, &h, crc));
    assert_eq!(value_parts(&v, &h), Some((key.as_slice(), val.as_slice())));

    // CHUNK, two pages, TXN-flagged.
    let cpage = page_id(0, 0x10_1000);
    let payload: Vec<u8> = (0..6000u32).map(|i| (i ^ (i >> 8)) as u8).collect();
    let ch = ExtHeader {
        kind: EXT_KIND_CHUNK,
        vtype: 0,
        db: 2,
        flags: EXT_FLAG_TXN,
        page: cpage,
        seq: Seq::new(4, 21),
        expire_at_ms: 0,
        n_pages: 2,
        key_len: 0,
        val_len: payload.len() as u64,
        txn: (3 << 20) + 1,
        h32: 0,
        twin: None,
    };
    let mut c = vec![0u8; 2 * PAGE];
    encode_extent(&ch, &[], &[&payload], crc, &mut c);
    let v = golden("extent_chunk", "CHUNK extent (DESIGN §10.9)", &c);
    let h = parse_extent_header(&v, cpage).expect("header");
    assert!(verify_extent(&v, &h, crc));
    assert_eq!(chunk_payload(&v, &h), Some(payload.as_slice()));

    // CHUNKMAP naming that chunk and a full-size one.
    let refs = [
        ChunkRef { page: cpage, n_pages: 2, len: 6000, crc: stored_crc(&c).expect("crc") },
        ChunkRef { page: page_id(0, 0x10_2000), n_pages: 2048, len: 2048 * 4096 - 80, crc: 0x0BAD_F00D },
    ];
    let mut body = [0u8; 8 + 2 * 24];
    encode_chunk_refs(&refs, &mut body);
    let mpage = page_id(0, 0x10_0080);
    let mh = ExtHeader {
        kind: EXT_KIND_CHUNKMAP,
        vtype: EXT_VTYPE_HASH,
        db: 2,
        flags: 0,
        page: mpage,
        seq: Seq::new(4, 22),
        expire_at_ms: 0,
        n_pages: 1,
        key_len: 5,
        val_len: 6000 + 2048 * 4096 - 80,
        txn: 0,
        h32: 0x0102_0304,
        twin: Some(page_id(0, 0x10_00C0)),
    };
    let mut m = vec![0u8; PAGE];
    encode_extent(&mh, b"large", &[&body], crc, &mut m);
    let v = golden("extent_chunkmap", "CHUNKMAP extent (DESIGN §10.9)", &m);
    let h = parse_extent_header(&v, mpage).expect("header");
    assert!(verify_extent(&v, &h, crc));
    let (k, refs_body) = chunkmap_parts(&v, &h).expect("parts");
    assert_eq!(k, b"large");
    assert_eq!(decode_chunkmap(refs_body), Some(refs.to_vec()));
}

#[test]
fn device_layouts() {
    // Layout positions are part of the format: a 1 TiB device 0 with default 3% regions and a
    // 16 TiB non-zero device without regions.
    let pages = (1u64 << 40) / 4096;
    let l = DeviceLayout::compute(pages, true, (31u64 << 30).div_ceil(16 << 20) * 4096).expect("layout");
    assert_eq!(
        (l.n_ags, l.ag_table_blocks, l.pmeta_page, l.nmeta_page, l.intent_page, l.intent_slot_page),
        (61_567, 31, 128, 384, 386, 388)
    );
    assert_eq!((l.snap_region_page, l.snap_region_pages, l.ag_area_page), ([4096, 8_130_560], 8_126_464, 16_257_024));
    let l = DeviceLayout::compute(1u64 << 32, false, 0).expect("layout");
    assert_eq!((l.n_ags, l.ag_table_blocks, l.ag_area_page), (1_048_575, 517, 4096));
    assert_eq!(DeviceLayout::ag_table_copy_page(516, AbCopy::B), 4 + 2 * 516 + 1);
}
