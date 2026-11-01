//! Coordinator state files (DESIGN §23.3, LC-02): fixed A/B halves, torn writes, constant
//! file sizes, exports.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use kv_coord::wsr::peers::{CoordRecord, PeerList, parse_coord_id};
use kv_coord::wsr::persist::{Export, META_FILE, MIN_HALF_BYTES, STATE_FILE, StateFiles, Versioned};

fn tmp(name: &str) -> PathBuf {
    let base = Path::new(env!("CARGO_TARGET_TMPDIR"));
    let d = base.join(format!("persist-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn me(name: &str, port: u16) -> CoordRecord {
    let a = |p: u16| SocketAddr::from(([127, 0, 0, 1], p));
    CoordRecord::new(parse_coord_id(name).unwrap(), a(port), a(port + 1), a(port + 2))
}

fn list(recs: &[CoordRecord]) -> PeerList {
    PeerList {
        r#gen: 1,
        peers: recs.to_vec(),
    }
}

fn v(version: u64, term: u64, fill: u8, len: usize) -> Versioned {
    Versioned {
        version,
        term,
        bytes: vec![fill; len],
    }
}

/// Byte offset of the half that holds the newest valid value, from the file contents.
fn newest_half(dir: &Path, half: usize) -> usize {
    let data = std::fs::read(dir.join(STATE_FILE)).unwrap();
    let seq = |i: usize| u64::from_le_bytes(data[i * half + 0x10..i * half + 0x18].try_into().unwrap());
    if seq(0) >= seq(1) { 0 } else { half }
}

fn corrupt(path: &Path, off: usize, len: usize) {
    let mut data = std::fs::read(path).unwrap();
    for b in &mut data[off..off + len] {
        *b ^= 0x5A;
    }
    std::fs::write(path, data).unwrap();
}

#[test]
fn create_open_write_reload() {
    let dir = tmp("basic");
    let r = me("c1", 7100);
    StateFiles::create_with_identity(&dir, MIN_HALF_BYTES, Some(&r), &list(std::slice::from_ref(&r))).unwrap();
    // creating twice is refused
    assert!(StateFiles::create(&dir, MIN_HALF_BYTES, &list(std::slice::from_ref(&r))).is_err());
    let (mut f, p) = StateFiles::open(&dir).unwrap();
    assert_eq!(p.me.as_ref(), Some(&r));
    assert_eq!(p.term, 0);
    assert_eq!(p.committed.version, 0, "no valid half yet");
    f.write_state(&v(1, 0, 1, 100), None).unwrap();
    f.write_state(&v(1, 0, 1, 100), Some(&v(2, 3, 2, 50))).unwrap();
    f.write_meta(3, Some(r.id), &list(std::slice::from_ref(&r))).unwrap();
    let (_, p) = StateFiles::open(&dir).unwrap();
    assert_eq!(p.term, 3);
    assert_eq!(p.voted_for, Some(r.id));
    assert_eq!(p.me.as_ref(), Some(&r), "the identity survives meta rewrites");
    assert_eq!(p.committed, v(1, 0, 1, 100));
    assert_eq!(p.accepted, Some(v(2, 3, 2, 50)));
    // too large for a half
    assert!(f.write_state(&v(3, 3, 0, MIN_HALF_BYTES), None).is_err());
}

#[test]
fn torn_state_write_keeps_the_previous_value() {
    let dir = tmp("torn");
    let r = me("c1", 7100);
    let half = MIN_HALF_BYTES;
    StateFiles::create_with_identity(&dir, half, Some(&r), &list(std::slice::from_ref(&r))).unwrap();
    let (mut f, _) = StateFiles::open(&dir).unwrap();
    f.write_state(&v(5, 1, 5, 20_000), None).unwrap();
    f.write_state(&v(6, 1, 6, 20_000), None).unwrap();
    drop(f);
    // A torn write of the newest half (here: its tail never reached the device).
    let off = newest_half(&dir, half);
    corrupt(&dir.join(STATE_FILE), off + 10_000, 4096);
    let (mut f, p) = StateFiles::open(&dir).unwrap();
    assert_eq!(p.committed, v(5, 1, 5, 20_000));
    // The next write goes over the torn half, never over the surviving one.
    f.write_state(&v(7, 2, 7, 100), None).unwrap();
    corrupt(&dir.join(STATE_FILE), newest_half(&dir, half), 64);
    let (_, p) = StateFiles::open(&dir).unwrap();
    assert_eq!(p.committed.version, 5, "torn header: the older half wins");
    // A torn meta copy falls back to the other copy.
    let (mut f, _) = StateFiles::open(&dir).unwrap();
    f.write_meta(8, None, &list(std::slice::from_ref(&r))).unwrap();
    f.write_meta(9, None, &list(std::slice::from_ref(&r))).unwrap();
    drop(f);
    let meta = std::fs::read(dir.join(META_FILE)).unwrap();
    let seq = |i: usize| u64::from_le_bytes(meta[i * 4096 + 0x10..i * 4096 + 0x18].try_into().unwrap());
    let newest = if seq(0) > seq(1) { 0 } else { 4096 };
    corrupt(&dir.join(META_FILE), newest + 100, 16);
    let (_, p) = StateFiles::open(&dir).unwrap();
    assert_eq!(p.term, 8);
}

/// LC-02 `ab_files_constant_size`: the files never change length however many commits are
/// written (2000 here; the 10^5 run is `ab_files_constant_size_long`).
#[test]
fn ab_files_constant_size() {
    constant_size(2_000);
}

#[test]
#[ignore = "10^5 fdatasync'd commits; run with --ignored"]
fn ab_files_constant_size_long() {
    constant_size(100_000);
}

fn constant_size(commits: u64) {
    let dir = tmp(&format!("size{commits}"));
    let r = me("c1", 7100);
    StateFiles::create_with_identity(&dir, MIN_HALF_BYTES, Some(&r), &list(std::slice::from_ref(&r))).unwrap();
    let len = |f: &str| std::fs::metadata(dir.join(f)).unwrap().len();
    let (m0, s0) = (len(META_FILE), len(STATE_FILE));
    let (mut f, _) = StateFiles::open(&dir).unwrap();
    for i in 1..=commits {
        let size = (i as usize * 37) % 20_000;
        f.write_state(&v(i, i / 10, i as u8, size), (i % 3 == 0).then(|| v(i + 1, i, 1, size / 2)).as_ref())
            .unwrap();
        if i % 10 == 0 {
            f.write_meta(i / 10, None, &list(std::slice::from_ref(&r))).unwrap();
        }
    }
    assert_eq!((len(META_FILE), len(STATE_FILE)), (m0, s0));
    let (_, p) = StateFiles::open(&dir).unwrap();
    assert_eq!(p.committed.version, commits);
}

#[test]
fn export_round_trip_and_state_file_import() {
    let x = Export {
        version: 42,
        coord_term: 7,
        value_term: 6,
        peer_gen: 3,
        bytes: vec![9; 1234],
    };
    let enc = x.encode();
    assert_eq!(Export::decode(&enc).unwrap(), x);
    let mut bad = enc.clone();
    bad[100] ^= 1;
    assert!(Export::decode(&bad).is_err());
    assert!(Export::decode(&enc[..enc.len() - 1]).is_err());

    let dir = tmp("import");
    let r = me("c1", 7100);
    StateFiles::create_with_identity(&dir, MIN_HALF_BYTES, Some(&r), &list(std::slice::from_ref(&r))).unwrap();
    let (mut f, _) = StateFiles::open(&dir).unwrap();
    f.write_state(&v(10, 4, 3, 500), None).unwrap();
    f.write_meta(5, None, &list(std::slice::from_ref(&r))).unwrap();
    let file = dir.join("x.export");
    std::fs::write(&file, &enc).unwrap();
    assert_eq!(Export::read_any(&file).unwrap(), x);
    let y = Export::read_any(&dir.join(STATE_FILE)).unwrap();
    assert_eq!((y.version, y.value_term, y.coord_term, y.peer_gen), (10, 4, 5, 1));
    assert_eq!(y.bytes, vec![3; 500]);
}
