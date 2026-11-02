//! End-to-end behaviour of the engine over RAM devices (DESIGN §14-§16, §18): small records,
//! extents and chunkmaps, deletes, TTLs, FLUSH, SCAN, clean restart and full-scan restart.

mod common;

use common::*;
use kv_storage::engine::{LocalEngine, format_devices};
use kv_storage::io::BlockDevice;
use kv_storage::recovery::RecoveryMode;
use kv_storage::ticket::TicketState;

const T0: u64 = 1_800_000_000_000;

fn fresh(pages: u64, seed: u64) -> Db<RamDev> {
    let mut d = RamDev::new(pages, 512, seed);
    format_devices(&mut [&mut d as &mut dyn BlockDevice], 16, false, 0, seed).unwrap();
    let mut eng = LocalEngine::open(vec![d], &cfg()).unwrap();
    assert_eq!(eng.recover().unwrap(), RecoveryMode::FullScan);
    Db::new(eng, T0)
}

fn reopen(devs: Vec<RamDev>, now: u64) -> (Db<RamDev>, RecoveryMode) {
    let mut eng = LocalEngine::open(devs, &cfg()).unwrap();
    let mode = eng.recover().unwrap();
    (Db::new(eng, now), mode)
}

#[test]
fn small_values_round_trip_and_survive_restart() {
    let mut db = fresh(64 * 4096, 1);
    for i in 0..300u64 {
        let k = format!("key:{i}");
        let t = db.set(0, k.as_bytes(), &value(i, 20 + (i as usize % 300)), None).unwrap();
        assert_eq!(db.wait(k.as_bytes(), t, TicketState::Written), TicketState::Durable);
    }
    for i in 0..300u64 {
        let k = format!("key:{i}");
        let (_, v) = db.get(0, k.as_bytes()).unwrap().expect("present");
        assert_eq!(v, value(i, 20 + (i as usize % 300)));
    }
    assert!(db.get(0, b"missing").unwrap().is_none());
    // crash restart (no snapshot): full scan
    let devs = db.eng.into_devices();
    let (mut db, mode) = reopen(devs, T0 + 1000);
    assert_eq!(mode, RecoveryMode::FullScan);
    for i in 0..300u64 {
        let k = format!("key:{i}");
        let (_, v) = db.get(0, k.as_bytes()).unwrap().unwrap_or_else(|| panic!("key {i} lost"));
        assert_eq!(v, value(i, 20 + (i as usize % 300)));
    }
    db.quiesce();
}

#[test]
fn updates_deletes_and_extents() {
    let mut db = fresh(64 * 4096, 2);
    let sizes = [10usize, 2000, 3000, 70_000, 300, 9_000_000];
    for (i, &n) in sizes.iter().enumerate() {
        let k = format!("v{i}");
        let t = db.set(0, k.as_bytes(), &value(i as u64, n), None).unwrap();
        db.wait(k.as_bytes(), t, TicketState::Durable);
    }
    // grow and shrink across the small/extent boundary
    for (i, &n) in sizes.iter().enumerate() {
        let k = format!("v{i}");
        let m = sizes[(i + 1) % sizes.len()];
        let t = db.set(0, k.as_bytes(), &value(100 + i as u64, m), None).unwrap();
        db.wait(k.as_bytes(), t, TicketState::Durable);
        let (_, v) = db.get(0, k.as_bytes()).unwrap().expect("present");
        assert_eq!(v.len(), m);
        assert_eq!(v, value(100 + i as u64, m));
        let _ = n;
    }
    let (existed, t) = db.del(0, b"v1").unwrap();
    assert!(existed);
    db.wait(b"v1", t, TicketState::Durable);
    assert!(db.get(0, b"v1").unwrap().is_none());
    db.quiesce();
    let devs = db.eng.into_devices();
    let (mut db, _) = reopen(devs, T0 + 5000);
    assert!(db.get(0, b"v1").unwrap().is_none(), "acked delete resurrected");
    for (i, _) in sizes.iter().enumerate() {
        if i == 1 {
            continue;
        }
        let k = format!("v{i}");
        let m = sizes[(i + 1) % sizes.len()];
        let (_, v) = db.get(0, k.as_bytes()).unwrap().unwrap_or_else(|| panic!("v{i} lost"));
        assert_eq!(v, value(100 + i as u64, m), "v{i}");
    }
    db.quiesce();
}

#[test]
fn ttl_lazy_expiry_and_no_return_after_restart() {
    let mut db = fresh(64 * 4096, 3);
    let t = db.set(0, b"short", b"x", Some(T0 + 500)).unwrap();
    db.wait(b"short", t, TicketState::Durable);
    let t = db.set(0, b"long", b"y", Some(T0 + 10_000_000)).unwrap();
    db.wait(b"long", t, TicketState::Durable);
    // shorten (LC-10): any write may shorten a TTL
    let (ok, t) = db.expire(0, b"long", Some(T0 + 1000)).unwrap();
    assert!(ok);
    db.wait(b"long", t, TicketState::Durable);
    assert_eq!(db.get(0, b"long").unwrap().unwrap().0.expire_at_ms, Some(T0 + 1000));
    db.advance(2000);
    assert!(db.get(0, b"short").unwrap().is_none());
    assert!(db.get(0, b"long").unwrap().is_none());
    let devs = db.eng.into_devices();
    let (mut db, _) = reopen(devs, T0 + 3000);
    assert!(db.get(0, b"short").unwrap().is_none(), "expired key returned");
    assert!(db.get(0, b"long").unwrap().is_none(), "expired key returned");
    db.quiesce();
}

#[test]
fn flushdb_and_flushall() {
    let mut db = fresh(128 * 4096, 4);
    for i in 0..50u64 {
        for d in 0..2u8 {
            let k = format!("k{i}");
            let t = db.set(d, k.as_bytes(), &value(i, 100), None).unwrap();
            db.wait(k.as_bytes(), t, TicketState::Written);
        }
    }
    for (p, t) in db.flushdb(Some(1)) {
        if let Some(t) = t {
            assert_eq!(db.eng.wait_ticket(p, t, TicketState::Durable), TicketState::Durable);
        }
    }
    assert!(db.get(1, b"k3").unwrap().is_none());
    assert!(db.get(0, b"k3").unwrap().is_some());
    let devs = db.eng.into_devices();
    let (mut db, _) = reopen(devs, T0 + 1);
    assert!(db.get(1, b"k3").unwrap().is_none(), "flushed key returned");
    assert_eq!(db.get(0, b"k3").unwrap().unwrap().1, value(3, 100));
    for (p, t) in db.flushdb(None) {
        if let Some(t) = t {
            db.eng.wait_ticket(p, t, TicketState::Durable);
        }
    }
    assert!(db.get(0, b"k3").unwrap().is_none());
    let t = db.set(0, b"after", b"z", None).unwrap();
    db.wait(b"after", t, TicketState::Durable);
    db.quiesce();
    let devs = db.eng.into_devices();
    let (mut db, _) = reopen(devs, T0 + 2);
    assert!(db.get(0, b"k3").unwrap().is_none());
    assert_eq!(db.get(0, b"after").unwrap().unwrap().1, b"z");
    db.quiesce();
}

#[test]
fn scan_returns_every_key_once() {
    let mut db = fresh(64 * 4096, 5);
    let mut want = Vec::new();
    for i in 0..400u64 {
        let k = format!("scan:{i}").into_bytes();
        let n = if i % 50 == 0 { 5000 } else { 40 };
        let t = db.set(0, &k, &value(i, n), None).unwrap();
        db.wait(&k, t, TicketState::Written);
        want.push(k);
    }
    want.sort();
    assert_eq!(db.scan_all(0), want);
    db.quiesce();
}

#[test]
fn clean_shutdown_restores_from_snapshot() {
    let mut d = RamDev::new(96 * 4096, 512, 6);
    format_devices(&mut [&mut d as &mut dyn BlockDevice], 16, false, 4096, 6).unwrap();
    let mut eng = LocalEngine::open(vec![d], &cfg()).unwrap();
    eng.recover().unwrap();
    let mut db = Db::new(eng, T0);
    for i in 0..200u64 {
        let k = format!("s{i}");
        let n = if i % 40 == 0 { 20_000 } else { 64 };
        let t = db.set(0, k.as_bytes(), &value(i, n), if i % 3 == 0 { Some(T0 + 1_000_000) } else { None }).unwrap();
        db.wait(k.as_bytes(), t, TicketState::Durable);
    }
    let devs = db.eng.shutdown(true).unwrap();
    let (mut db, mode) = reopen(devs, T0 + 10);
    assert_eq!(mode, RecoveryMode::Snapshot);
    for i in 0..200u64 {
        let k = format!("s{i}");
        let n = if i % 40 == 0 { 20_000 } else { 64 };
        let (m, v) = db.get(0, k.as_bytes()).unwrap().unwrap_or_else(|| panic!("s{i} lost"));
        assert_eq!(v, value(i, n));
        assert_eq!(m.expire_at_ms.is_some(), i % 3 == 0);
    }
    // writes after a snapshot restore, then a crash: full scan finds everything
    let t = db.set(0, b"post", b"p", None).unwrap();
    db.wait(b"post", t, TicketState::Durable);
    let devs = db.eng.into_devices();
    let (mut db, mode) = reopen(devs, T0 + 20);
    assert_eq!(mode, RecoveryMode::FullScan);
    assert_eq!(db.get(0, b"post").unwrap().unwrap().1, b"p");
    assert_eq!(db.get(0, b"s7").unwrap().unwrap().1, value(7, 64));
    db.quiesce();
}

#[test]
fn multi_key_writes_commit_atomically() {
    let mut db = fresh(64 * 4096, 7);
    // keys sharing a hash tag share a slot, hence a partition
    let kvs: Vec<(Vec<u8>, Vec<u8>)> = (0..5u64).map(|i| (format!("{{tag}}m{i}").into_bytes(), value(i, 50))).collect();
    let refs: Vec<(&[u8], &[u8])> = kvs.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
    let t = db.mset(0, &refs).unwrap();
    assert_eq!(db.wait(&kvs[0].0, t, TicketState::Durable), TicketState::Durable);
    for (k, v) in &kvs {
        assert_eq!(&db.get(0, k).unwrap().unwrap().1, v);
    }
    db.quiesce();
    let devs = db.eng.into_devices();
    let (mut db, _) = reopen(devs, T0 + 1);
    for (k, v) in &kvs {
        assert_eq!(&db.get(0, k).unwrap().unwrap().1, v);
    }
    db.quiesce();
}


#[test]
fn corrupt_snapshot_section_falls_back_to_the_full_scan() {
    let mut d = RamDev::new(96 * 4096, 512, 16);
    format_devices(&mut [&mut d as &mut dyn BlockDevice], 16, false, 4096, 16).unwrap();
    let mut eng = LocalEngine::open(vec![d], &cfg()).unwrap();
    eng.recover().unwrap();
    let mut db = Db::new(eng, T0);
    for i in 0..100u64 {
        let k = format!("c{i}");
        let t = db.set(0, k.as_bytes(), &value(i, 80), None).unwrap();
        db.wait(k.as_bytes(), t, TicketState::Durable);
    }
    let layout = db.eng.boot().layouts()[0];
    let mut devs = db.eng.shutdown(true).unwrap();
    // the snapshot went to region B (the superblock named A): damage a section byte there
    let region_b = layout.snap_region_page[1] as usize * 4096;
    devs[0].data[region_b + 4096 + 40] ^= 0x5A;
    let (mut db, mode) = reopen(devs, T0 + 1);
    assert_eq!(mode, RecoveryMode::FullScan, "a bad section makes the node take the full scan");
    for i in 0..100u64 {
        let k = format!("c{i}");
        assert_eq!(db.get(0, k.as_bytes()).unwrap().unwrap().1, value(i, 80));
    }
    db.quiesce();
}
