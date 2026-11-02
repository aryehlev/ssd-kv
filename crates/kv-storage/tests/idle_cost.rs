//! Idle cost of the partition tick (§16.4 active expiry, §19): with keys holding a TTL far in
//! the future and no input, ticks find nothing to do. The RAM expiry sweep is paced by time (one
//! pass over the TTL entries per second), not by the tick rate: it used to visit up to 256
//! tables and 64 Ki hint positions per tick, so a partition ticked every millisecond swept
//! continuously.

mod common;

use common::*;
use kv_storage::engine::{LocalEngine, format_devices};
use kv_storage::io::BlockDevice;
use kv_storage::ticket::TicketState;

const T0: u64 = 1_800_000_000_000;

#[test]
fn idle_ticks_with_ttl_keys_are_cheap() {
    let mut d = RamDev::new(256 * 4096, 4096, 1);
    format_devices(&mut [&mut d as &mut dyn BlockDevice], 16, false, 0, 1).unwrap();
    let mut eng = LocalEngine::open(vec![d], &cfg()).unwrap();
    eng.recover().unwrap();
    let mut db = Db::new(eng, T0);
    let n: u64 = std::env::var("IDLE_KEYS").ok().and_then(|s| s.parse().ok()).unwrap_or(20_000);
    for i in 0..n {
        let k = format!("ttl:{i}");
        let _ = db.set(0, k.as_bytes(), b"v", Some(T0 + 86_400_000 + i)).unwrap();
        if i % 64 == 0 {
            db.eng.pump();
        }
    }
    db.quiesce();
    let work = |db: &Db<RamDev>| (0..16).map(|p| db.eng.partition(p).counters().expire_sweep_work).sum::<u64>();
    let before = work(&db);
    // two idle seconds, ticked every millisecond
    let iters = 2000u32;
    let t = std::time::Instant::now();
    for _ in 0..iters {
        db.advance(1);
        db.eng.pump();
    }
    let per_pump_us = t.elapsed().as_secs_f64() * 1e6 / f64::from(iters);
    let swept = work(&db) - before;
    // two passes: every TTL entry and every table of db 0 (1024 per partition) twice
    let passes = 2 * (n + 16 * 1024);
    eprintln!("idle pump over 16 partitions with {n} TTL keys: {per_pump_us:.1} us, sweep work {swept} (two passes {passes})");
    assert!(swept <= passes + passes / 2, "the sweep did {swept} units of work in two idle seconds (two passes are {passes})");
    assert!(swept >= passes / 4, "the sweep stalled: {swept} units of work in two seconds");
}

/// Active expiry still removes keys nobody reads, within a few seconds of their expiry, small
/// records and extents alike, and leaves keys with a later TTL alone.
#[test]
fn active_expiry_removes_unread_keys() {
    let mut d = RamDev::new(128 * 4096, 512, 2);
    format_devices(&mut [&mut d as &mut dyn BlockDevice], 16, false, 0, 2).unwrap();
    let mut eng = LocalEngine::open(vec![d], &cfg()).unwrap();
    eng.recover().unwrap();
    let mut db = Db::new(eng, T0);
    for i in 0..400u64 {
        let k = format!("e:{i}");
        let len = if i % 10 == 0 { 3000 } else { 20 };
        let at = if i % 2 == 0 { T0 + 1000 } else { T0 + 3_600_000 };
        let t = db.set(0, k.as_bytes(), &value(i, len), Some(at)).unwrap();
        db.wait(k.as_bytes(), t, TicketState::Durable);
    }
    let keys = |db: &Db<RamDev>| (0..16).map(|p| db.eng.partition(p).counters().keys[0]).sum::<u64>();
    db.quiesce();
    assert_eq!(keys(&db), 400);
    // five idle seconds, ticked every millisecond, no reads
    for _ in 0..5000 {
        db.advance(1);
        db.eng.pump();
    }
    assert_eq!(keys(&db), 200, "keys past their TTL are removed without being read");
    db.quiesce();
    for i in (1..400u64).step_by(2) {
        assert!(db.get(0, format!("e:{i}").as_bytes()).unwrap().is_some());
    }
}
