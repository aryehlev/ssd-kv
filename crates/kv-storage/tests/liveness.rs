//! Liveness regressions: every accepted request reaches a terminal state in bounded time
//! (DESIGN §6.5, §14, §16; "no hangs"). Each test names the stuck state it once produced.

mod common;

use common::*;
use kv_storage::engine::{LocalEngine, format_devices};
use kv_storage::io::BlockDevice;
use kv_storage::ticket::TicketState;

const T0: u64 = 1_800_000_000_000;

fn fresh(pages: u64, seed: u64) -> Db<RamDev> {
    let mut d = RamDev::new(pages, 512, seed);
    format_devices(&mut [&mut d as &mut dyn BlockDevice], 16, false, 0, seed).unwrap();
    let mut eng = LocalEngine::open(vec![d], &cfg()).unwrap();
    eng.recover().unwrap();
    Db::new(eng, T0)
}

/// Two writes of one key executed in the same tick, the first staging a new location (an
/// extent, or a small record on another page) that the second replaces before it is written:
/// the first command's ticket is handed to the second's effect and must complete with it.
/// It used to be counted twice (once for the cancelled write, again for the new effect) and
/// stayed `Executed` forever.
#[test]
fn ticket_handed_from_a_cancelled_staged_write_completes() {
    let shapes: [(usize, usize); 6] = [(6000, 10), (10, 6000), (6000, 9000), (3_000_000, 10), (10, 3_000_000), (1500, 1800)];
    for (i, &(a, b)) in shapes.iter().enumerate() {
        let mut db = fresh(64 * 4096, 7 + i as u64);
        let key = format!("k{i}");
        let k = key.as_bytes();
        // an existing small version first, so that the second write may move the key
        let t0 = db.set(0, k, b"seed", None).unwrap();
        assert_eq!(db.wait(k, t0, TicketState::Written), TicketState::Durable);
        // `run` returns right after exec: the second write executes before any tick
        let t1 = db.set(0, k, &value(1, a), None).unwrap();
        let t2 = db.set(0, k, &value(2, b), None).unwrap();
        assert_eq!(db.wait(k, t2, TicketState::Durable), TicketState::Durable, "shape {a}->{b}: second write");
        assert_eq!(db.wait(k, t1, TicketState::Durable), TicketState::Durable, "shape {a}->{b}: first write's ticket");
        let (_, v) = db.get(0, k).unwrap().expect("present");
        assert_eq!(v, value(2, b));
        db.eng.assert_quiescent_complete(100_000);
    }
}

/// A key expired (lazily, without a write) while its page had an image in flight: the image's
/// record list, which becomes the page's M when durable, must not bring the expired record back
/// as Live. It used to, and the next write of the key that came back to that page counted it as a
/// third copy and waited forever for a source only that write could release.
#[test]
fn expiry_during_an_image_in_flight_does_not_revive_the_record() {
    for seed in 0..8u64 {
        let mut db = fresh(64 * 4096, 40 + seed);
        let (a, k): (&[u8], &[u8]) = (b"{t}a", b"{t}k");
        let t = db.set(0, a, b"a1", None).unwrap();
        db.wait(a, t, TicketState::Durable);
        let t = db.set(0, k, b"k1", Some(T0 + 1000)).unwrap();
        db.wait(k, t, TicketState::Durable);
        let (pa, pk) = (db.eng.locate(0, a).unwrap().page, db.eng.locate(0, k).unwrap().page);
        if pa != pk {
            continue;
        }
        // an image of the page in flight (built and submitted, not completed)
        let _ = db.set(0, a, b"a2", None).unwrap();
        db.eng.pump();
        // the key expires and is read: expiry without write
        db.advance(2000);
        assert!(db.get(0, k).unwrap().is_none());
        db.eng.assert_quiescent_complete(10_000);
        // later writes of the key go anywhere, including back to that page, and complete
        for i in 0..6u64 {
            let t = db.set(0, k, &value(i, 40 + 300 * i as usize), None).unwrap();
            assert_eq!(db.wait(k, t, TicketState::Durable), TicketState::Durable, "seed {seed} write {i}");
        }
        db.eng.assert_quiescent_complete(10_000);
        return;
    }
    panic!("no seed put both keys on one page");
}

/// Spare AGs are a latency optimization (§11.1): on a device with room for two AGs per
/// partition but not four, every partition must still get its first SMALL and its first EXTENT
/// AG. Spares used to be granted first come, first served, and partitions that asked late got
/// `-OOM` (and a one-second refusal latch) with the device almost empty.
#[test]
fn spare_ags_never_take_the_space_of_a_first_write() {
    use kv_storage::layout::AG_PAGES;
    // 64 partitions, 140 AGs (133 under the 0.95 fill limit): two per partition fit, four do not
    let pages = 140 * u64::from(AG_PAGES) + u64::from(AG_PAGES);
    let mut d = RamDev::new(pages, 4096, 3);
    format_devices(&mut [&mut d as &mut dyn BlockDevice], 64, false, 0, 3).unwrap();
    let mut eng = LocalEngine::open(vec![d], &cfg()).unwrap();
    eng.recover().unwrap();
    let mut db = Db::new(eng, T0);
    let mut parts = std::collections::BTreeSet::new();
    for i in 0..1200u64 {
        let k = format!("key:{i}");
        parts.insert(db.eng.partition_of(k.as_bytes()));
        let len = if i % 2 == 0 { 2048 + (i as usize * 37) % 14_000 } else { 1 + (i as usize * 13) % 300 };
        match db.set(0, k.as_bytes(), &value(i, len), None) {
            Ok(t) => {
                db.wait(k.as_bytes(), t, TicketState::Written);
            }
            Err(e) => panic!("write {i} ({len} bytes, partition {}): {e:?}", db.eng.partition_of(k.as_bytes())),
        }
    }
    assert_eq!(parts.len(), 64, "every partition wrote");
    let (used, total) = db.eng.meta().ag_usage();
    assert!(used <= total, "{used} of {total}");
    db.quiesce();
}

/// The capacity floor (two AGs per partition under the fill limit) is reported precisely.
#[test]
fn capacity_floor_is_reported() {
    use kv_storage::layout::{AG_PAGES, DeviceLayout};
    let small = DeviceLayout::compute(64 * u64::from(AG_PAGES), true, 0).unwrap();
    let e = kv_storage::engine::check_capacity(&[small], 64, 0.95).unwrap_err();
    assert!(e.to_string().contains("too small for 64 partitions"), "{e}");
    assert!(kv_storage::engine::check_capacity(&[small], 16, 0.95).is_ok());
}

/// A delete waits for the key's older copies (D1), and only for those: writes of the key after
/// the delete create newer copies that must not extend the wait (D2). Its ticket used to wait
/// until the key had no tracked copy at all, which under sustained rewrites that move the key
/// between pages and extents never happened.
#[test]
fn delete_is_not_starved_by_later_writes_of_its_key() {
    for seed in 0..4u64 {
        let mut db = fresh(64 * 4096, 60 + seed);
        let k: &[u8] = b"hot";
        let p = db.eng.partition_of(k);
        let t = db.set(0, k, &value(0, 20), None).unwrap();
        db.wait(k, t, TicketState::Durable);
        // move the key to an extent (its small copy becomes a tracked source), then delete it
        // before that extent is written
        let _ = db.set(0, k, &value(1, 6000), None).unwrap();
        let (_, del) = db.del(0, k).unwrap();
        let del = del.expect("delete ticket");
        let mut done_at = None;
        for i in 0..400u64 {
            // every rewrite moves the key (small <-> extent) and leaves a tracked copy behind
            let len = if i % 2 == 0 { 30 } else { 5000 + (i as usize % 7) * 900 };
            let _ = db.set(0, k, &value(10 + i, len), None).unwrap();
            db.eng.pump();
            if done_at.is_none() && matches!(db.eng.ticket_state(p, del), Some(TicketState::Durable | TicketState::Written)) {
                done_at = Some(i);
            }
        }
        let at = done_at.unwrap_or_else(|| panic!("seed {seed}: the delete was still waiting after 400 later writes of its key\n{}", db.eng.partition(p).diag_deep(db.eng.cache())));
        assert!(at < 50, "seed {seed}: the delete completed only after {at} later writes");
        db.quiesce();
    }
}
