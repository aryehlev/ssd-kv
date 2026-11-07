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
    let mut parts = [false; 64];
    for i in 0..1200u64 {
        let k = format!("key:{i}");
        parts[usize::from(db.eng.partition_of(k.as_bytes()))] = true;
        let len = if i % 2 == 0 { 2048 + (i as usize * 37) % 14_000 } else { 1 + (i as usize * 13) % 300 };
        match db.set(0, k.as_bytes(), &value(i, len), None) {
            Ok(t) => {
                db.wait(k.as_bytes(), t, TicketState::Written);
            }
            Err(e) => panic!("write {i} ({len} bytes, partition {}): {e:?}", db.eng.partition_of(k.as_bytes())),
        }
    }
    assert!(parts.iter().all(|&w| w), "every partition wrote");
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

/// FLUSHALL releases the partition's old AGs once nothing is in flight to them; a page whose
/// image was written but still waits for its flush (write-back cache) counts as in flight. It
/// did not: the page was forgotten, the flush's durability event found no page state, and the
/// write's ticket stayed `Written` forever.
#[cfg(feature = "sim")]
#[test]
fn flushall_waits_for_images_awaiting_their_flush() {
    use kv_storage::sim::SimDevice;
    for gap in 0..6u32 {
        for seed in 0..3u64 {
            let mut d = SimDevice::new(64 * 4096, 512, None, seed);
            format_devices(&mut [&mut d as &mut dyn BlockDevice], 16, false, 0, seed).unwrap();
            let mut eng = LocalEngine::open(vec![d], &cfg()).unwrap();
            eng.recover().unwrap();
            let mut db = Db::new(eng, T0);
            let mut ts = Vec::new();
            for i in 0..12u64 {
                let k = format!("f{i}");
                ts.push((k.clone(), db.set(0, k.as_bytes(), &value(i, 40 + 500 * (i as usize % 3)), None).unwrap()));
            }
            for _ in 0..gap {
                db.eng.pump();
            }
            let flushed = db.flushdb(None);
            for (k, t) in ts {
                db.wait(k.as_bytes(), t, TicketState::Durable);
            }
            for (p, t) in flushed {
                db.wait_in(p, t, TicketState::Durable);
            }
            db.quiesce();
        }
    }
}

/// A versioned write (replica image, snapshot record, migration) whose seq is not above every
/// copy of its key the partition holds: recovery would pick the older write's copy, and the
/// hand-off that releases a protected copy (a durable copy with a higher seq) never comes. Such a
/// write used to stage an effect that waited forever for that copy; it must be refused (or be a
/// no-op) and never leave anything waiting.
#[test]
fn versioned_write_at_or_below_an_existing_copy_never_waits() {
    use kv_common::Access;
    use kv_index::VType;
    use kv_storage::effect::Op;
    use kv_storage::keyspace::{ApplyMode, ApplyOutcome};
    use kv_storage::layout::Seq;
    use kv_storage::partition::{Origin, ReqKind, Request};
    use smallvec::smallvec;
    let shapes: [(usize, usize); 5] = [(20, 30), (20, 3000), (6000, 30), (6000, 9000), (30, 0)];
    for (i, &(a, b)) in shapes.iter().enumerate() {
        for mode in [ApplyMode::Image, ApplyMode::SnapshotMerge, ApplyMode::Ordered] {
            let mut db = fresh(64 * 4096, 90 + i as u64);
            let k: &[u8] = b"img";
            let p = db.eng.partition_of(k);
            let t = db.set(0, k, &value(1, a), None).unwrap();
            db.wait(k, t, TicketState::Durable);
            // a second version, still in flight for some shapes (the first one is a protected copy)
            let t2 = db.set(0, k, &value(2, a + 7), None).unwrap();
            let req = |s: Seq| Request { db: 0, keys: smallvec![(Box::<[u8]>::from(k), Access::Write)], kind: ReqKind::Keyed, origin: Origin::Image(s) };
            let (cur, _) = db.eng.run(p, req(Seq(1)), &mut |ks| ks.stored_seq(k)).unwrap();
            assert!(cur > Seq(1));
            for s in [Seq(1), cur] {
                let op = if b == 0 { Op::Del { db: 0, key: k.into() } } else { Op::Put { db: 0, vtype: 1, expire_at: 0, key: k.into(), value: value(3, b) } };
                let (o, out) = db.eng.run(p, req(s), &mut |ks| ks.apply_replicated(&op, s, mode)).unwrap();
                assert_eq!(o, ApplyOutcome::Skipped, "shape {a}->{b} {mode:?} at {s:?}");
                assert!(out.ticket.is_none());
            }
            // a versioned write that bypasses `apply_replicated` is refused, not staged
            let r = db.eng.run(p, req(Seq(1)), &mut |ks| ks.put(k, &value(4, b.max(1)), VType::String, None));
            assert!(matches!(r, Err(kv_storage::keyspace::StoreError::Io(_))), "shape {a}->{b}: low put refused");
            db.wait(k, t2, TicketState::Durable);
            db.quiesce();
            // the newest version is still the client's second write
            let (_, v) = db.get(0, k).unwrap().expect("present");
            assert_eq!(v, value(2, a + 7), "shape {a}->{b} {mode:?}: the newer version stays");
        }
    }
}

/// Spare AGs are a latency optimization (§11.1): a partition that needs an AG and is refused
/// gets the spares other partitions hold idle. They used to stay with their holders, so one
/// partition's large writes failed with `-OOM` while a sixth of the device sat in idle spares.
#[test]
fn idle_spare_ags_go_to_a_partition_refused_space() {
    use kv_storage::keyspace::StoreError;
    use kv_storage::layout::AG_PAGES;
    // 16 partitions, 40 AGs (38 under the 0.95 fill limit): two per partition and 6 spares
    let pages = 40 * u64::from(AG_PAGES) + u64::from(AG_PAGES);
    let mut d = RamDev::new(pages, 4096, 9);
    format_devices(&mut [&mut d as &mut dyn BlockDevice], 16, false, 0, 9).unwrap();
    let mut eng = LocalEngine::open(vec![d], &cfg()).unwrap();
    eng.recover().unwrap();
    let mut db = Db::new(eng, T0);
    // a small write in every partition first (each takes its SMALL and EXTENT AG)
    for i in 0..400u64 {
        let k = format!("w{i}");
        let t = db.set(0, k.as_bytes(), &value(i, if i % 2 == 0 { 5000 } else { 40 }), None).unwrap();
        db.wait(k.as_bytes(), t, TicketState::Written);
    }
    // then 1 MiB values into one partition only (one hash tag)
    let mut stored = 0u64;
    let mut refused = 0u32;
    for i in 0..200u64 {
        let k = format!("{{hot}}{i}");
        match db.set(0, k.as_bytes(), &value(1000 + i, 1 << 20), None) {
            Ok(t) => {
                db.wait(k.as_bytes(), t, TicketState::Written);
                stored += 1;
            }
            Err(StoreError::OutOfSpace) => {
                refused += 1;
                if refused > 12 {
                    break;
                }
                // the refusal latch lasts a second; idle spares go back meanwhile
                for _ in 0..20 {
                    db.advance(100);
                    db.eng.pump();
                }
            }
            Err(e) => panic!("{e:?}"),
        }
    }
    let (used, total) = db.eng.meta().ag_usage();
    // the partition's own EXTENT AG holds ~15 values; every spare it gets holds ~15 more
    assert!(stored >= 60, "only {stored} MiB stored in the hot partition before -OOM ({used}/{total} AGs used)");
    db.quiesce();
}

/// A full device must not refuse what makes room: a TTL change of an existing key (EXPIRE,
/// PERSIST) rewrites the key without growing it, and a multi-key delete only stages markers.
/// Both used to get `-OOM` once the partition reached the fill limit (they needed a new copy's
/// space like any write), so on a full device keys could be given a TTL to free space only one
/// DEL at a time.
#[test]
fn ttl_changes_and_deletes_work_on_a_full_partition() {
    use kv_storage::keyspace::StoreError;
    use kv_storage::layout::AG_PAGES;
    let pages = 40 * u64::from(AG_PAGES) + u64::from(AG_PAGES);
    let mut d = RamDev::new(pages, 4096, 9);
    format_devices(&mut [&mut d as &mut dyn BlockDevice], 16, false, 0, 9).unwrap();
    let mut eng = LocalEngine::open(vec![d], &cfg()).unwrap();
    eng.recover().unwrap();
    let mut db = Db::new(eng, T0);
    // fill the device through one partition (one hash tag) with values of 2-60 KiB until
    // it refuses
    let mut stored = Vec::new();
    let mut refused = 0;
    for i in 0..20_000u64 {
        let k = format!("{{full}}{i}");
        match db.set(0, k.as_bytes(), &value(i, 2100 + (i as usize * 9973) % 60_000), None) {
            Ok(t) => {
                db.wait(k.as_bytes(), t, TicketState::Written);
                stored.push(k);
            }
            Err(StoreError::OutOfSpace) => {
                refused += 1;
                if refused > 50 {
                    break;
                }
            }
            Err(e) => panic!("{e:?}"),
        }
    }
    assert!(refused > 50 && stored.len() > 100, "the partition filled up ({} keys stored)", stored.len());
    for (i, k) in stored.iter().take(40).enumerate() {
        let k = k.as_bytes();
        let at = if i % 2 == 0 { Some(T0 + 3_600_000) } else { None };
        let (hit, t) = db.expire(0, k, at).unwrap_or_else(|e| panic!("TTL change {i} on a full partition: {e:?}"));
        assert!(hit);
        db.wait(k, t, TicketState::Written);
    }
    // a multi-key delete (txn markers) frees space on a full partition
    let keys: Vec<&[u8]> = stored[40..44].iter().map(|k| k.as_bytes()).collect();
    let p = db.eng.partition_of(keys[0]);
    let (n, out) = db
        .eng
        .run(p, keyed(0, &keys, kv_common::Access::Write), &mut |ks| {
            ks.select(0);
            let mut n = 0;
            for k in &keys {
                n += u32::from(ks.delete(k)?);
            }
            Ok(n)
        })
        .unwrap_or_else(|e| panic!("multi-key delete on a full partition: {e:?}"));
    assert_eq!(n, 4);
    db.wait_in(p, out.ticket, TicketState::Written);
    db.quiesce();
}

/// FLUSHDB frees the space of the flushed records for every kind of write (§16.5, §11.7):
/// the cached pages' records of the db were never marked dead (the reclaim walk reads
/// uncached pages only, and nothing rebuilds a clean page), and no page holding only dead
/// records became fresh again, so no SMALL AG went back to the pool: after a FLUSHDB of a
/// device filled with small records every extent write got `-OOM` until a FLUSHALL.
#[test]
fn flushdb_frees_space_for_every_kind_of_write() {
    use kv_storage::keyspace::StoreError;
    use kv_storage::layout::AG_PAGES;
    for cache_mb in [8usize, 256] {
        // 16 partitions, 40 AGs: one partition (one hash tag) fills what the others leave
        let pages = 40 * u64::from(AG_PAGES) + u64::from(AG_PAGES);
        let mut d = RamDev::new(pages, 4096, 9);
        format_devices(&mut [&mut d as &mut dyn BlockDevice], 16, false, 0, 9).unwrap();
        let mut c = cfg();
        c.cache_bytes = cache_mb << 20;
        let mut eng = LocalEngine::open(vec![d], &c).unwrap();
        eng.recover().unwrap();
        let mut db = Db::new(eng, T0);
        let mut refused = 0;
        let mut n = 0u64;
        while refused < 200 {
            let k = format!("{{t}}s{n}");
            n += 1;
            match db.set(0, k.as_bytes(), &value(n, 100), None) {
                Ok(t) => {
                    db.wait(k.as_bytes(), t, TicketState::Written);
                }
                Err(StoreError::OutOfSpace) => refused += 1,
                Err(e) => panic!("{e:?}"),
            }
        }
        let (full, total) = db.eng.meta().ag_usage();
        for (p, t) in db.flushdb(Some(0)) {
            db.wait_in(p, t, TicketState::Durable);
        }
        // the reclaim walk is paced: give it time
        for _ in 0..600 {
            db.advance(100);
            db.eng.pump();
        }
        let (after, _) = db.eng.meta().ag_usage();
        let mut stored = 0;
        for i in 0..400u64 {
            let k = format!("{{t}}x{i}");
            match db.set(0, k.as_bytes(), &value(i, 2049 + (i as usize % 7) * 3000), None) {
                Ok(t) => {
                    db.wait(k.as_bytes(), t, TicketState::Written);
                    stored += 1;
                }
                Err(StoreError::OutOfSpace) => {
                    for _ in 0..20 {
                        db.advance(100);
                        db.eng.pump();
                    }
                }
                Err(e) => panic!("{e:?}"),
            }
        }
        assert!(stored >= 380, "cache {cache_mb} MiB: only {stored}/400 extent writes after FLUSHDB of a full device ({full}/{total} AGs used before, {after} after)");
        db.quiesce();
    }
}

/// Deleting every key of a full partition gives its AGs back to the pool (§11.5, §11.7): a
/// page whose records are all deleted is fresh once its empty image is durable, and an AG of
/// fresh pages is released, so extents can use the space again.
#[test]
fn deletes_give_ags_back() {
    use kv_storage::keyspace::StoreError;
    use kv_storage::layout::AG_PAGES;
    let pages = 40 * u64::from(AG_PAGES) + u64::from(AG_PAGES);
    let mut d = RamDev::new(pages, 4096, 9);
    format_devices(&mut [&mut d as &mut dyn BlockDevice], 16, false, 0, 9).unwrap();
    let mut eng = LocalEngine::open(vec![d], &cfg()).unwrap();
    eng.recover().unwrap();
    let mut db = Db::new(eng, T0);
    let base = db.eng.meta().ag_usage().0;
    let mut refused = 0;
    let mut keys = Vec::new();
    while refused < 200 {
        let k = format!("{{t}}s{}", keys.len() + refused);
        match db.set(0, k.as_bytes(), &value(keys.len() as u64, 100), None) {
            Ok(t) => {
                db.wait(k.as_bytes(), t, TicketState::Written);
                keys.push(k);
            }
            Err(StoreError::OutOfSpace) => refused += 1,
            Err(e) => panic!("{e:?}"),
        }
    }
    let (full, total) = db.eng.meta().ag_usage();
    for k in &keys {
        let (hit, t) = db.del(0, k.as_bytes()).unwrap();
        assert!(hit);
        db.wait(k.as_bytes(), t, TicketState::Written);
    }
    for _ in 0..600 {
        db.advance(100);
        db.eng.pump();
    }
    let (after, _) = db.eng.meta().ag_usage();
    let mut stored = 0;
    for i in 0..400u64 {
        let k = format!("{{t}}x{i}");
        match db.set(0, k.as_bytes(), &value(i, 1 << 20), None) {
            Ok(t) => {
                db.wait(k.as_bytes(), t, TicketState::Written);
                stored += 1;
            }
            Err(StoreError::OutOfSpace) => break,
            Err(e) => panic!("{e:?}"),
        }
    }
    let (end, _) = db.eng.meta().ag_usage();
    // every partition keeps a spare SMALL AG; the deleted keys' AGs are back in the pool
    let parts = u64::from(db.eng.partitions());
    assert!(after <= base + parts + 2, "{} keys deleted: {after} of {total} AGs still used ({full} when full, {base} before)", keys.len());
    assert!(stored >= 300, "only {stored} MiB values after the deletes ({after}/{total} AGs used before them, {end} at the end)");
    db.quiesce();
}

/// FLUSHDB more often than the paced reclaim walk takes frees the flushed space (§16.5): every
/// FLUSHDB restarted the walk over all of the partition's pages, with the extents queued
/// behind them, so a partition with many small-record pages never freed a flushed extent and
/// writes got `-OOM` on a nearly empty device. A FLUSHDB of the only db with keys in the
/// partition now frees space like FLUSHALL; with keys in another db the walk keeps its
/// progress across FLUSHDBs and reads extent headers first.
#[test]
fn frequent_flushdb_frees_extents() {
    use kv_storage::keyspace::StoreError;
    use kv_storage::layout::AG_PAGES;
    for other_db in [false, true] {
        let pages = 40 * u64::from(AG_PAGES) + u64::from(AG_PAGES);
        let mut d = RamDev::new(pages, 4096, 9);
        format_devices(&mut [&mut d as &mut dyn BlockDevice], 16, false, 0, 9).unwrap();
        let mut eng = LocalEngine::open(vec![d], &cfg()).unwrap();
        eng.recover().unwrap();
        let mut db = Db::new(eng, T0);
        if other_db {
            let t = db.set(1, b"{t}other", b"v", None).unwrap();
            db.wait(b"{t}other", t, TicketState::Written);
        }
        // small records on far more pages than the cache holds: the walk reads them all
        for i in 0..300_000u64 {
            let k = format!("{{t}}s{i}");
            let t = db.set(0, k.as_bytes(), &value(i, 100), None).unwrap();
            db.wait(k.as_bytes(), t, TicketState::Written);
        }
        let mut refused = 0;
        let mut written = 0u64;
        for cycle in 0..80u64 {
            for i in 0..8u64 {
                let k = format!("{{t}}x{cycle}_{i}");
                match db.set(0, k.as_bytes(), &value(i, 1 << 20), None) {
                    Ok(t) => {
                        db.wait(k.as_bytes(), t, TicketState::Written);
                        written += 1;
                    }
                    Err(StoreError::OutOfSpace) => refused += 1,
                    Err(e) => panic!("{e:?}"),
                }
            }
            for (p, t) in db.flushdb(Some(0)) {
                db.wait_in(p, t, TicketState::Durable);
            }
            for _ in 0..10 {
                db.advance(100);
                db.eng.pump();
            }
        }
        let (used, total) = db.eng.meta().ag_usage();
        assert!(refused == 0, "other db {other_db}: {refused} of {} 1 MiB writes refused ({written} MiB written, {used}/{total} AGs used)", refused + written);
        if other_db {
            let (_, v) = db.get(1, b"{t}other").unwrap().expect("the other db keeps its key");
            assert_eq!(v, b"v");
        }
        db.quiesce();
    }
}

/// A FLUSHALL (or FLUSHDB of the only db with keys) issued while writes of every kind are
/// staged or in flight gives every old AG back once that work is done, also while new writes
/// keep coming (each AG on its own, not when the whole partition is idle).
#[test]
fn flush_under_load_gives_old_ags_back() {
    use kv_storage::keyspace::StoreError;
    use kv_storage::layout::AG_PAGES;
    for wide in [None, Some(0u8)] {
        let pages = 40 * u64::from(AG_PAGES) + u64::from(AG_PAGES);
        let mut d = RamDev::new(pages, 4096, 9);
        format_devices(&mut [&mut d as &mut dyn BlockDevice], 16, false, 0, 9).unwrap();
        let mut eng = LocalEngine::open(vec![d], &cfg()).unwrap();
        eng.recover().unwrap();
        let mut db = Db::new(eng, T0);
        let mut refused = 0;
        let mut n = 0u64;
        for round in 0..40u64 {
            // writes of every size, not waited for, then a flush right behind them
            for i in 0..60u64 {
                n += 1;
                let len = [40usize, 900, 3000, 70_000, 1 << 20][(i % 5) as usize];
                let k = format!("{{t}}k{}", n % 500);
                match db.set(0, k.as_bytes(), &value(n, len), None) {
                    Ok(_) => {}
                    Err(StoreError::OutOfSpace) => refused += 1,
                    Err(e) => panic!("{e:?}"),
                }
                if i % 7 == 0 {
                    db.eng.pump();
                }
            }
            if round % 4 == 3 {
                let _ = db.flushdb(wide);
            }
            for _ in 0..3 {
                db.advance(50);
                db.eng.pump();
            }
        }
        for _ in 0..200 {
            db.advance(50);
            db.eng.pump();
        }
        let (used, total) = db.eng.meta().ag_usage();
        assert!(refused == 0 && used <= 24, "flush {wide:?}: {refused} writes refused, {used}/{total} AGs used at the end");
        db.quiesce();
    }
}

/// A multi-key delete works in every partition of a device filled with extents only. Its
/// deletes stage txn markers, small records (§14.6 step 2); a partition whose keys all live in
/// extents owns no SMALL AG, and once extent AGs had taken every free AG only the first two
/// partitions got one: every other partition's multi-key DEL answered `-OOM`, so a full device
/// could only be emptied one key per command. Such a delete now runs as independent deletes,
/// which need no space (keeping a free AG for each partition owning no SMALL AG instead cost a
/// third of a small device's capacity).
#[test]
fn multi_key_deletes_work_in_every_partition_of_a_device_full_of_extents() {
    use kv_storage::keyspace::StoreError;
    use kv_storage::layout::AG_PAGES;
    let pages = 40 * u64::from(AG_PAGES) + u64::from(AG_PAGES);
    let mut d = RamDev::new(pages, 4096, 11);
    format_devices(&mut [&mut d as &mut dyn BlockDevice], 16, false, 0, 11).unwrap();
    let mut eng = LocalEngine::open(vec![d], &cfg()).unwrap();
    eng.recover().unwrap();
    let mut db = Db::new(eng, T0);
    let parts = db.eng.partitions();
    let mut by_part: Vec<Vec<String>> = vec![Vec::new(); usize::from(parts)];
    let mut refused = 0;
    let mut i = 0u64;
    while refused < 400 {
        let k = format!("k{i}");
        i += 1;
        match db.set(0, k.as_bytes(), &value(i, 2100 + (i as usize * 7919) % 30_000), None) {
            Ok(t) => {
                db.wait(k.as_bytes(), t, TicketState::Written);
                by_part[usize::from(db.eng.partition_of(k.as_bytes()))].push(k);
            }
            Err(StoreError::OutOfSpace) => refused += 1,
            Err(e) => panic!("{e:?}"),
        }
    }
    let (used, total) = db.eng.meta().ag_usage();
    for (p, keys) in by_part.iter().enumerate() {
        assert!(keys.len() >= 2, "partition {p} stored {} keys", keys.len());
        let ks: Vec<&[u8]> = keys[..2].iter().map(|k| k.as_bytes()).collect();
        let (n, out) = db
            .eng
            .run(p as u16, keyed(0, &ks, kv_common::Access::Write), &mut |s| {
                s.select(0);
                let mut n = 0;
                for k in &ks {
                    n += u32::from(s.delete(k)?);
                }
                Ok(n)
            })
            .unwrap_or_else(|e| panic!("multi-key delete in partition {p} of a device full of extents ({used}/{total} AGs used): {e:?}"));
        assert_eq!(n, 2);
        db.wait_in(p as u16, out.ticket, TicketState::Written);
        for k in &ks {
            assert!(db.get(0, k).unwrap().is_none(), "partition {p}: a deleted key is back");
        }
    }
    let split: u64 = (0..parts).map(|p| db.eng.partition(p).counters().txn_deletes_split).sum();
    assert!(split > 0, "some partition had no space for txn markers");
    db.quiesce();
}

/// Values just over the extent maximum (8 MiB: a chunkmap, a 2048-page chunk, half an AG,
/// and a one-page chunk, §14.5) fill the device two to an AG. The one-page pieces were
/// carved at keyed positions anywhere in an AG, leaving no 2048-page run in it, so most
/// such values took an AG of their own and `-OOM` came with much of the device free.
#[test]
fn values_just_over_the_extent_maximum_fill_most_of_the_device() {
    use kv_storage::keyspace::StoreError;
    use kv_storage::layout::AG_PAGES;
    let pages = 40 * u64::from(AG_PAGES) + u64::from(AG_PAGES);
    let mut d = RamDev::new(pages, 4096, 13);
    format_devices(&mut [&mut d as &mut dyn BlockDevice], 16, false, 0, 13).unwrap();
    let mut eng = LocalEngine::open(vec![d], &cfg()).unwrap();
    eng.recover().unwrap();
    let mut db = Db::new(eng, T0);
    let (_, total) = db.eng.meta().ag_usage();
    let v = value(1, 8 << 20);
    let mut stored = 0u64;
    let mut refused = 0;
    for i in 0..400u64 {
        // four partitions write in turn
        let k = format!("{{b{}}}{i}", i % 4);
        match db.set(0, k.as_bytes(), &v, None) {
            Ok(t) => {
                db.wait(k.as_bytes(), t, TicketState::Written);
                stored += 1;
            }
            Err(StoreError::OutOfSpace) => {
                refused += 1;
                if refused > 20 {
                    break;
                }
            }
            Err(e) => panic!("{e:?}"),
        }
    }
    let (used, _) = db.eng.meta().ag_usage();
    // at most two per AG; each partition's first AG holds one and the small pieces, and
    // spares and the fill limit hold some back: more than one per AG of the device
    assert!(stored > total, "{stored} values of 8 MiB on {total} AGs ({used} used)");
    db.quiesce();
}

/// A write sent right after a FLUSHDB of one of two databases is answered waits for the space
/// the reclaim walk gives back instead of getting `-OOM`: the walk starts when the cutoff is
/// durable, which is also when the FLUSHDB is answered, so on a device the flush had just
/// emptied the first writes after the reply found every AG still held and were refused.
#[test]
fn writes_right_after_flushdb_wait_for_the_reclaim() {
    use kv_storage::keyspace::StoreError;
    use kv_storage::layout::AG_PAGES;
    let pages = 20 * u64::from(AG_PAGES) + u64::from(AG_PAGES);
    let mut d = RamDev::new(pages, 4096, 23);
    format_devices(&mut [&mut d as &mut dyn BlockDevice], 16, false, 0, 23).unwrap();
    let mut c = cfg();
    c.cache_bytes = 8 << 20;
    let mut eng = LocalEngine::open(vec![d], &c).unwrap();
    eng.recover().unwrap();
    let mut db = Db::new(eng, T0);
    // db 1 keeps a key, so the FLUSHDB of db 0 needs the walk (§16.5)
    let t = db.set(1, b"{t}keep", b"v", None).unwrap();
    db.wait(b"{t}keep", t, TicketState::Written);
    // the device full of 1 MiB values of db 0
    let mut stored = 0u64;
    let mut refused = 0;
    while refused < 5 {
        let k = format!("{{t}}f{stored}");
        match db.set(0, k.as_bytes(), &value(stored, 1 << 20), None) {
            Ok(t) => {
                db.wait(k.as_bytes(), t, TicketState::Written);
                stored += 1;
            }
            Err(StoreError::OutOfSpace) => refused += 1,
            Err(e) => panic!("{e:?}"),
        }
    }
    assert!(stored > 100, "only {stored} MiB values fit");
    for (q, t) in db.flushdb(Some(0)) {
        db.wait_in(q, t, TicketState::Durable);
    }
    // at once, with no engine time passing: every write succeeds
    for i in 0..stored / 2 {
        let k = format!("{{t}}x{i}");
        match db.set(0, k.as_bytes(), &value(i, 1 << 20), None) {
            Ok(t) => {
                db.wait(k.as_bytes(), t, TicketState::Written);
            }
            Err(e) => panic!("write {i} of {} after FLUSHDB of {stored} MiB values: {e:?}", stored / 2),
        }
    }
    db.quiesce();
}

/// After a FLUSHDB of one of two databases, the reclaim walk runs as fast as the device reads
/// while the partition's writes are refused space. It was paced (4000 reads/s shared by every
/// partition) whatever happened, so under frequent FLUSHDBs it fell behind and about a fifth
/// of the writes of a busy 64-partition device holding a few hundred MB got `-OOM`.
#[test]
fn reclaim_walk_speeds_up_while_writes_are_refused_space() {
    use kv_storage::keyspace::StoreError;
    use kv_storage::layout::AG_PAGES;
    let pages = 20 * u64::from(AG_PAGES) + u64::from(AG_PAGES);
    let mut d = RamDev::new(pages, 4096, 17);
    format_devices(&mut [&mut d as &mut dyn BlockDevice], 16, false, 0, 17).unwrap();
    let mut c = cfg();
    c.cache_bytes = 8 << 20;
    let mut eng = LocalEngine::open(vec![d], &c).unwrap();
    eng.recover().unwrap();
    let mut db = Db::new(eng, T0);
    // db 0 keeps a key, so the FLUSHDB of db 1 needs the walk (§16.5)
    let t = db.set(0, b"{t}keep", b"v", None).unwrap();
    db.wait(b"{t}keep", t, TicketState::Written);
    let p = db.eng.partition_of(b"{t}keep");
    // the device full of small records of db 1, two to a page
    let mut refused = 0;
    let mut n = 0u64;
    while refused < 50 {
        let k = format!("{{t}}f{n}");
        n += 1;
        match db.set(1, k.as_bytes(), &value(n, 1900), None) {
            Ok(t) => {
                db.wait(k.as_bytes(), t, TicketState::Written);
            }
            Err(StoreError::OutOfSpace) => refused += 1,
            Err(e) => panic!("{e:?}"),
        }
    }
    for (q, t) in db.flushdb(Some(1)) {
        db.wait_in(q, t, TicketState::Durable);
    }
    let before = db.eng.partition(p).counters().flush_reclaim_pending_pages;
    // two seconds of engine time with an extent write refused (no AG is free) now and then
    for i in 0..1000u64 {
        if i % 50 == 0 {
            let k = format!("{{t}}x{i}");
            if let Ok(t) = db.set(0, k.as_bytes(), &value(i, 1 << 20), None) {
                db.wait(k.as_bytes(), t, TicketState::Written);
            }
        }
        db.advance(2);
        db.eng.pump();
    }
    let after = db.eng.partition(p).counters().flush_reclaim_pending_pages;
    // paced, the walk takes 250 steps a second here: 500 in two seconds
    assert!(before > 5000 && before - after >= 4000, "{n} records flushed: the walk went from {before} to {after} pending pages in 2 s");
    db.quiesce();
}
