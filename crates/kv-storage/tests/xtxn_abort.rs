//! Abort of a cross-partition txn part (DESIGN §14.6 step 6).
//!
//! kvd's XTxn coordinator stages one part per partition (`ReqKind::Txn`). When a part fails
//! (e.g. `-OOM`) the other parts must be undone. Without an abort the applied parts stayed open
//! forever: their keys showed the uncommitted values, later requests on those keys never
//! completed (the pages holding the txn records stayed protected), and `resolved_w` could not
//! pass the txn id. These tests apply a part, abort it at every point of its write's progress,
//! and check that the key is back to its old version (also after a crash), that the abort's
//! ticket completes, that later writes of the key complete, and that a part applied after its
//! txn's abort is refused.

mod common;

use common::*;
use kv_common::Access;
use kv_index::VType;
use kv_storage::engine::{LocalEngine, format_devices};
use kv_storage::io::BlockDevice;
use kv_storage::partition::{Origin, ReqKind, Request};
use kv_storage::ticket::TicketState;

const T0: u64 = 1_800_000_000_000;

fn fresh(seed: u64) -> Db<RamDev> {
    let mut d = RamDev::new(128 * 4096, 512, seed);
    d.set_max_completions(1 + (seed % 2) as usize);
    format_devices(&mut [&mut d as &mut dyn BlockDevice], 16, false, 0, seed).unwrap();
    let mut eng = LocalEngine::open(vec![d], &cfg()).unwrap();
    eng.recover().unwrap();
    Db::new(eng, T0)
}

/// Two keys of different partitions.
fn two_keys(db: &Db<RamDev>) -> (Vec<u8>, Vec<u8>) {
    let a = b"{xa}key".to_vec();
    let pa = db.eng.partition_of(&a);
    for i in 0..1000 {
        let b = format!("{{xb{i}}}key").into_bytes();
        if db.eng.partition_of(&b) != pa {
            return (a, b);
        }
    }
    unreachable!()
}

#[derive(Clone, Copy, Debug)]
enum Part {
    Put(usize),
    Delete,
}

fn apply_part(db: &mut Db<RamDev>, key: &[u8], txn: u64, op: Part, v: &[u8]) -> Result<Option<kv_storage::ticket::TicketId>, kv_storage::keyspace::StoreError> {
    let p = db.eng.partition_of(key);
    let req = Request { db: 0, keys: [(Box::<[u8]>::from(key), Access::Write)].into_iter().collect(), kind: ReqKind::Txn { txn_id: txn }, origin: Origin::Client };
    let (_, out) = db.eng.run(p, req, &mut |ks| {
        ks.select(0);
        match op {
            Part::Put(_) => ks.put(key, v, VType::String, None),
            Part::Delete => ks.delete(key).map(|_| ()),
        }
    })?;
    Ok(out.ticket)
}

#[test]
fn an_aborted_part_leaves_its_keys_as_they_were() {
    let shapes = [Part::Put(100), Part::Put(20_000), Part::Put(1_500_000), Part::Delete];
    let olds = [None, Some(60usize), Some(9_000)];
    let mut runs = 0;
    for (si, &op) in shapes.iter().enumerate() {
        for (oi, &old) in olds.iter().enumerate() {
            if matches!(op, Part::Delete) && old.is_none() {
                continue;
            }
            for pumps in [0usize, 1, 2, 3, 5, 8, 400] {
                let seed = (si * 100 + oi * 10) as u64 + pumps as u64;
                runs += 1;
                let mut db = fresh(seed);
                let (a, b) = two_keys(&db);
                let old_v = old.map(|n| value(seed, n));
                if let Some(v) = &old_v {
                    let t = db.set(0, &a, v, None).unwrap();
                    db.wait(&a, t, TicketState::Durable);
                }
                let t = db.set(0, &b, b"b-old", None).unwrap();
                db.wait(&b, t, TicketState::Durable);
                let txn = db.eng.meta().txn_ids().next();
                assert_ne!(txn, 0);
                let new_v = match op {
                    Part::Put(n) => value(seed + 7, n),
                    Part::Delete => Vec::new(),
                };
                let _part_ticket = apply_part(&mut db, &a, txn, op, &new_v).expect("part applies");
                for _ in 0..pumps {
                    db.eng.pump();
                }
                let pa = db.eng.partition_of(&a);
                let pb = db.eng.partition_of(&b);
                // the other part was never applied: its abort only refuses a late Apply
                assert!(db.eng.abort_txn(pb, txn).is_none());
                let t = db.eng.abort_txn(pa, txn);
                let ctx = format!("{op:?} old={old:?} after {pumps} pumps");
                assert!(t.is_some(), "{ctx}: a part that staged something has an abort ticket");
                assert_eq!(db.wait_in(pa, t, TicketState::Durable), TicketState::Durable, "{ctx}: abort ticket");
                let got = db.get(0, &a).unwrap().map(|x| x.1);
                assert_eq!(got, old_v, "{ctx}: the key is back to its old version");
                let late = apply_part(&mut db, &b, txn, Part::Put(10), b"b-new");
                assert!(late.is_err(), "{ctx}: a part of an aborted txn is refused");
                assert_eq!(db.get(0, &b).unwrap().map(|x| x.1).as_deref(), Some(&b"b-old"[..]), "{ctx}");
                db.quiesce();
                // later writes and deletes of the key complete
                let t = db.set(0, &a, b"after", None).unwrap();
                db.wait(&a, t, TicketState::Durable);
                let (_, t) = db.del(0, &a).unwrap();
                db.wait(&a, t, TicketState::Durable);
                if let Some(v) = &old_v {
                    let t = db.set(0, &a, v, None).unwrap();
                    db.wait(&a, t, TicketState::Durable);
                }
                db.quiesce();
                // a crash right after: the aborted version never comes back
                let mut devs = db.eng.into_devices();
                devs[0].crash();
                let mut eng = LocalEngine::open(devs, &cfg()).unwrap();
                eng.recover().unwrap();
                let mut db = Db::new(eng, T0);
                assert_eq!(db.get(0, &a).unwrap().map(|x| x.1), old_v, "{ctx}: after a crash");
                db.quiesce();
            }
        }
    }
    assert!(runs > 50);
}

/// The abort comes while the part's write is in flight and the process is killed before the
/// abort completed: recovery finds the txn uncommitted and the key at its old version.
#[test]
fn a_crash_during_an_abort_keeps_the_old_version() {
    for pumps in 0..12usize {
        for size in [100usize, 20_000] {
            let seed = 500 + pumps as u64 * 3 + size as u64 % 7;
            let mut db = fresh(seed);
            let (a, _) = two_keys(&db);
            let t = db.set(0, &a, b"a-old", None).unwrap();
            db.wait(&a, t, TicketState::Durable);
            let txn = db.eng.meta().txn_ids().next();
            apply_part(&mut db, &a, txn, Part::Put(size), &value(seed, size)).unwrap();
            for _ in 0..pumps {
                db.eng.pump();
            }
            let pa = db.eng.partition_of(&a);
            db.eng.abort_txn(pa, txn);
            for _ in 0..pumps {
                db.eng.pump();
            }
            let mut devs = db.eng.into_devices();
            devs[0].crash();
            let mut eng = LocalEngine::open(devs, &cfg()).unwrap();
            eng.recover().unwrap();
            let mut db = Db::new(eng, T0);
            assert_eq!(db.get(0, &a).unwrap().map(|x| x.1).as_deref(), Some(&b"a-old"[..]), "size {size}, {pumps} pumps");
            db.quiesce();
        }
    }
}

/// A part never cancels a write of its key that is still staged (its request waits for the
/// tick's build): after the abort the key holds that write, whose ticket completes.
#[test]
fn an_aborted_part_never_takes_an_earlier_staged_write_with_it() {
    for (i, size) in [100usize, 20_000, 1_200_000].into_iter().enumerate() {
        let seed = 900 + i as u64;
        let mut db = fresh(seed);
        let (a, _) = two_keys(&db);
        let t = db.set(0, &a, b"a-old", None).unwrap();
        db.wait(&a, t, TicketState::Durable);
        // executed, not built yet: `run` returns right after exec
        let staged = value(seed, size);
        let t_staged = db.set(0, &a, &staged, None).unwrap();
        let txn = db.eng.meta().txn_ids().next();
        apply_part(&mut db, &a, txn, Part::Put(50), b"txn-version").unwrap();
        let pa = db.eng.partition_of(&a);
        let t = db.eng.abort_txn(pa, txn);
        db.wait_in(pa, t, TicketState::Durable);
        assert_eq!(db.wait(&a, t_staged, TicketState::Durable), TicketState::Durable, "size {size}");
        assert_eq!(db.get(0, &a).unwrap().map(|x| x.1), Some(staged.clone()), "size {size}");
        db.quiesce();
        let mut devs = db.eng.into_devices();
        devs[0].crash();
        let mut eng = LocalEngine::open(devs, &cfg()).unwrap();
        eng.recover().unwrap();
        let mut db = Db::new(eng, T0);
        assert_eq!(db.get(0, &a).unwrap().map(|x| x.1), Some(staged), "size {size} after a crash");
        db.quiesce();
    }
}
