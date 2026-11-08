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

/// The coordinator's commit of the txn failed (a meta write error): the part can be neither
/// committed nor undone. Before, nothing told the partition, so a later write of the part's
/// key and a barrier waited for the commit forever; now the partition turns read-only and
/// both are answered with the error.
#[test]
fn a_failed_commit_fails_what_waits_for_the_part() {
    for (i, size) in [100usize, 20_000].into_iter().enumerate() {
        let seed = 950 + i as u64;
        let mut db = fresh(seed);
        let (a, _) = two_keys(&db);
        let t = db.set(0, &a, b"a-old", None).unwrap();
        db.wait(&a, t, TicketState::Durable);
        let txn = db.eng.meta().txn_ids().next();
        apply_part(&mut db, &a, txn, Part::Put(size), &value(seed, size)).unwrap();
        for _ in 0..3 {
            db.eng.pump();
        }
        let later = db.set(0, &a, b"later", None).unwrap();
        for _ in 0..3 {
            db.eng.pump();
        }
        let pa = db.eng.partition_of(&a);
        db.eng.txn_commit_failed(pa, txn, -libc::EIO);
        let st = db.wait(&a, later, TicketState::Durable);
        assert!(matches!(st, TicketState::Failed(_)), "size {size}: the later write ends with the error, got {st:?}");
        assert!(db.set(0, &a, b"refused", None).is_err(), "size {size}: the partition is read-only");
    }
}

/// A part's version that its page's build could not place moves to another page (§14.3)
/// before the txn is aborted. The abort used to undo the version where it was staged: it found
/// no index entry there, put the old one back beside the entry naming the moved version and
/// dropped the tracking the move gave the old copy. The key's next write then landed beside
/// that untracked old copy while the moved, dead version was tracked as a source that no newer
/// copy would ever release: a third copy (E14) that waited forever, its txn with it.
#[cfg(feature = "sim")]
#[test]
fn an_aborted_part_whose_version_moved_leaves_its_key_as_it_was() {
    for pumps in [0usize, 1, 2, 3, 6, 400] {
        for old_len in [60usize, 900] {
            let seed = 1300 + pumps as u64 * 10 + old_len as u64;
            let ctx = format!("{pumps} pumps, old {old_len} bytes");
            let mut db = fresh(seed);
            let (a, _) = two_keys(&db);
            let pa = db.eng.partition_of(&a);
            let old_v = value(seed, old_len);
            let t = db.set(0, &a, &old_v, None).unwrap();
            db.wait(&a, t, TicketState::Durable);
            let page = db.eng.locate(0, &a).unwrap().page;
            let txn = db.eng.meta().txn_ids().next();
            apply_part(&mut db, &a, txn, Part::Put(100), &value(seed + 1, 100)).unwrap();
            assert_eq!(db.eng.locate(0, &a).unwrap().page, page, "{ctx}: the part's version is staged in the key's page");
            assert_eq!(db.eng.sim_move_staged(pa, page), 1, "{ctx}");
            assert_ne!(db.eng.locate(0, &a).unwrap().page, page, "{ctx}: the version moved");
            for _ in 0..pumps {
                db.eng.pump();
            }
            let t = db.eng.abort_txn(pa, txn);
            assert_eq!(db.wait_in(pa, t, TicketState::Durable), TicketState::Durable, "{ctx}: abort ticket");
            assert_eq!(db.get(0, &a).unwrap().map(|x| x.1), Some(old_v.clone()), "{ctx}: the key is back to its old version");
            assert_eq!(db.eng.locate(0, &a).unwrap().page, page, "{ctx}: the index names the old copy again");
            assert_eq!(db.eng.partition(pa).index().keys(0), 1, "{ctx}: one index entry for the key");
            assert_eq!(db.eng.partition(pa).stale_copies(), 0, "{ctx}: no tracked copy is left once the abort is durable");
            // later writes of the key complete, also cross-partition ones
            let t = db.set(0, &a, &value(seed + 2, 700), None).unwrap();
            assert_eq!(db.wait(&a, t, TicketState::Durable), TicketState::Durable, "{ctx}: a later write");
            let txn2 = db.eng.meta().txn_ids().next();
            let t = apply_part(&mut db, &a, txn2, Part::Put(300), &value(seed + 3, 300)).unwrap();
            for _ in 0..50 {
                db.eng.pump();
            }
            assert!(t.is_none() || db.eng.check_progress().is_ok(), "{ctx}: {:?}", db.eng.check_progress());
            let t = db.eng.abort_txn(pa, txn2);
            assert_eq!(db.wait_in(pa, t, TicketState::Durable), TicketState::Durable, "{ctx}: second abort");
            let (_, t) = db.del(0, &a).unwrap();
            assert_eq!(db.wait(&a, t, TicketState::Durable), TicketState::Durable, "{ctx}: a delete");
            db.quiesce();
            assert!(db.eng.check_progress().is_ok(), "{ctx}: {:?}", db.eng.check_progress());
            let mut devs = db.eng.into_devices();
            devs[0].crash();
            let mut eng = LocalEngine::open(devs, &cfg()).unwrap();
            eng.recover().unwrap();
            let mut db = Db::new(eng, T0);
            assert_eq!(db.get(0, &a).unwrap(), None, "{ctx}: after a crash the delete holds");
            db.quiesce();
        }
    }
}

/// A committed part whose version went to another page than the key's old copy (the old page
/// had no room): the old copy is protected until the version is durable and committed, then
/// zeroed. It stayed protected for good.
#[test]
fn a_committed_part_on_another_page_releases_the_old_copy() {
    for (old_len, fill) in [(895usize, true), (895, false), (60, true)] {
        let seed = 4400 + old_len as u64 + u64::from(fill);
        let ctx = format!("old {old_len} bytes, fill {fill}");
        let mut db = fresh(seed);
        let (a, _) = two_keys(&db);
        let pa = db.eng.partition_of(&a);
        let t = db.set(0, &a, &value(seed, old_len), None).unwrap();
        db.wait(&a, t, TicketState::Durable);
        let page = db.eng.locate(0, &a).unwrap().page;
        if fill {
            // other keys of the partition fill the old copy's page
            for i in 0..200 {
                let k = format!("{{xa}}f{i}").into_bytes();
                let t = db.set(0, &k, &value(seed + i, 300), None).unwrap();
                db.wait(&k, t, TicketState::Durable);
            }
        }
        let txn = db.eng.meta().txn_ids().next();
        let new_v = value(seed + 1, 1160);
        let t = apply_part(&mut db, &a, txn, Part::Put(1160), &new_v).unwrap();
        assert_eq!(db.wait_in(pa, t, TicketState::Durable), TicketState::Durable, "{ctx}: part");
        let moved = db.eng.locate(0, &a).unwrap().page != page;
        db.eng.meta_client().request(pa, 0, kv_storage::metastore::MetaReq::CommitTxn { txn });
        db.eng.pump();
        assert_eq!(db.get(0, &a).unwrap().map(|x| x.1), Some(new_v.clone()), "{ctx} (moved {moved})");
        db.quiesce();
        let t = db.set(0, &a, &value(seed + 2, 700), None).unwrap();
        assert_eq!(db.wait(&a, t, TicketState::Durable), TicketState::Durable, "{ctx}: a later write");
        db.quiesce();
    }
}
