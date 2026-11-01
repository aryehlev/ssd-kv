//! Request ordering (LC-15: per-key execution order equals arrival order; barriers wait for
//! everything older) and crash-atomic multi-key writes (I4, §14.6, §17.5).

mod common;

use common::*;
use kv_common::Access;
use kv_index::VType;
use kv_storage::engine::{LocalEngine, format_devices};
use kv_storage::io::BlockDevice;
use kv_storage::partition::{ExecStatus, Origin, ReqKind, Request};
use kv_storage::ticket::TicketState;
use smallvec::SmallVec;

const T0: u64 = 1_800_000_000_000;

fn fresh(seed: u64) -> Db<RamDev> {
    let mut d = RamDev::new(64 * 4096, 512, seed);
    format_devices(&mut [&mut d as &mut dyn BlockDevice], 16, false, 0, seed).unwrap();
    let mut eng = LocalEngine::open(vec![d], &cfg()).unwrap();
    eng.recover().unwrap();
    Db::new(eng, T0)
}

#[test]
fn same_key_requests_run_in_arrival_order() {
    let mut db = fresh(1);
    let k: &[u8] = b"order";
    let p = db.eng.partition_of(k);
    let ids: Vec<_> = (0..4).map(|_| db.eng.submit(p, keyed(0, &[k], Access::Write))).collect();
    let other = db.eng.submit(p, keyed(0, &[b"{order}x".as_slice()], Access::Write));
    let mut order = Vec::new();
    let mut ready = Vec::new();
    for _ in 0..1000 {
        ready.clear();
        db.eng.ready(&mut ready);
        // at most one request of the key is ready at a time
        assert!(ready.iter().filter(|(_, id)| ids.contains(id)).count() <= 1);
        for &(rp, id) in &ready {
            let n = order.iter().filter(|x| ids.contains(x)).count() as u8;
            let key: &[u8] = if id == other { b"{order}x" } else { k };
            match db.eng.exec(rp, id, &mut |ks| ks.put(key, &[n], VType::String, None)) {
                ExecStatus::Done(..) => order.push(id),
                ExecStatus::Requeued => {}
                ExecStatus::Failed(e) => panic!("{e}"),
            }
        }
        if order.len() == 5 {
            break;
        }
        db.eng.pump();
    }
    let keyed_order: Vec<_> = order.iter().copied().filter(|id| ids.contains(id)).collect();
    assert_eq!(keyed_order, ids, "per-key FIFO (LC-15)");
    assert!(order.contains(&other));
    assert_eq!(db.get(0, k).unwrap().unwrap().1, vec![3u8], "the last arrival wins");
}

#[test]
fn barrier_waits_for_older_requests_and_holds_newer_ones() {
    let mut db = fresh(2);
    let k: &[u8] = b"b1";
    let p = db.eng.partition_of(k);
    let older = db.eng.submit(p, keyed(0, &[k], Access::Write));
    let barrier = db.eng.submit(p, Request { db: 0, keys: SmallVec::new(), kind: ReqKind::Barrier, origin: Origin::Client });
    let newer = db.eng.submit(p, keyed(0, &[b"b2".as_slice()], Access::Write));
    let mut ready = Vec::new();
    db.eng.ready(&mut ready);
    let ids: Vec<_> = ready.iter().map(|r| r.1).collect();
    assert_eq!(ids, vec![older], "the barrier and requests behind it wait");
    // the first write after startup may wait for its seq reservation and AG: pump until it ran
    let mut done = false;
    for _ in 0..10_000 {
        match db.eng.exec(p, older, &mut |ks| ks.put(k, b"x", VType::String, None)) {
            ExecStatus::Done(..) => {
                done = true;
                break;
            }
            ExecStatus::Requeued => {
                db.eng.pump();
                ready.clear();
                db.eng.ready(&mut ready);
                assert!(ready.iter().all(|r| r.1 == older), "nothing overtakes the barrier");
            }
            ExecStatus::Failed(e) => panic!("{e}"),
        }
    }
    assert!(done);
    ready.clear();
    db.eng.ready(&mut ready);
    assert_eq!(ready.iter().map(|r| r.1).collect::<Vec<_>>(), vec![barrier]);
    let ExecStatus::Done((), out) = db.eng.exec(p, barrier, &mut |_| Ok(())) else { panic!("barrier") };
    // the barrier's ticket is durable once everything before it is
    assert_eq!(db.eng.wait_ticket(p, out.ticket.unwrap(), TicketState::Durable), TicketState::Durable);
    ready.clear();
    db.eng.ready(&mut ready);
    assert_eq!(ready.iter().map(|r| r.1).collect::<Vec<_>>(), vec![newer]);
}

/// MSET of three keys of one hash tag, repeated with the iteration number as every value;
/// after any crash all three keys carry the same iteration (I4), no older than the last
/// acknowledged one.
fn txn_rounds<D: BlockDevice>(mut devs: Vec<D>, crash: &mut dyn FnMut(&mut D, u64), seeds: u64, ack: TicketState) {
    let keys: [&[u8]; 3] = [b"{t}a", b"{t}b", b"{t}c"];
    let mut acked = 0u64;
    let mut iter = 0u64;
    for round in 0..seeds {
        let mut eng = LocalEngine::open(devs, &cfg()).unwrap();
        eng.recover().unwrap();
        let mut db = Db::new(eng, T0);
        let got: Vec<Option<Vec<u8>>> = keys.iter().map(|k| db.get(0, k).unwrap().map(|(_, v)| v)).collect();
        if let Some(Some(v0)) = got.first() {
            assert!(got.iter().all(|g| g.as_deref() == Some(v0.as_slice())), "round {round}: a multi-key write was applied partly");
            let n = u64::from_le_bytes(v0[..8].try_into().unwrap());
            assert!(n >= acked, "round {round}: acknowledged write {acked} lost (found {n})");
        } else {
            assert!(got.iter().all(Option::is_none), "round {round}: a multi-key write was applied partly");
            assert_eq!(acked, 0, "round {round}: acknowledged write lost");
        }
        let n = 3 + round % 5;
        for j in 0..n {
            iter += 1;
            let v: Vec<u8> = iter.to_le_bytes().iter().cycle().take(64 + (iter as usize % 3) * 2500).copied().collect();
            let kvs: Vec<(&[u8], &[u8])> = keys.iter().map(|k| (*k, v.as_slice())).collect();
            let t = db.mset(0, &kvs).unwrap();
            if j + 1 < n && db.wait(keys[0], t, ack) == TicketState::Durable {
                acked = iter;
            }
            db.eng.pump();
        }
        devs = db.eng.into_devices();
        for d in &mut devs {
            crash(d, round);
        }
    }
}

#[test]
fn multi_key_writes_are_all_or_nothing_after_kill9() {
    let mut d = RamDev::new(64 * 4096, 512, 5);
    d.set_max_completions(2);
    format_devices(&mut [&mut d as &mut dyn BlockDevice], 16, false, 0, 5).unwrap();
    txn_rounds(vec![d], &mut |d, _| d.crash(), 12, TicketState::Durable);
}

#[cfg(feature = "sim")]
#[test]
fn multi_key_writes_are_all_or_nothing_after_power_cuts() {
    use kv_storage::sim::{SimDevice, TearMode};
    for lba in [512u32, 4096] {
        let mut d = SimDevice::new(64 * 4096, lba, None, 9);
        d.set_max_completions(3);
        format_devices(&mut [&mut d as &mut dyn BlockDevice], 16, false, 0, 9).unwrap();
        let modes = [TearMode::Bytes, TearMode::Sectors, TearMode::Prefix, TearMode::None];
        txn_rounds(vec![d], &mut |d, round| d.power_cut(modes[(round % 4) as usize]), 16, TicketState::Durable);
    }
}
