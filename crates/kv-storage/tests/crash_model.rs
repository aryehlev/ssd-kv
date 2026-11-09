//! Randomized command sequences, crashes and recovery checked against a reference model
//! (DESIGN §25.3 I1-I3): every key ends in its last acknowledged state or a state produced by
//! an operation that was not acknowledged; acknowledged deletes, FLUSH and expiry never come
//! back. kill -9 runs on RAM devices; power cuts with torn writes on `SimDevice` (feature
//! `sim`).

mod common;

use common::*;
use kv_storage::engine::{EngineCfg, LocalEngine, format_devices};
use kv_storage::io::BlockDevice;
use kv_storage::ticket::TicketState;

const T0: u64 = 1_800_000_000_000;

/// A tiny deterministic generator.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn chance(&mut self, pct: u64) -> bool {
        self.below(100) < pct
    }
}

fn key_of(i: u64) -> Vec<u8> {
    if i.is_multiple_of(4) { format!("{{grp}}k{i}").into_bytes() } else { format!("key:{i}").into_bytes() }
}

/// Value sizes: mostly small records, some across the 2 KiB boundary and some extents.
fn size(r: &mut Rng) -> usize {
    match r.below(20) {
        0 => 2040 + r.below(64) as usize,
        1 => 5_000 + r.below(20_000) as usize,
        2 => 70_000,
        _ => 1 + r.below(600) as usize,
    }
}

/// Runs `ops` random operations on `db` against `model`; `ack` is the ticket state a reply
/// waits for (`Written` for kill -9, `Durable` for power loss). Returns the key space used.
fn workload<D: BlockDevice>(db: &mut Db<D>, model: &mut Model, r: &mut Rng, ops: usize, keys: u64, ack: TicketState) {
    let trace = std::env::var("KVSSD_TRACE").is_ok();
    for n in 0..ops {
        let i = r.below(keys);
        if trace {
            eprintln!("op {n} key {i}");
        }
        let k = key_of(i);
        let wait = r.chance(70);
        let kind = r.below(100);
        if trace {
            eprintln!("  kind {kind} wait {wait}");
        }
        match kind {
            0..=49 => {
                let v = value(r.next(), size(r));
                let t = db.set(0, &k, &v, if r.chance(10) { Some(T0 + 1_000_000_000) } else { None }).unwrap();
                model.executed(&k, Some(v));
                if wait && ok(db.wait(&k, t, ack), ack) {
                    model.acked(&k);
                }
            }
            50..=64 => {
                let (_, t) = db.del(0, &k).unwrap();
                model.executed(&k, None);
                if wait && ok(db.wait(&k, t, ack), ack) {
                    model.acked(&k);
                }
            }
            65..=72 => {
                // a TTL in the past deletes (§16.3); a future one keeps the value
                let past = r.chance(50);
                let at = if past { T0 - 1 } else { T0 + 2_000_000_000 };
                let cur = model_value(model, &k);
                let (existed, t) = db.expire(0, &k, Some(at)).unwrap();
                if existed {
                    model.executed(&k, if past { None } else { cur });
                    if wait && ok(db.wait(&k, t, ack), ack) {
                        model.acked(&k);
                    }
                }
            }
            73..=80 => {
                let data = value(r.next(), 1 + r.below(300) as usize);
                let mut cur = model_value(model, &k).unwrap_or_default();
                let (_, t) = db.append(0, &k, &data).unwrap();
                cur.extend_from_slice(&data);
                model.executed(&k, Some(cur));
                if wait && ok(db.wait(&k, t, ack), ack) {
                    model.acked(&k);
                }
            }
            81..=92 => {
                // MSET of keys of one hash tag (one partition, crash-atomic via the intent table)
                let n = 2 + r.below(3);
                let kvs: Vec<(Vec<u8>, Vec<u8>)> = (0..n).map(|j| (key_of(4 * ((i + j) % (keys / 4).max(1))), value(r.next(), size(r)))).collect();
                let mut uniq: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
                for (k, v) in kvs {
                    uniq.retain(|(x, _)| *x != k);
                    uniq.push((k, v));
                }
                let refs: Vec<(&[u8], &[u8])> = uniq.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
                let t = db.mset(0, &refs).unwrap();
                for (k, v) in &uniq {
                    model.executed(k, Some(v.clone()));
                }
                if wait && ok(db.wait(&uniq[0].0, t, ack), ack) {
                    for (k, _) in &uniq {
                        model.acked(k);
                    }
                }
            }
            93..=95 => {
                let v = value(r.next(), size(r));
                let t = db.set(1, &k, &v, None).unwrap();
                let _ = db.wait(&k, t, ack);
            }
            _ => {
                if r.chance(20) {
                    // FLUSHDB of db 1 (the model tracks db 0 only)
                    let ts = db.flushdb(Some(1));
                    if wait {
                        for (p, t) in ts {
                            db.wait_in(p, t, ack);
                        }
                    }
                } else {
                    db.eng.pump();
                }
            }
        }
        if r.chance(30) {
            db.eng.pump();
        }
    }
}

/// True when a wait for `want` succeeded (a wait for Written may already see Durable).
fn ok(got: TicketState, want: TicketState) -> bool {
    match want {
        TicketState::Written => matches!(got, TicketState::Written | TicketState::Durable),
        _ => got == TicketState::Durable,
    }
}

fn model_value(m: &Model, k: &[u8]) -> Option<Vec<u8>> {
    m.keys.get(k).and_then(|(a, later)| match later.last() {
        Some(v) => v.clone(),
        None => a.clone(),
    })
}

/// Checks every model key after recovery and makes the recovered state the new acked state.
fn verify<D: BlockDevice>(db: &mut Db<D>, model: &mut Model, keys: u64, seed: u64, round: usize) {
    for i in 0..keys {
        let k = key_of(i);
        let got = db.get(0, &k).unwrap().map(|(_, v)| v);
        if let Err(e) = model.check(&k, got.as_deref()) {
            panic!("seed {seed} round {round} key {i}: {e}");
        }
        model.resolve(&k, got);
    }
    // no key the model does not know (an acked delete or a never-written key)
    for k in db.scan_all(0) {
        assert!(model.keys.get(&k).is_some_and(|(v, _)| v.is_some()), "seed {seed} round {round}: unknown key in SCAN");
    }
}

#[test]
fn kill9_random_sequences_never_lose_acked_writes() {
    let seeds = std::env::var("KVSSD_CRASH_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(6u64);
    let from = std::env::var("KVSSD_SEED_FROM").ok().and_then(|s| s.parse().ok()).unwrap_or(0u64);
    for seed in from..seeds {
        let mut r = Rng(seed ^ 0xC0FFEE);
        let lba = if seed % 2 == 0 { 512 } else { 4096 };
        let mut dev = RamDev::new(96 * 4096, lba, seed);
        if seed % 3 == 1 {
            // atomic page-update mode (§13.1): in-place rewrites with RWF_ATOMIC
            dev = dev.with_atomic((lba, 4096));
        }
        dev.set_max_completions(1 + (seed % 3) as usize);
        format_devices(&mut [&mut dev as &mut dyn BlockDevice], 16, false, 0, seed).unwrap();
        let mut devs = vec![dev];
        let mut model = Model::default();
        let keys = 48u64;
        for round in 0..3 {
            let mut eng = LocalEngine::open(devs, &cfg()).unwrap();
            eng.recover().unwrap();
            let mut db = Db::new(eng, T0);
            verify(&mut db, &mut model, keys, seed, round);
            // post-recovery work (scrubs of stale copies, txn resolution) completes
            db.quiesce();
            workload(&mut db, &mut model, &mut r, 120, keys, TicketState::Written);
            if round == 2 {
                // the last round ends quiescent: nothing it accepted is left non-terminal
                db.quiesce();
            }
            devs = db.eng.into_devices();
            for d in &mut devs {
                d.crash();
            }
        }
    }
}

#[test]
fn kill9_on_a_file_device() {
    use kv_storage::io::{DevInfo, SyncFileDevice, SyncOpen};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kvd.data");
    let opts = kv_storage::format::FormatOpts {
        devices: vec![(path.clone(), Some(1 << 30))],
        partitions: 16,
        cluster: false,
        snapshot_region: kv_storage::format::SnapshotRegionSize::Bytes(16 << 20),
    };
    if kv_storage::format::format(&opts).is_err() {
        eprintln!("skipping: the temp directory is not an allowed substrate (LC-34)");
        return;
    }
    let open = || {
        let probe = kv_storage::devprobe::probe(&path).unwrap();
        let info = DevInfo::new((1 << 30) / 4096, probe.lba);
        SyncFileDevice::open_with(&path, info, SyncOpen { direct: true, exclusive: true, read_only: false }).unwrap()
    };
    let mut model = Model::default();
    let mut r = Rng(99);
    let keys = 40u64;
    for round in 0..3 {
        let mut eng = LocalEngine::open(vec![open()], &cfg()).unwrap();
        eng.recover().unwrap();
        let mut db = Db::new(eng, T0);
        verify(&mut db, &mut model, keys, 99, round);
        workload(&mut db, &mut model, &mut r, 100, keys, TicketState::Written);
        if round == 1 {
            // a clean shutdown in the middle: the next start restores the snapshot
            let _ = db.eng.shutdown(true).unwrap();
        } else {
            drop(db.eng.into_devices());
        }
    }
}

#[test]
fn engine_cfg_defaults_are_sane() {
    let c = EngineCfg::defaults(Vec::new());
    assert_eq!(c.page_slack_cap, 1024);
    assert_eq!(c.page_slack_atomic, 128);
}

#[cfg(feature = "sim")]
mod power {
    use super::*;
    use kv_storage::sim::{SimDevice, TearMode};

    fn tear(seed: u64) -> TearMode {
        match seed % 4 {
            0 => TearMode::Bytes,
            1 => TearMode::Sectors,
            2 => TearMode::Prefix,
            _ => TearMode::None,
        }
    }

    /// Power cuts with torn writes, `durability flushed` (acks wait for Durable, I3).
    #[test]
    fn power_cut_random_sequences_keep_durable_acks() {
        let seeds = std::env::var("KVSSD_CRASH_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(8u64);
        let from = std::env::var("KVSSD_SEED_FROM").ok().and_then(|s| s.parse().ok()).unwrap_or(0u64);
        for seed in from..seeds {
            let mut r = Rng(seed ^ 0xBADC0DE);
            let lba = if seed % 2 == 0 { 512 } else { 4096 };
            let atomic = (seed % 3 == 0).then_some(4096);
            let mut dev = SimDevice::new(96 * 4096, lba, atomic, seed);
            dev.set_max_completions(1 + (seed % 4) as usize);
            format_devices(&mut [&mut dev as &mut dyn BlockDevice], 16, false, 0, seed).unwrap();
            let trace = std::env::var("KVSSD_TRACE").is_ok();
            if trace {
                eprintln!("formatted");
            }
            let mut devs = vec![dev];
            let mut model = Model::default();
            let keys = 40u64;
            for round in 0..3 {
                let mut eng = LocalEngine::open(devs, &cfg()).unwrap();
                if trace {
                    eprintln!("opened");
                }
                eng.recover().unwrap();
                if trace {
                    eprintln!("recovered");
                }
                let mut db = Db::new(eng, T0);
                verify(&mut db, &mut model, keys, seed, round);
                db.quiesce();
                workload(&mut db, &mut model, &mut r, 100, keys, TicketState::Durable);
                if round == 2 {
                    db.quiesce();
                }
                devs = db.eng.into_devices();
                for d in &mut devs {
                    if round % 2 == 0 {
                        d.power_cut(tear(seed + round as u64));
                    } else {
                        d.crash(TearMode::None);
                    }
                }
            }
        }
    }
}


/// A device whose writes to `hold` (byte range) stay pending until the test lets them go; a
/// kill -9 drops them.
struct HoldDev {
    inner: RamDev,
    hold: std::rc::Rc<std::cell::Cell<(u64, u64)>>,
    held: Vec<kv_storage::io::IoReq>,
}

impl BlockDevice for HoldDev {
    fn info(&self) -> kv_storage::io::DevInfo {
        self.inner.info()
    }
    fn submit(&mut self, r: kv_storage::io::IoReq) {
        let (lo, hi) = self.hold.get();
        if matches!(r.op, kv_storage::io::IoOp::Write { .. }) && r.offset < hi && r.offset + u64::from(r.len) > lo {
            self.held.push(r);
        } else {
            self.inner.submit(r);
        }
    }
    fn poll(&mut self, out: &mut Vec<kv_storage::io::IoDone>, wait: bool) {
        if self.hold.get().0 >= self.hold.get().1 {
            for r in self.held.drain(..) {
                self.inner.submit(r);
            }
        }
        self.inner.poll(out, wait);
    }
}

/// A local multi-key write (RENAME a -> b, a txn) whose new version of `b` expires before the
/// txn is committed: the active expiry sweep, which does not go through key queues, removed it
/// and zeroed `b`'s older copy although recovery, finding the txn uncommitted, falls back to
/// that copy. After a kill -9 before the commit `a` kept its old value and `b` was gone: half
/// of the write was applied (I4). The sweep now leaves keys of uncommitted txns alone.
#[test]
fn expiry_of_an_uncommitted_txn_version_never_breaks_atomicity() {
    use kv_storage::layout::PAGE;
    let mut bad = Vec::new();
    for seed in 0..6u64 {
        let hold = std::rc::Rc::new(std::cell::Cell::new((0u64, 0u64)));
        let mut inner = RamDev::new(64 * 4096, 512, seed);
        inner.set_max_completions(1 + (seed % 2) as usize);
        format_devices(&mut [&mut inner as &mut dyn BlockDevice], 16, false, 0, seed).unwrap();
        let dev = HoldDev { inner, hold: hold.clone(), held: Vec::new() };
        let mut eng = LocalEngine::open(vec![dev], &cfg()).unwrap();
        eng.recover().unwrap();
        let mut db = Db::new(eng, T0);
        let (a, b): (&[u8], &[u8]) = (b"{r}a", b"{r}b");
        let long = value(seed, 6000);
        let t = db.set(0, a, &long, Some(T0 + 600_000)).unwrap();
        db.wait(a, t, TicketState::Durable);
        let t = db.set(0, b, b"B-value", None).unwrap();
        db.wait(b, t, TicketState::Durable);
        let p = db.eng.partition_of(a);
        // RENAME a b, except that the destination's expiry is short (a txn of two keys); the
        // new version of `b` is an extent whose write is held, so the txn cannot commit
        let (_, out) = db
            .eng
            .run(p, keyed(0, &[a, b], kv_common::Access::Write), &mut |ks| {
                ks.select(0);
                ks.delete(a)?;
                ks.put(b, &long, kv_index::VType::String, Some(T0 + 1_000))
            })
            .unwrap();
        let at = db.eng.locate(0, b).expect("b").page;
        let off = at.byte_offset();
        hold.set((off, off + 2 * PAGE as u64));
        // time passes beyond `b`'s expiry: the sweep runs while the txn waits for that write
        for _ in 0..400 {
            db.advance(10);
            db.eng.pump();
        }
        let done = matches!(db.eng.ticket_state(p, out.ticket.unwrap()), Some(TicketState::Written | TicketState::Durable));
        let mut devs = db.eng.into_devices();
        let mut dev = devs.pop().unwrap();
        dev.held.clear();
        let mut inner = dev.inner;
        inner.crash();
        let mut eng = LocalEngine::open(vec![inner], &cfg()).unwrap();
        eng.recover().unwrap();
        let mut db = Db::new(eng, T0 + 4_100);
        let got_a = db.get(0, a).unwrap().map(|x| x.1);
        let got_b = db.get(0, b).unwrap().map(|x| x.1);
        // applied: `a` gone and `b` expired; not applied: both old values
        let applied = got_a.is_none() && got_b.is_none();
        let reverted = got_a.as_deref() == Some(&long[..]) && got_b.as_deref() == Some(b"B-value");
        if done || !(applied || reverted) {
            bad.push(format!("seed {seed}: committed={done} a={:?} b={:?}", got_a.map(|v| v.len()), got_b.map(|v| v.len())));
        }
        db.quiesce();
    }
    assert!(bad.is_empty(), "multi-key write applied partly after kill -9:\n{}", bad.join("\n"));
}
