//! Randomized cross-partition txn parts beside plain writes on a small, nearly full two-image
//! device (DESIGN §14.3, §14.6; "no hangs"). kvd's coordinator stages one part per partition,
//! commits it once every part is durable or aborts it; meanwhile the keys of the part are held
//! (no other request of the node touches them). The parts here are those of one partition, the
//! other parts of their txns are left implicit. The liveness check runs at intervals; at the
//! end every txn is finished, every request must complete, the partition must become quiescent
//! and every key must read back as the model says, also after a crash.
//!
//! `XTXN_FUZZ_SEEDS=N` (default 24) and `XTXN_FUZZ_START=S` choose the seeds;
//! `XTXN_FUZZ_MIN=1` minimizes the first failing seed's operations and prints them.

mod common;

use common::*;
use hashbrown::HashMap;
use kv_common::Access;
use kv_index::VType;
use kv_storage::engine::{LocalEngine, format_devices};
use kv_storage::io::BlockDevice;
use kv_storage::layout::AG_PAGES;
use kv_storage::metastore::MetaReq;
use kv_storage::partition::{Origin, ReqKind, Request};
use kv_storage::ticket::{TicketId, TicketState};

const T0: u64 = 1_800_000_000_000;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

/// One step of a case.
#[derive(Clone, Debug)]
enum Op {
    Set { k: usize, len: usize },
    Del { k: usize },
    /// A local multi-key write (one partition: a local txn when its keys span pages).
    Mset { kv: Vec<(usize, usize)> },
    /// A local multi-key delete.
    MDel { ks: Vec<usize> },
    /// A part of a new txn on `k` (`None`: a delete).
    Part { k: usize, len: Option<usize> },
    /// Finishes the open txn of key `k`: commit (once its part is durable) or abort.
    Finish { k: usize, abort: bool },
    /// The coordinator reports the oldest abort (once its ticket is durable).
    Report,
    Pump(u32),
    Advance(u64),
}

/// Device and fill of a case.
#[derive(Clone, Debug)]
struct Setup {
    seed: u64,
    max_completions: usize,
    fill_limit: f32,
    /// The partition's space is filled to `-OOM` first (then every eighth filler deleted).
    fill: bool,
}

/// An applied part whose txn is not finished.
struct Open {
    txn: u64,
    new: Option<Vec<u8>>,
    ticket: Option<TicketId>,
}

fn key(i: usize) -> Vec<u8> {
    format!("{{t}}k{i}").into_bytes()
}

fn gen_len(r: &mut Rng) -> usize {
    match r.below(20) {
        0 => 2100 + r.below(20_000) as usize,
        1..=3 => 1200 + r.below(800) as usize,
        _ => 1 + r.below(1200) as usize,
    }
}

fn generate(seed: u64) -> (Setup, Vec<Op>) {
    let mut r = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
    let setup = Setup { seed, max_completions: 1 + r.below(3) as usize, fill_limit: 0.1 + 0.05 * r.below(3) as f32, fill: r.below(2) == 0 };
    let nkeys = 60 + r.below(200) as usize;
    let steps = 1500 + r.below(1500);
    let mut ops = Vec::new();
    for _ in 0..steps {
        let k = r.below(nkeys as u64) as usize;
        let kind = r.below(100);
        ops.push(match kind {
            0..22 => Op::Set { k, len: gen_len(&mut r) },
            22..28 => {
                let n = 2 + r.below(3) as usize;
                Op::Mset { kv: (0..n).map(|i| (if i == 0 { k } else { r.below(nkeys as u64) as usize }, gen_len(&mut r))).collect() }
            }
            28..31 => Op::MDel { ks: vec![k, r.below(nkeys as u64) as usize, r.below(nkeys as u64) as usize] },
            31..38 => Op::Del { k },
            38..60 => Op::Part { k, len: if r.below(6) == 0 { None } else { Some(gen_len(&mut r)) } },
            60..80 => Op::Finish { k, abort: r.below(3) == 0 },
            80..86 => Op::Report,
            86..96 => Op::Pump(r.below(4) as u32),
            _ => Op::Advance(r.below(40)),
        });
    }
    (setup, ops)
}

fn apply_part(db: &mut Db<RamDev>, k: &[u8], txn: u64, v: Option<&[u8]>) -> Result<Option<TicketId>, kv_storage::keyspace::StoreError> {
    let p = db.eng.partition_of(k);
    let req = Request { db: 0, keys: [(Box::<[u8]>::from(k), Access::Write)].into_iter().collect(), kind: ReqKind::Txn { txn_id: txn }, origin: Origin::Client };
    let (_, out) = db.eng.run(p, req, &mut |ks| {
        ks.select(0);
        match v {
            Some(v) => ks.put(k, v, VType::String, None),
            None => ks.delete(k).map(|_| ()),
        }
    })?;
    Ok(out.ticket)
}

/// Runs a case; `Err` describes the first failure.
fn run_case(s: &Setup, ops: &[Op]) -> Result<(), String> {
    let mut d = RamDev::new(21 * u64::from(AG_PAGES), 512, s.seed);
    d.set_max_completions(s.max_completions);
    format_devices(&mut [&mut d as &mut dyn BlockDevice], 16, false, 0, s.seed).unwrap();
    let mut c = cfg();
    // a fill limit the partition reaches: its first AGs and a few more
    c.max_storage_fill = s.fill_limit;
    let mut eng = LocalEngine::open(vec![d], &c).unwrap();
    eng.recover().unwrap();
    let mut db = Db::new(eng, T0);
    let p = db.eng.partition_of(&key(0));
    let mut model: HashMap<usize, Vec<u8>> = HashMap::new();
    let mut open: HashMap<usize, Open> = HashMap::new();
    let mut aborts: Vec<(u64, Option<TicketId>)> = Vec::new();
    let trace = std::env::var_os("XTXN_FUZZ_TRACE").is_some();
    if s.fill {
        let mut r = Rng(s.seed | 1);
        let (mut i, mut refused) = (0u64, 0);
        while refused < 20 && i < 200_000 {
            let k = format!("{{t}}f{i}").into_bytes();
            i += 1;
            match db.set(0, &k, &value(i, 1 + r.below(1900) as usize), None) {
                Ok(t) => {
                    refused = 0;
                    db.wait(&k, t, TicketState::Written);
                }
                Err(_) => refused += 1,
            }
        }
        for j in (0..i).step_by(8) {
            let _ = db.del(0, &format!("{{t}}f{j}").into_bytes());
        }
    }
    let wait = |db: &mut Db<RamDev>, t: Option<TicketId>, what: &str| -> Result<(), String> {
        let Some(t) = t else { return Ok(()) };
        match db.eng.wait_ticket(p, t, TicketState::Durable) {
            TicketState::Durable => Ok(()),
            got => Err(format!("{what}: {got:?}: {:?}", db.eng.check_progress())),
        }
    };
    for (i, op) in ops.iter().enumerate() {
        let ver = i as u64 * 7 + s.seed;
        match *op {
            Op::Set { k, len } if !open.contains_key(&k) => {
                let v = value(ver, len);
                if db.set(0, &key(k), &v, None).is_ok() {
                    model.insert(k, v);
                }
            }
            Op::Del { k } if !open.contains_key(&k) => {
                if db.del(0, &key(k)).is_ok() {
                    model.remove(&k);
                }
            }
            Op::Mset { ref kv } if kv.iter().all(|(k, _)| !open.contains_key(k)) => {
                let mut kv: Vec<(usize, usize)> = kv.clone();
                kv.sort_by_key(|x| x.0);
                kv.dedup_by_key(|x| x.0);
                let vals: Vec<(Vec<u8>, Vec<u8>)> = kv.iter().enumerate().map(|(j, &(k, n))| (key(k), value(ver + 11 * j as u64, n))).collect();
                let refs: Vec<(&[u8], &[u8])> = vals.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
                if db.mset(0, &refs).is_ok() {
                    for ((k, _), (_, v)) in kv.iter().zip(vals) {
                        model.insert(*k, v);
                    }
                }
            }
            Op::MDel { ref ks } if ks.iter().all(|k| !open.contains_key(k)) => {
                let mut ks = ks.clone();
                ks.sort();
                ks.dedup();
                let names: Vec<Vec<u8>> = ks.iter().map(|&k| key(k)).collect();
                let refs: Vec<&[u8]> = names.iter().map(|k| k.as_slice()).collect();
                let req = kv_storage::partition::Request { db: 0, keys: refs.iter().map(|k| (Box::<[u8]>::from(*k), Access::Write)).collect(), kind: ReqKind::Keyed, origin: Origin::Client };
                let res = db.eng.run(p, req, &mut |ks2| {
                    ks2.select(0);
                    for k in &refs {
                        ks2.delete(k)?;
                    }
                    Ok(())
                });
                if res.is_ok() {
                    for k in ks {
                        model.remove(&k);
                    }
                }
            }
            Op::Part { k, len } if !open.contains_key(&k) => {
                let txn = db.eng.meta().txn_ids().next();
                let new = len.map(|n| value(ver + 3, n));
                match apply_part(&mut db, &key(k), txn, new.as_deref()) {
                    Ok(ticket) => {
                        open.insert(k, Open { txn, new, ticket });
                    }
                    Err(_) => {
                        // a refused part staged nothing; its coordinator reports the txn aborted
                        let _ = db.eng.abort_txn(p, txn);
                        db.eng.meta_client().request(p, 0, MetaReq::TxnAborted { txn });
                    }
                }
            }
            Op::Finish { k, abort } if open.contains_key(&k) => {
                let o = open.remove(&k).unwrap();
                if abort {
                    // kvd undoes the part and releases the key's hold at once
                    let t = db.eng.abort_txn(p, o.txn);
                    aborts.push((o.txn, t));
                } else {
                    wait(&mut db, o.ticket, &format!("op {i}: part of txn {}", o.txn))?;
                    if trace {
                        trace_keys(&db, p, &format!("op {i}: part of txn {} durable", o.txn), ops);
                    }
                    // the commit is applied to the part before the key's hold goes
                    db.eng.meta_client().request(p, 0, MetaReq::CommitTxn { txn: o.txn });
                    db.eng.pump();
                    if trace {
                        trace_keys(&db, p, &format!("op {i}: txn {} committed", o.txn), ops);
                    }
                    match o.new {
                        Some(v) => model.insert(k, v),
                        None => model.remove(&k),
                    };
                }
            }
            Op::Report if !aborts.is_empty() => {
                let (txn, t) = aborts.remove(0);
                wait(&mut db, t, &format!("op {i}: abort of txn {txn}"))?;
                db.eng.meta_client().request(p, 0, MetaReq::TxnAborted { txn });
            }
            Op::Pump(n) => {
                for _ in 0..n {
                    db.eng.pump();
                }
            }
            Op::Advance(ms) => db.advance(ms),
            _ => {}
        }
        if i % 64 == 63 {
            db.eng.check_progress().map_err(|e| format!("op {i}: {e}"))?;
        }
        if trace {
            trace_keys(&db, p, &format!("after op {i} {op:?}"), ops);
        }
    }
    // every txn finishes
    let mut left: Vec<(usize, Open)> = core::mem::take(&mut open).into_iter().collect();
    left.sort_by_key(|x| x.0);
    for (k, o) in left {
        wait(&mut db, o.ticket, &format!("end: part of txn {}", o.txn))?;
        if trace {
            trace_keys(&db, p, &format!("end: part of txn {} durable", o.txn), ops);
        }
        db.eng.meta_client().request(p, 0, MetaReq::CommitTxn { txn: o.txn });
        db.eng.pump();
        if trace {
            trace_keys(&db, p, &format!("end: txn {} committed", o.txn), ops);
            db.eng.pump();
            trace_keys(&db, p, "and a pump", ops);
        }
        match o.new {
            Some(v) => model.insert(k, v),
            None => model.remove(&k),
        };
    }
    for (txn, t) in core::mem::take(&mut aborts) {
        wait(&mut db, t, &format!("end: abort of txn {txn}"))?;
        db.eng.meta_client().request(p, 0, MetaReq::TxnAborted { txn });
    }
    if trace {
        trace_keys(&db, p, "after the end's commits", ops);
    }
    db.eng.drive_to_quiescence(200_000).map_err(|e| format!("end: {e}"))?;
    if trace {
        trace_keys(&db, p, "quiescent", ops);
    }
    let keys: Vec<usize> = ops
        .iter()
        .map(|o| match *o {
            Op::Set { k, .. } | Op::Del { k } | Op::Part { k, .. } => vec![k],
            Op::Mset { ref kv } => kv.iter().map(|x| x.0).collect(),
            Op::MDel { ref ks } => ks.clone(),
            _ => Vec::new(),
        })
        .flatten()
        .collect();
    let check = |db: &mut Db<RamDev>, when: &str| -> Result<(), String> {
        for &k in &keys {
            let got = db.get(0, &key(k)).map_err(|e| format!("{when}: get {k}: {e:?}"))?.map(|x| x.1);
            if got.as_ref() != model.get(&k) {
                return Err(format!("{when}: key {k}: {:?} bytes, expected {:?}", got.map(|v| v.len()), model.get(&k).map(|v| v.len())));
            }
        }
        Ok(())
    };
    check(&mut db, "end")?;
    let mut devs = db.eng.into_devices();
    devs[0].crash();
    let mut eng = LocalEngine::open(devs, &c).unwrap();
    eng.recover().unwrap();
    let mut db = Db::new(eng, T0 + 1000);
    check(&mut db, "after a crash")?;
    db.eng.drive_to_quiescence(200_000).map_err(|e| format!("after a crash: {e}"))?;
    Ok(())
}

/// Prints the pages of the case's keys (`XTXN_FUZZ_TRACE`).
fn trace_keys(db: &Db<RamDev>, p: u16, when: &str, ops: &[Op]) {
    let mut pages: Vec<kv_storage::layout::PageId> = Vec::new();
    let mut line = format!("{when}:");
    for o in ops {
        let ks: Vec<usize> = match *o {
            Op::Set { k, .. } | Op::Del { k } | Op::Part { k, .. } => vec![k],
            Op::Mset { ref kv } => kv.iter().map(|x| x.0).collect(),
            Op::MDel { ref ks } => ks.clone(),
            _ => Vec::new(),
        };
        for k in ks {
            let loc = db.eng.locate(0, &key(k));
            let h = kv_common::KeyHash::of(db.eng.boot().hash_seed(), &key(k)).h32() & 0xffff;
            line.push_str(&format!(" k{k}(k={h:04x})@{:?}", loc.as_ref().map(|l| (l.page.0, l.off))));
            if let Some(l) = loc
                && !pages.contains(&l.page)
            {
                pages.push(l.page);
            }
        }
    }
    eprintln!("{line}");
    for pg in pages {
        eprintln!("   {}", db.eng.partition(p).diag_page(db.eng.cache(), pg));
    }
}

/// Removes operations while the case still fails (chunks, halving).
fn minimize(s: &Setup, mut ops: Vec<Op>) -> Vec<Op> {
    let mut chunk = ops.len() / 2;
    while chunk >= 1 {
        let mut i = 0;
        let mut progress = false;
        while i < ops.len() {
            let mut cand = ops.clone();
            cand.drain(i..(i + chunk).min(ops.len()));
            if run_case(s, &cand).is_err() {
                ops = cand;
                progress = true;
            } else {
                i += chunk;
            }
        }
        if !progress {
            chunk /= 2;
        }
    }
    ops
}

#[test]
fn txn_parts_beside_plain_writes_on_a_full_device() {
    let n: u64 = std::env::var("XTXN_FUZZ_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(24);
    let start: u64 = std::env::var("XTXN_FUZZ_START").ok().and_then(|s| s.parse().ok()).unwrap_or(1);
    for seed in start..start + n {
        let (s, ops) = generate(seed);
        if let Err(e) = run_case(&s, &ops) {
            if std::env::var_os("XTXN_FUZZ_MIN").is_some() {
                let min = minimize(&s, ops);
                panic!("seed {seed}: {e}\nminimized to {} ops: {s:?}\n{min:?}\nfails with: {:?}", min.len(), run_case(&s, &min));
            }
            panic!("seed {seed}: {e}");
        }
    }
}

/// Minimized failing cases of the fuzz test.
#[test]
fn minimized_cases() {
    let cases: Vec<(Setup, Vec<Op>)> = vec![
        // seed 7: a part's version beside its key's old copy in one page, and another txn's
        // part committed first; the old copy stayed protected after its txn committed
        (
            Setup { seed: 7, max_completions: 3, fill_limit: 0.2, fill: false },
            vec![Op::Set { k: 2, len: 991 }, Op::Part { k: 77, len: Some(369) }, Op::Part { k: 2, len: Some(698) }, Op::Finish { k: 77, abort: false }],
        ),
        // seed 4: an aborted part's record came back after a crash
        (
            Setup { seed: 4, max_completions: 1, fill_limit: 0.2, fill: false },
            vec![
                Op::Part { k: 41, len: Some(1075) },
                Op::Part { k: 128, len: Some(246) },
                Op::Pump(3),
                Op::Part { k: 64, len: Some(9) },
                Op::Finish { k: 41, abort: false },
                Op::Finish { k: 64, abort: true },
            ],
        ),
        // seed 22: two aborted parts in a row on a key; its value was lost
        (
            Setup { seed: 22, max_completions: 3, fill_limit: 0.15, fill: false },
            vec![
                Op::Mset { kv: vec![(50, 650), (130, 17444)] },
                Op::Set { k: 119, len: 168 },
                Op::Part { k: 119, len: Some(113) },
                Op::Mset { kv: vec![(29, 563), (130, 1000), (68, 191)] },
                Op::Finish { k: 119, abort: true },
                Op::Part { k: 119, len: Some(1148) },
                Op::Finish { k: 119, abort: true },
            ],
        ),
        (
            Setup { seed: 22, max_completions: 3, fill_limit: 0.15, fill: false },
            vec![
                Op::Part { k: 57, len: Some(264) },
                Op::Mset { kv: vec![(119, 391), (29, 915), (143, 235)] },
                Op::Part { k: 119, len: Some(113) },
                Op::Mset { kv: vec![(39, 389), (88, 43), (92, 1025), (134, 1005)] },
                Op::Set { k: 93, len: 8914 },
                Op::Set { k: 12, len: 229 },
                Op::Finish { k: 119, abort: true },
                Op::Part { k: 119, len: Some(1148) },
                Op::Finish { k: 57, abort: false },
                Op::Finish { k: 119, abort: true },
            ],
        ),
    ];
    let mut failed = Vec::new();
    for (s, ops) in cases {
        if let Err(e) = run_case(&s, &ops) {
            failed.push(format!("{s:?} {ops:?}: {e}"));
        }
    }
    assert!(failed.is_empty(), "{}", failed.join("\n\n"));
}
