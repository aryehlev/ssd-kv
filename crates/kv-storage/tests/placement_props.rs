//! Placement and copy-bound properties of DESIGN §25.2 (LC-01, LC-08, LC-10): updates stay in
//! their page (two-image mode) or at their offset (atomic mode), at most two copies of a key
//! exist on media (I11/I12), holes are reused, placement is uncorrelated with the version
//! order, records never span devices, and TTLs can be shortened.

mod common;

use common::*;
use kv_index::Kind;
use kv_storage::engine::{LocalEngine, format_devices};
use kv_storage::io::BlockDevice;
use kv_storage::layout::{AG_PAGES, PageId};
use kv_storage::ticket::TicketState;
use proptest::prelude::*;

const T0: u64 = 1_800_000_000_000;

fn engine(devs: Vec<RamDev>, seed: u64) -> Db<RamDev> {
    let mut devs = devs;
    {
        let mut refs: Vec<&mut dyn BlockDevice> = devs.iter_mut().map(|d| d as &mut dyn BlockDevice).collect();
        format_devices(&mut refs, 16, false, 0, seed).unwrap();
    }
    let mut eng = LocalEngine::open(devs, &cfg()).unwrap();
    eng.recover().unwrap();
    Db::new(eng, T0)
}

fn key(i: usize) -> Vec<u8> {
    format!("p:{i}").into_bytes()
}

/// Sizes of small records under the slack cap.
fn small_sizes() -> impl Strategy<Value = Vec<usize>> {
    proptest::collection::vec(16usize..400, 30..50)
}

fn update_keeps(atomic: bool, sizes: &[usize], seed: u64) -> Result<(), TestCaseError> {
    let mut dev = RamDev::new(64 * 4096, 512, seed);
    if atomic {
        dev = dev.with_atomic((512, 4096));
    }
    let mut db = engine(vec![dev], seed);
    let n = sizes.len();
    for (i, &s) in sizes.iter().enumerate() {
        let t = db.set(0, &key(i), &value(i as u64, s), None).unwrap();
        db.wait(&key(i), t, TicketState::Durable);
    }
    prop_assert!(db.eng.settle(100_000));
    let before: Vec<_> = (0..n).map(|i| db.eng.locate(0, &key(i)).unwrap()).collect();
    for round in 0..3u64 {
        for (i, &s) in sizes.iter().enumerate() {
            // same-size or shrinking updates (lengths never grow from one round to the next)
            let len = if round == 0 { s } else { s - s / 4 };
            let t = db.set(0, &key(i), &value(round * 1000 + i as u64, len), None).unwrap();
            db.wait(&key(i), t, TicketState::Durable);
            db.eng.settle(1000);
            let now = db.eng.locate(0, &key(i)).unwrap();
            prop_assert_eq!(now.page, before[i].page, "update moved the key to another page");
            if atomic {
                prop_assert_eq!(now.off, before[i].off, "atomic-mode update moved the record");
            }
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 6, failure_persistence: None, ..ProptestConfig::default() })]

    /// LC-01: two-image mode, same-size and shrinking updates keep `Entry.page`.
    #[test]
    fn prop_update_keeps_page(sizes in small_sizes(), seed in any::<u64>()) {
        update_keeps(false, &sizes, seed)?;
    }

    /// LC-01: atomic mode, same-size and shrinking updates keep the record offset (R1a).
    #[test]
    fn prop_update_keeps_offset(sizes in small_sizes(), seed in any::<u64>()) {
        update_keeps(true, &sizes, seed)?;
    }

    /// I11/I12: in every durable state at most two valid copies of a key are on media, across
    /// in-page updates, moves between pages, small/extent conversions and deletes.
    #[test]
    fn prop_bounded_copies(ops in proptest::collection::vec((0usize..12, 0u8..10), 40..120), seed in any::<u64>()) {
        let mut db = engine(vec![RamDev::new(64 * 4096, 512, seed)], seed);
        for (step, &(k, kind)) in ops.iter().enumerate() {
            let k = key(k);
            match kind {
                0 => {
                    let (_, t) = db.del(0, &k).unwrap();
                    db.wait(&k, t, TicketState::Durable);
                }
                1 => {
                    let t = db.set(0, &k, &value(step as u64, 3000 + step * 7), None).unwrap();
                    db.wait(&k, t, TicketState::Durable);
                }
                2 => {
                    let t = db.set(0, &k, &value(step as u64, 70_000), None).unwrap();
                    db.wait(&k, t, TicketState::Durable);
                }
                _ => {
                    let t = db.set(0, &k, &value(step as u64, 10 + (step * 131) % 1900), None).unwrap();
                    db.wait(&k, t, TicketState::Durable);
                }
            }
            let boot = db.eng.boot().clone();
            let copies = count_copies(&db.eng.device(0).data, 0, &boot);
            for ((_, kk), n) in copies {
                prop_assert!(n <= 2, "key {} has {} copies on media", String::from_utf8_lossy(&kk), n);
            }
        }
    }
}

/// LC-01: on a pre-filled device, inserts after random deletes land in the holes (>= 90%) and
/// their addresses are not monotonic.
#[test]
fn prop_holes_reused() {
    for seed in [3u64, 17, 99] {
        let mut db = engine(vec![RamDev::new(96 * 4096, 512, seed)], seed);
        let n = 800usize;
        for i in 0..n {
            let t = db.set(0, &key(i), &value(i as u64, 300), None).unwrap();
            db.wait(&key(i), t, TicketState::Written);
        }
        assert!(db.eng.settle(100_000));
        let used: hashbrown::HashSet<PageId> = (0..n).map(|i| db.eng.locate(0, &key(i)).unwrap().page).collect();
        let mut r = seed;
        for i in 0..n {
            r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            if (r >> 33) % 10 < 3 {
                let (_, t) = db.del(0, &key(i)).unwrap();
                db.wait(&key(i), t, TicketState::Durable);
            }
        }
        assert!(db.eng.settle(100_000));
        let mut pages = Vec::new();
        let mut in_holes = 0;
        let m = 150usize;
        for j in 0..m {
            let k = format!("new:{j}").into_bytes();
            let t = db.set(0, &k, &value(9_000 + j as u64, 300), None).unwrap();
            db.wait(&k, t, TicketState::Durable);
            let at = db.eng.locate(0, &k).unwrap().page;
            if used.contains(&at) {
                in_holes += 1;
            }
            pages.push(at.0);
        }
        assert!(in_holes * 10 >= m * 9, "seed {seed}: only {in_holes} of {m} inserts reused holes");
        let mut sorted = pages.clone();
        sorted.sort_unstable();
        assert_ne!(pages, sorted, "seed {seed}: insert addresses are monotonic");
    }
}

/// LC-01: a bulk load on an empty device has no correlation between record seq and device
/// address (small records) or first page (extents): |Spearman| < 0.05. Seqs are per partition,
/// so the correlation is taken within each partition and pooled.
#[test]
fn prop_placement_uncorrelated() {
    fn put_all(db: &mut Db<RamDev>, keys: &[Vec<u8>], size: &dyn Fn(usize) -> usize) -> Vec<(u16, f64)> {
        let mut out = Vec::with_capacity(keys.len());
        for (i, k) in keys.iter().enumerate() {
            let p = db.eng.partition_of(k);
            let v = value(i as u64, size(i));
            let (_, o) = db.eng.run(p, keyed(0, &[k], kv_common::Access::Write), &mut |ks| ks.put(k, &v, kv_index::VType::String, None)).unwrap();
            out.push((p, o.seq.unwrap().0 as f64));
            if i % 16 == 0 {
                db.eng.pump();
            }
        }
        out
    }
    fn groups(db: &Db<RamDev>, keys: &[Vec<u8>], seqs: &[(u16, f64)]) -> Vec<(Vec<f64>, Vec<f64>)> {
        let mut g: Vec<(Vec<f64>, Vec<f64>)> = vec![(Vec::new(), Vec::new()); 16];
        for (k, &(p, s)) in keys.iter().zip(seqs) {
            let l = db.eng.locate(0, k).unwrap();
            g[usize::from(p)].0.push(s);
            g[usize::from(p)].1.push((l.page.byte_offset() + u64::from(l.off.unwrap_or(0))) as f64);
        }
        g
    }
    for seed in [5u64, 11] {
        let mut db = engine(vec![RamDev::new(256 * 4096, 512, seed)], seed);
        let small: Vec<Vec<u8>> = (0..24_000).map(key).collect();
        let seqs = put_all(&mut db, &small, &|i| 100 + i % 200);
        assert!(db.eng.settle(200_000));
        let rho = pooled_spearman(&groups(&db, &small, &seqs));
        assert!(rho.abs() < 0.05, "seed {seed}: small records |Spearman(seq, address)| = {rho}");
        let ext: Vec<Vec<u8>> = (0..3000).map(|j| format!("ext:{j}").into_bytes()).collect();
        let seqs = put_all(&mut db, &ext, &|j| 5000 + (j % 4) * 4096);
        assert!(db.eng.settle(200_000));
        let rho = pooled_spearman(&groups(&db, &ext, &seqs));
        assert!(rho.abs() < 0.05, "seed {seed}: extents |Spearman(seq, first page)| = {rho}");
    }
}

/// LC-08: a record lies inside one AG of one device; a chunkmap's chunks are on its device.
#[test]
fn record_single_device() {
    let devs = vec![RamDev::new(96 * 4096, 512, 1), RamDev::new(96 * 4096, 512, 2)];
    let mut db = engine(devs, 21);
    let sizes = [10usize, 300, 3000, 70_000, 2_000_000, 9_000_000];
    for (i, &s) in sizes.iter().enumerate() {
        for j in 0..4 {
            let k = format!("d:{i}:{j}").into_bytes();
            let t = db.set(0, &k, &value((i * 10 + j) as u64, s), None).unwrap();
            db.wait(&k, t, TicketState::Durable);
        }
    }
    assert!(db.eng.settle(100_000));
    let boot = db.eng.boot().clone();
    let mut devs_used = hashbrown::HashSet::new();
    for (i, _) in sizes.iter().enumerate() {
        for j in 0..4 {
            let k = format!("d:{i}:{j}").into_bytes();
            let l = db.eng.locate(0, &k).unwrap();
            devs_used.insert(l.page.dev());
            let lay = boot.layouts()[usize::from(l.page.dev())];
            let ag = lay.ag_of_page(u64::from(l.page.page())).expect("in the AG area");
            let last = lay.ag_of_page(u64::from(l.page.page()) + u64::from(l.n_pages) - 1).expect("in the AG area");
            assert_eq!(ag, last, "a record spans AGs");
            if l.kind == Kind::ChunkMap {
                let img = &db.eng.device(usize::from(l.page.dev())).data;
                let at = l.page.byte_offset() as usize;
                let n = l.n_pages as usize * 4096;
                let h = kv_storage::extent::parse_extent_header(&img[at..at + 4096], l.page).unwrap();
                let (_, body) = kv_storage::extent::chunkmap_parts(&img[at..at + n], &h).unwrap();
                for c in kv_storage::extent::decode_chunkmap(body).unwrap() {
                    assert_eq!(c.page.dev(), l.page.dev(), "a chunk on another device");
                    let ca = lay.ag_of_page(u64::from(c.page.page())).unwrap();
                    let cl = lay.ag_of_page(u64::from(c.page.page()) + u64::from(c.n_pages) - 1).unwrap();
                    assert_eq!(ca, cl, "a chunk spans AGs");
                }
            }
        }
    }
    assert_eq!(devs_used.len(), 2, "both devices hold records (the allocator spreads AGs)");
    let _ = AG_PAGES;
}

/// LC-10: any write may set, shorten, lengthen or remove a TTL; the result survives restarts.
#[test]
fn ttl_shorten() {
    let mut db = engine(vec![RamDev::new(64 * 4096, 512, 8)], 8);
    let k: &[u8] = b"ttl";
    let t = db.set(0, k, b"v", Some(T0 + 1_000_000)).unwrap();
    db.wait(k, t, TicketState::Durable);
    for at in [Some(T0 + 10_000), Some(T0 + 5_000_000), None, Some(T0 + 20_000)] {
        let (ok, t) = db.expire(0, k, at).unwrap();
        assert!(ok);
        db.wait(k, t, TicketState::Durable);
        assert_eq!(db.get(0, k).unwrap().unwrap().0.expire_at_ms, at);
    }
    let devs = db.eng.into_devices();
    let mut eng = LocalEngine::open(devs, &cfg()).unwrap();
    eng.recover().unwrap();
    let mut db = Db::new(eng, T0 + 1);
    assert_eq!(db.get(0, k).unwrap().unwrap().0.expire_at_ms, Some(T0 + 20_000));
    db.advance(30_000);
    assert!(db.get(0, k).unwrap().is_none(), "shortened TTL not honoured");
}
