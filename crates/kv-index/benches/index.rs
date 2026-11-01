//! Probe / insert micro-benchmarks of the RAM index (DESIGN §9, §27).
//!
//! `cargo bench -p kv-index --bench index`. Each benchmark reports the time of one batch of
//! `n` operations with throughput `n` elements, so criterion's "thrpt" line gives operations
//! per second and `time / n` is the cost of one operation. Index memory per key of the tables
//! used is printed once at start-up.
//!
//! Table sizes: 60 000 keys is one slot table at 1 billion keys per node (§9.7: ~61K
//! keys/slot); 1 000 000 keys shows a table well beyond the CPU caches.

use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use kv_common::KeyHash;
use kv_index::{Entry, Kind, SlotTable, VType};
use smallvec::SmallVec;

const SEED: u64 = 0x005E_ED0F_1DE8;
const SIZES: [usize; 2] = [60_000, 1_000_000];
/// Operations per measured batch for the probe benchmarks.
const PROBES: usize = 10_000;

fn hashes(n: usize, salt: u64) -> Vec<KeyHash> {
    (0..n as u64)
        .map(|k| KeyHash::of(SEED ^ salt, &k.to_le_bytes()))
        .collect()
}

fn entry(k: usize) -> Entry {
    Entry {
        fp32: 0,
        x8: 0,
        page36: k as u64 + 1,
        kind: Kind::Small,
        vtype: VType::String,
        sync_mark: false,
    }
}

fn build(hs: &[KeyHash]) -> SlotTable {
    let mut t = SlotTable::new();
    for (k, h) in hs.iter().enumerate() {
        t.insert(*h, entry(k), 0);
    }
    t
}

struct XorShift(u64);

impl XorShift {
    fn below(&mut self, n: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        ((u128::from(self.0) * n as u128) >> 64) as usize
    }
}

/// A fixed pseudo-random visiting order of `count` indexes below `n` (with repeats).
fn order(n: usize, count: usize) -> Vec<usize> {
    let mut r = XorShift(0x1234_5678_9ABC_DEF0);
    (0..count).map(|_| r.below(n)).collect()
}

/// `count` distinct pseudo-random indexes below `n` (partial Fisher-Yates shuffle).
fn distinct(n: usize, count: usize) -> Vec<usize> {
    let mut r = XorShift(0x0FED_CBA9_8765_4321);
    let mut v: Vec<usize> = (0..n).collect();
    for i in 0..count {
        let j = i + r.below(n - i);
        v.swap(i, j);
    }
    v.truncate(count);
    v
}

fn report_memory() {
    for n in SIZES {
        let t = build(&hashes(n, 0));
        eprintln!(
            "kv-index: {n} keys: {:.2} B/key ({} segments, D = {})",
            t.memory_bytes() as f64 / n as f64,
            t.segments(),
            t.depth()
        );
        let mut tt = SlotTable::new();
        for (k, h) in hashes(n, 0).iter().enumerate() {
            tt.insert(*h, entry(k), 1);
        }
        eprintln!(
            "kv-index: {n} keys, all with TTL: {:.2} B/key",
            tt.memory_bytes() as f64 / n as f64
        );
    }
}

fn probe(c: &mut Criterion) {
    report_memory();
    let mut g = c.benchmark_group("probe");
    g.throughput(Throughput::Elements(PROBES as u64));
    for n in SIZES {
        let hs = hashes(n, 0);
        let t = build(&hs);
        let hits: Vec<KeyHash> = order(n, PROBES).into_iter().map(|i| hs[i]).collect();
        let misses = hashes(PROBES, 1);
        let mut out: SmallVec<[kv_index::Cand; 2]> = SmallVec::new();
        g.bench_with_input(BenchmarkId::new("hit", n), &hits, |b, keys| {
            b.iter(|| {
                let mut found = 0usize;
                for h in keys {
                    t.candidates(*h, &mut out);
                    found += out.len();
                }
                black_box(found)
            })
        });
        g.bench_with_input(BenchmarkId::new("miss", n), &misses, |b, keys| {
            b.iter(|| {
                let mut found = 0usize;
                for h in keys {
                    t.candidates(*h, &mut out);
                    found += out.len();
                }
                black_box(found)
            })
        });
    }
    g.finish();
}

fn insert(c: &mut Criterion) {
    let mut g = c.benchmark_group("insert");
    g.sample_size(20);
    for n in SIZES {
        let hs = hashes(n, 0);
        g.throughput(Throughput::Elements(n as u64));
        // growth from an empty table: every rebuild, split and directory doubling included
        g.bench_with_input(BenchmarkId::new("grow_from_empty", n), &hs, |b, hs| {
            b.iter_batched(
                SlotTable::new,
                |mut t| {
                    for (k, h) in hs.iter().enumerate() {
                        t.insert(*h, entry(k), 0);
                    }
                    t
                },
                BatchSize::PerIteration,
            )
        });
    }
    g.finish();

    // steady state: remove one key and insert another into a table of n keys
    let mut g = c.benchmark_group("churn");
    g.throughput(Throughput::Elements(PROBES as u64));
    for n in SIZES {
        let hs = hashes(n, 0);
        let fresh = hashes(PROBES, 2);
        let mut t = build(&hs);
        let victims = distinct(n, PROBES);
        let mut out: SmallVec<[kv_index::Cand; 2]> = SmallVec::new();
        let mut round = 0usize;
        g.bench_function(BenchmarkId::new("remove_insert", n), |b| {
            b.iter(|| {
                // alternate: remove victims / insert fresh keys, then the reverse
                let (gone, back) = if round.is_multiple_of(2) {
                    (&hs, &fresh)
                } else {
                    (&fresh, &hs)
                };
                for (j, &v) in victims.iter().enumerate() {
                    let (rm, rm_page, add, add_page) = if round.is_multiple_of(2) {
                        (gone[v], v, back[j], n + j)
                    } else {
                        (gone[j], n + j, back[v], v)
                    };
                    t.candidates(rm, &mut out);
                    if let Some(c) = out.iter().find(|c| c.entry.page36 == rm_page as u64 + 1) {
                        t.remove(c.pos);
                    }
                    t.insert(add, entry(add_page), 0);
                }
                round += 1;
            })
        });
    }
    g.finish();
}

criterion_group!(benches, probe, insert);
criterion_main!(benches);
