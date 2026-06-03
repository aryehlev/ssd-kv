//! Criterion benchmarks for the SIndex KV engine.
//!
//! Measures single-key latency, sequential PUT/GET throughput, mixed workloads,
//! variable value sizes, and multi-thread scaling.
//!
//! Key count is capped at 1500 per engine to stay under the process fd limit
//! (ulimit -n 4096) given the one-file-per-partition design (65536 partitions;
//! 1500 keys → ~1480 unique segment files).

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use ssd_kv::engine::KvEngine;
use std::sync::Arc;
use tempfile::TempDir;

const MAX_KEYS: usize = 1_500; // keeps open files well under 4096

fn make_engine() -> (TempDir, Arc<KvEngine>) {
    let dir = TempDir::new().unwrap();
    let engine = KvEngine::open(dir.path()).unwrap();
    (dir, engine)
}

fn key(i: usize) -> Vec<u8> {
    format!("k:{:010}", i).into_bytes()
}

fn val(i: usize) -> Vec<u8> {
    format!("v:{:020}", i).into_bytes()
}

fn populated(n: usize) -> (TempDir, Arc<KvEngine>) {
    assert!(n <= MAX_KEYS, "n={n} would open too many segment files");
    let (dir, engine) = make_engine();
    for i in 0..n {
        engine.put(&key(i), &val(i)).unwrap();
    }
    (dir, engine)
}

// ─── Benchmarks ──────────────────────────────────────────────────────────────

/// Single-operation latency: the paper's core claim is ≤ BTREE_MAX_HEIGHT
/// SSD reads per GET, giving deterministic latency.
fn bench_latency(c: &mut Criterion) {
    let n = 1_000usize;
    let (_dir, engine) = populated(n);

    let mut group = c.benchmark_group("latency");
    group.bench_function("single_get", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let _ = engine.get(black_box(&key(i % n))).unwrap();
            i += 1;
        });
    });
    group.bench_function("single_put_update", |b| {
        // Updates existing keys → no new partition files opened.
        let mut i = 0usize;
        b.iter(|| {
            engine.put(black_box(&key(i % n)), black_box(&val(i))).unwrap();
            i += 1;
        });
    });
    group.bench_function("single_delete", |b| {
        let n2 = 500usize;
        let (_d2, e2) = populated(n2);
        let mut i = 0usize;
        b.iter(|| {
            let _ = e2.delete(black_box(&key(i % n2))).unwrap();
            i += 1;
        });
    });
    group.finish();
}

/// Sequential PUT throughput (steady-state, engine pre-warmed).
/// Keys cycle over MAX_KEYS so we never open more segment files than exist.
fn bench_put_seq(c: &mut Criterion) {
    let mut group = c.benchmark_group("put_seq");
    for &batch in &[10usize, 50, 200] {
        group.throughput(Throughput::Elements(batch as u64));
        group.bench_with_input(BenchmarkId::from_parameter(batch), &batch, |b, &batch| {
            let n = MAX_KEYS;
            let (_dir, engine) = populated(n);
            let mut base = 0usize;
            b.iter(|| {
                for i in 0..batch {
                    // Wrap within pre-existing keys: this measures PUT with a
                    // mix of tree traversal + value log append + fsync.
                    engine.put(black_box(&key((base + i) % n)), black_box(&val(i))).unwrap();
                }
                base += batch;
            });
        });
    }
    group.finish();
}

/// GET throughput — 100% hit rate.
fn bench_get_hit(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_hit");
    for &n in &[100usize, 500, 1_000] {
        let (_dir, engine) = populated(n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                for i in 0..n {
                    let _ = engine.get(black_box(&key(i))).unwrap();
                }
            });
        });
    }
    group.finish();
}

/// GET throughput — 0% hit rate (all misses).
fn bench_get_miss(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_miss");
    for &n in &[200usize, 1_000] {
        let (_dir, engine) = populated(n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                for i in n..2 * n {
                    let _ = engine.get(black_box(&key(i))).unwrap();
                }
            });
        });
    }
    group.finish();
}

/// Mixed 80% read / 20% write.
fn bench_mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_80r_20w");
    for &n in &[200usize, 1_000] {
        let (_dir, engine) = populated(n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                for i in 0..n {
                    if i % 5 == 0 {
                        engine.put(black_box(&key(i)), black_box(&val(i + 1))).unwrap();
                    } else {
                        let _ = engine.get(black_box(&key(i))).unwrap();
                    }
                }
            });
        });
    }
    group.finish();
}

/// PUT latency vs value byte size — shows value-log append cost.
fn bench_value_size(c: &mut Criterion) {
    let sizes: &[usize] = &[16, 256, 4_096, 65_536];
    let mut group = c.benchmark_group("put_value_size");
    for &sz in sizes {
        let v = vec![0x42u8; sz];
        group.throughput(Throughput::Bytes(sz as u64));
        group.bench_with_input(BenchmarkId::from_parameter(sz), &sz, |b, _| {
            // Pre-warm 200 keys; cycle puts over them so no new files open.
            let n = 200usize;
            let (_dir, engine) = populated(n);
            let mut i = 0usize;
            b.iter(|| {
                engine.put(black_box(&key(i % n)), black_box(&v)).unwrap();
                i += 1;
            });
        });
    }
    group.finish();
}

/// Multi-thread PUT+GET. Each thread gets its own key shard so no partition
/// is shared between threads; shows DashMap + per-partition RwLock scaling.
fn bench_concurrent(c: &mut Criterion) {
    let keys_per_thread = 100usize; // 4 threads × 100 = 400 unique keys → safe
    let mut group = c.benchmark_group("concurrent_put_get");

    for &threads in &[1usize, 2, 4] {
        let n = threads * keys_per_thread;
        let (_dir, engine) = populated(n);
        let engine = Arc::clone(&engine);

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(
            BenchmarkId::new("threads", threads),
            &threads,
            move |b, &threads| {
                let engine = Arc::clone(&engine);
                b.iter(|| {
                    let handles: Vec<_> = (0..threads)
                        .map(|t| {
                            let eng = Arc::clone(&engine);
                            let offset = t * keys_per_thread;
                            std::thread::spawn(move || {
                                for i in 0..keys_per_thread {
                                    eng.put(&key(offset + i), &val(i)).unwrap();
                                    let _ = eng.get(&key(offset + i)).unwrap();
                                }
                            })
                        })
                        .collect();
                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_latency,
    bench_put_seq,
    bench_get_hit,
    bench_get_miss,
    bench_mixed,
    bench_value_size,
    bench_concurrent,
);
criterion_main!(benches);
