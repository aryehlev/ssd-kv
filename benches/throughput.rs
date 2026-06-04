//! Criterion benchmarks for the SIndex KV engine.
//!
//! ## Benchmark categories
//!
//! **latency/** — per-operation µs showing B-Tree traversal + I/O cost
//!   - `warm_get`  : read from OS page cache (DRAM-speed, hot-path)
//!   - `cold_get`  : drop OS page cache before each op → true SSD read latency
//!   - `put`       : value-log append + B-Tree insert + 2× fdatasync
//!   - `delete`    : B-Tree remove + fdatasync
//!
//! **throughput/** — ops/s across warm (cached) and cold (disk) paths
//!   - `get_warm_*`, `get_cold_*`, `mixed_*`, `put_seq_*`, `put_value_size_*`
//!
//! **concurrent/** — multi-thread scaling
//!
//! ## What "warm" vs "cold" means
//! `warm` benchmarks read from the OS page cache (data already in DRAM after the
//! first read). `cold` benchmarks call `drop_page_cache()` before each timed
//! section, forcing every read to reach the SSD — this is the honest measure of
//! SSD-based indexing performance.
//!
//! On bare-metal NVMe: cold ~50-150 µs/op. On this ext4/vda VM the number
//! includes virtualisation overhead but still shows the storage round-trip.

use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use ssd_kv::engine::KvEngine;
use std::sync::Arc;
use tempfile::TempDir;

/// Maximum unique keys. With 65536 partitions we open one fd per partition;
/// keep well under the 4096 fd limit.
const MAX_KEYS: usize = 1_200;

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

/// Populate `n` keys, close the engine (clears in-process dirty cache),
/// reopen so that subsequent reads must come from disk (or OS page cache).
fn populated_cold(n: usize) -> (TempDir, Arc<KvEngine>) {
    assert!(n <= MAX_KEYS);
    let (dir, engine) = make_engine();
    for i in 0..n {
        engine.put(&key(i), &val(i)).unwrap();
    }
    engine.flush().unwrap();
    drop(engine);
    let engine = KvEngine::open(dir.path()).unwrap();
    (dir, engine)
}

/// Drop the OS page cache so reads must go to storage.
/// Requires Linux + CAP_SYS_ADMIN (we run as root in this environment).
fn drop_page_cache() {
    #[cfg(target_os = "linux")]
    {
        use std::io::Write;
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .write(true)
            .open("/proc/sys/vm/drop_caches")
        {
            let _ = f.write_all(b"3\n");
        }
    }
}

// ─── latency ─────────────────────────────────────────────────────────────────

fn bench_latency(c: &mut Criterion) {
    let n = 800usize;
    let (_dir, engine) = populated_cold(n);

    let mut group = c.benchmark_group("latency");
    // Fewer samples for cold measurements so drop_caches overhead is bounded.
    group.sample_size(20);

    // Warm GET: OS page cache (DRAM-speed). Realistic for a hot working set.
    group.bench_function("warm_get", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let _ = engine.get(black_box(&key(i % n))).unwrap();
            i += 1;
        });
    });

    // Cold GET: drop OS page cache before each measurement → true SSD latency.
    // This is the honest number for an "SSD-based index".
    group.bench_function("cold_get", |b| {
        let mut i = 0usize;
        b.iter_batched(
            || drop_page_cache(),
            |_| {
                let r = engine.get(black_box(&key(i % n)));
                i += 1;
                r
            },
            BatchSize::PerIteration,
        );
    });

    // PUT: value-log append + B-Tree insert + 2× fdatasync.
    group.bench_function("put", |b| {
        let mut i = n;
        b.iter(|| {
            engine
                .put(black_box(&key(i % n)), black_box(&val(i)))
                .unwrap();
            i += 1;
        });
    });

    // DELETE: B-Tree remove + fdatasync.
    group.bench_function("delete", |b| {
        let n2 = 400usize;
        let (_d2, e2) = populated_cold(n2);
        let mut i = 0usize;
        b.iter(|| {
            let _ = e2.delete(black_box(&key(i % n2))).unwrap();
            i += 1;
        });
    });

    group.finish();
}

// ─── throughput (warm = OS page cache) ───────────────────────────────────────

fn bench_get_warm(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_warm");
    for &n in &[100usize, 500, 1_000] {
        let (_dir, engine) = populated_cold(n);
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

// ─── throughput (cold = drop page cache before each batch) ───────────────────

fn bench_get_cold(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_cold");
    // Reduce sample count: each setup call drops all page caches (~1 ms).
    group.sample_size(15);
    for &n in &[100usize, 500, 1_000] {
        let (_dir, engine) = populated_cold(n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || drop_page_cache(),
                |_| {
                    for i in 0..n {
                        let _ = engine.get(black_box(&key(i))).unwrap();
                    }
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

// ─── put throughput ──────────────────────────────────────────────────────────

fn bench_put_seq(c: &mut Criterion) {
    let mut group = c.benchmark_group("put_seq");
    for &batch in &[10usize, 50, 200] {
        group.throughput(Throughput::Elements(batch as u64));
        group.bench_with_input(BenchmarkId::from_parameter(batch), &batch, |b, &batch| {
            let n = MAX_KEYS;
            let (_dir, engine) = populated_cold(n);
            let mut base = 0usize;
            b.iter(|| {
                for i in 0..batch {
                    engine
                        .put(black_box(&key((base + i) % n)), black_box(&val(i)))
                        .unwrap();
                }
                base += batch;
            });
        });
    }
    group.finish();
}

// ─── miss benchmark ──────────────────────────────────────────────────────────

fn bench_get_miss(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_miss");
    for &n in &[200usize, 1_000] {
        let (_dir, engine) = populated_cold(n);
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

// ─── mixed workload ──────────────────────────────────────────────────────────

fn bench_mixed_warm(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_80r_20w_warm");
    for &n in &[200usize, 800] {
        let (_dir, engine) = populated_cold(n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                for i in 0..n {
                    if i % 5 == 0 {
                        engine
                            .put(black_box(&key(i % n)), black_box(&val(i + 1)))
                            .unwrap();
                    } else {
                        let _ = engine.get(black_box(&key(i))).unwrap();
                    }
                }
            });
        });
    }
    group.finish();
}

fn bench_mixed_cold(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_80r_20w_cold");
    group.sample_size(15);
    for &n in &[200usize, 800] {
        let (_dir, engine) = populated_cold(n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || drop_page_cache(),
                |_| {
                    for i in 0..n {
                        if i % 5 == 0 {
                            engine
                                .put(black_box(&key(i % n)), black_box(&val(i + 1)))
                                .unwrap();
                        } else {
                            let _ = engine.get(black_box(&key(i))).unwrap();
                        }
                    }
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

// ─── value size sweep ────────────────────────────────────────────────────────

fn bench_value_size(c: &mut Criterion) {
    let sizes: &[usize] = &[16, 256, 4_096, 65_536];
    let mut group = c.benchmark_group("put_value_size");
    for &sz in sizes {
        let v = vec![0x42u8; sz];
        group.throughput(Throughput::Bytes(sz as u64));
        group.bench_with_input(BenchmarkId::from_parameter(sz), &sz, |b, _| {
            let n = 200usize;
            let (_dir, engine) = populated_cold(n);
            let mut i = 0usize;
            b.iter(|| {
                engine
                    .put(black_box(&key(i % n)), black_box(&v))
                    .unwrap();
                i += 1;
            });
        });
    }
    group.finish();
}

// ─── concurrent ──────────────────────────────────────────────────────────────

fn bench_concurrent(c: &mut Criterion) {
    let keys_per_thread = 80usize;
    let mut group = c.benchmark_group("concurrent_put_get");

    for &threads in &[1usize, 2, 4] {
        let n = threads * keys_per_thread;
        let (_dir, engine) = populated_cold(n);
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
    bench_get_warm,
    bench_get_cold,
    bench_put_seq,
    bench_get_miss,
    bench_mixed_warm,
    bench_mixed_cold,
    bench_value_size,
    bench_concurrent,
);
criterion_main!(benches);
