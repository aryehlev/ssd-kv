//! Realistic benchmark: WAL-enabled, concurrent, Zipfian key distribution.
//!
//! Models a real workload:
//!  - WAL fsync on every durable write
//!  - 80/20 Zipfian read distribution (hot keys)
//!  - Realistic value sizes (50-500 bytes, occasional 8 KB blob)
//!  - Mixed read/write (80% GET, 20% SET)
//!  - Concurrent readers + writer threads

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};
use tempfile::tempdir;

use ssd_kv::engine::Index;
use ssd_kv::server::Handler;
use ssd_kv::storage::{SegmentManager, WriteBuffer, WalConfig, WriteAheadLog};

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn make_handler_with_wal() -> (Handler, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let sm = Arc::new(SegmentManager::new(dir.path()).unwrap());
    let index = Arc::new(Index::new());
    let wb = Arc::new(WriteBuffer::new(0u32, 0usize));
    let mut handler = Handler::new(Arc::clone(&index), Arc::clone(&sm), wb);

    let wal_dir = dir.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let config = WalConfig { dir: wal_dir, ..WalConfig::default() };
    let wal = Arc::new(WriteAheadLog::new(config).unwrap());
    handler.set_wal(wal);

    (handler, dir)
}

/// Zipfian index: given a sample in [0,1), return a key index biased toward 0.
/// Approximation: index = (n * u^1.5) as usize, giving ~80/20 distribution.
fn zipfian_idx(sample: f64, n: usize) -> usize {
    ((n as f64) * sample.powf(1.5)).min(n as f64 - 1.0) as usize
}

fn make_value(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 251) as u8).collect()
}

// ─── Warmup: pre-populate N keys ──────────────────────────────────────────────

fn populate(handler: &Handler, n: usize, value_size: usize) {
    let val = make_value(value_size);
    for i in 0..n {
        let key = format!("key:{:08}", i);
        handler.put_sync(key.as_bytes(), &val, 0).unwrap();
    }
}

// ─── Single-thread: WAL-enabled PUT ──────────────────────────────────────────

fn bench_put_wal(c: &mut Criterion) {
    let mut group = c.benchmark_group("realistic/put_wal");
    group.throughput(Throughput::Elements(1));

    for value_size in [64usize, 512, 4096] {
        let (handler, _dir) = make_handler_with_wal();
        let val = make_value(value_size);
        let mut i = 0u64;

        group.bench_with_input(
            BenchmarkId::new("small_key", format!("{}B_value", value_size)),
            &value_size,
            |b, _| {
                b.iter(|| {
                    i = i.wrapping_add(1);
                    let key = format!("k:{}", i);
                    handler.put_sync(key.as_bytes(), &val, 0)
                })
            },
        );
    }
    group.finish();
}

// ─── Single-thread: GET hot key (repeated access to same key) ─────────────────

fn bench_get_hot(c: &mut Criterion) {
    let mut group = c.benchmark_group("realistic/get");
    group.throughput(Throughput::Elements(1));

    // Hot key: same key read repeatedly (best case — key definitely in index)
    {
        let (handler, _dir) = make_handler_with_wal();
        populate(&handler, 10_000, 64);
        let hot_key = b"key:00000001";

        group.bench_function("hot_key_64B", |b| {
            b.iter(|| handler.get_value(hot_key))
        });
    }

    // Cold scan: sequential keys — each read hits a different ipage
    {
        let (handler, _dir) = make_handler_with_wal();
        populate(&handler, 10_000, 64);
        let mut i = 0usize;

        group.bench_function("sequential_64B", |b| {
            b.iter(|| {
                i = (i + 1) % 10_000;
                let key = format!("key:{:08}", i);
                handler.get_value(key.as_bytes())
            })
        });
    }

    group.finish();
}

// ─── Single-thread: mixed 80% GET / 20% PUT, Zipfian reads ───────────────────

fn bench_mixed_workload(c: &mut Criterion) {
    let (handler, _dir) = make_handler_with_wal();
    let n = 10_000usize;
    populate(&handler, n, 128);

    // Deterministic pseudo-random via LCG so benchmark is reproducible.
    let mut rng_state = 0x12345678u64;
    let mut lcg = move || -> f64 {
        rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        ((rng_state >> 33) as f64) / (u32::MAX as f64)
    };

    let val = make_value(128);
    let mut write_counter = 0u64;

    let mut group = c.benchmark_group("realistic/mixed_80_20");
    group.throughput(Throughput::Elements(1));

    group.bench_function("zipfian_reads_80pct", |b| {
        b.iter(|| {
            let r = lcg();
            if r < 0.80 {
                // Read: Zipfian-biased key index
                let idx = zipfian_idx(lcg(), n);
                let key = format!("key:{:08}", idx);
                handler.get_value(key.as_bytes())
            } else {
                // Write: new unique key every time
                write_counter = write_counter.wrapping_add(1);
                let key = format!("new:{}", write_counter);
                handler.put_sync(key.as_bytes(), &val, 0).ok();
                None
            }
        })
    });

    group.finish();
}

// ─── Multi-thread throughput: N concurrent threads, measure ops/sec ───────────

fn concurrent_throughput(
    handler: Arc<Handler>,
    n_threads: usize,
    duration_secs: u64,
    read_pct: f64,
    n_keys: usize,
    value_size: usize,
) -> (u64, Duration) {
    let barrier = Arc::new(Barrier::new(n_threads + 1));
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let total_ops = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let mut handles = Vec::new();
    for tid in 0..n_threads {
        let h = Arc::clone(&handler);
        let b = Arc::clone(&barrier);
        let d = Arc::clone(&done);
        let ops = Arc::clone(&total_ops);
        let val = make_value(value_size);

        handles.push(std::thread::spawn(move || {
            let mut rng = (tid as u64).wrapping_mul(0xdeadbeef12345678).wrapping_add(1);
            let mut lcg = move || -> f64 {
                rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                ((rng >> 33) as f64) / (u32::MAX as f64)
            };
            let mut local_ops = 0u64;
            let mut write_counter = (tid as u64) * 1_000_000;

            b.wait(); // sync start
            while !d.load(std::sync::atomic::Ordering::Relaxed) {
                let r = lcg();
                if r < read_pct {
                    let idx = zipfian_idx(lcg(), n_keys);
                    let key = format!("key:{:08}", idx);
                    let _ = h.get_value(key.as_bytes());
                } else {
                    write_counter = write_counter.wrapping_add(1);
                    let key = format!("wr:{}:{}", tid, write_counter);
                    let _ = h.put_sync(key.as_bytes(), &val, 0);
                }
                local_ops += 1;
            }
            ops.fetch_add(local_ops, std::sync::atomic::Ordering::Relaxed);
        }));
    }

    barrier.wait(); // release all threads
    let start = Instant::now();
    std::thread::sleep(Duration::from_secs(duration_secs));
    done.store(true, std::sync::atomic::Ordering::Relaxed);
    for h in handles { h.join().unwrap(); }
    let elapsed = start.elapsed();
    (total_ops.load(std::sync::atomic::Ordering::Relaxed), elapsed)
}

fn bench_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("realistic/concurrent");
    // One sample per configuration: we measure wall-clock ops/sec ourselves.
    group.sample_size(10);

    // Pre-populate under lock so the index is warm for all threads.
    let dir = tempdir().unwrap();
    let sm = Arc::new(SegmentManager::new(dir.path()).unwrap());
    let index = Arc::new(Index::new());
    let wb = Arc::new(WriteBuffer::new(0u32, 0usize));
    let mut handler = Handler::new(Arc::clone(&index), Arc::clone(&sm), wb);

    let wal_dir = dir.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let config = WalConfig { dir: wal_dir, ..WalConfig::default() };
    let wal = Arc::new(WriteAheadLog::new(config).unwrap());
    handler.set_wal(wal);

    let n_keys = 10_000usize;
    populate(&handler, n_keys, 128);

    let handler = Arc::new(handler);

    for n_threads in [1usize, 2, 4] {
        group.bench_with_input(
            BenchmarkId::new("80pct_read", format!("{}threads", n_threads)),
            &n_threads,
            |b, &t| {
                b.iter_custom(|_iters| {
                    // One 1-second run; return time-per-op so criterion can
                    // display latency.  Throughput = 1s / time_per_op.
                    let (ops, elapsed) = concurrent_throughput(
                        Arc::clone(&handler), t, 1, 0.80, n_keys, 128,
                    );
                    if ops == 0 { Duration::from_secs(1) }
                    else { elapsed / ops as u32 }
                })
            },
        );
    }

    group.finish();
    drop(dir);
}

criterion_group!(
    benches,
    bench_put_wal,
    bench_get_hot,
    bench_mixed_workload,
    bench_concurrent,
);
criterion_main!(benches);
