//! Throughput benchmarks: new SIndex-inspired ipage engine.
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use tempfile::tempdir;

use ssd_kv::engine::Index;
use ssd_kv::engine::index_entry::RecordLocation;
use ssd_kv::perf::{LockFreeBloomFilter, BloomFilter};
use ssd_kv::server::Handler;
use ssd_kv::storage::{SegmentManager, WriteBuffer};

// ─── Bloom filter ─────────────────────────────────────────────────────────────

fn bench_bloom_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("bloom_filter");
    group.throughput(Throughput::Elements(1));

    let mut bf = BloomFilter::new(1_000_000, 0.01);
    for i in 0..10000u64 { bf.add(i); }

    group.bench_function("regular_may_contain", |b| {
        let mut i = 0u64;
        b.iter(|| { i = i.wrapping_add(1); bf.may_contain(i % 10000) })
    });

    let lfbf = LockFreeBloomFilter::new(1_000_000, 0.01);
    for i in 0..10000u64 { lfbf.add(i); }

    group.bench_function("lockfree_may_contain", |b| {
        let mut i = 0u64;
        b.iter(|| { i = i.wrapping_add(1); lfbf.may_contain(i % 10000) })
    });

    group.bench_function("lockfree_add", |b| {
        let mut i = 10000u64;
        b.iter(|| { i = i.wrapping_add(1); lfbf.add(i) })
    });

    group.finish();
}

// ─── Index operations ─────────────────────────────────────────────────────────

fn bench_index_operations(c: &mut Criterion) {
    let index = Index::new();

    for i in 0..100_000u32 {
        let key = format!("index_key_{:08}", i);
        let loc = RecordLocation::ipage(0, i, 0);
        index.insert(key.as_bytes(), loc, i, 100);
    }

    let mut group = c.benchmark_group("index");
    group.throughput(Throughput::Elements(1));

    group.bench_function("get_hit", |b| {
        let mut i = 0usize;
        b.iter(|| {
            i = (i + 1) % 100_000;
            let key = format!("index_key_{:08}", i);
            index.get(key.as_bytes())
        })
    });

    group.bench_function("insert", |b| {
        let mut i = 100_000u32;
        b.iter(|| {
            i = i.wrapping_add(1);
            let key = format!("new_key_{}", i);
            let loc = RecordLocation::ipage(0, i, 0);
            index.insert(key.as_bytes(), loc, i, 100)
        })
    });

    group.finish();
}

// ─── Handler KV operations ────────────────────────────────────────────────────

fn make_handler() -> (Handler, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let sm = Arc::new(SegmentManager::new(dir.path()).unwrap());
    let index = Arc::new(Index::new());
    let wb = Arc::new(WriteBuffer::new(0u32, 0usize));
    let handler = Handler::new(Arc::clone(&index), Arc::clone(&sm), wb);
    (handler, dir)
}

fn bench_handler_small(c: &mut Criterion) {
    let (handler, _dir) = make_handler();

    // Pre-populate 10 K keys.
    for i in 0..10_000u32 {
        let key = format!("bench_key_{:06}", i);
        let val = format!("bench_value_{:06}", i);
        handler.put_sync(key.as_bytes(), val.as_bytes(), 0).unwrap();
    }

    let mut group = c.benchmark_group("handler/small_value");
    group.throughput(Throughput::Elements(1));

    group.bench_function("get", |b| {
        let key = b"bench_key_000001";
        b.iter(|| handler.get_value(key))
    });

    group.bench_function("put", |b| {
        let mut i = 100_000u32;
        let value = b"small_value_16B!";
        b.iter(|| {
            i = i.wrapping_add(1);
            let key = format!("put_key_{}", i);
            handler.put_sync(key.as_bytes(), value, 0)
        })
    });

    group.bench_function("put_then_get", |b| {
        let mut i = 200_000u32;
        let value = b"small_value_16B!";
        b.iter(|| {
            i = i.wrapping_add(1);
            let key = format!("rw_key_{}", i);
            handler.put_sync(key.as_bytes(), value, 0).unwrap();
            handler.get_value(key.as_bytes())
        })
    });

    group.bench_function("delete", |b| {
        let mut i = 0u32;
        b.iter(|| {
            let key = format!("bench_key_{:06}", i % 10_000);
            i = i.wrapping_add(1);
            handler.delete_sync(key.as_bytes())
        })
    });

    group.finish();
}

fn bench_handler_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("handler/large_value");
    group.throughput(Throughput::Elements(1));

    // Benchmark PUT for various large value sizes.
    for size in [4_096usize, 16_384, 65_536, 262_144] {
        let (handler, _dir) = make_handler();
        let value = vec![0xABu8; size];

        group.bench_with_input(
            BenchmarkId::new("put", format!("{}B", size)),
            &size,
            |b, _| {
                let mut i = 0u32;
                b.iter(|| {
                    i = i.wrapping_add(1);
                    let key = format!("large_key_{}", i);
                    handler.put_sync(key.as_bytes(), &value, 0)
                })
            },
        );
    }

    group.finish();
}

fn bench_handler_large_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("handler/large_value_get");
    group.throughput(Throughput::Elements(1));

    for size in [4_096usize, 16_384, 65_536] {
        let (handler, _dir) = make_handler();
        let value = vec![0xABu8; size];
        let key = format!("large_key_{}", size);
        handler.put_sync(key.as_bytes(), &value, 0).unwrap();

        group.bench_with_input(
            BenchmarkId::new("get", format!("{}B", size)),
            &size,
            |b, _| b.iter(|| handler.get_value(key.as_bytes())),
        );
    }

    group.finish();
}

// ─── ipage raw encode/decode ──────────────────────────────────────────────────

fn bench_ipage_raw(c: &mut Criterion) {
    use ssd_kv::storage::ipage::IPage;

    let mut group = c.benchmark_group("ipage");
    group.throughput(Throughput::Elements(1));

    group.bench_function("try_append_small", |b| {
        let mut page = IPage::new();
        let key = b"benchmark_key";
        let val = b"benchmark_value_data_here";
        let mut slot = 0u16;
        b.iter(|| {
            if page.try_append(key, val, 1, 0, 0, false).is_err() {
                page = IPage::new();
            }
        })
    });

    group.bench_function("read_entry", |b| {
        let mut page = IPage::new();
        let key = b"benchmark_key";
        let val = b"benchmark_value_data_here";
        let slot = page.try_append(key, val, 1, 1_000_000, 0, false).unwrap();
        b.iter(|| page.read_entry(slot))
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_bloom_filter,
    bench_index_operations,
    bench_handler_small,
    bench_handler_large,
    bench_handler_large_get,
    bench_ipage_raw,
);
criterion_main!(benches);
