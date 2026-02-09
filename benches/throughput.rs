//! Throughput benchmarks for SSD-KV optimizations.
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::sync::Arc;
use tempfile::tempdir;

use ssd_kv::engine::Index;
use ssd_kv::perf::{LockFreeBloomFilter, BloomFilter};
use ssd_kv::server::Handler;
use ssd_kv::storage::{FileManager, WriteBuffer, ParallelFileManager};

fn bench_bloom_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("bloom_filter");
    group.throughput(Throughput::Elements(1));

    // Regular bloom filter (with lock)
    let mut bf = BloomFilter::new(1_000_000, 0.01);
    for i in 0..10000u64 {
        bf.add(i);
    }

    group.bench_function("regular_may_contain", |b| {
        let mut i = 0u64;
        b.iter(|| {
            i = i.wrapping_add(1);
            bf.may_contain(i % 10000)
        })
    });

    // Lock-free bloom filter
    let lfbf = LockFreeBloomFilter::new(1_000_000, 0.01);
    for i in 0..10000u64 {
        lfbf.add(i);
    }

    group.bench_function("lockfree_may_contain", |b| {
        let mut i = 0u64;
        b.iter(|| {
            i = i.wrapping_add(1);
            lfbf.may_contain(i % 10000)
        })
    });

    group.bench_function("lockfree_add", |b| {
        let mut i = 10000u64;
        b.iter(|| {
            i = i.wrapping_add(1);
            lfbf.add(i)
        })
    });

    group.finish();
}

fn bench_handler_operations(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let file_manager = Arc::new(FileManager::new(dir.path()).unwrap());
    let index = Arc::new(Index::new());
    let write_buffer = Arc::new(WriteBuffer::new(0, 1023));

    file_manager.create_file().unwrap();

    let handler = Handler::new(
        Arc::clone(&index),
        Arc::clone(&file_manager),
        Arc::clone(&write_buffer),
    );

    // Pre-populate with some keys
    for i in 0..10000 {
        let key = format!("bench_key_{:06}", i);
        let value = format!("bench_value_{:06}", i);
        handler.put_sync(key.as_bytes(), value.as_bytes(), 0).unwrap();
    }

    let mut group = c.benchmark_group("handler");
    group.throughput(Throughput::Elements(1));

    // GET from hot cache (fastest path)
    group.bench_function("get_cache_hit", |b| {
        let key = b"bench_key_000001";
        b.iter(|| {
            handler.get_value(key)
        })
    });

    // PUT (small value, inline storage)
    group.bench_function("put_small", |b| {
        let mut i = 100000u32;
        let value = b"small_value_data";
        b.iter(|| {
            i = i.wrapping_add(1);
            let key = format!("put_key_{}", i);
            handler.put_sync(key.as_bytes(), value, 0)
        })
    });

    group.finish();
}

fn bench_index_operations(c: &mut Criterion) {
    let index = Index::new();

    // Pre-populate
    for i in 0..100000 {
        let key = format!("index_key_{:08}", i);
        let location = ssd_kv::storage::write_buffer::DiskLocation::new(0, 0, i as u32);
        index.insert(key.as_bytes(), location, i as u32, 100);
    }

    let mut group = c.benchmark_group("index");
    group.throughput(Throughput::Elements(1));

    group.bench_function("get_hit", |b| {
        let mut i = 0usize;
        b.iter(|| {
            i = (i + 1) % 100000;
            let key = format!("index_key_{:08}", i);
            index.get(key.as_bytes())
        })
    });

    group.bench_function("insert", |b| {
        let mut i = 100000u32;
        b.iter(|| {
            i = i.wrapping_add(1);
            let key = format!("new_key_{}", i);
            let location = ssd_kv::storage::write_buffer::DiskLocation::new(0, 0, i);
            index.insert(key.as_bytes(), location, i, 100)
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_bloom_filter,
    bench_handler_operations,
    bench_index_operations,
);
criterion_main!(benches);
