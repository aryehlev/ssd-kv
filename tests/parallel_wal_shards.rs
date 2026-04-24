//! Integration test: parallel WAL shards.
//!
//! Verifies the sharded-WAL durability invariant: with N > 1 shards
//! wired into a single Handler, writes dispatched to different shards
//! all survive a clean restart, and recovery merges every shard's
//! entries into the index.
//!
//! This test doesn't exercise the reactor — it directly uses
//! `Handler::put_nowait_on(shard_id, ...)` and the WAL's durable wait,
//! which is the contract the reactor relies on.

use std::sync::Arc;
use std::time::Duration;

use ssd_kv::engine::{recover_with_wal, Index};
use ssd_kv::server::Handler;
use ssd_kv::storage::wal::{WalConfig, WriteAheadLog};
use ssd_kv::storage::{FileManager, WriteBuffer};

use tempfile::tempdir;

fn open_with_shards(
    data_dir: &std::path::Path,
    num_shards: usize,
) -> (Handler, Vec<Arc<WriteAheadLog>>) {
    let fm = Arc::new(FileManager::new(data_dir).unwrap());
    if fm.file_count() == 0 {
        fm.create_file().unwrap();
    }
    let idx = Arc::new(Index::new());
    let wb = Arc::new(WriteBuffer::new(fm.file_count() as u32, 1023));

    let shards: Vec<Arc<WriteAheadLog>> = (0..num_shards)
        .map(|i| {
            let dir = data_dir.join(format!("wal_shard_{}", i));
            Arc::new(
                WriteAheadLog::new(WalConfig {
                    dir,
                    fsync_interval: Duration::from_micros(200),
                    fsync_batch: 16,
                    ..Default::default()
                })
                .unwrap(),
            )
        })
        .collect();

    let mut handler = Handler::new(Arc::clone(&idx), Arc::clone(&fm), Arc::clone(&wb));
    // Replay every shard BEFORE wiring them in — otherwise replayed
    // records re-append to the log.
    for w in &shards {
        let _ = recover_with_wal(&handler, &fm, w).unwrap();
    }
    handler.set_wal_shards(shards.clone());

    (handler, shards)
}

#[test]
fn multi_shard_writes_survive_restart() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path();

    // Session 1: write 200 keys split across 4 shards (round-robin).
    {
        let (handler, shards) = open_with_shards(data_dir, 4);
        for i in 0..200u32 {
            let shard = (i as usize) % 4;
            let key = format!("k-{:03}", i).into_bytes();
            let val = format!("v-{:03}", i).into_bytes();
            let pos = handler
                .put_nowait_on(shard, &key, &val, 0)
                .unwrap()
                .expect("wal_pos expected when sharded WAL is configured");
            // Wait for the specific shard's commit thread to make this
            // record durable before we drop the handler — that's the
            // contract the reactor relies on.
            shards[shard].wait_for_durable(pos);
        }
    }

    // Session 2: reopen, replay all shards, verify every key recovers.
    let (handler, _) = open_with_shards(data_dir, 4);
    for i in 0..200u32 {
        let key = format!("k-{:03}", i).into_bytes();
        let expected = format!("v-{:03}", i).into_bytes();
        match handler.get_value(&key) {
            Some(v) => assert_eq!(v, expected, "wrong value for key {}", i),
            None => panic!("key {} missing after multi-shard recovery", i),
        }
    }
}

#[test]
fn overwrite_on_different_shard_honours_generation() {
    // Same key written first on shard 0 then on shard 2 — after
    // recovery the LATER generation must win. Tests that shards share
    // a generation space via the Handler's atomic counter.
    let dir = tempdir().unwrap();
    let data_dir = dir.path();

    {
        let (handler, shards) = open_with_shards(data_dir, 3);
        let key = b"dup-key";

        let pos0 = handler.put_nowait_on(0, key, b"first", 0).unwrap().unwrap();
        shards[0].wait_for_durable(pos0);

        let pos2 = handler.put_nowait_on(2, key, b"second", 0).unwrap().unwrap();
        shards[2].wait_for_durable(pos2);
    }

    let (handler, _) = open_with_shards(data_dir, 3);
    assert_eq!(
        handler.get_value(b"dup-key").as_deref(),
        Some(b"second".as_slice()),
        "later generation on a different shard must win after recovery",
    );
}

#[test]
fn out_of_range_shard_hint_clamps_rather_than_panicking() {
    // A reactor_id > num_shards shouldn't crash — it should clamp to
    // the last shard. Verify that by feeding a huge hint and expecting
    // the write to land on shard N-1 (and durable-wait to work via
    // that shard).
    let dir = tempdir().unwrap();
    let (handler, shards) = open_with_shards(dir.path(), 2);

    let pos = handler
        .put_nowait_on(99, b"clamped-key", b"v", 0)
        .unwrap()
        .expect("expected wal_pos");
    // Must be within shard 1's range (the last shard).
    shards[1].wait_for_durable(pos);

    assert_eq!(
        handler.get_value(b"clamped-key").as_deref(),
        Some(b"v".as_slice()),
    );
}
