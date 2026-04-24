//! Integration test for all three WAL sync modes.
//!
//! Each mode must satisfy the same contract: after a clean restart,
//! `replay()` returns every entry that was acknowledged by
//! `append_put`. The sync modes differ in HOW the bytes reach the
//! disk, not in what recovery sees.

use std::sync::Arc;
use std::time::Duration;

use ssd_kv::storage::wal::{WalConfig, WalSyncMode, WriteAheadLog};

use tempfile::tempdir;

fn make_wal(dir: &std::path::Path, mode: WalSyncMode) -> Arc<WriteAheadLog> {
    Arc::new(
        WriteAheadLog::new(WalConfig {
            dir: dir.to_path_buf(),
            fsync_interval: Duration::from_micros(500),
            fsync_batch: 16,
            sync_mode: mode,
            ..Default::default()
        })
        .unwrap(),
    )
}

fn serial_roundtrip(mode: WalSyncMode) {
    let dir = tempdir().unwrap();
    {
        let wal = make_wal(dir.path(), mode);
        for i in 0..20u64 {
            let key = format!("k-{:03}", i);
            let val = format!("v-{:03}", i);
            wal.append_put(key.as_bytes(), val.as_bytes(), i, 0)
                .unwrap();
        }
    }
    let wal = make_wal(dir.path(), WalSyncMode::Buffered);
    let mut count = 0usize;
    let mut keys = std::collections::HashSet::new();
    wal.replay(|_h, k, _v| {
        count += 1;
        keys.insert(k);
        Ok(())
    })
    .unwrap();
    assert_eq!(count, 20, "{:?}: expected 20 replayed entries", mode);
    assert_eq!(keys.len(), 20, "{:?}: expected 20 distinct keys", mode);
}

fn concurrent_roundtrip(mode: WalSyncMode) {
    let dir = tempdir().unwrap();
    {
        let wal = make_wal(dir.path(), mode);
        let mut handles = Vec::new();
        for t in 0..4u64 {
            let wal = Arc::clone(&wal);
            handles.push(std::thread::spawn(move || {
                for i in 0..125u64 {
                    let key = format!("k-{:02}-{:03}", t, i);
                    let val = format!("v-{:02}-{:03}", t, i);
                    wal.append_put(
                        key.as_bytes(),
                        val.as_bytes(),
                        t * 10_000 + i,
                        0,
                    )
                    .unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }
    let wal = make_wal(dir.path(), WalSyncMode::Buffered);
    let mut count = 0usize;
    let mut keys = std::collections::HashSet::new();
    wal.replay(|_h, k, _v| {
        count += 1;
        keys.insert(k);
        Ok(())
    })
    .unwrap();
    assert_eq!(count, 500, "{:?}: expected 500 entries", mode);
    assert_eq!(keys.len(), 500, "{:?}: expected 500 distinct keys", mode);
}

#[test]
fn serial_buffered() {
    serial_roundtrip(WalSyncMode::Buffered);
}
#[test]
fn serial_odirect() {
    serial_roundtrip(WalSyncMode::ODirect);
}
#[test]
fn serial_odirect_no_fsync() {
    serial_roundtrip(WalSyncMode::ODirectNoFsync);
}

#[test]
fn concurrent_buffered() {
    concurrent_roundtrip(WalSyncMode::Buffered);
}
#[test]
fn concurrent_odirect() {
    concurrent_roundtrip(WalSyncMode::ODirect);
}
#[test]
fn concurrent_odirect_no_fsync() {
    concurrent_roundtrip(WalSyncMode::ODirectNoFsync);
}
