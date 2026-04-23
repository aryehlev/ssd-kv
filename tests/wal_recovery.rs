//! Integration test: WAL durability under simulated crash.
//!
//! We can't really crash the process mid-test, but we can prove the
//! durability invariant the WAL promises: if `put_sync` returned, the
//! record is on disk — even if we drop the Handler immediately without
//! flushing write_buffer to data files, a fresh Handler pointing at the
//! same data dir must recover it from the WAL.

use std::sync::Arc;
use std::time::Duration;

use ssd_kv::engine::{recover_with_wal, Index};
use ssd_kv::server::Handler;
use ssd_kv::storage::wal::{WalConfig, WriteAheadLog};
use ssd_kv::storage::{FileManager, WriteBuffer};

use tempfile::tempdir;

fn make_handler(
    data_dir: &std::path::Path,
) -> (
    Arc<Handler>,
    Arc<FileManager>,
    Arc<Index>,
    Arc<WriteBuffer>,
    Arc<WriteAheadLog>,
) {
    let fm = Arc::new(FileManager::new(data_dir).unwrap());
    if fm.file_count() == 0 {
        fm.create_file().unwrap();
    }
    let idx = Arc::new(Index::new());
    let wb = Arc::new(WriteBuffer::new(fm.file_count() as u32, 1023));

    let wal_dir = data_dir.join("wal");
    let wal = Arc::new(
        WriteAheadLog::new(WalConfig {
            dir: wal_dir,
            fsync_interval: Duration::from_micros(200),
            fsync_batch: 16,
            ..Default::default()
        })
        .unwrap(),
    );

    let mut handler = Handler::new(Arc::clone(&idx), Arc::clone(&fm), Arc::clone(&wb));
    let _ = recover_with_wal(&handler, &fm, &wal).unwrap();
    handler.set_wal(Arc::clone(&wal));

    (Arc::new(handler), fm, idx, wb, wal)
}

#[test]
fn wal_survives_handler_drop_before_flush() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path();

    // First run: PUT 500 keys and immediately drop without flushing the
    // write_buffer. If the WAL works, the records are durable anyway.
    {
        let (handler, _fm, _idx, _wb, wal) = make_handler(data_dir);
        for i in 0..500u32 {
            let key = format!("key_{:04}", i);
            let val = format!("value_{:04}_{}", i, "x".repeat(16));
            handler.put_sync(key.as_bytes(), val.as_bytes(), 0).unwrap();
        }
        // Intentionally: no handler.flush(); no WriteBuffer flush.
        // We drop the handler and the WAL simulating a clean shutdown
        // that never flushed pending writes.
        drop(handler);
        // Give the commit thread a beat to drain anything pending; drop
        // itself forces a final sync but not under load.
        drop(wal);
    }

    // Second run: fresh Handler, same data dir. Recovery should replay the
    // WAL and all 500 keys must be readable.
    {
        let (handler, _fm, _idx, _wb, _wal) = make_handler(data_dir);
        for i in 0..500u32 {
            let key = format!("key_{:04}", i);
            let val = format!("value_{:04}_{}", i, "x".repeat(16));
            let got = handler.get_value(key.as_bytes());
            assert_eq!(
                got.as_deref(),
                Some(val.as_bytes()),
                "lost record after restart: {}",
                key
            );
        }
    }
}

#[test]
fn wal_survives_concurrent_writes_then_restart() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path();

    // Fan out 8 writers, 100 puts each.
    {
        let (handler, _fm, _idx, _wb, wal) = make_handler(data_dir);
        let mut joins = Vec::new();
        for t in 0..8u32 {
            let h = Arc::clone(&handler);
            joins.push(std::thread::spawn(move || {
                for i in 0..100u32 {
                    let key = format!("t{}_k{}", t, i);
                    let val = format!("t{}_v{}", t, i);
                    h.put_sync(key.as_bytes(), val.as_bytes(), 0).unwrap();
                }
            }));
        }
        for j in joins {
            j.join().unwrap();
        }
        drop(handler);
        drop(wal);
    }

    // Recover and verify every single key.
    {
        let (handler, _fm, _idx, _wb, _wal) = make_handler(data_dir);
        let mut missing = 0;
        for t in 0..8u32 {
            for i in 0..100u32 {
                let key = format!("t{}_k{}", t, i);
                let val = format!("t{}_v{}", t, i);
                if handler.get_value(key.as_bytes()).as_deref() != Some(val.as_bytes()) {
                    missing += 1;
                }
            }
        }
        assert_eq!(missing, 0, "{} records lost after restart", missing);
    }
}

#[test]
fn wal_delete_tombstone_replayed() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path();

    {
        let (handler, _fm, _idx, _wb, wal) = make_handler(data_dir);
        handler.put_sync(b"alive", b"yes", 0).unwrap();
        handler.put_sync(b"doomed", b"soon", 0).unwrap();
        assert!(handler.delete_sync(b"doomed").unwrap());
        drop(handler);
        drop(wal);
    }

    {
        let (handler, _fm, _idx, _wb, _wal) = make_handler(data_dir);
        assert_eq!(handler.get_value(b"alive").as_deref(), Some(&b"yes"[..]));
        assert_eq!(handler.get_value(b"doomed"), None);
    }
}
