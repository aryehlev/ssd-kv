//! Integration test: GC compacts segments and all live keys remain readable.
//!
//! Uses a 1 MiB segment (255 data pages) so we can fill and seal a segment
//! without writing huge amounts of data. With 200-byte values and 8-byte keys,
//! each page holds ~17 entries, giving ~4335 entries per segment.
//! We write 5000 entries (> one full segment), delete 80%, then run GC and
//! verify every surviving key is still readable.

use std::sync::Arc;
use std::time::Duration;

use ssd_kv::engine::{recover_with_wal, Index};
use ssd_kv::server::Handler;
use ssd_kv::storage::gc::{GcConfig, GcRunner};
use ssd_kv::storage::wal::{WalConfig, WriteAheadLog};
use ssd_kv::storage::{FileManager, SegmentConfig, WriteBuffer};

use tempfile::tempdir;

fn make_handler_small_seg(
    data_dir: &std::path::Path,
) -> (Arc<Handler>, Arc<FileManager>, Arc<Index>, Arc<WriteBuffer>, Arc<WriteAheadLog>) {
    // 1 MiB segments: 255 data pages of 4 KB each. Small so tests fill them fast.
    let config = SegmentConfig {
        segment_size: 1 * 1024 * 1024,
        gc_threshold: 0.5,
        inline_value_max: 3800,
    };
    let fm = Arc::new(FileManager::with_config(data_dir, config).unwrap());
    let idx = Arc::new(Index::new());
    let wb = Arc::new(WriteBuffer::new(fm.file_count() as u32, 1023));

    let wal_dir = data_dir.join("wal");
    let wal = Arc::new(
        WriteAheadLog::new(WalConfig {
            dir: wal_dir,
            fsync_interval: Duration::from_micros(500),
            fsync_batch: 32,
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
fn gc_compacts_segment_and_reads_survive() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path();
    let (handler, fm, idx, _wb, _wal) = make_handler_small_seg(data_dir);

    // Each ipage holds ~17 entries (200-byte value + 8-byte key + 24B header + 4B slot).
    // 1 MiB segment has 255 data pages → ~4335 entries per segment.
    // Write 5000 keys to guarantee at least one full segment is sealed.
    let n_write = 5000usize;
    let value_bytes = vec![b'x'; 200];

    for i in 0..n_write {
        let key = format!("k:{:06}", i);
        handler.put_sync(key.as_bytes(), &value_bytes, 0).unwrap();
    }

    // Flush to ensure the active ipage is sealed and GC tracking is up to date.
    fm.flush().unwrap();

    // Confirm at least one sealed segment is tracked for GC.
    let sealed = fm.gc_sealed_segments(1.01); // threshold >1.0 catches everything
    assert!(
        !sealed.is_empty(),
        "expected sealed segments after {n_write} writes; got none"
    );

    // All keys should be readable right after writing.
    for i in 0..n_write {
        let key = format!("k:{:06}", i);
        let v = handler.get_value(key.as_bytes());
        assert!(v.is_some(), "key {key} missing before GC");
    }

    // Delete 80% of the keys — leaves utilisation at 0.2, below the 0.5 threshold.
    let keep_step = 5; // keep every 5th key (20%)
    let mut kept_keys: Vec<usize> = Vec::new();
    let mut deleted_keys: Vec<usize> = Vec::new();
    for i in 0..n_write {
        if i % keep_step == 0 {
            kept_keys.push(i);
        } else {
            deleted_keys.push(i);
            let key = format!("k:{:06}", i);
            handler.delete_sync(key.as_bytes()).unwrap();
        }
    }

    // Synchronous GC: compact all candidates in one pass (no background thread).
    // Use max_writes_per_sec=0 to disable throttling so the test runs fast.
    let gc = GcRunner::new(
        Arc::clone(&idx),
        Arc::clone(&fm),
        GcConfig { check_interval_secs: 0, gc_threshold: 0.5, max_writes_per_sec: 0 },
    );
    let (relocated, _skipped) = gc.run_once();
    assert!(relocated > 0, "GC relocated 0 entries — no compaction occurred");

    // All kept keys must still be readable after GC.
    let mut failures = 0usize;
    for &i in &kept_keys {
        let key = format!("k:{:06}", i);
        let v = handler.get_value(key.as_bytes());
        if v.is_none() {
            eprintln!("FAIL: key {key} missing after GC");
            failures += 1;
            if failures >= 10 {
                break;
            }
        } else {
            assert_eq!(v.unwrap(), value_bytes, "key {key} value corrupted after GC");
        }
    }
    assert_eq!(failures, 0, "{failures} keys missing after GC compaction");

    // Deleted keys must NOT be returned.
    for &i in deleted_keys.iter().take(200) {
        let key = format!("k:{:06}", i);
        let v = handler.get_value(key.as_bytes());
        assert!(v.is_none(), "deleted key {key} reappeared after GC");
    }
}
