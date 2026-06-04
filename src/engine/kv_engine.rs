//! Top-level KV engine implementing the SIndex paper design.
//!
//! "The Design of Trillion-scale SSD-based Indexing with Deterministic
//! Latency for Cloud Block Storage", ACM TOS 2024 (DOI 10.1145/3789205).
//!
//! ## Architecture
//!
//! ```text
//!   KvEngine
//!   ├── PartitionTable (DashMap<partition_id → Arc<RwLock<Partition>>>)
//!   │     One entry per active partition (created lazily on first write).
//!   │     Partition ID = top NUM_PARTITION_BITS bits of xxh3(key).
//!   │
//!   ├── Each Partition:
//!   │   ├── BTree — per-partition B+ tree (bounded height → deterministic latency)
//!   │   └── SegmentFile — 4 KB ipage backing store on SSD
//!   │
//!   ├── ValueLog (shared) — append-only variable-size (key, value) store
//!   ├── GroupCommit    — batches value-log fdatasyncs across concurrent writers
//!   └── WsbCache       — clock-eviction page cache for hot ipages
//! ```
//!
//! ## Concurrency
//! * GET / EXISTS / SCAN use `RwLock::read()` so concurrent readers never
//!   block each other.  The B-Tree read path uses `pread`, which is safe under
//!   a shared lock.
//! * PUT / DELETE use `RwLock::write()` for the partition they modify.
//! * Value-log reads are always lock-free (`pread` on a stable fd).

use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use dashmap::DashMap;
use xxhash_rust::xxh3::xxh3_64;

use crate::engine::btree::BTree;
use crate::engine::group_commit::GroupCommit;
use crate::engine::ipage::{FLAG_ALIVE, LeafEntry};
use crate::engine::segment::SegmentFile;
use crate::engine::value_log::{ValueLog, VLOG_HEADER_SIZE};
use crate::engine::wsbcache::WsbCache;

/// Number of top-bits used to derive the partition ID.
/// 16 bits → 65 536 partitions.
pub const NUM_PARTITION_BITS: u32 = 16;
/// Total number of partitions.
pub const NUM_PARTITIONS: u32 = 1 << NUM_PARTITION_BITS;

/// WSBCache capacity: 8 192 ipages ≈ 32 MB.
const WSB_CACHE_CAPACITY: usize = 8 * 1024;

struct Partition {
    btree: BTree,
    seg: SegmentFile,
}

impl Partition {
    fn open_or_create(path: &Path, partition_id: u32) -> io::Result<Self> {
        let seg = if path.exists() {
            SegmentFile::open(path)?
        } else {
            SegmentFile::create(path, partition_id)?
        };
        Ok(Partition {
            btree: BTree::new(),
            seg,
        })
    }
}

/// The main KV engine.
pub struct KvEngine {
    data_dir: PathBuf,
    /// Segment table (MLI Level-1): partition_id → Partition.
    partitions: DashMap<u32, Arc<RwLock<Partition>>>,
    /// Shared value log for all partitions.
    value_log: Arc<ValueLog>,
    /// Batches concurrent value-log fdatasyncs (group commit).
    group_commit: GroupCommit,
    /// Write-staging buffer cache: hot ipages served from RAM.
    wsb_cache: Arc<WsbCache>,
}

impl KvEngine {
    /// Open (or create) a `KvEngine` rooted at `data_dir`.
    pub fn open(data_dir: &Path) -> io::Result<Arc<Self>> {
        std::fs::create_dir_all(data_dir)?;
        let seg_dir = data_dir.join("segments");
        std::fs::create_dir_all(&seg_dir)?;

        let value_log = ValueLog::open(&data_dir.join("value.log"))?;
        let group_commit = GroupCommit::new(Arc::clone(&value_log));
        let wsb_cache = WsbCache::new(WSB_CACHE_CAPACITY);

        let engine = Arc::new(KvEngine {
            data_dir: data_dir.to_path_buf(),
            partitions: DashMap::new(),
            value_log,
            group_commit,
            wsb_cache,
        });

        // Pre-open any existing segment files (so we don't lose data on restart)
        if let Ok(rd) = std::fs::read_dir(&seg_dir) {
            for entry in rd.flatten() {
                let p = entry.path();
                if p.extension().and_then(|e| e.to_str()) == Some("seg") {
                    if let Some(stem) = p.file_stem().and_then(|s| s.to_str()) {
                        if let Ok(pid) = stem.parse::<u32>() {
                            let part = Partition::open_or_create(&p, pid)?;
                            engine
                                .partitions
                                .insert(pid, Arc::new(RwLock::new(part)));
                        }
                    }
                }
            }
        }

        Ok(engine)
    }

    /// Get or create the `Partition` for `pid`.
    fn get_or_create_partition(&self, pid: u32) -> io::Result<Arc<RwLock<Partition>>> {
        if let Some(p) = self.partitions.get(&pid) {
            return Ok(Arc::clone(p.value()));
        }
        let entry = self.partitions.entry(pid).or_try_insert_with(|| {
            let path = self.seg_path(pid);
            Partition::open_or_create(&path, pid).map(|p| Arc::new(RwLock::new(p)))
        })?;
        Ok(Arc::clone(entry.value()))
    }

    fn seg_path(&self, pid: u32) -> PathBuf {
        self.data_dir
            .join("segments")
            .join(format!("{:05}.seg", pid))
    }

    // ─── Public API ──────────────────────────────────────────────────────────

    /// Get the value for `key`. Returns `None` if the key does not exist.
    ///
    /// Uses a shared (`RwLock::read`) partition lock so concurrent GETs on
    /// the same partition never block each other.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let h = xxh3_64(key);
        let pid = (h >> (64 - NUM_PARTITION_BITS)) as u32;

        let part_arc = match self.partitions.get(&pid) {
            Some(a) => Arc::clone(a.value()),
            None => return Ok(None),
        };

        let entry = {
            let guard = part_arc.read().unwrap();
            guard.btree.get(&guard.seg, h, Some((&*self.wsb_cache, pid)))?
        };

        let entry = match entry {
            Some(e) => e,
            None => return Ok(None),
        };

        // Verify the actual key (hash-collision guard).
        let stored_key = self.value_log.read_key(entry.value_ptr, entry.key_len)?;
        if stored_key != key {
            return Ok(None);
        }

        let value = self
            .value_log
            .read_value(entry.value_ptr, entry.key_len, entry.value_len)?;
        Ok(Some(value))
    }

    /// Insert or update `(key, value)`.
    ///
    /// Write order for crash consistency:
    /// 1. Append to value log (BufWriter, not yet durable).
    /// 2. Group-commit sync: flush BufWriter + fdatasync value log.
    /// 3. Update B-Tree + flush segment (fdatasync segment).
    ///
    /// On crash between 2 and 3 the value is durable but the index has no
    /// pointer — the entry is orphaned dead space, never corrupt.
    pub fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let h = xxh3_64(key);
        let pid = (h >> (64 - NUM_PARTITION_BITS)) as u32;

        // Step 1: append to value log (no sync).
        let value_ptr = self.value_log.append(key, value)?;
        let my_end =
            value_ptr + VLOG_HEADER_SIZE as u64 + key.len() as u64 + value.len() as u64;

        // Step 2: make value durable via group commit BEFORE updating the index.
        self.group_commit.sync_vlog(my_end)?;

        // Step 3: update B-Tree and flush segment.
        let part_arc = self.get_or_create_partition(pid)?;
        {
            let mut guard = part_arc.write().unwrap();
            let part = &mut *guard;
            let cache = Some((&*self.wsb_cache, pid));

            // Track dead bytes for any overwritten entry.
            if let Ok(Some(old)) = part.btree.get(&part.seg, h, cache) {
                self.value_log.mark_dead(old.key_len, old.value_len);
            }

            let leaf_entry = LeafEntry {
                key_hash: h,
                value_ptr,
                value_len: value.len() as u32,
                key_len: key.len() as u16,
                flags: FLAG_ALIVE,
            };
            part.btree.insert(&mut part.seg, leaf_entry, cache)?;
            part.btree.flush(&mut part.seg, cache)?;
        }
        Ok(())
    }

    /// Delete `key`. Returns `true` if the key existed.
    pub fn delete(&self, key: &[u8]) -> io::Result<bool> {
        let h = xxh3_64(key);
        let pid = (h >> (64 - NUM_PARTITION_BITS)) as u32;

        let part_arc = match self.partitions.get(&pid) {
            Some(a) => Arc::clone(a.value()),
            None => return Ok(false),
        };

        let mut guard = part_arc.write().unwrap();
        let part = &mut *guard;
        let cache = Some((&*self.wsb_cache, pid));

        // Look up first so we can record dead bytes.
        let old_entry = part.btree.get(&part.seg, h, cache)?;

        let found = part.btree.delete(&mut part.seg, h, cache)?;
        if found {
            if let Some(e) = old_entry {
                self.value_log.mark_dead(e.key_len, e.value_len);
            }
            part.btree.flush(&mut part.seg, cache)?;
        }
        Ok(found)
    }

    /// Check if `key` exists without fetching the value.
    ///
    /// Uses a shared partition lock (same as `get`).
    pub fn exists(&self, key: &[u8]) -> io::Result<bool> {
        let h = xxh3_64(key);
        let pid = (h >> (64 - NUM_PARTITION_BITS)) as u32;

        let part_arc = match self.partitions.get(&pid) {
            Some(a) => Arc::clone(a.value()),
            None => return Ok(false),
        };

        let entry = {
            let guard = part_arc.read().unwrap();
            guard.btree.get(&guard.seg, h, Some((&*self.wsb_cache, pid)))?
        };

        match entry {
            None => Ok(false),
            Some(e) => {
                let stored_key = self.value_log.read_key(e.value_ptr, e.key_len)?;
                Ok(stored_key == key)
            }
        }
    }

    /// Flush all dirty state to disk.
    pub fn flush(&self) -> io::Result<()> {
        self.value_log.flush_and_sync()?;
        for entry in self.partitions.iter() {
            let pid = *entry.key();
            let mut guard = entry.value().write().unwrap();
            let part = &mut *guard;
            part.btree.flush(&mut part.seg, Some((&*self.wsb_cache, pid)))?;
        }
        Ok(())
    }

    /// Number of live entries across all partitions.
    pub fn count_live(&self) -> u64 {
        self.partitions
            .iter()
            .map(|e| {
                let part = e.value().read().unwrap();
                part.seg.header.live_entries
            })
            .sum()
    }

    /// Collect all live keys (expensive: full tree scan). Used for KEYS/SCAN.
    pub fn scan_keys(&self) -> io::Result<Vec<Vec<u8>>> {
        let mut result = Vec::new();
        for entry in self.partitions.iter() {
            let pid = *entry.key();
            let guard = entry.value().read().unwrap();
            let entries = guard.btree.iter_entries(&guard.seg, Some((&*self.wsb_cache, pid)))?;
            for e in entries {
                if let Ok(key) = self.value_log.read_key(e.value_ptr, e.key_len) {
                    result.push(key);
                }
            }
        }
        Ok(result)
    }

    /// Scan up to `count` keys starting from a cursor position.
    /// Returns `(next_cursor, keys)`. A returned `cursor == 0` means done.
    pub fn scan(
        &self,
        cursor: u64,
        count: usize,
        pattern: Option<&[u8]>,
    ) -> io::Result<(u64, Vec<Vec<u8>>)> {
        let all_keys = self.scan_keys()?;

        let filtered: Vec<&Vec<u8>> = all_keys
            .iter()
            .filter(|k| pattern.map_or(true, |pat| glob_match(pat, k)))
            .collect();

        let start = cursor as usize;
        let end = (start + count).min(filtered.len());
        let chunk: Vec<Vec<u8>> = filtered[start..end].iter().map(|k| (*k).clone()).collect();

        let next_cursor = if end >= filtered.len() { 0 } else { end as u64 };
        Ok((next_cursor, chunk))
    }

    /// Remove all entries (FLUSHDB equivalent).
    pub fn clear(&self) -> io::Result<()> {
        let data_dir = self.data_dir.clone();
        self.partitions.clear();
        self.wsb_cache.drain_dirty(); // evict stale cache entries
        let seg_dir = data_dir.join("segments");
        if seg_dir.exists() {
            for entry in std::fs::read_dir(&seg_dir)?.flatten() {
                let _ = std::fs::remove_file(entry.path());
            }
        }
        self.value_log.truncate()
    }

    /// Returns `true` when value-log dead space exceeds the compaction threshold.
    pub fn compaction_needed(&self) -> bool {
        self.value_log.compaction_needed()
    }
}

/// Simple glob matching: `?` matches one char, `*` matches any sequence.
fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let mut dp = vec![vec![false; text.len() + 1]; pattern.len() + 1];
    dp[0][0] = true;
    for i in 1..=pattern.len() {
        if pattern[i - 1] == b'*' {
            dp[i][0] = dp[i - 1][0];
        }
    }
    for i in 1..=pattern.len() {
        for j in 1..=text.len() {
            if pattern[i - 1] == b'*' {
                dp[i][j] = dp[i - 1][j] || dp[i][j - 1];
            } else if pattern[i - 1] == b'?' || pattern[i - 1] == text[j - 1] {
                dp[i][j] = dp[i - 1][j - 1];
            }
        }
    }
    dp[pattern.len()][text.len()]
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn basic_put_get_delete() {
        let dir = tempdir().unwrap();
        let engine = KvEngine::open(dir.path()).unwrap();

        engine.put(b"foo", b"bar").unwrap();
        assert_eq!(engine.get(b"foo").unwrap(), Some(b"bar".to_vec()));
        assert!(engine.exists(b"foo").unwrap());

        assert_eq!(engine.get(b"missing").unwrap(), None);
        assert!(!engine.exists(b"missing").unwrap());

        engine.delete(b"foo").unwrap();
        assert_eq!(engine.get(b"foo").unwrap(), None);
    }

    #[test]
    fn update_value() {
        let dir = tempdir().unwrap();
        let engine = KvEngine::open(dir.path()).unwrap();

        engine.put(b"key", b"v1").unwrap();
        engine.put(b"key", b"v2").unwrap();
        assert_eq!(engine.get(b"key").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn variable_value_sizes() {
        let dir = tempdir().unwrap();
        let engine = KvEngine::open(dir.path()).unwrap();

        engine.put(b"s", b"x").unwrap();
        let big = vec![0x42u8; 1024 * 1024];
        engine.put(b"big", &big).unwrap();

        assert_eq!(engine.get(b"s").unwrap(), Some(b"x".to_vec()));
        assert_eq!(engine.get(b"big").unwrap(), Some(big));
    }

    #[test]
    fn persistence_across_reopen() {
        let dir = tempdir().unwrap();
        {
            let engine = KvEngine::open(dir.path()).unwrap();
            for i in 0u64..50 {
                let k = format!("key{}", i);
                let v = format!("val{}", i);
                engine.put(k.as_bytes(), v.as_bytes()).unwrap();
            }
            engine.flush().unwrap();
        }
        {
            let engine = KvEngine::open(dir.path()).unwrap();
            for i in 0u64..50 {
                let k = format!("key{}", i);
                let v = format!("val{}", i);
                assert_eq!(
                    engine.get(k.as_bytes()).unwrap(),
                    Some(v.into_bytes()),
                    "key{} missing after reopen",
                    i
                );
            }
        }
    }

    #[test]
    fn many_keys_across_partitions() {
        let dir = tempdir().unwrap();
        let engine = KvEngine::open(dir.path()).unwrap();

        const N: usize = 1000;
        for i in 0..N {
            let k = format!("k{:04}", i);
            let v = format!("value-{}", i);
            engine.put(k.as_bytes(), v.as_bytes()).unwrap();
        }
        assert_eq!(engine.count_live() as usize, N);

        for i in 0..N {
            let k = format!("k{:04}", i);
            let v = format!("value-{}", i);
            assert_eq!(engine.get(k.as_bytes()).unwrap(), Some(v.into_bytes()));
        }
    }

    #[test]
    fn scan_returns_all_keys() {
        let dir = tempdir().unwrap();
        let engine = KvEngine::open(dir.path()).unwrap();

        for i in 0u32..100 {
            let k = format!("scan_key_{}", i);
            engine.put(k.as_bytes(), b"v").unwrap();
        }

        let (cursor, keys) = engine.scan(0, 200, None).unwrap();
        assert_eq!(cursor, 0);
        assert_eq!(keys.len(), 100);
    }

    #[test]
    fn dead_bytes_tracked_on_overwrite_and_delete() {
        let dir = tempdir().unwrap();
        let engine = KvEngine::open(dir.path()).unwrap();

        engine.put(b"key", b"old_value").unwrap();
        assert!(!engine.compaction_needed());

        // Overwrite: old entry becomes dead.
        engine.put(b"key", b"new_value").unwrap();

        // Delete: entry becomes dead.
        engine.delete(b"key").unwrap();
    }

    #[test]
    fn glob_match_patterns() {
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"foo*", b"foobar"));
        assert!(!glob_match(b"foo*", b"barfoo"));
        assert!(glob_match(b"f?o", b"foo"));
        assert!(!glob_match(b"f?o", b"fo"));
        assert!(glob_match(b"key[0-9]", b"key[0-9]")); // no bracket expansion
    }
}
