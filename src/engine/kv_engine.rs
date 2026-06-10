//! Top-level KV engine implementing the SIndex paper design.
//!
//! "SIndex: An SSD-based Large-scale Indexing with Deterministic Latency
//! for Cloud Block Storage" (ICPP '24, DOI 10.1145/3673038.3673041;
//! extended as ACM TOS 2026, DOI 10.1145/3789205).
//!
//! ## Architecture
//!
//! ```text
//!   KvEngine
//!   ├── Segment table / MLI high level (DashMap<partition_id → Partition>)
//!   │     The paper's concurrent hash table keyed by vplaneID; here the
//!   │     partition ID = top NUM_PARTITION_BITS bits of xxh3(key).
//!   │
//!   ├── Each Partition (MLI low level):
//!   │   ├── BTree — per-partition B+ tree, height ≤ BTREE_MAX_HEIGHT (= 3,
//!   │   │   as in the paper) with a dedicated RwLock per partition
//!   │   └── SegmentFile — 4 KB ipage backing store on SSD
//!   │
//!   ├── WSBCache (shared) — write-staging buffer cache, 16 clock lists;
//!   │     all ipage traffic goes through it (§4.3 of the paper)
//!   │
//!   └── ValueLog (shared) — append-only (key, value) store + redo journal
//! ```
//!
//! ## Write path (paper §4.3 write staging + §4.5 crash consistency)
//! A `put` appends to the value log (the WAL: "recording all BM updates
//! into the WAL before overwriting their related ipage") and updates the
//! B-Tree **in memory only** — modified ipages are staged dirty in the
//! WSBCache. No fsync happens on the request path. A background TSS
//! thread runs the two-stage sync every `SYNC_INTERVAL`:
//!
//! 1. Under a brief exclusive epoch guard, snapshot the dirty page set and
//!    the value-log high-water mark `P` (every entry below `P` is fully
//!    applied to the in-memory index by then).
//! 2. fsync the value log up to `P`, write + fsync the snapshot pages to
//!    their segment files, then atomically advance the on-disk checkpoint
//!    to `P`. Flushed pages stay cached (paper "buffered" state) so
//!    read-after-write is served from memory.
//!
//! ## Recovery
//! On open, the engine replays the value log from the last checkpoint:
//! ALIVE entries are re-inserted, DELETED tombstones re-applied, and a
//! torn tail (detected by magic/CRC) is truncated. Replay is idempotent —
//! re-inserting an already-indexed entry just rewrites the same pointer.
//!
//! ## Deterministic Latency Guarantee
//! A `get` touches at most `BTREE_MAX_HEIGHT` ipage reads (the B-Tree
//! traversal, each a WSBCache hit or one SSD pread) plus one value-log
//! pread — a fixed upper bound independent of data volume.
//!
//! ## Single-disk scope
//! The paper's inter-SSD scheduling (RO-/WO-SSD separation, epoch state
//! transition with Evading-flush Delay, dynamic read selection §4.4) needs
//! multiple physical SSDs and is out of scope for this single-volume
//! deployment; the staging/sync design here is what those mechanisms
//! build upon.

use std::collections::HashMap;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock, Weak};
use std::time::Duration;

use dashmap::DashMap;
use xxhash_rust::xxh3::xxh3_64;

use crate::engine::btree::BTree;
use crate::engine::ipage::{self, LeafEntry, FLAG_ALIVE};
use crate::engine::segment::SegmentFile;
use crate::engine::value_log::{ValueLog, VFLAG_DELETED};
use crate::engine::wsbcache::WsbCache;

/// Number of top-bits used to derive the partition ID.
/// 16 bits → 65 536 partitions. Keeps the segment-table size tiny
/// (< 1 MB in RAM) even at hundreds-of-billions scale.
pub const NUM_PARTITION_BITS: u32 = 16;
/// Total number of partitions.
pub const NUM_PARTITIONS: u32 = 1 << NUM_PARTITION_BITS;

/// WSBCache capacity in 4 KB pages (16 384 pages = 64 MB staged + hot set).
const WSB_CACHE_PAGES: usize = 16_384;

/// TSS sync interval. The paper defaults to 5 s epochs for multi-SSD state
/// transitions; on a single volume we sync far more often to keep the
/// post-crash replay window small.
const SYNC_INTERVAL: Duration = Duration::from_millis(50);

/// Statistics returned by [`KvEngine::compact`].
#[derive(Debug, Clone, Copy)]
pub struct CompactionStats {
    /// Value-log size before compaction.
    pub bytes_before: u64,
    /// Value-log size after compaction.
    pub bytes_after: u64,
    /// Bytes reclaimed (`bytes_before − bytes_after`).
    pub bytes_reclaimed: u64,
    /// Number of live entries rewritten into the new log.
    pub entries_compacted: usize,
}

struct Partition {
    btree: BTree,
    seg: SegmentFile,
}

impl Partition {
    fn open_or_create(
        path: &Path,
        partition_id: u32,
        cache: Arc<WsbCache>,
    ) -> io::Result<Self> {
        let seg = if path.exists() {
            SegmentFile::open(path)?
        } else {
            SegmentFile::create(path, partition_id)?
        };
        Ok(Partition {
            btree: BTree::new(partition_id, cache),
            seg,
        })
    }
}

/// The main KV engine.
pub struct KvEngine {
    data_dir: PathBuf,
    /// Segment table (MLI high level): partition_id → Partition.
    /// Partitions are created lazily on first write.
    partitions: DashMap<u32, Arc<RwLock<Partition>>>,
    /// Shared value log (data + redo journal) for all partitions.
    value_log: Arc<ValueLog>,
    /// Write-staging buffer cache shared by every partition's B-Tree.
    cache: Arc<WsbCache>,
    /// Held shared by writers for the (vlog append → index update) section;
    /// held exclusive by the sync cycle to snapshot a consistent cut.
    epoch: RwLock<()>,
    /// Serializes sync cycles (TSS thread vs explicit `flush()` calls):
    /// checkpoints must advance monotonically and the tmp+rename pair must
    /// not race itself.
    sync_lock: std::sync::Mutex<()>,
    /// Value-log offset below which the index is durable on disk.
    checkpoint: AtomicU64,
    /// Stops the TSS thread.
    shutdown: AtomicBool,
    /// When set, Drop skips the final flush (crash simulation in tests).
    skip_final_flush: AtomicBool,
}

impl KvEngine {
    /// Open (or create) a `KvEngine` rooted at `data_dir`, replaying any
    /// value-log tail past the last checkpoint, and start the TSS sync
    /// thread.
    pub fn open(data_dir: &Path) -> io::Result<Arc<Self>> {
        std::fs::create_dir_all(data_dir)?;
        let seg_dir = data_dir.join("segments");
        std::fs::create_dir_all(&seg_dir)?;

        let value_log = ValueLog::open(&data_dir.join("value.log"))?;
        let cache = WsbCache::new(WSB_CACHE_PAGES);

        let engine = Arc::new(KvEngine {
            data_dir: data_dir.to_path_buf(),
            partitions: DashMap::new(),
            value_log,
            cache,
            epoch: RwLock::new(()),
            sync_lock: std::sync::Mutex::new(()),
            checkpoint: AtomicU64::new(0),
            shutdown: AtomicBool::new(false),
            skip_final_flush: AtomicBool::new(false),
        });

        // Pre-open existing segment files.
        if let Ok(rd) = std::fs::read_dir(&seg_dir) {
            for entry in rd.flatten() {
                let p = entry.path();
                if p.extension().and_then(|e| e.to_str()) == Some("seg") {
                    if let Some(stem) = p.file_stem().and_then(|s| s.to_str()) {
                        if let Ok(pid) = stem.parse::<u32>() {
                            let part = Partition::open_or_create(
                                &p,
                                pid,
                                Arc::clone(&engine.cache),
                            )?;
                            engine.partitions.insert(pid, Arc::new(RwLock::new(part)));
                        }
                    }
                }
            }
        }

        engine.recover()?;

        // TSS background thread. It waits on a cloned cache handle and only
        // upgrades its Weak engine ref for the duration of a sync cycle, so
        // dropping the last user Arc runs the engine's final-flush Drop
        // promptly (not delayed by a sleeping thread).
        let weak: Weak<KvEngine> = Arc::downgrade(&engine);
        let wait_cache = Arc::clone(&engine.cache);
        std::thread::Builder::new()
            .name("sindex-tss".into())
            .spawn(move || loop {
                wait_cache.wait_for_work(SYNC_INTERVAL);
                match weak.upgrade() {
                    Some(engine) => {
                        if engine.shutdown.load(Ordering::Acquire) {
                            break;
                        }
                        if let Err(e) = engine.sync_cycle() {
                            eprintln!("sindex: TSS sync cycle failed: {}", e);
                        }
                    }
                    None => break,
                }
            })
            .expect("failed to spawn TSS thread");

        Ok(engine)
    }

    // ─── Recovery (paper §4.5) ───────────────────────────────────────────────

    fn checkpoint_path(&self) -> PathBuf {
        self.data_dir.join("checkpoint")
    }

    fn read_checkpoint(&self) -> u64 {
        match std::fs::read(self.checkpoint_path()) {
            Ok(bytes) if bytes.len() == 12 => {
                let pos = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
                let crc = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
                if crc32fast::hash(&bytes[0..8]) == crc {
                    pos
                } else {
                    0
                }
            }
            _ => 0,
        }
    }

    fn write_checkpoint(&self, pos: u64) -> io::Result<()> {
        let mut bytes = Vec::with_capacity(12);
        bytes.extend_from_slice(&pos.to_le_bytes());
        bytes.extend_from_slice(&crc32fast::hash(&pos.to_le_bytes()).to_le_bytes());
        // Atomic via temp + rename.
        let tmp = self.data_dir.join("checkpoint.tmp");
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&bytes)?;
            f.sync_data()?;
        }
        std::fs::rename(&tmp, self.checkpoint_path())?;
        self.checkpoint.store(pos, Ordering::Release);
        Ok(())
    }

    /// Replay the value-log tail past the checkpoint into the index, then
    /// truncate any torn tail and persist a fresh checkpoint.
    fn recover(&self) -> io::Result<()> {
        let ckpt = self.read_checkpoint().min(self.value_log.size());
        self.checkpoint.store(ckpt, Ordering::Release);

        let (valid_end, entries) = self.value_log.scan_from(ckpt)?;
        if entries.is_empty() && valid_end == self.value_log.size() && ckpt == valid_end {
            return Ok(());
        }

        for e in &entries {
            let h = xxh3_64(&e.key);
            let pid = (h >> (64 - NUM_PARTITION_BITS)) as u32;
            if e.flags == VFLAG_DELETED {
                if let Some(part_arc) = self.partitions.get(&pid).map(|p| Arc::clone(&p)) {
                    let mut guard = part_arc.write().unwrap();
                    let part = &mut *guard;
                    part.btree.delete(&mut part.seg, h)?;
                }
            } else {
                let part_arc = self.get_or_create_partition(pid)?;
                let mut guard = part_arc.write().unwrap();
                let part = &mut *guard;
                part.btree.insert(
                    &mut part.seg,
                    LeafEntry {
                        key_hash: h,
                        value_ptr: e.offset,
                        value_len: e.value_len,
                        key_len: e.key.len() as u16,
                        flags: FLAG_ALIVE,
                    },
                )?;
            }
        }

        // Drop a torn tail so future appends don't land after garbage.
        if valid_end < self.value_log.size() {
            self.value_log.truncate_to(valid_end)?;
        }

        // Persist the replayed state and advance the checkpoint.
        self.flush()
    }

    // ─── TSS sync cycle (paper §4.3 two-stage sync) ──────────────────────────

    /// One two-stage-sync cycle: snapshot a consistent cut, make the value
    /// log durable, write staged ipages to their segments, advance the
    /// checkpoint, and mark the written pages clean (they stay cached).
    fn sync_cycle(&self) -> io::Result<()> {
        let _sync = self.sync_lock.lock().unwrap();

        // Stage 0: consistent cut. The exclusive epoch guard waits out all
        // in-flight (append → index-update) sections, so every vlog entry
        // below `p` is reflected in the staged pages we snapshot.
        let (p, mut dirty) = {
            let _g = self.epoch.write().unwrap();
            (self.value_log.size(), self.cache.collect_dirty())
        };

        if dirty.is_empty() && p == self.checkpoint.load(Ordering::Acquire) {
            return Ok(());
        }

        // Stage 1: journal first — the index must never durably reference
        // value-log bytes that aren't on disk.
        self.value_log.flush()?;

        // Stage 2: write staged ipages per partition, fsync each segment.
        //
        // Three-phase per partition — the paper's key latency guarantee is
        // that reads are never stalled by the sync cycle's I/O:
        //
        // 2a. Write dirty pages under a *read* lock: `write_page` is a
        //     positional pwrite (&self) so GET operations can proceed
        //     concurrently on the same partition.
        //
        // 2b. Flush the segment header under a brief *write* lock (one 4 KB
        //     pwrite to page 0 — microseconds). Clone the file descriptor so
        //     we can fsync after releasing the lock.
        //
        // 2c. fdatasync with *no partition lock* held. GET readers hitting
        //     WSBCache never contend with this.
        let mut by_pid: HashMap<u32, Vec<usize>> = HashMap::new();
        for (i, ((pid, _), _, _)) in dirty.iter().enumerate() {
            by_pid.entry(*pid).or_default().push(i);
        }
        let mut seg_fds: Vec<std::fs::File> = Vec::with_capacity(by_pid.len());
        for (pid, idxs) in &by_pid {
            let part_arc = match self.partitions.get(pid) {
                Some(p) => Arc::clone(&p),
                None => continue, // partition cleared meanwhile
            };

            // 2a: write dirty page data — read lock (concurrent GETs unblocked)
            {
                let guard = part_arc.read().unwrap();
                for &i in idxs {
                    let ((_, page_idx), _, page) = &mut dirty[i];
                    ipage::seal(page);
                    guard.seg.write_page(*page_idx, page)?;
                }
            }

            // 2b: flush segment header — brief write lock, then clone fd
            let fd = {
                let guard = part_arc.write().unwrap();
                let fd = guard.seg.try_clone_file()?;
                guard.seg.flush_header()?;
                fd
            };
            seg_fds.push(fd);
        }

        // 2c: fdatasync all affected segments — no partition lock held
        for fd in seg_fds {
            fd.sync_data()?;
        }

        // Checkpoint: everything below `p` is now durable in the index.
        self.write_checkpoint(p)?;

        let written: Vec<_> = dirty.iter().map(|(k, g, _)| (*k, *g)).collect();
        self.cache.mark_clean(&written);
        Ok(())
    }

    // ─── Partitioning ────────────────────────────────────────────────────────

    /// Get or create the `Partition` for `partition_id`.
    fn get_or_create_partition(&self, pid: u32) -> io::Result<Arc<RwLock<Partition>>> {
        if let Some(p) = self.partitions.get(&pid) {
            return Ok(Arc::clone(p.value()));
        }
        let cache = Arc::clone(&self.cache);
        let entry = self.partitions.entry(pid).or_try_insert_with(|| {
            let path = self.seg_path(pid);
            Partition::open_or_create(&path, pid, cache).map(|p| Arc::new(RwLock::new(p)))
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
    /// Takes a shared epoch guard (prevents compaction from truncating the
    /// vlog between the B-Tree lookup and the value pread) and a shared
    /// partition lock (concurrent readers proceed in parallel).
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let h = xxh3_64(key);
        let pid = (h >> (64 - NUM_PARTITION_BITS)) as u32;

        let _epoch = self.epoch.read().unwrap();

        let part_arc = match self.partitions.get(&pid) {
            Some(a) => Arc::clone(a.value()),
            None => return Ok(None),
        };

        let entry = {
            let guard = part_arc.read().unwrap();
            guard.btree.get(&guard.seg, h)?
        };

        let entry = match entry {
            Some(e) => e,
            None => return Ok(None),
        };

        // One positional read for key + value; comparing the stored key
        // guards against hash collisions.
        let (stored_key, value) =
            self.value_log
                .read_key_value(entry.value_ptr, entry.key_len, entry.value_len)?;
        if stored_key != key {
            return Ok(None);
        }
        Ok(Some(value))
    }

    /// Insert or update `(key, value)`.
    ///
    /// Appends to the value log (journal) and stages the B-Tree update in
    /// the WSBCache — no fsync on the request path (paper write staging).
    pub fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let h = xxh3_64(key);
        let pid = (h >> (64 - NUM_PARTITION_BITS)) as u32;

        let _epoch = self.epoch.read().unwrap();

        let part_arc = self.get_or_create_partition(pid)?;
        let mut guard = part_arc.write().unwrap();

        // Journal first (WAL before ipage update, paper §4.5).
        let value_ptr = self.value_log.append(key, value)?;

        let part = &mut *guard;
        part.btree.insert(
            &mut part.seg,
            LeafEntry {
                key_hash: h,
                value_ptr,
                value_len: value.len() as u32,
                key_len: key.len() as u16,
                flags: FLAG_ALIVE,
            },
        )?;
        Ok(())
    }

    /// Delete `key`. Returns `true` if the key existed.
    ///
    /// Appends a tombstone to the value log so recovery replays the delete.
    pub fn delete(&self, key: &[u8]) -> io::Result<bool> {
        let h = xxh3_64(key);
        let pid = (h >> (64 - NUM_PARTITION_BITS)) as u32;

        let part_arc = match self.partitions.get(&pid) {
            Some(a) => Arc::clone(a.value()),
            None => return Ok(false),
        };

        let _epoch = self.epoch.read().unwrap();
        let mut guard = part_arc.write().unwrap();
        let part = &mut *guard;
        let found = part.btree.delete(&mut part.seg, h)?;
        if found {
            self.value_log.append_tombstone(key)?;
        }
        Ok(found)
    }

    /// Check if `key` exists without fetching the value.
    pub fn exists(&self, key: &[u8]) -> io::Result<bool> {
        let h = xxh3_64(key);
        let pid = (h >> (64 - NUM_PARTITION_BITS)) as u32;

        let _epoch = self.epoch.read().unwrap();

        let part_arc = match self.partitions.get(&pid) {
            Some(a) => Arc::clone(a.value()),
            None => return Ok(false),
        };

        let entry = {
            let guard = part_arc.read().unwrap();
            guard.btree.get(&guard.seg, h)?
        };

        match entry {
            None => Ok(false),
            Some(e) => {
                let stored_key = self.value_log.read_key(e.value_ptr, e.key_len)?;
                Ok(stored_key == key)
            }
        }
    }

    /// Flush all staged state to disk (full TSS cycle). Durable on return.
    pub fn flush(&self) -> io::Result<()> {
        self.sync_cycle()
    }

    /// Compact the value log by rewriting only live entries.
    ///
    /// Every `put` appends a new record to the value log; overwritten and
    /// deleted values leave dead space that is never reclaimed during normal
    /// operation. This method reclaims that space:
    ///
    /// 1. Walk every B-Tree partition and collect all live `(key, value)` pairs.
    /// 2. Clear all on-disk and in-memory state (segments + vlog).
    /// 3. Re-insert every live pair, rebuilding a fresh vlog and B-Tree.
    /// 4. Flush the rebuilt state to disk atomically.
    ///
    /// This is a **stop-the-world** operation. The sync lock and exclusive
    /// epoch guard are held for the entire duration: no reads, writes, or
    /// background sync cycles can run concurrently. The flush is done inline
    /// (not via `sync_cycle`) to avoid re-acquiring `epoch.write()`.
    pub fn compact(&self) -> io::Result<CompactionStats> {
        // Take sync_lock first (matches sync_cycle's acquisition order) so
        // any in-progress sync_cycle finishes before we start.
        let _sync = self.sync_lock.lock().unwrap();

        let bytes_before = self.value_log.size();

        // Exclusive epoch: drain all in-flight readers/writers.
        let _epoch = self.epoch.write().unwrap();

        // Collect all live entries (key + value).
        let mut live: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for entry in self.partitions.iter() {
            let guard = entry.value().read().unwrap();
            let btree_entries = guard.btree.iter_entries(&guard.seg)?;
            for e in btree_entries {
                let (key, value) = self.value_log.read_key_value(
                    e.value_ptr, e.key_len, e.value_len,
                )?;
                live.push((key, value));
            }
        }
        let entries_compacted = live.len();

        // Wipe all in-memory and on-disk state.
        self.partitions.clear();
        self.cache.clear();
        let seg_dir = self.data_dir.join("segments");
        if seg_dir.exists() {
            for entry in std::fs::read_dir(&seg_dir)?.flatten() {
                let _ = std::fs::remove_file(entry.path());
            }
        }
        self.value_log.truncate()?;
        self.checkpoint.store(0, Ordering::Release);

        // Rebuild index and vlog from the live set.
        for (key, value) in &live {
            let h = xxh3_64(key);
            let pid = (h >> (64 - NUM_PARTITION_BITS)) as u32;
            let part_arc = self.get_or_create_partition(pid)?;
            let mut guard = part_arc.write().unwrap();
            let value_ptr = self.value_log.append(key, value)?;
            let part = &mut *guard;
            part.btree.insert(
                &mut part.seg,
                LeafEntry {
                    key_hash: h,
                    value_ptr,
                    value_len: value.len() as u32,
                    key_len: key.len() as u16,
                    flags: FLAG_ALIVE,
                },
            )?;
        }

        let bytes_after = self.value_log.size();

        // Flush inline (cannot call sync_cycle here — that would deadlock on
        // epoch.write() which we're already holding).
        self.value_log.flush()?;

        let mut dirty = self.cache.collect_dirty();
        let mut by_pid: HashMap<u32, Vec<usize>> = HashMap::new();
        for (i, ((pid, _), _, _)) in dirty.iter().enumerate() {
            by_pid.entry(*pid).or_default().push(i);
        }

        let mut seg_fds: Vec<std::fs::File> = Vec::with_capacity(by_pid.len());
        for (pid, idxs) in &by_pid {
            let part_arc = match self.partitions.get(pid) {
                Some(p) => Arc::clone(&p),
                None => continue,
            };
            {
                let guard = part_arc.read().unwrap();
                for &i in idxs {
                    let ((_, page_idx), _, page) = &mut dirty[i];
                    ipage::seal(page);
                    guard.seg.write_page(*page_idx, page)?;
                }
            }
            let fd = {
                let guard = part_arc.write().unwrap();
                let f = guard.seg.try_clone_file()?;
                guard.seg.flush_header()?;
                f
            };
            seg_fds.push(fd);
        }
        for fd in seg_fds {
            fd.sync_data()?;
        }

        self.write_checkpoint(bytes_after)?;

        let written: Vec<_> = dirty.iter().map(|(k, g, _)| (*k, *g)).collect();
        self.cache.mark_clean(&written);

        Ok(CompactionStats {
            bytes_before,
            bytes_after,
            bytes_reclaimed: bytes_before.saturating_sub(bytes_after),
            entries_compacted,
        })
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
        let _epoch = self.epoch.read().unwrap();
        let mut result = Vec::new();
        for entry in self.partitions.iter() {
            let guard = entry.value().read().unwrap();
            let entries = guard.btree.iter_entries(&guard.seg)?;
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

        // Filter first, then paginate — slicing before filtering would skip
        // matched keys and under-fill pages when a pattern is active.
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
        let _g = self.epoch.write().unwrap(); // drain in-flight writers
        self.partitions.clear();
        self.cache.clear();
        let seg_dir = self.data_dir.join("segments");
        if seg_dir.exists() {
            for entry in std::fs::read_dir(&seg_dir)?.flatten() {
                let _ = std::fs::remove_file(entry.path());
            }
        }
        // Truncate the value log in-place so self.value_log remains valid.
        self.value_log.truncate()?;
        self.write_checkpoint(0)
    }

    /// Test hook: drop the engine *without* the final flush, as a crash
    /// would. The TSS thread is stopped first so it can't flush either.
    #[cfg(test)]
    pub fn simulate_crash(self: Arc<Self>) {
        self.shutdown.store(true, Ordering::Release);
        self.skip_final_flush.store(true, Ordering::Release);
        drop(self);
    }
}

impl Drop for KvEngine {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if !self.skip_final_flush.load(Ordering::Acquire) {
            let _ = self.sync_cycle();
        }
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

        // Small value
        engine.put(b"s", b"x").unwrap();
        // Large value (1 MB)
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
    fn crash_recovery_replays_value_log() {
        let dir = tempdir().unwrap();
        {
            let engine = KvEngine::open(dir.path()).unwrap();
            for i in 0u64..30 {
                let k = format!("key{}", i);
                let v = format!("val{}", i);
                engine.put(k.as_bytes(), v.as_bytes()).unwrap();
            }
            engine.delete(b"key7").unwrap();
            // Crash: no flush, no TSS cycle, staged index pages are lost.
            engine.simulate_crash();
        }
        {
            // The value log survives; recovery must replay it.
            let engine = KvEngine::open(dir.path()).unwrap();
            for i in 0u64..30 {
                let k = format!("key{}", i);
                if i == 7 {
                    assert_eq!(engine.get(k.as_bytes()).unwrap(), None, "tombstone lost");
                } else {
                    let v = format!("val{}", i);
                    assert_eq!(
                        engine.get(k.as_bytes()).unwrap(),
                        Some(v.into_bytes()),
                        "key{} lost after crash",
                        i
                    );
                }
            }
        }
    }

    #[test]
    fn drop_without_explicit_flush_is_durable() {
        let dir = tempdir().unwrap();
        {
            let engine = KvEngine::open(dir.path()).unwrap();
            engine.put(b"k", b"v").unwrap();
            // No explicit flush: Drop must run a final sync cycle.
        }
        {
            let engine = KvEngine::open(dir.path()).unwrap();
            assert_eq!(engine.get(b"k").unwrap(), Some(b"v".to_vec()));
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
    fn concurrent_readers_and_writer() {
        let dir = tempdir().unwrap();
        let engine = KvEngine::open(dir.path()).unwrap();
        for i in 0..200 {
            engine
                .put(format!("k{:03}", i).as_bytes(), b"seed")
                .unwrap();
        }

        let mut handles = Vec::new();
        for t in 0..4 {
            let eng = Arc::clone(&engine);
            handles.push(std::thread::spawn(move || {
                for round in 0..200 {
                    let i = (t * 53 + round * 7) % 200;
                    let k = format!("k{:03}", i);
                    if t == 0 {
                        eng.put(k.as_bytes(), format!("v{}", round).as_bytes())
                            .unwrap();
                    } else {
                        let _ = eng.get(k.as_bytes()).unwrap();
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
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
        assert_eq!(cursor, 0); // all done in one page
        assert_eq!(keys.len(), 100);
    }

    #[test]
    fn clear_then_reuse() {
        let dir = tempdir().unwrap();
        let engine = KvEngine::open(dir.path()).unwrap();
        engine.put(b"a", b"1").unwrap();
        engine.clear().unwrap();
        assert_eq!(engine.get(b"a").unwrap(), None);
        engine.put(b"b", b"2").unwrap();
        assert_eq!(engine.get(b"b").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn compact_reclaims_dead_vlog_space() {
        let dir = tempdir().unwrap();
        let engine = KvEngine::open(dir.path()).unwrap();

        // Write 50 keys, then overwrite each three times so 3/4 of the vlog is dead.
        for i in 0u32..50 {
            let k = format!("k{:04}", i);
            engine.put(k.as_bytes(), b"v1").unwrap();
            engine.put(k.as_bytes(), b"v2").unwrap();
            engine.put(k.as_bytes(), b"v3").unwrap();
            engine.put(k.as_bytes(), format!("final-{}", i).as_bytes()).unwrap();
        }
        // Delete a handful to ensure tombstones are also gone after compaction.
        for i in 40..50u32 {
            engine.delete(format!("k{:04}", i).as_bytes()).unwrap();
        }

        let stats = engine.compact().unwrap();
        assert!(stats.bytes_reclaimed > 0, "no space was reclaimed");
        assert_eq!(stats.entries_compacted, 40); // 50 − 10 deleted
        assert!(stats.bytes_after < stats.bytes_before);

        // All remaining keys still readable, deleted keys gone.
        for i in 0u32..40 {
            let k = format!("k{:04}", i);
            let expected = format!("final-{}", i);
            assert_eq!(
                engine.get(k.as_bytes()).unwrap(),
                Some(expected.into_bytes())
            );
        }
        for i in 40..50u32 {
            assert_eq!(engine.get(format!("k{:04}", i).as_bytes()).unwrap(), None);
        }

        // Durable: survives a reopen.
        engine.flush().unwrap();
        drop(engine);
        let engine = KvEngine::open(dir.path()).unwrap();
        for i in 0u32..40 {
            let k = format!("k{:04}", i);
            let expected = format!("final-{}", i);
            assert_eq!(engine.get(k.as_bytes()).unwrap(), Some(expected.into_bytes()));
        }
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
