//! Write-Ahead Log (WAL) for fast sequential writes.
//!
//! The WAL provides:
//! - Fast sequential writes (no random I/O)
//! - Durability guarantees
//! - Crash recovery
//! - Async background flush to main storage
//!
//! Write path:
//! 1. Append to WAL (sequential, fast)
//! 2. Update in-memory index
//! 3. Return success to client
//! 4. Background: flush WAL to main storage
//!
//! This is similar to Aerospike's streaming write buffer approach.

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

/// WAL entry header (24 bytes).
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct WalEntryHeader {
    /// Magic number for validation
    pub magic: u32,
    /// Entry type (PUT, DELETE)
    pub entry_type: u8,
    /// Flags
    pub flags: u8,
    /// Key length
    pub key_len: u16,
    /// Value length
    pub value_len: u32,
    /// Generation number
    pub generation: u32,
    /// TTL (seconds, 0 = no expiry)
    pub ttl: u32,
    /// CRC32 of key + value
    pub crc: u32,
}

const WAL_MAGIC: u32 = 0x57414C21; // "WAL!"
const WAL_ENTRY_PUT: u8 = 1;
const WAL_ENTRY_DELETE: u8 = 2;
const WAL_HEADER_SIZE: usize = std::mem::size_of::<WalEntryHeader>();

impl WalEntryHeader {
    pub fn new_put(key_len: u16, value_len: u32, generation: u32, ttl: u32) -> Self {
        Self {
            magic: WAL_MAGIC,
            entry_type: WAL_ENTRY_PUT,
            flags: 0,
            key_len,
            value_len,
            generation,
            ttl,
            crc: 0, // Calculated later
        }
    }

    pub fn new_delete(key_len: u16, generation: u32) -> Self {
        Self {
            magic: WAL_MAGIC,
            entry_type: WAL_ENTRY_DELETE,
            flags: 0,
            key_len,
            value_len: 0,
            generation,
            ttl: 0,
            crc: 0,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.magic == WAL_MAGIC
    }

    pub fn is_put(&self) -> bool {
        self.entry_type == WAL_ENTRY_PUT
    }

    pub fn is_delete(&self) -> bool {
        self.entry_type == WAL_ENTRY_DELETE
    }

    pub fn to_bytes(&self) -> [u8; WAL_HEADER_SIZE] {
        unsafe { std::mem::transmute_copy(self) }
    }

    pub fn from_bytes(bytes: &[u8; WAL_HEADER_SIZE]) -> Self {
        unsafe { std::mem::transmute_copy(bytes) }
    }
}

/// WAL entry for the write queue.
pub struct WalEntry {
    pub header: WalEntryHeader,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// WAL configuration.
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// WAL directory
    pub dir: PathBuf,
    /// Maximum WAL file size (default: 64MB)
    pub max_file_size: u64,
    /// Buffer size for the internal BufWriter (default: 1MB)
    pub buffer_size: usize,
    /// Group-commit batch budget: the commit thread wakes at least this often
    /// even if no writer pokes it. Smaller = lower p99, higher syscall rate.
    pub fsync_interval: Duration,
    /// Maximum writers allowed to stack up before the commit thread wakes
    /// early and issues a sync_data. 0 means "only tick-based". Setting this
    /// to something like 256 bounds worst-case queue depth at high QPS.
    pub fsync_batch: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("./wal"),
            max_file_size: 64 * 1024 * 1024, // 64MB
            buffer_size: 1024 * 1024,        // 1MB
            fsync_interval: Duration::from_micros(500),
            fsync_batch: 256,
        }
    }
}

/// WAL statistics.
#[derive(Debug, Default)]
pub struct WalStats {
    pub entries_written: AtomicU64,
    pub bytes_written: AtomicU64,
    pub syncs: AtomicU64,
    pub flushes: AtomicU64,
    pub current_file_size: AtomicU64,
}

/// Group-commit coordinator shared between writers and the commit thread.
/// Writers bump `written_pos` after staging bytes into the BufWriter and then
/// wait for `durable_pos` to catch up. The commit thread periodically flushes
/// the BufWriter, calls `sync_data()`, publishes `written_pos` as the new
/// `durable_pos`, and wakes everyone on `cond`.
struct GroupCommit {
    /// Bytes staged into the BufWriter (pre-fsync).
    written_pos: AtomicU64,
    /// Bytes durably on disk (post-fsync).
    durable_pos: AtomicU64,
    /// Count of writers blocked waiting for fsync; the commit thread wakes
    /// early when this crosses `fsync_batch`.
    waiters: AtomicU64,
    cond_lock: Mutex<()>,
    cond: Condvar,
}

impl GroupCommit {
    fn new(initial_pos: u64) -> Self {
        Self {
            written_pos: AtomicU64::new(initial_pos),
            durable_pos: AtomicU64::new(initial_pos),
            waiters: AtomicU64::new(0),
            cond_lock: Mutex::new(()),
            cond: Condvar::new(),
        }
    }

    /// Block until `durable_pos >= target`.
    fn wait_for_durable(&self, target: u64) {
        if self.durable_pos.load(Ordering::Acquire) >= target {
            return;
        }
        self.waiters.fetch_add(1, Ordering::AcqRel);
        let mut guard = self.cond_lock.lock();
        while self.durable_pos.load(Ordering::Acquire) < target {
            self.cond.wait(&mut guard);
        }
        self.waiters.fetch_sub(1, Ordering::AcqRel);
    }

    /// Publish a new durable position and wake all waiters.
    fn publish(&self, pos: u64) {
        self.durable_pos.store(pos, Ordering::Release);
        let _g = self.cond_lock.lock();
        self.cond.notify_all();
    }
}

/// Write-Ahead Log for durability and fast writes.
///
/// Writers stage their bytes into a BufWriter under a mutex, record their
/// end-position, then block on a Condvar until a dedicated commit thread
/// flushes + calls `sync_data()` on the underlying file and publishes the
/// advanced durable position. Many writes amortize one fsync — the Aerospike
/// streaming-write-buffer shape.
pub struct WriteAheadLog {
    config: WalConfig,
    /// Current WAL file. Arc so the commit thread can share the same mutex.
    writer: Arc<Mutex<BufWriter<File>>>,
    /// Current file sequence number
    file_seq: AtomicU64,
    /// Current staged position in file (bytes appended, pre-fsync).
    position: AtomicU64,
    /// Statistics
    stats: Arc<WalStats>,
    /// Group-commit coordinator
    gc: Arc<GroupCommit>,
    /// Background commit thread
    sync_thread: Option<JoinHandle<()>>,
    /// Shutdown flag
    shutdown: Arc<AtomicBool>,
}

impl WriteAheadLog {
    /// Create a new WAL.
    pub fn new(config: WalConfig) -> io::Result<Self> {
        std::fs::create_dir_all(&config.dir)?;

        // Find latest WAL file or create new one
        let file_seq = Self::find_latest_seq(&config.dir)?;
        let file_path = Self::wal_file_path(&config.dir, file_seq);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)?;

        let position = file.metadata()?.len();

        let writer = Arc::new(Mutex::new(BufWriter::with_capacity(
            config.buffer_size.max(4096),
            file,
        )));

        let mut wal = Self {
            config,
            writer,
            file_seq: AtomicU64::new(file_seq),
            position: AtomicU64::new(position),
            stats: Arc::new(WalStats::default()),
            gc: Arc::new(GroupCommit::new(position)),
            sync_thread: None,
            shutdown: Arc::new(AtomicBool::new(false)),
        };

        // Start background commit thread
        wal.start_sync_thread();

        Ok(wal)
    }

    /// Find the latest WAL sequence number.
    fn find_latest_seq(dir: &Path) -> io::Result<u64> {
        let mut max_seq = 0u64;

        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.starts_with("wal_") && name.ends_with(".log") {
                        if let Ok(seq) = name[4..name.len() - 4].parse::<u64>() {
                            max_seq = max_seq.max(seq);
                        }
                    }
                }
            }
        }

        Ok(max_seq)
    }

    /// Get WAL file path for a sequence number.
    fn wal_file_path(dir: &Path, seq: u64) -> PathBuf {
        dir.join(format!("wal_{:08}.log", seq))
    }

    /// Start the background group-commit thread.
    ///
    /// Wakes every `fsync_interval` OR as soon as `fsync_batch` writers are
    /// stacked up waiting, flushes the BufWriter, issues a `sync_data`, and
    /// publishes the new durable position so all waiters release together.
    fn start_sync_thread(&mut self) {
        let stats = Arc::clone(&self.stats);
        let shutdown = Arc::clone(&self.shutdown);
        let gc = Arc::clone(&self.gc);
        let writer_handle = Arc::clone(&self.writer);
        let interval = self.config.fsync_interval;
        let batch = self.config.fsync_batch as u64;

        let handle = thread::Builder::new()
            .name("wal-commit".to_string())
            .spawn(move || {
                let mut last_tick = Instant::now();
                loop {
                    if shutdown.load(Ordering::Relaxed) {
                        // Final flush before exit so nothing dangles.
                        Self::commit_once(&writer_handle, &gc, &stats);
                        return;
                    }

                    let now = Instant::now();
                    let elapsed = now.duration_since(last_tick);
                    let stacked = gc.waiters.load(Ordering::Relaxed);

                    // Wake on timer OR batch threshold.
                    if elapsed < interval && (batch == 0 || stacked < batch) {
                        let nap = (interval - elapsed).min(Duration::from_micros(250));
                        thread::sleep(nap);
                        continue;
                    }

                    Self::commit_once(&writer_handle, &gc, &stats);
                    last_tick = Instant::now();
                }
            })
            .expect("Failed to spawn WAL commit thread");

        self.sync_thread = Some(handle);
    }

    /// Flush BufWriter → `sync_data()` → publish the new durable position.
    /// Takes the writer mutex only for as long as flush + fsync need it.
    fn commit_once(
        writer: &Arc<Mutex<BufWriter<File>>>,
        gc: &Arc<GroupCommit>,
        stats: &Arc<WalStats>,
    ) {
        let target = gc.written_pos.load(Ordering::Acquire);
        if target == gc.durable_pos.load(Ordering::Acquire) {
            return; // nothing new, skip the syscall
        }
        let mut w = writer.lock();
        if let Err(e) = w.flush() {
            tracing::error!("wal: BufWriter flush failed: {}", e);
            return;
        }
        // sync_data: persist contents without forcing a metadata flush
        // (metadata doesn't change on append). Aerospike shape.
        if let Err(e) = w.get_ref().sync_data() {
            tracing::error!("wal: sync_data failed: {}", e);
            return;
        }
        drop(w);
        stats.syncs.fetch_add(1, Ordering::Relaxed);
        gc.publish(target);
    }

    /// Append a PUT entry to the WAL.
    pub fn append_put(&self, key: &[u8], value: &[u8], generation: u32, ttl: u32) -> io::Result<u64> {
        let mut header = WalEntryHeader::new_put(
            key.len() as u16,
            value.len() as u32,
            generation,
            ttl,
        );

        // Calculate CRC
        header.crc = self.calculate_crc(key, value);

        self.append_entry(&header, key, Some(value))
    }

    /// Append a DELETE entry to the WAL.
    pub fn append_delete(&self, key: &[u8], generation: u32) -> io::Result<u64> {
        let header = WalEntryHeader::new_delete(key.len() as u16, generation);
        self.append_entry(&header, key, None)
    }

    /// Append an entry to the WAL and block until it is durable.
    ///
    /// The hot path: grab the writer mutex, memcpy bytes into the BufWriter,
    /// bump `written_pos`, release the mutex, wait on the commit thread.
    /// Many concurrent calls see one fsync at the backend.
    fn append_entry(
        &self,
        header: &WalEntryHeader,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> io::Result<u64> {
        let entry_size = WAL_HEADER_SIZE + key.len() + value.map(|v| v.len()).unwrap_or(0);

        // Check if we need to rotate
        let current_pos = self.position.load(Ordering::Relaxed);
        if current_pos + entry_size as u64 > self.config.max_file_size {
            self.rotate()?;
        }

        let new_pos = {
            let mut writer = self.writer.lock();

            writer.write_all(&header.to_bytes())?;
            writer.write_all(key)?;
            if let Some(value) = value {
                writer.write_all(value)?;
            }

            let new_pos = self.position.fetch_add(entry_size as u64, Ordering::Relaxed)
                + entry_size as u64;

            self.stats.entries_written.fetch_add(1, Ordering::Relaxed);
            self.stats.bytes_written.fetch_add(entry_size as u64, Ordering::Relaxed);
            self.stats.current_file_size.store(new_pos, Ordering::Relaxed);

            // Publish the staged position so the commit thread knows how far
            // to advance `durable_pos` on the next flush.
            self.gc.written_pos.store(new_pos, Ordering::Release);

            new_pos
        };

        // Block until this record's bytes are durably on disk.
        self.gc.wait_for_durable(new_pos);

        Ok(new_pos)
    }

    /// Calculate CRC32 of key and value.
    fn calculate_crc(&self, key: &[u8], value: &[u8]) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(key);
        hasher.update(value);
        hasher.finalize()
    }

    /// Rotate to a new WAL file.
    fn rotate(&self) -> io::Result<()> {
        let new_seq = self.file_seq.fetch_add(1, Ordering::Relaxed) + 1;
        let new_path = Self::wal_file_path(&self.config.dir, new_seq);

        let new_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&new_path)?;

        let mut writer = self.writer.lock();

        // Flush + durable-sync the previous file so we don't lose records
        // across a rotation.
        writer.flush()?;
        writer.get_ref().sync_data()?;

        // Switch to new file. The new file starts at offset 0 in its own
        // space; however the group-commit counters (written_pos/durable_pos)
        // are process-lifetime monotonic, so we treat the rotation as a
        // boundary where we just reset `position` (the in-file offset used
        // for max_file_size checks). The gc counters keep going.
        *writer = BufWriter::with_capacity(self.config.buffer_size.max(4096), new_file);
        self.position.store(0, Ordering::Relaxed);

        self.stats.flushes.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Force a synchronous flush + sync_data of the WAL. Rarely needed by
    /// callers now that `append_entry` is itself group-commit durable;
    /// provided for shutdown paths.
    pub fn sync(&self) -> io::Result<()> {
        let mut writer = self.writer.lock();
        writer.flush()?;
        writer.get_ref().sync_data()?;
        drop(writer);
        let target = self.gc.written_pos.load(Ordering::Acquire);
        self.gc.publish(target);
        self.stats.syncs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Get WAL statistics.
    pub fn stats(&self) -> &WalStats {
        &self.stats
    }

    /// Replay WAL entries for recovery.
    pub fn replay<F>(&self, mut callback: F) -> io::Result<u64>
    where
        F: FnMut(WalEntryHeader, Vec<u8>, Vec<u8>) -> io::Result<()>,
    {
        let mut count = 0u64;

        // Find all WAL files
        let mut files: Vec<u64> = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&self.config.dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.starts_with("wal_") && name.ends_with(".log") {
                        if let Ok(seq) = name[4..name.len() - 4].parse::<u64>() {
                            files.push(seq);
                        }
                    }
                }
            }
        }

        files.sort();

        // Replay each file in order
        for seq in files {
            let path = Self::wal_file_path(&self.config.dir, seq);
            let mut file = File::open(&path)?;

            let mut header_buf = [0u8; WAL_HEADER_SIZE];

            loop {
                // Read header
                match file.read_exact(&mut header_buf) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(e),
                }

                let header = WalEntryHeader::from_bytes(&header_buf);

                if !header.is_valid() {
                    break; // Corrupted entry, stop replay
                }

                // Read key
                let mut key = vec![0u8; header.key_len as usize];
                file.read_exact(&mut key)?;

                // Read value (if PUT)
                let value = if header.is_put() {
                    let mut value = vec![0u8; header.value_len as usize];
                    file.read_exact(&mut value)?;
                    value
                } else {
                    Vec::new()
                };

                callback(header, key, value)?;
                count += 1;
            }
        }

        Ok(count)
    }

    /// Clean up old WAL files.
    pub fn cleanup(&self, keep_files: usize) -> io::Result<usize> {
        let mut files: Vec<u64> = Vec::new();

        if let Ok(entries) = std::fs::read_dir(&self.config.dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.starts_with("wal_") && name.ends_with(".log") {
                        if let Ok(seq) = name[4..name.len() - 4].parse::<u64>() {
                            files.push(seq);
                        }
                    }
                }
            }
        }

        files.sort();

        let current_seq = self.file_seq.load(Ordering::Relaxed);
        let total_files = files.len();
        let mut removed = 0;

        for seq in files {
            if seq < current_seq && total_files - removed > keep_files {
                let path = Self::wal_file_path(&self.config.dir, seq);
                if std::fs::remove_file(&path).is_ok() {
                    removed += 1;
                }
            }
        }

        Ok(removed)
    }
}

impl Drop for WriteAheadLog {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);

        // Final sync
        let _ = self.sync();

        if let Some(handle) = self.sync_thread.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_wal_basic() {
        let dir = tempdir().unwrap();
        let config = WalConfig {
            dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let wal = WriteAheadLog::new(config).unwrap();

        // Write entries
        wal.append_put(b"key1", b"value1", 1, 0).unwrap();
        wal.append_put(b"key2", b"value2", 2, 3600).unwrap();
        wal.append_delete(b"key1", 3).unwrap();

        wal.sync().unwrap();

        assert_eq!(wal.stats().entries_written.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn test_wal_replay() {
        let dir = tempdir().unwrap();
        let config = WalConfig {
            dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        // Write entries
        {
            let wal = WriteAheadLog::new(config.clone()).unwrap();
            wal.append_put(b"key1", b"value1", 1, 0).unwrap();
            wal.append_put(b"key2", b"value2", 2, 0).unwrap();
            wal.sync().unwrap();
        }

        // Replay
        {
            let wal = WriteAheadLog::new(config).unwrap();
            let mut entries = Vec::new();

            wal.replay(|header, key, value| {
                entries.push((header.generation, key, value));
                Ok(())
            }).unwrap();

            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].1, b"key1");
            assert_eq!(entries[1].1, b"key2");
        }
    }

    #[test]
    fn test_wal_header() {
        let header = WalEntryHeader::new_put(5, 10, 42, 3600);
        assert!(header.is_valid());
        assert!(header.is_put());
        assert!(!header.is_delete());

        let bytes = header.to_bytes();
        let restored = WalEntryHeader::from_bytes(&bytes);
        // Copy fields from packed struct to avoid unaligned reference issues
        let key_len = restored.key_len;
        let value_len = restored.value_len;
        let generation = restored.generation;
        assert_eq!(key_len, 5);
        assert_eq!(value_len, 10);
        assert_eq!(generation, 42);
    }

    #[test]
    fn group_commit_durability_after_append() {
        // Invariant: when append_entry returns, the bytes are durable on disk
        // (at least as far as sync_data promises). Fire many concurrent writers
        // and then read the WAL file back without calling sync(): every entry
        // should be visible.
        let dir = tempdir().unwrap();
        let config = WalConfig {
            dir: dir.path().to_path_buf(),
            fsync_interval: Duration::from_micros(200),
            fsync_batch: 16,
            ..Default::default()
        };
        let wal = Arc::new(WriteAheadLog::new(config.clone()).unwrap());

        let mut handles = Vec::new();
        for t in 0..8u32 {
            let wal = Arc::clone(&wal);
            handles.push(std::thread::spawn(move || {
                for i in 0..100u32 {
                    let key = format!("k-{}-{}", t, i);
                    let val = format!("v-{}-{}", t, i);
                    wal.append_put(key.as_bytes(), val.as_bytes(), t * 1000 + i, 0)
                        .unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // Open a fresh WAL over the same directory and replay — every single
        // entry we returned from append_put should be readable.
        drop(wal);
        let wal = WriteAheadLog::new(config).unwrap();
        let mut count = 0;
        wal.replay(|_h, _k, _v| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 8 * 100);
    }
}
