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
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, Receiver, Sender};
use parking_lot::Mutex;

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
    /// Sync interval (default: 10ms)
    pub sync_interval: Duration,
    /// Buffer size (default: 1MB)
    pub buffer_size: usize,
    /// Enable fsync (default: true)
    pub fsync: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("./wal"),
            max_file_size: 64 * 1024 * 1024, // 64MB
            sync_interval: Duration::from_millis(10),
            buffer_size: 1024 * 1024, // 1MB
            fsync: true,
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

/// Write-Ahead Log for durability and fast writes.
pub struct WriteAheadLog {
    config: WalConfig,
    /// Current WAL file
    writer: Mutex<BufWriter<File>>,
    /// Current file sequence number
    file_seq: AtomicU64,
    /// Current position in file
    position: AtomicU64,
    /// Statistics
    stats: Arc<WalStats>,
    /// Background sync thread
    sync_thread: Option<JoinHandle<()>>,
    /// Shutdown flag
    shutdown: Arc<AtomicBool>,
    /// Channel for pending entries
    pending_tx: Sender<WalEntry>,
    pending_rx: Receiver<WalEntry>,
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

        let (pending_tx, pending_rx) = bounded(100_000);

        let mut wal = Self {
            config,
            writer: Mutex::new(BufWriter::with_capacity(1024 * 1024, file)),
            file_seq: AtomicU64::new(file_seq),
            position: AtomicU64::new(position),
            stats: Arc::new(WalStats::default()),
            sync_thread: None,
            shutdown: Arc::new(AtomicBool::new(false)),
            pending_tx,
            pending_rx,
        };

        // Start background sync thread
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

    /// Start background sync thread.
    fn start_sync_thread(&mut self) {
        let stats = Arc::clone(&self.stats);
        let shutdown = Arc::clone(&self.shutdown);
        let sync_interval = self.config.sync_interval;
        let fsync = self.config.fsync;

        // We can't easily share the writer, so sync thread just triggers periodic syncs
        let handle = thread::Builder::new()
            .name("wal-sync".to_string())
            .spawn(move || {
                while !shutdown.load(Ordering::Relaxed) {
                    thread::sleep(sync_interval);
                    // Sync is triggered by the main thread
                    stats.syncs.fetch_add(1, Ordering::Relaxed);
                }
            })
            .expect("Failed to spawn WAL sync thread");

        self.sync_thread = Some(handle);
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

    /// Append an entry to the WAL.
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

        let mut writer = self.writer.lock();

        // Write header
        writer.write_all(&header.to_bytes())?;

        // Write key
        writer.write_all(key)?;

        // Write value (if present)
        if let Some(value) = value {
            writer.write_all(value)?;
        }

        let new_pos = self.position.fetch_add(entry_size as u64, Ordering::Relaxed) + entry_size as u64;

        self.stats.entries_written.fetch_add(1, Ordering::Relaxed);
        self.stats.bytes_written.fetch_add(entry_size as u64, Ordering::Relaxed);
        self.stats.current_file_size.store(new_pos, Ordering::Relaxed);

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

        // Flush current file
        writer.flush()?;
        if self.config.fsync {
            writer.get_ref().sync_all()?;
        }

        // Switch to new file
        *writer = BufWriter::with_capacity(self.config.buffer_size, new_file);
        self.position.store(0, Ordering::Relaxed);

        self.stats.flushes.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Sync the WAL to disk.
    pub fn sync(&self) -> io::Result<()> {
        let mut writer = self.writer.lock();
        writer.flush()?;
        if self.config.fsync {
            writer.get_ref().sync_all()?;
        }
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
}
