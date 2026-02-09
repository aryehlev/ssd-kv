//! Data file lifecycle management.
//!
//! File format:
//! - 4KB header (magic, version, file_id, metadata)
//! - 1023 WBlocks (1MB each)
//! - Total: ~1GB per file

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::io::aligned_buf::{AlignedBuffer, ALIGNMENT};
use crate::storage::direct_io::DirectFile;
use crate::storage::write_buffer::{WBlock, WBLOCK_SIZE};

/// File header size (4KB).
pub const FILE_HEADER_SIZE: usize = 4096;

/// Number of WBlocks per file.
pub const WBLOCKS_PER_FILE: u32 = 1023;

/// Total file size (~1GB).
pub const FILE_SIZE: u64 = FILE_HEADER_SIZE as u64 + (WBLOCKS_PER_FILE as u64 * WBLOCK_SIZE as u64);

/// Magic number for data files.
pub const FILE_MAGIC: u32 = 0x53534B56; // "SSKV"

/// Current file format version.
pub const FILE_VERSION: u32 = 1;

/// File header structure.
#[derive(Debug, Clone)]
#[repr(C)]
pub struct FileHeader {
    pub magic: u32,
    pub version: u32,
    pub file_id: u32,
    pub created_at: u64,
    pub wblock_count: u32,
    pub flags: u32,
    pub reserved: [u8; FILE_HEADER_SIZE - 24],
}

impl FileHeader {
    pub fn new(file_id: u32) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Self {
            magic: FILE_MAGIC,
            version: FILE_VERSION,
            file_id,
            created_at,
            wblock_count: 0,
            flags: 0,
            reserved: [0; FILE_HEADER_SIZE - 24],
        }
    }

    pub fn is_valid(&self) -> bool {
        self.magic == FILE_MAGIC && self.version == FILE_VERSION
    }

    pub fn to_bytes(&self) -> [u8; FILE_HEADER_SIZE] {
        let mut buf = [0u8; FILE_HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..8].copy_from_slice(&self.version.to_le_bytes());
        buf[8..12].copy_from_slice(&self.file_id.to_le_bytes());
        buf[12..20].copy_from_slice(&self.created_at.to_le_bytes());
        buf[20..24].copy_from_slice(&self.wblock_count.to_le_bytes());
        // flags and reserved are already zero
        buf
    }

    pub fn from_bytes(buf: &[u8; FILE_HEADER_SIZE]) -> Self {
        Self {
            magic: u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]),
            version: u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]),
            file_id: u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]),
            created_at: u64::from_le_bytes([
                buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18], buf[19],
            ]),
            wblock_count: u32::from_le_bytes([buf[20], buf[21], buf[22], buf[23]]),
            flags: 0,
            reserved: [0; FILE_HEADER_SIZE - 24],
        }
    }
}

/// Metadata for a WBlock in a file.
#[derive(Debug, Clone)]
pub struct WBlockMeta {
    /// Number of live records.
    pub live_records: u32,
    /// Total records (including dead).
    pub total_records: u32,
    /// Bytes used in this block.
    pub bytes_used: u32,
}

impl WBlockMeta {
    pub fn utilization(&self) -> f32 {
        if self.total_records == 0 {
            return 1.0;
        }
        self.live_records as f32 / self.total_records as f32
    }
}

/// A data file on disk.
pub struct DataFile {
    pub id: u32,
    pub path: PathBuf,
    file: DirectFile,
    header: FileHeader,
    wblock_meta: Vec<WBlockMeta>,
    next_wblock: AtomicU32,
}

impl DataFile {
    /// Creates a new data file.
    pub fn create<P: AsRef<Path>>(dir: P, file_id: u32) -> io::Result<Self> {
        let path = dir.as_ref().join(format!("data_{:08}.sst", file_id));

        let file = DirectFile::create(&path)?;

        // Pre-allocate the file
        file.preallocate(FILE_SIZE)?;

        // Write header
        let header = FileHeader::new(file_id);
        let header_bytes = header.to_bytes();
        let mut header_buf = AlignedBuffer::new(FILE_HEADER_SIZE);
        header_buf.extend_from_slice(&header_bytes);

        file.write_at(&header_buf, 0)?;
        file.sync_data()?;

        Ok(Self {
            id: file_id,
            path,
            file,
            header,
            wblock_meta: vec![
                WBlockMeta {
                    live_records: 0,
                    total_records: 0,
                    bytes_used: 0,
                };
                WBLOCKS_PER_FILE as usize
            ],
            next_wblock: AtomicU32::new(0),
        })
    }

    /// Opens an existing data file.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let file = DirectFile::open(&path)?;

        // Read header
        let mut header_buf = AlignedBuffer::new(FILE_HEADER_SIZE);
        file.read_at(&mut header_buf, 0, FILE_HEADER_SIZE)?;

        if header_buf.len() < FILE_HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "File too small for header",
            ));
        }

        let mut header_bytes = [0u8; FILE_HEADER_SIZE];
        header_bytes.copy_from_slice(&header_buf[..FILE_HEADER_SIZE]);
        let header = FileHeader::from_bytes(&header_bytes);

        if !header.is_valid() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid file header",
            ));
        }

        let wblock_count = header.wblock_count;
        Ok(Self {
            id: header.file_id,
            path: path_buf,
            file,
            header,
            wblock_meta: vec![
                WBlockMeta {
                    live_records: 0,
                    total_records: 0,
                    bytes_used: 0,
                };
                WBLOCKS_PER_FILE as usize
            ],
            next_wblock: AtomicU32::new(wblock_count),
        })
    }

    /// Writes a WBlock to the file.
    pub fn write_wblock(&self, block: &mut WBlock) -> io::Result<()> {
        let wblock_id = block.block_id;
        if wblock_id >= WBLOCKS_PER_FILE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "WBlock ID exceeds file capacity",
            ));
        }

        let offset = FILE_HEADER_SIZE as u64 + (wblock_id as u64 * WBLOCK_SIZE as u64);
        let buffer = block.finalize();

        self.file.write_at(buffer, offset)?;

        // Update wblock count if this block extends the file
        let mut current = self.next_wblock.load(Ordering::SeqCst);
        while wblock_id >= current {
            match self.next_wblock.compare_exchange(
                current,
                wblock_id + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(c) => current = c,
            }
        }

        Ok(())
    }

    /// Reads a WBlock from the file.
    pub fn read_wblock(&self, wblock_id: u32) -> io::Result<AlignedBuffer> {
        if wblock_id >= WBLOCKS_PER_FILE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "WBlock ID exceeds file capacity",
            ));
        }

        // Check if this block has been written
        let written_blocks = self.next_wblock.load(Ordering::SeqCst);
        if wblock_id >= written_blocks {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("WBlock {} not yet written (have {} blocks)", wblock_id, written_blocks),
            ));
        }

        let offset = FILE_HEADER_SIZE as u64 + (wblock_id as u64 * WBLOCK_SIZE as u64);
        let mut buffer = AlignedBuffer::new(WBLOCK_SIZE);

        self.file.read_at(&mut buffer, offset, WBLOCK_SIZE)?;

        Ok(buffer)
    }

    /// Reads data at a specific location.
    pub fn read_at(&self, offset: u64, len: usize) -> io::Result<AlignedBuffer> {
        // Align the read
        let aligned_offset = offset & !(ALIGNMENT as u64 - 1);
        let offset_in_block = (offset - aligned_offset) as usize;
        let aligned_len = ((offset_in_block + len + ALIGNMENT - 1) / ALIGNMENT) * ALIGNMENT;

        let mut buffer = AlignedBuffer::new(aligned_len);
        self.file.read_at(&mut buffer, aligned_offset, aligned_len)?;

        Ok(buffer)
    }

    /// Reads a partial WBlock - only the portion needed for the record.
    /// This avoids reading the full 1MB WBlock for small records.
    /// `record_size` should be the expected total record size (header + key + value).
    pub fn read_partial_wblock(
        &self,
        wblock_id: u32,
        offset_in_block: u32,
        record_size: u32,
    ) -> io::Result<AlignedBuffer> {
        if wblock_id >= WBLOCKS_PER_FILE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "WBlock ID exceeds file capacity",
            ));
        }

        // Calculate absolute offset
        let wblock_offset = FILE_HEADER_SIZE as u64 + (wblock_id as u64 * WBLOCK_SIZE as u64);
        let absolute_offset = wblock_offset + offset_in_block as u64;

        // Calculate read size: align to 4KB for O_DIRECT, minimum 4KB
        // Add some extra for record header parsing
        let read_size = ((record_size as usize + 128).max(ALIGNMENT))
            .next_power_of_two()
            .min(WBLOCK_SIZE); // Cap at WBlock size

        // Align the read
        let aligned_offset = absolute_offset & !(ALIGNMENT as u64 - 1);
        let offset_adjustment = (absolute_offset - aligned_offset) as usize;
        let aligned_len = ((offset_adjustment + read_size + ALIGNMENT - 1) / ALIGNMENT) * ALIGNMENT;
        let aligned_len = aligned_len.min(WBLOCK_SIZE);

        let mut buffer = AlignedBuffer::new(aligned_len);
        self.file.read_at(&mut buffer, aligned_offset, aligned_len)?;

        Ok(buffer)
    }

    /// Syncs the file to disk.
    pub fn sync(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    /// Returns the file ID.
    pub fn file_id(&self) -> u32 {
        self.id
    }

    /// Returns the raw file descriptor.
    pub fn raw_fd(&self) -> std::os::unix::io::RawFd {
        use std::os::unix::io::AsRawFd;
        self.file.as_raw_fd()
    }

    /// Returns the number of WBlocks written.
    pub fn wblock_count(&self) -> u32 {
        self.next_wblock.load(Ordering::SeqCst)
    }

    /// Updates WBlock metadata.
    pub fn update_wblock_meta(&mut self, wblock_id: u32, meta: WBlockMeta) {
        if (wblock_id as usize) < self.wblock_meta.len() {
            self.wblock_meta[wblock_id as usize] = meta;
        }
    }

    /// Gets WBlock metadata.
    pub fn get_wblock_meta(&self, wblock_id: u32) -> Option<&WBlockMeta> {
        self.wblock_meta.get(wblock_id as usize)
    }

    /// Returns blocks with utilization below threshold.
    pub fn get_fragmented_blocks(&self, threshold: f32) -> Vec<u32> {
        self.wblock_meta
            .iter()
            .enumerate()
            .filter(|(_, meta)| meta.utilization() < threshold && meta.total_records > 0)
            .map(|(i, _)| i as u32)
            .collect()
    }
}

/// Manages multiple data files.
pub struct FileManager {
    data_dir: PathBuf,
    files: RwLock<HashMap<u32, Arc<Mutex<DataFile>>>>,
    next_file_id: AtomicU32,
}

impl FileManager {
    /// Creates a new file manager.
    pub fn new<P: AsRef<Path>>(data_dir: P) -> io::Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir)?;

        let manager = Self {
            data_dir,
            files: RwLock::new(HashMap::new()),
            next_file_id: AtomicU32::new(0),
        };

        // Scan for existing files
        manager.scan_existing_files()?;

        Ok(manager)
    }

    /// Scans the data directory for existing files.
    fn scan_existing_files(&self) -> io::Result<()> {
        let mut max_id: Option<u32> = None;
        let mut files = self.files.write();

        for entry in fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().map_or(false, |ext| ext == "sst") {
                if let Some(stem) = path.file_stem() {
                    if let Some(stem_str) = stem.to_str() {
                        if let Some(id_str) = stem_str.strip_prefix("data_") {
                            if let Ok(file_id) = id_str.parse::<u32>() {
                                match DataFile::open(&path) {
                                    Ok(data_file) => {
                                        max_id = Some(max_id.map_or(file_id, |m| m.max(file_id)));
                                        files.insert(file_id, Arc::new(Mutex::new(data_file)));
                                    }
                                    Err(e) => {
                                        tracing::warn!("Failed to open data file {:?}: {}", path, e);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Only update next_file_id if we found existing files
        if let Some(max) = max_id {
            self.next_file_id.store(max + 1, Ordering::SeqCst);
        }
        Ok(())
    }

    /// Creates a new data file.
    pub fn create_file(&self) -> io::Result<Arc<Mutex<DataFile>>> {
        let file_id = self.next_file_id.fetch_add(1, Ordering::SeqCst);
        let data_file = DataFile::create(&self.data_dir, file_id)?;
        let file = Arc::new(Mutex::new(data_file));

        self.files.write().insert(file_id, Arc::clone(&file));

        Ok(file)
    }

    /// Gets a file by ID.
    pub fn get_file(&self, file_id: u32) -> Option<Arc<Mutex<DataFile>>> {
        self.files.read().get(&file_id).cloned()
    }

    /// Gets or creates a file by ID.
    pub fn get_or_create_file(&self, file_id: u32) -> io::Result<Arc<Mutex<DataFile>>> {
        // Check if exists
        if let Some(file) = self.files.read().get(&file_id) {
            return Ok(Arc::clone(file));
        }

        // Create new file
        let data_file = DataFile::create(&self.data_dir, file_id)?;
        let file = Arc::new(Mutex::new(data_file));

        self.files.write().insert(file_id, Arc::clone(&file));

        // Update next_file_id if necessary
        let mut next = self.next_file_id.load(Ordering::SeqCst);
        while file_id >= next {
            match self.next_file_id.compare_exchange(
                next,
                file_id + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(current) => next = current,
            }
        }

        Ok(file)
    }

    /// Returns all file IDs.
    pub fn file_ids(&self) -> Vec<u32> {
        self.files.read().keys().copied().collect()
    }

    /// Returns the number of files.
    pub fn file_count(&self) -> usize {
        self.files.read().len()
    }

    /// Returns the data directory.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Removes a file (for compaction).
    pub fn remove_file(&self, file_id: u32) -> io::Result<()> {
        if let Some(file) = self.files.write().remove(&file_id) {
            let path = file.lock().path.clone();
            drop(file); // Release the file handle
            fs::remove_file(path)?;
        }
        Ok(())
    }
}

/// Parallel file manager that distributes keys across multiple files.
/// Each partition has its own FileManager for reduced contention.
pub struct ParallelFileManager {
    managers: Vec<Arc<FileManager>>,
    num_partitions: usize,
}

impl ParallelFileManager {
    /// Creates a new parallel file manager with the specified number of partitions.
    /// Each partition gets its own subdirectory.
    pub fn new<P: AsRef<Path>>(data_dir: P, num_partitions: usize) -> io::Result<Self> {
        let data_dir = data_dir.as_ref();
        let num_partitions = num_partitions.max(1);

        let mut managers = Vec::with_capacity(num_partitions);

        for i in 0..num_partitions {
            let partition_dir = data_dir.join(format!("partition_{:02}", i));
            let manager = FileManager::new(&partition_dir)?;
            managers.push(Arc::new(manager));
        }

        Ok(Self {
            managers,
            num_partitions,
        })
    }

    /// Returns the partition index for a given key hash.
    #[inline]
    pub fn partition_for(&self, key_hash: u64) -> usize {
        ((key_hash >> 48) as usize) % self.num_partitions
    }

    /// Gets the FileManager for a given key hash.
    #[inline]
    pub fn get_manager(&self, key_hash: u64) -> &Arc<FileManager> {
        let partition = self.partition_for(key_hash);
        &self.managers[partition]
    }

    /// Gets the FileManager for a specific partition.
    #[inline]
    pub fn get_partition(&self, partition: usize) -> &Arc<FileManager> {
        &self.managers[partition % self.num_partitions]
    }

    /// Gets a file by partition and file_id.
    pub fn get_file(&self, partition: usize, file_id: u32) -> Option<Arc<Mutex<DataFile>>> {
        self.managers[partition % self.num_partitions].get_file(file_id)
    }

    /// Creates a new file in the specified partition.
    pub fn create_file(&self, partition: usize) -> io::Result<Arc<Mutex<DataFile>>> {
        self.managers[partition % self.num_partitions].create_file()
    }

    /// Gets or creates a file by partition and file_id.
    pub fn get_or_create_file(&self, partition: usize, file_id: u32) -> io::Result<Arc<Mutex<DataFile>>> {
        self.managers[partition % self.num_partitions].get_or_create_file(file_id)
    }

    /// Returns the number of partitions.
    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    /// Returns total file count across all partitions.
    pub fn total_file_count(&self) -> usize {
        self.managers.iter().map(|m| m.file_count()).sum()
    }

    /// Returns all managers.
    pub fn managers(&self) -> &[Arc<FileManager>] {
        &self.managers
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_file_header() {
        let header = FileHeader::new(42);
        let bytes = header.to_bytes();
        let restored = FileHeader::from_bytes(&bytes);

        assert!(restored.is_valid());
        assert_eq!(restored.file_id, 42);
    }

    #[test]
    fn test_data_file_create() {
        let dir = tempdir().unwrap();
        let file = DataFile::create(dir.path(), 0).unwrap();

        assert_eq!(file.file_id(), 0);
    }

    #[test]
    fn test_file_manager() {
        let dir = tempdir().unwrap();
        let manager = FileManager::new(dir.path()).unwrap();

        let file1 = manager.create_file().unwrap();
        let file1_id = file1.lock().id;
        let file2 = manager.create_file().unwrap();
        let file2_id = file2.lock().id;

        assert_eq!(file1_id, 0);
        assert_eq!(file2_id, 1);
        assert_eq!(manager.file_count(), 2);
    }

    #[test]
    fn test_parallel_file_manager() {
        let dir = tempdir().unwrap();
        let pfm = ParallelFileManager::new(dir.path(), 4).unwrap();

        assert_eq!(pfm.num_partitions(), 4);

        // Test partition distribution
        // partition = (key_hash >> 48) % 4
        // We need high 16 bits to give us different partitions after mod 4
        let hash1: u64 = 0x0000_0000_0000_0000; // >> 48 = 0, % 4 = 0
        let hash2: u64 = 0x0001_0000_0000_0000; // >> 48 = 1, % 4 = 1
        let hash3: u64 = 0x0002_0000_0000_0000; // >> 48 = 2, % 4 = 2
        let hash4: u64 = 0x0003_0000_0000_0000; // >> 48 = 3, % 4 = 3

        assert_eq!(pfm.partition_for(hash1), 0);
        assert_eq!(pfm.partition_for(hash2), 1);
        assert_eq!(pfm.partition_for(hash3), 2);
        assert_eq!(pfm.partition_for(hash4), 3);

        // Create files in different partitions
        let file1 = pfm.create_file(0).unwrap();
        let file2 = pfm.create_file(1).unwrap();

        assert_eq!(pfm.total_file_count(), 2);
    }
}
