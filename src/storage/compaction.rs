//! Background compaction: defragments WBlocks with low utilization.

use std::collections::HashSet;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tracing::{debug, error, info, trace, warn};

use crate::engine::index::Index;
use crate::engine::index_entry::hash_key;
use crate::storage::file_manager::{FileManager, WBLOCKS_PER_FILE, FILE_HEADER_SIZE};
use crate::storage::record::{Record, RECORD_MAGIC};
use crate::storage::write_buffer::{DiskLocation, WBlock, WriteBuffer, WBLOCK_SIZE};

/// Compaction configuration.
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Utilization threshold below which a WBlock is considered for compaction.
    pub utilization_threshold: f32,
    /// Minimum number of fragmented blocks before compaction runs.
    pub min_blocks_to_compact: usize,
    /// How often to check for compaction opportunities (in seconds).
    pub check_interval_secs: u64,
    /// Maximum blocks to compact in one run.
    pub max_blocks_per_run: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            utilization_threshold: 0.5,
            min_blocks_to_compact: 10,
            check_interval_secs: 60,
            max_blocks_per_run: 100,
        }
    }
}

/// Compaction statistics.
#[derive(Debug, Default)]
pub struct CompactionStats {
    pub runs: AtomicU64,
    pub blocks_compacted: AtomicU64,
    pub records_moved: AtomicU64,
    pub bytes_reclaimed: AtomicU64,
    pub errors: AtomicU64,
}

impl CompactionStats {
    pub fn to_json(&self) -> String {
        format!(
            r#"{{"runs":{},"blocks_compacted":{},"records_moved":{},"bytes_reclaimed":{},"errors":{}}}"#,
            self.runs.load(Ordering::Relaxed),
            self.blocks_compacted.load(Ordering::Relaxed),
            self.records_moved.load(Ordering::Relaxed),
            self.bytes_reclaimed.load(Ordering::Relaxed),
            self.errors.load(Ordering::Relaxed),
        )
    }
}

/// The compaction manager.
pub struct Compactor {
    config: CompactionConfig,
    index: Arc<Index>,
    file_manager: Arc<FileManager>,
    write_buffer: Arc<WriteBuffer>,
    stats: Arc<CompactionStats>,
    running: AtomicBool,
}

impl Compactor {
    /// Creates a new compactor.
    pub fn new(
        config: CompactionConfig,
        index: Arc<Index>,
        file_manager: Arc<FileManager>,
        write_buffer: Arc<WriteBuffer>,
    ) -> Self {
        Self {
            config,
            index,
            file_manager,
            write_buffer,
            stats: Arc::new(CompactionStats::default()),
            running: AtomicBool::new(false),
        }
    }

    /// Returns compaction statistics.
    pub fn stats(&self) -> Arc<CompactionStats> {
        Arc::clone(&self.stats)
    }

    /// Runs the compaction loop (blocking).
    pub fn run(&self, stop: Arc<AtomicBool>) {
        info!("Compaction thread started");
        self.running.store(true, Ordering::SeqCst);

        while !stop.load(Ordering::Relaxed) {
            // Sleep for the check interval
            std::thread::sleep(Duration::from_secs(self.config.check_interval_secs));

            if stop.load(Ordering::Relaxed) {
                break;
            }

            // Find fragmented blocks
            match self.find_fragmented_blocks() {
                Ok(blocks) => {
                    if blocks.len() >= self.config.min_blocks_to_compact {
                        info!("Found {} fragmented blocks, starting compaction", blocks.len());
                        if let Err(e) = self.compact_blocks(&blocks) {
                            error!("Compaction error: {}", e);
                            self.stats.errors.fetch_add(1, Ordering::Relaxed);
                        }
                        self.stats.runs.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(e) => {
                    warn!("Error scanning for fragmented blocks: {}", e);
                }
            }
        }

        self.running.store(false, Ordering::SeqCst);
        info!("Compaction thread stopped");
    }

    /// Runs a single compaction pass (for testing).
    pub fn run_once(&self) -> io::Result<usize> {
        let blocks = self.find_fragmented_blocks()?;
        if blocks.is_empty() {
            return Ok(0);
        }
        self.compact_blocks(&blocks)?;
        Ok(blocks.len())
    }

    /// Finds WBlocks with utilization below the threshold.
    fn find_fragmented_blocks(&self) -> io::Result<Vec<(u32, u32)>> {
        let mut fragmented = Vec::new();

        for file_id in self.file_manager.file_ids() {
            if let Some(file) = self.file_manager.get_file(file_id) {
                let file_guard = file.lock();
                let blocks = file_guard.get_fragmented_blocks(self.config.utilization_threshold);
                for block_id in blocks {
                    fragmented.push((file_id, block_id));
                }
            }
        }

        // Sort by file_id for sequential access
        fragmented.sort();

        // Limit to max blocks per run
        fragmented.truncate(self.config.max_blocks_per_run);

        Ok(fragmented)
    }

    /// Compacts the given blocks by moving live records to new locations.
    fn compact_blocks(&self, blocks: &[(u32, u32)]) -> io::Result<()> {
        for &(file_id, wblock_id) in blocks {
            match self.compact_block(file_id, wblock_id) {
                Ok(moved) => {
                    self.stats.blocks_compacted.fetch_add(1, Ordering::Relaxed);
                    self.stats.records_moved.fetch_add(moved as u64, Ordering::Relaxed);
                    debug!(
                        "Compacted block {}/{}, moved {} records",
                        file_id, wblock_id, moved
                    );
                }
                Err(e) => {
                    warn!("Failed to compact block {}/{}: {}", file_id, wblock_id, e);
                    self.stats.errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        Ok(())
    }

    /// Compacts a single WBlock by moving live records.
    fn compact_block(&self, file_id: u32, wblock_id: u32) -> io::Result<usize> {
        let file = self
            .file_manager
            .get_file(file_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "File not found"))?;

        // Read the WBlock
        let wblock_data = {
            let file_guard = file.lock();
            file_guard.read_wblock(wblock_id)?
        };

        let mut moved = 0;
        let mut offset = 0usize;

        while offset + 32 <= wblock_data.len() {
            // Check for end of data
            if wblock_data.len() < offset + 2 {
                break;
            }
            let magic = u16::from_le_bytes([wblock_data[offset], wblock_data[offset + 1]]);
            if magic != RECORD_MAGIC {
                break;
            }

            // Parse record
            let record = match Record::from_bytes(&wblock_data[offset..]) {
                Ok(r) => r,
                Err(_) => break,
            };

            let record_size = record.serialized_size();

            // Check if record is still live in the index
            if record.is_live() {
                let key_hash = hash_key(&record.key);
                if let Some(entry) = self.index.get(&record.key) {
                    // Only move if this record is the current version
                    if entry.location.file_id == file_id
                        && entry.location.wblock_id == wblock_id as u16
                        && entry.location.offset == offset as u32
                    {
                        // Re-write the record to a new location
                        let mut new_record = Record::new(
                            record.key.clone(),
                            record.value.clone(),
                            record.header.generation,
                            record.header.ttl,
                        )?;
                        new_record.header.reserved = record.header.reserved;

                        let new_location = self.write_buffer.append(&mut new_record)?;

                        // Update index atomically
                        self.index.insert(
                            &record.key,
                            new_location,
                            record.header.generation,
                            record.header.value_len,
                        );

                        moved += 1;
                    }
                }
            }

            offset += record_size;
        }

        // Mark the old block as reclaimable
        // In a full implementation, we'd track this and eventually delete the file
        self.stats
            .bytes_reclaimed
            .fetch_add(WBLOCK_SIZE as u64, Ordering::Relaxed);

        Ok(moved)
    }

    /// Returns true if compaction is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

/// Starts the compaction thread.
pub fn start_compaction_thread(
    config: CompactionConfig,
    index: Arc<Index>,
    file_manager: Arc<FileManager>,
    write_buffer: Arc<WriteBuffer>,
) -> (Arc<Compactor>, Arc<AtomicBool>) {
    let compactor = Arc::new(Compactor::new(config, index, file_manager, write_buffer));
    let stop = Arc::new(AtomicBool::new(false));

    let compactor_clone = Arc::clone(&compactor);
    let stop_clone = Arc::clone(&stop);

    std::thread::Builder::new()
        .name("compaction".to_string())
        .spawn(move || {
            compactor_clone.run(stop_clone);
        })
        .expect("Failed to spawn compaction thread");

    (compactor, stop)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::write_buffer::WBlock;
    use tempfile::tempdir;

    #[test]
    fn test_compaction_config_default() {
        let config = CompactionConfig::default();
        assert_eq!(config.utilization_threshold, 0.5);
        assert_eq!(config.min_blocks_to_compact, 10);
    }

    #[test]
    fn test_compactor_no_fragmentation() {
        let dir = tempdir().unwrap();
        let file_manager = Arc::new(FileManager::new(dir.path()).unwrap());
        let index = Arc::new(Index::new());
        let write_buffer = Arc::new(WriteBuffer::new(0, WBLOCKS_PER_FILE));

        let compactor = Compactor::new(
            CompactionConfig::default(),
            index,
            file_manager,
            write_buffer,
        );

        let compacted = compactor.run_once().unwrap();
        assert_eq!(compacted, 0);
    }
}
