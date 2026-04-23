//! Index recovery: rebuilds the in-memory index from data files on startup.

use std::io;

use tracing::{debug, info, warn};

use crate::engine::index::Index;
use crate::engine::index_entry::hash_key;
use crate::server::Handler;
use crate::storage::file_manager::{FileManager, WBLOCKS_PER_FILE};
use crate::storage::record::{Record, HEADER_SIZE, RECORD_ALIGNMENT, RECORD_MAGIC};
use crate::storage::wal::WriteAheadLog;
use crate::storage::write_buffer::DiskLocation;

/// Recovery statistics.
#[derive(Debug, Default)]
pub struct RecoveryStats {
    pub files_scanned: u32,
    pub wblocks_scanned: u64,
    pub records_found: u64,
    pub records_indexed: u64,
    pub records_expired: u64,
    pub records_deleted: u64,
    pub errors: u64,
    /// WAL entries replayed (if a WAL was passed to `recover_with_wal`).
    pub wal_entries_replayed: u64,
    /// Highest generation observed across data files + WAL.
    pub max_generation: u32,
}

/// Recovers the index from data files.
pub fn recover_index(
    index: &Index,
    file_manager: &FileManager,
) -> io::Result<RecoveryStats> {
    let mut stats = RecoveryStats::default();

    let file_ids = file_manager.file_ids();
    info!("Starting recovery, found {} data files", file_ids.len());

    for file_id in file_ids {
        match recover_file(index, file_manager, file_id, &mut stats) {
            Ok(_) => {
                stats.files_scanned += 1;
            }
            Err(e) => {
                warn!("Error recovering file {}: {}", file_id, e);
                stats.errors += 1;
            }
        }
    }

    info!(
        "Recovery complete: {} files, {} records indexed, {} expired, {} deleted, {} errors",
        stats.files_scanned,
        stats.records_indexed,
        stats.records_expired,
        stats.records_deleted,
        stats.errors
    );

    Ok(stats)
}

/// Full recovery: scan data files, then replay the WAL for records that
/// were ack'd to clients but not yet flushed to data files. Finally,
/// advance the handler's generation counter past anything we've seen so
/// new writes don't collide.
pub fn recover_with_wal(
    handler: &Handler,
    file_manager: &FileManager,
    wal: &WriteAheadLog,
) -> io::Result<RecoveryStats> {
    // Step 1: scan data files
    let mut stats = recover_index(handler.index(), file_manager)?;

    // Step 2: replay WAL entries. Index::insert compares generations so
    // anything already in the index at a higher gen stays; otherwise the
    // WAL entry takes over (this covers the "acked but not flushed" window).
    let max_gen_before = stats.max_generation;
    let replayed = wal.replay(|header, key, value| -> io::Result<()> {
        if header.generation > stats.max_generation {
            stats.max_generation = header.generation;
        }
        if header.is_put() {
            handler.put_from_wal(&key, &value, header.generation, header.ttl)?;
        } else if header.is_delete() {
            handler.delete_from_wal(&key, header.generation)?;
        }
        Ok(())
    })?;
    stats.wal_entries_replayed = replayed;

    // Step 3: make sure future writes get fresh generations.
    if stats.max_generation > 0 {
        handler.bump_generation_past(stats.max_generation);
    }

    if replayed > 0 {
        info!(
            "Replayed {} WAL entries (max gen {} -> {})",
            replayed, max_gen_before, stats.max_generation
        );
    }

    Ok(stats)
}

/// Recovers a single data file.
fn recover_file(
    index: &Index,
    file_manager: &FileManager,
    file_id: u32,
    stats: &mut RecoveryStats,
) -> io::Result<()> {
    let file = file_manager
        .get_file(file_id)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "File not found"))?;

    let file_guard = file.lock();
    let wblock_count = file_guard.wblock_count();

    debug!("Recovering file {} with {} wblocks", file_id, wblock_count);

    // Scan each WBlock
    for wblock_id in 0..wblock_count.min(WBLOCKS_PER_FILE) {
        match recover_wblock(index, &file_guard, file_id, wblock_id, stats) {
            Ok(_) => {
                stats.wblocks_scanned += 1;
            }
            Err(e) => {
                debug!("Error in wblock {} of file {}: {}", wblock_id, file_id, e);
                // Continue to next block
            }
        }
    }

    Ok(())
}

/// Recovers records from a single WBlock.
fn recover_wblock(
    index: &Index,
    file: &crate::storage::file_manager::DataFile,
    file_id: u32,
    wblock_id: u32,
    stats: &mut RecoveryStats,
) -> io::Result<()> {
    let wblock_data = file.read_wblock(wblock_id)?;

    let mut offset = 0usize;

    while offset + HEADER_SIZE <= wblock_data.len() {
        // Check for end of data (zero magic)
        if wblock_data.len() < offset + 2 {
            break;
        }
        let magic = u16::from_le_bytes([wblock_data[offset], wblock_data[offset + 1]]);
        if magic != RECORD_MAGIC {
            // End of valid records or corruption
            break;
        }

        // Parse record
        match Record::from_bytes(&wblock_data[offset..]) {
            Ok(record) => {
                stats.records_found += 1;
                let record_size = record.serialized_size();

                if record.header.generation > stats.max_generation {
                    stats.max_generation = record.header.generation;
                }

                if record.header.is_expired() {
                    stats.records_expired += 1;
                } else if record.header.is_deleted() {
                    stats.records_deleted += 1;
                    // Index the deletion
                    let _key_hash = hash_key(&record.key);
                    index.delete(&record.key, record.header.generation);
                } else {
                    // Index the record
                    let location = DiskLocation::new(file_id, wblock_id as u16, offset as u32);
                    index.insert(
                        &record.key,
                        location,
                        record.header.generation,
                        record.header.value_len,
                    );
                    stats.records_indexed += 1;
                }

                offset += record_size;
            }
            Err(e) => {
                debug!("Failed to parse record at offset {}: {}", offset, e);
                // Skip to next alignment boundary
                offset = ((offset / RECORD_ALIGNMENT) + 1) * RECORD_ALIGNMENT;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use crate::storage::file_manager::DataFile;
    use crate::storage::write_buffer::WBlock;

    #[test]
    fn test_recovery_empty() {
        let dir = tempdir().unwrap();
        let file_manager = FileManager::new(dir.path()).unwrap();
        let index = Index::new();

        let stats = recover_index(&index, &file_manager).unwrap();
        assert_eq!(stats.files_scanned, 0);
        assert_eq!(stats.records_indexed, 0);
    }

    #[test]
    fn test_recovery_with_records() {
        let dir = tempdir().unwrap();
        let file_manager = FileManager::new(dir.path()).unwrap();

        // Create a file with some records
        let file = file_manager.create_file().unwrap();
        {
            let mut file_guard = file.lock();
            let mut wblock = WBlock::new(0, 0);

            for i in 0..10 {
                let mut record = Record::new(
                    format!("key_{}", i).into_bytes(),
                    format!("value_{}", i).into_bytes(),
                    i as u32,
                    0,
                ).unwrap();
                wblock.try_append(&mut record).unwrap();
            }

            file_guard.write_wblock(&mut wblock).unwrap();
        }

        // Now recover
        let index = Index::new();
        let stats = recover_index(&index, &file_manager).unwrap();

        assert_eq!(stats.files_scanned, 1);
        assert_eq!(stats.records_indexed, 10);

        // Verify index
        for i in 0..10 {
            let key = format!("key_{}", i);
            let entry = index.get(key.as_bytes()).unwrap();
            assert_eq!(entry.generation, i as u32);
        }
    }
}
