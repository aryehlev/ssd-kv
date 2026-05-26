//! Recovery: WAL replay to rebuild the in-memory index after a restart.

use std::io;

use tracing::{debug, info};

use crate::server::Handler;
use crate::storage::segment_manager::SegmentManager;
use crate::storage::wal::WriteAheadLog;

/// Statistics returned by `recover_with_wal`.
#[derive(Debug, Default)]
pub struct RecoveryStats {
    pub files_scanned: u32,
    pub records_found: u64,
    pub records_indexed: u64,
    pub records_expired: u64,
    pub records_deleted: u64,
    pub errors: u64,
    pub wal_entries_replayed: u64,
    pub max_generation: u32,
    // Legacy fields kept for callers that pattern-match on RecoveryStats.
    pub wblocks_scanned: u64,
    pub wblocks_footer_ok: u64,
    pub wblocks_footer_missing: u64,
    pub wblocks_footer_mismatch: u64,
}

/// Rebuild the in-memory index by scanning segment files on disk.
///
/// This is the data-file pass — it finds every live entry that was
/// already flushed to a segment before the previous shutdown.
pub fn recover_index(
    handler: &Handler,
    sm: &SegmentManager,
) -> io::Result<RecoveryStats> {
    let mut stats = RecoveryStats::default();
    sm.recover_from_segments(handler.index())?;
    stats.files_scanned = sm.file_count() as u32;
    Ok(stats)
}

/// Full crash recovery:
/// 1. Scan segment files → rebuild index from durable data.
/// 2. Replay WAL → recover writes acknowledged but not yet fsync'd to segments.
pub fn recover_with_wal(
    handler: &Handler,
    sm: &SegmentManager,
    wal: &WriteAheadLog,
) -> io::Result<RecoveryStats> {
    let mut stats = recover_index(handler, sm)?;

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

    if stats.max_generation > 0 {
        handler.bump_generation_past(stats.max_generation);
    }

    if replayed > 0 {
        info!("Replayed {} WAL entries (max gen {})", replayed, stats.max_generation);
    }

    Ok(stats)
}
