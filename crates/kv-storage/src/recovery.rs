//! Recovery and restart (DESIGN §18).
//!
//! Clean path (§18.2): when the superblock has CLEAN and SNAPSHOT_VALID and the snapshot header
//! crc matches, each partition reads its section from the snapshot region the superblock names,
//! verifies its crc, bulk-builds its slot tables and hint planes and restores allocator state;
//! any section error makes the whole node take the full scan.
//!
//! Full scan (§18.3, the crash path, correct by construction): per partition, over every AG the
//! AG table assigns to it, in 1 MiB reads (`recovery-qd`): SMALL pages are header-checked and
//! tiling-parsed with crc; dead records are filtered (flush cutoff, `expire_at <=
//! max(now, clock_floor)`, uncommitted txn ids above W); same-key records keep the higher seq;
//! extent headers are validated and crc-checked; fingerprint conflicts are resolved by re-reading
//! and grouping by actual key, losers going to `stale_copies`; chunkmaps are checked against
//! their chunks; allocator state, hint planes and counters are rebuilt;
//! `next_seq = max(seq_floor, max_seq_seen + 1)`. Cluster: winners above `repl_commit_seq` are
//! collected into `maybe_divergent` (§18.4). This is the only module that reads whole AGs
//! (LC-12).

/// Which restart path a partition takes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RecoveryMode {
    /// Clean-shutdown snapshot (§18.2).
    Snapshot,
    /// Full scan of allocated AGs (§18.3).
    FullScan,
}

/// Progress of `PartitionStore::recover`, driven to completion by the worker.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RecoveryProgress {
    /// I/O outstanding; call again after completions.
    Running,
    /// The partition is Ready.
    Done { bytes_scanned: u64 },
    /// A snapshot section failed verification: the node must restart every partition with
    /// `RecoveryMode::FullScan`.
    SnapshotInvalid,
}
