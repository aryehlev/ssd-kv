//! Operator-initiated slot migration orchestration (DESIGN §23.13).
//!
//! Copy: commit the migration, push `MigrateOut` to the source primary and `MigrateIn` (with the
//! source's replication address) to the destination primary; destination partitions dial the
//! source. Ready: the source reports `MigrationReady` after its position barriers complete.
//! Cutover: commit `slots := dst` (pushed to the source first); the source seals; the
//! destination reports `SealDone`; commit Done; the source purges. Any failover of either shard
//! before Done reverts routing to the source and pauses the migration; the destination's partial
//! copy stays inert until the operator aborts or resumes. Nothing resumes or re-plans by itself.

use kv_ctrl::MigPhase;

/// Leader-side progress of one migration.
pub struct MigrationRun {
    pub id: u64,
    pub phase: MigPhase,
    pub ready_reported: bool,
    pub seal_done: bool,
}
