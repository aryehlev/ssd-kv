//! Lazy and active expiry (DESIGN §9.5, §16.3, §16.4).
//!
//! Lazy: any `exec` that finds `expire_at <= now_ms` treats the key as absent and expires it
//! without a write: the index entry and counters are removed, and the record is dead once
//! `expire_at <= clock_floor_durable` (cluster primaries replicate `Op::Expired`). Active: each
//! partition sweeps its tables round-robin in RAM (64 Ki positions per tick) comparing hint
//! planes with `now`; candidates are verified by reading their record, paced by the
//! `active-expire-reads-per-sec / P` bucket, and either expired without write or re-hinted. It
//! never scans the drive (LC-12). Replicas never expire keys on their own: they hide logically
//! expired keys on reads and remove them when the primary's op arrives. The hint base is rebased
//! hourly (sliced RAM pass, `SlotTable::rehint`).

use kv_index::Pos;

/// Resume point of the active expiry RAM sweep of one partition.
pub(crate) struct ExpirySweep {
    pub(crate) db: u8,
    pub(crate) slot_local: u16,
    pub(crate) pos: Pos,
    /// Unix seconds that hints are encoded against (`now_s - 60` at startup and every hour).
    pub(crate) hint_base_s: u64,
}
