//! Slot tables (DESIGN §9.1, §9.4, §9.8, §16.6).
//!
//! A [`SlotTable`] holds every key of one `(db, slot)`: an extendible-hashing directory of
//! `2^D + 1` u32 segment ids (the last is a `u32::MAX` sentinel; `seg = dir[fp >> (32 - D)]`,
//! `D = 0 -> dir[0]`) over ordered linear-probing segments (see `segment.rs`). Lookups return
//! fingerprint candidates; the caller verifies the key against the SSD bytes (LC-07).
//!
//! Iteration (LC-18): SCAN, KEYS, replica full sync and migration walk a table only through
//! [`SlotTable::scan_pages`], which consumes whole equal-fp32 groups in fingerprint order,
//! applies the filter to each entry before anything is emitted, and outputs only
//! de-duplicated [`PageRef`]s plus one scalar [`ScanSpan`]; nothing carrying a per-entry
//! fingerprint, hash or entry leaves the walk. A step stops after `max_entries` entries or
//! [`SCAN_STEP_MAX_PAGES`] distinct pages, a compile-time constant tied to the COUNT batch.
//! [`SlotTable::snapshot_iter`] is reserved to the clean-shutdown snapshot writer
//! (`kv-storage/src/snapshot.rs`).
//!
//! Open point for the implementer (LC-09): with `D = 0` the directory is `2 * 4 = 8` bytes, a
//! power of two; either start tables at `D = 1` or keep the `D = 0` directory inline.

use kv_common::KeyHash;
use smallvec::SmallVec;

use crate::entry::{Cand, Entry, Kind, Pos, VType};
use crate::segment::Segment;
use crate::sysbuf::SysBuf;

/// A page (or extent) referenced by entries of a scanned span.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct PageRef {
    pub page36: u64,
    pub kind: Kind,
    pub class: u8,
}

/// The fingerprint range covered by one `scan_pages` step.
#[derive(Clone, Copy, Debug)]
pub struct ScanSpan {
    pub fp_lo: u32,
    /// 2^32 = end of table
    pub fp_hi_excl: u64,
    pub next: Option<u32>,
}

/// Built-in key-only filters applied inside the walk (LC-17).
#[derive(Clone, Copy, Debug)]
pub struct ScanFilter {
    pub vtype: Option<VType>,
    /// Merge-sync prune walk: only entries whose `sync_mark` is unset.
    pub unmarked_only: bool,
}

/// compile-time constant, never configurable (LC-18)
pub const SCAN_STEP_MAX_PAGES: usize = 32;

/// The index of one `(db, slot)`.
pub struct SlotTable {
    /// `2^D + 1` u32 segment ids, last = sentinel.
    dir: SysBuf,
    depth: u8,
    segs: Vec<Segment>,
    len: usize,
    ttl_len: usize,
}

impl SlotTable {
    pub fn new() -> Self {
        todo!("DESIGN §9.4: empty table")
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Number of entries that carry a TTL hint.
    pub fn ttl_len(&self) -> usize {
        self.ttl_len
    }

    /// Bytes of segments, hint planes and the directory (§9.6).
    pub fn memory_bytes(&self) -> usize {
        todo!("DESIGN §9.6: index memory")
    }

    /// Fingerprint candidates for `h` (§9.4): equal `fp32` and, for small entries, equal `x8`.
    pub fn candidates(&self, h: KeyHash, out: &mut SmallVec<[Cand; 2]>) {
        todo!("DESIGN §9.4: candidates")
    }

    pub fn insert(&mut self, h: KeyHash, e: Entry, hint: u16) {
        todo!("DESIGN §9.4: insert")
    }

    /// fp32 must not change
    pub fn update(&mut self, pos: Pos, e: Entry, hint: u16) {
        todo!("DESIGN §9.4: update in place")
    }

    pub fn remove(&mut self, pos: Pos) {
        todo!("DESIGN §9.4: backward-shift remove")
    }

    /// SCAN/KEYS/sync/migration walk: consumes whole equal-fp groups from `fp_from` until
    /// `max_entries` entries or SCAN_STEP_MAX_PAGES distinct pages; filter applied before
    /// emitting; emits de-duplicated page refs only.
    pub fn scan_pages(&self, fp_from: u32, max_entries: usize, f: ScanFilter, out: &mut Vec<PageRef>) -> ScanSpan {
        todo!("DESIGN §16.6: page-reference walk")
    }

    /// Snapshot writer only (kv-storage/src/snapshot.rs; lint LC-18).
    pub fn snapshot_iter(&self) -> impl Iterator<Item = (Entry, u16)> + '_ {
        core::iter::from_fn(|| -> Option<(Entry, u16)> { todo!("DESIGN §9.8: snapshot iteration") })
    }

    pub fn set_sync_mark(&mut self, pos: Pos, on: bool) {
        todo!("DESIGN §23.7: sync mark")
    }

    pub fn clear_sync_marks(&mut self) {
        todo!("DESIGN §23.7: clear sync marks")
    }

    /// Builds a table from entries sorted by fp32 (clean restart, §9.8, §18.2).
    pub fn bulk_build(sorted: Vec<(Entry, u16)>) -> Self {
        todo!("DESIGN §9.8: bulk build")
    }

    /// Active expiry RAM sweep (§16.4): pushes positions whose hint lower bound is `<= now_lb`,
    /// visiting at most `budget` positions from `start`; returns the resume position.
    pub fn expiry_candidates(&self, start: Pos, budget: usize, now_lb: u16, out: &mut Vec<Pos>) -> Pos {
        todo!("DESIGN §16.4: expiry candidates")
    }

    /// rebase pass (sliced by caller)
    pub fn rehint(&mut self, f: &mut dyn FnMut(u16) -> u16) {
        todo!("DESIGN §9.5: hint rebase")
    }

    /// A uniformly drawn entry for RANDOMKEY (§16.6).
    pub fn random_entry(&self, r: u64) -> Option<(Pos, Entry)> {
        todo!("DESIGN §16.6: random entry")
    }
}
