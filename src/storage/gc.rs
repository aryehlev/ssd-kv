//! Background GC: compacts low-utilisation segments at bounded IOPS so it
//! never causes tail-latency spikes on the foreground read path.
//!
//! The paper (SIndex §4.2) requires GC to run at controlled I/O rate so the
//! NVMe queue-depth budget stays with foreground clients.  Here we enforce
//! that by sleeping `1s / max_writes_per_sec` between each relocated entry.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{debug, error, info};

use crate::engine::index::Index;
use crate::engine::index_entry::RecordLocation;
use crate::storage::segment_manager::{now_micros, SegmentManager};

/// GC tuning knobs.
#[derive(Clone, Debug)]
pub struct GcConfig {
    /// How often to scan for GC candidates (seconds).
    pub check_interval_secs: u64,
    /// Segments with `live/total < gc_threshold` are compacted.
    pub gc_threshold: f32,
    /// Maximum number of entry relocations per second.
    /// Limits disk-write bandwidth consumed by GC so it doesn't crowd out
    /// foreground writes.  1000 ≈ 4 MB/s at average 4 KB entries.
    pub max_writes_per_sec: u32,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self { check_interval_secs: 30, gc_threshold: 0.5, max_writes_per_sec: 1000 }
    }
}

pub struct GcRunner {
    index: Arc<Index>,
    sm:    Arc<SegmentManager>,
    config: GcConfig,
}

impl GcRunner {
    pub fn new(index: Arc<Index>, sm: Arc<SegmentManager>, config: GcConfig) -> Self {
        Self { index, sm, config }
    }

    /// Compact one sealed segment: scan, relocate live entries, mark reclaimed.
    /// Returns (entries_relocated, entries_skipped).
    fn compact_segment(&self, file_id: u32, seg_id: u32) -> std::io::Result<(usize, usize)> {
        let write_interval = if self.config.max_writes_per_sec == 0 {
            Duration::ZERO
        } else {
            Duration::from_secs(1) / self.config.max_writes_per_sec
        };

        let entries = self.sm.scan_segment(file_id, seg_id)?;
        let mut relocated = 0usize;
        let mut skipped   = 0usize;

        for e in entries {
            // Skip tombstones — nothing to preserve.
            if e.is_deleted { skipped += 1; continue; }

            // Is this entry still the live copy in the index?
            let current = match self.index.get(&e.key) {
                None => { skipped += 1; continue; }
                Some(idx) => idx,
            };
            if !current.is_live()
                || current.location != e.loc
                || current.generation != e.generation
            {
                // Overwritten or deleted since the scan.
                skipped += 1;
                continue;
            }

            let t0 = Instant::now();

            // Write a fresh copy to the current active segment.
            let new_loc = match self.sm.write_entry(
                &e.key, &e.value, e.ts, e.ttl, e.generation, false,
            ) {
                Ok(l) => l,
                Err(err) => {
                    error!("gc write_entry failed: {}", err);
                    skipped += 1;
                    continue;
                }
            };

            // Atomically redirect the index from old → new location.
            // Fails silently if the entry was concurrently updated — the
            // orphaned new copy will be collected in a future GC pass.
            if self.index.relocate(&e.key, e.generation, new_loc) {
                relocated += 1;
            } else {
                skipped += 1;
            }

            // IOPS throttle: sleep for any remaining budget.
            let elapsed = t0.elapsed();
            if write_interval > elapsed {
                std::thread::sleep(write_interval - elapsed);
            }
        }

        // Mark reclaimed AFTER all relocations are durable.
        self.sm.mark_segment_reclaimed(file_id, seg_id)?;
        // Remove the whole file if all its segments are now reclaimed.
        let _ = self.sm.try_delete_file_if_all_reclaimed(file_id);

        Ok((relocated, skipped))
    }

    /// Run a single GC pass (scan for candidates, compact them). Returns total
    /// (relocated, skipped) counts across all compacted segments.
    pub fn run_once(&self) -> (usize, usize) {
        let candidates = self.sm.gc_sealed_segments(self.config.gc_threshold);
        let mut total_relocated = 0;
        let mut total_skipped   = 0;
        for (file_id, seg_id, util) in candidates {
            debug!("GC run_once: compacting ({},{}) util={:.2}", file_id, seg_id, util);
            match self.compact_segment(file_id, seg_id) {
                Ok((r, s)) => { total_relocated += r; total_skipped += s; }
                Err(e)     => error!("GC run_once: ({},{}) failed: {}", file_id, seg_id, e),
            }
        }
        (total_relocated, total_skipped)
    }

    pub fn run(self) {
        let interval = Duration::from_secs(self.config.check_interval_secs.max(1));
        loop {
            std::thread::sleep(interval);

            let candidates = self.sm.gc_sealed_segments(self.config.gc_threshold);
            if candidates.is_empty() { continue; }

            info!("GC: {} segment(s) below {:.0}% utilisation",
                candidates.len(), self.config.gc_threshold * 100.0);

            for (file_id, seg_id, util) in candidates {
                debug!("GC: compacting ({},{}) util={:.2}", file_id, seg_id, util);
                match self.compact_segment(file_id, seg_id) {
                    Ok((r, s)) => info!("GC: ({},{}) done — {} relocated, {} skipped", file_id, seg_id, r, s),
                    Err(e)     => error!("GC: ({},{}) failed: {}", file_id, seg_id, e),
                }
            }
        }
    }
}

/// Spawn the GC background thread.  Returns the JoinHandle so the caller can
/// keep it alive for the process lifetime.
pub fn start_gc_thread(
    index: Arc<Index>,
    sm:    Arc<SegmentManager>,
    config: GcConfig,
) -> std::thread::JoinHandle<()> {
    let runner = GcRunner::new(index, sm, config);
    std::thread::Builder::new()
        .name("gc-thread".into())
        .spawn(move || runner.run())
        .expect("failed to spawn GC thread")
}
