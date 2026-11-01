//! INFO counters and Prometheus metrics (DESIGN §21).
//!
//! Per-worker counters are plain `u64`s in a `#[repr(C)]` struct, published through `AtomicU64`
//! mirrors every tick; latencies in per-worker histograms (1 us - 60 s, 3 significant digits),
//! merged on demand. Prometheus names are `kvd_<section>_<field>` on `metrics-port`, served by
//! the agent thread. Labels are limited to `worker`, `partition`, `device` and `command`; no
//! metric or label carries node ids, shard ids, terms, slot sets or user bytes (LC-24, LC-32).

use std::sync::atomic::{AtomicU64, Ordering};

use kv_storage::partition::PartitionCounters;

/// Plain per-worker counters (§21 stats/clients sections).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct WorkerCounters {
    pub connected_clients: u64,
    pub backpressured_clients: u64,
    pub total_connections_received: u64,
    pub rejected_connections: u64,
    pub total_commands_processed: u64,
    pub total_net_input_bytes: u64,
    pub total_net_output_bytes: u64,
    pub xmsg_hops: u64,
    pub large_value_backpressured_conns: u64,
}

/// Atomic mirror of one partition's counters, read by INFO, DBSIZE and `/metrics`.
#[derive(Default)]
pub struct PartitionCountersAtomic {
    pub keys: [AtomicU64; 16],
    pub expires: [AtomicU64; 16],
    pub keyspace_hits: AtomicU64,
    pub keyspace_misses: AtomicU64,
    pub expired_keys: AtomicU64,
    pub page_reads: AtomicU64,
    pub page_writes: AtomicU64,
    pub extent_writes: AtomicU64,
    pub host_bytes_written: AtomicU64,
    pub user_bytes_written: AtomicU64,
    pub stale_copies: AtomicU64,
    pub corrupt_pages: AtomicU64,
    pub index_bytes: AtomicU64,
    pub alloc_state_bytes: AtomicU64,
    pub dirty_bytes: AtomicU64,
}

impl PartitionCountersAtomic {
    /// Publishes a partition's plain counters (once per tick).
    pub fn publish(&self, c: &PartitionCounters) {
        todo!("DESIGN §21: counter mirror")
    }

    /// Keys of `db` (DBSIZE contribution).
    pub fn keys(&self, db: u8) -> u64 {
        self.keys[usize::from(db)].load(Ordering::Relaxed)
    }
}

/// Renders the Prometheus text exposition.
pub fn render_prometheus(shared: &crate::shared::NodeShared, out: &mut Vec<u8>) {
    todo!("DESIGN §21: /metrics")
}
