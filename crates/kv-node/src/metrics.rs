//! INFO counters and Prometheus metrics (DESIGN §21).
//!
//! Per-worker counters are plain `u64`s in a `#[repr(C)]` struct, published through `AtomicU64`
//! mirrors every tick; latencies in per-worker histograms (1 us - 60 s, 3 significant digits),
//! merged on demand. Prometheus names are `kvd_<section>_<field>` on `metrics-port`, served by
//! the agent thread. Labels are limited to `worker`, `partition`, `device` and `command`; no
//! metric or label carries node ids, shard ids, terms, slot sets or user bytes (LC-24, LC-32).
//!
//! Readers (INFO, DBSIZE, `/metrics`) only load the atomic mirrors; the owning worker is the only
//! writer of each mirror, so every value is a consistent per-field snapshot at tick granularity.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};

use kv_cmd::CmdId;
use kv_common::metrics::Hist;
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

/// Generates the atomic mirror of a plain counter struct.
macro_rules! mirror {
    ($(#[$m:meta])* $name:ident for $plain:ident { $($f:ident),* $(,)? }) => {
        $(#[$m])*
        #[derive(Default)]
        pub struct $name {
            $(pub $f: AtomicU64,)*
        }

        impl $name {
            /// Publishes the owner's plain counters (once per tick).
            pub fn publish(&self, c: &$plain) {
                $(self.$f.store(c.$f, Ordering::Relaxed);)*
            }

            /// The last published values.
            pub fn snapshot(&self) -> $plain {
                $plain { $($f: self.$f.load(Ordering::Relaxed),)* }
            }
        }
    };
}

mirror! {
    /// Atomic mirror of one worker's [`WorkerCounters`].
    WorkerCountersAtomic for WorkerCounters {
        connected_clients, backpressured_clients, total_connections_received,
        rejected_connections, total_commands_processed, total_net_input_bytes,
        total_net_output_bytes, xmsg_hops, large_value_backpressured_conns,
    }
}

impl WorkerCounters {
    /// Field-wise sum (node totals).
    pub fn add(&mut self, o: &WorkerCounters) {
        self.connected_clients += o.connected_clients;
        self.backpressured_clients += o.backpressured_clients;
        self.total_connections_received += o.total_connections_received;
        self.rejected_connections += o.rejected_connections;
        self.total_commands_processed += o.total_commands_processed;
        self.total_net_input_bytes += o.total_net_input_bytes;
        self.total_net_output_bytes += o.total_net_output_bytes;
        self.xmsg_hops += o.xmsg_hops;
        self.large_value_backpressured_conns += o.large_value_backpressured_conns;
    }
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
    /// Sum of the `page_writes_*` counters.
    pub page_writes: AtomicU64,
    pub page_writes_update: AtomicU64,
    pub page_writes_insert: AtomicU64,
    pub page_writes_delete: AtomicU64,
    pub page_writes_move: AtomicU64,
    pub page_writes_scrub: AtomicU64,
    pub page_writes_txn: AtomicU64,
    pub extent_writes: AtomicU64,
    pub host_bytes_written: AtomicU64,
    pub user_bytes_written: AtomicU64,
    pub sector_writes: AtomicU64,
    pub full_page_writes: AtomicU64,
    pub moves: AtomicU64,
    pub fp_false_positive_reads: AtomicU64,
    pub stale_copies: AtomicU64,
    pub foreground_scrub_writes: AtomicU64,
    pub txn_markers: AtomicU64,
    pub txn_deletes_split: AtomicU64,
    pub space_wait_timeouts: AtomicU64,
    pub flush_reclaim_pending_pages: AtomicU64,
    pub corrupt_pages: AtomicU64,
    pub unexpected_orphans: AtomicU64,
    pub index_bytes: AtomicU64,
    pub alloc_state_bytes: AtomicU64,
    pub dirty_bytes: AtomicU64,
}

/// `store` every listed field of `src` into the same-named atomic of `dst`.
macro_rules! store_fields {
    ($dst:expr, $src:expr, $($f:ident),*) => {
        $($dst.$f.store($src.$f, Ordering::Relaxed);)*
    };
}

/// `load` every listed atomic of `src` into the same-named field of `dst`.
macro_rules! load_fields {
    ($dst:expr, $src:expr, $($f:ident),*) => {
        $($dst.$f = $src.$f.load(Ordering::Relaxed);)*
    };
}

impl PartitionCountersAtomic {
    /// Publishes a partition's plain counters (once per tick).
    pub fn publish(&self, c: &PartitionCounters) {
        for db in 0..16 {
            self.keys[db].store(c.keys[db], Ordering::Relaxed);
            self.expires[db].store(c.expires[db], Ordering::Relaxed);
        }
        let writes = c.page_writes_update
            + c.page_writes_insert
            + c.page_writes_delete
            + c.page_writes_move
            + c.page_writes_scrub
            + c.page_writes_txn;
        self.page_writes.store(writes, Ordering::Relaxed);
        store_fields!(self, c, keyspace_hits, keyspace_misses, expired_keys, page_reads,
            page_writes_update, page_writes_insert, page_writes_delete, page_writes_move,
            page_writes_scrub, page_writes_txn, extent_writes, host_bytes_written,
            user_bytes_written, sector_writes, full_page_writes, moves, fp_false_positive_reads,
            stale_copies, foreground_scrub_writes, txn_markers, txn_deletes_split, space_wait_timeouts, flush_reclaim_pending_pages,
            corrupt_pages, unexpected_orphans, index_bytes, alloc_state_bytes, dirty_bytes);
    }

    /// The last published values.
    pub fn snapshot(&self) -> PartitionCounters {
        let mut c = PartitionCounters::default();
        for db in 0..16 {
            c.keys[db] = self.keys[db].load(Ordering::Relaxed);
            c.expires[db] = self.expires[db].load(Ordering::Relaxed);
        }
        load_fields!(c, self, keyspace_hits, keyspace_misses, expired_keys, page_reads,
            page_writes_update, page_writes_insert, page_writes_delete, page_writes_move,
            page_writes_scrub, page_writes_txn, extent_writes, host_bytes_written,
            user_bytes_written, sector_writes, full_page_writes, moves, fp_false_positive_reads,
            stale_copies, foreground_scrub_writes, txn_markers, txn_deletes_split, space_wait_timeouts, flush_reclaim_pending_pages,
            corrupt_pages, unexpected_orphans, index_bytes, alloc_state_bytes, dirty_bytes);
        c
    }

    /// Publishes the key counts of every database, so that DBSIZE and INFO keyspace already
    /// count a write when its reply is released (the other counters follow at tick
    /// granularity).
    #[inline]
    pub fn publish_keys(&self, c: &PartitionCounters) {
        for db in 0..16 {
            self.keys[db].store(c.keys[db], Ordering::Relaxed);
            self.expires[db].store(c.expires[db], Ordering::Relaxed);
        }
    }

    /// Keys of `db` (DBSIZE contribution).
    pub fn keys(&self, db: u8) -> u64 {
        self.keys[usize::from(db)].load(Ordering::Relaxed)
    }
}

/// Field-wise sum of partition counters (node totals for INFO).
pub fn sum_partitions(parts: &[PartitionCountersAtomic]) -> PartitionCounters {
    let mut t = PartitionCounters::default();
    for p in parts {
        let c = p.snapshot();
        for db in 0..16 {
            t.keys[db] += c.keys[db];
            t.expires[db] += c.expires[db];
        }
        macro_rules! add {
            ($($f:ident),*) => { $(t.$f += c.$f;)* };
        }
        add!(keyspace_hits, keyspace_misses, expired_keys, page_reads, page_writes_update,
            page_writes_insert, page_writes_delete, page_writes_move, page_writes_scrub,
            page_writes_txn, extent_writes, host_bytes_written, user_bytes_written, sector_writes,
            full_page_writes, moves, fp_false_positive_reads, stale_copies,
            foreground_scrub_writes, txn_markers, txn_deletes_split, space_wait_timeouts, flush_reclaim_pending_pages, corrupt_pages,
            unexpected_orphans, index_bytes, alloc_state_bytes, dirty_bytes);
    }
    t
}

/// Per-worker command statistics (§21 commandstats): one counter set per [`CmdId`], written
/// only by the owning worker. `rejected_calls` counts commands refused before execution
/// (arity, NOAUTH, LOADING, gate), `failed_calls` commands that replied an error.
pub struct CommandStats {
    calls: Box<[AtomicU64]>,
    usec: Box<[AtomicU64]>,
    rejected: Box<[AtomicU64]>,
    failed: Box<[AtomicU64]>,
}

/// One command's statistics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CommandStat {
    pub calls: u64,
    pub usec: u64,
    pub rejected_calls: u64,
    pub failed_calls: u64,
}

fn zeroed(n: usize) -> Box<[AtomicU64]> {
    (0..n).map(|_| AtomicU64::new(0)).collect()
}

impl Default for CommandStats {
    fn default() -> Self {
        CommandStats {
            calls: zeroed(CmdId::COUNT),
            usec: zeroed(CmdId::COUNT),
            rejected: zeroed(CmdId::COUNT),
            failed: zeroed(CmdId::COUNT),
        }
    }
}

impl CommandStats {
    /// One executed call of `id` taking `us` microseconds (`failed`: it replied an error).
    #[inline]
    pub fn record(&self, id: CmdId, us: u64, failed: bool) {
        let i = id as usize;
        // single writer: plain load + store is enough and avoids locked instructions
        bump(&self.calls[i], 1);
        bump(&self.usec[i], us);
        if failed {
            bump(&self.failed[i], 1);
        }
    }

    /// A call of `id` refused before execution.
    #[inline]
    pub fn reject(&self, id: CmdId) {
        bump(&self.rejected[id as usize], 1);
    }

    pub fn get(&self, id: CmdId) -> CommandStat {
        let i = id as usize;
        CommandStat {
            calls: self.calls[i].load(Ordering::Relaxed),
            usec: self.usec[i].load(Ordering::Relaxed),
            rejected_calls: self.rejected[i].load(Ordering::Relaxed),
            failed_calls: self.failed[i].load(Ordering::Relaxed),
        }
    }

    /// CONFIG RESETSTAT.
    pub fn reset(&self) {
        for v in [&self.calls, &self.usec, &self.rejected, &self.failed] {
            for a in v.iter() {
                a.store(0, Ordering::Relaxed);
            }
        }
    }
}

/// Single-writer increment (the owning worker is the only writer).
#[inline]
fn bump(a: &AtomicU64, n: u64) {
    a.store(a.load(Ordering::Relaxed).wrapping_add(n), Ordering::Relaxed);
}

/// Per-worker command latency histograms (§21 latencystats). The worker records samples into a
/// local batch and folds them in once per tick, so the lock is taken once per tick, not per
/// command; INFO takes it to merge.
#[derive(Default)]
pub struct LatencyStats {
    hists: parking_lot::Mutex<Vec<Option<Box<Hist>>>>,
}

impl LatencyStats {
    /// Folds a tick's `(command, microseconds)` samples in.
    pub fn record_batch(&self, samples: &[(CmdId, u32)]) {
        if samples.is_empty() {
            return;
        }
        let mut h = self.hists.lock();
        if h.is_empty() {
            h.resize_with(CmdId::COUNT, || None);
        }
        for &(id, us) in samples {
            h[id as usize].get_or_insert_with(|| Box::new(Hist::new())).record_us(u64::from(us));
        }
    }

    /// Merges this worker's histogram of `id` into `into`.
    pub fn merge_into(&self, id: CmdId, into: &mut Hist) -> bool {
        let h = self.hists.lock();
        match h.get(id as usize).and_then(Option::as_ref) {
            Some(x) => {
                into.merge(x);
                true
            }
            None => false,
        }
    }

    /// CONFIG RESETSTAT.
    pub fn reset(&self) {
        self.hists.lock().clear();
    }
}

/// Renders the Prometheus text exposition.
pub fn render_prometheus(shared: &crate::shared::NodeShared, out: &mut Vec<u8>) {
    let mut s = String::with_capacity(8192);
    let mut w = WorkerCounters::default();
    for wc in &shared.worker_counters {
        w.add(&wc.snapshot());
    }
    let gauge = |s: &mut String, name: &str, v: u64| {
        let _ = writeln!(s, "# TYPE {name} gauge\n{name} {v}");
    };
    let counter = |s: &mut String, name: &str, v: u64| {
        let _ = writeln!(s, "# TYPE {name} counter\n{name} {v}");
    };
    gauge(&mut s, "kvd_clients_connected_clients", w.connected_clients);
    gauge(&mut s, "kvd_clients_backpressured_clients", w.backpressured_clients);
    gauge(&mut s, "kvd_kvd_large_value_backpressured_conns", w.large_value_backpressured_conns);
    counter(&mut s, "kvd_stats_total_connections_received", w.total_connections_received);
    counter(&mut s, "kvd_stats_rejected_connections", w.rejected_connections);
    counter(&mut s, "kvd_stats_total_commands_processed", w.total_commands_processed);
    counter(&mut s, "kvd_stats_total_net_input_bytes", w.total_net_input_bytes);
    counter(&mut s, "kvd_stats_total_net_output_bytes", w.total_net_output_bytes);
    counter(&mut s, "kvd_stats_xmsg_hops", w.xmsg_hops);
    gauge(&mut s, "kvd_stats_instantaneous_ops_per_sec", shared.ops_per_sec.load(Ordering::Relaxed));
    gauge(&mut s, "kvd_persistence_loading", u64::from(shared.loading()));
    gauge(&mut s, "kvd_memory_large_value_bytes", shared.budget.used());

    let t = sum_partitions(&shared.counters);
    counter(&mut s, "kvd_stats_keyspace_hits", t.keyspace_hits);
    counter(&mut s, "kvd_stats_keyspace_misses", t.keyspace_misses);
    counter(&mut s, "kvd_stats_expired_keys", t.expired_keys);
    gauge(&mut s, "kvd_memory_index_bytes", t.index_bytes);
    gauge(&mut s, "kvd_memory_alloc_state_bytes", t.alloc_state_bytes);
    gauge(&mut s, "kvd_memory_dirty_bytes", t.dirty_bytes);
    counter(&mut s, "kvd_kvd_page_reads", t.page_reads);
    for (name, v) in [
        ("update", t.page_writes_update),
        ("insert", t.page_writes_insert),
        ("delete", t.page_writes_delete),
        ("move", t.page_writes_move),
        ("scrub", t.page_writes_scrub),
        ("txn", t.page_writes_txn),
    ] {
        counter(&mut s, &format!("kvd_kvd_page_writes_{name}"), v);
    }
    counter(&mut s, "kvd_kvd_extent_writes", t.extent_writes);
    counter(&mut s, "kvd_kvd_host_bytes_written", t.host_bytes_written);
    counter(&mut s, "kvd_kvd_user_bytes_written", t.user_bytes_written);
    counter(&mut s, "kvd_kvd_sector_writes", t.sector_writes);
    counter(&mut s, "kvd_kvd_full_page_writes", t.full_page_writes);
    counter(&mut s, "kvd_kvd_moves", t.moves);
    counter(&mut s, "kvd_kvd_fp_false_positive_reads", t.fp_false_positive_reads);
    gauge(&mut s, "kvd_kvd_stale_copies", t.stale_copies);
    counter(&mut s, "kvd_kvd_foreground_scrub_writes", t.foreground_scrub_writes);
    gauge(&mut s, "kvd_kvd_txn_markers", t.txn_markers);
    gauge(&mut s, "kvd_kvd_txn_deletes_split", t.txn_deletes_split);
    gauge(&mut s, "kvd_kvd_space_wait_timeouts", t.space_wait_timeouts);
    gauge(&mut s, "kvd_kvd_flush_reclaim_pending_pages", t.flush_reclaim_pending_pages);
    gauge(&mut s, "kvd_kvd_corrupt_pages", t.corrupt_pages);
    gauge(&mut s, "kvd_kvd_unexpected_orphans", t.unexpected_orphans);

    let _ = writeln!(s, "# TYPE kvd_keyspace_keys gauge");
    for (p, c) in shared.counters.iter().enumerate() {
        let n: u64 = c.keys.iter().map(|k| k.load(Ordering::Relaxed)).sum();
        let _ = writeln!(s, "kvd_keyspace_keys{{partition=\"{p}\"}} {n}");
    }
    let _ = writeln!(s, "# TYPE kvd_stats_commands_processed counter");
    for (i, wc) in shared.worker_counters.iter().enumerate() {
        let _ = writeln!(s, "kvd_stats_commands_processed{{worker=\"{i}\"}} {}", wc.total_commands_processed.load(Ordering::Relaxed));
    }
    let _ = writeln!(s, "# TYPE kvd_commandstats_calls counter");
    let mut usec = String::from("# TYPE kvd_commandstats_usec counter\n");
    for spec in kv_cmd::all_with_subcommands() {
        let mut total = CommandStat::default();
        for cs in &shared.command_stats {
            let c = cs.get(spec.id);
            total.calls += c.calls;
            total.usec += c.usec;
        }
        if total.calls > 0 {
            let _ = writeln!(s, "kvd_commandstats_calls{{command=\"{}\"}} {}", spec.name, total.calls);
            let _ = writeln!(usec, "kvd_commandstats_usec{{command=\"{}\"}} {}", spec.name, total.usec);
        }
    }
    s.push_str(&usec);
    out.extend_from_slice(s.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_mirror_round_trips() {
        let mut c = PartitionCounters::default();
        c.keys[3] = 7;
        c.expires[3] = 2;
        c.page_writes_insert = 5;
        c.page_writes_scrub = 1;
        c.corrupt_pages = 9;
        let a = PartitionCountersAtomic::default();
        a.publish(&c);
        assert_eq!(a.keys(3), 7);
        assert_eq!(a.expires[3].load(Ordering::Relaxed), 2);
        assert_eq!(a.page_writes.load(Ordering::Relaxed), 6);
        let s = a.snapshot();
        assert_eq!(s.corrupt_pages, 9);
        assert_eq!(s.page_writes_insert, 5);
        let t = sum_partitions(&[a, PartitionCountersAtomic::default()]);
        assert_eq!(t.keys[3], 7);
    }

    #[test]
    fn worker_mirror_and_command_stats() {
        let w = WorkerCountersAtomic::default();
        let c = WorkerCounters { total_commands_processed: 3, connected_clients: 2, ..Default::default() };
        w.publish(&c);
        assert_eq!(w.snapshot().total_commands_processed, 3);
        let cs = CommandStats::default();
        cs.record(CmdId::Get, 5, false);
        cs.record(CmdId::Get, 7, true);
        cs.reject(CmdId::Get);
        assert_eq!(cs.get(CmdId::Get), CommandStat { calls: 2, usec: 12, rejected_calls: 1, failed_calls: 1 });
        cs.reset();
        assert_eq!(cs.get(CmdId::Get), CommandStat::default());
        let l = LatencyStats::default();
        l.record_batch(&[(CmdId::Set, 10), (CmdId::Set, 20)]);
        let mut h = Hist::new();
        assert!(l.merge_into(CmdId::Set, &mut h));
        assert_eq!(h.count(), 2);
        assert!(!l.merge_into(CmdId::Get, &mut h));
    }
}
