//! Node-wide shared state (DESIGN §6.1, §6.3, §24.11).
//!
//! `NodeShared` is created once at startup and shared by workers, the meta thread's reply hooks
//! and the agent thread: the current configuration (swapped whole on CONFIG SET), per-partition
//! counter mirrors, worker handles (inbound queues and doorbells), the current ownership
//! projection (swapped whole on every applied assignment), the lease cell and the start time.
//!
//! Besides the §24.11 fields it holds the node-global pieces several threads read: partition
//! readiness (`-LOADING`, §18.1), per-worker counter mirrors and command statistics (§21), the
//! large-value budget (§6.9), the client count checked at accept (`maxclients`), client ids, the
//! cross-partition transaction gate (§6.8), the last durability barrier time (LASTSAVE) and the
//! shutdown request.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering};

use kv_common::Now;
use kv_storage::partition::PartState;

use crate::cluster::lease::LeaseCell;
use crate::cluster::ownership::Ownership;
use crate::config::Config;
use crate::dispatch::xtxn::XTxnGate;
use crate::metrics::{CommandStats, LatencyStats, PartitionCountersAtomic, WorkerCountersAtomic};
use crate::net::budget::LargeValueBudget;
use crate::runtime::ring::sys;
use crate::runtime::xmsg::{XMSG_QUEUE_CAP, XMsg};

/// Commit positions of the intent table (64 slots x 60 ids, §14.6).
pub const TXN_CREDITS: u32 = (kv_storage::intent::INTENT_SLOTS * kv_storage::intent::IDS_PER_COPY) as u32;

pub struct NodeShared {
    pub cfg: parking_lot::RwLock<Arc<Config>>,
    pub counters: Vec<PartitionCountersAtomic>,
    pub workers: Vec<WorkerHandle>,
    pub ownership: parking_lot::RwLock<Arc<Ownership>>,
    pub lease: LeaseCell,
    pub start: Now,
    /// Bumped on every configuration swap; workers refresh their cached copy when it changes.
    pub cfg_gen: AtomicU64,
    /// P (from the superblock).
    pub partitions: u16,
    /// Per partition: [`PartState`] as `u8` (0 Loading, 1 Ready, 2 ReadOnly).
    pub part_state: Vec<AtomicU8>,
    /// Partitions still loading (INFO `loading`).
    pub loading_partitions: AtomicU32,
    pub worker_counters: Vec<WorkerCountersAtomic>,
    pub command_stats: Vec<CommandStats>,
    pub latency: Vec<LatencyStats>,
    /// Node-global large-value soft limit (§6.9).
    pub budget: LargeValueBudget,
    /// Connected clients (checked against `maxclients` at accept).
    pub clients: AtomicU64,
    next_client_id: AtomicU64,
    /// 40 hex characters, random per process (INFO `run_id`).
    pub run_id: String,
    /// Sampled by the service thread (INFO `instantaneous_ops_per_sec`).
    pub ops_per_sec: AtomicU64,
    /// Unix seconds of the last completed durability barrier (LASTSAVE; startup time first).
    pub lastsave_unix: AtomicU64,
    /// Restart facts for INFO persistence (§21).
    pub recovery: RecoveryInfo,
    /// One cross-partition transaction at a time (§6.8; taken without blocking).
    pub xtxn: XTxnGate,
    /// Txn credits not in use (§14.6: one per consumed multi-key write; returned when the
    /// meta thread reports the txn resolved).
    pub txn_credits: AtomicU32,
    /// A snapshot section failed: every partition takes the full scan (§18.2).
    pub recovery_fallback: AtomicBool,
    /// Partitions whose recovery finished.
    pub partitions_recovered: AtomicU32,
    /// Whether the shutdown writes the index snapshot (`SHUTDOWN NOSAVE` clears it).
    pub shutdown_snapshot: AtomicBool,
    shutdown: AtomicBool,
    /// Cluster mode: the node-wide cluster state (set once at startup).
    cluster: std::sync::OnceLock<Arc<crate::cluster::ClusterShared>>,
}

/// Restart facts (§18, INFO persistence).
#[derive(Default)]
pub struct RecoveryInfo {
    /// 0 = snapshot, 1 = full scan.
    pub scanned: AtomicBool,
    pub duration_ms: AtomicU64,
    pub bytes_scanned: AtomicU64,
    pub last_shutdown_clean: AtomicBool,
}

/// How other threads reach worker `id`.
pub struct WorkerHandle {
    pub id: u16,
    /// `inbox[src]`: the SPSC queue from worker `src` (capacity 4096).
    pub inbox: Vec<Arc<crossbeam_queue::ArrayQueue<XMsg>>>,
    /// Doorbell eventfd.
    pub eventfd: i32,
    /// Set while the worker waits in `io_uring_enter`; senders ring the doorbell only then.
    pub parked: Arc<AtomicBool>,
    /// Messages from threads that are not workers (meta thread, agent, service thread):
    /// unbounded, so those threads never block or drop a message.
    pub side: crossbeam_queue::SegQueue<XMsg>,
}

impl Drop for WorkerHandle {
    fn drop(&mut self) {
        sys::close(self.eventfd);
    }
}

fn part_state_code(s: PartState) -> u8 {
    match s {
        PartState::Loading => 0,
        PartState::Ready => 1,
        PartState::ReadOnly => 2,
    }
}

impl NodeShared {
    /// Node state for `partitions` partitions served by `workers` workers.
    pub fn new(cfg: Config, partitions: u16, workers: u16) -> std::io::Result<Self> {
        let mut handles = Vec::with_capacity(usize::from(workers));
        for id in 0..workers {
            handles.push(WorkerHandle {
                id,
                inbox: (0..workers).map(|_| Arc::new(crossbeam_queue::ArrayQueue::new(XMSG_QUEUE_CAP))).collect(),
                eventfd: sys::eventfd()?,
                parked: Arc::new(AtomicBool::new(false)),
                side: crossbeam_queue::SegQueue::new(),
            });
        }
        let start = Now::read();
        let large = cfg.large_value_memory;
        Ok(NodeShared {
            cfg: parking_lot::RwLock::new(Arc::new(cfg)),
            counters: (0..partitions).map(|_| PartitionCountersAtomic::default()).collect(),
            workers: handles,
            ownership: parking_lot::RwLock::new(Arc::new(Ownership::standalone())),
            lease: LeaseCell::new(),
            start,
            cfg_gen: AtomicU64::new(0),
            partitions,
            part_state: (0..partitions).map(|_| AtomicU8::new(0)).collect(),
            loading_partitions: AtomicU32::new(u32::from(partitions)),
            worker_counters: (0..workers).map(|_| WorkerCountersAtomic::default()).collect(),
            command_stats: (0..workers).map(|_| CommandStats::default()).collect(),
            latency: (0..workers).map(|_| LatencyStats::default()).collect(),
            budget: LargeValueBudget::new(large),
            clients: AtomicU64::new(0),
            next_client_id: AtomicU64::new(1),
            run_id: random_run_id(start),
            ops_per_sec: AtomicU64::new(0),
            lastsave_unix: AtomicU64::new(start.unix_ms / 1000),
            recovery: RecoveryInfo::default(),
            xtxn: XTxnGate::new(),
            txn_credits: AtomicU32::new(TXN_CREDITS),
            recovery_fallback: AtomicBool::new(false),
            partitions_recovered: AtomicU32::new(0),
            shutdown_snapshot: AtomicBool::new(true),
            shutdown: AtomicBool::new(false),
            cluster: std::sync::OnceLock::new(),
        })
    }

    /// The node-wide cluster state (cluster mode only).
    #[inline]
    pub fn cluster(&self) -> Option<Arc<crate::cluster::ClusterShared>> {
        self.cluster.get().cloned()
    }

    /// Installs the cluster state (once, at startup).
    pub fn set_cluster(&self, cs: Arc<crate::cluster::ClusterShared>) {
        let _ = self.cluster.set(cs);
    }

    /// The current configuration.
    #[inline]
    pub fn config(&self) -> Arc<Config> {
        self.cfg.read().clone()
    }

    /// CONFIG SET: applies `f` to a copy of the configuration and swaps it in when `f` succeeds
    /// (workers pick the new copy up on their next tick; the diagnostics level applies at once).
    pub fn update_config<E>(&self, f: impl FnOnce(&mut Config) -> Result<(), E>) -> Result<(), E> {
        let mut w = self.cfg.write();
        let mut next = Config::clone(&w);
        f(&mut next)?;
        if next.loglevel != w.loglevel {
            crate::log::set_level(next.loglevel);
        }
        if next.large_value_memory != w.large_value_memory {
            self.budget.set_limit(next.large_value_memory);
        }
        *w = Arc::new(next);
        self.cfg_gen.fetch_add(1, Ordering::Release);
        Ok(())
    }

    /// A fresh client id (unique per process, starting at 1).
    #[inline]
    pub fn next_client_id(&self) -> u64 {
        self.next_client_id.fetch_add(1, Ordering::Relaxed)
    }

    /// The published state of partition `p`.
    pub fn partition_state(&self, p: u16) -> PartState {
        match self.part_state[usize::from(p)].load(Ordering::Acquire) {
            0 => PartState::Loading,
            1 => PartState::Ready,
            _ => PartState::ReadOnly,
        }
    }

    /// Publishes the state of partition `p` (owner worker only).
    pub fn set_partition_state(&self, p: u16, s: PartState) {
        let old = self.part_state[usize::from(p)].swap(part_state_code(s), Ordering::AcqRel);
        if old == 0 && s != PartState::Loading {
            self.loading_partitions.fetch_sub(1, Ordering::AcqRel);
        } else if old != 0 && s == PartState::Loading {
            self.loading_partitions.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// Whether any partition is still loading (INFO `loading`).
    #[inline]
    pub fn loading(&self) -> bool {
        self.loading_partitions.load(Ordering::Acquire) != 0
    }

    /// DBSIZE: the sum of the partitions' published key counts of `db`.
    pub fn dbsize(&self, db: u8) -> u64 {
        self.counters.iter().map(|c| c.keys(db)).sum()
    }

    /// Asks every worker to stop (SHUTDOWN, SIGTERM/SIGINT); idempotent.
    pub fn request_shutdown(&self) {
        if !self.shutdown.swap(true, Ordering::AcqRel) {
            for w in &self.workers {
                w.send_side(XMsg::Shutdown);
            }
        }
    }

    #[inline]
    pub fn shutdown_requested(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    /// Takes a txn credit before a multi-key write is consumed (§6.7, §14.6).
    pub fn take_txn_credit(&self) -> bool {
        self.txn_credits.fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1)).is_ok()
    }

    /// Returns txn credits (resolved transactions, or a command answered before it reached a
    /// partition).
    pub fn give_back_txn_credits(&self, n: u32) {
        let after = self.txn_credits.fetch_add(n, Ordering::AcqRel) + n;
        debug_assert!(after <= TXN_CREDITS, "more txn credits returned than taken");
    }

    /// Wakes every parked worker (the transaction gate moved on).
    pub fn wake_all(&self) {
        for w in &self.workers {
            w.wake();
        }
    }
}

/// 40 hex characters from the kernel's random source (falls back to the clocks).
fn random_run_id(now: Now) -> String {
    use std::io::Read;
    let mut b = [0u8; 20];
    if std::fs::File::open("/dev/urandom").and_then(|mut f| f.read_exact(&mut b)).is_err() {
        let mix = now.mono_ns ^ now.unix_ms.rotate_left(17) ^ u64::from(std::process::id());
        for (i, x) in b.iter_mut().enumerate() {
            *x = (mix.rotate_left(i as u32 * 7) & 0xff) as u8;
        }
    }
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_states_track_loading() {
        let s = NodeShared::new(Config::default(), 4, 2).unwrap();
        assert!(s.loading());
        for p in 0..4 {
            assert_eq!(s.partition_state(p), PartState::Loading);
            s.set_partition_state(p, PartState::Ready);
        }
        assert!(!s.loading());
        s.set_partition_state(1, PartState::ReadOnly);
        assert!(!s.loading());
        assert_eq!(s.partition_state(1), PartState::ReadOnly);
        assert_eq!(s.run_id.len(), 40);
        assert_eq!(s.next_client_id(), 1);
        assert_eq!(s.next_client_id(), 2);
    }

    #[test]
    fn config_updates_swap_and_bump_generation() {
        let s = NodeShared::new(Config::default(), 4, 1).unwrap();
        let g = s.cfg_gen.load(Ordering::Acquire);
        s.update_config(|c| c.set("maxclients", "9")).unwrap();
        assert_eq!(s.config().maxclients, 9);
        assert_eq!(s.cfg_gen.load(Ordering::Acquire), g + 1);
        assert!(s.update_config(|c| c.set("port", "1")).is_err());
        assert_eq!(s.cfg_gen.load(Ordering::Acquire), g + 1);
    }

    #[test]
    fn shutdown_is_broadcast_once() {
        let s = NodeShared::new(Config::default(), 4, 2).unwrap();
        s.request_shutdown();
        s.request_shutdown();
        assert!(s.shutdown_requested());
        for w in &s.workers {
            assert!(matches!(w.side.pop(), Some(XMsg::Shutdown)));
            assert!(w.side.pop().is_none());
        }
    }
}
