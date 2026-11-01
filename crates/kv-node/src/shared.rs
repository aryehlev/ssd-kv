//! Node-wide shared state (DESIGN §6.1, §6.3, §24.11).
//!
//! `NodeShared` is created once at startup and shared by workers, the meta thread's reply hooks
//! and the agent thread: the current configuration (swapped whole on CONFIG SET), per-partition
//! counter mirrors, worker handles (inbound queues and doorbells), the current ownership
//! projection (swapped whole on every applied assignment), the lease cell and the start time.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use kv_common::Now;

use crate::cluster::lease::LeaseCell;
use crate::cluster::ownership::Ownership;
use crate::config::Config;
use crate::metrics::PartitionCountersAtomic;
use crate::runtime::xmsg::XMsg;

pub struct NodeShared {
    pub cfg: parking_lot::RwLock<Arc<Config>>,
    pub counters: Vec<PartitionCountersAtomic>,
    pub workers: Vec<WorkerHandle>,
    pub ownership: parking_lot::RwLock<Arc<Ownership>>,
    pub lease: LeaseCell,
    pub start: Now,
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
}
