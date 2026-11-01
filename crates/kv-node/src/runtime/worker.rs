//! The worker tick loop (DESIGN §6.4).
//!
//! 1. Reap completions: accept, recv, send, disk completions (to the owning partition), flush
//!    completions (to the device's flush tracker), doorbell, timeout.
//! 2. For each connection with new bytes and not backpressured: parse complete commands (bounded
//!    by `max-pending-per-conn`; the large-value soft limit and txn credits are checked before a
//!    command is consumed, §6.7) and route each (local partition: submit inline; remote:
//!    `XMsg::Cmd`).
//! 3. Drain inbound queues, one FIFO turn per source.
//! 4. For each owned partition: execute ready requests to completion, then `tick` (page writer,
//!    foreground scrubs, background tasks under static rates).
//! 5. Flush trackers: at most one flush per device in flight.
//! 6. Release replies whose tickets / commits are satisfied, strictly in request order per
//!    connection; one send per connection with all ready bytes.
//! 7. Enter the ring (wait only when nothing is runnable).

use std::sync::Arc;

use kv_storage::cache::PageCache;
use kv_storage::flush::FlushTracker;
use kv_storage::partition::PartitionStore;

use crate::runtime::ring::Ring;
use crate::shared::NodeShared;

pub struct Worker {
    pub id: u16,
    ring: Ring,
    shared: Arc<NodeShared>,
    partitions: Vec<PartitionStore>,
    cache: PageCache,
    flush: Vec<FlushTracker>,
}

impl Worker {
    /// Runs the tick loop until shutdown.
    pub fn run(&mut self) {
        todo!("DESIGN §6.4: tick loop")
    }
}
