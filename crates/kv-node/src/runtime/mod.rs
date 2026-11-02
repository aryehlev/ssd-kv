//! Worker runtime (DESIGN §6.1-§6.4).
//!
//! * [`ring`]: the io_uring wrapper (single issuer, deferred task running, SQ 4096 / CQ 16384);
//!   the only file of kv-node allowed to contain `unsafe`.
//! * [`worker`]: the per-worker tick loop, connections and network I/O.
//! * [`exec`]: command dispatch and execution on a worker (jobs, durability waits,
//!   cross-partition transactions, scans, node operations).
//! * [`backend`]: what a worker drives its partitions through (the page store, or an in-RAM
//!   store in tests).
//! * [`xmsg`]: cross-worker messages over bounded SPSC queues with an eventfd doorbell.
//! * [`watch`]: the node's WATCH sessions and the keys they watch.
//! * [`node`]: node startup (devices, engine, meta thread, listeners, workers, service thread)
//!   and shutdown.

pub mod backend;
pub mod exec;
pub mod node;
pub mod ring;
pub mod watch;
pub mod worker;
pub mod xmsg;

#[cfg(test)]
mod tests;
