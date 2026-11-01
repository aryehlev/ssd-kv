//! Worker runtime (DESIGN §6.1-§6.4).
//!
//! * [`ring`]: the io_uring wrapper (single issuer, deferred task running, SQ 4096 / CQ 16384);
//!   the only file of kv-node allowed to contain `unsafe`.
//! * [`worker`]: the per-worker tick loop.
//! * [`xmsg`]: cross-worker messages over bounded SPSC queues with an eventfd doorbell.

pub mod ring;
pub mod worker;
pub mod xmsg;
