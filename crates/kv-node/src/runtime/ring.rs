//! io_uring wrapper (DESIGN §6.1, §6.4, §7.1, §13.6, §13.8).
//!
//! One ring per worker with `IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN |
//! IORING_SETUP_COOP_TASKRUN`, SQ size 4096, CQ size 16384 (Linux >= 6.1). Operations: multishot
//! accept, single-shot recv into the connection's buffer, sendmsg with iovecs for shared value
//! bodies, `O_DIRECT` reads and writes (with `RWF_DSYNC` / `RWF_ATOMIC` where configured),
//! datasync flushes, the eventfd doorbell read, and a 1 ms timeout armed only while background
//! work is pending (timers drive background pacing only, never page writes). Optional fixed
//! buffers for page-cache frames.
//!
//! This is the only file of kv-node allowed to contain `unsafe` (§24); every block carries a
//! `SAFETY:` comment. Buffers handed to the kernel stay owned by the ring until their completion.
#![allow(unsafe_code)]

/// SQ entries.
pub const SQ_ENTRIES: u32 = 4096;
/// CQ entries.
pub const CQ_ENTRIES: u32 = 16384;

pub struct Ring {
    ring: io_uring::IoUring,
}

impl Ring {
    pub fn new() -> std::io::Result<Self> {
        todo!("DESIGN §6.1: ring setup")
    }

    /// Submits queued entries and waits for at least `wait` completions.
    pub fn submit_and_wait(&mut self, wait: usize) -> std::io::Result<usize> {
        todo!("DESIGN §6.4: io_uring_enter")
    }
}
