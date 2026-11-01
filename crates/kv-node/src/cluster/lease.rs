//! The node's lease (DESIGN §23.4, §23.10).
//!
//! The agent sets the expiry (CLOCK_BOOTTIME ns, `t_send + lease_ms * 0.95 - 10 ms`) from each
//! `LeaseGrant`; a primary or readable replica serves only while `now < expiry`, re-checked
//! right before each reply is released. A primary cut off from the coordinator stops serving at
//! expiry and answers MOVED to a coordinator.

use std::sync::atomic::{AtomicU64, Ordering};

/// expiry boot_ns
pub struct LeaseCell(AtomicU64);

impl LeaseCell {
    /// An expired lease.
    pub fn new() -> Self {
        LeaseCell(AtomicU64::new(0))
    }

    pub fn valid(&self, boot_ns: u64) -> bool {
        boot_ns < self.0.load(Ordering::Acquire)
    }

    pub fn set(&self, expiry_boot_ns: u64) {
        self.0.store(expiry_boot_ns, Ordering::Release);
    }
}

impl Default for LeaseCell {
    fn default() -> Self {
        Self::new()
    }
}
