//! Large-value memory soft limit (DESIGN §6.7, §6.9, LC-15).
//!
//! One node-global atomic counter of bytes held by extent/chunkmap buffers being received, read
//! or written. When the parser reports a bulk longer than `SMALL_REC_MAX` for the command being
//! received and the command holds no reservation yet, the connection checks
//! `counter < large-value-memory / 2`. If the check fails the bytes stay unconsumed in
//! `read_buf` and the connection stops re-arming recv; if it passes, every large bulk of that
//! command reserves its rounded length unconditionally. Reads never consult the limit. The
//! check happens only before a command is consumed; nothing ever queues for budget.

use std::sync::atomic::{AtomicU64, Ordering};

pub struct LargeValueBudget {
    used: AtomicU64,
    soft_limit: u64,
}

impl LargeValueBudget {
    pub fn new(large_value_memory: u64) -> Self {
        LargeValueBudget { used: AtomicU64::new(0), soft_limit: large_value_memory / 2 }
    }

    /// The pre-consume check for a command holding no reservation yet.
    pub fn may_start(&self) -> bool {
        self.used.load(Ordering::Relaxed) < self.soft_limit
    }

    /// Unconditional reservation of a consumed command's buffer.
    pub fn reserve(&self, bytes: u64) {
        self.used.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn release(&self, bytes: u64) {
        self.used.fetch_sub(bytes, Ordering::Relaxed);
    }

    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }
}
