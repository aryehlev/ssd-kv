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

use kv_storage::layout::{PAGE, SMALL_REC_MAX};

pub struct LargeValueBudget {
    used: AtomicU64,
    soft_limit: AtomicU64,
}

impl LargeValueBudget {
    pub fn new(large_value_memory: u64) -> Self {
        LargeValueBudget { used: AtomicU64::new(0), soft_limit: AtomicU64::new(large_value_memory / 2) }
    }

    /// CONFIG SET large-value-memory: the soft limit is half the new maximum.
    pub fn set_limit(&self, large_value_memory: u64) {
        self.soft_limit.store(large_value_memory / 2, Ordering::Relaxed);
    }

    /// The pre-consume check for a command holding no reservation yet.
    pub fn may_start(&self) -> bool {
        self.used.load(Ordering::Relaxed) < self.soft_limit.load(Ordering::Relaxed)
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

/// Whether a bulk of `len` bytes is a large value (it does not fit a small record).
#[inline]
pub fn is_large(len: usize) -> bool {
    len > SMALL_REC_MAX
}

/// The accounted size of a large bulk: its length rounded up to whole 4 KiB pages.
#[inline]
pub fn rounded(len: usize) -> u64 {
    (len as u64).div_ceil(PAGE as u64) * PAGE as u64
}

/// The reservation held by one consumed command (released when its buffers are freed, i.e.
/// when this value is dropped). Holds an `Arc` of the node state that owns the counter.
pub struct Reservation {
    owner: Option<std::sync::Arc<crate::shared::NodeShared>>,
    bytes: u64,
}

impl Reservation {
    /// No reservation (small commands).
    pub const NONE: Reservation = Reservation { owner: None, bytes: 0 };

    /// Reserves `bytes` on `shared.budget` (unconditionally: the check came before consuming).
    pub fn take(shared: &std::sync::Arc<crate::shared::NodeShared>, bytes: u64) -> Self {
        if bytes == 0 {
            return Reservation::NONE;
        }
        shared.budget.reserve(bytes);
        Reservation { owner: Some(shared.clone()), bytes }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if let Some(s) = self.owner.take() {
            s.budget.release(self.bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn soft_limit_is_half_and_single() {
        let b = LargeValueBudget::new(8192);
        assert!(b.may_start());
        b.reserve(4095);
        assert!(b.may_start());
        b.reserve(1);
        assert!(!b.may_start(), "the same threshold stops and resumes");
        b.release(1);
        assert!(b.may_start());
        b.set_limit(4);
        assert!(!b.may_start());
        assert_eq!(b.used(), 4095);
    }

    #[test]
    fn rounding_and_size_classes() {
        assert!(!is_large(2048));
        assert!(is_large(2049));
        assert_eq!(rounded(2049), 4096);
        assert_eq!(rounded(4096), 4096);
        assert_eq!(rounded(4097), 8192);
    }

    #[test]
    fn reservations_release_on_drop() {
        let s = std::sync::Arc::new(crate::shared::NodeShared::new(crate::config::Config::default(), 4, 1).unwrap());
        {
            let _r = Reservation::take(&s, 8192);
            assert_eq!(s.budget.used(), 8192);
            let _n = Reservation::take(&s, 0);
            assert_eq!(s.budget.used(), 8192);
        }
        assert_eq!(s.budget.used(), 0);
    }
}
