//! The per-partition sync decision (DESIGN §23.7, §23.9, LC-25, LC-39).
//!
//! 1. `last_term == term` and the backlog covers `applied + 1` -> `Continue { from: applied + 1 }`
//!    (current-term frames only).
//! 2. `last_term < term`, not a learner, and the needed key sets are known -> `Repair { at }`:
//!    keys of the member's own groups after its term's takeover point (its received ring, or its
//!    post-scan divergent set sent as `RepairKeys`), keys of `(applied, takeover]` from the
//!    primary's received ring of that term, keys of whole intermediate terms and of the current
//!    term up to `at` from the primary's backlog; then one image per key stamped `at`.
//! 3. Learner (`replica add`) or operator `replica resync` -> `Full { at }` (in-place merge).
//! 4. Otherwise (including `SyncInterrupted`) -> `Reject(NeedOperatorResync)`. There is no
//!    automatic path to a full sync.

use crate::Seq;
use crate::backlog::Backlog;
use crate::frame::{Hello, RejectReason};

/// What the primary knows when a member says Hello.
pub struct PrimaryView<'a> {
    pub term: u32,
    /// `(term, takeover seq per partition)` of past terms.
    pub history: &'a [(u32, Vec<u64>)],
    pub backlog: &'a Backlog,
    pub received: &'a Backlog,
    pub is_member: bool,
    /// replica add/resync only
    pub operator_full_sync: bool,
    pub last_executed: Seq,
}

pub enum SyncDecision {
    Continue { from: Seq },
    Repair { at: Seq, primary_keys: Vec<(u8, Box<[u8]>)>, want_member_keys: bool },
    Full { at: Seq },
    Reject(RejectReason),
}

pub fn decide(h: &Hello, v: &PrimaryView<'_>) -> SyncDecision {
    todo!("DESIGN §23.7: sync decision")
}
