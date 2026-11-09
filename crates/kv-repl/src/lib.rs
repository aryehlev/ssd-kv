//! `kv-repl`: per-partition replication (DESIGN §23.7-§23.9, §23.13, §24.8).
//!
//! One stream per (shard, partition): the primary's partition `p` ships [`Group`]s to partition
//! `p` of every member. Members dial the primary's replication port with a `Hello`; the primary
//! answers `Accept { Continue | Repair | Full }` or `Reject` ([`sync::decide`]). Frames carry the
//! sender's term (fencing). Every op carries its full payload (LC-29); a primary ships only the
//! groups produced in its own term, and cross-term catch-up sends key images from its own state
//! (Repair). Backlogs are RAM rings that never reach any disk (LC-23). Full sync is an in-place
//! merge that confirms identical records and writes only differing ones (LC-39). Migration
//! sources and full-sync sources advance through one scalar walk position and position barriers
//! only (LC-36, LC-37).
//!
//! * [`codec`]: group encoding; [`frame`]: frame header, handshake and frame types;
//!   [`backlog`]: RAM rings; [`primary`]: `ReplSender`; [`member`]: `MemberStream`;
//!   [`sync`]: the per-partition sync decision; [`walk`]: the scalar walk position;
//!   [`migrate`]: migration source/destination state.
#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(unsafe_code)]

pub use kv_storage::effect::{Group, Op};
pub use kv_storage::layout::Seq;

pub mod backlog;
pub mod codec;
pub mod frame;
pub mod member;
pub mod migrate;
pub mod primary;
pub mod sync;

/// single scalar walk position shared by full sync and migration sources (LC-18, LC-37)
pub mod walk {
    /// `slot_ordinal << 32 | fp32`: everything strictly below has been passed by the walk.
    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
    pub struct WalkPos(pub u64);

    /// one scalar comparison: has the walk passed `(slot_ordinal, fp)`?
    pub fn passed(cursor: WalkPos, slot_ordinal: u16, fp: u32) -> bool {
        ((u64::from(slot_ordinal) << 32) | u64::from(fp)) < cursor.0
    }
}
