//! `kv-index`: the RAM index (DESIGN §9, §24.6).
//!
//! Per partition a [`PartitionIndex`] holds one [`SlotTable`] per `(db, slot)` of that partition
//! (§9.1). A slot table is an extendible-hashing directory over ordered linear-probing segments
//! of 10-byte packed [`Entry`]s (§9.2-§9.4): a hash table, never a tree (LC-04), with no
//! pointers between entries (LC-20). Entries store only the SSD page of the record; the record
//! is found inside the page by `h32` and a full key compare (LC-07). Optional per-segment TTL
//! hint planes give a 16-bit lower bound of the expiry for the active expiry sweep (§9.5).
//!
//! * [`entry`]: `Entry`, `Kind`, `VType`, `Pos`, `Cand`.
//! * [`sysbuf`]: individual allocations for segment arrays, hint planes and directories
//!   (`seg_alloc`/`seg_free`, LC-05). The only file of this crate allowed to contain `unsafe`.
//! * [`segment`]: the ordered linear-probing segment and its capacity sequence (LC-09).
//! * [`table`]: `SlotTable`, the page-reference SCAN walk (LC-18), bulk build and the snapshot
//!   iterator (snapshot writer only).
//! * [`hint`], [`cursor`]: TTL hint and SCAN cursor encodings.
//! * [`partition_index`]: `PartitionIndex`.
#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(unsafe_code)]
#![allow(dead_code, unused_variables)]

pub mod cursor;
pub mod entry;
pub mod hint;
pub mod partition_index;
pub mod segment;
pub mod sysbuf;
pub mod table;

pub use entry::{Cand, Entry, Kind, Pos, VType};
pub use kv_common::KeyHash;
pub use partition_index::PartitionIndex;
pub use table::{PageRef, SCAN_STEP_MAX_PAGES, ScanFilter, ScanSpan, SlotTable};
