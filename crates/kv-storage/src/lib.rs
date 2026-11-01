//! `kv-storage`: the in-place SSD page store (DESIGN §10-§19, §24.7).
//!
//! Records live in 4 KiB slotted SMALL pages (records <= 2048 B) or in allocator-chosen extents
//! at fixed addresses inside 16 MiB allocation groups (AGs). Updates stay in place (atomic page
//! mode) or inside their page with every superseded copy zeroed by the page's next build
//! (two-image mode); free space anywhere is reused by unrelated keys, and every choice among
//! equal candidates is a keyed pseudo-random draw (LC-01). Small fixed-location metadata
//! (superblock, AG table, partition and node meta, intent table) is rewritten whole into A/B
//! blocks (LC-02). There is exactly one storage engine, [`engine::PageStore`] (LC-03).
//!
//! Module map (owners in `crates/OWNERSHIP.md`):
//! * formats and I/O: [`layout`], [`superblock`], [`agtable`], [`pmeta`], [`intent`],
//!   [`record`], [`page`], [`extent`], [`io`], `sim` (feature `sim`), [`devprobe`], [`format`];
//! * engine: [`cache`], [`flush`], [`alloc`], [`stale`], [`ticket`], [`effect`], [`image`],
//!   [`keyspace`], [`partition`], [`expire`], [`recovery`], [`snapshot`], [`metastore`],
//!   [`engine`].
//!
//! `unsafe` is allowed only in `io.rs` and `cache.rs` (§24). This crate never depends on the
//! control-plane crates (LC-30).
#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(unsafe_code)]
#![allow(dead_code, unused_variables)]

pub mod agtable;
pub mod alloc;
pub mod cache;
pub mod devprobe;
pub mod effect;
pub mod engine;
pub mod expire;
pub mod extent;
pub mod flush;
pub mod format;
pub mod image;
pub mod intent;
pub mod io;
pub mod keyspace;
pub mod layout;
pub mod metastore;
pub mod page;
pub mod partition;
pub mod pmeta;
pub mod record;
pub mod recovery;
#[cfg(feature = "sim")]
pub mod sim;
pub mod snapshot;
pub mod stale;
pub mod superblock;
pub mod ticket;

pub use engine::{EngineCfg, PageStore, StorageEngine};
pub use io::{BlockDevice, DevInfo, PageUpdateMode, SyncFileDevice};
pub use keyspace::{Keyspace, StoreError};
pub use layout::{AgId, PageId, Seq};
pub use partition::{Durability, PartitionCfg, PartitionStore};
