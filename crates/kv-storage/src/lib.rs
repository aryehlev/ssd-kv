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
//!   [`record`], [`page`], [`extent`], [`io`], `sim` (feature `sim`), [`devprobe`], [`mod@format`];
//! * engine: [`cache`], [`flush`], [`alloc`], [`stale`], [`ticket`], [`effect`], [`image`],
//!   [`keyspace`], [`partition`], [`progress`], [`expire`], [`recovery`], [`snapshot`],
//!   [`metastore`], [`engine`].
//!
//! Runtime structure: one [`partition::PartitionStore`] per partition, driven by its owner
//! worker (submit / take_ready / exec / tick / I/O completions, see the contract at the top of
//! `partition.rs`); one page cache and one flush tracker per device per worker; one meta thread
//! ([`metastore::MetaStore`], logic in [`metastore::MetaCore`]) for every fixed-location block.
//! [`engine::PageStore`] opens the devices and hands out the partitions;
//! [`engine::LocalEngine`] drives a whole engine synchronously in one thread (tools, tests,
//! the simulator), and [`engine::format_devices`] formats in-memory devices.
//!
//! `unsafe` is allowed only in `io.rs` and `cache.rs` (§24). This crate never depends on the
//! control-plane crates (LC-30).
#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(unsafe_code)]

pub(crate) mod abort;
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
pub mod progress;
pub mod record;
pub mod recovery;
#[cfg(feature = "sim")]
pub mod sim;
pub mod snapshot;
pub mod stale;
pub mod superblock;
pub mod ticket;

pub use devprobe::probe;
pub use engine::{Boot, EngineCfg, LocalEngine, Located, PageStore, StorageEngine, format_devices};
pub use io::{BlockDevice, DevInfo, LockedMem, PageUpdateMode, SyncFileDevice};
pub use keyspace::{Keyspace, StoreError};
pub use layout::{AgId, PageId, Seq};
pub use metastore::{MetaClient, MetaCore, MetaStore};
pub use partition::{Durability, PartitionCfg, PartitionStore};
pub use recovery::{RecoveryMode, RecoveryProgress};
pub use snapshot::SnapshotWriter;
