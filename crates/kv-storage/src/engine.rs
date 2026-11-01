//! The storage engine entry point (DESIGN §18.1, LC-03).
//!
//! [`StorageEngine`] has exactly one implementation, [`PageStore`] (LC-03): `open` probes and
//! opens the devices (substrate rules LC-34, page-update and cache modes), reads superblocks, AG
//! tables, metas and the intent table, writes the startup superblock (E7) and returns one
//! [`PartitionStore`] per partition for the workers to recover and serve; `shutdown` drains,
//! writes the clean-shutdown snapshot when it fits its region and sets CLEAN (§8.2 SHUTDOWN).

use std::path::PathBuf;

use crate::partition::{BgRates, Durability, PartitionStore};

/// `page-update` setting (§13.1, §20.2).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PageUpdatePolicy {
    Auto,
    TwoImage,
    /// Refuses to start unless the device verifies atomic 4 KiB writes.
    AtomicOverwrite,
}

/// `device-cache` setting (§13.8).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DeviceCachePolicy {
    Auto,
    WriteThrough,
    WriteBack,
}

/// `flush-method` setting (§13.8).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FlushMethod {
    Auto,
    Fdatasync,
    Dsync,
    None,
}

/// `write-granularity` setting (§13.6).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum GranularityPolicy {
    Auto,
    Page,
    Lba,
}

/// Engine configuration derived from the node configuration (§20.2).
#[derive(Clone, Debug)]
pub struct EngineCfg {
    /// Data devices or files, in device-index order.
    pub devices: Vec<PathBuf>,
    pub workers: u16,
    pub cluster: bool,
    pub durability: Durability,
    pub page_update: PageUpdatePolicy,
    pub device_cache: DeviceCachePolicy,
    pub flush_method: FlushMethod,
    pub write_granularity: GranularityPolicy,
    pub page_slack_cap: u16,
    pub page_slack_atomic: u16,
    pub max_storage_fill: f32,
    pub cache_bytes: usize,
    pub recovery_qd: u32,
    pub rates: BgRates,
}

pub trait StorageEngine: Sized {
    type Partition;
    fn open(cfg: &EngineCfg) -> std::io::Result<(Self, Vec<Self::Partition>)>;
    fn shutdown(&mut self, clean: bool) -> std::io::Result<()>;
}

/// The page store engine.
pub struct PageStore {
    cfg: EngineCfg,
}

// the only impl (LC-03)
impl StorageEngine for PageStore {
    type Partition = PartitionStore;

    fn open(cfg: &EngineCfg) -> std::io::Result<(Self, Vec<Self::Partition>)> {
        todo!("DESIGN §18.1: open")
    }

    fn shutdown(&mut self, clean: bool) -> std::io::Result<()> {
        todo!("DESIGN §8.2: shutdown")
    }
}
