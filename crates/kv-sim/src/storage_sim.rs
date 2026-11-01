//! Storage simulation (DESIGN §17, §25.3).
//!
//! N partitions on `SimDevice` (durable + volatile layers, FLUSH semantics, lost unflushed
//! writes, byte/sector/prefix tearing; atomic-unit mode), both page-update modes, a
//! single-threaded driver with a seeded RNG choosing command mixes (SET variants, DEL, EXPIRE*,
//! PERSIST, RENAME, MSET, MSETNX, APPEND/SETRANGE on all size classes incl. chunkmaps, FLUSHDB,
//! FLUSHALL), crash points, crash type (kill -9 / power cut), durability mode and lba 512/4096.
//! Invariants I1-I14 are checked after every recovery.

use kv_storage::sim::{SimDevice, TearMode};

/// One simulation run's parameters.
#[derive(Clone, Copy, Debug)]
pub struct StorageSimCfg {
    pub seed: u64,
    pub partitions: u16,
    pub lba: u32,
    pub atomic_unit: Option<u32>,
    pub tear: TearMode,
    pub ops: u64,
}

/// The simulated node: its devices and the reference model.
pub struct StorageSim {
    devices: Vec<SimDevice>,
    model: crate::model::RefModel,
}

/// Runs one seed; `Err` carries the violated invariant.
pub fn run(cfg: &StorageSimCfg) -> Result<(), String> {
    todo!("DESIGN §25.3: storage simulation")
}
