//! `kv-sim`: deterministic simulation and model checking (DESIGN §25.3). Test-only.
//!
//! * [`storage_sim`]: partitions on `SimDevice` with byte/sector/prefix tearing, both page-update
//!   modes, seeded command mixes, crash points at any I/O completion boundary; checks I1-I14
//!   against [`model`] after recovery.
//! * [`cluster_sim`]: 3 coordinators + 6 nodes in-process over [`simnet`] (drop, delay,
//!   duplicate, partition, clock drift, crashes); per-key checks in [`linearize`].
//! * [`wsr_model`]: Stateright model of the coordinators' whole-state replication and of
//!   single-server peer-list changes.
//!
//! Failure artifacts are seeds and final device images only ([`write_artifact`]); nothing here
//! writes a per-write trace (LC-33).
#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(unsafe_code)]
#![allow(dead_code, unused_variables)]

pub mod cluster_sim;
pub mod linearize;
pub mod model;
pub mod simnet;
pub mod storage_sim;
pub mod wsr_model;

use std::path::{Path, PathBuf};

/// The only artifact writer: the failing seed and the final device image.
pub fn write_artifact(dir: &Path, seed: u64, image: &[u8]) -> std::io::Result<PathBuf> {
    todo!("DESIGN §25.3: failure artifact")
}
