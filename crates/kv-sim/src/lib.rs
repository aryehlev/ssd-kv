//! `kv-sim`: deterministic simulation and model checking (DESIGN §25.3). Test-only.
//!
//! * [`storage_sim`]: partitions on `SimDevice` with byte/sector/prefix tearing, both page-update
//!   modes, write-through and write-back caches, durability modes, seeded pipelined command
//!   mixes, late and out-of-order completions, crash points at any I/O completion boundary;
//!   checks I1-I4, I10 against [`model`] after recovery and the liveness invariant I15 (every
//!   accepted request reaches a terminal state within a bounded number of ticks).
//!   `cargo run -p kv-sim --release -- storage --seeds N` runs a campaign.
//! * [`cluster_sim`]: 3 coordinators + 6 nodes in-process over [`simnet`] (drop, delay,
//!   duplicate, partition, clock drift, crashes); per-key checks in [`linearize`].
//! * [`wsr_model`]: Stateright model of the coordinators' whole-state replication and of
//!   single-server peer-list changes.
//!
//! Failure artifacts are seeds (as replayable case files: the seed, the run parameters and the
//! minimized list of step descriptors, from which every value is regenerated) and final device
//! images only ([`write_artifact`]); nothing here writes a per-write trace (LC-33).
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

/// Header of a storage simulation case file (see [`storage_sim::case_text`]).
const CASE_MAGIC: &[u8] = b"kv-sim storage case ";

/// The only artifact writer: the failing seed and the final device image. `image` is either a
/// device image (written as `seed-<seed>.img`) or the seed's replayable case file (it starts
/// with the case header; written as `seed-<seed>.case`). Returns the path written.
pub fn write_artifact(dir: &Path, seed: u64, image: &[u8]) -> std::io::Result<PathBuf> {
    std::fs::create_dir_all(dir)?;
    let ext = if image.starts_with(CASE_MAGIC) { "case" } else { "img" };
    let path = dir.join(format!("seed-{seed}.{ext}"));
    std::fs::write(&path, image)?;
    Ok(path)
}
