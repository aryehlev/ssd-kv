//! Coordinator configuration-state files (DESIGN §23.3, LC-02, LC-34).
//!
//! Pre-allocated by `kvc init`, never extended; written with `O_DIRECT` `pwrite` + `fdatasync`:
//! * `coord.meta` = 2 x 4 KiB AbBlocks `{term, voted_for, peer_gen, peers (<= 7)}`;
//! * `coord.state` = 2 x 8 MiB halves `{magic, half_seq, committed {version, term, len, bytes},
//!   accepted: Option<{version, term, len, bytes}>, crc32c}`; load picks the valid half with the
//!   higher `half_seq`; writes alternate halves.
//!
//! The files stay constant in size forever (test `ab_files_constant_size`).
//!
//! Open point: the state files are subject to the substrate rules of LC-34, whose probing lives
//! in kv-storage's `devprobe.rs`, which this crate must not link (LC-38).

use std::path::Path;

use kv_ctrl::CoordId;

use super::peers::PeerList;

/// Bytes of one `coord.state` half.
pub const STATE_HALF_BYTES: usize = 8 << 20;
/// Largest encoded state accepted (`StateTooLarge` above).
pub const STATE_MAX_BYTES: usize = STATE_HALF_BYTES - (64 << 10);

/// One versioned state value.
#[derive(Clone)]
pub struct Versioned {
    pub version: u64,
    pub term: u64,
    /// Encoded `ClusterState`.
    pub bytes: Vec<u8>,
}

/// Everything loaded at startup.
pub struct Persisted {
    pub term: u64,
    pub voted_for: Option<CoordId>,
    pub peers: PeerList,
    pub committed: Versioned,
    pub accepted: Option<Versioned>,
}

pub struct StateFiles {
    meta: std::fs::File,
    state: std::fs::File,
    meta_seq: u64,
    half_seq: u64,
}

impl StateFiles {
    pub fn create(dir: &Path, half_bytes: usize, peers: &PeerList) -> std::io::Result<()> {
        todo!("DESIGN §23.3: create state files")
    }

    pub fn open(dir: &Path) -> std::io::Result<(Self, Persisted)> {
        todo!("DESIGN §23.3: load state files")
    }

    pub fn write_meta(&mut self, term: u64, voted_for: Option<CoordId>, peers: &PeerList) -> std::io::Result<()> {
        todo!("DESIGN §23.3: write coord.meta")
    }

    pub fn write_state(&mut self, committed: &Versioned, accepted: Option<&Versioned>) -> std::io::Result<()> {
        todo!("DESIGN §23.3: write coord.state")
    }
}
