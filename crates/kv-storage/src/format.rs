//! `kvd format` (DESIGN §10.1, §10.10, §20.1, LC-34).
//!
//! Probes every device (refusing substrates outside LC-34), writes the superblocks, AG tables,
//! device-0 metadata blocks and reserves the two fixed snapshot regions per device (default 3% of
//! the device's capacity each, rounded up to 16 MiB; 0 disables them). Data files are
//! pre-allocated and always zero-filled, whatever their size, so no unwritten-extent conversion
//! ever happens while serving; they are never extended afterwards (LC-02). Raw devices may be
//! discarded first. This is the only module that pre-allocates file space, and every
//! pre-allocation is followed by the zero-fill pass (LC-34).

use std::path::PathBuf;

/// Size of each of the two snapshot regions per device.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum SnapshotRegionSize {
    Bytes(u64),
    Percent(f32),
}

impl Default for SnapshotRegionSize {
    fn default() -> Self {
        SnapshotRegionSize::Percent(3.0)
    }
}

/// files are always zero-filled
#[derive(Clone, Debug)]
pub struct FormatOpts {
    /// `(path, size)`; the size is required for files that do not exist yet.
    pub devices: Vec<(PathBuf, Option<u64>)>,
    /// P: 16, 32, 64 or 128.
    pub partitions: u16,
    pub cluster: bool,
    /// `Bytes(u64) | Percent(f32)`, default 3%
    pub snapshot_region: SnapshotRegionSize,
}

pub fn format(opts: &FormatOpts) -> std::io::Result<()> {
    todo!("DESIGN §10.1: format")
}
