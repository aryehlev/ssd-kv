//! Storage substrate probing (DESIGN §10.1, §13.1, §13.8, LC-18, LC-34).
//!
//! The only module that reads `/sys/block`, `/proc/self/mountinfo`, `statfs`/`statx` (including
//! the direct-I/O alignment and atomic-write limits) and device-mapper tables. Used at format,
//! inspect and open only. Allowed substrates: raw block devices, dm `linear` and `crypt`, md
//! raid0/raid1/raid10 without extra cache or journal members, loop devices whose backing file
//! passes these rules, and files on XFS or on ext4 in its default ordered data mode. Refused,
//! with exit code 1: other file systems (btrfs, f2fs, zfs, bcachefs, nilfs2, tmpfs, ...), ext4
//! mounted `data=journal` or files carrying the per-file data-journaling flag, zoned devices
//! (`queue/zoned != none`), other device-mapper targets (`log-writes`, `zoned`, `vdo`,
//! `writecache`, `cache`, `era`, ...), other md levels.
//!
//! [`DevProbe`] deliberately has no queue-count, `nr_requests` or queue-depth field: nothing in
//! the product derives concurrency from device attributes (LC-18, LC-37).

/// Result of probing one data device or file.
#[derive(Clone, Copy, Debug)]
pub struct DevProbe {
    pub substrate: Substrate,
    pub lba: u32,
    pub write_cache: bool,
    pub discard: bool,
    pub atomic_unit: Option<(u32, u32)>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Substrate {
    RawBlock,
    DmLinear,
    DmCrypt,
    MdRaid,
    Loop,
    XfsFile,
    Ext4File,
}

/// Why a substrate is refused (rendered as the one-line exit reason).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SubstrateRefusal {
    FileSystem(&'static str),
    Ext4DataMode,
    Ext4PerFileDataFlag,
    Zoned,
    DmTarget(String),
    MdLevel(String),
    LoopBacking(Box<SubstrateRefusal>),
}

impl core::fmt::Display for SubstrateRefusal {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            SubstrateRefusal::FileSystem(fs) => write!(f, "file system {fs} is not supported (XFS or ext4 required)"),
            SubstrateRefusal::Ext4DataMode => f.write_str("ext4 mounted with data=journal is not supported"),
            SubstrateRefusal::Ext4PerFileDataFlag => f.write_str("file has the ext4 per-file data journaling flag"),
            SubstrateRefusal::Zoned => f.write_str("zoned block devices are not supported"),
            SubstrateRefusal::DmTarget(t) => write!(f, "device-mapper target {t} is not supported"),
            SubstrateRefusal::MdLevel(l) => write!(f, "md level {l} is not supported"),
            SubstrateRefusal::LoopBacking(inner) => write!(f, "loop device backing file refused: {inner}"),
        }
    }
}

/// Probes `path` (a block device or a data file) against the substrate rules.
pub fn probe(path: &std::path::Path) -> Result<DevProbe, SubstrateRefusal> {
    todo!("DESIGN LC-34: substrate probe")
}
