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
//!
//! How a path is checked:
//! * a **file**: `fstatfs` type XFS or ext4 (ext2/ext3 mounts of the ext4 driver are refused by
//!   their mount type); for ext4 the mount's options must not say `data=journal` and the inode
//!   must not carry the data-journaling flag; then the file system's backing device is checked
//!   as a block device stack (below);
//! * a **block device stack**, walked through `slaves/` from the device down to every member:
//!   no device of the stack may be zoned; a dm device's table (`DM_TABLE_STATUS`) may hold only
//!   `linear` and `crypt` targets; an md device must be raid0/raid1/raid10 without a journal
//!   member; a loop device's backing file must pass the file rules. A partition is checked
//!   through its disk. Anything that cannot be read is refused (never assumed safe).
//!
//! The system is reached through the [`Host`] trait so that the rules are unit-tested over
//! captured `mountinfo`/sysfs trees; the system calls themselves are in `io::sys`.

use std::path::Path;

/// Result of probing one data device or file.
#[derive(Clone, Copy, Debug)]
pub struct DevProbe {
    pub substrate: Substrate,
    pub lba: u32,
    pub write_cache: bool,
    pub discard: bool,
    pub atomic_unit: Option<(u32, u32)>,
}

impl DevProbe {
    /// True when a 4 KiB page write at a 4 KiB-aligned offset is verified power-fail atomic
    /// (`unit_min <= 4096 <= unit_max`, §13.1).
    pub fn page_atomic(&self) -> bool {
        self.atomic_unit.is_some_and(|(min, max)| min <= 4096 && 4096 <= max)
    }
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

impl Substrate {
    pub fn is_file(self) -> bool {
        matches!(self, Substrate::XfsFile | Substrate::Ext4File)
    }
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
    /// The path is neither a regular file nor a block device.
    NotFileOrBlock,
    /// Direct-I/O alignment other than 512 or 4096 bytes.
    Alignment(u32),
    /// A fact the rules need could not be read (`errno`, 0 when not applicable).
    Probe { what: &'static str, errno: i32 },
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
            SubstrateRefusal::NotFileOrBlock => f.write_str("not a regular file or a block device"),
            SubstrateRefusal::Alignment(a) => write!(f, "direct I/O alignment {a} is not supported (512 or 4096 required)"),
            SubstrateRefusal::Probe { what, errno: 0 } => write!(f, "cannot verify the storage substrate: {what}"),
            SubstrateRefusal::Probe { what, errno } => {
                write!(f, "cannot verify the storage substrate: {what} ({})", std::io::Error::from_raw_os_error(*errno))
            }
        }
    }
}

impl std::error::Error for SubstrateRefusal {}

/// Probes `path` (a block device or a data file) against the substrate rules.
pub fn probe(path: &std::path::Path) -> Result<DevProbe, SubstrateRefusal> {
    probe_with(&LinuxHost, path)
}

/// Probes `path` through `host` (the real system, or a captured tree in tests).
pub fn probe_with(host: &dyn Host, path: &Path) -> Result<DevProbe, SubstrateRefusal> {
    Prober { host }.probe(path, 0)
}

// ---------------------------------------------------------------------------------------------
// What probing needs from the system

/// Facts read from an opened path by system calls (`statx`, `fstatfs`, `FS_IOC_GETFLAGS`,
/// `BLKSSZGET`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct OpenFacts {
    pub kind: PathKind,
    /// `fstatfs` `f_type` (files).
    pub fs_magic: u64,
    /// Files: the file system's device; block devices: the device number.
    pub dev: (u32, u32),
    /// Inode flags (files), when the file system reports them.
    pub inode_flags: Option<u32>,
    /// `STATX_DIOALIGN` offset alignment, when reported.
    pub dio_offset_align: Option<u32>,
    /// `STATX_WRITE_ATOMIC` unit `(min, max)`, when supported.
    pub atomic: Option<(u32, u32)>,
    /// `BLKSSZGET` (block devices).
    pub blk_lba: Option<u32>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PathKind {
    File,
    Block,
}

/// Access to the system for probing. Paths are absolute strings (`/sys/...`, `/proc/...`).
pub trait Host {
    fn open_facts(&self, path: &Path) -> Result<OpenFacts, SubstrateRefusal>;
    /// Contents of a small text file.
    fn read(&self, path: &str) -> Option<String>;
    /// Entry names of a directory.
    fn list(&self, dir: &str) -> Option<Vec<String>>;
    /// Canonical path (symbolic links resolved), if it exists.
    fn resolve(&self, path: &str) -> Option<String>;
    /// Target type names of the table of device-mapper device `major:minor`, or `errno`.
    fn dm_targets(&self, major: u32, minor: u32) -> Result<Vec<String>, i32>;
}

/// The running system.
pub struct LinuxHost;

impl Host for LinuxHost {
    fn open_facts(&self, path: &Path) -> Result<OpenFacts, SubstrateRefusal> {
        use std::os::unix::fs::FileTypeExt;
        let errno = |e: std::io::Error| e.raw_os_error().unwrap_or(libc::EIO);
        let md = std::fs::metadata(path).map_err(|e| SubstrateRefusal::Probe { what: "stat", errno: errno(e) })?;
        let kind = if md.file_type().is_file() {
            PathKind::File
        } else if md.file_type().is_block_device() {
            PathKind::Block
        } else {
            return Err(SubstrateRefusal::NotFileOrBlock);
        };
        let f = std::fs::File::open(path).map_err(|e| SubstrateRefusal::Probe { what: "open", errno: errno(e) })?;
        let st = crate::io::sys::statx(&f).map_err(|e| SubstrateRefusal::Probe { what: "statx", errno: errno(e) })?;
        let mut facts = OpenFacts {
            kind,
            fs_magic: 0,
            dev: st.dev,
            inode_flags: None,
            dio_offset_align: st.dio_offset_align,
            atomic: st.atomic_write,
            blk_lba: None,
        };
        match kind {
            PathKind::File => {
                facts.fs_magic =
                    crate::io::sys::fs_type(&f).map_err(|e| SubstrateRefusal::Probe { what: "statfs", errno: errno(e) })?;
                facts.inode_flags = crate::io::sys::inode_flags(&f).ok();
            }
            PathKind::Block => {
                facts.dev = st.rdev;
                facts.blk_lba = crate::io::sys::logical_block_size(&f).ok();
            }
        }
        Ok(facts)
    }

    fn read(&self, path: &str) -> Option<String> {
        std::fs::read_to_string(path).ok()
    }

    fn list(&self, dir: &str) -> Option<Vec<String>> {
        let rd = std::fs::read_dir(dir).ok()?;
        Some(rd.filter_map(|e| e.ok().and_then(|e| e.file_name().into_string().ok())).collect())
    }

    fn resolve(&self, path: &str) -> Option<String> {
        std::fs::canonicalize(path).ok().and_then(|p| p.into_os_string().into_string().ok())
    }

    fn dm_targets(&self, major: u32, minor: u32) -> Result<Vec<String>, i32> {
        let control = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/mapper/control")
            .map_err(|e| e.raw_os_error().unwrap_or(libc::EIO))?;
        let mut size = 16 << 10;
        loop {
            let mut buf = vec![0u8; size];
            crate::io::sys::dm_table_status(&control, major, minor, &mut buf).map_err(|e| e.raw_os_error().unwrap_or(libc::EIO))?;
            match parse_dm_table(&buf) {
                Ok(t) => return Ok(t),
                Err(DmParse::BufferFull) if size < (4 << 20) => size *= 4,
                Err(_) => return Err(libc::EPROTO),
            }
        }
    }
}

/// Why a `DM_TABLE_STATUS` reply could not be read.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DmParse {
    /// The reply did not fit the buffer; retry with a larger one.
    BufferFull,
    Malformed,
}

const DM_BUFFER_FULL_FLAG: u32 = 1 << 8;
const DM_TARGET_SPEC_BYTES: usize = 40;

/// Target type names of a `DM_TABLE_STATUS` reply (`struct dm_ioctl`, then `target_count`
/// `struct dm_target_spec` entries from `data_start`, each followed by its parameter string,
/// linked by `next` offsets relative to `data_start`).
pub fn parse_dm_table(buf: &[u8]) -> Result<Vec<String>, DmParse> {
    let u32_at = |off: usize| buf.get(off..off + 4).map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]));
    let flags = u32_at(28).ok_or(DmParse::Malformed)?;
    if flags & DM_BUFFER_FULL_FLAG != 0 {
        return Err(DmParse::BufferFull);
    }
    let start = u32_at(16).ok_or(DmParse::Malformed)? as usize;
    let count = u32_at(20).ok_or(DmParse::Malformed)? as usize;
    if count > 1 << 16 {
        return Err(DmParse::Malformed);
    }
    let mut out = Vec::with_capacity(count);
    let mut rel = 0usize;
    for _ in 0..count {
        let at = start.checked_add(rel).ok_or(DmParse::Malformed)?;
        let spec = buf.get(at..at + DM_TARGET_SPEC_BYTES).ok_or(DmParse::Malformed)?;
        let name = &spec[24..40];
        let end = name.iter().position(|&b| b == 0).unwrap_or(name.len());
        out.push(String::from_utf8_lossy(&name[..end]).into_owned());
        let next = u32::from_le_bytes([spec[20], spec[21], spec[22], spec[23]]) as usize;
        if next <= rel {
            return if out.len() == count { Ok(out) } else { Err(DmParse::Malformed) };
        }
        rel = next;
    }
    Ok(out)
}

// ---------------------------------------------------------------------------------------------
// The rules

/// `fstatfs` magics of the allowed and of commonly met file systems (names for the refusal).
const XFS_MAGIC: u64 = 0x5846_5342;
const EXT_MAGIC: u64 = 0xEF53;
/// `FS_JOURNAL_DATA_FL`: per-file data journaling (ext4 `chattr +j`).
const FS_JOURNAL_DATA_FL: u32 = 0x0000_4000;
const MAX_STACK_DEPTH: u32 = 16;

fn fs_name(magic: u64) -> &'static str {
    match magic {
        XFS_MAGIC => "xfs",
        EXT_MAGIC => "ext4",
        0x9123_683E => "btrfs",
        0xF2F5_2010 => "f2fs",
        0x2FC1_2FC1 => "zfs",
        0xCA45_1A4E => "bcachefs",
        0x3434 => "nilfs2",
        0x0102_1994 => "tmpfs",
        0x8584_58F6 => "ramfs",
        0x9584_58F6 => "hugetlbfs",
        0x794C_7630 => "overlayfs",
        0x6969 => "nfs",
        0xFF53_4D42 => "cifs",
        0xFE53_4D42 => "smb2",
        0x6573_5546 => "fuse",
        0x7371_7368 => "squashfs",
        0x5346_544E => "ntfs",
        0x4D44 => "vfat",
        0x2011_BAB0 => "exfat",
        0x5265_4973 => "reiserfs",
        0x3153_464A => "jfs",
        0x2405_1905 => "ubifs",
        0x0116_1970 => "gfs2",
        0x7461_636F => "ocfs2",
        0x00C3_6400 => "ceph",
        0x9FA0 => "proc",
        0x6265_6572 => "sysfs",
        _ => "unknown",
    }
}

/// `fstype` names returned for refusals of ext-family mounts other than ext4.
fn ext_family_name(fstype: &str) -> &'static str {
    match fstype {
        "ext2" => "ext2",
        "ext3" => "ext3",
        _ => "unknown",
    }
}

/// One `/proc/self/mountinfo` entry (the fields the rules need).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MountEntry {
    pub dev: (u32, u32),
    pub fstype: String,
    pub mount_opts: String,
    pub super_opts: String,
}

/// Parses `mountinfo` and returns the first entry of device `dev` (every mount of one file
/// system shares its super options).
pub fn find_mount(mountinfo: &str, dev: (u32, u32)) -> Option<MountEntry> {
    mountinfo.lines().filter_map(parse_mount_line).find(|m| m.dev == dev)
}

fn parse_mount_line(line: &str) -> Option<MountEntry> {
    let f: Vec<&str> = line.split(' ').collect();
    let sep = f.iter().position(|&x| x == "-")?;
    if sep < 6 || f.len() < sep + 3 {
        return None;
    }
    let (maj, min) = f[2].split_once(':')?;
    Some(MountEntry {
        dev: (maj.parse().ok()?, min.parse().ok()?),
        fstype: f[sep + 1].to_string(),
        mount_opts: f[5].to_string(),
        super_opts: f.get(sep + 3).copied().unwrap_or("").to_string(),
    })
}

fn has_opt(opts: &str, want: &str) -> bool {
    opts.split(',').any(|o| o == want)
}

fn norm_lba(v: u32) -> Result<u32, SubstrateRefusal> {
    match v {
        1..=512 => Ok(512),
        513..=4096 => Ok(4096),
        _ => Err(SubstrateRefusal::Alignment(v)),
    }
}

/// Top of a checked block device stack.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Top {
    Plain,
    DmLinear,
    DmCrypt,
    Md,
    Loop,
}

/// Queue attributes of the device that carries a probed path.
struct Queue {
    lba: Option<u32>,
    write_cache: bool,
    discard: bool,
    atomic: Option<(u32, u32)>,
}

struct Prober<'h> {
    host: &'h dyn Host,
}

impl Prober<'_> {
    fn probe(&self, path: &Path, depth: u32) -> Result<DevProbe, SubstrateRefusal> {
        let facts = self.host.open_facts(path)?;
        match facts.kind {
            PathKind::File => self.probe_file(&facts, depth),
            PathKind::Block => self.probe_block(&facts, depth),
        }
    }

    fn probe_file(&self, facts: &OpenFacts, depth: u32) -> Result<DevProbe, SubstrateRefusal> {
        let substrate = match facts.fs_magic {
            XFS_MAGIC => Substrate::XfsFile,
            EXT_MAGIC => Substrate::Ext4File,
            m => return Err(SubstrateRefusal::FileSystem(fs_name(m))),
        };
        if substrate == Substrate::Ext4File {
            let info = self.host.read("/proc/self/mountinfo").ok_or(SubstrateRefusal::Probe { what: "mount table", errno: 0 })?;
            let m = find_mount(&info, facts.dev).ok_or(SubstrateRefusal::Probe { what: "mount of the file system", errno: 0 })?;
            if m.fstype != "ext4" {
                return Err(SubstrateRefusal::FileSystem(ext_family_name(&m.fstype)));
            }
            if has_opt(&m.super_opts, "data=journal") || has_opt(&m.mount_opts, "data=journal") {
                return Err(SubstrateRefusal::Ext4DataMode);
            }
            let flags = facts.inode_flags.ok_or(SubstrateRefusal::Probe { what: "inode flags", errno: 0 })?;
            if flags & FS_JOURNAL_DATA_FL != 0 {
                return Err(SubstrateRefusal::Ext4PerFileDataFlag);
            }
        }
        let dir = self.dev_dir(facts.dev)?;
        self.check_stack(&dir, depth)?;
        let q = self.queue(&dir);
        let lba = match facts.dio_offset_align {
            Some(a) => norm_lba(a)?,
            None => norm_lba(q.lba.unwrap_or(512))?,
        };
        Ok(DevProbe { substrate, lba, write_cache: q.write_cache, discard: false, atomic_unit: facts.atomic })
    }

    fn probe_block(&self, facts: &OpenFacts, depth: u32) -> Result<DevProbe, SubstrateRefusal> {
        let dir = self.dev_dir(facts.dev)?;
        let top = self.check_stack(&dir, depth)?;
        let q = self.queue(&dir);
        let lba = norm_lba(facts.blk_lba.or(q.lba).or(facts.dio_offset_align).unwrap_or(512))?;
        let substrate = match top {
            Top::Plain => Substrate::RawBlock,
            Top::DmLinear => Substrate::DmLinear,
            Top::DmCrypt => Substrate::DmCrypt,
            Top::Md => Substrate::MdRaid,
            Top::Loop => Substrate::Loop,
        };
        Ok(DevProbe { substrate, lba, write_cache: q.write_cache, discard: q.discard, atomic_unit: facts.atomic.or(q.atomic) })
    }

    /// Canonical sysfs directory of block device `major:minor`.
    fn dev_dir(&self, dev: (u32, u32)) -> Result<String, SubstrateRefusal> {
        self.host
            .resolve(&format!("/sys/dev/block/{}:{}", dev.0, dev.1))
            .ok_or(SubstrateRefusal::Probe { what: "block device in /sys/dev/block", errno: 0 })
    }

    /// The disk directory of a device directory (the parent of a partition).
    fn disk_dir(&self, dir: &str) -> String {
        if self.host.read(&format!("{dir}/partition")).is_some()
            && let Some((parent, _)) = dir.rsplit_once('/')
        {
            return parent.to_string();
        }
        dir.to_string()
    }

    fn queue(&self, dir: &str) -> Queue {
        let disk = self.disk_dir(dir);
        let num = |name: &str| self.host.read(&format!("{disk}/queue/{name}")).and_then(|s| s.trim().parse::<u64>().ok());
        let atomic = match (num("atomic_write_unit_min_bytes"), num("atomic_write_unit_max_bytes")) {
            (Some(min), Some(max)) if min > 0 && max >= min => Some((min as u32, max.min(u64::from(u32::MAX)) as u32)),
            _ => None,
        };
        Queue {
            lba: num("logical_block_size").map(|v| v as u32),
            // Unknown cache mode: assume a volatile cache (flushes are then always issued).
            write_cache: self.host.read(&format!("{disk}/queue/write_cache")).is_none_or(|s| s.trim() != "write through"),
            discard: num("discard_max_bytes").is_some_and(|v| v > 0),
            atomic,
        }
    }

    /// Checks the device at `dir` and, through `slaves/`, every device below it.
    fn check_stack(&self, dir: &str, depth: u32) -> Result<Top, SubstrateRefusal> {
        if depth > MAX_STACK_DEPTH {
            return Err(SubstrateRefusal::Probe { what: "block device stack too deep", errno: 0 });
        }
        let disk = self.disk_dir(dir);
        let name = disk.rsplit('/').next().unwrap_or("");
        if let Some(z) = self.host.read(&format!("{disk}/queue/zoned"))
            && z.trim() != "none"
        {
            return Err(SubstrateRefusal::Zoned);
        }
        let mut top = Top::Plain;
        if self.host.list(&format!("{disk}/dm")).is_some() || name.starts_with("dm-") {
            let dev = self
                .host
                .read(&format!("{disk}/dev"))
                .and_then(|s| {
                    let (a, b) = s.trim().split_once(':')?;
                    Some((a.parse::<u32>().ok()?, b.parse::<u32>().ok()?))
                })
                .ok_or(SubstrateRefusal::Probe { what: "device-mapper device number", errno: 0 })?;
            let targets =
                self.host.dm_targets(dev.0, dev.1).map_err(|errno| SubstrateRefusal::Probe { what: "device-mapper table", errno })?;
            if targets.is_empty() {
                return Err(SubstrateRefusal::DmTarget("(no table)".to_string()));
            }
            if let Some(bad) = targets.iter().find(|t| t.as_str() != "linear" && t.as_str() != "crypt") {
                return Err(SubstrateRefusal::DmTarget(bad.clone()));
            }
            top = if targets.iter().any(|t| t == "crypt") { Top::DmCrypt } else { Top::DmLinear };
        } else if let Some(level) = self.host.read(&format!("{disk}/md/level")) {
            let level = level.trim();
            if !matches!(level, "raid0" | "raid1" | "raid10") {
                return Err(SubstrateRefusal::MdLevel(level.to_string()));
            }
            for e in self.host.list(&format!("{disk}/md")).unwrap_or_default() {
                if e.starts_with("dev-")
                    && self.host.read(&format!("{disk}/md/{e}/state")).is_some_and(|s| s.split(',').any(|x| x.trim() == "journal"))
                {
                    return Err(SubstrateRefusal::MdLevel(format!("{level} with a journal device")));
                }
            }
            top = Top::Md;
        } else if name.starts_with("loop") {
            let backing = self
                .host
                .read(&format!("{disk}/loop/backing_file"))
                .ok_or(SubstrateRefusal::Probe { what: "loop device backing file", errno: 0 })?;
            self.probe(Path::new(backing.trim()), depth + 1).map_err(|e| SubstrateRefusal::LoopBacking(Box::new(e)))?;
            top = Top::Loop;
        }
        for s in self.host.list(&format!("{disk}/slaves")).unwrap_or_default() {
            let sdir = self
                .host
                .resolve(&format!("/sys/class/block/{s}"))
                .ok_or(SubstrateRefusal::Probe { what: "member device in /sys/class/block", errno: 0 })?;
            self.check_stack(&sdir, depth + 1)?;
        }
        Ok(top)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hashbrown::HashMap;
    use std::path::PathBuf;

    /// A captured system: text files, symbolic links, per-path open facts and dm tables.
    #[derive(Default)]
    struct Fake {
        files: HashMap<String, String>,
        links: HashMap<String, String>,
        facts: HashMap<PathBuf, OpenFacts>,
        dm: HashMap<(u32, u32), Result<Vec<String>, i32>>,
        mountinfo: Vec<String>,
    }

    impl Host for Fake {
        fn open_facts(&self, path: &Path) -> Result<OpenFacts, SubstrateRefusal> {
            self.facts.get(path).copied().ok_or(SubstrateRefusal::Probe { what: "stat", errno: libc::ENOENT })
        }
        fn read(&self, path: &str) -> Option<String> {
            if path == "/proc/self/mountinfo" {
                return Some(self.mountinfo.join("\n"));
            }
            self.files.get(&self.canon(path)).cloned()
        }
        fn list(&self, dir: &str) -> Option<Vec<String>> {
            let d = format!("{}/", self.canon(dir));
            let mut names: Vec<String> = self
                .files
                .keys()
                .chain(self.links.keys())
                .filter_map(|k| k.strip_prefix(&d).map(|r| r.split('/').next().unwrap_or("").to_string()))
                .collect();
            names.sort();
            names.dedup();
            (!names.is_empty()).then_some(names)
        }
        fn resolve(&self, path: &str) -> Option<String> {
            let c = self.canon(path);
            (self.files.contains_key(&c) || self.list(&c).is_some()).then_some(c)
        }
        fn dm_targets(&self, major: u32, minor: u32) -> Result<Vec<String>, i32> {
            self.dm.get(&(major, minor)).cloned().unwrap_or(Err(libc::ENXIO))
        }
    }

    impl Fake {
        fn canon(&self, p: &str) -> String {
            for (l, t) in &self.links {
                if p == l {
                    return t.clone();
                }
                if let Some(rest) = p.strip_prefix(&format!("{l}/")) {
                    return format!("{t}/{rest}");
                }
            }
            p.to_string()
        }

        fn file(&mut self, p: &str, v: &str) {
            self.files.insert(p.to_string(), v.to_string());
        }

        /// A whole disk `name` with number `maj:min` and default queue attributes.
        fn disk(&mut self, name: &str, maj: u32, min: u32) -> String {
            let d = format!("/sys/block/{name}");
            self.file(&format!("{d}/dev"), &format!("{maj}:{min}\n"));
            self.file(&format!("{d}/queue/zoned"), "none\n");
            self.file(&format!("{d}/queue/write_cache"), "write back\n");
            self.file(&format!("{d}/queue/logical_block_size"), "512\n");
            self.file(&format!("{d}/queue/discard_max_bytes"), "2147483648\n");
            self.file(&format!("{d}/queue/atomic_write_unit_min_bytes"), "0\n");
            self.file(&format!("{d}/queue/atomic_write_unit_max_bytes"), "0\n");
            self.links.insert(format!("/sys/dev/block/{maj}:{min}"), d.clone());
            self.links.insert(format!("/sys/class/block/{name}"), d.clone());
            d
        }

        fn partition(&mut self, disk: &str, part: &str, maj: u32, min: u32) {
            let d = format!("/sys/block/{disk}/{part}");
            self.file(&format!("{d}/partition"), "1\n");
            self.file(&format!("{d}/dev"), &format!("{maj}:{min}\n"));
            self.links.insert(format!("/sys/dev/block/{maj}:{min}"), d.clone());
            self.links.insert(format!("/sys/class/block/{part}"), d);
        }

        fn slave(&mut self, upper: &str, lower: &str) {
            self.file(&format!("/sys/block/{upper}/slaves/{lower}"), "");
        }

        fn dm(&mut self, name: &str, maj: u32, min: u32, targets: &[&str]) {
            let d = self.disk(name, maj, min);
            self.file(&format!("{d}/dm/name"), name);
            self.dm.insert((maj, min), Ok(targets.iter().map(|s| s.to_string()).collect()));
        }

        fn mount(&mut self, maj: u32, min: u32, fstype: &str, super_opts: &str) {
            let n = self.mountinfo.len() + 20;
            self.mountinfo.push(format!("{n} 1 {maj}:{min} / /data{n} rw,relatime shared:1 - {fstype} /dev/x {super_opts}"));
        }

        fn file_on(&mut self, path: &str, magic: u64, dev: (u32, u32), flags: u32) {
            self.facts.insert(
                PathBuf::from(path),
                OpenFacts {
                    kind: PathKind::File,
                    fs_magic: magic,
                    dev,
                    inode_flags: Some(flags),
                    dio_offset_align: Some(512),
                    atomic: None,
                    blk_lba: None,
                },
            );
        }

        fn bdev(&mut self, path: &str, dev: (u32, u32), lba: u32) {
            self.facts.insert(
                PathBuf::from(path),
                OpenFacts { kind: PathKind::Block, fs_magic: 0, dev, inode_flags: None, dio_offset_align: None, atomic: None, blk_lba: Some(lba) },
            );
        }
    }

    fn run(h: &Fake, p: &str) -> Result<DevProbe, SubstrateRefusal> {
        probe_with(h, Path::new(p))
    }

    /// An ext4 root on vda (as on the CI box) and an XFS file system on an NVMe partition.
    fn base() -> Fake {
        let mut h = Fake::default();
        h.disk("vda", 254, 0);
        h.mount(254, 0, "ext4", "rw,discard");
        h.file_on("/data/ext4.img", EXT_MAGIC, (254, 0), 0x80000);
        h.disk("nvme0n1", 259, 0);
        h.file("/sys/block/nvme0n1/queue/logical_block_size", "4096\n");
        h.file("/sys/block/nvme0n1/queue/write_cache", "write through\n");
        h.partition("nvme0n1", "nvme0n1p1", 259, 1);
        h.mount(259, 1, "xfs", "rw,attr2,inode64");
        h.file_on("/xfs/data.img", XFS_MAGIC, (259, 1), 0);
        h
    }

    #[test]
    fn allowed_files() {
        let h = base();
        let p = run(&h, "/data/ext4.img").unwrap();
        assert_eq!((p.substrate, p.lba, p.write_cache, p.discard), (Substrate::Ext4File, 512, true, false));
        assert!(p.substrate.is_file());
        let mut h = base();
        h.facts.get_mut(Path::new("/xfs/data.img")).unwrap().dio_offset_align = None;
        let p = run(&h, "/xfs/data.img").unwrap();
        // lba from the partition's disk; write-through cache of the disk
        assert_eq!((p.substrate, p.lba, p.write_cache), (Substrate::XfsFile, 4096, false));
    }

    #[test]
    fn refused_file_systems() {
        for (magic, name) in [
            (0x9123_683Eu64, "btrfs"),
            (0xF2F5_2010, "f2fs"),
            (0x2FC1_2FC1, "zfs"),
            (0xCA45_1A4E, "bcachefs"),
            (0x3434, "nilfs2"),
            (0x0102_1994, "tmpfs"),
            (0x1234_5678, "unknown"),
        ] {
            let mut h = base();
            h.file_on("/x.img", magic, (0, 40), 0);
            assert_eq!(run(&h, "/x.img").unwrap_err(), SubstrateRefusal::FileSystem(name));
        }
        let mut h = base();
        h.disk("sdb", 8, 16);
        h.mount(8, 16, "ext3", "rw");
        h.file_on("/old/x.img", EXT_MAGIC, (8, 16), 0);
        assert_eq!(run(&h, "/old/x.img").unwrap_err(), SubstrateRefusal::FileSystem("ext3"));
    }

    #[test]
    fn ext4_data_modes() {
        let mut h = base();
        h.disk("sdc", 8, 32);
        h.mount(8, 32, "ext4", "rw,data=journal");
        h.file_on("/j/x.img", EXT_MAGIC, (8, 32), 0);
        assert_eq!(run(&h, "/j/x.img").unwrap_err(), SubstrateRefusal::Ext4DataMode);
        let mut h = base();
        h.file_on("/data/plusj.img", EXT_MAGIC, (254, 0), FS_JOURNAL_DATA_FL | 0x80000);
        assert_eq!(run(&h, "/data/plusj.img").unwrap_err(), SubstrateRefusal::Ext4PerFileDataFlag);
        let mut h = base();
        h.facts.get_mut(Path::new("/data/ext4.img")).unwrap().inode_flags = None;
        assert!(matches!(run(&h, "/data/ext4.img"), Err(SubstrateRefusal::Probe { .. })));
        let mut h = base();
        h.mountinfo.clear();
        assert!(matches!(run(&h, "/data/ext4.img"), Err(SubstrateRefusal::Probe { .. })));
    }

    #[test]
    fn device_mapper_targets() {
        let mut h = base();
        h.disk("sda", 8, 0);
        h.dm("dm-0", 253, 0, &["linear", "linear"]);
        h.slave("dm-0", "sda");
        h.bdev("/dev/dm-0", (253, 0), 512);
        let p = run(&h, "/dev/dm-0").unwrap();
        assert_eq!((p.substrate, p.discard), (Substrate::DmLinear, true));
        h.dm("dm-1", 253, 1, &["crypt"]);
        h.slave("dm-1", "dm-0");
        h.bdev("/dev/dm-1", (253, 1), 4096);
        let p = run(&h, "/dev/dm-1").unwrap();
        assert_eq!((p.substrate, p.lba), (Substrate::DmCrypt, 4096));
        for t in ["log-writes", "zoned", "vdo", "writecache", "cache", "era", "thin", "striped"] {
            let mut h = base();
            h.disk("sda", 8, 0);
            h.dm("dm-5", 253, 5, &["linear", t]);
            h.slave("dm-5", "sda");
            h.bdev("/dev/dm-5", (253, 5), 512);
            assert_eq!(run(&h, "/dev/dm-5").unwrap_err(), SubstrateRefusal::DmTarget(t.to_string()), "{t}");
            // The same target lower in the stack, under an allowed one, is refused too.
            h.dm("dm-6", 253, 6, &["crypt"]);
            h.slave("dm-6", "dm-5");
            h.bdev("/dev/dm-6", (253, 6), 512);
            assert_eq!(run(&h, "/dev/dm-6").unwrap_err(), SubstrateRefusal::DmTarget(t.to_string()));
        }
        // An unreadable table is refused, not assumed safe.
        let mut h = base();
        h.disk("dm-7", 253, 7);
        h.bdev("/dev/dm-7", (253, 7), 512);
        assert_eq!(run(&h, "/dev/dm-7").unwrap_err(), SubstrateRefusal::Probe { what: "device-mapper table", errno: libc::ENXIO });
        // A file on an ext4 file system that sits on a log-writes target.
        let mut h = base();
        h.disk("sdd", 8, 48);
        h.dm("dm-8", 253, 8, &["log-writes"]);
        h.slave("dm-8", "sdd");
        h.mount(253, 8, "ext4", "rw");
        h.file_on("/lw/x.img", EXT_MAGIC, (253, 8), 0);
        assert_eq!(run(&h, "/lw/x.img").unwrap_err(), SubstrateRefusal::DmTarget("log-writes".to_string()));
    }

    #[test]
    fn zoned_devices() {
        let mut h = base();
        h.disk("nvme1n1", 259, 5);
        h.file("/sys/block/nvme1n1/queue/zoned", "host-managed\n");
        h.bdev("/dev/nvme1n1", (259, 5), 4096);
        assert_eq!(run(&h, "/dev/nvme1n1").unwrap_err(), SubstrateRefusal::Zoned);
        // A file system on a zoned device's partition.
        h.partition("nvme1n1", "nvme1n1p1", 259, 6);
        h.mount(259, 6, "xfs", "rw");
        h.file_on("/z/x.img", XFS_MAGIC, (259, 6), 0);
        assert_eq!(run(&h, "/z/x.img").unwrap_err(), SubstrateRefusal::Zoned);
        // A zoned member under a linear target.
        h.dm("dm-3", 253, 3, &["linear"]);
        h.slave("dm-3", "nvme1n1");
        h.bdev("/dev/dm-3", (253, 3), 4096);
        assert_eq!(run(&h, "/dev/dm-3").unwrap_err(), SubstrateRefusal::Zoned);
    }

    #[test]
    fn md_levels() {
        let mk = |level: &str, journal: bool| {
            let mut h = base();
            h.disk("sda", 8, 0);
            h.disk("sdb", 8, 16);
            h.disk("md0", 9, 0);
            h.file("/sys/block/md0/md/level", &format!("{level}\n"));
            h.file("/sys/block/md0/md/dev-sda/state", "in_sync\n");
            h.file("/sys/block/md0/md/dev-sdb/state", if journal { "journal\n" } else { "in_sync,write_mostly\n" });
            h.slave("md0", "sda");
            h.slave("md0", "sdb");
            h.bdev("/dev/md0", (9, 0), 512);
            run(&h, "/dev/md0")
        };
        for l in ["raid0", "raid1", "raid10"] {
            assert_eq!(mk(l, false).unwrap().substrate, Substrate::MdRaid);
        }
        assert_eq!(mk("raid5", false).unwrap_err(), SubstrateRefusal::MdLevel("raid5".to_string()));
        assert_eq!(mk("linear", false).unwrap_err(), SubstrateRefusal::MdLevel("linear".to_string()));
        assert_eq!(mk("raid1", true).unwrap_err(), SubstrateRefusal::MdLevel("raid1 with a journal device".to_string()));
    }

    #[test]
    fn loop_devices() {
        let mut h = base();
        h.disk("loop0", 7, 0);
        h.file("/sys/block/loop0/loop/backing_file", "/data/ext4.img\n");
        h.bdev("/dev/loop0", (7, 0), 512);
        assert_eq!(run(&h, "/dev/loop0").unwrap().substrate, Substrate::Loop);
        h.file_on("/btr/x.img", 0x9123_683E, (0, 50), 0);
        h.file("/sys/block/loop0/loop/backing_file", "/btr/x.img\n");
        assert_eq!(
            run(&h, "/dev/loop0").unwrap_err(),
            SubstrateRefusal::LoopBacking(Box::new(SubstrateRefusal::FileSystem("btrfs")))
        );
        h.files.remove("/sys/block/loop0/loop/backing_file");
        assert!(matches!(run(&h, "/dev/loop0"), Err(SubstrateRefusal::Probe { .. })));
    }

    #[test]
    fn raw_block_attributes() {
        let mut h = base();
        h.bdev("/dev/nvme0n1", (259, 0), 4096);
        h.file("/sys/block/nvme0n1/queue/atomic_write_unit_min_bytes", "4096\n");
        h.file("/sys/block/nvme0n1/queue/atomic_write_unit_max_bytes", "65536\n");
        let p = run(&h, "/dev/nvme0n1").unwrap();
        assert_eq!((p.substrate, p.lba, p.write_cache, p.discard), (Substrate::RawBlock, 4096, false, true));
        assert_eq!(p.atomic_unit, Some((4096, 65536)));
        assert!(p.page_atomic());
        h.bdev("/dev/vda", (254, 0), 512);
        let p = run(&h, "/dev/vda").unwrap();
        assert_eq!(p.atomic_unit, None);
        assert!(!p.page_atomic());
        h.bdev("/dev/odd", (254, 0), 8192);
        assert_eq!(run(&h, "/dev/odd").unwrap_err(), SubstrateRefusal::Alignment(8192));
        assert!(matches!(run(&h, "/dev/missing"), Err(SubstrateRefusal::Probe { .. })));
    }

    #[test]
    fn mountinfo_parsing() {
        let info = "23 28 0:22 / /proc rw,relatime - proc proc rw\n\
                    28 1 254:0 / / rw,relatime shared:1 master:2 - ext4 /dev/vda rw,discard,data=ordered\n\
                    bogus line\n";
        let m = find_mount(info, (254, 0)).unwrap();
        assert_eq!(m.fstype, "ext4");
        assert_eq!(m.mount_opts, "rw,relatime");
        assert!(has_opt(&m.super_opts, "data=ordered"));
        assert!(!has_opt(&m.super_opts, "data=journal"));
        assert!(find_mount(info, (8, 0)).is_none());
    }

    #[test]
    fn dm_table_reply_parsing() {
        // dm_ioctl header (312 bytes) + two target specs with parameter strings.
        let mut b = vec![0u8; 1024];
        b[16..20].copy_from_slice(&312u32.to_le_bytes());
        b[20..24].copy_from_slice(&2u32.to_le_bytes());
        let spec = |b: &mut Vec<u8>, at: usize, next: u32, name: &str, params: &str| {
            b[at + 20..at + 24].copy_from_slice(&next.to_le_bytes());
            b[at + 24..at + 24 + name.len()].copy_from_slice(name.as_bytes());
            b[at + 40..at + 40 + params.len()].copy_from_slice(params.as_bytes());
        };
        spec(&mut b, 312, 64, "linear", "8:0 2048");
        spec(&mut b, 312 + 64, 128, "crypt", "aes-xts-plain64");
        assert_eq!(parse_dm_table(&b), Ok(vec!["linear".to_string(), "crypt".to_string()]));
        let mut full = b.clone();
        full[28..32].copy_from_slice(&DM_BUFFER_FULL_FLAG.to_le_bytes());
        assert_eq!(parse_dm_table(&full), Err(DmParse::BufferFull));
        let mut bad = b.clone();
        bad[20..24].copy_from_slice(&40u32.to_le_bytes());
        assert_eq!(parse_dm_table(&bad), Err(DmParse::Malformed));
        assert_eq!(parse_dm_table(&b[..10]), Err(DmParse::Malformed));
        let mut zero = b;
        zero[20..24].copy_from_slice(&0u32.to_le_bytes());
        assert_eq!(parse_dm_table(&zero), Ok(vec![]));
    }

    #[test]
    fn display_is_one_line() {
        for r in [
            SubstrateRefusal::FileSystem("btrfs"),
            SubstrateRefusal::Ext4DataMode,
            SubstrateRefusal::LoopBacking(Box::new(SubstrateRefusal::Zoned)),
            SubstrateRefusal::Probe { what: "device-mapper table", errno: libc::EPERM },
            SubstrateRefusal::Alignment(1024),
        ] {
            let s = r.to_string();
            assert!(!s.contains('\n') && !s.is_empty());
        }
    }

    #[test]
    fn real_system_probe() {
        // Whatever substrate the test directory lives on, probing it either passes the rules
        // (ext4/XFS on a plain disk, as on the CI box) or refuses with a reason.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("probe.img");
        std::fs::write(&p, vec![0u8; 8192]).unwrap();
        let r = probe(&p);
        eprintln!("probe of the test directory: {r:?}");
        match r {
            Ok(d) => {
                assert!(d.substrate.is_file());
                assert!(d.lba == 512 || d.lba == 4096);
                assert!(!d.discard);
            }
            Err(r) => assert!(!r.to_string().is_empty()),
        }
        assert_eq!(probe(Path::new("/dev/null")).unwrap_err(), SubstrateRefusal::NotFileOrBlock);
        assert!(matches!(probe(Path::new("/nonexistent/kvssd")), Err(SubstrateRefusal::Probe { .. })));
    }
}
