//! Coordinator configuration-state files (DESIGN §23.3, LC-02, LC-34).
//!
//! Pre-allocated by `kvc init`, never extended; written with `O_DIRECT` `pwrite` + `fdatasync`:
//! * `coord.meta` = 2 x 4 KiB AbBlocks (magic `KVCMETA1`) `{term, voted_for, peer_gen, peers
//!   (<= 7)}`;
//! * `coord.state` = 2 halves `{magic, half_seq, committed {version, term, len, bytes},
//!   accepted: Option<{version, term, len, bytes}>, crc32c}`; load picks the valid half with
//!   the higher `half_seq`; a write always goes to the half that does not hold the newest valid
//!   value, so a torn write leaves the previous value intact.
//!
//! A half holds both the committed and the accepted value, each up to
//! [`kv_topo::state::MAX_STATE_BYTES`], so a half is [`STATE_HALF_BYTES`] = 16 MiB (two 8 MiB
//! values; the spec's 8 MiB figure is the per-value bound).
//!
//! The files stay constant in size forever (test `ab_files_constant_size`): only the used prefix
//! of a half is rewritten, rounded up to 4 KiB.
//!
//! Substrate (LC-34): the directory must be on XFS or ext4, and ext4 must not be mounted
//! `data=journal` (read from `/proc/self/mountinfo`). The per-file data-journaling attribute and
//! the backing block device's stacking are not inspected here: that probing lives in
//! kv-storage's `devprobe.rs`, which this crate must not link (LC-38; open point 11).

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use kv_common::abblock::{AB_BLOCK, AB_PAYLOAD_MAX, AbCopy, ab_decode, ab_encode, ab_next_write, ab_pick, magic};
use kv_common::crc::crc32c;
use kv_ctrl::CoordId;
use serde::{Deserialize, Serialize};

use super::peers::PeerList;

/// Bytes of one `coord.state` half: room for a committed and an accepted value of
/// [`STATE_MAX_BYTES`] each plus the header.
pub const STATE_HALF_BYTES: usize = 16 << 20;
/// Largest encoded state accepted (`StateTooLarge` above).
pub const STATE_MAX_BYTES: usize = (8 << 20) - (64 << 10);
/// Smallest half `create` accepts (tests use small halves).
pub const MIN_HALF_BYTES: usize = 64 << 10;

pub const META_FILE: &str = "coord.meta";
pub const STATE_FILE: &str = "coord.state";

const META_MAGIC: u64 = magic(b"KVCMETA1");
const STATE_MAGIC: u64 = magic(b"KVCSTAT1");
const STATE_FORMAT: u32 = 1;
/// Alignment of every `O_DIRECT` transfer.
const ALIGN: usize = 4096;
/// Bytes of the state-half header; the committed value follows, then the accepted value.
const HEADER: usize = 0x80;

const _: () = assert!(STATE_HALF_BYTES % ALIGN == 0 && MIN_HALF_BYTES % ALIGN == 0);
const _: () = assert!(2 * STATE_MAX_BYTES + HEADER <= STATE_HALF_BYTES);
const _: () = assert!(STATE_MAX_BYTES == kv_topo::state::MAX_STATE_BYTES);

/// One versioned state value.
#[derive(Clone, PartialEq, Eq)]
pub struct Versioned {
    pub version: u64,
    pub term: u64,
    /// Encoded `ClusterState`.
    pub bytes: Vec<u8>,
}

impl core::fmt::Debug for Versioned {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Versioned")
            .field("version", &self.version)
            .field("term", &self.term)
            .field("len", &self.bytes.len())
            .finish()
    }
}

/// Everything loaded at startup.
pub struct Persisted {
    pub term: u64,
    pub voted_for: Option<CoordId>,
    pub peers: PeerList,
    pub committed: Versioned,
    pub accepted: Option<Versioned>,
}

/// `coord.meta` payload.
#[derive(Serialize, Deserialize)]
struct MetaPayload {
    term: u64,
    voted_for: Option<CoordId>,
    peers: PeerList,
}

pub struct StateFiles {
    dir: PathBuf,
    meta: File,
    state: File,
    half_bytes: usize,
    /// Newest valid meta copy and its sequence number.
    meta_newest: Option<(AbCopy, u64)>,
    /// Newest valid state half and its `half_seq`.
    state_newest: Option<(AbCopy, u64)>,
    buf: AlignedBuf,
}

/// A zeroed buffer whose usable window starts on a 4 KiB boundary (for `O_DIRECT`).
struct AlignedBuf {
    raw: Vec<u8>,
    off: usize,
}

impl AlignedBuf {
    fn new() -> Self {
        AlignedBuf {
            raw: Vec::new(),
            off: 0,
        }
    }

    /// A zeroed, aligned window of exactly `len` bytes (`len` a multiple of 4 KiB).
    fn window(&mut self, len: usize) -> &mut [u8] {
        if self.raw.len() < self.off + len {
            self.raw = vec![0u8; len + ALIGN];
            self.off = self.raw.as_ptr().align_offset(ALIGN);
        }
        let w = &mut self.raw[self.off..self.off + len];
        w.fill(0);
        w
    }
}

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

fn round_up(n: usize) -> usize {
    n.div_ceil(ALIGN) * ALIGN
}

fn open_direct(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

impl StateFiles {
    /// Creates `coord.meta` (term 0, no vote, `peers`) and `coord.state` (`2 x half_bytes`
    /// zero bytes: no valid half yet) in `dir`, which must not hold them already. The files are
    /// written in full here and never change size afterwards.
    pub fn create(dir: &Path, half_bytes: usize, peers: &PeerList) -> std::io::Result<()> {
        if half_bytes < MIN_HALF_BYTES || half_bytes % ALIGN != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "state half size must be a multiple of 4 KiB and at least 64 KiB",
            ));
        }
        std::fs::create_dir_all(dir)?;
        check_substrate(dir)?;
        let meta_path = dir.join(META_FILE);
        let state_path = dir.join(STATE_FILE);
        for p in [&meta_path, &state_path] {
            if p.exists() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("{} already exists", p.display()),
                ));
            }
        }
        // Zero-fill both files completely (no sparse ranges, no later growth: LC-02).
        for (path, len) in [(&meta_path, 2 * AB_BLOCK), (&state_path, 2 * half_bytes)] {
            let mut f = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)?;
            let zeros = vec![0u8; 1 << 20];
            let mut left = len;
            while left > 0 {
                let n = left.min(zeros.len());
                f.write_all(&zeros[..n])?;
                left -= n;
            }
            f.sync_all()?;
        }
        sync_dir(dir)?;
        let (mut files, _) = Self::open(dir)?;
        files.write_meta(0, None, peers)?;
        Ok(())
    }

    /// Opens the files of `dir` and loads the newest valid meta copy and state half. A state
    /// file with no valid half (right after [`StateFiles::create`]) loads as version 0.
    pub fn open(dir: &Path) -> std::io::Result<(Self, Persisted)> {
        check_substrate(dir)?;
        let meta_path = dir.join(META_FILE);
        let state_path = dir.join(STATE_FILE);
        let meta_len = std::fs::metadata(&meta_path)?.len();
        let state_len = std::fs::metadata(&state_path)?.len();
        if meta_len != (2 * AB_BLOCK) as u64 {
            return Err(invalid(format!("{} has the wrong size", meta_path.display())));
        }
        if state_len == 0 || state_len % (2 * ALIGN as u64) != 0 {
            return Err(invalid(format!("{} has the wrong size", state_path.display())));
        }
        let half_bytes = usize::try_from(state_len / 2)
            .map_err(|_| invalid("state file too large"))?;
        let mut files = StateFiles {
            dir: dir.to_path_buf(),
            meta: open_direct(&meta_path)?,
            state: open_direct(&state_path)?,
            half_bytes,
            meta_newest: None,
            state_newest: None,
            buf: AlignedBuf::new(),
        };
        let p = files.load()?;
        Ok((files, p))
    }

    /// Re-reads both files (also updates which copies are the newest valid ones).
    pub fn load(&mut self) -> std::io::Result<Persisted> {
        let mut meta_raw = vec![0u8; 2 * AB_BLOCK];
        File::open(self.dir.join(META_FILE))?.read_exact(&mut meta_raw)?;
        let a = ab_decode(META_MAGIC, &meta_raw[..AB_BLOCK]);
        let b = ab_decode(META_MAGIC, &meta_raw[AB_BLOCK..]);
        let Some((copy, seq, payload)) = ab_pick(a, b) else {
            return Err(invalid("coord.meta has no valid copy"));
        };
        let meta: MetaPayload = postcard::take_from_bytes(payload)
            .map(|(m, _)| m)
            .map_err(|_| invalid("coord.meta payload does not decode"))?;
        self.meta_newest = Some((copy, seq));

        let mut sf = File::open(self.dir.join(STATE_FILE))?;
        let mut best: Option<(AbCopy, u64, Vec<u8>)> = None;
        for copy in [AbCopy::A, AbCopy::B] {
            if let Some((seq, img)) = read_half(&mut sf, self.half_bytes, copy)? {
                if best.as_ref().is_none_or(|(_, s, _)| seq > *s) {
                    best = Some((copy, seq, img));
                }
            }
        }
        let (committed, accepted) = match &best {
            Some((copy, seq, img)) => {
                self.state_newest = Some((*copy, *seq));
                parse_half(img)?
            }
            None => {
                self.state_newest = None;
                (
                    Versioned {
                        version: 0,
                        term: 0,
                        bytes: Vec::new(),
                    },
                    None,
                )
            }
        };
        Ok(Persisted {
            term: meta.term,
            voted_for: meta.voted_for,
            peers: meta.peers,
            committed,
            accepted,
        })
    }

    pub fn write_meta(&mut self, term: u64, voted_for: Option<CoordId>, peers: &PeerList) -> std::io::Result<()> {
        let payload = postcard::to_stdvec(&MetaPayload {
            term,
            voted_for,
            peers: peers.clone(),
        })
        .map_err(|_| invalid("coord.meta payload does not encode"))?;
        if payload.len() > AB_PAYLOAD_MAX {
            return Err(invalid("coord.meta payload exceeds one block"));
        }
        let (copy, seq) = ab_next_write(self.meta_newest);
        let w = self.buf.window(AB_BLOCK);
        let block: &mut [u8; AB_BLOCK] = w.try_into().map_err(|_| invalid("block buffer"))?;
        ab_encode(META_MAGIC, seq, &payload, block);
        self.meta
            .write_all_at(block, (copy.index() * AB_BLOCK) as u64)?;
        self.meta.sync_data()?;
        self.meta_newest = Some((copy, seq));
        Ok(())
    }

    pub fn write_state(&mut self, committed: &Versioned, accepted: Option<&Versioned>) -> std::io::Result<()> {
        self.write_state_raw(
            (committed.version, committed.term, &committed.bytes),
            accepted.map(|a| (a.version, a.term, a.bytes.as_slice())),
        )
    }

    /// [`StateFiles::write_state`] from borrowed `(version, term, bytes)` values.
    pub fn write_state_raw(
        &mut self,
        committed: (u64, u64, &[u8]),
        accepted: Option<(u64, u64, &[u8])>,
    ) -> std::io::Result<()> {
        let acc_len = accepted.map_or(0, |a| a.2.len());
        let total = HEADER + committed.2.len() + acc_len;
        if committed.2.len() > STATE_MAX_BYTES || acc_len > STATE_MAX_BYTES || total > self.half_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cluster state too large for a state half",
            ));
        }
        let (copy, seq) = ab_next_write(self.state_newest);
        let len = round_up(total);
        let w = self.buf.window(len);
        let put = |w: &mut [u8], off: usize, v: u64| w[off..off + 8].copy_from_slice(&v.to_le_bytes());
        put(w, 0x00, STATE_MAGIC);
        w[0x0C..0x10].copy_from_slice(&STATE_FORMAT.to_le_bytes());
        put(w, 0x10, seq);
        put(w, 0x18, total as u64);
        put(w, 0x20, committed.0);
        put(w, 0x28, committed.1);
        put(w, 0x30, committed.2.len() as u64);
        if let Some((v, t, b)) = accepted {
            w[0x38] = 1;
            put(w, 0x40, v);
            put(w, 0x48, t);
            put(w, 0x50, b.len() as u64);
        }
        let c_end = HEADER + committed.2.len();
        w[HEADER..c_end].copy_from_slice(committed.2);
        if let Some((_, _, b)) = accepted {
            w[c_end..c_end + b.len()].copy_from_slice(b);
        }
        let crc = crc32c(&w[0x0C..total]);
        w[0x08..0x0C].copy_from_slice(&crc.to_le_bytes());
        self.state
            .write_all_at(w, (copy.index() * self.half_bytes) as u64)?;
        self.state.sync_data()?;
        self.state_newest = Some((copy, seq));
        Ok(())
    }

    /// Bytes of one state half.
    pub fn half_bytes(&self) -> usize {
        self.half_bytes
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

/// Reads and validates one half: `(half_seq, image of total_len bytes)`, or `None` when the
/// half holds no valid value (never written, or torn).
fn read_half(f: &mut File, half_bytes: usize, copy: AbCopy) -> io::Result<Option<(u64, Vec<u8>)>> {
    let base = (copy.index() * half_bytes) as u64;
    let mut head = [0u8; HEADER];
    f.seek(SeekFrom::Start(base))?;
    f.read_exact(&mut head)?;
    let u64_at = |b: &[u8], o: usize| u64::from_le_bytes(b[o..o + 8].try_into().unwrap_or([0; 8]));
    if u64_at(&head, 0) != STATE_MAGIC
        || u32::from_le_bytes([head[0x0C], head[0x0D], head[0x0E], head[0x0F]]) != STATE_FORMAT
    {
        return Ok(None);
    }
    let total = u64_at(&head, 0x18);
    if total < HEADER as u64 || total > half_bytes as u64 {
        return Ok(None);
    }
    let total = total as usize;
    let mut img = vec![0u8; total];
    f.seek(SeekFrom::Start(base))?;
    f.read_exact(&mut img)?;
    let crc = u32::from_le_bytes([img[8], img[9], img[10], img[11]]);
    if crc != crc32c(&img[0x0C..]) {
        return Ok(None);
    }
    Ok(Some((u64_at(&img, 0x10), img)))
}

fn parse_half(img: &[u8]) -> io::Result<(Versioned, Option<Versioned>)> {
    let u64_at = |o: usize| u64::from_le_bytes(img[o..o + 8].try_into().unwrap_or([0; 8]));
    let total = img.len();
    let c_len = usize::try_from(u64_at(0x30)).map_err(|_| invalid("state length"))?;
    let c_end = HEADER
        .checked_add(c_len)
        .filter(|&e| e <= total)
        .ok_or_else(|| invalid("state half: committed value out of bounds"))?;
    let committed = Versioned {
        version: u64_at(0x20),
        term: u64_at(0x28),
        bytes: img[HEADER..c_end].to_vec(),
    };
    let accepted = if img[0x38] == 1 {
        let a_len = usize::try_from(u64_at(0x50)).map_err(|_| invalid("state length"))?;
        let a_end = c_end
            .checked_add(a_len)
            .filter(|&e| e == total)
            .ok_or_else(|| invalid("state half: accepted value out of bounds"))?;
        Some(Versioned {
            version: u64_at(0x40),
            term: u64_at(0x48),
            bytes: img[c_end..a_end].to_vec(),
        })
    } else {
        if c_end != total {
            return Err(invalid("state half: length mismatch"));
        }
        None
    };
    Ok((committed, accepted))
}

// ---------------------------------------------------------------------------------------------
// LC-34 substrate check (file-system type and ext4 data mode).

/// One `/proc/self/mountinfo` line: mount point, file-system type, super options.
struct Mount {
    point: PathBuf,
    fstype: String,
    super_opts: String,
}

/// Undoes mountinfo's octal escapes (`\040` = space, ...).
fn unescape(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\\' && i + 3 < b.len() && b[i + 1..i + 4].iter().all(|c| (b'0'..=b'7').contains(c)) {
            out.push((b[i + 1] - b'0') * 64 + (b[i + 2] - b'0') * 8 + (b[i + 3] - b'0'));
            i += 4;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn parse_mountinfo(text: &str) -> Vec<Mount> {
    let mut out = Vec::new();
    for line in text.lines() {
        let fields: Vec<&str> = line.split(' ').collect();
        let Some(sep) = fields.iter().position(|f| *f == "-") else {
            continue;
        };
        if fields.len() < 5 || fields.len() < sep + 4 {
            continue;
        }
        out.push(Mount {
            point: PathBuf::from(unescape(fields[4])),
            fstype: fields[sep + 1].to_owned(),
            super_opts: fields[sep + 3].to_owned(),
        });
    }
    out
}

/// The verdict for `path` (already canonical) against a mount table: `Err(reason)` when the
/// file system holding it is not an allowed substrate (LC-34).
fn substrate_verdict(path: &Path, mounts: &[Mount]) -> Result<(), String> {
    // The last mount with the longest matching mount point is the one that covers the path.
    let mut best: Option<&Mount> = None;
    for m in mounts {
        if path.starts_with(&m.point)
            && best.is_none_or(|b| m.point.as_os_str().len() >= b.point.as_os_str().len())
        {
            best = Some(m);
        }
    }
    let Some(m) = best else {
        return Err("no mount covers the state directory".into());
    };
    match m.fstype.as_str() {
        "xfs" => Ok(()),
        "ext4" => {
            if m.super_opts.split(',').any(|o| o == "data=journal") {
                Err("ext4 mounted with data=journal is not an allowed substrate".into())
            } else {
                Ok(())
            }
        }
        other => Err(format!(
            "file system type '{other}' is not an allowed substrate (XFS or ext4 only)"
        )),
    }
}

/// Refuses a state directory outside the allowed substrates (LC-34): the file system must be
/// XFS or ext4 in its default ordered data mode.
pub fn check_substrate(dir: &Path) -> io::Result<()> {
    let path = std::fs::canonicalize(dir)?;
    let text = std::fs::read_to_string("/proc/self/mountinfo")?;
    substrate_verdict(&path, &parse_mountinfo(&text)).map_err(|reason| {
        io::Error::new(
            io::ErrorKind::Unsupported,
            format!("{}: {reason}", path.display()),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const MOUNTINFO: &str = "\
23 28 0:22 / /proc rw,relatime - proc proc rw
28 1 254:0 / / rw,relatime - ext4 /dev/vda rw,discard
40 28 0:40 / /tmp rw,relatime - tmpfs tmpfs rw,size=1g
41 28 254:16 / /data rw,relatime - xfs /dev/vdb rw,attr2
42 28 254:32 / /mnt/j\\040k rw,relatime - ext4 /dev/vdc rw,data=journal
43 28 0:41 / /var/lib/docker/x rw - overlay overlay rw,lowerdir=a
44 28 254:48 / /srv rw - btrfs /dev/vdd rw
";

    #[test]
    fn substrate_rules() {
        let m = parse_mountinfo(MOUNTINFO);
        assert_eq!(m.len(), 7);
        assert_eq!(m[4].point, PathBuf::from("/mnt/j k"));
        let v = |p: &str| substrate_verdict(Path::new(p), &m);
        assert!(v("/home/user/coord").is_ok());
        assert!(v("/data/coord").is_ok());
        assert!(v("/tmp/coord").is_err());
        assert!(v("/mnt/j k/coord").is_err());
        assert!(v("/var/lib/docker/x/c").is_err());
        assert!(v("/srv/c").is_err());
        // a path that merely shares a string prefix with a mount point is not under it
        assert!(v("/database/c").is_ok());
    }

    #[test]
    fn aligned_windows() {
        let mut b = AlignedBuf::new();
        for len in [4096, 8192, 4096, 1 << 20] {
            let w = b.window(len);
            assert_eq!(w.len(), len);
            assert_eq!(w.as_ptr() as usize % ALIGN, 0);
            assert!(w.iter().all(|&x| x == 0));
            w[0] = 1;
        }
    }
}
