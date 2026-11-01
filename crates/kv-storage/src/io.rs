//! Device I/O interface (DESIGN §6.4, §13.6, §13.8, §17.1).
//!
//! Partitions never perform I/O themselves: they push [`IoReq`]s into the worker's queue and
//! receive [`IoDone`]s, so the same partition code runs on the io_uring ring (kv-node), on
//! [`SyncFileDevice`] (tools, tests, recovery tooling) and on `SimDevice` (simulation). Buffers
//! are 4096-byte aligned [`AlignedBuf`]s for `O_DIRECT`. Writes may carry `RWF_DSYNC`
//! (`flush-method dsync`) or `RWF_ATOMIC` (atomic page-update mode, §13.1). Devices are
//! fixed-size: nothing here grows a file (LC-02).
//!
//! This is one of the files of this crate allowed to contain `unsafe` (§24); every block carries
//! a `SAFETY:` comment.
#![allow(unsafe_code)]

use core::ptr::NonNull;

/// 4096-byte aligned, zero-initialized I/O buffer.
pub struct AlignedBuf {
    ptr: NonNull<u8>,
    len: usize,
    cap: usize,
}

// SAFETY: an AlignedBuf exclusively owns its allocation; no other pointer to it exists, so it
// may move between threads (I/O buffers travel between a worker and its completion path).
unsafe impl Send for AlignedBuf {}

impl AlignedBuf {
    pub fn zeroed(len: usize) -> Self {
        todo!("DESIGN §12: aligned buffer allocation")
    }

    pub fn as_slice(&self) -> &[u8] {
        todo!("DESIGN §12: aligned buffer view")
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        todo!("DESIGN §12: aligned buffer view")
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// Identifies a request to its owner.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct IoTag {
    pub partition: u16,
    pub id: u64,
}

pub enum IoOp {
    Read {
        buf: AlignedBuf,
    },
    Write {
        buf: AlignedBuf,
        /// Offset of the written span inside `buf`.
        buf_off: u32,
        dsync: bool,
        /// RWF_ATOMIC
        atomic: bool,
    },
    Flush,
    Discard,
}

pub struct IoReq {
    pub tag: IoTag,
    pub dev: u8,
    pub offset: u64,
    pub len: u32,
    pub op: IoOp,
}

pub struct IoDone {
    pub tag: IoTag,
    /// Bytes transferred, or `-errno`.
    pub res: Result<u32, i32>,
    pub buf: Option<AlignedBuf>,
}

/// Per-device facts established at open (devprobe + configuration).
#[derive(Clone, Copy)]
pub struct DevInfo {
    pub pages: u64,
    pub lba: u32,
    pub write_through: bool,
    pub discard: bool,
    pub page_update: PageUpdateMode,
    /// (min, max) verified
    pub atomic_unit: Option<(u32, u32)>,
}

/// Page-update mode of a device (§13.1).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PageUpdateMode {
    Atomic,
    TwoImage,
}

/// A block device as seen by the storage engine.
pub trait BlockDevice {
    fn info(&self) -> DevInfo;
    fn submit(&mut self, r: IoReq);
    fn poll(&mut self, out: &mut Vec<IoDone>, wait: bool);
}

/// Synchronous `pread`/`pwrite`/`fdatasync` device: tools, tests, recovery tooling.
pub struct SyncFileDevice {
    file: std::fs::File,
    info: DevInfo,
    done: Vec<IoDone>,
}

impl SyncFileDevice {
    /// Opens a probed data file or block device with `O_DIRECT`.
    pub fn open(path: &std::path::Path, info: DevInfo) -> std::io::Result<Self> {
        todo!("DESIGN §18.1: open a device")
    }
}

impl BlockDevice for SyncFileDevice {
    fn info(&self) -> DevInfo {
        self.info
    }

    fn submit(&mut self, r: IoReq) {
        todo!("DESIGN §24.7: synchronous I/O")
    }

    fn poll(&mut self, out: &mut Vec<IoDone>, wait: bool) {
        out.append(&mut self.done);
    }
}
