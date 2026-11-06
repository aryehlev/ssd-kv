//! Device I/O interface (DESIGN §6.4, §13.6, §13.8, §17.1).
//!
//! Partitions never perform I/O themselves: they push [`IoReq`]s into the worker's queue and
//! receive [`IoDone`]s, so the same partition code runs on the io_uring ring (kv-node), on
//! [`SyncFileDevice`] (tools, tests, recovery tooling) and on `SimDevice` (simulation). Buffers
//! are 4096-byte aligned [`AlignedBuf`]s for `O_DIRECT`. Writes may carry `RWF_DSYNC`
//! (`flush-method dsync`) or `RWF_ATOMIC` (atomic page-update mode, §13.1). Devices are
//! fixed-size: nothing here grows a file (LC-02).
//!
//! Request rules shared by every device ([`IoReq::check`]): `offset` and `len` are multiples
//! of the device's `lba` and inside the device; a read fills `buf[..len]`; a write writes
//! `buf[buf_off..buf_off + len]` (`buf_off` a multiple of `lba`); a flush covers every write
//! completed before it was submitted; a discard covers `[offset, offset + len)`. Completions
//! carry the byte count or `-errno` and hand the buffer back.
//!
//! This is one of the files of this crate allowed to contain `unsafe` (§24); every block carries
//! a `SAFETY:` comment. Besides the buffers it holds [`LockedMem`] (non-swappable, non-dumpable
//! memory for replication backlogs, LC-23) and the few system calls `devprobe.rs` and
//! `format.rs` need ([`sys`]), each behind a safe, narrowly typed wrapper.
#![allow(unsafe_code)]

use core::ptr::NonNull;
use std::alloc::Layout;
use std::fs::File;
use std::os::unix::fs::{FileExt, FileTypeExt, OpenOptionsExt};

use crate::layout::PAGE;

/// Memory and offset alignment of every I/O buffer.
pub const IO_ALIGN: usize = 4096;

/// 4096-byte aligned, zero-initialized I/O buffer.
///
/// Every byte of the capacity is initialized (allocated zeroed), so
/// [`AlignedBuf::set_visible_len`] may expose any prefix of it.
pub struct AlignedBuf {
    ptr: NonNull<u8>,
    len: usize,
    cap: usize,
}

// SAFETY: an AlignedBuf exclusively owns its allocation; no other pointer to it exists, so it
// may move between threads (I/O buffers travel between a worker and its completion path).
unsafe impl Send for AlignedBuf {}
// SAFETY: shared references only allow reads of initialized bytes; mutation needs `&mut`.
unsafe impl Sync for AlignedBuf {}

impl AlignedBuf {
    fn layout(cap: usize) -> Layout {
        Layout::from_size_align(cap, IO_ALIGN).expect("I/O buffer layout")
    }

    /// A zeroed buffer of `len` bytes (capacity rounded up to 4096, at least 4096).
    pub fn zeroed(len: usize) -> Self {
        let cap = len.max(1).next_multiple_of(IO_ALIGN);
        let layout = Self::layout(cap);
        // SAFETY: `layout` has a non-zero size (cap >= 4096) and a power-of-two alignment.
        let raw = unsafe { std::alloc::alloc_zeroed(layout) };
        let ptr = NonNull::new(raw).unwrap_or_else(|| std::alloc::handle_alloc_error(layout));
        AlignedBuf { ptr, len, cap }
    }

    /// A buffer holding a copy of `data`.
    pub fn from_slice(data: &[u8]) -> Self {
        let mut b = Self::zeroed(data.len());
        b.as_mut_slice().copy_from_slice(data);
        b
    }

    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: `ptr` points to `cap >= len` initialized bytes owned by `self`; the shared
        // borrow of `self` keeps them alive and unmodified for the returned lifetime.
        unsafe { core::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: as in `as_slice`; the exclusive borrow of `self` makes the slice unique.
        unsafe { core::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Allocated bytes (a multiple of 4096).
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// Changes the visible length to `len <= capacity()`; bytes keep their previous contents.
    pub fn set_visible_len(&mut self, len: usize) {
        assert!(len <= self.cap, "AlignedBuf length beyond capacity");
        self.len = len;
    }

    /// Zeroes the visible bytes.
    pub fn clear(&mut self) {
        self.as_mut_slice().fill(0);
    }

    /// Start of the buffer, for submission to the kernel.
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Page `i` of the buffer. Panics if the buffer is shorter than `(i + 1) * 4096` bytes.
    pub fn page(&self, i: usize) -> &[u8; PAGE] {
        self.as_slice()[i * PAGE..(i + 1) * PAGE].try_into().expect("page-sized slice")
    }

    pub fn page_mut(&mut self, i: usize) -> &mut [u8; PAGE] {
        (&mut self.as_mut_slice()[i * PAGE..(i + 1) * PAGE]).try_into().expect("page-sized slice")
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        // SAFETY: `ptr` was allocated by `alloc_zeroed` with exactly this layout and is freed
        // once, here.
        unsafe { std::alloc::dealloc(self.ptr.as_ptr(), Self::layout(self.cap)) }
    }
}

impl Clone for AlignedBuf {
    fn clone(&self) -> Self {
        Self::from_slice(self.as_slice())
    }
}

impl core::ops::Deref for AlignedBuf {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl core::ops::DerefMut for AlignedBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
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

const EINVAL: i32 = libc::EINVAL;

impl IoReq {
    /// Reads `buf.len()` bytes at `offset` into `buf`.
    pub fn read(tag: IoTag, dev: u8, offset: u64, buf: AlignedBuf) -> Self {
        let len = buf.len() as u32;
        IoReq { tag, dev, offset, len, op: IoOp::Read { buf } }
    }

    /// Writes the whole of `buf` at `offset` (plain write-back write).
    pub fn write(tag: IoTag, dev: u8, offset: u64, buf: AlignedBuf) -> Self {
        let len = buf.len() as u32;
        IoReq { tag, dev, offset, len, op: IoOp::Write { buf, buf_off: 0, dsync: false, atomic: false } }
    }

    /// Writes `buf[buf_off..buf_off + len]` at `offset`, optionally `RWF_DSYNC` / `RWF_ATOMIC`.
    #[allow(clippy::too_many_arguments)]
    pub fn write_span(tag: IoTag, dev: u8, offset: u64, buf: AlignedBuf, buf_off: u32, len: u32, dsync: bool, atomic: bool) -> Self {
        IoReq { tag, dev, offset, len, op: IoOp::Write { buf, buf_off, dsync, atomic } }
    }

    /// Makes every write completed before its submission durable.
    pub fn flush(tag: IoTag, dev: u8) -> Self {
        IoReq { tag, dev, offset: 0, len: 0, op: IoOp::Flush }
    }

    pub fn discard(tag: IoTag, dev: u8, offset: u64, len: u32) -> Self {
        IoReq { tag, dev, offset, len, op: IoOp::Discard }
    }

    pub fn is_write(&self) -> bool {
        matches!(self.op, IoOp::Write { .. })
    }

    /// Checks the request rules for a device of `dev_bytes` bytes with sector size `lba`;
    /// `Err(-EINVAL)` when broken.
    pub fn check(&self, lba: u32, dev_bytes: u64) -> Result<(), i32> {
        let lba = u64::from(lba.max(1));
        let len = u64::from(self.len);
        let span_ok = |len: u64| {
            len > 0 && self.offset.is_multiple_of(lba) && len.is_multiple_of(lba) && self.offset.checked_add(len).is_some_and(|e| e <= dev_bytes)
        };
        let ok = match &self.op {
            IoOp::Read { buf } => span_ok(len) && len <= buf.len() as u64,
            IoOp::Write { buf, buf_off, .. } => {
                span_ok(len) && u64::from(*buf_off).is_multiple_of(lba) && u64::from(*buf_off) + len <= buf.len() as u64
            }
            IoOp::Flush => true,
            IoOp::Discard => span_ok(len),
        };
        if ok { Ok(()) } else { Err(-EINVAL) }
    }

    /// Takes the buffer out of a read or write request.
    pub fn into_buf(self) -> Option<AlignedBuf> {
        match self.op {
            IoOp::Read { buf } | IoOp::Write { buf, .. } => Some(buf),
            IoOp::Flush | IoOp::Discard => None,
        }
    }
}

pub struct IoDone {
    pub tag: IoTag,
    /// Bytes transferred, or `-errno`.
    pub res: Result<u32, i32>,
    pub buf: Option<AlignedBuf>,
}

/// Per-device facts established at open (devprobe + configuration).
#[derive(Clone, Copy, Debug)]
pub struct DevInfo {
    pub pages: u64,
    pub lba: u32,
    pub write_through: bool,
    pub discard: bool,
    pub page_update: PageUpdateMode,
    /// (min, max) verified
    pub atomic_unit: Option<(u32, u32)>,
}

impl DevInfo {
    /// A write-back, two-image device without discard or atomic writes.
    pub fn new(pages: u64, lba: u32) -> Self {
        DevInfo { pages, lba, write_through: false, discard: false, page_update: PageUpdateMode::TwoImage, atomic_unit: None }
    }

    pub fn bytes(&self) -> u64 {
        self.pages * PAGE as u64
    }

    /// True when a write of `len` bytes at a `len`-aligned offset is within the verified atomic
    /// write unit (§13.1, §14.4).
    pub fn atomic_covers(&self, len: u32) -> bool {
        self.atomic_unit.is_some_and(|(min, max)| len >= min && len <= max && len.is_power_of_two())
    }
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
    /// The open file behind a synchronous device, for callers that issue several plain
    /// positioned writes at once from threads of their own (the meta thread's batches).
    fn sync_file(&self) -> Option<&File> {
        None
    }
}

/// How [`SyncFileDevice::open_with`] opens a device.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SyncOpen {
    /// `O_DIRECT` (always for data devices; off only for tools on substrates without it).
    pub direct: bool,
    /// Exclusive access: `flock(LOCK_EX | LOCK_NB)` for files, `O_EXCL` for block devices.
    pub exclusive: bool,
    pub read_only: bool,
}

impl Default for SyncOpen {
    fn default() -> Self {
        SyncOpen { direct: true, exclusive: true, read_only: false }
    }
}

/// Synchronous `pread`/`pwrite`/`fdatasync` device: tools, tests, recovery tooling.
pub struct SyncFileDevice {
    file: File,
    info: DevInfo,
    done: Vec<IoDone>,
    block: bool,
}

fn neg_errno(e: &std::io::Error) -> i32 {
    -e.raw_os_error().unwrap_or(libc::EIO)
}

impl SyncFileDevice {
    /// Opens a probed data file or block device with `O_DIRECT`.
    pub fn open(path: &std::path::Path, info: DevInfo) -> std::io::Result<Self> {
        Self::open_with(path, info, SyncOpen::default())
    }

    /// Opens with explicit options. `info.pages == 0` takes the size from the device; a size
    /// larger than the device is refused (devices never grow, LC-02).
    pub fn open_with(path: &std::path::Path, mut info: DevInfo, opts: SyncOpen) -> std::io::Result<Self> {
        let block = std::fs::metadata(path)?.file_type().is_block_device();
        let mut flags = libc::O_CLOEXEC;
        if opts.direct {
            flags |= libc::O_DIRECT;
        }
        if block && opts.exclusive {
            flags |= libc::O_EXCL;
        }
        let file = std::fs::OpenOptions::new().read(true).write(!opts.read_only).custom_flags(flags).open(path)?;
        if !block && opts.exclusive {
            file.try_lock().map_err(|e| match e {
                std::fs::TryLockError::WouldBlock => {
                    std::io::Error::new(std::io::ErrorKind::WouldBlock, "data file is locked by another process")
                }
                std::fs::TryLockError::Error(e) => e,
            })?;
        }
        let size = device_size(&file)?;
        if info.pages == 0 {
            info.pages = size / PAGE as u64;
        } else if info.pages * PAGE as u64 > size {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "device smaller than its recorded size"));
        }
        Ok(SyncFileDevice { file, info, done: Vec::new(), block })
    }

    /// The open file (for probing the same descriptor).
    pub fn file(&self) -> &File {
        &self.file
    }

    pub fn is_block_device(&self) -> bool {
        self.block
    }

    /// Writes `data` at `offset` (both multiples of `lba`; `data` aligned for `O_DIRECT`, e.g.
    /// from an [`AlignedBuf`]). For tools that do not go through requests.
    pub fn write_at(&mut self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        self.check_span(offset, data.len())?;
        self.file.write_all_at(data, offset)
    }

    /// Reads `out.len()` bytes at `offset` (same rules as [`SyncFileDevice::write_at`]).
    pub fn read_at(&mut self, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
        self.check_span(offset, out.len())?;
        self.file.read_exact_at(out, offset)
    }

    /// `fdatasync`: every completed write is durable when it returns.
    pub fn sync(&mut self) -> std::io::Result<()> {
        self.file.sync_data()
    }

    fn check_span(&self, offset: u64, len: usize) -> std::io::Result<()> {
        let lba = u64::from(self.info.lba.max(1));
        let ok = offset.is_multiple_of(lba)
            && (len as u64).is_multiple_of(lba)
            && offset.checked_add(len as u64).is_some_and(|e| e <= self.info.bytes());
        if ok { Ok(()) } else { Err(std::io::Error::from_raw_os_error(EINVAL)) }
    }

    /// Performs one request synchronously.
    pub fn execute(&mut self, r: &mut IoReq) -> Result<u32, i32> {
        r.check(self.info.lba, self.info.bytes())?;
        let len = r.len as usize;
        match &mut r.op {
            IoOp::Read { buf } => {
                self.file.read_exact_at(&mut buf.as_mut_slice()[..len], r.offset).map_err(|e| neg_errno(&e))?;
                Ok(r.len)
            }
            IoOp::Write { buf, buf_off, dsync, atomic } => {
                let data = &buf.as_slice()[*buf_off as usize..*buf_off as usize + len];
                if !*dsync && !*atomic {
                    self.file.write_all_at(data, r.offset).map_err(|e| neg_errno(&e))?;
                } else {
                    let mut fl = 0;
                    if *dsync {
                        fl |= libc::RWF_DSYNC;
                    }
                    if *atomic {
                        if !self.info.atomic_covers(r.len) || !r.offset.is_multiple_of(u64::from(r.len)) {
                            return Err(-EINVAL);
                        }
                        fl |= libc::RWF_ATOMIC;
                    }
                    sys::pwrite_flags(&self.file, data, r.offset, fl, *atomic).map_err(|e| neg_errno(&e))?;
                }
                Ok(r.len)
            }
            IoOp::Flush => {
                self.file.sync_data().map_err(|e| neg_errno(&e))?;
                Ok(0)
            }
            IoOp::Discard => {
                if !self.block || !self.info.discard {
                    // Files are never hole-punched (that would recreate unwritten extents).
                    return Err(-libc::EOPNOTSUPP);
                }
                sys::discard(&self.file, r.offset, u64::from(r.len)).map_err(|e| neg_errno(&e))?;
                Ok(r.len)
            }
        }
    }
}

/// Size in bytes of an open file or block device.
pub fn device_size(f: &File) -> std::io::Result<u64> {
    use std::io::Seek;
    if f.metadata()?.file_type().is_block_device() {
        let mut g = f;
        g.seek(std::io::SeekFrom::End(0))
    } else {
        Ok(f.metadata()?.len())
    }
}

impl BlockDevice for SyncFileDevice {
    fn info(&self) -> DevInfo {
        self.info
    }

    fn submit(&mut self, mut r: IoReq) {
        let res = self.execute(&mut r);
        let tag = r.tag;
        self.done.push(IoDone { tag, res, buf: r.into_buf() });
    }

    fn poll(&mut self, out: &mut Vec<IoDone>, _wait: bool) {
        out.append(&mut self.done);
    }

    fn sync_file(&self) -> Option<&File> {
        Some(&self.file)
    }
}

// ---------------------------------------------------------------------------------------------
// Locked, non-dumpable memory (LC-23, §23.7)

/// Private anonymous memory that is never swapped (`MAP_HUGETLB`, else `mlock`) and never
/// appears in core dumps (`MADV_DONTDUMP`). Used for replication backlogs (LC-23); it is never
/// backed by a file.
pub struct LockedMem {
    ptr: NonNull<u8>,
    len: usize,
    map_len: usize,
    huge: bool,
}

// SAFETY: LockedMem exclusively owns its mapping; no other pointer to it exists.
unsafe impl Send for LockedMem {}
// SAFETY: shared references only allow reads; mutation needs `&mut`.
unsafe impl Sync for LockedMem {}

const HUGE_PAGE: usize = 2 << 20;

impl LockedMem {
    /// Maps `len` zeroed bytes: 2 MiB huge pages when available, else 4 KiB pages locked with
    /// `mlock` (fails if the lock limit does not allow it).
    pub fn new(len: usize) -> std::io::Result<Self> {
        let len = len.max(1);
        if let Some(m) = Self::map(len.next_multiple_of(HUGE_PAGE), libc::MAP_HUGETLB) {
            return m.finish(len, true);
        }
        let map_len = len.next_multiple_of(IO_ALIGN);
        let m = Self::map(map_len, 0).ok_or_else(std::io::Error::last_os_error)?;
        // SAFETY: the range is exactly the mapping created above, owned by `m`.
        if unsafe { libc::mlock(m.ptr.as_ptr().cast(), map_len) } != 0 {
            return Err(std::io::Error::last_os_error()); // `m` unmaps on drop
        }
        m.finish(len, false)
    }

    fn map(map_len: usize, extra: libc::c_int) -> Option<Self> {
        // SAFETY: an anonymous private mapping with a null hint aliases no existing memory; the
        // result is checked against MAP_FAILED.
        let p = unsafe {
            libc::mmap(
                core::ptr::null_mut(),
                map_len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | extra,
                -1,
                0,
            )
        };
        if p == libc::MAP_FAILED {
            return None;
        }
        NonNull::new(p.cast::<u8>()).map(|ptr| LockedMem { ptr, len: 0, map_len, huge: extra != 0 })
    }

    fn finish(mut self, len: usize, huge: bool) -> std::io::Result<Self> {
        // SAFETY: the range is exactly this object's mapping.
        if unsafe { libc::madvise(self.ptr.as_ptr().cast(), self.map_len, libc::MADV_DONTDUMP) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        self.len = len;
        self.huge = huge;
        Ok(self)
    }

    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: the mapping holds `map_len >= len` zero-initialized bytes owned by `self`.
        unsafe { core::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: as in `as_slice`; the exclusive borrow makes the slice unique.
        unsafe { core::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// True when backed by huge pages (else `mlock`ed small pages).
    pub fn is_huge(&self) -> bool {
        self.huge
    }
}

impl Drop for LockedMem {
    fn drop(&mut self) {
        // SAFETY: unmaps exactly the mapping this object created, once.
        unsafe {
            libc::munmap(self.ptr.as_ptr().cast(), self.map_len);
        }
    }
}

// ---------------------------------------------------------------------------------------------
// System calls used by devprobe.rs and format.rs (safe wrappers; interpretation stays there).

pub mod sys {
    use std::fs::File;
    use std::io;
    use std::os::fd::AsRawFd;

    /// `statx` facts of an open file or block device.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct StatxInfo {
        pub mask: u32,
        pub mode: u32,
        pub attributes: u64,
        pub size: u64,
        /// File system device (files).
        pub dev: (u32, u32),
        /// Device number (block devices).
        pub rdev: (u32, u32),
        /// Direct-I/O alignment, when reported (`STATX_DIOALIGN`).
        pub dio_mem_align: Option<u32>,
        pub dio_offset_align: Option<u32>,
        /// Atomic write unit `(min, max)`, when the file supports atomic writes.
        pub atomic_write: Option<(u32, u32)>,
    }

    pub fn statx(f: &File) -> io::Result<StatxInfo> {
        // SAFETY: `libc::statx` is plain old data; all-zero is a valid value.
        let mut st: libc::statx = unsafe { core::mem::zeroed() };
        let mask = libc::STATX_BASIC_STATS | libc::STATX_DIOALIGN | libc::STATX_WRITE_ATOMIC;
        // SAFETY: valid fd, NUL-terminated empty path with AT_EMPTY_PATH, `st` is writable.
        let r = unsafe { libc::statx(f.as_raw_fd(), c"".as_ptr(), libc::AT_EMPTY_PATH, mask, &mut st) };
        if r != 0 {
            return Err(io::Error::last_os_error());
        }
        let dio = st.stx_mask & libc::STATX_DIOALIGN != 0 && st.stx_dio_offset_align != 0;
        let atomic = st.stx_mask & libc::STATX_WRITE_ATOMIC != 0
            && st.stx_attributes & libc::STATX_ATTR_WRITE_ATOMIC as u64 != 0
            && st.stx_atomic_write_unit_min != 0
            && st.stx_atomic_write_unit_max >= st.stx_atomic_write_unit_min;
        Ok(StatxInfo {
            mask: st.stx_mask,
            mode: u32::from(st.stx_mode),
            attributes: st.stx_attributes,
            size: st.stx_size,
            dev: (st.stx_dev_major, st.stx_dev_minor),
            rdev: (st.stx_rdev_major, st.stx_rdev_minor),
            dio_mem_align: dio.then_some(st.stx_dio_mem_align),
            dio_offset_align: dio.then_some(st.stx_dio_offset_align),
            atomic_write: atomic.then_some((st.stx_atomic_write_unit_min, st.stx_atomic_write_unit_max)),
        })
    }

    /// `fstatfs(2)` `f_type` of the file system holding `f`.
    pub fn fs_type(f: &File) -> io::Result<u64> {
        // SAFETY: `libc::statfs` is plain old data; all-zero is a valid value.
        let mut st: libc::statfs = unsafe { core::mem::zeroed() };
        // SAFETY: valid fd and a writable statfs buffer.
        if unsafe { libc::fstatfs(f.as_raw_fd(), &mut st) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(st.f_type as u64)
    }

    /// Inode flags (`FS_IOC_GETFLAGS`; the kernel writes an `int`).
    pub fn inode_flags(f: &File) -> io::Result<u32> {
        let mut v: libc::c_int = 0;
        // SAFETY: FS_IOC_GETFLAGS writes one int through the pointer, which is valid.
        if unsafe { libc::ioctl(f.as_raw_fd(), libc::FS_IOC_GETFLAGS, &mut v) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(v as u32)
    }

    /// Logical block size of a block device (`BLKSSZGET`).
    pub fn logical_block_size(f: &File) -> io::Result<u32> {
        let mut v: libc::c_int = 0;
        // SAFETY: BLKSSZGET writes one int through the pointer, which is valid.
        if unsafe { libc::ioctl(f.as_raw_fd(), libc::BLKSSZGET, &mut v) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(v as u32)
    }

    /// `_IO(0x12, 119)`
    const BLKDISCARD: libc::Ioctl = 0x1277;

    /// Discards `[off, off + len)` of a block device (`BLKDISCARD`).
    pub fn discard(f: &File, off: u64, len: u64) -> io::Result<()> {
        let range = [off, len];
        // SAFETY: BLKDISCARD reads two u64 through the pointer, which is valid.
        if unsafe { libc::ioctl(f.as_raw_fd(), BLKDISCARD, range.as_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Bytes of the device-mapper ioctl header (`struct dm_ioctl`).
    pub const DM_IOCTL_BYTES: usize = 312;
    /// `_IOWR(0xfd, 12, struct dm_ioctl)`
    const DM_TABLE_STATUS: libc::Ioctl = 0xC138_FD0C;
    const DM_STATUS_TABLE_FLAG: u32 = 1 << 4;

    /// Issues `DM_TABLE_STATUS` (table form) for device `major:minor` on the device-mapper
    /// control file into `buf` (at least [`DM_IOCTL_BYTES`]); the reply's bytes are
    /// interpreted by `devprobe.rs`.
    pub fn dm_table_status(control: &File, major: u32, minor: u32, buf: &mut [u8]) -> io::Result<()> {
        if buf.len() < DM_IOCTL_BYTES + 64 || buf.len() > u32::MAX as usize {
            return Err(io::Error::from_raw_os_error(libc::EINVAL));
        }
        buf.fill(0);
        let put32 = |b: &mut [u8], off: usize, v: u32| b[off..off + 4].copy_from_slice(&v.to_le_bytes());
        put32(buf, 0, 4); // version 4.0.0
        put32(buf, 12, buf.len() as u32); // data_size
        put32(buf, 16, DM_IOCTL_BYTES as u32); // data_start
        put32(buf, 28, DM_STATUS_TABLE_FLAG); // flags
        buf[40..48].copy_from_slice(&libc::makedev(major, minor).to_le_bytes()); // dev
        // SAFETY: `buf` is a writable buffer of `data_size` bytes starting with a dm_ioctl
        // header; the kernel writes at most `data_size` bytes into it.
        if unsafe { libc::ioctl(control.as_raw_fd(), DM_TABLE_STATUS, buf.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// `pwritev2` with `RWF_*` flags; a short write of an atomic request is an error, other
    /// short writes are continued.
    pub(crate) fn pwrite_flags(f: &File, mut data: &[u8], mut off: u64, flags: libc::c_int, atomic: bool) -> io::Result<()> {
        while !data.is_empty() {
            let iov = libc::iovec { iov_base: data.as_ptr() as *mut libc::c_void, iov_len: data.len() };
            // SAFETY: valid fd; one iovec describing `data`, which stays borrowed for the call.
            let n = unsafe { libc::pwritev2(f.as_raw_fd(), &iov, 1, off as libc::off_t, flags) };
            if n < 0 {
                let e = io::Error::last_os_error();
                if e.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(e);
            }
            let n = n as usize;
            if n == 0 || (atomic && n != data.len()) {
                return Err(io::Error::from_raw_os_error(libc::EIO));
            }
            data = &data[n..];
            off += n as u64;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tag(id: u64) -> IoTag {
        IoTag { partition: 1, id }
    }

    #[test]
    fn aligned_buf_basics() {
        for len in [0usize, 1, 4095, 4096, 4097, 1 << 20] {
            let mut b = AlignedBuf::zeroed(len);
            assert_eq!(b.len(), len);
            assert_eq!(b.as_ptr() as usize % IO_ALIGN, 0);
            assert!(b.capacity() >= len.max(1) && b.capacity().is_multiple_of(IO_ALIGN));
            assert!(b.iter().all(|&x| x == 0));
            b.as_mut_slice().fill(7);
            let c = b.clone();
            assert_eq!(c.as_slice(), b.as_slice());
        }
        let mut b = AlignedBuf::from_slice(&[1, 2, 3]);
        b.set_visible_len(4096);
        assert_eq!(&b[..4], &[1, 2, 3, 0]);
        b.page_mut(0)[5] = 9;
        assert_eq!(b.page(0)[5], 9);
        b.clear();
        assert!(b.iter().all(|&x| x == 0));
    }

    #[test]
    fn request_checks() {
        let r = IoReq::read(tag(1), 0, 4096, AlignedBuf::zeroed(4096));
        assert_eq!(r.check(512, 1 << 20), Ok(()));
        assert_eq!(r.check(512, 8191), Err(-EINVAL));
        let r = IoReq::read(tag(1), 0, 100, AlignedBuf::zeroed(512));
        assert_eq!(r.check(512, 1 << 20), Err(-EINVAL));
        let r = IoReq::write_span(tag(1), 0, 512, AlignedBuf::zeroed(4096), 512, 1024, false, false);
        assert_eq!(r.check(512, 1 << 20), Ok(()));
        assert_eq!(r.check(4096, 1 << 20), Err(-EINVAL));
        let r = IoReq::write_span(tag(1), 0, 512, AlignedBuf::zeroed(4096), 3584, 1024, false, false);
        assert_eq!(r.check(512, 1 << 20), Err(-EINVAL));
        let r = IoReq::read(tag(1), 0, 0, AlignedBuf::zeroed(0));
        assert_eq!(r.check(512, 1 << 20), Err(-EINVAL));
        assert_eq!(IoReq::flush(tag(2), 0).check(512, 0), Ok(()));
        assert_eq!(IoReq::discard(tag(3), 0, 0, 4096).check(4096, 4096), Ok(()));
        assert!(IoReq::write(tag(1), 0, 0, AlignedBuf::zeroed(4096)).is_write());
    }

    #[test]
    fn dev_info_atomic_rule() {
        let mut i = DevInfo::new(10, 512);
        assert!(!i.atomic_covers(4096));
        i.atomic_unit = Some((512, 16384));
        assert!(i.atomic_covers(4096) && i.atomic_covers(512) && i.atomic_covers(16384));
        assert!(!i.atomic_covers(256) && !i.atomic_covers(32768) && !i.atomic_covers(3072));
        assert_eq!(i.bytes(), 40960);
    }

    fn temp_file(bytes: u64) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("dev.img");
        std::fs::write(&p, vec![0u8; bytes as usize]).unwrap();
        (dir, p)
    }

    #[test]
    fn sync_file_device_io() {
        let (_d, p) = temp_file(1 << 20);
        let opts = SyncOpen { direct: true, exclusive: true, read_only: false };
        let mut dev = match SyncFileDevice::open_with(&p, DevInfo::new(0, 512), opts) {
            Ok(d) => d,
            // Some test substrates have no O_DIRECT; the buffered path is the same code.
            Err(_) => SyncFileDevice::open_with(&p, DevInfo::new(0, 512), SyncOpen { direct: false, ..opts }).unwrap(),
        };
        assert_eq!(dev.info().pages, 256);
        // A second exclusive open of the same file is refused.
        assert!(SyncFileDevice::open_with(&p, DevInfo::new(0, 512), SyncOpen { direct: false, ..opts }).is_err());

        let mut w = AlignedBuf::zeroed(8192);
        w.as_mut_slice()[4096..].fill(0xAB);
        dev.submit(IoReq::write_span(tag(1), 0, 8192, w, 4096, 4096, false, false));
        let mut w2 = AlignedBuf::zeroed(512);
        w2.fill(0xCD);
        dev.submit(IoReq::write_span(tag(2), 0, 8192 + 512, w2, 0, 512, true, false));
        dev.submit(IoReq::flush(tag(3), 0));
        dev.submit(IoReq::read(tag(4), 0, 8192, AlignedBuf::zeroed(4096)));
        dev.submit(IoReq::read(tag(5), 0, 3, AlignedBuf::zeroed(512)));
        dev.submit(IoReq::discard(tag(6), 0, 0, 4096));
        let mut out = Vec::new();
        dev.poll(&mut out, true);
        let res: Vec<_> = out.iter().map(|d| (d.tag.id, d.res)).collect();
        assert_eq!(res, vec![(1, Ok(4096)), (2, Ok(512)), (3, Ok(0)), (4, Ok(4096)), (5, Err(-EINVAL)), (6, Err(-libc::EOPNOTSUPP))]);
        let r = out[3].buf.as_ref().unwrap();
        assert!(r[..512].iter().all(|&x| x == 0xAB));
        assert!(r[512..1024].iter().all(|&x| x == 0xCD));
        assert!(r[1024..].iter().all(|&x| x == 0xAB));
        // Atomic writes need a verified unit.
        dev.submit(IoReq::write_span(tag(7), 0, 0, AlignedBuf::zeroed(4096), 0, 4096, false, true));
        dev.poll(&mut out, false);
        assert_eq!(out.last().unwrap().res, Err(-EINVAL));
        // Devices never grow: a recorded size beyond the file is refused.
        drop(dev);
        assert!(SyncFileDevice::open_with(&p, DevInfo::new(257, 512), SyncOpen { direct: false, ..opts }).is_err());
    }

    #[test]
    fn sys_wrappers_on_a_file() {
        let (_d, p) = temp_file(4096);
        let f = File::open(&p).unwrap();
        let st = sys::statx(&f).unwrap();
        assert_eq!(st.size, 4096);
        assert_eq!(st.mode & libc::S_IFMT, libc::S_IFREG);
        assert!(sys::fs_type(&f).unwrap() != 0);
        let _ = sys::inode_flags(&f); // not every file system supports it
        assert!(sys::logical_block_size(&f).is_err()); // not a block device
        assert_eq!(device_size(&f).unwrap(), 4096);
    }

    #[test]
    fn locked_mem() {
        // RLIMIT_MEMLOCK may forbid locking in a test environment; then only the error path runs.
        match LockedMem::new(10_000) {
            Ok(mut m) => {
                assert_eq!(m.len(), 10_000);
                assert!(m.as_slice().iter().all(|&x| x == 0));
                m.as_mut_slice()[9_999] = 1;
                assert_eq!(m.as_slice()[9_999], 1);
                let _ = m.is_huge();
            }
            Err(e) => assert!(e.raw_os_error().is_some()),
        }
    }
}
