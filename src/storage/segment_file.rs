//! Single segment file on disk: 4 KB file header + consecutive 4 KB data pages.
//!
//! Layout:
//! ```text
//! [0..4096]            file header (magic, version, file_id, segment_count)
//! [4096 .. 4096+S]     segment 0 (4 KB seg header + data pages)
//! [4096+S .. 4096+2S]  segment 1
//! ...
//! ```
//! where S = segment_size_bytes (must be a multiple of PAGE_SIZE).

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

use crate::storage::ipage::{IPage, PAGE_SIZE};

// ─── Constants ───────────────────────────────────────────────────────────────

pub const FILE_HEADER_SIZE: usize = PAGE_SIZE;
pub const FILE_HEADER_MAGIC: u32 = 0x5346494C; // "SFIL"
pub const FILE_HEADER_VERSION: u32 = 1;

pub const SEG_HEADER_SIZE: usize = PAGE_SIZE;
pub const SEG_HEADER_MAGIC: u32 = 0x47455353; // "SEGG"

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentState {
    Active      = 0,
    Sealed      = 1,
    GcCandidate = 2,
    Reclaimed   = 3,
}

impl TryFrom<u32> for SegmentState {
    type Error = ();
    fn try_from(v: u32) -> Result<Self, ()> {
        match v { 0 => Ok(Self::Active), 1 => Ok(Self::Sealed),
                  2 => Ok(Self::GcCandidate), 3 => Ok(Self::Reclaimed), _ => Err(()) }
    }
}

#[derive(Debug, Clone)]
pub struct SegmentMeta {
    pub file_id: u32,
    pub segment_id: u32,
    pub page_count: u32,
    pub live_pages: u32,
    pub state: SegmentState,
}

impl SegmentMeta {
    pub fn utilization(&self) -> f32 {
        if self.page_count == 0 { 1.0 } else { self.live_pages as f32 / self.page_count as f32 }
    }
}

// ─── AlignedBuffer ───────────────────────────────────────────────────────────

/// 4 KB-aligned heap buffer required by O_DIRECT.
pub struct AlignedBuffer {
    ptr: *mut u8,
    layout: std::alloc::Layout,
    len: usize,
}

impl AlignedBuffer {
    pub fn new(size: usize) -> Self {
        let aligned_size = (size + 4095) & !4095;
        let layout = std::alloc::Layout::from_size_align(aligned_size, 4096).unwrap();
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if ptr.is_null() { std::alloc::handle_alloc_error(layout); }
        Self { ptr, layout, len: aligned_size }
    }
    pub fn as_slice(&self) -> &[u8] { unsafe { std::slice::from_raw_parts(self.ptr, self.len) } }
    pub fn as_mut_slice(&mut self) -> &mut [u8] { unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) } }
}
impl Drop for AlignedBuffer {
    fn drop(&mut self) { unsafe { std::alloc::dealloc(self.ptr, self.layout) } }
}
unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

// ─── SegmentFile ─────────────────────────────────────────────────────────────

/// Manages one data file on disk. Not `Clone`; own it behind `Arc<Mutex<>>`.
pub struct SegmentFile {
    pub file_id: u32,
    pub path: PathBuf,
    pub segment_size: usize,          // bytes per segment
    file: File,
    pub segment_count: AtomicU32,     // segments written so far
    pub active_seg_id: AtomicU32,     // current writeable segment
    pub active_page_offset: AtomicU32, // next free page index within active segment
}

impl SegmentFile {
    // ─── Constructors ─────────────────────────────────────────────────────

    pub fn create(path: impl AsRef<Path>, file_id: u32, segment_size: usize) -> io::Result<Self> {
        assert_eq!(segment_size % PAGE_SIZE, 0, "segment_size must be page-aligned");
        let file = open_file(path.as_ref(), true)?;
        let sf = Self {
            file_id,
            path: path.as_ref().to_owned(),
            segment_size,
            file,
            segment_count: AtomicU32::new(0),
            active_seg_id: AtomicU32::new(0),
            active_page_offset: AtomicU32::new(0),
        };
        sf.write_file_header(0)?;
        Ok(sf)
    }

    pub fn open(path: impl AsRef<Path>, file_id: u32, segment_size: usize) -> io::Result<Self> {
        let file = open_file(path.as_ref(), false)?;
        let sf = Self {
            file_id,
            path: path.as_ref().to_owned(),
            segment_size,
            file,
            segment_count: AtomicU32::new(0),
            active_seg_id: AtomicU32::new(0),
            active_page_offset: AtomicU32::new(0),
        };
        sf.read_file_header()?;
        Ok(sf)
    }

    // ─── File header ──────────────────────────────────────────────────────

    pub fn write_file_header(&self, segment_count: u32) -> io::Result<()> {
        let mut buf = AlignedBuffer::new(FILE_HEADER_SIZE);
        let b = buf.as_mut_slice();
        b[0..4].copy_from_slice(&FILE_HEADER_MAGIC.to_le_bytes());
        b[4..8].copy_from_slice(&FILE_HEADER_VERSION.to_le_bytes());
        b[8..12].copy_from_slice(&self.file_id.to_le_bytes());
        b[12..16].copy_from_slice(&segment_count.to_le_bytes());
        pwrite_all(&self.file, b, 0)
    }

    fn read_file_header(&self) -> io::Result<()> {
        let mut buf = AlignedBuffer::new(FILE_HEADER_SIZE);
        pread_exact(&self.file, buf.as_mut_slice(), 0)?;
        let b = buf.as_slice();
        let magic = u32_le(&b[0..4]);
        if magic != FILE_HEADER_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad file header magic"));
        }
        let seg_count = u32_le(&b[12..16]);
        self.segment_count.store(seg_count, Ordering::Relaxed);
        // Active segment is the last one (recovery may override this).
        if seg_count > 0 {
            self.active_seg_id.store(seg_count - 1, Ordering::Relaxed);
        }
        Ok(())
    }

    // ─── Segment header ───────────────────────────────────────────────────

    pub fn start_segment(&self, seg_id: u32) -> io::Result<()> {
        let meta = SegmentMeta {
            file_id: self.file_id,
            segment_id: seg_id,
            page_count: 0,
            live_pages: 0,
            state: SegmentState::Active,
        };
        self.write_segment_header(&meta)?;
        self.segment_count.fetch_max(seg_id + 1, Ordering::AcqRel);
        self.write_file_header(self.segment_count.load(Ordering::Relaxed))?;
        Ok(())
    }

    pub fn write_segment_header(&self, meta: &SegmentMeta) -> io::Result<()> {
        let mut buf = AlignedBuffer::new(SEG_HEADER_SIZE);
        let b = buf.as_mut_slice();
        b[0..4].copy_from_slice(&SEG_HEADER_MAGIC.to_le_bytes());
        b[4..8].copy_from_slice(&meta.segment_id.to_le_bytes());
        b[8..12].copy_from_slice(&meta.page_count.to_le_bytes());
        b[12..16].copy_from_slice(&meta.live_pages.to_le_bytes());
        b[16..20].copy_from_slice(&(meta.state as u32).to_le_bytes());
        b[20..24].copy_from_slice(&meta.file_id.to_le_bytes());
        pwrite_all(&self.file, b, self.seg_start(meta.segment_id))
    }

    pub fn read_segment_header(&self, seg_id: u32) -> io::Result<SegmentMeta> {
        let mut buf = AlignedBuffer::new(SEG_HEADER_SIZE);
        pread_exact(&self.file, buf.as_mut_slice(), self.seg_start(seg_id))?;
        let b = buf.as_slice();
        let magic = u32_le(&b[0..4]);
        if magic != SEG_HEADER_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad segment header magic"));
        }
        let state = SegmentState::try_from(u32_le(&b[16..20]))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid segment state"))?;
        Ok(SegmentMeta {
            segment_id: u32_le(&b[4..8]),
            page_count: u32_le(&b[8..12]),
            live_pages: u32_le(&b[12..16]),
            state,
            file_id: u32_le(&b[20..24]),
        })
    }

    // ─── Page I/O ─────────────────────────────────────────────────────────

    /// Eagerly write an IPage at a specific (seg_id, page_idx) location.
    pub fn write_ipage_at(&self, seg_id: u32, page_idx: u32, page: &IPage) -> io::Result<()> {
        let mut buf = AlignedBuffer::new(PAGE_SIZE);
        buf.as_mut_slice().copy_from_slice(page.as_bytes());
        pwrite_all(&self.file, buf.as_slice(), self.page_offset(seg_id, page_idx))
    }

    /// Write a multi-page buffer (for LargePages). Data must be page-aligned in length.
    pub fn write_pages_at(&self, seg_id: u32, page_idx: u32, data: &[u8]) -> io::Result<()> {
        assert_eq!(data.len() % PAGE_SIZE, 0);
        // Copy into aligned buffer to satisfy O_DIRECT.
        let mut buf = AlignedBuffer::new(data.len());
        buf.as_mut_slice()[..data.len()].copy_from_slice(data);
        pwrite_all(&self.file, buf.as_slice(), self.page_offset(seg_id, page_idx))
    }

    /// Read one IPage from (seg_id, page_idx).
    pub fn read_ipage(&self, seg_id: u32, page_idx: u32) -> io::Result<IPage> {
        let mut buf = AlignedBuffer::new(PAGE_SIZE);
        pread_exact(&self.file, buf.as_mut_slice(), self.page_offset(seg_id, page_idx))?;
        IPage::from_bytes(buf.as_slice())
    }

    /// Read `span` consecutive pages.
    pub fn read_pages(&self, seg_id: u32, page_idx: u32, span: u32) -> io::Result<Vec<u8>> {
        let size = span as usize * PAGE_SIZE;
        let mut buf = AlignedBuffer::new(size);
        pread_exact(&self.file, buf.as_mut_slice(), self.page_offset(seg_id, page_idx))?;
        Ok(buf.as_slice()[..size].to_vec())
    }

    // ─── Capacity helpers ─────────────────────────────────────────────────

    pub fn data_pages_per_segment(&self) -> u32 {
        ((self.segment_size - SEG_HEADER_SIZE) / PAGE_SIZE) as u32
    }

    pub fn active_pages_used(&self) -> u32 {
        self.active_page_offset.load(Ordering::Acquire)
    }

    pub fn is_active_segment_full(&self, span: u32) -> bool {
        self.active_pages_used() + span > self.data_pages_per_segment()
    }

    pub fn advance_page_offset(&self, span: u32) -> u32 {
        self.active_page_offset.fetch_add(span, Ordering::AcqRel)
    }

    pub fn start_new_segment(&self) -> io::Result<u32> {
        let new_seg_id = self.segment_count.load(Ordering::Acquire);
        self.active_seg_id.store(new_seg_id, Ordering::Release);
        self.active_page_offset.store(0, Ordering::Release);
        self.start_segment(new_seg_id)?;
        Ok(new_seg_id)
    }

    pub fn active_seg_id(&self) -> u32 { self.active_seg_id.load(Ordering::Acquire) }

    pub fn fdatasync(&self) -> io::Result<()> {
        use std::os::unix::io::AsRawFd;
        let ret = unsafe { libc::fdatasync(self.file.as_raw_fd()) };
        if ret != 0 { Err(io::Error::last_os_error()) } else { Ok(()) }
    }

    // ─── Offsets ──────────────────────────────────────────────────────────

    pub fn seg_start(&self, seg_id: u32) -> u64 {
        FILE_HEADER_SIZE as u64 + seg_id as u64 * self.segment_size as u64
    }

    pub fn page_offset(&self, seg_id: u32, page_idx: u32) -> u64 {
        self.seg_start(seg_id) + SEG_HEADER_SIZE as u64 + page_idx as u64 * PAGE_SIZE as u64
    }

    /// Convert an absolute page index (as stored in RecordLocation) back to (seg_id, page_idx).
    pub fn abs_to_seg_page(&self, abs_idx: u32) -> (u32, u32) {
        let cap = self.data_pages_per_segment();
        (abs_idx / cap, abs_idx % cap)
    }

    /// Convert (seg_id, page_idx) → absolute page index.
    pub fn seg_page_to_abs(&self, seg_id: u32, page_idx: u32) -> u32 {
        seg_id * self.data_pages_per_segment() + page_idx
    }
}

// ─── Low-level I/O helpers ────────────────────────────────────────────────────

fn open_file(path: &Path, create: bool) -> io::Result<File> {
    // Try O_DIRECT first; fall back with a fresh OpenOptions if not supported
    // (e.g. tmpfs used in tests returns EINVAL for O_DIRECT at open time).
    let r = {
        let mut opts = OpenOptions::new();
        opts.read(true).write(true);
        if create { opts.create(true).truncate(true); }
        opts.custom_flags(libc::O_DIRECT).open(path)
    };
    match r {
        Ok(f) => Ok(f),
        Err(e) if e.raw_os_error() == Some(libc::EINVAL) => {
            let mut opts = OpenOptions::new();
            opts.read(true).write(true);
            if create { opts.create(true).truncate(true); }
            opts.open(path)
        }
        Err(e) => Err(e),
    }
}

pub fn pwrite_all(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    let mut written = 0usize;
    while written < buf.len() {
        let n = unsafe {
            libc::pwrite(
                file.as_raw_fd(),
                buf[written..].as_ptr() as *const libc::c_void,
                buf.len() - written,
                (offset + written as u64) as libc::off_t,
            )
        };
        if n < 0 { return Err(io::Error::last_os_error()); }
        written += n as usize;
    }
    Ok(())
}

pub fn pread_exact(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    let mut read = 0usize;
    while read < buf.len() {
        let n = unsafe {
            libc::pread(
                file.as_raw_fd(),
                buf[read..].as_mut_ptr() as *mut libc::c_void,
                buf.len() - read,
                (offset + read as u64) as libc::off_t,
            )
        };
        if n < 0 { return Err(io::Error::last_os_error()); }
        if n == 0 { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF")); }
        read += n as usize;
    }
    Ok(())
}

fn u32_le(b: &[u8]) -> u32 { u32::from_le_bytes(b[..4].try_into().unwrap()) }
