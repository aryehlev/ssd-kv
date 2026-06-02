//! Per-partition segment file: manages a file of 4 KB ipages for one B-Tree.
//!
//! Each partition in SIndex owns a dedicated segment. The segment file starts
//! with a SegmentHeader at page 0, followed by B-Tree nodes at pages 1..N.
//! Pages are addressed by index (`page_idx`); byte offset = page_idx × 4096.
//!
//! On-disk segment header (page 0, 4096 bytes):
//! ```text
//! [0..4]   magic         u32 = SEGMENT_MAGIC
//! [4..8]   partition_id  u32
//! [8..12]  root_page     u32 (0 = empty tree)
//! [12..16] next_free     u32 (index of next page to allocate; starts at 1)
//! [16..24] live_entries  u64
//! [24..28] version       u32
//! [28..32] checksum      u32
//! [32..4096] padding
//! ```

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::engine::ipage::IPAGE_SIZE;

pub const SEGMENT_MAGIC: u32 = 0x53454758; // "SEGX"

/// In-memory view of the segment header (page 0).
#[derive(Debug, Clone)]
pub struct SegmentHeader {
    pub partition_id: u32,
    /// Page index of the B-Tree root node (0 = no tree yet).
    pub root_page: u32,
    /// Next page index to allocate; starts at 1 (page 0 is the header).
    pub next_free: u32,
    /// Count of live (non-deleted) entries in this partition.
    pub live_entries: u64,
    pub version: u32,
}

impl SegmentHeader {
    fn write_to(&self, page: &mut [u8; IPAGE_SIZE]) {
        page.fill(0);
        write_u32(page, 0, SEGMENT_MAGIC);
        write_u32(page, 4, self.partition_id);
        write_u32(page, 8, self.root_page);
        write_u32(page, 12, self.next_free);
        write_u64(page, 16, self.live_entries);
        write_u32(page, 24, self.version);
        // checksum: crc32 of bytes [0..28]
        let crc = crc32fast::hash(&page[0..28]);
        write_u32(page, 28, crc);
    }

    fn read_from(page: &[u8; IPAGE_SIZE]) -> io::Result<Self> {
        let magic = read_u32(page, 0);
        if magic != SEGMENT_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bad segment magic: 0x{:08x}", magic),
            ));
        }
        let stored_crc = read_u32(page, 28);
        let computed = crc32fast::hash(&page[0..28]);
        if stored_crc != computed {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "segment header checksum mismatch",
            ));
        }
        Ok(SegmentHeader {
            partition_id: read_u32(page, 4),
            root_page: read_u32(page, 8),
            next_free: read_u32(page, 12),
            live_entries: read_u64(page, 16),
            version: read_u32(page, 24),
        })
    }
}

/// A segment file managing the B-Tree pages for one partition.
pub struct SegmentFile {
    file: File,
    pub header: SegmentHeader,
}

impl SegmentFile {
    /// Open an existing segment file.
    pub fn open(path: &Path) -> io::Result<Self> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let mut page = [0u8; IPAGE_SIZE];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut page)?;
        let header = SegmentHeader::read_from(&page)?;
        Ok(SegmentFile { file, header })
    }

    /// Create a new segment file for `partition_id`.
    pub fn create(path: &Path, partition_id: u32) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;

        let header = SegmentHeader {
            partition_id,
            root_page: 0,
            next_free: 1, // page 0 is the header
            live_entries: 0,
            version: 1,
        };
        let mut page = [0u8; IPAGE_SIZE];
        header.write_to(&mut page);
        file.write_all(&page)?;
        file.flush()?;
        Ok(SegmentFile { file, header })
    }

    /// Read page `idx` into `buf`.
    pub fn read_page(&mut self, idx: u32, buf: &mut [u8; IPAGE_SIZE]) -> io::Result<()> {
        let offset = (idx as u64) * (IPAGE_SIZE as u64);
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(buf)?;
        Ok(())
    }

    /// Write `buf` to page `idx`.
    pub fn write_page(&mut self, idx: u32, buf: &[u8; IPAGE_SIZE]) -> io::Result<()> {
        let offset = (idx as u64) * (IPAGE_SIZE as u64);
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(buf)?;
        Ok(())
    }

    /// Allocate a new page, returning its index. Updates the header in memory
    /// but does NOT persist — call `flush_header()` when appropriate.
    pub fn alloc_page(&mut self) -> u32 {
        let idx = self.header.next_free;
        self.header.next_free += 1;
        idx
    }

    /// Persist the in-memory header to page 0.
    pub fn flush_header(&mut self) -> io::Result<()> {
        let mut page = [0u8; IPAGE_SIZE];
        self.header.write_to(&mut page);
        self.write_page(0, &page)
    }

    /// Flush and fsync the segment file for durability.
    pub fn sync(&mut self) -> io::Result<()> {
        self.flush_header()?;
        self.file.flush()?;
        self.file.sync_data()
    }
}

// ─── Little-endian helpers (duplicate here to avoid cross-module dep) ────────

#[inline]
fn read_u32(b: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(b[off..off + 4].try_into().unwrap())
}

#[inline]
fn read_u64(b: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(b[off..off + 8].try_into().unwrap())
}

#[inline]
fn write_u32(b: &mut [u8], off: usize, v: u32) {
    b[off..off + 4].copy_from_slice(&v.to_le_bytes());
}

#[inline]
fn write_u64(b: &mut [u8], off: usize, v: u64) {
    b[off..off + 8].copy_from_slice(&v.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn create_and_open() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("part0.seg");
        {
            let mut seg = SegmentFile::create(&path, 7).unwrap();
            assert_eq!(seg.header.partition_id, 7);
            assert_eq!(seg.header.root_page, 0);
            assert_eq!(seg.header.next_free, 1);
            // allocate some pages
            assert_eq!(seg.alloc_page(), 1);
            assert_eq!(seg.alloc_page(), 2);
            seg.sync().unwrap();
        }
        {
            let seg = SegmentFile::open(&path).unwrap();
            assert_eq!(seg.header.partition_id, 7);
            assert_eq!(seg.header.next_free, 3);
        }
    }

    #[test]
    fn write_and_read_page() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.seg");
        let mut seg = SegmentFile::create(&path, 0).unwrap();
        let idx = seg.alloc_page();
        let mut wb = [0u8; IPAGE_SIZE];
        wb[0] = 0xDE;
        wb[4095] = 0xAD;
        seg.write_page(idx, &wb).unwrap();

        let mut rb = [0u8; IPAGE_SIZE];
        seg.read_page(idx, &mut rb).unwrap();
        assert_eq!(rb[0], 0xDE);
        assert_eq!(rb[4095], 0xAD);
    }
}
