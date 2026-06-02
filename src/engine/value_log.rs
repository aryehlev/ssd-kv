//! Append-only value log — stores variable-size (key, value) pairs.
//!
//! The value log is an extension of the SIndex paper design to support
//! variable-length values (the original paper targets fixed-size block
//! storage mappings). Each leaf B-Tree entry stores only a `value_ptr`
//! (byte offset into this file) plus `key_len` and `value_len`; the
//! actual bytes live here.
//!
//! ## Entry format
//! ```text
//! [0..4]  magic      u32 = VLOG_MAGIC
//! [4..6]  key_len    u16
//! [6..10] value_len  u32
//! [10]    flags      u8   (FLAG_ALIVE | FLAG_DELETED)
//! [11]    _pad       u8
//! [12..16] checksum  u32  (crc32 over [key][value])
//! [16..]  key        [u8; key_len]
//! [16+key_len..] value [u8; value_len]
//! ```
//! Entries are NOT padded — they are read by absolute offset + lengths.

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

pub const VLOG_MAGIC: u32 = 0x564C4F47; // "VLOG"
pub const VLOG_HEADER_SIZE: usize = 16;

pub const VFLAG_ALIVE: u8 = 1;
pub const VFLAG_DELETED: u8 = 2;

/// Thread-safe append-only value log.
pub struct ValueLog {
    inner: Mutex<ValueLogInner>,
}

struct ValueLogInner {
    write_file: BufWriter<File>,
    read_file: File,
    /// Current write position (= end of the log).
    write_pos: u64,
}

impl ValueLog {
    /// Open an existing value log (or create one if the file does not exist).
    pub fn open(path: &Path) -> io::Result<Arc<Self>> {
        let write_f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        let read_f = OpenOptions::new().read(true).open(path)?;
        let write_pos = write_f.metadata()?.len();
        Ok(Arc::new(ValueLog {
            inner: Mutex::new(ValueLogInner {
                write_file: BufWriter::new(write_f),
                read_file: read_f,
                write_pos,
            }),
        }))
    }

    /// Append `(key, value)` and return the byte offset where the entry starts.
    ///
    /// The returned offset is stored in the B-Tree leaf entry so that the
    /// value can be retrieved later via `read()`.
    pub fn append(&self, key: &[u8], value: &[u8]) -> io::Result<u64> {
        assert!(key.len() <= u16::MAX as usize, "key too long");
        assert!(value.len() <= u32::MAX as usize, "value too long");

        let crc = {
            let mut h = crc32fast::Hasher::new();
            h.update(key);
            h.update(value);
            h.finalize()
        };

        let mut inner = self.inner.lock().unwrap();
        let offset = inner.write_pos;

        let mut hdr = [0u8; VLOG_HEADER_SIZE];
        write_u32(&mut hdr, 0, VLOG_MAGIC);
        write_u16(&mut hdr, 4, key.len() as u16);
        write_u32(&mut hdr, 6, value.len() as u32);
        hdr[10] = VFLAG_ALIVE;
        hdr[11] = 0;
        write_u32(&mut hdr, 12, crc);

        inner.write_file.write_all(&hdr)?;
        inner.write_file.write_all(key)?;
        inner.write_file.write_all(value)?;
        inner.write_file.flush()?;

        inner.write_pos += (VLOG_HEADER_SIZE + key.len() + value.len()) as u64;
        Ok(offset)
    }

    /// Read the key and value stored at `offset`.
    ///
    /// `key_len` and `value_len` come from the B-Tree leaf entry and serve
    /// as a fast path; the header is still read and validated for safety.
    pub fn read(
        &self,
        offset: u64,
        key_len: u16,
        value_len: u32,
    ) -> io::Result<(Vec<u8>, Vec<u8>)> {
        let mut inner = self.inner.lock().unwrap();

        inner.read_file.seek(SeekFrom::Start(offset))?;

        let mut hdr = [0u8; VLOG_HEADER_SIZE];
        inner.read_file.read_exact(&mut hdr)?;

        let magic = read_u32(&hdr, 0);
        if magic != VLOG_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bad value log magic at offset {}: 0x{:08x}", offset, magic),
            ));
        }

        let stored_key_len = read_u16(&hdr, 4);
        let stored_val_len = read_u32(&hdr, 6);
        let flags = hdr[10];
        let stored_crc = read_u32(&hdr, 12);

        // Use lengths from the header; caller's hints are sanity-checked.
        if stored_key_len != key_len || stored_val_len != value_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "value log length mismatch at {}: expected key={} val={}, got key={} val={}",
                    offset, key_len, value_len, stored_key_len, stored_val_len
                ),
            ));
        }

        if flags == VFLAG_DELETED {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "value log entry is deleted",
            ));
        }

        let mut key_buf = vec![0u8; key_len as usize];
        let mut val_buf = vec![0u8; value_len as usize];
        inner.read_file.read_exact(&mut key_buf)?;
        inner.read_file.read_exact(&mut val_buf)?;

        // Verify checksum
        let actual_crc = {
            let mut h = crc32fast::Hasher::new();
            h.update(&key_buf);
            h.update(&val_buf);
            h.finalize()
        };
        if actual_crc != stored_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "value log checksum mismatch",
            ));
        }

        Ok((key_buf, val_buf))
    }

    /// Read only the value (not the key) stored at `offset`.
    pub fn read_value(&self, offset: u64, key_len: u16, value_len: u32) -> io::Result<Vec<u8>> {
        let mut inner = self.inner.lock().unwrap();

        inner
            .read_file
            .seek(SeekFrom::Start(offset + VLOG_HEADER_SIZE as u64 + key_len as u64))?;
        let mut val_buf = vec![0u8; value_len as usize];
        inner.read_file.read_exact(&mut val_buf)?;
        Ok(val_buf)
    }

    /// Read only the key stored at `offset` (for collision detection).
    pub fn read_key(&self, offset: u64, key_len: u16) -> io::Result<Vec<u8>> {
        let mut inner = self.inner.lock().unwrap();

        inner
            .read_file
            .seek(SeekFrom::Start(offset + VLOG_HEADER_SIZE as u64))?;
        let mut key_buf = vec![0u8; key_len as usize];
        inner.read_file.read_exact(&mut key_buf)?;
        Ok(key_buf)
    }

    /// Flush buffered writes to the OS.
    pub fn flush(&self) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.write_file.flush()?;
        inner.write_file.get_ref().sync_data()
    }

    /// Current end-of-log position.
    pub fn size(&self) -> u64 {
        self.inner.lock().unwrap().write_pos
    }
}

// ─── Little-endian helpers ───────────────────────────────────────────────────

#[inline]
fn read_u16(b: &[u8], off: usize) -> u16 {
    u16::from_le_bytes(b[off..off + 2].try_into().unwrap())
}

#[inline]
fn read_u32(b: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(b[off..off + 4].try_into().unwrap())
}

#[inline]
fn write_u16(b: &mut [u8], off: usize, v: u16) {
    b[off..off + 2].copy_from_slice(&v.to_le_bytes());
}

#[inline]
fn write_u32(b: &mut [u8], off: usize, v: u32) {
    b[off..off + 4].copy_from_slice(&v.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn append_and_read() {
        let dir = tempdir().unwrap();
        let vlog = ValueLog::open(&dir.path().join("v.log")).unwrap();

        let offset = vlog.append(b"hello", b"world").unwrap();
        let (k, v) = vlog.read(offset, 5, 5).unwrap();
        assert_eq!(k, b"hello");
        assert_eq!(v, b"world");
    }

    #[test]
    fn multiple_entries() {
        let dir = tempdir().unwrap();
        let vlog = ValueLog::open(&dir.path().join("v.log")).unwrap();

        let mut offsets = Vec::new();
        for i in 0u64..100 {
            let key = format!("key_{}", i);
            let val = format!("val_{}_longer", i);
            offsets.push((
                vlog.append(key.as_bytes(), val.as_bytes()).unwrap(),
                key.len() as u16,
                val.len() as u32,
                val,
            ));
        }

        for (offset, kl, vl, expected_val) in offsets {
            let v = vlog.read_value(offset, kl, vl).unwrap();
            assert_eq!(v, expected_val.as_bytes());
        }
    }

    #[test]
    fn large_value() {
        let dir = tempdir().unwrap();
        let vlog = ValueLog::open(&dir.path().join("v.log")).unwrap();

        let key = b"bigkey";
        let value = vec![0xABu8; 1024 * 1024]; // 1 MB value
        let offset = vlog.append(key, &value).unwrap();
        let v = vlog.read_value(offset, key.len() as u16, value.len() as u32).unwrap();
        assert_eq!(v, value);
    }
}
