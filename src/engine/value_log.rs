//! Append-only value log — stores variable-size (key, value) pairs.
//!
//! The value log is an extension of the SIndex paper design to support
//! variable-length values (the original paper targets fixed-size block
//! storage mappings). Each leaf B-Tree entry stores only a `value_ptr`
//! (byte offset into this file) plus `key_len` and `value_len`; the
//! actual bytes live here.
//!
//! The log doubles as the engine's redo journal: every put appends an
//! ALIVE entry and every delete appends a DELETED tombstone, so the
//! B-Tree index can be staged in memory (WSBCache) and rebuilt from the
//! log tail after a crash (see `KvEngine` recovery).
//!
//! ## Entry format
//! ```text
//! [0..4]  magic      u32 = VLOG_MAGIC
//! [4..6]  key_len    u16
//! [6..10] value_len  u32
//! [10]    flags      u8   (VFLAG_ALIVE | VFLAG_DELETED)
//! [11]    _pad       u8
//! [12..16] checksum  u32  (crc32 over [key][value])
//! [16..]  key        [u8; key_len]
//! [16+key_len..] value [u8; value_len]
//! ```
//! Entries are NOT padded — they are read by absolute offset + lengths.
//!
//! ## Concurrency
//! Appends serialize behind a writer mutex (the log is sequential by
//! design). Reads use `pread` on a separate descriptor and take **no
//! lock**, so concurrent GETs never contend here.

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::{Arc, Mutex};

pub const VLOG_MAGIC: u32 = 0x564C4F47; // "VLOG"
pub const VLOG_HEADER_SIZE: usize = 16;

pub const VFLAG_ALIVE: u8 = 1;
pub const VFLAG_DELETED: u8 = 2;

/// A decoded entry yielded by `scan_from` during recovery replay.
pub struct ReplayEntry {
    /// Byte offset of the entry header (the `value_ptr` for the index).
    pub offset: u64,
    pub key: Vec<u8>,
    pub value_len: u32,
    pub flags: u8,
}

/// Thread-safe append-only value log.
pub struct ValueLog {
    write_inner: Mutex<WriteInner>,
    /// Separate descriptor for lock-free positional reads.
    read_file: File,
}

struct WriteInner {
    write_file: BufWriter<File>,
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
            write_inner: Mutex::new(WriteInner {
                write_file: BufWriter::new(write_f),
                write_pos,
            }),
            read_file: read_f,
        }))
    }

    fn append_entry(&self, key: &[u8], value: &[u8], flags: u8) -> io::Result<u64> {
        assert!(key.len() <= u16::MAX as usize, "key too long");
        assert!(value.len() <= u32::MAX as usize, "value too long");

        let crc = {
            let mut h = crc32fast::Hasher::new();
            h.update(key);
            h.update(value);
            h.finalize()
        };

        let mut inner = self.write_inner.lock().unwrap();
        let offset = inner.write_pos;

        let mut hdr = [0u8; VLOG_HEADER_SIZE];
        write_u32(&mut hdr, 0, VLOG_MAGIC);
        write_u16(&mut hdr, 4, key.len() as u16);
        write_u32(&mut hdr, 6, value.len() as u32);
        hdr[10] = flags;
        hdr[11] = 0;
        write_u32(&mut hdr, 12, crc);

        inner.write_file.write_all(&hdr)?;
        inner.write_file.write_all(key)?;
        inner.write_file.write_all(value)?;
        // Flush to the OS (no fsync) so the read descriptor sees the bytes
        // immediately. Durability comes from the TSS sync cycle.
        inner.write_file.flush()?;

        inner.write_pos += (VLOG_HEADER_SIZE + key.len() + value.len()) as u64;
        Ok(offset)
    }

    /// Append a live `(key, value)` entry, returning its byte offset.
    pub fn append(&self, key: &[u8], value: &[u8]) -> io::Result<u64> {
        self.append_entry(key, value, VFLAG_ALIVE)
    }

    /// Append a delete tombstone for `key`, returning its byte offset.
    pub fn append_tombstone(&self, key: &[u8]) -> io::Result<u64> {
        self.append_entry(key, &[], VFLAG_DELETED)
    }

    /// Read the key and value stored at `offset`, with full validation.
    pub fn read(
        &self,
        offset: u64,
        key_len: u16,
        value_len: u32,
    ) -> io::Result<(Vec<u8>, Vec<u8>)> {
        let mut hdr = [0u8; VLOG_HEADER_SIZE];
        self.read_file.read_exact_at(&mut hdr, offset)?;

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

        let mut payload = vec![0u8; key_len as usize + value_len as usize];
        self.read_file
            .read_exact_at(&mut payload, offset + VLOG_HEADER_SIZE as u64)?;

        let actual_crc = crc32fast::hash(&payload);
        if actual_crc != stored_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "value log checksum mismatch",
            ));
        }

        let value = payload.split_off(key_len as usize);
        Ok((payload, value))
    }

    /// Read `(key, value)` at `offset` in a single positional read, without
    /// header/CRC validation. Trusted fast path for GET: the offset and
    /// lengths come from the B-Tree index, and the caller compares the
    /// returned key against the requested key (which also catches any
    /// corruption that matters for correctness).
    pub fn read_key_value(
        &self,
        offset: u64,
        key_len: u16,
        value_len: u32,
    ) -> io::Result<(Vec<u8>, Vec<u8>)> {
        let mut payload = vec![0u8; key_len as usize + value_len as usize];
        self.read_file
            .read_exact_at(&mut payload, offset + VLOG_HEADER_SIZE as u64)?;
        let value = payload.split_off(key_len as usize);
        Ok((payload, value))
    }

    /// Read only the key stored at `offset` (for collision detection).
    pub fn read_key(&self, offset: u64, key_len: u16) -> io::Result<Vec<u8>> {
        let mut key_buf = vec![0u8; key_len as usize];
        self.read_file
            .read_exact_at(&mut key_buf, offset + VLOG_HEADER_SIZE as u64)?;
        Ok(key_buf)
    }

    /// Read only the value stored at `offset`.
    pub fn read_value(&self, offset: u64, key_len: u16, value_len: u32) -> io::Result<Vec<u8>> {
        let mut val_buf = vec![0u8; value_len as usize];
        self.read_file.read_exact_at(
            &mut val_buf,
            offset + VLOG_HEADER_SIZE as u64 + key_len as u64,
        )?;
        Ok(val_buf)
    }

    /// Sequentially decode entries from `offset` to the end of the log,
    /// validating magic + CRC. Returns the replayable entries and the offset
    /// just past the last *valid* entry (a torn tail stops the scan).
    pub fn scan_from(&self, offset: u64) -> io::Result<(u64, Vec<ReplayEntry>)> {
        let end = self.size();
        let mut pos = offset.min(end);
        let mut entries = Vec::new();

        while pos + VLOG_HEADER_SIZE as u64 <= end {
            let mut hdr = [0u8; VLOG_HEADER_SIZE];
            if self.read_file.read_exact_at(&mut hdr, pos).is_err() {
                break;
            }
            if read_u32(&hdr, 0) != VLOG_MAGIC {
                break;
            }
            let key_len = read_u16(&hdr, 4) as u64;
            let value_len = read_u32(&hdr, 6) as u64;
            let flags = hdr[10];
            let stored_crc = read_u32(&hdr, 12);
            let entry_end = pos + VLOG_HEADER_SIZE as u64 + key_len + value_len;
            if entry_end > end {
                break; // torn tail
            }
            let mut payload = vec![0u8; (key_len + value_len) as usize];
            if self
                .read_file
                .read_exact_at(&mut payload, pos + VLOG_HEADER_SIZE as u64)
                .is_err()
            {
                break;
            }
            if crc32fast::hash(&payload) != stored_crc {
                break; // torn tail
            }
            payload.truncate(key_len as usize);
            entries.push(ReplayEntry {
                offset: pos,
                key: payload,
                value_len: value_len as u32,
                flags,
            });
            pos = entry_end;
        }
        Ok((pos, entries))
    }

    /// Flush buffered writes and fsync for durability.
    pub fn flush(&self) -> io::Result<()> {
        let mut inner = self.write_inner.lock().unwrap();
        inner.write_file.flush()?;
        inner.write_file.get_ref().sync_data()
    }

    /// Current end-of-log position.
    pub fn size(&self) -> u64 {
        self.write_inner.lock().unwrap().write_pos
    }

    /// Truncate the log to `len` bytes (used to drop a torn tail after
    /// crash recovery). The caller must ensure no index entry references
    /// offsets >= `len`.
    pub fn truncate_to(&self, len: u64) -> io::Result<()> {
        let mut inner = self.write_inner.lock().unwrap();
        inner.write_file.flush()?;
        inner.write_file.get_ref().set_len(len)?;
        inner.write_file.seek(SeekFrom::Start(len))?;
        inner.write_pos = len;
        Ok(())
    }

    /// Truncate the value log to empty, resetting the write position.
    /// The caller must have already cleared the B-Tree index so no
    /// dangling value_ptr references remain.
    pub fn truncate(&self) -> io::Result<()> {
        self.truncate_to(0)
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
    fn append_and_read_roundtrip() {
        let dir = tempdir().unwrap();
        let vlog = ValueLog::open(&dir.path().join("v.log")).unwrap();

        let off1 = vlog.append(b"key1", b"value-one").unwrap();
        let off2 = vlog.append(b"key2", b"value-two").unwrap();

        let (k, v) = vlog.read(off1, 4, 9).unwrap();
        assert_eq!(k, b"key1");
        assert_eq!(v, b"value-one");

        let (k, v) = vlog.read_key_value(off2, 4, 9).unwrap();
        assert_eq!(k, b"key2");
        assert_eq!(v, b"value-two");
    }

    #[test]
    fn scan_replays_all_entries_and_tombstones() {
        let dir = tempdir().unwrap();
        let vlog = ValueLog::open(&dir.path().join("v.log")).unwrap();

        vlog.append(b"a", b"1").unwrap();
        let mid = vlog.append(b"b", b"2").unwrap();
        vlog.append_tombstone(b"a").unwrap();

        let (end, entries) = vlog.scan_from(0).unwrap();
        assert_eq!(end, vlog.size());
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].key, b"a");
        assert_eq!(entries[0].flags, VFLAG_ALIVE);
        assert_eq!(entries[2].key, b"a");
        assert_eq!(entries[2].flags, VFLAG_DELETED);

        // Partial scan from the middle
        let (_, tail) = vlog.scan_from(mid).unwrap();
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[0].key, b"b");
    }

    #[test]
    fn scan_stops_at_torn_tail() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("v.log");
        let vlog = ValueLog::open(&path).unwrap();
        vlog.append(b"good", b"entry").unwrap();
        let valid_end = vlog.size();
        // Simulate a torn write: garbage bytes at the tail.
        {
            let f = OpenOptions::new().append(true).open(&path).unwrap();
            let mut w = BufWriter::new(f);
            w.write_all(&[0xDE, 0xAD, 0xBE, 0xEF]).unwrap();
            w.flush().unwrap();
        }
        let vlog = ValueLog::open(&path).unwrap();
        let (end, entries) = vlog.scan_from(0).unwrap();
        assert_eq!(end, valid_end);
        assert_eq!(entries.len(), 1);
        vlog.truncate_to(end).unwrap();
        assert_eq!(vlog.size(), valid_end);
    }
}
