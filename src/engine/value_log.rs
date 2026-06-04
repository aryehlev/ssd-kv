//! Append-only value log — stores variable-size (key, value) pairs.
//!
//! ## Concurrency model
//! Writes are serialized through `Mutex<WriteInner>`. Reads use `pread` on a
//! stable file descriptor and do NOT hold the write mutex, so concurrent
//! readers never block writers and concurrent writers never block readers.
//!
//! ## Durability
//! `append()` writes into a `BufWriter` without syncing. Call
//! `flush_and_sync()` (or use `GroupCommit`) to make data durable. This
//! enables group-commit batching: N concurrent writers pay for one `fdatasync`
//! instead of N.
//!
//! ## Entry format
//! ```text
//! [0..4]   magic      u32 = VLOG_MAGIC
//! [4..6]   key_len    u16
//! [6..10]  value_len  u32
//! [10]     flags      u8   (VFLAG_ALIVE | VFLAG_DELETED)
//! [11]     _pad       u8
//! [12..16] checksum   u32  (crc32 over [key][value])
//! [16..]   key        [u8; key_len]
//! [16+key_len..] value [u8; value_len]
//! ```

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub const VLOG_MAGIC: u32 = 0x564C4F47; // "VLOG"
pub const VLOG_HEADER_SIZE: usize = 16;

pub const VFLAG_ALIVE: u8 = 1;
pub const VFLAG_DELETED: u8 = 2;

/// Threshold at which compaction is suggested: dead / total > 50%.
const COMPACTION_DEAD_RATIO: f64 = 0.5;

struct WriteInner {
    file: BufWriter<File>,
    write_pos: u64,
    /// Kept alive so that `read_fd` remains valid.
    _read_file: File,
}

/// Thread-safe append-only value log with lock-free reads.
pub struct ValueLog {
    inner: Mutex<WriteInner>,
    /// Stable fd used by all readers via `pread` — never closed while `self`
    /// is alive because `_read_file` inside the mutex owns it.
    read_fd: RawFd,
    /// Total bytes ever appended (monotonically increasing).
    total_bytes: AtomicU64,
    /// Bytes from overwritten or deleted entries (dead space).
    dead_bytes: AtomicU64,
    /// Highest write position that has been durably persisted via fdatasync.
    /// Updated by `flush_and_sync()`. Used by `GroupCommit` to avoid
    /// redundant fsyncs when multiple writers share one flush.
    fsynced_through: AtomicU64,
}

impl ValueLog {
    /// Open (or create) the value log at `path`.
    pub fn open(path: &Path) -> io::Result<Arc<Self>> {
        let write_f = OpenOptions::new()
            .read(true).write(true).create(true)
            .open(path)?;
        let read_f = OpenOptions::new().read(true).open(path)?;
        let write_pos = write_f.metadata()?.len();
        let read_fd = read_f.as_raw_fd();

        Ok(Arc::new(ValueLog {
            inner: Mutex::new(WriteInner {
                file: BufWriter::new(write_f),
                write_pos,
                _read_file: read_f,
            }),
            read_fd,
            total_bytes: AtomicU64::new(write_pos),
            dead_bytes: AtomicU64::new(0),
            // Existing file data is already durable from a prior run.
            fsynced_through: AtomicU64::new(write_pos),
        }))
    }

    // ─── Write path ───────────────────────────────────────────────────────────

    /// Append `(key, value)` to the log and return the byte offset of the new
    /// entry.  Does NOT fsync — call `flush_and_sync()` for durability.
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

        inner.file.write_all(&hdr)?;
        inner.file.write_all(key)?;
        inner.file.write_all(value)?;
        // No sync — caller uses flush_and_sync() / GroupCommit.

        let entry_size = (VLOG_HEADER_SIZE + key.len() + value.len()) as u64;
        inner.write_pos += entry_size;
        self.total_bytes.fetch_add(entry_size, Ordering::Relaxed);

        Ok(offset)
    }

    /// Flush the write buffer to the kernel and call `fdatasync`.
    ///
    /// Releases the inner lock *before* calling `fdatasync` so that concurrent
    /// `append()` calls are not blocked for the full duration of the sync.
    /// After `fdatasync` returns, `fsynced_through` is updated to the captured
    /// write position, which `GroupCommit` uses to skip redundant fsyncs.
    pub fn flush_and_sync(&self) -> io::Result<()> {
        let (fd, captured_pos) = {
            let mut inner = self.inner.lock().unwrap();
            inner.file.flush()?;
            let pos = inner.write_pos;
            let fd = inner.file.get_ref().as_raw_fd();
            (fd, pos)
            // inner lock released here
        };
        unsafe {
            if libc::fdatasync(fd) != 0 {
                return Err(io::Error::last_os_error());
            }
        }
        // Use Release so that readers of fsynced_through (using Acquire) see
        // all the data that was written before this store.
        self.fsynced_through.fetch_max(captured_pos, Ordering::Release);
        Ok(())
    }

    /// The highest write position that has been durably persisted on disk.
    /// Used by `GroupCommit` to avoid redundant fsyncs.
    #[inline]
    pub fn fsynced_through(&self) -> u64 {
        self.fsynced_through.load(Ordering::Acquire)
    }

    /// Current dead-byte count (for compaction decisions).
    #[inline]
    pub fn dead_bytes_count(&self) -> u64 {
        self.dead_bytes.load(Ordering::Relaxed)
    }

    /// Account for `bytes` of dead space (overwritten / deleted entry).
    pub fn mark_dead(&self, key_len: u16, value_len: u32) {
        let dead = (VLOG_HEADER_SIZE as u64) + key_len as u64 + value_len as u64;
        self.dead_bytes.fetch_add(dead, Ordering::Relaxed);
    }

    // ─── Read path (lock-free via pread) ─────────────────────────────────────

    /// Read the value stored at `offset` without holding the write lock.
    pub fn read_value(&self, offset: u64, key_len: u16, value_len: u32) -> io::Result<Vec<u8>> {
        let pos = (offset + VLOG_HEADER_SIZE as u64 + key_len as u64) as i64;
        let mut buf = vec![0u8; value_len as usize];
        pread_exact(self.read_fd, &mut buf, pos)?;
        Ok(buf)
    }

    /// Read only the key stored at `offset` (for hash-collision detection).
    pub fn read_key(&self, offset: u64, key_len: u16) -> io::Result<Vec<u8>> {
        let pos = (offset + VLOG_HEADER_SIZE as u64) as i64;
        let mut buf = vec![0u8; key_len as usize];
        pread_exact(self.read_fd, &mut buf, pos)?;
        Ok(buf)
    }

    /// Read the key and value stored at `offset`.
    pub fn read(&self, offset: u64, key_len: u16, value_len: u32) -> io::Result<(Vec<u8>, Vec<u8>)> {
        let pos = (offset + VLOG_HEADER_SIZE as u64) as i64;
        let total = key_len as usize + value_len as usize;
        let mut buf = vec![0u8; total];
        pread_exact(self.read_fd, &mut buf, pos)?;
        let (k, v) = buf.split_at(key_len as usize);
        Ok((k.to_vec(), v.to_vec()))
    }

    // ─── Compaction support ───────────────────────────────────────────────────

    /// Return true when dead space exceeds the compaction threshold.
    pub fn compaction_needed(&self) -> bool {
        let dead = self.dead_bytes.load(Ordering::Relaxed);
        let total = self.total_bytes.load(Ordering::Relaxed);
        total > 0 && (dead as f64 / total as f64) > COMPACTION_DEAD_RATIO
    }

    /// Current write position (= total bytes written including header).
    pub fn size(&self) -> u64 {
        self.inner.lock().unwrap().write_pos
    }

    /// Reset dead-byte counter after a successful compaction.
    pub fn reset_dead_bytes(&self) {
        self.dead_bytes.store(0, Ordering::Relaxed);
    }

    /// Flush and truncate to zero (FLUSHDB / FLUSHALL).  The caller must have
    /// already cleared all B-Tree references so no dangling `value_ptr`s remain.
    pub fn truncate(&self) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.file.flush()?;
        inner.file.get_ref().set_len(0)?;
        // Re-seek the write file to position 0
        use std::io::Seek;
        inner.file.seek(std::io::SeekFrom::Start(0))?;
        inner.write_pos = 0;
        self.total_bytes.store(0, Ordering::Relaxed);
        self.dead_bytes.store(0, Ordering::Relaxed);
        self.fsynced_through.store(0, Ordering::Relaxed);
        Ok(())
    }

    pub fn flush(&self) -> io::Result<()> {
        self.flush_and_sync()
    }
}

// ─── pread helper ─────────────────────────────────────────────────────────────

fn pread_exact(fd: RawFd, buf: &mut [u8], offset: i64) -> io::Result<()> {
    let n = unsafe {
        libc::pread(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len(), offset)
    };
    if n == buf.len() as isize {
        Ok(())
    } else if n < 0 {
        Err(io::Error::last_os_error())
    } else {
        Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short pread on value log"))
    }
}

// ─── Little-endian helpers ────────────────────────────────────────────────────

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
        vlog.flush_and_sync().unwrap();
        let (k, v) = vlog.read(offset, 5, 5).unwrap();
        assert_eq!(k, b"hello");
        assert_eq!(v, b"world");
    }

    #[test]
    fn concurrent_reads() {
        use std::sync::Arc;
        let dir = tempdir().unwrap();
        let vlog = ValueLog::open(&dir.path().join("v.log")).unwrap();
        let mut offsets = Vec::new();
        for i in 0u32..100 {
            let key = format!("key{}", i);
            let val = format!("val{}", i);
            let off = vlog.append(key.as_bytes(), val.as_bytes()).unwrap();
            offsets.push((off, key.len() as u16, val.len() as u32, val));
        }
        vlog.flush_and_sync().unwrap();

        let vlog = Arc::new(vlog);
        let offsets = Arc::new(offsets);
        let mut handles = Vec::new();
        for _ in 0..8 {
            let vl = Arc::clone(&vlog);
            let offs = Arc::clone(&offsets);
            handles.push(std::thread::spawn(move || {
                for (off, kl, vl_len, expected) in offs.iter() {
                    let v = vl.read_value(*off, *kl, *vl_len).unwrap();
                    assert_eq!(v, expected.as_bytes());
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
    }

    #[test]
    fn large_value() {
        let dir = tempdir().unwrap();
        let vlog = ValueLog::open(&dir.path().join("v.log")).unwrap();
        let key = b"bigkey";
        let value = vec![0xABu8; 1024 * 1024]; // 1 MB
        let offset = vlog.append(key, &value).unwrap();
        vlog.flush_and_sync().unwrap();
        let v = vlog.read_value(offset, key.len() as u16, value.len() as u32).unwrap();
        assert_eq!(v, value);
    }

    #[test]
    fn dead_bytes_tracking() {
        let dir = tempdir().unwrap();
        let vlog = ValueLog::open(&dir.path().join("v.log")).unwrap();
        vlog.append(b"k", b"old").unwrap();
        assert!(!vlog.compaction_needed());
        // Mark old value as dead
        vlog.mark_dead(1, 3);
        // total = 16+1+3=20, dead = 20, ratio = 1.0 > 0.5 → needed
        assert!(vlog.compaction_needed());
    }
}
