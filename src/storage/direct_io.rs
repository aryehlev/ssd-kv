//! O_DIRECT file operations for bypassing the page cache.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use crate::io::aligned_buf::{AlignedBuffer, ALIGNMENT};

/// Flags for O_DIRECT on Linux.
#[cfg(target_os = "linux")]
const O_DIRECT: i32 = libc::O_DIRECT;

/// On macOS, we use F_NOCACHE instead of O_DIRECT.
#[cfg(target_os = "macos")]
const O_DIRECT: i32 = 0;

/// A file handle for direct I/O operations.
pub struct DirectFile {
    file: File,
    path: String,
}

impl DirectFile {
    /// Opens a file for direct I/O.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();

        #[cfg(target_os = "linux")]
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(O_DIRECT)
            .open(&path)?;

        #[cfg(target_os = "macos")]
        let file = {
            let f = OpenOptions::new().read(true).write(true).open(&path)?;
            // Use F_NOCACHE on macOS
            unsafe {
                libc::fcntl(std::os::unix::io::AsRawFd::as_raw_fd(&f), libc::F_NOCACHE, 1);
            }
            f
        };

        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        let file = OpenOptions::new().read(true).write(true).open(&path)?;

        Ok(Self {
            file,
            path: path_str,
        })
    }

    /// Creates a new file for direct I/O.
    pub fn create<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();

        #[cfg(target_os = "linux")]
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .custom_flags(O_DIRECT)
            .open(&path)?;

        #[cfg(target_os = "macos")]
        let file = {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)?;
            unsafe {
                libc::fcntl(std::os::unix::io::AsRawFd::as_raw_fd(&f), libc::F_NOCACHE, 1);
            }
            f
        };

        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        Ok(Self {
            file,
            path: path_str,
        })
    }

    /// Opens or creates a file for direct I/O.
    pub fn open_or_create<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();

        #[cfg(target_os = "linux")]
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .custom_flags(O_DIRECT)
            .open(&path)?;

        #[cfg(target_os = "macos")]
        let file = {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)?;
            unsafe {
                libc::fcntl(std::os::unix::io::AsRawFd::as_raw_fd(&f), libc::F_NOCACHE, 1);
            }
            f
        };

        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        Ok(Self {
            file,
            path: path_str,
        })
    }

    /// Writes an aligned buffer at the specified offset.
    /// The buffer and offset must both be aligned to ALIGNMENT.
    pub fn write_at(&self, buf: &AlignedBuffer, offset: u64) -> io::Result<usize> {
        if offset as usize % ALIGNMENT != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Offset {} is not aligned to {}", offset, ALIGNMENT),
            ));
        }

        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            let written = unsafe {
                libc::pwrite(
                    self.file.as_raw_fd(),
                    buf.as_ptr() as *const libc::c_void,
                    buf.len(),
                    offset as libc::off_t,
                )
            };
            if written < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(written as usize)
        }

        #[cfg(not(target_os = "linux"))]
        {
            use std::os::unix::io::AsRawFd;
            let written = unsafe {
                libc::pwrite(
                    self.file.as_raw_fd(),
                    buf.as_ptr() as *const libc::c_void,
                    buf.len(),
                    offset as libc::off_t,
                )
            };
            if written < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(written as usize)
        }
    }

    /// Reads into an aligned buffer at the specified offset.
    /// The buffer capacity and offset must both be aligned to ALIGNMENT.
    pub fn read_at(&self, buf: &mut AlignedBuffer, offset: u64, len: usize) -> io::Result<usize> {
        if offset as usize % ALIGNMENT != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Offset {} is not aligned to {}", offset, ALIGNMENT),
            ));
        }
        if len > buf.capacity() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Read length exceeds buffer capacity",
            ));
        }

        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let read_bytes = unsafe {
                libc::pread(
                    self.file.as_raw_fd(),
                    buf.as_mut_ptr() as *mut libc::c_void,
                    len,
                    offset as libc::off_t,
                )
            };
            if read_bytes < 0 {
                return Err(io::Error::last_os_error());
            }
            unsafe {
                buf.set_len(read_bytes as usize);
            }
            Ok(read_bytes as usize)
        }

        #[cfg(not(unix))]
        {
            // Fallback for non-Unix systems
            let mut file = &self.file;
            file.seek(SeekFrom::Start(offset))?;
            let mut temp = vec![0u8; len];
            let n = file.read(&mut temp)?;
            buf.clear();
            buf.extend_from_slice(&temp[..n]);
            Ok(n)
        }
    }

    /// Syncs the file to disk.
    pub fn sync(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    /// Syncs only the data to disk (not metadata).
    pub fn sync_data(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    /// Returns the file size.
    pub fn size(&self) -> io::Result<u64> {
        self.file.metadata().map(|m| m.len())
    }

    /// Pre-allocates space for the file.
    #[cfg(target_os = "linux")]
    pub fn preallocate(&self, size: u64) -> io::Result<()> {
        use std::os::unix::io::AsRawFd;
        let ret = unsafe {
            libc::fallocate(
                self.file.as_raw_fd(),
                0,
                0,
                size as libc::off_t,
            )
        };
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    pub fn preallocate(&self, size: u64) -> io::Result<()> {
        // Fallback: write zeros (less efficient)
        self.file.set_len(size)
    }

    /// Returns the file path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Returns a reference to the underlying file.
    pub fn inner(&self) -> &File {
        &self.file
    }

    /// Returns a mutable reference to the underlying file.
    pub fn inner_mut(&mut self) -> &mut File {
        &mut self.file
    }
}

impl std::os::unix::io::AsRawFd for DirectFile {
    fn as_raw_fd(&self) -> std::os::unix::io::RawFd {
        std::os::unix::io::AsRawFd::as_raw_fd(&self.file)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_direct_file_write_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.dat");

        let file = DirectFile::create(&path).unwrap();

        // Write aligned data
        let mut write_buf = AlignedBuffer::new(ALIGNMENT);
        write_buf.extend_from_slice(b"Hello, Direct I/O!");
        write_buf.resize(ALIGNMENT); // Pad to alignment

        let written = file.write_at(&write_buf, 0).unwrap();
        assert_eq!(written, ALIGNMENT);

        // Read it back
        let mut read_buf = AlignedBuffer::new(ALIGNMENT);
        let read = file.read_at(&mut read_buf, 0, ALIGNMENT).unwrap();
        assert_eq!(read, ALIGNMENT);
        assert_eq!(&read_buf[..18], b"Hello, Direct I/O!");
    }
}
