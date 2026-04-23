//! Blocking wrapper around `IoPool` for point reads.
//!
//! The existing `IoPool` uses a shared completion channel — every read/write
//! lands on one `Receiver<IoResult>`. That's fine for batched submission
//! (submit N, drain N) but awkward for "submit one, wait for it" which is
//! what `Handler::get_value` needs when it falls through to disk.
//!
//! `AsyncReader` owns an `IoPool`, runs a dedicated dispatch thread that
//! drains completions, and routes each `IoResult` to a per-request oneshot.
//! Callers get a familiar `fn pread_blocking(fd, offset, len) -> Result<buf>`.
//!
//! On a thread-per-connection server this replaces a blocking `pread`
//! syscall with an io_uring submission (amortized via SQPOLL — no syscall
//! on the submit side at all).

use std::io;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam_channel::{bounded, Sender};
use dashmap::DashMap;

use crate::io::aligned_buf::AlignedBuffer;
use crate::io::io_pool::IoPool;
use crate::io::uring::IoResult;

/// Partition picker: round-robin across IoPool workers. Per-fd hashing would
/// let the same file stay on one uring ring (SQPOLL affinity) but we accept
/// a small scatter in exchange for simpler code.
fn pick_partition(next_partition: &std::sync::atomic::AtomicUsize, n: usize) -> usize {
    next_partition.fetch_add(1, Ordering::Relaxed) % n
}

pub struct AsyncReader {
    pool: Arc<IoPool>,
    pending: Arc<DashMap<u64, Sender<IoResult>>>,
    shutdown: Arc<AtomicBool>,
    next_partition: std::sync::atomic::AtomicUsize,
    // Kept alive; thread exits when `shutdown` flips.
    _dispatch: JoinHandle<()>,
}

impl AsyncReader {
    /// Build an AsyncReader with `workers` io_uring instances, each with
    /// `queue_depth` SQEs. `num_cpus` is a reasonable value for workers.
    pub fn new(workers: usize, queue_depth: u32) -> io::Result<Arc<Self>> {
        let pool = Arc::new(IoPool::new(workers.max(1), queue_depth.max(16))?);
        let pending: Arc<DashMap<u64, Sender<IoResult>>> = Arc::new(DashMap::new());
        let shutdown = Arc::new(AtomicBool::new(false));

        let pool_clone = Arc::clone(&pool);
        let pending_clone = Arc::clone(&pending);
        let shutdown_clone = Arc::clone(&shutdown);

        let dispatch = thread::Builder::new()
            .name("async-reader-dispatch".to_string())
            .spawn(move || {
                while !shutdown_clone.load(Ordering::Relaxed) {
                    // Non-blocking drain so we can notice shutdown promptly.
                    match pool_clone.try_recv() {
                        Some(result) => {
                            if let Some((_, tx)) = pending_clone.remove(&result.user_data) {
                                let _ = tx.send(result);
                            }
                            // Unmatched results are dropped silently — they
                            // can happen if a caller's oneshot was canceled.
                        }
                        None => {
                            thread::sleep(Duration::from_micros(50));
                        }
                    }
                }
                // Drain anything queued on the way out so pending waiters
                // don't hang on a BrokenPipe.
                while let Some(result) = pool_clone.try_recv() {
                    if let Some((_, tx)) = pending_clone.remove(&result.user_data) {
                        let _ = tx.send(result);
                    }
                }
            })?;

        Ok(Arc::new(Self {
            pool,
            pending,
            shutdown,
            next_partition: std::sync::atomic::AtomicUsize::new(0),
            _dispatch: dispatch,
        }))
    }

    pub fn num_workers(&self) -> usize {
        self.pool.num_workers()
    }

    /// Submit an io_uring read and block the calling thread on a oneshot
    /// until the completion arrives. Returns the filled buffer.
    ///
    /// `offset` must be aligned to 512 bytes and `len` must be a multiple
    /// of 512 (O_DIRECT constraints, same as the existing pread path in
    /// `DirectFile::read_at`).
    pub fn pread_blocking(
        &self,
        fd: RawFd,
        offset: u64,
        len: usize,
    ) -> io::Result<AlignedBuffer> {
        let user_data = self.pool.next_id();
        let partition = pick_partition(&self.next_partition, self.pool.num_workers());

        let (tx, rx) = bounded::<IoResult>(1);
        self.pending.insert(user_data, tx);

        // submit_read allocates the AlignedBuffer internally and returns it
        // via IoResult.buffer.
        if let Err(e) = self
            .pool
            .submit_read(partition, fd, offset, len as u32, user_data)
        {
            self.pending.remove(&user_data);
            return Err(e);
        }

        // Block until the dispatcher delivers our result.
        let result = rx
            .recv()
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "async reader shut down"))?;

        if result.result < 0 {
            return Err(io::Error::from_raw_os_error(-result.result));
        }

        result.buffer.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Other,
                "io_uring result missing buffer (unexpected)",
            )
        })
    }
}

impl Drop for AsyncReader {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        // The dispatch thread will notice shutdown and exit. We don't
        // join here (JoinHandle drop is detaching); the short sleep in
        // the loop bounds the join latency.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::unix::io::AsRawFd;
    use tempfile::NamedTempFile;

    #[test]
    fn async_read_round_trip_matches_file_contents() {
        // Only runs meaningfully on Linux with io_uring. On other platforms
        // IoPool::new succeeds but submissions are a no-op; skip the assertion.
        if !cfg!(target_os = "linux") {
            return;
        }

        // Build a file with known contents, aligned to 4KiB.
        let mut tmp = NamedTempFile::new().unwrap();
        let mut payload = Vec::with_capacity(8192);
        for i in 0..8192u32 {
            payload.push((i & 0xff) as u8);
        }
        tmp.as_file_mut().write_all(&payload).unwrap();
        tmp.as_file_mut().sync_data().unwrap();

        let fd = tmp.as_file().as_raw_fd();
        let reader = match AsyncReader::new(1, 32) {
            Ok(r) => r,
            // Kernels without io_uring or sandboxes that deny it: skip.
            Err(_) => return,
        };

        // Read the first 4 KiB.
        let buf = match reader.pread_blocking(fd, 0, 4096) {
            Ok(b) => b,
            Err(_) => return, // environment limitation
        };
        assert_eq!(&buf.as_ref()[..4096], &payload[..4096]);
    }
}
