//! Write-Ahead Log (WAL) for fast sequential writes.
//!
//! The WAL provides:
//! - Fast sequential writes (no random I/O)
//! - Durability guarantees
//! - Crash recovery
//! - Async background flush to main storage
//!
//! Write path:
//! 1. Append to WAL (sequential, fast)
//! 2. Update in-memory index
//! 3. Return success to client
//! 4. Background: flush WAL to main storage
//!
//! This is similar to Aerospike's streaming write buffer approach.

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

/// WAL entry header (24 bytes).
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct WalEntryHeader {
    /// Magic number for validation
    pub magic: u32,
    /// Entry type (PUT, DELETE)
    pub entry_type: u8,
    /// Flags
    pub flags: u8,
    /// Key length
    pub key_len: u16,
    /// Value length
    pub value_len: u32,
    /// Generation number
    pub generation: u32,
    /// TTL (seconds, 0 = no expiry)
    pub ttl: u32,
    /// CRC32 of key + value
    pub crc: u32,
}

const WAL_MAGIC: u32 = 0x57414C21; // "WAL!"
const WAL_ENTRY_PUT: u8 = 1;
const WAL_ENTRY_DELETE: u8 = 2;
const WAL_HEADER_SIZE: usize = std::mem::size_of::<WalEntryHeader>();

impl WalEntryHeader {
    pub fn new_put(key_len: u16, value_len: u32, generation: u32, ttl: u32) -> Self {
        Self {
            magic: WAL_MAGIC,
            entry_type: WAL_ENTRY_PUT,
            flags: 0,
            key_len,
            value_len,
            generation,
            ttl,
            crc: 0, // Calculated later
        }
    }

    pub fn new_delete(key_len: u16, generation: u32) -> Self {
        Self {
            magic: WAL_MAGIC,
            entry_type: WAL_ENTRY_DELETE,
            flags: 0,
            key_len,
            value_len: 0,
            generation,
            ttl: 0,
            crc: 0,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.magic == WAL_MAGIC
    }

    pub fn is_put(&self) -> bool {
        self.entry_type == WAL_ENTRY_PUT
    }

    pub fn is_delete(&self) -> bool {
        self.entry_type == WAL_ENTRY_DELETE
    }

    pub fn to_bytes(&self) -> [u8; WAL_HEADER_SIZE] {
        unsafe { std::mem::transmute_copy(self) }
    }

    pub fn from_bytes(bytes: &[u8; WAL_HEADER_SIZE]) -> Self {
        unsafe { std::mem::transmute_copy(bytes) }
    }
}

/// WAL entry for the write queue.
pub struct WalEntry {
    pub header: WalEntryHeader,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// WAL configuration.
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// WAL directory
    pub dir: PathBuf,
    /// Maximum WAL file size (default: 64MB)
    pub max_file_size: u64,
    /// Buffer size for the internal BufWriter (default: 1MB)
    pub buffer_size: usize,
    /// Group-commit batch budget: the commit thread wakes at least this often
    /// even if no writer pokes it. Smaller = lower p99, higher syscall rate.
    pub fsync_interval: Duration,
    /// Maximum writers allowed to stack up before the commit thread wakes
    /// early and issues a sync_data. 0 means "only tick-based". Setting this
    /// to something like 256 bounds worst-case queue depth at high QPS.
    pub fsync_batch: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("./wal"),
            // Bigger WAL files mean rarer rotations — even with
            // non-blocking rotate, fewer rotations = less pending_close
            // traffic for the commit thread.
            max_file_size: 256 * 1024 * 1024, // 256MB
            buffer_size: 1024 * 1024,         // 1MB
            fsync_interval: Duration::from_micros(500),
            fsync_batch: 256,
        }
    }
}

/// WAL statistics.
#[derive(Debug, Default)]
pub struct WalStats {
    pub entries_written: AtomicU64,
    pub bytes_written: AtomicU64,
    pub syncs: AtomicU64,
    pub flushes: AtomicU64,
    pub current_file_size: AtomicU64,
}

/// Group-commit coordinator shared between writers and the commit thread.
/// Writers bump `written_pos` after staging bytes into the BufWriter and then
/// wait for `durable_pos` to catch up. The commit thread periodically flushes
/// the BufWriter, calls `sync_data()`, publishes `written_pos` as the new
/// `durable_pos`, and wakes everyone on `cond`.
struct GroupCommit {
    /// Bytes staged into the BufWriter (pre-fsync).
    written_pos: AtomicU64,
    /// Bytes durably on disk (post-fsync).
    durable_pos: AtomicU64,
    /// Count of writers blocked waiting for fsync; the commit thread wakes
    /// early when this crosses `fsync_batch`.
    waiters: AtomicU64,
    cond_lock: Mutex<()>,
    cond: Condvar,
    /// Dedicated condvar/mutex for the commit thread's wait. Writers
    /// notify this after staging bytes so the commit thread wakes in
    /// microseconds instead of polling every `fsync_interval`.
    wake_lock: Mutex<()>,
    wake_cond: Condvar,
    /// True while the commit thread is parked on `wake_cond`. Writers
    /// check this Relaxed before taking `wake_lock` — if the commit
    /// thread isn't sleeping, we skip the mutex entirely (fast path).
    commit_sleeping: AtomicBool,
    /// Optional eventfds written by the commit thread when durable_pos
    /// advances. Each reactor registers its own fd here so its io_uring
    /// loop wakes immediately on durable advance instead of waiting out
    /// its own 200µs timeout. Supports up to 8 reactors (which is more
    /// than any realistic deployment). A slot of -1 means "unused".
    wake_eventfds: [AtomicI32; 8],
}

impl GroupCommit {
    fn new(initial_pos: u64) -> Self {
        Self {
            written_pos: AtomicU64::new(initial_pos),
            durable_pos: AtomicU64::new(initial_pos),
            waiters: AtomicU64::new(0),
            cond_lock: Mutex::new(()),
            cond: Condvar::new(),
            wake_lock: Mutex::new(()),
            wake_cond: Condvar::new(),
            commit_sleeping: AtomicBool::new(false),
            wake_eventfds: [
                AtomicI32::new(-1),
                AtomicI32::new(-1),
                AtomicI32::new(-1),
                AtomicI32::new(-1),
                AtomicI32::new(-1),
                AtomicI32::new(-1),
                AtomicI32::new(-1),
                AtomicI32::new(-1),
            ],
        }
    }

    /// Notify the commit thread that new bytes were staged. Only takes
    /// the wake lock if the commit thread might actually be sleeping
    /// (indicated by `commit_sleeping`). Under high load the commit
    /// thread is rarely asleep — it's already fsyncing — so the fast
    /// path is a single Relaxed load and a branch.
    #[inline]
    fn notify_commit(&self) {
        if self.commit_sleeping.load(Ordering::Relaxed) {
            let _g = self.wake_lock.lock();
            self.wake_cond.notify_one();
        }
    }

    /// Block until `durable_pos >= target`.
    fn wait_for_durable(&self, target: u64) {
        if self.durable_pos.load(Ordering::Acquire) >= target {
            return;
        }
        self.waiters.fetch_add(1, Ordering::AcqRel);
        let mut guard = self.cond_lock.lock();
        while self.durable_pos.load(Ordering::Acquire) < target {
            self.cond.wait(&mut guard);
        }
        self.waiters.fetch_sub(1, Ordering::AcqRel);
    }

    /// Publish a new durable position and wake all waiters.
    fn publish(&self, pos: u64) {
        self.durable_pos.store(pos, Ordering::Release);
        let _g = self.cond_lock.lock();
        self.cond.notify_all();

        // Wake every reactor watching an eventfd. Each reactor has its
        // own fd so the kernel's eventfd counter isn't consumed by one
        // reactor at the expense of the others.
        let one: u64 = 1;
        for slot in &self.wake_eventfds {
            let fd = slot.load(Ordering::Relaxed);
            if fd < 0 {
                continue;
            }
            unsafe {
                let _ = libc::write(
                    fd,
                    &one as *const u64 as *const libc::c_void,
                    std::mem::size_of::<u64>(),
                );
            }
        }
    }
}

/// Closed-file metadata used by the cleanup path.
#[derive(Clone, Copy, Debug)]
struct ClosedFile {
    seq: u64,
    /// Highest generation ever appended to this file while it was current.
    /// Only meaningful for files we wrote during this process's lifetime;
    /// files carried over from a previous run have `max_gen = u32::MAX`
    /// so we never prematurely delete them.
    max_gen: u32,
}

/// Write-Ahead Log for durability and fast writes.
///
/// Writers stage their bytes into a BufWriter under a mutex, record their
/// end-position, then block on a Condvar until a dedicated commit thread
/// flushes + calls `sync_data()` on the underlying file and publishes the
/// advanced durable position. Many writes amortize one fsync — the Aerospike
/// streaming-write-buffer shape.
/// A WAL file tracked as two handles to the same inode.
///
/// Writers take `buf_writer`'s Mutex for the brief memcpy into
/// BufWriter. The commit thread uses `sync_file` (a dup'd fd) to call
/// `sync_data()` WITHOUT holding the Mutex — so fsync and new appends
/// can proceed concurrently. The kernel serializes at the inode level
/// but doesn't block writes during fsync.
///
/// Before this split, fsync was held under the BufWriter mutex, so a
/// ~500µs fsync on virtio-blk blocked every appending writer for that
/// entire duration. The fsync time is unchanged, but the writer path
/// is now freely concurrent with it.
pub(crate) struct WalFile {
    pub buf_writer: Mutex<BufWriter<File>>,
    pub sync_file: File,
}

impl WalFile {
    fn new(file: File, buffer_size: usize) -> io::Result<Self> {
        let sync_file = file.try_clone()?;
        Ok(Self {
            buf_writer: Mutex::new(BufWriter::with_capacity(buffer_size.max(4096), file)),
            sync_file,
        })
    }
}

pub struct WriteAheadLog {
    config: WalConfig,
    /// Current WAL file. `ArcSwap` lets rotate() atomically replace it
    /// without blocking writers — they load the Arc once per append,
    /// take the inner Mutex for the memcpy, and move on. The old writer
    /// goes onto `pending_close` for the commit thread to drain + sync
    /// off the hot path.
    writer: Arc<arc_swap::ArcSwap<WalFile>>,
    /// Old writers retired by rotate() that still need their final
    /// flush + sync_data + close. Drained by the commit thread.
    pending_close: Arc<Mutex<Vec<Arc<WalFile>>>>,
    /// Held during rotate() so two writers don't race to rotate.
    rotate_lock: Mutex<()>,
    /// Current file sequence number
    file_seq: AtomicU64,
    /// Current staged position in file (bytes appended, pre-fsync).
    position: AtomicU64,
    /// Highest generation appended to the current WAL file. Bumped by every
    /// append_put / append_delete; frozen into `closed_files` on rotate.
    current_max_gen: AtomicU32,
    /// Closed WAL files in seq order; used by `cleanup_up_to_gen` to
    /// delete files whose entire contents are safely on disk.
    closed_files: Mutex<Vec<ClosedFile>>,
    /// Statistics
    stats: Arc<WalStats>,
    /// Group-commit coordinator
    gc: Arc<GroupCommit>,
    /// Background commit thread
    sync_thread: Option<JoinHandle<()>>,
    /// Shutdown flag
    shutdown: Arc<AtomicBool>,
}

impl WriteAheadLog {
    /// Create a new WAL.
    pub fn new(config: WalConfig) -> io::Result<Self> {
        std::fs::create_dir_all(&config.dir)?;

        // Find latest WAL file or create new one
        let file_seq = Self::find_latest_seq(&config.dir)?;
        let file_path = Self::wal_file_path(&config.dir, file_seq);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)?;

        let position = file.metadata()?.len();

        let wal_file = WalFile::new(file, config.buffer_size)?;
        let writer = Arc::new(arc_swap::ArcSwap::from(Arc::new(wal_file)));

        // Any WAL files on disk older than this are from a prior run; we
        // can't reason about their max_gen without scanning, so mark them
        // with u32::MAX so cleanup treats them as "always newer than any
        // watermark" and leaves them alone. Recovery-path cleanup (via
        // the shutdown `cleanup(keep_files)` call) takes care of those.
        let closed = Self::find_closed_file_seqs(&config.dir, file_seq)?
            .into_iter()
            .map(|seq| ClosedFile { seq, max_gen: u32::MAX })
            .collect::<Vec<_>>();

        let mut wal = Self {
            config,
            writer,
            pending_close: Arc::new(Mutex::new(Vec::new())),
            rotate_lock: Mutex::new(()),
            file_seq: AtomicU64::new(file_seq),
            position: AtomicU64::new(position),
            current_max_gen: AtomicU32::new(0),
            closed_files: Mutex::new(closed),
            stats: Arc::new(WalStats::default()),
            gc: Arc::new(GroupCommit::new(position)),
            sync_thread: None,
            shutdown: Arc::new(AtomicBool::new(false)),
        };

        // Start background commit thread
        wal.start_sync_thread();

        Ok(wal)
    }

    /// Find the latest WAL sequence number.
    /// Return the sorted list of WAL file seqs on disk, excluding the one
    /// we just opened as current.
    fn find_closed_file_seqs(dir: &Path, current_seq: u64) -> io::Result<Vec<u64>> {
        let mut seqs = Vec::new();
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.starts_with("wal_") && name.ends_with(".log") {
                        if let Ok(seq) = name[4..name.len() - 4].parse::<u64>() {
                            if seq != current_seq {
                                seqs.push(seq);
                            }
                        }
                    }
                }
            }
        }
        seqs.sort();
        Ok(seqs)
    }

    fn find_latest_seq(dir: &Path) -> io::Result<u64> {
        let mut max_seq = 0u64;

        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.starts_with("wal_") && name.ends_with(".log") {
                        if let Ok(seq) = name[4..name.len() - 4].parse::<u64>() {
                            max_seq = max_seq.max(seq);
                        }
                    }
                }
            }
        }

        Ok(max_seq)
    }

    /// Get WAL file path for a sequence number.
    fn wal_file_path(dir: &Path, seq: u64) -> PathBuf {
        dir.join(format!("wal_{:08}.log", seq))
    }

    /// Start the background group-commit thread.
    ///
    /// Wakes every `fsync_interval` OR as soon as `fsync_batch` writers are
    /// stacked up waiting, flushes the BufWriter, issues a `sync_data`, and
    /// publishes the new durable position so all waiters release together.
    fn start_sync_thread(&mut self) {
        let stats = Arc::clone(&self.stats);
        let shutdown = Arc::clone(&self.shutdown);
        let gc = Arc::clone(&self.gc);
        let writer_handle = Arc::clone(&self.writer);
        let pending_close_handle = Arc::clone(&self.pending_close);
        let interval = self.config.fsync_interval;
        let batch = self.config.fsync_batch as u64;

        let handle = thread::Builder::new()
            .name("wal-commit".to_string())
            .spawn(move || {
                let mut last_tick = Instant::now();
                loop {
                    if shutdown.load(Ordering::Relaxed) {
                        // Final flush before exit so nothing dangles.
                        Self::drain_pending_close(&pending_close_handle);
                        Self::commit_once(&writer_handle, &gc, &stats);
                        // Also wake any stragglers blocked on durability.
                        let _g = gc.cond_lock.lock();
                        gc.cond.notify_all();
                        return;
                    }

                    let written = gc.written_pos.load(Ordering::Acquire);
                    let durable = gc.durable_pos.load(Ordering::Acquire);
                    let stacked = gc.waiters.load(Ordering::Relaxed);
                    let elapsed = last_tick.elapsed();
                    let new_work = written > durable;
                    let over_batch = batch > 0 && stacked >= batch;
                    let past_interval = elapsed >= interval;

                    // Commit if (a) batch filled or (b) we've been
                    // accumulating for `interval`. This preserves the
                    // group-commit batching window — many writes amortize
                    // one fsync — while still waking quickly.
                    if new_work && (over_batch || past_interval) {
                        Self::drain_pending_close(&pending_close_handle);
                        Self::commit_once(&writer_handle, &gc, &stats);
                        last_tick = Instant::now();
                        continue;
                    }

                    // Sleep on the wake condvar. On idle load, writers
                    // notify us the instant they stage the first byte so
                    // we don't sit out the full `interval`. Under active
                    // load, we get here only briefly between commit
                    // cycles and the timeout caps how long we can miss
                    // a notify.
                    let remaining = interval.saturating_sub(elapsed)
                        .max(Duration::from_micros(10));
                    let mut guard = gc.wake_lock.lock();
                    // Re-check under lock to avoid lost-wake.
                    let written2 = gc.written_pos.load(Ordering::Acquire);
                    let durable2 = gc.durable_pos.load(Ordering::Acquire);
                    if written2 <= durable2
                        && !(batch > 0 && gc.waiters.load(Ordering::Relaxed) >= batch)
                        && !shutdown.load(Ordering::Relaxed)
                    {
                        gc.commit_sleeping.store(true, Ordering::Release);
                        let _ = gc.wake_cond.wait_for(&mut guard, remaining);
                        gc.commit_sleeping.store(false, Ordering::Release);
                    }
                }
            })
            .expect("Failed to spawn WAL commit thread");

        self.sync_thread = Some(handle);
    }

    /// Final-flush + sync + close every writer that rotate() retired.
    /// Called by the commit thread off the hot path so writers never
    /// pay the rotation fsync cost.
    fn drain_pending_close(
        pending_close: &Arc<Mutex<Vec<Arc<WalFile>>>>,
    ) {
        let olds: Vec<_> = {
            let mut g = pending_close.lock();
            std::mem::take(&mut *g)
        };
        for old in olds {
            // Flush the BufWriter under its Mutex first, then release
            // the Mutex and fsync through the separate sync_file handle
            // — same concurrency shape as commit_once. This also keeps
            // the inner lock out of scope before we touch
            // `pending_close` on the error path (lock order vs
            // rotate()'s push site would otherwise invert).
            let flush_ok = {
                let mut w = old.buf_writer.lock();
                match w.flush() {
                    Err(e) => {
                        tracing::error!("wal: retired BufWriter flush failed: {}", e);
                        false
                    }
                    Ok(()) => true,
                }
            };

            let sync_ok = if flush_ok {
                match old.sync_file.sync_data() {
                    Err(e) => {
                        tracing::error!("wal: retired sync_data failed: {}", e);
                        false
                    }
                    Ok(()) => true,
                }
            } else {
                false
            };

            if flush_ok && sync_ok {
                // Dropping `old` when the last Arc goes closes both Files.
                continue;
            }

            // Error path: re-queue so the next tick retries. Dropping the
            // Arc here would drop the BufWriter and discard any bytes
            // still in its buffer — we'd lose data that a writer has
            // already been told is staged. ENOSPC / EIO may be transient,
            // and persistent failures just produce log noise until the
            // operator intervenes, which is strictly better than silent
            // loss.
            pending_close.lock().push(old);
        }
    }

    /// Flush BufWriter → `sync_data()` → publish the new durable position.
    ///
    /// Only the flush is done under the BufWriter's Mutex; the fsync is
    /// done on a separate `sync_file` handle (same inode, dup'd fd) so
    /// writers can keep appending while fsync is in flight. On virtio
    /// that's a ~500µs window in which the writer path used to be
    /// blocked; on NVMe the window is smaller but the same principle
    /// holds.
    fn commit_once(
        writer: &Arc<arc_swap::ArcSwap<WalFile>>,
        gc: &Arc<GroupCommit>,
        stats: &Arc<WalStats>,
    ) {
        let target = gc.written_pos.load(Ordering::Acquire);
        if target == gc.durable_pos.load(Ordering::Acquire) {
            return; // nothing new, skip the syscall
        }
        let slot = writer.load_full();

        // Stage 1: flush BufWriter's buffer to the File. Needs the
        // Mutex, but finishes in a few µs — it's just a memcpy to the
        // kernel's page cache.
        {
            let mut w = slot.buf_writer.lock();
            if let Err(e) = w.flush() {
                tracing::error!("wal: BufWriter flush failed: {}", e);
                return;
            }
        }

        // Stage 2: sync_data on the dup'd handle, outside the Mutex.
        // Writers may append more bytes concurrently — those bytes sit
        // in BufWriter's buffer or hit the File via BufWriter's
        // internal flush. Either way, `target` is the offset we promised
        // to cover and everything up to it is now in kernel's write-back
        // path, so this fsync makes it durable.
        if let Err(e) = slot.sync_file.sync_data() {
            tracing::error!("wal: sync_data failed: {}", e);
            return;
        }
        stats.syncs.fetch_add(1, Ordering::Relaxed);
        gc.publish(target);
    }

    /// Append a PUT entry to the WAL and block until durable.
    pub fn append_put(&self, key: &[u8], value: &[u8], generation: u32, ttl: u32) -> io::Result<u64> {
        let pos = self.append_put_nowait(key, value, generation, ttl)?;
        self.gc.wait_for_durable(pos);
        Ok(pos)
    }

    /// Append a PUT entry to the WAL but DON'T wait for durability.
    /// Returns the WAL position at which the record ends. The caller is
    /// responsible for checking `durable_position()` before treating the
    /// write as durable (this is how the reactor pipelines many writes
    /// per fsync).
    pub fn append_put_nowait(&self, key: &[u8], value: &[u8], generation: u32, ttl: u32) -> io::Result<u64> {
        let mut header = WalEntryHeader::new_put(
            key.len() as u16,
            value.len() as u32,
            generation,
            ttl,
        );
        header.crc = self.calculate_crc(key, value);
        self.append_entry(&header, key, Some(value))
    }

    /// Append a DELETE entry to the WAL and block until durable.
    pub fn append_delete(&self, key: &[u8], generation: u32) -> io::Result<u64> {
        let pos = self.append_delete_nowait(key, generation)?;
        self.gc.wait_for_durable(pos);
        Ok(pos)
    }

    /// Append a DELETE entry to the WAL but DON'T wait for durability.
    pub fn append_delete_nowait(&self, key: &[u8], generation: u32) -> io::Result<u64> {
        let header = WalEntryHeader::new_delete(key.len() as u16, generation);
        self.append_entry(&header, key, None)
    }

    /// Current durable position — every WAL byte up to this offset is
    /// safely on disk. Reactors poll this to decide when a pipelined
    /// write's response can be sent.
    #[inline]
    pub fn durable_position(&self) -> u64 {
        self.gc.durable_pos.load(Ordering::Acquire)
    }

    /// Register an eventfd that should be signalled every time
    /// durable_pos advances. Each reactor registers its own fd so
    /// its io_uring loop wakes immediately instead of timing out.
    /// Returns `true` if a slot was available. Up to 8 reactors
    /// may register.
    pub fn register_wake_eventfd(&self, fd: i32) -> bool {
        for slot in &self.gc.wake_eventfds {
            if slot
                .compare_exchange(-1, fd, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
        false
    }

    /// Remove a previously-registered wake eventfd. Call on reactor
    /// shutdown before closing the fd.
    pub fn unregister_wake_eventfd(&self, fd: i32) {
        for slot in &self.gc.wake_eventfds {
            if slot
                .compare_exchange(fd, -1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Block the calling thread until `durable_position() >= target`.
    /// Used by put_sync / delete_sync to keep the blocking-durability
    /// guarantee for non-reactor callers (tests, eviction thread,
    /// WAL-trim thread's final flush).
    pub fn wait_for_durable(&self, target: u64) {
        self.gc.wait_for_durable(target);
    }

    /// Append an entry to the WAL. Returns the WAL position immediately
    /// after writing. Does NOT wait for durability — that's the caller's
    /// responsibility (see `append_put` / `append_delete` for the blocking
    /// wrappers).
    ///
    /// The hot path: grab the writer mutex, memcpy bytes into the BufWriter,
    /// bump `written_pos`, release the mutex. Many concurrent calls see
    /// one fsync at the backend once the commit thread wakes.
    fn append_entry(
        &self,
        header: &WalEntryHeader,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> io::Result<u64> {
        let entry_size = WAL_HEADER_SIZE + key.len() + value.map(|v| v.len()).unwrap_or(0);

        // Check if we need to rotate. Snapshot the file_seq we're about
        // to write to so rotate() can skip if a peer already rotated —
        // a position-based check can misfire when the new file has
        // already been written past our old position's threshold.
        let current_pos = self.position.load(Ordering::Relaxed);
        if current_pos + entry_size as u64 > self.config.max_file_size {
            let observed_seq = self.file_seq.load(Ordering::Acquire);
            self.rotate(observed_seq)?;
        }

        // Bump the per-file high-water generation so cleanup_up_to_gen
        // knows when this file is safe to delete. Monotonic: only grows.
        //
        // SAFETY of the read-from-packed-struct pattern: WalEntryHeader is
        // #[repr(C, packed)] with no unaligned references escaping; we
        // copy the u32 out directly.
        let header_gen = {
            let g = header.generation;
            g
        };
        let mut prev = self.current_max_gen.load(Ordering::Relaxed);
        while header_gen > prev {
            match self.current_max_gen.compare_exchange_weak(
                prev,
                header_gen,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => prev = actual,
            }
        }

        let new_pos = {
            // Load the current writer Arc. If rotate() swaps concurrently
            // after we load but before we lock, our bytes still land in the
            // (now retired) writer we hold — recovery replays WAL files in
            // seq order, so those bytes are found exactly once. The old
            // file gets its final sync from the commit thread via
            // drain_pending_close.
            let slot = self.writer.load_full();
            let mut writer = slot.buf_writer.lock();

            writer.write_all(&header.to_bytes())?;
            writer.write_all(key)?;
            if let Some(value) = value {
                writer.write_all(value)?;
            }

            let new_pos = self.position.fetch_add(entry_size as u64, Ordering::Relaxed)
                + entry_size as u64;

            self.stats.entries_written.fetch_add(1, Ordering::Relaxed);
            self.stats.bytes_written.fetch_add(entry_size as u64, Ordering::Relaxed);
            self.stats.current_file_size.store(new_pos, Ordering::Relaxed);

            // Publish the staged position so the commit thread knows how far
            // to advance `durable_pos` on the next flush.
            self.gc.written_pos.store(new_pos, Ordering::Release);

            new_pos
        };

        // Wake the commit thread. Cheap — just a short mutex lock +
        // notify_one. Eliminates the ~200µs polling gap that the commit
        // thread used to sit in when load was low.
        self.gc.notify_commit();

        // Caller decides whether to wait for durability. The blocking
        // wrappers (append_put / append_delete) call
        // `self.gc.wait_for_durable(new_pos)` after we return.
        Ok(new_pos)
    }

    /// Calculate CRC32 of key and value.
    fn calculate_crc(&self, key: &[u8], value: &[u8]) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(key);
        hasher.update(value);
        hasher.finalize()
    }

    /// Rotate to a new WAL file. NON-BLOCKING: the old writer is atomically
    /// swapped out and queued for the commit thread to drain + fsync +
    /// close. Writers that already hold the old Arc finish their writes
    /// against it; any new appends after the swap go to the new file.
    /// No fsync happens under a lock held by the writer path.
    fn rotate(&self, observed_file_seq: u64) -> io::Result<()> {
        // Serialize rotates so two writers don't both advance file_seq
        // and open duplicate files.
        let _rotate_guard = self.rotate_lock.lock();

        // Re-check under the rotate lock using the caller's observed
        // sequence. If the sequence has advanced, a peer already rotated
        // the file we were trying to rotate — our observation is stale
        // and we should skip rather than rotate again.
        let current_file_seq = self.file_seq.load(Ordering::Acquire);
        if current_file_seq != observed_file_seq {
            return Ok(());
        }

        let old_seq = current_file_seq;
        let new_seq = self.file_seq.fetch_add(1, Ordering::Relaxed) + 1;
        let new_path = Self::wal_file_path(&self.config.dir, new_seq);

        let new_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&new_path)?;
        let new_writer = Arc::new(WalFile::new(new_file, self.config.buffer_size)?);

        // Freeze the old file's max_gen BEFORE swap so cleanup_up_to_gen
        // doesn't race. Reset current_max_gen for the new file.
        let old_max_gen = self.current_max_gen.swap(0, Ordering::AcqRel);
        self.closed_files.lock().push(ClosedFile {
            seq: old_seq,
            max_gen: old_max_gen,
        });

        // Atomic pointer swap. O(1), no fsync under any lock. After this,
        // new appends hit the new writer; in-flight appends on the old
        // writer complete normally.
        let old_writer = self.writer.swap(new_writer);

        // Position is per-file, so reset.
        self.position.store(0, Ordering::Relaxed);

        // Hand the old writer to the commit thread to drain. The commit
        // thread does the final flush + sync_data + close off the hot
        // path — our writer threads never see the cost.
        self.pending_close.lock().push(old_writer);

        self.stats.flushes.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Force a synchronous flush + sync_data of the WAL. Rarely needed by
    /// callers now that `append_entry` is itself group-commit durable;
    /// provided for shutdown paths.
    pub fn sync(&self) -> io::Result<()> {
        // Drain any pending old-file closes first so they don't get
        // abandoned on shutdown.
        Self::drain_pending_close(&self.pending_close);

        let slot = self.writer.load_full();
        {
            let mut writer = slot.buf_writer.lock();
            writer.flush()?;
        }
        slot.sync_file.sync_data()?;
        let target = self.gc.written_pos.load(Ordering::Acquire);
        self.gc.publish(target);
        self.stats.syncs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Get WAL statistics.
    pub fn stats(&self) -> &WalStats {
        &self.stats
    }

    /// Delete closed WAL files whose entire contents are durable (i.e.
    /// max_gen <= `durable_gen`). Returns the number of files removed.
    ///
    /// Called by the periodic WAL-trim thread right after a successful
    /// `handler.flush()` has fsynced the WBlocks that carried all records
    /// up to `durable_gen` into data files.
    pub fn cleanup_up_to_gen(&self, durable_gen: u32) -> io::Result<usize> {
        let mut to_delete = Vec::new();
        {
            let mut closed = self.closed_files.lock();
            closed.retain(|cf| {
                if cf.max_gen <= durable_gen {
                    to_delete.push(cf.seq);
                    false
                } else {
                    true
                }
            });
        }

        let mut removed = 0;
        for seq in to_delete {
            let path = Self::wal_file_path(&self.config.dir, seq);
            match std::fs::remove_file(&path) {
                Ok(_) => removed += 1,
                Err(e) => {
                    tracing::warn!("wal: failed to remove old file {}: {}", path.display(), e);
                }
            }
        }
        Ok(removed)
    }

    /// Replay WAL entries for recovery.
    pub fn replay<F>(&self, mut callback: F) -> io::Result<u64>
    where
        F: FnMut(WalEntryHeader, Vec<u8>, Vec<u8>) -> io::Result<()>,
    {
        let mut count = 0u64;

        // Find all WAL files
        let mut files: Vec<u64> = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&self.config.dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.starts_with("wal_") && name.ends_with(".log") {
                        if let Ok(seq) = name[4..name.len() - 4].parse::<u64>() {
                            files.push(seq);
                        }
                    }
                }
            }
        }

        files.sort();

        // Replay each file in order
        for seq in files {
            let path = Self::wal_file_path(&self.config.dir, seq);
            let mut file = File::open(&path)?;

            let mut header_buf = [0u8; WAL_HEADER_SIZE];

            loop {
                // Read header
                match file.read_exact(&mut header_buf) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(e),
                }

                let header = WalEntryHeader::from_bytes(&header_buf);

                if !header.is_valid() {
                    break; // Corrupted entry, stop replay
                }

                // Read key. A short read here (UnexpectedEof) means the
                // WAL was torn mid-entry — most commonly by a kill -9
                // between the header write and the value write. Treat
                // it as the end of valid entries; the prior iteration's
                // records are still good.
                let mut key = vec![0u8; header.key_len as usize];
                match file.read_exact(&mut key) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(e),
                }

                // Read value (if PUT). Same torn-write handling.
                let value = if header.is_put() {
                    let mut value = vec![0u8; header.value_len as usize];
                    match file.read_exact(&mut value) {
                        Ok(()) => {}
                        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                        Err(e) => return Err(e),
                    }
                    value
                } else {
                    Vec::new()
                };

                callback(header, key, value)?;
                count += 1;
            }
        }

        Ok(count)
    }

    /// Clean up old WAL files.
    pub fn cleanup(&self, keep_files: usize) -> io::Result<usize> {
        let mut files: Vec<u64> = Vec::new();

        if let Ok(entries) = std::fs::read_dir(&self.config.dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.starts_with("wal_") && name.ends_with(".log") {
                        if let Ok(seq) = name[4..name.len() - 4].parse::<u64>() {
                            files.push(seq);
                        }
                    }
                }
            }
        }

        files.sort();

        let current_seq = self.file_seq.load(Ordering::Relaxed);
        let total_files = files.len();
        let mut removed = 0;

        for seq in files {
            if seq < current_seq && total_files - removed > keep_files {
                let path = Self::wal_file_path(&self.config.dir, seq);
                if std::fs::remove_file(&path).is_ok() {
                    removed += 1;
                }
            }
        }

        Ok(removed)
    }
}

impl Drop for WriteAheadLog {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);

        // Wake the commit thread if it's parked on the wake condvar.
        {
            let _g = self.gc.wake_lock.lock();
            self.gc.wake_cond.notify_all();
        }

        // Final sync
        let _ = self.sync();

        if let Some(handle) = self.sync_thread.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_wal_basic() {
        let dir = tempdir().unwrap();
        let config = WalConfig {
            dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let wal = WriteAheadLog::new(config).unwrap();

        // Write entries
        wal.append_put(b"key1", b"value1", 1, 0).unwrap();
        wal.append_put(b"key2", b"value2", 2, 3600).unwrap();
        wal.append_delete(b"key1", 3).unwrap();

        wal.sync().unwrap();

        assert_eq!(wal.stats().entries_written.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn test_wal_replay() {
        let dir = tempdir().unwrap();
        let config = WalConfig {
            dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        // Write entries
        {
            let wal = WriteAheadLog::new(config.clone()).unwrap();
            wal.append_put(b"key1", b"value1", 1, 0).unwrap();
            wal.append_put(b"key2", b"value2", 2, 0).unwrap();
            wal.sync().unwrap();
        }

        // Replay
        {
            let wal = WriteAheadLog::new(config).unwrap();
            let mut entries = Vec::new();

            wal.replay(|header, key, value| {
                entries.push((header.generation, key, value));
                Ok(())
            }).unwrap();

            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].1, b"key1");
            assert_eq!(entries[1].1, b"key2");
        }
    }

    #[test]
    fn test_wal_header() {
        let header = WalEntryHeader::new_put(5, 10, 42, 3600);
        assert!(header.is_valid());
        assert!(header.is_put());
        assert!(!header.is_delete());

        let bytes = header.to_bytes();
        let restored = WalEntryHeader::from_bytes(&bytes);
        // Copy fields from packed struct to avoid unaligned reference issues
        let key_len = restored.key_len;
        let value_len = restored.value_len;
        let generation = restored.generation;
        assert_eq!(key_len, 5);
        assert_eq!(value_len, 10);
        assert_eq!(generation, 42);
    }

    #[test]
    fn group_commit_durability_after_append() {
        // Invariant: when append_entry returns, the bytes are durable on disk
        // (at least as far as sync_data promises). Fire many concurrent writers
        // and then read the WAL file back without calling sync(): every entry
        // should be visible.
        let dir = tempdir().unwrap();
        let config = WalConfig {
            dir: dir.path().to_path_buf(),
            fsync_interval: Duration::from_micros(200),
            fsync_batch: 16,
            ..Default::default()
        };
        let wal = Arc::new(WriteAheadLog::new(config.clone()).unwrap());

        let mut handles = Vec::new();
        for t in 0..8u32 {
            let wal = Arc::clone(&wal);
            handles.push(std::thread::spawn(move || {
                for i in 0..100u32 {
                    let key = format!("k-{}-{}", t, i);
                    let val = format!("v-{}-{}", t, i);
                    wal.append_put(key.as_bytes(), val.as_bytes(), t * 1000 + i, 0)
                        .unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // Open a fresh WAL over the same directory and replay — every single
        // entry we returned from append_put should be readable.
        drop(wal);
        let wal = WriteAheadLog::new(config).unwrap();
        let mut count = 0;
        wal.replay(|_h, _k, _v| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 8 * 100);
    }

    #[test]
    fn cleanup_up_to_gen_deletes_only_fully_durable_files() {
        // Force small max_file_size so we get multiple files after a few
        // appends, letting us test the per-file max_gen logic.
        let dir = tempdir().unwrap();
        let config = WalConfig {
            dir: dir.path().to_path_buf(),
            max_file_size: 128, // tiny so writes rotate quickly
            fsync_interval: Duration::from_micros(200),
            fsync_batch: 4,
            ..Default::default()
        };
        let wal = WriteAheadLog::new(config.clone()).unwrap();

        // Write three "batches" with increasing generations. Each batch
        // is large enough to force a rotation so we end up with multiple
        // closed files.
        for i in 1u32..=3 {
            let val = vec![b'x'; 40];
            wal.append_put(format!("k{}", i).as_bytes(), &val, i * 100, 0)
                .unwrap();
        }
        // Count files before cleanup.
        let files_before = count_wal_files(dir.path());
        assert!(files_before >= 2, "expected rotations, got {} files", files_before);

        // Trim everything with max_gen <= 100 (only the earliest file).
        let removed = wal.cleanup_up_to_gen(100).unwrap();
        assert!(removed >= 1, "expected at least one file removed");

        // Files with max_gen > 100 stay.
        let files_after = count_wal_files(dir.path());
        assert!(files_after < files_before);

        // And the WAL is still functional — replay picks up the surviving
        // entries.
        drop(wal);
        let wal = WriteAheadLog::new(config).unwrap();
        let mut count = 0;
        wal.replay(|_h, _k, _v| {
            count += 1;
            Ok(())
        })
        .unwrap();
        // The earliest entry is gone (its file was deleted), but gen 200 and
        // 300 must still be there.
        assert!(count >= 2, "expected >= 2 entries to survive, got {}", count);
    }
}

// Test helper (kept out of tests mod to avoid inherent-method name clash).
#[cfg(test)]
fn count_wal_files(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .filter(|e| {
                    e.file_name()
                        .to_str()
                        .map_or(false, |n| n.starts_with("wal_") && n.ends_with(".log"))
                })
                .count()
        })
        .unwrap_or(0)
}
