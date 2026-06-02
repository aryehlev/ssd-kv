//! SegmentManager: pure disk I/O for the SIndex engine.
//!
//! This module handles only storage — it never touches the in-memory index.
//! All index operations live in Handler (server/handler.rs).
//!
//! Write path: Handler → write_entry() → lazy ipage flush → RecordLocation.
//! Read path:  Handler → read_at(RecordLocation) → PageEntry (served from
//!             the in-memory active ipage when the page has not yet been
//!             flushed to disk).

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use parking_lot::RwLock as PRwLock;

use crate::engine::index::Index;
use crate::engine::index_entry::{RecordLocation};
use crate::storage::ipage::{IPage, LargePage, PageEntry, PAGE_SIZE, IPAGE_MAGIC, LRGP_MAGIC};
use crate::storage::segment_file::{AlignedBuffer, SegmentFile, SegmentMeta, SegmentState, SEG_HEADER_SIZE};

// ─── GC scan entry ───────────────────────────────────────────────────────────

/// One decoded entry returned by `SegmentManager::scan_segment`.
/// Used by the GC runner to decide whether to relocate.
pub struct ScannedEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub ts: u64,
    pub ttl: u32,
    pub generation: u32,
    pub loc: RecordLocation,
    pub is_deleted: bool,
}

// ─── Config ───────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct SegmentConfig {
    pub segment_size: usize,     // default 64 MiB
    pub gc_threshold: f32,       // reclaim segments below this utilisation
    pub inline_value_max: usize, // values larger → LargePage
}

impl Default for SegmentConfig {
    fn default() -> Self {
        Self { segment_size: 64 * 1024 * 1024, gc_threshold: 0.5, inline_value_max: 3800 }
    }
}

// ─── Write-staging buffer ─────────────────────────────────────────────────────
//
// Sealed ipages are kept here after being written to disk so that reads of
// recently-written data are served from RAM (O(1) lookup) rather than
// issuing a pread(). This is the paper's two-stage design: data lives in the
// staging buffer until it's cold, then only on SSD.
//
// The ring holds up to STAGE_CAPACITY entries keyed by absolute page index.
// When the ring is full the oldest entry is evicted. Reads check this before
// going to disk, then fall through to the active ipage check, then pread.

// 4096 pages × 4 KB = 16 MB of staging RAM.  Covers ~200 K entries at 50
// entries/page and is negligible compared to even a small NVMe.
const STAGE_CAPACITY: usize = 4096;

// DashMap shards staging across 16 internal buckets — concurrent reads from
// multiple reactor threads proceed with no global lock, only a per-bucket
// read-lock. The eviction ring uses a separate Mutex (only ever locked under
// write_lock.write(), so effectively uncontested) to keep the code safe.
struct WriteStaging {
    map:  DashMap<(u32, u32), IPage>,
    // Insertion-order ring for LRU eviction.
    ring: parking_lot::Mutex<std::collections::VecDeque<(u32, u32)>>,
}

impl WriteStaging {
    fn new() -> Self {
        Self {
            map:  DashMap::with_capacity(STAGE_CAPACITY),
            ring: parking_lot::Mutex::new(std::collections::VecDeque::with_capacity(STAGE_CAPACITY + 1)),
        }
    }

    // Called under exclusive write_lock — ring.lock() is uncontested here.
    fn insert(&self, file_id: u32, abs_page_idx: u32, ipage: IPage) {
        let key = (file_id, abs_page_idx);
        let mut ring = self.ring.lock();
        if self.map.len() >= STAGE_CAPACITY {
            if let Some(old) = ring.pop_front() {
                self.map.remove(&old);
            }
        }
        self.map.insert(key, ipage);
        ring.push_back(key);
    }
}

// ─── Active write state ───────────────────────────────────────────────────────

struct ActiveState {
    sf: SegmentFile,
    current_ipage: IPage,
    page_allocated: bool,
    current_seg: u32,
    current_page: u32,
}

impl ActiveState {
    fn new(sf: SegmentFile) -> Self {
        Self { sf, current_ipage: IPage::new(), page_allocated: false, current_seg: 0, current_page: 0 }
    }
}

// ─── SegmentManager ───────────────────────────────────────────────────────────

pub struct SegmentManager {
    pub data_dir: PathBuf,
    pub config: SegmentConfig,
    // Write lock: exclusive for writes/sealing, shared only for the rare
    // active-page read (most reads skip this lock via active_encoded).
    write_lock: PRwLock<ActiveState>,
    // Old (sealed) segment files. Shared reads allow concurrent pread.
    files: PRwLock<HashMap<u32, SegmentFile>>,
    // Atomic encoding of the active page: upper 32 bits = file_id,
    // lower 32 bits = abs_page_idx.  Readers do a single AtomicU64 load
    // to check membership without taking write_lock.  0xFFFF_FFFF_FFFF_FFFF
    // means "no active page".
    active_encoded: AtomicU64,
    // Recently-sealed pages (DashMap → concurrent read, no global lock).
    staging: WriteStaging,
    next_file_id: Mutex<u32>,
    next_generation: Mutex<u32>,
    // ── GC live-page tracking ────────────────────────────────────────────
    // Populated when a segment is sealed. Key = (file_id, seg_id).
    // Decremented by Handler on overwrite / delete of a live entry.
    // GC targets segments where live_pages / total_pages < gc_threshold.
    seg_total_pages: DashMap<(u32, u32), u32>,
    seg_live_pages:  DashMap<(u32, u32), AtomicI32>,
}

impl SegmentManager {
    // ─── Constructors ──────────────────────────────────────────────────────

    pub fn new(data_dir: impl AsRef<Path>) -> io::Result<Self> {
        Self::with_config(data_dir, SegmentConfig::default())
    }

    pub fn with_config(data_dir: impl AsRef<Path>, config: SegmentConfig) -> io::Result<Self> {
        std::fs::create_dir_all(data_dir.as_ref())?;

        let mut seg_files: Vec<(u32, SegmentFile)> = Vec::new();
        for entry in std::fs::read_dir(data_dir.as_ref())? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.starts_with("seg_") && name_str.ends_with(".dat") {
                let id_str = &name_str[4..name_str.len() - 4];
                if let Ok(file_id) = id_str.parse::<u32>() {
                    let sf = SegmentFile::open(entry.path(), file_id, config.segment_size)?;
                    seg_files.push((file_id, sf));
                }
            }
        }
        seg_files.sort_by_key(|(id, _)| *id);

        if seg_files.is_empty() {
            let sf = SegmentFile::create(
                data_dir.as_ref().join("seg_000000.dat"), 0, config.segment_size)?;
            sf.start_new_segment()?;
            let active = ActiveState::new(sf);
            return Ok(Self {
                data_dir: data_dir.as_ref().to_owned(),
                config,
                write_lock: PRwLock::new(active),
                files: PRwLock::new(HashMap::new()),
                active_encoded: AtomicU64::new(u64::MAX),
                staging: WriteStaging::new(),
                next_file_id: Mutex::new(1),
                next_generation: Mutex::new(1),
                seg_total_pages: DashMap::new(),
                seg_live_pages:  DashMap::new(),
            });
        }

        let max_file_id = seg_files.last().map(|(id, _)| *id).unwrap_or(0);
        let next_file_id = max_file_id + 1;

        let mut files_map: HashMap<u32, SegmentFile> = HashMap::new();
        let mut seg_files_iter = seg_files.into_iter().peekable();
        while let Some((id, sf)) = seg_files_iter.next() {
            if seg_files_iter.peek().is_some() {
                files_map.insert(id, sf);
            } else {
                // Initialize GC live-page tracking for all sealed segments
                // in old files (in `files_map`).  We use page_count from the
                // header as the initial live estimate — overwrites since last
                // seal are unknown, so we start high and let decrements bring
                // it down as new writes happen.
                let seg_total_pages: DashMap<(u32, u32), u32> = DashMap::new();
                let seg_live_pages:  DashMap<(u32, u32), AtomicI32> = DashMap::new();
                for (fid, fsf) in &files_map {
                    let seg_cnt = fsf.segment_count.load(Ordering::Relaxed);
                    for sid in 0..seg_cnt {
                        if let Ok(meta) = fsf.read_segment_header(sid) {
                            if matches!(meta.state, SegmentState::Sealed | SegmentState::GcCandidate) && meta.page_count > 0 {
                                seg_total_pages.insert((*fid, sid), meta.page_count);
                                seg_live_pages.insert((*fid, sid), AtomicI32::new(meta.page_count as i32));
                            }
                        }
                    }
                }
                let active = ActiveState::new(sf);
                return Ok(Self {
                    data_dir: data_dir.as_ref().to_owned(),
                    config,
                    write_lock: PRwLock::new(active),
                    files: PRwLock::new(files_map),
                    active_encoded: AtomicU64::new(u64::MAX),
                    staging: WriteStaging::new(),
                    next_file_id: Mutex::new(next_file_id),
                    next_generation: Mutex::new(1),
                    seg_total_pages,
                    seg_live_pages,
                });
            }
        }
        unreachable!()
    }

    // ─── Core disk write API ──────────────────────────────────────────────

    /// Write one entry (or tombstone) to disk. Returns its location.
    /// The caller is responsible for updating the in-memory index.
    pub fn write_entry(
        &self, key: &[u8], value: &[u8], ts: u64, ttl: u32, generation: u32, is_deleted: bool,
    ) -> io::Result<RecordLocation> {
        if !is_deleted && value.len() > self.config.inline_value_max {
            self.write_large(key, value, ts, ttl, generation)
        } else {
            self.write_ipage(key, value, ts, ttl, generation, is_deleted)
        }
    }

    // ─── Core disk read API ───────────────────────────────────────────────

    /// Check staging + active page only — no disk I/O. Returns `None` if the
    /// page is not in RAM and the caller must issue an async pread.
    pub fn try_read_staged(&self, loc: RecordLocation) -> Option<PageEntry> {
        if loc.is_large() { return None; }

        let enc = self.active_encoded.load(Ordering::Acquire);
        let afile = (enc >> 32) as u32;
        let apage = (enc & 0xFFFF_FFFF) as u32;
        if loc.file_id == afile && loc.ipage_idx == apage {
            let st = self.write_lock.read();
            if st.page_allocated {
                let abs = st.sf.seg_page_to_abs(st.current_seg, st.current_page);
                if loc.ipage_idx == abs {
                    return st.current_ipage.read_entry(loc.slot_idx);
                }
            }
        }

        self.staging.map.get(&(loc.file_id, loc.ipage_idx))
            .and_then(|ipage| ipage.read_entry(loc.slot_idx))
    }

    /// Return `(fd, byte_offset, byte_len)` for an async pread of this record.
    /// The `RawFd` is valid for the lifetime of this `SegmentManager`.
    pub fn disk_read_coords(&self, loc: RecordLocation) -> io::Result<(std::os::unix::io::RawFd, u64, usize)> {
        let page_count = if loc.is_large() { loc.span as usize } else { 1 };
        let size = page_count * PAGE_SIZE;

        let enc = self.active_encoded.load(Ordering::Acquire);
        let afile = (enc >> 32) as u32;
        if loc.file_id == afile {
            let st = self.write_lock.read();
            if loc.file_id == st.sf.file_id {
                let (seg_id, page_idx) = st.sf.abs_to_seg_page(loc.ipage_idx);
                let offset = st.sf.page_offset(seg_id, page_idx);
                return Ok((st.sf.as_raw_fd(), offset, size));
            }
        }

        let files = self.files.read();
        let sf = files.get(&loc.file_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))?;
        let (seg_id, page_idx) = sf.abs_to_seg_page(loc.ipage_idx);
        let offset = sf.page_offset(seg_id, page_idx);
        Ok((sf.as_raw_fd(), offset, size))
    }

    /// Read a previously-written entry from disk (or from the active in-memory
    /// ipage when the entry has not yet been flushed).
    pub fn read_at(&self, loc: RecordLocation) -> io::Result<PageEntry> {
        if !loc.is_large() {
            // Fast path 1: atomic active-page check — one cache-line load, no lock.
            // active_encoded = (file_id << 32) | abs_page_idx.
            let enc = self.active_encoded.load(Ordering::Acquire);
            let afile = (enc >> 32) as u32;
            let apage = (enc & 0xFFFF_FFFF) as u32;
            if loc.file_id == afile && loc.ipage_idx == apage {
                // Match: take shared lock to read the actual ipage bytes.
                let st = self.write_lock.read();
                if st.page_allocated {
                    let abs = st.sf.seg_page_to_abs(st.current_seg, st.current_page);
                    if loc.ipage_idx == abs {
                        return st.current_ipage
                            .read_entry(loc.slot_idx)
                            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "slot missing"));
                    }
                }
                // Page was sealed between the atomic load and the lock — fall
                // through to staging below.
            }

            // Fast path 2: staging DashMap — no global lock, O(1), concurrent.
            if let Some(ipage) = self.staging.map.get(&(loc.file_id, loc.ipage_idx)) {
                return ipage.read_entry(loc.slot_idx)
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "slot missing"));
            }
        }

        // Slow path: pread from disk.
        // Try the active segment file first (takes read lock; safe even during
        // concurrent writes because write_lock.read() blocks until the write
        // completes and active_encoded is updated).
        {
            let st = self.write_lock.read();
            if loc.file_id == st.sf.file_id {
                return self.read_from_file(&st.sf, loc);
            }
        }

        // Not the active file — must be an older sealed file.
        let files = self.files.read();
        let sf = files.get(&loc.file_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))?;
        self.read_from_file(sf, loc)
    }

    // ─── Internal: seal the active ipage to disk and stage it in RAM ─────

    // Called with write_lock held exclusively. Writes the current ipage to
    // disk and stages a copy in the DashMap so subsequent reads of the same
    // page hit RAM instead of disk.
    fn seal_active_page(
        st: &mut ActiveState,
        staging: &WriteStaging,
        active_encoded: &AtomicU64,
    ) -> io::Result<()> {
        if !st.page_allocated { return Ok(()); }
        st.current_ipage.write_checksum();
        let abs = st.sf.seg_page_to_abs(st.current_seg, st.current_page);
        st.sf.write_ipage_at(st.current_seg, st.current_page, &st.current_ipage)?;
        staging.insert(st.sf.file_id, abs, st.current_ipage.clone());
        // Clear active-page atomic so readers don't spin into the lock path.
        active_encoded.store(u64::MAX, Ordering::Release);
        Ok(())
    }

    // ─── Flush ────────────────────────────────────────────────────────────

    pub fn flush(&self) -> io::Result<()> {
        let mut st = self.write_lock.write();
        Self::seal_active_page(&mut st, &self.staging, &self.active_encoded)?;
        st.sf.fdatasync()
    }

    // ─── GC live-page tracking API ───────────────────────────────────────

    /// Called by Handler when a live entry is overwritten or deleted.
    /// Decrements the live-page count for the segment that owned the old entry.
    /// The active segment is never in the tracking maps, so this is a no-op
    /// for entries still in the unsent active ipage.
    pub fn decrement_live(&self, file_id: u32, ipage_idx: u32, span: u32) {
        let dpps = ((self.config.segment_size - SEG_HEADER_SIZE) / PAGE_SIZE) as u32;
        let seg_id = ipage_idx / dpps;
        if let Some(e) = self.seg_live_pages.get(&(file_id, seg_id)) {
            e.value().fetch_sub(span as i32, Ordering::Relaxed);
        }
    }

    /// Return sealed segments whose live ratio is below `threshold`, ordered
    /// from most-garbage to least-garbage (lowest utilisation first).
    pub fn gc_sealed_segments(&self, threshold: f32) -> Vec<(u32, u32, f32)> {
        let mut out = Vec::new();
        for e in self.seg_total_pages.iter() {
            let (file_id, seg_id) = *e.key();
            let total = *e.value();
            if total == 0 { continue; }
            let live = self.seg_live_pages.get(&(file_id, seg_id))
                .map(|v| v.value().load(Ordering::Relaxed).max(0) as u32)
                .unwrap_or(total);
            let util = live as f32 / total as f32;
            if util < threshold {
                out.push((file_id, seg_id, util));
            }
        }
        out.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal));
        out
    }

    /// Scan every page in `(file_id, seg_id)` and return decoded entries.
    /// Holds the segment file lock only briefly per page (not across the pread)
    /// so foreground writes are not stalled during long scans.
    pub fn scan_segment(&self, file_id: u32, seg_id: u32) -> io::Result<Vec<ScannedEntry>> {
        let dpps = ((self.config.segment_size - SEG_HEADER_SIZE) / PAGE_SIZE) as u32;
        let active_id = self.write_lock.read().sf.file_id;
        let mut entries = Vec::new();
        let mut page_idx = 0u32;

        while page_idx < dpps {
            // Grab fd + offset briefly, then release the lock before pread.
            let (fd, offset) = if file_id == active_id {
                let st = self.write_lock.read();
                (st.sf.as_raw_fd(), st.sf.page_offset(seg_id, page_idx))
            } else {
                let files = self.files.read();
                let sf = files.get(&file_id)
                    .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))?;
                (sf.as_raw_fd(), sf.page_offset(seg_id, page_idx))
            };

            let mut buf = AlignedBuffer::new(PAGE_SIZE);
            let n = unsafe {
                libc::pread(fd, buf.as_mut_slice().as_mut_ptr() as *mut libc::c_void,
                            PAGE_SIZE, offset as libc::off_t)
            };
            if n <= 0 { break; }

            let magic = u32::from_le_bytes(buf.as_slice()[0..4].try_into().unwrap_or([0;4]));

            if magic == LRGP_MAGIC {
                let span = u32::from_le_bytes(buf.as_slice()[4..8].try_into().unwrap_or([0;4]));
                if span == 0 { break; }
                // Read full large-page span.
                let (fd2, off2) = if file_id == active_id {
                    let st = self.write_lock.read();
                    (st.sf.as_raw_fd(), st.sf.page_offset(seg_id, page_idx))
                } else {
                    let files = self.files.read();
                    let sf = files.get(&file_id)
                        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))?;
                    (sf.as_raw_fd(), sf.page_offset(seg_id, page_idx))
                };
                let size = span as usize * PAGE_SIZE;
                let mut fbuf = AlignedBuffer::new(size);
                let fn2 = unsafe {
                    libc::pread(fd2, fbuf.as_mut_slice().as_mut_ptr() as *mut libc::c_void,
                                size, off2 as libc::off_t)
                };
                if fn2 > 0 {
                    if let Ok(large) = LargePage::decode(fbuf.as_slice()) {
                        let abs = seg_id * dpps + page_idx;
                        let pe = large.into_entry();
                        entries.push(ScannedEntry {
                            key: pe.key, value: pe.value,
                            ts: pe.ts, ttl: pe.ttl, generation: pe.generation,
                            loc: RecordLocation::large(file_id, abs, span as u16),
                            is_deleted: pe.is_deleted,
                        });
                    }
                }
                page_idx += span;
            } else if magic == IPAGE_MAGIC {
                if let Ok(ipage) = IPage::from_bytes(buf.as_slice()) {
                    let abs = seg_id * dpps + page_idx;
                    for slot in 0..ipage.entry_count() {
                        if let Some(pe) = ipage.read_entry(slot) {
                            entries.push(ScannedEntry {
                                key: pe.key, value: pe.value,
                                ts: pe.ts, ttl: pe.ttl, generation: pe.generation,
                                loc: RecordLocation::ipage(file_id, abs, slot),
                                is_deleted: pe.is_deleted,
                            });
                        }
                    }
                }
                page_idx += 1;
            } else {
                break;
            }
        }
        Ok(entries)
    }

    /// Mark a segment as reclaimed after GC has relocated all its live entries.
    /// Removes it from the GC tracking maps and updates the on-disk header.
    pub fn mark_segment_reclaimed(&self, file_id: u32, seg_id: u32) -> io::Result<()> {
        self.seg_total_pages.remove(&(file_id, seg_id));
        self.seg_live_pages.remove(&(file_id, seg_id));
        let meta = SegmentMeta {
            file_id, segment_id: seg_id,
            page_count: 0, live_pages: 0,
            state: SegmentState::Reclaimed,
        };
        let active_id = self.write_lock.read().sf.file_id;
        if file_id == active_id {
            self.write_lock.read().sf.write_segment_header(&meta)
        } else {
            let files = self.files.read();
            if let Some(sf) = files.get(&file_id) {
                sf.write_segment_header(&meta)
            } else {
                Ok(())
            }
        }
    }

    /// If all tracked segments in an old file have been reclaimed, delete the
    /// file from disk and remove it from `self.files`.  Never touches the
    /// active file.  Returns true if the file was deleted.
    pub fn try_delete_file_if_all_reclaimed(&self, file_id: u32) -> io::Result<bool> {
        // Never delete the active file.
        if file_id == self.write_lock.read().sf.file_id { return Ok(false); }
        // Any tracked segments still alive?
        let has_live = self.seg_total_pages.iter().any(|e| e.key().0 == file_id);
        if has_live { return Ok(false); }
        let path = self.files.read().get(&file_id).map(|sf| sf.path.clone());
        if let Some(p) = path {
            self.files.write().remove(&file_id);
            let _ = std::fs::remove_file(&p);
            return Ok(true);
        }
        Ok(false)
    }

    // ─── Generation counter ───────────────────────────────────────────────

    pub fn bump_generation_past(&self, seen: u32) {
        let mut g = self.next_generation.lock().unwrap();
        if *g <= seen { *g = seen + 1; }
    }

    pub fn next_generation(&self) -> u32 {
        let mut g = self.next_generation.lock().unwrap();
        let v = *g;
        *g += 1;
        v
    }

    // ─── File-manager compatibility API ───────────────────────────────────

    pub fn file_count(&self) -> usize {
        self.files.write().len() + 1
    }

    pub fn create_file(&self) -> io::Result<u32> {
        let file_id = {
            let mut id = self.next_file_id.lock().unwrap();
            let v = *id;
            *id += 1;
            v
        };
        let name = format!("seg_{:06}.dat", file_id);
        let sf = SegmentFile::create(self.data_dir.join(&name), file_id, self.config.segment_size)?;
        sf.start_new_segment()?;
        self.files.write().insert(file_id, sf);
        Ok(file_id)
    }

    // ─── Recovery: scan segment files → rebuild index ─────────────────────

    /// Scan all segment files on disk and populate `index` with live entries.
    pub fn recover_from_segments(&self, index: &Index) -> io::Result<()> {
        let active_id = self.write_lock.write().sf.file_id;
        let mut file_ids: Vec<u32> = self.files.write().keys().copied().collect();
        file_ids.push(active_id);
        file_ids.sort();

        for file_id in file_ids {
            self.recover_file(file_id, index)?;
        }
        Ok(())
    }

    fn recover_file(&self, file_id: u32, index: &Index) -> io::Result<()> {
        let active_id = self.write_lock.write().sf.file_id;

        let seg_count = if file_id == active_id {
            self.write_lock.write().sf.segment_count.load(std::sync::atomic::Ordering::Relaxed)
        } else {
            self.files.write()
                .get(&file_id)
                .map(|sf| sf.segment_count.load(std::sync::atomic::Ordering::Relaxed))
                .unwrap_or(0)
        };

        for seg_id in 0..seg_count {
            let meta_result = if file_id == active_id {
                self.write_lock.write().sf.read_segment_header(seg_id)
            } else {
                self.files.write()
                    .get(&file_id)
                    .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, ""))?
                    .read_segment_header(seg_id)
            };

            let meta = match meta_result {
                Ok(m) => m,
                Err(_) => continue,
            };
            if meta.state == SegmentState::Reclaimed { continue; }

            let cap = if file_id == active_id {
                self.write_lock.write().sf.data_pages_per_segment()
            } else {
                self.files.write()
                    .get(&file_id)
                    .map(|sf| sf.data_pages_per_segment())
                    .unwrap_or(0)
            };

            let mut page_idx = 0u32;
            while page_idx < meta.page_count.max(cap) {
                let raw = self.read_raw_page(file_id, active_id, seg_id, page_idx, 1);
                let raw = match raw {
                    Ok(r) => r,
                    Err(_) => break,
                };

                let magic = u32::from_le_bytes(raw[0..4].try_into().unwrap_or([0; 4]));
                if magic == LRGP_MAGIC {
                    let span = u32::from_le_bytes(raw[4..8].try_into().unwrap_or([0; 4]));
                    if span == 0 { break; }
                    if let Ok(data) = self.read_raw_page(file_id, active_id, seg_id, page_idx, span) {
                        if let Ok(large) = LargePage::decode(&data) {
                            let key_bytes = large.key.clone();
                            let val_len = large.value.len() as u32;
                            let gen = large.generation;
                            let entry = large.into_entry();
                            if !entry.is_deleted {
                                let abs = self.seg_page_to_abs(file_id, active_id, seg_id, page_idx);
                                let loc = RecordLocation::large(file_id, abs, span as u16);
                                index.insert(&key_bytes, loc, gen, val_len);
                            }
                        }
                    }
                    page_idx += span;
                } else if magic == IPAGE_MAGIC {
                    if let Ok(page) = IPage::from_bytes(&raw) {
                        let abs = self.seg_page_to_abs(file_id, active_id, seg_id, page_idx);
                        let count = page.entry_count();
                        for slot in 0..count {
                            if let Some(pe) = page.read_entry(slot) {
                                if !pe.is_deleted {
                                    let loc = RecordLocation::ipage(file_id, abs, slot);
                                    index.insert(&pe.key, loc, pe.generation, pe.value.len() as u32);
                                }
                            }
                        }
                    }
                    page_idx += 1;
                } else {
                    break;
                }
            }

            // After scanning the active segment, restore the write position so
            // WAL replay appends AFTER existing pages rather than overwriting them.
            // Without this, WAL replay starts at page 0 and corrupts slot
            // assignments that the index already maps to from the scan above.
            if file_id == active_id {
                let mut st = self.write_lock.write();
                st.sf.active_seg_id.store(seg_id, std::sync::atomic::Ordering::Release);
                st.sf.active_page_offset.store(page_idx, std::sync::atomic::Ordering::Release);
            }
        }
        Ok(())
    }

    fn read_raw_page(&self, file_id: u32, active_id: u32, seg_id: u32, page_idx: u32, span: u32) -> io::Result<Vec<u8>> {
        if file_id == active_id {
            self.write_lock.write().sf.read_pages(seg_id, page_idx, span)
        } else {
            self.files.write()
                .get(&file_id)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, ""))?
                .read_pages(seg_id, page_idx, span)
        }
    }

    fn seg_page_to_abs(&self, file_id: u32, active_id: u32, seg_id: u32, page_idx: u32) -> u32 {
        if file_id == active_id {
            self.write_lock.write().sf.seg_page_to_abs(seg_id, page_idx)
        } else {
            self.files.write()
                .get(&file_id)
                .map(|sf| sf.seg_page_to_abs(seg_id, page_idx))
                .unwrap_or(0)
        }
    }

    // ─── Private write helpers ────────────────────────────────────────────

    fn write_ipage(
        &self, key: &[u8], value: &[u8], ts: u64, ttl: u32, generation: u32, is_deleted: bool,
    ) -> io::Result<RecordLocation> {
        let mut st = self.write_lock.write();

        if st.page_allocated && !st.current_ipage.fits(key.len(), value.len()) {
            Self::seal_active_page(&mut st, &self.staging, &self.active_encoded)?;
            st.current_ipage = IPage::new();
            st.page_allocated = false;
        }

        if !st.page_allocated {
            if let Some((fid, sid, pages)) = Self::alloc_page_slot_inner(&mut st, &self.staging, &self.active_encoded)? {
                // A segment was sealed to make room; register it for GC tracking.
                self.seg_total_pages.insert((fid, sid), pages);
                self.seg_live_pages.insert((fid, sid), AtomicI32::new(pages as i32));
            }
            let (seg_id, page_idx) = (st.current_seg, st.current_page);
            st.current_seg = seg_id;
            st.current_page = page_idx;
            st.page_allocated = true;
        }

        let slot_idx = st.current_ipage
            .try_append(key, value, generation, ts, ttl, is_deleted)
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "ipage slot full"))?;

        let abs = st.sf.seg_page_to_abs(st.current_seg, st.current_page);
        let file_id = st.sf.file_id;
        // Publish active-page so readers can check without taking write_lock.
        self.active_encoded.store(((file_id as u64) << 32) | (abs as u64), Ordering::Release);
        Ok(RecordLocation::ipage(file_id, abs, slot_idx))
    }

    fn write_large(
        &self, key: &[u8], value: &[u8], ts: u64, ttl: u32, generation: u32,
    ) -> io::Result<RecordLocation> {
        let span = LargePage::pages_needed(key.len(), value.len());
        let large = LargePage { span: span as u32, flags: 0, key: key.to_vec(),
            value: value.to_vec(), ts, ttl, generation };
        let encoded = large.encode();

        let mut st = self.write_lock.write();
        if let Some((fid, sid, pages)) = Self::ensure_seg_capacity(&mut st, &self.staging, &self.active_encoded, span as u32)? {
            self.seg_total_pages.insert((fid, sid), pages);
            self.seg_live_pages.insert((fid, sid), AtomicI32::new(pages as i32));
        }

        let seg_id = st.sf.active_seg_id();
        let page_idx = st.sf.advance_page_offset(span as u32);
        st.sf.write_pages_at(seg_id, page_idx, &encoded)?;

        let abs = st.sf.seg_page_to_abs(seg_id, page_idx);
        Ok(RecordLocation::large(st.sf.file_id, abs, span))
    }

    // Returns Option<(sealed_file_id, sealed_seg_id, sealed_page_count)> when a
    // segment was sealed so the caller can update GC tracking without &self.
    fn alloc_page_slot_inner(
        st: &mut ActiveState,
        sg: &WriteStaging,
        active_encoded: &AtomicU64,
    ) -> io::Result<Option<(u32, u32, u32)>> {
        let sealed = Self::ensure_seg_capacity(st, sg, active_encoded, 1)?;
        let seg_id = st.sf.active_seg_id();
        let page_idx = st.sf.advance_page_offset(1);
        st.current_seg  = seg_id;
        st.current_page = page_idx;
        Ok(sealed)
    }

    // Returns Some((file_id, seg_id, page_count)) when a segment was sealed so
    // callers can update the GC tracking maps without needing &self inside here.
    fn ensure_seg_capacity(
        st: &mut ActiveState,
        sg: &WriteStaging,
        active_encoded: &AtomicU64,
        pages_needed: u32,
    ) -> io::Result<Option<(u32, u32, u32)>> {
        if st.sf.is_active_segment_full(pages_needed) {
            let seg_id = st.sf.active_seg_id();
            let used  = st.sf.active_pages_used();
            Self::seal_active_page(st, sg, active_encoded)?;
            let meta = SegmentMeta {
                file_id: st.sf.file_id, segment_id: seg_id,
                page_count: used, live_pages: used, state: SegmentState::Sealed,
            };
            st.sf.write_segment_header(&meta)?;
            st.sf.start_new_segment()?;
            st.current_ipage = IPage::new();
            st.page_allocated = false;
            return Ok(Some((st.sf.file_id, seg_id, used)));
        }
        Ok(None)
    }

    // ─── Private read helper ──────────────────────────────────────────────

    fn read_from_file(&self, sf: &SegmentFile, loc: RecordLocation) -> io::Result<PageEntry> {
        let (seg_id, page_idx) = sf.abs_to_seg_page(loc.ipage_idx);
        if loc.is_large() {
            let data = sf.read_pages(seg_id, page_idx, loc.span as u32)?;
            let large = LargePage::decode(&data)?;
            Ok(large.into_entry())
        } else {
            let page = sf.read_ipage(seg_id, page_idx)?;
            page.read_entry(loc.slot_idx)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "slot missing"))
        }
    }
}

pub fn now_micros() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_micros() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_write_read_roundtrip() {
        let dir = tempdir().unwrap();
        let sm = SegmentManager::new(dir.path()).unwrap();
        let index = Index::new();

        let ts = now_micros();
        let loc = sm.write_entry(b"hello", b"world", ts, 0, 1, false).unwrap();
        let pe = sm.read_at(loc).unwrap();
        assert_eq!(pe.value, b"world");
    }

    #[test]
    fn test_write_read_with_ttl() {
        let dir = tempdir().unwrap();
        let sm = SegmentManager::new(dir.path()).unwrap();

        let ts = now_micros();
        let loc = sm.write_entry(b"key", b"val", ts, 3600, 1, false).unwrap();
        let pe = sm.read_at(loc).unwrap();
        assert_eq!(pe.ttl, 3600);
    }
}
