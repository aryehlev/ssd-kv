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
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::engine::index::Index;
use crate::engine::index_entry::{RecordLocation};
use crate::storage::ipage::{IPage, LargePage, PageEntry, PAGE_SIZE, IPAGE_MAGIC, LRGP_MAGIC};
use crate::storage::segment_file::{SegmentFile, SegmentMeta, SegmentState};

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
    write_lock: Mutex<ActiveState>,
    files: Mutex<HashMap<u32, SegmentFile>>,
    next_file_id: Mutex<u32>,
    next_generation: Mutex<u32>,
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
                write_lock: Mutex::new(active),
                files: Mutex::new(HashMap::new()),
                next_file_id: Mutex::new(1),
                next_generation: Mutex::new(1),
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
                let active = ActiveState::new(sf);
                return Ok(Self {
                    data_dir: data_dir.as_ref().to_owned(),
                    config,
                    write_lock: Mutex::new(active),
                    files: Mutex::new(files_map),
                    next_file_id: Mutex::new(next_file_id),
                    next_generation: Mutex::new(1),
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

    /// Read a previously-written entry from disk (or from the active in-memory
    /// ipage when the entry has not yet been flushed).
    pub fn read_at(&self, loc: RecordLocation) -> io::Result<PageEntry> {
        // Fast path: if the location points to the active in-memory ipage,
        // serve it directly without a disk read. This is always correct
        // because write_ipage() only returns a RecordLocation after appending
        // the slot to current_ipage — the slot is live in RAM until the page
        // seals and is written to disk by flush() / ensure_seg_capacity().
        {
            let st = self.write_lock.lock().unwrap();
            if st.page_allocated
                && loc.file_id == st.sf.file_id
                && !loc.is_large()
            {
                let abs = st.sf.seg_page_to_abs(st.current_seg, st.current_page);
                if loc.ipage_idx == abs {
                    return st.current_ipage
                        .read_entry(loc.slot_idx)
                        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "slot missing"));
                }
            }
            // Check if the location is in the active segment file so we can
            // read from it while still holding the lock.
            if loc.file_id == st.sf.file_id {
                return self.read_from_file(&st.sf, loc);
            }
            // Drop the lock before the (potentially slow) disk read below.
        }
        let files = self.files.lock().unwrap();
        let sf = files.get(&loc.file_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))?;
        self.read_from_file(sf, loc)
    }

    // ─── Flush ────────────────────────────────────────────────────────────

    pub fn flush(&self) -> io::Result<()> {
        let mut st = self.write_lock.lock().unwrap();
        // Seal the active in-memory ipage to disk before calling fdatasync so
        // that any entries appended since the last page-seal are durable.
        // Without this step, entries in the current (partially-filled) ipage
        // would survive only in RAM and be lost on crash.
        if st.page_allocated {
            st.current_ipage.write_checksum();
            st.sf.write_ipage_at(st.current_seg, st.current_page, &st.current_ipage)?;
        }
        st.sf.fdatasync()
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
        self.files.lock().unwrap().len() + 1
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
        self.files.lock().unwrap().insert(file_id, sf);
        Ok(file_id)
    }

    // ─── Recovery: scan segment files → rebuild index ─────────────────────

    /// Scan all segment files on disk and populate `index` with live entries.
    pub fn recover_from_segments(&self, index: &Index) -> io::Result<()> {
        let active_id = self.write_lock.lock().unwrap().sf.file_id;
        let mut file_ids: Vec<u32> = self.files.lock().unwrap().keys().copied().collect();
        file_ids.push(active_id);
        file_ids.sort();

        for file_id in file_ids {
            self.recover_file(file_id, index)?;
        }
        Ok(())
    }

    fn recover_file(&self, file_id: u32, index: &Index) -> io::Result<()> {
        let active_id = self.write_lock.lock().unwrap().sf.file_id;

        let seg_count = if file_id == active_id {
            self.write_lock.lock().unwrap().sf.segment_count.load(std::sync::atomic::Ordering::Relaxed)
        } else {
            self.files.lock().unwrap()
                .get(&file_id)
                .map(|sf| sf.segment_count.load(std::sync::atomic::Ordering::Relaxed))
                .unwrap_or(0)
        };

        for seg_id in 0..seg_count {
            let meta_result = if file_id == active_id {
                self.write_lock.lock().unwrap().sf.read_segment_header(seg_id)
            } else {
                self.files.lock().unwrap()
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
                self.write_lock.lock().unwrap().sf.data_pages_per_segment()
            } else {
                self.files.lock().unwrap()
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
        }
        Ok(())
    }

    fn read_raw_page(&self, file_id: u32, active_id: u32, seg_id: u32, page_idx: u32, span: u32) -> io::Result<Vec<u8>> {
        if file_id == active_id {
            self.write_lock.lock().unwrap().sf.read_pages(seg_id, page_idx, span)
        } else {
            self.files.lock().unwrap()
                .get(&file_id)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, ""))?
                .read_pages(seg_id, page_idx, span)
        }
    }

    fn seg_page_to_abs(&self, file_id: u32, active_id: u32, seg_id: u32, page_idx: u32) -> u32 {
        if file_id == active_id {
            self.write_lock.lock().unwrap().sf.seg_page_to_abs(seg_id, page_idx)
        } else {
            self.files.lock().unwrap()
                .get(&file_id)
                .map(|sf| sf.seg_page_to_abs(seg_id, page_idx))
                .unwrap_or(0)
        }
    }

    // ─── Private write helpers ────────────────────────────────────────────

    fn write_ipage(
        &self, key: &[u8], value: &[u8], ts: u64, ttl: u32, generation: u32, is_deleted: bool,
    ) -> io::Result<RecordLocation> {
        let mut st = self.write_lock.lock().unwrap();

        if st.page_allocated && !st.current_ipage.fits(key.len(), value.len()) {
            // Seal the outgoing page to disk before abandoning it. The page
            // will not be written again (a new one is allocated below), so
            // this is the only opportunity to persist it.
            st.current_ipage.write_checksum();
            st.sf.write_ipage_at(st.current_seg, st.current_page, &st.current_ipage)?;
            st.current_ipage = IPage::new();
            st.page_allocated = false;
        }

        if !st.page_allocated {
            let (seg_id, page_idx) = Self::alloc_page_slot(&mut st)?;
            st.current_seg = seg_id;
            st.current_page = page_idx;
            st.page_allocated = true;
        }

        let slot_idx = st.current_ipage
            .try_append(key, value, generation, ts, ttl, is_deleted)
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "ipage slot full"))?;

        // Do NOT write to disk here. The page accumulates slots in memory and
        // is flushed lazily: when it seals (page full or segment rolls over)
        // or when flush() is called. Reads of not-yet-flushed slots are served
        // directly from the in-memory page in read_at().
        let abs = st.sf.seg_page_to_abs(st.current_seg, st.current_page);
        let file_id = st.sf.file_id;
        Ok(RecordLocation::ipage(file_id, abs, slot_idx))
    }

    fn write_large(
        &self, key: &[u8], value: &[u8], ts: u64, ttl: u32, generation: u32,
    ) -> io::Result<RecordLocation> {
        let span = LargePage::pages_needed(key.len(), value.len());
        let large = LargePage { span: span as u32, flags: 0, key: key.to_vec(),
            value: value.to_vec(), ts, ttl, generation };
        let encoded = large.encode();

        let mut st = self.write_lock.lock().unwrap();
        Self::ensure_seg_capacity(&mut st, span as u32)?;

        let seg_id = st.sf.active_seg_id();
        let page_idx = st.sf.advance_page_offset(span as u32);
        st.sf.write_pages_at(seg_id, page_idx, &encoded)?;

        let abs = st.sf.seg_page_to_abs(seg_id, page_idx);
        Ok(RecordLocation::large(st.sf.file_id, abs, span))
    }

    fn alloc_page_slot(st: &mut ActiveState) -> io::Result<(u32, u32)> {
        Self::ensure_seg_capacity(st, 1)?;
        let seg_id = st.sf.active_seg_id();
        let page_idx = st.sf.advance_page_offset(1);
        Ok((seg_id, page_idx))
    }

    fn ensure_seg_capacity(st: &mut ActiveState, pages_needed: u32) -> io::Result<()> {
        if st.sf.is_active_segment_full(pages_needed) {
            // Seal the active in-memory ipage to disk before starting a new
            // segment. Once start_new_segment() runs, the page can no longer
            // be written at its slot (the segment is sealed), so we must
            // persist it now.
            if st.page_allocated {
                st.current_ipage.write_checksum();
                st.sf.write_ipage_at(st.current_seg, st.current_page, &st.current_ipage)?;
            }

            let seg_id = st.sf.active_seg_id();
            let used = st.sf.active_pages_used();
            let meta = SegmentMeta {
                file_id: st.sf.file_id, segment_id: seg_id,
                page_count: used, live_pages: used, state: SegmentState::Sealed,
            };
            st.sf.write_segment_header(&meta)?;
            st.sf.start_new_segment()?;
            st.current_ipage = IPage::new();
            st.page_allocated = false;
        }
        Ok(())
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
