//! Per-partition B+ tree stored as ipages in a segment file.
//!
//! ## Read path (concurrent-safe)
//! `get()` and `iter_entries()` take `seg: &SegmentFile` — they use
//! `read_page_ro` (pread) so multiple concurrent readers on the same partition
//! are safe under a `RwLock::read()` guard in kv_engine.
//!
//! ## Write path
//! `insert()`, `delete()`, and `flush()` take `seg: &mut SegmentFile`.
//! Modified pages are held in the `dirty` HashMap; `flush()` writes them to
//! the segment and calls `sync()`.
//!
//! ## WSBCache integration
//! Read and write methods accept an `Option<(&WsbCache, u32)>` cache context
//! `(cache, partition_id)`. On a cache miss the page is loaded from the
//! segment file and inserted into the cache. `flush()` updates the cache after
//! writing dirty pages to disk.

use std::collections::HashMap;
use std::io;

use crate::engine::ipage::{
    init_internal, init_leaf, internal_child, internal_count, internal_find_child,
    internal_height, internal_insert, internal_key, internal_set_child, internal_split,
    leaf_count, leaf_entry_at, leaf_insert, leaf_lower_bound, leaf_next, leaf_remove,
    leaf_set_next, leaf_split, leaf_write_entry, page_node_type, LeafEntry, IPAGE_SIZE,
    NODE_INTERNAL, NODE_LEAF,
};
use crate::engine::segment::SegmentFile;
use crate::engine::wsbcache::WsbCache;

/// Maximum allowed B-Tree height (from the SIndex paper).
pub const BTREE_MAX_HEIGHT: u8 = 4;

/// Cache context: a reference to the shared WSBCache plus the partition id
/// used as the first component of the cache key.
pub type CacheCtx<'a> = (&'a WsbCache, u32);

/// B+ tree backed by a segment file.
pub struct BTree {
    /// In-memory dirty pages not yet flushed to the segment file.
    dirty: HashMap<u32, [u8; IPAGE_SIZE]>,
}

impl BTree {
    pub fn new() -> Self {
        BTree { dirty: HashMap::new() }
    }

    // ─── Internal helpers ────────────────────────────────────────────────────

    /// Load a page: dirty cache → WSBCache → segment file (pread).
    fn load_page(
        &self,
        seg: &SegmentFile,
        idx: u32,
        cache: Option<CacheCtx<'_>>,
    ) -> io::Result<[u8; IPAGE_SIZE]> {
        // Hot path: in-memory dirty copy
        if let Some(p) = self.dirty.get(&idx) {
            return Ok(*p);
        }
        // WSBCache hit
        if let Some((wc, pid)) = cache {
            if let Some(p) = wc.get((pid, idx)) {
                return Ok(*p);
            }
        }
        // Cold path: read from SSD
        let mut buf = [0u8; IPAGE_SIZE];
        seg.read_page_ro(idx, &mut buf)?;
        // Populate cache on miss
        if let Some((wc, pid)) = cache {
            wc.insert((pid, idx), Box::new(buf), false);
        }
        Ok(buf)
    }

    fn mark_dirty(&mut self, idx: u32, page: [u8; IPAGE_SIZE]) {
        self.dirty.insert(idx, page);
    }

    // ─── Search (read-only, safe under RwLock::read) ──────────────────────────

    /// Look up `key_hash` in the tree.
    ///
    /// Takes an immutable reference to `seg` — uses `pread` internally so it
    /// is safe to call concurrently from multiple threads holding a shared
    /// partition lock.
    pub fn get(
        &self,
        seg: &SegmentFile,
        key_hash: u64,
        cache: Option<CacheCtx<'_>>,
    ) -> io::Result<Option<LeafEntry>> {
        let root = seg.header.root_page;
        if root == 0 {
            return Ok(None);
        }
        self.get_inner(seg, root, key_hash, cache)
    }

    fn get_inner(
        &self,
        seg: &SegmentFile,
        page_idx: u32,
        key_hash: u64,
        cache: Option<CacheCtx<'_>>,
    ) -> io::Result<Option<LeafEntry>> {
        let page = self.load_page(seg, page_idx, cache)?;
        match page_node_type(&page) {
            NODE_INTERNAL => {
                let child = internal_child(&page, internal_find_child(&page, key_hash));
                self.get_inner(seg, child, key_hash, cache)
            }
            NODE_LEAF => {
                let pos = leaf_lower_bound(&page, key_hash);
                let count = leaf_count(&page) as usize;
                let mut i = pos;
                while i < count {
                    let e = leaf_entry_at(&page, i);
                    if e.key_hash != key_hash {
                        break;
                    }
                    if e.is_alive() {
                        return Ok(Some(e));
                    }
                    i += 1;
                }
                Ok(None)
            }
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "unknown page type")),
        }
    }

    // ─── Insert ──────────────────────────────────────────────────────────────

    pub fn insert(
        &mut self,
        seg: &mut SegmentFile,
        entry: LeafEntry,
        cache: Option<CacheCtx<'_>>,
    ) -> io::Result<bool> {
        let root = seg.header.root_page;
        if root == 0 {
            let leaf_idx = seg.alloc_page();
            let mut leaf = [0u8; IPAGE_SIZE];
            init_leaf(&mut leaf);
            leaf_insert(&mut leaf, 0, &entry);
            self.mark_dirty(leaf_idx, leaf);
            seg.header.root_page = leaf_idx;
            seg.header.live_entries += 1;
            return Ok(true);
        }

        let result = self.insert_recursive(seg, root, entry, cache)?;
        match result {
            InsertResult::Split { sep_key, right_child, is_new } => {
                let new_root_idx = seg.alloc_page();
                let height = {
                    let page = self.load_page(seg, root, cache)?;
                    if page_node_type(&page) == NODE_INTERNAL {
                        internal_height(&page) + 1
                    } else {
                        1
                    }
                };
                let mut new_root = [0u8; IPAGE_SIZE];
                init_internal(&mut new_root, height);
                internal_set_child(&mut new_root, 0, root);
                internal_insert(&mut new_root, 0, sep_key, right_child);
                self.mark_dirty(new_root_idx, new_root);
                seg.header.root_page = new_root_idx;
                if is_new { seg.header.live_entries += 1; }
                Ok(is_new)
            }
            InsertResult::Done { is_new } => {
                if is_new { seg.header.live_entries += 1; }
                Ok(is_new)
            }
        }
    }

    fn insert_recursive(
        &mut self,
        seg: &mut SegmentFile,
        page_idx: u32,
        entry: LeafEntry,
        cache: Option<CacheCtx<'_>>,
    ) -> io::Result<InsertResult> {
        let page = self.load_page(seg, page_idx, cache)?;

        match page_node_type(&page) {
            NODE_INTERNAL => {
                let child_pos = internal_find_child(&page, entry.key_hash);
                let child_idx = internal_child(&page, child_pos);
                let result = self.insert_recursive(seg, child_idx, entry, cache)?;
                match result {
                    InsertResult::Done { is_new } => Ok(InsertResult::Done { is_new }),
                    InsertResult::Split { sep_key, right_child, is_new } => {
                        let mut page = self.load_page(seg, page_idx, cache)?;
                        let count = internal_count(&page) as usize;
                        let mut ins_pos = 0;
                        while ins_pos < count && internal_key(&page, ins_pos) < sep_key {
                            ins_pos += 1;
                        }
                        if internal_insert(&mut page, ins_pos, sep_key, right_child) {
                            self.mark_dirty(page_idx, page);
                            Ok(InsertResult::Done { is_new })
                        } else {
                            let mut right_page = [0u8; IPAGE_SIZE];
                            let push_up = internal_split(&mut page, &mut right_page);
                            let right_idx = seg.alloc_page();
                            if sep_key <= push_up {
                                internal_insert(&mut page, ins_pos, sep_key, right_child);
                            } else {
                                let right_count = internal_count(&right_page) as usize;
                                let mut rpos = 0;
                                while rpos < right_count && internal_key(&right_page, rpos) < sep_key {
                                    rpos += 1;
                                }
                                internal_insert(&mut right_page, rpos, sep_key, right_child);
                            }
                            self.mark_dirty(page_idx, page);
                            self.mark_dirty(right_idx, right_page);
                            Ok(InsertResult::Split { sep_key: push_up, right_child: right_idx, is_new })
                        }
                    }
                }
            }
            NODE_LEAF => {
                let mut page = page;
                let count = leaf_count(&page) as usize;
                let pos = leaf_lower_bound(&page, entry.key_hash);
                let mut i = pos;
                while i < count && leaf_entry_at(&page, i).key_hash == entry.key_hash {
                    let mut e = leaf_entry_at(&page, i);
                    if e.is_alive() {
                        e.value_ptr = entry.value_ptr;
                        e.value_len = entry.value_len;
                        e.key_len = entry.key_len;
                        leaf_write_entry(&mut page, i, &e);
                        self.mark_dirty(page_idx, page);
                        return Ok(InsertResult::Done { is_new: false });
                    }
                    leaf_write_entry(&mut page, i, &entry);
                    self.mark_dirty(page_idx, page);
                    return Ok(InsertResult::Done { is_new: true });
                }

                if leaf_insert(&mut page, pos, &entry) {
                    self.mark_dirty(page_idx, page);
                    Ok(InsertResult::Done { is_new: true })
                } else {
                    let mut right_page = [0u8; IPAGE_SIZE];
                    let sep_key = leaf_split(&mut page, &mut right_page);
                    let right_idx = seg.alloc_page();
                    let old_next = leaf_next(&page);
                    leaf_set_next(&mut page, right_idx);
                    leaf_set_next(&mut right_page, old_next);
                    if entry.key_hash < sep_key {
                        let ins_pos = leaf_lower_bound(&page, entry.key_hash);
                        leaf_insert(&mut page, ins_pos, &entry);
                    } else {
                        let ins_pos = leaf_lower_bound(&right_page, entry.key_hash);
                        leaf_insert(&mut right_page, ins_pos, &entry);
                    }
                    self.mark_dirty(page_idx, page);
                    self.mark_dirty(right_idx, right_page);
                    Ok(InsertResult::Split { sep_key, right_child: right_idx, is_new: true })
                }
            }
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "unknown page type during insert")),
        }
    }

    // ─── Delete ──────────────────────────────────────────────────────────────

    pub fn delete(
        &mut self,
        seg: &mut SegmentFile,
        key_hash: u64,
        cache: Option<CacheCtx<'_>>,
    ) -> io::Result<bool> {
        let root = seg.header.root_page;
        if root == 0 { return Ok(false); }
        let found = self.delete_recursive(seg, root, key_hash, cache)?;
        if found {
            seg.header.live_entries = seg.header.live_entries.saturating_sub(1);
        }
        Ok(found)
    }

    fn delete_recursive(
        &mut self,
        seg: &mut SegmentFile,
        page_idx: u32,
        key_hash: u64,
        cache: Option<CacheCtx<'_>>,
    ) -> io::Result<bool> {
        let page = self.load_page(seg, page_idx, cache)?;
        match page_node_type(&page) {
            NODE_INTERNAL => {
                let child = internal_child(&page, internal_find_child(&page, key_hash));
                self.delete_recursive(seg, child, key_hash, cache)
            }
            NODE_LEAF => {
                let mut page = page;
                let count = leaf_count(&page) as usize;
                let pos = leaf_lower_bound(&page, key_hash);
                let mut i = pos;
                while i < count && leaf_entry_at(&page, i).key_hash == key_hash {
                    let e = leaf_entry_at(&page, i);
                    if e.is_alive() {
                        leaf_remove(&mut page, i);
                        self.mark_dirty(page_idx, page);
                        return Ok(true);
                    }
                    i += 1;
                }
                Ok(false)
            }
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "unknown page type during delete")),
        }
    }

    // ─── Flush ───────────────────────────────────────────────────────────────

    /// Write all dirty pages to the segment file and sync.
    ///
    /// Updated pages are also written into the WSBCache so subsequent reads
    /// see the latest data without going to disk.
    pub fn flush(
        &mut self,
        seg: &mut SegmentFile,
        cache: Option<CacheCtx<'_>>,
    ) -> io::Result<()> {
        if self.dirty.is_empty() {
            return Ok(());
        }
        for (&idx, page) in &self.dirty {
            seg.write_page(idx, page)?;
            if let Some((wc, pid)) = cache {
                wc.insert((pid, idx), Box::new(*page), false);
            }
        }
        self.dirty.clear();
        seg.sync()
    }

    // ─── Iteration (read-only) ────────────────────────────────────────────────

    pub fn iter_entries(
        &self,
        seg: &SegmentFile,
        cache: Option<CacheCtx<'_>>,
    ) -> io::Result<Vec<LeafEntry>> {
        let root = seg.header.root_page;
        if root == 0 { return Ok(Vec::new()); }

        // Descend to leftmost leaf
        let mut cur_idx = root;
        loop {
            let page = self.load_page(seg, cur_idx, cache)?;
            if page_node_type(&page) == NODE_LEAF { break; }
            cur_idx = internal_child(&page, 0);
        }

        // Follow leaf chain
        let mut result = Vec::new();
        let mut leaf_idx = cur_idx;
        while leaf_idx != 0 {
            let page = self.load_page(seg, leaf_idx, cache)?;
            let count = leaf_count(&page) as usize;
            for i in 0..count {
                let e = leaf_entry_at(&page, i);
                if e.is_alive() { result.push(e); }
            }
            leaf_idx = leaf_next(&page);
        }
        Ok(result)
    }

    /// Count of live entries tracked in the segment header.
    pub fn live_count(seg: &SegmentFile) -> u64 {
        seg.header.live_entries
    }
}

impl Default for BTree {
    fn default() -> Self { Self::new() }
}

enum InsertResult {
    Done { is_new: bool },
    Split { sep_key: u64, right_child: u32, is_new: bool },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::ipage::{FLAG_ALIVE, MAX_LEAF_ENTRIES};
    use tempfile::tempdir;

    fn make_entry(hash: u64, ptr: u64) -> LeafEntry {
        LeafEntry { key_hash: hash, value_ptr: ptr, value_len: 10, key_len: 4, flags: FLAG_ALIVE }
    }

    fn open_seg(path: &std::path::Path) -> SegmentFile {
        SegmentFile::create(path, 0).unwrap()
    }

    #[test]
    fn basic_insert_and_get() {
        let dir = tempdir().unwrap();
        let mut seg = open_seg(&dir.path().join("p0.seg"));
        let mut tree = BTree::new();

        for i in 0u64..100 {
            tree.insert(&mut seg, make_entry(i * 7, i * 100), None).unwrap();
        }
        for i in 0u64..100 {
            let e = tree.get(&seg, i * 7, None).unwrap().unwrap();
            assert_eq!(e.value_ptr, i * 100);
        }
        assert!(tree.get(&seg, 9999, None).unwrap().is_none());
    }

    #[test]
    fn insert_many_causes_splits() {
        let dir = tempdir().unwrap();
        let mut seg = open_seg(&dir.path().join("p0.seg"));
        let mut tree = BTree::new();
        let n = MAX_LEAF_ENTRIES * 10;
        for i in 0u64..n as u64 {
            tree.insert(&mut seg, make_entry(i, i * 10), None).unwrap();
        }
        assert_eq!(seg.header.live_entries, n as u64);
        for i in 0u64..n as u64 {
            let e = tree.get(&seg, i, None).unwrap().unwrap();
            assert_eq!(e.value_ptr, i * 10);
        }
    }

    #[test]
    fn delete_removes_entry() {
        let dir = tempdir().unwrap();
        let mut seg = open_seg(&dir.path().join("p0.seg"));
        let mut tree = BTree::new();
        for i in 0u64..50 {
            tree.insert(&mut seg, make_entry(i, i), None).unwrap();
        }
        assert!(tree.delete(&mut seg, 25, None).unwrap());
        assert!(tree.get(&seg, 25, None).unwrap().is_none());
        assert!(tree.get(&seg, 24, None).unwrap().is_some());
        assert!(!tree.delete(&mut seg, 999, None).unwrap());
    }

    #[test]
    fn flush_and_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("p0.seg");
        {
            let mut seg = SegmentFile::create(&path, 0).unwrap();
            let mut tree = BTree::new();
            for i in 0u64..50 {
                tree.insert(&mut seg, make_entry(i, i * 10), None).unwrap();
            }
            tree.flush(&mut seg, None).unwrap();
        }
        {
            let seg = SegmentFile::open(&path).unwrap();
            let tree = BTree::new();
            assert_eq!(seg.header.live_entries, 50);
            for i in 0u64..50 {
                let e = tree.get(&seg, i, None).unwrap().unwrap();
                assert_eq!(e.value_ptr, i * 10);
            }
        }
    }

    #[test]
    fn iter_entries_in_order() {
        let dir = tempdir().unwrap();
        let mut seg = open_seg(&dir.path().join("p0.seg"));
        let mut tree = BTree::new();
        let mut hashes: Vec<u64> = (0u64..200).map(|i| i * 3 + 1).collect();
        for &h in &hashes {
            tree.insert(&mut seg, make_entry(h, h * 2), None).unwrap();
        }
        let entries = tree.iter_entries(&seg, None).unwrap();
        assert_eq!(entries.len(), hashes.len());
        hashes.sort();
        for (got, expected_h) in entries.iter().zip(hashes.iter()) {
            assert_eq!(got.key_hash, *expected_h);
        }
    }
}
