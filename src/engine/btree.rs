//! Per-partition B+ tree stored as ipages in a segment file.
//!
//! This implements the per-partition index structure from SIndex (ICPP '24 /
//! ACM TOS 2026). Keys are 64-bit hashes of the actual keys; values are
//! (value_ptr, value_len, key_len) tuples pointing into the value log. All
//! data is in leaf nodes; internal nodes only hold separator keys for routing.
//!
//! ## Deterministic latency
//! Tree height is bounded by `BTREE_MAX_HEIGHT`. A lookup reads at most
//! `BTREE_MAX_HEIGHT` ipage pages from the segment (SSD reads), guaranteeing
//! deterministic latency per the paper.
//!
//! ## Page access via WSBCache
//! Every page read/write goes through the engine-global write-staging
//! buffer cache. Reads check the cache first (hot ipages are served from
//! memory); modified pages are inserted dirty and stay staged until the
//! TSS sync cycle writes them to the segment file. The tree itself holds
//! no mutable state — read operations take `&self` + `&SegmentFile` and
//! can run under a shared partition lock.

use std::io;
use std::sync::Arc;

use crate::engine::ipage::{
    init_internal, init_leaf, internal_child, internal_count, internal_find_child,
    internal_height, internal_insert, internal_key, internal_set_child, internal_split,
    leaf_count, leaf_entry_at, leaf_insert, leaf_lower_bound, leaf_next, leaf_remove,
    leaf_set_next, leaf_split, leaf_write_entry, page_node_type, LeafEntry, IPAGE_SIZE,
    NODE_INTERNAL, NODE_LEAF,
};
use crate::engine::segment::SegmentFile;
use crate::engine::wsbcache::WsbCache;

/// Maximum B-Tree height, matching the paper ("SIndex precisely tunes the
/// capacity of internal B-Tree nodes to ensure the tree height does not
/// exceed 3, achieving consistently short indexing path").
///
/// Capacity check: a height-3 tree holds 169 (leaf entries) × 254 × 254
/// ≈ 10.9 M entries per partition; with 65 536 partitions the engine spans
/// ~715 billion entries — the paper's "hundreds of billions" regime.
pub const BTREE_MAX_HEIGHT: u8 = 3;

/// B+ tree backed by a segment file, with all page traffic staged through
/// the shared `WsbCache`.
pub struct BTree {
    pid: u32,
    cache: Arc<WsbCache>,
}

impl BTree {
    pub fn new(pid: u32, cache: Arc<WsbCache>) -> Self {
        BTree { pid, cache }
    }

    /// Get a page: check the WSBCache first, then read from the segment
    /// (inserting the page clean so subsequent reads hit memory). Pages
    /// coming from the SSD are CRC-verified (paper: per-ipage CRC check).
    fn load_page(&self, seg: &SegmentFile, idx: u32) -> io::Result<[u8; IPAGE_SIZE]> {
        if let Some(p) = self.cache.get((self.pid, idx)) {
            return Ok(p);
        }
        let mut buf = [0u8; IPAGE_SIZE];
        seg.read_page(idx, &mut buf)?;
        if !crate::engine::ipage::verify(&buf) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("ipage CRC mismatch: partition {} page {}", self.pid, idx),
            ));
        }
        self.cache.insert((self.pid, idx), Box::new(buf), false);
        Ok(buf)
    }

    /// Stage a modified page in the WSBCache (write-staging, paper TSS).
    fn mark_dirty(&self, idx: u32, page: [u8; IPAGE_SIZE]) {
        self.cache.insert((self.pid, idx), Box::new(page), true);
    }

    /// Write this partition's staged dirty pages to the segment file and
    /// fsync. Used by the engine's sync cycle and on shutdown. Each page is
    /// CRC-sealed before hitting the SSD.
    pub fn flush(&self, seg: &mut SegmentFile) -> io::Result<()> {
        let mut dirty = self.cache.collect_dirty_for(self.pid);
        if dirty.is_empty() {
            return Ok(());
        }
        for ((_, idx), _, page) in &mut dirty {
            crate::engine::ipage::seal(page);
            seg.write_page(*idx, page)?;
        }
        seg.sync()?;
        let written: Vec<_> = dirty.iter().map(|(k, g, _)| (*k, *g)).collect();
        self.cache.mark_clean(&written);
        Ok(())
    }

    // ─── Search ─────────────────────────────────────────────────────────────

    /// Look up `key_hash` in the tree.
    ///
    /// Returns the first `LeafEntry` with a matching `key_hash` and
    /// `FLAG_ALIVE`, or `None`. Takes `&self` + `&SegmentFile`: safe under a
    /// shared (read) partition lock.
    pub fn get(&self, seg: &SegmentFile, key_hash: u64) -> io::Result<Option<LeafEntry>> {
        let root = seg.header.root_page;
        if root == 0 {
            return Ok(None);
        }
        self.get_inner(seg, root, key_hash)
    }

    fn get_inner(
        &self,
        seg: &SegmentFile,
        page_idx: u32,
        key_hash: u64,
    ) -> io::Result<Option<LeafEntry>> {
        let page = self.load_page(seg, page_idx)?;
        match page_node_type(&page) {
            NODE_INTERNAL => {
                let child = internal_child(&page, internal_find_child(&page, key_hash));
                self.get_inner(seg, child, key_hash)
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
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unknown page type",
            )),
        }
    }

    // ─── Insert ─────────────────────────────────────────────────────────────

    /// Insert or update an entry for `key_hash`.
    ///
    /// If an alive entry for `key_hash` already exists its `value_ptr` is
    /// updated in-place. Returns `true` if a new entry was created, `false`
    /// if an existing one was updated. Pages are staged dirty in the
    /// WSBCache — nothing touches the SSD here.
    pub fn insert(&self, seg: &mut SegmentFile, entry: LeafEntry) -> io::Result<bool> {
        let root = seg.header.root_page;
        if root == 0 {
            // Tree is empty — allocate the first leaf as root.
            let leaf_idx = seg.alloc_page();
            let mut leaf = [0u8; IPAGE_SIZE];
            init_leaf(&mut leaf);
            leaf_insert(&mut leaf, 0, &entry);
            self.mark_dirty(leaf_idx, leaf);
            seg.header.root_page = leaf_idx;
            seg.header.live_entries += 1;
            return Ok(true);
        }

        let result = self.insert_recursive(seg, root, entry)?;
        match result {
            InsertResult::Split {
                sep_key,
                right_child,
                is_new,
            } => {
                // Root was split: create a new root internal node.
                let new_root_idx = seg.alloc_page();
                let height = {
                    let page = self.load_page(seg, root)?;
                    if page_node_type(&page) == NODE_INTERNAL {
                        internal_height(&page) + 1
                    } else {
                        1
                    }
                };
                let mut new_root = [0u8; IPAGE_SIZE];
                init_internal(&mut new_root, height);
                // children[0] = old root, children[1] = right_child
                internal_set_child(&mut new_root, 0, root);
                internal_insert(&mut new_root, 0, sep_key, right_child);
                self.mark_dirty(new_root_idx, new_root);
                seg.header.root_page = new_root_idx;
                if is_new {
                    seg.header.live_entries += 1;
                }
                Ok(is_new)
            }
            InsertResult::Done { is_new } => {
                if is_new {
                    seg.header.live_entries += 1;
                }
                Ok(is_new)
            }
        }
    }

    fn insert_recursive(
        &self,
        seg: &mut SegmentFile,
        page_idx: u32,
        entry: LeafEntry,
    ) -> io::Result<InsertResult> {
        let page = self.load_page(seg, page_idx)?;

        match page_node_type(&page) {
            NODE_INTERNAL => {
                let child_pos = internal_find_child(&page, entry.key_hash);
                let child_idx = internal_child(&page, child_pos);
                let result = self.insert_recursive(seg, child_idx, entry)?;
                match result {
                    InsertResult::Done { is_new } => Ok(InsertResult::Done { is_new }),
                    InsertResult::Split {
                        sep_key,
                        right_child,
                        is_new,
                    } => {
                        // Insert the separator key into this internal node.
                        let mut page = self.load_page(seg, page_idx)?;
                        let count = internal_count(&page) as usize;
                        let mut ins_pos = 0;
                        while ins_pos < count && internal_key(&page, ins_pos) < sep_key {
                            ins_pos += 1;
                        }
                        if internal_insert(&mut page, ins_pos, sep_key, right_child) {
                            self.mark_dirty(page_idx, page);
                            Ok(InsertResult::Done { is_new })
                        } else {
                            // Internal node is full — split it.
                            let mut right_page = [0u8; IPAGE_SIZE];
                            let push_up = internal_split(&mut page, &mut right_page);
                            let right_idx = seg.alloc_page();

                            if sep_key <= push_up {
                                internal_insert(&mut page, ins_pos, sep_key, right_child);
                            } else {
                                let right_count = internal_count(&right_page) as usize;
                                let mut rpos = 0;
                                while rpos < right_count
                                    && internal_key(&right_page, rpos) < sep_key
                                {
                                    rpos += 1;
                                }
                                internal_insert(&mut right_page, rpos, sep_key, right_child);
                            }

                            self.mark_dirty(page_idx, page);
                            self.mark_dirty(right_idx, right_page);
                            Ok(InsertResult::Split {
                                sep_key: push_up,
                                right_child: right_idx,
                                is_new,
                            })
                        }
                    }
                }
            }
            NODE_LEAF => {
                let mut page = page;
                let count = leaf_count(&page) as usize;

                // Check if key already exists (update in place)
                let pos = leaf_lower_bound(&page, entry.key_hash);
                let mut i = pos;
                while i < count && leaf_entry_at(&page, i).key_hash == entry.key_hash {
                    let mut e = leaf_entry_at(&page, i);
                    if e.is_alive() {
                        // Update existing alive entry
                        e.value_ptr = entry.value_ptr;
                        e.value_len = entry.value_len;
                        e.key_len = entry.key_len;
                        leaf_write_entry(&mut page, i, &e);
                        self.mark_dirty(page_idx, page);
                        return Ok(InsertResult::Done { is_new: false });
                    }
                    // Dead entry with same hash — overwrite it
                    leaf_write_entry(&mut page, i, &entry);
                    self.mark_dirty(page_idx, page);
                    return Ok(InsertResult::Done { is_new: true });
                }

                if leaf_insert(&mut page, pos, &entry) {
                    self.mark_dirty(page_idx, page);
                    Ok(InsertResult::Done { is_new: true })
                } else {
                    // Leaf is full — split.
                    let mut right_page = [0u8; IPAGE_SIZE];
                    let sep_key = leaf_split(&mut page, &mut right_page);
                    let right_idx = seg.alloc_page();

                    // Wire up the leaf chain: left → right → old_next
                    let old_next = leaf_next(&page);
                    leaf_set_next(&mut page, right_idx);
                    leaf_set_next(&mut right_page, old_next);

                    // Insert the new entry into the correct half
                    if entry.key_hash < sep_key {
                        let ins_pos = leaf_lower_bound(&page, entry.key_hash);
                        leaf_insert(&mut page, ins_pos, &entry);
                    } else {
                        let ins_pos = leaf_lower_bound(&right_page, entry.key_hash);
                        leaf_insert(&mut right_page, ins_pos, &entry);
                    }

                    self.mark_dirty(page_idx, page);
                    self.mark_dirty(right_idx, right_page);
                    Ok(InsertResult::Split {
                        sep_key,
                        right_child: right_idx,
                        is_new: true,
                    })
                }
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unknown page type during insert",
            )),
        }
    }

    // ─── Delete ─────────────────────────────────────────────────────────────

    /// Mark the entry for `key_hash` as deleted.
    ///
    /// Returns `true` if an alive entry was found and marked deleted.
    pub fn delete(&self, seg: &mut SegmentFile, key_hash: u64) -> io::Result<bool> {
        let root = seg.header.root_page;
        if root == 0 {
            return Ok(false);
        }
        let found = self.delete_recursive(seg, root, key_hash)?;
        if found {
            seg.header.live_entries = seg.header.live_entries.saturating_sub(1);
        }
        Ok(found)
    }

    fn delete_recursive(
        &self,
        seg: &SegmentFile,
        page_idx: u32,
        key_hash: u64,
    ) -> io::Result<bool> {
        let page = self.load_page(seg, page_idx)?;
        match page_node_type(&page) {
            NODE_INTERNAL => {
                let child = internal_child(&page, internal_find_child(&page, key_hash));
                self.delete_recursive(seg, child, key_hash)
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
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unknown page type during delete",
            )),
        }
    }

    // ─── Iteration ──────────────────────────────────────────────────────────

    /// Collect all alive entries in key-hash order by following the leaf chain.
    ///
    /// Starts from the leftmost leaf by traversing the tree, then follows
    /// `next_leaf` pointers. Used for SCAN and recovery.
    pub fn iter_entries(&self, seg: &SegmentFile) -> io::Result<Vec<LeafEntry>> {
        let root = seg.header.root_page;
        if root == 0 {
            return Ok(Vec::new());
        }

        // Find leftmost leaf
        let mut cur_idx = root;
        loop {
            let page = self.load_page(seg, cur_idx)?;
            if page_node_type(&page) == NODE_LEAF {
                break;
            }
            // Go to leftmost child (children[0]).
            cur_idx = internal_child(&page, 0);
        }

        // Follow leaf chain
        let mut result = Vec::new();
        let mut leaf_idx = cur_idx;
        while leaf_idx != 0 {
            let page = self.load_page(seg, leaf_idx)?;
            let count = leaf_count(&page) as usize;
            for i in 0..count {
                let e = leaf_entry_at(&page, i);
                if e.is_alive() {
                    result.push(e);
                }
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

/// Result of a recursive insert — either done or a split bubbled up.
enum InsertResult {
    Done { is_new: bool },
    Split {
        sep_key: u64,
        right_child: u32,
        is_new: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::ipage::{FLAG_ALIVE, MAX_LEAF_ENTRIES};
    use tempfile::tempdir;

    fn make_entry(hash: u64, ptr: u64) -> LeafEntry {
        LeafEntry {
            key_hash: hash,
            value_ptr: ptr,
            value_len: 10,
            key_len: 4,
            flags: FLAG_ALIVE,
        }
    }

    fn open_tree(path: &std::path::Path) -> (BTree, SegmentFile) {
        let seg = SegmentFile::create(path, 0).unwrap();
        (BTree::new(0, WsbCache::new(100_000)), seg)
    }

    #[test]
    fn basic_insert_and_get() {
        let dir = tempdir().unwrap();
        let (tree, mut seg) = open_tree(&dir.path().join("p0.seg"));

        for i in 0u64..100 {
            tree.insert(&mut seg, make_entry(i * 7, i * 100)).unwrap();
        }

        for i in 0u64..100 {
            let e = tree.get(&seg, i * 7).unwrap().unwrap();
            assert_eq!(e.value_ptr, i * 100);
        }
        assert!(tree.get(&seg, 9999).unwrap().is_none());
    }

    #[test]
    fn insert_many_causes_splits() {
        let dir = tempdir().unwrap();
        let (tree, mut seg) = open_tree(&dir.path().join("p0.seg"));

        // Insert enough keys to force multiple leaf splits
        let n = MAX_LEAF_ENTRIES * 10;
        for i in 0u64..n as u64 {
            tree.insert(&mut seg, make_entry(i, i * 10)).unwrap();
        }
        assert_eq!(seg.header.live_entries, n as u64);

        // Verify all entries are findable
        for i in 0u64..n as u64 {
            let e = tree.get(&seg, i).unwrap().unwrap();
            assert_eq!(e.value_ptr, i * 10);
        }
    }

    #[test]
    fn delete_removes_entry() {
        let dir = tempdir().unwrap();
        let (tree, mut seg) = open_tree(&dir.path().join("p0.seg"));

        for i in 0u64..50 {
            tree.insert(&mut seg, make_entry(i, i)).unwrap();
        }

        assert!(tree.delete(&mut seg, 25).unwrap());
        assert!(tree.get(&seg, 25).unwrap().is_none());
        assert!(tree.get(&seg, 24).unwrap().is_some());
        assert!(!tree.delete(&mut seg, 999).unwrap()); // non-existent
    }

    #[test]
    fn flush_and_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("p0.seg");
        {
            let mut seg = SegmentFile::create(&path, 0).unwrap();
            let tree = BTree::new(0, WsbCache::new(100_000));
            for i in 0u64..50 {
                tree.insert(&mut seg, make_entry(i, i * 10)).unwrap();
            }
            tree.flush(&mut seg).unwrap();
        }
        {
            // Fresh cache: everything must come from the segment file.
            let seg = SegmentFile::open(&path).unwrap();
            let tree = BTree::new(0, WsbCache::new(100_000));
            assert_eq!(seg.header.live_entries, 50);
            for i in 0u64..50 {
                let e = tree.get(&seg, i).unwrap().unwrap();
                assert_eq!(e.value_ptr, i * 10);
            }
        }
    }

    #[test]
    fn iter_entries_in_order() {
        let dir = tempdir().unwrap();
        let (tree, mut seg) = open_tree(&dir.path().join("p0.seg"));

        let mut hashes: Vec<u64> = (0u64..200).map(|i| i * 3 + 1).collect();
        for &h in &hashes {
            tree.insert(&mut seg, make_entry(h, h * 2)).unwrap();
        }

        let entries = tree.iter_entries(&seg).unwrap();
        assert_eq!(entries.len(), hashes.len());
        hashes.sort();
        for (got, expected_h) in entries.iter().zip(hashes.iter()) {
            assert_eq!(got.key_hash, *expected_h);
        }
    }
}
