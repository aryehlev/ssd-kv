//! Index page (ipage) — the fundamental 4 KB unit of on-SSD storage in SIndex.
//!
//! Based on: "The Design of Trillion-scale SSD-based Indexing with Deterministic
//! Latency for Cloud Block Storage", ACM TOS 2024 (DOI 10.1145/3789205).
//!
//! Each ipage is exactly 4096 bytes (one SSD page). The B-Tree's internal and
//! leaf nodes are stored as ipages, enabling direct SSD reads/writes at page
//! granularity and bounding lookup latency to at most BTREE_MAX_HEIGHT + 1
//! SSD reads.
//!
//! ## Internal node layout (4096 bytes)
//! ```text
//! [0..4]      magic      u32 = MAGIC_INTERNAL
//! [4]         node_type  u8  = NODE_INTERNAL
//! [5]         height     u8  (1 = direct parent of leaves)
//! [6..8]      count      u16 (number of separator keys; children = count+1)
//! [8..32]     padding
//! [32..2056]  keys       [u64; 253]     — separator key hashes
//! [2056..3072] children  [u32; 254]     — child page indices in segment
//! [3072..4096] padding
//! ```
//!
//! ## Leaf node layout (4096 bytes)
//! ```text
//! [0..4]     magic      u32 = MAGIC_LEAF
//! [4]        node_type  u8  = NODE_LEAF
//! [5]        _reserved  u8
//! [6..8]     count      u16 (number of entries)
//! [8..12]    next_leaf  u32 (page index of sibling; 0 = end of chain)
//! [12..32]   padding
//! [32..4088] entries    [LeafEntry; 169]  (each 24 bytes)
//! [4088..4096] padding
//! ```
//!
//! ## LeafEntry layout (24 bytes)
//! ```text
//! [0..8]   key_hash   u64  — full hash, used for B-Tree ordering
//! [8..16]  value_ptr  u64  — byte offset in the value log
//! [16..20] value_len  u32  — length of value data
//! [20..22] key_len    u16  — length of key data (precedes value in log)
//! [22]     flags      u8   — FLAG_ALIVE | FLAG_DELETED
//! [23]     _pad       u8
//! ```

/// Size of one index page in bytes (= one SSD page).
pub const IPAGE_SIZE: usize = 4096;

/// Maximum number of entries in a leaf node.
/// (4096 − 32 header) / 24 bytes per entry = 169 entries; 8 bytes of padding.
pub const MAX_LEAF_ENTRIES: usize = 169;

/// Maximum number of separator keys in an internal node.
/// Keys region: [32..2056] = 2024 bytes = 253 × 8 bytes.
/// Children region: [2056..3072] = 1016 bytes = 254 × 4 bytes.
pub const MAX_INTERNAL_KEYS: usize = 253;

pub const NODE_INTERNAL: u8 = 1;
pub const NODE_LEAF: u8 = 2;

/// Magic values for page type identification.
pub const MAGIC_INTERNAL: u32 = 0x494E4F44; // "INOD"
pub const MAGIC_LEAF: u32 = 0x4C454146; // "LEAF"

/// Entry status flags.
pub const FLAG_ALIVE: u8 = 1;
pub const FLAG_DELETED: u8 = 2;

/// Byte offsets within the leaf header.
const LEAF_OFF_COUNT: usize = 6;
const LEAF_OFF_NEXT: usize = 8;
const LEAF_ENTRIES_START: usize = 32;
const LEAF_ENTRY_SIZE: usize = 24;

/// Byte offset of the per-page CRC32 (paper: "A CRC check is incorporated
/// to verify data integrity" per ipage). Lives in the padding region of
/// both node layouts: leaf [12..32], internal [8..32].
const PAGE_CRC_OFF: usize = 12;

/// Stamp the page CRC. Called when a page is written to the segment file;
/// the CRC field itself is zeroed during computation.
pub fn seal(page: &mut [u8; IPAGE_SIZE]) {
    write_u32(page, PAGE_CRC_OFF, 0);
    let crc = crc32fast::hash(page);
    write_u32(page, PAGE_CRC_OFF, crc);
}

/// Verify the page CRC stamped by [`seal`]. Called when a page is read from
/// the segment file (not for cache hits — cached pages are trusted).
pub fn verify(page: &[u8; IPAGE_SIZE]) -> bool {
    let stored = read_u32(page, PAGE_CRC_OFF);
    let mut copy = *page;
    write_u32(&mut copy, PAGE_CRC_OFF, 0);
    crc32fast::hash(&copy) == stored
}

/// Byte offsets within the internal node header.
const INT_OFF_HEIGHT: usize = 5;
const INT_OFF_COUNT: usize = 6;
const INT_KEYS_START: usize = 32;
const INT_CHILDREN_START: usize = 2056;

/// A KV metadata record stored in a leaf node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeafEntry {
    /// Full 64-bit key hash — used for B-Tree ordering and as identity key.
    pub key_hash: u64,
    /// Byte offset in the value log where `key_len` bytes of key followed
    /// by `value_len` bytes of value are stored.
    pub value_ptr: u64,
    /// Length of the value (variable-size support).
    pub value_len: u32,
    /// Length of the key stored in the value log.
    pub key_len: u16,
    /// FLAG_ALIVE or FLAG_DELETED.
    pub flags: u8,
}

impl LeafEntry {
    #[inline]
    pub fn is_alive(&self) -> bool {
        self.flags == FLAG_ALIVE
    }
}

// ─── Byte-level read/write helpers ──────────────────────────────────────────

#[inline]
pub fn read_u16(buf: &[u8], off: usize) -> u16 {
    u16::from_le_bytes(buf[off..off + 2].try_into().unwrap())
}

#[inline]
pub fn read_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(buf[off..off + 4].try_into().unwrap())
}

#[inline]
pub fn read_u64(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

#[inline]
pub fn write_u16(buf: &mut [u8], off: usize, v: u16) {
    buf[off..off + 2].copy_from_slice(&v.to_le_bytes());
}

#[inline]
pub fn write_u32(buf: &mut [u8], off: usize, v: u32) {
    buf[off..off + 4].copy_from_slice(&v.to_le_bytes());
}

#[inline]
pub fn write_u64(buf: &mut [u8], off: usize, v: u64) {
    buf[off..off + 8].copy_from_slice(&v.to_le_bytes());
}

// ─── Page type detection ─────────────────────────────────────────────────────

/// Returns the node type byte of any page.
#[inline]
pub fn page_node_type(page: &[u8]) -> u8 {
    page[4]
}

// ─── Leaf page operations ────────────────────────────────────────────────────

/// Initialize a fresh zeroed leaf page with the correct magic and type.
pub fn init_leaf(page: &mut [u8; IPAGE_SIZE]) {
    page.fill(0);
    write_u32(page, 0, MAGIC_LEAF);
    page[4] = NODE_LEAF;
}

/// Number of live entries in a leaf page.
#[inline]
pub fn leaf_count(page: &[u8]) -> u16 {
    read_u16(page, LEAF_OFF_COUNT)
}

/// Index of the next sibling leaf page (0 = none).
#[inline]
pub fn leaf_next(page: &[u8]) -> u32 {
    read_u32(page, LEAF_OFF_NEXT)
}

/// Set the next sibling leaf page index.
#[inline]
pub fn leaf_set_next(page: &mut [u8], next: u32) {
    write_u32(page, LEAF_OFF_NEXT, next);
}

/// Read the i-th entry from a leaf page.
pub fn leaf_entry_at(page: &[u8], i: usize) -> LeafEntry {
    let off = LEAF_ENTRIES_START + i * LEAF_ENTRY_SIZE;
    LeafEntry {
        key_hash: read_u64(page, off),
        value_ptr: read_u64(page, off + 8),
        value_len: read_u32(page, off + 16),
        key_len: read_u16(page, off + 20),
        flags: page[off + 22],
    }
}

/// Write a LeafEntry into position i of a leaf page.
pub fn leaf_write_entry(page: &mut [u8], i: usize, e: &LeafEntry) {
    let off = LEAF_ENTRIES_START + i * LEAF_ENTRY_SIZE;
    write_u64(page, off, e.key_hash);
    write_u64(page, off + 8, e.value_ptr);
    write_u32(page, off + 16, e.value_len);
    write_u16(page, off + 20, e.key_len);
    page[off + 22] = e.flags;
    page[off + 23] = 0; // pad
}

/// Binary-search for the first position where `key_hash >= target`.
pub fn leaf_lower_bound(page: &[u8], target: u64) -> usize {
    let count = leaf_count(page) as usize;
    let mut lo = 0usize;
    let mut hi = count;
    while lo < hi {
        let mid = (lo + hi) / 2;
        if leaf_entry_at(page, mid).key_hash < target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// Insert `entry` at position `pos`, shifting subsequent entries right.
/// Returns `false` if the page is full.
pub fn leaf_insert(page: &mut [u8; IPAGE_SIZE], pos: usize, entry: &LeafEntry) -> bool {
    let count = leaf_count(page) as usize;
    if count >= MAX_LEAF_ENTRIES {
        return false;
    }
    for i in (pos..count).rev() {
        let e = leaf_entry_at(page, i);
        leaf_write_entry(page, i + 1, &e);
    }
    leaf_write_entry(page, pos, entry);
    write_u16(page, LEAF_OFF_COUNT, (count + 1) as u16);
    true
}

/// Remove the entry at `pos`, shifting subsequent entries left.
pub fn leaf_remove(page: &mut [u8; IPAGE_SIZE], pos: usize) {
    let count = leaf_count(page) as usize;
    for i in pos..count - 1 {
        let e = leaf_entry_at(page, i + 1);
        leaf_write_entry(page, i, &e);
    }
    let last_off = LEAF_ENTRIES_START + (count - 1) * LEAF_ENTRY_SIZE;
    page[last_off..last_off + LEAF_ENTRY_SIZE].fill(0);
    write_u16(page, LEAF_OFF_COUNT, (count - 1) as u16);
}

/// Split a full leaf into two halves.
///
/// After the call `page` holds the lower half, `right` holds the upper half.
/// Returns the separator key (first key of `right`), which must be pushed up
/// into the parent internal node.
pub fn leaf_split(
    page: &mut [u8; IPAGE_SIZE],
    right: &mut [u8; IPAGE_SIZE],
) -> u64 {
    let count = leaf_count(page) as usize;
    let mid = count / 2;

    init_leaf(right);
    for i in mid..count {
        let e = leaf_entry_at(page, i);
        leaf_write_entry(right, i - mid, &e);
        // clear from original
        let off = LEAF_ENTRIES_START + i * LEAF_ENTRY_SIZE;
        page[off..off + LEAF_ENTRY_SIZE].fill(0);
    }
    write_u16(right, LEAF_OFF_COUNT, (count - mid) as u16);
    write_u16(page, LEAF_OFF_COUNT, mid as u16);
    // caller must wire up the leaf chain
    leaf_entry_at(right, 0).key_hash
}

// ─── Internal page operations ────────────────────────────────────────────────

/// Initialize a fresh zeroed internal page.
pub fn init_internal(page: &mut [u8; IPAGE_SIZE], height: u8) {
    page.fill(0);
    write_u32(page, 0, MAGIC_INTERNAL);
    page[4] = NODE_INTERNAL;
    page[INT_OFF_HEIGHT] = height;
}

#[inline]
pub fn internal_height(page: &[u8]) -> u8 {
    page[INT_OFF_HEIGHT]
}

#[inline]
pub fn internal_count(page: &[u8]) -> u16 {
    read_u16(page, INT_OFF_COUNT)
}

#[inline]
pub fn internal_key(page: &[u8], i: usize) -> u64 {
    read_u64(page, INT_KEYS_START + i * 8)
}

#[inline]
pub fn internal_child(page: &[u8], i: usize) -> u32 {
    read_u32(page, INT_CHILDREN_START + i * 4)
}

#[inline]
fn internal_set_key(page: &mut [u8], i: usize, k: u64) {
    write_u64(page, INT_KEYS_START + i * 8, k);
}

#[inline]
pub fn internal_set_child(page: &mut [u8], i: usize, c: u32) {
    write_u32(page, INT_CHILDREN_START + i * 4, c);
}

/// Find the child index to follow for `target` in an internal node.
/// Returns `i` such that `children[i]` is the correct subtree.
pub fn internal_find_child(page: &[u8], target: u64) -> usize {
    let count = internal_count(page) as usize;
    let mut i = 0;
    while i < count && internal_key(page, i) <= target {
        i += 1;
    }
    i
}

/// Insert `(sep_key, right_child)` at position `pos`.
/// `right_child` becomes `children[pos+1]`.
/// Returns `false` if the page is full.
pub fn internal_insert(
    page: &mut [u8; IPAGE_SIZE],
    pos: usize,
    sep_key: u64,
    right_child: u32,
) -> bool {
    let count = internal_count(page) as usize;
    if count >= MAX_INTERNAL_KEYS {
        return false;
    }
    for i in (pos..count).rev() {
        let k = internal_key(page, i);
        internal_set_key(page, i + 1, k);
    }
    for i in (pos + 1..=count).rev() {
        let c = internal_child(page, i);
        internal_set_child(page, i + 1, c);
    }
    internal_set_key(page, pos, sep_key);
    internal_set_child(page, pos + 1, right_child);
    write_u16(page, INT_OFF_COUNT, (count + 1) as u16);
    true
}

/// Split a full internal node. The median key is returned (pushed to parent),
/// `page` retains the left half, `right` receives the right half.
pub fn internal_split(
    page: &mut [u8; IPAGE_SIZE],
    right: &mut [u8; IPAGE_SIZE],
) -> u64 {
    let height = internal_height(page);
    let count = internal_count(page) as usize;
    let mid = count / 2;
    let push_up = internal_key(page, mid);

    init_internal(right, height);
    let right_count = count - mid - 1;
    for i in 0..right_count {
        internal_set_key(right, i, internal_key(page, mid + 1 + i));
    }
    for i in 0..=right_count {
        internal_set_child(right, i, internal_child(page, mid + 1 + i));
    }
    write_u16(right, INT_OFF_COUNT, right_count as u16);

    // Clear moved keys/children from left page
    for i in mid..count {
        write_u64(page, INT_KEYS_START + i * 8, 0);
    }
    for i in mid + 1..=count {
        write_u32(page, INT_CHILDREN_START + i * 4, 0);
    }
    write_u16(page, INT_OFF_COUNT, mid as u16);

    push_up
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaf_insert_and_search() {
        let mut page = [0u8; IPAGE_SIZE];
        init_leaf(&mut page);
        assert_eq!(leaf_count(&page), 0);

        for i in 0u64..10 {
            let e = LeafEntry {
                key_hash: i * 10,
                value_ptr: i * 100,
                value_len: 42,
                key_len: 8,
                flags: FLAG_ALIVE,
            };
            let pos = leaf_lower_bound(&page, e.key_hash);
            assert!(leaf_insert(&mut page, pos, &e));
        }
        assert_eq!(leaf_count(&page), 10);

        let pos = leaf_lower_bound(&page, 50);
        let e = leaf_entry_at(&page, pos);
        assert_eq!(e.key_hash, 50);
        assert_eq!(e.value_ptr, 500);
    }

    #[test]
    fn leaf_split_halves() {
        let mut page = [0u8; IPAGE_SIZE];
        init_leaf(&mut page);

        for i in 0u64..MAX_LEAF_ENTRIES as u64 {
            let e = LeafEntry {
                key_hash: i,
                value_ptr: i * 10,
                value_len: 4,
                key_len: 4,
                flags: FLAG_ALIVE,
            };
            let pos = leaf_lower_bound(&page, i);
            assert!(leaf_insert(&mut page, pos, &e));
        }
        assert_eq!(leaf_count(&page), MAX_LEAF_ENTRIES as u16);

        let mut right = [0u8; IPAGE_SIZE];
        let sep = leaf_split(&mut page, &mut right);

        let left_cnt = leaf_count(&page) as usize;
        let right_cnt = leaf_count(&right) as usize;
        assert_eq!(left_cnt + right_cnt, MAX_LEAF_ENTRIES);
        assert_eq!(leaf_entry_at(&right, 0).key_hash, sep);
        assert!(left_cnt >= right_cnt - 1 && left_cnt <= right_cnt + 1);
    }

    #[test]
    fn internal_node_operations() {
        let mut page = [0u8; IPAGE_SIZE];
        init_internal(&mut page, 1);

        // Set up an initial left child (child 0)
        internal_set_child(&mut page, 0, 42);

        // Insert several separator keys with right children
        for i in 0u64..10 {
            let _pos = internal_find_child(&page, i * 100 + 99);
            let cnt = internal_count(&page) as usize;
            // Insert at position `cnt` (append)
            assert!(internal_insert(&mut page, cnt, i * 100 + 100, (i + 1) as u32 + 42));
        }
        assert_eq!(internal_count(&page), 10);
        // Finding child for key=50 should route to first child (child 0)
        assert_eq!(internal_find_child(&page, 50), 0);
        // Finding child for key=150 should route to child 1
        assert_eq!(internal_find_child(&page, 150), 1);
    }
}
