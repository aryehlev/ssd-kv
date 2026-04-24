//! Lock-Free Index with Open Addressing (Aerospike SPRIGS-inspired)
//!
//! Features:
//! - Open addressing with linear probing (better cache locality than chaining)
//! - Lock-free reads using atomic operations
//! - Per-bucket locks only for writes (fine-grained locking)
//! - SIMD-accelerated key comparison
//! - Cache-line aligned buckets

use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::io;
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};

use crate::engine::index_entry::hash_key;
use crate::storage::write_buffer::DiskLocation;

/// Cache line size (64 bytes on most modern CPUs).
const CACHE_LINE_SIZE: usize = 64;

/// Maximum key size for inline storage.
const MAX_INLINE_KEY: usize = 24;

/// Size of disk location data.
const DISK_LOCATION_SIZE: usize = 16;

/// Bucket states.
const BUCKET_EMPTY: u8 = 0;
const BUCKET_OCCUPIED: u8 = 1;
const BUCKET_DELETED: u8 = 2;
const BUCKET_LOCKED: u8 = 3;

/// A single bucket in the hash table.
/// Designed to fit in a cache line (64 bytes).
/// Values are always stored on disk, only keys and metadata in memory.
#[repr(C, align(64))]
pub struct Bucket {
    /// Bucket state (empty, occupied, deleted, locked)
    state: AtomicU8,
    /// Key length
    key_len: u8,
    /// Reserved for alignment
    _reserved: u8,
    /// Flags (inline_key)
    flags: u8,
    /// Generation number
    generation: AtomicU64,
    /// Key hash (for fast comparison)
    key_hash: u64,
    /// Inline key (up to 24 bytes)
    inline_key: [u8; MAX_INLINE_KEY],
    /// Disk location (16 bytes)
    disk_location: [u8; DISK_LOCATION_SIZE],
}

const _: () = assert!(mem::size_of::<Bucket>() <= 64); // Fits in 1 cache line

/// Flags for bucket data.
const FLAG_INLINE_KEY: u8 = 0x01;

impl Bucket {
    /// Create an empty bucket.
    pub const fn empty() -> Self {
        Self {
            state: AtomicU8::new(BUCKET_EMPTY),
            key_len: 0,
            _reserved: 0,
            flags: 0,
            generation: AtomicU64::new(0),
            key_hash: 0,
            inline_key: [0; MAX_INLINE_KEY],
            disk_location: [0; DISK_LOCATION_SIZE],
        }
    }

    /// Check if bucket is empty (never written or fully cleared).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.state.load(Ordering::Acquire) == BUCKET_EMPTY
    }

    /// Check if bucket is deleted (tombstone — must not stop probing here).
    #[inline]
    pub fn is_deleted(&self) -> bool {
        self.state.load(Ordering::Acquire) == BUCKET_DELETED
    }

    /// Check if bucket is empty or deleted.
    #[inline]
    pub fn is_available(&self) -> bool {
        let state = self.state.load(Ordering::Acquire);
        state == BUCKET_EMPTY || state == BUCKET_DELETED
    }

    /// Check if bucket is occupied.
    #[inline]
    pub fn is_occupied(&self) -> bool {
        self.state.load(Ordering::Acquire) == BUCKET_OCCUPIED
    }

    /// Try to lock the bucket for writing.
    #[inline]
    pub fn try_lock(&self) -> bool {
        self.state
            .compare_exchange(
                BUCKET_OCCUPIED,
                BUCKET_LOCKED,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_ok()
            || self
                .state
                .compare_exchange(
                    BUCKET_EMPTY,
                    BUCKET_LOCKED,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            || self
                .state
                .compare_exchange(
                    BUCKET_DELETED,
                    BUCKET_LOCKED,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
    }

    /// Unlock the bucket, setting it to occupied.
    #[inline]
    pub fn unlock_occupied(&self) {
        self.state.store(BUCKET_OCCUPIED, Ordering::Release);
    }

    /// Unlock the bucket, marking it as deleted (preserves linear probing chains).
    #[inline]
    pub fn unlock_deleted(&self) {
        self.state.store(BUCKET_DELETED, Ordering::Release);
    }

    /// Try to lock the bucket only if it is currently OCCUPIED.
    /// Used by update/delete paths to avoid locking tombstones or empty buckets.
    #[inline]
    pub fn try_lock_occupied(&self) -> bool {
        self.state
            .compare_exchange(
                BUCKET_OCCUPIED,
                BUCKET_LOCKED,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    /// Check if key matches (fast path: compare hash first).
    #[inline]
    pub fn key_matches(&self, key: &[u8], key_hash: u64) -> bool {
        if self.key_hash != key_hash {
            return false;
        }

        if self.key_len as usize != key.len() {
            return false;
        }

        if self.flags & FLAG_INLINE_KEY != 0 {
            // Inline key - direct comparison
            &self.inline_key[..self.key_len as usize] == key
        } else {
            // TODO: External key comparison
            false
        }
    }

    /// Get disk location.
    #[inline]
    pub fn get_disk_location(&self) -> DiskLocation {
        // Layout: file_id (4 bytes) | wblock_id (2 bytes) | offset (4 bytes) | record_size (4 bytes)
        DiskLocation {
            file_id: u32::from_le_bytes([
                self.disk_location[0],
                self.disk_location[1],
                self.disk_location[2],
                self.disk_location[3],
            ]),
            wblock_id: u16::from_le_bytes([self.disk_location[4], self.disk_location[5]]),
            offset: u32::from_le_bytes([
                self.disk_location[6],
                self.disk_location[7],
                self.disk_location[8],
                self.disk_location[9],
            ]),
            record_size: u32::from_le_bytes([
                self.disk_location[10],
                self.disk_location[11],
                self.disk_location[12],
                self.disk_location[13],
            ]),
        }
    }

    /// Set entry data (values always on disk).
    pub fn set(
        &mut self,
        key: &[u8],
        key_hash: u64,
        location: &DiskLocation,
        generation: u64,
    ) {
        self.key_hash = key_hash;
        self.key_len = key.len() as u8;
        self.flags = 0;

        // Store key
        if key.len() <= MAX_INLINE_KEY {
            self.inline_key[..key.len()].copy_from_slice(key);
            self.flags |= FLAG_INLINE_KEY;
        }

        // Store disk location
        // Layout: file_id (4 bytes) | wblock_id (2 bytes) | offset (4 bytes) | record_size (4 bytes)
        self.disk_location[0..4].copy_from_slice(&location.file_id.to_le_bytes());
        self.disk_location[4..6].copy_from_slice(&location.wblock_id.to_le_bytes());
        self.disk_location[6..10].copy_from_slice(&location.offset.to_le_bytes());
        self.disk_location[10..14].copy_from_slice(&location.record_size.to_le_bytes());
        // Release store AFTER all field writes so that a reader's Acquire load
        // of generation guarantees visibility of the complete bucket update.
        self.generation.store(generation, Ordering::Release);
    }
}

/// Lock-free hash table with open addressing.
///
/// Keys larger than `MAX_INLINE_KEY` (24 bytes) live in the `heap_keys`
/// side table, keyed by bucket index. The `flags` byte in the bucket
/// tells readers which place to look. This avoids breaking the 64-byte
/// cache-line layout of Bucket while supporting arbitrary-length keys.
pub struct LockFreeIndex {
    /// Buckets array
    buckets: *mut Bucket,
    /// Side table for keys longer than `MAX_INLINE_KEY`. Keyed by the
    /// bucket index the key occupies.
    heap_keys: dashmap::DashMap<usize, std::sync::Arc<[u8]>>,
    /// Number of buckets (power of 2)
    capacity: usize,
    /// Mask for fast modulo (capacity - 1)
    mask: usize,
    /// Number of entries
    count: AtomicU64,
    /// Layout for deallocation
    layout: Layout,
}

unsafe impl Send for LockFreeIndex {}
unsafe impl Sync for LockFreeIndex {}

impl LockFreeIndex {
    /// Create a new lock-free index.
    pub fn new(capacity: usize) -> Self {
        // Round up to power of 2
        let capacity = capacity.next_power_of_two();

        let layout = Layout::array::<Bucket>(capacity).expect("Invalid layout");
        let buckets = unsafe { alloc_zeroed(layout) as *mut Bucket };

        if buckets.is_null() {
            panic!("Failed to allocate index");
        }

        // Initialize all buckets as empty
        for i in 0..capacity {
            unsafe {
                ptr::write(buckets.add(i), Bucket::empty());
            }
        }

        Self {
            buckets,
            heap_keys: dashmap::DashMap::new(),
            capacity,
            mask: capacity - 1,
            count: AtomicU64::new(0),
            layout,
        }
    }

    /// Does the bucket at `index` hold this key? Works for both inline and
    /// heap-stored keys; replaces the old `Bucket::key_matches` which
    /// silently returned `false` for any key > 24 bytes.
    #[inline]
    fn bucket_matches(&self, index: usize, key: &[u8], key_hash: u64) -> bool {
        let bucket = self.bucket(index);
        if bucket.key_hash != key_hash {
            return false;
        }
        if bucket.flags & FLAG_INLINE_KEY != 0 {
            if bucket.key_len as usize != key.len() {
                return false;
            }
            &bucket.inline_key[..bucket.key_len as usize] == key
        } else {
            // Heap-stored key: look up the side table.
            match self.heap_keys.get(&index) {
                Some(stored) => stored.as_ref() == key,
                None => false,
            }
        }
    }

    /// Get bucket at index.
    #[inline]
    fn bucket(&self, index: usize) -> &Bucket {
        unsafe { &*self.buckets.add(index) }
    }

    /// Get raw pointer to bucket at index.
    /// Returns `*mut Bucket` instead of `&mut Bucket` to avoid creating a
    /// mutable reference that would violate aliasing rules when an `&Bucket`
    /// from `bucket()` is still in scope.
    #[inline]
    fn bucket_ptr(&self, index: usize) -> *mut Bucket {
        unsafe { self.buckets.add(index) }
    }

    /// Find bucket for key (linear probing).
    /// Only stops at truly empty buckets — deleted (tombstone) buckets are skipped
    /// to preserve linear probing chains.
    #[inline]
    fn find_bucket(&self, key: &[u8], key_hash: u64) -> Option<usize> {
        let start = (key_hash as usize) & self.mask;

        for i in 0..self.capacity {
            let index = (start + i) & self.mask;
            let bucket = self.bucket(index);

            if bucket.is_empty() {
                return None; // Key not found — end of probing chain
            }

            if bucket.is_occupied() && self.bucket_matches(index, key, key_hash) {
                return Some(index);
            }
            // Deleted buckets: continue probing
        }

        None
    }

    /// Find empty bucket for insertion (linear probing).
    #[inline]
    fn find_empty_bucket(&self, key_hash: u64) -> Option<usize> {
        let start = (key_hash as usize) & self.mask;

        for i in 0..self.capacity {
            let index = (start + i) & self.mask;
            let bucket = self.bucket(index);

            if bucket.is_available() {
                return Some(index);
            }
        }

        None
    }

    /// GET - Lock-free read! Returns disk location for value.
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<DiskLocation> {
        let key_hash = hash_key(key);

        loop {
            let index = self.find_bucket(key, key_hash)?;
            let bucket = self.bucket(index);

            let gen1 = bucket.generation.load(Ordering::Acquire);
            let location = bucket.get_disk_location();
            let gen2 = bucket.generation.load(Ordering::Acquire);

            if gen1 != gen2 {
                // Concurrent write in progress, retry the whole lookup
                std::hint::spin_loop();
                continue;
            }

            // Re-validate: bucket may have been deleted or repurposed between
            // find_bucket and reading the generation-stable location.
            if bucket.is_occupied() && self.bucket_matches(index, key, key_hash) {
                return Some(location);
            }

            // Bucket changed or deleted between find_bucket and here; retry lookup
            return None;
        }
    }

    /// PUT - Uses fine-grained per-bucket locking. Values always on disk.
    /// Supports any key length: keys <= MAX_INLINE_KEY live inline in the
    /// bucket, longer keys are stored in the `heap_keys` side table.
    pub fn put(
        &self,
        key: &[u8],
        location: &DiskLocation,
        generation: u64,
    ) -> io::Result<()> {
        let key_hash = hash_key(key);

        // First, check if key already exists
        if let Some(index) = self.find_bucket(key, key_hash) {
            let bucket = self.bucket(index);

            // Spin until we can lock — only lock OCCUPIED buckets
            while !bucket.try_lock_occupied() {
                if !bucket.is_occupied() {
                    // Bucket was deleted/emptied while we waited; restart insertion
                    return self.put(key, location, generation);
                }
                std::hint::spin_loop();
            }

            // Re-verify key still matches after acquiring lock (could have been deleted/modified)
            if !self.bucket_matches(index, key, key_hash) {
                bucket.unlock_occupied();
                return self.put(key, location, generation);
            }

            // Update existing entry. Same key, so heap_keys stays put if it
            // was there; otherwise it was inline and stays inline.
            unsafe { &mut *self.bucket_ptr(index) }.set(key, key_hash, location, generation);
            bucket.unlock_occupied();

            return Ok(());
        }

        // Find empty bucket for new entry
        if let Some(index) = self.find_empty_bucket(key_hash) {
            let bucket = self.bucket(index);

            // Spin until we can lock
            while !bucket.try_lock() {
                std::hint::spin_loop();
            }

            // After acquiring lock, re-check that this key doesn't already exist
            // (another thread may have inserted it while we were waiting)
            if self.find_bucket(key, key_hash).is_some() {
                bucket.unlock_deleted();
                return self.put(key, location, generation);
            }

            // Drop any stale heap-key at this index (the bucket was
            // tombstoned by another key previously). We do this before
            // `set()` so a concurrent reader seeing the post-set bucket
            // won't transiently find a wrong heap key.
            self.heap_keys.remove(&index);

            // For long keys, install the heap-side entry FIRST, while the
            // bucket is still LOCKED — readers see state=LOCKED and skip.
            if key.len() > MAX_INLINE_KEY {
                self.heap_keys.insert(index, std::sync::Arc::from(key));
            }

            unsafe { &mut *self.bucket_ptr(index) }.set(key, key_hash, location, generation);
            bucket.unlock_occupied();

            self.count.fetch_add(1, Ordering::Relaxed);

            return Ok(());
        }

        Err(io::Error::new(io::ErrorKind::Other, "Index full"))
    }

    /// DELETE - Uses fine-grained per-bucket locking.
    pub fn delete(&self, key: &[u8]) -> bool {
        let key_hash = hash_key(key);

        if let Some(index) = self.find_bucket(key, key_hash) {
            let bucket = self.bucket(index);

            // Spin until we can lock — only lock OCCUPIED buckets
            while !bucket.try_lock_occupied() {
                if !bucket.is_occupied() {
                    // Bucket was already deleted by another thread
                    return false;
                }
                std::hint::spin_loop();
            }

            // Re-verify key matches after acquiring lock
            if !self.bucket_matches(index, key, key_hash) {
                bucket.unlock_occupied();
                return false;
            }

            // Mark as deleted. Drop the heap-side key first so no reader
            // ever sees a tombstoned bucket still pointing at a heap key.
            self.heap_keys.remove(&index);
            bucket.unlock_deleted();
            self.count.fetch_sub(1, Ordering::Relaxed);

            return true;
        }

        false
    }

    /// Get number of entries.
    pub fn len(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get load factor.
    pub fn load_factor(&self) -> f64 {
        self.len() as f64 / self.capacity as f64
    }
}

impl Drop for LockFreeIndex {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.buckets as *mut u8, self.layout);
        }
    }
}


// SIMD-accelerated key comparison (x86_64)
#[cfg(target_arch = "x86_64")]
pub fn compare_keys_simd(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    let len = a.len();

    // For small keys, use regular comparison
    if len < 32 {
        return a == b;
    }

    // Runtime AVX2 detection to avoid SIGILL on CPUs without AVX2
    if is_x86_feature_detected!("avx2") {
        // Safety: we just confirmed AVX2 is available at runtime.
        return unsafe { compare_keys_avx2(a, b) };
    }

    // Fallback for non-AVX2 CPUs
    a == b
}

/// AVX2 key comparison helper. Must only be called after runtime AVX2 check.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn compare_keys_avx2(a: &[u8], b: &[u8]) -> bool {
    use std::arch::x86_64::*;

    let len = a.len();
    let chunks = len / 32;
    let remainder = len % 32;

    for i in 0..chunks {
        let offset = i * 32;
        let va = _mm256_loadu_si256(a.as_ptr().add(offset) as *const __m256i);
        let vb = _mm256_loadu_si256(b.as_ptr().add(offset) as *const __m256i);
        let cmp = _mm256_cmpeq_epi8(va, vb);
        let mask = _mm256_movemask_epi8(cmp);
        if mask != -1 {
            return false;
        }
    }

    // Compare remainder
    if remainder > 0 {
        let offset = chunks * 32;
        return &a[offset..] == &b[offset..];
    }

    true
}

#[cfg(not(target_arch = "x86_64"))]
pub fn compare_keys_simd(a: &[u8], b: &[u8]) -> bool {
    a == b
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_basic() {
        let mut bucket = Bucket::empty();
        assert!(bucket.is_available());

        let location = DiskLocation::new(1, 2, 100);
        bucket.set(b"key", hash_key(b"key"), &location, 1);
        bucket.state.store(BUCKET_OCCUPIED, Ordering::Release);

        assert!(bucket.is_occupied());
        assert!(bucket.key_matches(b"key", hash_key(b"key")));
        let disk_loc = bucket.get_disk_location();
        assert_eq!(disk_loc.file_id, 1);
        assert_eq!(disk_loc.wblock_id, 2);
        assert_eq!(disk_loc.offset, 100);
    }

    #[test]
    fn test_lockfree_index() {
        let index = LockFreeIndex::new(1024);

        let loc1 = DiskLocation::new(1, 1, 100);
        let loc2 = DiskLocation::new(2, 2, 200);

        // PUT
        index.put(b"key1", &loc1, 1).unwrap();
        index.put(b"key2", &loc2, 2).unwrap();

        // GET
        let result = index.get(b"key1").expect("Expected disk location");
        assert_eq!(result.file_id, 1);
        assert_eq!(result.wblock_id, 1);
        assert_eq!(result.offset, 100);

        // DELETE
        assert!(index.delete(b"key1"));
        assert!(index.get(b"key1").is_none());

        assert_eq!(index.len(), 1);
    }

    #[test]
    fn test_compare_keys_simd() {
        let a = b"this is a test key for simd comparison";
        let b = b"this is a test key for simd comparison";
        let c = b"this is a test key for simd comparisox";

        assert!(compare_keys_simd(a, b));
        assert!(!compare_keys_simd(a, c));
    }

    #[test]
    fn long_keys_round_trip_through_heap_side_table() {
        // Keys longer than MAX_INLINE_KEY (24 bytes) used to be rejected
        // by put() and silently mishandled by key_matches. Verify they
        // now store/retrieve correctly.
        let index = LockFreeIndex::new(64);
        let loc = DiskLocation::new(7, 1, 42);

        let short = b"short";
        let long = b"this-is-a-long-key-that-exceeds-the-24-byte-inline-limit";
        let other_long = b"another-long-key-of-meaningfully-different-content-here";

        index.put(short, &loc, 1).unwrap();
        index.put(long, &loc, 2).unwrap();
        index.put(other_long, &loc, 3).unwrap();

        assert!(index.get(short).is_some(), "short key missing");
        assert!(index.get(long).is_some(), "long key missing");
        assert!(index.get(other_long).is_some(), "other long key missing");

        // Update the long key and re-read.
        let loc2 = DiskLocation::new(8, 2, 99);
        index.put(long, &loc2, 4).unwrap();
        assert!(index.get(long).is_some());

        // Delete and verify gone.
        assert!(index.delete(long));
        assert!(index.get(long).is_none());
        // The other long key must be unaffected.
        assert!(index.get(other_long).is_some());
    }

    #[test]
    fn long_key_collision_handled_correctly() {
        // Two different long keys with similar-but-not-equal content
        // shouldn't match each other.
        let index = LockFreeIndex::new(64);
        let loc = DiskLocation::new(0, 0, 0);

        let k1 = b"prefix/common-path/object-id-0000000000000001";
        let k2 = b"prefix/common-path/object-id-0000000000000002";

        index.put(k1, &loc, 1).unwrap();
        index.put(k2, &loc, 2).unwrap();

        assert!(index.get(k1).is_some());
        assert!(index.get(k2).is_some());
        // Neither of these is in the index.
        assert!(index.get(b"prefix/common-path/object-id-0000000000000003").is_none());
    }
}
