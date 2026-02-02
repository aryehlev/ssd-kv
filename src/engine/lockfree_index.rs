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

/// Maximum value size for inline storage.
const MAX_INLINE_VALUE: usize = 64;

/// Bucket states.
const BUCKET_EMPTY: u8 = 0;
const BUCKET_OCCUPIED: u8 = 1;
const BUCKET_DELETED: u8 = 2;
const BUCKET_LOCKED: u8 = 3;

/// A single bucket in the hash table.
/// Designed to fit in a cache line (64 bytes).
#[repr(C, align(64))]
pub struct Bucket {
    /// Bucket state (empty, occupied, deleted, locked)
    state: AtomicU8,
    /// Key length
    key_len: u8,
    /// Value length (if inline)
    value_len: u8,
    /// Flags (inline_key, inline_value, on_disk)
    flags: u8,
    /// Generation number
    generation: AtomicU32,
    /// Key hash (for fast comparison)
    key_hash: u64,
    /// Inline key (up to 24 bytes)
    inline_key: [u8; MAX_INLINE_KEY],
    /// Inline value (up to 64 bytes) OR disk location (16 bytes)
    data: [u8; MAX_INLINE_VALUE],
}

const _: () = assert!(mem::size_of::<Bucket>() <= 128); // Fits in 2 cache lines

/// Flags for bucket data.
const FLAG_INLINE_KEY: u8 = 0x01;
const FLAG_INLINE_VALUE: u8 = 0x02;
const FLAG_ON_DISK: u8 = 0x04;

impl Bucket {
    /// Create an empty bucket.
    pub const fn empty() -> Self {
        Self {
            state: AtomicU8::new(BUCKET_EMPTY),
            key_len: 0,
            value_len: 0,
            flags: 0,
            generation: AtomicU32::new(0),
            key_hash: 0,
            inline_key: [0; MAX_INLINE_KEY],
            data: [0; MAX_INLINE_VALUE],
        }
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

    /// Unlock the bucket.
    #[inline]
    pub fn unlock(&self, occupied: bool) {
        self.state.store(
            if occupied { BUCKET_OCCUPIED } else { BUCKET_EMPTY },
            Ordering::Release,
        );
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

    /// Get inline value (if available).
    #[inline]
    pub fn get_inline_value(&self) -> Option<&[u8]> {
        if self.flags & FLAG_INLINE_VALUE != 0 {
            Some(&self.data[..self.value_len as usize])
        } else {
            None
        }
    }

    /// Get disk location (if value is on disk).
    #[inline]
    pub fn get_disk_location(&self) -> Option<DiskLocation> {
        if self.flags & FLAG_ON_DISK != 0 {
            // Deserialize DiskLocation from data
            // Layout: file_id (4 bytes) | wblock_id (2 bytes) | offset (4 bytes) | record_size (4 bytes)
            Some(DiskLocation {
                file_id: u32::from_le_bytes([self.data[0], self.data[1], self.data[2], self.data[3]]),
                wblock_id: u16::from_le_bytes([self.data[4], self.data[5]]),
                offset: u32::from_le_bytes([self.data[6], self.data[7], self.data[8], self.data[9]]),
                record_size: u32::from_le_bytes([
                    self.data[10],
                    self.data[11],
                    self.data[12],
                    self.data[13],
                ]),
            })
        } else {
            None
        }
    }

    /// Set entry data.
    pub fn set(
        &mut self,
        key: &[u8],
        key_hash: u64,
        value: Option<&[u8]>,
        location: Option<&DiskLocation>,
        generation: u32,
    ) {
        self.key_hash = key_hash;
        self.key_len = key.len() as u8;
        self.generation.store(generation, Ordering::Release);
        self.flags = 0;

        // Store key
        if key.len() <= MAX_INLINE_KEY {
            self.inline_key[..key.len()].copy_from_slice(key);
            self.flags |= FLAG_INLINE_KEY;
        }

        // Store value or location
        if let Some(value) = value {
            if value.len() <= MAX_INLINE_VALUE {
                self.data[..value.len()].copy_from_slice(value);
                self.value_len = value.len() as u8;
                self.flags |= FLAG_INLINE_VALUE;
            }
        } else if let Some(loc) = location {
            // Layout: file_id (4 bytes) | wblock_id (2 bytes) | offset (4 bytes) | record_size (4 bytes)
            self.data[0..4].copy_from_slice(&loc.file_id.to_le_bytes());
            self.data[4..6].copy_from_slice(&loc.wblock_id.to_le_bytes());
            self.data[6..10].copy_from_slice(&loc.offset.to_le_bytes());
            self.data[10..14].copy_from_slice(&loc.record_size.to_le_bytes());
            self.flags |= FLAG_ON_DISK;
        }
    }
}

/// Lock-free hash table with open addressing.
pub struct LockFreeIndex {
    /// Buckets array
    buckets: *mut Bucket,
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
            capacity,
            mask: capacity - 1,
            count: AtomicU64::new(0),
            layout,
        }
    }

    /// Get bucket at index.
    #[inline]
    fn bucket(&self, index: usize) -> &Bucket {
        unsafe { &*self.buckets.add(index) }
    }

    /// Get mutable bucket at index.
    #[inline]
    fn bucket_mut(&self, index: usize) -> &mut Bucket {
        unsafe { &mut *self.buckets.add(index) }
    }

    /// Find bucket for key (linear probing).
    #[inline]
    fn find_bucket(&self, key: &[u8], key_hash: u64) -> Option<usize> {
        let start = (key_hash as usize) & self.mask;

        for i in 0..self.capacity {
            let index = (start + i) & self.mask;
            let bucket = self.bucket(index);

            if bucket.is_available() {
                return None; // Key not found
            }

            if bucket.is_occupied() && bucket.key_matches(key, key_hash) {
                return Some(index);
            }
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

    /// GET - Lock-free read!
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<GetResult> {
        let key_hash = hash_key(key);

        if let Some(index) = self.find_bucket(key, key_hash) {
            let bucket = self.bucket(index);

            // Read generation before and after to detect concurrent modification
            let gen1 = bucket.generation.load(Ordering::Acquire);

            let result = if let Some(value) = bucket.get_inline_value() {
                Some(GetResult::Inline(value.to_vec()))
            } else if let Some(location) = bucket.get_disk_location() {
                Some(GetResult::OnDisk(location))
            } else {
                None
            };

            let gen2 = bucket.generation.load(Ordering::Acquire);

            // If generation changed, retry
            if gen1 != gen2 {
                return self.get(key); // Recursive retry
            }

            return result;
        }

        None
    }

    /// PUT - Uses fine-grained per-bucket locking.
    pub fn put(
        &self,
        key: &[u8],
        value: Option<&[u8]>,
        location: Option<&DiskLocation>,
        generation: u32,
    ) -> io::Result<()> {
        let key_hash = hash_key(key);

        // First, check if key already exists
        if let Some(index) = self.find_bucket(key, key_hash) {
            let bucket = self.bucket(index);

            // Spin until we can lock
            while !bucket.try_lock() {
                std::hint::spin_loop();
            }

            // Update existing entry
            let bucket_mut = self.bucket_mut(index);
            bucket_mut.set(key, key_hash, value, location, generation);
            bucket.unlock(true);

            return Ok(());
        }

        // Find empty bucket for new entry
        if let Some(index) = self.find_empty_bucket(key_hash) {
            let bucket = self.bucket(index);

            // Spin until we can lock
            while !bucket.try_lock() {
                std::hint::spin_loop();
            }

            // Double-check it's still available
            if !self.bucket(index).is_available() {
                bucket.unlock(false);
                return self.put(key, value, location, generation); // Retry
            }

            // Insert new entry
            let bucket_mut = self.bucket_mut(index);
            bucket_mut.set(key, key_hash, value, location, generation);
            bucket.unlock(true);

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

            // Spin until we can lock
            while !bucket.try_lock() {
                std::hint::spin_loop();
            }

            // Mark as deleted
            bucket.unlock(false);
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

/// Result of a GET operation.
#[derive(Debug, Clone)]
pub enum GetResult {
    /// Value stored inline
    Inline(Vec<u8>),
    /// Value on disk
    OnDisk(DiskLocation),
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

    // Use AVX2 for 32-byte chunks
    #[cfg(target_feature = "avx2")]
    {
        use std::arch::x86_64::*;

        let chunks = len / 32;
        let remainder = len % 32;

        for i in 0..chunks {
            let offset = i * 32;
            unsafe {
                let va = _mm256_loadu_si256(a.as_ptr().add(offset) as *const __m256i);
                let vb = _mm256_loadu_si256(b.as_ptr().add(offset) as *const __m256i);
                let cmp = _mm256_cmpeq_epi8(va, vb);
                let mask = _mm256_movemask_epi8(cmp);
                if mask != -1 {
                    return false;
                }
            }
        }

        // Compare remainder
        if remainder > 0 {
            let offset = chunks * 32;
            return &a[offset..] == &b[offset..];
        }

        return true;
    }

    // Fallback
    #[cfg(not(target_feature = "avx2"))]
    {
        a == b
    }
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

        bucket.set(b"key", hash_key(b"key"), Some(b"value"), None, 1);
        bucket.state.store(BUCKET_OCCUPIED, Ordering::Release);

        assert!(bucket.is_occupied());
        assert!(bucket.key_matches(b"key", hash_key(b"key")));
        assert_eq!(bucket.get_inline_value(), Some(b"value".as_slice()));
    }

    #[test]
    fn test_lockfree_index() {
        let index = LockFreeIndex::new(1024);

        // PUT
        index.put(b"key1", Some(b"value1"), None, 1).unwrap();
        index.put(b"key2", Some(b"value2"), None, 2).unwrap();

        // GET
        match index.get(b"key1") {
            Some(GetResult::Inline(v)) => assert_eq!(v, b"value1"),
            _ => panic!("Expected inline value"),
        }

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
}
