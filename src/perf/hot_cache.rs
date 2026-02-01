//! Lock-free hot cache for frequently accessed keys.
//!
//! Uses a sharded design with lock-free reads and minimal contention writes.
//! Implements LRU eviction with approximate counting for cache-friendliness.

use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

/// Number of cache shards (power of 2 for fast modulo).
const NUM_SHARDS: usize = 64;

/// Entries per shard.
const ENTRIES_PER_SHARD: usize = 4096;

/// Cache entry states.
const STATE_EMPTY: u8 = 0;
const STATE_WRITING: u8 = 1;
const STATE_VALID: u8 = 2;
const STATE_EVICTING: u8 = 3;

/// A single cache entry with atomic state management.
#[repr(C, align(64))] // Cache-line aligned
pub struct CacheEntry {
    /// Entry state for lock-free operations.
    state: AtomicU8,
    /// Access counter for approximate LRU.
    access_count: AtomicU8,
    /// Key hash for fast comparison.
    key_hash: AtomicU64,
    /// Generation for conflict detection.
    generation: AtomicU32,
    /// Value length.
    value_len: AtomicU32,
    /// Inline key (up to 24 bytes).
    key: [u8; 24],
    /// Inline value (up to 128 bytes for hot data).
    value: [u8; 128],
}

impl CacheEntry {
    const fn new() -> Self {
        Self {
            state: AtomicU8::new(STATE_EMPTY),
            access_count: AtomicU8::new(0),
            key_hash: AtomicU64::new(0),
            generation: AtomicU32::new(0),
            value_len: AtomicU32::new(0),
            key: [0; 24],
            value: [0; 128],
        }
    }

    /// Tries to read the value without locking.
    /// Returns None if entry is invalid or being modified.
    #[inline]
    pub fn try_read(&self, key: &[u8], key_hash: u64) -> Option<Vec<u8>> {
        // Check state first (fast path)
        if self.state.load(Ordering::Acquire) != STATE_VALID {
            return None;
        }

        // Verify key hash
        if self.key_hash.load(Ordering::Relaxed) != key_hash {
            return None;
        }

        // Verify key length
        if key.len() > 24 || key.len() != self.key[..key.len()].len() {
            return None;
        }

        // Compare keys
        if &self.key[..key.len()] != key {
            return None;
        }

        // Double-check state hasn't changed
        if self.state.load(Ordering::Acquire) != STATE_VALID {
            return None;
        }

        // Read value
        let value_len = self.value_len.load(Ordering::Relaxed) as usize;
        if value_len > 128 {
            return None; // Value too large for inline cache
        }

        // Increment access counter (saturating)
        let _ = self.access_count.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |c| Some(c.saturating_add(1)),
        );

        Some(self.value[..value_len].to_vec())
    }

    /// Tries to write a value using compare-and-swap.
    #[inline]
    pub fn try_write(&mut self, key: &[u8], key_hash: u64, value: &[u8], generation: u32) -> bool {
        // Only cache small keys and values
        if key.len() > 24 || value.len() > 128 {
            return false;
        }

        // Try to acquire write lock via state transition
        match self.state.compare_exchange(
            STATE_EMPTY,
            STATE_WRITING,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            Ok(_) => {}
            Err(STATE_VALID) => {
                // Entry exists, check if we should update
                if self.generation.load(Ordering::Relaxed) >= generation {
                    return false; // Stale write
                }
                // Try to transition to writing
                if self
                    .state
                    .compare_exchange(
                        STATE_VALID,
                        STATE_WRITING,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    )
                    .is_err()
                {
                    return false;
                }
            }
            Err(_) => return false,
        }

        // Write data
        self.key[..key.len()].copy_from_slice(key);
        self.value[..value.len()].copy_from_slice(value);
        self.key_hash.store(key_hash, Ordering::Relaxed);
        self.value_len.store(value.len() as u32, Ordering::Relaxed);
        self.generation.store(generation, Ordering::Relaxed);
        self.access_count.store(1, Ordering::Relaxed);

        // Publish
        self.state.store(STATE_VALID, Ordering::Release);
        true
    }

    /// Invalidates the entry.
    #[inline]
    pub fn invalidate(&self, key_hash: u64) -> bool {
        if self.key_hash.load(Ordering::Relaxed) == key_hash {
            self.state.store(STATE_EMPTY, Ordering::Release);
            true
        } else {
            false
        }
    }
}

/// A cache shard containing multiple entries.
struct CacheShard {
    entries: Box<[CacheEntry; ENTRIES_PER_SHARD]>,
    /// Clock hand for approximate LRU eviction.
    clock_hand: AtomicU32,
}

impl CacheShard {
    fn new() -> Self {
        // Use Box to avoid stack overflow with large arrays
        let entries = unsafe {
            let layout = std::alloc::Layout::new::<[CacheEntry; ENTRIES_PER_SHARD]>();
            let ptr = std::alloc::alloc_zeroed(layout) as *mut [CacheEntry; ENTRIES_PER_SHARD];
            Box::from_raw(ptr)
        };

        Self {
            entries,
            clock_hand: AtomicU32::new(0),
        }
    }

    #[inline]
    fn get_slot(&self, key_hash: u64) -> usize {
        (key_hash as usize) % ENTRIES_PER_SHARD
    }

    fn get(&self, key: &[u8], key_hash: u64) -> Option<Vec<u8>> {
        let slot = self.get_slot(key_hash);

        // Try primary slot
        if let Some(v) = self.entries[slot].try_read(key, key_hash) {
            return Some(v);
        }

        // Try next few slots (linear probing)
        for i in 1..4 {
            let probe_slot = (slot + i) % ENTRIES_PER_SHARD;
            if let Some(v) = self.entries[probe_slot].try_read(key, key_hash) {
                return Some(v);
            }
        }

        None
    }

    fn put(&mut self, key: &[u8], key_hash: u64, value: &[u8], generation: u32) {
        let slot = self.get_slot(key_hash);

        // Try primary slot
        if self.entries[slot].try_write(key, key_hash, value, generation) {
            return;
        }

        // Try linear probing
        for i in 1..4 {
            let probe_slot = (slot + i) % ENTRIES_PER_SHARD;
            if self.entries[probe_slot].try_write(key, key_hash, value, generation) {
                return;
            }
        }

        // Evict using clock algorithm
        self.evict_and_insert(key, key_hash, value, generation);
    }

    fn evict_and_insert(&mut self, key: &[u8], key_hash: u64, value: &[u8], generation: u32) {
        let start = self.clock_hand.load(Ordering::Relaxed) as usize;

        for i in 0..ENTRIES_PER_SHARD {
            let slot = (start + i) % ENTRIES_PER_SHARD;
            let entry = &mut self.entries[slot];

            let access = entry.access_count.load(Ordering::Relaxed);
            if access == 0 {
                // Found victim, try to evict
                if entry
                    .state
                    .compare_exchange(
                        STATE_VALID,
                        STATE_EVICTING,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    entry.state.store(STATE_EMPTY, Ordering::Release);
                    if entry.try_write(key, key_hash, value, generation) {
                        self.clock_hand.store((slot + 1) as u32, Ordering::Relaxed);
                        return;
                    }
                }
            } else {
                // Decrement access count
                entry.access_count.fetch_sub(1, Ordering::Relaxed);
            }
        }

        // Update clock hand even if we couldn't insert
        self.clock_hand
            .store(((start + ENTRIES_PER_SHARD / 2) % ENTRIES_PER_SHARD) as u32, Ordering::Relaxed);
    }

    fn invalidate(&self, key_hash: u64) {
        let slot = self.get_slot(key_hash);
        self.entries[slot].invalidate(key_hash);

        // Also check probed slots
        for i in 1..4 {
            let probe_slot = (slot + i) % ENTRIES_PER_SHARD;
            self.entries[probe_slot].invalidate(key_hash);
        }
    }
}

/// Lock-free hot cache for frequently accessed small key-value pairs.
pub struct HotCache {
    shards: Vec<RwLock<CacheShard>>,
}

impl HotCache {
    /// Creates a new hot cache.
    pub fn new() -> Self {
        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            shards.push(RwLock::new(CacheShard::new()));
        }
        Self { shards }
    }

    #[inline]
    fn shard_for(&self, key_hash: u64) -> usize {
        ((key_hash >> 48) as usize) % NUM_SHARDS
    }

    /// Gets a value from the cache. Returns None if not cached.
    #[inline]
    pub fn get(&self, key: &[u8], key_hash: u64) -> Option<Vec<u8>> {
        let shard_idx = self.shard_for(key_hash);
        let shard = self.shards[shard_idx].read();
        shard.get(key, key_hash)
    }

    /// Puts a value into the cache.
    #[inline]
    pub fn put(&self, key: &[u8], key_hash: u64, value: &[u8], generation: u32) {
        // Only cache small values
        if key.len() > 24 || value.len() > 128 {
            return;
        }

        let shard_idx = self.shard_for(key_hash);
        let mut shard = self.shards[shard_idx].write();
        shard.put(key, key_hash, value, generation);
    }

    /// Invalidates a cache entry.
    #[inline]
    pub fn invalidate(&self, key_hash: u64) {
        let shard_idx = self.shard_for(key_hash);
        let shard = self.shards[shard_idx].read();
        shard.invalidate(key_hash);
    }

    /// Returns cache statistics.
    pub fn stats(&self) -> CacheStats {
        let mut valid_entries = 0u64;
        for shard in &self.shards {
            let s = shard.read();
            for entry in s.entries.iter() {
                if entry.state.load(Ordering::Relaxed) == STATE_VALID {
                    valid_entries += 1;
                }
            }
        }
        CacheStats {
            capacity: (NUM_SHARDS * ENTRIES_PER_SHARD) as u64,
            used: valid_entries,
        }
    }
}

impl Default for HotCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Cache statistics.
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub capacity: u64,
    pub used: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hot_cache_basic() {
        let cache = HotCache::new();
        let key = b"test_key";
        let value = b"test_value";
        let key_hash = xxhash_rust::xxh3::xxh3_64(key);

        // Initially empty
        assert!(cache.get(key, key_hash).is_none());

        // Put and get
        cache.put(key, key_hash, value, 1);
        let result = cache.get(key, key_hash);
        assert_eq!(result, Some(value.to_vec()));
    }

    #[test]
    fn test_hot_cache_update() {
        let cache = HotCache::new();
        let key = b"update_key";
        let key_hash = xxhash_rust::xxh3::xxh3_64(key);

        cache.put(key, key_hash, b"value1", 1);
        cache.put(key, key_hash, b"value2", 2);

        let result = cache.get(key, key_hash);
        assert_eq!(result, Some(b"value2".to_vec()));
    }

    #[test]
    fn test_hot_cache_invalidate() {
        let cache = HotCache::new();
        let key = b"inv_key";
        let key_hash = xxhash_rust::xxh3::xxh3_64(key);

        cache.put(key, key_hash, b"value", 1);
        assert!(cache.get(key, key_hash).is_some());

        cache.invalidate(key_hash);
        assert!(cache.get(key, key_hash).is_none());
    }
}
