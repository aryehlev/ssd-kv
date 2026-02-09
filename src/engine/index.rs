//! Sharded hashmap index with 256 shards for concurrent access.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use crate::engine::index_entry::{hash_key, IndexEntry};
use crate::storage::write_buffer::DiskLocation;

/// Number of shards in the index.
pub const NUM_SHARDS: usize = 256;

/// A single shard of the index.
pub struct IndexShard {
    /// The actual data: key hash -> entry.
    /// We use the full key hash as the map key, with collision handling via equals check.
    entries: HashMap<u64, Vec<IndexEntry>>,
    /// Number of live entries in this shard.
    live_count: usize,
    /// Number of deleted entries (tombstones).
    tombstone_count: usize,
}

impl IndexShard {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            live_count: 0,
            tombstone_count: 0,
        }
    }

    /// Looks up an entry by key.
    pub fn get(&self, key: &[u8], key_hash: u64) -> Option<&IndexEntry> {
        self.entries.get(&key_hash).and_then(|entries| {
            entries.iter().find(|e| e.matches(key, key_hash) && e.is_live())
        })
    }

    /// Inserts or updates an entry.
    /// Returns the old entry if it existed.
    pub fn insert(&mut self, entry: IndexEntry) -> Option<IndexEntry> {
        let key_hash = entry.key_hash;
        let key = entry.key.as_bytes().to_vec();

        let bucket = self.entries.entry(key_hash).or_insert_with(Vec::new);

        // Look for existing entry with same key
        for existing in bucket.iter_mut() {
            if existing.key.as_bytes() == key {
                // Check generation for conflict resolution
                if entry.generation > existing.generation {
                    let old = std::mem::replace(existing, entry);
                    if old.is_deleted() && !existing.is_deleted() {
                        self.tombstone_count = self.tombstone_count.saturating_sub(1);
                        self.live_count += 1;
                    } else if !old.is_deleted() && existing.is_deleted() {
                        self.live_count = self.live_count.saturating_sub(1);
                        self.tombstone_count += 1;
                    }
                    return Some(old);
                } else {
                    // Existing entry has higher or equal generation, ignore
                    return Some(entry);
                }
            }
        }

        // New entry
        if entry.is_deleted() {
            self.tombstone_count += 1;
        } else {
            self.live_count += 1;
        }
        bucket.push(entry);
        None
    }

    /// Marks an entry as deleted.
    /// Returns true if the entry was found and deleted.
    pub fn delete(&mut self, key: &[u8], key_hash: u64, generation: u32) -> bool {
        if let Some(bucket) = self.entries.get_mut(&key_hash) {
            for entry in bucket.iter_mut() {
                if entry.matches(key, key_hash) {
                    if generation > entry.generation {
                        if entry.is_live() {
                            self.live_count = self.live_count.saturating_sub(1);
                            self.tombstone_count += 1;
                        }
                        entry.mark_deleted(generation);
                        return true;
                    }
                    return false; // Stale delete
                }
            }
        }
        false
    }

    /// Removes an entry completely (for compaction).
    pub fn remove(&mut self, key: &[u8], key_hash: u64) -> Option<IndexEntry> {
        if let Some(bucket) = self.entries.get_mut(&key_hash) {
            if let Some(pos) = bucket.iter().position(|e| e.matches(key, key_hash)) {
                let entry = bucket.swap_remove(pos);
                if bucket.is_empty() {
                    self.entries.remove(&key_hash);
                }
                if entry.is_deleted() {
                    self.tombstone_count = self.tombstone_count.saturating_sub(1);
                } else {
                    self.live_count = self.live_count.saturating_sub(1);
                }
                return Some(entry);
            }
        }
        None
    }

    /// Returns the number of live entries.
    pub fn live_count(&self) -> usize {
        self.live_count
    }

    /// Returns the number of tombstones.
    pub fn tombstone_count(&self) -> usize {
        self.tombstone_count
    }

    /// Iterates over all live entries.
    pub fn iter_live(&self) -> impl Iterator<Item = &IndexEntry> {
        self.entries
            .values()
            .flat_map(|bucket| bucket.iter())
            .filter(|e| e.is_live())
    }

    /// Iterates over all entries (including tombstones).
    pub fn iter_all(&self) -> impl Iterator<Item = &IndexEntry> {
        self.entries.values().flat_map(|bucket| bucket.iter())
    }
}

/// The main sharded index.
pub struct Index {
    pub(crate) shards: Vec<RwLock<IndexShard>>,
    total_entries: AtomicU64,
    total_data_bytes: AtomicU64,
}

impl Index {
    /// Creates a new empty index.
    pub fn new() -> Self {
        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            shards.push(RwLock::new(IndexShard::new()));
        }

        Self {
            shards,
            total_entries: AtomicU64::new(0),
            total_data_bytes: AtomicU64::new(0),
        }
    }

    /// Returns the shard index for a key hash.
    #[inline]
    fn shard_for(&self, key_hash: u64) -> usize {
        // Use the high bits for shard selection (low bits are used in the hashmap)
        ((key_hash >> 56) as usize) % NUM_SHARDS
    }

    /// Looks up an entry by key.
    pub fn get(&self, key: &[u8]) -> Option<IndexEntry> {
        let key_hash = hash_key(key);
        let shard_idx = self.shard_for(key_hash);
        let shard = self.shards[shard_idx].read();
        shard.get(key, key_hash).cloned()
    }

    /// Inserts or updates an entry.
    pub fn insert(
        &self,
        key: &[u8],
        location: DiskLocation,
        generation: u32,
        value_len: u32,
    ) -> Option<IndexEntry> {
        let key_hash = hash_key(key);
        let entry = IndexEntry::new(key, key_hash, location, generation, value_len);

        let shard_idx = self.shard_for(key_hash);
        let mut shard = self.shards[shard_idx].write();

        let old = shard.insert(entry);
        if let Some(ref old_entry) = old {
            // shard.insert returns Some(old) on replacement (old gen < new gen)
            // or Some(entry) on rejection (old gen >= new gen).
            // Only adjust accounting on actual replacement.
            if old_entry.generation < generation {
                let old_size = old_entry.value_len as u64 + old_entry.key.len() as u64;
                let new_size = value_len as u64 + key.len() as u64;
                if new_size > old_size {
                    self.total_data_bytes.fetch_add(new_size - old_size, Ordering::Relaxed);
                } else {
                    self.total_data_bytes.fetch_sub(old_size - new_size, Ordering::Relaxed);
                }
            }
        } else {
            self.total_entries.fetch_add(1, Ordering::Relaxed);
            self.total_data_bytes.fetch_add(value_len as u64 + key.len() as u64, Ordering::Relaxed);
        }
        old
    }

    /// Marks a key as deleted.
    pub fn delete(&self, key: &[u8], generation: u32) -> bool {
        let key_hash = hash_key(key);
        let shard_idx = self.shard_for(key_hash);
        let mut shard = self.shards[shard_idx].write();
        shard.delete(key, key_hash, generation)
    }

    /// Removes an entry completely.
    pub fn remove(&self, key: &[u8]) -> Option<IndexEntry> {
        let key_hash = hash_key(key);
        let shard_idx = self.shard_for(key_hash);
        let mut shard = self.shards[shard_idx].write();

        let entry = shard.remove(key, key_hash);
        if let Some(ref e) = entry {
            self.total_entries.fetch_sub(1, Ordering::Relaxed);
            let size = e.value_len as u64 + e.key.len() as u64;
            self.total_data_bytes.fetch_sub(size.min(self.total_data_bytes.load(Ordering::Relaxed)), Ordering::Relaxed);
        }
        entry
    }

    /// Returns the total number of entries (including tombstones).
    pub fn len(&self) -> u64 {
        self.total_entries.load(Ordering::Relaxed)
    }

    /// Returns true if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the total tracked data bytes.
    pub fn total_data_bytes(&self) -> u64 {
        self.total_data_bytes.load(Ordering::Relaxed)
    }

    /// Returns statistics about the index.
    pub fn stats(&self) -> IndexStats {
        let mut live = 0;
        let mut tombstones = 0;

        for shard in &self.shards {
            let s = shard.read();
            live += s.live_count();
            tombstones += s.tombstone_count();
        }

        IndexStats {
            total_entries: self.len(),
            live_entries: live as u64,
            tombstones: tombstones as u64,
            total_data_bytes: self.total_data_bytes.load(Ordering::Relaxed),
        }
    }

    /// Iterates over all entries in a shard (for recovery/compaction).
    pub fn iter_shard(&self, shard_idx: usize) -> Vec<IndexEntry> {
        if shard_idx >= NUM_SHARDS {
            return Vec::new();
        }
        let shard = self.shards[shard_idx].read();
        shard.iter_all().cloned().collect()
    }

    /// Clears all entries from the index.
    pub fn clear(&self) {
        for shard in &self.shards {
            let mut s = shard.write();
            s.entries.clear();
            s.live_count = 0;
            s.tombstone_count = 0;
        }
        self.total_entries.store(0, Ordering::SeqCst);
        self.total_data_bytes.store(0, Ordering::SeqCst);
    }
}

impl Default for Index {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about the index.
#[derive(Debug, Clone)]
pub struct IndexStats {
    pub total_entries: u64,
    pub live_entries: u64,
    pub tombstones: u64,
    pub total_data_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_basic() {
        let index = Index::new();

        // Insert
        let key = b"test_key";
        let location = DiskLocation::new(0, 1, 128);
        index.insert(key, location, 1, 100);

        // Get
        let entry = index.get(key).unwrap();
        assert_eq!(entry.location, location);
        assert_eq!(entry.generation, 1);
        assert_eq!(entry.value_len, 100);
    }

    #[test]
    fn test_index_update() {
        let index = Index::new();
        let key = b"update_key";

        index.insert(key, DiskLocation::new(0, 0, 0), 1, 100);
        index.insert(key, DiskLocation::new(1, 1, 128), 2, 200);

        let entry = index.get(key).unwrap();
        assert_eq!(entry.location.file_id, 1);
        assert_eq!(entry.generation, 2);
        assert_eq!(entry.value_len, 200);
    }

    #[test]
    fn test_index_delete() {
        let index = Index::new();
        let key = b"delete_key";

        index.insert(key, DiskLocation::new(0, 0, 0), 1, 100);
        assert!(index.get(key).is_some());

        index.delete(key, 2);
        assert!(index.get(key).is_none());
    }

    #[test]
    fn test_index_generation_conflict() {
        let index = Index::new();
        let key = b"conflict_key";

        // Insert with generation 5
        index.insert(key, DiskLocation::new(0, 0, 0), 5, 100);

        // Try to insert with lower generation - should be ignored
        index.insert(key, DiskLocation::new(1, 1, 1), 3, 200);

        let entry = index.get(key).unwrap();
        assert_eq!(entry.generation, 5);
        assert_eq!(entry.value_len, 100);
    }

    #[test]
    fn test_index_stats() {
        let index = Index::new();

        for i in 0..100 {
            let key = format!("key_{}", i);
            index.insert(key.as_bytes(), DiskLocation::new(0, 0, i as u32), i as u32, 100);
        }

        // Delete some
        for i in 0..50 {
            let key = format!("key_{}", i);
            index.delete(key.as_bytes(), 1000 + i as u32);
        }

        let stats = index.stats();
        assert_eq!(stats.live_entries, 50);
        assert_eq!(stats.tombstones, 50);
    }
}
