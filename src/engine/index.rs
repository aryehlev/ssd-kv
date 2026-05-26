//! Sharded in-memory index: 256 RwLock-guarded HashMap shards.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use crate::engine::index_entry::{hash_key, IndexEntry, RecordLocation};

pub const NUM_SHARDS: usize = 256;

pub struct IndexStats {
    pub live_entries: u64,
    pub total_data_bytes: u64,
}

pub struct IndexShard {
    entries: HashMap<u64, Vec<IndexEntry>>,
    pub live_count: usize,
    tombstone_count: usize,
}

impl IndexShard {
    fn new() -> Self {
        Self { entries: HashMap::new(), live_count: 0, tombstone_count: 0 }
    }

    pub fn get(&self, key: &[u8], key_hash: u64) -> Option<&IndexEntry> {
        self.entries.get(&key_hash)?.iter().find(|e| e.matches(key, key_hash) && e.is_live())
    }

    pub fn insert(&mut self, entry: IndexEntry) -> Option<IndexEntry> {
        let key_hash = entry.key_hash;
        let key_bytes = entry.key.as_bytes().to_vec();
        let bucket = self.entries.entry(key_hash).or_default();
        for existing in bucket.iter_mut() {
            if existing.key.as_bytes() == key_bytes {
                if entry.generation > existing.generation {
                    let (was_del, now_del) = (existing.is_deleted(), entry.is_deleted());
                    let old = std::mem::replace(existing, entry);
                    match (was_del, now_del) {
                        (true, false) => { self.tombstone_count = self.tombstone_count.saturating_sub(1); self.live_count += 1; }
                        (false, true) => { self.live_count = self.live_count.saturating_sub(1); self.tombstone_count += 1; }
                        _ => {}
                    }
                    return Some(old);
                }
                return Some(entry);
            }
        }
        if entry.is_deleted() { self.tombstone_count += 1; } else { self.live_count += 1; }
        bucket.push(entry);
        None
    }

    pub fn delete(&mut self, key: &[u8], key_hash: u64, generation: u32) -> bool {
        if let Some(bucket) = self.entries.get_mut(&key_hash) {
            for e in bucket.iter_mut() {
                if e.matches(key, key_hash) {
                    if generation > e.generation {
                        if e.is_live() { self.live_count = self.live_count.saturating_sub(1); self.tombstone_count += 1; }
                        e.mark_deleted(generation);
                        return true;
                    }
                    return false;
                }
            }
        }
        false
    }

    pub fn remove(&mut self, key: &[u8], key_hash: u64) -> Option<IndexEntry> {
        if let Some(bucket) = self.entries.get_mut(&key_hash) {
            if let Some(pos) = bucket.iter().position(|e| e.matches(key, key_hash)) {
                let e = bucket.swap_remove(pos);
                if bucket.is_empty() { self.entries.remove(&key_hash); }
                if e.is_deleted() { self.tombstone_count = self.tombstone_count.saturating_sub(1); }
                else { self.live_count = self.live_count.saturating_sub(1); }
                return Some(e);
            }
        }
        None
    }

    pub fn live_count(&self) -> usize { self.live_count }
    pub fn iter_live(&self) -> impl Iterator<Item = &IndexEntry> {
        self.entries.values().flat_map(|b| b.iter()).filter(|e| e.is_live())
    }
}

pub struct Index {
    pub shards: Vec<RwLock<IndexShard>>,
    total_entries: AtomicU64,
    total_data_bytes: AtomicU64,
}

impl Index {
    pub fn new() -> Self {
        let shards = (0..NUM_SHARDS).map(|_| RwLock::new(IndexShard::new())).collect();
        Self { shards, total_entries: AtomicU64::new(0), total_data_bytes: AtomicU64::new(0) }
    }

    #[inline]
    fn shard_for(&self, key_hash: u64) -> usize { ((key_hash >> 56) as usize) % NUM_SHARDS }

    pub fn get(&self, key: &[u8]) -> Option<IndexEntry> {
        let h = hash_key(key);
        self.shards[self.shard_for(h)].read().get(key, h).cloned()
    }

    pub fn insert(&self, key: &[u8], location: RecordLocation, generation: u32, value_len: u32) {
        let h = hash_key(key);
        let entry = IndexEntry::new(key, h, location, generation, value_len);
        let mut shard = self.shards[self.shard_for(h)].write();
        match shard.insert(entry) {
            None => {
                self.total_entries.fetch_add(1, Ordering::Relaxed);
                self.total_data_bytes.fetch_add(value_len as u64, Ordering::Relaxed);
            }
            Some(ref old_e) if old_e.is_live() => {
                let diff = value_len as i64 - old_e.value_len as i64;
                if diff >= 0 { self.total_data_bytes.fetch_add(diff as u64, Ordering::Relaxed); }
                else { self.total_data_bytes.fetch_sub((-diff) as u64, Ordering::Relaxed); }
            }
            Some(_) => {
                self.total_entries.fetch_add(1, Ordering::Relaxed);
                self.total_data_bytes.fetch_add(value_len as u64, Ordering::Relaxed);
            }
        }
    }

    pub fn delete(&self, key: &[u8], generation: u32) -> bool {
        let h = hash_key(key);
        let mut shard = self.shards[self.shard_for(h)].write();
        let deleted = shard.delete(key, h, generation);
        if deleted { self.total_entries.fetch_sub(1, Ordering::Relaxed); }
        deleted
    }

    pub fn remove(&self, key: &[u8]) {
        let h = hash_key(key);
        let mut shard = self.shards[self.shard_for(h)].write();
        if let Some(e) = shard.remove(key, h) {
            if e.is_live() {
                self.total_entries.fetch_sub(1, Ordering::Relaxed);
                self.total_data_bytes.fetch_sub(e.value_len as u64, Ordering::Relaxed);
            }
        }
    }

    pub fn total_data_bytes(&self) -> u64 { self.total_data_bytes.load(Ordering::Relaxed) }

    /// Remove all entries (FLUSHDB).
    pub fn clear(&self) {
        for shard in &self.shards {
            let mut s = shard.write();
            *s = IndexShard::new();
        }
        self.total_entries.store(0, Ordering::Relaxed);
        self.total_data_bytes.store(0, Ordering::Relaxed);
    }

    /// Iterate all live entries in a given shard (used by rebalance / eviction).
    pub fn iter_shard(&self, shard_id: usize) -> Vec<crate::engine::index_entry::IndexEntry> {
        self.shards[shard_id % NUM_SHARDS].read().iter_live().cloned().collect()
    }

    pub fn stats(&self) -> IndexStats {
        IndexStats {
            live_entries: self.total_entries.load(Ordering::Relaxed),
            total_data_bytes: self.total_data_bytes.load(Ordering::Relaxed),
        }
    }
}

impl Default for Index {
    fn default() -> Self { Self::new() }
}
