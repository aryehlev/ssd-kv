//! In-memory key-value store backed by DashMap.
//!
//! Provides the same API as `Handler` but stores values in RAM instead of SSD.
//! Used for memory-only databases (e.g. SELECT 1 when configured as memory DB).

use std::io;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;

use crate::server::handler::RecordMeta;
use crate::storage::eviction::EvictionPolicy;

/// A single entry stored in the memory store.
struct MemoryEntry {
    value: Vec<u8>,
    generation: u32,
    timestamp_micros: u64,
    ttl_secs: u32,
}

impl MemoryEntry {
    fn is_expired(&self) -> bool {
        if self.ttl_secs == 0 {
            return false;
        }
        let now_micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);
        let expiry = self.timestamp_micros + (self.ttl_secs as u64 * 1_000_000);
        now_micros >= expiry
    }
}

/// Concurrent in-memory key-value store.
pub struct MemoryStore {
    data: DashMap<Vec<u8>, MemoryEntry>,
    next_generation: AtomicU32,
    total_entries: AtomicU64,
    total_data_bytes: AtomicU64,
    eviction_policy: EvictionPolicy,
    max_entries: u64,
    max_data_bytes: u64,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            data: DashMap::new(),
            next_generation: AtomicU32::new(1),
            total_entries: AtomicU64::new(0),
            total_data_bytes: AtomicU64::new(0),
            eviction_policy: EvictionPolicy::NoEviction,
            max_entries: 0,
            max_data_bytes: 0,
        }
    }

    pub fn set_eviction_config(&mut self, policy: EvictionPolicy, max_entries: u64, max_data_mb: u64) {
        self.eviction_policy = policy;
        self.max_entries = max_entries;
        self.max_data_bytes = max_data_mb * 1024 * 1024;
    }

    pub fn put_sync(&self, key: &[u8], value: &[u8], ttl: u32) -> io::Result<()> {
        // Capacity check
        if matches!(self.eviction_policy, EvictionPolicy::NoEviction) {
            if self.max_entries > 0 && self.total_entries.load(Ordering::Relaxed) >= self.max_entries {
                // Check if we're replacing an existing key (doesn't increase count)
                if !self.data.contains_key(key) {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "OOM command not allowed when used memory > 'maxmemory'",
                    ));
                }
            }
            if self.max_data_bytes > 0 && self.total_data_bytes.load(Ordering::Relaxed) >= self.max_data_bytes {
                if !self.data.contains_key(key) {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "OOM command not allowed when used memory > 'maxmemory'",
                    ));
                }
            }
        }

        let generation = self.next_generation.fetch_add(1, Ordering::SeqCst);
        let now_micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);

        let data_size = (key.len() + value.len()) as u64;
        let entry = MemoryEntry {
            value: value.to_vec(),
            generation,
            timestamp_micros: now_micros,
            ttl_secs: ttl,
        };

        if let Some(old) = self.data.insert(key.to_vec(), entry) {
            // Replacing existing entry
            let old_size = (key.len() + old.value.len()) as u64;
            // Adjust data bytes (add new, remove old)
            self.total_data_bytes.fetch_add(data_size, Ordering::Relaxed);
            self.total_data_bytes.fetch_sub(old_size, Ordering::Relaxed);
        } else {
            // New entry
            self.total_entries.fetch_add(1, Ordering::Relaxed);
            self.total_data_bytes.fetch_add(data_size, Ordering::Relaxed);
        }

        Ok(())
    }

    pub fn get_value(&self, key: &[u8]) -> Option<Vec<u8>> {
        let entry = self.data.get(key)?;
        if entry.is_expired() {
            drop(entry);
            self.remove_entry(key);
            return None;
        }
        Some(entry.value.clone())
    }

    pub fn delete_sync(&self, key: &[u8]) -> io::Result<bool> {
        Ok(self.remove_entry(key))
    }

    pub fn get_with_meta(&self, key: &[u8]) -> Option<RecordMeta> {
        let entry = self.data.get(key)?;
        if entry.is_expired() {
            drop(entry);
            self.remove_entry(key);
            return None;
        }
        Some(RecordMeta {
            value: entry.value.clone(),
            timestamp_micros: entry.timestamp_micros,
            ttl_secs: entry.ttl_secs,
        })
    }

    pub fn update_ttl(&self, key: &[u8], new_ttl: u32) -> io::Result<bool> {
        if let Some(mut entry) = self.data.get_mut(key) {
            if entry.is_expired() {
                drop(entry);
                self.remove_entry(key);
                return Ok(false);
            }
            entry.ttl_secs = new_ttl;
            entry.timestamp_micros = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_micros() as u64)
                .unwrap_or(0);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn flush(&self) -> io::Result<()> {
        // No-op for memory store
        Ok(())
    }

    pub fn stats(&self) -> (u64, u64) {
        (
            self.total_entries.load(Ordering::Relaxed),
            self.total_data_bytes.load(Ordering::Relaxed),
        )
    }

    pub fn clear(&self) {
        self.data.clear();
        self.total_entries.store(0, Ordering::Relaxed);
        self.total_data_bytes.store(0, Ordering::Relaxed);
    }

    /// Iterate all keys (for KEYS/SCAN commands). Returns (key, generation) pairs.
    pub fn iter_keys<F>(&self, mut f: F)
    where
        F: FnMut(&[u8], u32),
    {
        for entry in self.data.iter() {
            if !entry.value().is_expired() {
                f(entry.key(), entry.value().generation);
            }
        }
    }

    /// Get the generation for a key (for WATCH support).
    pub fn get_generation(&self, key: &[u8]) -> Option<u32> {
        let entry = self.data.get(key)?;
        if entry.is_expired() {
            return None;
        }
        Some(entry.generation)
    }

    fn remove_entry(&self, key: &[u8]) -> bool {
        if let Some((k, v)) = self.data.remove(key) {
            let data_size = (k.len() + v.value.len()) as u64;
            self.total_entries.fetch_sub(1, Ordering::Relaxed);
            self.total_data_bytes.fetch_sub(data_size, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Evict expired entries. Returns count removed.
    pub fn evict_expired(&self) -> usize {
        let mut expired_keys = Vec::new();
        for entry in self.data.iter() {
            if entry.value().is_expired() {
                expired_keys.push(entry.key().clone());
            }
        }
        let count = expired_keys.len();
        for key in expired_keys {
            self.remove_entry(&key);
        }
        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_store_put_get() {
        let store = MemoryStore::new();
        store.put_sync(b"key1", b"value1", 0).unwrap();
        assert_eq!(store.get_value(b"key1"), Some(b"value1".to_vec()));
    }

    #[test]
    fn test_memory_store_delete() {
        let store = MemoryStore::new();
        store.put_sync(b"key1", b"value1", 0).unwrap();
        assert!(store.delete_sync(b"key1").unwrap());
        assert_eq!(store.get_value(b"key1"), None);
        assert!(!store.delete_sync(b"key1").unwrap());
    }

    #[test]
    fn test_memory_store_ttl_expiry() {
        let store = MemoryStore::new();
        store.put_sync(b"key1", b"value1", 1).unwrap();
        assert!(store.get_value(b"key1").is_some());
        std::thread::sleep(std::time::Duration::from_secs(2));
        assert_eq!(store.get_value(b"key1"), None);
    }

    #[test]
    fn test_memory_store_get_with_meta() {
        let store = MemoryStore::new();
        store.put_sync(b"key1", b"value1", 3600).unwrap();
        let meta = store.get_with_meta(b"key1").unwrap();
        assert_eq!(meta.value, b"value1");
        assert_eq!(meta.ttl_secs, 3600);
        assert!(meta.timestamp_micros > 0);
    }

    #[test]
    fn test_memory_store_update_ttl() {
        let store = MemoryStore::new();
        store.put_sync(b"key1", b"value1", 0).unwrap();
        assert!(store.update_ttl(b"key1", 7200).unwrap());
        let meta = store.get_with_meta(b"key1").unwrap();
        assert_eq!(meta.ttl_secs, 7200);
    }

    #[test]
    fn test_memory_store_capacity_check() {
        let mut store = MemoryStore::new();
        store.set_eviction_config(EvictionPolicy::NoEviction, 2, 0);
        store.put_sync(b"k1", b"v1", 0).unwrap();
        store.put_sync(b"k2", b"v2", 0).unwrap();
        assert!(store.put_sync(b"k3", b"v3", 0).is_err());
        // Replacing existing key should work
        store.put_sync(b"k1", b"v1_new", 0).unwrap();
    }

    #[test]
    fn test_memory_store_stats() {
        let store = MemoryStore::new();
        store.put_sync(b"k1", b"v1", 0).unwrap();
        store.put_sync(b"k2", b"v2", 0).unwrap();
        let (entries, bytes) = store.stats();
        assert_eq!(entries, 2);
        assert!(bytes > 0);
    }

    #[test]
    fn test_memory_store_clear() {
        let store = MemoryStore::new();
        store.put_sync(b"k1", b"v1", 0).unwrap();
        store.clear();
        assert_eq!(store.get_value(b"k1"), None);
        let (entries, bytes) = store.stats();
        assert_eq!(entries, 0);
        assert_eq!(bytes, 0);
    }

    #[test]
    fn test_memory_store_evict_expired() {
        let store = MemoryStore::new();
        store.put_sync(b"exp1", b"v1", 1).unwrap();
        store.put_sync(b"perm", b"v2", 0).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(2));
        let removed = store.evict_expired();
        assert_eq!(removed, 1);
        assert_eq!(store.get_value(b"perm"), Some(b"v2".to_vec()));
    }
}
