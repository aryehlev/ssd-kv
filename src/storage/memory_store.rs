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
    generation: u64,
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
    next_generation: AtomicU64,
    total_entries: AtomicU64,
    total_data_bytes: AtomicU64,
    eviction_policy: EvictionPolicy,
    max_entries: u64,
    max_data_bytes: u64,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            // Use at least 64 shards for good concurrency regardless of CPU count
            data: DashMap::with_shard_amount(64),
            next_generation: AtomicU64::new(1),
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
        let data_size = (key.len() + value.len()) as u64;
        // Capacity check (only when limits are configured)
        if self.max_entries > 0 || self.max_data_bytes > 0 {
            if matches!(self.eviction_policy, EvictionPolicy::NoEviction) {
                let mut existing_size = None;
                // Try to remove expired entry atomically (no TOCTOU race)
                self.remove_if_expired(key);
                if let Some(entry) = self.data.get(key) {
                    existing_size = Some((key.len() + entry.value.len()) as u64);
                }

                let current_entries = self.total_entries.load(Ordering::Relaxed);
                let current_bytes = self.total_data_bytes.load(Ordering::Relaxed);
                let projected_entries = current_entries + if existing_size.is_none() { 1 } else { 0 };
                let projected_bytes = current_bytes + data_size - existing_size.unwrap_or(0);

                if self.max_entries > 0 && projected_entries > self.max_entries {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "OOM command not allowed when used memory > 'maxmemory'",
                    ));
                }
                if self.max_data_bytes > 0 && projected_bytes > self.max_data_bytes {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "OOM command not allowed when used memory > 'maxmemory'",
                    ));
                }
            }
        }

        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        // Skip syscall when no TTL — timestamp only matters for expiry
        let now_micros = if ttl != 0 {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_micros() as u64)
                .unwrap_or(0)
        } else {
            0
        };

        let entry = MemoryEntry {
            value: value.to_vec(),
            generation,
            timestamp_micros: now_micros,
            ttl_secs: ttl,
        };

        if let Some(old) = self.data.insert(key.to_vec(), entry) {
            // Replacing existing entry — adjust data bytes atomically
            let old_size = (key.len() + old.value.len()) as u64;
            if data_size != old_size {
                if data_size > old_size {
                    self.total_data_bytes.fetch_add(data_size - old_size, Ordering::Relaxed);
                } else {
                    self.total_data_bytes.fetch_sub(old_size - data_size, Ordering::Relaxed);
                }
            }
        } else {
            // New entry
            self.total_entries.fetch_add(1, Ordering::Relaxed);
            self.total_data_bytes.fetch_add(data_size, Ordering::Relaxed);
        }

        Ok(())
    }

    pub fn get_value(&self, key: &[u8]) -> Option<Vec<u8>> {
        let entry = self.data.get(key)?;
        if entry.ttl_secs != 0 && entry.is_expired() {
            drop(entry);
            self.remove_if_expired(key);
            return None;
        }
        Some(entry.value.clone())
    }

    /// Zero-copy GET: writes RESP bulk string directly into output buffer
    /// while holding the DashMap read guard, avoiding a Vec clone.
    #[inline]
    pub fn get_value_into(&self, key: &[u8], out: &mut Vec<u8>) -> bool {
        if let Some(entry) = self.data.get(key) {
            if entry.ttl_secs != 0 && entry.is_expired() {
                drop(entry);
                self.remove_if_expired(key);
                return false;
            }
            let value = &entry.value;
            out.push(b'$');
            out.extend_from_slice(itoa::Buffer::new().format(value.len()).as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(value);
            out.extend_from_slice(b"\r\n");
            return true;
        }
        false
    }

    pub fn delete_sync(&self, key: &[u8]) -> io::Result<bool> {
        Ok(self.remove_entry(key))
    }

    pub fn get_with_meta(&self, key: &[u8]) -> Option<RecordMeta> {
        let entry = self.data.get(key)?;
        if entry.is_expired() {
            drop(entry);
            self.remove_if_expired(key);
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
                self.remove_if_expired(key);
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
        // Use retain(|_,_| false) to atomically remove all entries per-shard,
        // decrementing counters for each removed entry. This is safe with
        // concurrent writers: each shard is locked individually, so puts to
        // other shards proceed normally, and counters stay consistent.
        self.data.retain(|k, v| {
            let data_size = (k.len() + v.value.len()) as u64;
            self.total_entries.fetch_sub(1, Ordering::Relaxed);
            self.total_data_bytes.fetch_sub(data_size, Ordering::Relaxed);
            false // remove all entries
        });
    }

    /// Iterate all keys (for KEYS/SCAN commands). Returns (key, generation) pairs.
    pub fn iter_keys<F>(&self, mut f: F)
    where
        F: FnMut(&[u8], u64),
    {
        for entry in self.data.iter() {
            if !entry.value().is_expired() {
                f(entry.key(), entry.value().generation);
            }
        }
    }

    /// Get the generation for a key (for WATCH support).
    pub fn get_generation(&self, key: &[u8]) -> Option<u64> {
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

    /// Atomically remove an entry only if it is expired, avoiding TOCTOU races.
    fn remove_if_expired(&self, key: &[u8]) -> bool {
        if let Some((k, v)) = self.data.remove_if(key, |_, v| v.is_expired()) {
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
        let mut count = 0;
        for key in expired_keys {
            // Use remove_if_expired to avoid deleting a fresh entry that was
            // inserted between collecting the key and removing it (TOCTOU).
            if self.remove_if_expired(&key) {
                count += 1;
            }
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
