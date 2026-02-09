//! Multi-database support: DbHandler enum and DatabaseManager.
//!
//! Each database can be either SSD-backed (via Handler) or memory-only (via MemoryStore).

use std::io;
use std::sync::Arc;

use crate::server::handler::{Handler, RecordMeta};
use crate::storage::memory_store::MemoryStore;

/// A database handler — either SSD-backed or memory-only.
pub enum DbHandler {
    Ssd(Arc<Handler>),
    Memory(Arc<MemoryStore>),
}

impl DbHandler {
    pub fn put_sync(&self, key: &[u8], value: &[u8], ttl: u32) -> io::Result<()> {
        match self {
            DbHandler::Ssd(h) => h.put_sync(key, value, ttl),
            DbHandler::Memory(m) => m.put_sync(key, value, ttl),
        }
    }

    pub fn get_value(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self {
            DbHandler::Ssd(h) => h.get_value(key),
            DbHandler::Memory(m) => m.get_value(key),
        }
    }

    /// Zero-copy GET: writes RESP bulk string directly into output buffer.
    /// Returns true if key was found and written.
    #[inline]
    pub fn get_value_into(&self, key: &[u8], out: &mut Vec<u8>) -> bool {
        match self {
            DbHandler::Memory(m) => m.get_value_into(key, out),
            DbHandler::Ssd(h) => {
                // Fallback to allocating path for SSD handler
                match h.get_value(key) {
                    Some(value) => {
                        out.push(b'$');
                        out.extend_from_slice(itoa::Buffer::new().format(value.len()).as_bytes());
                        out.extend_from_slice(b"\r\n");
                        out.extend_from_slice(&value);
                        out.extend_from_slice(b"\r\n");
                        true
                    }
                    None => false,
                }
            }
        }
    }

    pub fn delete_sync(&self, key: &[u8]) -> io::Result<bool> {
        match self {
            DbHandler::Ssd(h) => h.delete_sync(key),
            DbHandler::Memory(m) => m.delete_sync(key),
        }
    }

    pub fn get_with_meta(&self, key: &[u8]) -> Option<RecordMeta> {
        match self {
            DbHandler::Ssd(h) => h.get_with_meta(key),
            DbHandler::Memory(m) => m.get_with_meta(key),
        }
    }

    pub fn update_ttl(&self, key: &[u8], new_ttl: u32) -> io::Result<bool> {
        match self {
            DbHandler::Ssd(h) => h.update_ttl(key, new_ttl),
            DbHandler::Memory(m) => m.update_ttl(key, new_ttl),
        }
    }

    pub async fn flush(&self) -> io::Result<()> {
        match self {
            DbHandler::Ssd(h) => h.flush().await,
            DbHandler::Memory(m) => m.flush(),
        }
    }

    pub fn is_memory(&self) -> bool {
        matches!(self, DbHandler::Memory(_))
    }

    /// Returns the SSD handler if this is an SSD-backed database.
    pub fn as_ssd(&self) -> Option<&Arc<Handler>> {
        match self {
            DbHandler::Ssd(h) => Some(h),
            DbHandler::Memory(_) => None,
        }
    }

    /// Returns the memory store if this is a memory-only database.
    pub fn as_memory(&self) -> Option<&Arc<MemoryStore>> {
        match self {
            DbHandler::Memory(m) => Some(m),
            DbHandler::Ssd(_) => None,
        }
    }

    /// Clear all entries in this database (for FLUSHDB).
    pub fn clear(&self) {
        match self {
            DbHandler::Ssd(h) => h.index().clear(),
            DbHandler::Memory(m) => m.clear(),
        }
    }

    /// Get live entry count (for DBSIZE).
    pub fn live_entries(&self) -> u64 {
        match self {
            DbHandler::Ssd(h) => h.index().stats().live_entries,
            DbHandler::Memory(m) => m.stats().0,
        }
    }

    /// Get data bytes (for INFO).
    pub fn total_data_bytes(&self) -> u64 {
        match self {
            DbHandler::Ssd(h) => h.index().total_data_bytes(),
            DbHandler::Memory(m) => m.stats().1,
        }
    }

    /// Get the generation for a key (for WATCH support).
    pub fn get_generation(&self, key: &[u8]) -> Option<u32> {
        match self {
            DbHandler::Ssd(h) => h.index().get(key).map(|e| e.generation),
            DbHandler::Memory(m) => m.get_generation(key),
        }
    }

    /// Iterate live keys with a callback (for KEYS/SCAN commands).
    pub fn iter_keys<F>(&self, mut f: F)
    where
        F: FnMut(&[u8]),
    {
        match self {
            DbHandler::Ssd(h) => {
                for shard_idx in 0..crate::engine::index::NUM_SHARDS {
                    let shard = h.index().shards[shard_idx].read();
                    for entry in shard.iter_live() {
                        f(entry.key.as_bytes());
                    }
                }
            }
            DbHandler::Memory(m) => {
                m.iter_keys(|key, _gen| f(key));
            }
        }
    }

    /// Iterate live keys in a specific shard range (for SCAN cursor support).
    /// Returns (next_shard, next_pos, done).
    pub fn scan_keys(
        &self,
        start_shard: usize,
        start_pos: usize,
        count: usize,
        pattern: Option<&[u8]>,
        results: &mut Vec<Vec<u8>>,
        glob_match_fn: fn(&[u8], &[u8]) -> bool,
    ) -> (usize, usize, bool) {
        match self {
            DbHandler::Ssd(h) => {
                let num_shards = crate::engine::index::NUM_SHARDS;
                let mut next_shard = start_shard;
                let mut next_pos = start_pos;

                for shard_idx in start_shard..num_shards {
                    let shard = h.index().shards[shard_idx].read();
                    let mut pos = if shard_idx == start_shard { start_pos } else { 0 };

                    for entry in shard.iter_live().skip(pos) {
                        let key_bytes = entry.key.as_bytes();
                        pos += 1;

                        if let Some(pat) = pattern {
                            if !glob_match_fn(pat, key_bytes) {
                                continue;
                            }
                        }

                        results.push(key_bytes.to_vec());
                        if results.len() >= count {
                            next_shard = shard_idx;
                            next_pos = pos;
                            return (next_shard, next_pos, false);
                        }
                    }

                    if shard_idx + 1 < num_shards {
                        next_shard = shard_idx + 1;
                        next_pos = 0;
                    } else {
                        return (0, 0, true);
                    }
                }
                (0, 0, true)
            }
            DbHandler::Memory(m) => {
                // For memory store, we collect all matching keys and paginate
                // using a simple skip-based approach with sorted keys for consistency
                let mut all_keys: Vec<Vec<u8>> = Vec::new();
                m.iter_keys(|key, _gen| {
                    if let Some(pat) = pattern {
                        if glob_match_fn(pat, key) {
                            all_keys.push(key.to_vec());
                        }
                    } else {
                        all_keys.push(key.to_vec());
                    }
                });
                all_keys.sort();

                let cursor = (start_shard << 56) | start_pos;
                let skip = cursor;
                let take = count;

                for key in all_keys.iter().skip(skip).take(take) {
                    results.push(key.clone());
                }

                if skip + take >= all_keys.len() {
                    (0, 0, true)
                } else {
                    let next = skip + take;
                    (next >> 56, next & 0x00FFFFFFFFFFFFFF, false)
                }
            }
        }
    }

    /// Get a random key from this database.
    pub fn random_key(&self) -> Option<Vec<u8>> {
        match self {
            DbHandler::Ssd(h) => {
                for shard_idx in 0..crate::engine::index::NUM_SHARDS {
                    let shard = h.index().shards[shard_idx].read();
                    let key_data = shard.iter_live().next().map(|e| e.key.as_bytes().to_vec());
                    drop(shard);
                    if let Some(key) = key_data {
                        return Some(key);
                    }
                }
                None
            }
            DbHandler::Memory(m) => {
                let mut result = None;
                m.iter_keys(|key, _gen| {
                    if result.is_none() {
                        result = Some(key.to_vec());
                    }
                });
                result
            }
        }
    }
}

/// Manages all 16 databases.
pub struct DatabaseManager {
    dbs: Vec<DbHandler>,
    num_dbs: u8,
}

impl DatabaseManager {
    pub fn new(dbs: Vec<DbHandler>) -> Self {
        let num_dbs = dbs.len() as u8;
        Self { dbs, num_dbs }
    }

    pub fn db(&self, id: u8) -> Option<&DbHandler> {
        self.dbs.get(id as usize)
    }

    pub fn num_dbs(&self) -> u8 {
        self.num_dbs
    }

    pub async fn flush_all(&self) -> io::Result<()> {
        for db in &self.dbs {
            db.flush().await?;
        }
        Ok(())
    }
}
