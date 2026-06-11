//! Multi-database manager. Each database is a separate `KvEngine` instance.

use std::io;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;

use crate::server::handler::{Handler, RecordMeta};

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// A single database — always SSD-backed via KvEngine.
///
/// Expiry is tracked in a per-database in-memory map (key → absolute Unix
/// seconds). TTLs are not persisted across restarts; after a restart all
/// previously-expiring keys appear as non-expiring until they are naturally
/// overwritten or deleted. This matches the behaviour of Redis when running
/// without persistence.
pub struct DbHandler {
    handler: Arc<Handler>,
    /// key → absolute Unix timestamp in seconds at which the key expires.
    expiry: DashMap<Vec<u8>, u64>,
}

impl DbHandler {
    pub fn new(handler: Arc<Handler>) -> Self {
        DbHandler {
            handler,
            expiry: DashMap::new(),
        }
    }

    // ── Expiry helpers ──────────────────────────────────────────────────────

    fn set_expiry_rel(&self, key: &[u8], ttl_secs: u32) {
        if ttl_secs == 0 {
            self.expiry.remove(key);
        } else {
            self.expiry
                .insert(key.to_vec(), now_secs() + ttl_secs as u64);
        }
    }

    /// Set expiry to an absolute Unix timestamp (seconds). Used by EXPIREAT.
    pub fn set_expiry_abs(&self, key: &[u8], abs_secs: u64) {
        self.expiry.insert(key.to_vec(), abs_secs);
    }

    /// Remove any expiry for `key`. Returns true if there was one.
    pub fn persist(&self, key: &[u8]) -> bool {
        if !self.handler.engine().exists(key).unwrap_or(false) {
            return false;
        }
        self.expiry.remove(key).is_some()
    }

    /// Returns the remaining TTL in seconds, or:
    ///  -2  key does not exist
    ///  -1  key exists but has no expiry
    pub fn ttl_secs(&self, key: &[u8]) -> i64 {
        if !self.handler.engine().exists(key).unwrap_or(false) {
            return -2;
        }
        match self.expiry.get(key) {
            None => -1,
            Some(exp) => {
                let now = now_secs();
                if *exp <= now {
                    -2 // expired — caller should treat as missing
                } else {
                    (*exp - now) as i64
                }
            }
        }
    }

    /// Returns the absolute expiry timestamp, or None if no expiry is set.
    pub fn expiry_abs(&self, key: &[u8]) -> Option<u64> {
        self.expiry.get(key).map(|e| *e)
    }

    /// Returns true if `key` has an expiry set and that expiry has passed.
    fn is_expired(&self, key: &[u8]) -> bool {
        match self.expiry.get(key) {
            Some(exp) => *exp <= now_secs(),
            None => false,
        }
    }

    /// Check expiry and perform lazy deletion. Returns true if expired.
    fn lazy_expire(&self, key: &[u8]) -> bool {
        if self.is_expired(key) {
            let _ = self.handler.delete_sync(key);
            self.expiry.remove(key);
            true
        } else {
            false
        }
    }

    // ── KV operations (expiry-aware) ────────────────────────────────────────

    pub fn put_sync(&self, key: &[u8], value: &[u8], ttl: u32) -> io::Result<()> {
        self.handler.put_sync(key, value, ttl)?;
        self.set_expiry_rel(key, ttl);
        Ok(())
    }

    pub fn put_nowait(&self, key: &[u8], value: &[u8], ttl: u32) -> io::Result<Option<u64>> {
        let r = self.handler.put_nowait(key, value, ttl)?;
        self.set_expiry_rel(key, ttl);
        Ok(r)
    }

    pub fn put_nowait_on(
        &self,
        shard_hint: usize,
        key: &[u8],
        value: &[u8],
        ttl: u32,
    ) -> io::Result<Option<u64>> {
        let r = self.handler.put_nowait_on(shard_hint, key, value, ttl)?;
        self.set_expiry_rel(key, ttl);
        Ok(r)
    }

    pub fn delete_nowait(&self, key: &[u8]) -> io::Result<(bool, Option<u64>)> {
        let r = self.handler.delete_nowait(key)?;
        self.expiry.remove(key);
        Ok(r)
    }

    pub fn delete_nowait_on(
        &self,
        shard_hint: usize,
        key: &[u8],
    ) -> io::Result<(bool, Option<u64>)> {
        let r = self.handler.delete_nowait_on(shard_hint, key)?;
        self.expiry.remove(key);
        Ok(r)
    }

    pub fn get_value(&self, key: &[u8]) -> Option<Vec<u8>> {
        if self.lazy_expire(key) {
            return None;
        }
        self.handler.get_value(key)
    }

    /// Write a RESP bulk string for `key`'s value directly into `out`.
    /// Returns `true` if the key was found and has not expired.
    #[inline]
    pub fn get_value_into(&self, key: &[u8], out: &mut Vec<u8>) -> bool {
        if self.lazy_expire(key) {
            return false;
        }
        match self.handler.get_value(key) {
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

    pub fn delete_sync(&self, key: &[u8]) -> io::Result<bool> {
        let r = self.handler.delete_sync(key)?;
        self.expiry.remove(key);
        Ok(r)
    }

    pub fn get_with_meta(&self, key: &[u8]) -> Option<RecordMeta> {
        if self.lazy_expire(key) {
            return None;
        }
        self.handler.get_with_meta(key)
    }

    /// Set or replace the TTL for an existing key.
    /// `new_ttl == 0` means remove the expiry (PERSIST behaviour).
    /// Returns false if the key does not exist.
    pub fn update_ttl(&self, key: &[u8], new_ttl: u32) -> io::Result<bool> {
        if self.lazy_expire(key) {
            return Ok(false);
        }
        if !self.handler.engine().exists(key).unwrap_or(false) {
            return Ok(false);
        }
        self.set_expiry_rel(key, new_ttl);
        Ok(true)
    }

    pub async fn flush(&self) -> io::Result<()> {
        self.handler.flush().await
    }

    pub fn live_entries(&self) -> u64 {
        self.handler.live_entries()
    }

    pub fn total_data_bytes(&self) -> u64 {
        self.handler.total_data_bytes()
    }

    pub fn get_generation(&self, _key: &[u8]) -> Option<u32> {
        Some(0)
    }

    pub fn iter_keys<F>(&self, mut f: F)
    where
        F: FnMut(&[u8]),
    {
        if let Ok(keys) = self.handler.engine().scan_keys() {
            for k in &keys {
                if !self.is_expired(k) {
                    f(k);
                }
            }
        }
    }

    pub fn scan_keys(
        &self,
        cursor: u64,
        count: usize,
        pattern: Option<&[u8]>,
        results: &mut Vec<Vec<u8>>,
    ) -> u64 {
        let (next_cursor, keys) = self
            .handler
            .engine()
            .scan(cursor, count, pattern)
            .unwrap_or((0, Vec::new()));
        // Filter out expired keys without performing lazy deletion here
        // (scan result set is already materialised; deletions would mutate state
        //  under the caller, which is surprising).
        results.extend(keys.into_iter().filter(|k| !self.is_expired(k)));
        next_cursor
    }

    pub fn random_key(&self) -> Option<Vec<u8>> {
        if let Ok(keys) = self.handler.engine().scan_keys() {
            keys.into_iter().find(|k| !self.is_expired(k))
        } else {
            None
        }
    }

    pub fn clear(&self) {
        let _ = self.handler.engine().clear();
        self.expiry.clear();
    }

    pub fn compact(&self) -> io::Result<crate::engine::CompactionStats> {
        self.handler.compact()
    }

    pub fn durable_position(&self) -> u64 {
        u64::MAX
    }

    /// Returns a reference to the underlying Handler.
    pub fn handler(&self) -> &Arc<Handler> {
        &self.handler
    }
}

/// Manages all databases (one per SELECT index).
pub struct DatabaseManager {
    dbs: Vec<DbHandler>,
}

impl DatabaseManager {
    pub fn new(dbs: Vec<DbHandler>) -> Self {
        DatabaseManager { dbs }
    }

    pub fn db(&self, id: u8) -> Option<&DbHandler> {
        self.dbs.get(id as usize)
    }

    pub fn num_dbs(&self) -> u8 {
        self.dbs.len() as u8
    }

    pub async fn flush_all(&self) -> io::Result<()> {
        for db in &self.dbs {
            db.flush().await?;
        }
        Ok(())
    }
}
