//! Multi-database manager. Each database is a separate `KvEngine` instance.

use std::io;
use std::sync::Arc;

use crate::server::handler::{Handler, RecordMeta};

/// A single database — always SSD-backed via KvEngine.
pub struct DbHandler {
    handler: Arc<Handler>,
}

impl DbHandler {
    pub fn new(handler: Arc<Handler>) -> Self {
        DbHandler { handler }
    }

    pub fn put_sync(&self, key: &[u8], value: &[u8], ttl: u32) -> io::Result<()> {
        self.handler.put_sync(key, value, ttl)
    }

    pub fn put_nowait(&self, key: &[u8], value: &[u8], ttl: u32) -> io::Result<Option<u64>> {
        self.handler.put_nowait(key, value, ttl)
    }

    pub fn put_nowait_on(
        &self,
        shard_hint: usize,
        key: &[u8],
        value: &[u8],
        ttl: u32,
    ) -> io::Result<Option<u64>> {
        self.handler.put_nowait_on(shard_hint, key, value, ttl)
    }

    pub fn delete_nowait(&self, key: &[u8]) -> io::Result<(bool, Option<u64>)> {
        self.handler.delete_nowait(key)
    }

    pub fn delete_nowait_on(
        &self,
        shard_hint: usize,
        key: &[u8],
    ) -> io::Result<(bool, Option<u64>)> {
        self.handler.delete_nowait_on(shard_hint, key)
    }

    pub fn get_value(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.handler.get_value(key)
    }

    /// Write a RESP bulk string for `key`'s value directly into `out`.
    /// Returns `true` if the key was found.
    #[inline]
    pub fn get_value_into(&self, key: &[u8], out: &mut Vec<u8>) -> bool {
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
        self.handler.delete_sync(key)
    }

    pub fn get_with_meta(&self, key: &[u8]) -> Option<RecordMeta> {
        self.handler.get_with_meta(key)
    }

    pub fn update_ttl(&self, key: &[u8], new_ttl: u32) -> io::Result<bool> {
        self.handler.update_ttl(key, new_ttl)
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
        Some(0) // generation tracking not implemented in SIndex
    }

    pub fn iter_keys<F>(&self, mut f: F)
    where
        F: FnMut(&[u8]),
    {
        if let Ok(keys) = self.handler.engine().scan_keys() {
            for k in &keys {
                f(k);
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
        results.extend(keys);
        next_cursor
    }

    pub fn random_key(&self) -> Option<Vec<u8>> {
        if let Ok(keys) = self.handler.engine().scan_keys() {
            keys.into_iter().next()
        } else {
            None
        }
    }

    pub fn clear(&self) {
        let _ = self.handler.engine().clear();
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
