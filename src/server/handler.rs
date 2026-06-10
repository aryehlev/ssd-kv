//! Thin KV engine wrapper used by the Redis protocol layer.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::engine::KvEngine;

/// Metadata returned by get_with_meta.
pub struct RecordMeta {
    pub value: Vec<u8>,
}

/// Per-engine statistics.
#[derive(Debug, Default)]
pub struct HandlerStats {
    pub gets: AtomicU64,
    pub puts: AtomicU64,
    pub deletes: AtomicU64,
    pub get_hits: AtomicU64,
    pub get_misses: AtomicU64,
    pub errors: AtomicU64,
}

impl HandlerStats {
    pub fn to_json(&self) -> String {
        format!(
            r#"{{"gets":{},"puts":{},"deletes":{},"hits":{},"misses":{},"errors":{}}}"#,
            self.gets.load(Ordering::Relaxed),
            self.puts.load(Ordering::Relaxed),
            self.deletes.load(Ordering::Relaxed),
            self.get_hits.load(Ordering::Relaxed),
            self.get_misses.load(Ordering::Relaxed),
            self.errors.load(Ordering::Relaxed),
        )
    }
}

/// Handler wraps a `KvEngine` and exposes the interface expected by the
/// Redis command layer.
pub struct Handler {
    engine: Arc<KvEngine>,
    pub stats: Arc<HandlerStats>,
}

impl Handler {
    pub fn new(engine: Arc<KvEngine>) -> Self {
        Handler {
            engine,
            stats: Arc::new(HandlerStats::default()),
        }
    }

    pub fn engine(&self) -> &Arc<KvEngine> {
        &self.engine
    }

    pub fn stats(&self) -> Arc<HandlerStats> {
        Arc::clone(&self.stats)
    }

    // ─── KV operations ───────────────────────────────────────────────────────

    pub fn get_value(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.stats.gets.fetch_add(1, Ordering::Relaxed);
        match self.engine.get(key) {
            Ok(Some(v)) => {
                self.stats.get_hits.fetch_add(1, Ordering::Relaxed);
                Some(v)
            }
            Ok(None) => {
                self.stats.get_misses.fetch_add(1, Ordering::Relaxed);
                None
            }
            Err(_) => {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    pub fn get_with_meta(&self, key: &[u8]) -> Option<RecordMeta> {
        self.get_value(key).map(|value| RecordMeta { value })
    }

    pub fn put_sync(&self, key: &[u8], value: &[u8], _ttl: u32) -> io::Result<()> {
        self.stats.puts.fetch_add(1, Ordering::Relaxed);
        self.engine.put(key, value)
    }

    /// Non-blocking PUT (writes synchronously but returns immediately).
    /// Returns `None` (no WAL position to wait for).
    pub fn put_nowait(&self, key: &[u8], value: &[u8], ttl: u32) -> io::Result<Option<u64>> {
        self.put_sync(key, value, ttl)?;
        Ok(None)
    }

    /// Shard-aware non-blocking PUT (shard_hint is ignored; kept for API compat).
    pub fn put_nowait_on(
        &self,
        _shard_hint: usize,
        key: &[u8],
        value: &[u8],
        ttl: u32,
    ) -> io::Result<Option<u64>> {
        self.put_nowait(key, value, ttl)
    }

    pub fn delete_sync(&self, key: &[u8]) -> io::Result<bool> {
        self.stats.deletes.fetch_add(1, Ordering::Relaxed);
        self.engine.delete(key)
    }

    pub fn delete_nowait(&self, key: &[u8]) -> io::Result<(bool, Option<u64>)> {
        let found = self.delete_sync(key)?;
        Ok((found, None))
    }

    pub fn delete_nowait_on(
        &self,
        _shard_hint: usize,
        key: &[u8],
    ) -> io::Result<(bool, Option<u64>)> {
        self.delete_nowait(key)
    }

    pub fn exists(&self, key: &[u8]) -> bool {
        self.engine.exists(key).unwrap_or(false)
    }

    pub fn update_ttl(&self, _key: &[u8], _ttl: u32) -> io::Result<bool> {
        // TTL not implemented in SIndex; return false (key unmodified).
        Ok(false)
    }

    pub async fn flush(&self) -> io::Result<()> {
        self.engine.flush()
    }

    pub fn compact(&self) -> io::Result<crate::engine::CompactionStats> {
        self.engine.compact()
    }

    pub fn live_entries(&self) -> u64 {
        self.engine.count_live()
    }

    pub fn total_data_bytes(&self) -> u64 {
        // Approximate: value log size
        0 // placeholder
    }

    /// Returns None; WAL positions are not used in SIndex.
    pub fn take_wal_position(&self) -> u64 {
        0
    }

    pub fn durable_position(&self) -> u64 {
        u64::MAX // always durable (sync writes)
    }

    pub fn wal_shards(&self) -> &[std::sync::Arc<()>] {
        &[]
    }
}
