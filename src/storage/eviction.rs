//! Background eviction: expires stale entries and evicts under capacity pressure.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info};

use crate::engine::index::Index;
use crate::server::handler::Handler;

/// Eviction policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    NoEviction,
    AllKeysLru,
    VolatileLru,
    AllKeysRandom,
    VolatileRandom,
    VolatileTtl,
}

impl EvictionPolicy {
    pub fn from_str(s: &str) -> Self {
        match s {
            "allkeys-lru" => Self::AllKeysLru,
            "volatile-lru" => Self::VolatileLru,
            "allkeys-random" => Self::AllKeysRandom,
            "volatile-random" => Self::VolatileRandom,
            "volatile-ttl" => Self::VolatileTtl,
            _ => Self::NoEviction,
        }
    }
}

/// Eviction configuration.
#[derive(Debug, Clone)]
pub struct EvictionConfig {
    pub policy: EvictionPolicy,
    pub max_entries: u64,
    pub max_data_mb: u64,
    pub check_interval_secs: u64,
    pub sample_size: usize,
}

impl Default for EvictionConfig {
    fn default() -> Self {
        Self {
            policy: EvictionPolicy::NoEviction,
            max_entries: 0,
            max_data_mb: 0,
            check_interval_secs: 1,
            sample_size: 16,
        }
    }
}

/// Eviction statistics.
#[derive(Debug, Default)]
pub struct EvictionStats {
    pub eviction_runs: AtomicU64,
    pub keys_evicted: AtomicU64,
    pub expired_collected: AtomicU64,
}

/// Simple xorshift64 PRNG.
struct Rng(u64);

impl Rng {
    fn new() -> Self {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(42);
        // Ensure non-zero seed
        Self(seed | 1)
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// The background evictor.
pub struct Evictor {
    config: EvictionConfig,
    handler: Arc<Handler>,
    index: Arc<Index>,
    stats: Arc<EvictionStats>,
    /// Round-robin shard cursor for expired entry scanning.
    shard_cursor: std::sync::atomic::AtomicUsize,
}

impl Evictor {
    pub fn new(
        config: EvictionConfig,
        handler: Arc<Handler>,
        index: Arc<Index>,
    ) -> Self {
        Self {
            config,
            handler,
            index,
            stats: Arc::new(EvictionStats::default()),
            shard_cursor: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub fn stats(&self) -> Arc<EvictionStats> {
        Arc::clone(&self.stats)
    }

    /// Main eviction loop — runs on a background thread.
    pub fn run(&self, stop: Arc<AtomicBool>) {
        info!("Eviction thread started (policy={:?})", self.config.policy);

        while !stop.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_secs(self.config.check_interval_secs));

            if stop.load(Ordering::Relaxed) {
                break;
            }

            // Always clean up expired entries
            let expired = self.evict_expired();
            if expired > 0 {
                debug!("Evicted {} expired entries", expired);
            }

            // Check if we need capacity-based eviction
            if self.should_evict() {
                let evicted = self.evict_batch();
                if evicted > 0 {
                    debug!("Capacity eviction: removed {} entries", evicted);
                    self.stats.eviction_runs.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        info!("Eviction thread stopped");
    }

    /// Returns true if capacity thresholds are exceeded.
    fn should_evict(&self) -> bool {
        if self.config.policy == EvictionPolicy::NoEviction {
            return false;
        }

        let stats = self.index.stats();

        if self.config.max_entries > 0 && stats.live_entries >= self.config.max_entries {
            return true;
        }

        if self.config.max_data_mb > 0 {
            let max_bytes = self.config.max_data_mb * 1024 * 1024;
            if stats.total_data_bytes >= max_bytes {
                return true;
            }
        }

        false
    }

    /// Collects candidate keys by scanning shards. More thorough than pure random
    /// sampling because it visits shards that actually have entries.
    fn collect_candidates(&self, rng: &mut Rng) -> Vec<(Vec<u8>, u64)> {
        use crate::engine::index::NUM_SHARDS;

        let sample_size = self.config.sample_size;
        let mut candidates: Vec<(Vec<u8>, u64)> = Vec::with_capacity(sample_size);

        // Scan shards round-robin starting from a random offset
        let start_shard = (rng.next() as usize) % NUM_SHARDS;

        for i in 0..NUM_SHARDS {
            if candidates.len() >= sample_size {
                break;
            }

            let shard_idx = (start_shard + i) % NUM_SHARDS;
            let sampled = {
                let shard = self.index.shards[shard_idx].read();
                let live_count = shard.live_count();
                if live_count == 0 {
                    None
                } else {
                    let entry_idx = (rng.next() as usize) % live_count;
                    shard
                        .iter_live()
                        .nth(entry_idx)
                        .map(|entry| (entry.key.as_bytes().to_vec(), entry.generation))
                }
            };
            if let Some(candidate) = sampled {
                candidates.push(candidate);
            }
        }

        candidates
    }

    /// Evicts entries until below capacity thresholds. Returns count evicted.
    fn evict_batch(&self) -> usize {
        let mut evicted = 0;
        let mut rng = Rng::new();
        let max_rounds = self.config.sample_size * 8;

        for _ in 0..max_rounds {
            if !self.should_evict() {
                break;
            }

            let candidates = self.collect_candidates(&mut rng);
            if candidates.is_empty() {
                break;
            }

            if let Some(victim) = self.select_victim(&candidates) {
                let _ = self.handler.delete_sync(&victim);
                self.index.remove(&victim);
                self.stats.keys_evicted.fetch_add(1, Ordering::Relaxed);
                evicted += 1;
            } else {
                break;
            }
        }
        evicted
    }

    /// Selects a victim from candidates based on the eviction policy.
    fn select_victim(&self, candidates: &[(Vec<u8>, u64)]) -> Option<Vec<u8>> {
        match self.config.policy {
            EvictionPolicy::AllKeysLru => {
                // Lowest generation ≈ oldest write ≈ LRU
                candidates
                    .iter()
                    .min_by_key(|(_, gen)| *gen)
                    .map(|(k, _)| k.clone())
            }
            EvictionPolicy::AllKeysRandom => {
                candidates.first().map(|(k, _)| k.clone())
            }
            EvictionPolicy::VolatileLru => {
                // Filter to keys with TTL, then pick lowest generation
                let mut best: Option<(Vec<u8>, u64)> = None;
                for (key, gen) in candidates {
                    if let Some(meta) = self.handler.get_with_meta(key) {
                        if meta.ttl_secs > 0 {
                            if best.is_none() || *gen < best.as_ref().unwrap().1 {
                                best = Some((key.clone(), *gen));
                            }
                        }
                    }
                }
                best.map(|(k, _)| k)
            }
            EvictionPolicy::VolatileRandom => {
                for (key, _) in candidates {
                    if let Some(meta) = self.handler.get_with_meta(key) {
                        if meta.ttl_secs > 0 {
                            return Some(key.clone());
                        }
                    }
                }
                None
            }
            EvictionPolicy::VolatileTtl => {
                // Pick key with TTL closest to expiry
                let now_micros = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_micros() as u64)
                    .unwrap_or(0);

                let mut best: Option<(Vec<u8>, u64)> = None;
                for (key, _) in candidates {
                    if let Some(meta) = self.handler.get_with_meta(key) {
                        if meta.ttl_secs > 0 {
                            let expiry = meta.timestamp_micros + (meta.ttl_secs as u64 * 1_000_000);
                            let remaining = expiry.saturating_sub(now_micros);
                            if best.is_none() || remaining < best.as_ref().unwrap().1 {
                                best = Some((key.clone(), remaining));
                            }
                        }
                    }
                }
                best.map(|(k, _)| k)
            }
            EvictionPolicy::NoEviction => None,
        }
    }

    /// Scans a portion of shards for expired entries and removes them.
    /// Uses get_with_meta to properly check TTL even for write-buffer entries.
    fn evict_expired(&self) -> usize {
        use crate::engine::index::NUM_SHARDS;

        let shards_per_cycle = 16.min(NUM_SHARDS);
        let start = self.shard_cursor.fetch_add(shards_per_cycle, Ordering::Relaxed) % NUM_SHARDS;
        let mut removed = 0;

        let now_micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);

        for i in 0..shards_per_cycle {
            let shard_idx = (start + i) % NUM_SHARDS;

            // Collect keys that might be expired (only those with TTL set)
            let keys: Vec<Vec<u8>> = {
                let shard = self.index.shards[shard_idx].read();
                shard
                    .iter_live()
                    .map(|e| e.key.as_bytes().to_vec())
                    .collect()
            };

            for key in keys {
                // Use get_with_meta to check actual TTL (works for write-buffer entries too)
                match self.handler.get_with_meta(&key) {
                    None => {
                        // Key was deleted concurrently or doesn't exist, clean up index
                        if self.index.get(&key).is_some() {
                            self.index.remove(&key);
                            removed += 1;
                        }
                    }
                    Some(meta) => {
                        if meta.ttl_secs > 0 {
                            let expiry_micros =
                                meta.timestamp_micros + (meta.ttl_secs as u64 * 1_000_000);
                            if now_micros >= expiry_micros {
                                let _ = self.handler.delete_sync(&key);
                                self.index.remove(&key);
                                removed += 1;
                            }
                        }
                    }
                }
            }
        }

        if removed > 0 {
            self.stats
                .expired_collected
                .fetch_add(removed as u64, Ordering::Relaxed);
        }

        removed
    }
}

/// Starts the eviction thread. Returns the evictor and a stop handle.
pub fn start_eviction_thread(
    config: EvictionConfig,
    handler: Arc<Handler>,
    index: Arc<Index>,
) -> (Arc<Evictor>, Arc<AtomicBool>) {
    let evictor = Arc::new(Evictor::new(config, handler, index));
    let stop = Arc::new(AtomicBool::new(false));

    let evictor_clone = Arc::clone(&evictor);
    let stop_clone = Arc::clone(&stop);

    std::thread::Builder::new()
        .name("eviction".to_string())
        .spawn(move || {
            evictor_clone.run(stop_clone);
        })
        .expect("Failed to spawn eviction thread");

    (evictor, stop)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::index::Index;
    use crate::storage::file_manager::{FileManager, WBLOCKS_PER_FILE};
    use crate::storage::write_buffer::WriteBuffer;
    use tempfile::tempdir;

    fn create_test_handler() -> (Arc<Handler>, Arc<Index>, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let file_manager = Arc::new(FileManager::new(dir.path()).unwrap());
        let index = Arc::new(Index::new());
        let write_buffer = Arc::new(WriteBuffer::new(0, WBLOCKS_PER_FILE));
        file_manager.create_file().unwrap();
        let handler = Arc::new(Handler::new(
            Arc::clone(&index),
            file_manager,
            write_buffer,
        ));
        (handler, index, dir)
    }

    #[test]
    fn test_evict_expired_entries() {
        let (handler, index, _dir) = create_test_handler();

        // Insert a key with 1-second TTL
        handler.put_sync(b"expire_me", b"value", 1).unwrap();
        assert!(handler.get_with_meta(b"expire_me").is_some());

        // Wait for expiry
        std::thread::sleep(Duration::from_secs(2));

        let config = EvictionConfig {
            policy: EvictionPolicy::NoEviction,
            ..Default::default()
        };
        let evictor = Evictor::new(config, Arc::clone(&handler), Arc::clone(&index));

        // Call evict_expired enough times to cover all 256 shards (16 shards per cycle)
        let mut total_removed = 0;
        for _ in 0..16 {
            total_removed += evictor.evict_expired();
        }
        assert!(total_removed >= 1, "Should have cleaned up at least 1 expired entry");

        // Verify the key is gone from the index
        assert!(index.get(b"expire_me").is_none());
    }

    #[test]
    fn test_no_eviction_policy_rejects() {
        let dir = tempdir().unwrap();
        let file_manager = Arc::new(FileManager::new(dir.path()).unwrap());
        let index = Arc::new(Index::new());
        let write_buffer = Arc::new(WriteBuffer::new(0, WBLOCKS_PER_FILE));
        file_manager.create_file().unwrap();

        let mut handler = Handler::new(
            Arc::clone(&index),
            file_manager,
            write_buffer,
        );
        handler.set_eviction_config(EvictionPolicy::NoEviction, 5, 0);
        let handler = Arc::new(handler);

        // Insert up to the limit
        for i in 0..5 {
            let key = format!("key_{}", i);
            handler.put_sync(key.as_bytes(), b"val", 0).unwrap();
        }

        // The 6th insert should fail
        let result = handler.put_sync(b"key_overflow", b"val", 0);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("OOM"));
    }

    #[test]
    fn test_allkeys_random_evicts() {
        let (handler, index, _dir) = create_test_handler();

        // Insert some keys
        for i in 0..20 {
            let key = format!("evict_key_{}", i);
            handler.put_sync(key.as_bytes(), b"value", 0).unwrap();
        }

        let config = EvictionConfig {
            policy: EvictionPolicy::AllKeysRandom,
            max_entries: 10,
            sample_size: 16,
            ..Default::default()
        };
        let evictor = Evictor::new(config, Arc::clone(&handler), Arc::clone(&index));

        // Should evict entries until at or below limit
        let evicted = evictor.evict_batch();
        assert!(evicted > 0, "Should have evicted entries");

        let stats = index.stats();
        assert!(
            stats.live_entries <= 10,
            "Expected <= 10 live entries, got {}",
            stats.live_entries
        );
    }

    #[test]
    fn test_volatile_ttl_evicts_soonest() {
        let (handler, index, _dir) = create_test_handler();

        // Insert keys with varying TTLs
        handler.put_sync(b"short_ttl", b"val", 10).unwrap();
        handler.put_sync(b"medium_ttl", b"val", 3600).unwrap();
        handler.put_sync(b"long_ttl", b"val", 86400).unwrap();
        handler.put_sync(b"no_ttl", b"val", 0).unwrap();

        let config = EvictionConfig {
            policy: EvictionPolicy::VolatileTtl,
            max_entries: 3,
            sample_size: 16,
            ..Default::default()
        };
        let evictor = Evictor::new(config, Arc::clone(&handler), Arc::clone(&index));

        let evicted = evictor.evict_batch();
        assert!(evicted >= 1, "Should have evicted at least 1 entry");

        let remaining = index.stats().live_entries;
        assert!(remaining <= 3, "Expected <= 3 live entries, got {}", remaining);
    }

    #[test]
    fn test_max_entries_enforced() {
        let dir = tempdir().unwrap();
        let file_manager = Arc::new(FileManager::new(dir.path()).unwrap());
        let index = Arc::new(Index::new());
        let write_buffer = Arc::new(WriteBuffer::new(0, WBLOCKS_PER_FILE));
        file_manager.create_file().unwrap();

        let mut handler = Handler::new(
            Arc::clone(&index),
            Arc::clone(&file_manager),
            write_buffer,
        );
        handler.set_eviction_config(EvictionPolicy::NoEviction, 10, 0);
        let handler = Arc::new(handler);

        // Insert 10 keys (at limit)
        for i in 0..10 {
            let key = format!("limit_key_{}", i);
            handler.put_sync(key.as_bytes(), b"value", 0).unwrap();
        }

        // 11th should fail
        let result = handler.put_sync(b"over_limit", b"value", 0);
        assert!(result.is_err());
    }
}
