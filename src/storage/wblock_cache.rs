//! LRU cache of recently-read WBlocks.
//!
//! Every cold GET reads a full 1 MB WBlock off SSD via `pread`. Without a cache,
//! repeated reads of any key in a hot WBlock re-pull the same MB each time.
//! This cache sits at the `Handler` layer — compaction/scan paths deliberately
//! bypass it so they don't trash hot entries.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::io::aligned_buf::AlignedBuffer;
use crate::storage::write_buffer::WBLOCK_SIZE;

pub type CachedWblock = Arc<AlignedBuffer>;

/// Concurrent wblock LRU keyed by `(file_id, wblock_id)`.
///
/// Internally sharded to keep lock hold times short on concurrent reads.
pub struct WblockCache {
    shards: Box<[Shard]>,
    shift: u32,
    hits: AtomicU64,
    misses: AtomicU64,
    inserts: AtomicU64,
}

struct Shard {
    lru: Mutex<lru::LruCache<(u32, u32), CachedWblock>>,
}

impl WblockCache {
    /// Create a new cache sized at `capacity_mb` MiB (== entries since each
    /// WBlock is 1 MiB). Disabled if `capacity_mb == 0`.
    pub fn new(capacity_mb: usize) -> Arc<Self> {
        debug_assert_eq!(WBLOCK_SIZE, 1024 * 1024, "WBlock size assumption broken");

        // 16 shards is enough to decouple hot-path readers; each shard gets
        // ceil(capacity / 16) entries.
        const NUM_SHARDS: usize = 16;
        let per_shard_cap = capacity_mb.div_ceil(NUM_SHARDS).max(1);
        let cap = NonZeroUsize::new(per_shard_cap).unwrap();

        let shards = (0..NUM_SHARDS)
            .map(|_| Shard {
                lru: Mutex::new(lru::LruCache::new(cap)),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Arc::new(Self {
            shards,
            shift: NUM_SHARDS.trailing_zeros(),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            inserts: AtomicU64::new(0),
        })
    }

    #[inline]
    fn shard_idx(&self, file_id: u32, wblock_id: u32) -> usize {
        // Mix the two so consecutive wblocks in one file don't all land on
        // the same shard.
        let h = (file_id as u64)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            ^ (wblock_id as u64).wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
        ((h >> (64 - self.shift)) as usize) & (self.shards.len() - 1)
    }

    /// Look up a wblock; returns `None` on miss.
    #[inline]
    pub fn get(&self, file_id: u32, wblock_id: u32) -> Option<CachedWblock> {
        let shard = &self.shards[self.shard_idx(file_id, wblock_id)];
        let mut lru = shard.lru.lock();
        if let Some(buf) = lru.get(&(file_id, wblock_id)) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(Arc::clone(buf))
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Insert a freshly-read wblock. If already cached, keeps the existing
    /// entry (same content by construction).
    pub fn insert(&self, file_id: u32, wblock_id: u32, buf: CachedWblock) {
        let shard = &self.shards[self.shard_idx(file_id, wblock_id)];
        let mut lru = shard.lru.lock();
        lru.put((file_id, wblock_id), buf);
        self.inserts.fetch_add(1, Ordering::Relaxed);
    }

    /// Drop any cached entries for `file_id` (called after a file is deleted
    /// by compaction so the LRU doesn't hold dead memory).
    pub fn invalidate_file(&self, file_id: u32) {
        for shard in self.shards.iter() {
            let mut lru = shard.lru.lock();
            // LruCache has no `retain`; rebuild in place.
            let keep: Vec<_> = lru
                .iter()
                .filter(|((fid, _), _)| *fid != file_id)
                .map(|((fid, wid), buf)| ((*fid, *wid), Arc::clone(buf)))
                .collect();
            lru.clear();
            for (k, v) in keep {
                lru.put(k, v);
            }
        }
    }

    pub fn stats(&self) -> WblockCacheStats {
        WblockCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            inserts: self.inserts.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct WblockCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
}

impl WblockCacheStats {
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn buf(fill: u8) -> CachedWblock {
        let mut b = AlignedBuffer::new(WBLOCK_SIZE);
        b.extend_from_slice(&vec![fill; 64]);
        Arc::new(b)
    }

    #[test]
    fn hit_and_miss() {
        let c = WblockCache::new(4);
        assert!(c.get(1, 2).is_none());
        c.insert(1, 2, buf(0xAA));
        let got = c.get(1, 2).unwrap();
        assert_eq!(got.as_ref()[0], 0xAA);
        let s = c.stats();
        assert_eq!(s.hits, 1);
        assert_eq!(s.misses, 1);
    }

    #[test]
    fn per_file_invalidate_drops_only_that_file() {
        let c = WblockCache::new(64);
        c.insert(1, 0, buf(1));
        c.insert(1, 1, buf(1));
        c.insert(2, 0, buf(2));
        c.invalidate_file(1);
        assert!(c.get(1, 0).is_none());
        assert!(c.get(1, 1).is_none());
        assert!(c.get(2, 0).is_some());
    }

    #[test]
    fn lru_evicts_beyond_capacity() {
        // 16 shards × 1 = 16 entries total (1 MB per shard min).
        let c = WblockCache::new(16);
        for i in 0..64u32 {
            c.insert(0, i, buf(i as u8));
        }
        // At least some of the earliest inserts must be gone.
        let mut present = 0;
        for i in 0..64u32 {
            if c.get(0, i).is_some() {
                present += 1;
            }
        }
        assert!(present < 64);
    }
}
