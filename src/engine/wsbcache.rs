//! Write-staging buffer cache (WSBCache) — SIndex paper §4.3.
//!
//! From "SIndex: An SSD-based Large-scale Indexing with Deterministic
//! Latency for Cloud Block Storage" (ICPP '24; extended in ACM TOS 2026):
//!
//! > WSBCache employs multiple clock lists (i.e., 16 in default) organized
//! > as linked-list to collaboratively manage [pointers] corresponding to
//! > cached ipages. [...] WSBCache proposes a two-stage sync mechanism
//! > (TSS) that strategically buffers the evicted ipages in memory to
//! > provide stable latency of read-after-write operations.
//!
//! ## This implementation
//! - 16 independently-locked shards, each with its own clock list — the
//!   paper's contention-avoidance structure. (Deviation: pages map to a
//!   shard by key hash rather than "shortest list" insertion, because we
//!   look pages up by key rather than through swizzled pointers; eviction
//!   runs inline on insert rather than on 16 dedicated threads.)
//! - Per-page `AccCount` decremented by the clock sweep, exactly as in
//!   the paper's Algorithm 1.
//! - **Dirty pages are never evicted** — they are staged in memory until
//!   the TSS sync cycle writes them to their segment file and marks them
//!   clean (paper ipage states: cached → dirty → buffered → released).
//!   Flushed pages *stay cached* so read-after-write is served from
//!   memory, which is the entire point of the staging design. If every
//!   page in a shard is dirty the shard temporarily overflows capacity
//!   and the sync trigger is notified (back-pressure to the sync thread).
//! - Each page carries a `gen` counter bumped on every dirty insert, so
//!   the sync cycle can snapshot dirty pages, write them without holding
//!   shard locks, and afterwards mark clean *only* pages that were not
//!   re-dirtied concurrently (paper state ❺: re-updated during TSS).

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use crate::engine::ipage::IPAGE_SIZE;

/// Number of independently-locked clock lists (paper default: 16).
const NUM_SHARDS: usize = 16;

/// A cached ipage together with metadata.
struct CachedPage {
    data: Box<[u8; IPAGE_SIZE]>,
    /// Dirty flag: page has been modified and not yet flushed to SSD.
    dirty: bool,
    /// Bumped on every dirty insert; used to detect re-dirtying races.
    gen: u64,
    /// Access count, decremented by the clock eviction sweep (paper AccCount).
    acc_count: u32,
}

pub type PageKey = (u32, u32); // (partition_id, page_index)

struct Shard {
    pages: HashMap<PageKey, CachedPage>,
    /// Clock list for this shard.
    clock_order: Vec<PageKey>,
    clock_pos: usize,
    capacity: usize,
}

impl Shard {
    /// Clock eviction sweep over clean pages only (dirty pages are staged).
    fn evict_one_clean(&mut self) -> Option<PageKey> {
        let len = self.clock_order.len();
        if len == 0 {
            return None;
        }
        // Up to two full sweeps: the first pass decrements access counts,
        // the second finds a zero-count clean victim.
        for _ in 0..len * 2 {
            let pos = self.clock_pos % len;
            self.clock_pos = (pos + 1) % len;
            let key = self.clock_order[pos];
            if let Some(cp) = self.pages.get_mut(&key) {
                if cp.dirty {
                    continue; // staged — never evict
                }
                if cp.acc_count == 0 {
                    self.pages.remove(&key);
                    self.clock_order.swap_remove(pos);
                    let new_len = self.clock_order.len();
                    if self.clock_pos >= new_len && new_len > 0 {
                        self.clock_pos = new_len - 1;
                    }
                    return Some(key);
                }
                cp.acc_count = cp.acc_count.saturating_sub(1);
            }
        }
        None
    }
}

/// Write-staging buffer cache.
pub struct WsbCache {
    shards: Vec<Mutex<Shard>>,
    /// Paired with `trigger_lock`; notified when dirty pages pile up to
    /// wake the TSS sync thread early.
    sync_trigger: Condvar,
    trigger_lock: Mutex<()>,
}

impl WsbCache {
    /// Create a cache with `capacity` total page entries.
    pub fn new(capacity: usize) -> Arc<Self> {
        let per_shard = (capacity / NUM_SHARDS).max(1);
        let shards = (0..NUM_SHARDS)
            .map(|_| {
                Mutex::new(Shard {
                    pages: HashMap::with_capacity(per_shard),
                    clock_order: Vec::with_capacity(per_shard),
                    clock_pos: 0,
                    capacity: per_shard,
                })
            })
            .collect();
        Arc::new(WsbCache {
            shards,
            sync_trigger: Condvar::new(),
            trigger_lock: Mutex::new(()),
        })
    }

    #[inline]
    fn shard_for(&self, key: PageKey) -> &Mutex<Shard> {
        // Cheap mix of partition id and page index.
        let h = (key.0 as u64).wrapping_mul(0x9E3779B97F4A7C15) ^ (key.1 as u64);
        &self.shards[(h as usize) % NUM_SHARDS]
    }

    /// Look up a page. Returns a copy on hit, `None` on a miss.
    pub fn get(&self, key: PageKey) -> Option<[u8; IPAGE_SIZE]> {
        let mut shard = self.shard_for(key).lock().unwrap();
        if let Some(cp) = shard.pages.get_mut(&key) {
            cp.acc_count = cp.acc_count.saturating_add(1);
            return Some(*cp.data);
        }
        None
    }

    /// Insert or update a page, marking it dirty if `dirty=true`.
    ///
    /// At capacity a *clean* page is evicted via the clock sweep (evicted
    /// clean pages need no writeback and are simply dropped). Dirty pages
    /// are never evicted; the shard overflows instead and the sync thread
    /// is poked.
    pub fn insert(&self, key: PageKey, data: Box<[u8; IPAGE_SIZE]>, dirty: bool) {
        let mut overflowed = false;
        {
            let mut shard = self.shard_for(key).lock().unwrap();

            if let Some(cp) = shard.pages.get_mut(&key) {
                cp.data = data;
                cp.acc_count = cp.acc_count.saturating_add(1);
                if dirty {
                    cp.dirty = true;
                    cp.gen += 1;
                }
                return;
            }

            if shard.pages.len() >= shard.capacity && shard.evict_one_clean().is_none() {
                overflowed = true; // all dirty: stage anyway
            }

            shard.pages.insert(
                key,
                CachedPage {
                    data,
                    dirty,
                    gen: 1,
                    acc_count: 1,
                },
            );
            shard.clock_order.push(key);
        }
        if dirty || overflowed {
            self.sync_trigger.notify_one();
        }
    }

    /// Snapshot all dirty pages **without** clearing the dirty flag.
    ///
    /// Returns `(key, gen, data)` tuples. After writing the pages to their
    /// segment files (and fsyncing), call [`mark_clean`](Self::mark_clean)
    /// with the same `(key, gen)` pairs — pages re-dirtied in the interim
    /// keep their dirty flag thanks to the generation check.
    pub fn collect_dirty(&self) -> Vec<(PageKey, u64, Box<[u8; IPAGE_SIZE]>)> {
        let mut out = Vec::new();
        for shard in &self.shards {
            let shard = shard.lock().unwrap();
            out.extend(
                shard
                    .pages
                    .iter()
                    .filter(|(_, cp)| cp.dirty)
                    .map(|(k, cp)| (*k, cp.gen, cp.data.clone())),
            );
        }
        out
    }

    /// Snapshot dirty pages belonging to one partition (for targeted flush).
    pub fn collect_dirty_for(&self, pid: u32) -> Vec<(PageKey, u64, Box<[u8; IPAGE_SIZE]>)> {
        let mut out = Vec::new();
        for shard in &self.shards {
            let shard = shard.lock().unwrap();
            out.extend(
                shard
                    .pages
                    .iter()
                    .filter(|(k, cp)| k.0 == pid && cp.dirty)
                    .map(|(k, cp)| (*k, cp.gen, cp.data.clone())),
            );
        }
        out
    }

    /// Clear the dirty flag for pages whose generation is unchanged since
    /// the matching `collect_dirty` snapshot. The pages stay cached
    /// (paper "buffered" state) so read-after-write hits memory.
    pub fn mark_clean(&self, written: &[(PageKey, u64)]) {
        for &(key, gen) in written {
            let mut shard = self.shard_for(key).lock().unwrap();
            if let Some(cp) = shard.pages.get_mut(&key) {
                if cp.gen == gen {
                    cp.dirty = false;
                }
            }
        }
    }

    /// Remove all entries for a given partition (on partition destruction).
    pub fn evict_partition(&self, partition_id: u32) {
        for shard in &self.shards {
            let mut shard = shard.lock().unwrap();
            shard.pages.retain(|k, _| k.0 != partition_id);
            shard.clock_order.retain(|k| k.0 != partition_id);
            shard.clock_pos = 0;
        }
    }

    /// Drop every cached page (FLUSHDB path).
    pub fn clear(&self) {
        for shard in &self.shards {
            let mut shard = shard.lock().unwrap();
            shard.pages.clear();
            shard.clock_order.clear();
            shard.clock_pos = 0;
        }
    }

    /// Number of pages currently in the cache.
    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.lock().unwrap().pages.len())
            .sum()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Block until either `interval` elapses or a dirty insert pokes the
    /// sync trigger. Used by the TSS background thread between cycles.
    pub fn wait_for_work(&self, interval: Duration) {
        let guard = self.trigger_lock.lock().unwrap();
        let _ = self.sync_trigger.wait_timeout(guard, interval);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_page(byte: u8) -> Box<[u8; IPAGE_SIZE]> {
        let mut p = Box::new([0u8; IPAGE_SIZE]);
        p[0] = byte;
        p
    }

    #[test]
    fn basic_insert_and_get() {
        let cache = WsbCache::new(160);
        let key = (0u32, 1u32);
        cache.insert(key, make_page(42), false);
        let got = cache.get(key).unwrap();
        assert_eq!(got[0], 42);
        assert!(cache.get((0, 99)).is_none());
    }

    #[test]
    fn eviction_on_capacity_spares_dirty() {
        // Tiny cache: 16 shards of 1 page each.
        let cache = WsbCache::new(16);
        cache.insert((0, 0), make_page(0), true);
        cache.insert((0, 1), make_page(1), true);
        for i in 2u32..40 {
            cache.insert((0, i), make_page(i as u8), false);
        }
        assert!(cache.get((0, 0)).is_some(), "dirty page was evicted");
        assert!(cache.get((0, 1)).is_some(), "dirty page was evicted");
        assert_eq!(cache.collect_dirty().len(), 2);
    }

    #[test]
    fn collect_and_mark_clean_with_generation_race() {
        let cache = WsbCache::new(1600);
        cache.insert((0, 1), make_page(1), true);
        cache.insert((0, 2), make_page(2), true);

        let snap = cache.collect_dirty();
        assert_eq!(snap.len(), 2);

        // Page (0,1) gets re-dirtied between snapshot and mark_clean.
        cache.insert((0, 1), make_page(9), true);

        let written: Vec<(PageKey, u64)> = snap.iter().map(|(k, g, _)| (*k, *g)).collect();
        cache.mark_clean(&written);

        // (0,1) must remain dirty (newer gen); (0,2) is clean now.
        let still_dirty = cache.collect_dirty();
        assert_eq!(still_dirty.len(), 1);
        assert_eq!(still_dirty[0].0, (0, 1));
        assert_eq!(still_dirty[0].2[0], 9);
    }

    #[test]
    fn all_dirty_overflows_capacity() {
        let cache = WsbCache::new(16); // 1 page per shard
        for i in 0u32..80 {
            cache.insert((0, i), make_page(i as u8), true);
        }
        // Nothing evictable: cache stages all 80 dirty pages.
        assert_eq!(cache.len(), 80);
        assert_eq!(cache.collect_dirty().len(), 80);
    }

    #[test]
    fn flushed_pages_stay_cached_for_read_after_write() {
        let cache = WsbCache::new(160);
        cache.insert((3, 7), make_page(0xAB), true);
        let snap = cache.collect_dirty();
        let written: Vec<(PageKey, u64)> = snap.iter().map(|(k, g, _)| (*k, *g)).collect();
        cache.mark_clean(&written);
        // Paper "buffered" state: the page is clean but still in memory.
        assert_eq!(cache.get((3, 7)).unwrap()[0], 0xAB);
        assert!(cache.collect_dirty().is_empty());
    }
}
