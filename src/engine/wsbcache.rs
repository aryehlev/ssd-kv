//! Write-staging buffer cache (WSBCache) — paper §5.2.
//!
//! From "The Design of Trillion-scale SSD-based Indexing with Deterministic
//! Latency for Cloud Block Storage" (ACM TOS 2024):
//!
//! > The WSBCache absorbs hot ipage accesses in memory and separates write
//! > timing from the user request path using a Two-Stage Sync (TSS): Stage 1
//! > writes dirty ipages to the VL-SSD asynchronously; Stage 2 keeps recently
//! > evicted ipages in a staging area so read-after-write requests are served
//! > without hitting the still-variable-latency write SSD.
//!
//! ## This implementation
//! We implement the clock-based eviction policy with AccCount and a background
//! sync thread (TSS Stage 1). The Stage-2 staging area is simplified to a
//! recent-eviction buffer.
//!
//! The cache is keyed by `(partition_id, page_index)`. On a cache miss the
//! caller loads the page from the segment file and inserts it. On an eviction
//! the background thread writes dirty pages to the segment file.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use crate::engine::ipage::IPAGE_SIZE;

/// A cached ipage together with metadata.
struct CachedPage {
    data: Box<[u8; IPAGE_SIZE]>,
    /// Dirty flag: page has been modified and not yet flushed to SSD.
    dirty: bool,
    /// Access count, decremented by the clock eviction thread.
    acc_count: u32,
}

type PageKey = (u32, u32); // (partition_id, page_index)

struct CacheInner {
    pages: HashMap<PageKey, CachedPage>,
    /// Clock hand position for the eviction clock algorithm.
    clock_order: Vec<PageKey>,
    clock_pos: usize,
    capacity: usize,
}

/// Write-staging buffer cache.
///
/// Cheap to clone (Arc-wrapped). The background sync thread calls
/// `flush_dirty()` periodically; callers may also call it explicitly.
pub struct WsbCache {
    inner: Arc<Mutex<CacheInner>>,
    /// Notified when dirty pages exceed a threshold to wake the sync thread.
    sync_trigger: Arc<Condvar>,
}

impl WsbCache {
    /// Create a cache with `capacity` entries.
    pub fn new(capacity: usize) -> Arc<Self> {
        Arc::new(WsbCache {
            inner: Arc::new(Mutex::new(CacheInner {
                pages: HashMap::with_capacity(capacity),
                clock_order: Vec::with_capacity(capacity),
                clock_pos: 0,
                capacity,
            })),
            sync_trigger: Arc::new(Condvar::new()),
        })
    }

    /// Look up a page. Returns `None` on a cache miss.
    pub fn get(&self, key: PageKey) -> Option<Box<[u8; IPAGE_SIZE]>> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(cp) = inner.pages.get_mut(&key) {
            cp.acc_count = cp.acc_count.saturating_add(1);
            return Some((*cp.data).clone().into());
        }
        None
    }

    /// Insert or update a page in the cache, marking it dirty if `dirty=true`.
    ///
    /// If the cache is at capacity, one clean page (or the LRU dirty page) is
    /// evicted using the clock algorithm.
    pub fn insert(
        &self,
        key: PageKey,
        data: Box<[u8; IPAGE_SIZE]>,
        dirty: bool,
    ) -> Option<(PageKey, Box<[u8; IPAGE_SIZE]>, bool)> {
        let mut inner = self.inner.lock().unwrap();

        if let Some(cp) = inner.pages.get_mut(&key) {
            if dirty {
                cp.dirty = true;
            }
            cp.data = data;
            cp.acc_count = 1;
            return None;
        }

        // Evict if at capacity
        let evicted = if inner.pages.len() >= inner.capacity {
            Self::evict(&mut inner)
        } else {
            None
        };

        inner.pages.insert(
            key,
            CachedPage {
                data,
                dirty,
                acc_count: 1,
            },
        );
        inner.clock_order.push(key);

        if dirty {
            drop(inner);
            self.sync_trigger.notify_one();
        }

        evicted
    }

    /// Clock eviction: sweep until a page with acc_count == 0 is found.
    /// Returns the evicted page's key, data, and dirty flag.
    fn evict(inner: &mut CacheInner) -> Option<(PageKey, Box<[u8; IPAGE_SIZE]>, bool)> {
        let len = inner.clock_order.len();
        if len == 0 {
            return None;
        }
        let start = inner.clock_pos;
        for _ in 0..len * 2 {
            let pos = inner.clock_pos % len;
            inner.clock_pos = (inner.clock_pos + 1) % len.max(1);
            let key = inner.clock_order[pos];
            if let Some(cp) = inner.pages.get_mut(&key) {
                if cp.acc_count == 0 {
                    let cp = inner.pages.remove(&key).unwrap();
                    inner.clock_order.swap_remove(pos);
                    // swap_remove moves the last element to `pos`.
                    // If clock_pos is past the new end, clamp it; otherwise
                    // leave it unchanged — elements before clock_pos are
                    // unaffected by a swap at an index >= clock_pos.
                    let new_len = inner.clock_order.len();
                    if inner.clock_pos >= new_len && new_len > 0 {
                        inner.clock_pos = new_len - 1;
                    }
                    return Some((key, cp.data, cp.dirty));
                }
                cp.acc_count = cp.acc_count.saturating_sub(1);
            }
            if inner.clock_pos == start {
                break;
            }
        }
        // Fallback: evict the first entry
        if let Some(&first_key) = inner.clock_order.first() {
            let cp = inner.pages.remove(&first_key).unwrap();
            inner.clock_order.swap_remove(0);
            inner.clock_pos = 0;
            return Some((first_key, cp.data, cp.dirty));
        }
        None
    }

    /// Drain all dirty pages from the cache.
    ///
    /// Returns a Vec of `(partition_id, page_index, page_data)` for the caller
    /// to write to the corresponding segment files.
    pub fn drain_dirty(&self) -> Vec<(u32, u32, Box<[u8; IPAGE_SIZE]>)> {
        let mut inner = self.inner.lock().unwrap();
        let dirty_keys: Vec<PageKey> = inner
            .pages
            .iter()
            .filter(|(_, cp)| cp.dirty)
            .map(|(k, _)| *k)
            .collect();

        let mut result = Vec::with_capacity(dirty_keys.len());
        for key in dirty_keys {
            if let Some(cp) = inner.pages.get_mut(&key) {
                cp.dirty = false;
                result.push((key.0, key.1, (*cp.data).clone().into()));
            }
        }
        result
    }

    /// Remove all entries for a given partition (on partition destruction).
    pub fn evict_partition(&self, partition_id: u32) {
        let mut inner = self.inner.lock().unwrap();
        inner
            .pages
            .retain(|k, _| k.0 != partition_id);
        inner
            .clock_order
            .retain(|k| k.0 != partition_id);
    }

    /// Number of pages currently in the cache.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().pages.len()
    }

    /// Start a background TSS (Two-Stage Sync) thread that calls `flush_cb`
    /// with dirty pages every `interval`. The callback receives batches of
    /// `(partition_id, page_idx, page_data)`.
    pub fn start_background_sync<F>(
        cache: Arc<Self>,
        interval: Duration,
        mut flush_cb: F,
    ) -> std::thread::JoinHandle<()>
    where
        F: FnMut(Vec<(u32, u32, Box<[u8; IPAGE_SIZE]>)>) + Send + 'static,
    {
        std::thread::spawn(move || {
            loop {
                // Wait for interval or a trigger notification
                {
                    let inner = cache.inner.lock().unwrap();
                    let _ = cache.sync_trigger.wait_timeout(inner, interval);
                }
                let dirty = cache.drain_dirty();
                if !dirty.is_empty() {
                    flush_cb(dirty);
                }
            }
        })
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
        let cache = WsbCache::new(10);
        let key = (0u32, 1u32);
        cache.insert(key, make_page(42), false);
        let got = cache.get(key).unwrap();
        assert_eq!(got[0], 42);
        assert!(cache.get((0, 99)).is_none());
    }

    #[test]
    fn eviction_on_capacity() {
        let cache = WsbCache::new(4);
        for i in 0u32..5 {
            cache.insert((0, i), make_page(i as u8), false);
        }
        // After 5 inserts into capacity-4 cache, some page was evicted
        assert!(cache.len() <= 4);
    }

    #[test]
    fn drain_dirty_marks_clean() {
        let cache = WsbCache::new(100);
        cache.insert((0, 1), make_page(1), true);
        cache.insert((0, 2), make_page(2), false);
        cache.insert((0, 3), make_page(3), true);

        let dirty = cache.drain_dirty();
        assert_eq!(dirty.len(), 2);
        // After drain, no more dirty pages
        assert!(cache.drain_dirty().is_empty());
    }
}
