//! WATCH sessions (DESIGN §8.5): which keys each connection watches and whether a committed
//! change of one of them made its session dirty.
//!
//! A connection's WATCH session has a node-unique token. `WATCH k` adds `(partition, db, k)`
//! to the table and registers one watcher of the key with the partition that owns it
//! (`PartitionStore::watch_key`), on the partition's own worker; the WATCH reply waits until
//! every owner registered its keys, so no change committed after the reply is missed. Every
//! worker drains its partitions' reports of changed watched keys (`take_touched`: writes,
//! deletes, expiries, FLUSHDB/FLUSHALL, replicated applies) right after the request or tick
//! that committed them, before any reply of that request is released, and marks the sessions
//! that watch the key dirty here. EXEC checks its session right before its body executes on
//! the partition's worker (and answers a null array when dirty), then releases it; DISCARD,
//! UNWATCH, RESET, a refused EXEC and a closing connection release it too. Releasing removes
//! the session's keys and unregisters each with its owner.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use smallvec::SmallVec;

/// One watched key: `(partition, db, key)`.
pub type WatchedKey = (u16, u8, Box<[u8]>);

#[derive(Default)]
struct Inner {
    /// Watched key -> the sessions watching it.
    by_key: hashbrown::HashMap<WatchedKey, SmallVec<[u64; 2]>, foldhash::fast::RandomState>,
    /// Session -> its keys.
    sessions: hashbrown::HashMap<u64, Vec<WatchedKey>, foldhash::fast::RandomState>,
    /// Sessions a committed change of a watched key made dirty.
    dirty: hashbrown::HashSet<u64, foldhash::fast::RandomState>,
}

/// The node's WATCH sessions (one table, shared by the workers).
#[derive(Default)]
pub struct WatchTable {
    next: AtomicU64,
    /// Watched keys over every session (0: nothing to look up after a request).
    keys: AtomicUsize,
    inner: parking_lot::Mutex<Inner>,
}

impl WatchTable {
    /// A new session token (never 0).
    pub fn token(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Whether any key is watched (the workers skip draining their partitions' reports
    /// otherwise).
    pub fn any(&self) -> bool {
        self.keys.load(Ordering::Acquire) > 0
    }

    /// Adds `key` to session `token`. Returns false when the session already watches it (no
    /// new partition registration then).
    pub fn add(&self, token: u64, key: WatchedKey) -> bool {
        let mut g = self.inner.lock();
        let s = g.sessions.entry(token).or_default();
        if s.contains(&key) {
            return false;
        }
        s.push(key.clone());
        g.by_key.entry(key).or_default().push(token);
        self.keys.fetch_add(1, Ordering::AcqRel);
        true
    }

    /// A committed change of `(p, db, key)`: every session watching it is dirty.
    pub fn touched(&self, p: u16, db: u8, key: &[u8]) {
        let mut g = self.inner.lock();
        let k: WatchedKey = (p, db, key.into());
        let Some(tokens) = g.by_key.get(&k).cloned() else {
            return;
        };
        g.dirty.extend(tokens);
    }

    /// Whether a committed change of one of the session's keys happened since it watched it.
    pub fn is_dirty(&self, token: u64) -> bool {
        token != 0 && self.inner.lock().dirty.contains(&token)
    }

    /// Ends session `token`; returns its keys (each to unregister with its owner).
    pub fn release(&self, token: u64) -> Vec<WatchedKey> {
        if token == 0 {
            return Vec::new();
        }
        let mut g = self.inner.lock();
        g.dirty.remove(&token);
        let keys = g.sessions.remove(&token).unwrap_or_default();
        for k in &keys {
            if let Some(v) = g.by_key.get_mut(k) {
                v.retain(|t| *t != token);
                if v.is_empty() {
                    g.by_key.remove(k);
                }
            }
        }
        self.keys.fetch_sub(keys.len(), Ordering::AcqRel);
        keys
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(p: u16, s: &str) -> WatchedKey {
        (p, 0, s.as_bytes().into())
    }

    #[test]
    fn sessions_are_dirtied_by_their_keys_only_and_released_clean() {
        let t = WatchTable::default();
        let (a, b) = (t.token(), t.token());
        assert!(a != 0 && a != b);
        assert!(!t.any());
        assert!(t.add(a, k(1, "x")));
        assert!(!t.add(a, k(1, "x")), "once per session");
        assert!(t.add(b, k(1, "x")));
        assert!(t.add(b, k(2, "y")));
        assert!(t.any());
        t.touched(2, 0, b"y");
        t.touched(1, 1, b"x"); // another database
        t.touched(3, 0, b"x"); // another partition
        assert!(!t.is_dirty(a) && t.is_dirty(b));
        t.touched(1, 0, b"x");
        assert!(t.is_dirty(a));
        assert_eq!(t.release(a), vec![k(1, "x")]);
        assert!(!t.is_dirty(a), "a released session is clean");
        let mut rest = t.release(b);
        rest.sort();
        assert_eq!(rest, vec![k(1, "x"), k(2, "y")]);
        assert!(!t.any());
        assert!(t.release(0).is_empty());
        // a new session of the same keys starts clean
        let c = t.token();
        t.add(c, k(1, "x"));
        assert!(!t.is_dirty(c));
    }
}
