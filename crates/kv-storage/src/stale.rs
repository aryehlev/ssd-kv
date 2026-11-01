//! Tracked stale copies and the no-resurrection rule (DESIGN §14.3, §16.2, §18.5).
//!
//! `stale_copies` maps exact `(db, key)` (never hashes) to the older copies of the key that
//! live on other pages or extents: populated by moves, extent updates and recovery, cleared
//! when the copy's zeroing is durable. By I11/I12 there is at most one such copy per key.
//!
//! Rule D1: the newest durable copy of a key may be dropped only when it is the only valid copy
//! (E9). A command whose completion needs older copies gone (DEL/UNLINK/GETDEL, EXPIRE-to-delete,
//! RENAME source removal, MSET/COPY overwrites, the third-copy rule, SHUTDOWN's drain) has their
//! zeroing built by the foreground page writer in the tick it executes, constrained only by the
//! E1/E9 ordering edges and never by background rates (LC-15). Rule D2: a delete completes when
//! the newest copy is durably gone or durably superseded. Zeroings no command waits for
//! (post-move cleanup, post-recovery copies of untouched keys) are background work (§19).
//!
//! Each tracked copy has a state: `CopyState::Source` while the key's newer copy (the one the
//! index names) is not durable yet (the copy is protected, §13.3 d, edge E2), then
//! `CopyState::Stale` until its zeroing is durable. Every entry counts towards the key's
//! copies on media, which the third-copy rule (E14) bounds at two. Copies inside the page the
//! index names are visible in that page's records and are never tracked here.

use core::hash::{Hash, Hasher};

use hashbrown::{Equivalent, HashMap};
use smallvec::SmallVec;

use crate::layout::PageId;

/// Location of one older copy of a key.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StaleRef {
    /// A record inside a SMALL page.
    Small { page: PageId },
    /// A VALUE or CHUNKMAP extent (its first sector is zeroed).
    Extent { page: PageId, n_pages: u32 },
}

impl StaleRef {
    pub fn page(&self) -> PageId {
        match *self {
            StaleRef::Small { page } | StaleRef::Extent { page, .. } => page,
        }
    }
}

/// Lifecycle of a tracked copy.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum CopyState {
    /// The key's newer copy is not durable yet: this copy is protected (E2).
    Source,
    /// The newer copy is durable: this copy awaits its zeroing.
    Stale,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct StaleEntry {
    pub(crate) at: StaleRef,
    pub(crate) state: CopyState,
    /// A received command waits for this zeroing: it is built by the foreground page writer.
    pub(crate) foreground: bool,
    /// The location also holds an older, superseded copy of the key whose zeroing is not
    /// durable yet (it counts as one more copy for the third-copy rule, E14).
    pub(crate) older: bool,
}

/// Owned map key: `(db, exact key bytes)`.
#[derive(PartialEq, Eq)]
struct Key {
    db: u8,
    key: Box<[u8]>,
}

impl Hash for Key {
    fn hash<H: Hasher>(&self, h: &mut H) {
        h.write_u8(self.db);
        self.key.hash(h);
    }
}

/// Borrowed lookup form of [`Key`] (hashes identically).
struct KeyRef<'a> {
    db: u8,
    key: &'a [u8],
}

impl Hash for KeyRef<'_> {
    fn hash<H: Hasher>(&self, h: &mut H) {
        h.write_u8(self.db);
        self.key.hash(h);
    }
}

impl Equivalent<Key> for KeyRef<'_> {
    fn equivalent(&self, k: &Key) -> bool {
        self.db == k.db && *self.key == *k.key
    }
}

/// `(db, exact key) -> older copies`.
type StaleMap = HashMap<Key, SmallVec<[StaleEntry; 1]>, foldhash::fast::FixedState>;

/// The partition's tracked stale copies (exact keys).
pub(crate) struct StaleCopies {
    map: StaleMap,
    /// Entries per SMALL page or extent first page (eviction, AG release, snapshot rules).
    pages: HashMap<PageId, u32, foldhash::fast::FixedState>,
    entries: usize,
}

impl Default for StaleCopies {
    fn default() -> Self {
        Self::new()
    }
}

impl StaleCopies {
    pub(crate) fn new() -> Self {
        StaleCopies {
            map: HashMap::with_hasher(Default::default()),
            pages: HashMap::with_hasher(Default::default()),
            entries: 0,
        }
    }

    /// Tracked copies (`stale_copies` counter).
    pub(crate) fn len(&self) -> usize {
        self.entries
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries == 0
    }

    /// Registers a copy of `(db, key)` at `at` (no duplicate entries per location).
    pub(crate) fn add(&mut self, db: u8, key: &[u8], at: StaleRef, state: CopyState) {
        let entry = StaleEntry { at, state, foreground: false, older: false };
        match self.map.get_mut(&KeyRef { db, key }) {
            Some(list) => {
                if let Some(e) = list.iter_mut().find(|e| e.at == at) {
                    // a stale copy never becomes a source again
                    if e.state == CopyState::Source {
                        e.state = state;
                    }
                    return;
                }
                list.push(entry);
            }
            None => {
                let mut list = SmallVec::new();
                list.push(entry);
                self.map.insert(Key { db, key: key.into() }, list);
            }
        }
        self.entries += 1;
        *self.pages.entry(at.page()).or_insert(0) += 1;
    }

    /// The tracked copies of `(db, key)`.
    pub(crate) fn get(&self, db: u8, key: &[u8]) -> &[StaleEntry] {
        self.map.get(&KeyRef { db, key }).map_or(&[], |v| v.as_slice())
    }

    /// Number of tracked copies of `(db, key)` (copies on media besides the indexed one).
    pub(crate) fn count(&self, db: u8, key: &[u8]) -> usize {
        if self.entries == 0 {
            return 0;
        }
        self.get(db, key).len()
    }

    /// Copies of `(db, key)` tracked outside `except` (an older copy sharing a tracked
    /// location counts too).
    pub(crate) fn count_outside(&self, db: u8, key: &[u8], except: PageId) -> usize {
        if self.entries == 0 {
            return 0;
        }
        self.get(db, key).iter().filter(|e| e.at.page() != except).map(|e| 1 + usize::from(e.older)).sum()
    }

    /// Sets or clears the older-copy flag of the copy of `(db, key)` at `at`.
    pub(crate) fn set_older(&mut self, db: u8, key: &[u8], at: PageId, on: bool) {
        if let Some(list) = self.map.get_mut(&KeyRef { db, key })
            && let Some(e) = list.iter_mut().find(|e| e.at.page() == at)
        {
            e.older = on;
        }
    }

    /// The state of the copy of `(db, key)` at `at`, if tracked.
    pub(crate) fn state_at(&self, db: u8, key: &[u8], at: PageId) -> Option<CopyState> {
        if self.entries == 0 {
            return None;
        }
        self.get(db, key).iter().find(|e| e.at.page() == at).map(|e| e.state)
    }

    /// The key's newer copy became durable: its sources become stale. Returns the locations
    /// whose zeroing may now be built (all of the key's stale copies).
    pub(crate) fn sources_superseded(&mut self, db: u8, key: &[u8]) -> SmallVec<[StaleEntry; 1]> {
        if self.entries == 0 {
            return SmallVec::new();
        }
        let Some(list) = self.map.get_mut(&KeyRef { db, key }) else { return SmallVec::new() };
        for e in list.iter_mut() {
            e.state = CopyState::Stale;
        }
        list.clone()
    }

    /// A command waits for the key's copies to be zeroed: marks them foreground and returns
    /// them (sources included: a delete turns them into copies to drop).
    pub(crate) fn require(&mut self, db: u8, key: &[u8], sources_too: bool) -> SmallVec<[StaleEntry; 1]> {
        if self.entries == 0 {
            return SmallVec::new();
        }
        let Some(list) = self.map.get_mut(&KeyRef { db, key }) else { return SmallVec::new() };
        for e in list.iter_mut() {
            if sources_too {
                e.state = CopyState::Stale;
            }
            if e.state == CopyState::Stale {
                e.foreground = true;
            }
        }
        list.clone()
    }

    /// The zeroing of the copy at `at` is durable: forget it.
    pub(crate) fn remove_at(&mut self, db: u8, key: &[u8], at: PageId) -> bool {
        if self.entries == 0 {
            return false;
        }
        let Some(list) = self.map.get_mut(&KeyRef { db, key }) else { return false };
        let Some(i) = list.iter().position(|e| e.at.page() == at) else { return false };
        list.remove(i);
        if list.is_empty() {
            self.map.remove(&KeyRef { db, key });
        }
        self.entries -= 1;
        if let Some(c) = self.pages.get_mut(&at) {
            *c -= 1;
            if *c == 0 {
                self.pages.remove(&at);
            }
        }
        true
    }

    /// Tracked copies located at `page` (SMALL page or extent first page).
    pub(crate) fn at_page(&self, page: PageId) -> u32 {
        if self.entries == 0 {
            return 0;
        }
        self.pages.get(&page).copied().unwrap_or(0)
    }

    /// Every tracked copy (paced scrub scheduling, SHUTDOWN drain).
    pub(crate) fn iter(&self) -> impl Iterator<Item = (u8, &[u8], &StaleEntry)> + '_ {
        self.map.iter().flat_map(|(k, v)| v.iter().map(move |e| (k.db, &*k.key, e)))
    }

    /// Drops every entry of `db` (FLUSHDB: the copies are dead by the durable cutoff).
    pub(crate) fn drop_db(&mut self, db: u8) {
        let mut removed: SmallVec<[PageId; 8]> = SmallVec::new();
        self.map.retain(|k, v| {
            if k.db == db {
                removed.extend(v.iter().map(|e| e.at.page()));
                false
            } else {
                true
            }
        });
        for p in removed {
            self.entries -= 1;
            if let Some(c) = self.pages.get_mut(&p) {
                *c -= 1;
                if *c == 0 {
                    self.pages.remove(&p);
                }
            }
        }
    }

    pub(crate) fn clear(&mut self) {
        self.map.clear();
        self.pages.clear();
        self.entries = 0;
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    fn small(n: u64) -> StaleRef {
        StaleRef::Small { page: PageId(n) }
    }

    #[test]
    fn lifecycle_source_stale_removed() {
        let mut s = StaleCopies::new();
        assert_eq!(s.count(0, b"k"), 0);
        s.add(0, b"k", small(7), CopyState::Source);
        s.add(0, b"k", small(7), CopyState::Source);
        assert_eq!(s.len(), 1, "one entry per location");
        assert_eq!(s.count(0, b"k"), 1);
        assert_eq!(s.count(1, b"k"), 0, "db is part of the key");
        assert_eq!(s.state_at(0, b"k", PageId(7)), Some(CopyState::Source));
        let st = s.sources_superseded(0, b"k");
        assert_eq!(st.len(), 1);
        assert_eq!(s.state_at(0, b"k", PageId(7)), Some(CopyState::Stale));
        assert_eq!(s.at_page(PageId(7)), 1);
        assert!(s.remove_at(0, b"k", PageId(7)));
        assert!(!s.remove_at(0, b"k", PageId(7)));
        assert!(s.is_empty());
        assert_eq!(s.at_page(PageId(7)), 0);
    }

    #[test]
    fn require_marks_foreground() {
        let mut s = StaleCopies::new();
        s.add(2, b"a", small(1), CopyState::Stale);
        s.add(2, b"a", StaleRef::Extent { page: PageId(9), n_pages: 4 }, CopyState::Source);
        let r = s.require(2, b"a", false);
        assert_eq!(r.iter().filter(|e| e.foreground).count(), 1, "sources stay protected");
        let r = s.require(2, b"a", true);
        assert!(r.iter().all(|e| e.foreground && e.state == CopyState::Stale));
        s.add(3, b"b", small(1), CopyState::Stale);
        assert_eq!(s.at_page(PageId(1)), 2);
        s.drop_db(2);
        assert_eq!(s.len(), 1);
        assert_eq!(s.at_page(PageId(1)), 1);
        assert_eq!(s.iter().count(), 1);
    }
}
