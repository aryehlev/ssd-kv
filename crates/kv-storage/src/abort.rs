//! Abort of a cross-partition txn part (DESIGN §14.6 step 6).
//!
//! An XTxn part stages TXN-flagged versions of its keys; the coordinating worker commits them
//! once every part is durable. When the coordinator gives the txn up instead (another part
//! failed, e.g. `-OOM`, or its commit failed) every part that staged something must undo it:
//! otherwise the part's `OpenTxn` stays open forever, its keys show the uncommitted version, the
//! pages holding its records stay protected, and `resolved_w` can never pass the txn id, so the
//! node runs out of commit positions and every multi-key write hangs.
//!
//! [`PartitionStore::abort_txn`] undoes a part:
//! * the txn leaves `core.txns`, so its records are dead (`Core::dead`): uncommitted and not
//!   open;
//! * every key's index entry goes back to the one it had before the part (or the key leaves
//!   the index), the tracking the part added for the key's previous copy is removed and that
//!   copy is current again (records the part protected become Live again);
//! * the part's new versions are dropped: a staged record that never reached an image is taken
//!   out of its page, a record already in an image is zeroed by a scrub build, an extent write not
//!   issued is cancelled and its run freed, an extent written (or in flight) is zeroed once its
//!   write is durable.
//!
//! It returns a ticket that becomes `Durable` once nothing of the txn is left on media or in
//! flight in this partition (`None` when nothing ever was). Only then may the coordinator report
//! `TxnAborted` to the meta thread: `resolved_w` passing the id makes every record of it valid
//! for good (E10). A part whose `Apply` arrives after its abort is refused without staging
//! anything.
//!
//! A part never cancels a staged effect of another write (that effect's tickets would have
//! to come back on abort): its request waits until the staged effects of its keys are in an
//! image (`Core::part_wait`, woken after each tick's builds).

use smallvec::SmallVec;

use crate::cache::PageCache;
use crate::image::RecStatus;
use crate::keyspace::Cur;
use crate::layout::{PageId, Seq};
use crate::partition::{PartitionStore, StorageCtx, ZeroWhy};
use crate::ticket::TicketId;
use kv_common::KeyHash;
use kv_index::Entry;

/// Where an XTxn part put the new version of a key.
#[derive(Clone, Copy, Debug)]
pub(crate) enum NewLoc {
    /// Nothing was staged (a txn delete of a key the partition does not hold).
    None,
    /// A SMALL record (a value or a txn delete marker) on `page`.
    Small { page: PageId, seq: Seq, marker: bool },
    /// A VALUE extent or a CHUNKMAP (its chunks go with it).
    Ext { start: PageId, n_pages: u32 },
}

/// What an XTxn part changed for one key, to be undone if the txn is aborted.
pub(crate) struct TxnUndo {
    pub(crate) db: u8,
    pub(crate) slot: u16,
    pub(crate) kh: KeyHash,
    pub(crate) key: Box<[u8]>,
    /// The key's version before the part.
    pub(crate) cur: Cur,
    /// The index entry (with its hint) naming `cur`, `None` when the key was absent.
    pub(crate) old: Option<(Entry, u16)>,
    /// The location of `cur` had no tracked entry before the part (the part added it).
    pub(crate) untracked_before: bool,
    pub(crate) new: NewLoc,
}

/// One item an abort still waits for.
#[derive(Clone, Copy, Debug)]
pub(crate) enum AbortItem {
    /// A dead record of the txn in this page (staged, in an image in flight or in M).
    Page(PageId),
    /// A dead extent of the txn: `zeroing` once its zero write is queued.
    Ext { start: PageId, n_pages: u32, zeroing: bool },
}

/// An abort in progress: `ticket` holds one dependency per item.
pub(crate) struct Aborting {
    pub(crate) txn: u64,
    pub(crate) ticket: TicketId,
    pub(crate) items: SmallVec<[AbortItem; 2]>,
}

impl PartitionStore {
    /// Aborts this partition's part of txn `txn` (a cross-partition txn the coordinator gave
    /// up before its commit). See the module documentation. Returns the ticket that is
    /// `Durable` once nothing of the txn is left on media or in flight here, `None` when the
    /// part staged nothing (a later `Apply` of `txn` is refused).
    pub fn abort_txn(&mut self, cx: &mut StorageCtx<'_>, txn: u64) -> Option<TicketId> {
        self.core.now = cx.now;
        self.core.tickets.now_ns = cx.now.mono_ns;
        let Some(o) = self.core.txns.get(&txn) else {
            // the part's Apply has not run: it never will
            if txn != 0 && !self.core.committed.contains(&txn) {
                self.core.aborted.insert(txn);
            }
            return None;
        };
        if o.local || o.requested {
            // a local txn commits by itself; a requested commit cannot be taken back
            return None;
        }
        let o = self.core.txns.remove(&txn).expect("present");
        for &t in &o.barriers {
            self.core.tickets.meta_done(t);
        }
        let mut items: SmallVec<[AbortItem; 2]> = SmallVec::new();
        // newest change first: a key changed twice by the part goes back step by step
        for u in o.undo.into_iter().rev() {
            self.undo_index(&u);
            self.undo_old_copy(cx, &u);
            match u.new {
                NewLoc::None => {}
                NewLoc::Small { page, seq, marker } => {
                    if marker
                        && let Some(a) = self.core.stale.absent_of(u.db, &u.key)
                        && a.page == page
                        && a.seq == seq
                    {
                        self.core.stale.take_absent(u.db, &u.key);
                    }
                    if self.drop_staged(cx.cache, page, u.db, u.kh.h32(), &u.key, txn) && !items.iter().any(|i| matches!(i, AbortItem::Page(p) if *p == page)) {
                        items.push(AbortItem::Page(page));
                    }
                }
                NewLoc::Ext { start, n_pages } => {
                    if self.cancel_ext(start, n_pages) {
                        items.push(AbortItem::Ext { start, n_pages, zeroing: false });
                    }
                }
            }
        }
        for p in &o.pages {
            if !items.iter().any(|i| matches!(i, AbortItem::Page(x) if x == p)) && self.page_holds_txn(cx.cache, *p, txn) {
                items.push(AbortItem::Page(*p));
            }
        }
        self.core.counters.txn_aborts += 1;
        let ticket = self.core.tickets.create(0);
        for _ in &items {
            self.core.tickets.add_effect(ticket);
        }
        self.core.tickets.seal(ticket);
        self.core.aborting.push(Aborting { txn, ticket, items });
        self.abort_progress(cx);
        Some(ticket)
    }

    /// The coordinator's commit of cross-partition txn `txn` failed (the meta store's write
    /// failed with `errno`): whether the commit reached media is unknown, so the part can be
    /// neither committed nor undone. The partition becomes read-only and every ticket fails
    /// (`write_failed`), so nothing waits for the txn's commit for good: barriers, writes the
    /// part superseded and later writes of its keys are answered with the error. Recovery
    /// settles the txn from what the commit slots hold.
    pub fn txn_commit_failed(&mut self, txn: u64, errno: i32) {
        if self.core.txns.get(&txn).is_some_and(|o| !o.local) {
            self.write_failed(errno);
        }
    }

    /// Puts the key's index entry back to what it was before the part.
    fn undo_index(&mut self, u: &TxnUndo) {
        let new36 = match u.new {
            NewLoc::Small { page, marker: false, .. } => Some(page.to36()),
            NewLoc::Ext { start, .. } => Some(start.to36()),
            NewLoc::Small { marker: true, .. } | NewLoc::None => None,
        };
        let pos = new36.and_then(|p| self.core.find_entry(u.db, u.slot, u.kh, p));
        let t = self.core.index.table_mut(u.db, u.slot);
        match (pos, u.old) {
            (Some(pos), Some((e, hint))) => t.update(pos, e, hint),
            (Some(pos), None) => t.remove(pos),
            (None, Some((e, hint))) => {
                // a marker took the key out of the index
                if t.try_insert(u.kh, e, hint).is_err() {
                    self.core.counters.unexpected_orphans += 1;
                }
            }
            (None, None) => {}
        }
    }

    /// The key's previous copy is current again: the part's tracking of it goes, and its records
    /// (protected by the part, or marked superseded when the part's version became durable) are
    /// Live again.
    fn undo_old_copy(&mut self, cx: &mut StorageCtx<'_>, u: &TxnUndo) {
        let (at, seq) = match u.cur {
            Cur::Absent => return,
            Cur::Small { page, seq, .. } => (page, seq),
            Cur::Extent { start, seq, .. } => (start, seq),
        };
        if u.untracked_before {
            self.core.stale.remove_at(u.db, &u.key, at);
        }
        if let Cur::Small { page, .. } = u.cur
            && let Some(f) = cx.cache.peek(page)
            && let Some(mut st) = cx.cache.take_state(f)
        {
            let h32 = u.kh.h32();
            let relive = |recs: &mut [crate::image::RecDesc], img: &[u8; crate::layout::PAGE]| {
                for r in recs.iter_mut() {
                    if r.db == u.db && r.h32 == h32 && r.seq == seq && matches!(r.status, RecStatus::Protected | RecStatus::Superseded) && r.key(img) == &*u.key {
                        r.status = RecStatus::Live;
                    }
                }
            };
            relive(&mut st.recs, cx.cache.frame(st.m_frame));
            if let Some(inf) = st.inflight.as_mut() {
                relive(&mut inf.recs, cx.cache.frame(inf.n_frame));
            }
            cx.cache.put_state(f, st);
        }
    }

    /// Takes the part's staged record of the key out of `page`; true when the page still holds
    /// (or will hold) a record of `txn` that a scrub build must zero.
    fn drop_staged(&mut self, cache: &mut PageCache, page: PageId, db: u8, h32: u32, key: &[u8], txn: u64) -> bool {
        let mut tickets: SmallVec<[TicketId; 2]> = SmallVec::new();
        if let Some(st) = cache.page_state_mut(page) {
            st.pending.retain(|p| {
                let ours = matches!(p, crate::image::Pending::Put(r) if r.desc.txn == txn) && p.is_key(db, h32, key);
                if ours {
                    tickets.extend(p.tickets().iter().copied());
                }
                !ours
            });
        }
        for t in tickets {
            self.core.tickets.effect_resolved(t);
        }
        self.core.note_page_space(cache, page);
        self.page_holds_txn(cache, page, txn) || cache.page_state(page).is_none()
    }

    /// True when `page` (cached) has a record of `txn` in M, in its image in flight or staged.
    fn page_holds_txn(&self, cache: &PageCache, page: PageId, txn: u64) -> bool {
        let Some(st) = cache.page_state(page) else { return false };
        st.recs.iter().any(|r| r.txn == txn && r.status != RecStatus::Droppable)
            || st.inflight.as_ref().is_some_and(|i| i.recs.iter().any(|r| r.txn == txn && r.status != RecStatus::Droppable))
            || st.pending.iter().any(|p| matches!(p, crate::image::Pending::Put(r) if r.desc.txn == txn))
    }

    /// Cancels the part's extent write at `start` if it was never issued (the run goes back);
    /// true when the extent is (or will be) on media and must be zeroed.
    fn cancel_ext(&mut self, start: PageId, n_pages: u32) -> bool {
        match self.core.ext.writes.get(&start).map(|w| w.state) {
            Some(crate::partition::ExtState::Pending) => {
                let w = self.core.ext.writes.remove(&start).expect("present");
                for t in w.tickets {
                    self.core.tickets.effect_resolved(t);
                }
                self.wake(w.waiters);
                self.core.alloc.free_extent(start, n_pages);
                self.cancel_chunks(start);
                false
            }
            _ => true,
        }
    }

    /// Drives the aborts in progress: an item is done once nothing of its txn is left there.
    /// Called by every tick.
    pub(crate) fn abort_progress(&mut self, cx: &mut StorageCtx<'_>) {
        if self.core.aborting.is_empty() {
            return;
        }
        let mut list = core::mem::take(&mut self.core.aborting);
        for a in list.iter_mut() {
            let txn = a.txn;
            let ticket = a.ticket;
            let mut keep: SmallVec<[AbortItem; 2]> = SmallVec::new();
            for it in a.items.drain(..) {
                let done = match it {
                    AbortItem::Page(page) => match cx.cache.page_state(page) {
                        Some(st) => {
                            if !self.page_holds_txn(cx.cache, page, txn) {
                                true
                            } else {
                                if !st.scrub && st.inflight.is_none() && !st.dirty {
                                    self.ensure_scrub(cx, page);
                                }
                                false
                            }
                        }
                        None => {
                            // evicted (its M was durable and clean): load it to check
                            if !self.core.loads.contains_key(&page) {
                                self.ensure_scrub(cx, page);
                            }
                            false
                        }
                    },
                    AbortItem::Ext { start, n_pages, zeroing } => {
                        if self.core.ext.writes.contains_key(&start) {
                            // the write completes (or fails) first
                            false
                        } else if !zeroing {
                            if !self.core.ext.zeros.contains_key(&start) {
                                self.core.ext.zeros.insert(start, crate::partition::ExtZero::pending(n_pages, ZeroWhy::Dead));
                            }
                            keep.push(AbortItem::Ext { start, n_pages, zeroing: true });
                            continue;
                        } else {
                            !self.core.ext.zeros.contains_key(&start)
                        }
                    }
                };
                if done {
                    self.core.tickets.effect_resolved(ticket);
                } else {
                    keep.push(it);
                }
            }
            a.items = keep;
        }
        list.retain(|a| !a.items.is_empty());
        list.extend(self.core.aborting.drain(..));
        self.core.aborting = list;
    }
}

impl PartitionStore {
    /// What undoing `s` (about to be applied by an XTxn part with plan `plan`) needs.
    pub(crate) fn part_undo(&self, s: &crate::keyspace::Staged, plan: &crate::partition::Plan, seq: Seq) -> TxnUndo {
        use crate::partition::Plan;
        let at = match s.cur {
            Cur::Absent => None,
            Cur::Small { page, .. } => Some(page),
            Cur::Extent { start, .. } => Some(start),
        };
        let old = at.and_then(|p| {
            let pos = self.core.find_entry(s.db, s.slot, s.kh, p.to36())?;
            self.core.index.table(s.db, s.slot)?.get(pos)
        });
        let untracked_before = at.is_some_and(|a| self.core.stale.state_at(s.db, &s.key, a).is_none());
        let marker = matches!(s.change, crate::keyspace::Change::Delete);
        let new = match *plan {
            Plan::None => NewLoc::None,
            Plan::Small { page, .. } => NewLoc::Small { page, seq, marker },
            Plan::Ext { start, n_pages, .. } => NewLoc::Ext { start, n_pages },
            Plan::Chunked { map, map_pages, .. } => NewLoc::Ext { start: map, n_pages: map_pages },
        };
        TxnUndo { db: s.db, slot: s.slot, kh: s.kh, key: s.key.clone(), cur: s.cur, old, untracked_before, new }
    }
}

impl PartitionStore {
    /// An XTxn part must not cancel a staged effect of another write: true when a key of
    /// `staged` has a staged effect not in an image yet (its page's pending list, an extent
    /// write not issued) or an unretired absent copy. The request waits for the next tick's
    /// builds (`Core::part_wait`).
    pub(crate) fn part_must_wait(&self, cache: &PageCache, staged: &[crate::keyspace::Staged]) -> Option<crate::keyspace::Need> {
        for s in staged.iter().filter(|s| matches!(s.change, crate::keyspace::Change::Put { .. } | crate::keyspace::Change::Delete)) {
            let h32 = s.kh.h32();
            match s.cur {
                Cur::Small { page, .. } => {
                    if cache.page_state(page).is_some_and(|st| st.pending.iter().any(|p| p.is_change() && p.is_key(s.db, h32, &s.key))) {
                        return Some(crate::keyspace::Need::Page(page));
                    }
                }
                Cur::Extent { start, .. } => {
                    if self.core.ext.writes.get(&start).is_some_and(|w| w.state == crate::partition::ExtState::Pending) {
                        return Some(crate::keyspace::Need::ExtWrite(start));
                    }
                }
                Cur::Absent => {}
            }
            if let Some(a) = self.core.stale.absent_of(s.db, &s.key) {
                return Some(crate::keyspace::Need::Page(a.page));
            }
        }
        None
    }
}
