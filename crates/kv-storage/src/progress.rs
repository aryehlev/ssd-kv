//! Liveness: every accepted request reaches a terminal state in bounded time ("no hangs").
//!
//! # Wait and wakeup design
//!
//! A partition is driven only by its worker's calls ([`PartitionStore::submit`], `exec`,
//! `tick`, `on_io_done`, `on_durable`, `on_meta_done`); it never blocks and has no timers of its
//! own. Everything that is not finished waits for one of a closed set of events, and every wait
//! is registered *where that event is delivered*, under a key that does not change while the
//! wait lasts:
//!
//! | waiter | waits for | registered in | woken by |
//! |---|---|---|---|
//! | request (`io > 0`) | a page read / extent read / extent write it reads | `loads[page].waiters`, `io[tag] = ExtRead{req}`, `ext.writes[start].waiters` (id + generation; only while the write is still to complete) | `page_read_done` (a read that finds no frame is issued again, `load_retry`), `ext_read_done`, `ext_write_done`, the write's cancellation or durability (`wake`) |
//! | request (`waits > 0`) | the request ahead in a key queue | `reqs.busy[(db, key hash)]` | `finish` of the head |
//! | request (`blocked`) | a barrier submitted before it | `reqs.blocked` | `finish` of the barrier |
//! | barrier request | every older request | `reqs.barriers` | `finish` of the oldest request |
//! | request (`parked`, space) | an AG grant | `space_wait` (only while an AG request is wanted or in flight) | every AG reply (granted or refused) |
//! | request (`parked`, seq) | a seq reservation / txn id range | `seq_wait` (only while a reservation is asked, or the reserve allows a retry) | `ReserveSeq` reply; `txn_progress` each tick |
//! | executed local txn (held) | its durable commit | `reqs.held[txn]` | `txn_committed`, or ReadOnly |
//! | staged page effect | the page writer | `dirty` / `retry` (built next tick), image in flight (re-queued when durable), `deferred` (older copies) | `tick`, `image_durable`, `stale_gone`, FLUSH (copies dead by the cutoff) |
//! | record that must move after acceptance (§14.3) | room on another page | `retry` + an AG request of class `Reserved` (served above the fill limit) | AG reply, next tick |
//! | deferred effect (E14, D1) | the zeroing of a tracked older copy | `stale` entry (Stale: scrub or extent zeroing scheduled; Source: its newer copy is staged or in flight, or the older copy it shares a page with is being zeroed) | `image_durable` / `ext_zero_durable` / a build that drops it without a write → `stale_gone` re-dirties every deferred page |
//! | write of a key whose newest copy is an unretired txn delete marker | nothing: the marker becomes a tracked older copy of the key (`StaleCopies::take_absent`), its page is excluded from the placement | the "tracked source" row |
//! | delete / expiry removal | tracked copies with a lower seq than itself (D1), or a later durable write (D2) | `gone_wait` (seq), `Pending::Remove { seq }`, `ZeroWhy::Delete` | `stale_gone`, `sources_superseded`; copies that later writes create never extend it |
//! | tracked source | a durable copy with a higher seq | `stale` entry seq | `image_durable`, `ext_write_durable`, `txn_committed`; the page writer's newest-durable-copy check counts a missed one (`missed_handoffs`) |
//! | page / extent write | device completion, then a flush on write-back devices | `io[tag]`, `durable_wait[wid]` + [`crate::flush::FlushTracker`] | `on_io_done`, `on_durable` (FLUSHALL keeps the pages of its old AGs until both happened) |
//! | ticket | each effect holding it, and its metadata | the effects' ticket lists (one dependency per holder, handed over with its count on supersession, D2); handed to an effect of a txn from a staged write outside it, also one metadata dependency in `OpenTxn::barriers` | the effects' written / durable events, meta replies, `txn_committed` |
//! | barrier ticket (SAVE, SHUTDOWN drain) | the effects, open txns and tracked copies present when it executed | one dependency on each (ticket lists, `OpenTxn::barriers`, watched `stale` entries) | their events; later work never extends it |
//! | txn (fx ticket durable) | its `COMMITTED` listing | `meta_wait` (FIFO) | `on_meta_done` |
//! | XTxn part request (`parked`) | the build of a staged effect of its key (a part never cancels one) | `part_wait` | the end of every tick |
//! | abort ticket of an XTxn part | every page / extent still holding a record of the txn | `aborting` items (one dependency each) | `abort_progress` each tick: scrub builds, zeroing of the part's extents once written |
//!
//! Two rules make the table hold under every interleaving: (1) a waiter is registered before the
//! event can be delivered (the partition is single threaded and events are delivered only
//! between calls, so a registration made inside a call is never late), and (2) every event
//! source in the table is itself finite: device requests complete, meta requests are answered in
//! FIFO order, paced work (scrubs no command waits for, warming, reclaim) is bounded by its token
//! bucket, and the page writer builds every dirty page at the next tick.
//!
//! [`PartitionStore::check_progress`] checks the table at any time between calls: every live
//! request and ticket names at least one concrete pending event, request I/O counts match their
//! registrations, ticket dependency counts match the effects that hold them, staged effects sit in
//! pages the writer will build, and no deferred effect waits on a source that only it could
//! release. [`PartitionStore::check_quiescent_complete`] adds that once no input arrives and every
//! submitted I/O completed, nothing is left non-terminal.

use smallvec::SmallVec;

use crate::cache::PageCache;
use crate::image::{InflightState, Pending, RecStatus};
use crate::layout::{PageId, Seq};
use crate::partition::{ExtState, IoKind, PartState, PartitionStore, ReqId, ZeroWhy};
use crate::stale::{CopyState, StaleRef};
use crate::ticket::{TicketId, TicketState};

/// The concrete event a non-terminal item waits for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WaitFor {
    /// Ready: the worker executes it next (`take_ready` then `exec`).
    Exec,
    /// A page read in flight.
    PageRead { page: PageId },
    /// An extent read in flight (`IoTag::id`).
    ExtRead { io: u64 },
    /// A staged or in-flight extent write whose value it reads.
    ExtWrite { start: PageId },
    /// The request ahead of it in one of its key queues.
    KeyQueue { ahead: ReqId },
    /// A barrier (FLUSH, SAVE) submitted before it.
    Barrier { barrier: ReqId },
    /// The oldest request submitted before this barrier.
    Older { oldest: ReqId },
    /// An AG grant (`AcquireAg` wanted or in flight).
    AgGrant,
    /// A seq reservation (E12) or the txn id range extension.
    SeqReservation,
    /// The durable commit of txn `txn`.
    TxnCommit { txn: u64 },
    /// A staged effect in a page the page writer builds at its next tick.
    PageBuild { page: PageId },
    /// A page image write in flight.
    PageWrite { page: PageId },
    /// A flush covering a completed write (write-back cache, §13.8).
    Flush { dev: u8 },
    /// An extent write or zeroing not issued yet (ordering edge E3/E4/E14/D1).
    ExtIssue { start: PageId },
    /// The zeroing of a tracked older copy of its key (D1, E14) at `at`.
    OlderCopy { at: PageId },
    /// A meta-thread reply (FLUSH cutoff durable, txn commit, barrier drain).
    Meta,
    /// An XTxn part request: the staged effects of its keys are built at the next tick
    /// (`Core::part_wait`).
    PartBuild,
}

/// A non-terminal item of the partition.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Item {
    Request(ReqId),
    Ticket(TicketId),
}

/// A non-terminal item, its age and the events it waits for.
#[derive(Clone, Debug)]
pub struct Waiting {
    pub item: Item,
    /// Milliseconds since submission (requests) or execution (tickets), on the partition's
    /// monotonic clock.
    pub age_ms: u64,
    pub waits: SmallVec<[WaitFor; 2]>,
}

/// Where a ticket's dependencies are held: counts per state.
#[derive(Default, Clone, Copy)]
struct Holds {
    unwritten: u32,
    undurable: u32,
    meta: u32,
}

impl PartitionStore {
    /// Pages of this partition in the cache (owned SMALL pages, and pages the partition still
    /// has queued work for).
    pub(crate) fn own_pages<'c>(&'c self, cache: &'c PageCache) -> impl Iterator<Item = (PageId, &'c crate::image::PageState)> + 'c {
        let c = &self.core;
        cache.page_states().filter(move |(p, _)| {
            c.alloc.owns_small(*p)
                || c.dirty.contains(p)
                || c.retry.contains(p)
                || c.deferred.contains(p)
                || c.alloc.ag_of(*p).is_some_and(|(ag, _)| c.retiring_ags.contains(&ag))
        })
    }

    /// Every live request and ticket with the events it waits for (diagnostics, the stall
    /// counters, [`PartitionStore::check_progress`]).
    pub fn waiting(&self, cache: &PageCache) -> Vec<Waiting> {
        let now = self.core.now.mono_ns;
        let mut out = Vec::new();
        for (i, slot) in self.reqs.slots.iter().enumerate() {
            let Some(r) = slot else { continue };
            let id = ReqId(i as u32);
            out.push(Waiting { item: Item::Request(id), age_ms: now.saturating_sub(r.born_ns) / 1_000_000, waits: self.request_waits(id) });
        }
        let holders = self.ticket_holders(cache);
        for (t, k) in self.core.tickets.iter() {
            let waits = holders.iter().filter(|(h, _)| *h == t).map(|(_, w)| *w).collect();
            out.push(Waiting { item: Item::Ticket(t), age_ms: now.saturating_sub(k.born_ns) / 1_000_000, waits });
        }
        out
    }

    /// The events request `id` waits for (empty: nothing will wake it).
    fn request_waits(&self, id: ReqId) -> SmallVec<[WaitFor; 2]> {
        let mut w = SmallVec::new();
        let Some(r) = self.reqs.get(id) else { return w };
        let generation = r.generation;
        if let Some((&txn, _)) = self.reqs.held.iter().find(|(_, h)| **h == id) {
            w.push(WaitFor::TxnCommit { txn });
            return w;
        }
        if r.queued {
            w.push(WaitFor::Exec);
            return w;
        }
        // one entry per registration (a request may register twice for one read)
        for (page, l) in &self.core.loads {
            for _ in l.waiters.iter().filter(|x| **x == (id, generation)) {
                w.push(WaitFor::PageRead { page: *page });
            }
        }
        for (tag, k) in &self.core.io {
            if let IoKind::ExtRead { req, generation: g, .. } = k
                && *req == id
                && *g == generation
            {
                w.push(WaitFor::ExtRead { io: *tag });
            }
        }
        for (start, x) in &self.core.ext.writes {
            for _ in x.waiters.iter().filter(|w| **w == (id, generation)) {
                w.push(WaitFor::ExtWrite { start: *start });
            }
        }
        if r.waits > 0 {
            for q in self.reqs.busy.values() {
                if let Some(pos) = q.iter().position(|x| *x == id)
                    && pos > 0
                {
                    w.push(WaitFor::KeyQueue { ahead: q[pos - 1] });
                }
            }
        }
        if r.parked {
            if self.core.space_wait.contains(&id) && self.core.alloc.ag_outstanding() {
                w.push(WaitFor::AgGrant);
            }
            if self.core.seq_wait.contains(&id) && (self.core.seq_asked || self.core.next_seq < self.core.seq_limit) {
                w.push(WaitFor::SeqReservation);
            }
            if self.core.part_wait.contains(&id) {
                w.push(WaitFor::PartBuild);
            }
        }
        if r.blocked
            && let Some(&(o, b)) = self.reqs.barriers.front()
            && o < r.order
        {
            w.push(WaitFor::Barrier { barrier: b });
        }
        if r.barrier
            && let Some(&(o, oldest)) = self.reqs.order.iter().find(|(o, x)| self.reqs.get(*x).is_some_and(|r| r.order == *o))
            && o < r.order
        {
            w.push(WaitFor::Older { oldest });
        }
        w
    }

    /// Every `(ticket, event)` pair: one entry per dependency an effect or a metadata request
    /// holds for the ticket.
    fn ticket_holders(&self, cache: &PageCache) -> Vec<(TicketId, WaitFor)> {
        let c = &self.core;
        let mut out = Vec::new();
        for (page, st) in self.own_pages(cache) {
            for e in st.pending.iter() {
                for &t in e.tickets() {
                    out.push((t, WaitFor::PageBuild { page }));
                }
            }
            if let Some(inf) = &st.inflight {
                let w = match inf.state {
                    InflightState::Writing => WaitFor::PageWrite { page },
                    InflightState::AwaitFlush => WaitFor::Flush { dev: page.dev() },
                };
                for e in inf.applied.iter() {
                    for &t in e.tickets() {
                        out.push((t, w));
                    }
                }
            }
        }
        for (start, x) in &c.ext.writes {
            let w = match x.state {
                ExtState::Pending => WaitFor::ExtIssue { start: *start },
                ExtState::Writing => WaitFor::PageWrite { page: *start },
                ExtState::AwaitFlush => WaitFor::Flush { dev: start.dev() },
            };
            for &t in &x.tickets {
                out.push((t, w));
            }
        }
        for (start, z) in &c.ext.zeros {
            let w = match z.state {
                ExtState::Pending => WaitFor::ExtIssue { start: *start },
                ExtState::Writing => WaitFor::PageWrite { page: *start },
                ExtState::AwaitFlush => WaitFor::Flush { dev: start.dev() },
            };
            for &t in &z.waiters {
                out.push((t, w));
            }
            if let ZeroWhy::Delete { tickets, .. } = &z.why {
                for &t in tickets {
                    out.push((t, w));
                }
            }
        }
        for (t, at) in c.stale.watched() {
            out.push((t, WaitFor::OlderCopy { at }));
        }
        // aborted txn parts: one dependency per page or extent still holding the txn
        for a in &c.aborting {
            for it in &a.items {
                let w = match *it {
                    crate::abort::AbortItem::Page(page) => WaitFor::PageBuild { page },
                    crate::abort::AbortItem::Ext { start, .. } => WaitFor::ExtIssue { start },
                };
                out.push((a.ticket, w));
            }
        }
        for (db, key, _, ts) in &c.gone_wait {
            let at = c.stale.get(*db, key).first().map_or(PageId(0), |e| e.at.page());
            for &t in ts {
                out.push((t, WaitFor::OlderCopy { at }));
            }
        }
        out
    }

    /// The written / durable / metadata dependencies the partition holds for ticket `t`.
    fn holds(&self, t: TicketId, holders: &[(TicketId, WaitFor)]) -> Holds {
        let mut h = Holds::default();
        for (x, w) in holders {
            if *x != t {
                continue;
            }
            h.undurable += 1;
            if !matches!(w, WaitFor::Flush { .. }) {
                h.unwritten += 1;
            }
        }
        let c = &self.core;
        for o in c.txns.values() {
            if o.local && o.ticket == Some(t) {
                h.meta += 1;
            }
        }
        for m in &c.meta_wait {
            if let crate::partition::MetaWait::Cutoff { ticket, .. } = m
                && *ticket == t
            {
                h.meta += 1;
            }
        }
        for o in c.txns.values() {
            h.meta += o.barriers.iter().filter(|&&b| b == t).count() as u32;
        }
        h
    }

    /// Structural liveness check, valid between any two driver calls: `Err` names the first
    /// item that nothing will ever wake, with the partition's state dump.
    pub fn check_progress(&self, cache: &PageCache) -> Result<(), String> {
        let fail = |what: String| Err(format!("p{} liveness: {what}\n{}", self.core.cfg.id, self.diag_deep(cache)));
        if self.core.state == PartState::Loading {
            return Ok(());
        }
        let c = &self.core;
        let failed = c.state == PartState::ReadOnly;
        // requests: a registered wakeup for every reason it is not ready
        for (i, slot) in self.reqs.slots.iter().enumerate() {
            let Some(r) = slot else { continue };
            let id = ReqId(i as u32);
            let waits = self.request_waits(id);
            if waits.is_empty() {
                return fail(format!("request {i} (io={} waits={} parked={} blocked={} barrier={}) waits for nothing", r.io, r.waits, r.parked, r.blocked, r.barrier));
            }
            if self.reqs.held.values().any(|h| *h == id) {
                continue;
            }
            let io = waits.iter().filter(|w| matches!(w, WaitFor::PageRead { .. } | WaitFor::ExtRead { .. } | WaitFor::ExtWrite { .. })).count() as u32;
            if !r.queued && io != r.io {
                return fail(format!("request {i} counts {} outstanding reads, {io} are registered", r.io));
            }
            let kq = waits.iter().filter(|w| matches!(w, WaitFor::KeyQueue { .. })).count() as u32;
            if !r.queued && kq != r.waits {
                return fail(format!("request {i} counts {} key queues ahead, {kq} found", r.waits));
            }
            if r.parked && !waits.iter().any(|w| matches!(w, WaitFor::AgGrant | WaitFor::SeqReservation | WaitFor::PartBuild)) {
                return fail(format!("request {i} is parked with no AG request or seq reservation outstanding"));
            }
        }
        // tickets: every dependency is held by an effect or a metadata request
        let holders = self.ticket_holders(cache);
        for (t, k) in c.tickets.iter() {
            if failed {
                break;
            }
            let h = self.holds(t, &holders);
            if k.undurable() != h.undurable || k.unwritten() != h.unwritten || k.meta() != h.meta {
                let pages: Vec<String> = k
                    .deps
                    .iter()
                    .map(|(p, _)| match cache.page_state(*p) {
                        None => format!("{:#x}: not cached", p.0),
                        Some(st) => format!(
                            "{:#x}: owned={} pending={} inflight={:?} dirty={}",
                            p.0,
                            c.alloc.owns_small(*p),
                            st.pending.len(),
                            st.inflight.as_ref().map(|i| (i.state, i.applied.iter().map(|e| e.tickets().len()).sum::<usize>())),
                            st.dirty
                        ),
                    })
                    .collect();
                return fail(format!(
                    "ticket {} counts unwritten={} undurable={} meta={} (txn effects {:?}), the partition holds unwritten={} undurable={} meta={}; pages [{}] extents {:?}; awaiting flush {:?}",
                    t.0,
                    k.unwritten(),
                    k.undurable(),
                    k.meta(),
                    k.txn_effects(),
                    h.unwritten,
                    h.undurable,
                    h.meta,
                    pages.join(", "),
                    k.extent_deps.iter().map(|p| p.0).collect::<Vec<_>>(),
                    c.durable_wait.values().map(|d| format!("{d:?}")).collect::<Vec<_>>()
                ));
            }
        }
        // staged effects: in pages the page writer will build (a read-only partition builds
        // nothing; its tickets failed)
        for (page, st) in self.own_pages(cache).filter(|_| !failed) {
            if st.pending.is_empty() || st.corrupt {
                continue;
            }
            let queued = c.dirty.contains(&page) || c.retry.contains(&page) || st.inflight.is_some();
            if queued {
                continue;
            }
            if !c.deferred.contains(&page) {
                return fail(format!("page {:#x} holds {} staged effects and is neither dirty nor deferred", page.0, st.pending.len()));
            }
            // a deferred effect waits for an older copy of its key that is on its way out
            for e in st.pending.iter() {
                let (db, key) = (e.db(), e.key());
                let others: SmallVec<[_; 2]> = c.stale.get(db, key).iter().filter(|x| x.at.page() != page).copied().collect();
                if others.is_empty() {
                    continue;
                }
                let moving = others.iter().any(|x| match (x.state, x.at) {
                    (CopyState::Stale, StaleRef::Small { page: at }) => self.scrub_scheduled(cache, db, key, at),
                    (CopyState::Stale, StaleRef::Extent { page: at, .. }) => c.ext.zeros.contains_key(&at),
                    // the older copy that location also holds is being zeroed (E14)
                    (CopyState::Source, StaleRef::Small { page: at }) if x.older && self.scrub_scheduled(cache, db, key, at) => true,
                    (CopyState::Source, _) => self.newer_copy_elsewhere(cache, db, key, x.seq, page),
                });
                if !moving {
                    return fail(format!(
                        "deferred effect at page {:#x} waits for older copies of its key that nothing zeroes ({})",
                        page.0,
                        others.iter().map(|x| format!("{:#x} {:?} seq={:#x}", x.at.page().0, x.state, x.seq.0)).collect::<Vec<_>>().join(", ")
                    ));
                }
            }
        }
        Ok(())
    }

    /// True when a build zeroing the tracked copy of `(db, key)` at `page` is scheduled (paced
    /// or foreground).
    fn scrub_scheduled(&self, cache: &PageCache, db: u8, key: &[u8], page: PageId) -> bool {
        let c = &self.core;
        c.scrub_queue.contains(&page)
            || c.loads.get(&page).is_some_and(|l| l.scrub)
            || c.dirty.contains(&page)
            || c.retry.contains(&page)
            || cache.page_state(page).is_some_and(|st| st.scrub || st.dirty || st.inflight.is_some())
            || c.fg_keys.iter().any(|(d, k)| *d == db && **k == *key)
    }

    /// True when a copy of `(db, key)` newer than `seq` is staged or being written outside
    /// `except` (the event that releases a tracked source).
    fn newer_copy_elsewhere(&self, cache: &PageCache, db: u8, key: &[u8], seq: Seq, except: PageId) -> bool {
        let c = &self.core;
        let in_pages = self.own_pages(cache).any(|(p, st)| {
            p != except
                && (st.pending.iter().any(|e| matches!(e, Pending::Put(r) if r.desc.db == db && r.desc.seq > seq && r.key() == key))
                    || st.inflight.as_ref().is_some_and(|inf| inf.applied.iter().any(|e| matches!(e, Pending::Put(r) if r.desc.db == db && r.desc.seq > seq && r.key() == key))))
        });
        // a txn whose records are durable releases its sources at its commit, unless one of
        // its records is the deferred effect itself (it cannot commit before that is written)
        in_pages
            || c.ext.writes.values().any(|w| w.db == db && w.hdr.seq > seq && *w.key == *key)
            || c.txns.values().any(|o| o.seq > seq && !o.pages.contains(&except) && o.keys.iter().any(|(d, k)| *d == db && **k == *key))
    }

    /// Quiescence check: with no new input and every submitted I/O completed (and a bounded
    /// number of ticks run), nothing may be left non-terminal: no live request or ticket, no
    /// open txn, nothing staged, in flight or waiting for durability, no tracked copy, no absent
    /// newest copy waiting for retirement; every Live record of a cached page is the one its
    /// key's index entry names (or a tracked copy), and no record is left Protected.
    pub fn check_quiescent_complete(&self, cache: &PageCache) -> Result<(), String> {
        self.check_progress(cache)?;
        let c = &self.core;
        let fail = |what: String| Err(format!("p{} not complete: {what}\n{}", c.cfg.id, self.diag_deep(cache)));
        if c.state == PartState::Loading {
            return fail("still loading".into());
        }
        if let Some(w) = self.waiting(cache).first() {
            return fail(format!("{:?} (age {} ms) still waits for {:?}", w.item, w.age_ms, w.waits));
        }
        let queues = [
            ("dirty pages", c.dirty.len()),
            ("pages to retry", c.retry.len()),
            ("deferred pages", c.deferred.len()),
            ("writes in flight", c.writes_in_flight as usize),
            ("extent writes", c.ext.writes.len()),
            ("extent zeroings", c.ext.zeros.len()),
            ("writes waiting for a flush", c.durable_wait.len() + c.durable_q.len()),
            ("open txns", c.txns.len()),
            ("meta requests", c.meta_wait.len()),
            ("deletes waiting for older copies", c.gone_wait.len()),
            ("requests parked on space", c.space_wait.len()),
            ("requests parked on seqs", c.seq_wait.len()),
            ("held txn requests", self.reqs.held.len()),
            ("tracked copies", c.stale.len()),
            ("absent copies to retire", c.retire.len()),
            ("foreground scrub keys", c.fg_keys.len()),
            ("lost hand-offs", c.counters.missed_handoffs as usize),
        ];
        for (what, n) in queues {
            if n > 0 {
                return fail(format!("{n} {what}"));
            }
        }
        // records: a Live record of a cached page is its key's current copy here
        let seed = c.cfg.hash_seed;
        for (page, st) in self.own_pages(cache) {
            if !st.pending.is_empty() || st.inflight.is_some() || st.corrupt {
                return fail(format!("page {:#x} still has staged or in-flight work", page.0));
            }
            let img = cache.frame(st.m_frame);
            for r in st.recs.iter() {
                match r.status {
                    RecStatus::Protected => return fail(format!("page {:#x}: record seq {:#x} is still protected", page.0, r.seq.0)),
                    RecStatus::Live if !r.is_marker() && !c.dead(r.db, r.seq, r.txn) => {
                        let key = r.key(img);
                        let kh = kv_common::KeyHash::of(seed, key);
                        let slot = kv_common::key_slot(key);
                        if !c.indexed_at(r.db, slot, kh, page.to36()) && c.stale.state_at(r.db, key, page).is_none() {
                            return fail(format!("page {:#x}: Live record seq {:#x} (key of {} bytes) is not the indexed copy", page.0, r.seq.0, key.len()));
                        }
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    /// Panics with the state dump unless [`PartitionStore::check_quiescent_complete`] holds.
    pub fn assert_quiescent_complete(&self, cache: &PageCache) {
        if let Err(e) = self.check_quiescent_complete(cache) {
            panic!("{e}");
        }
    }

    /// Refreshes the stall counters (`requests_waiting_over_1s`, `oldest_wait_ms`,
    /// `stalled_total`) from the ages of live requests and tickets.
    pub(crate) fn update_stall_counters(&mut self, cache: &PageCache) {
        let now = self.core.now.mono_ns;
        let mut over = 0u64;
        let mut oldest = 0u64;
        let mut newly = 0u64;
        for r in self.reqs.slots.iter_mut().flatten() {
            let age = now.saturating_sub(r.born_ns);
            oldest = oldest.max(age);
            if age >= STALL_NS {
                over += 1;
                if !r.stalled {
                    r.stalled = true;
                    newly += 1;
                }
            }
        }
        for k in self.core.tickets.iter_mut() {
            let age = now.saturating_sub(k.born_ns);
            oldest = oldest.max(age);
            if age >= STALL_NS {
                over += 1;
                if !k.stalled {
                    k.stalled = true;
                    newly += 1;
                }
            }
        }
        let c = &mut self.core.counters;
        c.requests_waiting_over_1s = over;
        c.oldest_wait_ms = oldest / 1_000_000;
        c.stalled_total += newly;
        if let Some(ms) = stall_trace_ms() {
            self.trace_stalls(cache, now, ms * 1_000_000);
        }
    }

    /// The stall trace's deep dump (`KVSSD_STALL_TRACE_MS`): once per partition and stuck
    /// ticket older than ten times the trace limit, the deferred pages' staged effects and the
    /// tracked copies (key lengths only).
    pub(crate) fn trace_stalls_deep(&mut self, cache: &PageCache) {
        let Some(ms) = stall_trace_ms() else { return };
        let now = self.core.now.mono_ns;
        let limit = ms * 10_000_000;
        let mut hit = false;
        for k in self.core.tickets.iter_mut() {
            if k.state == TicketState::Executed && now.saturating_sub(k.born_ns) >= limit && !k.deep_traced {
                k.deep_traced = true;
                hit = true;
            }
        }
        if hit {
            eprintln!("kvssd stall trace deep p{}: {}", self.core.cfg.id, self.diag_deep(cache).replace('\n', " || "));
        }
    }

    /// `KVSSD_STALL_TRACE_MS=<ms>`: prints, once per request and per unwritten ticket older
    /// than `<ms>`, what it waits for (diagnostics on stderr; no key or value bytes).
    fn trace_stalls(&mut self, cache: &PageCache, now: u64, limit: u64) {
        let mut lines = Vec::new();
        for (i, r) in self.reqs.slots.iter_mut().enumerate() {
            let Some(r) = r else { continue };
            let age = now.saturating_sub(r.born_ns);
            if age < limit || r.traced {
                continue;
            }
            r.traced = true;
            lines.push(format!(
                "request {i} kind={:?} keys={} age={}ms io={} waits={} parked={} queued={} blocked={} barrier={} failed={:?}",
                r.req.kind,
                r.keys.len(),
                age / 1_000_000,
                r.io,
                r.waits,
                r.parked,
                r.queued,
                r.blocked,
                r.barrier,
                r.failed
            ));
        }
        for k in self.core.tickets.iter_mut() {
            let age = now.saturating_sub(k.born_ns);
            if age < limit || k.traced || k.state != TicketState::Executed {
                continue;
            }
            k.traced = true;
            lines.push(format!(
                "ticket age={}ms unwritten={} undurable={} meta={} txn={:?} txn_effects={:?} pages={:?} extents={:?}",
                age / 1_000_000,
                k.unwritten(),
                k.undurable(),
                k.meta(),
                k.txn,
                k.txn_effects(),
                k.deps.iter().map(|d| page_wait(cache, d.0, now)).collect::<Vec<_>>(),
                k.extent_deps.iter().map(|p| p.0).collect::<Vec<_>>()
            ));
        }
        if !lines.is_empty() {
            let d = self.diag();
            for l in lines {
                eprintln!("kvssd stall trace p{}: {l} | {d}", self.core.cfg.id);
            }
        }
    }
}

/// What a page a stalled ticket depends on is doing (stall trace): `W<ms>` its image write in
/// flight for that long, `F<ms>` written and awaiting a covering flush for that long, `D<n>` no
/// image in flight and `n` staged changes (waiting for a build), `-` not cached.
fn page_wait(cache: &PageCache, p: crate::layout::PageId, now: u64) -> String {
    match cache.page_state(p) {
        None => format!("{}:-", p.0),
        Some(st) => match &st.inflight {
            Some(i) => {
                let ms = now.saturating_sub(i.since_ns) / 1_000_000;
                match i.state {
                    InflightState::Writing => format!("{}:W{ms}", p.0),
                    InflightState::AwaitFlush => format!("{}:F{ms}", p.0),
                }
            }
            None => format!("{}:D{}", p.0, st.pending.len()),
        },
    }
}

pub(crate) fn stall_trace_ms() -> Option<u64> {
    static MS: std::sync::OnceLock<Option<u64>> = std::sync::OnceLock::new();
    *MS.get_or_init(|| std::env::var("KVSSD_STALL_TRACE_MS").ok().and_then(|v| v.parse().ok()))
}

/// `KVSSD_CHECK_PROGRESS=1` makes every tick run [`PartitionStore::check_progress`] and panic
/// on a lost wakeup (simulator and test campaigns; the check walks the partition's cached
/// pages, too slow for production).
pub(crate) fn self_check() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("KVSSD_CHECK_PROGRESS").is_some_and(|v| v != "0"))
}

/// Age after which a request or ticket counts as stalled.
const STALL_NS: u64 = 1_000_000_000;
