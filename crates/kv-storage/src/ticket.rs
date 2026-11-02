//! Tickets: when a command's storage effects are written / durable (DESIGN §13.9, §14.7).
//!
//! `Ticket { deps: (page, gen) pairs, extent deps, txn, flags HAS_DELETE | HAS_META }` moves
//! `Executed -> Written -> Durable` (or `Failed(errno)`). `Written` = every dependency's image
//! write completed; `Durable` = durable per the device cache model (and the txn committed
//! durably, meta writes durable). Reply release per durability mode: `async` after exec
//! (deletes, FLUSH and txn commits after Written), `written` after Written, `flushed` after
//! Durable; in cluster mode additionally after the shard commit (§23.8).
//!
//! Implementation: every staged effect of a command (a page `Put`/`Remove`, an extent write or
//! zeroing) carries the ids of the tickets waiting for it and counts as one dependency of each.
//! When the image or extent write that materializes the effect completes, each of its tickets
//! has one dependency written; when that write is durable, one dependency durable. A staged
//! effect superseded by a later effect of the same key hands its tickets to the survivor (D2),
//! so the count is preserved. Metadata dependencies (FLUSH cutoffs, txn commits) count towards
//! both states: a ticket is `Written` only once its metadata is durable too (E5, E6).

use hashbrown::HashMap;
use smallvec::SmallVec;

use crate::layout::PageId;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct TicketId(pub u64);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TicketState {
    Executed,
    Written,
    Durable,
    Failed(i32),
}

/// Ticket flag: the command deleted a key (reply waits per §16.2).
pub(crate) const TICKET_HAS_DELETE: u8 = 1;
/// Ticket flag: the command needs a metadata write (FLUSH cutoff, txn commit).
pub(crate) const TICKET_HAS_META: u8 = 2;

pub(crate) struct Ticket {
    /// `(page, gen)` image dependencies (diagnostics: the pages the command touched).
    pub(crate) deps: SmallVec<[(PageId, u32); 2]>,
    /// Extent writes (first page).
    pub(crate) extent_deps: SmallVec<[PageId; 1]>,
    pub(crate) txn: Option<u64>,
    pub(crate) flags: u8,
    pub(crate) state: TicketState,
    /// Effect dependencies whose write has not completed.
    unwritten: u32,
    /// Effect dependencies whose write is not durable.
    undurable: u32,
    /// Metadata dependencies not durable.
    meta: u32,
    /// Still being assembled by the executing command (no state is reported before `seal`).
    open: bool,
    /// Internal ticket of a txn's effects (§14.6): its completion is reported through
    /// [`Tickets::take_txns_durable`], never to a reply.
    txn_effects: Option<u64>,
    /// Monotonic time the ticket was created (age tracking, stall counters).
    pub(crate) born_ns: u64,
    /// Counted once in the stall counter.
    pub(crate) stalled: bool,
    /// Reported once by the stall trace (`KVSSD_STALL_TRACE_MS`).
    pub(crate) traced: bool,
    /// Reported once by the stall trace's deep dump.
    pub(crate) deep_traced: bool,
}

impl Ticket {
    /// Effect dependencies whose write has not completed.
    pub(crate) fn unwritten(&self) -> u32 {
        self.unwritten
    }

    /// Effect dependencies whose write is not durable.
    pub(crate) fn undurable(&self) -> u32 {
        self.undurable
    }

    /// Metadata dependencies not durable.
    pub(crate) fn meta(&self) -> u32 {
        self.meta
    }

    /// The txn whose effects this internal ticket counts.
    pub(crate) fn txn_effects(&self) -> Option<u64> {
        self.txn_effects
    }
}

/// The partition's live tickets.
#[derive(Default)]
pub(crate) struct Tickets {
    map: HashMap<u64, Ticket, foldhash::fast::FixedState>,
    next: u64,
    changes: Vec<(TicketId, TicketState)>,
    /// Txns whose every effect is durable (internal tickets that completed).
    txns_durable: Vec<u64>,
    /// Txns whose internal ticket failed: `(txn, errno)`.
    txns_failed: Vec<(u64, i32)>,
    /// The partition's monotonic clock (set before commands execute; new tickets' birth time).
    pub(crate) now_ns: u64,
}

impl Tickets {
    pub(crate) fn new() -> Self {
        Tickets {
            map: HashMap::with_hasher(foldhash::fast::FixedState::default()),
            next: 1,
            changes: Vec::new(),
            txns_durable: Vec::new(),
            txns_failed: Vec::new(),
            now_ns: 0,
        }
    }

    /// A new open ticket (dependencies are added while the command commits; see `seal`).
    pub(crate) fn create(&mut self, flags: u8) -> TicketId {
        let id = self.next;
        self.next += 1;
        self.map.insert(
            id,
            Ticket {
                deps: SmallVec::new(),
                extent_deps: SmallVec::new(),
                txn: None,
                flags,
                state: TicketState::Executed,
                unwritten: 0,
                undurable: 0,
                meta: 0,
                open: true,
                txn_effects: None,
                born_ns: self.now_ns,
                stalled: false,
                traced: false,
                deep_traced: false,
            },
        );
        TicketId(id)
    }

    /// An internal ticket counting the effects of txn `txn` (§14.6). It travels with the
    /// txn's staged effects like a command ticket (superseded effects hand it to their
    /// survivor, D2); when it is durable the txn may be committed.
    pub(crate) fn create_txn(&mut self, txn: u64) -> TicketId {
        let t = self.create(0);
        if let Some(k) = self.map.get_mut(&t.0) {
            k.txn_effects = Some(txn);
            k.txn = Some(txn);
        }
        t
    }

    /// True for the internal ticket of a txn.
    pub(crate) fn is_internal(&self, t: TicketId) -> bool {
        self.map.get(&t.0).is_some_and(|k| k.txn_effects.is_some())
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, t: TicketId) -> bool {
        self.map.contains_key(&t.0)
    }

    /// Live tickets (not Durable or Failed yet).
    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }

    /// Every live ticket (liveness checks and stall counters).
    pub(crate) fn iter(&self) -> impl Iterator<Item = (TicketId, &Ticket)> + '_ {
        self.map.iter().map(|(&id, k)| (TicketId(id), k))
    }

    /// Every live ticket, mutable (stall counters).
    pub(crate) fn iter_mut(&mut self) -> impl Iterator<Item = &mut Ticket> + '_ {
        self.map.values_mut()
    }

    /// One more effect dependency (one write to complete, one durability to reach).
    pub(crate) fn add_effect(&mut self, t: TicketId) {
        if let Some(k) = self.map.get_mut(&t.0) {
            k.unwritten += 1;
            k.undurable += 1;
        }
    }

    /// One more dependency on an effect whose write completed already (it waits only for
    /// durability).
    pub(crate) fn add_durable_only(&mut self, t: TicketId) {
        if let Some(k) = self.map.get_mut(&t.0) {
            k.undurable += 1;
        }
    }

    /// One more metadata dependency.
    pub(crate) fn add_meta(&mut self, t: TicketId) {
        if let Some(k) = self.map.get_mut(&t.0) {
            k.meta += 1;
            k.flags |= TICKET_HAS_META;
        }
    }

    pub(crate) fn note_page(&mut self, t: TicketId, page: PageId, r#gen: u32) {
        if let Some(k) = self.map.get_mut(&t.0)
            && k.deps.len() < 8
            && !k.deps.iter().any(|d| d.0 == page)
        {
            k.deps.push((page, r#gen));
        }
    }

    pub(crate) fn note_extent(&mut self, t: TicketId, page: PageId) {
        if let Some(k) = self.map.get_mut(&t.0)
            && k.extent_deps.len() < 8
        {
            k.extent_deps.push(page);
        }
    }

    /// The command finished assembling the ticket: report what is already satisfied.
    pub(crate) fn seal(&mut self, t: TicketId) {
        if let Some(k) = self.map.get_mut(&t.0) {
            k.open = false;
        }
        self.evaluate(t);
    }

    /// One effect dependency's write completed.
    pub(crate) fn effect_written(&mut self, t: TicketId) {
        if let Some(k) = self.map.get_mut(&t.0) {
            k.unwritten = k.unwritten.saturating_sub(1);
        }
        self.evaluate(t);
    }

    /// One effect dependency's write is durable (implies written).
    pub(crate) fn effect_durable(&mut self, t: TicketId) {
        if let Some(k) = self.map.get_mut(&t.0) {
            k.undurable = k.undurable.saturating_sub(1);
            k.unwritten = k.unwritten.min(k.undurable);
        }
        self.evaluate(t);
    }

    /// An effect dependency resolved without any write (nothing on media to change).
    pub(crate) fn effect_resolved(&mut self, t: TicketId) {
        self.effect_written(t);
        self.effect_durable(t);
    }

    /// One metadata dependency is durable.
    pub(crate) fn meta_done(&mut self, t: TicketId) {
        if let Some(k) = self.map.get_mut(&t.0) {
            k.meta = k.meta.saturating_sub(1);
        }
        self.evaluate(t);
    }

    /// The ticket failed (write I/O error); later dependency events for it are ignored.
    pub(crate) fn fail(&mut self, t: TicketId, errno: i32) {
        if let Some(k) = self.map.remove(&t.0) {
            debug_assert!(!matches!(k.state, TicketState::Durable));
            match k.txn_effects {
                Some(txn) => self.txns_failed.push((txn, errno)),
                None => self.changes.push((t, TicketState::Failed(errno))),
            }
        }
    }

    /// Fails every live ticket (the partition entered ReadOnly after a write error).
    pub(crate) fn fail_all(&mut self, errno: i32) {
        let ids: Vec<u64> = self.map.keys().copied().collect();
        for id in ids {
            self.fail(TicketId(id), errno);
        }
    }

    fn evaluate(&mut self, t: TicketId) {
        let Some(k) = self.map.get_mut(&t.0) else { return };
        if k.open || k.meta > 0 {
            return;
        }
        if let Some(txn) = k.txn_effects {
            if k.undurable == 0 {
                self.map.remove(&t.0);
                self.txns_durable.push(txn);
            }
            return;
        }
        if k.unwritten == 0 && k.state == TicketState::Executed {
            k.state = TicketState::Written;
            self.changes.push((t, TicketState::Written));
        }
        if k.undurable == 0 {
            self.map.remove(&t.0);
            self.changes.push((t, TicketState::Durable));
        }
    }

    /// Txns whose effects all became durable since the last call.
    pub(crate) fn take_txns_durable(&mut self, out: &mut Vec<u64>) {
        out.append(&mut self.txns_durable);
    }

    /// Txns whose effects failed since the last call.
    pub(crate) fn take_txns_failed(&mut self, out: &mut Vec<(u64, i32)>) {
        out.append(&mut self.txns_failed);
    }


    /// Drains the state changes since the last call (in the order they happened).
    pub(crate) fn take(&mut self, out: &mut Vec<(TicketId, TicketState)>) {
        out.append(&mut self.changes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drain(t: &mut Tickets) -> Vec<(TicketId, TicketState)> {
        let mut v = Vec::new();
        t.take(&mut v);
        v
    }

    #[test]
    fn effects_move_written_then_durable() {
        let mut t = Tickets::new();
        let a = t.create(0);
        t.add_effect(a);
        t.add_effect(a);
        t.seal(a);
        assert!(drain(&mut t).is_empty());
        t.effect_written(a);
        assert!(drain(&mut t).is_empty());
        t.effect_written(a);
        assert_eq!(drain(&mut t), vec![(a, TicketState::Written)]);
        t.effect_durable(a);
        t.effect_durable(a);
        assert_eq!(drain(&mut t), vec![(a, TicketState::Durable)]);
        assert!(!t.contains(a));
    }

    #[test]
    fn empty_ticket_is_durable_at_seal() {
        let mut t = Tickets::new();
        let a = t.create(TICKET_HAS_DELETE);
        t.seal(a);
        assert_eq!(drain(&mut t), vec![(a, TicketState::Written), (a, TicketState::Durable)]);
    }

    #[test]
    fn durable_without_written_event_reports_both() {
        let mut t = Tickets::new();
        let a = t.create(0);
        t.add_effect(a);
        t.seal(a);
        t.effect_durable(a);
        assert_eq!(drain(&mut t), vec![(a, TicketState::Written), (a, TicketState::Durable)]);
    }

    #[test]
    fn meta_dependency_gates_both_states() {
        let mut t = Tickets::new();
        let a = t.create(0);
        t.add_effect(a);
        t.add_meta(a);
        t.seal(a);
        t.effect_resolved(a);
        assert!(drain(&mut t).is_empty());
        t.meta_done(a);
        assert_eq!(drain(&mut t), vec![(a, TicketState::Written), (a, TicketState::Durable)]);
    }

    #[test]
    fn txn_ticket_reports_to_the_txn_list_only() {
        let mut t = Tickets::new();
        let x = t.create_txn(77);
        assert!(t.is_internal(x));
        t.add_effect(x);
        t.add_effect(x);
        t.seal(x);
        t.effect_durable(x);
        let mut done = Vec::new();
        t.take_txns_durable(&mut done);
        assert!(done.is_empty());
        t.effect_durable(x);
        t.take_txns_durable(&mut done);
        assert_eq!(done, vec![77]);
        assert!(drain(&mut t).is_empty(), "internal tickets never reach replies");
        let y = t.create_txn(78);
        t.add_effect(y);
        t.seal(y);
        t.fail_all(-5);
        let mut failed = Vec::new();
        t.take_txns_failed(&mut failed);
        assert_eq!(failed, vec![(78, -5)]);
        assert!(drain(&mut t).is_empty());
    }

    #[test]
    fn failure_is_terminal() {
        let mut t = Tickets::new();
        let a = t.create(0);
        t.add_effect(a);
        t.seal(a);
        t.fail(a, -5);
        t.effect_resolved(a);
        assert_eq!(drain(&mut t), vec![(a, TicketState::Failed(-5))]);
        let b = t.create(0);
        t.add_effect(b);
        t.seal(b);
        t.fail_all(-5);
        assert_eq!(drain(&mut t), vec![(b, TicketState::Failed(-5))]);
        assert_eq!(t.len(), 0);
    }
}
