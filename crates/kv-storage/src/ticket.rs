//! Tickets: when a command's storage effects are written / durable (DESIGN §13.9, §14.7).
//!
//! `Ticket { deps: (page, gen) pairs, extent deps, txn, flags HAS_DELETE | HAS_META }` moves
//! `Executed -> Written -> Durable` (or `Failed(errno)`). `Written` = every dependency's image
//! write completed; `Durable` = durable per the device cache model (and the txn committed
//! durably, meta writes durable). Reply release per durability mode: `async` after exec
//! (deletes, FLUSH and txn commits after Written), `written` after Written, `flushed` after
//! Durable; in cluster mode additionally after the shard commit (§23.8).

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
    /// `(page, gen)` image dependencies.
    pub(crate) deps: SmallVec<[(PageId, u32); 2]>,
    /// Extent writes (first page).
    pub(crate) extent_deps: SmallVec<[PageId; 1]>,
    pub(crate) txn: Option<u64>,
    pub(crate) flags: u8,
    pub(crate) state: TicketState,
}
