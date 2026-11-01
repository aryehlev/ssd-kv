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

/// `(db, exact key) -> older copies`.
type StaleMap = hashbrown::HashMap<(u8, Box<[u8]>), SmallVec<[StaleRef; 1]>, foldhash::fast::RandomState>;

/// The partition's tracked stale copies (exact keys).
pub(crate) struct StaleCopies {
    map: StaleMap,
}
