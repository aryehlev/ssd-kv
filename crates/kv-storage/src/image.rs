//! The page writer: building the next image of a SMALL page (DESIGN §13).
//!
//! Let M be a page's last durable image and N the image being built. Rules:
//! * R1 (two-image mode) every protected record of M appears in N byte-identical at the same
//!   offset; R1a (atomic mode) a non-txn update whose rounded length is unchanged or smaller is
//!   rewritten at its existing offset.
//! * R2 records that need free bytes are placed first-fit-decreasing by encoded length (ties by
//!   `h32`, then key bytes) at the lowest 16-aligned offset that overlaps no live/protected
//!   record of M and nothing already placed in N; placement depends on sizes only.
//! * R3 a record of M is dropped only if droppable; in two-image mode every superseded copy is
//!   zeroed by the page's first build after its newer version became durable (I11).
//! * R4 every byte of the written range not in a kept or placed record is zero.
//! * R5 at most one image write per page in flight; N is issued only after M is durable (E1).
//!
//! Classification at build time (§13.3): Live, Protected (newest durable copy while a newer one
//! is in flight; removal target while other copies exist; superseded by an uncommitted txn;
//! source of a move whose new copy is not durable), Superseded, Droppable. Builds are event
//! driven at the end of the tick, never timed (§13.7). I/O shape per §13.6.

use smallvec::SmallVec;

use crate::cache::FrameIdx;
use crate::layout::{PageId, Seq};

/// In-RAM state of a cached SMALL page (§13.2).
pub struct PageState {
    pub page: PageId,
    /// M bytes (fresh page: treated as all-garbage, never parsed)
    pub m_frame: FrameIdx,
    /// valid records of M (from tiling parse or from the last build)
    pub recs: SmallVec<[RecDesc; 16]>,
    /// staged changes not in any image yet, in execution order (read overlay); placement
    /// order is R2, not this order
    pub pending: SmallVec<[Pending; 4]>,
    /// N frame, its recs, sub-I/O count, state Writing|AwaitFlush
    pub inflight: Option<Inflight>,
    pub fresh: bool,
    /// +1 per durable build (tickets reference (page, gen))
    pub gen: u32,
}

/// A record of an image.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RecDesc {
    pub off: u16,
    pub len: u16,
    pub h32: u32,
    pub db: u8,
    pub kind: u8,
    pub flags: u8,
    pub seq: Seq,
    pub txn: u64,
    pub status: RecStatus,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RecStatus {
    Live,
    Protected,
    Superseded,
    Droppable,
}

/// An encoded record waiting to be placed: boxed bytes + RecDesc template.
pub struct EncodedRecord {
    pub bytes: Box<[u8]>,
    pub desc: RecDesc,
}

/// A staged change (holds user bytes: no `Debug`, LC-32).
pub enum Pending {
    Put(EncodedRecord),
    Remove { db: u8, h32: u32, key: Box<[u8]> },
}

/// Write state of an in-flight image.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum InflightState {
    Writing,
    AwaitFlush,
}

/// The image N being written.
pub struct Inflight {
    pub n_frame: FrameIdx,
    pub recs: SmallVec<[RecDesc; 16]>,
    /// Outstanding sub-I/Os (up to two sector spans in two-image mode).
    pub sub_ios: u8,
    pub state: InflightState,
}
