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
//!
//! [`build`] is pure: it reads M, writes N into a frame the caller provides and reports what the
//! image materializes, what it must defer (deletes and writes waiting for older copies to be
//! zeroed, D1/E9/E14) and what it could not place (the partition moves those records to another
//! page, §14.3). The partition decides when a build may run (R5) and owns durability.

use kv_common::RecordCrc;
use smallvec::SmallVec;

use crate::cache::FrameIdx;
use crate::io::PageUpdateMode;
use crate::layout::{PAGE, PageId, SMALL_HDR, Seq};
use crate::page::write_small_header;
use crate::record::{REC_FLAG_TXN, REC_HDR, REC_HDR_TXN, REC_KIND_TXN_DELETE_MARKER, seal_small};
use crate::stale::CopyState;
use crate::ticket::TicketId;

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
    /// +1 per durable build (tickets reference (page, gen)); `gen` is reserved in edition 2024,
    /// hence the raw identifier.
    pub r#gen: u32,
    /// Queued for a build by the partition.
    pub(crate) dirty: bool,
    /// A build is wanted to zero superseded or tracked stale copies (scrub).
    pub(crate) scrub: bool,
    /// The page failed verification (§15); never served.
    pub(crate) corrupt: bool,
    /// The partition holds one frame pin for this page's pending/inflight work.
    pub(crate) work_pin: bool,
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
    /// Expiry (unix ms) when `flags` has HAS_EXPIRE, else 0.
    pub expire_at_ms: u64,
    pub key_len: u16,
    pub val_len: u16,
}

impl RecDesc {
    /// Header bytes (32, or 40 with a txn id).
    pub fn hdr_len(&self) -> usize {
        if self.flags & REC_FLAG_TXN != 0 { REC_HDR_TXN } else { REC_HDR }
    }

    /// Key bytes of this record inside the image `img`.
    pub fn key<'a>(&self, img: &'a [u8]) -> &'a [u8] {
        let s = usize::from(self.off) + self.hdr_len();
        &img[s..s + usize::from(self.key_len)]
    }

    /// Value bytes of this record inside the image `img`.
    pub fn val<'a>(&self, img: &'a [u8]) -> &'a [u8] {
        let s = usize::from(self.off) + self.hdr_len() + usize::from(self.key_len);
        &img[s..s + usize::from(self.val_len)]
    }

    pub fn expire_at(&self) -> Option<u64> {
        (self.flags & crate::record::REC_FLAG_HAS_EXPIRE != 0).then_some(self.expire_at_ms)
    }

    pub fn is_marker(&self) -> bool {
        self.kind == REC_KIND_TXN_DELETE_MARKER
    }

    fn end(&self) -> u16 {
        self.off + self.len
    }
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
    /// The record's tile (header, key, value, zero padding); sealed for its location when placed.
    pub bytes: Box<[u8]>,
    pub desc: RecDesc,
    /// Tickets waiting for this effect.
    pub(crate) tickets: SmallVec<[TicketId; 1]>,
}

impl EncodedRecord {
    pub fn key(&self) -> &[u8] {
        let s = self.desc.hdr_len();
        &self.bytes[s..s + usize::from(self.desc.key_len)]
    }

    pub fn val(&self) -> &[u8] {
        let s = self.desc.hdr_len() + usize::from(self.desc.key_len);
        &self.bytes[s..s + usize::from(self.desc.val_len)]
    }
}

/// A staged change (holds user bytes: no `Debug`, LC-32).
pub enum Pending {
    Put(EncodedRecord),
    Remove { db: u8, h32: u32, key: Box<[u8]>, tickets: SmallVec<[TicketId; 1]> },
    /// Drops the copy of the key with exactly `seq` (a committed txn delete marker, or an
    /// expired newest copy kept while older copies existed, §16.2) once it is the key's only
    /// copy (D1, E9). A newer version of the key makes it a no-op.
    Retire { db: u8, h32: u32, key: Box<[u8]>, seq: Seq, tickets: SmallVec<[TicketId; 1]> },
}

impl Pending {
    pub fn db(&self) -> u8 {
        match self {
            Pending::Put(r) => r.desc.db,
            Pending::Remove { db, .. } | Pending::Retire { db, .. } => *db,
        }
    }

    pub fn h32(&self) -> u32 {
        match self {
            Pending::Put(r) => r.desc.h32,
            Pending::Remove { h32, .. } | Pending::Retire { h32, .. } => *h32,
        }
    }

    pub fn key(&self) -> &[u8] {
        match self {
            Pending::Put(r) => r.key(),
            Pending::Remove { key, .. } | Pending::Retire { key, .. } => key,
        }
    }

    /// True for a client-visible change (a put or a remove), false for a retirement.
    pub fn is_change(&self) -> bool {
        !matches!(self, Pending::Retire { .. })
    }

    pub(crate) fn tickets_mut(&mut self) -> &mut SmallVec<[TicketId; 1]> {
        match self {
            Pending::Put(r) => &mut r.tickets,
            Pending::Remove { tickets, .. } | Pending::Retire { tickets, .. } => tickets,
        }
    }

    /// The tickets waiting for this effect.
    pub(crate) fn tickets(&self) -> &[TicketId] {
        match self {
            Pending::Put(r) => &r.tickets,
            Pending::Remove { tickets, .. } | Pending::Retire { tickets, .. } => tickets,
        }
    }

    /// True when this effect concerns `(db, key)` (h32 first, then the bytes).
    pub fn is_key(&self, db: u8, h32: u32, key: &[u8]) -> bool {
        self.db() == db && self.h32() == h32 && self.key() == key
    }

    /// Encoded bytes this effect adds to the page (staged-write memory, §6.7).
    pub fn bytes(&self) -> usize {
        match self {
            Pending::Put(r) => r.bytes.len(),
            Pending::Remove { key, .. } | Pending::Retire { key, .. } => key.len() + 16,
        }
    }
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
    /// Effects this image materializes (resolved when it is durable).
    pub(crate) applied: SmallVec<[Pending; 2]>,
    /// Tracked copies at this page that N zeroes: `(db, key)`.
    pub(crate) cleared: SmallVec<[(u8, Box<[u8]>); 1]>,
    /// Partition-local write id (flush tracking).
    pub(crate) wid: u64,
    /// First error of a sub-I/O.
    pub(crate) failed: Option<i32>,
}

impl PageState {
    /// State of a page loaded from media with records `recs`.
    pub fn loaded(page: PageId, m_frame: FrameIdx, recs: SmallVec<[RecDesc; 16]>) -> Self {
        PageState {
            page,
            m_frame,
            recs,
            pending: SmallVec::new(),
            inflight: None,
            fresh: false,
            r#gen: 0,
            dirty: false,
            scrub: false,
            corrupt: false,
            work_pin: false,
        }
    }

    /// State of a fresh page (never parsed; its M frame holds a zero page with header).
    pub fn fresh(page: PageId, m_frame: FrameIdx) -> Self {
        let mut s = Self::loaded(page, m_frame, SmallVec::new());
        s.fresh = true;
        s
    }

    /// Records of the current image (N while in flight, else M) and that image's frame.
    pub fn image(&self) -> (&[RecDesc], FrameIdx) {
        match &self.inflight {
            Some(i) => (&i.recs, i.n_frame),
            None => (&self.recs, self.m_frame),
        }
    }

    /// True when nothing is staged, in flight or wanted: the page may be evicted.
    pub fn is_clean(&self) -> bool {
        self.pending.is_empty() && self.inflight.is_none() && !self.scrub && !self.dirty
    }

    /// Bytes the page will hold in the worst case (current image records that are not
    /// droppable or superseded, plus staged puts): the allocator's free-space view.
    pub fn used_bytes(&self) -> u16 {
        let (recs, _) = self.image();
        let img: u32 = recs
            .iter()
            .filter(|r| matches!(r.status, RecStatus::Live | RecStatus::Protected))
            .map(|r| u32::from(r.len))
            .sum();
        let staged: u32 =
            self.pending.iter().map(|p| if let Pending::Put(r) = p { u32::from(r.desc.len) } else { 0 }).sum();
        (img + staged).min(u32::from(PAGE_ROOM)) as u16
    }

    /// The longest run of bytes no live or protected record of the current image occupies,
    /// less the bytes of staged puts: what one more record can count on finding as one gap
    /// (placement is first-fit, R2, so free bytes split into small gaps do not help).
    pub fn largest_gap(&self) -> u16 {
        let (recs, _) = self.image();
        let mut spans: SmallVec<[(u16, u16); 24]> =
            recs.iter().filter(|r| matches!(r.status, RecStatus::Live | RecStatus::Protected)).map(|r| (r.off, r.end())).collect();
        spans.sort_unstable();
        let mut at = SMALL_HDR as u16;
        let mut best = 0u16;
        for (a, b) in spans {
            if a > at {
                best = best.max(a - at);
            }
            at = at.max(b);
        }
        best = best.max(PAGE as u16 - at);
        let staged: u16 = self.pending.iter().map(|p| if let Pending::Put(r) = p { r.desc.len } else { 0 }).sum();
        best.saturating_sub(staged)
    }

    /// Largest record of the page (slack rule, §11.3).
    pub fn largest(&self) -> u16 {
        let (recs, _) = self.image();
        let a = recs.iter().map(|r| r.len).fold(0, u16::max);
        let b = self.pending.iter().map(|p| if let Pending::Put(r) = p { r.desc.len } else { 0 }).fold(0, u16::max);
        a.max(b)
    }

    /// Staged-write memory of this page.
    pub fn staged_bytes(&self) -> usize {
        self.pending.iter().map(Pending::bytes).sum()
    }
}

const PAGE_ROOM: u16 = (PAGE - SMALL_HDR) as u16;

/// Write granularity of page images (§13.6).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Granularity {
    Page,
    Lba,
}

/// What the builder needs to know about the rest of the partition.
pub(crate) trait BuildEnv {
    fn mode(&self) -> PageUpdateMode;
    fn crc(&self) -> RecordCrc;
    fn partition(&self) -> u16;
    fn lba(&self) -> u32;
    fn granularity(&self) -> Granularity;
    /// Smallest atomic write (`max(lba, unit_min)`).
    fn atomic_min(&self) -> u32;
    /// Copies of `(db, key)` tracked outside this page (stale registry).
    fn others(&self, db: u8, key: &[u8]) -> usize;
    /// State of a tracked (moved-away or older) copy of `(db, key)` located in this page.
    fn tracked_here(&self, db: u8, key: &[u8]) -> Option<CopyState>;
    /// The record is dead: below a durable flush cutoff, or of an aborted txn (§16.2).
    fn dead(&self, r: &RecDesc) -> bool;
    /// The record belongs to a txn whose commit is not durable yet (§13.3 c).
    fn txn_open(&self, txn: u64) -> bool;
}

/// Result of [`build`].
pub(crate) struct Built {
    /// `(offset, len)` spans of N to write (`lba` aligned, at most two); empty = no write.
    pub(crate) spans: SmallVec<[(u16, u16); 2]>,
    /// Issue the single span with `RWF_ATOMIC` (atomic mode).
    pub(crate) atomic: bool,
    /// N's records.
    pub(crate) recs: SmallVec<[RecDesc; 16]>,
    /// Effects N materializes.
    pub(crate) applied: SmallVec<[Pending; 2]>,
    /// Puts that do not fit this page (moved by the partition, §14.3).
    pub(crate) relocate: SmallVec<[EncodedRecord; 1]>,
    /// Keys whose tracked copies elsewhere must be zeroed by foreground writes now (D1, E14).
    pub(crate) foreground: SmallVec<[(u8, Box<[u8]>); 1]>,
    /// Tracked copies in this page that N zeroes.
    pub(crate) cleared: SmallVec<[(u8, Box<[u8]>); 1]>,
    /// Records zeroed and placed (counters).
    pub(crate) zeroed: u16,
    pub(crate) placed: u16,
    /// Bytes of user records placed (write amplification counters).
    pub(crate) user_bytes: u32,
}

impl Built {
    #[cfg(test)]
    pub(crate) fn writes(&self) -> bool {
        !self.spans.is_empty()
    }
}

/// Byte ranges changed by a build, merged on demand.
#[derive(Default)]
struct Changes {
    full: bool,
    ranges: SmallVec<[(u16, u16); 8]>,
}

impl Changes {
    fn add(&mut self, off: u16, end: u16) {
        if end > off {
            self.ranges.push((off, end));
        }
    }

    fn merged(&self) -> SmallVec<[(u16, u16); 8]> {
        let mut r = self.ranges.clone();
        r.sort_unstable();
        let mut out: SmallVec<[(u16, u16); 8]> = SmallVec::new();
        for (a, b) in r {
            match out.last_mut() {
                Some(last) if a <= last.1 => last.1 = last.1.max(b),
                _ => out.push((a, b)),
            }
        }
        out
    }
}

/// The I/O shape of an image (§13.6): `(spans, atomic)`.
fn io_shape(ch: &Changes, env: &dyn BuildEnv) -> (SmallVec<[(u16, u16); 2]>, bool) {
    let mut out = SmallVec::new();
    let merged = ch.merged();
    if !ch.full && merged.is_empty() {
        return (out, false);
    }
    let atomic = env.mode() == PageUpdateMode::Atomic;
    if ch.full {
        out.push((0, PAGE as u16));
        return (out, atomic);
    }
    let lo = u32::from(merged[0].0);
    let hi = u32::from(merged[merged.len() - 1].1);
    if atomic {
        // the smallest naturally aligned power-of-two span covering every change
        let mut size = env.atomic_min().clamp(512, PAGE as u32).next_power_of_two();
        while size < PAGE as u32 && (lo / size) != ((hi - 1) / size) {
            size *= 2;
        }
        let start = lo / size * size;
        out.push((start as u16, size as u16));
        return (out, true);
    }
    if env.granularity() == Granularity::Page || hi - lo > (PAGE / 2) as u32 {
        out.push((0, PAGE as u16));
        return (out, false);
    }
    let lba = env.lba().clamp(512, PAGE as u32);
    let mut spans: SmallVec<[(u32, u32); 4]> = SmallVec::new();
    for &(a, b) in &merged {
        let s = u32::from(a) / lba * lba;
        let e = u32::from(b).div_ceil(lba) * lba;
        match spans.last_mut() {
            Some(last) if s <= last.1 => last.1 = last.1.max(e),
            _ => spans.push((s, e)),
        }
    }
    if spans.len() > 2 {
        out.push((0, PAGE as u16));
        return (out, false);
    }
    for (s, e) in spans {
        out.push((s as u16, (e - s) as u16));
    }
    (out, false)
}

/// Lowest 16-aligned offset >= 0x20 where `len` bytes overlap nothing in `occupied`
/// (`(off, end)` pairs, any order).
fn find_gap(occupied: &mut SmallVec<[(u16, u16); 24]>, len: u16) -> Option<u16> {
    occupied.sort_unstable();
    let mut at = SMALL_HDR as u16;
    for &(a, b) in occupied.iter() {
        if b <= at {
            continue;
        }
        if a >= at && a - at >= len {
            return Some(at);
        }
        at = at.max(b);
    }
    (PAGE as u16 - at >= len).then_some(at)
}

/// Builds N for `st` (§13.4). `m` is M's bytes (ignored for a fresh page), `n` receives N.
/// `st.pending` is consumed: effects N cannot materialize yet stay in `st.pending`.
pub(crate) fn build(st: &mut PageState, m: &[u8; PAGE], n: &mut [u8; PAGE], env: &dyn BuildEnv) -> Built {
    let mode = env.mode();
    let page = st.page;
    let crc = env.crc();
    let mut ch = Changes::default();
    if st.fresh {
        n.fill(0);
        write_small_header(n, page, env.partition());
        ch.full = true;
    } else {
        n.copy_from_slice(m);
    }
    let mut out = Built {
        spans: SmallVec::new(),
        atomic: false,
        recs: SmallVec::new(),
        applied: SmallVec::new(),
        relocate: SmallVec::new(),
        foreground: SmallVec::new(),
        cleared: SmallVec::new(),
        zeroed: 0,
        placed: 0,
        user_bytes: 0,
    };

    // 1. classify M's records: `keep` stays in N, `drop` is zeroed, droppables become free space
    //    (a scrub build zeroes them too, §13.4 step 3 iii)
    let mut keep: SmallVec<[RecDesc; 16]> = SmallVec::new();
    let mut drop: SmallVec<[RecDesc; 8]> = SmallVec::new();
    if !st.fresh {
        for (i, r) in st.recs.iter().enumerate() {
            let mut r = *r;
            let key = r.key(m);
            let tracked = if env.dead(&r) { None } else { env.tracked_here(r.db, key) };
            if env.dead(&r) {
                r.status = RecStatus::Droppable;
            } else if let Some(t) = tracked {
                r.status = match t {
                    CopyState::Stale => RecStatus::Superseded,
                    // the tracked (current) copy is its newest one here; older ones go (R3)
                    CopyState::Source if newest_here(&st.recs, i, m) => RecStatus::Protected,
                    CopyState::Source => RecStatus::Superseded,
                };
            }
            if r.status == RecStatus::Superseded && newer_open_txn(&st.recs, i, m, env) {
                // a txn version that supersedes it is not committed yet (§13.3 c, E6)
                r.status = RecStatus::Protected;
            }
            match r.status {
                RecStatus::Superseded => {
                    if tracked == Some(CopyState::Stale) {
                        out.cleared.push((r.db, key.into()));
                    }
                    drop.push(r);
                }
                RecStatus::Droppable if st.scrub => drop.push(r),
                RecStatus::Droppable => {}
                RecStatus::Live | RecStatus::Protected => keep.push(r),
            }
        }
    }

    // 2. staged effects (one per key: staging coalesces)
    let pending = core::mem::take(&mut st.pending);
    // puts to place by R2, with the offset of the old copy that the same image zeroes once the
    // new version is placed in it (atomic mode, grown records)
    let mut puts: SmallVec<[(EncodedRecord, Option<u16>); 4]> = SmallVec::new();
    for eff in pending {
        let (db, h32) = (eff.db(), eff.h32());
        let same = |r: &RecDesc| r.db == db && r.h32 == h32;
        // indexes in `keep` of this key's copies, newest last
        let mut idx: SmallVec<[usize; 2]> = SmallVec::new();
        for (i, r) in keep.iter().enumerate() {
            if same(r) && r.key(m) == eff.key() {
                idx.push(i);
            }
        }
        idx.sort_by_key(|&i| keep[i].seq);
        let newest = idx.last().copied();
        let others = env.others(db, eff.key());
        // older in-page copies of a key with a staged effect are zeroed now (superseded)
        let older: SmallVec<[usize; 2]> = idx.iter().copied().filter(|&i| Some(i) != newest).collect();
        match eff {
            Pending::Remove { db, h32, key, tickets } => {
                let txn_protected = newest.is_some_and(|i| keep[i].txn != 0 && env.txn_open(keep[i].txn));
                // a superseded copy of the key zeroed by this very image is an older copy too
                let older_dropping = drop.iter().any(|r| same(r) && r.key(m) == &*key);
                let in_page_older = !older.is_empty() || older_dropping;
                if others > 0 || txn_protected || (mode == PageUpdateMode::TwoImage && in_page_older && newest.is_some()) {
                    // D1: the newest copy is dropped only after every other copy is gone
                    mark_drop(&mut keep, &older, &mut drop);
                    if let Some(i) = newest.map(|i| i - older.iter().filter(|&&o| o < i).count()) {
                        keep[i].status = RecStatus::Protected;
                    }
                    if others > 0 {
                        out.foreground.push((db, key.clone()));
                    }
                    st.pending.push(Pending::Remove { db, h32, key, tickets });
                } else {
                    let mut all = older.clone();
                    all.extend(newest);
                    mark_drop(&mut keep, &all, &mut drop);
                    if env.tracked_here(db, &key).is_some() {
                        // the deleted copy was tracked as a copy of the key: gone when N is
                        out.cleared.push((db, key.clone()));
                    }
                    out.applied.push(Pending::Remove { db, h32, key, tickets });
                }
            }
            Pending::Retire { db, h32, key, seq, tickets } => {
                let target = idx.iter().copied().find(|&i| keep[i].seq == seq);
                let newer = idx.iter().any(|&i| keep[i].seq > seq);
                let Some(_) = target.filter(|_| !newer) else {
                    // already gone, or a newer version makes it an ordinary older copy
                    out.applied.push(Pending::Retire { db, h32, key, seq, tickets });
                    continue;
                };
                let older: SmallVec<[usize; 2]> = idx.iter().copied().filter(|&i| keep[i].seq < seq).collect();
                let older_dropping = drop.iter().any(|r| same(r) && r.seq < seq && r.key(m) == &*key);
                let wait_older = mode == PageUpdateMode::TwoImage && (!older.is_empty() || older_dropping);
                if others > 0 || wait_older {
                    // D1: the older copies go first; those of this page are zeroed now
                    mark_drop(&mut keep, &older, &mut drop);
                    if others > 0 {
                        out.foreground.push((db, key.clone()));
                    }
                    st.pending.push(Pending::Retire { db, h32, key, seq, tickets });
                } else {
                    let all: SmallVec<[usize; 2]> = idx.iter().copied().filter(|&i| keep[i].seq <= seq).collect();
                    mark_drop(&mut keep, &all, &mut drop);
                    out.applied.push(Pending::Retire { db, h32, key, seq, tickets });
                }
            }
            Pending::Put(rec) => {
                let is_txn = rec.desc.txn != 0;
                let kept_newest = mode == PageUpdateMode::TwoImage && newest.is_some();
                let newest_txn_open = newest.is_some_and(|i| keep[i].txn != 0 && env.txn_open(keep[i].txn));
                if others + usize::from(kept_newest) + 1 > 2 || newest_txn_open {
                    // E14: a further copy waits until the older tracked copy is durably zeroed
                    if others > 0 {
                        out.foreground.push((db, rec.key().into()));
                    }
                    st.pending.push(Pending::Put(rec));
                    continue;
                }
                mark_drop(&mut keep, &older, &mut drop);
                let newest = newest.map(|i| i - older.iter().filter(|&&o| o < i).count());
                match (mode, newest) {
                    (PageUpdateMode::TwoImage, Some(i)) => {
                        // R1: the previous version stays byte-identical until N is durable
                        keep[i].status = RecStatus::Protected;
                        puts.push((rec, None));
                    }
                    (PageUpdateMode::Atomic, Some(i)) if !is_txn && rec.desc.len <= keep[i].len => {
                        // R1a: rewritten at its existing offset
                        let old = keep[i];
                        let off = old.off;
                        let mut d = rec.desc;
                        d.off = off;
                        d.status = RecStatus::Live;
                        write_rec(n, &rec, off, page, crc);
                        n[usize::from(off + d.len)..usize::from(old.end())].fill(0);
                        ch.add(off, old.end());
                        keep[i] = d;
                        out.placed += 1;
                        out.user_bytes += u32::from(d.len);
                        out.applied.push(Pending::Put(rec));
                    }
                    (PageUpdateMode::Atomic, Some(i)) => {
                        // grown (or txn) update: placed by R2; the old copy is zeroed in the
                        // same image when the new version lands in it, else it stays protected
                        // while the record moves (E2); a txn version never replaces it (13.3 c)
                        keep[i].status = RecStatus::Protected;
                        let old = (!is_txn).then_some(keep[i].off);
                        puts.push((rec, old));
                    }
                    (_, None) => puts.push((rec, None)),
                }
            }
        }
    }

    // 3. zero what is dropped (R3)
    for r in &drop {
        n[usize::from(r.off)..usize::from(r.end())].fill(0);
        ch.add(r.off, r.end());
        out.zeroed += 1;
    }

    // 4. R2: first-fit-decreasing placement over the bytes no kept record occupies
    puts.sort_by(|(a, _), (b, _)| b.desc.len.cmp(&a.desc.len).then(a.desc.h32.cmp(&b.desc.h32)).then_with(|| a.key().cmp(b.key())));
    let mut occupied: SmallVec<[(u16, u16); 24]> = keep.iter().map(|r| (r.off, r.end())).collect();
    let mut replaced: SmallVec<[u16; 2]> = SmallVec::new();
    for (rec, old) in puts {
        match find_gap(&mut occupied, rec.desc.len) {
            Some(off) => {
                write_rec(n, &rec, off, page, crc);
                let mut d = rec.desc;
                d.off = off;
                d.status = RecStatus::Live;
                occupied.push((off, d.end()));
                ch.add(off, d.end());
                keep.push(d);
                out.placed += 1;
                out.user_bytes += u32::from(d.len);
                out.applied.push(Pending::Put(rec));
                replaced.extend(old);
            }
            None => out.relocate.push(rec),
        }
    }
    for off in replaced {
        // atomic mode: the grown record's old copy goes in the image that holds the new one
        if let Some(i) = keep.iter().position(|r| r.off == off && r.status == RecStatus::Protected) {
            let r = keep.remove(i);
            n[usize::from(r.off)..usize::from(r.end())].fill(0);
            ch.add(r.off, r.end());
            out.zeroed += 1;
        }
    }

    // 5. I/O shape, then R4 inside the written spans
    let (spans, atomic) = io_shape(&ch, env);
    if spans.is_empty() && !out.applied.iter().any(|p| matches!(p, Pending::Put(_))) && !st.fresh {
        // nothing changes on media (a remove of a key with no copy here)
        out.recs = keep;
        return out;
    }
    keep.sort_by_key(|r| r.off);
    for &(s, len) in &spans {
        let (s, e) = (s.max(SMALL_HDR as u16), s + len);
        let mut at = s;
        for r in keep.iter() {
            if r.end() <= at || r.off >= e {
                continue;
            }
            if r.off > at {
                n[usize::from(at)..usize::from(r.off)].fill(0);
            }
            at = at.max(r.end());
        }
        if e > at {
            n[usize::from(at)..usize::from(e)].fill(0);
        }
    }
    out.spans = spans;
    out.atomic = atomic;
    out.recs = keep;
    out
}

/// True when `recs[i]` has the highest seq among its key's records of the image.
fn newest_here(recs: &[RecDesc], i: usize, img: &[u8; PAGE]) -> bool {
    let a = recs[i];
    !recs.iter().enumerate().any(|(j, b)| j != i && b.db == a.db && b.h32 == a.h32 && b.seq > a.seq && b.key(img) == a.key(img))
}

/// True when a newer copy of `recs[i]`'s key in the same image belongs to a txn whose commit
/// is not durable yet.
fn newer_open_txn(recs: &[RecDesc], i: usize, img: &[u8; PAGE], env: &dyn BuildEnv) -> bool {
    let a = recs[i];
    recs.iter().enumerate().any(|(j, b)| {
        j != i && b.txn != 0 && b.db == a.db && b.h32 == a.h32 && b.seq > a.seq && env.txn_open(b.txn) && b.key(img) == a.key(img)
    })
}

/// Moves `keep[idx]` to `drop` (indexes refer to `keep` before removal).
fn mark_drop(keep: &mut SmallVec<[RecDesc; 16]>, idx: &[usize], drop: &mut SmallVec<[RecDesc; 8]>) {
    let mut v: SmallVec<[usize; 2]> = SmallVec::from_slice(idx);
    v.sort_unstable();
    for &i in v.iter().rev() {
        drop.push(keep.remove(i));
    }
}

/// Copies `rec` to `off` of `n` and seals its checksum for `(page, off)`.
fn write_rec(n: &mut [u8; PAGE], rec: &EncodedRecord, off: u16, page: PageId, crc: RecordCrc) {
    let o = usize::from(off);
    let len = rec.bytes.len();
    n[o..o + len].copy_from_slice(&rec.bytes);
    let sealed = seal_small(&mut n[o..o + len], page, off, crc);
    debug_assert!(sealed, "staged record bytes have a valid header");
}

/// After N became durable: versions superseded by a newer copy of their key in the same image
/// become Superseded (zeroed by the page's next build, R3).
pub(crate) fn settle(recs: &mut [RecDesc], img: &[u8; PAGE]) {
    for i in 0..recs.len() {
        if !matches!(recs[i].status, RecStatus::Live | RecStatus::Protected) {
            continue;
        }
        let (a, ka) = (recs[i], recs[i].key(img));
        let newer = recs.iter().enumerate().any(|(j, b)| {
            j != i && b.db == a.db && b.h32 == a.h32 && b.seq > a.seq && b.key(img) == ka
        });
        if newer {
            recs[i].status = RecStatus::Superseded;
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::page::{PageParse, parse_small_page};
    use crate::record::{REC_FLAG_HAS_EXPIRE, REC_KIND_STRING, RecHeader, encode_small, small_len};
    use hashbrown::HashMap;
    use proptest::prelude::*;

    pub(crate) const SALT: u32 = 0x5EED_0001;
    const PART: u16 = 5;

    pub(crate) struct Env {
        pub(crate) mode: PageUpdateMode,
        pub(crate) lba: u32,
        pub(crate) others: HashMap<Vec<u8>, usize>,
    }

    impl BuildEnv for Env {
        fn mode(&self) -> PageUpdateMode {
            self.mode
        }
        fn crc(&self) -> RecordCrc {
            RecordCrc::new(SALT)
        }
        fn partition(&self) -> u16 {
            PART
        }
        fn lba(&self) -> u32 {
            self.lba
        }
        fn granularity(&self) -> Granularity {
            Granularity::Lba
        }
        fn atomic_min(&self) -> u32 {
            self.lba
        }
        fn others(&self, _db: u8, key: &[u8]) -> usize {
            self.others.get(key).copied().unwrap_or(0)
        }
        fn tracked_here(&self, _db: u8, _key: &[u8]) -> Option<CopyState> {
            None
        }
        fn dead(&self, _r: &RecDesc) -> bool {
            false
        }
        fn txn_open(&self, _t: u64) -> bool {
            false
        }
    }

    fn h32(key: &[u8]) -> u32 {
        kv_common::KeyHash::of(1, key).h32()
    }

    pub(crate) fn encoded(key: &[u8], val: &[u8], seq: u64) -> EncodedRecord {
        let len = small_len(key.len(), val.len(), false);
        let h = RecHeader {
            kind: REC_KIND_STRING,
            db: 0,
            flags: 0,
            seq: Seq(seq),
            expire_at_ms: 0,
            key_len: key.len() as u16,
            val_len: val.len() as u16,
            h32: h32(key),
            txn: None,
        };
        let mut b = vec![0u8; len].into_boxed_slice();
        encode_small(&h, key, val, PageId(0), 0x20, RecordCrc::new(SALT), &mut b);
        EncodedRecord {
            bytes: b,
            desc: RecDesc {
                off: 0,
                len: len as u16,
                h32: h.h32,
                db: 0,
                kind: REC_KIND_STRING,
                flags: 0,
                seq: Seq(seq),
                txn: 0,
                status: RecStatus::Live,
                expire_at_ms: 0,
                key_len: key.len() as u16,
                val_len: val.len() as u16,
            },
            tickets: SmallVec::new(),
        }
    }

    fn page() -> PageId {
        PageId((1 << 32) | 77)
    }

    /// Parses `img` and resolves each key to its max-seq value.
    fn resolve(img: &[u8; PAGE]) -> HashMap<Vec<u8>, (u64, Vec<u8>)> {
        let mut v = Vec::new();
        let mut out: HashMap<Vec<u8>, (u64, Vec<u8>)> = HashMap::new();
        if parse_small_page(img, page(), PART, RecordCrc::new(SALT), &mut v) != PageParse::Ok {
            return out;
        }
        for r in &v {
            let e = out.entry(r.key.to_vec()).or_insert((0, Vec::new()));
            if r.hdr.seq.0 > e.0 {
                *e = (r.hdr.seq.0, r.val.to_vec());
            }
        }
        out
    }

    fn recs_of(img: &[u8; PAGE]) -> SmallVec<[RecDesc; 16]> {
        let mut v = Vec::new();
        parse_small_page(img, page(), PART, RecordCrc::new(SALT), &mut v);
        let mut recs: SmallVec<[RecDesc; 16]> = v
            .iter()
            .map(|s| RecDesc {
                off: s.off,
                len: s.len,
                h32: s.hdr.h32,
                db: s.hdr.db,
                kind: s.hdr.kind,
                flags: s.hdr.flags,
                seq: s.hdr.seq,
                txn: s.hdr.txn.unwrap_or(0),
                status: RecStatus::Live,
                expire_at_ms: s.hdr.expire_at_ms,
                key_len: s.hdr.key_len,
                val_len: s.hdr.val_len,
            })
            .collect();
        settle(&mut recs, img);
        recs
    }

    /// Runs one build of `st` (M in `m`) and returns N.
    fn run(st: &mut PageState, m: &[u8; PAGE], env: &Env) -> ([u8; PAGE], Built) {
        let mut n = [0u8; PAGE];
        let b = build(st, m, &mut n, env);
        (n, b)
    }

    fn state(m: &[u8; PAGE], fresh: bool) -> PageState {
        let mut s = if fresh { PageState::fresh(page(), FrameIdx(0)) } else { PageState::loaded(page(), FrameIdx(0), recs_of(m)) };
        s.fresh = fresh;
        s
    }

    #[test]
    fn fresh_page_first_build_is_one_full_write() {
        let env = Env { mode: PageUpdateMode::TwoImage, lba: 512, others: HashMap::new() };
        let m = [0u8; PAGE];
        let mut st = state(&m, true);
        st.pending.push(Pending::Put(encoded(b"a", b"1", 1)));
        st.pending.push(Pending::Put(encoded(b"bb", &[7; 300], 2)));
        let (n, b) = run(&mut st, &m, &env);
        assert_eq!(b.spans.as_slice(), &[(0, 4096)]);
        assert_eq!(b.applied.len(), 2);
        let r = resolve(&n);
        assert_eq!(r[&b"a".to_vec()].1, b"1");
        // FFD: the larger record is placed first, at 0x20
        let big = b.recs.iter().find(|r| r.len > 300).unwrap();
        assert_eq!(big.off, 0x20);
    }

    #[test]
    fn two_image_update_keeps_old_copy_until_durable_then_zeroes_it() {
        let env = Env { mode: PageUpdateMode::TwoImage, lba: 512, others: HashMap::new() };
        let mut st = state(&[0; PAGE], true);
        st.pending.push(Pending::Put(encoded(b"k", b"v1", 1)));
        let (m1, b) = run(&mut st, &[0; PAGE], &env);
        let mut st = PageState::loaded(page(), FrameIdx(0), b.recs);
        st.pending.push(Pending::Put(encoded(b"k", b"v2", 2)));
        let (m2, b2) = run(&mut st, &m1, &env);
        assert!(b2.spans.len() <= 2 && b2.spans.iter().all(|s| s.1 <= 1024), "sector-granular update");
        assert_eq!(b2.recs.len(), 2, "both versions in N (R1)");
        assert_eq!(resolve(&m2)[&b"k".to_vec()].1, b"v2");
        // durable: old copy superseded; the next build zeroes it (R3) and keeps one copy
        let mut recs = b2.recs.clone();
        settle(&mut recs, &m2);
        assert_eq!(recs.iter().filter(|r| r.status == RecStatus::Superseded).count(), 1);
        let mut st = PageState::loaded(page(), FrameIdx(0), recs);
        st.pending.push(Pending::Put(encoded(b"other", b"x", 3)));
        let (m3, b3) = run(&mut st, &m2, &env);
        assert_eq!(b3.zeroed, 1);
        let mut v = Vec::new();
        parse_small_page(&m3, page(), PART, RecordCrc::new(SALT), &mut v);
        assert_eq!(v.iter().filter(|r| r.key == b"k").count(), 1);
    }

    #[test]
    fn atomic_update_rewrites_in_place() {
        let env = Env { mode: PageUpdateMode::Atomic, lba: 512, others: HashMap::new() };
        let mut st = state(&[0; PAGE], true);
        st.pending.push(Pending::Put(encoded(b"k", &[1; 200], 1)));
        st.pending.push(Pending::Put(encoded(b"z", &[2; 100], 1)));
        let (m1, b) = run(&mut st, &[0; PAGE], &env);
        let off = b.recs.iter().find(|r| r.key(&m1) == b"k").unwrap().off;
        let mut st = PageState::loaded(page(), FrameIdx(0), b.recs);
        st.pending.push(Pending::Put(encoded(b"k", &[3; 150], 2)));
        let (m2, b2) = run(&mut st, &m1, &env);
        assert!(b2.atomic && b2.spans.len() == 1);
        assert_eq!(b2.recs.iter().find(|r| r.key(&m2) == b"k").unwrap().off, off, "same offset");
        assert_eq!(b2.recs.len(), 2);
        // grown: placed elsewhere, old copy zeroed in the same image
        let mut st = PageState::loaded(page(), FrameIdx(0), b2.recs);
        st.pending.push(Pending::Put(encoded(b"k", &[4; 600], 3)));
        let (m3, b3) = run(&mut st, &m2, &env);
        assert_eq!(b3.recs.len(), 2);
        assert_eq!(resolve(&m3)[&b"k".to_vec()].1, vec![4; 600]);
    }

    #[test]
    fn remove_waits_for_older_copies() {
        let mut env = Env { mode: PageUpdateMode::TwoImage, lba: 512, others: HashMap::new() };
        let mut st = state(&[0; PAGE], true);
        st.pending.push(Pending::Put(encoded(b"k", b"v", 1)));
        let (m1, b) = run(&mut st, &[0; PAGE], &env);
        env.others.insert(b"k".to_vec(), 1);
        let mut st = PageState::loaded(page(), FrameIdx(0), b.recs.clone());
        st.pending.push(Pending::Remove { db: 0, h32: h32(b"k"), key: (*b"k").into(), tickets: SmallVec::new() });
        let (_n, b2) = run(&mut st, &m1, &env);
        assert!(b2.applied.is_empty() && st.pending.len() == 1, "deferred");
        assert_eq!(b2.foreground.len(), 1);
        env.others.clear();
        let (n3, b3) = run(&mut st, &m1, &env);
        assert_eq!(b3.applied.len(), 1);
        assert!(resolve(&n3).is_empty());
    }

    #[test]
    fn third_copy_is_deferred() {
        let mut env = Env { mode: PageUpdateMode::TwoImage, lba: 512, others: HashMap::new() };
        let mut st = state(&[0; PAGE], true);
        st.pending.push(Pending::Put(encoded(b"k", b"v", 1)));
        let (m1, b) = run(&mut st, &[0; PAGE], &env);
        env.others.insert(b"k".to_vec(), 1);
        let mut st = PageState::loaded(page(), FrameIdx(0), b.recs);
        st.pending.push(Pending::Put(encoded(b"k", b"v2", 2)));
        let (_n, b2) = run(&mut st, &m1, &env);
        assert!(!b2.writes() && st.pending.len() == 1 && b2.foreground.len() == 1);
    }

    #[test]
    fn records_that_do_not_fit_are_relocated() {
        let env = Env { mode: PageUpdateMode::TwoImage, lba: 512, others: HashMap::new() };
        let mut st = state(&[0; PAGE], true);
        st.pending.push(Pending::Put(encoded(b"a", &[1; 2000], 1)));
        st.pending.push(Pending::Put(encoded(b"b", &[1; 2000], 1)));
        st.pending.push(Pending::Put(encoded(b"c", &[1; 1000], 1)));
        let (_n, b) = run(&mut st, &[0; PAGE], &env);
        assert_eq!(b.placed, 2);
        assert_eq!(b.relocate.len(), 1);
    }

    #[test]
    fn io_shape_rules() {
        let env = Env { mode: PageUpdateMode::TwoImage, lba: 512, others: HashMap::new() };
        let mut ch = Changes::default();
        ch.add(0x20, 0x60);
        ch.add(0x400, 0x410);
        assert_eq!(io_shape(&ch, &env).0.as_slice(), &[(0, 512), (1024, 512)]);
        ch.add(0x700, 0x710);
        assert_eq!(io_shape(&ch, &env).0.as_slice(), &[(0, 512), (1024, 1024)], "adjacent sectors merge");
        let mut ch = Changes::default();
        ch.add(0x20, 0x30);
        ch.add(0xF00, 0xF10);
        assert_eq!(io_shape(&ch, &env).0.as_slice(), &[(0, 4096)], "changed span over 2048");
        let env = Env { mode: PageUpdateMode::Atomic, lba: 512, others: HashMap::new() };
        let mut ch = Changes::default();
        ch.add(0x1F0, 0x210);
        let (s, a) = io_shape(&ch, &env);
        assert!(a);
        assert_eq!(s.as_slice(), &[(0, 1024)], "aligned power of two covering the change");
        let env = Env { mode: PageUpdateMode::TwoImage, lba: 4096, others: HashMap::new() };
        let mut ch = Changes::default();
        ch.add(0x20, 0x30);
        assert_eq!(io_shape(&ch, &env).0.as_slice(), &[(0, 4096)]);
    }

    // -----------------------------------------------------------------------------------------
    // Page theorem (§17.3, §25.2): for every build, every byte mix of M and N resolves every key
    // to its state before N or after N.

    #[derive(Clone, Debug)]
    enum Step {
        Put(u8, u16),
        Del(u8),
    }

    fn step() -> impl Strategy<Value = Step> {
        prop_oneof![
            3 => (0u8..6, 0u16..700).prop_map(|(k, l)| Step::Put(k, l)),
            1 => (0u8..6).prop_map(Step::Del),
        ]
    }

    fn key(k: u8) -> Vec<u8> {
        vec![b'k', k]
    }

    /// Mixes M and N per sector according to `mask` bits (sector i from N if bit i set).
    fn mix(m: &[u8; PAGE], n: &[u8; PAGE], lba: usize, mask: u64) -> [u8; PAGE] {
        let mut x = *m;
        for s in 0..PAGE / lba {
            if mask >> s & 1 == 1 {
                x[s * lba..(s + 1) * lba].copy_from_slice(&n[s * lba..(s + 1) * lba]);
            }
        }
        x
    }

    fn check_mixes(m: &[u8; PAGE], n: &[u8; PAGE], spans: &[(u16, u16)], atomic: bool, bytes_seed: u64) -> Result<(), TestCaseError> {
        let before = resolve(m);
        let after = resolve(n);
        let ok = |x: &[u8; PAGE]| -> Result<(), TestCaseError> {
            let got = resolve(x);
            for k in 0..6u8 {
                let k = key(k);
                let g = got.get(&k).map(|v| &v.1);
                let b = before.get(&k).map(|v| &v.1);
                let a = after.get(&k).map(|v| &v.1);
                prop_assert!(g == b || g == a, "key resolves to neither old nor new state");
            }
            Ok(())
        };
        if atomic {
            ok(m)?;
            return ok(n);
        }
        // sectors that differ
        let lba = 512usize;
        let mut changed = Vec::new();
        for s in 0..PAGE / lba {
            if m[s * lba..(s + 1) * lba] != n[s * lba..(s + 1) * lba] {
                changed.push(s);
            }
        }
        prop_assert!(
            changed.iter().all(|&s| spans.iter().any(|&(o, l)| (s * lba) as u16 >= o && ((s + 1) * lba) as u16 <= o + l)),
            "changed sector outside the written spans"
        );
        if changed.len() <= 8 {
            for mask in 0..(1u64 << changed.len()) {
                let mut full = 0u64;
                for (i, &s) in changed.iter().enumerate() {
                    if mask >> i & 1 == 1 {
                        full |= 1 << s;
                    }
                }
                ok(&mix(m, n, lba, full))?;
            }
        }
        // random byte mixes
        let mut r = bytes_seed | 1;
        for _ in 0..16 {
            let mut x = *m;
            for i in 0..PAGE {
                r ^= r << 13;
                r ^= r >> 7;
                r ^= r << 17;
                if r & 1 == 1 {
                    x[i] = n[i];
                }
            }
            ok(&x)?;
        }
        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 96, failure_persistence: None, ..ProptestConfig::default() })]

        #[test]
        fn prop_page_theorem(steps in proptest::collection::vec(proptest::collection::vec(step(), 1..5), 1..8),
                             atomic in any::<bool>(), seed in any::<u64>()) {
            let mode = if atomic { PageUpdateMode::Atomic } else { PageUpdateMode::TwoImage };
            let env = Env { mode, lba: 512, others: HashMap::new() };
            let mut m = [0u8; PAGE];
            let mut st = state(&m, true);
            let mut seq = 0u64;
            for batch in steps {
                for s in batch {
                    seq += 1;
                    let eff = match s {
                        Step::Put(k, l) => Pending::Put(encoded(&key(k), &vec![seq as u8; usize::from(l)], seq)),
                        Step::Del(k) => Pending::Remove { db: 0, h32: h32(&key(k)), key: key(k).into(), tickets: SmallVec::new() },
                    };
                    // staging coalesces per key
                    if let Some(i) = st.pending.iter().position(|p| p.key() == eff.key()) {
                        st.pending.remove(i);
                    }
                    st.pending.push(eff);
                }
                let (n, b) = run(&mut st, &m, &env);
                prop_assert!(b.relocate.len() <= st.pending.len() + b.relocate.len());
                if b.writes() {
                    check_mixes(&m, &n, &b.spans, b.atomic, seed ^ seq)?;
                    m = n;
                    let mut recs = b.recs;
                    settle(&mut recs, &m);
                    let mut next = PageState::loaded(page(), FrameIdx(0), recs);
                    next.pending = core::mem::take(&mut st.pending);
                    st = next;
                    // at most two copies of a key in a durable image (I11)
                    let mut v = Vec::new();
                    parse_small_page(&m, page(), PART, RecordCrc::new(SALT), &mut v);
                    for k in 0..6u8 {
                        prop_assert!(v.iter().filter(|r| r.key == key(k).as_slice()).count() <= 2);
                    }
                }
            }
        }
    }

    #[test]
    fn expiry_flag_round_trips_through_recdesc() {
        let mut e = encoded(b"t", b"v", 9);
        e.desc.flags = REC_FLAG_HAS_EXPIRE;
        e.desc.expire_at_ms = 5;
        assert_eq!(e.desc.expire_at(), Some(5));
        assert_eq!(e.key(), b"t");
        assert_eq!(e.val(), b"v");
    }
}
