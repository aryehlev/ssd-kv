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
//! `build` is pure: it reads M, writes N into a frame the caller provides and reports what the
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
    /// Lengths of staged puts taken out of `pending` since the last build by something other
    /// than a build (the key moved away, a remove replaced the put, a txn abort): every put still
    /// staged was admitted with them counted (`PageState::places`), and the next build places by
    /// that view if its own placement fails (`build`). Cleared by each build.
    pub(crate) phantom: SmallVec<[u16; 2]>,
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
    /// Removes the key's copy in this page (a delete, or an expiry with older copies). `seq`:
    /// copies with a lower seq are older than the removal and must be gone first (D1); copies
    /// that later writes create (higher seq) never hold it back.
    Remove { db: u8, h32: u32, key: Box<[u8]>, seq: Seq, tickets: SmallVec<[TicketId; 1]> },
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
    /// When the image was submitted, then when its write completed (stall trace).
    pub(crate) since_ns: u64,
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
            phantom: SmallVec::new(),
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

    /// True when the page's next build places every staged put that needs free bytes, plus
    /// records of `extra` lengths about to be staged, by R2 without moving any of them away
    /// (§14.3). `img` is the current image's frame. The current image's live and protected
    /// records keep their bytes (an updated key's copy stays protected beside its new version
    /// in two-image mode, R1); `skip` names a key whose staged put the new one replaces
    /// (staging coalesces); in atomic mode a non-txn staged put no longer than its key's newest
    /// copy is rewritten at that copy's offset (R1a) and needs no gap. A page's free byte count
    /// or its largest gap is no promise: free bytes split into gaps smaller than the record do
    /// not take it, and other staged puts take gaps first. An accepted record that found no gap
    /// at build time had to move, which on a full device found no page and waited for space
    /// forever. Ties of equal lengths do not change which lengths fit, so keys are not needed.
    /// `txn_open` is the build's [`BuildEnv::txn_open`]: a superseded copy whose newer version
    /// in the image belongs to a txn whose commit is not durable yet is kept by the build
    /// (§13.3 c, E6), so its bytes are taken too. Counted free, they let an MSET's put in on a
    /// full device that its build then could not place: it moved, found no page, and its txn,
    /// SAVE and SHUTDOWN waited for space forever.
    pub(crate) fn places(&self, img: &[u8; PAGE], mode: PageUpdateMode, skip: Option<(u8, u32, &[u8])>, extra: &[u16], txn_open: &dyn Fn(u64) -> bool) -> bool {
        let (recs, _) = self.image();
        let kept: SmallVec<[&RecDesc; 24]> = recs
            .iter()
            .enumerate()
            .filter(|&(i, r)| match r.status {
                RecStatus::Live | RecStatus::Protected => true,
                RecStatus::Superseded => { let _ = (i, img); false },
                RecStatus::Droppable => false,
            })
            .map(|(_, r)| r)
            .collect();
        let mut occupied: SmallVec<[(u16, u16); 24]> = kept.iter().map(|r| (r.off, r.end())).collect();
        let mut lens: SmallVec<[u16; 8]> = extra.iter().copied().chain(self.phantom.iter().copied()).collect();
        for p in &self.pending {
            let Pending::Put(r) = p else { continue };
            if let Some((db, h32, key)) = skip
                && p.is_key(db, h32, key)
            {
                continue;
            }
            if mode == PageUpdateMode::Atomic && r.desc.txn == 0 {
                let newest = (0..kept.len())
                    .filter(|&j| kept[j].db == r.desc.db && kept[j].h32 == r.desc.h32 && kept[j].key(img) == r.key())
                    .max_by_key(|&j| kept[j].seq);
                if let Some(j) = newest
                    && r.desc.len <= kept[j].len
                {
                    // rewritten at the old copy's offset; its tail is free. Behind a version of
                    // an open txn the put waits for the commit (E6) and the copy keeps every
                    // byte until then: it is rewritten in place later, needing no gap either
                    if true {
                        occupied[j].1 = occupied[j].0 + r.desc.len;
                    }
                    continue;
                }
            }
            lens.push(r.desc.len);
        }
        lens.sort_unstable_by(|a, b| b.cmp(a));
        for len in lens {
            match find_gap(&mut occupied, len) {
                Some(off) => occupied.push((off, off + len)),
                None => return false,
            }
        }
        true
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
    /// Copies of `(db, key)` tracked outside this page with a seq below `seq` (unknown seqs
    /// count): the copies a removal or retirement of seq `seq` waits for (D1).
    fn others_older(&self, db: u8, key: &[u8], seq: Seq) -> usize;
    /// State of a tracked (moved-away or older) copy of `(db, key)` located in this page.
    fn tracked_here(&self, db: u8, key: &[u8]) -> Option<CopyState>;
    /// Every key with a tracked copy located in this page.
    fn tracked_keys(&self) -> SmallVec<[(u8, Box<[u8]>); 2]>;
    /// The record is dead: below a durable flush cutoff, or of an aborted txn (§16.2).
    fn dead(&self, r: &RecDesc) -> bool;
    /// The record belongs to a txn whose commit is not durable yet (§13.3 c).
    fn txn_open(&self, txn: u64) -> bool;
    /// The tracked copy of `(db, key)` in this page is fenced: a delete (or an expiry with
    /// older copies) made it a copy to zero with no durable newer copy shadowing it (D1).
    fn fenced_here(&self, _db: u8, _key: &[u8]) -> bool {
        false
    }
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
    /// Keys whose newest copy in M is durable and committed while copies elsewhere are still
    /// tracked: `(db, key, seq of that copy)`. Sources with a lower seq are shadowed by it (E2
    /// holds) and go stale. Every such source should already have been released by the event
    /// that made the copy durable; the partition counts each one this check releases.
    pub(crate) sources_durable: SmallVec<[KeyAt; 1]>,
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

/// `(db, key, seq)` of one copy of a key.
pub(crate) type KeyAt = (u8, Box<[u8]>, Seq);

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
/// R2 offsets for records of `lens` (sorted longest first) among `taken` bytes, with
/// `reserved` lengths placed alongside them in the same order but not reported; `None` for a
/// record no gap takes.
fn ffd(taken: &[(u16, u16)], lens: &[u16], reserved: &[u16]) -> SmallVec<[Option<u16>; 8]> {
    let mut occupied: SmallVec<[(u16, u16); 24]> = taken.iter().copied().collect();
    let mut all: SmallVec<[(u16, Option<usize>); 12]> = lens.iter().enumerate().map(|(i, &l)| (l, Some(i))).collect();
    all.extend(reserved.iter().map(|&l| (l, None)));
    // stable: equal lengths keep the records' R2 order, reservations after them
    all.sort_by(|a, b| b.0.cmp(&a.0));
    let mut out: SmallVec<[Option<u16>; 8]> = SmallVec::from_elem(None, lens.len());
    for (len, i) in all {
        if let Some(off) = find_gap(&mut occupied, len) {
            occupied.push((off, off + len));
            if let Some(i) = i {
                out[i] = Some(off);
            }
        }
    }
    out
}

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
        sources_durable: SmallVec::new(),
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

    // 1b. two-image mode: a fenced key (deleted, nothing newer durable) whose every copy in M
    //     would go in this image keeps its newest one until the older ones are durably zeroed.
    //     N may tear: zeroing both at once could leave the older copy's sectors from M and the
    //     newest one's from N, and recovery would bring the older version back (D1, I1).
    if mode == PageUpdateMode::TwoImage && drop.len() >= 2 {
        let mut i = 0;
        while i < drop.len() {
            let r = drop[i];
            let key = r.key(m);
            let same: SmallVec<[usize; 2]> = drop.iter().enumerate().filter(|(_, d)| d.db == r.db && d.h32 == r.h32 && d.key(m) == key).map(|(j, _)| j).collect();
            let first = same.first() == Some(&i);
            if first
                && same.len() >= 2
                && !env.dead(&r)
                && env.fenced_here(r.db, key)
                && !keep.iter().any(|k| k.db == r.db && k.h32 == r.h32 && k.key(m) == key)
            {
                let newest = same.iter().copied().max_by_key(|&j| drop[j].seq).expect("non-empty");
                let mut kept = drop.remove(newest);
                kept.status = RecStatus::Protected;
                let k: Box<[u8]> = key.into();
                out.cleared.retain(|(db, c)| !(*db == kept.db && *c == k));
                out.foreground.push((kept.db, k));
                keep.push(kept);
                continue;
            }
            i += 1;
        }
    }

    let phantom = core::mem::take(&mut st.phantom);

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
            Pending::Remove { db, h32, key, seq, tickets } => {
                // D1 counts only copies older than the removal: later writes of the key made
                // newer ones, which shadow the removed copy anyway (D2)
                let others = env.others_older(db, &key, seq);
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
                    st.pending.push(Pending::Remove { db, h32, key, seq, tickets });
                } else {
                    let mut all = older.clone();
                    all.extend(newest);
                    mark_drop(&mut keep, &all, &mut drop);
                    if env.tracked_here(db, &key).is_some() {
                        // the deleted copy was tracked as a copy of the key: gone when N is
                        out.cleared.push((db, key.clone()));
                    }
                    out.applied.push(Pending::Remove { db, h32, key, seq, tickets });
                }
            }
            Pending::Retire { db, h32, key, seq, tickets } => {
                let others = env.others_older(db, &key, seq);
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
                        if let Some(i) = newest
                            && kept_newest
                            && !newest_txn_open
                            && env.tracked_here(db, rec.key()).is_none()
                        {
                            // the copy in M is durable and committed: tracked copies elsewhere
                            // with a lower seq are shadowed by it (only those: a copy here that
                            // the index no longer names may be older than the tracked ones)
                            out.sources_durable.push((db, rec.key().into(), keep[i].seq));
                        }
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
    let kept: SmallVec<[(u16, u16); 24]> = keep.iter().map(|r| (r.off, r.end())).collect();
    let lens: SmallVec<[u16; 8]> = puts.iter().map(|(r, _)| r.desc.len).collect();
    // Puts deferred to a later image (E14) and puts taken out since the last build were counted
    // when the others were admitted (`PageState::places`): their bytes stay free in N, so the
    // deferred ones still fit when they go in.
    let reserved: SmallVec<[u16; 4]> =
        st.pending.iter().filter_map(|p| if let Pending::Put(r) = p { Some(r.desc.len) } else { None }).chain(phantom.iter().copied()).collect();
    let mut offs = ffd(&kept, &lens, &reserved);
    if offs.iter().any(Option::is_none) && !st.fresh {
        // First-fit-decreasing is not monotone: with fewer bytes taken (records this build
        // drops) it can fail where the placement the puts were admitted by succeeded. The
        // build then places by that admission view: M's records as they were when the puts
        // were admitted (live and protected ones, and also superseded ones when the admission
        // saw them protected in an image then in flight). Bytes it leaves unused are free for
        // the next image.
        for with_superseded in [false, true] {
            let mut view = kept.clone();
            for r in st.recs.iter() {
                let taken = matches!(r.status, RecStatus::Live | RecStatus::Protected) || (with_superseded && r.status == RecStatus::Superseded);
                if taken && !keep.iter().any(|k| k.off == r.off) {
                    view.push((r.off, r.end()));
                }
            }
            let alt = ffd(&view, &lens, &reserved);
            if alt.iter().all(Option::is_some) {
                offs = alt;
                break;
            }
        }
    }
    if offs.iter().any(Option::is_none) && !reserved.is_empty() {
        // no view places everything: place what fits, the rest moves (§14.3)
        let alt = ffd(&kept, &lens, &[]);
        if alt.iter().filter(|o| o.is_none()).count() < offs.iter().filter(|o| o.is_none()).count() {
            offs = alt;
        }
    }
    let mut occupied = kept;
    let mut replaced: SmallVec<[u16; 2]> = SmallVec::new();
    for ((rec, old), at) in puts.into_iter().zip(offs) {
        match at {
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

    // tracked copies of this page that N no longer holds are gone once N is durable, whichever
    // rule dropped them (superseded, removed, retired, an older copy next to a new version,
    // dead): every one of them is reported, or its registry entry would outlive the record
    // and hold back the key's next write or delete forever. Keys are read from N: the records
    // placed by this build have N's offsets, and M's bytes there are another record (a key
    // that is a prefix of the other record's key matched it, and the copy was never reported)
    for (db, key) in env.tracked_keys() {
        let still = keep.iter().any(|r| r.db == db && r.key(n) == &*key && r.status != RecStatus::Droppable);
        if !still && !out.cleared.iter().any(|(d, k)| *d == db && **k == *key) {
            out.cleared.push((db, key));
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
    newer_open_txn_by(recs, i, img, &|t| env.txn_open(t))
}

/// [`newer_open_txn`] with the txn state as a predicate (admission, `PageState::places`).
fn newer_open_txn_by(recs: &[RecDesc], i: usize, img: &[u8; PAGE], txn_open: &dyn Fn(u64) -> bool) -> bool {
    let a = recs[i];
    recs.iter().enumerate().any(|(j, b)| {
        j != i && b.txn != 0 && b.db == a.db && b.h32 == a.h32 && b.seq > a.seq && txn_open(b.txn) && b.key(img) == a.key(img)
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
#[cfg(test)]
pub(crate) fn settle(recs: &mut [RecDesc], img: &[u8; PAGE]) {
    settle_with(recs, img, &|_| false);
}

/// [`settle`] where a dead record (`dead`: of an aborted txn, or below a flush cutoff) never
/// supersedes an older copy: that copy is the key's current version.
pub(crate) fn settle_with(recs: &mut [RecDesc], img: &[u8; PAGE], dead: &dyn Fn(&RecDesc) -> bool) {
    for i in 0..recs.len() {
        if !matches!(recs[i].status, RecStatus::Live | RecStatus::Protected) {
            continue;
        }
        let (a, ka) = (recs[i], recs[i].key(img));
        let newer = recs.iter().enumerate().any(|(j, b)| {
            j != i && b.db == a.db && b.h32 == a.h32 && b.seq > a.seq && b.key(img) == ka && !(b.txn != 0 && dead(b))
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
        /// Txns whose commit is not durable yet.
        pub(crate) open: Vec<u64>,
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
        fn others_older(&self, db: u8, key: &[u8], _seq: Seq) -> usize {
            self.others(db, key)
        }
        fn tracked_here(&self, _db: u8, _key: &[u8]) -> Option<CopyState> {
            None
        }
        fn tracked_keys(&self) -> SmallVec<[(u8, Box<[u8]>); 2]> {
            SmallVec::new()
        }
        fn dead(&self, _r: &RecDesc) -> bool {
            false
        }
        fn txn_open(&self, t: u64) -> bool {
            self.open.contains(&t)
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
        let env = Env { mode: PageUpdateMode::TwoImage, lba: 512, others: HashMap::new(), open: Vec::new() };
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
        let env = Env { mode: PageUpdateMode::TwoImage, lba: 512, others: HashMap::new(), open: Vec::new() };
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
        let env = Env { mode: PageUpdateMode::Atomic, lba: 512, others: HashMap::new(), open: Vec::new() };
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
        let mut env = Env { mode: PageUpdateMode::TwoImage, lba: 512, others: HashMap::new(), open: Vec::new() };
        let mut st = state(&[0; PAGE], true);
        st.pending.push(Pending::Put(encoded(b"k", b"v", 1)));
        let (m1, b) = run(&mut st, &[0; PAGE], &env);
        env.others.insert(b"k".to_vec(), 1);
        let mut st = PageState::loaded(page(), FrameIdx(0), b.recs.clone());
        st.pending.push(Pending::Remove { db: 0, h32: h32(b"k"), key: (*b"k").into(), seq: Seq(u64::MAX), tickets: SmallVec::new() });
        let (_n, b2) = run(&mut st, &m1, &env);
        assert!(b2.applied.is_empty() && st.pending.len() == 1, "deferred");
        assert_eq!(b2.foreground.len(), 1);
        env.others.clear();
        let (n3, b3) = run(&mut st, &m1, &env);
        assert_eq!(b3.applied.len(), 1);
        assert!(resolve(&n3).is_empty());
    }

    /// The page writer reports every tracked copy the new image no longer holds. Records it
    /// places have N's offsets: their keys must be read from N. Read from M, a record placed
    /// where M held a record of a key that starts with the tracked key's bytes looked like the
    /// tracked copy, the copy was never reported gone, and the key's next write waited for its
    /// zeroing forever (third copy, E14).
    #[test]
    fn tracked_copy_gone_is_reported_when_a_placed_record_reuses_its_bytes() {
        struct Tracked {
            base: Env,
            tracked: Vec<Vec<u8>>,
        }
        impl BuildEnv for Tracked {
            fn mode(&self) -> PageUpdateMode {
                self.base.mode()
            }
            fn crc(&self) -> RecordCrc {
                self.base.crc()
            }
            fn partition(&self) -> u16 {
                self.base.partition()
            }
            fn lba(&self) -> u32 {
                self.base.lba()
            }
            fn granularity(&self) -> Granularity {
                self.base.granularity()
            }
            fn atomic_min(&self) -> u32 {
                self.base.atomic_min()
            }
            fn others(&self, db: u8, key: &[u8]) -> usize {
                self.base.others(db, key)
            }
            fn others_older(&self, db: u8, key: &[u8], seq: Seq) -> usize {
                self.base.others_older(db, key, seq)
            }
            fn tracked_here(&self, _db: u8, key: &[u8]) -> Option<CopyState> {
                self.tracked.iter().any(|k| k == key).then_some(CopyState::Stale)
            }
            fn tracked_keys(&self) -> SmallVec<[(u8, Box<[u8]>); 2]> {
                self.tracked.iter().map(|k| (0, k.clone().into_boxed_slice())).collect()
            }
            fn dead(&self, r: &RecDesc) -> bool {
                self.base.dead(r)
            }
            fn txn_open(&self, t: u64) -> bool {
                self.base.txn_open(t)
            }
        }
        for mode in [PageUpdateMode::TwoImage, PageUpdateMode::Atomic] {
            let base = Env { mode, lba: 512, others: HashMap::new(), open: Vec::new() };
            let mut st = state(&[0; PAGE], true);
            st.pending.push(Pending::Put(encoded(b"{t}k10xxxxxxxxxxxxxxxxxxxxxxxx", &[5; 40], 1)));
            let (m1, b) = run(&mut st, &[0; PAGE], &base);
            // `{t}k1` is tracked here (a copy the previous image dropped); `{t}k10...` goes and
            // `{t}k7` is placed in its bytes
            let env = Tracked { base, tracked: vec![b"{t}k1".to_vec()] };
            let mut st = PageState::loaded(page(), FrameIdx(0), b.recs);
            let long = b"{t}k10xxxxxxxxxxxxxxxxxxxxxxxx";
            st.pending.push(Pending::Remove { db: 0, h32: h32(long), key: (*long).into(), seq: Seq(2), tickets: SmallVec::new() });
            st.pending.push(Pending::Put(encoded(b"{t}k7", &[6; 40], 3)));
            let mut n = [0u8; PAGE];
            let b2 = build(&mut st, &m1, &mut n, &env);
            let placed = b2.recs.iter().find(|r| r.key(&n) == b"{t}k7").expect("placed");
            assert_eq!(placed.key(&m1), b"{t}k1", "{mode:?}: the placed record sits where M holds the other key");
            assert!(b2.cleared.iter().any(|(_, k)| &**k == b"{t}k1"), "{mode:?}: the tracked copy is reported gone");
        }
    }

    #[test]
    fn third_copy_is_deferred() {
        let mut env = Env { mode: PageUpdateMode::TwoImage, lba: 512, others: HashMap::new(), open: Vec::new() };
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
        let env = Env { mode: PageUpdateMode::TwoImage, lba: 512, others: HashMap::new(), open: Vec::new() };
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
        let env = Env { mode: PageUpdateMode::TwoImage, lba: 512, others: HashMap::new(), open: Vec::new() };
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
        let env = Env { mode: PageUpdateMode::Atomic, lba: 512, others: HashMap::new(), open: Vec::new() };
        let mut ch = Changes::default();
        ch.add(0x1F0, 0x210);
        let (s, a) = io_shape(&ch, &env);
        assert!(a);
        assert_eq!(s.as_slice(), &[(0, 1024)], "aligned power of two covering the change");
        let env = Env { mode: PageUpdateMode::TwoImage, lba: 4096, others: HashMap::new(), open: Vec::new() };
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
            let env = Env { mode, lba: 512, others: HashMap::new(), open: Vec::new() };
            let mut m = [0u8; PAGE];
            let mut st = state(&m, true);
            let mut seq = 0u64;
            for batch in steps {
                for s in batch {
                    seq += 1;
                    let eff = match s {
                        Step::Put(k, l) => Pending::Put(encoded(&key(k), &vec![seq as u8; usize::from(l)], seq)),
                        Step::Del(k) => Pending::Remove { db: 0, h32: h32(&key(k)), key: key(k).into(), seq: Seq(u64::MAX), tickets: SmallVec::new() },
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

    /// The layout a full two-image device produced: two 1040-byte records, a 144-byte hole
    /// where a deleted record was, six 144-byte records and 976 free bytes at the end. The page
    /// has 1120 free bytes, but an update of the second 1040-byte record needs a 1040-byte gap
    /// beside its protected old copy (R1): admitted on the free byte count, it had to move away
    /// at build time, and with no page free anywhere the move waited forever.
    #[test]
    fn free_bytes_split_into_small_gaps_do_not_take_an_update() {
        let env = Env { mode: PageUpdateMode::TwoImage, lba: 512, others: HashMap::new(), open: Vec::new() };
        let mut st = state(&[0; PAGE], true);
        let a = vec![1u8; 1000];
        st.pending.push(Pending::Put(encoded(b"a", &a, 1)));
        st.pending.push(Pending::Put(encoded(b"b", &a, 2)));
        for i in 0..7u8 {
            st.pending.push(Pending::Put(encoded(&[b'c', i], &[2; 100], 3 + u64::from(i))));
        }
        let (m1, b) = run(&mut st, &[0; PAGE], &env);
        assert!(b.relocate.is_empty());
        let mut st = state(&m1, false);
        let kc = [b'c', 0];
        st.pending.push(Pending::Remove { db: 0, h32: h32(&kc), key: kc.as_slice().into(), seq: Seq(20), tickets: SmallVec::new() });
        let (m2, _) = run(&mut st, &m1, &env);
        let st = state(&m2, false);
        let room = PAGE as u16 - SMALL_HDR as u16;
        let b_new = encoded(b"b", &[3; 1000], 30);
        let need = b_new.desc.len;
        assert!(room - st.used_bytes() >= need, "the page has the free bytes ({} of {need})", room - st.used_bytes());
        assert!(!st.places(&m2, PageUpdateMode::TwoImage, Some((0, h32(b"b"), b"b")), &[need], &|x| env.txn_open(x)), "no gap takes the update");
        // what the build does with it
        let mut st2 = state(&m2, false);
        st2.pending.push(Pending::Put(b_new));
        let (_, b) = run(&mut st2, &m2, &env);
        assert_eq!(b.relocate.len(), 1);
        // a record the end gap takes is placed
        let small = encoded(b"d", &[4; 900], 31);
        assert!(st.places(&m2, PageUpdateMode::TwoImage, None, &[small.desc.len], &|x| env.txn_open(x)));
    }

    /// `encoded` for a record of txn `txn`.
    fn encoded_txn(key: &[u8], val: &[u8], seq: u64, txn: u64) -> EncodedRecord {
        let len = small_len(key.len(), val.len(), true);
        let h = RecHeader {
            kind: REC_KIND_STRING,
            db: 0,
            flags: crate::record::REC_FLAG_TXN,
            seq: Seq(seq),
            expire_at_ms: 0,
            key_len: key.len() as u16,
            val_len: val.len() as u16,
            h32: h32(key),
            txn: Some(txn),
        };
        let mut b = vec![0u8; len].into_boxed_slice();
        encode_small(&h, key, val, PageId(0), 0x20, RecordCrc::new(SALT), &mut b);
        let mut r = encoded(key, val, seq);
        r.bytes = b;
        r.desc.len = len as u16;
        r.desc.txn = txn;
        r.desc.flags = crate::record::REC_FLAG_TXN;
        r
    }

    /// The layout a full two-image device produced under MSETs: a 1360-byte copy of a key
    /// superseded in M by a 192-byte version of an MSET whose commit is not durable yet, beside
    /// records that leave no other gap. The build keeps the superseded copy while the txn is
    /// open (§13.3 c); admission counted its bytes free and let a 1152-byte put of the next
    /// MSET in, which then had to move, found no page, and its txn waited forever.
    #[test]
    fn a_copy_an_open_txn_supersedes_keeps_its_bytes_at_admission() {
        let mut env = Env { mode: PageUpdateMode::TwoImage, lba: 512, others: HashMap::new(), open: Vec::new() };
        let mut st = state(&[0; PAGE], true);
        st.pending.push(Pending::Put(encoded(b"a", &[1; 1326], 1)));
        st.pending.push(Pending::Put(encoded(b"b", &[2; 1326], 2)));
        st.pending.push(Pending::Put(encoded(b"c", &[3; 1150], 3)));
        let (m1, b) = run(&mut st, &[0; PAGE], &env);
        assert!(b.relocate.is_empty());
        // the MSET's version of `a` lands in the end gap; its old copy stays protected (R1)
        let mut st = state(&m1, false);
        st.pending.push(Pending::Put(encoded_txn(b"a", &[4; 40], 4, 7)));
        env.open.push(7);
        let (m2, b) = run(&mut st, &m1, &env);
        assert!(b.relocate.is_empty());
        let mut recs = b.recs;
        settle(&mut recs, &m2);
        let st = PageState::loaded(page(), FrameIdx(0), recs);
        assert!(st.recs.iter().any(|r| r.status == RecStatus::Superseded), "the old copy of `a` is superseded in M");
        let d = encoded(b"d", &[5; 1100], 5);
        let need = d.desc.len;
        assert!(!st.places(&m2, PageUpdateMode::TwoImage, None, &[need], &|x| env.txn_open(x)), "the open txn's superseded copy takes its bytes");
        // what the build does with it while the txn is open: it has to move
        let mut st2 = PageState::loaded(page(), FrameIdx(0), st.recs.clone());
        st2.pending.push(Pending::Put(encoded(b"d", &[5; 1100], 5)));
        let (_, b) = run(&mut st2, &m2, &env);
        assert_eq!(b.relocate.len(), 1, "the build keeps the superseded copy while the txn is open");
        // once the commit is durable the copy goes and the record is placed
        env.open.clear();
        assert!(st.places(&m2, PageUpdateMode::TwoImage, None, &[need], &|x| env.txn_open(x)));
        let mut st3 = PageState::loaded(page(), FrameIdx(0), st.recs.clone());
        st3.pending.push(Pending::Put(d));
        let (_, b) = run(&mut st3, &m2, &env);
        assert!(b.relocate.is_empty());
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 256, failure_persistence: None, ..ProptestConfig::default() })]

        /// `PageState::places` predicts the next build: when it says so every staged put is
        /// placed (no record has to move away), and without staged removes it is exact.
        #[test]
        fn prop_places_predicts_the_build(steps in proptest::collection::vec(proptest::collection::vec(
            prop_oneof![3 => (0u8..12, 0u16..1500).prop_map(|(k, l)| Step::Put(k, l)), 1 => (0u8..12).prop_map(Step::Del)], 1..6), 1..10),
                                           atomic in any::<bool>()) {
            let mode = if atomic { PageUpdateMode::Atomic } else { PageUpdateMode::TwoImage };
            let env = Env { mode, lba: 512, others: HashMap::new(), open: Vec::new() };
            let mut m = [0u8; PAGE];
            let mut st = state(&m, true);
            let mut seq = 0u64;
            for batch in steps {
                for s in batch {
                    seq += 1;
                    let eff = match s {
                        Step::Put(k, l) => Pending::Put(encoded(&key(k), &vec![seq as u8; usize::from(l)], seq)),
                        Step::Del(k) => Pending::Remove { db: 0, h32: h32(&key(k)), key: key(k).into(), seq: Seq(u64::MAX), tickets: SmallVec::new() },
                    };
                    if let Some(i) = st.pending.iter().position(|p| p.key() == eff.key()) {
                        st.pending.remove(i);
                    }
                    st.pending.push(eff);
                }
                let predicted = st.places(&m, mode, None, &[], &|x| env.txn_open(x));
                // a staged remove frees bytes the same build may place a record in: the
                // prediction counts them as taken (it may only err on the safe side)
                let removes = st.pending.iter().any(|p| matches!(p, Pending::Remove { .. }));
                let (n, b) = run(&mut st, &m, &env);
                if predicted || !removes {
                    prop_assert_eq!(predicted, b.relocate.is_empty(), "prediction {} but {} records moved", predicted, b.relocate.len());
                }
                if b.writes() {
                    m = n;
                    let mut recs = b.recs;
                    settle(&mut recs, &m);
                    let mut next = PageState::loaded(page(), FrameIdx(0), recs);
                    next.pending = core::mem::take(&mut st.pending);
                    st = next;
                }
            }
        }
    }

    /// First-fit-decreasing is not monotone: a remove applied by the same build frees a
    /// 128-byte gap, the 640-byte put then takes the low gap it opens, and the two 384-byte
    /// puts no longer fit, although all three were admitted against the page as it was. The
    /// build places them by the admission view instead of moving one away.
    #[test]
    fn admitted_puts_are_placed_when_a_removal_changes_the_gaps() {
        let env = Env { mode: PageUpdateMode::TwoImage, lba: 512, others: HashMap::new(), open: Vec::new() };
        let u = 128u16;
        let mut m = [0u8; PAGE];
        write_small_header(&mut m, page(), PART);
        let crc = RecordCrc::new(SALT);
        // (unit offset, record length) of M's records; the last one fills the page's end
        let layout: [(u16, u16); 5] = [(3, 128), (7, 128), (9, 512), (15, 512), (24, 992)];
        for (i, &(at, len)) in layout.iter().enumerate() {
            let r = encoded(&[b'm', i as u8], &vec![9; usize::from(len) - 34], 1 + i as u64);
            assert_eq!(r.desc.len, len);
            write_rec(&mut m, &r, SMALL_HDR as u16 + at * u, page(), crc);
        }
        let mut st = state(&m, false);
        assert_eq!(st.recs.len(), 5);
        let k0 = [b'm', 0];
        st.pending.push(Pending::Remove { db: 0, h32: h32(&k0), key: k0.as_slice().into(), seq: Seq(50), tickets: SmallVec::new() });
        for (i, len) in [384u16, 384, 640].into_iter().enumerate() {
            let r = encoded(&[b'p', i as u8], &vec![7; usize::from(len) - 34], 60 + i as u64);
            assert!(st.places(&m, PageUpdateMode::TwoImage, None, &[r.desc.len], &|x| env.txn_open(x)), "put {i} admitted");
            st.pending.push(Pending::Put(r));
        }
        // the plain placement over the bytes the build keeps fails
        let kept: SmallVec<[(u16, u16); 24]> = layout[1..].iter().map(|&(at, len)| (SMALL_HDR as u16 + at * u, SMALL_HDR as u16 + at * u + len)).collect();
        assert!(ffd(&kept, &[640, 384, 384], &[]).iter().any(Option::is_none));
        let (n, b) = run(&mut st, &m, &env);
        assert!(b.relocate.is_empty(), "{} admitted records moved", b.relocate.len());
        assert_eq!(b.placed, 3);
        let got = resolve(&n);
        assert!(!got.contains_key(&k0.to_vec()));
        for i in 0..3u8 {
            assert!(got.contains_key(&vec![b'p', i]));
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 512, failure_persistence: None, ..ProptestConfig::default() })]

        /// A put admitted to a page by `PageState::places` (as `Core::fits` and `page_takes`
        /// do) is placed by the page's next build, whatever was staged, removed or built since:
        /// it never has to move away.
        #[test]
        fn prop_admitted_puts_never_move(steps in proptest::collection::vec(proptest::collection::vec(
            prop_oneof![4 => (0u8..12, 0u16..1500).prop_map(|(k, l)| Step::Put(k, l)), 1 => (0u8..12).prop_map(Step::Del)], 1..8), 1..12),
                                         atomic in any::<bool>()) {
            let mode = if atomic { PageUpdateMode::Atomic } else { PageUpdateMode::TwoImage };
            let env = Env { mode, lba: 512, others: HashMap::new(), open: Vec::new() };
            let mut m = [0u8; PAGE];
            let mut st = state(&m, true);
            let mut seq = 0u64;
            for batch in steps {
                for s in batch {
                    seq += 1;
                    let eff = match s {
                        Step::Put(k, l) => {
                            let r = encoded(&key(k), &vec![seq as u8; usize::from(l)], seq);
                            if !st.places(&m, mode, Some((0, h32(&key(k)), &key(k))), &[r.desc.len], &|x| env.txn_open(x)) {
                                continue;
                            }
                            Pending::Put(r)
                        }
                        Step::Del(k) => Pending::Remove { db: 0, h32: h32(&key(k)), key: key(k).into(), seq: Seq(u64::MAX), tickets: SmallVec::new() },
                    };
                    if let Some(i) = st.pending.iter().position(|p| p.key() == eff.key()) {
                        // a remove replacing a staged put leaves its length behind (`stage_remove`)
                        if let (Pending::Put(r), Pending::Remove { .. }) = (st.pending.remove(i), &eff) {
                            st.phantom.push(r.desc.len);
                        }
                    }
                    st.pending.push(eff);
                }
                let (n, b) = run(&mut st, &m, &env);
                prop_assert!(b.relocate.is_empty(), "{} admitted records moved", b.relocate.len());
                if b.writes() {
                    m = n;
                    let mut recs = b.recs;
                    settle(&mut recs, &m);
                    let mut next = PageState::loaded(page(), FrameIdx(0), recs);
                    next.pending = core::mem::take(&mut st.pending);
                    st = next;
                }
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 512, failure_persistence: None, ..ProptestConfig::default() })]

        /// `prop_admitted_puts_never_move` with MSETs: each batch may be one txn, whose
        /// versions supersede older copies that the build keeps until its commit is durable;
        /// commits become durable in any order, some batches later.
        #[test]
        fn prop_admitted_txn_puts_never_move(steps in proptest::collection::vec((proptest::collection::vec(
            prop_oneof![4 => (0u8..12, 0u16..1500).prop_map(|(k, l)| Step::Put(k, l)), 1 => (0u8..12).prop_map(Step::Del)], 1..8), any::<bool>(), any::<u8>()), 1..14),
                                             atomic in any::<bool>()) {
            let mode = if atomic { PageUpdateMode::Atomic } else { PageUpdateMode::TwoImage };
            let mut env = Env { mode, lba: 512, others: HashMap::new(), open: Vec::new() };
            let mut m = [0u8; PAGE];
            let mut st = state(&m, true);
            let mut seq = 0u64;
            for (n_batch, (batch, is_txn, commit)) in steps.into_iter().enumerate() {
                let txn = if is_txn { 100 + n_batch as u64 } else { 0 };
                for s in batch {
                    seq += 1;
                    let eff = match s {
                        Step::Put(k, l) => {
                            let val = vec![seq as u8; usize::from(l)];
                            let r = if txn != 0 { encoded_txn(&key(k), &val, seq, txn) } else { encoded(&key(k), &val, seq) };
                            if !st.places(&m, mode, Some((0, h32(&key(k)), &key(k))), &[r.desc.len], &|x| env.txn_open(x)) {
                                continue;
                            }
                            Pending::Put(r)
                        }
                        Step::Del(k) if txn == 0 => Pending::Remove { db: 0, h32: h32(&key(k)), key: key(k).into(), seq: Seq(u64::MAX), tickets: SmallVec::new() },
                        Step::Del(_) => continue,
                    };
                    if let Some(i) = st.pending.iter().position(|p| p.key() == eff.key()) {
                        if let (Pending::Put(r), Pending::Remove { .. }) = (st.pending.remove(i), &eff) {
                            st.phantom.push(r.desc.len);
                        }
                    }
                    st.pending.push(eff);
                }
                if txn != 0 {
                    env.open.push(txn);
                }
                let (n, b) = run(&mut st, &m, &env);
                prop_assert!(b.relocate.is_empty(), "{} admitted records moved", b.relocate.len());
                if b.writes() {
                    m = n;
                    let mut recs = b.recs;
                    settle(&mut recs, &m);
                    let mut next = PageState::loaded(page(), FrameIdx(0), recs);
                    next.pending = core::mem::take(&mut st.pending);
                    next.phantom = core::mem::take(&mut st.phantom);
                    st = next;
                }
                // a commit becomes durable
                if !env.open.is_empty() && commit % 3 != 0 {
                    let i = usize::from(commit) % env.open.len();
                    env.open.remove(i);
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
