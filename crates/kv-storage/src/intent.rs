//! Intent table: crash-atomic multi-key commits (DESIGN §10.7, §14.6, §17.5).
//!
//! Header (AbBlock magic `KVINTNT1`): `txn_floor` (ids handed out are below it; reserved
//! 2^20 at a time) and `resolved_w` (every txn id <= W is resolved). 64 slots, each with two
//! 512-byte copies in separate `lba` sectors: `"KVIS"`, crc32c, `copy_seq`, `n <= 60` committed
//! txn ids. The writer overwrites the copy with the lower `copy_seq`. A commit batch goes into
//! the lowest-index slot whose open ids plus the batch fit in 60; resolved ids are dropped by
//! the next rewrite of a slot after a durable `resolved_w` covers them (E10). Slots are never
//! selected by rotation and nothing is ordered by slot index or `copy_seq` across slots. The
//! table holds only committed txn ids: no values, keys or record headers (LC-02).
//!
//! A TXN-flagged record with id `t` is valid iff `t <= resolved_w` or `t` is listed in some
//! slot's newest valid copy.
//!
//! Txn credits: 3840 commit positions divided statically among workers (`3840 / W` each); a
//! worker takes one before it consumes a multi-key write and gets it back when the txn resolves,
//! so a consumed txn never stalls on a slot (LC-15).
//!
//! ```text
//! header  0x018 u64 txn_floor   0x020 u64 resolved_w   0x028 u32 n_slots = 64   0x02C u32 ids_per_copy = 60
//! slot    0x00 u32 "KVIS"   0x04 u32 crc32c([0x08, 0x200))   0x08 u64 copy_seq   0x10 u16 n   0x12 [6] zero
//!         0x18 u64 txn_ids[60]   0x1F8 [8] zero   (rest of the lba sector zero)
//! ```

use core::sync::atomic::{AtomicU64, Ordering};

use kv_common::abblock::AbCopy;
use smallvec::SmallVec;

use crate::layout::{PAGE, ab_build, ab_parse, ab_select, le};

/// `KVINTNT1`
pub const INTENT_MAGIC: u64 = kv_common::abblock::magic(b"KVINTNT1");
/// `"KVIS"` slot copy magic.
pub const INTENT_SLOT_MAGIC: u32 = u32::from_le_bytes(*b"KVIS");
/// Bytes of one slot copy (the first 512 B of its sector).
pub const INTENT_SLOT_COPY_BYTES: usize = 512;

pub const INTENT_SLOTS: usize = 64;
/// 3840 txn credits
pub const IDS_PER_COPY: usize = 60;
/// Commit positions of the whole table (`INTENT_SLOTS * IDS_PER_COPY`).
pub const TXN_CREDITS: u32 = (INTENT_SLOTS * IDS_PER_COPY) as u32;
/// Txn ids reserved per `txn_floor` extension (§10.7).
pub const TXN_ID_RESERVE: u64 = 1 << 20;
/// Never a valid txn id (ids start at 1).
pub const TXN_ID_NONE: u64 = 0;

const SLOT_IDS_OFF: usize = 0x18;
const _: () = assert!(SLOT_IDS_OFF + 8 * IDS_PER_COPY + 8 == INTENT_SLOT_COPY_BYTES);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct IntentHeader {
    pub txn_floor: u64,
    pub resolved_w: u64,
}

impl IntentHeader {
    /// The header written at format: no id handed out yet (ids start at 1), nothing resolved.
    pub fn initial() -> Self {
        IntentHeader { txn_floor: 1, resolved_w: 0 }
    }

    pub fn encode(&self, seq: u64, out: &mut [u8; PAGE]) {
        ab_build(INTENT_MAGIC, seq, out, |b| {
            le::put_u64(b, 0x018, self.txn_floor);
            le::put_u64(b, 0x020, self.resolved_w);
            le::put_u32(b, 0x028, INTENT_SLOTS as u32);
            le::put_u32(b, 0x02C, IDS_PER_COPY as u32);
        });
    }

    /// Rejects a table geometry other than 64 slots x 60 ids.
    pub fn decode(b: &[u8]) -> Option<(u64, Self)> {
        ab_parse(INTENT_MAGIC, b, |b| {
            let geometry_ok = le::u32_at(b, 0x028) == INTENT_SLOTS as u32 && le::u32_at(b, 0x02C) == IDS_PER_COPY as u32;
            geometry_ok.then(|| IntentHeader { txn_floor: le::u64_at(b, 0x018), resolved_w: le::u64_at(b, 0x020) })
        })
    }

    /// Reads both copies and returns the valid one with the higher `block_seq`.
    pub fn pick(a: &[u8], b: &[u8]) -> Option<(AbCopy, u64, Self)> {
        ab_select(Self::decode(a), Self::decode(b))
    }
}

/// One copy of one intent slot.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct IntentSlot {
    pub copy_seq: u64,
    pub txn_ids: SmallVec<[u64; 60]>,
}

impl IntentSlot {
    /// Panics if more than 60 ids are listed (a caller bug: see [`plan_commit`]).
    pub fn encode(&self, out: &mut [u8; INTENT_SLOT_COPY_BYTES]) {
        assert!(self.txn_ids.len() <= IDS_PER_COPY, "an intent slot copy lists at most 60 ids");
        out.fill(0);
        le::put_u32(out, 0x00, INTENT_SLOT_MAGIC);
        le::put_u64(out, 0x08, self.copy_seq);
        le::put_u16(out, 0x10, self.txn_ids.len() as u16);
        for (i, &t) in self.txn_ids.iter().enumerate() {
            le::put_u64(out, SLOT_IDS_OFF + 8 * i, t);
        }
        let crc = kv_common::crc::crc32c(&out[0x08..INTENT_SLOT_COPY_BYTES]);
        le::put_u32(out, 0x04, crc);
    }

    /// Encodes into a whole `lba` sector (`sector.len() >= 512`): the copy, then zeros.
    pub fn encode_sector(&self, sector: &mut [u8]) {
        let (head, rest) = sector.split_at_mut(INTENT_SLOT_COPY_BYTES);
        let head: &mut [u8; INTENT_SLOT_COPY_BYTES] = head.try_into().expect("512-byte slot copy");
        self.encode(head);
        rest.fill(0);
    }

    /// Validates magic, crc and `n <= 60`; reads the first 512 bytes of `b`.
    pub fn decode(b: &[u8]) -> Option<Self> {
        if b.len() < INTENT_SLOT_COPY_BYTES || le::u32_at(b, 0x00) != INTENT_SLOT_MAGIC {
            return None;
        }
        if le::u32_at(b, 0x04) != kv_common::crc::crc32c(&b[0x08..INTENT_SLOT_COPY_BYTES]) {
            return None;
        }
        let n = usize::from(le::u16_at(b, 0x10));
        if n > IDS_PER_COPY || !le::zero(b, 0x12..0x18) {
            return None;
        }
        let txn_ids = (0..n).map(|i| le::u64_at(b, SLOT_IDS_OFF + 8 * i)).collect();
        Some(IntentSlot { copy_seq: le::u64_at(b, 0x08), txn_ids })
    }
}

/// The two copies of one slot as read from media (invalid copies are `None`).
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct SlotPair {
    pub a: Option<IntentSlot>,
    pub b: Option<IntentSlot>,
}

impl SlotPair {
    pub fn decode(a: &[u8], b: &[u8]) -> Self {
        SlotPair { a: IntentSlot::decode(a), b: IntentSlot::decode(b) }
    }

    /// The newest valid copy (higher `copy_seq`; ties: A).
    pub fn newest(&self) -> Option<(AbCopy, &IntentSlot)> {
        ab_select(self.a.as_ref().map(|s| (s.copy_seq, s)), self.b.as_ref().map(|s| (s.copy_seq, s)))
            .map(|(c, _, s)| (c, s))
    }

    /// The copy the next write overwrites: the one with the lower `copy_seq`, an invalid copy
    /// counting as the lowest. A torn write therefore never damages the newest valid copy.
    pub fn target(&self) -> AbCopy {
        match self.newest() {
            Some((AbCopy::A, _)) => AbCopy::B,
            _ => AbCopy::A,
        }
    }

    /// Ids of the newest valid copy that are not yet resolved (`> resolved_w`).
    pub fn open_ids(&self, resolved_w: u64) -> impl Iterator<Item = u64> + '_ {
        self.newest().into_iter().flat_map(move |(_, s)| s.txn_ids.iter().copied().filter(move |&t| t > resolved_w))
    }

    pub fn open_count(&self, resolved_w: u64) -> usize {
        self.open_ids(resolved_w).count()
    }

    /// The new copy listing exactly the open ids plus `batch`, and where it goes. Resolved ids
    /// are dropped; the caller makes sure the covering `resolved_w` is durable first (E10).
    ///
    /// Returns `None` when the open ids plus the batch do not fit in 60.
    pub fn next_copy(&self, resolved_w: u64, batch: &[u64]) -> Option<(AbCopy, IntentSlot)> {
        let mut txn_ids: SmallVec<[u64; 60]> = self.open_ids(resolved_w).collect();
        if txn_ids.len() + batch.len() > IDS_PER_COPY {
            return None;
        }
        txn_ids.extend_from_slice(batch);
        let seq = self.newest().map_or(0, |(_, s)| s.copy_seq) + 1;
        Some((self.target(), IntentSlot { copy_seq: seq, txn_ids }))
    }

    /// Writes `copy` into this pair after it became durable.
    pub fn store(&mut self, c: AbCopy, copy: IntentSlot) {
        match c {
            AbCopy::A => self.a = Some(copy),
            AbCopy::B => self.b = Some(copy),
        }
    }
}

/// Assigns a commit batch of `batch` ids to slots: the lowest-index slot whose open ids plus
/// the whole batch fit in 60; when no slot takes it whole, the batch is split over the
/// lowest-index slots with room, in index order (§10.7). Each slot appears at most once, so
/// one flush writes one copy per slot. `open[s]` is slot `s`'s open id count.
///
/// Returns `false` (and leaves `out` empty) when the table has no room for the batch, which the
/// txn credits rule out.
pub fn plan_commit(open: &[usize; INTENT_SLOTS], batch: usize, out: &mut SmallVec<[(u8, u8); 4]>) -> bool {
    out.clear();
    if batch == 0 {
        return true;
    }
    if let Some(s) = (0..INTENT_SLOTS).find(|&s| open[s] + batch <= IDS_PER_COPY) {
        out.push((s as u8, batch as u8));
        return true;
    }
    let mut left = batch;
    for (s, &n) in open.iter().enumerate() {
        let room = IDS_PER_COPY.saturating_sub(n);
        if room == 0 {
            continue;
        }
        let take = room.min(left);
        out.push((s as u8, take as u8));
        left -= take;
        if left == 0 {
            return true;
        }
    }
    out.clear();
    false
}

/// The committed-txn view recovery needs: `resolved_w` plus every id listed in a slot's newest
/// valid copy (§10.7). Membership only: nothing is ordered by slot or `copy_seq`.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct CommittedSet {
    pub resolved_w: u64,
    /// Sorted, de-duplicated listed ids above `resolved_w`.
    listed: Vec<u64>,
}

impl CommittedSet {
    pub fn new(resolved_w: u64, slots: &[SlotPair]) -> Self {
        let mut listed: Vec<u64> = slots.iter().flat_map(|p| p.open_ids(resolved_w)).collect();
        listed.sort_unstable();
        listed.dedup();
        CommittedSet { resolved_w, listed }
    }

    /// True when a TXN-flagged record with id `t` is valid: `t <= resolved_w` or `COMMITTED(t)`.
    pub fn is_committed(&self, t: u64) -> bool {
        t != TXN_ID_NONE && (t <= self.resolved_w || self.listed.binary_search(&t).is_ok())
    }

    /// Listed ids above `resolved_w` (ascending).
    pub fn listed(&self) -> &[u64] {
        &self.listed
    }
}

/// Per-worker pool of txn credits: `3840 / W` each; taken before consuming a multi-key write,
/// returned on `MetaDone::Resolved`.
pub struct TxnCredits {
    total: u32,
    in_use: u32,
}

impl TxnCredits {
    pub fn new(total: u32) -> Self {
        TxnCredits { total, in_use: 0 }
    }

    /// Worker `w`'s share of the table for `workers` workers (the remainder goes to the
    /// lowest-numbered workers so that every credit is assigned).
    pub fn share(workers: u32, w: u32) -> u32 {
        let workers = workers.max(1);
        let base = TXN_CREDITS / workers;
        let extra = TXN_CREDITS - base * workers;
        base + u32::from(w < extra)
    }

    pub fn try_take(&mut self) -> bool {
        if self.in_use < self.total {
            self.in_use += 1;
            true
        } else {
            false
        }
    }

    pub fn give_back(&mut self, n: u32) {
        debug_assert!(n <= self.in_use);
        self.in_use -= n.min(self.in_use);
    }

    pub fn in_use(&self) -> u32 {
        self.in_use
    }

    pub fn total(&self) -> u32 {
        self.total
    }
}

/// Node-global txn id source inside the reserved `txn_floor` range; the meta thread extends the
/// floor by 2^20 at half use, so `next` never stalls.
///
/// Ids are handed out only below a floor that is durable: the meta thread writes the intent
/// header with the new floor, and calls [`TxnIds::extend_floor`] once that write is durable.
pub struct TxnIds {
    next: AtomicU64,
    floor: AtomicU64,
}

impl TxnIds {
    /// `start` = the durable `txn_floor` read at startup (ids below it may exist on media),
    /// `floor` = the durable floor ids may be handed out below.
    pub fn new(start: u64, floor: u64) -> Self {
        let start = start.max(1);
        TxnIds { next: AtomicU64::new(start), floor: AtomicU64::new(floor.max(start)) }
    }

    /// The next txn id, or [`TXN_ID_NONE`] when the reserved range is exhausted (the caller
    /// then leaves the multi-key write unconsumed, exactly like a missing credit, §14.6).
    pub fn next(&self) -> u64 {
        self.try_next().unwrap_or(TXN_ID_NONE)
    }

    /// The next txn id, `None` when the reserved range is exhausted.
    pub fn try_next(&self) -> Option<u64> {
        self.next
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| (n < self.floor.load(Ordering::Acquire)).then_some(n + 1))
            .ok()
    }

    /// Ids left below the floor.
    pub fn available(&self) -> u64 {
        self.floor.load(Ordering::Acquire).saturating_sub(self.next.load(Ordering::Acquire))
    }

    /// True when fewer than half a reservation is left: the meta thread should extend.
    pub fn wants_extension(&self) -> bool {
        self.available() < TXN_ID_RESERVE / 2
    }

    /// The floor the next reservation writes (`next + 2^20`, never lower than the current one).
    pub fn next_floor(&self) -> u64 {
        let next = self.next.load(Ordering::Acquire);
        (next + TXN_ID_RESERVE).max(self.floor.load(Ordering::Acquire))
    }

    /// Raises the floor once a header naming `floor` is durable (never lowers it).
    pub fn extend_floor(&self, floor: u64) {
        self.floor.fetch_max(floor, Ordering::AcqRel);
    }

    pub fn floor(&self) -> u64 {
        self.floor.load(Ordering::Acquire)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use proptest::prelude::*;
    use smallvec::smallvec;

    proptest! {
        #![proptest_config(ProptestConfig { cases: 256, failure_persistence: None, ..ProptestConfig::default() })]

        #[test]
        fn prop_slot_round_trip(copy_seq in any::<u64>(), ids in proptest::collection::vec(any::<u64>(), 0..=IDS_PER_COPY)) {
            let s = IntentSlot { copy_seq, txn_ids: ids.into_iter().collect() };
            let mut b = [0u8; INTENT_SLOT_COPY_BYTES];
            s.encode(&mut b);
            prop_assert_eq!(IntentSlot::decode(&b), Some(s));
        }

        #[test]
        fn prop_header_round_trip(floor in any::<u64>(), w in any::<u64>(), seq in any::<u64>()) {
            let h = IntentHeader { txn_floor: floor, resolved_w: w };
            let mut b = [0u8; PAGE];
            h.encode(seq, &mut b);
            prop_assert_eq!(IntentHeader::decode(&b), Some((seq, h)));
        }

        #[test]
        fn prop_random_copies_never_decode(b in proptest::collection::vec(any::<u8>(), INTENT_SLOT_COPY_BYTES)) {
            prop_assert!(IntentSlot::decode(&b).is_none());
        }

        /// A torn slot write (§17.5): the copy being overwritten holds any byte mix of its old and
        /// new image; the slot then reads as the new copy only if it is whole, else as the
        /// previous newest copy, so a batch is either committed whole or not at all.
        #[test]
        fn prop_torn_slot_copy(
            old_ids in proptest::collection::vec(1u64..1000, 0..30),
            batch in proptest::collection::vec(1000u64..2000, 1..30),
            mask in proptest::collection::vec(any::<bool>(), INTENT_SLOT_COPY_BYTES),
            target_b in any::<bool>(),
        ) {
            let newest = IntentSlot { copy_seq: 10, txn_ids: old_ids.iter().copied().collect() };
            let older = IntentSlot { copy_seq: 9, txn_ids: smallvec![] };
            let mut pair = if target_b {
                SlotPair { a: Some(newest.clone()), b: Some(older.clone()) }
            } else {
                SlotPair { a: Some(older.clone()), b: Some(newest.clone()) }
            };
            let (c, next) = pair.next_copy(0, &batch).unwrap();
            prop_assert_eq!(c, if target_b { AbCopy::B } else { AbCopy::A });
            let (mut img_old, mut img_new) = ([0u8; INTENT_SLOT_COPY_BYTES], [0u8; INTENT_SLOT_COPY_BYTES]);
            older.encode(&mut img_old);
            next.encode(&mut img_new);
            let mut torn = img_old;
            for (i, &m) in mask.iter().enumerate() {
                if m { torn[i] = img_new[i]; }
            }
            let decoded = IntentSlot::decode(&torn);
            match c { AbCopy::A => pair.a = decoded, AbCopy::B => pair.b = decoded }
            let open: Vec<u64> = pair.open_ids(0).collect();
            if torn == img_new {
                let mut want = old_ids.clone();
                want.extend(&batch);
                prop_assert_eq!(open, want);
            } else {
                prop_assert_eq!(open, old_ids);
            }
        }
    }

    pub(crate) fn sample_slot() -> IntentSlot {
        IntentSlot { copy_seq: 77, txn_ids: smallvec![5, 6, 1 << 40, 9] }
    }

    #[test]
    fn header_round_trip() {
        let h = IntentHeader { txn_floor: 3 << 20, resolved_w: 12345 };
        let mut b = [0u8; PAGE];
        h.encode(6, &mut b);
        assert_eq!(&b[..8], b"KVINTNT1");
        assert_eq!(le::u64_at(&b, 0x18), 3 << 20);
        assert_eq!(le::u64_at(&b, 0x20), 12345);
        assert_eq!(le::u32_at(&b, 0x28), 64);
        assert_eq!(le::u32_at(&b, 0x2C), 60);
        assert!(le::zero(&b, 0x30..PAGE));
        assert_eq!(IntentHeader::decode(&b), Some((6, h)));
        assert_eq!(IntentHeader::pick(&b, &[0u8; PAGE]), Some((AbCopy::A, 6, h)));
        let mut p = [0u8; PAGE];
        le::put_u32(&mut p, 0x28, 32);
        le::put_u32(&mut p, 0x2C, 60);
        kv_common::abblock::ab_encode(INTENT_MAGIC, 1, &p[0x18..], &mut b);
        assert!(IntentHeader::decode(&b).is_none());
    }

    #[test]
    fn slot_round_trip() {
        let s = sample_slot();
        let mut b = [0u8; INTENT_SLOT_COPY_BYTES];
        s.encode(&mut b);
        assert_eq!(&b[..4], b"KVIS");
        assert_eq!(le::u64_at(&b, 8), 77);
        assert_eq!(le::u16_at(&b, 0x10), 4);
        assert_eq!(le::u64_at(&b, 0x18 + 16), 1 << 40);
        assert!(le::zero(&b, 0x18 + 32..512));
        assert_eq!(le::u32_at(&b, 4), kv_common::crc::crc32c(&b[8..512]));
        assert_eq!(IntentSlot::decode(&b), Some(s.clone()));
        let full = IntentSlot { copy_seq: 1, txn_ids: (1..=60).collect() };
        full.encode(&mut b);
        assert_eq!(IntentSlot::decode(&b), Some(full));
        let mut sector = vec![0xEEu8; 4096];
        s.encode_sector(&mut sector);
        assert_eq!(IntentSlot::decode(&sector), Some(s));
        assert!(sector[512..].iter().all(|&x| x == 0));
    }

    #[test]
    fn slot_rejects_damage() {
        let mut b = [0u8; INTENT_SLOT_COPY_BYTES];
        sample_slot().encode(&mut b);
        for off in [0usize, 4, 8, 0x10, 0x12, 0x18, 0x1FF] {
            let mut d = b;
            d[off] ^= 0x10;
            assert!(IntentSlot::decode(&d).is_none(), "flip at {off}");
        }
        assert!(IntentSlot::decode(&b[..511]).is_none());
        assert!(IntentSlot::decode(&[0u8; 512]).is_none());
        // n > 60 with a valid crc
        let mut d = b;
        le::put_u16(&mut d, 0x10, 61);
        let crc = kv_common::crc::crc32c(&d[8..512]);
        le::put_u32(&mut d, 4, crc);
        assert!(IntentSlot::decode(&d).is_none());
    }

    #[test]
    fn pair_copy_rules() {
        let mut p = SlotPair::default();
        assert_eq!(p.target(), AbCopy::A);
        assert_eq!(p.open_count(0), 0);
        let (c, s) = p.next_copy(0, &[10, 11]).unwrap();
        assert_eq!((c, s.copy_seq, s.txn_ids.as_slice()), (AbCopy::A, 1, &[10u64, 11][..]));
        p.store(c, s);
        let (c, s) = p.next_copy(0, &[12]).unwrap();
        assert_eq!((c, s.copy_seq, s.txn_ids.as_slice()), (AbCopy::B, 2, &[10u64, 11, 12][..]));
        p.store(c, s);
        // resolved_w = 11 drops 10 and 11 from the next copy, which overwrites A (lower seq)
        assert_eq!(p.open_ids(11).collect::<Vec<_>>(), vec![12]);
        let (c, s) = p.next_copy(11, &[13]).unwrap();
        assert_eq!((c, s.copy_seq, s.txn_ids.as_slice()), (AbCopy::A, 3, &[12u64, 13][..]));
        // full: 59 open + 2 do not fit
        let big = SlotPair { a: Some(IntentSlot { copy_seq: 9, txn_ids: (100..159).collect() }), b: None };
        assert!(big.next_copy(0, &[1, 2]).is_none());
        assert!(big.next_copy(0, &[1]).is_some());
        assert_eq!(big.target(), AbCopy::B);
        // B newer but torn: A is the newest valid copy and B is overwritten again
        let mut sa = [0u8; 512];
        let mut sb = [0u8; 512];
        IntentSlot { copy_seq: 4, txn_ids: smallvec![1] }.encode(&mut sa);
        IntentSlot { copy_seq: 5, txn_ids: smallvec![1, 2] }.encode(&mut sb);
        sb[300] ^= 1;
        let p = SlotPair::decode(&sa, &sb);
        assert_eq!(p.newest().map(|(c, s)| (c, s.copy_seq)), Some((AbCopy::A, 4)));
        assert_eq!(p.target(), AbCopy::B);
    }

    #[test]
    fn plan_lowest_index_first() {
        let mut out = SmallVec::new();
        let mut open = [0usize; INTENT_SLOTS];
        assert!(plan_commit(&open, 0, &mut out) && out.is_empty());
        assert!(plan_commit(&open, 5, &mut out));
        assert_eq!(out.as_slice(), &[(0, 5)]);
        open[0] = 58;
        open[1] = 30;
        assert!(plan_commit(&open, 2, &mut out));
        assert_eq!(out.as_slice(), &[(0, 2)]);
        assert!(plan_commit(&open, 3, &mut out));
        assert_eq!(out.as_slice(), &[(1, 3)]);
        assert!(plan_commit(&open, 31, &mut out));
        assert_eq!(out.as_slice(), &[(2, 31)]);
        // a batch no slot takes whole is split over the lowest slots with room
        let mut open = [60usize; INTENT_SLOTS];
        open[3] = 50;
        open[9] = 55;
        open[40] = 0;
        assert!(plan_commit(&open, 20, &mut out));
        assert_eq!(out.as_slice(), &[(40, 20)]);
        open[40] = 50;
        assert!(plan_commit(&open, 22, &mut out));
        assert_eq!(out.as_slice(), &[(3, 10), (9, 5), (40, 7)]);
        assert!(!plan_commit(&open, 26, &mut out));
        assert!(out.is_empty());
    }

    #[test]
    fn committed_membership() {
        let p1 = SlotPair { a: Some(IntentSlot { copy_seq: 2, txn_ids: smallvec![7, 30, 31] }), b: Some(IntentSlot { copy_seq: 1, txn_ids: smallvec![99] }) };
        let p2 = SlotPair { a: None, b: Some(IntentSlot { copy_seq: 8, txn_ids: smallvec![40, 30] }) };
        let c = CommittedSet::new(10, &[p1, p2, SlotPair::default()]);
        assert_eq!(c.listed(), &[30, 31, 40]);
        assert!(c.is_committed(1) && c.is_committed(10) && c.is_committed(7));
        assert!(c.is_committed(30) && c.is_committed(31) && c.is_committed(40));
        assert!(!c.is_committed(11) && !c.is_committed(99) && !c.is_committed(41));
        assert!(!c.is_committed(TXN_ID_NONE));
    }

    #[test]
    fn credits_and_ids() {
        assert_eq!(TXN_CREDITS, 3840);
        assert_eq!((0..7).map(|w| TxnCredits::share(7, w)).sum::<u32>(), 3840);
        assert_eq!(TxnCredits::share(4, 0), 960);
        let mut c = TxnCredits::new(2);
        assert!(c.try_take() && c.try_take() && !c.try_take());
        c.give_back(2);
        assert_eq!(c.in_use(), 0);
        assert_eq!(c.total(), 2);

        let ids = TxnIds::new(1, 1);
        assert_eq!(ids.available(), 0);
        assert!(ids.wants_extension());
        assert_eq!(ids.next(), TXN_ID_NONE);
        assert_eq!(ids.next_floor(), 1 + TXN_ID_RESERVE);
        ids.extend_floor(4);
        assert_eq!((ids.next(), ids.next(), ids.next()), (1, 2, 3));
        assert_eq!(ids.try_next(), None);
        ids.extend_floor(2);
        assert_eq!(ids.floor(), 4);
        ids.extend_floor(4 + TXN_ID_RESERVE);
        assert!(!ids.wants_extension());
        assert_eq!(ids.try_next(), Some(4));
    }
}
