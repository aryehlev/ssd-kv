//! Hash slots (DESIGN §5, §6.2, §23.14).
//!
//! `slot = CRC16-XMODEM(tag or key) % 16384`, where the tag is the bytes between the first `{`
//! and the next `}` when non-empty (Redis Cluster rule). Test vector:
//! `crc16_xmodem(b"123456789") == 0x31C3`. [`SlotSet`] is a fixed 2 KiB bitmap of slots, used for
//! owned/served slot sets (§23.4, §23.6).

use core::fmt;

/// Number of hash slots.
pub const SLOTS: usize = 16384;

const WORDS: usize = SLOTS / 64;

/// CRC16-XMODEM lookup table (polynomial 0x1021, MSB first), built at compile time.
const CRC16_TABLE: [u16; 256] = {
    let mut t = [0u16; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = (i as u16) << 8;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 0x8000 != 0 { (crc << 1) ^ 0x1021 } else { crc << 1 };
            bit += 1;
        }
        t[i] = crc;
        i += 1;
    }
    t
};

/// CRC16-XMODEM (polynomial 0x1021, init 0, no reflection, no final xor), as used by Redis
/// Cluster.
#[inline]
pub fn crc16_xmodem(data: &[u8]) -> u16 {
    data.iter().fold(0u16, |crc, &b| (crc << 8) ^ CRC16_TABLE[usize::from((crc >> 8) as u8 ^ b)])
}

/// Redis hashtag rule: the bytes between the first `{` and the next `}` if that range is
/// non-empty, else the whole key.
#[inline]
pub fn hash_tag(key: &[u8]) -> &[u8] {
    if let Some(open) = key.iter().position(|&b| b == b'{') {
        let rest = &key[open + 1..];
        if let Some(len) = rest.iter().position(|&b| b == b'}')
            && len > 0
        {
            return &rest[..len];
        }
    }
    key
}

/// `crc16(hash_tag(key)) & 16383`.
#[inline]
pub fn key_slot(key: &[u8]) -> u16 {
    crc16_xmodem(hash_tag(key)) & (SLOTS as u16 - 1)
}

/// Bitmap of the 16384 hash slots (2 KiB). Slot arguments must be `< SLOTS`; larger values
/// panic (they are never valid slots).
#[derive(Clone, PartialEq, Eq)]
pub struct SlotSet(Box<[u64; 256]>);

impl SlotSet {
    pub fn new() -> Self {
        SlotSet(Box::new([0u64; WORDS]))
    }

    /// The set of all 16384 slots.
    pub fn full() -> Self {
        SlotSet(Box::new([u64::MAX; WORDS]))
    }

    #[inline]
    pub fn insert(&mut self, s: u16) {
        self.0[usize::from(s >> 6)] |= 1u64 << (s & 63);
    }

    #[inline]
    pub fn remove(&mut self, s: u16) {
        self.0[usize::from(s >> 6)] &= !(1u64 << (s & 63));
    }

    #[inline]
    pub fn contains(&self, s: u16) -> bool {
        self.0[usize::from(s >> 6)] & (1u64 << (s & 63)) != 0
    }

    /// Inserts every slot of the inclusive range `lo..=hi` (nothing when `lo > hi`).
    pub fn insert_range(&mut self, lo: u16, hi: u16) {
        self.set_range(lo, hi, true);
    }

    /// Removes every slot of the inclusive range `lo..=hi` (nothing when `lo > hi`).
    pub fn remove_range(&mut self, lo: u16, hi: u16) {
        self.set_range(lo, hi, false);
    }

    fn set_range(&mut self, lo: u16, hi: u16, on: bool) {
        assert!(usize::from(hi) < SLOTS || lo > hi, "slot {hi} out of range");
        if lo > hi {
            return;
        }
        let (lo, hi) = (usize::from(lo), usize::from(hi));
        for w in lo / 64..=hi / 64 {
            let from = if w == lo / 64 { lo % 64 } else { 0 };
            let to = if w == hi / 64 { hi % 64 } else { 63 };
            // bits from..=to
            let mask = (u64::MAX >> (63 - to)) & (u64::MAX << from);
            if on {
                self.0[w] |= mask;
            } else {
                self.0[w] &= !mask;
            }
        }
    }

    /// Removes every slot.
    pub fn clear(&mut self) {
        self.0.fill(0);
    }

    /// First contained slot `>= from`.
    fn next_set(&self, from: usize) -> Option<usize> {
        let mut w = from / 64;
        if w >= WORDS {
            return None;
        }
        let mut bits = self.0[w] & (u64::MAX << (from % 64));
        loop {
            if bits != 0 {
                return Some(w * 64 + bits.trailing_zeros() as usize);
            }
            w += 1;
            if w == WORDS {
                return None;
            }
            bits = self.0[w];
        }
    }

    /// First slot `>= from` that is not contained (`SLOTS` if none).
    fn next_clear(&self, from: usize) -> usize {
        let mut w = from / 64;
        if w >= WORDS {
            return SLOTS;
        }
        let mut bits = !self.0[w] & (u64::MAX << (from % 64));
        loop {
            if bits != 0 {
                return w * 64 + bits.trailing_zeros() as usize;
            }
            w += 1;
            if w == WORDS {
                return SLOTS;
            }
            bits = !self.0[w];
        }
    }

    /// Maximal runs of contained slots as inclusive `(first, last)` pairs, ascending.
    pub fn ranges(&self) -> Vec<(u16, u16)> {
        let mut out = Vec::new();
        let mut from = 0;
        while let Some(lo) = self.next_set(from) {
            let end = self.next_clear(lo);
            out.push((lo as u16, (end - 1) as u16));
            from = end;
        }
        out
    }

    /// Contained slots in ascending order.
    pub fn iter(&self) -> impl Iterator<Item = u16> + '_ {
        self.0.iter().enumerate().flat_map(|(w, &word)| {
            let mut bits = word;
            core::iter::from_fn(move || {
                if bits == 0 {
                    return None;
                }
                let b = bits.trailing_zeros();
                bits &= bits - 1;
                Some((w * 64) as u16 + b as u16)
            })
        })
    }

    pub fn len(&self) -> usize {
        self.0.iter().map(|w| w.count_ones() as usize).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.0.iter().all(|&w| w == 0)
    }

    /// The raw bitmap words (slot `s` is bit `s % 64` of word `s / 64`); used by wire codecs.
    pub fn words(&self) -> &[u64; 256] {
        &self.0
    }

    /// Builds a set from raw bitmap words (inverse of [`SlotSet::words`]).
    pub fn from_words(words: [u64; 256]) -> Self {
        SlotSet(Box::new(words))
    }
}

impl Default for SlotSet {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for SlotSet {
    /// Slot ranges, e.g. `SlotSet[0-5460, 7000]`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SlotSet[")?;
        for (i, (lo, hi)) in self.ranges().into_iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            if lo == hi {
                write!(f, "{lo}")?;
            } else {
                write!(f, "{lo}-{hi}")?;
            }
        }
        f.write_str("]")
    }
}

impl FromIterator<u16> for SlotSet {
    fn from_iter<I: IntoIterator<Item = u16>>(iter: I) -> Self {
        let mut s = SlotSet::new();
        for slot in iter {
            s.insert(slot);
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Bit-at-a-time reference implementation.
    fn crc16_ref(data: &[u8]) -> u16 {
        let mut crc = 0u16;
        for &b in data {
            crc ^= u16::from(b) << 8;
            for _ in 0..8 {
                crc = if crc & 0x8000 != 0 { (crc << 1) ^ 0x1021 } else { crc << 1 };
            }
        }
        crc
    }

    #[test]
    fn crc16_vectors() {
        assert_eq!(crc16_xmodem(b"123456789"), 0x31C3);
        assert_eq!(crc16_xmodem(b""), 0);
        assert_eq!(crc16_xmodem(b"A"), 0x58E5);
        for s in [&b"foo"[..], b"bar", b"\x00\xff", b"hello world"] {
            assert_eq!(crc16_xmodem(s), crc16_ref(s));
        }
    }

    #[test]
    fn key_slots_match_redis_cluster() {
        // Values from `CLUSTER KEYSLOT` on Redis 7.0.
        assert_eq!(key_slot(b"foo"), 12182);
        assert_eq!(key_slot(b"bar"), 5061);
        assert_eq!(key_slot(b"hello"), 866);
        assert_eq!(key_slot(b""), 0);
        assert_eq!(key_slot(b"{user1000}.following"), key_slot(b"user1000"));
        assert_eq!(key_slot(b"{user1000}.followers"), key_slot(b"{user1000}.following"));
        assert_eq!(key_slot(b"foo{}{bar}"), key_slot(b"foo{}{bar}")); // whole key hashed
        assert_eq!(key_slot(b"foo{{bar}}zap"), key_slot(b"{bar"));
        assert_eq!(key_slot(b"foo{bar}{zap}"), key_slot(b"bar"));
    }

    #[test]
    fn hashtag_rule() {
        assert_eq!(hash_tag(b"{user1000}.following"), b"user1000");
        assert_eq!(hash_tag(b"foo{}{bar}"), b"foo{}{bar}"); // first tag empty: whole key
        assert_eq!(hash_tag(b"foo{{bar}}zap"), b"{bar");
        assert_eq!(hash_tag(b"foo{bar}{zap}"), b"bar");
        assert_eq!(hash_tag(b"foo"), b"foo");
        assert_eq!(hash_tag(b"foo{bar"), b"foo{bar"); // no closing brace
        assert_eq!(hash_tag(b"foo}bar{"), b"foo}bar{");
        assert_eq!(hash_tag(b"}{x}"), b"x");
        assert_eq!(hash_tag(b"{}"), b"{}");
        assert_eq!(hash_tag(b"{"), b"{");
        assert_eq!(hash_tag(b""), b"");
        assert_eq!(hash_tag(b"a{\x00}b"), b"\x00");
    }

    #[test]
    fn slot_set_basics() {
        let mut s = SlotSet::new();
        assert!(s.is_empty());
        assert_eq!(s.ranges(), vec![]);
        s.insert(0);
        s.insert(1);
        s.insert(2);
        s.insert(63);
        s.insert(64);
        s.insert(16383);
        assert_eq!(s.len(), 6);
        assert!(s.contains(63) && s.contains(64) && !s.contains(65));
        assert_eq!(s.ranges(), vec![(0, 2), (63, 64), (16383, 16383)]);
        s.remove(1);
        assert_eq!(s.ranges(), vec![(0, 0), (2, 2), (63, 64), (16383, 16383)]);
        assert_eq!(s.iter().collect::<Vec<_>>(), vec![0, 2, 63, 64, 16383]);
        assert_eq!(format!("{s:?}"), "SlotSet[0, 2, 63-64, 16383]");
        assert_eq!(SlotSet::full().ranges(), vec![(0, 16383)]);
        assert_eq!(SlotSet::full().len(), SLOTS);
        let mut r = SlotSet::new();
        r.insert_range(5, 200);
        r.insert_range(10, 3); // empty
        assert_eq!(r.ranges(), vec![(5, 200)]);
        r.remove_range(64, 127);
        assert_eq!(r.ranges(), vec![(5, 63), (128, 200)]);
        r.insert_range(0, 16383);
        assert_eq!(r, SlotSet::full());
        r.clear();
        assert!(r.is_empty());
        let words = *SlotSet::full().words();
        assert_eq!(SlotSet::from_words(words), SlotSet::full());
    }

    proptest! {
        #[test]
        fn crc16_table_equals_bitwise(data in proptest::collection::vec(any::<u8>(), 0..64)) {
            prop_assert_eq!(crc16_xmodem(&data), crc16_ref(&data));
        }

        #[test]
        fn slot_set_matches_model(ops in proptest::collection::vec((any::<bool>(), 0u16..16384, 0u16..300), 0..60)) {
            let mut s = SlotSet::new();
            let mut model = vec![false; SLOTS];
            for (ins, lo, n) in ops {
                let hi = lo.saturating_add(n).min(16383);
                if ins { s.insert_range(lo, hi) } else { s.remove_range(lo, hi) }
                for x in lo..=hi { model[usize::from(x)] = ins; }
            }
            let mut ranges = Vec::new();
            let mut i = 0;
            while i < SLOTS {
                if model[i] {
                    let lo = i;
                    while i < SLOTS && model[i] { i += 1; }
                    ranges.push((lo as u16, (i - 1) as u16));
                } else {
                    i += 1;
                }
            }
            prop_assert_eq!(s.ranges(), ranges);
            prop_assert_eq!(s.len(), model.iter().filter(|&&b| b).count());
            let collected: SlotSet = s.iter().collect();
            prop_assert_eq!(collected, s);
        }
    }
}
