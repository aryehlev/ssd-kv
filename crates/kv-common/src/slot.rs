//! Hash slots (DESIGN §5, §6.2, §23.14).
//!
//! `slot = CRC16-XMODEM(tag or key) % 16384`, where the tag is the bytes between the first `{`
//! and the next `}` when non-empty (Redis Cluster rule). Test vector:
//! `crc16_xmodem(b"123456789") == 0x31C3`. [`SlotSet`] is a fixed 2 KiB bitmap of slots, used for
//! owned/served slot sets (§23.4, §23.6).

/// Number of hash slots.
pub const SLOTS: usize = 16384;

/// CRC16-XMODEM (polynomial 0x1021, init 0), as used by Redis Cluster.
pub fn crc16_xmodem(data: &[u8]) -> u16 {
    todo!("DESIGN §6.2: CRC16-XMODEM")
}

/// Redis hashtag rule: the bytes between the first `{` and the next `}` if that range is
/// non-empty, else the whole key.
pub fn hash_tag(key: &[u8]) -> &[u8] {
    todo!("DESIGN §23.14: hashtag rule")
}

/// `crc16(hash_tag(key)) & 16383`.
pub fn key_slot(key: &[u8]) -> u16 {
    crc16_xmodem(hash_tag(key)) & (SLOTS as u16 - 1)
}

/// Bitmap of the 16384 hash slots (2 KiB).
#[derive(Clone, PartialEq, Eq)]
pub struct SlotSet(Box<[u64; 256]>);

impl SlotSet {
    pub fn new() -> Self {
        SlotSet(Box::new([0u64; 256]))
    }

    pub fn insert(&mut self, s: u16) {
        self.0[usize::from(s >> 6)] |= 1u64 << (s & 63);
    }

    pub fn remove(&mut self, s: u16) {
        self.0[usize::from(s >> 6)] &= !(1u64 << (s & 63));
    }

    pub fn contains(&self, s: u16) -> bool {
        self.0[usize::from(s >> 6)] & (1u64 << (s & 63)) != 0
    }

    /// Maximal runs of contained slots as inclusive `(first, last)` pairs, ascending.
    pub fn ranges(&self) -> Vec<(u16, u16)> {
        todo!("DESIGN §23.5: slot ranges")
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
