//! AG table codec (DESIGN §10.4; AbBlock magic `KVAGTBL1`).
//!
//! Payload: `0x018 u32 block_index`, `0x01C u16 n_entries (<= 2032)`, `0x01E u16 zero`,
//! `0x020 entries[2032] x {u8 kind (0 FREE, 1 SMALL, 2 EXTENT, 3 reserved), u8 owner_partition
//! (0xFF none)}`. Only the changed block is rewritten (A/B) when an AG is acquired, released or
//! changes kind; an AG changes kind or owner only when it holds no live record and no tracked
//! stale copy. The entry is durable before the first write into the AG (E11).
//!
//! AG `k` of a device is entry `k % 2032` of block `k / 2032` ([`AgTableBlock::locate`]).

use kv_common::abblock::AbCopy;

use crate::layout::{PAGE, ab_build, ab_parse, ab_select, le};

/// `KVAGTBL1`
pub const AGT_MAGIC: u64 = kv_common::abblock::magic(b"KVAGTBL1");
/// Entries per AG table block.
pub const AG_TABLE_ENTRIES: usize = crate::layout::AG_TABLE_ENTRIES as usize;
/// `owner_partition` of a FREE AG.
pub const OWNER_NONE: u8 = 0xFF;

/// Block offset of the first entry.
const ENTRIES_OFF: usize = 0x020;
const _: () = assert!(ENTRIES_OFF + 2 * AG_TABLE_ENTRIES == PAGE);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AgKind {
    Free,
    Small,
    Extent,
}

impl AgKind {
    /// On-media code (0 FREE, 1 SMALL, 2 EXTENT).
    pub fn code(self) -> u8 {
        match self {
            AgKind::Free => 0,
            AgKind::Small => 1,
            AgKind::Extent => 2,
        }
    }

    /// Decodes an on-media code; 3 (reserved) and above are invalid.
    pub fn from_code(c: u8) -> Option<Self> {
        match c {
            0 => Some(AgKind::Free),
            1 => Some(AgKind::Small),
            2 => Some(AgKind::Extent),
            _ => None,
        }
    }
}

/// One AG table block: `(kind, owner_partition)` per AG.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AgTableBlock {
    pub index: u32,
    pub entries: Vec<(AgKind, u8)>,
}

impl AgTableBlock {
    /// Block `index` of a device with `n_ags` AGs, every entry FREE.
    pub fn free(index: u32, n_ags: u32) -> Self {
        let first = index as usize * AG_TABLE_ENTRIES;
        let n = (n_ags as usize).saturating_sub(first).min(AG_TABLE_ENTRIES);
        AgTableBlock { index, entries: vec![(AgKind::Free, OWNER_NONE); n] }
    }

    /// `(block, entry)` of AG `ag`.
    pub fn locate(ag: u32) -> (u32, usize) {
        let per = AG_TABLE_ENTRIES as u32;
        (ag / per, (ag % per) as usize)
    }

    /// The AG index of entry `i` of this block.
    pub fn ag_of(&self, i: usize) -> u32 {
        self.index * AG_TABLE_ENTRIES as u32 + i as u32
    }

    /// Panics if more than 2032 entries are present (a caller bug: the block size is fixed).
    pub fn encode(&self, seq: u64, out: &mut [u8; PAGE]) {
        assert!(self.entries.len() <= AG_TABLE_ENTRIES, "AG table block holds at most 2032 entries");
        ab_build(AGT_MAGIC, seq, out, |b| {
            le::put_u32(b, 0x018, self.index);
            le::put_u16(b, 0x01C, self.entries.len() as u16);
            for (i, &(kind, owner)) in self.entries.iter().enumerate() {
                b[ENTRIES_OFF + 2 * i] = kind.code();
                b[ENTRIES_OFF + 2 * i + 1] = if kind == AgKind::Free { OWNER_NONE } else { owner };
            }
        });
    }

    /// Validates the AbBlock and returns `(block_seq, block)`.
    ///
    /// Rejects `n_entries > 2032`, reserved kinds and FREE entries with an owner.
    pub fn decode(b: &[u8]) -> Option<(u64, Self)> {
        ab_parse(AGT_MAGIC, b, |b| {
            let index = le::u32_at(b, 0x018);
            let n = usize::from(le::u16_at(b, 0x01C));
            if n > AG_TABLE_ENTRIES || le::u16_at(b, 0x01E) != 0 {
                return None;
            }
            let mut entries = Vec::with_capacity(n);
            for i in 0..n {
                let kind = AgKind::from_code(b[ENTRIES_OFF + 2 * i])?;
                let owner = b[ENTRIES_OFF + 2 * i + 1];
                if kind == AgKind::Free && owner != OWNER_NONE {
                    return None;
                }
                entries.push((kind, owner));
            }
            Some(AgTableBlock { index, entries })
        })
    }

    /// Reads both copies of one block and returns the valid one with the higher `block_seq`;
    /// a copy whose `block_index` differs from `index` is ignored (misdirected write).
    pub fn pick(index: u32, a: &[u8], b: &[u8]) -> Option<(AbCopy, u64, Self)> {
        let ok = |d: Option<(u64, Self)>| d.filter(|(_, t)| t.index == index);
        ab_select(ok(Self::decode(a)), ok(Self::decode(b)))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig { cases: 256, failure_persistence: None, ..ProptestConfig::default() })]

        #[test]
        fn prop_round_trip(
            index in any::<u32>(),
            entries in proptest::collection::vec((0u8..3, any::<u8>()), 0..=AG_TABLE_ENTRIES),
            seq in any::<u64>(),
        ) {
            let entries: Vec<(AgKind, u8)> = entries
                .into_iter()
                .map(|(k, o)| { let k = AgKind::from_code(k).unwrap(); (k, if k == AgKind::Free { OWNER_NONE } else { o }) })
                .collect();
            let t = AgTableBlock { index, entries };
            let mut b = [0u8; PAGE];
            t.encode(seq, &mut b);
            prop_assert_eq!(AgTableBlock::decode(&b), Some((seq, t)));
        }

        #[test]
        fn prop_random_blocks_never_decode(b in proptest::collection::vec(any::<u8>(), PAGE)) {
            prop_assert!(AgTableBlock::decode(&b).is_none());
        }
    }

    pub(crate) fn sample() -> AgTableBlock {
        let mut t = AgTableBlock::free(1, 2032 + 100);
        t.entries[0] = (AgKind::Small, 3);
        t.entries[1] = (AgKind::Extent, 3);
        t.entries[99] = (AgKind::Small, 127);
        t
    }

    #[test]
    fn locate_and_free() {
        assert_eq!(AgTableBlock::locate(0), (0, 0));
        assert_eq!(AgTableBlock::locate(2031), (0, 2031));
        assert_eq!(AgTableBlock::locate(2032), (1, 0));
        assert_eq!(AgTableBlock::locate(5000), (2, 936));
        assert_eq!(AgTableBlock::free(0, 10).entries.len(), 10);
        assert_eq!(AgTableBlock::free(0, 5000).entries.len(), 2032);
        assert_eq!(AgTableBlock::free(2, 5000).entries.len(), 936);
        assert_eq!(AgTableBlock::free(3, 5000).entries.len(), 0);
        assert_eq!(sample().ag_of(99), 2032 + 99);
    }

    #[test]
    fn round_trip() {
        let t = sample();
        let mut b = [0u8; PAGE];
        t.encode(9, &mut b);
        assert_eq!(&b[..8], b"KVAGTBL1");
        assert_eq!(le::u32_at(&b, 0x18), 1);
        assert_eq!(le::u16_at(&b, 0x1C), 100);
        assert_eq!(&b[0x20..0x24], &[1, 3, 2, 3]);
        assert_eq!(&b[0x24..0x26], &[0, 0xFF]);
        assert_eq!(&b[0x20 + 2 * 99..0x20 + 200], &[1, 127]);
        assert!(le::zero(&b, 0x20 + 200..PAGE));
        assert_eq!(AgTableBlock::decode(&b), Some((9, t.clone())));
        let full = AgTableBlock { index: 0, entries: vec![(AgKind::Extent, 5); AG_TABLE_ENTRIES] };
        full.encode(1, &mut b);
        assert_eq!(AgTableBlock::decode(&b), Some((1, full)));
    }

    #[test]
    fn decode_rejects_invalid() {
        let mut b = [0u8; PAGE];
        // reserved kind 3
        let mut t = sample();
        t.encode(1, &mut b);
        let mut payload = [0u8; PAGE];
        payload.copy_from_slice(&b);
        payload[0x20] = 3;
        let mut re = [0u8; PAGE];
        kv_common::abblock::ab_encode(AGT_MAGIC, 1, &payload[0x18..], &mut re);
        assert!(AgTableBlock::decode(&re).is_none());
        // FREE with an owner
        payload[0x20] = 0;
        payload[0x21] = 4;
        kv_common::abblock::ab_encode(AGT_MAGIC, 1, &payload[0x18..], &mut re);
        assert!(AgTableBlock::decode(&re).is_none());
        // n_entries too large
        let mut payload = [0u8; PAGE];
        le::put_u16(&mut payload, 0x1C, 2033);
        kv_common::abblock::ab_encode(AGT_MAGIC, 1, &payload[0x18..], &mut re);
        assert!(AgTableBlock::decode(&re).is_none());
        // wrong magic
        t.index = 0;
        t.encode(1, &mut b);
        b[0] ^= 1;
        assert!(AgTableBlock::decode(&b).is_none());
    }

    #[test]
    fn pick_checks_index_and_seq() {
        let t1 = sample();
        let mut t2 = sample();
        t2.entries[5] = (AgKind::Small, 9);
        let (mut a, mut b) = ([0u8; PAGE], [0u8; PAGE]);
        t1.encode(4, &mut a);
        t2.encode(5, &mut b);
        assert_eq!(AgTableBlock::pick(1, &a, &b).map(|(c, s, t)| (c, s, t.entries[5])), Some((AbCopy::B, 5, (AgKind::Small, 9))));
        assert_eq!(AgTableBlock::pick(0, &a, &b), None);
        b[100] ^= 1;
        assert_eq!(AgTableBlock::pick(1, &a, &b).map(|(c, s, _)| (c, s)), Some((AbCopy::A, 4)));
    }
}
