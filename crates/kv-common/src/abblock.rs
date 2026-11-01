//! A/B fixed-location metadata blocks (DESIGN §10.2, §17.6).
//!
//! Layout of every 4096-byte block: `0x000 u64 magic`, `0x008 u32 crc32c over [0x00C, 0x1000)`,
//! `0x00C u32 version (1)`, `0x010 u64 block_seq`, `0x018` payload (4072 bytes). Each block has
//! two copies; the writer always writes the copy that does not hold the newest valid state and
//! never writes both copies of one block in the same batch, so a torn write damages only the
//! copy being written. Readers take the valid copy with the higher `block_seq`.

use crate::crc::crc32c;

/// Size of one block copy.
pub const AB_BLOCK: usize = 4096;
/// Offset of the payload inside a block.
pub const AB_PAYLOAD_OFF: usize = 0x18;
/// Maximum payload length.
pub const AB_PAYLOAD_MAX: usize = AB_BLOCK - AB_PAYLOAD_OFF;
/// The only block format version.
pub const AB_VERSION: u32 = 1;

const CRC_OFF: usize = 0x08;
const CRC_FROM: usize = 0x0C;
const VERSION_OFF: usize = 0x0C;
const SEQ_OFF: usize = 0x10;

/// Little-endian `u64` of an 8-byte ASCII tag such as `b"KVSSDSB1"`.
pub const fn magic(tag: &[u8; 8]) -> u64 {
    u64::from_le_bytes(*tag)
}

/// Which copy of an A/B pair.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AbCopy {
    A,
    B,
}

impl AbCopy {
    /// The other copy of the pair.
    pub fn other(self) -> Self {
        match self {
            AbCopy::A => AbCopy::B,
            AbCopy::B => AbCopy::A,
        }
    }

    /// 0 for A, 1 for B (the copy's index in its pair of locations).
    pub fn index(self) -> usize {
        match self {
            AbCopy::A => 0,
            AbCopy::B => 1,
        }
    }
}

/// Encodes `payload` (at most [`AB_PAYLOAD_MAX`] bytes, zero padded) into `out`.
///
/// # Panics
/// If `payload` is longer than [`AB_PAYLOAD_MAX`] (a caller bug: every block layout is fixed).
pub fn ab_encode(magic: u64, block_seq: u64, payload: &[u8], out: &mut [u8; AB_BLOCK]) {
    assert!(
        payload.len() <= AB_PAYLOAD_MAX,
        "AbBlock payload of {} bytes exceeds {AB_PAYLOAD_MAX}",
        payload.len()
    );
    out[..8].copy_from_slice(&magic.to_le_bytes());
    out[VERSION_OFF..VERSION_OFF + 4].copy_from_slice(&AB_VERSION.to_le_bytes());
    out[SEQ_OFF..SEQ_OFF + 8].copy_from_slice(&block_seq.to_le_bytes());
    out[AB_PAYLOAD_OFF..AB_PAYLOAD_OFF + payload.len()].copy_from_slice(payload);
    out[AB_PAYLOAD_OFF + payload.len()..].fill(0);
    let crc = crc32c(&out[CRC_FROM..]);
    out[CRC_OFF..CRC_OFF + 4].copy_from_slice(&crc.to_le_bytes());
}

/// Validates length (exactly [`AB_BLOCK`]), magic, version and crc; returns
/// `(block_seq, payload)` where `payload` is the whole 4072-byte payload area.
pub fn ab_decode(magic: u64, block: &[u8]) -> Option<(u64, &[u8])> {
    if block.len() != AB_BLOCK {
        return None;
    }
    let u32_at = |o: usize| u32::from_le_bytes(block[o..o + 4].try_into().expect("4 bytes"));
    let u64_at = |o: usize| u64::from_le_bytes(block[o..o + 8].try_into().expect("8 bytes"));
    if u64_at(0) != magic || u32_at(VERSION_OFF) != AB_VERSION {
        return None;
    }
    if u32_at(CRC_OFF) != crc32c(&block[CRC_FROM..]) {
        return None;
    }
    Some((u64_at(SEQ_OFF), &block[AB_PAYLOAD_OFF..]))
}

/// Picks the valid copy with the higher `block_seq`. Equal sequence numbers cannot be produced
/// by a correct writer; the tie goes to A so the choice stays deterministic.
pub fn ab_pick<'a>(
    a: Option<(u64, &'a [u8])>,
    b: Option<(u64, &'a [u8])>,
) -> Option<(AbCopy, u64, &'a [u8])> {
    match (a, b) {
        (Some((sa, pa)), Some((sb, pb))) => {
            if sb > sa {
                Some((AbCopy::B, sb, pb))
            } else {
                Some((AbCopy::A, sa, pa))
            }
        }
        (Some((sa, pa)), None) => Some((AbCopy::A, sa, pa)),
        (None, Some((sb, pb))) => Some((AbCopy::B, sb, pb)),
        (None, None) => None,
    }
}

/// The copy to overwrite next and the `block_seq` to give it, from the currently newest valid
/// copy (`None` when neither copy is valid, e.g. right after format): always the other copy,
/// with the next sequence number. Neither the copy nor the number depends on anything but the
/// last valid state, so a torn write is simply retried into the same copy.
pub fn ab_next_write(newest: Option<(AbCopy, u64)>) -> (AbCopy, u64) {
    match newest {
        Some((copy, seq)) => (copy.other(), seq + 1),
        None => (AbCopy::A, 1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const M: u64 = magic(b"KVTEST01");

    fn block(seq: u64, payload: &[u8]) -> Box<[u8; AB_BLOCK]> {
        let mut b = Box::new([0xEEu8; AB_BLOCK]);
        ab_encode(M, seq, payload, &mut b);
        b
    }

    #[test]
    fn layout_is_byte_exact() {
        let b = block(0x0102_0304_0506_0708, b"hello");
        assert_eq!(&b[0..8], b"KVTEST01");
        assert_eq!(&b[0x0C..0x10], &1u32.to_le_bytes());
        assert_eq!(&b[0x10..0x18], &0x0102_0304_0506_0708u64.to_le_bytes());
        assert_eq!(&b[0x18..0x1D], b"hello");
        assert!(
            b[0x1D..].iter().all(|&x| x == 0),
            "payload area is zero padded"
        );
        assert_eq!(&b[8..12], &crc32c(&b[0x0C..]).to_le_bytes());
    }

    #[test]
    fn round_trip() {
        let b = block(7, b"payload");
        let (seq, p) = ab_decode(M, &b[..]).unwrap();
        assert_eq!(seq, 7);
        assert_eq!(p.len(), AB_PAYLOAD_MAX);
        assert_eq!(&p[..7], b"payload");
        let full = vec![0x5A; AB_PAYLOAD_MAX];
        let b = block(1, &full);
        assert_eq!(ab_decode(M, &b[..]).unwrap().1, &full[..]);
    }

    #[test]
    fn rejects_damage() {
        let b = block(7, b"payload");
        assert!(
            ab_decode(magic(b"KVOTHER1"), &b[..]).is_none(),
            "wrong magic"
        );
        assert!(ab_decode(M, &b[..AB_BLOCK - 1]).is_none(), "short");
        let mut long = b.to_vec();
        long.push(0);
        assert!(ab_decode(M, &long).is_none(), "long");
        // every single-byte change anywhere is detected
        for off in (0..AB_BLOCK)
            .step_by(97)
            .chain([0x08, 0x0C, 0x10, 0x18, AB_BLOCK - 1])
        {
            let mut d = b.clone();
            d[off] ^= 0x01;
            assert!(ab_decode(M, &d[..]).is_none(), "flip at {off:#x}");
        }
        let mut v2 = b.clone();
        v2[0x0C] = 2;
        let crc = crc32c(&v2[0x0C..]);
        v2[8..12].copy_from_slice(&crc.to_le_bytes());
        assert!(ab_decode(M, &v2[..]).is_none(), "unknown version");
        assert!(ab_decode(M, &[0u8; AB_BLOCK]).is_none(), "zeroed block");
    }

    #[test]
    fn torn_copy_falls_back_to_the_other() {
        let a = block(10, b"old state");
        let mut b = block(11, b"new state");
        // a torn write: the tail of copy B still holds old bytes
        b[2000..].fill(0xEE);
        let pick = ab_pick(ab_decode(M, &a[..]), ab_decode(M, &b[..])).unwrap();
        assert_eq!((pick.0, pick.1), (AbCopy::A, 10));
        assert_eq!(&pick.2[..9], b"old state");
        // the next write goes to B again, with the next sequence number
        assert_eq!(ab_next_write(Some((pick.0, pick.1))), (AbCopy::B, 11));
    }

    #[test]
    fn pick_rules() {
        let p: &[u8] = b"x";
        assert_eq!(
            ab_pick(Some((3, p)), Some((4, p))).map(|x| (x.0, x.1)),
            Some((AbCopy::B, 4))
        );
        assert_eq!(
            ab_pick(Some((5, p)), Some((4, p))).map(|x| (x.0, x.1)),
            Some((AbCopy::A, 5))
        );
        assert_eq!(
            ab_pick(Some((5, p)), Some((5, p))).map(|x| (x.0, x.1)),
            Some((AbCopy::A, 5))
        );
        assert_eq!(
            ab_pick(None, Some((1, p))).map(|x| (x.0, x.1)),
            Some((AbCopy::B, 1))
        );
        assert_eq!(
            ab_pick(Some((1, p)), None).map(|x| (x.0, x.1)),
            Some((AbCopy::A, 1))
        );
        assert!(ab_pick(None, None).is_none());
        assert_eq!(ab_next_write(None), (AbCopy::A, 1));
        assert_eq!(ab_next_write(Some((AbCopy::A, 1))), (AbCopy::B, 2));
        assert_eq!(AbCopy::A.other(), AbCopy::B);
        assert_eq!(AbCopy::B.index(), 1);
    }

    #[test]
    #[should_panic(expected = "exceeds")]
    fn oversized_payload_panics() {
        let mut out = [0u8; AB_BLOCK];
        ab_encode(M, 1, &[0u8; AB_PAYLOAD_MAX + 1], &mut out);
    }
}
