//! SCAN cursor encoding (DESIGN §16.6).
//!
//! `cursor = partition:16 << 48 | slot_local:16 << 32 | fp32`, a single scalar position, with
//! `slot = slot_local * P + partition`. Cursor 0 is the start and the end of an iteration.

pub fn encode(partition: u16, slot_local: u16, fp: u32) -> u64 {
    (u64::from(partition) << 48) | (u64::from(slot_local) << 32) | u64::from(fp)
}

/// Inverse of [`encode`]: `(partition, slot_local, fp32)`.
pub fn decode(c: u64) -> (u16, u16, u32) {
    ((c >> 48) as u16, (c >> 32) as u16, c as u32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn golden() {
        assert_eq!(encode(0, 0, 0), 0);
        assert_eq!(encode(1, 0, 0), 0x0001_0000_0000_0000);
        assert_eq!(encode(63, 255, 0xDEAD_BEEF), 0x003F_00FF_DEAD_BEEF);
        assert_eq!(encode(u16::MAX, u16::MAX, u32::MAX), u64::MAX);
        assert_eq!(decode(0x003F_00FF_DEAD_BEEF), (63, 255, 0xDEAD_BEEF));
    }

    proptest! {
        #[test]
        fn roundtrip(p in any::<u16>(), l in any::<u16>(), fp in any::<u32>()) {
            prop_assert_eq!(decode(encode(p, l, fp)), (p, l, fp));
        }

        /// Cursor order is (partition, slot_local, fp) order.
        #[test]
        fn ordered(a in any::<(u16, u16, u32)>(), b in any::<(u16, u16, u32)>()) {
            prop_assert_eq!(encode(a.0, a.1, a.2).cmp(&encode(b.0, b.1, b.2)), a.cmp(&b));
        }
    }
}
