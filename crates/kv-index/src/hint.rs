//! TTL hints (DESIGN §9.5).
//!
//! `hint: u16`: 0 = no TTL; else `scale = hint >> 14`, `v = hint & 0x3FFF`, and the lower bound
//! of `expire_at` in unix seconds is: scale 0: `base + v - 1` (1 s units, ~4.5 h), scale 1:
//! `base + v * 16` (~73 h), scale 2: `base + v * 256` (~48 d), scale 3: `base + v * 4096`
//! (~776 d; `0x3FFF` at scale 3 means "far", still a lower bound). `base` is the partition's
//! `hint_base_s` (`now_s - 60` at startup and every hour). Encoding always floors, so
//! `decode_lb_s(encode(t)) <= t / 1000`. Active expiry candidates satisfy
//! `decode * 1000 <= now_ms` (§16.4).
//!
//! The encoder is monotonic: `t1 <= t2` implies `encode(t1) <= encode(t2)` as plain `u16`
//! values, and each hint value it produces stands for an interval of expiry seconds that starts
//! at the hint's lower bound. Scale 0 is therefore used up to offset 16 367 s (not 16 382 s), so
//! the first scale-1 value (`v = 1023`, lower bound `base + 16 368`) is above every scale-0 value.
//! With that property the active-expiry test is a single unsigned compare per entry against
//! [`now_code`]: `h != 0 && h <= now_code(now_ms, base)` holds exactly when
//! `decode_lb_s(h, base) * 1000 <= now_ms` (for hints made by [`encode`] and `now_ms >= base`).
//! That is what lets the RAM sweep compare whole hint planes with SIMD (§16.4).
//!
//! An expiry earlier than `base` encodes as the smallest hint (lower bound `base`); since `base`
//! never exceeds the current time, such a key is an expiry candidate immediately.

/// Largest offset (seconds above base) stored at 1 s resolution.
const SCALE0_MAX: u64 = 16_367;
const V_MAX: u64 = 0x3FFF;
const SCALE1_UNIT: u64 = 16;
const SCALE2_UNIT: u64 = 256;
const SCALE3_UNIT: u64 = 4096;

/// Encodes an absolute expiry (unix ms) relative to `base_s`; `None` = no TTL (hint 0).
pub fn encode(expire_at_ms: Option<u64>, base_s: u64) -> u16 {
    let Some(ms) = expire_at_ms else { return 0 };
    let d = (ms / 1000).saturating_sub(base_s);
    let (scale, v) = if d <= SCALE0_MAX {
        (0, d + 1)
    } else if d / SCALE1_UNIT <= V_MAX {
        (1, d / SCALE1_UNIT)
    } else if d / SCALE2_UNIT <= V_MAX {
        (2, d / SCALE2_UNIT)
    } else {
        (3, (d / SCALE3_UNIT).min(V_MAX))
    };
    ((scale << 14) | v) as u16
}

/// Lower bound of the expiry in unix seconds, `None` for hint 0.
pub fn decode_lb_s(h: u16, base_s: u64) -> Option<u64> {
    if h == 0 {
        return None;
    }
    let v = u64::from(h & 0x3FFF);
    Some(match h >> 14 {
        // v >= 1 here because h != 0
        0 => base_s + v - 1,
        1 => base_s + v * SCALE1_UNIT,
        2 => base_s + v * SCALE2_UNIT,
        _ => base_s + v * SCALE3_UNIT,
    })
}

/// The hint value of "now": an entry with hint `h` is an active-expiry candidate
/// (`decode_lb_s(h) * 1000 <= now_ms`) exactly when `h != 0 && h <= now_code(now_ms, base_s)`.
/// This is the `now_lb` argument of [`crate::SlotTable::expiry_candidates`].
pub fn now_code(now_ms: u64, base_s: u64) -> u16 {
    encode(Some(now_ms), base_s)
}

/// Re-encodes a hint made against `old_base_s` against `new_base_s` (the hourly rebase, §9.5).
/// The result is still a lower bound of the key's expiry. Use with
/// [`crate::SlotTable::rehint`].
pub fn rebase(h: u16, old_base_s: u64, new_base_s: u64) -> u16 {
    match decode_lb_s(h, old_base_s) {
        None => 0,
        Some(lb_s) => encode(Some(lb_s.saturating_mul(1000)), new_base_s),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const BASE: u64 = 1_790_000_000;

    fn ms(off_s: u64) -> Option<u64> {
        Some((BASE + off_s) * 1000)
    }

    #[test]
    fn golden_values() {
        assert_eq!(encode(None, BASE), 0);
        assert_eq!(decode_lb_s(0, BASE), None);
        let cases: [(u64, u16, u64); 12] = [
            // (offset seconds, hint, decoded lower bound offset)
            (0, 0x0001, 0),
            (5, 0x0006, 5),
            (16_367, 0x3FF0, 16_367),
            (16_368, 0x43FF, 16_368),
            (16_383, 0x43FF, 16_368),
            (16_384, 0x4400, 16_384),
            (262_143, 0x7FFF, 262_128),
            (262_144, 0x8400, 262_144),
            (4_194_303, 0xBFFF, 4_194_048),
            (4_194_304, 0xC400, 4_194_304),
            (16_383 * 4096, 0xFFFF, 16_383 * 4096),
            (u64::MAX / 2000, 0xFFFF, 16_383 * 4096),
        ];
        for (off, hint, lb) in cases {
            let t = (BASE + off).saturating_mul(1000);
            assert_eq!(encode(Some(t), BASE), hint, "offset {off}");
            assert_eq!(decode_lb_s(hint, BASE), Some(BASE + lb), "offset {off}");
        }
        // sub-second part is floored
        assert_eq!(encode(Some(BASE * 1000 + 999), BASE), 1);
        // expiries before base: smallest hint, a candidate as soon as now >= base
        assert_eq!(encode(Some(1000), BASE), 1);
        assert!(1 <= now_code(BASE * 1000, BASE));
    }

    #[test]
    fn ranges_per_scale() {
        // ~4.5 h, ~73 h, ~48 d, ~776 d as documented
        assert_eq!(decode_lb_s(0x3FFF, 0), Some(16_382));
        assert_eq!(decode_lb_s(0x7FFF, 0), Some(16_383 * 16));
        assert_eq!(decode_lb_s(0xBFFF, 0), Some(16_383 * 256));
        assert_eq!(decode_lb_s(0xFFFF, 0), Some(16_383 * 4096));
    }

    #[test]
    fn rebase_keeps_lower_bound() {
        let t = ms(100_000).unwrap();
        let h = encode(Some(t), BASE);
        let h2 = rebase(h, BASE, BASE + 3600);
        let lb2 = decode_lb_s(h2, BASE + 3600).unwrap();
        assert!(lb2 * 1000 <= t);
        assert!(lb2 >= decode_lb_s(h, BASE).unwrap());
        assert_eq!(rebase(0, BASE, BASE + 3600), 0);
    }

    proptest! {
        #[test]
        fn lower_bound(off in 0u64..80_000_000, sub in 0u64..1000) {
            let t = (BASE + off) * 1000 + sub;
            let h = encode(Some(t), BASE);
            prop_assert_ne!(h, 0);
            let lb = decode_lb_s(h, BASE).unwrap();
            prop_assert!(lb * 1000 <= t);
            // tight up to the scale's unit (except "far")
            let unit = match h >> 14 { 0 => 1, 1 => 16, 2 => 256, _ => 4096 };
            if h != 0xFFFF {
                prop_assert!(t / 1000 - lb < unit);
            }
        }

        #[test]
        fn monotonic(a in 0u64..80_000_000, b in 0u64..80_000_000) {
            let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
            prop_assert!(encode(ms(lo), BASE) <= encode(ms(hi), BASE));
        }

        /// The single-compare candidate test equals the decoded comparison of §9.5.
        #[test]
        fn candidate_compare_exact(exp in 0u64..80_000_000, now in 0u64..80_000_000, sub in 0u64..1000) {
            let h = encode(ms(exp), BASE);
            let now_ms = (BASE + now) * 1000 + sub;
            let by_code = h != 0 && h <= now_code(now_ms, BASE);
            let by_decode = decode_lb_s(h, BASE).unwrap() * 1000 <= now_ms;
            prop_assert_eq!(by_code, by_decode);
        }
    }
}
