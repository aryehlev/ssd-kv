//! TTL hints (DESIGN §9.5).
//!
//! `hint: u16`: 0 = no TTL; else `scale = hint >> 14`, `v = hint & 0x3FFF`, and the lower bound
//! of `expire_at` in unix seconds is: scale 0: `base + v - 1` (1 s units, ~4.5 h), scale 1:
//! `base + v * 16` (~73 h), scale 2: `base + v * 256` (~48 d), scale 3: `base + v * 4096`
//! (~776 d; `0x3FFF` at scale 3 means "far", still a lower bound). `base` is the partition's
//! `hint_base_s` (`now_s - 60` at startup and every hour). Encoding always floors, so
//! `decode_lb_s(encode(t)) <= t / 1000`. Active expiry candidates satisfy
//! `decode * 1000 <= now_ms` (§16.4).

/// Encodes an absolute expiry (unix ms) relative to `base_s`; `None` = no TTL (hint 0).
pub fn encode(expire_at_ms: Option<u64>, base_s: u64) -> u16 {
    todo!("DESIGN §9.5: hint encode")
}

/// Lower bound of the expiry in unix seconds, `None` for hint 0.
pub fn decode_lb_s(h: u16, base_s: u64) -> Option<u64> {
    todo!("DESIGN §9.5: hint decode")
}
