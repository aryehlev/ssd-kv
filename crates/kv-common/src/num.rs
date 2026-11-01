//! Redis-compatible number parsing and formatting (DESIGN §7.3, §8.1).
//!
//! * [`string2ll`]: Redis `string2ll` exactly (no `+`, no leading zeros, no spaces, `-0`
//!   invalid, full i64 range).
//! * [`parse_f64_strict`]: strict float parsing for commands that take doubles.
//! * [`fmt_double_17g`]: `%.17g`-compatible rendering of doubles for RESP replies (Redis 7.0
//!   `addReplyDouble`), including `inf`/`-inf`.

pub fn string2ll(s: &[u8]) -> Option<i64> {
    todo!("DESIGN §8.1: Redis string2ll")
}

/// Appends the decimal rendering of `v`.
pub fn ll2buf(v: i64, out: &mut Vec<u8>) {
    out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
}

pub fn parse_f64_strict(s: &[u8]) -> Option<f64> {
    todo!("DESIGN §8.1: strict double parsing")
}

/// Appends `v` formatted like C `printf("%.17g")`.
pub fn fmt_double_17g(v: f64, out: &mut Vec<u8>) {
    todo!("DESIGN §7.3: %.17g formatting")
}
