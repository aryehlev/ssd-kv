//! Redis-compatible number parsing and formatting (DESIGN §7.3, §8.1).
//!
//! * [`string2ll`] / [`string2l`]: Redis `string2ll` exactly (no `+`, no leading zeros, no
//!   spaces, `-0` invalid, full i64 range, at most 20 bytes).
//! * [`ll2string`] / [`ll2buf`] / [`ull2buf`]: decimal rendering.
//! * [`parse_scan_cursor`]: the SCAN cursor rule of Redis 7.0 (`strtoul` base 10).
//! * [`parse_f64_strict`]: Redis `string2d`, the strict double parsing of commands that take
//!   doubles (via the C shim's `strtod`, so hex floats and `inf` behave as in Redis).
//! * [`fmt_double_17g`]: `%.17g`-compatible rendering of doubles for RESP replies (Redis 7.0
//!   `addReplyDouble`), including `inf`/`-inf`.
//!
//! The parsing rules follow Redis <= 7.2 `util.c` (BSD-3-Clause, see NOTICE).

use core::fmt::Write as _;

/// Redis `LONG_STR_SIZE`: inputs of this length or more are never valid integers.
const LONG_STR_SIZE: usize = 21;

/// Redis `string2ll`: the whole input must be a canonical decimal i64 (`0`, or an optional `-`
/// followed by a non-zero digit and more digits), with no sign `+`, no leading zeros, no
/// spaces, `-0` refused, and fewer than 21 bytes.
#[inline]
pub fn string2ll(s: &[u8]) -> Option<i64> {
    let len = s.len();
    if len == 0 || len >= LONG_STR_SIZE {
        return None;
    }
    if len == 1 && s[0] == b'0' {
        return Some(0);
    }
    let (negative, digits) = match s.split_first() {
        Some((b'-', rest)) => (true, rest),
        _ => (false, s),
    };
    let (&first, rest) = digits.split_first()?;
    if !(b'1'..=b'9').contains(&first) {
        return None;
    }
    let mut v = u64::from(first - b'0');
    for &c in rest {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(u64::from(c - b'0'))?;
    }
    if negative {
        // up to 2^63, which is i64::MIN
        if v > i64::MAX as u64 + 1 {
            None
        } else {
            Some((v as i64).wrapping_neg())
        }
    } else if v > i64::MAX as u64 {
        None
    } else {
        Some(v as i64)
    }
}

/// Redis `string2l`: `string2ll` restricted to the C `long` range, which is the i64 range on the
/// LP64 targets kvssd supports.
#[inline]
pub fn string2l(s: &[u8]) -> Option<i64> {
    string2ll(s)
}

/// Redis `ll2string`: writes the decimal rendering of `v` into the start of `out` and returns its
/// length, or 0 (writing nothing) when `out` is too short (at most 20 bytes are needed). Unlike
/// the C function no terminating NUL is written.
pub fn ll2string(v: i64, out: &mut [u8]) -> usize {
    let mut b = itoa::Buffer::new();
    let s = b.format(v).as_bytes();
    match out.get_mut(..s.len()) {
        Some(dst) => {
            dst.copy_from_slice(s);
            s.len()
        }
        None => 0,
    }
}

/// Appends the decimal rendering of `v`.
#[inline]
pub fn ll2buf(v: i64, out: &mut Vec<u8>) {
    out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
}

/// Appends the decimal rendering of `v`.
#[inline]
pub fn ull2buf(v: u64, out: &mut Vec<u8>) {
    out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
}

/// C locale `isspace`.
#[inline]
fn is_c_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

/// The SCAN cursor rule of Redis 7.0 (`parseScanCursorOrReply`): `strtoul(arg, &end, 10)` over
/// the NUL-terminated argument, refused (`-ERR invalid cursor`) when the first byte is a space,
/// when parsing stops before the terminator, or on overflow. Hence: the empty string is cursor
/// 0, a leading `+` or `-` is accepted (`-n` wraps to `2^64 - n`, as `strtoul` negates), leading
/// zeros are accepted, and bytes after an embedded NUL are ignored.
pub fn parse_scan_cursor(s: &[u8]) -> Option<u64> {
    let s = match memchr_nul(s) {
        Some(n) => &s[..n],
        None => s,
    };
    let Some(&first) = s.first() else {
        return Some(0);
    };
    if is_c_space(first) {
        return None;
    }
    let (negative, digits) = match first {
        b'+' => (false, &s[1..]),
        b'-' => (true, &s[1..]),
        _ => (false, s),
    };
    if digits.is_empty() {
        return None;
    }
    let mut v = 0u64;
    for &c in digits {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(u64::from(c - b'0'))?;
    }
    Some(if negative { v.wrapping_neg() } else { v })
}

#[inline]
fn memchr_nul(s: &[u8]) -> Option<usize> {
    s.iter().position(|&b| b == 0)
}

/// Redis 7.0 `string2d` (strict double parsing, used by `getDoubleFromObject`): `strtod` over
/// the whole argument; refused when empty, starting with a space, not fully consumed (including
/// an embedded NUL), overflowing to infinity, underflowing to zero, or NaN. Accepts hex floats
/// (`0x1p-3`), `inf`/`infinity` (any case) and denormals.
pub fn parse_f64_strict(s: &[u8]) -> Option<f64> {
    crate::ldouble::string2d(s)
}

/// Longest output of [`fmt_double_17g_buf`] (`-1.2345678901234567e-308` is 24 bytes).
pub const DOUBLE_17G_MAX: usize = 32;

/// Fixed-capacity `fmt::Write` sink on the stack.
struct StackBuf<const N: usize> {
    buf: [u8; N],
    len: usize,
}

impl<const N: usize> core::fmt::Write for StackBuf<N> {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        let end = self.len + s.len();
        self.buf
            .get_mut(self.len..end)
            .ok_or(core::fmt::Error)?
            .copy_from_slice(s.as_bytes());
        self.len = end;
        Ok(())
    }
}

/// Writes `v` formatted like C `printf("%.17g")` into `out` and returns the length; infinities
/// are `inf`/`-inf` and NaN is `nan`/`-nan` (glibc's spellings).
pub fn fmt_double_17g_buf(v: f64, out: &mut [u8; DOUBLE_17G_MAX]) -> usize {
    let mut len = 0usize;
    let mut put = |bytes: &[u8]| {
        out[len..len + bytes.len()].copy_from_slice(bytes);
        len += bytes.len();
    };
    if v.is_sign_negative() {
        put(b"-");
    }
    if v.is_nan() {
        put(b"nan");
        return len;
    }
    if v.is_infinite() {
        put(b"inf");
        return len;
    }
    // Exactly 17 significant digits, correctly rounded (round half to even on the exact binary
    // value, like glibc): "d.dddddddddddddddde<exp>".
    let mut sci = StackBuf::<40> {
        buf: [0; 40],
        len: 0,
    };
    write!(sci, "{:.16e}", v.abs()).expect("40 bytes hold any {:.16e} rendering");
    let sci = &sci.buf[..sci.len];
    let e_at = sci
        .iter()
        .position(|&b| b == b'e')
        .expect("exponent marker");
    let mut digits = [0u8; 17];
    digits[0] = sci[0];
    digits[1..].copy_from_slice(&sci[2..e_at]);
    let exp: i32 = core::str::from_utf8(&sci[e_at + 1..])
        .ok()
        .and_then(|e| e.parse().ok())
        .expect("exponent");
    // %g drops trailing zeros of the fraction
    let sig = 17
        - digits
            .iter()
            .rev()
            .take_while(|&&d| d == b'0')
            .count()
            .min(16);
    if !(-4..17).contains(&exp) {
        put(&digits[..1]);
        if sig > 1 {
            put(b".");
            put(&digits[1..sig]);
        }
        put(if exp < 0 { b"e-" } else { b"e+" });
        let a = exp.unsigned_abs();
        if a < 10 {
            put(b"0");
        }
        put(itoa::Buffer::new().format(a).as_bytes());
    } else if exp >= 0 {
        let int_len = exp as usize + 1;
        put(&digits[..int_len]);
        if sig > int_len {
            put(b".");
            put(&digits[int_len..sig]);
        }
    } else {
        put(b"0.");
        for _ in 0..(-exp - 1) {
            put(b"0");
        }
        put(&digits[..sig]);
    }
    len
}

/// Appends `v` formatted like C `printf("%.17g")` (see [`fmt_double_17g_buf`]).
pub fn fmt_double_17g(v: f64, out: &mut Vec<u8>) {
    let mut b = [0u8; DOUBLE_17G_MAX];
    let n = fmt_double_17g_buf(v, &mut b);
    out.extend_from_slice(&b[..n]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn string2ll_table() {
        // (input, result) following Redis's util.c test_string2ll
        let cases: &[(&[u8], Option<i64>)] = &[
            (b"", None),
            (b" 1", None),
            (b"1 ", None),
            (b"+1", None),
            (b"-", None),
            (b"0", Some(0)),
            (b"-0", None),
            (b"00", None),
            (b"01", None),
            (b"-01", None),
            (b"1", Some(1)),
            (b"99", Some(99)),
            (b"-1", Some(-1)),
            (b"-99", Some(-99)),
            (b"1.5", None),
            (b"1e3", None),
            (b"0x10", None),
            (b"abc", None),
            (b"1\x00", None),
            (b"-9223372036854775808", Some(i64::MIN)),
            (b"-9223372036854775809", None),
            (b"9223372036854775807", Some(i64::MAX)),
            (b"9223372036854775808", None),
            (b"18446744073709551615", None),
            (b"18446744073709551616", None),
            (b"99999999999999999999", None),
            (b"-99999999999999999999", None),
            (b"100000000000000000000", None), // 21 bytes
        ];
        for &(s, want) in cases {
            assert_eq!(string2ll(s), want, "{:?}", String::from_utf8_lossy(s));
            assert_eq!(string2l(s), want);
        }
    }

    #[test]
    fn ll2string_rules() {
        let mut b = [0u8; 20];
        assert_eq!(ll2string(i64::MIN, &mut b), 20);
        assert_eq!(&b, b"-9223372036854775808");
        let mut small = [0u8; 3];
        assert_eq!(ll2string(1234, &mut small), 0);
        assert_eq!(ll2string(-12, &mut small), 3);
        assert_eq!(&small, b"-12");
        let mut v = Vec::new();
        ll2buf(-5, &mut v);
        ull2buf(u64::MAX, &mut v);
        assert_eq!(v, b"-518446744073709551615");
    }

    #[test]
    fn scan_cursor_rules() {
        // Checked against Redis 7.0.15 SCAN.
        let ok: &[(&[u8], u64)] = &[
            (b"", 0),
            (b"0", 0),
            (b"+0", 0),
            (b"-0", 0),
            (b"007", 7),
            (b"-1", u64::MAX),
            (b"18446744073709551615", u64::MAX),
            (b"-18446744073709551615", 1),
            (b"1\x00x", 1),
            (b"\x00abc", 0),
        ];
        for &(s, v) in ok {
            assert_eq!(parse_scan_cursor(s), Some(v), "{s:?}");
        }
        for s in [
            &b" 1"[..],
            b"1 ",
            b"\t1",
            b"18446744073709551616",
            b"+",
            b"-",
            b"abc",
            b"0x10",
            b"1e3",
            b"+-1",
        ] {
            assert_eq!(parse_scan_cursor(s), None, "{s:?}");
        }
    }

    #[test]
    fn strict_double() {
        assert_eq!(parse_f64_strict(b"1.5"), Some(1.5));
        assert_eq!(parse_f64_strict(b"0x1p-3"), Some(0.125));
        assert_eq!(parse_f64_strict(b"-inf"), Some(f64::NEG_INFINITY));
        assert_eq!(parse_f64_strict(b"1e400"), None);
        assert_eq!(parse_f64_strict(b"nan"), None);
        assert_eq!(parse_f64_strict(b" 1"), None);
    }

    fn g17(v: f64) -> String {
        let mut out = Vec::new();
        fmt_double_17g(v, &mut out);
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn double_17g_golden() {
        // Expected strings produced by glibc printf("%.17g").
        let cases: &[(f64, &str)] = &[
            (0.0, "0"),
            (-0.0, "-0"),
            (1.0, "1"),
            (-1.5, "-1.5"),
            (0.1, "0.10000000000000001"),
            (0.3, "0.29999999999999999"),
            (1.0 / 3.0, "0.33333333333333331"),
            (100.0, "100"),
            (123456789.0, "123456789"),
            (1e16, "10000000000000000"),
            (1e17, "1e+17"),
            (12345678901234567.0, "12345678901234568"),
            (1e21, "1e+21"),
            (1e100, "1e+100"),
            (1.5e300, "1.5000000000000001e+300"),
            (0.0001, "0.0001"),
            (0.00001, "1.0000000000000001e-05"),
            (0.000123, "0.00012300000000000001"),
            (5e-324, "4.9406564584124654e-324"),
            (f64::MAX, "1.7976931348623157e+308"),
            (f64::MIN_POSITIVE, "2.2250738585072014e-308"),
            (2.5, "2.5"),
            (4.35, "4.3499999999999996"),
            (10.6, "10.6"),
            (123.0, "123"),
            (-1e-300, "-1e-300"),
            (9007199254740993.0, "9007199254740992"),
            // exact halfway cases at the 17th significant digit: round half to even
            (12345678.0 + 1.0 / 1024.0, "12345678.000976562"),
            (12345678.0 + 3.0 / 1024.0, "12345678.002929688"),
            (f64::INFINITY, "inf"),
            (f64::NEG_INFINITY, "-inf"),
            (f64::NAN, "nan"),
            (-f64::NAN, "-nan"),
        ];
        for &(v, want) in cases {
            assert_eq!(g17(v), want, "{v:e}");
        }
    }

    #[test]
    fn double_17g_buffer_bound() {
        let mut b = [0u8; DOUBLE_17G_MAX];
        for v in [
            -1.2345678901234567e-308,
            -f64::MAX,
            -f64::MIN_POSITIVE,
            -5e-324,
            -0.00012345678901234567,
        ] {
            let n = fmt_double_17g_buf(v, &mut b);
            assert!(n <= 24, "{n}");
        }
    }

    proptest! {
        #[test]
        fn string2ll_round_trips(v in any::<i64>()) {
            let mut s = Vec::new();
            ll2buf(v, &mut s);
            prop_assert_eq!(string2ll(&s), Some(v));
        }

        #[test]
        fn string2ll_matches_canonical_model(s in "[-+0-9 a]{0,22}") {
            let b = s.as_bytes();
            let canonical = s.parse::<i64>().ok().filter(|v| v.to_string() == s);
            prop_assert_eq!(string2ll(b), canonical);
        }

        #[test]
        fn double_17g_round_trips(bits in any::<u64>()) {
            let v = f64::from_bits(bits);
            let s = g17(v);
            if v.is_nan() {
                prop_assert!(s == "nan" || s == "-nan");
            } else {
                // 17 significant digits always identify the double
                let back: f64 = s.parse().unwrap();
                prop_assert_eq!(back.to_bits(), v.to_bits(), "{}", s);
            }
        }
    }
}
