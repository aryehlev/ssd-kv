//! INCRBYFLOAT arithmetic in C `long double` (DESIGN §8.1, §8.3, §24.3), and the crate's only
//! FFI boundary to the C shim.
//!
//! The arithmetic and the rendering are done by the C shim `c/ldouble.c` (compiled by
//! `build.rs`), which reproduces Redis 7.0's `string2ld` / `ld2string(LD_STR_HUMAN)`: the current
//! value and the increment are parsed with `strtold` (hex floats and values beyond the `f64`
//! range accepted, NaN rejected, overflow or underflow to zero rejected, no surrounding spaces,
//! at most 5119 bytes), added in `long double`, and a NaN/infinite result is refused; the result
//! is printed with `%.17Lf`, then trailing zeros after the `.` and a trailing `.` are removed and
//! `-0` becomes `0`. Replication ships the formatted result as a `Put` (§23.7).
//!
//! Two more shim functions are declared here because this is the only `kv-common` file allowed to
//! contain `unsafe` (§24): Redis `string2d` (strict `strtod`, used by
//! [`crate::num::parse_f64_strict`]) and the clock reads behind [`crate::clock`]
//! (`c/clock.c`; `std` has no CLOCK_BOOTTIME reader). Each `unsafe` item has a `SAFETY:` note.
#![allow(unsafe_code)]

use core::ffi::{c_int, c_uchar};

/// Error of [`incrbyfloat`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LdError {
    /// `-ERR value is not a valid float`
    NotFloat,
    /// `-ERR increment would produce NaN or Infinity`
    NanOrInf,
}

/// Redis `MAX_LONG_DOUBLE_CHARS`; also the size of the output buffer handed to the shim.
const MAX_LONG_DOUBLE_CHARS: usize = 5 * 1024;

const KV_LD_OK: c_int = 0;
const KV_LD_NOT_FLOAT: c_int = 1;

// SAFETY: these declarations match the definitions in c/ldouble.c (C11, `size_t` == `usize` on
// every supported target); the function only reads and writes the buffers it is given.
unsafe extern "C" {
    fn kv_incrbyfloat(
        cur: *const c_uchar,
        cur_len: usize,
        has_cur: c_int,
        incr: *const c_uchar,
        incr_len: usize,
        out: *mut c_uchar,
        out_cap: usize,
        out_len: *mut usize,
    ) -> c_int;
}

// SAFETY: matches `int kv_string2d(const unsigned char *, size_t, double *)` in c/ldouble.c; it
// reads `len` bytes from `s`, writes one `double` through `out` and keeps no pointer.
unsafe extern "C" {
    fn kv_string2d(s: *const c_uchar, len: usize, out: *mut f64) -> c_int;
}

// SAFETY: matches `uint64_t kv_clock_ns(int)` in c/clock.c: it takes an integer by value,
// returns an integer, touches no memory owned by Rust and accepts every `int` (unknown selectors
// read CLOCK_MONOTONIC), so calling it is safe.
unsafe extern "C" {
    safe fn kv_clock_ns(which: c_int) -> u64;
}

/// Clock selectors of `kv_clock_ns` (mirrored in c/clock.c).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum ShimClock {
    Monotonic = 0,
    Boottime = 1,
    Realtime = 2,
}

/// Nanoseconds of `clock` (`clock_gettime`); 0 if it cannot be read.
#[inline]
pub(crate) fn clock_ns(clock: ShimClock) -> u64 {
    kv_clock_ns(clock as c_int)
}

/// Redis 7.0 `string2d`: `strtod` over the whole argument, refusing an empty string, a leading
/// space, trailing bytes (including an embedded NUL), overflow, underflow to zero and NaN.
/// Hex floats and `inf`/`infinity` are accepted, as by `strtod`.
pub(crate) fn string2d(s: &[u8]) -> Option<f64> {
    if s.is_empty() {
        return None;
    }
    let mut out = 0f64;
    // SAFETY: `s` is a live, initialized slice of `s.len()` bytes that the shim only reads (it
    // copies them into its own NUL-terminated buffer); `out` is a valid, exclusive pointer for
    // the duration of the call. The shim keeps no pointer after returning.
    let ok = unsafe { kv_string2d(s.as_ptr(), s.len(), &mut out) };
    (ok != 0).then_some(out)
}

/// `current + incr` with Redis 7.0 INCRBYFLOAT semantics; `current = None` means the key does
/// not exist (treated as 0). Returns the formatted result to store and reply.
pub fn incrbyfloat(current: Option<&[u8]>, incr: &[u8]) -> Result<Vec<u8>, LdError> {
    let mut out = vec![0u8; MAX_LONG_DOUBLE_CHARS];
    let mut out_len = 0usize;
    let (cur_ptr, cur_len, has_cur) = match current {
        Some(c) => (c.as_ptr(), c.len(), 1),
        None => (core::ptr::null(), 0, 0),
    };
    // SAFETY: `cur_ptr`/`cur_len` and `incr` describe live, initialized byte slices (or a null
    // pointer with `has_cur == 0`, which the shim never dereferences); the shim reads at most
    // `len` bytes from each and copies them into its own NUL-terminated buffer. `out` is a live
    // buffer of `out.len()` bytes and the shim writes at most `out_cap` bytes into it.
    // `out_len` is a valid, exclusive pointer for the duration of the call. The shim keeps no
    // pointer after returning.
    let rc = unsafe {
        kv_incrbyfloat(
            cur_ptr,
            cur_len,
            has_cur,
            incr.as_ptr(),
            incr.len(),
            out.as_mut_ptr(),
            out.len(),
            &mut out_len,
        )
    };
    match rc {
        KV_LD_OK => {
            out.truncate(out_len);
            Ok(out)
        }
        KV_LD_NOT_FLOAT => Err(LdError::NotFloat),
        _ => Err(LdError::NanOrInf),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(cur: Option<&str>, incr: &str) -> String {
        String::from_utf8(incrbyfloat(cur.map(str::as_bytes), incr.as_bytes()).unwrap()).unwrap()
    }

    fn err(cur: Option<&str>, incr: &str) -> LdError {
        incrbyfloat(cur.map(str::as_bytes), incr.as_bytes()).unwrap_err()
    }

    #[test]
    fn redis_documented_examples() {
        // INCRBYFLOAT documentation examples.
        assert_eq!(ok(Some("10.50"), "0.1"), "10.6");
        assert_eq!(ok(Some("10.6"), "-5"), "5.6");
        assert_eq!(ok(Some("5.0e3"), "2.0e2"), "5200");
    }

    #[test]
    fn decimal_rounding() {
        assert_eq!(ok(Some("0.1"), "0.2"), "0.3");
        assert_eq!(ok(Some("10.5"), "0.1"), "10.6");
        assert_eq!(ok(Some("1.0"), "0.0000000000000000001"), "1");
        assert_eq!(ok(Some("3"), "1.5"), "4.5");
    }

    #[test]
    fn missing_key_is_zero() {
        assert_eq!(ok(None, "1.5"), "1.5");
        assert_eq!(ok(None, "3"), "3");
        assert_eq!(ok(None, "-2.25"), "-2.25");
    }

    #[test]
    fn integers_and_zero() {
        assert_eq!(ok(Some("3"), "0"), "3");
        assert_eq!(ok(Some("-1"), "1"), "0");
        assert_eq!(ok(None, "-0"), "0");
        // "-0.00000000000000000" -> "-0" -> "0"
        assert_eq!(ok(None, "-1e-20"), "0");
        // 2^63 - 1 is exact in an x87 long double (64-bit mantissa)
        #[cfg(target_arch = "x86_64")]
        assert_eq!(ok(Some("9223372036854775807"), "0"), "9223372036854775807");
    }

    #[test]
    fn strtold_forms() {
        assert_eq!(ok(Some("0x10"), "1"), "17");
        assert_eq!(ok(Some("+1.5"), "1"), "2.5");
        assert_eq!(ok(Some("1e2"), "1E1"), "110");
        // Beyond the f64 range but inside the long double range: accepted and printed in full.
        #[cfg(target_arch = "x86_64")]
        {
            let big = ok(None, "1e400");
            assert!(big.len() == 400 || big.len() == 401, "{}", big.len());
            assert!(big.bytes().all(|b| b.is_ascii_digit()));
        }
    }

    #[test]
    fn not_a_float() {
        assert_eq!(err(Some("abc"), "1"), LdError::NotFloat);
        assert_eq!(err(Some("1"), "abc"), LdError::NotFloat);
        assert_eq!(err(Some(""), "1"), LdError::NotFloat);
        assert_eq!(err(None, ""), LdError::NotFloat);
        assert_eq!(err(None, " 1"), LdError::NotFloat);
        assert_eq!(err(None, "1 "), LdError::NotFloat);
        assert_eq!(err(None, "nan"), LdError::NotFloat);
        assert_eq!(err(None, "1.5x"), LdError::NotFloat);
        // underflow to zero is ERANGE + FP_ZERO
        assert_eq!(err(None, "1e-5000"), LdError::NotFloat);
        // input of MAX_LONG_DOUBLE_CHARS bytes or more is refused
        let long = "1".repeat(MAX_LONG_DOUBLE_CHARS);
        assert_eq!(err(None, &long), LdError::NotFloat);
    }

    #[test]
    fn string2d_rules() {
        assert_eq!(string2d(b"1.5"), Some(1.5));
        assert_eq!(string2d(b"-0"), Some(-0.0));
        assert_eq!(string2d(b"+3"), Some(3.0));
        assert_eq!(string2d(b"1e3"), Some(1000.0));
        assert_eq!(string2d(b"0x10"), Some(16.0));
        assert_eq!(string2d(b"0x1p-3"), Some(0.125));
        assert_eq!(string2d(b"inf"), Some(f64::INFINITY));
        assert_eq!(string2d(b"-Infinity"), Some(f64::NEG_INFINITY));
        assert_eq!(string2d(b"4.9e-324"), Some(4.9e-324)); // denormal: accepted
        assert_eq!(string2d(b""), None);
        assert_eq!(string2d(b" 1"), None);
        assert_eq!(string2d(b"1 "), None);
        assert_eq!(string2d(b"1\x002"), None);
        assert_eq!(string2d(b"nan"), None);
        assert_eq!(string2d(b"1e400"), None); // overflow
        assert_eq!(string2d(b"1e-400"), None); // underflow to zero
        assert_eq!(string2d(b"abc"), None);
        // no length limit (Redis parses the argument in place)
        let mut long = b"0.5".to_vec();
        long.extend(std::iter::repeat_n(b'0', 6000));
        assert_eq!(string2d(&long), Some(0.5));
    }

    #[test]
    fn clocks_advance() {
        let a = clock_ns(ShimClock::Monotonic);
        let b = clock_ns(ShimClock::Boottime);
        let c = clock_ns(ShimClock::Realtime);
        assert!(a > 0 && b > 0);
        assert!(
            b >= a,
            "boottime includes suspend time, so it is never behind monotonic"
        );
        // after 2020-01-01
        assert!(c > 1_577_836_800_000_000_000);
        assert!(clock_ns(ShimClock::Monotonic) >= a);
    }

    #[test]
    fn nan_or_infinity() {
        assert_eq!(err(None, "inf"), LdError::NanOrInf);
        assert_eq!(err(Some("-inf"), "1"), LdError::NanOrInf);
        #[cfg(target_arch = "x86_64")]
        assert_eq!(err(Some("1e4932"), "1e4932"), LdError::NanOrInf);
    }
}
