//! Clocks (DESIGN §22, §23.4, LC-28).
//!
//! `mono_ns` = CLOCK_MONOTONIC (timeouts), `boot_ns` = CLOCK_BOOTTIME (leases: counts VM
//! suspension, §23.10), `unix_ms` = wall clock, used only for TTL (§16.3) and diagnostics.
//! Versions are integers, never clocks (LC-28). Recovery uses `max(now, clock_floor)` (§16.4).
//!
//! `std` exposes no CLOCK_BOOTTIME reader and this crate's only file allowed to contain `unsafe`
//! is `ldouble.rs` (§24), so all three clocks are read through the small C shim function
//! declared there (`c/clock.c`, `clock_gettime`, served from the vDSO without a system call).
//! CLOCK_MONOTONIC is the clock behind `std::time::Instant` on Linux, so `mono_ns` readings and
//! `Instant`s measure the same time line.

use crate::ldouble::{ShimClock, clock_ns};

/// One reading of the three clocks.
#[derive(Clone, Copy, Debug)]
pub struct Now {
    /// CLOCK_MONOTONIC, nanoseconds since an arbitrary point at boot.
    pub mono_ns: u64,
    /// CLOCK_BOOTTIME, nanoseconds since boot including time spent suspended.
    pub boot_ns: u64,
    /// Wall clock, milliseconds since the Unix epoch (0 if the clock lies before 1970).
    pub unix_ms: u64,
}

impl Now {
    /// Reads the three clocks (in this order: monotonic, boot, wall).
    pub fn read() -> Self {
        Now {
            mono_ns: mono_ns(),
            boot_ns: boot_ns(),
            unix_ms: unix_ms(),
        }
    }
}

/// CLOCK_MONOTONIC in nanoseconds (timeouts, pacing).
#[inline]
pub fn mono_ns() -> u64 {
    clock_ns(ShimClock::Monotonic)
}

/// CLOCK_BOOTTIME in nanoseconds (leases, §23.10).
#[inline]
pub fn boot_ns() -> u64 {
    clock_ns(ShimClock::Boottime)
}

/// Wall clock in milliseconds since the Unix epoch (TTLs, §16.3; diagnostics).
#[inline]
pub fn unix_ms() -> u64 {
    clock_ns(ShimClock::Realtime) / 1_000_000
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[test]
    fn wall_clock_matches_std() {
        let ours = unix_ms();
        let std_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        assert!(ours.abs_diff(std_ms) < 1_000, "{ours} vs {std_ms}");
    }

    #[test]
    fn monotonic_and_boot_clocks_advance() {
        let a = Now::read();
        std::thread::sleep(Duration::from_millis(5));
        let b = Now::read();
        assert!(b.mono_ns - a.mono_ns >= 4_000_000);
        assert!(b.boot_ns - a.boot_ns >= 4_000_000);
        assert!(b.unix_ms >= a.unix_ms);
        assert!(a.boot_ns >= a.mono_ns);
    }

    #[test]
    fn monotonic_matches_instant_time_line() {
        // `Instant` reads CLOCK_MONOTONIC on Linux: an interval measured both ways agrees.
        let (i0, m0) = (std::time::Instant::now(), mono_ns());
        std::thread::sleep(Duration::from_millis(3));
        let (i1, m1) = (std::time::Instant::now(), mono_ns());
        let by_instant = (i1 - i0).as_nanos() as i128;
        let by_mono = i128::from(m1 - m0);
        assert!(
            (by_instant - by_mono).abs() < 2_000_000,
            "{by_instant} vs {by_mono}"
        );
    }
}
