//! Clocks (DESIGN §22, §23.4, LC-28).
//!
//! `mono_ns` = CLOCK_MONOTONIC (timeouts), `boot_ns` = CLOCK_BOOTTIME (leases: counts VM
//! suspension, §23.10), `unix_ms` = wall clock, used only for TTL (§16.3) and diagnostics.
//! Versions are integers, never clocks (LC-28). Recovery uses `max(now, clock_floor)` (§16.4).
//!
//! Note for the implementer: `std` exposes no CLOCK_BOOTTIME reader and this crate's only file
//! allowed to contain `unsafe` is `ldouble.rs` (§24), so the boottime read goes through a small
//! function in the C shim declared there.

/// One reading of the three clocks.
#[derive(Clone, Copy, Debug)]
pub struct Now {
    pub mono_ns: u64,
    pub boot_ns: u64,
    pub unix_ms: u64,
}

impl Now {
    pub fn read() -> Self {
        todo!("DESIGN §22: read CLOCK_MONOTONIC, CLOCK_BOOTTIME and the wall clock")
    }
}
