//! `kv-common`: primitives shared by every kvssd crate (DESIGN §24.3).
//!
//! * [`slot`]: CRC16 hash slots, hashtag rule, [`SlotSet`] bitmaps (§6.2, §23.14).
//! * [`hash`]: seeded xxh3 [`KeyHash`] and its fingerprint views (§6.2, §9.2).
//! * [`crc`]: CRC32C and the keyed, location-bound record checksum (§10.8, §10.9).
//! * [`clock`]: monotonic / boot / wall clocks ([`Now`], §22).
//! * [`pace`]: the only background pacing APIs, static token buckets and the
//!   device-latency controller (§19, LC-16).
//! * [`abblock`]: the A/B fixed-location metadata block codec (§10.2, §17.6).
//! * [`glob`], [`num`], [`ldouble`]: Redis-compatible pattern matching, integer/float
//!   parsing and formatting, INCRBYFLOAT arithmetic (§8.1).
//! * [`metrics`]: latency histograms (§21).
//! * [`shared_buf`]: reference-counted reply bodies passed between threads (§7.1).
//! * [`redact`]: the only way user bytes may appear in a diagnostic (LC-32).
//!
//! `unsafe` is allowed only in `ldouble.rs`, the crate's single FFI boundary to its C shim
//! (§24).
#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(unsafe_code)]

pub mod abblock;
pub mod clock;
pub mod crc;
pub mod glob;
pub mod hash;
pub mod ldouble;
pub mod metrics;
pub mod num;
pub mod pace;
pub mod redact;
pub mod shared_buf;
pub mod slot;

pub use clock::Now;
pub use crc::RecordCrc;
pub use hash::KeyHash;
pub use redact::Redacted;
pub use shared_buf::SharedBuf;
pub use slot::{SLOTS, SlotSet, key_slot};

/// Per-key access mode of a command (§6.5, §8.1): `Meta` = header only (existence, type, TTL,
/// length), `Read` = full value, `Write` = write (prefetches an existing candidate record),
/// `ReadWrite` = read-modify-write.
///
/// Defined here so that `kv-storage` (whose `partition::Request` carries it) does not depend on
/// `kv-cmd` (§24.2); `kv-cmd` re-exports it as `kv_cmd::Access`, the name §24.5 and §24.7 use.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Access {
    Meta,
    Read,
    Write,
    ReadWrite,
}
