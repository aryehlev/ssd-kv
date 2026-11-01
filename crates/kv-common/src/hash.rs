//! Key identity hash (DESIGN §6.2, §9.2, §10.8, LC-06).
//!
//! `KeyHash = xxh3_64_with_seed(key, node_seed)`; the seed is secret and per node (superblock
//! `hash_seed`). It is computed by the owner partition from the key bytes, never carried in
//! cross-worker messages (LC-37). Views: `fp32` = bits 63..32 (index fingerprint and sort key),
//! `x8` = bits 31..24 (extra fingerprint bits of small entries), `h32` = bits 31..0 (record
//! header field used to locate a record inside its page). Non-cryptographic by design (LC-06).

use xxhash_rust::xxh3::xxh3_64_with_seed;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct KeyHash(u64);

impl KeyHash {
    /// `xxh3_64_with_seed(key, seed)`.
    pub fn of(seed: u64, key: &[u8]) -> Self {
        KeyHash(xxh3_64_with_seed(key, seed))
    }

    /// Bits 63..32: the index fingerprint (§9.2).
    pub fn fp32(self) -> u32 {
        (self.0 >> 32) as u32
    }

    /// Bits 31..24: extra fingerprint bits stored in small entries (§9.2).
    pub fn x8(self) -> u8 {
        (self.0 >> 24) as u8
    }

    /// Bits 31..0: the record header `h32` (§10.8).
    pub fn h32(self) -> u32 {
        self.0 as u32
    }

    pub fn raw(self) -> u64 {
        self.0
    }
}
