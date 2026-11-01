//! Free-space management (DESIGN §11).
//!
//! Per partition: `page_free` (one byte per owned SMALL page: free bytes / 16, 255 = fresh) and,
//! per EXTENT AG, `alloc`/`start` bitmaps plus the free runs sorted by `(len, start)`. This is
//! allocator state, not index (reported as `alloc_state_bytes`, LC-09). Placement of a small
//! record (§11.3): best fit among cached qualifying pages, else a fresh page of the owned SMALL
//! AG with the fewest fresh pages left, else best fit among uncached pages by hint, else the
//! spare SMALL AG, else `-OOM` at the fill limit (§11.6). Extents (§11.4): the key's twin run if
//! free, else the best-fit hole, carving at a drawn `n`-aligned offset of a longer run, else the
//! spare EXTENT AG. Freed space is reusable at once by any key of the partition; empty AGs are
//! released to the node pool (§11.7). Live records are never moved to reclaim space.
//!
//! Every choice among free pages, AGs, tie-breaks and carve offsets goes through
//! [`KeyedPick`] (LC-01): `r = xxh3(domain || boot_gen || draws, seed)` selects the
//! `(r mod n)`-th candidate, so the acquisition sequence is neither monotonic nor periodic and
//! placement is uncorrelated with time and device address.

use kv_common::KeyHash;

/// Keyed pseudo-random selection among equal candidates.
pub struct KeyedPick {
    seed: u64,
    boot_gen: u32,
    draws: u64,
}

impl KeyedPick {
    pub fn new(hash_seed: u64, boot_gen: u32) -> Self {
        KeyedPick { seed: hash_seed, boot_gen, draws: 0 }
    }

    /// index of the (r mod n)-th set bit, r = xxh3(domain || boot_gen || draws, seed); None if
    /// no bit is set
    pub fn pick(&mut self, domain: u64, candidates: &[u64]) -> Option<usize> {
        todo!("DESIGN §11.1: keyed draw over a candidate bitmap")
    }

    /// among n equal candidates
    pub fn pick_index(&mut self, domain: u64, n: usize) -> Option<usize> {
        todo!("DESIGN §11.1: keyed draw among n candidates")
    }

    /// The next draw value `xxh3(domain || boot_gen || draws, seed)`.
    fn draw(&mut self, domain: u64) -> u64 {
        let mut msg = [0u8; 20];
        msg[..8].copy_from_slice(&domain.to_le_bytes());
        msg[8..12].copy_from_slice(&self.boot_gen.to_le_bytes());
        msg[12..].copy_from_slice(&self.draws.to_le_bytes());
        self.draws += 1;
        KeyHash::of(self.seed, &msg).raw()
    }
}
