//! Ordered linear-probing segments (DESIGN §9.3, §9.4).
//!
//! A segment has `cap` home positions plus a tail of `8 + cap / 64` overflow positions (no
//! wraparound). `home(fp) = (((fp << d) as u32) as u64 * cap as u64) >> 32`, monotonic in `fp`.
//! Invariants after every operation: (I-a) entries sorted by `fp32` over the whole array, (I-b)
//! every entry at a position >= its home, (I-c) no EMPTY between an entry's home and its
//! position. Insert shifts the run up to the next EMPTY right by one (memmove of entries and
//! hints); remove is a backward shift (no RAM tombstones); grow/shrink rebuild in one linear
//! pass; a full segment at `SEG_MAX` splits on fp bit `31 - d`.
//!
//! Capacity sequence: 16, 32, ..., 1024, then `ceil(cap * 9 / 8)` up to [`SEG_MAX`]. The entry
//! array's byte length `10 * (cap + 8 + cap / 64)` is never a multiple of 64 for any capacity of
//! the sequence (LC-09, unit test below). Grow when `len > 0.90 * cap`; shrink when
//! `len < 0.25 * cap` and `cap > 16`.

use crate::sysbuf::SysBuf;

/// Smallest segment capacity.
pub const SEG_MIN_CAP: u32 = 16;
/// Largest segment capacity; a full segment of this capacity splits.
pub const SEG_MAX: u32 = 8192;

/// Tail positions after the `cap` home positions.
pub const fn tail(cap: u32) -> u32 {
    8 + cap / 64
}

/// Byte length of a segment's entry array.
pub const fn seg_bytes(cap: u32) -> usize {
    10 * (cap as usize + tail(cap) as usize)
}

/// The next capacity of the sequence after `cap`, or `None` at [`SEG_MAX`].
pub const fn next_cap(cap: u32) -> Option<u32> {
    if cap >= SEG_MAX {
        None
    } else if cap < 1024 {
        Some(cap * 2)
    } else {
        let n = (cap * 9).div_ceil(8);
        Some(if n > SEG_MAX { SEG_MAX } else { n })
    }
}

/// Every capacity of the sequence, ascending.
pub fn cap_sequence() -> impl Iterator<Item = u32> {
    core::iter::successors(Some(SEG_MIN_CAP), |&c| next_cap(c))
}

/// One segment (crate-internal; §9.3).
pub(crate) struct Segment {
    /// Home positions; total positions = `cap + tail(cap)`.
    pub(crate) cap: u32,
    pub(crate) len: u32,
    pub(crate) ttl_count: u32,
    /// Local depth: number of top fp bits fixed for this segment.
    pub(crate) depth: u8,
    /// `10 * (cap + tail)` bytes from `seg_alloc`.
    pub(crate) ents: SysBuf,
    /// `2 * (cap + tail)` bytes, allocated when the first TTL key arrives.
    pub(crate) hints: Option<SysBuf>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seg_bytes_never_multiple_of_64() {
        // LC-09
        let caps: Vec<u32> = cap_sequence().collect();
        assert_eq!(caps.first(), Some(&SEG_MIN_CAP));
        assert_eq!(caps.last(), Some(&SEG_MAX));
        for cap in caps {
            assert_ne!(seg_bytes(cap) % 64, 0, "cap {cap}");
        }
    }
}
