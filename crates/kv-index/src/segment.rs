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
//! the sequence (LC-09, unit test below); neither is the hint plane's `2 * (cap + 8 + cap / 64)`.
//! Grow when `len > 0.90 * cap`; shrink when `len < 0.25 * cap` and `cap > 16`.
//!
//! Given its entries, capacity and depth, the layout of a segment is unique (each entry sits at
//! `max(previous position + 1, home)`), so the incremental insert/remove paths and the rebuild
//! pass always produce the same bytes. EMPTY positions are ten zero bytes with hint 0.

use core::ops::Range;

use crate::entry::{Entry, Raw, raw_fp32};
use crate::sysbuf::{SysBuf, seg_alloc};

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

/// Byte length of a segment's TTL hint plane.
pub const fn hint_bytes(cap: u32) -> usize {
    2 * (cap as usize + tail(cap) as usize)
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

/// `n` entries stay within the grow threshold of capacity `cap` (`n <= 0.90 * cap`).
#[inline]
pub(crate) const fn within_grow_threshold(n: u32, cap: u32) -> bool {
    n as u64 * 10 <= cap as u64 * 9
}

/// A segment of capacity `cap` holding `n` entries is shrunk (`n < 0.25 * cap`, `cap > 16`).
#[inline]
pub(crate) const fn below_shrink_threshold(n: u32, cap: u32) -> bool {
    cap > SEG_MIN_CAP && (n as u64) * 4 < cap as u64
}

/// A load factor `num / den`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Load {
    pub(crate) num: u32,
    pub(crate) den: u32,
}

/// The smallest capacity of the sequence whose load with `n` entries is at most `load`
/// ([`SEG_MAX`] if there is none).
pub(crate) fn cap_for_load(n: u32, load: Load) -> u32 {
    cap_sequence()
        .find(|&c| u64::from(n) * u64::from(load.den) <= u64::from(c) * u64::from(load.num))
        .unwrap_or(SEG_MAX)
}

/// Home position of `fp` in a segment of local depth `depth` and capacity `cap`.
#[inline(always)]
pub(crate) fn home(fp: u32, depth: u8, cap: u32) -> usize {
    let shifted = (u64::from(fp) << depth) as u32;
    ((u64::from(shifted) * u64::from(cap)) >> 32) as usize
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

impl Segment {
    /// An empty segment.
    pub(crate) fn new(cap: u32, depth: u8) -> Self {
        Segment {
            cap,
            len: 0,
            ttl_count: 0,
            depth,
            ents: seg_alloc(seg_bytes(cap)),
            hints: None,
        }
    }

    /// Number of positions (`cap + tail(cap)`).
    #[inline(always)]
    pub(crate) fn total(&self) -> usize {
        (self.cap + tail(self.cap)) as usize
    }

    #[inline(always)]
    pub(crate) fn home(&self, fp: u32) -> usize {
        home(fp, self.depth, self.cap)
    }

    /// The packed entries, one per position.
    #[inline(always)]
    pub(crate) fn ents(&self) -> &[Raw] {
        self.ents.as_slice().as_chunks::<{ Entry::BYTES }>().0
    }

    #[inline(always)]
    pub(crate) fn ents_mut(&mut self) -> &mut [Raw] {
        self.ents
            .as_mut_slice()
            .as_chunks_mut::<{ Entry::BYTES }>()
            .0
    }

    /// The hint plane, one little-endian `u16` per position, if allocated.
    #[inline(always)]
    pub(crate) fn hints(&self) -> Option<&[[u8; 2]]> {
        self.hints.as_ref().map(|h| h.as_slice().as_chunks::<2>().0)
    }

    #[inline(always)]
    pub(crate) fn hint_at(&self, i: usize) -> u16 {
        match &self.hints {
            None => 0,
            Some(h) => u16::from_le_bytes(h.as_slice().as_chunks::<2>().0[i]),
        }
    }

    /// Writes the hint of position `i`, allocating the plane for the first non-zero hint.
    #[inline]
    fn put_hint(&mut self, i: usize, hint: u16) {
        if self.hints.is_none() {
            if hint == 0 {
                return;
            }
            self.hints = Some(seg_alloc(hint_bytes(self.cap)));
        }
        if let Some(h) = &mut self.hints {
            h.as_mut_slice().as_chunks_mut::<2>().0[i] = hint.to_le_bytes();
        }
    }

    /// Bytes allocated for this segment's arrays.
    pub(crate) fn mem_bytes(&self) -> usize {
        self.ents.alloc_bytes() + self.hints.as_ref().map_or(0, SysBuf::alloc_bytes)
    }

    /// Inserts `raw` (non-EMPTY) keeping I-a..I-c: from `home(fp)`, skips entries with
    /// `fp32 <= fp`, then shifts the run up to the next EMPTY right by one. Returns `false`
    /// (segment unchanged) if no EMPTY exists between that position and the end of the tail.
    pub(crate) fn try_insert(&mut self, raw: &Raw, hint: u16) -> bool {
        let fp = raw_fp32(raw);
        let ents = self.ents();
        let total = ents.len();
        let mut i = self.home(fp);
        while i < total && !Entry::is_empty_raw(&ents[i]) && raw_fp32(&ents[i]) <= fp {
            i += 1;
        }
        let Some(off) = ents[i..].iter().position(Entry::is_empty_raw) else {
            return false;
        };
        let j = i + off;
        const B: usize = Entry::BYTES;
        let bytes = self.ents.as_mut_slice();
        bytes.copy_within(i * B..j * B, (i + 1) * B);
        bytes[i * B..(i + 1) * B].copy_from_slice(raw);
        if let Some(h) = &mut self.hints {
            let hb = h.as_mut_slice();
            hb.copy_within(i * 2..j * 2, (i + 1) * 2);
            hb[i * 2..i * 2 + 2].copy_from_slice(&hint.to_le_bytes());
        } else {
            self.put_hint(i, hint);
        }
        self.len += 1;
        if hint != 0 {
            self.ttl_count += 1;
        }
        true
    }

    /// Removes the entry at `i` by backward shift: the following entries that sit past their
    /// home move left by one; the last vacated position becomes EMPTY. Returns the removed
    /// entry's hint, or `None` (segment unchanged) if position `i` is EMPTY or out of range.
    pub(crate) fn remove_at(&mut self, i: usize) -> Option<u16> {
        let ents = self.ents();
        let total = ents.len();
        if i >= total || Entry::is_empty_raw(&ents[i]) {
            return None;
        }
        let mut k = i + 1;
        while k < total && !Entry::is_empty_raw(&ents[k]) && self.home(raw_fp32(&ents[k])) < k {
            k += 1;
        }
        let old_hint = self.hint_at(i);
        const B: usize = Entry::BYTES;
        let bytes = self.ents.as_mut_slice();
        bytes.copy_within((i + 1) * B..k * B, i * B);
        bytes[(k - 1) * B..k * B].fill(0);
        if let Some(h) = &mut self.hints {
            let hb = h.as_mut_slice();
            hb.copy_within((i + 1) * 2..k * 2, i * 2);
            hb[(k - 1) * 2..k * 2].fill(0);
        }
        self.len -= 1;
        if old_hint != 0 {
            self.ttl_count -= 1;
        }
        Some(old_hint)
    }

    /// Rewrites the non-EMPTY entry at `i` in place (same fp32) and its hint; returns the old
    /// hint.
    pub(crate) fn replace_at(&mut self, i: usize, raw: &Raw, hint: u16) -> u16 {
        debug_assert_eq!(raw_fp32(raw), raw_fp32(&self.ents()[i]));
        let old = self.hint_at(i);
        self.ents_mut()[i] = *raw;
        self.put_hint(i, hint);
        match (old != 0, hint != 0) {
            (false, true) => self.ttl_count += 1,
            (true, false) => self.ttl_count -= 1,
            _ => {}
        }
        old
    }

    /// The non-EMPTY entries of positions `r`, with their hints, in position (= fp) order.
    pub(crate) fn iter_range(
        &self,
        r: Range<usize>,
    ) -> impl Iterator<Item = (Raw, u16)> + Clone + '_ {
        let start = r.start;
        self.ents()[r]
            .iter()
            .enumerate()
            .filter(|(_, raw)| !Entry::is_empty_raw(raw))
            .map(move |(k, raw)| (*raw, self.hint_at(start + k)))
    }

    /// Builds a segment of capacity `cap` and depth `depth` from `src` (non-EMPTY entries
    /// sorted by fp32, every one belonging to the segment's fp prefix) in one linear pass,
    /// placing each entry at `max(previous position + 1, home)`. `None` if the placement runs
    /// past the end of the tail. The hint plane is allocated only if some hint is non-zero.
    pub(crate) fn build(
        cap: u32,
        depth: u8,
        src: impl Iterator<Item = (Raw, u16)>,
    ) -> Option<Segment> {
        let mut seg = Segment::new(cap, depth);
        let total = seg.total();
        let mut next = 0usize;
        for (raw, hint) in src {
            debug_assert!(!Entry::is_empty_raw(&raw));
            let pos = next.max(seg.home(raw_fp32(&raw)));
            if pos >= total {
                return None;
            }
            seg.ents_mut()[pos] = raw;
            if hint != 0 {
                seg.put_hint(pos, hint);
                seg.ttl_count += 1;
            }
            seg.len += 1;
            next = pos + 1;
        }
        Some(seg)
    }

    /// Clears the merge-sync mark of every entry (EMPTY positions are all-zero already).
    pub(crate) fn clear_sync_marks(&mut self) {
        for raw in self.ents_mut() {
            crate::entry::raw_set_sync_mark(raw, false);
        }
    }

    /// Checks I-a, I-b, I-c on the cluster (maximal non-EMPTY run) around position `i` and
    /// the fp order against the nearest entries outside it (tests only; O(cluster)).
    #[cfg(test)]
    pub(crate) fn check_cluster(&self, i: usize) -> Result<(), String> {
        let ents = self.ents();
        let i = i.min(ents.len().saturating_sub(1));
        let mut s = i;
        while s > 0 && !Entry::is_empty_raw(&ents[s - 1]) {
            s -= 1;
        }
        let mut e = i;
        while e < ents.len() && !Entry::is_empty_raw(&ents[e]) {
            e += 1;
        }
        let before = ents[..s]
            .iter()
            .rev()
            .find(|r| !Entry::is_empty_raw(r))
            .map(raw_fp32);
        let after = ents[e..]
            .iter()
            .find(|r| !Entry::is_empty_raw(r))
            .map(raw_fp32);
        let mut prev = before;
        for (p, raw) in ents.iter().enumerate().take(e).skip(s) {
            let fp = raw_fp32(raw);
            if prev.is_some_and(|q| q > fp) {
                return Err(format!("I-a: position {p} out of fp order"));
            }
            prev = Some(fp);
            let h = self.home(fp);
            if h > p || h < s {
                return Err(format!(
                    "I-b/I-c: position {p}, home {h}, cluster start {s}"
                ));
            }
        }
        if let (Some(q), Some(a)) = (prev, after)
            && q > a
        {
            return Err("I-a: cluster above the next entry".into());
        }
        Ok(())
    }

    /// Checks I-a, I-b, I-c and the bookkeeping of this segment (tests only).
    #[cfg(test)]
    pub(crate) fn check(&self) -> Result<(), String> {
        let ents = self.ents();
        if ents.len() != self.total() || self.ents.len() != seg_bytes(self.cap) {
            return Err("entry array length".into());
        }
        if let Some(h) = &self.hints
            && h.len() != hint_bytes(self.cap)
        {
            return Err("hint plane length".into());
        }
        if self.ents.len().is_multiple_of(64)
            || self.hints.as_ref().is_some_and(|h| h.len() % 64 == 0)
        {
            return Err("LC-09: allocation length is a multiple of 64".into());
        }
        if !cap_sequence().any(|c| c == self.cap) {
            return Err(format!("cap {} not in the sequence", self.cap));
        }
        let mut len = 0u32;
        let mut ttl = 0u32;
        let mut run_start = 0usize;
        let mut prev_fp: Option<u32> = None;
        for (p, raw) in ents.iter().enumerate() {
            let hint = self.hint_at(p);
            if Entry::is_empty_raw(raw) {
                if *raw != crate::entry::RAW_EMPTY || hint != 0 {
                    return Err(format!("EMPTY position {p} not zeroed"));
                }
                run_start = p + 1;
                continue;
            }
            len += 1;
            if hint != 0 {
                ttl += 1;
            }
            let fp = raw_fp32(raw);
            if prev_fp.is_some_and(|q| q > fp) {
                return Err(format!("I-a: position {p} out of fp order"));
            }
            prev_fp = Some(fp);
            let h = self.home(fp);
            if h > p {
                return Err(format!("I-b: position {p} before its home {h}"));
            }
            if h < run_start {
                return Err(format!("I-c: EMPTY between home {h} and position {p}"));
            }
            if Entry::try_decode(raw).is_none() {
                return Err(format!("reserved kind at {p}"));
            }
        }
        if len != self.len || ttl != self.ttl_count {
            return Err(format!(
                "counts: len {} vs {len}, ttl {} vs {ttl}",
                self.len, self.ttl_count
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::{Kind, VType};

    fn raw(fp32: u32, page36: u64) -> Raw {
        Entry {
            fp32,
            x8: 0,
            page36,
            kind: Kind::Small,
            vtype: VType::String,
            sync_mark: false,
        }
        .to_raw()
    }

    #[test]
    fn seg_bytes_never_multiple_of_64() {
        // LC-09
        let caps: Vec<u32> = cap_sequence().collect();
        assert_eq!(caps.first(), Some(&SEG_MIN_CAP));
        assert_eq!(caps.last(), Some(&SEG_MAX));
        for cap in caps {
            assert_ne!(seg_bytes(cap) % 64, 0, "cap {cap}");
            assert_ne!(hint_bytes(cap) % 64, 0, "hint plane, cap {cap}");
        }
    }

    #[test]
    fn capacity_sequence() {
        let caps: Vec<u32> = cap_sequence().collect();
        assert_eq!(&caps[..7], &[16, 32, 64, 128, 256, 512, 1024]);
        for w in caps.windows(2) {
            assert!(w[1] > w[0]);
            if w[0] >= 1024 && w[1] < SEG_MAX {
                assert_eq!(w[1], (w[0] * 9).div_ceil(8));
            }
        }
        assert_eq!(caps.len(), 25);
        assert_eq!(cap_for_load(0, Load { num: 8, den: 10 }), 16);
        assert_eq!(cap_for_load(13, Load { num: 8, den: 10 }), 32);
        assert_eq!(cap_for_load(3687, Load { num: 8, den: 10 }), 4743);
        assert_eq!(cap_for_load(100_000, Load { num: 8, den: 10 }), SEG_MAX);
        assert!(within_grow_threshold(14, 16) && !within_grow_threshold(15, 16));
        assert!(below_shrink_threshold(7, 32) && !below_shrink_threshold(8, 32));
        assert!(!below_shrink_threshold(0, 16));
    }

    #[test]
    fn home_is_monotonic_and_in_range() {
        for depth in [0u8, 1, 5, 19] {
            for cap in cap_sequence() {
                let mut prev = 0usize;
                // fps sharing the top `depth` bits (prefix 0)
                let span = if depth == 0 {
                    u64::from(u32::MAX)
                } else {
                    (1u64 << (32 - depth)) - 1
                };
                for k in 0..=64u64 {
                    let fp = (span * k / 64) as u32;
                    let h = home(fp, depth, cap);
                    assert!(h < cap as usize);
                    assert!(h >= prev);
                    prev = h;
                }
            }
        }
    }

    #[test]
    fn insert_remove_shift() {
        let mut s = Segment::new(16, 0);
        // all with the same home (fp small), forcing a run that spills into the tail
        let fps = [5u32, 1, 3, 3, 2, 4, 0, 6];
        for (k, &fp) in fps.iter().enumerate() {
            assert!(s.try_insert(&raw(fp, k as u64 + 1), if k % 2 == 0 { 7 } else { 0 }));
            s.check().unwrap();
        }
        assert_eq!(s.len, 8);
        assert_eq!(s.ttl_count, 4);
        let order: Vec<u32> = s
            .iter_range(0..s.total())
            .map(|(r, _)| raw_fp32(&r))
            .collect();
        assert_eq!(order, vec![0, 1, 2, 3, 3, 4, 5, 6]);
        // remove from the middle and the front
        assert!(s.remove_at(3).is_some());
        s.check().unwrap();
        assert!(s.remove_at(0).is_some());
        s.check().unwrap();
        assert_eq!(s.remove_at(20), None);
        assert_eq!(s.len, 6);
        // the incremental layout equals the rebuilt (canonical) layout
        let rebuilt = Segment::build(16, 0, s.iter_range(0..s.total())).unwrap();
        assert_eq!(rebuilt.ents.as_slice(), s.ents.as_slice());
    }

    #[test]
    fn full_tail_refuses_insert() {
        let mut s = Segment::new(16, 0);
        let total = s.total();
        for k in 0..total {
            // every home is the last home position; the run fills the tail
            assert!(s.try_insert(&raw(u32::MAX, k as u64 + 1), 0) || k >= total - (16 - 1));
        }
        s.check().unwrap();
        let before = s.ents.as_slice().to_vec();
        assert!(!s.try_insert(&raw(u32::MAX, 999), 0));
        assert_eq!(before, s.ents.as_slice());
        assert!(
            Segment::build(
                16,
                0,
                s.iter_range(0..total).chain([(raw(u32::MAX, 999), 0)])
            )
            .is_none()
        );
    }
}
