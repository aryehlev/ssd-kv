//! Replication backlogs (DESIGN §23.7, LC-23).
//!
//! A per-partition RAM ring of encoded groups (`repl-backlog-size`, default 32 MiB) with a
//! sparse seq -> offset index every 64 KiB; the oldest groups are overwritten. Members keep a
//! ring of received groups (repair key enumeration after a failover). The ring memory is a
//! huge-page mapping (not swappable) or `mlock`ed, and marked `MADV_DONTDUMP`; that mapping is
//! obtained through the safe `LockedMem` wrapper of the storage crate's I/O module, since this
//! crate contains no `unsafe` (§24). A backlog is never persisted in any form: no file, no
//! descriptor, no swap, no core dump. Compile-time assertions below keep I/O traits off the
//! type.
//!
//! Entries are the self-delimiting encodings of `codec` (`u32 len | u64 seq | ...`), stored
//! back to back; an entry may wrap around the end of the ring. A group larger than the whole
//! ring is not retained (it is counted as evicted at once).

use std::collections::VecDeque;

use kv_storage::io::LockedMem;

use crate::Seq;
use crate::codec::{self, GROUP_LEN_PREFIX};

/// Spacing of the sparse seq -> offset index.
const INDEX_EVERY: usize = 64 << 10;

pub struct Backlog {
    /// Ring bytes (locked, not dumpable).
    ring: LockedMem,
    /// Offset of the oldest retained group.
    head: usize,
    /// Retained bytes.
    used: usize,
    /// Sparse `(seq, ring offset)` index, one entry per 64 KiB, oldest first.
    index: VecDeque<(Seq, usize)>,
    /// Bytes pushed since the last index entry.
    since_index: usize,
    first: Option<Seq>,
    last: Option<Seq>,
    /// Every group at or below this seq is gone (evicted, too large, or older than the
    /// backlog itself).
    floor: Seq,
    /// Retained groups.
    count: usize,
}

static_assertions::assert_not_impl_any!(
    Backlog: std::io::Read,
    std::io::Write,
    std::os::fd::AsRawFd,
    std::os::fd::AsFd
);

impl Backlog {
    /// A ring of `cap` bytes in locked, non-dumpable memory.
    ///
    /// # Panics
    ///
    /// When the memory cannot be locked; [`Backlog::try_new`] reports that instead (the node
    /// checks it at startup and refuses cluster mode, LC-23).
    pub fn new(cap: usize) -> Self {
        match Self::try_new(cap) {
            Ok(b) => b,
            Err(e) => panic!("replication backlog memory cannot be locked (raise LimitMEMLOCK): {e}"),
        }
    }

    /// A ring of `cap` bytes, or the reason the memory could not be locked.
    pub fn try_new(cap: usize) -> std::io::Result<Self> {
        let ring = LockedMem::new(cap.max(64))?;
        Ok(Backlog {
            ring,
            head: 0,
            used: 0,
            index: VecDeque::new(),
            since_index: 0,
            first: None,
            last: None,
            floor: Seq::ZERO,
            count: 0,
        })
    }

    /// Ring capacity in bytes.
    pub fn capacity(&self) -> usize {
        self.ring.len()
    }

    /// Bytes retained.
    pub fn used_bytes(&self) -> usize {
        self.used
    }

    /// Groups retained.
    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Whether the ring memory is backed by huge pages (else `mlock`ed small pages).
    pub fn is_huge(&self) -> bool {
        self.ring.is_huge()
    }

    /// Address of the ring memory (diagnostics and the `backlog_never_reaches_disk` test).
    pub fn ring_addr(&self) -> usize {
        self.ring.as_slice().as_ptr() as usize
    }

    /// Declares that nothing at or below `s` will ever be held (a new primary's backlog starts
    /// at its term's first seq: `start_after(Seq::new(term, 0))`).
    pub fn start_after(&mut self, s: Seq) {
        self.floor = self.floor.max(s);
    }

    /// Drops every retained group (a partition's stream restarts from scratch).
    pub fn clear(&mut self) {
        if let Some(l) = self.last {
            self.floor = self.floor.max(l);
        }
        self.head = 0;
        self.used = 0;
        self.index.clear();
        self.since_index = 0;
        self.first = None;
        self.count = 0;
    }

    fn copy_in(&mut self, at: usize, b: &[u8]) {
        let cap = self.capacity();
        let first = (cap - at).min(b.len());
        let ring = self.ring.as_mut_slice();
        ring[at..at + first].copy_from_slice(&b[..first]);
        ring[..b.len() - first].copy_from_slice(&b[first..]);
    }

    fn copy_out(&self, at: usize, n: usize, out: &mut Vec<u8>) {
        let cap = self.capacity();
        let first = (cap - at).min(n);
        let ring = self.ring.as_slice();
        out.extend_from_slice(&ring[at..at + first]);
        out.extend_from_slice(&ring[..n - first]);
    }

    /// `(total bytes, seq)` of the entry at ring offset `at`.
    fn entry_at(&self, at: usize) -> (usize, Seq) {
        let mut h = Vec::with_capacity(GROUP_LEN_PREFIX + 8);
        self.copy_out(at, GROUP_LEN_PREFIX + 8, &mut h);
        let len = u32::from_le_bytes([h[0], h[1], h[2], h[3]]) as usize;
        let mut s = [0u8; 8];
        s.copy_from_slice(&h[4..12]);
        (GROUP_LEN_PREFIX + len, Seq(u64::from_le_bytes(s)))
    }

    fn evict_one(&mut self) {
        let (n, seq) = self.entry_at(self.head);
        self.head = (self.head + n) % self.capacity();
        self.used -= n;
        self.count -= 1;
        self.floor = self.floor.max(seq);
        while self.index.front().is_some_and(|&(s, _)| s <= seq) {
            self.index.pop_front();
        }
        if self.count == 0 {
            self.first = None;
            self.head = 0;
            self.used = 0;
            self.since_index = 0;
        } else {
            self.first = Some(self.entry_at(self.head).1);
        }
    }

    /// Stores one encoded group (its seq must be above every seq pushed before).
    pub fn push(&mut self, seq: Seq, encoded: &[u8]) {
        debug_assert!(self.last.is_none_or(|l| seq > l), "backlog seqs must increase");
        debug_assert_eq!(codec::peek_seq(encoded), Some(seq));
        let n = encoded.len();
        if n > self.capacity() || codec::peek_len(encoded) != Some(n) {
            // never retained: every older group is unreachable through this backlog too
            self.clear();
            self.floor = self.floor.max(seq);
            self.last = Some(seq);
            return;
        }
        while self.used + n > self.capacity() {
            self.evict_one();
        }
        let at = (self.head + self.used) % self.capacity();
        self.copy_in(at, encoded);
        if self.index.is_empty() || self.since_index >= INDEX_EVERY {
            self.index.push_back((seq, at));
            self.since_index = 0;
        }
        self.since_index += n;
        self.used += n;
        self.count += 1;
        if self.first.is_none() {
            self.first = Some(seq);
        }
        self.last = Some(seq);
    }

    pub fn first(&self) -> Option<Seq> {
        self.first
    }

    pub fn last(&self) -> Option<Seq> {
        self.last
    }

    /// Every group at or below this seq is unavailable.
    pub fn floor(&self) -> Seq {
        self.floor
    }

    /// Whether every group after `s` is still retained.
    pub fn covers_after(&self, s: Seq) -> bool {
        s >= self.floor
    }

    /// Ring offset of the first retained group with seq > `s` (`None`: no such group).
    fn locate_after(&self, s: Seq) -> Option<usize> {
        if self.count == 0 || self.last.is_none_or(|l| l <= s) {
            return None;
        }
        // the last index entry at or below `s`, else the oldest group
        let start = match self.index.partition_point(|&(q, _)| q <= s) {
            0 => self.head,
            i => self.index[i - 1].1,
        };
        let cap = self.capacity();
        let mut at = start;
        let mut walked = (at + cap - self.head) % cap;
        while walked < self.used {
            let (n, seq) = self.entry_at(at);
            if seq > s {
                return Some(at);
            }
            at = (at + n) % cap;
            walked += n;
        }
        None
    }

    /// Appends whole encoded groups after `s` (up to `max_bytes`, at least one group); returns
    /// the last seq copied.
    pub fn read_after(&self, s: Seq, max_bytes: usize, out: &mut Vec<u8>) -> Option<Seq> {
        let mut at = self.locate_after(s)?;
        let cap = self.capacity();
        let mut walked = (at + cap - self.head) % cap;
        let mut copied = 0usize;
        let mut last = None;
        while walked < self.used {
            let (n, seq) = self.entry_at(at);
            if last.is_some() && copied + n > max_bytes {
                break;
            }
            self.copy_out(at, n, out);
            copied += n;
            last = Some(seq);
            at = (at + n) % cap;
            walked += n;
        }
        last
    }

    /// Visits every retained group with seq in `(after, upto]` as one contiguous buffer.
    fn for_each_between(&self, after: Seq, upto: Seq, f: &mut dyn FnMut(&[u8])) {
        let Some(mut at) = self.locate_after(after) else { return };
        let cap = self.capacity();
        let mut walked = (at + cap - self.head) % cap;
        let mut buf = Vec::new();
        while walked < self.used {
            let (n, seq) = self.entry_at(at);
            if seq > upto {
                break;
            }
            buf.clear();
            self.copy_out(at, n, &mut buf);
            f(&buf);
            at = (at + n) % cap;
            walked += n;
        }
    }

    /// The `(db, key)` pairs of groups in `(after, upto]`, `None` if not covered.
    pub fn keys_between(&self, after: Seq, upto: Seq) -> Option<Vec<(u8, Box<[u8]>)>> {
        let mut ks = codec::KeySet::default();
        self.keys_between_into(after, upto, &mut ks)?;
        Some(ks.into_vec())
    }

    /// [`Backlog::keys_between`] into an existing set; `Some(wide)` tells whether any of those
    /// groups holds a partition-wide op (`FlushDb`, `FlushSlot`), `None` if not covered.
    pub fn keys_between_into(&self, after: Seq, upto: Seq, out: &mut codec::KeySet) -> Option<bool> {
        if upto <= after {
            return Some(false);
        }
        if !self.covers_after(after) {
            return None;
        }
        let mut wide = false;
        let mut bad = false;
        self.for_each_between(after, upto, &mut |g| match codec::group_keys_and_wide(g, out) {
            Ok(w) => wide |= w,
            Err(_) => bad = true,
        });
        (!bad).then_some(wide)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Group, Op};

    fn group(seq: Seq, key: &[u8], vlen: usize) -> Vec<u8> {
        let mut b = Vec::new();
        codec::encode_group(&Group { seq, ops: vec![Op::Put { db: 0, vtype: 1, expire_at: 0, key: key.into(), value: vec![7; vlen] }] }, &mut b);
        b
    }

    #[test]
    fn push_read_and_evict() {
        let mut b = Backlog::new(4096);
        b.start_after(Seq::new(1, 0));
        assert!(b.covers_after(Seq::new(1, 0)));
        assert!(!b.covers_after(Seq::ZERO));
        let mut sizes = Vec::new();
        for i in 1..=200u64 {
            let g = group(Seq::new(1, i), format!("k{i}").as_bytes(), (i % 50) as usize);
            sizes.push(g.len());
            b.push(Seq::new(1, i), &g);
            assert!(b.used_bytes() <= b.capacity());
        }
        assert_eq!(b.last(), Some(Seq::new(1, 200)));
        let first = b.first().unwrap();
        assert!(first > Seq::new(1, 1), "old groups were overwritten");
        assert!(b.covers_after(Seq(first.0 - 1)));
        assert!(!b.covers_after(Seq(first.0 - 2)));
        // reads return whole groups in order, from right after the asked seq
        let mut out = Vec::new();
        let last = b.read_after(Seq(first.0 + 3), usize::MAX, &mut out).unwrap();
        assert_eq!(last, Seq::new(1, 200));
        let seqs: Vec<Seq> = codec::iter_groups(&out).map(|(s, _)| s).collect();
        assert_eq!(seqs.first(), Some(&Seq(first.0 + 4)));
        assert_eq!(seqs.len() as u64, 200 - (first.counter() + 3));
        // byte bound: at least one group, never a partial one
        let mut out = Vec::new();
        let l = b.read_after(Seq(first.0 - 1), 1, &mut out).unwrap();
        assert_eq!(l, first);
        assert_eq!(codec::iter_groups(&out).count(), 1);
        assert!(b.read_after(Seq::new(1, 200), 100, &mut Vec::new()).is_none());
        // keys of a covered range; an uncovered range is None
        let ks = b.keys_between(Seq::new(1, 197), Seq::new(1, 199)).unwrap();
        assert_eq!(ks, vec![(0, b"k198".as_slice().into()), (0, b"k199".as_slice().into())]);
        assert!(b.keys_between(Seq::new(1, 1), Seq::new(1, 199)).is_none());
    }

    #[test]
    fn oversize_groups_are_not_retained() {
        let mut b = Backlog::new(1024);
        b.push(Seq::new(1, 1), &group(Seq::new(1, 1), b"a", 10));
        b.push(Seq::new(1, 2), &group(Seq::new(1, 2), b"b", 5000));
        assert!(b.is_empty());
        assert!(!b.covers_after(Seq::new(1, 1)));
        assert!(b.covers_after(Seq::new(1, 2)));
        b.push(Seq::new(1, 3), &group(Seq::new(1, 3), b"c", 10));
        let mut out = Vec::new();
        assert_eq!(b.read_after(Seq::new(1, 2), usize::MAX, &mut out), Some(Seq::new(1, 3)));
    }

    #[test]
    fn wrapping_entries_read_back_intact() {
        let mut b = Backlog::new(1000);
        let mut all = Vec::new();
        for i in 1..=50u64 {
            let g = group(Seq::new(2, i), &[i as u8; 9], 37 + (i as usize % 13));
            all.push(g.clone());
            b.push(Seq::new(2, i), &g);
        }
        let first = b.first().unwrap();
        let mut out = Vec::new();
        b.read_after(Seq(first.0 - 1), usize::MAX, &mut out);
        let want: Vec<u8> = all[(first.counter() - 1) as usize..].concat();
        assert_eq!(out, want);
    }
}
