//! Position trackers of the replication glue (DESIGN §23.7, §23.8, §23.13).
//!
//! [`Prefix`] keeps the contiguous done prefix of increasing positions: a primary's locally
//! durable groups (`local_durable`), a member's durable applied groups (its acknowledged
//! position). Entries become done out of order (their tickets complete in any order); the
//! prefix only moves over a run of done entries. A *gate* entry holds the prefix at a position
//! until the caller opens it (a repair or full sync is complete only when every image or
//! snapshot record is durable).
//!
//! [`Markers`] follows a migration destination's apply pipeline: items are numbered in stream
//! order; each yields the destination seq it wrote (or nothing); a source position marker is
//! acknowledged once every item before it executed and the destination's commit point covers
//! the highest seq they wrote. Both are pure position tests (LC-36).

use std::collections::VecDeque;

use kv_storage::layout::Seq;

/// The contiguous done prefix of increasing positions.
pub struct Prefix {
    /// `(position, done)` in increasing position order.
    items: VecDeque<(Seq, bool)>,
    /// Every position at or below this one is done.
    upto: Seq,
}

impl Prefix {
    /// A prefix in which everything up to `upto` is done.
    pub fn new(upto: Seq) -> Self {
        Prefix {
            items: VecDeque::new(),
            upto,
        }
    }

    /// The done prefix.
    pub fn upto(&self) -> Seq {
        self.upto
    }

    /// Restarts at `upto` (a new stream position); entries are dropped.
    pub fn reset(&mut self, upto: Seq) {
        self.items.clear();
        self.upto = upto;
    }

    /// Adds an entry at `s` (above every entry added before; lower or equal positions are
    /// ignored: they are covered already). `done`: complete at once.
    pub fn push(&mut self, s: Seq, done: bool) {
        let last = self.items.back().map_or(self.upto, |&(x, _)| x);
        if s <= last {
            return;
        }
        self.items.push_back((s, done));
        self.advance();
    }

    /// Marks the entry at `s` done.
    pub fn done(&mut self, s: Seq) {
        if let Ok(i) = self.items.binary_search_by(|&(x, _)| x.cmp(&s)) {
            self.items[i].1 = true;
            self.advance();
        }
    }

    /// Entries not done yet.
    pub fn open(&self) -> usize {
        self.items.iter().filter(|(_, d)| !*d).count()
    }

    fn advance(&mut self) {
        while let Some(&(s, true)) = self.items.front() {
            self.upto = s;
            self.items.pop_front();
        }
    }
}

/// A migration destination's acknowledgement tracker.
pub struct Markers {
    /// Items submitted so far (the next item gets this index).
    next: u64,
    /// `(index, destination seq written)` of items still running or done but not yet passed.
    items: VecDeque<(u64, Option<Option<Seq>>)>,
    /// Every item below this index executed.
    executed_below: u64,
    /// Highest destination seq written by the executed prefix.
    hwm: Seq,
    /// `(source position, items before it)` of markers not acknowledged yet.
    markers: VecDeque<(Seq, u64)>,
}

impl Default for Markers {
    fn default() -> Self {
        Self::new()
    }
}

impl Markers {
    pub fn new() -> Self {
        Markers {
            next: 0,
            items: VecDeque::new(),
            executed_below: 0,
            hwm: Seq::ZERO,
            markers: VecDeque::new(),
        }
    }

    /// A new item in stream order; returns its index.
    pub fn item(&mut self) -> u64 {
        let i = self.next;
        self.next += 1;
        self.items.push_back((i, None));
        i
    }

    /// The index the next item gets.
    pub fn next_item(&self) -> u64 {
        self.next
    }

    /// Every item below this index executed.
    pub fn executed_below(&self) -> u64 {
        self.executed_below
    }

    /// The highest destination seq written by the executed prefix.
    pub fn written(&self) -> Seq {
        self.hwm
    }

    /// Item `i` executed, writing destination seq `wrote` (or nothing).
    pub fn executed(&mut self, i: u64, wrote: Option<Seq>) {
        if let Some(e) = self.items.iter_mut().find(|e| e.0 == i) {
            e.1 = Some(wrote);
        }
        while let Some(&(idx, Some(w))) = self.items.front() {
            if let Some(s) = w {
                self.hwm = self.hwm.max(s);
            }
            self.executed_below = idx + 1;
            self.items.pop_front();
        }
    }

    /// A source position marker after every item so far.
    pub fn marker(&mut self, pos: Seq) {
        self.markers.push_back((pos, self.next));
    }

    /// The highest source position whose items all executed and are committed (`commit` = the
    /// destination's commit point); acknowledged markers are dropped.
    pub fn acked(&mut self, commit: Seq) -> Option<Seq> {
        let mut out = None;
        while let Some(&(pos, before)) = self.markers.front() {
            if self.executed_below < before || commit < self.hwm {
                break;
            }
            out = Some(pos);
            self.markers.pop_front();
        }
        out
    }

    /// Items not executed yet.
    pub fn running(&self) -> usize {
        self.items.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_moves_over_contiguous_done_entries() {
        let mut p = Prefix::new(Seq(10));
        p.push(Seq(11), false);
        p.push(Seq(12), false);
        p.push(Seq(13), true);
        assert_eq!(p.upto(), Seq(10));
        p.done(Seq(12));
        assert_eq!(p.upto(), Seq(10), "11 still open");
        p.done(Seq(11));
        assert_eq!(p.upto(), Seq(13));
        p.push(Seq(9), false);
        assert_eq!(p.open(), 0, "covered positions are ignored");
        // a gate holds the prefix until opened
        p.push(Seq(20), false);
        p.push(Seq(21), true);
        assert_eq!(p.upto(), Seq(13));
        p.done(Seq(20));
        assert_eq!(p.upto(), Seq(21));
        p.reset(Seq(5));
        assert_eq!(p.upto(), Seq(5));
    }

    #[test]
    fn markers_need_executed_items_and_the_commit_point() {
        let mut m = Markers::new();
        let a = m.item();
        let b = m.item();
        m.marker(Seq(100));
        let c = m.item();
        m.marker(Seq(200));
        assert_eq!(m.acked(Seq(1000)), None, "nothing executed");
        m.executed(b, Some(Seq(7)));
        assert_eq!(m.acked(Seq(1000)), None, "a is still running");
        m.executed(a, None);
        assert_eq!(m.acked(Seq(6)), None, "b's write is not committed");
        assert_eq!(m.acked(Seq(7)), Some(Seq(100)));
        m.executed(c, Some(Seq(9)));
        assert_eq!(m.acked(Seq(8)), None);
        assert_eq!(m.acked(Seq(9)), Some(Seq(200)));
        assert_eq!(m.acked(Seq(9)), None, "acknowledged once");
        // a marker with no items before it is acknowledged at once
        m.marker(Seq(300));
        assert_eq!(m.acked(Seq(9)), Some(Seq(300)));
        assert_eq!(m.running(), 0);
    }
}
