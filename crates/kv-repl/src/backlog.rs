//! Replication backlogs (DESIGN §23.7, LC-23).
//!
//! A per-partition RAM ring of encoded groups (`repl-backlog-size`, default 32 MiB) with a
//! sparse seq -> offset index every 64 KiB; the oldest groups are overwritten. Members keep a
//! ring of received groups (repair key enumeration after a failover). The ring memory is a
//! huge-page mapping (not swappable) or `mlock`ed, and marked `MADV_DONTDUMP`; that mapping is
//! obtained through a safe wrapper of the storage crate's I/O module, since this crate contains
//! no `unsafe` (§24). A backlog is never persisted in any form: no file, no descriptor, no swap,
//! no core dump. Compile-time assertions below keep I/O traits off the type.

use crate::Seq;

pub struct Backlog {
    /// Ring bytes.
    ring: Box<[u8]>,
    /// Offset of the oldest retained group.
    head: usize,
    /// Retained bytes.
    used: usize,
    /// Sparse `(seq, ring offset)` index, one entry per 64 KiB.
    index: Vec<(Seq, usize)>,
    first: Option<Seq>,
    last: Option<Seq>,
}

static_assertions::assert_not_impl_any!(
    Backlog: std::io::Read,
    std::io::Write,
    std::os::fd::AsRawFd,
    std::os::fd::AsFd
);

impl Backlog {
    pub fn new(cap: usize) -> Self {
        todo!("DESIGN §23.7: backlog ring")
    }

    pub fn push(&mut self, seq: Seq, encoded: &[u8]) {
        todo!("DESIGN §23.7: backlog push")
    }

    pub fn first(&self) -> Option<Seq> {
        self.first
    }

    pub fn last(&self) -> Option<Seq> {
        self.last
    }

    /// Whether every group after `s` is still retained.
    pub fn covers_after(&self, s: Seq) -> bool {
        todo!("DESIGN §23.7: backlog coverage")
    }

    /// Appends whole encoded groups after `s` (up to `max_bytes`); returns the last seq copied.
    pub fn read_after(&self, s: Seq, max_bytes: usize, out: &mut Vec<u8>) -> Option<Seq> {
        todo!("DESIGN §23.7: backlog read")
    }

    /// The `(db, key)` pairs of groups in `(after, upto]`, `None` if not covered.
    pub fn keys_between(&self, after: Seq, upto: Seq) -> Option<Vec<(u8, Box<[u8]>)>> {
        todo!("DESIGN §23.7: repair key enumeration")
    }
}
