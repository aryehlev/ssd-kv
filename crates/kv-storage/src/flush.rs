//! Flush batching (DESIGN §13.8).
//!
//! One `FlushTracker` per worker per device. Write-through devices (and `RWF_DSYNC` writes) are
//! durable at completion and issue no flushes. On write-back devices each completion records the
//! flush epoch it needs (`submitted_epoch + 1`); when completed-but-unflushed writes exist and no
//! flush is in flight, one `fdatasync`-style flush is submitted; on its completion every write
//! whose epoch is covered is durable. Flushes are event driven, never timed; edge E1 makes them
//! mandatory before re-writing a page on write-back devices in every durability mode.
//!
//! Routing: a [`WriteId`] carries its partition in the top 16 bits so that the worker can hand
//! the writes a flush made durable back to their partitions; flush requests carry
//! [`FLUSH_TAG_PARTITION`] as their `IoTag::partition` and the device index as `IoTag::id`.

use crate::io::{IoReq, IoTag};

/// A tracked page or extent write: `partition << 48 | partition-local id`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct WriteId(pub u64);

const LOCAL_BITS: u32 = 48;
const LOCAL_MASK: u64 = (1u64 << LOCAL_BITS) - 1;

impl WriteId {
    /// The write `local` of `partition` (`local` is truncated to 48 bits).
    pub fn new(partition: u16, local: u64) -> Self {
        WriteId((u64::from(partition) << LOCAL_BITS) | (local & LOCAL_MASK))
    }

    /// The partition that issued the write (routes durable writes back, §6.4 step 1).
    pub fn partition(self) -> u16 {
        (self.0 >> LOCAL_BITS) as u16
    }

    /// The partition-local part of the id.
    pub fn local(self) -> u64 {
        self.0 & LOCAL_MASK
    }
}

/// `IoTag::partition` of every flush request; `IoTag::id` is the device index.
pub const FLUSH_TAG_PARTITION: u16 = 0xFFFF;

/// The device of a flush completion, `None` for any other tag.
pub fn flush_tag_dev(tag: IoTag) -> Option<u8> {
    (tag.partition == FLUSH_TAG_PARTITION).then_some(tag.id as u8)
}

pub struct FlushTracker {
    dev: u8,
    write_through: bool,
    submitted_epoch: u64,
    completed_epoch: u64,
    flush_in_flight: bool,
    /// Completed writes waiting for a covering flush: `(write, epoch needed)`.
    waiting: Vec<(WriteId, u64)>,
    flushes: u64,
    /// When the flush in flight was submitted, with the writes it covers (stall trace only).
    traced: Option<(std::time::Instant, usize)>,
}

impl FlushTracker {
    pub fn new(dev: u8, write_through: bool) -> Self {
        FlushTracker {
            dev,
            write_through,
            submitted_epoch: 0,
            completed_epoch: 0,
            flush_in_flight: false,
            waiting: Vec::new(),
            flushes: 0,
            traced: None,
        }
    }

    pub fn dev(&self) -> u8 {
        self.dev
    }

    /// True when completions are durable without a flush.
    pub fn write_through(&self) -> bool {
        self.write_through
    }

    /// A write completed; returns it when it is durable now (write-through).
    pub fn on_write_done(&mut self, w: WriteId) -> Option<WriteId /*durable now*/> {
        if self.write_through {
            return Some(w);
        }
        // A flush already in flight was submitted before this completion: it does not cover it.
        self.waiting.push((w, self.submitted_epoch + 1));
        None
    }

    /// Submits at most one flush when completed writes are waiting and none is in flight.
    pub fn poll(&mut self, io: &mut Vec<IoReq>) {
        if self.write_through || self.flush_in_flight || self.waiting.is_empty() {
            return;
        }
        self.submitted_epoch += 1;
        self.flush_in_flight = true;
        self.flushes += 1;
        if crate::progress::stall_trace_ms().is_some() {
            self.traced = Some((std::time::Instant::now(), self.waiting.len()));
        }
        io.push(IoReq::flush(IoTag { partition: FLUSH_TAG_PARTITION, id: u64::from(self.dev) }, self.dev));
    }

    /// The submitted flush completed; returns the writes that are durable now.
    pub fn on_flush_done(&mut self) -> Vec<WriteId> {
        let mut out = Vec::new();
        self.on_flush_done_into(&mut out);
        out
    }

    /// [`FlushTracker::on_flush_done`] into a caller-owned vector (no allocation per flush).
    pub fn on_flush_done_into(&mut self, out: &mut Vec<WriteId>) {
        if !self.flush_in_flight {
            return;
        }
        self.flush_in_flight = false;
        self.completed_epoch = self.submitted_epoch;
        let done = self.completed_epoch;
        // `KVSSD_STALL_TRACE_MS`: a flush slower than the limit is printed
        if let (Some(limit), Some((t0, n))) = (crate::progress::stall_trace_ms(), self.traced.take()) {
            let ms = t0.elapsed().as_millis() as u64;
            if ms >= limit {
                eprintln!("kvssd stall trace flush dev{}: {ms}ms covering {n} writes ({} waiting now)", self.dev, self.waiting.len());
            }
        }
        self.waiting.retain(|&(w, need)| {
            if need <= done {
                out.push(w);
                false
            } else {
                true
            }
        });
    }

    /// The submitted flush failed: the writes it covered may be lost and are returned so that
    /// their owners fail the affected tickets and go read-only (§22). Later writes stay waiting.
    pub fn on_flush_failed(&mut self) -> Vec<WriteId> {
        let mut out = Vec::new();
        if !self.flush_in_flight {
            return out;
        }
        self.flush_in_flight = false;
        let covered = self.submitted_epoch;
        self.waiting.retain(|&(w, need)| {
            if need <= covered {
                out.push(w);
                false
            } else {
                true
            }
        });
        out
    }

    /// Completed writes not yet durable.
    pub fn waiting(&self) -> usize {
        self.waiting.len()
    }

    pub fn flush_in_flight(&self) -> bool {
        self.flush_in_flight
    }

    /// Flushes submitted so far (INFO `flushes_total`).
    pub fn flushes(&self) -> u64 {
        self.flushes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::IoOp;

    fn w(n: u64) -> WriteId {
        WriteId::new(3, n)
    }

    #[test]
    fn write_id_packing() {
        let id = WriteId::new(127, 0xABCD_EF01_2345);
        assert_eq!(id.partition(), 127);
        assert_eq!(id.local(), 0xABCD_EF01_2345);
        assert_eq!(WriteId::new(1, u64::MAX).local(), LOCAL_MASK);
    }

    #[test]
    fn write_through_is_durable_at_completion() {
        let mut t = FlushTracker::new(0, true);
        assert_eq!(t.on_write_done(w(1)), Some(w(1)));
        let mut io = Vec::new();
        t.poll(&mut io);
        assert!(io.is_empty());
        assert_eq!(t.waiting(), 0);
    }

    #[test]
    fn one_flush_covers_writes_completed_before_it() {
        let mut t = FlushTracker::new(2, false);
        let mut io = Vec::new();
        t.poll(&mut io);
        assert!(io.is_empty(), "no flush without completed writes");
        assert_eq!(t.on_write_done(w(1)), None);
        assert_eq!(t.on_write_done(w(2)), None);
        t.poll(&mut io);
        assert_eq!(io.len(), 1);
        assert!(matches!(io[0].op, IoOp::Flush));
        assert_eq!(flush_tag_dev(io[0].tag), Some(2));
        // a write completing while the flush is in flight needs the next flush
        assert_eq!(t.on_write_done(w(3)), None);
        t.poll(&mut io);
        assert_eq!(io.len(), 1, "at most one flush in flight");
        let mut durable = t.on_flush_done();
        durable.sort_by_key(|x| x.0);
        assert_eq!(durable, vec![w(1), w(2)]);
        t.poll(&mut io);
        assert_eq!(io.len(), 2);
        assert_eq!(t.on_flush_done(), vec![w(3)]);
        assert_eq!(t.waiting(), 0);
        assert_eq!(t.flushes(), 2);
        assert!(t.on_flush_done().is_empty(), "spurious completion is ignored");
    }

    #[test]
    fn failed_flush_returns_covered_writes() {
        let mut t = FlushTracker::new(0, false);
        let mut io = Vec::new();
        t.on_write_done(w(1));
        t.poll(&mut io);
        t.on_write_done(w(2));
        assert_eq!(t.on_flush_failed(), vec![w(1)]);
        assert!(!t.flush_in_flight());
        t.poll(&mut io);
        assert_eq!(t.on_flush_done(), vec![w(2)]);
    }

    #[test]
    fn other_tags_are_not_flushes() {
        assert_eq!(flush_tag_dev(IoTag { partition: 0, id: 1 }), None);
    }
}
