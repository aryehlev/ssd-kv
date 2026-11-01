//! Flush batching (DESIGN §13.8).
//!
//! One `FlushTracker` per worker per device. Write-through devices (and `RWF_DSYNC` writes) are
//! durable at completion and issue no flushes. On write-back devices each completion records the
//! flush epoch it needs (`submitted_epoch + 1`); when completed-but-unflushed writes exist and no
//! flush is in flight, one `fdatasync`-style flush is submitted; on its completion every write
//! whose epoch is covered is durable. Flushes are event driven, never timed; edge E1 makes them
//! mandatory before re-writing a page on write-back devices in every durability mode.

use crate::io::IoReq;

/// A tracked page or extent write.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct WriteId(pub u64);

pub struct FlushTracker {
    dev: u8,
    write_through: bool,
    submitted_epoch: u64,
    completed_epoch: u64,
    flush_in_flight: bool,
    /// Completed writes waiting for a covering flush: `(write, epoch needed)`.
    waiting: Vec<(WriteId, u64)>,
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
        }
    }

    /// A write completed; returns it when it is durable now (write-through).
    pub fn on_write_done(&mut self, w: WriteId) -> Option<WriteId /*durable now*/> {
        todo!("DESIGN §13.8: write completion")
    }

    /// Submits at most one flush when completed writes are waiting and none is in flight.
    pub fn poll(&mut self, io: &mut Vec<IoReq>) {
        todo!("DESIGN §13.8: flush submission")
    }

    /// The submitted flush completed; returns the writes that are durable now.
    pub fn on_flush_done(&mut self) -> Vec<WriteId> {
        todo!("DESIGN §13.8: flush completion")
    }
}
