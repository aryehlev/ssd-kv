//! Group-commit helper for the value log.
//!
//! Multiple concurrent writers call `sync_vlog(end_offset)` after appending.
//! The first caller that finds no flush in progress becomes the *leader* and
//! performs one `flush_and_sync()` on behalf of all concurrent writers whose
//! data was captured in the BufWriter flush.  All other callers that appended
//! *before* the leader's BufWriter flush wait and return once the leader's
//! fdatasync completes.  Callers that appended *after* the flush (rare narrow
//! window) see `fsynced_through < their end_offset` after waking and become
//! the next leader, issuing another fdatasync.
//!
//! Net effect: N concurrent writers pay for ≈ 1 fdatasync instead of N.

use std::io;
use std::sync::{Arc, Condvar, Mutex};

use crate::engine::value_log::ValueLog;

struct GcState {
    flushing: bool,
}

pub struct GroupCommit {
    vlog: Arc<ValueLog>,
    state: Mutex<GcState>,
    cv: Condvar,
}

impl GroupCommit {
    pub fn new(vlog: Arc<ValueLog>) -> Self {
        GroupCommit {
            vlog,
            state: Mutex::new(GcState { flushing: false }),
            cv: Condvar::new(),
        }
    }

    /// Ensure the value-log bytes ending at `end_offset` are durable.
    ///
    /// `end_offset` must be the write position *after* the caller's append
    /// (i.e., `append_return_offset + VLOG_HEADER_SIZE + key_len + value_len`).
    pub fn sync_vlog(&self, end_offset: u64) -> io::Result<()> {
        loop {
            // Fast path: a prior flush already covers our data.
            if self.vlog.fsynced_through() >= end_offset {
                return Ok(());
            }

            let is_leader = {
                let mut s = self.state.lock().unwrap();
                // Re-check under the state lock to close the TOCTOU window.
                if self.vlog.fsynced_through() >= end_offset {
                    return Ok(());
                }
                if s.flushing {
                    // Another thread is the leader; wait for it to finish.
                    drop(self.cv.wait(s).unwrap());
                    false // loop and re-check fsynced_through
                } else {
                    s.flushing = true;
                    true
                }
            };

            if is_leader {
                let result = self.vlog.flush_and_sync();
                {
                    let mut s = self.state.lock().unwrap();
                    s.flushing = false;
                }
                self.cv.notify_all();
                return result;
            }
            // Follower: loop back to the fast-path check.
        }
    }
}
