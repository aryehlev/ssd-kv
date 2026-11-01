//! Standalone multi-partition reads and deletes (DESIGN §6.8, LC-37).
//!
//! MGET, EXISTS, DEL, UNLINK and TOUCH whose keys map to several partitions are split by
//! `FanOutSplit` (a fixed per-command routine) into exactly one sub-request per distinct
//! partition, each carrying key bytes only, and gathered in the connection's reply slot. Not
//! atomic across partitions (documented). Cluster mode never fans out: keys must share one slot.

use crate::cmd::FanOutSplit;

/// Splits the key arguments at `pos` by partition.
pub fn split(argv: &[&[u8]], pos: &[u16], partitions: u16) -> FanOutSplit {
    todo!("DESIGN §6.8: fan-out split")
}
