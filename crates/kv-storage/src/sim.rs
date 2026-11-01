//! Simulated block device (DESIGN §17.1, §25.3, §25.4; feature `sim`).
//!
//! `SimDevice` models the failure model of §17.1: a durable byte image plus a volatile
//! write-back set; FLUSH makes completed writes durable; `crash` (kill -9 model) keeps completed
//! writes and loses unsubmitted ones; `power_cut` drops unflushed writes and tears writes in
//! flight at byte, sector or prefix granularity, except `RWF_ATOMIC` writes no larger than the
//! atomic unit, which are all-or-nothing. Deterministic under its seed.
//!
//! It keeps only the durable image and the volatile write-back set in RAM; it records no
//! per-write history (LC-33). Used by `kv-sim` and by `tests/crash/faultdev`.

use crate::io::{BlockDevice, DevInfo, IoDone, IoReq};

/// How in-flight writes are torn by `crash` / `power_cut`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TearMode {
    None,
    Bytes,
    Sectors,
    Prefix,
}

pub struct SimDevice {
    pages: u64,
    lba: u32,
    atomic_unit: Option<u32>,
    rng: u64,
    /// The durable byte image.
    durable: Vec<u8>,
    /// Completed but unflushed sector contents, by sector index.
    volatile: hashbrown::HashMap<u64, Box<[u8]>, foldhash::fast::RandomState>,
    /// Submitted, not yet completed.
    in_flight: Vec<IoReq>,
    done: Vec<IoDone>,
}

impl SimDevice {
    pub fn new(pages: u64, lba: u32, atomic_unit: Option<u32>, seed: u64) -> Self {
        todo!("DESIGN §25.3: SimDevice")
    }

    /// kill -9 model.
    pub fn crash(&mut self, mode: TearMode) {
        todo!("DESIGN §17.1: crash")
    }

    /// Power loss model.
    pub fn power_cut(&mut self, mode: TearMode) {
        todo!("DESIGN §17.1: power cut")
    }

    pub fn durable_image(&self) -> &[u8] {
        &self.durable
    }
}

impl BlockDevice for SimDevice {
    fn info(&self) -> DevInfo {
        todo!("DESIGN §25.3: SimDevice info")
    }

    fn submit(&mut self, r: IoReq) {
        todo!("DESIGN §25.3: SimDevice submit")
    }

    fn poll(&mut self, out: &mut Vec<IoDone>, wait: bool) {
        todo!("DESIGN §25.3: SimDevice completions")
    }
}
