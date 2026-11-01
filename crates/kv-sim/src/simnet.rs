//! Simulated network (DESIGN §25.3): seeded message drop, delay, duplication and partitions,
//! per-process clock rate drift up to 1e-3, process crash and restart. Single-threaded and
//! deterministic under its seed.

/// Simulated time in nanoseconds.
pub type SimTime = u64;

/// Fault probabilities and bounds of one run.
#[derive(Clone, Copy, Debug)]
pub struct NetFaults {
    pub drop: f64,
    pub duplicate: f64,
    pub max_delay_ns: u64,
    pub max_clock_drift: f64,
}

pub struct SimNet {
    seed: u64,
    now: SimTime,
    faults: NetFaults,
}

impl SimNet {
    pub fn new(seed: u64, faults: NetFaults) -> Self {
        todo!("DESIGN §25.3: simulated network")
    }
}
