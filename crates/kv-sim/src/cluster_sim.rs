//! Cluster simulation (DESIGN §23.11, §25.3).
//!
//! 3 coordinators and 6 data nodes in-process over the simulated network. Checks: no
//! acknowledged write lost; at most one serving primary per shard per instant; writes of an old
//! term never commit after a fence; membership and slot map changed only by admin ops; zero
//! full-sync or migration bytes caused by failures alone; repaired members converge; per-key
//! read/write checks; failover picks by the static rule for every fence-reply pattern; merge
//! resync rewrites only divergent keys; migrations reach Ready exactly at recorded barriers.

/// One cluster simulation run's parameters.
#[derive(Clone, Copy, Debug)]
pub struct ClusterSimCfg {
    pub seed: u64,
    pub coordinators: u8,
    pub nodes: u8,
    pub steps: u64,
}

pub fn run(cfg: &ClusterSimCfg) -> Result<(), String> {
    todo!("DESIGN §25.3: cluster simulation")
}
