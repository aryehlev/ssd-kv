//! Simulated network (DESIGN §25.3): seeded message drop, delay, duplication and partitions,
//! per-process clock rate drift up to 1e-3, process crash and restart. Single-threaded and
//! deterministic under its seed.
//!
//! Processes are numbered `0..n`. A message sent at global time `t` is delivered at
//! `t + delay` (uniform in `[min_delay_ns, max_delay_ns]`) unless it is dropped (probability
//! `drop`), the two processes are on different sides of a partition, or the receiver crashes
//! before delivery; with probability `duplicate` a second copy is scheduled with its own delay,
//! so delivery order is not send order. Each process has a clock rate `1 + d`,
//! `|d| <= max_clock_drift`: [`SimNet::after`] converts a local duration into the global time it
//! ends at. Nothing here records per-message traces (LC-33).

use std::collections::BTreeMap;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// Simulated time in nanoseconds.
pub type SimTime = u64;

/// Fault probabilities and bounds of one run.
#[derive(Clone, Copy, Debug)]
pub struct NetFaults {
    pub drop: f64,
    pub duplicate: f64,
    pub max_delay_ns: u64,
    pub max_clock_drift: f64,
    pub min_delay_ns: u64,
}

impl Default for NetFaults {
    /// A loss-free LAN: 50-500 us delays, no drift.
    fn default() -> Self {
        NetFaults {
            drop: 0.0,
            duplicate: 0.0,
            max_delay_ns: 500_000,
            max_clock_drift: 0.0,
            min_delay_ns: 50_000,
        }
    }
}

impl NetFaults {
    /// Lossy, duplicating, reordering network with the maximum clock drift of §25.3.
    pub fn hostile() -> Self {
        NetFaults {
            drop: 0.1,
            duplicate: 0.05,
            max_delay_ns: 20_000_000,
            max_clock_drift: 1e-3,
            min_delay_ns: 10_000,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Proc {
    up: bool,
    /// Clock rate minus one.
    drift: f64,
    /// Partition side (processes talk only within a side).
    side: u8,
}

pub struct SimNet<M = Vec<u8>> {
    seed: u64,
    now: SimTime,
    faults: NetFaults,
    rng: StdRng,
    procs: Vec<Proc>,
    /// `(delivery time, send number) -> (from, to, message)`.
    queue: BTreeMap<(SimTime, u64), (usize, usize, M)>,
    sent: u64,
    dropped: u64,
}

impl<M: Clone> SimNet<M> {
    pub fn new(seed: u64, faults: NetFaults) -> Self {
        SimNet {
            seed,
            now: 0,
            faults,
            rng: StdRng::seed_from_u64(seed),
            procs: Vec::new(),
            queue: BTreeMap::new(),
            sent: 0,
            dropped: 0,
        }
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Registers processes up to and including `p`, each with a drawn clock drift.
    pub fn add_process(&mut self, p: usize) {
        while self.procs.len() <= p {
            let d = self.faults.max_clock_drift;
            let drift = if d > 0.0 { self.rng.random_range(-d..=d) } else { 0.0 };
            self.procs.push(Proc {
                up: true,
                drift,
                side: 0,
            });
        }
    }

    pub fn processes(&self) -> usize {
        self.procs.len()
    }

    /// Global time.
    pub fn now(&self) -> SimTime {
        self.now
    }

    /// Process `p`'s local clock reading (drifted).
    pub fn local_time(&self, p: usize) -> u64 {
        (self.now as f64 * (1.0 + self.procs[p].drift)) as u64
    }

    /// The global time at which `local_ns` of process `p`'s clock will have elapsed from now.
    pub fn after(&self, p: usize, local_ns: u64) -> SimTime {
        self.now + (local_ns as f64 / (1.0 + self.procs[p].drift)) as u64
    }

    /// Advances global time (never backwards).
    pub fn advance_to(&mut self, t: SimTime) {
        self.now = self.now.max(t);
    }

    fn connected(&self, a: usize, b: usize) -> bool {
        self.procs[a].up && self.procs[b].up && self.procs[a].side == self.procs[b].side
    }

    fn delay(&mut self) -> u64 {
        let (lo, hi) = (self.faults.min_delay_ns, self.faults.max_delay_ns.max(self.faults.min_delay_ns));
        self.rng.random_range(lo..=hi)
    }

    /// Sends `m` from `from` to `to`, subject to the faults.
    pub fn send(&mut self, from: usize, to: usize, m: M) {
        if !self.connected(from, to) || self.rng.random_bool(self.faults.drop.clamp(0.0, 1.0)) {
            self.dropped += 1;
            return;
        }
        if self.rng.random_bool(self.faults.duplicate.clamp(0.0, 1.0)) {
            let t = self.now + self.delay();
            self.sent += 1;
            self.queue.insert((t, self.sent), (from, to, m.clone()));
        }
        let t = self.now + self.delay();
        self.sent += 1;
        self.queue.insert((t, self.sent), (from, to, m));
    }

    /// Delivery time of the next message in flight.
    pub fn next_delivery_time(&self) -> Option<SimTime> {
        self.queue.keys().next().map(|k| k.0)
    }

    /// Advances to the next delivery and returns `(to, message)`; messages whose endpoints were
    /// separated since they were sent are dropped on the way.
    pub fn deliver_next(&mut self) -> Option<(usize, M)> {
        while let Some((&(t, n), _)) = self.queue.iter().next() {
            let (from, to, m) = self.queue.remove(&(t, n))?;
            self.advance_to(t);
            if self.connected(from, to) {
                return Some((to, m));
            }
            self.dropped += 1;
        }
        None
    }

    /// Process `p` stops: every message in flight to it is lost.
    pub fn crash(&mut self, p: usize) {
        self.procs[p].up = false;
        self.queue.retain(|_, (_, to, _)| *to != p);
    }

    pub fn restart(&mut self, p: usize) {
        self.procs[p].up = true;
    }

    pub fn is_up(&self, p: usize) -> bool {
        self.procs[p].up
    }

    /// Splits the processes: those in `side` on one side, everybody else on the other.
    pub fn partition(&mut self, side: &[usize]) {
        for (i, pr) in self.procs.iter_mut().enumerate() {
            pr.side = u8::from(side.contains(&i));
        }
    }

    pub fn heal(&mut self) {
        for pr in &mut self.procs {
            pr.side = 0;
        }
    }

    /// Messages in flight.
    pub fn in_flight(&self) -> usize {
        self.queue.len()
    }

    /// `(scheduled, dropped)` message counts so far.
    pub fn counts(&self) -> (u64, u64) {
        (self.sent, self.dropped)
    }

    /// A random draw from the network's generator (for drivers that want one seeded stream).
    pub fn rng(&mut self) -> &mut StdRng {
        &mut self.rng
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_under_seed() {
        let run = |seed| {
            let mut n: SimNet<u32> = SimNet::new(seed, NetFaults::hostile());
            n.add_process(2);
            for i in 0..200u32 {
                n.send((i % 3) as usize, ((i + 1) % 3) as usize, i);
            }
            let mut got = Vec::new();
            while let Some((to, m)) = n.deliver_next() {
                got.push((to, m, n.now()));
            }
            got
        };
        assert_eq!(run(7), run(7));
        assert_ne!(run(7), run(8));
    }

    #[test]
    fn faults_drop_reorder_partition_crash() {
        let mut n: SimNet<u32> = SimNet::new(1, NetFaults::hostile());
        n.add_process(3);
        for i in 0..1000u32 {
            n.send(0, 1, i);
        }
        let mut got = Vec::new();
        while let Some((_, m)) = n.deliver_next() {
            got.push(m);
        }
        assert!(got.len() < 1000 + 100 && got.len() > 700, "{}", got.len());
        assert!(got.windows(2).any(|w| w[0] > w[1]), "reordering expected");

        n.partition(&[0, 1]);
        n.send(0, 2, 1);
        n.send(0, 1, 2);
        let mut got = Vec::new();
        while let Some((to, m)) = n.deliver_next() {
            got.push((to, m));
        }
        assert!(got.iter().all(|&(to, _)| to == 1));
        n.heal();

        let mut n: SimNet<u32> = SimNet::new(2, NetFaults::default());
        n.add_process(1);
        n.send(0, 1, 5);
        n.crash(1);
        assert_eq!(n.in_flight(), 0);
        n.restart(1);
        n.send(0, 1, 6);
        assert_eq!(n.deliver_next(), Some((1, 6)));
    }

    #[test]
    fn clock_drift_bounded() {
        let mut n: SimNet<u32> = SimNet::new(3, NetFaults::hostile());
        n.add_process(9);
        n.advance_to(1_000_000_000);
        for p in 0..10 {
            let local = n.local_time(p) as f64;
            assert!((local / 1e9 - 1.0).abs() <= 1e-3 + 1e-9);
            let end = n.after(p, 1_000_000);
            assert!(end >= n.now() + 998_000 && end <= n.now() + 1_002_000);
        }
    }
}
