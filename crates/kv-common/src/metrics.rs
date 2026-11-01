//! Latency histograms (DESIGN §21).
//!
//! Per-worker `hdrhistogram::Histogram<u32>` covering 1 us .. 60 s with 3 significant digits,
//! merged on demand for INFO latencystats and `/metrics`. Values outside the range saturate.

/// Highest trackable latency (60 s) in microseconds.
pub const HIST_MAX_US: u64 = 60_000_000;

/// A latency histogram in microseconds (1 us .. 60 s, 3 significant digits).
pub struct Hist(hdrhistogram::Histogram<u32>);

impl Hist {
    pub fn new() -> Self {
        Hist(
            hdrhistogram::Histogram::new_with_bounds(1, HIST_MAX_US, 3)
                .expect("static histogram bounds are valid"),
        )
    }

    /// Records one latency; values are clamped into `1 ..= HIST_MAX_US`, and a count that
    /// would overflow saturates.
    #[inline]
    pub fn record_us(&mut self, us: u64) {
        self.0.saturating_record(us.clamp(1, HIST_MAX_US));
    }

    /// Value at percentile `p` (0.0 ..= 100.0), in microseconds; 0 when empty.
    pub fn percentile(&self, p: f64) -> u64 {
        if self.0.is_empty() { 0 } else { self.0.value_at_percentile(p.clamp(0.0, 100.0)) }
    }

    /// Adds every sample of `o`.
    pub fn merge(&mut self, o: &Hist) {
        // Both histograms share the same static bounds, so `add` cannot fail.
        let _ = self.0.add(&o.0);
    }

    /// Number of recorded samples.
    pub fn count(&self) -> u64 {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Largest recorded value (0 when empty).
    pub fn max(&self) -> u64 {
        if self.0.is_empty() { 0 } else { self.0.max() }
    }

    /// Mean of the recorded values (0 when empty).
    pub fn mean(&self) -> f64 {
        if self.0.is_empty() { 0.0 } else { self.0.mean() }
    }

    /// Removes every sample (CONFIG RESETSTAT).
    pub fn reset(&mut self) {
        self.0.reset();
    }
}

impl Default for Hist {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for Hist {
    fn clone(&self) -> Self {
        Hist(self.0.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_and_merge() {
        let mut a = Hist::new();
        assert_eq!((a.percentile(99.0), a.max(), a.count()), (0, 0, 0));
        for us in 1..=1000 {
            a.record_us(us);
        }
        assert_eq!(a.count(), 1000);
        let p50 = a.percentile(50.0);
        let p99 = a.percentile(99.0);
        assert!((499..=501).contains(&p50), "{p50}");
        assert!((989..=991).contains(&p99), "{p99}");
        let mut b = Hist::new();
        b.record_us(0); // clamped to 1
        b.record_us(u64::MAX); // clamped to 60 s
        a.merge(&b);
        assert_eq!(a.count(), 1002);
        assert!(a.max() >= HIST_MAX_US * 999 / 1000);
        assert!(a.mean() > 0.0);
        let c = a.clone();
        a.reset();
        assert!(a.is_empty());
        assert_eq!(c.count(), 1002);
    }
}
