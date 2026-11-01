//! Latency histograms (DESIGN §21).
//!
//! Per-worker `hdrhistogram::Histogram<u32>` covering 1 us .. 60 s with 3 significant digits,
//! merged on demand for INFO latencystats and `/metrics`. Values outside the range saturate.

/// Highest trackable latency (60 s) in microseconds.
pub const HIST_MAX_US: u64 = 60_000_000;

pub struct Hist(hdrhistogram::Histogram<u32>);

impl Hist {
    pub fn new() -> Self {
        Hist(
            hdrhistogram::Histogram::new_with_bounds(1, HIST_MAX_US, 3)
                .expect("static histogram bounds are valid"),
        )
    }

    pub fn record_us(&mut self, us: u64) {
        self.0.saturating_record(us.clamp(1, HIST_MAX_US));
    }

    /// Value at percentile `p` (0.0 ..= 100.0), in microseconds.
    pub fn percentile(&self, p: f64) -> u64 {
        self.0.value_at_percentile(p)
    }

    pub fn merge(&mut self, o: &Hist) {
        // Both histograms share the same static bounds, so `add` cannot fail.
        let _ = self.0.add(&o.0);
    }
}

impl Default for Hist {
    fn default() -> Self {
        Self::new()
    }
}
