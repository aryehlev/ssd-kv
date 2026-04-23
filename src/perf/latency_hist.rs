//! Lock-free latency histograms with log-bucket atomic counters.
//!
//! Each observation is a u64 latency in microseconds. Bucket `i`
//! counts observations in `[2^i, 2^(i+1))` µs. With 24 buckets we
//! cover 1 µs up to 16 seconds — beyond that everything clamps into
//! the last bucket.
//!
//! Recording is a single atomic fetch_add on the bucket plus another
//! on the running count and sum — ~3 relaxed atomics, nanoseconds of
//! overhead on the hot path. Percentiles are computed by walking the
//! buckets at read time (rare), which is cheap since there are only
//! 24 of them.
//!
//! Why not `hdrhistogram`? That crate is excellent but synchronous by
//! default, and its recorder needs a Mutex or per-thread instance +
//! merge. Log buckets are coarser (factor-of-2 precision) but fully
//! lock-free, and "p99 of 1.3 ms" is plenty of precision for
//! operability — to the nearest doubling.

use std::sync::atomic::{AtomicU64, Ordering};

const NUM_BUCKETS: usize = 24;

#[derive(Debug)]
pub struct LatencyHistogram {
    /// buckets[i] = count of observations with latency in
    /// [2^i, 2^(i+1)) microseconds. bucket 0 is [0, 2) — which covers
    /// sub-µs observations too.
    buckets: [AtomicU64; NUM_BUCKETS],
    count: AtomicU64,
    total_us: AtomicU64,
    max_us: AtomicU64,
}

impl LatencyHistogram {
    pub const fn new() -> Self {
        // Can't use [AtomicU64::new(0); N] in const context pre-1.79.
        const ZERO: AtomicU64 = AtomicU64::new(0);
        Self {
            buckets: [ZERO; NUM_BUCKETS],
            count: AtomicU64::new(0),
            total_us: AtomicU64::new(0),
            max_us: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn record(&self, latency_us: u64) {
        let bucket = bucket_for(latency_us);
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total_us.fetch_add(latency_us, Ordering::Relaxed);

        // Racy max update is fine — if two observations both try to
        // set a larger max, at least one wins; the other's value is
        // dropped but the monotone invariant holds.
        let mut prev = self.max_us.load(Ordering::Relaxed);
        while latency_us > prev {
            match self.max_us.compare_exchange_weak(
                prev,
                latency_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(p) => prev = p,
            }
        }
    }

    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    pub fn mean_us(&self) -> u64 {
        let c = self.count.load(Ordering::Relaxed);
        if c == 0 {
            0
        } else {
            self.total_us.load(Ordering::Relaxed) / c
        }
    }

    pub fn max_us(&self) -> u64 {
        self.max_us.load(Ordering::Relaxed)
    }

    /// Approximate percentile in microseconds. Returns the upper bound
    /// of the bucket that crosses the target fraction — coarser than
    /// HDR but always within 2× of the true value.
    pub fn percentile(&self, p: f64) -> u64 {
        let p = p.clamp(0.0, 1.0);
        let total: u64 = self
            .buckets
            .iter()
            .map(|b| b.load(Ordering::Relaxed))
            .sum();
        if total == 0 {
            return 0;
        }
        let target = ((total as f64) * p).ceil() as u64;
        let mut cumulative = 0u64;
        for (i, b) in self.buckets.iter().enumerate() {
            cumulative += b.load(Ordering::Relaxed);
            if cumulative >= target {
                // Upper bound of this bucket. Bucket 0 represents
                // [0, 2) so return 1µs; general case: 2^(i+1).
                return 1u64 << (i + 1).min(NUM_BUCKETS - 1);
            }
        }
        1u64 << NUM_BUCKETS
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

#[inline]
fn bucket_for(latency_us: u64) -> usize {
    if latency_us < 2 {
        return 0;
    }
    let bits = 64 - latency_us.leading_zeros() as usize;
    bits.saturating_sub(1).min(NUM_BUCKETS - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_histogram_reports_zero() {
        let h = LatencyHistogram::new();
        assert_eq!(h.count(), 0);
        assert_eq!(h.percentile(0.5), 0);
        assert_eq!(h.percentile(0.99), 0);
    }

    #[test]
    fn single_observation_hits_expected_bucket() {
        let h = LatencyHistogram::new();
        h.record(100); // between 64 and 128 → bucket 6
        assert_eq!(h.count(), 1);
        let p50 = h.percentile(0.5);
        // Bucket 6 upper bound is 2^7 = 128
        assert_eq!(p50, 128);
    }

    #[test]
    fn bimodal_distribution_percentiles_split() {
        let h = LatencyHistogram::new();
        // 99 fast observations (~100 µs each), 1 slow (~50 ms).
        for _ in 0..99 {
            h.record(100);
        }
        h.record(50_000);
        let p50 = h.percentile(0.5);
        let p99 = h.percentile(0.99);
        let p999 = h.percentile(0.999);
        assert!(p50 <= 128, "p50={}", p50);
        assert!(p99 <= 128, "p99={}", p99); // 99% of obs in fast bucket
        assert!(p999 >= 32_768, "p999={}", p999); // slow obs visible in tail
    }

    #[test]
    fn bucket_saturation_clamps() {
        let h = LatencyHistogram::new();
        h.record(u64::MAX / 2);
        // Should clamp to last bucket, no panic.
        assert_eq!(h.count(), 1);
        let _ = h.percentile(0.5);
    }
}
