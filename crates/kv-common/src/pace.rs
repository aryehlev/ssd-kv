//! Background pacing (DESIGN §19, LC-16).
//!
//! The only two pacing mechanisms in the product:
//! * [`TokenBucket`]: a static rate with a burst; constructors take rates only.
//! * [`LatencyController`]: optional continuous proportional control that scales every
//!   background bucket rate by `clamp(EWMA(target_p99 / measured_p99), min_scale, 1.0)`, fed with
//!   device read latency samples only.
//!
//! Pacing applies to background work only; nothing on the completion path of a received
//! command is paced or scaled (LC-15). This module imports nothing outside `std`/`core`.

/// Nanoseconds per second; one token is `NANO` units of bucket level.
const NANO: u128 = 1_000_000_000;

/// Static token bucket.
///
/// The level is kept in units of 10^-9 token, so a refill at `rate` tokens per second adds
/// exactly `rate` units per elapsed nanosecond and no fraction is lost however often the bucket
/// is polled. The bucket starts full. A request larger than the burst is granted when the
/// bucket is full and leaves it in debt (a negative level) that the refill repays, so every
/// request size eventually passes and the long-run rate never exceeds `rate`. With `burst = 0`
/// one request passes whenever no debt is outstanding. A rate of 0 refills nothing: once the
/// initial burst is spent, the task is paused until the rate is raised.
pub struct TokenBucket {
    rate_per_s: u64,
    burst: u64,
    /// Bucket level in 10^-9 tokens; negative while in debt.
    level: i128,
    last_ns: u64,
    scale: f32,
    /// `rate_per_s * scale`, tokens per second.
    effective_rate: u64,
}

impl TokenBucket {
    pub fn new(rate_per_s: u64, burst: u64) -> Self {
        TokenBucket {
            rate_per_s,
            burst,
            level: i128::from(burst) * NANO as i128,
            last_ns: 0,
            scale: 1.0,
            effective_rate: rate_per_s,
        }
    }

    /// Replaces the static rate (CONFIG SET); the burst and the current level are kept.
    pub fn set_rate(&mut self, rate_per_s: u64) {
        self.rate_per_s = rate_per_s;
        self.effective_rate = Self::apply_scale(rate_per_s, self.scale);
    }

    /// The configured (unscaled) rate in tokens per second.
    pub fn rate_per_s(&self) -> u64 {
        self.rate_per_s
    }

    /// The rate currently applied (`rate * scale`), tokens per second.
    pub fn effective_rate(&self) -> u64 {
        self.effective_rate
    }

    pub fn burst(&self) -> u64 {
        self.burst
    }

    fn cap(&self) -> i128 {
        i128::from(self.burst) * NANO as i128
    }

    fn refill(&mut self, now_ns: u64) {
        if self.last_ns == 0 {
            // first use: the bucket starts full at this instant
            self.last_ns = now_ns;
            return;
        }
        if now_ns <= self.last_ns {
            return;
        }
        let elapsed = u128::from(now_ns - self.last_ns);
        self.last_ns = now_ns;
        let cap = self.cap();
        if self.level >= cap {
            return;
        }
        let gain = elapsed.saturating_mul(u128::from(self.effective_rate));
        let room = (cap - self.level) as u128;
        self.level += gain.min(room) as i128;
    }

    /// Takes `n` tokens if available at `now_ns` (any monotonic nanosecond clock; readings that
    /// go backwards are ignored).
    pub fn try_take(&mut self, now_ns: u64, n: u64) -> bool {
        self.refill(now_ns);
        let need = i128::from(n) * NANO as i128;
        let cap = self.cap();
        // a request larger than the burst needs a full bucket and then leaves a debt
        if self.level >= need.min(cap) && self.level >= 0 {
            self.level -= need;
            true
        } else {
            false
        }
    }

    /// Tokens available at `now_ns` without taking any (0 while in debt).
    pub fn available(&mut self, now_ns: u64) -> u64 {
        self.refill(now_ns);
        u64::try_from(self.level.max(0) / NANO as i128).unwrap_or(u64::MAX)
    }

    /// Applies the latency controller's scale to the static rate (`scale` is clamped to
    /// `[0, 1]`; NaN is treated as 1).
    pub fn scaled(&mut self, scale: f32) {
        self.scale = if scale.is_nan() {
            1.0
        } else {
            scale.clamp(0.0, 1.0)
        };
        self.effective_rate = Self::apply_scale(self.rate_per_s, self.scale);
    }

    /// The scale currently applied.
    pub fn scale(&self) -> f32 {
        self.scale
    }

    fn apply_scale(rate: u64, scale: f32) -> u64 {
        if scale >= 1.0 {
            rate
        } else {
            (rate as f64 * f64::from(scale)) as u64
        }
    }
}

/// One device latency measurement (the controller's only input).
#[derive(Clone, Copy)]
pub struct DeviceLatencySample {
    pub p99_read_us: u32,
}

/// Continuous device-latency controller for background rates (§19): every sample (taken every
/// 100 ms by the caller) updates `ewma = (1 - ALPHA) * ewma + ALPHA * ratio` with
/// `ratio = target_p99 / measured_p99`, and the returned scale is `clamp(ewma, min_scale, 1)`.
/// The ratio is clamped to `[min_scale, 1]` before it enters the average (anti-windup): a long
/// stretch of very fast reads cannot bank a large average that would delay the reaction to a
/// later latency spike. The only input is the device read latency.
pub struct LatencyController {
    target_p99_us: u32,
    min_scale: f32,
    ewma: f32,
}

impl LatencyController {
    /// Smoothing factor of the average (weight of the newest sample).
    pub const ALPHA: f32 = 0.3;

    /// `min_scale` is clamped to `[0, 1]` (NaN: 0.1, the documented default).
    pub fn new(target_p99_us: u32, min_scale: f32) -> Self {
        let min_scale = if min_scale.is_nan() {
            0.1
        } else {
            min_scale.clamp(0.0, 1.0)
        };
        LatencyController {
            target_p99_us,
            min_scale,
            ewma: 1.0,
        }
    }

    /// Feeds one sample; returns the scale in `[min_scale, 1.0]`.
    pub fn update(&mut self, s: DeviceLatencySample) -> f32 {
        let ratio = if s.p99_read_us == 0 {
            1.0
        } else {
            (f64::from(self.target_p99_us) / f64::from(s.p99_read_us)) as f32
        };
        let ratio = ratio.clamp(self.min_scale, 1.0);
        self.ewma = (1.0 - Self::ALPHA) * self.ewma + Self::ALPHA * ratio;
        self.scale()
    }

    /// The current scale in `[min_scale, 1.0]`.
    pub fn scale(&self) -> f32 {
        self.ewma.clamp(self.min_scale, 1.0)
    }
}

#[cfg(test)]
mod tests {
    // (aliases rather than a `use` of the parent: this module imports nothing but std/core)
    type TokenBucket = super::TokenBucket;
    type LatencyController = super::LatencyController;
    type DeviceLatencySample = super::DeviceLatencySample;

    const S: u64 = 1_000_000_000;

    #[test]
    fn bucket_starts_full_and_refills_at_rate() {
        let mut b = TokenBucket::new(1000, 10);
        let t0 = 5 * S;
        for _ in 0..10 {
            assert!(b.try_take(t0, 1));
        }
        assert!(!b.try_take(t0, 1));
        // 1 ms at 1000/s = 1 token
        assert!(!b.try_take(t0 + 999_999, 1));
        assert!(b.try_take(t0 + 1_000_000, 1));
        assert!(!b.try_take(t0 + 1_000_000, 1));
        // never more than the burst
        assert_eq!(b.available(t0 + 100 * S), 10);
    }

    #[test]
    fn fine_grained_polling_loses_nothing() {
        // 4000/s polled every microsecond: exactly 4 tokens per millisecond
        let mut b = TokenBucket::new(4000, 0);
        let t0 = S;
        assert!(b.try_take(t0, 1)); // burst 0: one request while no debt
        let mut granted = 0;
        for us in 1..=10_000u64 {
            if b.try_take(t0 + us * 1000, 1) {
                granted += 1;
            }
        }
        // 10 ms at 4000/s = 40 tokens; the first take left a 1-token debt
        assert!((39..=40).contains(&granted), "{granted}");
    }

    #[test]
    fn long_run_rate_is_bounded() {
        let mut b = TokenBucket::new(500, 50);
        let mut granted = 0u64;
        let t0 = S;
        for ms in 0..10_000u64 {
            while b.try_take(t0 + ms * 1_000_000, 3) {
                granted += 3;
            }
        }
        // 10 s at 500/s plus the initial burst
        assert!(granted <= 500 * 10 + 50 + 3, "{granted}");
        assert!(granted >= 500 * 10 - 3, "{granted}");
    }

    #[test]
    fn oversized_requests_pass_with_debt() {
        let mut b = TokenBucket::new(100, 10);
        let t0 = S;
        assert!(
            b.try_take(t0, 25),
            "full bucket grants an oversized request"
        );
        assert!(!b.try_take(t0, 1));
        // the 15-token debt plus 10 tokens of refill take 250 ms
        assert!(!b.try_take(t0 + 240_000_000, 25));
        assert!(b.try_take(t0 + 250_000_000, 25));
    }

    #[test]
    fn zero_rate_pauses_after_burst() {
        let mut b = TokenBucket::new(0, 2);
        assert!(b.try_take(S, 1));
        assert!(b.try_take(S, 1));
        assert!(!b.try_take(100 * S, 1));
        b.set_rate(10);
        assert!(!b.try_take(100 * S, 1)); // the rate applies from now on
        assert!(b.try_take(100 * S + 100_000_000, 1));
    }

    #[test]
    fn scale_multiplies_the_rate() {
        let mut b = TokenBucket::new(1000, 0);
        b.scaled(0.5);
        assert_eq!(b.effective_rate(), 500);
        assert!(b.try_take(S, 1));
        assert!(!b.try_take(S + 1_999_999, 1));
        assert!(b.try_take(S + 2_000_000, 1));
        b.scaled(f32::NAN);
        assert_eq!(b.effective_rate(), 1000);
        b.scaled(7.0);
        assert_eq!(b.scale(), 1.0);
        b.scaled(-1.0);
        assert_eq!(b.effective_rate(), 0);
        b.set_rate(2000);
        assert_eq!(b.rate_per_s(), 2000);
        assert_eq!(b.effective_rate(), 0);
    }

    #[test]
    fn backwards_clock_is_ignored() {
        let mut b = TokenBucket::new(1000, 1);
        assert!(b.try_take(10 * S, 1));
        assert!(!b.try_take(5 * S, 1));
        assert!(b.try_take(10 * S + 1_000_000, 1));
    }

    #[test]
    fn controller_tracks_latency() {
        let mut c = LatencyController::new(1000, 0.1);
        // fast device: full rate
        for _ in 0..20 {
            assert_eq!(c.update(DeviceLatencySample { p99_read_us: 200 }), 1.0);
        }
        // 4x over target: converges to 0.25
        let mut s = 1.0;
        for _ in 0..50 {
            s = c.update(DeviceLatencySample { p99_read_us: 4000 });
        }
        assert!((s - 0.25).abs() < 0.01, "{s}");
        // pathological latency: clamped at min_scale
        for _ in 0..50 {
            s = c.update(DeviceLatencySample {
                p99_read_us: 1_000_000,
            });
        }
        assert!((s - 0.1).abs() < 1e-3, "{s}");
        // recovery is proportional, not a jump
        let first = c.update(DeviceLatencySample { p99_read_us: 500 });
        assert!(first > 0.1 && first < 0.5, "{first}");
        for _ in 0..50 {
            s = c.update(DeviceLatencySample { p99_read_us: 500 });
        }
        assert!((s - 1.0).abs() < 1e-3);
        // no reading counts as on target
        assert_eq!(c.update(DeviceLatencySample { p99_read_us: 0 }), c.scale());
    }

    #[test]
    fn controller_output_always_in_range() {
        for min in [0.0f32, 0.1, 0.5, 1.0, f32::NAN, -3.0, 9.0] {
            let mut c = LatencyController::new(800, min);
            for us in [0u32, 1, 10, 800, 801, 5000, u32::MAX] {
                let s = c.update(DeviceLatencySample { p99_read_us: us });
                assert!((0.0..=1.0).contains(&s), "{s}");
                let lo = if min.is_nan() {
                    0.1
                } else {
                    min.clamp(0.0, 1.0)
                };
                assert!(s >= lo - 1e-6, "{s} < {lo}");
            }
        }
    }
}
