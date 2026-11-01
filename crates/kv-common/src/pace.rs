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

/// Static token bucket.
pub struct TokenBucket {
    rate_per_s: u64,
    burst: u64,
    tokens: u64,
    last_ns: u64,
    scale: f32,
}

impl TokenBucket {
    pub fn new(rate_per_s: u64, burst: u64) -> Self {
        todo!("DESIGN §19: token bucket")
    }

    pub fn set_rate(&mut self, rate_per_s: u64) {
        todo!("DESIGN §19: token bucket")
    }

    /// Takes `n` tokens if available at `now_ns`.
    pub fn try_take(&mut self, now_ns: u64, n: u64) -> bool {
        todo!("DESIGN §19: token bucket")
    }

    /// Applies the latency controller's scale to the static rate.
    pub fn scaled(&mut self, scale: f32) {
        todo!("DESIGN §19: token bucket")
    }
}

/// One device latency measurement (the controller's only input).
#[derive(Clone, Copy)]
pub struct DeviceLatencySample {
    pub p99_read_us: u32,
}

/// Continuous device-latency controller for background rates.
pub struct LatencyController {
    target_p99_us: u32,
    min_scale: f32,
    ewma: f32,
}

impl LatencyController {
    pub fn new(target_p99_us: u32, min_scale: f32) -> Self {
        todo!("DESIGN §19: latency controller")
    }

    /// Feeds one sample; returns the scale in `[min_scale, 1.0]`.
    pub fn update(&mut self, s: DeviceLatencySample) -> f32 {
        todo!("DESIGN §19: latency controller")
    }
}
