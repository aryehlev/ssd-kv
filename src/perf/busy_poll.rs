//! Busy polling and CPU isolation for ultra-low latency.
//!
//! Busy polling eliminates context switches by constantly polling for events
//! instead of sleeping. Combined with CPU isolation, this provides:
//!
//! - Sub-microsecond latency
//! - Zero context switches
//! - Dedicated CPU for networking
//!
//! Trade-offs:
//! - 100% CPU usage even when idle
//! - Requires dedicated CPUs (isolcpus kernel parameter)

use std::io;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Configuration for busy polling.
#[derive(Debug, Clone)]
pub struct BusyPollConfig {
    /// Enable busy polling on sockets.
    pub enabled: bool,
    /// Busy poll time in microseconds (0 = disabled).
    pub busy_poll_us: u32,
    /// Busy read time in microseconds.
    pub busy_read_us: u32,
    /// Use adaptive polling (reduce CPU when idle).
    pub adaptive: bool,
    /// Idle threshold before switching to sleep (in iterations).
    pub idle_threshold: u64,
    /// Sleep duration when in idle mode.
    pub idle_sleep_us: u64,
}

impl Default for BusyPollConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            busy_poll_us: 50,
            busy_read_us: 50,
            adaptive: true,
            idle_threshold: 10000,
            idle_sleep_us: 10,
        }
    }
}

/// Statistics for busy polling.
#[derive(Debug, Default)]
pub struct BusyPollStats {
    pub poll_iterations: AtomicU64,
    pub events_found: AtomicU64,
    pub idle_iterations: AtomicU64,
    pub sleeps: AtomicU64,
    pub max_batch: AtomicU64,
}

impl BusyPollStats {
    pub fn to_json(&self) -> String {
        format!(
            r#"{{"poll_iterations":{},"events_found":{},"idle_iterations":{},"sleeps":{},"max_batch":{}}}"#,
            self.poll_iterations.load(Ordering::Relaxed),
            self.events_found.load(Ordering::Relaxed),
            self.idle_iterations.load(Ordering::Relaxed),
            self.sleeps.load(Ordering::Relaxed),
            self.max_batch.load(Ordering::Relaxed),
        )
    }
}

/// Enable busy polling on a socket.
#[cfg(target_os = "linux")]
pub fn enable_socket_busy_poll(fd: RawFd, busy_poll_us: u32) -> io::Result<()> {
    let optval = busy_poll_us as libc::c_int;

    // SO_BUSY_POLL
    let ret = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_BUSY_POLL,
            &optval as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };

    if ret < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn enable_socket_busy_poll(_fd: RawFd, _busy_poll_us: u32) -> io::Result<()> {
    Ok(())
}

/// Set system-wide busy poll parameters.
/// Requires root privileges.
#[cfg(target_os = "linux")]
pub fn set_system_busy_poll(busy_poll_us: u32, busy_read_us: u32) -> io::Result<()> {
    use std::fs;

    // Set net.core.busy_poll
    fs::write(
        "/proc/sys/net/core/busy_poll",
        format!("{}", busy_poll_us),
    ).map_err(|e| io::Error::new(
        io::ErrorKind::PermissionDenied,
        format!("Cannot set busy_poll (need root): {}", e),
    ))?;

    // Set net.core.busy_read
    fs::write(
        "/proc/sys/net/core/busy_read",
        format!("{}", busy_read_us),
    ).map_err(|e| io::Error::new(
        io::ErrorKind::PermissionDenied,
        format!("Cannot set busy_read (need root): {}", e),
    ))?;

    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn set_system_busy_poll(_busy_poll_us: u32, _busy_read_us: u32) -> io::Result<()> {
    Ok(())
}

/// Busy polling event loop.
pub struct BusyPoller {
    config: BusyPollConfig,
    stats: BusyPollStats,
    idle_count: u64,
    is_idle: bool,
}

impl BusyPoller {
    /// Create a new busy poller.
    pub fn new(config: BusyPollConfig) -> Self {
        Self {
            config,
            stats: BusyPollStats::default(),
            idle_count: 0,
            is_idle: false,
        }
    }

    /// Poll for events (non-blocking).
    /// Returns the number of events found.
    pub fn poll<F>(&mut self, mut poll_fn: F) -> usize
    where
        F: FnMut() -> usize,
    {
        self.stats.poll_iterations.fetch_add(1, Ordering::Relaxed);

        let events = poll_fn();

        if events > 0 {
            self.stats.events_found.fetch_add(events as u64, Ordering::Relaxed);

            // Update max batch
            let current_max = self.stats.max_batch.load(Ordering::Relaxed);
            if events as u64 > current_max {
                self.stats.max_batch.store(events as u64, Ordering::Relaxed);
            }

            // Reset idle state
            self.idle_count = 0;
            self.is_idle = false;
        } else if self.config.adaptive {
            self.idle_count += 1;
            self.stats.idle_iterations.fetch_add(1, Ordering::Relaxed);

            if self.idle_count > self.config.idle_threshold {
                self.is_idle = true;
                // Sleep briefly to reduce CPU usage
                std::thread::sleep(Duration::from_micros(self.config.idle_sleep_us));
                self.stats.sleeps.fetch_add(1, Ordering::Relaxed);
            }
        }

        events
    }

    /// Check if currently in idle mode.
    pub fn is_idle(&self) -> bool {
        self.is_idle
    }

    /// Get statistics.
    pub fn stats(&self) -> &BusyPollStats {
        &self.stats
    }
}

/// CPU isolation helper.
pub struct CpuIsolation {
    /// CPUs isolated for network processing.
    pub network_cpus: Vec<usize>,
    /// CPUs isolated for disk I/O.
    pub io_cpus: Vec<usize>,
    /// CPUs for general application work.
    pub app_cpus: Vec<usize>,
}

impl CpuIsolation {
    /// Detect available CPUs and suggest isolation.
    pub fn auto_detect() -> Self {
        let num_cpus = num_cpus();

        if num_cpus <= 4 {
            // Few CPUs: no isolation
            let all: Vec<usize> = (0..num_cpus).collect();
            return Self {
                network_cpus: all.clone(),
                io_cpus: all.clone(),
                app_cpus: all,
            };
        }

        // Many CPUs: isolate
        // CPU 0: system (don't use)
        // CPU 1-2: network
        // CPU 3-4: I/O
        // Rest: application
        let network_cpus = vec![1, 2];
        let io_cpus = vec![3, 4];
        let app_cpus: Vec<usize> = (5..num_cpus).collect();

        Self {
            network_cpus,
            io_cpus,
            app_cpus: if app_cpus.is_empty() { vec![0] } else { app_cpus },
        }
    }

    /// Print recommended kernel parameters.
    pub fn print_kernel_params(&self) {
        let isolated: Vec<String> = self.network_cpus.iter()
            .chain(self.io_cpus.iter())
            .map(|c| c.to_string())
            .collect();

        if !isolated.is_empty() {
            println!("Recommended kernel parameters:");
            println!("  isolcpus={}", isolated.join(","));
            println!("  nohz_full={}", isolated.join(","));
            println!("  rcu_nocbs={}", isolated.join(","));
        }
    }

    /// Apply thread affinity to current thread.
    pub fn apply_network_affinity(&self) -> io::Result<()> {
        if let Some(&cpu) = self.network_cpus.first() {
            crate::perf::pin_to_cpu(cpu)
        } else {
            Ok(())
        }
    }

    /// Apply I/O thread affinity.
    pub fn apply_io_affinity(&self) -> io::Result<()> {
        if let Some(&cpu) = self.io_cpus.first() {
            crate::perf::pin_to_cpu(cpu)
        } else {
            Ok(())
        }
    }
}

/// Get number of CPUs.
fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1)
}

/// High-resolution spin wait.
/// More precise than thread::sleep for very short waits.
pub fn spin_wait(duration: Duration) {
    let start = Instant::now();
    while start.elapsed() < duration {
        std::hint::spin_loop();
    }
}

/// Spin wait until a condition is true.
pub fn spin_until<F>(mut condition: F, timeout: Duration) -> bool
where
    F: FnMut() -> bool,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if condition() {
            return true;
        }
        std::hint::spin_loop();
    }
    false
}

/// Adaptive spinner that transitions between spinning and sleeping.
pub struct AdaptiveSpinner {
    spin_iterations: u32,
    max_spin_iterations: u32,
    yield_iterations: u32,
}

impl AdaptiveSpinner {
    pub fn new() -> Self {
        Self {
            spin_iterations: 0,
            max_spin_iterations: 1000,
            yield_iterations: 0,
        }
    }

    /// Wait for something to happen.
    /// Returns true if should continue waiting.
    pub fn wait(&mut self) -> bool {
        if self.spin_iterations < self.max_spin_iterations {
            // Pure spin
            self.spin_iterations += 1;
            std::hint::spin_loop();
        } else if self.yield_iterations < 10 {
            // Yield to OS
            self.yield_iterations += 1;
            std::thread::yield_now();
        } else {
            // Sleep
            std::thread::sleep(Duration::from_micros(1));
        }
        true
    }

    /// Reset the spinner (called when work is found).
    pub fn reset(&mut self) {
        self.spin_iterations = 0;
        self.yield_iterations = 0;
    }
}

impl Default for AdaptiveSpinner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_busy_poll_config() {
        let config = BusyPollConfig::default();
        assert!(config.enabled);
        assert_eq!(config.busy_poll_us, 50);
    }

    #[test]
    fn test_busy_poller() {
        let config = BusyPollConfig::default();
        let mut poller = BusyPoller::new(config);

        let mut count = 0;
        let events = poller.poll(|| {
            count += 1;
            if count < 3 { 0 } else { 5 }
        });

        assert_eq!(events, 5);
    }

    #[test]
    fn test_cpu_isolation() {
        let isolation = CpuIsolation::auto_detect();
        assert!(!isolation.network_cpus.is_empty());
    }

    #[test]
    fn test_adaptive_spinner() {
        let mut spinner = AdaptiveSpinner::new();
        for _ in 0..100 {
            spinner.wait();
        }
        spinner.reset();
        assert_eq!(spinner.spin_iterations, 0);
    }

    #[test]
    fn test_spin_until() {
        let mut counter = 0;
        let result = spin_until(|| {
            counter += 1;
            counter >= 10
        }, Duration::from_millis(100));

        assert!(result);
        assert!(counter >= 10);
    }
}
