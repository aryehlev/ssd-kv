//! Performance optimization layer.
//!
//! This module provides advanced optimizations for achieving Aerospike-level
//! performance:
//!
//! - **Hot Cache**: Lock-free L1 cache for frequently accessed small keys
//! - **Batch Writer**: Write coalescing for maximum SSD throughput
//! - **Prefetch**: CPU cache prefetching and read-ahead
//! - **NUMA**: NUMA-aware memory allocation and thread pinning
//! - **XDP**: eBPF/XDP packet acceleration (Linux only)

pub mod hot_cache;
pub mod batch_writer;
pub mod prefetch;
pub mod numa;
pub mod xdp;
pub mod object_pool;

pub use hot_cache::{HotCache, CacheStats};
pub use batch_writer::{BatchWriter, BatchWriterStats, WriteRequest};
pub use prefetch::{prefetch_read, prefetch_write, prefetch_range, BloomFilter, LockFreeBloomFilter, ReadAhead};
pub use numa::{CpuTopology, NumaThreadPool, pin_to_cpu, pin_to_numa_node, numa_alloc, numa_free};
pub use xdp::{XdpAccelerator, XdpConfig, XdpStats};
pub use object_pool::{BufferPool, PooledBuffer, READ_BUFFER_POOL, WRITE_BUFFER_POOL};

/// Performance tuning recommendations based on hardware.
#[derive(Debug)]
pub struct PerfTuning {
    /// Recommended number of network threads.
    pub network_threads: usize,
    /// Recommended number of write threads.
    pub write_threads: usize,
    /// Recommended hot cache size.
    pub hot_cache_entries: usize,
    /// Recommended batch size.
    pub batch_size: usize,
    /// Whether to use XDP.
    pub use_xdp: bool,
    /// Whether to use huge pages.
    pub use_huge_pages: bool,
}

impl PerfTuning {
    /// Generates tuning recommendations based on hardware.
    pub fn auto_tune() -> Self {
        let topology = CpuTopology::detect();
        let num_cpus = topology.num_cpus;
        let num_nodes = topology.num_numa_nodes;

        // Network threads: 1-2 per NUMA node
        let network_threads = (num_nodes * 2).max(2).min(num_cpus / 2);

        // Write threads: 1 per NUMA node
        let write_threads = num_nodes.max(1);

        // Hot cache: scale with available memory (assume 1GB available)
        // Each entry is ~200 bytes, so 64K entries = ~12MB
        let hot_cache_entries = 64 * 1024;

        // Batch size: balance latency vs throughput
        let batch_size = 1000;

        // XDP: only on Linux with root
        #[cfg(target_os = "linux")]
        let use_xdp = unsafe { libc::geteuid() == 0 };
        #[cfg(not(target_os = "linux"))]
        let use_xdp = false;

        // Huge pages: available on Linux
        #[cfg(target_os = "linux")]
        let use_huge_pages = std::path::Path::new("/sys/kernel/mm/hugepages").exists();
        #[cfg(not(target_os = "linux"))]
        let use_huge_pages = false;

        Self {
            network_threads,
            write_threads,
            hot_cache_entries,
            batch_size,
            use_xdp,
            use_huge_pages,
        }
    }
}

/// Benchmark results.
#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    pub operation: String,
    pub ops_per_sec: f64,
    pub p50_latency_us: f64,
    pub p99_latency_us: f64,
    pub p999_latency_us: f64,
}

impl BenchmarkResult {
    pub fn to_json(&self) -> String {
        format!(
            r#"{{"operation":"{}","ops_per_sec":{:.0},"p50_latency_us":{:.1},"p99_latency_us":{:.1},"p999_latency_us":{:.1}}}"#,
            self.operation,
            self.ops_per_sec,
            self.p50_latency_us,
            self.p99_latency_us,
            self.p999_latency_us,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auto_tune() {
        let tuning = PerfTuning::auto_tune();
        assert!(tuning.network_threads > 0);
        assert!(tuning.write_threads > 0);
        assert!(tuning.hot_cache_entries > 0);
        assert!(tuning.batch_size > 0);
    }
}
