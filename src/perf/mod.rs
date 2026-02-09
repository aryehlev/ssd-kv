//! Performance optimization layer.
//!
//! This module provides advanced optimizations for achieving Aerospike-level
//! performance:
//!
//! - **Batch Writer**: Write coalescing for maximum SSD throughput
//! - **Prefetch**: CPU cache prefetching and read-ahead
//! - **NUMA**: NUMA-aware memory allocation and thread pinning
//! - **XDP**: eBPF/XDP packet acceleration (Linux only)
//! - **AF_XDP**: Zero-copy networking (Linux only)
//! - **Huge Pages**: Reduce TLB misses for large allocations
//! - **SIMD**: Vectorized batch operations (AVX2/NEON) for 2-4x throughput

pub mod batch_writer;
pub mod prefetch;
pub mod numa;
pub mod xdp;
pub mod af_xdp;
pub mod huge_pages;
pub mod busy_poll;
pub mod object_pool;
pub mod simd;

pub use batch_writer::{BatchWriter, BatchWriterStats, WriteRequest};
pub use prefetch::{prefetch_read, prefetch_write, prefetch_range, BloomFilter, LockFreeBloomFilter, ReadAhead};
pub use numa::{CpuTopology, NumaThreadPool, pin_to_cpu, pin_to_numa_node, numa_alloc, numa_free};
pub use xdp::{XdpAccelerator, XdpConfig, XdpStats};
pub use af_xdp::{AfXdpConfig, AfXdpSocket, AfXdpStats, XdpDesc, is_af_xdp_available};
pub use huge_pages::{HugePageAlloc, HugePageConfig, huge_page_alloc, huge_page_free, is_huge_pages_available};
pub use busy_poll::{BusyPollConfig, BusyPoller, BusyPollStats, CpuIsolation, AdaptiveSpinner, enable_socket_busy_poll, set_system_busy_poll};
pub use object_pool::{BufferPool, PooledBuffer, READ_BUFFER_POOL, WRITE_BUFFER_POOL};
pub use simd::{
    simd_key_eq, simd_memcpy, simd_memset, crc32c,
    batch_bloom_check, batch_hash_keys_4, batch_hash_keys_8,
    batch_index_lookup, batch_index_lookup_4, group_by_shard,
    BloomBatchResult, BatchGetResult, prefetch_batch,
};

/// Performance tuning recommendations based on hardware.
#[derive(Debug)]
pub struct PerfTuning {
    /// Recommended number of network threads.
    pub network_threads: usize,
    /// Recommended number of write threads.
    pub write_threads: usize,
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
        assert!(tuning.batch_size > 0);
    }
}
