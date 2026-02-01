//! NUMA-aware memory allocation and thread pinning.
//!
//! On NUMA systems, memory access latency varies based on which NUMA node
//! the memory is allocated on. This module provides utilities to:
//! 1. Pin threads to specific CPU cores
//! 2. Allocate memory on specific NUMA nodes
//! 3. Distribute workloads across NUMA nodes

use std::io;

/// CPU topology information.
#[derive(Debug, Clone)]
pub struct CpuTopology {
    pub num_cpus: usize,
    pub num_numa_nodes: usize,
    pub cpus_per_node: Vec<Vec<usize>>,
}

impl CpuTopology {
    /// Detects the CPU topology of the current system.
    pub fn detect() -> Self {
        let num_cpus = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1);

        // Try to detect NUMA topology
        #[cfg(target_os = "linux")]
        {
            if let Ok(nodes) = detect_numa_nodes() {
                return nodes;
            }
        }

        // Fallback: assume single NUMA node
        Self {
            num_cpus,
            num_numa_nodes: 1,
            cpus_per_node: vec![(0..num_cpus).collect()],
        }
    }

    /// Returns the NUMA node for a given CPU.
    pub fn numa_node_for_cpu(&self, cpu: usize) -> usize {
        for (node, cpus) in self.cpus_per_node.iter().enumerate() {
            if cpus.contains(&cpu) {
                return node;
            }
        }
        0
    }

    /// Returns CPUs for a given NUMA node.
    pub fn cpus_for_node(&self, node: usize) -> &[usize] {
        self.cpus_per_node.get(node).map(|v| v.as_slice()).unwrap_or(&[])
    }
}

/// Pins the current thread to a specific CPU core.
#[cfg(target_os = "linux")]
pub fn pin_to_cpu(cpu: usize) -> io::Result<()> {
    use std::mem;

    unsafe {
        let mut cpuset: libc::cpu_set_t = mem::zeroed();
        libc::CPU_ZERO(&mut cpuset);
        libc::CPU_SET(cpu, &mut cpuset);

        let result = libc::sched_setaffinity(0, mem::size_of::<libc::cpu_set_t>(), &cpuset);

        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

#[cfg(target_os = "macos")]
pub fn pin_to_cpu(_cpu: usize) -> io::Result<()> {
    // macOS doesn't support CPU pinning in the same way
    // Thread affinity is managed by the OS
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn pin_to_cpu(_cpu: usize) -> io::Result<()> {
    Ok(())
}

/// Pins the current thread to a NUMA node (any CPU on that node).
pub fn pin_to_numa_node(node: usize) -> io::Result<()> {
    let topology = CpuTopology::detect();
    if let Some(cpus) = topology.cpus_per_node.get(node) {
        if let Some(&cpu) = cpus.first() {
            return pin_to_cpu(cpu);
        }
    }
    Ok(())
}

/// Detects NUMA nodes on Linux by reading /sys/devices/system/node/.
#[cfg(target_os = "linux")]
fn detect_numa_nodes() -> io::Result<CpuTopology> {
    use std::fs;

    let num_cpus = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1);

    let node_path = std::path::Path::new("/sys/devices/system/node");
    if !node_path.exists() {
        return Err(io::Error::new(io::ErrorKind::NotFound, "NUMA info not found"));
    }

    let mut cpus_per_node = Vec::new();

    for entry in fs::read_dir(node_path)? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        if name_str.starts_with("node") && name_str[4..].parse::<usize>().is_ok() {
            let cpulist_path = entry.path().join("cpulist");
            if let Ok(content) = fs::read_to_string(&cpulist_path) {
                let cpus = parse_cpulist(&content);
                cpus_per_node.push(cpus);
            }
        }
    }

    if cpus_per_node.is_empty() {
        return Err(io::Error::new(io::ErrorKind::NotFound, "No NUMA nodes found"));
    }

    Ok(CpuTopology {
        num_cpus,
        num_numa_nodes: cpus_per_node.len(),
        cpus_per_node,
    })
}

/// Parses a CPU list string like "0-3,5,7-9" into a vector of CPU numbers.
#[cfg(target_os = "linux")]
fn parse_cpulist(s: &str) -> Vec<usize> {
    let mut cpus = Vec::new();

    for part in s.trim().split(',') {
        if let Some(dash_pos) = part.find('-') {
            if let (Ok(start), Ok(end)) = (
                part[..dash_pos].parse::<usize>(),
                part[dash_pos + 1..].parse::<usize>(),
            ) {
                cpus.extend(start..=end);
            }
        } else if let Ok(cpu) = part.parse::<usize>() {
            cpus.push(cpu);
        }
    }

    cpus
}

/// Thread pool with NUMA-aware thread distribution.
pub struct NumaThreadPool {
    topology: CpuTopology,
    handles: Vec<std::thread::JoinHandle<()>>,
}

impl NumaThreadPool {
    /// Creates a thread pool with threads distributed across NUMA nodes.
    pub fn new<F>(threads_per_node: usize, f: F) -> Self
    where
        F: Fn(usize, usize) + Send + Sync + Clone + 'static,
    {
        let topology = CpuTopology::detect();
        let mut handles = Vec::new();

        for node in 0..topology.num_numa_nodes {
            let cpus = topology.cpus_for_node(node);

            for i in 0..threads_per_node {
                let cpu = cpus.get(i % cpus.len()).copied().unwrap_or(0);
                let f = f.clone();

                let handle = std::thread::Builder::new()
                    .name(format!("worker-n{}-t{}", node, i))
                    .spawn(move || {
                        // Pin to CPU
                        let _ = pin_to_cpu(cpu);
                        f(node, i);
                    })
                    .expect("Failed to spawn worker thread");

                handles.push(handle);
            }
        }

        Self { topology, handles }
    }

    /// Returns the topology used by this pool.
    pub fn topology(&self) -> &CpuTopology {
        &self.topology
    }

    /// Waits for all threads to complete.
    pub fn join(self) {
        for handle in self.handles {
            let _ = handle.join();
        }
    }
}

/// Allocates memory with a hint to place it on a specific NUMA node.
#[cfg(target_os = "linux")]
pub fn numa_alloc(size: usize, node: usize) -> io::Result<*mut u8> {
    use std::ptr;

    unsafe {
        let ptr = libc::mmap(
            ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        );

        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }

        // Try to bind to NUMA node using mbind if available
        // This is a best-effort hint
        #[cfg(feature = "numa")]
        {
            let mut nodemask = 1u64 << node;
            libc::mbind(
                ptr,
                size,
                libc::MPOL_BIND,
                &nodemask as *const _ as *const libc::c_ulong,
                64,
                libc::MPOL_MF_MOVE,
            );
        }

        Ok(ptr as *mut u8)
    }
}

#[cfg(not(target_os = "linux"))]
pub fn numa_alloc(size: usize, _node: usize) -> io::Result<*mut u8> {
    use std::alloc::{alloc, Layout};

    let layout = Layout::from_size_align(size, 4096).map_err(|e| {
        io::Error::new(io::ErrorKind::InvalidInput, e)
    })?;

    let ptr = unsafe { alloc(layout) };
    if ptr.is_null() {
        Err(io::Error::new(io::ErrorKind::OutOfMemory, "Allocation failed"))
    } else {
        Ok(ptr)
    }
}

/// Frees memory allocated with numa_alloc.
#[cfg(target_os = "linux")]
pub fn numa_free(ptr: *mut u8, size: usize) {
    unsafe {
        libc::munmap(ptr as *mut libc::c_void, size);
    }
}

#[cfg(not(target_os = "linux"))]
pub fn numa_free(ptr: *mut u8, size: usize) {
    use std::alloc::{dealloc, Layout};

    if let Ok(layout) = Layout::from_size_align(size, 4096) {
        unsafe {
            dealloc(ptr, layout);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cpu_topology() {
        let topology = CpuTopology::detect();
        assert!(topology.num_cpus > 0);
        assert!(topology.num_numa_nodes > 0);
        assert!(!topology.cpus_per_node.is_empty());
    }

    #[test]
    fn test_pin_to_cpu() {
        // Just test that it doesn't panic
        let _ = pin_to_cpu(0);
    }

    #[test]
    fn test_numa_alloc() {
        let ptr = numa_alloc(4096, 0).unwrap();
        assert!(!ptr.is_null());
        numa_free(ptr, 4096);
    }
}
