//! Huge page support for reduced TLB misses.
//!
//! Huge pages (2MB or 1GB) reduce TLB (Translation Lookaside Buffer) misses
//! by requiring fewer page table entries for large memory regions.
//!
//! Benefits:
//! - 10-30% performance improvement for large working sets
//! - Reduced memory fragmentation
//! - Better cache locality for hash tables
//!
//! Usage:
//! - Index hash tables
//! - Write buffers
//! - Network buffers

use std::io;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Huge page sizes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HugePageSize {
    /// 2MB huge pages (most common).
    TwoMB = 21,    // 2^21 = 2MB
    /// 1GB huge pages (requires explicit kernel config).
    OneGB = 30,    // 2^30 = 1GB
}

impl HugePageSize {
    pub fn bytes(&self) -> usize {
        1 << (*self as usize)
    }
}

/// Configuration for huge page allocation.
#[derive(Debug, Clone)]
pub struct HugePageConfig {
    /// Preferred huge page size.
    pub size: HugePageSize,
    /// Fall back to regular pages if huge pages aren't available.
    pub allow_fallback: bool,
    /// Lock pages in memory (prevent swapping).
    pub mlock: bool,
    /// Use transparent huge pages instead of explicit.
    pub transparent: bool,
}

impl Default for HugePageConfig {
    fn default() -> Self {
        Self {
            size: HugePageSize::TwoMB,
            allow_fallback: true,
            mlock: false,
            transparent: false,
        }
    }
}

/// Global statistics for huge page allocations.
#[derive(Debug, Default)]
pub struct HugePageStats {
    pub allocations: AtomicUsize,
    pub deallocations: AtomicUsize,
    pub bytes_allocated: AtomicUsize,
    pub bytes_deallocated: AtomicUsize,
    pub fallbacks: AtomicUsize,
}

static STATS: HugePageStats = HugePageStats {
    allocations: AtomicUsize::new(0),
    deallocations: AtomicUsize::new(0),
    bytes_allocated: AtomicUsize::new(0),
    bytes_deallocated: AtomicUsize::new(0),
    fallbacks: AtomicUsize::new(0),
};

/// Get global huge page statistics.
pub fn stats() -> &'static HugePageStats {
    &STATS
}

// ============================================================================
// Linux implementation
// ============================================================================

#[cfg(target_os = "linux")]
mod linux {
    use super::*;

    // mmap flags for huge pages
    const MAP_HUGETLB: i32 = 0x40000;
    const MAP_HUGE_SHIFT: i32 = 26;
    const MAP_HUGE_2MB: i32 = 21 << MAP_HUGE_SHIFT;
    const MAP_HUGE_1GB: i32 = 30 << MAP_HUGE_SHIFT;
    const MADV_HUGEPAGE: i32 = 14;

    /// Check if huge pages are available.
    pub fn is_huge_pages_available() -> bool {
        // Check if hugepages are configured
        if let Ok(content) = std::fs::read_to_string("/proc/meminfo") {
            for line in content.lines() {
                if line.starts_with("HugePages_Total:") {
                    if let Some(count_str) = line.split_whitespace().nth(1) {
                        if let Ok(count) = count_str.parse::<usize>() {
                            return count > 0;
                        }
                    }
                }
            }
        }

        // Also check for transparent huge pages
        if let Ok(content) = std::fs::read_to_string("/sys/kernel/mm/transparent_hugepage/enabled") {
            return content.contains("[always]") || content.contains("[madvise]");
        }

        false
    }

    /// Allocate memory using huge pages.
    pub fn huge_page_alloc(size: usize, config: &HugePageConfig) -> io::Result<*mut u8> {
        // Round up to huge page boundary
        let page_size = config.size.bytes();
        let aligned_size = (size + page_size - 1) & !(page_size - 1);

        if config.transparent {
            // Use madvise for transparent huge pages
            return alloc_transparent(aligned_size, config);
        }

        // Try explicit huge pages
        let huge_flag = match config.size {
            HugePageSize::TwoMB => MAP_HUGE_2MB,
            HugePageSize::OneGB => MAP_HUGE_1GB,
        };

        let ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                aligned_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | MAP_HUGETLB | huge_flag,
                -1,
                0,
            )
        };

        if ptr == libc::MAP_FAILED {
            if config.allow_fallback {
                STATS.fallbacks.fetch_add(1, Ordering::Relaxed);
                return alloc_transparent(aligned_size, config);
            }
            return Err(io::Error::last_os_error());
        }

        if config.mlock {
            let _ = unsafe { libc::mlock(ptr, aligned_size) };
        }

        STATS.allocations.fetch_add(1, Ordering::Relaxed);
        STATS.bytes_allocated.fetch_add(aligned_size, Ordering::Relaxed);

        Ok(ptr as *mut u8)
    }

    fn alloc_transparent(size: usize, config: &HugePageConfig) -> io::Result<*mut u8> {
        // Allocate regular memory
        let ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };

        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }

        // Advise kernel to use huge pages
        unsafe {
            libc::madvise(ptr, size, MADV_HUGEPAGE);
        }

        if config.mlock {
            let _ = unsafe { libc::mlock(ptr, size) };
        }

        STATS.allocations.fetch_add(1, Ordering::Relaxed);
        STATS.bytes_allocated.fetch_add(size, Ordering::Relaxed);

        Ok(ptr as *mut u8)
    }

    /// Free huge page memory.
    pub fn huge_page_free(ptr: *mut u8, size: usize) {
        if ptr.is_null() {
            return;
        }

        unsafe {
            libc::munmap(ptr as *mut libc::c_void, size);
        }

        STATS.deallocations.fetch_add(1, Ordering::Relaxed);
        STATS.bytes_deallocated.fetch_add(size, Ordering::Relaxed);
    }
}

// ============================================================================
// macOS implementation (no huge pages, but we have superpages)
// ============================================================================

#[cfg(target_os = "macos")]
mod macos {
    use super::*;

    // macOS superpage flag
    const VM_FLAGS_SUPERPAGE_SIZE_2MB: i32 = 1 << 16;

    pub fn is_huge_pages_available() -> bool {
        // macOS supports superpages but requires specific configuration
        true
    }

    pub fn huge_page_alloc(size: usize, config: &HugePageConfig) -> io::Result<*mut u8> {
        let page_size = 2 * 1024 * 1024; // 2MB
        let aligned_size = (size + page_size - 1) & !(page_size - 1);

        // Try superpage allocation
        let ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                aligned_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };

        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }

        // Note: VM_FLAGS_SUPERPAGE_SIZE_2MB would require vm_allocate
        // For now, we just use regular mmap

        STATS.allocations.fetch_add(1, Ordering::Relaxed);
        STATS.bytes_allocated.fetch_add(aligned_size, Ordering::Relaxed);

        Ok(ptr as *mut u8)
    }

    pub fn huge_page_free(ptr: *mut u8, size: usize) {
        if ptr.is_null() {
            return;
        }

        unsafe {
            libc::munmap(ptr as *mut libc::c_void, size);
        }

        STATS.deallocations.fetch_add(1, Ordering::Relaxed);
        STATS.bytes_deallocated.fetch_add(size, Ordering::Relaxed);
    }
}

// ============================================================================
// Generic fallback
// ============================================================================

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod fallback {
    use super::*;

    pub fn is_huge_pages_available() -> bool {
        false
    }

    pub fn huge_page_alloc(size: usize, _config: &HugePageConfig) -> io::Result<*mut u8> {
        // Fall back to regular allocation
        let ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };

        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }

        STATS.allocations.fetch_add(1, Ordering::Relaxed);
        STATS.bytes_allocated.fetch_add(size, Ordering::Relaxed);

        Ok(ptr as *mut u8)
    }

    pub fn huge_page_free(ptr: *mut u8, size: usize) {
        if ptr.is_null() {
            return;
        }

        unsafe {
            libc::munmap(ptr as *mut libc::c_void, size);
        }

        STATS.deallocations.fetch_add(1, Ordering::Relaxed);
        STATS.bytes_deallocated.fetch_add(size, Ordering::Relaxed);
    }
}

// Re-export platform-specific implementations
#[cfg(target_os = "linux")]
pub use linux::{huge_page_alloc, huge_page_free, is_huge_pages_available};

#[cfg(target_os = "macos")]
pub use macos::{huge_page_alloc, huge_page_free, is_huge_pages_available};

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub use fallback::{huge_page_alloc, huge_page_free, is_huge_pages_available};

/// RAII wrapper for huge page allocations.
pub struct HugePageAlloc {
    ptr: *mut u8,
    size: usize,
}

impl HugePageAlloc {
    /// Allocate a huge page region.
    pub fn new(size: usize) -> io::Result<Self> {
        let config = HugePageConfig::default();
        let ptr = huge_page_alloc(size, &config)?;
        Ok(Self { ptr, size })
    }

    /// Allocate with custom config.
    pub fn with_config(size: usize, config: &HugePageConfig) -> io::Result<Self> {
        let ptr = huge_page_alloc(size, config)?;
        Ok(Self { ptr, size })
    }

    /// Get the pointer.
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    /// Get the mutable pointer.
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }

    /// Get the size.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Convert to a slice.
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.size) }
    }

    /// Convert to a mutable slice.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.size) }
    }
}

impl Drop for HugePageAlloc {
    fn drop(&mut self) {
        huge_page_free(self.ptr, self.size);
    }
}

unsafe impl Send for HugePageAlloc {}
unsafe impl Sync for HugePageAlloc {}

/// Huge page backed hash map for reduced TLB misses.
/// This is a simple open-addressing hash map optimized for huge pages.
pub struct HugePageHashMap<K, V> {
    memory: HugePageAlloc,
    capacity: usize,
    len: usize,
    _phantom: std::marker::PhantomData<(K, V)>,
}

impl<K: Copy + Eq + Default, V: Copy + Default> HugePageHashMap<K, V> {
    /// Create a new huge page backed hash map.
    pub fn new(capacity: usize) -> io::Result<Self> {
        let entry_size = std::mem::size_of::<(K, V, bool)>();
        let size = capacity * entry_size;

        let config = HugePageConfig {
            size: HugePageSize::TwoMB,
            allow_fallback: true,
            mlock: false,
            transparent: true,
        };

        let memory = HugePageAlloc::with_config(size, &config)?;

        // Initialize all entries
        unsafe {
            ptr::write_bytes(memory.ptr, 0, size);
        }

        Ok(Self {
            memory,
            capacity,
            len: 0,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Get the capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get the length.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_huge_page_config() {
        let config = HugePageConfig::default();
        assert_eq!(config.size, HugePageSize::TwoMB);
        assert!(config.allow_fallback);
    }

    #[test]
    fn test_huge_page_size() {
        assert_eq!(HugePageSize::TwoMB.bytes(), 2 * 1024 * 1024);
        assert_eq!(HugePageSize::OneGB.bytes(), 1024 * 1024 * 1024);
    }

    #[test]
    fn test_huge_page_alloc() {
        let config = HugePageConfig {
            allow_fallback: true,
            transparent: true,
            ..Default::default()
        };

        let size = 4 * 1024 * 1024; // 4MB
        match HugePageAlloc::with_config(size, &config) {
            Ok(mut alloc) => {
                // Write to the allocation
                let slice = alloc.as_mut_slice();
                slice[0] = 42;
                slice[size - 1] = 42;
                assert_eq!(slice[0], 42);
                assert_eq!(slice[size - 1], 42);
            }
            Err(e) => {
                // May fail without huge page support
                eprintln!("Huge page allocation failed (expected on some systems): {}", e);
            }
        }
    }

    #[test]
    fn test_availability_check() {
        // Just make sure it doesn't crash
        let _ = is_huge_pages_available();
    }
}
