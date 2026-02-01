//! CPU prefetching utilities and read-ahead optimization.
//!
//! Uses hardware prefetch instructions and read-ahead to minimize
//! cache misses and memory latency.

use std::arch::asm;

/// Prefetch data for reading (T0 = all cache levels).
#[inline(always)]
pub fn prefetch_read<T>(ptr: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        asm!(
            "prefetcht0 [{ptr}]",
            ptr = in(reg) ptr,
            options(nostack, preserves_flags)
        );
    }

    #[cfg(target_arch = "aarch64")]
    unsafe {
        asm!(
            "prfm pldl1keep, [{ptr}]",
            ptr = in(reg) ptr,
            options(nostack, preserves_flags)
        );
    }
}

/// Prefetch data for writing.
#[inline(always)]
pub fn prefetch_write<T>(ptr: *mut T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        asm!(
            "prefetchw [{ptr}]",
            ptr = in(reg) ptr,
            options(nostack, preserves_flags)
        );
    }

    #[cfg(target_arch = "aarch64")]
    unsafe {
        asm!(
            "prfm pstl1keep, [{ptr}]",
            ptr = in(reg) ptr,
            options(nostack, preserves_flags)
        );
    }
}

/// Prefetch for non-temporal access (streaming, won't pollute cache).
#[inline(always)]
pub fn prefetch_nta<T>(ptr: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        asm!(
            "prefetchnta [{ptr}]",
            ptr = in(reg) ptr,
            options(nostack, preserves_flags)
        );
    }

    #[cfg(target_arch = "aarch64")]
    unsafe {
        asm!(
            "prfm pldl1strm, [{ptr}]",
            ptr = in(reg) ptr,
            options(nostack, preserves_flags)
        );
    }
}

/// Prefetch multiple cache lines ahead.
#[inline(always)]
pub fn prefetch_range<T>(ptr: *const T, len: usize) {
    const CACHE_LINE_SIZE: usize = 64;
    let bytes = len * std::mem::size_of::<T>();
    let lines = (bytes + CACHE_LINE_SIZE - 1) / CACHE_LINE_SIZE;

    let base = ptr as *const u8;
    for i in 0..lines.min(8) {
        // Limit to 8 prefetches
        prefetch_read(unsafe { base.add(i * CACHE_LINE_SIZE) });
    }
}

/// Compiler memory barrier to prevent reordering.
#[inline(always)]
pub fn compiler_fence() {
    std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
}

/// Read-ahead buffer for sequential access patterns.
pub struct ReadAhead {
    buffer: Vec<u8>,
    file_offset: u64,
    valid_bytes: usize,
    read_size: usize,
}

impl ReadAhead {
    /// Creates a new read-ahead buffer.
    pub fn new(read_size: usize) -> Self {
        Self {
            buffer: vec![0u8; read_size * 2], // Double buffer
            file_offset: 0,
            valid_bytes: 0,
            read_size,
        }
    }

    /// Checks if the requested range is in the buffer.
    #[inline]
    pub fn contains(&self, offset: u64, len: usize) -> bool {
        offset >= self.file_offset
            && offset + len as u64 <= self.file_offset + self.valid_bytes as u64
    }

    /// Gets data from the buffer if available.
    #[inline]
    pub fn get(&self, offset: u64, len: usize) -> Option<&[u8]> {
        if !self.contains(offset, len) {
            return None;
        }
        let start = (offset - self.file_offset) as usize;
        Some(&self.buffer[start..start + len])
    }

    /// Updates the buffer with new data.
    pub fn fill(&mut self, offset: u64, data: &[u8]) {
        self.file_offset = offset;
        self.valid_bytes = data.len().min(self.buffer.len());
        self.buffer[..self.valid_bytes].copy_from_slice(&data[..self.valid_bytes]);
    }

    /// Returns the suggested next read offset for read-ahead.
    #[inline]
    pub fn next_read_offset(&self) -> u64 {
        self.file_offset + self.valid_bytes as u64
    }

    /// Returns the read size.
    #[inline]
    pub fn read_size(&self) -> usize {
        self.read_size
    }
}

/// Bloom filter for fast negative lookups (key doesn't exist).
pub struct BloomFilter {
    bits: Vec<u64>,
    num_hashes: u32,
}

impl BloomFilter {
    /// Creates a new bloom filter with approximately `expected_items` capacity
    /// and `false_positive_rate` error rate.
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        // Calculate optimal size
        let ln2 = std::f64::consts::LN_2;
        let bits_needed = -(expected_items as f64 * false_positive_rate.ln()) / (ln2 * ln2);
        let num_bits = (bits_needed as usize).max(64).next_power_of_two();
        let num_hashes = ((num_bits as f64 / expected_items as f64) * ln2).ceil() as u32;

        Self {
            bits: vec![0u64; num_bits / 64],
            num_hashes: num_hashes.max(1).min(16),
        }
    }

    /// Adds a key to the filter.
    #[inline]
    pub fn add(&mut self, key_hash: u64) {
        let num_bits = self.bits.len() * 64;
        for i in 0..self.num_hashes {
            let h = self.hash(key_hash, i);
            let bit_idx = (h as usize) % num_bits;
            let word_idx = bit_idx / 64;
            let bit_pos = bit_idx % 64;
            self.bits[word_idx] |= 1u64 << bit_pos;
        }
    }

    /// Checks if a key might be in the filter.
    /// Returns false if definitely not present, true if maybe present.
    #[inline]
    pub fn may_contain(&self, key_hash: u64) -> bool {
        let num_bits = self.bits.len() * 64;
        for i in 0..self.num_hashes {
            let h = self.hash(key_hash, i);
            let bit_idx = (h as usize) % num_bits;
            let word_idx = bit_idx / 64;
            let bit_pos = bit_idx % 64;
            if (self.bits[word_idx] & (1u64 << bit_pos)) == 0 {
                return false;
            }
        }
        true
    }

    #[inline]
    fn hash(&self, key_hash: u64, i: u32) -> u64 {
        // Double hashing technique
        let h1 = key_hash;
        let h2 = key_hash.rotate_left(32);
        h1.wrapping_add((i as u64).wrapping_mul(h2))
    }

    /// Returns the approximate fill ratio.
    pub fn fill_ratio(&self) -> f64 {
        let set_bits: u64 = self.bits.iter().map(|w| w.count_ones() as u64).sum();
        set_bits as f64 / (self.bits.len() * 64) as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_ahead() {
        let mut ra = ReadAhead::new(4096);
        let data = vec![1u8; 4096];
        ra.fill(1000, &data);

        assert!(ra.contains(1000, 100));
        assert!(ra.contains(1000, 4096));
        assert!(!ra.contains(999, 1));
        assert!(!ra.contains(1000, 4097));

        let slice = ra.get(1000, 100).unwrap();
        assert_eq!(slice.len(), 100);
    }

    #[test]
    fn test_bloom_filter() {
        let mut bf = BloomFilter::new(10000, 0.01);

        // Add some keys
        for i in 0..1000u64 {
            bf.add(i);
        }

        // Check present keys
        for i in 0..1000u64 {
            assert!(bf.may_contain(i));
        }

        // Check absent keys (some false positives expected)
        let mut false_positives = 0;
        for i in 1000..2000u64 {
            if bf.may_contain(i) {
                false_positives += 1;
            }
        }

        // Should have low false positive rate
        assert!(false_positives < 50); // <5%
    }
}
