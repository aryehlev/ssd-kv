//! Object pooling to avoid allocations in hot paths.
//!
//! Pre-allocates buffers and reuses them to reduce allocation overhead.

use std::sync::atomic::{AtomicUsize, Ordering};
use parking_lot::Mutex;

/// A simple object pool for reusable buffers
pub struct BufferPool {
    buffers: Mutex<Vec<Vec<u8>>>,
    buffer_size: usize,
    max_buffers: usize,
    allocated: AtomicUsize,
    reused: AtomicUsize,
}

impl BufferPool {
    /// Creates a new buffer pool
    pub fn new(buffer_size: usize, initial_count: usize, max_buffers: usize) -> Self {
        let mut buffers = Vec::with_capacity(initial_count);
        for _ in 0..initial_count {
            buffers.push(vec![0u8; buffer_size]);
        }

        Self {
            buffers: Mutex::new(buffers),
            buffer_size,
            max_buffers,
            allocated: AtomicUsize::new(initial_count),
            reused: AtomicUsize::new(0),
        }
    }

    /// Gets a buffer from the pool (or allocates a new one)
    #[inline]
    pub fn get(&self) -> PooledBuffer {
        let buffer = {
            let mut pool = self.buffers.lock();
            pool.pop()
        };

        match buffer {
            Some(mut buf) => {
                self.reused.fetch_add(1, Ordering::Relaxed);
                buf.clear();
                PooledBuffer { buffer: buf, pool: self }
            }
            None => {
                self.allocated.fetch_add(1, Ordering::Relaxed);
                PooledBuffer {
                    buffer: Vec::with_capacity(self.buffer_size),
                    pool: self,
                }
            }
        }
    }

    /// Returns a buffer to the pool
    fn return_buffer(&self, buffer: Vec<u8>) {
        let mut pool = self.buffers.lock();
        if pool.len() < self.max_buffers {
            pool.push(buffer);
        }
        // Otherwise just drop it
    }

    /// Returns pool statistics
    pub fn stats(&self) -> PoolStats {
        PoolStats {
            allocated: self.allocated.load(Ordering::Relaxed),
            reused: self.reused.load(Ordering::Relaxed),
            pooled: self.buffers.lock().len(),
        }
    }
}

/// A buffer borrowed from the pool
pub struct PooledBuffer<'a> {
    buffer: Vec<u8>,
    pool: &'a BufferPool,
}

impl<'a> PooledBuffer<'a> {
    /// Access the underlying buffer
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.buffer
    }

    /// Access the underlying buffer mutably
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.buffer
    }

    /// Extend the buffer with data
    #[inline]
    pub fn extend_from_slice(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    /// Get the length
    #[inline]
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Check if empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Resize the buffer
    #[inline]
    pub fn resize(&mut self, new_len: usize, value: u8) {
        self.buffer.resize(new_len, value);
    }

    /// Take ownership of the buffer (won't return to pool)
    pub fn take(mut self) -> Vec<u8> {
        std::mem::take(&mut self.buffer)
    }
}

impl<'a> Drop for PooledBuffer<'a> {
    fn drop(&mut self) {
        if !self.buffer.is_empty() {
            let buffer = std::mem::take(&mut self.buffer);
            self.pool.return_buffer(buffer);
        }
    }
}

impl<'a> std::ops::Deref for PooledBuffer<'a> {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl<'a> std::ops::DerefMut for PooledBuffer<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer
    }
}

#[derive(Debug)]
pub struct PoolStats {
    pub allocated: usize,
    pub reused: usize,
    pub pooled: usize,
}

/// Global buffer pool for read operations
pub static READ_BUFFER_POOL: once_cell::sync::Lazy<BufferPool> =
    once_cell::sync::Lazy::new(|| BufferPool::new(1024 * 1024, 16, 64));

/// Global buffer pool for write operations
pub static WRITE_BUFFER_POOL: once_cell::sync::Lazy<BufferPool> =
    once_cell::sync::Lazy::new(|| BufferPool::new(1024 * 1024, 8, 32));

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool() {
        let pool = BufferPool::new(1024, 2, 10);

        // Get buffers
        let mut buf1 = pool.get();
        buf1.extend_from_slice(b"hello");
        assert_eq!(buf1.len(), 5);

        let buf2 = pool.get();

        // Return buf1
        drop(buf1);

        // Get another - should reuse buf1
        let buf3 = pool.get();

        let stats = pool.stats();
        assert!(stats.reused > 0);
    }
}
