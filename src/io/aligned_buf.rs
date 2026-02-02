//! 4KB-aligned buffer pool for O_DIRECT operations.

use std::alloc::{self, Layout};
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::Arc;

use crossbeam_channel::{bounded, Receiver, Sender};

use crate::perf::simd::{simd_memcpy, simd_memset};

/// Alignment required for O_DIRECT (typically 4KB for most SSDs).
pub const ALIGNMENT: usize = 4096;

/// A buffer aligned to 4KB for direct I/O operations.
pub struct AlignedBuffer {
    ptr: NonNull<u8>,
    len: usize,
    capacity: usize,
    layout: Layout,
}

impl std::fmt::Debug for AlignedBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlignedBuffer")
            .field("len", &self.len)
            .field("capacity", &self.capacity)
            .finish()
    }
}

// SAFETY: AlignedBuffer owns its memory and can be sent between threads.
unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

impl AlignedBuffer {
    /// Creates a new aligned buffer with the given capacity.
    /// Capacity will be rounded up to the nearest ALIGNMENT boundary.
    pub fn new(capacity: usize) -> Self {
        let aligned_capacity = align_up(capacity, ALIGNMENT);
        let layout = Layout::from_size_align(aligned_capacity, ALIGNMENT)
            .expect("Invalid layout for aligned buffer");

        // SAFETY: Layout is valid (non-zero size, power-of-two alignment).
        let ptr = unsafe { alloc::alloc_zeroed(layout) };
        let ptr = NonNull::new(ptr).expect("Failed to allocate aligned buffer");

        Self {
            ptr,
            len: 0,
            capacity: aligned_capacity,
            layout,
        }
    }

    /// Returns the current length of data in the buffer.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the total capacity of the buffer.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the remaining capacity available.
    #[inline]
    pub fn remaining(&self) -> usize {
        self.capacity - self.len
    }

    /// Sets the length of valid data in the buffer.
    ///
    /// # Safety
    /// Caller must ensure that `new_len <= capacity` and that
    /// data up to `new_len` is properly initialized.
    #[inline]
    pub unsafe fn set_len(&mut self, new_len: usize) {
        debug_assert!(new_len <= self.capacity);
        self.len = new_len;
    }

    /// Returns the buffer as a raw pointer.
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    /// Returns the buffer as a mutable raw pointer.
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Clears the buffer, setting length to 0.
    #[inline]
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Appends data to the buffer. Returns the number of bytes written.
    /// Uses SIMD-accelerated copy for large data.
    pub fn extend_from_slice(&mut self, data: &[u8]) -> usize {
        let to_copy = data.len().min(self.remaining());
        if to_copy > 0 {
            // SAFETY: We checked bounds and the memory is valid.
            let dst = unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr().add(self.len), to_copy) };
            simd_memcpy(dst, &data[..to_copy]);
            self.len += to_copy;
        }
        to_copy
    }

    /// Resizes the buffer, filling new space with zeros if growing.
    /// Uses SIMD-accelerated memset for large fills.
    pub fn resize(&mut self, new_len: usize) {
        if new_len > self.capacity {
            panic!("Cannot resize beyond capacity");
        }
        if new_len > self.len {
            // Zero-fill the new space using SIMD
            let fill_len = new_len - self.len;
            let dst = unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr().add(self.len), fill_len) };
            simd_memset(dst, 0);
        }
        self.len = new_len;
    }

    /// Returns the aligned length (rounded up to ALIGNMENT).
    #[inline]
    pub fn aligned_len(&self) -> usize {
        align_up(self.len, ALIGNMENT)
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        // SAFETY: ptr was allocated with this layout.
        unsafe {
            alloc::dealloc(self.ptr.as_ptr(), self.layout);
        }
    }
}

impl Deref for AlignedBuffer {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        // SAFETY: ptr is valid and len bytes are initialized.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl DerefMut for AlignedBuffer {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: ptr is valid and len bytes are initialized.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl Clone for AlignedBuffer {
    fn clone(&self) -> Self {
        let mut new_buf = AlignedBuffer::new(self.capacity);
        new_buf.extend_from_slice(self);
        new_buf
    }
}

/// A pool of pre-allocated aligned buffers for reuse.
pub struct BufferPool {
    sender: Sender<AlignedBuffer>,
    receiver: Receiver<AlignedBuffer>,
    buffer_size: usize,
}

impl BufferPool {
    /// Creates a new buffer pool with the specified number of buffers and buffer size.
    pub fn new(pool_size: usize, buffer_size: usize) -> Arc<Self> {
        let (sender, receiver) = bounded(pool_size);

        // Pre-allocate buffers
        for _ in 0..pool_size {
            let buf = AlignedBuffer::new(buffer_size);
            let _ = sender.try_send(buf);
        }

        Arc::new(Self {
            sender,
            receiver,
            buffer_size,
        })
    }

    /// Acquires a buffer from the pool, blocking if none available.
    pub fn acquire(&self) -> AlignedBuffer {
        self.receiver
            .recv()
            .unwrap_or_else(|_| AlignedBuffer::new(self.buffer_size))
    }

    /// Tries to acquire a buffer without blocking.
    pub fn try_acquire(&self) -> Option<AlignedBuffer> {
        self.receiver.try_recv().ok()
    }

    /// Returns a buffer to the pool.
    pub fn release(&self, mut buf: AlignedBuffer) {
        buf.clear();
        // If pool is full, buffer is dropped
        let _ = self.sender.try_send(buf);
    }

    /// Returns the configured buffer size.
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }
}

/// Rounds `n` up to the nearest multiple of `align`.
#[inline]
pub const fn align_up(n: usize, align: usize) -> usize {
    debug_assert!(align.is_power_of_two());
    (n + align - 1) & !(align - 1)
}

/// Rounds `n` down to the nearest multiple of `align`.
#[inline]
pub const fn align_down(n: usize, align: usize) -> usize {
    debug_assert!(align.is_power_of_two());
    n & !(align - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aligned_buffer_creation() {
        let buf = AlignedBuffer::new(1000);
        assert_eq!(buf.capacity(), ALIGNMENT); // Rounded up to 4096
        assert_eq!(buf.len(), 0);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_aligned_buffer_write() {
        let mut buf = AlignedBuffer::new(4096);
        let data = b"Hello, World!";
        let written = buf.extend_from_slice(data);
        assert_eq!(written, data.len());
        assert_eq!(buf.len(), data.len());
        assert_eq!(&buf[..data.len()], data);
    }

    #[test]
    fn test_buffer_pool() {
        let pool = BufferPool::new(2, 4096);

        let buf1 = pool.acquire();
        let buf2 = pool.acquire();

        assert!(pool.try_acquire().is_none());

        pool.release(buf1);
        assert!(pool.try_acquire().is_some());

        pool.release(buf2);
    }

    #[test]
    fn test_align_up() {
        assert_eq!(align_up(0, 4096), 0);
        assert_eq!(align_up(1, 4096), 4096);
        assert_eq!(align_up(4096, 4096), 4096);
        assert_eq!(align_up(4097, 4096), 8192);
    }
}
