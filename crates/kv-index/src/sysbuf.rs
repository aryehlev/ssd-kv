//! Individual index allocations (DESIGN §9.3, LC-05, LC-09).
//!
//! Every segment array, hint plane and directory is one individual allocation made through
//! [`seg_alloc`] and returned through [`seg_free`]: an anonymous private `mmap` for requests of
//! 64 KiB or more, otherwise `std::alloc::System` (the C allocator, freed per object). There is
//! no pooling and no grouping of several objects in one allocation. Index allocations are
//! private process memory only (LC-19).
//!
//! If the kernel refuses a mapping (for example when `vm.max_map_count` is reached) the request
//! is served by `std::alloc::System` instead, which is still one individual allocation; only if
//! that fails too does the process report an allocation failure.
//!
//! This is the only file of `kv-index` allowed to contain `unsafe` (§24); every block carries a
//! `SAFETY:` comment.
#![allow(unsafe_code)]

use core::ptr::NonNull;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Requests of at least this many bytes are served by an anonymous mapping.
pub const MMAP_THRESHOLD: usize = 64 * 1024;

/// Alignment of allocations served by `System`.
const ALIGN: usize = 8;

/// One index allocation (zero-initialized bytes).
pub struct SysBuf {
    ptr: NonNull<u8>,
    len: usize,
    mapped: bool,
}

// SAFETY: a SysBuf exclusively owns its allocation (no aliasing pointers exist outside it), so
// moving it to another thread is sound; shared references only permit reads.
unsafe impl Send for SysBuf {}
// SAFETY: `&SysBuf` exposes only `&[u8]` reads of memory the buffer owns.
unsafe impl Sync for SysBuf {}

impl SysBuf {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// `true` when the buffer is an anonymous mapping.
    pub fn is_mapped(&self) -> bool {
        self.mapped
    }

    /// Bytes the allocation occupies: the length rounded up to whole pages for mappings, the
    /// requested length otherwise (the C allocator's own per-object header is not counted).
    pub fn alloc_bytes(&self) -> usize {
        if self.mapped {
            self.len.next_multiple_of(page_size())
        } else {
            self.len
        }
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: `ptr` points to `len` initialized (zeroed at allocation) bytes owned by this
        // buffer for its whole lifetime (or is dangling with `len == 0`), and `&self` excludes
        // concurrent mutation.
        unsafe { core::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: as in `as_slice`; `&mut self` guarantees the returned slice is the only live
        // reference to these bytes.
        unsafe { core::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl Drop for SysBuf {
    fn drop(&mut self) {
        if self.len == 0 {
            return;
        }
        if self.mapped {
            // SAFETY: `ptr`/`len` describe exactly the mapping returned by `mmap` in
            // `seg_alloc`; it is unmapped once, here, and no reference into it survives `drop`.
            let rc = unsafe { libc::munmap(self.ptr.as_ptr().cast(), self.len) };
            debug_assert_eq!(rc, 0, "munmap of an index buffer failed");
        } else {
            // SAFETY: the block was allocated by `System.alloc_zeroed` with this same layout
            // (`len` was a valid layout size then) and is freed once, here.
            unsafe {
                System.dealloc(
                    self.ptr.as_ptr(),
                    Layout::from_size_align_unchecked(self.len, ALIGN),
                )
            };
        }
    }
}

impl core::fmt::Debug for SysBuf {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SysBuf")
            .field("len", &self.len)
            .field("mapped", &self.mapped)
            .finish()
    }
}

/// Allocates `bytes` zeroed bytes as one individual allocation.
pub fn seg_alloc(bytes: usize) -> SysBuf {
    if bytes == 0 {
        return SysBuf {
            ptr: NonNull::dangling(),
            len: 0,
            mapped: false,
        };
    }
    if bytes >= MMAP_THRESHOLD
        && let Some(ptr) = map_anon(bytes)
    {
        return SysBuf {
            ptr,
            len: bytes,
            mapped: true,
        };
    }
    let layout = match Layout::from_size_align(bytes, ALIGN) {
        Ok(l) => l,
        Err(_) => panic!("index allocation of {bytes} bytes exceeds the address space"),
    };
    // SAFETY: `layout` has a non-zero size.
    let p = unsafe { System.alloc_zeroed(layout) };
    match NonNull::new(p) {
        Some(ptr) => SysBuf {
            ptr,
            len: bytes,
            mapped: false,
        },
        None => std::alloc::handle_alloc_error(layout),
    }
}

/// Returns an allocation made by [`seg_alloc`].
pub fn seg_free(b: SysBuf) {
    drop(b);
}

/// A private anonymous read/write mapping of `bytes` bytes, or `None` if the kernel refuses.
fn map_anon(bytes: usize) -> Option<NonNull<u8>> {
    // SAFETY: an anonymous private mapping at a kernel-chosen address does not alias any
    // existing Rust object; the arguments are valid for `mmap(2)`.
    let p = unsafe {
        libc::mmap(
            core::ptr::null_mut(),
            bytes,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        )
    };
    if p == libc::MAP_FAILED {
        None
    } else {
        NonNull::new(p.cast())
    }
}

/// The system page size (cached).
fn page_size() -> usize {
    static PAGE: AtomicUsize = AtomicUsize::new(0);
    let cached = PAGE.load(Ordering::Relaxed);
    if cached != 0 {
        return cached;
    }
    // SAFETY: `sysconf` has no memory-safety preconditions.
    let v = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    let v = if v > 0 { v as usize } else { 4096 };
    PAGE.store(v, Ordering::Relaxed);
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zeroed_and_writable() {
        for bytes in [
            2,
            10,
            240,
            4095,
            MMAP_THRESHOLD - 1,
            MMAP_THRESHOLD,
            83_280,
            1 << 20,
        ] {
            let mut b = seg_alloc(bytes);
            assert_eq!(b.len(), bytes);
            assert!(!b.is_empty());
            assert_eq!(b.is_mapped(), bytes >= MMAP_THRESHOLD);
            assert!(b.as_slice().iter().all(|&x| x == 0));
            let s = b.as_mut_slice();
            s[0] = 1;
            s[bytes - 1] = 2;
            assert_eq!(b.as_slice()[0], 1);
            assert_eq!(b.as_slice()[bytes - 1], 2);
            assert!(b.alloc_bytes() >= bytes);
            if b.is_mapped() {
                assert_eq!(b.alloc_bytes() % page_size(), 0);
            } else {
                assert_eq!(b.alloc_bytes(), bytes);
            }
            seg_free(b);
        }
    }

    #[test]
    fn zero_length() {
        let b = seg_alloc(0);
        assert!(b.is_empty());
        assert_eq!(b.as_slice(), &[] as &[u8]);
        assert_eq!(b.alloc_bytes(), 0);
        seg_free(b);
    }

    #[test]
    fn independent_allocations() {
        // Each request is its own allocation: writing one never shows in another.
        let mut a = seg_alloc(100);
        let b = seg_alloc(100);
        a.as_mut_slice().fill(0xAA);
        assert!(b.as_slice().iter().all(|&x| x == 0));
        assert_ne!(a.as_slice().as_ptr(), b.as_slice().as_ptr());
    }
}
