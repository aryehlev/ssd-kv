//! Individual index allocations (DESIGN §9.3, LC-05, LC-09).
//!
//! Every segment array, hint plane and directory is one individual allocation made through
//! [`seg_alloc`] and returned through [`seg_free`]: an anonymous private `mmap` for requests of
//! 64 KiB or more, otherwise `std::alloc::System` (the C allocator, freed per object). There is
//! no pooling and no grouping of several objects in one allocation. Index allocations are
//! private process memory only (LC-19).
//!
//! This is the only file of `kv-index` allowed to contain `unsafe` (§24); every block carries a
//! `SAFETY:` comment.
#![allow(unsafe_code)]

use core::ptr::NonNull;

/// Requests of at least this many bytes are served by an anonymous mapping.
pub const MMAP_THRESHOLD: usize = 64 * 1024;

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

    pub fn as_slice(&self) -> &[u8] {
        todo!("DESIGN §9.3: SysBuf view")
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        todo!("DESIGN §9.3: SysBuf view")
    }
}

/// Allocates `bytes` zeroed bytes as one individual allocation.
pub fn seg_alloc(bytes: usize) -> SysBuf {
    todo!("DESIGN §9.3: seg_alloc (mmap >= 64 KiB, else System)")
}

/// Returns an allocation made by [`seg_alloc`].
pub fn seg_free(b: SysBuf) {
    todo!("DESIGN §9.3: seg_free")
}
