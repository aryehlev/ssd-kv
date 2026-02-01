//! I/O layer: aligned buffers and io_uring wrapper.

pub mod aligned_buf;
pub mod uring;

pub use aligned_buf::{AlignedBuffer, BufferPool, ALIGNMENT};
pub use uring::{AsyncUring, IoOperation, IoResult, UringManager};
