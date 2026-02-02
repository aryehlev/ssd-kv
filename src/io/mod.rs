//! I/O layer: aligned buffers, io_uring wrapper, and I/O thread pool.

pub mod aligned_buf;
pub mod io_pool;
pub mod uring;

pub use aligned_buf::{AlignedBuffer, BufferPool, ALIGNMENT};
pub use io_pool::{BatchedReader, IoPool, IoPoolRequest, IoPoolStats};
pub use uring::{AsyncUring, BatchedUring, IoOperation, IoResult, UringManager};
