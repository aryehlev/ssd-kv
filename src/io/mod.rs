//! I/O layer: aligned buffers, io_uring wrapper, and I/O thread pool.

pub mod aligned_buf;
pub mod async_reader;
pub mod io_pool;
pub mod uring;
pub mod uring_net;

pub use aligned_buf::{AlignedBuffer, BufferPool, ALIGNMENT};
pub use async_reader::AsyncReader;
pub use io_pool::{BatchedReader, IoPool, IoPoolRequest, IoPoolStats};
pub use uring::{AsyncUring, BatchedUring, IoOperation, IoResult, UringManager};
pub use uring_net::{NetBufferPool, NetEvent, NetOperation, NetResult, TrackedConnection, UringNet, UringServer};
