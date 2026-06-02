//! I/O layer: io_uring networking.

pub mod uring_net;

pub use uring_net::{NetBufferPool, NetEvent, NetOperation, NetResult, TrackedConnection, UringNet, UringServer};
