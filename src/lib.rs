//! SSD-KV: High-Performance Key-Value Store
//!
//! - Index in RAM for fast lookups
//! - Data on SSD with O_DIRECT for consistent latency
//! - io_uring for async I/O (Linux)
//! - Sharded hashmap index with 256 shards
//! - Lock-free hot cache for frequently accessed keys
//! - Bloom filter for fast negative lookups
//! - CPU prefetching and NUMA awareness

pub mod cluster;
pub mod config;
pub mod engine;
pub mod io;
pub mod perf;
pub mod server;
pub mod storage;
