//! SIndex KV: Trillion-scale SSD-based indexing with deterministic latency.
//!
//! Implements: "The Design of Trillion-scale SSD-based Indexing with
//! Deterministic Latency for Cloud Block Storage", ACM TOS 2024.
//!
//! ## Key design points
//! - Multi-Level Index (MLI): partition hash table + per-partition B+ trees
//! - Segments: per-partition 4 KB ipage files on SSD
//! - Variable-value support via a shared append-only value log
//! - Bounded B-Tree height → at most BTREE_MAX_HEIGHT + 1 SSD reads per GET
//! - io_uring for high-throughput network I/O

pub mod config;
pub mod engine;
pub mod io;
pub mod server;
