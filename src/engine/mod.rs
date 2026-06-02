//! SIndex engine — trillion-scale SSD-based KV indexing with deterministic latency.
//!
//! Implements: "The Design of Trillion-scale SSD-based Indexing with Deterministic
//! Latency for Cloud Block Storage", ACM TOS 2024, DOI 10.1145/3789205.

pub mod btree;
pub mod ipage;
pub mod kv_engine;
pub mod segment;
pub mod value_log;
pub mod wsbcache;

pub use kv_engine::KvEngine;
