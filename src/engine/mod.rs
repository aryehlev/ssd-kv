//! Engine layer: index and recovery.
//!
//! This module provides multiple index implementations:
//! - **Index**: Original sharded RwLock-based index
//! - **LockFreeIndex**: Lock-free hash table with open addressing (Aerospike SPRIGS-inspired)
//! - **ShardPerCoreEngine**: Shard-per-core architecture (ScyllaDB-inspired)

pub mod index;
pub mod index_entry;
pub mod recovery;
pub mod lockfree_index;
pub mod shard_per_core;

pub use index::{Index, IndexStats, NUM_SHARDS};
pub use index_entry::{hash_key, IndexEntry, KeyStorage, MAX_INLINE_KEY_SIZE};
pub use recovery::{recover_index, RecoveryStats};
pub use lockfree_index::{LockFreeIndex, Bucket, GetResult, compare_keys_simd};
pub use shard_per_core::{ShardPerCoreEngine, Shard, ShardMessage, ShardStats, SwmrCell};
