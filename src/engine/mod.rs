//! Engine layer: index and recovery.

pub mod index;
pub mod index_entry;
pub mod recovery;

pub use index::{Index, IndexStats, NUM_SHARDS};
pub use index_entry::{hash_key, IndexEntry, KeyStorage, MAX_INLINE_KEY_SIZE};
pub use recovery::{recover_index, RecoveryStats};
