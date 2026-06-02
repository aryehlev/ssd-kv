//! Storage layer: ipage format, segment files, segment manager, WAL, eviction.

pub mod ipage;
pub mod file_manager;
pub mod gc;
pub mod segment_file;
pub mod segment_manager;
pub mod write_buffer;
pub mod eviction;
pub mod memory_store;
pub mod wal;

pub use segment_manager::{SegmentManager, SegmentConfig};
pub use write_buffer::WriteBuffer;
pub use wal::{WriteAheadLog, WalConfig, WalEntry, WalEntryHeader, WalStats};

/// Compatibility alias: existing tests import `FileManager`.
pub type FileManager = SegmentManager;
