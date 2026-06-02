//! Compatibility shim: re-exports SegmentManager as FileManager.
pub use crate::storage::segment_manager::SegmentManager as FileManager;
pub use crate::storage::segment_manager::SegmentManager;
// Legacy constants kept for test compat (values are unused in new engine).
pub const FILE_SIZE: u64 = 64 * 1024 * 1024;
pub const FILE_HEADER_SIZE: usize = 4096;
pub const WBLOCKS_PER_FILE: usize = 64;
