//! Storage layer: direct I/O, records, write buffering, and file management.
//!
//! This module provides:
//! - **DirectFile**: O_DIRECT file I/O for bypassing kernel page cache
//! - **WriteBuffer**: Batched write buffer aligned to SSD pages
//! - **WAL**: Write-ahead log for fast sequential writes and crash recovery
//! - **FileManager**: Data file lifecycle management
//! - **Compaction**: Background compaction and defragmentation

pub mod direct_io;
pub mod record;
pub mod write_buffer;
pub mod file_manager;
pub mod compaction;
pub mod wal;

pub use direct_io::DirectFile;
pub use record::{Record, RecordHeader, RecordFlags, RECORD_MAGIC, HEADER_SIZE, RECORD_ALIGNMENT};
pub use write_buffer::{WriteBuffer, WBlock, WBLOCK_SIZE};
pub use file_manager::{FileManager, DataFile, ParallelFileManager, FILE_SIZE, FILE_HEADER_SIZE, WBLOCKS_PER_FILE};
pub use wal::{WriteAheadLog, WalConfig, WalEntry, WalEntryHeader, WalStats};
