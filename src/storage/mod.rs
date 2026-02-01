//! Storage layer: direct I/O, records, write buffering, and file management.

pub mod direct_io;
pub mod record;
pub mod write_buffer;
pub mod file_manager;
pub mod compaction;

pub use direct_io::DirectFile;
pub use record::{Record, RecordHeader, RecordFlags, RECORD_MAGIC, HEADER_SIZE, RECORD_ALIGNMENT};
pub use write_buffer::{WriteBuffer, WBlock, WBLOCK_SIZE};
pub use file_manager::{FileManager, DataFile, FILE_SIZE, FILE_HEADER_SIZE, WBLOCKS_PER_FILE};
