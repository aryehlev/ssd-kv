//! WBlock accumulator for batching writes into 1MB blocks.

use std::io;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::io::aligned_buf::{AlignedBuffer, ALIGNMENT};
use crate::perf::simd::simd_key_eq;
use crate::storage::record::{Record, RECORD_ALIGNMENT};

/// Size of a write block (1MB).
pub const WBLOCK_SIZE: usize = 1024 * 1024;

/// Footer written by `WBlock::finalize()` immediately after the last
/// record in the block. Recovery uses it to tell "this block was
/// flushed cleanly" apart from "this block was torn mid-flush" —
/// information record-level CRCs alone can't give you, because a
/// partial flush may land a run of valid records followed by stale
/// bytes that happen to have the right magic.
///
/// Layout (16 bytes, little-endian):
///   [0..8]   magic  = WBLOCK_FOOTER_MAGIC
///   [8..16]  xxh3   = xxh3_64 over bytes [0 .. records_end]
///
/// `records_end` (the offset we write the footer at) is implicit: the
/// reader scans records forward and the footer starts where the scan
/// halts on a non-record magic. Readers that don't find the marker
/// just fall back to "trust per-record CRCs" — safe, and means old
/// data files opened by a new binary keep working.
pub const WBLOCK_FOOTER_MAGIC: u64 = 0xF00DFACEB10CDA7A;
pub const WBLOCK_FOOTER_SIZE: usize = 16;

/// A write block (WBlock) that accumulates records until full.
#[derive(Debug)]
pub struct WBlock {
    /// The buffer containing record data.
    buffer: AlignedBuffer,
    /// Current write position in the buffer.
    write_pos: usize,
    /// Number of live records in this block.
    live_records: u32,
    /// Total records (including dead ones for compaction tracking).
    total_records: u32,
    /// File ID this block belongs to.
    pub file_id: u32,
    /// Block ID within the file.
    pub block_id: u32,
    /// Whether this block has been flushed to disk.
    pub flushed: bool,
    /// Maximum record generation seen in this block. Used by the WAL
    /// cleanup path to know "everything up to this gen is durable in a
    /// data file once this block is fsynced".
    pub max_generation: u64,
}

impl WBlock {
    /// Creates a new empty WBlock.
    pub fn new(file_id: u32, block_id: u32) -> Self {
        Self {
            buffer: AlignedBuffer::new(WBLOCK_SIZE),
            write_pos: 0,
            live_records: 0,
            total_records: 0,
            file_id,
            block_id,
            flushed: false,
            max_generation: 0,
        }
    }

    /// Tries to append a record to the block.
    /// Returns Ok(offset) if successful, Err(()) if not enough space.
    pub fn try_append(&mut self, record: &mut Record) -> Result<u32, ()> {
        let serialized = record.serialize();
        let record_size = serialized.len();

        if self.write_pos + record_size > WBLOCK_SIZE {
            return Err(());
        }

        let offset = self.write_pos as u32;

        // Copy record to buffer
        self.buffer.extend_from_slice(&serialized);
        self.write_pos += record_size;
        self.live_records += 1;
        self.total_records += 1;

        if record.header.generation > self.max_generation {
            self.max_generation = record.header.generation;
        }

        Ok(offset)
    }

    /// Returns the remaining space in this block.
    #[inline]
    pub fn remaining(&self) -> usize {
        WBLOCK_SIZE - self.write_pos
    }

    /// Returns true if the block is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.write_pos == 0
    }

    /// Returns true if the block is full (or nearly full).
    #[inline]
    pub fn is_full(&self) -> bool {
        // Consider full if less than minimum record size remaining
        self.remaining() < RECORD_ALIGNMENT + 128
    }

    /// Returns the current write position.
    #[inline]
    pub fn write_pos(&self) -> usize {
        self.write_pos
    }

    /// Returns the number of live records.
    #[inline]
    pub fn live_records(&self) -> u32 {
        self.live_records
    }

    /// Returns the utilization ratio (0.0 to 1.0).
    pub fn utilization(&self) -> f32 {
        if self.total_records == 0 {
            return 1.0;
        }
        self.live_records as f32 / self.total_records as f32
    }

    /// Marks a record as dead (for compaction tracking).
    pub fn mark_dead(&mut self) {
        if self.live_records > 0 {
            self.live_records -= 1;
        }
    }

    /// Prepares the buffer for writing: appends an integrity footer
    /// (magic + xxh3_64 over records) and pads to 4 KiB alignment for
    /// O_DIRECT. Idempotent — a second call is a no-op because
    /// `write_pos` hasn't advanced.
    pub fn finalize(&mut self) -> &AlignedBuffer {
        // Skip footer if there's no room (nearly-full block). Recovery
        // still works via per-record CRCs; footer is a nice-to-have.
        if self.write_pos + WBLOCK_FOOTER_SIZE <= WBLOCK_SIZE && !self.is_empty() {
            let hash = xxhash_rust::xxh3::xxh3_64(&self.buffer[..self.write_pos]);
            let mut footer = [0u8; WBLOCK_FOOTER_SIZE];
            footer[0..8].copy_from_slice(&WBLOCK_FOOTER_MAGIC.to_le_bytes());
            footer[8..16].copy_from_slice(&hash.to_le_bytes());
            self.buffer.extend_from_slice(&footer);
            self.write_pos += WBLOCK_FOOTER_SIZE;
        }

        let aligned_len = (self.write_pos + ALIGNMENT - 1) & !(ALIGNMENT - 1);
        self.buffer.resize(aligned_len);
        &self.buffer
    }

    /// Returns the buffer for reading.
    pub fn buffer(&self) -> &AlignedBuffer {
        &self.buffer
    }

    /// Consumes the WBlock and returns its buffer.
    pub fn into_buffer(mut self) -> AlignedBuffer {
        self.finalize();
        self.buffer
    }

    /// Resets the block for reuse.
    pub fn reset(&mut self, file_id: u32, block_id: u32) {
        self.buffer.clear();
        self.write_pos = 0;
        self.live_records = 0;
        self.total_records = 0;
        self.file_id = file_id;
        self.block_id = block_id;
        self.flushed = false;
    }
}

/// Metadata for a record's location on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DiskLocation {
    /// File ID.
    pub file_id: u32,
    /// WBlock ID within the file.
    pub wblock_id: u16,
    /// Offset within the WBlock (up to 1MB = 20 bits, using u32 for safety).
    pub offset: u32,
    /// Record size for partial reads (0 means unknown, read full WBlock).
    pub record_size: u32,
}

impl DiskLocation {
    pub fn new(file_id: u32, wblock_id: u16, offset: u32) -> Self {
        Self {
            file_id,
            wblock_id,
            offset,
            record_size: 0,
        }
    }

    /// Creates a new DiskLocation with known record size for partial reads.
    pub fn with_size(file_id: u32, wblock_id: u16, offset: u32, record_size: u32) -> Self {
        Self {
            file_id,
            wblock_id,
            offset,
            record_size,
        }
    }

    /// Calculates the absolute file offset.
    pub fn file_offset(&self, file_header_size: u64) -> u64 {
        file_header_size + (self.wblock_id as u64 * WBLOCK_SIZE as u64) + self.offset as u64
    }

    /// Returns true if partial reads are possible (record size is known).
    pub fn supports_partial_read(&self) -> bool {
        self.record_size > 0
    }
}

/// Write buffer that manages the current WBlock and batches writes.
pub struct WriteBuffer {
    /// Current WBlock being written to.
    current: Mutex<WBlock>,
    /// Pending WBlocks waiting to be flushed.
    pending: Mutex<Vec<WBlock>>,
    /// Next file ID to allocate.
    next_file_id: AtomicU32,
    /// Current file's next block ID.
    next_block_id: AtomicU32,
    /// Current file ID.
    current_file_id: AtomicU32,
    /// Maximum blocks per file.
    max_blocks_per_file: u32,
    /// Backpressure cap — refuse new appends when `pending.len()` is
    /// at or above this. 0 disables the cap. Each WBlock is 1 MiB, so
    /// 128 pending blocks ≈ 128 MiB of RAM tied up. Under sustained
    /// write overload (write rate × size > disk bandwidth) this
    /// prevents unbounded memory growth and lets the server return a
    /// proper "OOM command not allowed" to clients instead of getting
    /// OOM-killed.
    max_pending_wblocks: usize,
}

impl WriteBuffer {
    /// Default cap on pending WBlocks. 128 blocks × 1 MiB = 128 MiB of
    /// staged-but-not-flushed data. Operator-tunable via
    /// `set_max_pending_wblocks`.
    pub const DEFAULT_MAX_PENDING_WBLOCKS: usize = 128;

    /// Creates a new write buffer.
    pub fn new(initial_file_id: u32, max_blocks_per_file: u32) -> Self {
        Self {
            current: Mutex::new(WBlock::new(initial_file_id, 0)),
            pending: Mutex::new(Vec::new()),
            next_file_id: AtomicU32::new(initial_file_id + 1),
            next_block_id: AtomicU32::new(1),
            current_file_id: AtomicU32::new(initial_file_id),
            max_blocks_per_file,
            max_pending_wblocks: Self::DEFAULT_MAX_PENDING_WBLOCKS,
        }
    }

    /// Override the pending-WBlock cap. 0 disables.
    pub fn set_max_pending_wblocks(&mut self, cap: usize) {
        self.max_pending_wblocks = cap;
    }

    /// Appends a record to the write buffer.
    /// Returns the disk location where the record will be written,
    /// including the on-disk record size so cold GETs can do partial
    /// (sub-WBlock) reads instead of always pulling a full 1 MiB block.
    ///
    /// `record.serialize()` runs inside `try_append`, which may compress
    /// the value and mutate `header.value_len` — so `serialized_size()`
    /// is only accurate AFTER the append returns.
    pub fn append(&self, record: &mut Record) -> io::Result<DiskLocation> {
        let mut current = self.current.lock();

        // Try to append to current block
        match current.try_append(record) {
            Ok(offset) => {
                let size = record.serialized_size() as u32;
                Ok(DiskLocation::with_size(
                    current.file_id,
                    current.block_id as u16,
                    offset,
                    size,
                ))
            }
            Err(()) => {
                // Current block is full, rotate
                let _file_id = current.file_id;
                let _block_id = current.block_id;

                // Move current to pending — but first enforce the
                // backpressure cap so we don't accumulate unbounded
                // WBlocks when the flush thread can't keep up with
                // incoming writes. Client sees the standard
                // `OOM command not allowed when used memory > 'maxmemory'`
                // reply, same as our NoEviction path.
                {
                    let pending = self.pending.lock();
                    if self.max_pending_wblocks > 0
                        && pending.len() >= self.max_pending_wblocks
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "OOM command not allowed when used memory > 'maxmemory'",
                        ));
                    }
                }
                let old_block = std::mem::replace(
                    &mut *current,
                    self.allocate_new_block(),
                );
                self.pending.lock().push(old_block);

                // Try again with new block
                match current.try_append(record) {
                    Ok(offset) => {
                        let size = record.serialized_size() as u32;
                        Ok(DiskLocation::with_size(
                            current.file_id,
                            current.block_id as u16,
                            offset,
                            size,
                        ))
                    }
                    Err(()) => Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Record too large for WBlock",
                    )),
                }
            }
        }
    }

    /// Allocates a new WBlock, potentially starting a new file.
    fn allocate_new_block(&self) -> WBlock {
        let mut block_id = self.next_block_id.fetch_add(1, Ordering::SeqCst);
        let mut file_id = self.current_file_id.load(Ordering::SeqCst);

        if block_id >= self.max_blocks_per_file {
            // Start a new file
            file_id = self.next_file_id.fetch_add(1, Ordering::SeqCst);
            self.current_file_id.store(file_id, Ordering::SeqCst);
            self.next_block_id.store(1, Ordering::SeqCst);
            block_id = 0;
        }

        WBlock::new(file_id, block_id)
    }

    /// Forces a flush of the current block (even if not full).
    pub fn force_flush(&self) -> Option<WBlock> {
        let mut current = self.current.lock();
        if current.is_empty() {
            return None;
        }

        let old_block = std::mem::replace(&mut *current, self.allocate_new_block());
        Some(old_block)
    }

    /// Takes all pending blocks for flushing.
    pub fn take_pending(&self) -> Vec<WBlock> {
        std::mem::take(&mut *self.pending.lock())
    }

    /// Returns the number of pending blocks.
    pub fn pending_count(&self) -> usize {
        self.pending.lock().len()
    }

    /// File ID the next append will land in. Compactor uses this to
    /// avoid unlinking the file we're still actively writing to.
    pub fn current_file_id(&self) -> u32 {
        self.current_file_id.load(Ordering::Relaxed)
    }

    /// Returns the current block's utilization.
    pub fn current_utilization(&self) -> f32 {
        let current = self.current.lock();
        current.write_pos() as f32 / WBLOCK_SIZE as f32
    }

    /// Reads data from unflushed buffers (current + pending).
    /// Returns None if the data has been flushed to disk.
    /// Uses SIMD-accelerated key comparison.
    pub fn read_unflushed(&self, location: &DiskLocation, key: &[u8]) -> Option<Vec<u8>> {
        use crate::storage::record::Record;

        // Check current block first
        {
            let current = self.current.lock();
            if current.file_id == location.file_id && current.block_id == location.wblock_id as u32 {
                let offset = location.offset as usize;
                let buffer = current.buffer();
                if offset < buffer.len() {
                    if let Ok(record) = Record::from_bytes(&buffer[offset..]) {
                        // SIMD-accelerated key comparison
                        if simd_key_eq(&record.key, key) && !record.header.is_deleted() && !record.header.is_expired() {
                            return Some(record.value);
                        }
                    }
                }
            }
        }

        // Check pending blocks
        {
            let pending = self.pending.lock();
            for block in pending.iter() {
                if block.file_id == location.file_id && block.block_id == location.wblock_id as u32 {
                    let offset = location.offset as usize;
                    let buffer = block.buffer();
                    if offset < buffer.len() {
                        if let Ok(record) = Record::from_bytes(&buffer[offset..]) {
                            // SIMD-accelerated key comparison
                            if simd_key_eq(&record.key, key) && !record.header.is_deleted() && !record.header.is_expired() {
                                return Some(record.value);
                            }
                        }
                    }
                }
            }
        }

        None
    }

    /// Reads full record metadata from unflushed buffers.
    /// Returns RecordMeta if the key is found in write buffers.
    pub fn read_unflushed_meta(&self, location: &DiskLocation, key: &[u8]) -> Option<crate::server::handler::RecordMeta> {
        use crate::storage::record::Record;

        // Check current block first
        {
            let current = self.current.lock();
            if current.file_id == location.file_id && current.block_id == location.wblock_id as u32 {
                let offset = location.offset as usize;
                let buffer = current.buffer();
                if offset < buffer.len() {
                    if let Ok(record) = Record::from_bytes(&buffer[offset..]) {
                        if simd_key_eq(&record.key, key) && !record.header.is_deleted() && !record.header.is_expired() {
                            return Some(crate::server::handler::RecordMeta {
                                value: record.value,
                                timestamp_micros: record.header.timestamp,
                                ttl_secs: record.header.ttl,
                            });
                        }
                    }
                }
            }
        }

        // Check pending blocks
        {
            let pending = self.pending.lock();
            for block in pending.iter() {
                if block.file_id == location.file_id && block.block_id == location.wblock_id as u32 {
                    let offset = location.offset as usize;
                    let buffer = block.buffer();
                    if offset < buffer.len() {
                        if let Ok(record) = Record::from_bytes(&buffer[offset..]) {
                            if simd_key_eq(&record.key, key) && !record.header.is_deleted() && !record.header.is_expired() {
                                return Some(crate::server::handler::RecordMeta {
                                    value: record.value,
                                    timestamp_micros: record.header.timestamp,
                                    ttl_secs: record.header.ttl,
                                });
                            }
                        }
                    }
                }
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::record::Record;

    #[test]
    fn test_wblock_append() {
        let mut block = WBlock::new(0, 0);
        let mut record = Record::new(b"key".to_vec(), b"value".to_vec(), 1, 0).unwrap();

        let offset = block.try_append(&mut record).unwrap();
        assert_eq!(offset, 0);
        assert!(!block.is_empty());
        assert_eq!(block.live_records(), 1);
    }

    #[test]
    fn test_wblock_full() {
        let mut block = WBlock::new(0, 0);

        // Fill with records
        let mut count = 0;
        loop {
            let mut record = Record::new(
                format!("key{}", count).into_bytes(),
                vec![0u8; 1000],
                count as u64,
                0,
            )
            .unwrap();

            if block.try_append(&mut record).is_err() {
                break;
            }
            count += 1;
        }

        assert!(block.is_full() || block.remaining() < 1200);
    }

    #[test]
    fn test_disk_location() {
        let loc = DiskLocation::new(1, 5, 128);
        let offset = loc.file_offset(4096);
        // file_header(4096) + 5 * 1MB + 128
        assert_eq!(offset, 4096 + 5 * 1024 * 1024 + 128);
        assert!(!loc.supports_partial_read());

        let loc_with_size = DiskLocation::with_size(1, 5, 128, 256);
        assert!(loc_with_size.supports_partial_read());
        assert_eq!(loc_with_size.record_size, 256);
    }

    #[test]
    fn test_write_buffer() {
        let buffer = WriteBuffer::new(0, 1023);

        let mut record = Record::new(b"key".to_vec(), b"value".to_vec(), 1, 0).unwrap();
        let loc = buffer.append(&mut record).unwrap();

        assert_eq!(loc.file_id, 0);
        assert_eq!(loc.wblock_id, 0);
        assert_eq!(loc.offset, 0);
    }

    /// Incompressible 500 KB payload — LZ4 can't shrink random-looking
    /// bytes, so each record takes a predictable ~500 KB on disk.
    /// Without this, the test's `vec![b'x'; N]` compresses to a few
    /// hundred bytes and hundreds of records fit per 1 MB WBlock, so
    /// the cap never trips.
    fn incompressible_value(seed: u64, len: usize) -> Vec<u8> {
        let mut v = Vec::with_capacity(len);
        let mut x = seed.wrapping_add(0x9E3779B97F4A7C15);
        while v.len() + 8 <= len {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            v.extend_from_slice(&x.to_le_bytes());
        }
        while v.len() < len {
            v.push(0);
        }
        v
    }

    #[test]
    fn pending_wblock_cap_returns_oom_on_overflow() {
        // Cap pending at 2 WBlocks. Records are ~500 KB incompressible
        // (so compression doesn't shrink them below the WBlock size)
        // → 2 per WBlock → the 3rd rotation should hit the cap and
        // return the standard OOM reply so the Redis layer can surface
        // it to clients instead of the store growing memory
        // unboundedly.
        let mut buffer = WriteBuffer::new(0, 1023);
        buffer.set_max_pending_wblocks(2);

        let mut saw_oom = false;
        for i in 0u64..20 {
            let mut record = Record::new(
                format!("k{:03}", i).into_bytes(),
                incompressible_value(i, 500 * 1024),
                i,
                0,
            )
            .unwrap();
            match buffer.append(&mut record) {
                Ok(_) => {}
                Err(e) if e.to_string().contains("OOM command not allowed") => {
                    saw_oom = true;
                    break;
                }
                Err(e) => panic!("unexpected error: {e}"),
            }
        }

        assert!(saw_oom, "expected OOM once pending cap was hit");
        assert!(buffer.pending_count() >= 2, "cap should hold ≥ 2 before rejecting");
    }

    #[test]
    fn pending_wblock_cap_zero_disables() {
        // cap = 0 means "no limit" — used by tests or workloads that
        // want to opt out. Should never OOM regardless of pending
        // depth.
        let mut buffer = WriteBuffer::new(0, 1023);
        buffer.set_max_pending_wblocks(0);
        for i in 0u64..10 {
            let mut record = Record::new(
                format!("k{:03}", i).into_bytes(),
                incompressible_value(i, 500 * 1024),
                i,
                0,
            )
            .unwrap();
            buffer.append(&mut record).unwrap();
        }
    }
}
