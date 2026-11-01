//! Extent codec (DESIGN §10.9, §14.4, §14.5).
//!
//! ```text
//! 0x00 u32 "KVX1"   0x04 u8 kind (1 VALUE, 2 CHUNKMAP, 3 CHUNK)   0x05 u8 vtype (1 string, 2 hash; CHUNK 0)
//! 0x06 u8 db        0x07 u8 flags (b0 HAS_EXPIRE, b1 TXN)
//! 0x08 u64 page_id (first page)   0x10 u64 seq   0x18 u64 expire_at_unix_ms
//! 0x20 u32 n_pages  0x24 u32 key_len   0x28 u64 val_len   0x30 u64 txn_id   0x38 u32 h32   0x3C u32 zero
//! 0x40 u64 twin_page_id   (allocation hint only; 0 = none; never dereferenced for data)
//! 0x48 u32 zero     0x4C u32 crc = crc32c_seeded(salt; page_id || hdr[0x00..0x4C] || body)
//! 0x50 body: VALUE = key || value; CHUNKMAP = key || u32 n_chunks || u32 zero || n_chunks x
//!      ChunkRef{u64 page_id, u32 n_pages, u32 len, u32 chunk_crc, u32 zero}; CHUNK = payload
//! zero padding to n_pages * 4096
//! ```
//! An extent update writes the new version into another run (the key's twin run when free, else
//! best fit), or in place where the device's atomic write unit covers it; at most two extents of
//! a key exist on media (I12). The twin run named by `twin_page_id` is a preference for the next
//! allocation, never a readable older version (LC-13).

use kv_common::RecordCrc;

use crate::layout::{PageId, Seq};

/// `"KVX1"`
pub const EXT_MAGIC: u32 = u32::from_le_bytes(*b"KVX1");
pub const EXT_KIND_VALUE: u8 = 1;
pub const EXT_KIND_CHUNKMAP: u8 = 2;
pub const EXT_KIND_CHUNK: u8 = 3;

/// Decoded extent header.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ExtHeader {
    pub kind: u8,
    pub vtype: u8,
    pub db: u8,
    pub flags: u8,
    pub page: PageId,
    pub seq: Seq,
    pub expire_at_ms: u64,
    pub n_pages: u32,
    pub key_len: u32,
    pub val_len: u64,
    pub txn: u64,
    pub h32: u32,
    /// allocation hint only, §10.9
    pub twin: Option<PageId>,
}

/// One chunk of a CHUNKMAP.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ChunkRef {
    pub page: PageId,
    pub n_pages: u32,
    pub len: u32,
    pub crc: u32,
}

/// Encodes header + key + body parts into `out` (`n_pages * 4096` bytes, zero padded).
pub fn encode_extent(h: &ExtHeader, key: &[u8], body: &[&[u8]], crc: RecordCrc, out: &mut [u8]) {
    todo!("DESIGN §10.9: extent encode")
}

/// Validates magic, page id, kind and `n_pages` of the header in `first_page`.
pub fn parse_extent_header(first_page: &[u8], page: PageId) -> Option<ExtHeader> {
    todo!("DESIGN §10.9: extent header parse")
}

/// Verifies the whole-extent crc.
pub fn verify_extent(buf: &[u8], h: &ExtHeader, crc: RecordCrc) -> bool {
    todo!("DESIGN §10.9: extent crc")
}

/// Decodes a CHUNKMAP body (after the key).
pub fn decode_chunkmap(body: &[u8]) -> Option<Vec<ChunkRef>> {
    todo!("DESIGN §10.9: chunkmap decode")
}
