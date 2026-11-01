//! Small record codec (DESIGN §10.8).
//!
//! ```text
//! 0x00 u8  magic 0xA5
//! 0x01 u8  kind: 1 STRING, 2 HASH, 4 TXN_DELETE_MARKER
//! 0x02 u8  db
//! 0x03 u8  flags: b0 HAS_EXPIRE, b1 TXN (b2..b7 zero)
//! 0x04 u32 crc = crc32c_seeded(salt; page_id u64 || off u16 || rec[0x00..0x04] || rec[0x08..len_unpadded])
//! 0x08 u64 seq
//! 0x10 u64 expire_at_unix_ms (0 unless HAS_EXPIRE)
//! 0x18 u16 key_len   0x1A u16 val_len   0x1C u32 h32 (KeyHash bits 31..0)
//! 0x20 [u64 txn_id]  only if TXN
//! then key bytes, value bytes, zero padding to a 16-byte boundary (total <= 2048 B)
//! ```
//! A TXN_DELETE_MARKER has TXN set, HAS_EXPIRE clear and `val_len = 0`; it has no expiry field
//! and lives only until its txn's phase 2 completes (LC-11).

use kv_common::RecordCrc;

use crate::layout::{PageId, Seq};

pub const REC_MAGIC: u8 = 0xA5;
pub const REC_KIND_STRING: u8 = 1;
pub const REC_KIND_HASH: u8 = 2;
pub const REC_KIND_TXN_DELETE_MARKER: u8 = 4;
pub const REC_FLAG_HAS_EXPIRE: u8 = 1;
pub const REC_FLAG_TXN: u8 = 2;
/// Header bytes without / with the txn id.
pub const REC_HDR: usize = 32;
pub const REC_HDR_TXN: usize = 40;

/// Decoded small record header.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RecHeader {
    pub kind: u8,
    pub db: u8,
    pub flags: u8,
    pub seq: Seq,
    pub expire_at_ms: u64,
    pub key_len: u16,
    pub val_len: u16,
    pub h32: u32,
    pub txn: Option<u64>,
}

/// header + key + val rounded to 16
pub fn small_len(key: usize, val: usize, txn: bool) -> usize {
    let hdr = if txn { REC_HDR_TXN } else { REC_HDR };
    (hdr + key + val).next_multiple_of(16)
}

/// Encodes a record for location `(page, off)` into `out` (`small_len` bytes, zero padded).
pub fn encode_small(h: &RecHeader, key: &[u8], val: &[u8], page: PageId, off: u16, crc: RecordCrc, out: &mut [u8]) {
    todo!("DESIGN §10.8: small record encode")
}
