//! SMALL page header and the tiling parser (DESIGN §10.8, §17.3).
//!
//! Header (32 B, identical in every image while the page is SMALL for this partition):
//! `0x00 u32 "KVP1"`, `0x04 u8 kind = 1`, `0x05 u8 partition`, `0x06 u16 format = 1`,
//! `0x08 u64 page_id`, zeros, `0x1C u32 crc32c([0x00, 0x1C))`. Records are 16-byte aligned tiles
//! in `[0x20, 0x1000)`.
//!
//! Tiling parse: at `off = 0x20`, if `rec[0] == 0xA5`, kind and flags are valid, the length fits
//! before 0x1000 and the keyed crc matches, the record is accepted and `off += round16(len)`;
//! otherwise `off += 16`. Bytes inside an accepted record are never examined as headers, so a
//! torn mix of two images resolves every record to one image or the other, and a record image
//! embedded in a value never validates (the crc is keyed and location bound).

use kv_common::RecordCrc;

use crate::layout::{PAGE, PageId};
use crate::record::RecHeader;

/// `"KVP1"`
pub const PAGE_MAGIC: u32 = u32::from_le_bytes(*b"KVP1");
pub const PAGE_KIND_SMALL: u8 = 1;
pub const PAGE_FORMAT: u16 = 1;

/// One record accepted by the tiling parser. Holds user bytes: no `Debug` (LC-32).
pub struct SmallView<'a> {
    pub off: u16,
    pub len: u16,
    pub hdr: RecHeader,
    pub key: &'a [u8],
    pub val: &'a [u8],
}

/// Outcome of [`parse_small_page`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PageParse {
    Ok,
    /// Valid header of another partition (or not a SMALL page): holds only dead data for us.
    Foreign,
    /// No valid header: garbage or never written.
    BadHeader,
}

pub fn write_small_header(buf: &mut [u8; PAGE], page: PageId, partition: u16) {
    todo!("DESIGN §10.8: page header")
}

pub fn parse_small_page<'a>(
    buf: &'a [u8; PAGE],
    page: PageId,
    partition: u16,
    crc: RecordCrc,
    out: &mut Vec<SmallView<'a>>,
) -> PageParse {
    todo!("DESIGN §10.8: tiling parse")
}
