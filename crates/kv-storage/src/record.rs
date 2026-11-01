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
//!
//! The checksum is keyed with the node's secret salt and bound to the record's location
//! `(page_id, off)` through [`RecordCrc`], so a record image is valid only at the place it was
//! written for: a copy at another offset or page, or an image embedded by a client inside a
//! value, never validates. Moving a record means re-sealing it for its new location
//! ([`seal_small`]).

use kv_common::RecordCrc;

use crate::layout::{PAGE, PageId, SMALL_REC_MAX, Seq, le};
use crate::page::SmallView;

pub const REC_MAGIC: u8 = 0xA5;
pub const REC_KIND_STRING: u8 = 1;
pub const REC_KIND_HASH: u8 = 2;
pub const REC_KIND_TXN_DELETE_MARKER: u8 = 4;
pub const REC_FLAG_HAS_EXPIRE: u8 = 1;
pub const REC_FLAG_TXN: u8 = 2;
/// Every defined flag bit.
pub const REC_FLAGS_KNOWN: u8 = REC_FLAG_HAS_EXPIRE | REC_FLAG_TXN;
/// Header bytes without / with the txn id.
pub const REC_HDR: usize = 32;
pub const REC_HDR_TXN: usize = 40;
/// Records are 16-byte aligned tiles.
pub const REC_ALIGN: usize = 16;

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

impl RecHeader {
    /// Header bytes of this record (32, or 40 with a txn id).
    pub fn hdr_len(&self) -> usize {
        if self.txn.is_some() { REC_HDR_TXN } else { REC_HDR }
    }

    /// Header + key + value, before padding.
    pub fn len_unpadded(&self) -> usize {
        self.hdr_len() + usize::from(self.key_len) + usize::from(self.val_len)
    }

    /// Encoded (padded) record length.
    pub fn len(&self) -> usize {
        self.len_unpadded().next_multiple_of(REC_ALIGN)
    }

    /// `expire_at_ms` when HAS_EXPIRE is set.
    pub fn expire_at(&self) -> Option<u64> {
        (self.flags & REC_FLAG_HAS_EXPIRE != 0).then_some(self.expire_at_ms)
    }

    pub fn is_marker(&self) -> bool {
        self.kind == REC_KIND_TXN_DELETE_MARKER
    }
}

/// header + key + val rounded to 16
pub fn small_len(key: usize, val: usize, txn: bool) -> usize {
    let hdr = if txn { REC_HDR_TXN } else { REC_HDR };
    (hdr + key + val).next_multiple_of(REC_ALIGN)
}

/// True when a record with these lengths fits a SMALL page tile (`<= 2048 B`).
pub fn fits_small(key: usize, val: usize, txn: bool) -> bool {
    small_len(key, val, txn) <= SMALL_REC_MAX
}

/// Encodes a record for location `(page, off)` into `out[..small_len]` (zero padded; bytes of
/// `out` beyond the record are not touched).
///
/// The flags are taken from `h.flags` (HAS_EXPIRE) and `h.txn` (TXN); the expiry field is
/// written only with HAS_EXPIRE. Lengths are those of `key` and `val`. Panics if `out` is
/// shorter than the record or the record exceeds 2048 bytes (callers size records with
/// [`small_len`]).
pub fn encode_small(h: &RecHeader, key: &[u8], val: &[u8], page: PageId, off: u16, crc: RecordCrc, out: &mut [u8]) {
    debug_assert_eq!(h.flags & !REC_FLAGS_KNOWN, 0);
    debug_assert_eq!(h.flags & REC_FLAG_TXN != 0, h.txn.is_some());
    debug_assert!(usize::from(h.key_len) == key.len() && usize::from(h.val_len) == val.len());
    let txn = h.txn.is_some();
    let hdr = if txn { REC_HDR_TXN } else { REC_HDR };
    let unpadded = hdr + key.len() + val.len();
    let len = unpadded.next_multiple_of(REC_ALIGN);
    assert!(len <= SMALL_REC_MAX && len <= out.len(), "small record does not fit its buffer");
    let flags = (h.flags & REC_FLAG_HAS_EXPIRE) | if txn { REC_FLAG_TXN } else { 0 };
    let rec = &mut out[..len];
    rec[0] = REC_MAGIC;
    rec[1] = h.kind;
    rec[2] = h.db;
    rec[3] = flags;
    le::put_u64(rec, 0x08, h.seq.0);
    le::put_u64(rec, 0x10, if flags & REC_FLAG_HAS_EXPIRE != 0 { h.expire_at_ms } else { 0 });
    le::put_u16(rec, 0x18, key.len() as u16);
    le::put_u16(rec, 0x1A, val.len() as u16);
    le::put_u32(rec, 0x1C, h.h32);
    if let Some(t) = h.txn {
        le::put_u64(rec, 0x20, t);
    }
    rec[hdr..hdr + key.len()].copy_from_slice(key);
    rec[hdr + key.len()..unpadded].copy_from_slice(val);
    rec[unpadded..].fill(0);
    let c = record_crc(rec, unpadded, page, off, crc);
    le::put_u32(rec, 0x04, c);
}

/// Recomputes the checksum of an encoded record for a new location `(page, off)` (a record
/// copied to another offset or page must be re-sealed there). `rec` starts at the record and
/// holds at least its unpadded length. Returns `false` (leaving `rec` unchanged) when `rec`
/// does not start with a structurally valid header.
pub fn seal_small(rec: &mut [u8], page: PageId, off: u16, crc: RecordCrc) -> bool {
    let Some((_, unpadded)) = decode_small_header(rec) else {
        return false;
    };
    let c = record_crc(rec, unpadded, page, off, crc);
    le::put_u32(rec, 0x04, c);
    true
}

/// The keyed, location-bound checksum of the record in `rec[..unpadded]`.
#[inline]
pub fn record_crc(rec: &[u8], unpadded: usize, page: PageId, off: u16, crc: RecordCrc) -> u32 {
    crc.compute(page.0, off, &[&rec[0x00..0x04], &rec[0x08..unpadded]])
}

/// Structural header checks (no checksum): magic, kind, flags, the marker rules, a zero expiry
/// field without HAS_EXPIRE, a non-zero txn id with TXN, and a padded length of at most 2048
/// bytes that fits in `rec`. Returns the header and the unpadded length.
#[inline]
pub fn decode_small_header(rec: &[u8]) -> Option<(RecHeader, usize)> {
    if rec.len() < REC_HDR || rec[0] != REC_MAGIC {
        return None;
    }
    let (kind, db, flags) = (rec[1], rec[2], rec[3]);
    if !matches!(kind, REC_KIND_STRING | REC_KIND_HASH | REC_KIND_TXN_DELETE_MARKER) || flags & !REC_FLAGS_KNOWN != 0 {
        return None;
    }
    let txn = flags & REC_FLAG_TXN != 0;
    let has_expire = flags & REC_FLAG_HAS_EXPIRE != 0;
    let expire_at_ms = le::u64_at(rec, 0x10);
    let key_len = le::u16_at(rec, 0x18);
    let val_len = le::u16_at(rec, 0x1A);
    if (!has_expire && expire_at_ms != 0) || (kind == REC_KIND_TXN_DELETE_MARKER && (!txn || has_expire || val_len != 0)) {
        return None;
    }
    let hdr = if txn { REC_HDR_TXN } else { REC_HDR };
    let unpadded = hdr + usize::from(key_len) + usize::from(val_len);
    if unpadded.next_multiple_of(REC_ALIGN) > SMALL_REC_MAX || unpadded.next_multiple_of(REC_ALIGN) > rec.len() {
        return None;
    }
    let txn_id = if txn {
        let t = le::u64_at(rec, 0x20);
        if t == 0 {
            return None;
        }
        Some(t)
    } else {
        None
    };
    let h = RecHeader {
        kind,
        db,
        flags,
        seq: Seq(le::u64_at(rec, 0x08)),
        expire_at_ms,
        key_len,
        val_len,
        h32: le::u32_at(rec, 0x1C),
        txn: txn_id,
    };
    Some((h, unpadded))
}

/// Validates the record at byte `off` of the SMALL page image `buf` (header rules, location in
/// `[0x20, 0x1000)`, keyed location-bound checksum for `(page, off)`) and returns its view.
#[inline]
pub fn read_small<'a>(buf: &'a [u8], page: PageId, off: u16, crc: RecordCrc) -> Option<SmallView<'a>> {
    let o = usize::from(off);
    if o < crate::layout::SMALL_HDR || o >= PAGE.min(buf.len()) || o % REC_ALIGN != 0 {
        return None;
    }
    let rec = &buf[o..PAGE.min(buf.len())];
    let (hdr, unpadded) = decode_small_header(rec)?;
    if le::u32_at(rec, 0x04) != record_crc(rec, unpadded, page, off, crc) {
        return None;
    }
    let h = hdr.hdr_len();
    let k = usize::from(hdr.key_len);
    Some(SmallView {
        off,
        len: unpadded.next_multiple_of(REC_ALIGN) as u16,
        hdr,
        key: &rec[h..h + k],
        val: &rec[h + k..unpadded],
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) const SALT: u32 = 0x5EED_5A17;

    pub(crate) fn hdr(kind: u8, key: &[u8], val: &[u8], txn: Option<u64>, expire: Option<u64>) -> RecHeader {
        RecHeader {
            kind,
            db: 3,
            flags: u8::from(expire.is_some()) | if txn.is_some() { REC_FLAG_TXN } else { 0 },
            seq: Seq::new(2, 1000),
            expire_at_ms: expire.unwrap_or(0),
            key_len: key.len() as u16,
            val_len: val.len() as u16,
            h32: 0xCAFE_F00D,
            txn,
        }
    }

    #[test]
    fn lengths() {
        assert_eq!(small_len(0, 0, false), 32);
        assert_eq!(small_len(1, 0, false), 48);
        assert_eq!(small_len(20, 300, false), 352);
        assert_eq!(small_len(20, 300, true), 368);
        assert!(fits_small(16, 2000, false));
        assert!(!fits_small(17, 2000, false));
        assert!(!fits_small(16, 2000, true));
    }

    #[test]
    fn encode_layout_and_read_back() {
        let crc = RecordCrc::new(SALT);
        let page = PageId(0x0000_0002_0000_1234);
        let (key, val) = (b"user:42".as_slice(), b"hello world".as_slice());
        let h = hdr(REC_KIND_STRING, key, val, None, Some(1_800_000_000_000));
        let mut buf = [0xFFu8; PAGE];
        let off = 0x60u16;
        encode_small(&h, key, val, page, off, crc, &mut buf[0x60..]);
        let rec = &buf[0x60..0x60 + 64];
        assert_eq!(&rec[..4], &[0xA5, 1, 3, 1]);
        assert_eq!(le::u64_at(rec, 8), Seq::new(2, 1000).0);
        assert_eq!(le::u64_at(rec, 0x10), 1_800_000_000_000);
        assert_eq!(le::u16_at(rec, 0x18), 7);
        assert_eq!(le::u16_at(rec, 0x1A), 11);
        assert_eq!(le::u32_at(rec, 0x1C), 0xCAFE_F00D);
        assert_eq!(&rec[0x20..0x27], b"user:42");
        assert_eq!(&rec[0x27..0x32], b"hello world");
        assert!(rec[0x32..64].iter().all(|&b| b == 0));
        assert_eq!(buf[0x60 + 64], 0xFF, "bytes past the record untouched");
        let want = crc.compute(page.0, off, &[&rec[0..4], &rec[8..0x32]]);
        assert_eq!(le::u32_at(rec, 4), want);

        let v = read_small(&buf, page, off, crc).unwrap();
        assert_eq!((v.off, v.len, v.key, v.val), (off, 64, key, val));
        assert_eq!(v.hdr, h);
        assert_eq!(v.hdr.expire_at(), Some(1_800_000_000_000));
        // wrong location or salt
        assert!(read_small(&buf, PageId(page.0 + 1), off, crc).is_none());
        assert!(read_small(&buf, page, off, RecordCrc::new(SALT ^ 1)).is_none());
        // the same image moved 16 bytes later is not valid there until re-sealed
        let mut moved = [0u8; PAGE];
        moved[0x70..0x70 + 64].copy_from_slice(rec);
        assert!(read_small(&moved, page, 0x70, crc).is_none());
        assert!(seal_small(&mut moved[0x70..], page, 0x70, crc));
        assert!(read_small(&moved, page, 0x70, crc).is_some());
        assert!(!seal_small(&mut [0u8; 64], page, 0x70, crc));
    }

    #[test]
    fn txn_records_and_markers() {
        let crc = RecordCrc::new(SALT);
        let page = PageId(77);
        let mut buf = [0u8; PAGE];
        let h = hdr(REC_KIND_HASH, b"k", b"v", Some(1 << 33), None);
        encode_small(&h, b"k", b"v", page, 0x20, crc, &mut buf[0x20..]);
        assert_eq!(buf[0x23], REC_FLAG_TXN);
        assert_eq!(le::u64_at(&buf, 0x40), 1 << 33);
        let v = read_small(&buf, page, 0x20, crc).unwrap();
        assert_eq!((v.len, v.key, v.val, v.hdr.txn), (48, b"k".as_slice(), b"v".as_slice(), Some(1 << 33)));
        let m = hdr(REC_KIND_TXN_DELETE_MARKER, b"gone", b"", Some(9), None);
        encode_small(&m, b"gone", b"", page, 0x60, crc, &mut buf[0x60..]);
        let v = read_small(&buf, page, 0x60, crc).unwrap();
        assert!(v.hdr.is_marker() && v.val.is_empty());
    }

    #[test]
    fn header_rules() {
        let crc = RecordCrc::new(SALT);
        let page = PageId(5);
        let mut base = [0u8; 64];
        let h = hdr(REC_KIND_STRING, b"key", b"val", None, None);
        encode_small(&h, b"key", b"val", page, 0x20, crc, &mut base);
        assert!(decode_small_header(&base).is_some());
        let bad = |f: &dyn Fn(&mut [u8; 64])| {
            let mut b = base;
            f(&mut b);
            decode_small_header(&b).is_none()
        };
        assert!(bad(&|b| b[0] = 0xA4));
        assert!(bad(&|b| b[1] = 3));
        assert!(bad(&|b| b[1] = 0));
        assert!(bad(&|b| b[3] = 4));
        assert!(bad(&|b| b[0x10] = 1)); // expiry without HAS_EXPIRE
        assert!(bad(&|b| b[1] = REC_KIND_TXN_DELETE_MARKER)); // marker without TXN
        assert!(bad(&|b| le::put_u16(b, 0x1A, 2017))); // longer than 2048 B
        assert!(bad(&|b| le::put_u16(b, 0x1A, 40))); // does not fit in the buffer
        assert!(bad(&|b| {
            b[3] = REC_FLAG_TXN;
            le::put_u64(b, 0x20, 0)
        })); // txn id 0
        assert!(decode_small_header(&base[..31]).is_none());
    }

    #[test]
    fn largest_record() {
        let crc = RecordCrc::new(SALT);
        let page = PageId(1);
        let key = vec![b'k'; 16];
        let val = vec![b'v'; 2000];
        let h = hdr(REC_KIND_STRING, &key, &val, None, None);
        let mut buf = [0u8; PAGE];
        let off = (PAGE - SMALL_REC_MAX) as u16;
        encode_small(&h, &key, &val, page, off, crc, &mut buf[usize::from(off)..]);
        let v = read_small(&buf, page, off, crc).unwrap();
        assert_eq!(usize::from(v.len), SMALL_REC_MAX);
        // one byte further would cross the page end
        assert!(read_small(&buf, page, off + 16, crc).is_none());
        assert!(read_small(&buf, page, 0x10, crc).is_none());
        assert!(read_small(&buf, page, off + 1, crc).is_none());
    }
}
