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
//!
//! The checksum is keyed and bound to the extent's location ([`RecordCrc::compute_extent`]:
//! salt-seeded over `page_id || hdr[0x00..0x4C] || body`; an extent always starts at offset 0
//! of its first page), so an extent image copied to another run never validates.
//! `ChunkRef.crc` is the referenced CHUNK extent's own header crc (offset 0x4C), so a chunkmap
//! names one exact version of each chunk.

use kv_common::RecordCrc;

use crate::layout::{EXT_HDR, EXT_MAX_PAGES, PAGE, PageId, Seq, class_exact, le};

/// `"KVX1"`
pub const EXT_MAGIC: u32 = u32::from_le_bytes(*b"KVX1");
pub const EXT_KIND_VALUE: u8 = 1;
pub const EXT_KIND_CHUNKMAP: u8 = 2;
pub const EXT_KIND_CHUNK: u8 = 3;
/// Extent `vtype` values (VALUE and CHUNKMAP; CHUNK uses 0).
pub const EXT_VTYPE_STRING: u8 = 1;
pub const EXT_VTYPE_HASH: u8 = 2;
pub const EXT_FLAG_HAS_EXPIRE: u8 = 1;
pub const EXT_FLAG_TXN: u8 = 2;
/// Bytes of one `ChunkRef` in a CHUNKMAP body, and of the `n_chunks` prefix.
pub const CHUNK_REF_BYTES: usize = 24;
pub const CHUNKMAP_PREFIX: usize = 8;
/// Offset of the crc field in the header.
pub const EXT_CRC_OFF: usize = 0x4C;

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

impl ExtHeader {
    /// `expire_at_ms` when HAS_EXPIRE is set.
    pub fn expire_at(&self) -> Option<u64> {
        (self.flags & EXT_FLAG_HAS_EXPIRE != 0).then_some(self.expire_at_ms)
    }

    /// The txn id when TXN is set.
    pub fn txn_id(&self) -> Option<u64> {
        (self.flags & EXT_FLAG_TXN != 0).then_some(self.txn)
    }

    /// Bytes of the extent's run (`n_pages * 4096`).
    pub fn run_bytes(&self) -> usize {
        self.n_pages as usize * PAGE
    }

    /// Body length when it follows from the header alone (VALUE, CHUNK); CHUNKMAP bodies
    /// depend on `n_chunks` ([`body_len`]).
    pub fn fixed_body_len(&self) -> Option<u64> {
        match self.kind {
            EXT_KIND_VALUE => Some(u64::from(self.key_len) + self.val_len),
            EXT_KIND_CHUNK => Some(self.val_len),
            _ => None,
        }
    }
}

/// One chunk of a CHUNKMAP.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ChunkRef {
    pub page: PageId,
    pub n_pages: u32,
    pub len: u32,
    pub crc: u32,
}

/// Pages of a VALUE extent holding `key` + `val` bytes (header included).
pub fn value_pages(key: usize, val: u64) -> u64 {
    (EXT_HDR as u64 + key as u64 + val).div_ceil(PAGE as u64).max(1)
}

/// True when `key` + `val` fit one VALUE extent (`80 + key + value <= 8 MiB`).
pub fn fits_value_extent(key: usize, val: u64) -> bool {
    value_pages(key, val) <= u64::from(EXT_MAX_PAGES)
}

/// Largest CHUNK payload (one 2048-page extent minus its header).
pub const CHUNK_MAX_PAYLOAD: usize = EXT_MAX_PAGES as usize * PAGE - EXT_HDR;

/// Body length of a CHUNKMAP with `n` chunk references and a `key`-byte key.
pub fn chunkmap_body_len(key: usize, n: usize) -> usize {
    key + CHUNKMAP_PREFIX + n * CHUNK_REF_BYTES
}

/// Writes `u32 n_chunks || u32 zero || refs` (the CHUNKMAP body after the key) into
/// `out[..8 + 24n]` and returns that length. Panics if `out` is too short.
pub fn encode_chunk_refs(refs: &[ChunkRef], out: &mut [u8]) -> usize {
    let len = CHUNKMAP_PREFIX + refs.len() * CHUNK_REF_BYTES;
    assert!(out.len() >= len, "chunk reference buffer too short");
    le::put_u32(out, 0, refs.len() as u32);
    le::put_u32(out, 4, 0);
    for (i, r) in refs.iter().enumerate() {
        let o = CHUNKMAP_PREFIX + i * CHUNK_REF_BYTES;
        le::put_u64(out, o, r.page.0);
        le::put_u32(out, o + 8, r.n_pages);
        le::put_u32(out, o + 12, r.len);
        le::put_u32(out, o + 16, r.crc);
        le::put_u32(out, o + 20, 0);
    }
    len
}

/// Encodes header + key + body parts into `out` (`n_pages * 4096` bytes, zero padded).
///
/// `h.key_len` must equal `key.len()`; for VALUE and CHUNK extents `h.val_len` must equal the
/// value/payload bytes in `body` (CHUNKMAP: the logical value length, with `body` holding
/// [`encode_chunk_refs`] output). The expiry and txn fields are written only with their flag.
/// Panics if `out` cannot hold the header, key and body.
pub fn encode_extent(h: &ExtHeader, key: &[u8], body: &[&[u8]], crc: RecordCrc, out: &mut [u8]) {
    let body_bytes: usize = body.iter().map(|p| p.len()).sum();
    debug_assert_eq!(h.key_len as usize, key.len());
    debug_assert!(h.kind == EXT_KIND_CHUNKMAP || h.val_len == body_bytes as u64);
    debug_assert!(h.kind != EXT_KIND_CHUNK || key.is_empty());
    let end = EXT_HDR + key.len() + body_bytes;
    assert!(end <= out.len(), "extent buffer too short");
    let flags = h.flags & (EXT_FLAG_HAS_EXPIRE | EXT_FLAG_TXN);
    let hdr = &mut out[..EXT_HDR];
    hdr.fill(0);
    le::put_u32(hdr, 0x00, EXT_MAGIC);
    hdr[0x04] = h.kind;
    hdr[0x05] = h.vtype;
    hdr[0x06] = h.db;
    hdr[0x07] = flags;
    le::put_u64(hdr, 0x08, h.page.0);
    le::put_u64(hdr, 0x10, h.seq.0);
    le::put_u64(hdr, 0x18, if flags & EXT_FLAG_HAS_EXPIRE != 0 { h.expire_at_ms } else { 0 });
    le::put_u32(hdr, 0x20, h.n_pages);
    le::put_u32(hdr, 0x24, key.len() as u32);
    le::put_u64(hdr, 0x28, h.val_len);
    le::put_u64(hdr, 0x30, if flags & EXT_FLAG_TXN != 0 { h.txn } else { 0 });
    le::put_u32(hdr, 0x38, h.h32);
    le::put_u64(hdr, 0x40, h.twin.map_or(0, |p| p.0));
    let mut at = EXT_HDR;
    out[at..at + key.len()].copy_from_slice(key);
    at += key.len();
    for p in body {
        out[at..at + p.len()].copy_from_slice(p);
        at += p.len();
    }
    out[at..].fill(0);
    let c = crc.compute_extent(h.page.0, &[&out[..EXT_CRC_OFF], &out[EXT_HDR..end]]);
    le::put_u32(out, EXT_CRC_OFF, c);
}

/// Validates magic, page id, kind and `n_pages` of the header in `first_page`.
///
/// Also checks the structural rules a header written by [`encode_extent`] satisfies: `n_pages`
/// is a size class (§10.9), vtype matches the kind, unknown flags and reserved fields are zero,
/// the expiry and txn fields are zero without their flag, a CHUNK has no key, and the header and
/// body fit in the run. The checksum is verified by [`verify_extent`].
pub fn parse_extent_header(first_page: &[u8], page: PageId) -> Option<ExtHeader> {
    let b = first_page.get(..EXT_HDR)?;
    if le::u32_at(b, 0x00) != EXT_MAGIC || le::u64_at(b, 0x08) != page.0 {
        return None;
    }
    let (kind, vtype, db, flags) = (b[0x04], b[0x05], b[0x06], b[0x07]);
    let vtype_ok = match kind {
        EXT_KIND_VALUE | EXT_KIND_CHUNKMAP => matches!(vtype, EXT_VTYPE_STRING | EXT_VTYPE_HASH),
        EXT_KIND_CHUNK => vtype == 0,
        _ => false,
    };
    if !vtype_ok || flags & !(EXT_FLAG_HAS_EXPIRE | EXT_FLAG_TXN) != 0 {
        return None;
    }
    let n_pages = le::u32_at(b, 0x20);
    if n_pages == 0 || n_pages > EXT_MAX_PAGES || class_exact(n_pages).is_none() {
        return None;
    }
    let expire_at_ms = le::u64_at(b, 0x18);
    let txn = le::u64_at(b, 0x30);
    if (flags & EXT_FLAG_HAS_EXPIRE == 0 && expire_at_ms != 0)
        || (flags & EXT_FLAG_TXN == 0 && txn != 0)
        || (flags & EXT_FLAG_TXN != 0 && txn == 0)
        || le::u32_at(b, 0x3C) != 0
        || le::u32_at(b, 0x48) != 0
    {
        return None;
    }
    let key_len = le::u32_at(b, 0x24);
    let val_len = le::u64_at(b, 0x28);
    let room = u64::from(n_pages) * PAGE as u64 - EXT_HDR as u64;
    let fits = match kind {
        EXT_KIND_VALUE => u64::from(key_len) + val_len <= room,
        EXT_KIND_CHUNKMAP => u64::from(key_len) + CHUNKMAP_PREFIX as u64 <= room,
        _ => key_len == 0 && val_len <= room,
    };
    if !fits {
        return None;
    }
    let twin = le::u64_at(b, 0x40);
    Some(ExtHeader {
        kind,
        vtype,
        db,
        flags,
        page,
        seq: Seq(le::u64_at(b, 0x10)),
        expire_at_ms,
        n_pages,
        key_len,
        val_len,
        txn,
        h32: le::u32_at(b, 0x38),
        twin: (twin != 0).then_some(PageId(twin)),
    })
}

/// Body length of the extent in `buf` described by `h` (CHUNKMAP: from its `n_chunks`), `None`
/// when it does not fit in `buf` or the run.
pub fn body_len(buf: &[u8], h: &ExtHeader) -> Option<usize> {
    let room = h.run_bytes() - EXT_HDR;
    let len = match h.kind {
        EXT_KIND_CHUNKMAP => {
            let at = EXT_HDR + h.key_len as usize;
            let n = le::u32_at(buf.get(at..at + 4)?, 0) as usize;
            chunkmap_body_len(h.key_len as usize, n)
        }
        _ => usize::try_from(h.fixed_body_len()?).ok()?,
    };
    (len <= room && EXT_HDR + len <= buf.len()).then_some(len)
}

/// The checksum the extent in `buf` should carry (`None` if its body does not fit `buf`).
pub fn extent_crc(buf: &[u8], h: &ExtHeader, crc: RecordCrc) -> Option<u32> {
    let len = body_len(buf, h)?;
    Some(crc.compute_extent(h.page.0, &[&buf[..EXT_CRC_OFF], &buf[EXT_HDR..EXT_HDR + len]]))
}

/// The crc field stored in an extent header (what a `ChunkRef` names).
pub fn stored_crc(first_page: &[u8]) -> Option<u32> {
    first_page.get(EXT_CRC_OFF..EXT_HDR).map(|b| le::u32_at(b, 0))
}

/// Verifies the whole-extent crc. `buf` holds the extent from its first byte (at least the
/// header, key and body; normally the whole run).
pub fn verify_extent(buf: &[u8], h: &ExtHeader, crc: RecordCrc) -> bool {
    if buf.len() < EXT_HDR || parse_extent_header(buf, h.page).as_ref() != Some(h) {
        return false;
    }
    extent_crc(buf, h, crc).is_some_and(|c| Some(c) == stored_crc(buf))
}

/// Key and value bytes of a verified VALUE extent.
pub fn value_parts<'a>(buf: &'a [u8], h: &ExtHeader) -> Option<(&'a [u8], &'a [u8])> {
    if h.kind != EXT_KIND_VALUE {
        return None;
    }
    let k = h.key_len as usize;
    let end = EXT_HDR + k + usize::try_from(h.val_len).ok()?;
    let b = buf.get(..end)?;
    Some((&b[EXT_HDR..EXT_HDR + k], &b[EXT_HDR + k..]))
}

/// Key and chunk-reference body of a verified CHUNKMAP extent (pass the body to
/// [`decode_chunkmap`]).
pub fn chunkmap_parts<'a>(buf: &'a [u8], h: &ExtHeader) -> Option<(&'a [u8], &'a [u8])> {
    if h.kind != EXT_KIND_CHUNKMAP {
        return None;
    }
    let k = h.key_len as usize;
    let len = body_len(buf, h)?;
    Some((&buf[EXT_HDR..EXT_HDR + k], &buf[EXT_HDR + k..EXT_HDR + len]))
}

/// Payload of a verified CHUNK extent.
pub fn chunk_payload<'a>(buf: &'a [u8], h: &ExtHeader) -> Option<&'a [u8]> {
    if h.kind != EXT_KIND_CHUNK {
        return None;
    }
    buf.get(EXT_HDR..EXT_HDR + usize::try_from(h.val_len).ok()?)
}

/// Decodes a CHUNKMAP body (after the key).
///
/// Rejects a body whose length differs from `8 + 24 n`, non-zero reserved fields, and chunk
/// references that cannot describe a CHUNK extent (`n_pages` not a size class, `len` larger
/// than the run's payload room).
pub fn decode_chunkmap(body: &[u8]) -> Option<Vec<ChunkRef>> {
    if body.len() < CHUNKMAP_PREFIX || le::u32_at(body, 4) != 0 {
        return None;
    }
    let n = le::u32_at(body, 0) as usize;
    if body.len() != CHUNKMAP_PREFIX + n.checked_mul(CHUNK_REF_BYTES)? {
        return None;
    }
    let mut refs = Vec::with_capacity(n);
    for i in 0..n {
        let o = CHUNKMAP_PREFIX + i * CHUNK_REF_BYTES;
        let r = ChunkRef {
            page: PageId(le::u64_at(body, o)),
            n_pages: le::u32_at(body, o + 8),
            len: le::u32_at(body, o + 12),
            crc: le::u32_at(body, o + 16),
        };
        if le::u32_at(body, o + 20) != 0
            || class_exact(r.n_pages).is_none()
            || u64::from(r.len) > u64::from(r.n_pages) * PAGE as u64 - EXT_HDR as u64
        {
            return None;
        }
        refs.push(r);
    }
    Some(refs)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) const SALT: u32 = 0x00E1_7E47;

    pub(crate) fn value_hdr(page: PageId, n_pages: u32, key: &[u8], val: &[u8]) -> ExtHeader {
        ExtHeader {
            kind: EXT_KIND_VALUE,
            vtype: EXT_VTYPE_STRING,
            db: 2,
            flags: EXT_FLAG_HAS_EXPIRE,
            page,
            seq: Seq::new(1, 555),
            expire_at_ms: 1_900_000_000_000,
            n_pages,
            key_len: key.len() as u32,
            val_len: val.len() as u64,
            txn: 0,
            h32: 0x1357_9BDF,
            twin: Some(PageId(page.0 + 4096)),
        }
    }

    #[test]
    fn value_extent_round_trip() {
        let crc = RecordCrc::new(SALT);
        let page = PageId((3u64 << 32) | 81_920);
        let key = b"big-key";
        let val: Vec<u8> = (0..10_000u32).map(|i| (i * 7) as u8).collect();
        let n = value_pages(key.len(), val.len() as u64) as u32;
        assert_eq!(n, 3);
        let h = value_hdr(page, n, key, &val);
        let mut buf = vec![0xEEu8; n as usize * PAGE];
        encode_extent(&h, key, &[&val[..4000], &val[4000..]], crc, &mut buf);
        assert_eq!(&buf[..4], b"KVX1");
        assert_eq!(&buf[4..8], &[1, 1, 2, 1]);
        assert_eq!(le::u64_at(&buf, 8), page.0);
        assert_eq!(le::u32_at(&buf, 0x20), 3);
        assert_eq!(le::u32_at(&buf, 0x24), 7);
        assert_eq!(le::u64_at(&buf, 0x28), 10_000);
        assert_eq!(le::u64_at(&buf, 0x40), page.0 + 4096);
        assert_eq!(&buf[0x50..0x57], key);
        assert!(buf[0x50 + 7 + 10_000..].iter().all(|&b| b == 0));
        let p = parse_extent_header(&buf, page).unwrap();
        assert_eq!(p, h);
        assert!(verify_extent(&buf, &p, crc));
        let (k, v) = value_parts(&buf, &p).unwrap();
        assert_eq!((k, v), (key.as_slice(), val.as_slice()));
        assert_eq!(stored_crc(&buf), extent_crc(&buf, &p, crc));
        // Wrong salt, wrong location, flipped body byte, short buffer.
        assert!(!verify_extent(&buf, &p, RecordCrc::new(SALT + 1)));
        assert!(parse_extent_header(&buf, PageId(page.0 + 1)).is_none());
        let mut d = buf.clone();
        d[0x50 + 5000] ^= 1;
        assert!(!verify_extent(&d, &p, crc));
        assert!(!verify_extent(&buf[..5000], &p, crc));
        // First-sector zeroing invalidates the extent (§14.4).
        let mut z = buf.clone();
        z[..512].fill(0);
        assert!(parse_extent_header(&z, page).is_none());
    }

    #[test]
    fn chunkmap_and_chunks() {
        let crc = RecordCrc::new(SALT);
        let cpage = PageId(1 << 20);
        let payload = vec![0x42u8; 5000];
        let ch = ExtHeader {
            kind: EXT_KIND_CHUNK,
            vtype: 0,
            db: 0,
            flags: EXT_FLAG_TXN,
            page: cpage,
            seq: Seq::new(0, 9),
            expire_at_ms: 0,
            n_pages: 2,
            key_len: 0,
            val_len: 5000,
            txn: 77,
            h32: 0,
            twin: None,
        };
        let mut cbuf = vec![0u8; 2 * PAGE];
        encode_extent(&ch, &[], &[&payload], crc, &mut cbuf);
        let cp = parse_extent_header(&cbuf, cpage).unwrap();
        assert!(verify_extent(&cbuf, &cp, crc));
        assert_eq!(cp.txn_id(), Some(77));
        assert_eq!(chunk_payload(&cbuf, &cp), Some(payload.as_slice()));

        let refs = [
            ChunkRef { page: cpage, n_pages: 2, len: 5000, crc: stored_crc(&cbuf).unwrap() },
            ChunkRef { page: PageId(cpage.0 + 2), n_pages: 2048, len: CHUNK_MAX_PAYLOAD as u32, crc: 0xABCD },
        ];
        let mut body = vec![0u8; 8 + 2 * 24];
        assert_eq!(encode_chunk_refs(&refs, &mut body), body.len());
        assert_eq!(decode_chunkmap(&body), Some(refs.to_vec()));
        let key = b"huge";
        let mpage = PageId(cpage.0 + 5000);
        let mh = ExtHeader {
            kind: EXT_KIND_CHUNKMAP,
            vtype: EXT_VTYPE_HASH,
            db: 1,
            flags: 0,
            page: mpage,
            seq: Seq::new(0, 10),
            expire_at_ms: 0,
            n_pages: 1,
            key_len: 4,
            val_len: 5000 + CHUNK_MAX_PAYLOAD as u64,
            txn: 0,
            h32: 9,
            twin: None,
        };
        let mut mbuf = vec![0u8; PAGE];
        encode_extent(&mh, key, &[&body], crc, &mut mbuf);
        let mp = parse_extent_header(&mbuf, mpage).unwrap();
        assert_eq!(body_len(&mbuf, &mp), Some(chunkmap_body_len(4, 2)));
        assert!(verify_extent(&mbuf, &mp, crc));
        let (k, b) = chunkmap_parts(&mbuf, &mp).unwrap();
        assert_eq!(k, key);
        assert_eq!(decode_chunkmap(b), Some(refs.to_vec()));
        assert!(value_parts(&mbuf, &mp).is_none());
    }

    #[test]
    fn chunkmap_decode_rules() {
        assert_eq!(decode_chunkmap(&[0u8; 8]), Some(vec![]));
        assert!(decode_chunkmap(&[0u8; 7]).is_none());
        let r = ChunkRef { page: PageId(5), n_pages: 1, len: 4016, crc: 1 };
        let mut body = vec![0u8; 32];
        encode_chunk_refs(&[r], &mut body);
        assert!(decode_chunkmap(&body).is_some());
        assert!(decode_chunkmap(&body[..31]).is_none());
        let mut d = body.clone();
        d[4] = 1; // reserved
        assert!(decode_chunkmap(&d).is_none());
        let mut d = body.clone();
        le::put_u32(&mut d, 8 + 8, 42); // not a size class
        assert!(decode_chunkmap(&d).is_none());
        let mut d = body.clone();
        le::put_u32(&mut d, 8 + 12, 4017); // larger than one page's payload room
        assert!(decode_chunkmap(&d).is_none());
        let mut d = body;
        le::put_u32(&mut d, 0, u32::MAX);
        assert!(decode_chunkmap(&d).is_none());
    }

    #[test]
    fn header_rules() {
        let crc = RecordCrc::new(SALT);
        let page = PageId(4096);
        let h = value_hdr(page, 1, b"k", b"v");
        let mut buf = vec![0u8; PAGE];
        encode_extent(&h, b"k", &[b"v"], crc, &mut buf);
        assert!(parse_extent_header(&buf, page).is_some());
        let bad = |f: &dyn Fn(&mut Vec<u8>)| {
            let mut b = buf.clone();
            f(&mut b);
            parse_extent_header(&b, page).is_none()
        };
        assert!(bad(&|b| b[0] = 0));
        assert!(bad(&|b| b[4] = 4));
        assert!(bad(&|b| b[5] = 0));
        assert!(bad(&|b| b[5] = 3));
        assert!(bad(&|b| b[7] = 4));
        assert!(bad(&|b| le::put_u32(b, 0x20, 0)));
        assert!(bad(&|b| le::put_u32(b, 0x20, 42)));
        assert!(bad(&|b| le::put_u32(b, 0x20, 2049)));
        assert!(bad(&|b| b[7] = 0)); // expiry field without HAS_EXPIRE
        assert!(bad(&|b| le::put_u64(b, 0x30, 5))); // txn id without TXN
        assert!(bad(&|b| b[7] |= EXT_FLAG_TXN)); // TXN without an id
        assert!(bad(&|b| b[0x3C] = 1));
        assert!(bad(&|b| b[0x48] = 1));
        assert!(bad(&|b| le::put_u64(b, 0x28, 4096))); // body larger than the run
        assert!(parse_extent_header(&buf[..79], page).is_none());
        assert!(fits_value_extent(16, (8 << 20) - 96));
        assert!(!fits_value_extent(16, (8 << 20) - 95));
    }
}
