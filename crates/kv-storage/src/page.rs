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

use crate::layout::{PAGE, PageId, SMALL_HDR, le};
use crate::record::{REC_ALIGN, REC_MAGIC, RecHeader, read_small};

/// `"KVP1"`
pub const PAGE_MAGIC: u32 = u32::from_le_bytes(*b"KVP1");
pub const PAGE_KIND_SMALL: u8 = 1;
pub const PAGE_FORMAT: u16 = 1;

/// One record accepted by the tiling parser. Holds user bytes: no `Debug` (LC-32).
///
/// `len` is the record's tile length (header + key + value rounded up to 16 bytes).
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
    /// No valid header: garbage, never written, or written for another location.
    BadHeader,
}

/// Writes the 32-byte SMALL page header into `buf[..32]` (the rest of `buf` is not touched).
pub fn write_small_header(buf: &mut [u8; PAGE], page: PageId, partition: u16) {
    debug_assert!(partition <= u16::from(u8::MAX));
    let h = &mut buf[..SMALL_HDR];
    h.fill(0);
    le::put_u32(h, 0x00, PAGE_MAGIC);
    h[0x04] = PAGE_KIND_SMALL;
    h[0x05] = partition as u8;
    le::put_u16(h, 0x06, PAGE_FORMAT);
    le::put_u64(h, 0x08, page.0);
    let crc = kv_common::crc::crc32c(&h[..0x1C]);
    le::put_u32(h, 0x1C, crc);
}

/// Checks the page header: magic, header crc, format and page id (`BadHeader` otherwise), then
/// kind and partition (`Foreign` otherwise).
pub fn check_small_header(buf: &[u8; PAGE], page: PageId, partition: u16) -> PageParse {
    let h = &buf[..SMALL_HDR];
    if le::u32_at(h, 0x00) != PAGE_MAGIC
        || le::u32_at(h, 0x1C) != kv_common::crc::crc32c(&h[..0x1C])
        || le::u16_at(h, 0x06) != PAGE_FORMAT
        || le::u64_at(h, 0x08) != page.0
        || !le::zero(h, 0x10..0x1C)
    {
        return PageParse::BadHeader;
    }
    if h[0x04] != PAGE_KIND_SMALL || u16::from(h[0x05]) != partition {
        return PageParse::Foreign;
    }
    PageParse::Ok
}

/// Parses the SMALL page image `buf` of `page`, owned by `partition`: checks the header, then
/// tiles `[0x20, 0x1000)` and pushes every record that validates at its offset to `out`
/// (cleared first), in offset order. Records are accepted only for `PageParse::Ok`.
///
/// Never panics, whatever the bytes. A record is accepted only where an encoder with this salt
/// wrote it for exactly `(page, off)`; bytes inside an accepted record are never examined.
pub fn parse_small_page<'a>(
    buf: &'a [u8; PAGE],
    page: PageId,
    partition: u16,
    crc: RecordCrc,
    out: &mut Vec<SmallView<'a>>,
) -> PageParse {
    out.clear();
    let st = check_small_header(buf, page, partition);
    if st != PageParse::Ok {
        return st;
    }
    let mut off = SMALL_HDR;
    while off < PAGE {
        if buf[off] == REC_MAGIC
            && let Some(v) = read_small(buf, page, off as u16, crc)
        {
            off += usize::from(v.len);
            out.push(v);
            continue;
        }
        off += REC_ALIGN;
    }
    PageParse::Ok
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::{SMALL_REC_MAX, Seq};
    use crate::record::{REC_FLAG_TXN, REC_KIND_STRING, encode_small, small_len};
    use proptest::prelude::*;

    const SALT: u32 = 0x0BAD_CAFE;
    const P: u16 = 9;

    fn page_id() -> PageId {
        PageId((1u64 << 32) | 4242)
    }

    fn fresh(page: PageId) -> [u8; PAGE] {
        let mut b = [0u8; PAGE];
        write_small_header(&mut b, page, P);
        b
    }

    fn h(key: &[u8], val: &[u8], seq: u64, txn: Option<u64>) -> RecHeader {
        RecHeader {
            kind: REC_KIND_STRING,
            db: 0,
            flags: if txn.is_some() { REC_FLAG_TXN } else { 0 },
            seq: Seq(seq),
            expire_at_ms: 0,
            key_len: key.len() as u16,
            val_len: val.len() as u16,
            h32: seq as u32,
            txn,
        }
    }

    #[test]
    fn header_layout_and_checks() {
        let page = page_id();
        let b = fresh(page);
        assert_eq!(&b[..4], b"KVP1");
        assert_eq!(b[4], 1);
        assert_eq!(b[5], 9);
        assert_eq!(le::u16_at(&b, 6), 1);
        assert_eq!(le::u64_at(&b, 8), page.0);
        assert!(le::zero(&b, 0x10..0x1C));
        assert_eq!(le::u32_at(&b, 0x1C), kv_common::crc::crc32c(&b[..0x1C]));
        assert_eq!(check_small_header(&b, page, P), PageParse::Ok);
        assert_eq!(check_small_header(&b, page, P + 1), PageParse::Foreign);
        assert_eq!(check_small_header(&b, PageId(page.0 + 1), P), PageParse::BadHeader);
        let mut d = b;
        d[0x12] = 1;
        assert_eq!(check_small_header(&d, page, P), PageParse::BadHeader);
        assert_eq!(check_small_header(&[0u8; PAGE], page, P), PageParse::BadHeader);
        let mut out = Vec::new();
        assert_eq!(parse_small_page(&[0u8; PAGE], page, P, RecordCrc::new(SALT), &mut out), PageParse::BadHeader);
    }

    #[test]
    fn parses_records_in_offset_order() {
        let page = page_id();
        let crc = RecordCrc::new(SALT);
        let mut b = fresh(page);
        let recs: [(&[u8], &[u8], u16); 3] = [(b"a", b"1", 0x20), (b"bb", &[7u8; 300], 0x100), (b"ccc", b"", 0xF00)];
        for (i, &(k, v, off)) in recs.iter().enumerate() {
            encode_small(&h(k, v, i as u64 + 1, None), k, v, page, off, crc, &mut b[usize::from(off)..]);
        }
        let mut out = Vec::new();
        assert_eq!(parse_small_page(&b, page, P, crc, &mut out), PageParse::Ok);
        let got: Vec<_> = out.iter().map(|v| (v.key, v.val, v.off)).collect();
        assert_eq!(got, recs.iter().map(|&(k, v, o)| (k, v, o)).collect::<Vec<_>>());
        assert_eq!(out[1].len as usize, small_len(2, 300, false));
        // Foreign partition: nothing accepted.
        assert_eq!(parse_small_page(&b, page, P + 1, crc, &mut out), PageParse::Foreign);
        assert!(out.is_empty());
        // Another salt: header is fine, no record validates.
        assert_eq!(parse_small_page(&b, page, P, RecordCrc::new(SALT + 1), &mut out), PageParse::Ok);
        assert!(out.is_empty());
    }

    #[test]
    fn torn_record_exposes_embedded_images_that_do_not_validate() {
        let page = page_id();
        let crc = RecordCrc::new(SALT);
        // An inner record image encoded with the right salt for a different location, and one
        // encoded for this very location with another salt, embedded in an outer value.
        let mut inner = [0u8; 64];
        encode_small(&h(b"in", b"ner", 5, None), b"in", b"ner", page, 0x200, crc, &mut inner);
        let mut forged = [0u8; 64];
        encode_small(&h(b"fo", b"rged", 6, None), b"fo", b"rged", page, 0x60, RecordCrc::new(!SALT), &mut forged);
        let mut val = vec![0u8; 256];
        // The outer value starts at 0x20 + 32 (header) + 5 (key) = 0x45.
        val[0x60 - 0x45..0x60 - 0x45 + 64].copy_from_slice(&inner); // lands at page offset 0x60
        val[0xA0 - 0x45..0xA0 - 0x45 + 64].copy_from_slice(&forged); // lands at page offset 0xA0
        let mut b = fresh(page);
        encode_small(&h(b"outer", &val, 7, None), b"outer", &val, page, 0x20, crc, &mut b[0x20..]);
        assert_eq!(b[0x60], REC_MAGIC);
        {
            let mut out = Vec::new();
            parse_small_page(&b, page, P, crc, &mut out);
            assert_eq!(out.len(), 1);
            assert_eq!(out[0].key, b"outer");
        }
        // Tear the outer record: the parser now steps through its bytes but accepts nothing.
        b[0x30] ^= 0xFF;
        let mut out = Vec::new();
        parse_small_page(&b, page, P, crc, &mut out);
        assert!(out.is_empty());
    }

    // ----------------------------------------------------------------------------------------
    // Property tests (§25.2): never panics; never accepts a record not written at that
    // location (random bytes, records embedded in values, torn images).

    #[derive(Clone, Debug)]
    struct Rec {
        klen: usize,
        vlen: usize,
        txn: bool,
        /// Embed a record image (wrong location or wrong salt) inside the value.
        embed: Option<(u16, bool)>,
    }

    fn rec_strategy() -> impl Strategy<Value = Rec> {
        (0usize..40, 0usize..700, any::<bool>(), proptest::option::of((0u16..250, any::<bool>())))
            .prop_map(|(klen, vlen, txn, embed)| Rec { klen, vlen, txn, embed })
    }

    /// Builds a page from `recs` placed first-fit at 16-byte offsets; returns the image and the
    /// encoded bytes of every placed record by offset.
    fn build(page: PageId, crc: RecordCrc, recs: &[Rec], seed: u8) -> ([u8; PAGE], Vec<(u16, Vec<u8>)>) {
        let mut b = fresh(page);
        let mut placed = Vec::new();
        let mut off = SMALL_HDR;
        for (i, r) in recs.iter().enumerate() {
            let len = small_len(r.klen, r.vlen, r.txn);
            if len > SMALL_REC_MAX || off + len > PAGE {
                continue;
            }
            let key: Vec<u8> = (0..r.klen).map(|j| (i * 31 + j) as u8 ^ seed).collect();
            let mut val: Vec<u8> = (0..r.vlen).map(|j| (i * 7 + j * 13) as u8 ^ seed).collect();
            if let Some((slot, same_salt)) = r.embed {
                let inner_key = b"emb";
                let inner_val = [0xA5u8; 20];
                let ilen = small_len(3, 20, false);
                let hdr = if r.txn { 40 } else { 32 };
                // Absolute 16-aligned page offset inside this record's value.
                let at = (off + hdr + r.klen).next_multiple_of(16) + 16 * usize::from(slot);
                if at + ilen <= off + hdr + r.klen + r.vlen {
                    let mut img = vec![0u8; ilen];
                    let (loc, c) = if same_salt {
                        // right salt, encoded for a location other than `at`
                        (((at + 16) as u16).min(0xFF0), crc)
                    } else {
                        // right location, unknown salt
                        (at as u16, RecordCrc::new(!SALT))
                    };
                    encode_small(&h(inner_key, &inner_val, 99, None), inner_key, &inner_val, page, loc, c, &mut img);
                    let vstart = off + hdr + r.klen;
                    val[at - vstart..at - vstart + ilen].copy_from_slice(&img);
                }
            }
            let txn = r.txn.then_some(i as u64 + 1);
            encode_small(&h(&key, &val, i as u64 + 1, txn), &key, &val, page, off as u16, crc, &mut b[off..]);
            placed.push((off as u16, b[off..off + len].to_vec()));
            off += len + 16 * (i & 1);
        }
        (b, placed)
    }

    /// Every accepted record is one written at that offset: same tile length and the same
    /// checksummed bytes (header, key, value; the padding is not covered by the checksum).
    fn accepted_are_written(out: &[SmallView<'_>], buf: &[u8; PAGE], placed: &[(u16, Vec<u8>)]) -> bool {
        out.iter().all(|v| {
            let o = usize::from(v.off);
            let covered = v.hdr.len_unpadded();
            placed.iter().any(|(po, bytes)| {
                *po == v.off && bytes.len() == usize::from(v.len) && buf[o..o + covered] == bytes[..covered]
            })
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 512, failure_persistence: None, ..ProptestConfig::default() })]

        #[test]
        fn random_bytes_never_panic_or_validate(body in proptest::collection::vec(any::<u8>(), PAGE - SMALL_HDR), magic_every in 1usize..8) {
            let page = page_id();
            let crc = RecordCrc::new(SALT);
            let mut b = fresh(page);
            b[SMALL_HDR..].copy_from_slice(&body);
            // Bias towards plausible headers so that the checksum is the only barrier.
            for (i, off) in (SMALL_HDR..PAGE - 32).step_by(16).enumerate() {
                if i % magic_every == 0 {
                    b[off] = REC_MAGIC;
                    b[off + 1] = 1 + (b[off + 1] & 1);
                    b[off + 3] &= 3;
                    if b[off + 3] & 1 == 0 { b[off + 0x10..off + 0x18].fill(0); }
                    b[off + 0x19] = 0;
                    b[off + 0x1B] = 0;
                }
            }
            let mut out = Vec::new();
            prop_assert_eq!(parse_small_page(&b, page, P, crc, &mut out), PageParse::Ok);
            prop_assert!(out.is_empty());
        }

        #[test]
        fn whole_page_random_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), PAGE), partition in 0u16..300) {
            let b: [u8; PAGE] = bytes.try_into().unwrap();
            let mut out = Vec::new();
            let st = parse_small_page(&b, page_id(), partition, RecordCrc::new(SALT), &mut out);
            prop_assert!(st == PageParse::BadHeader || out.is_empty());
        }

        #[test]
        fn intact_pages_parse_exactly(recs in proptest::collection::vec(rec_strategy(), 0..24), seed in any::<u8>()) {
            let page = page_id();
            let crc = RecordCrc::new(SALT);
            let (b, placed) = build(page, crc, &recs, seed);
            let mut out = Vec::new();
            prop_assert_eq!(parse_small_page(&b, page, P, crc, &mut out), PageParse::Ok);
            prop_assert_eq!(out.len(), placed.len());
            for (v, (o, bytes)) in out.iter().zip(&placed) {
                prop_assert_eq!(v.off, *o);
                prop_assert_eq!(usize::from(v.len), bytes.len());
            }
            // The same image at another page id validates nothing (misdirected write).
            prop_assert_eq!(parse_small_page(&b, PageId(page.0 ^ 0x10), P, crc, &mut out), PageParse::BadHeader);
            let mut moved = b;
            write_small_header(&mut moved, PageId(page.0 ^ 0x10), P);
            prop_assert_eq!(parse_small_page(&moved, PageId(page.0 ^ 0x10), P, crc, &mut out), PageParse::Ok);
            prop_assert!(out.is_empty());
        }

        #[test]
        fn torn_pages_accept_only_written_records(
            recs in proptest::collection::vec(rec_strategy(), 1..24),
            seed in any::<u8>(),
            tears in proptest::collection::vec((SMALL_HDR..PAGE, any::<u8>()), 1..40),
            shift in 0usize..3,
        ) {
            let page = page_id();
            let crc = RecordCrc::new(SALT);
            let (mut b, placed) = build(page, crc, &recs, seed);
            // A second image N of the same page with the records shifted (as a rebuild might
            // place them), mixed byte-wise into M: every accepted record must be a record
            // written at that offset in M or in N.
            let shifted: Vec<Rec> = recs.iter().skip(shift).cloned().collect();
            let (n, placed_n) = build(page, crc, &shifted, seed.wrapping_add(1));
            for &(pos, mask) in &tears {
                b[pos] = (b[pos] & !mask) | (n[pos] & mask);
            }
            let mut out = Vec::new();
            prop_assert_eq!(parse_small_page(&b, page, P, crc, &mut out), PageParse::Ok);
            let mut written = placed.clone();
            written.extend(placed_n);
            prop_assert!(accepted_are_written(&out, &b, &written));
            // Accepted records never overlap.
            for w in out.windows(2) {
                prop_assert!(usize::from(w[0].off) + usize::from(w[0].len) <= usize::from(w[1].off));
            }
        }
    }
}
