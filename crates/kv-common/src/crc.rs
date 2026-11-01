//! Integrity checksums (DESIGN §10.2, §10.8, §10.9, §17.3).
//!
//! CRC32C (hardware accelerated) for every on-media structure. [`RecordCrc`] is the keyed,
//! location-bound record checksum: seeded with the node's secret `record_crc_salt` and covering
//! the page id and offset, so a record image copied elsewhere, or embedded by a client inside a
//! value, never validates (§10.8). The salt never leaves the node and is never printed (no
//! `Debug`).
//!
//! "Seeded with the salt" means the CRC32C state starts from the salt as if it were the checksum
//! of a preceding message: `crc32c_seeded(salt; m) == crc32c_append(salt, m)`. Every multi-byte
//! integer is fed in little-endian order.

/// CRC32C (Castagnoli) of `data`.
#[inline]
pub fn crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

/// Continues a CRC32C over `data`: `crc32c_append(crc32c(a), b) == crc32c(a || b)`.
#[inline]
pub fn crc32c_append(crc: u32, data: &[u8]) -> u32 {
    crc32c::crc32c_append(crc, data)
}

/// Keyed record checksum (§10.8, §10.9).
#[derive(Clone, Copy)]
pub struct RecordCrc {
    salt: u32,
}

impl RecordCrc {
    pub fn new(salt: u32) -> Self {
        RecordCrc { salt }
    }

    /// Small-record checksum (§10.8):
    /// `crc32c_seeded(salt; page_id u64 || off u16 || parts...)`, where `off` is the record's
    /// byte offset inside its page and `parts` are the covered record bytes in order (for a small
    /// record: `rec[0x00..0x04]`, then `rec[0x08..len_unpadded]`).
    #[inline]
    pub fn compute(&self, page_id: u64, off: u16, parts: &[&[u8]]) -> u32 {
        let mut crc = crc32c_append(self.salt, &page_id.to_le_bytes());
        crc = crc32c_append(crc, &off.to_le_bytes());
        parts.iter().fold(crc, |crc, p| crc32c_append(crc, p))
    }

    /// Extent checksum (§10.9): `crc32c_seeded(salt; page_id u64 || parts...)` with `page_id` the
    /// extent's first page and `parts` = `hdr[0x00..0x4C]`, then the body. An extent always starts
    /// at offset 0 of its first page, so the location binding is the page id alone.
    #[inline]
    pub fn compute_extent(&self, page_id: u64, parts: &[&[u8]]) -> u32 {
        let crc = crc32c_append(self.salt, &page_id.to_le_bytes());
        parts.iter().fold(crc, |crc, p| crc32c_append(crc, p))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32c_check_value() {
        // The CRC-32C check value.
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(b""), 0);
        assert_eq!(crc32c_append(crc32c(b"1234"), b"56789"), 0xE306_9283);
    }

    #[test]
    fn record_crc_is_seeded_and_location_bound() {
        let k = RecordCrc::new(0xDEAD_BEEF);
        let base = k.compute(42, 0x20, &[b"head", b"body"]);
        // parts are a plain concatenation
        assert_eq!(base, k.compute(42, 0x20, &[b"headbody"]));
        assert_eq!(base, k.compute(42, 0x20, &[b"he", b"", b"adbo", b"dy"]));
        // explicit definition
        let mut m = 42u64.to_le_bytes().to_vec();
        m.extend_from_slice(&0x20u16.to_le_bytes());
        m.extend_from_slice(b"headbody");
        assert_eq!(base, crc32c_append(0xDEAD_BEEF, &m));
        // bound to page, offset and salt
        assert_ne!(base, k.compute(43, 0x20, &[b"headbody"]));
        assert_ne!(base, k.compute(42, 0x30, &[b"headbody"]));
        assert_ne!(
            base,
            RecordCrc::new(0xDEAD_BEEE).compute(42, 0x20, &[b"headbody"])
        );
    }

    #[test]
    fn extent_crc() {
        let k = RecordCrc::new(7);
        let mut m = 9u64.to_le_bytes().to_vec();
        m.extend_from_slice(b"hdrbody");
        assert_eq!(
            k.compute_extent(9, &[b"hdr", b"body"]),
            crc32c_append(7, &m)
        );
        assert_ne!(
            k.compute_extent(9, &[b"hdrbody"]),
            k.compute_extent(10, &[b"hdrbody"])
        );
        // differs from the small-record form with offset 0 (no offset bytes are fed)
        assert_ne!(k.compute_extent(9, &[b"x"]), k.compute(9, 0, &[b"x"]));
    }
}
