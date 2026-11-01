//! Integrity checksums (DESIGN §10.2, §10.8, §10.9, §17.3).
//!
//! CRC32C (hardware accelerated) for every on-media structure. [`RecordCrc`] is the keyed,
//! location-bound record checksum: seeded with the node's secret `record_crc_salt` and covering
//! the page id and offset, so a record image copied elsewhere, or embedded by a client inside a
//! value, never validates (§10.8). The salt never leaves the node and is never printed (no
//! `Debug`).

pub fn crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

/// Continues a CRC32C over `data`.
pub fn crc32c_append(crc: u32, data: &[u8]) -> u32 {
    crc32c::crc32c_append(crc, data)
}

/// Keyed record checksum: `crc32c_seeded(salt; page_id u64 || off u16 || parts...)` (§10.8).
#[derive(Clone, Copy)]
pub struct RecordCrc {
    salt: u32,
}

impl RecordCrc {
    pub fn new(salt: u32) -> Self {
        RecordCrc { salt }
    }

    /// Checksum of the record parts located at `(page_id, off)`.
    pub fn compute(&self, page_id: u64, off: u16, parts: &[&[u8]]) -> u32 {
        todo!("DESIGN §10.8: keyed location-bound record crc")
    }
}
