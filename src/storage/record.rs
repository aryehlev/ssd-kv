//! On-disk record format with 32-byte header.
//!
//! Record layout:
//! - Magic: 2 bytes (0x4B56 = "KV")
//! - Key length: 2 bytes
//! - Value length: 4 bytes
//! - Generation: 4 bytes
//! - Timestamp: 8 bytes (microseconds since epoch)
//! - TTL: 4 bytes (seconds, 0 = no expiry)
//! - Flags: 1 byte
//! - Reserved: 3 bytes
//! - CRC32: 4 bytes (covers header + key + value)
//! - Key: variable
//! - Value: variable
//! - Padding: to 128-byte boundary

use std::io::{self, Read, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::perf::simd::{crc32c, simd_memcpy};

/// Magic number for records: "KV" in little-endian.
pub const RECORD_MAGIC: u16 = 0x564B; // 'K' 'V'

/// Record header size in bytes.
pub const HEADER_SIZE: usize = 32;

/// Alignment for records on disk.
pub const RECORD_ALIGNMENT: usize = 128;

/// Maximum key size (64KB - 1).
pub const MAX_KEY_SIZE: usize = 65535;

/// Maximum value size (16MB).
pub const MAX_VALUE_SIZE: usize = 16 * 1024 * 1024;

/// Value type stored in reserved[0] of the record header.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    String = 0,
    Hash = 1,
}

impl From<u8> for ValueType {
    fn from(v: u8) -> Self {
        match v {
            1 => ValueType::Hash,
            _ => ValueType::String,
        }
    }
}

/// Record flags.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordFlags {
    None = 0,
    Deleted = 1,
    Compressed = 2,
}

impl From<u8> for RecordFlags {
    fn from(v: u8) -> Self {
        match v {
            1 => RecordFlags::Deleted,
            2 => RecordFlags::Compressed,
            _ => RecordFlags::None,
        }
    }
}

/// On-disk record header.
#[derive(Debug, Clone)]
#[repr(C)]
pub struct RecordHeader {
    pub magic: u16,
    pub key_len: u16,
    pub value_len: u32,
    pub generation: u32,
    pub timestamp: u64,
    pub ttl: u32,
    pub flags: u8,
    pub reserved: [u8; 3],
    pub crc32: u32,
}

impl RecordHeader {
    /// Creates a new record header.
    pub fn new(key_len: u16, value_len: u32, generation: u32, ttl: u32, flags: RecordFlags) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);

        Self {
            magic: RECORD_MAGIC,
            key_len,
            value_len,
            generation,
            timestamp,
            ttl,
            flags: flags as u8,
            reserved: [0; 3],
            crc32: 0, // Computed later
        }
    }

    /// Returns true if this is a valid record header.
    pub fn is_valid(&self) -> bool {
        self.magic == RECORD_MAGIC
    }

    /// Returns true if the record is marked as deleted.
    pub fn is_deleted(&self) -> bool {
        self.flags & (RecordFlags::Deleted as u8) != 0
    }

    /// Returns true if the record has expired.
    pub fn is_expired(&self) -> bool {
        if self.ttl == 0 {
            return false;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);
        let expiry = self.timestamp + (self.ttl as u64 * 1_000_000);
        now > expiry
    }

    /// Returns the value type stored in reserved[0].
    pub fn value_type(&self) -> ValueType {
        ValueType::from(self.reserved[0])
    }

    /// Sets the value type in reserved[0].
    pub fn set_value_type(&mut self, vt: ValueType) {
        self.reserved[0] = vt as u8;
    }

    /// Returns the total record size including padding.
    pub fn total_size(&self) -> usize {
        let unpadded = HEADER_SIZE + self.key_len as usize + self.value_len as usize;
        align_to_boundary(unpadded, RECORD_ALIGNMENT)
    }

    /// Returns the data size (key + value) without padding.
    pub fn data_size(&self) -> usize {
        self.key_len as usize + self.value_len as usize
    }

    /// Serializes the header to bytes.
    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..2].copy_from_slice(&self.magic.to_le_bytes());
        buf[2..4].copy_from_slice(&self.key_len.to_le_bytes());
        buf[4..8].copy_from_slice(&self.value_len.to_le_bytes());
        buf[8..12].copy_from_slice(&self.generation.to_le_bytes());
        buf[12..20].copy_from_slice(&self.timestamp.to_le_bytes());
        buf[20..24].copy_from_slice(&self.ttl.to_le_bytes());
        buf[24] = self.flags;
        buf[25..28].copy_from_slice(&self.reserved);
        buf[28..32].copy_from_slice(&self.crc32.to_le_bytes());
        buf
    }

    /// Deserializes a header from bytes.
    pub fn from_bytes(buf: &[u8; HEADER_SIZE]) -> Self {
        Self {
            magic: u16::from_le_bytes([buf[0], buf[1]]),
            key_len: u16::from_le_bytes([buf[2], buf[3]]),
            value_len: u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]),
            generation: u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]),
            timestamp: u64::from_le_bytes([
                buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18], buf[19],
            ]),
            ttl: u32::from_le_bytes([buf[20], buf[21], buf[22], buf[23]]),
            flags: buf[24],
            reserved: [buf[25], buf[26], buf[27]],
            crc32: u32::from_le_bytes([buf[28], buf[29], buf[30], buf[31]]),
        }
    }
}

/// A complete record with header, key, and value.
#[derive(Debug, Clone)]
pub struct Record {
    pub header: RecordHeader,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Record {
    /// Creates a new record.
    pub fn new(key: Vec<u8>, value: Vec<u8>, generation: u32, ttl: u32) -> io::Result<Self> {
        if key.len() > MAX_KEY_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Key too large",
            ));
        }
        if value.len() > MAX_VALUE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Value too large",
            ));
        }

        let header = RecordHeader::new(
            key.len() as u16,
            value.len() as u32,
            generation,
            ttl,
            RecordFlags::None,
        );

        Ok(Self { header, key, value })
    }

    /// Creates a tombstone record for deletion.
    pub fn tombstone(key: Vec<u8>, generation: u32) -> io::Result<Self> {
        if key.len() > MAX_KEY_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Key too large",
            ));
        }

        let header = RecordHeader::new(key.len() as u16, 0, generation, 0, RecordFlags::Deleted);

        Ok(Self {
            header,
            key,
            value: Vec::new(),
        })
    }

    /// Computes the CRC32 of the record using hardware acceleration.
    pub fn compute_crc(&self) -> u32 {
        // CRC covers header (excluding CRC field) + key + value
        // Build contiguous buffer for SIMD-accelerated CRC
        let header_bytes = self.header.to_bytes();
        let total_len = 28 + self.key.len() + self.value.len();
        let mut buf = vec![0u8; total_len];

        buf[..28].copy_from_slice(&header_bytes[..28]); // Exclude CRC field
        buf[28..28 + self.key.len()].copy_from_slice(&self.key);
        buf[28 + self.key.len()..].copy_from_slice(&self.value);

        // Hardware-accelerated CRC32C (SSE4.2 on x86_64)
        crc32c(&buf)
    }

    /// Serializes the record to bytes with padding.
    /// Uses SIMD-accelerated memory copy for large values.
    pub fn serialize(&mut self) -> Vec<u8> {
        self.header.crc32 = self.compute_crc();
        let total_size = self.header.total_size();
        let mut buf = vec![0u8; total_size];

        buf[..HEADER_SIZE].copy_from_slice(&self.header.to_bytes());

        // Use SIMD memcpy for key and value (faster for larger data)
        simd_memcpy(&mut buf[HEADER_SIZE..HEADER_SIZE + self.key.len()], &self.key);
        simd_memcpy(
            &mut buf[HEADER_SIZE + self.key.len()..HEADER_SIZE + self.key.len() + self.value.len()],
            &self.value,
        );
        // Remaining bytes are already zero (padding)

        buf
    }

    /// Deserializes a record from a reader.
    pub fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut header_buf = [0u8; HEADER_SIZE];
        reader.read_exact(&mut header_buf)?;

        let header = RecordHeader::from_bytes(&header_buf);
        if !header.is_valid() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid record magic",
            ));
        }

        let mut key = vec![0u8; header.key_len as usize];
        reader.read_exact(&mut key)?;

        let mut value = vec![0u8; header.value_len as usize];
        reader.read_exact(&mut value)?;

        // Skip padding
        let data_size = HEADER_SIZE + header.key_len as usize + header.value_len as usize;
        let padded_size = align_to_boundary(data_size, RECORD_ALIGNMENT);
        let padding = padded_size - data_size;
        if padding > 0 {
            let mut pad_buf = vec![0u8; padding];
            reader.read_exact(&mut pad_buf)?;
        }

        let record = Self { header, key, value };

        // Verify CRC
        let computed_crc = record.compute_crc();
        if computed_crc != record.header.crc32 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "CRC mismatch: expected {:08x}, got {:08x}",
                    record.header.crc32, computed_crc
                ),
            ));
        }

        Ok(record)
    }

    /// Deserializes a record from a byte slice.
    pub fn from_bytes(data: &[u8]) -> io::Result<Self> {
        let mut cursor = std::io::Cursor::new(data);
        Self::deserialize(&mut cursor)
    }

    /// Returns the total serialized size of this record.
    pub fn serialized_size(&self) -> usize {
        self.header.total_size()
    }

    /// Returns true if the record is valid (not deleted and not expired).
    pub fn is_live(&self) -> bool {
        !self.header.is_deleted() && !self.header.is_expired()
    }
}

/// Aligns a size up to the given boundary.
#[inline]
pub const fn align_to_boundary(size: usize, boundary: usize) -> usize {
    (size + boundary - 1) & !(boundary - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_header_serialization() {
        let header = RecordHeader::new(10, 100, 1, 3600, RecordFlags::None);
        let bytes = header.to_bytes();
        let restored = RecordHeader::from_bytes(&bytes);

        assert_eq!(restored.magic, RECORD_MAGIC);
        assert_eq!(restored.key_len, 10);
        assert_eq!(restored.value_len, 100);
        assert_eq!(restored.generation, 1);
        assert_eq!(restored.ttl, 3600);
    }

    #[test]
    fn test_record_serialization() {
        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();
        let mut record = Record::new(key.clone(), value.clone(), 1, 0).unwrap();

        let serialized = record.serialize();
        assert_eq!(serialized.len() % RECORD_ALIGNMENT, 0);

        let restored = Record::from_bytes(&serialized).unwrap();
        assert_eq!(restored.key, key);
        assert_eq!(restored.value, value);
        assert_eq!(restored.header.generation, 1);
    }

    #[test]
    fn test_tombstone() {
        let key = b"deleted_key".to_vec();
        let mut record = Record::tombstone(key.clone(), 5).unwrap();

        let serialized = record.serialize();
        let restored = Record::from_bytes(&serialized).unwrap();

        assert!(restored.header.is_deleted());
        assert!(!restored.is_live());
    }

    #[test]
    fn test_alignment() {
        assert_eq!(align_to_boundary(1, 128), 128);
        assert_eq!(align_to_boundary(128, 128), 128);
        assert_eq!(align_to_boundary(129, 128), 256);
    }
}
