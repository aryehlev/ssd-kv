//! Binary protocol with 16-byte header.
//!
//! Header format:
//! - Magic: 2 bytes (0x4B56 = "KV")
//! - Version: 1 byte
//! - Opcode: 1 byte
//! - Request ID: 4 bytes
//! - Flags: 1 byte
//! - Reserved: 3 bytes
//! - Payload length: 4 bytes

use std::io::{self, Read, Write};
use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Protocol magic number: "KV"
pub const PROTOCOL_MAGIC: u16 = 0x564B;

/// Current protocol version.
pub const PROTOCOL_VERSION: u8 = 1;

/// Header size in bytes.
pub const HEADER_SIZE: usize = 16;

/// Maximum payload size (16MB).
pub const MAX_PAYLOAD_SIZE: u32 = 16 * 1024 * 1024;

/// Operation codes.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Opcode {
    // Basic operations
    Get = 0x01,
    Put = 0x02,
    Delete = 0x03,

    // Batch operations
    MultiGet = 0x11,
    MultiPut = 0x12,

    // Admin operations
    Ping = 0x20,
    Stats = 0x21,
    Shutdown = 0x22,

    // Response codes
    Success = 0x80,
    NotFound = 0x81,
    Error = 0x82,
    InvalidRequest = 0x83,
}

impl From<u8> for Opcode {
    fn from(v: u8) -> Self {
        match v {
            0x01 => Opcode::Get,
            0x02 => Opcode::Put,
            0x03 => Opcode::Delete,
            0x11 => Opcode::MultiGet,
            0x12 => Opcode::MultiPut,
            0x20 => Opcode::Ping,
            0x21 => Opcode::Stats,
            0x22 => Opcode::Shutdown,
            0x80 => Opcode::Success,
            0x81 => Opcode::NotFound,
            0x82 => Opcode::Error,
            0x83 => Opcode::InvalidRequest,
            _ => Opcode::InvalidRequest,
        }
    }
}

/// Request/Response flags.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Flags {
    #[default]
    None = 0,
    /// Don't wait for durability
    NoFlush = 1,
    /// Return only metadata (no value)
    MetadataOnly = 2,
}

impl From<u8> for Flags {
    fn from(v: u8) -> Self {
        match v {
            1 => Flags::NoFlush,
            2 => Flags::MetadataOnly,
            _ => Flags::None,
        }
    }
}

/// Protocol header.
#[derive(Debug, Clone)]
pub struct Header {
    pub magic: u16,
    pub version: u8,
    pub opcode: Opcode,
    pub request_id: u32,
    pub flags: u8,
    pub reserved: [u8; 3],
    pub payload_len: u32,
}

impl Header {
    /// Creates a new request header.
    pub fn request(opcode: Opcode, request_id: u32, payload_len: u32) -> Self {
        Self {
            magic: PROTOCOL_MAGIC,
            version: PROTOCOL_VERSION,
            opcode,
            request_id,
            flags: 0,
            reserved: [0; 3],
            payload_len,
        }
    }

    /// Creates a new response header.
    pub fn response(opcode: Opcode, request_id: u32, payload_len: u32) -> Self {
        Self::request(opcode, request_id, payload_len)
    }

    /// Returns true if this is a valid header.
    pub fn is_valid(&self) -> bool {
        self.magic == PROTOCOL_MAGIC && self.version == PROTOCOL_VERSION
    }

    /// Serializes the header to bytes.
    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..2].copy_from_slice(&self.magic.to_le_bytes());
        buf[2] = self.version;
        buf[3] = self.opcode as u8;
        buf[4..8].copy_from_slice(&self.request_id.to_le_bytes());
        buf[8] = self.flags;
        buf[9..12].copy_from_slice(&self.reserved);
        buf[12..16].copy_from_slice(&self.payload_len.to_le_bytes());
        buf
    }

    /// Deserializes a header from bytes.
    pub fn from_bytes(buf: &[u8; HEADER_SIZE]) -> Self {
        Self {
            magic: u16::from_le_bytes([buf[0], buf[1]]),
            version: buf[2],
            opcode: Opcode::from(buf[3]),
            request_id: u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]),
            flags: buf[8],
            reserved: [buf[9], buf[10], buf[11]],
            payload_len: u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]),
        }
    }

    /// Writes the header to an async writer.
    pub async fn write_async<W: AsyncWrite + Unpin>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&self.to_bytes()).await
    }

    /// Reads a header from an async reader.
    pub async fn read_async<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Self> {
        let mut buf = [0u8; HEADER_SIZE];
        reader.read_exact(&mut buf).await?;
        let header = Self::from_bytes(&buf);

        if !header.is_valid() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid protocol header",
            ));
        }

        if header.payload_len > MAX_PAYLOAD_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Payload too large",
            ));
        }

        Ok(header)
    }
}

/// A complete request message.
#[derive(Debug, Clone)]
pub struct Request {
    pub header: Header,
    pub payload: Vec<u8>,
}

impl Request {
    /// Creates a GET request.
    pub fn get(request_id: u32, key: &[u8]) -> Self {
        let mut payload = Vec::with_capacity(2 + key.len());
        payload.extend_from_slice(&(key.len() as u16).to_le_bytes());
        payload.extend_from_slice(key);

        Self {
            header: Header::request(Opcode::Get, request_id, payload.len() as u32),
            payload,
        }
    }

    /// Creates a PUT request.
    pub fn put(request_id: u32, key: &[u8], value: &[u8], ttl: u32) -> Self {
        let mut payload = Vec::with_capacity(2 + key.len() + 4 + value.len() + 4);
        payload.extend_from_slice(&(key.len() as u16).to_le_bytes());
        payload.extend_from_slice(key);
        payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
        payload.extend_from_slice(value);
        payload.extend_from_slice(&ttl.to_le_bytes());

        Self {
            header: Header::request(Opcode::Put, request_id, payload.len() as u32),
            payload,
        }
    }

    /// Creates a DELETE request.
    pub fn delete(request_id: u32, key: &[u8]) -> Self {
        let mut payload = Vec::with_capacity(2 + key.len());
        payload.extend_from_slice(&(key.len() as u16).to_le_bytes());
        payload.extend_from_slice(key);

        Self {
            header: Header::request(Opcode::Delete, request_id, payload.len() as u32),
            payload,
        }
    }

    /// Creates a PING request.
    pub fn ping(request_id: u32) -> Self {
        Self {
            header: Header::request(Opcode::Ping, request_id, 0),
            payload: Vec::new(),
        }
    }

    /// Creates a STATS request.
    pub fn stats(request_id: u32) -> Self {
        Self {
            header: Header::request(Opcode::Stats, request_id, 0),
            payload: Vec::new(),
        }
    }

    /// Parses a GET request payload.
    pub fn parse_get(&self) -> io::Result<Vec<u8>> {
        if self.payload.len() < 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "GET payload too small",
            ));
        }
        let key_len = u16::from_le_bytes([self.payload[0], self.payload[1]]) as usize;
        if self.payload.len() < 2 + key_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "GET key truncated",
            ));
        }
        Ok(self.payload[2..2 + key_len].to_vec())
    }

    /// Parses a PUT request payload.
    pub fn parse_put(&self) -> io::Result<(Vec<u8>, Vec<u8>, u32)> {
        if self.payload.len() < 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "PUT payload too small",
            ));
        }
        let key_len = u16::from_le_bytes([self.payload[0], self.payload[1]]) as usize;
        let mut offset = 2;

        if self.payload.len() < offset + key_len + 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "PUT key truncated",
            ));
        }
        let key = self.payload[offset..offset + key_len].to_vec();
        offset += key_len;

        let value_len = u32::from_le_bytes([
            self.payload[offset],
            self.payload[offset + 1],
            self.payload[offset + 2],
            self.payload[offset + 3],
        ]) as usize;
        offset += 4;

        if self.payload.len() < offset + value_len + 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "PUT value truncated",
            ));
        }
        let value = self.payload[offset..offset + value_len].to_vec();
        offset += value_len;

        let ttl = u32::from_le_bytes([
            self.payload[offset],
            self.payload[offset + 1],
            self.payload[offset + 2],
            self.payload[offset + 3],
        ]);

        Ok((key, value, ttl))
    }

    /// Parses a DELETE request payload.
    pub fn parse_delete(&self) -> io::Result<Vec<u8>> {
        self.parse_get() // Same format as GET
    }

    /// Reads a request from an async reader.
    pub async fn read_async<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Self> {
        let header = Header::read_async(reader).await?;
        let mut payload = vec![0u8; header.payload_len as usize];
        if header.payload_len > 0 {
            reader.read_exact(&mut payload).await?;
        }
        Ok(Self { header, payload })
    }

    /// Writes the request to an async writer.
    pub async fn write_async<W: AsyncWrite + Unpin>(&self, writer: &mut W) -> io::Result<()> {
        self.header.write_async(writer).await?;
        if !self.payload.is_empty() {
            writer.write_all(&self.payload).await?;
        }
        Ok(())
    }
}

/// A complete response message.
#[derive(Debug, Clone)]
pub struct Response {
    pub header: Header,
    pub payload: Vec<u8>,
}

impl Response {
    /// Creates a success response with a value.
    pub fn success(request_id: u32, value: &[u8]) -> Self {
        Self {
            header: Header::response(Opcode::Success, request_id, value.len() as u32),
            payload: value.to_vec(),
        }
    }

    /// Creates a not-found response.
    pub fn not_found(request_id: u32) -> Self {
        Self {
            header: Header::response(Opcode::NotFound, request_id, 0),
            payload: Vec::new(),
        }
    }

    /// Creates an error response.
    pub fn error(request_id: u32, message: &str) -> Self {
        let payload = message.as_bytes().to_vec();
        Self {
            header: Header::response(Opcode::Error, request_id, payload.len() as u32),
            payload,
        }
    }

    /// Creates an invalid request response.
    pub fn invalid(request_id: u32, message: &str) -> Self {
        let payload = message.as_bytes().to_vec();
        Self {
            header: Header::response(Opcode::InvalidRequest, request_id, payload.len() as u32),
            payload,
        }
    }

    /// Creates a pong response.
    pub fn pong(request_id: u32) -> Self {
        Self {
            header: Header::response(Opcode::Success, request_id, 0),
            payload: Vec::new(),
        }
    }

    /// Creates a stats response.
    pub fn stats(request_id: u32, stats_json: &str) -> Self {
        let payload = stats_json.as_bytes().to_vec();
        Self {
            header: Header::response(Opcode::Success, request_id, payload.len() as u32),
            payload,
        }
    }

    /// Serializes the response to bytes (header + payload).
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HEADER_SIZE + self.payload.len());
        buf.extend_from_slice(&self.header.to_bytes());
        buf.extend_from_slice(&self.payload);
        buf
    }

    /// Reads a response from an async reader.
    pub async fn read_async<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Self> {
        let header = Header::read_async(reader).await?;
        let mut payload = vec![0u8; header.payload_len as usize];
        if header.payload_len > 0 {
            reader.read_exact(&mut payload).await?;
        }
        Ok(Self { header, payload })
    }

    /// Writes the response to an async writer.
    pub async fn write_async<W: AsyncWrite + Unpin>(&self, writer: &mut W) -> io::Result<()> {
        self.header.write_async(writer).await?;
        if !self.payload.is_empty() {
            writer.write_all(&self.payload).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_serialization() {
        let header = Header::request(Opcode::Get, 12345, 100);
        let bytes = header.to_bytes();
        let restored = Header::from_bytes(&bytes);

        assert!(restored.is_valid());
        assert_eq!(restored.opcode, Opcode::Get);
        assert_eq!(restored.request_id, 12345);
        assert_eq!(restored.payload_len, 100);
    }

    #[test]
    fn test_request_get() {
        let req = Request::get(1, b"test_key");
        let key = req.parse_get().unwrap();
        assert_eq!(key, b"test_key");
    }

    #[test]
    fn test_request_put() {
        let req = Request::put(1, b"key", b"value", 3600);
        let (key, value, ttl) = req.parse_put().unwrap();
        assert_eq!(key, b"key");
        assert_eq!(value, b"value");
        assert_eq!(ttl, 3600);
    }

    #[tokio::test]
    async fn test_async_roundtrip() {
        let req = Request::put(42, b"async_key", b"async_value", 0);

        let mut buf = Vec::new();
        req.write_async(&mut buf).await.unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        let restored = Request::read_async(&mut cursor).await.unwrap();

        assert_eq!(restored.header.request_id, 42);
        let (key, value, ttl) = restored.parse_put().unwrap();
        assert_eq!(key, b"async_key");
        assert_eq!(value, b"async_value");
    }
}
