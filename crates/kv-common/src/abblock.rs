//! A/B fixed-location metadata blocks (DESIGN §10.2, §17.6).
//!
//! Layout of every 4096-byte block: `0x000 u64 magic`, `0x008 u32 crc32c over [0x00C, 0x1000)`,
//! `0x00C u32 version (1)`, `0x010 u64 block_seq`, `0x018` payload (4072 bytes). Each block has
//! two copies; the writer always writes the copy that does not hold the newest valid state and
//! never writes both copies of one block in the same batch, so a torn write damages only the
//! copy being written. Readers take the valid copy with the higher `block_seq`.

/// Size of one block copy.
pub const AB_BLOCK: usize = 4096;
/// Offset of the payload inside a block.
pub const AB_PAYLOAD_OFF: usize = 0x18;
/// Maximum payload length.
pub const AB_PAYLOAD_MAX: usize = AB_BLOCK - AB_PAYLOAD_OFF;

/// Little-endian `u64` of an 8-byte ASCII tag such as `b"KVSSDSB1"`.
pub const fn magic(tag: &[u8; 8]) -> u64 {
    u64::from_le_bytes(*tag)
}

/// Which copy of an A/B pair.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AbCopy {
    A,
    B,
}

/// Encodes `payload` (at most [`AB_PAYLOAD_MAX`] bytes, zero padded) into `out`.
pub fn ab_encode(magic: u64, block_seq: u64, payload: &[u8], out: &mut [u8; AB_BLOCK]) {
    todo!("DESIGN §10.2: AbBlock encode")
}

/// Validates magic, version and crc; returns `(block_seq, payload)`.
pub fn ab_decode<'a>(magic: u64, block: &'a [u8]) -> Option<(u64, &'a [u8])> {
    todo!("DESIGN §10.2: AbBlock decode")
}

/// Picks the valid copy with the higher `block_seq`.
pub fn ab_pick<'a>(
    a: Option<(u64, &'a [u8])>,
    b: Option<(u64, &'a [u8])>,
) -> Option<(AbCopy, u64, &'a [u8])> {
    todo!("DESIGN §10.2: AbBlock copy selection")
}
