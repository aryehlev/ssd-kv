//! SCAN cursor encoding (DESIGN §16.6).
//!
//! `cursor = partition:16 << 48 | slot_local:16 << 32 | fp32`, a single scalar position, with
//! `slot = slot_local * P + partition`. Cursor 0 is the start and the end of an iteration.

pub fn encode(partition: u16, slot_local: u16, fp: u32) -> u64 {
    (u64::from(partition) << 48) | (u64::from(slot_local) << 32) | u64::from(fp)
}

/// Inverse of [`encode`]: `(partition, slot_local, fp32)`.
pub fn decode(c: u64) -> (u16, u16, u32) {
    ((c >> 48) as u16, (c >> 32) as u16, c as u32)
}
