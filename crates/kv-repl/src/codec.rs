//! Group encoding (DESIGN §23.7).
//!
//! Group: `{seq: u64, n_ops: u16, ops}`; ops `Put{db, vtype, expire_at, key, value}`,
//! `Del{db, key}`, `Expired{db, key}`, `Expire{db, key, expire_at /*0 = persist*/}`,
//! `Append{db, key, data}`, `SetRange{db, key, offset, data}`, `FlushDb{db | 0xFF}`,
//! `FlushSlot{slot}`, `Noop`. Every op carries its full payload; there is no digest form
//! (LC-29). [`group_keys`] lists the `(db, key)` pairs of encoded groups for Repair key sets.

use crate::{Group, Seq};

/// Malformed group bytes (no contents: never echoes user bytes, LC-32).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct DecodeError;

pub fn encode_group(g: &Group, out: &mut Vec<u8>) {
    todo!("DESIGN §23.7: group encode")
}

/// Decodes one group; returns it and the bytes consumed.
pub fn decode_group(b: &[u8]) -> Result<(Group, usize), DecodeError> {
    todo!("DESIGN §23.7: group decode")
}

/// The `(db, key)` pairs touched by the encoded groups in `b`.
pub fn group_keys(b: &[u8]) -> Result<Vec<(u8, Box<[u8]>)>, DecodeError> {
    todo!("DESIGN §23.7: group key listing")
}

/// Seq of the encoded group at the start of `b` without decoding its ops.
pub fn peek_seq(b: &[u8]) -> Option<Seq> {
    todo!("DESIGN §23.7: group seq")
}
