//! Group encoding (DESIGN §23.7).
//!
//! Group: `{seq: u64, n_ops: u16, ops}`; ops `Put{db, vtype, expire_at, key, value}`,
//! `Del{db, key}`, `Expired{db, key}`, `Expire{db, key, expire_at /*0 = persist*/}`,
//! `Append{db, key, data}`, `SetRange{db, key, offset, data}`, `FlushDb{db | 0xFF}`,
//! `FlushSlot{slot}`, `Noop`. Every op carries its full payload; there is no reduced form
//! (LC-29). [`group_keys`] lists the `(db, key)` pairs of encoded groups for Repair key sets.
//!
//! Byte layout (little endian). Every encoded group is self-delimiting, so a buffer may hold
//! several groups back to back (a `GROUPS` frame payload, a backlog read):
//!
//! ```text
//! u32 len (bytes after this field) | u64 seq | u16 n_ops | op...
//! op = u8 tag | fields
//!   1 Put      u8 db, u8 vtype, u64 expire_at, u32 klen, key, u32 vlen, value
//!   2 Del      u8 db, u32 klen, key
//!   3 Expired  u8 db, u32 klen, key
//!   4 Expire   u8 db, u32 klen, key, u64 expire_at
//!   5 Append   u8 db, u32 klen, key, u32 dlen, data
//!   6 SetRange u8 db, u32 klen, key, u64 offset, u32 dlen, data
//!   7 FlushDb  u8 db (0xFF = every database)
//!   8 FlushSlot u16 slot
//!   9 Noop
//! ```

use crate::{Group, Op, Seq};

/// Malformed group bytes (no contents: never echoes user bytes, LC-32).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct DecodeError;

impl core::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("malformed replication group")
    }
}

impl std::error::Error for DecodeError {}

/// Bytes of the length prefix of an encoded group.
pub const GROUP_LEN_PREFIX: usize = 4;
/// Bytes of the fixed group header after the length prefix (`seq`, `n_ops`).
pub const GROUP_HDR: usize = 10;
/// Largest encoded group accepted by the decoder (a 512 MiB value plus its key and headers).
pub const MAX_GROUP_BYTES: usize = (512 << 20) + (1 << 20);

const TAG_PUT: u8 = 1;
const TAG_DEL: u8 = 2;
const TAG_EXPIRED: u8 = 3;
const TAG_EXPIRE: u8 = 4;
const TAG_APPEND: u8 = 5;
const TAG_SET_RANGE: u8 = 6;
const TAG_FLUSH_DB: u8 = 7;
const TAG_FLUSH_SLOT: u8 = 8;
const TAG_NOOP: u8 = 9;

/// `FlushDb { db: None }` on the wire.
const ALL_DBS: u8 = 0xFF;

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
    out.extend_from_slice(b);
}

/// Encoded length of one op.
fn op_len(op: &Op) -> usize {
    1 + match op {
        Op::Put { key, value, .. } => 1 + 1 + 8 + 4 + key.len() + 4 + value.len(),
        Op::Del { key, .. } | Op::Expired { key, .. } => 1 + 4 + key.len(),
        Op::Expire { key, .. } => 1 + 4 + key.len() + 8,
        Op::Append { key, data, .. } => 1 + 4 + key.len() + 4 + data.len(),
        Op::SetRange { key, data, .. } => 1 + 4 + key.len() + 8 + 4 + data.len(),
        Op::FlushDb { .. } => 1,
        Op::FlushSlot { .. } => 2,
        Op::Noop => 0,
    }
}

/// Bytes `encode_group(g)` appends.
pub fn encoded_len(g: &Group) -> usize {
    GROUP_LEN_PREFIX + GROUP_HDR + g.ops.iter().map(op_len).sum::<usize>()
}

/// Appends the encoding of `g` (length prefix included) to `out`.
pub fn encode_group(g: &Group, out: &mut Vec<u8>) {
    let total = encoded_len(g);
    out.reserve(total);
    out.extend_from_slice(&((total - GROUP_LEN_PREFIX) as u32).to_le_bytes());
    out.extend_from_slice(&g.seq.0.to_le_bytes());
    out.extend_from_slice(&(g.ops.len().min(u16::MAX as usize) as u16).to_le_bytes());
    for op in g.ops.iter().take(u16::MAX as usize) {
        encode_op(op, out);
    }
}

fn encode_op(op: &Op, out: &mut Vec<u8>) {
    match op {
        Op::Put { db, vtype, expire_at, key, value } => {
            out.push(TAG_PUT);
            out.push(*db);
            out.push(*vtype);
            out.extend_from_slice(&expire_at.to_le_bytes());
            put_bytes(out, key);
            put_bytes(out, value);
        }
        Op::Del { db, key } => {
            out.push(TAG_DEL);
            out.push(*db);
            put_bytes(out, key);
        }
        Op::Expired { db, key } => {
            out.push(TAG_EXPIRED);
            out.push(*db);
            put_bytes(out, key);
        }
        Op::Expire { db, key, expire_at } => {
            out.push(TAG_EXPIRE);
            out.push(*db);
            put_bytes(out, key);
            out.extend_from_slice(&expire_at.to_le_bytes());
        }
        Op::Append { db, key, data } => {
            out.push(TAG_APPEND);
            out.push(*db);
            put_bytes(out, key);
            put_bytes(out, data);
        }
        Op::SetRange { db, key, offset, data } => {
            out.push(TAG_SET_RANGE);
            out.push(*db);
            put_bytes(out, key);
            out.extend_from_slice(&offset.to_le_bytes());
            put_bytes(out, data);
        }
        Op::FlushDb { db } => {
            out.push(TAG_FLUSH_DB);
            out.push(db.unwrap_or(ALL_DBS));
        }
        Op::FlushSlot { slot } => {
            out.push(TAG_FLUSH_SLOT);
            out.extend_from_slice(&slot.to_le_bytes());
        }
        Op::Noop => out.push(TAG_NOOP),
    }
}

/// A bounds-checked little-endian reader over one group body.
pub(crate) struct Rd<'a> {
    b: &'a [u8],
    at: usize,
}

impl<'a> Rd<'a> {
    pub(crate) fn new(b: &'a [u8]) -> Self {
        Rd { b, at: 0 }
    }

    pub(crate) fn done(&self) -> bool {
        self.at == self.b.len()
    }

    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        let end = self.at.checked_add(n).ok_or(DecodeError)?;
        let s = self.b.get(self.at..end).ok_or(DecodeError)?;
        self.at = end;
        Ok(s)
    }

    pub(crate) fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u16(&mut self) -> Result<u16, DecodeError> {
        let s = self.take(2)?;
        Ok(u16::from_le_bytes([s[0], s[1]]))
    }

    pub(crate) fn u32(&mut self) -> Result<u32, DecodeError> {
        let s = self.take(4)?;
        Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
    }

    pub(crate) fn u64(&mut self) -> Result<u64, DecodeError> {
        let s = self.take(8)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(s);
        Ok(u64::from_le_bytes(a))
    }

    pub(crate) fn bytes(&mut self) -> Result<&'a [u8], DecodeError> {
        let n = self.u32()? as usize;
        self.take(n)
    }
}

/// The body (after the length prefix) of the encoded group at the start of `b`, and the bytes
/// the group occupies in total.
fn split_group(b: &[u8]) -> Result<(&[u8], usize), DecodeError> {
    let len = b.get(..GROUP_LEN_PREFIX).ok_or(DecodeError)?;
    let len = u32::from_le_bytes([len[0], len[1], len[2], len[3]]) as usize;
    if !(GROUP_HDR..=MAX_GROUP_BYTES).contains(&len) {
        return Err(DecodeError);
    }
    let end = GROUP_LEN_PREFIX + len;
    let body = b.get(GROUP_LEN_PREFIX..end).ok_or(DecodeError)?;
    Ok((body, end))
}

/// Decodes one group; returns it and the bytes consumed.
pub fn decode_group(b: &[u8]) -> Result<(Group, usize), DecodeError> {
    let (body, used) = split_group(b)?;
    let mut r = Rd::new(body);
    let seq = Seq(r.u64()?);
    let n = r.u16()?;
    let mut ops = Vec::with_capacity(usize::from(n));
    for _ in 0..n {
        ops.push(decode_op(&mut r)?);
    }
    if !r.done() {
        return Err(DecodeError);
    }
    Ok((Group { seq, ops }, used))
}

fn decode_op(r: &mut Rd<'_>) -> Result<Op, DecodeError> {
    Ok(match r.u8()? {
        TAG_PUT => {
            let db = r.u8()?;
            let vtype = r.u8()?;
            let expire_at = r.u64()?;
            let key = r.bytes()?.into();
            let value = r.bytes()?.to_vec();
            Op::Put { db, vtype, expire_at, key, value }
        }
        TAG_DEL => {
            let db = r.u8()?;
            Op::Del { db, key: r.bytes()?.into() }
        }
        TAG_EXPIRED => {
            let db = r.u8()?;
            Op::Expired { db, key: r.bytes()?.into() }
        }
        TAG_EXPIRE => {
            let db = r.u8()?;
            let key = r.bytes()?.into();
            Op::Expire { db, key, expire_at: r.u64()? }
        }
        TAG_APPEND => {
            let db = r.u8()?;
            let key = r.bytes()?.into();
            Op::Append { db, key, data: r.bytes()?.to_vec() }
        }
        TAG_SET_RANGE => {
            let db = r.u8()?;
            let key = r.bytes()?.into();
            let offset = r.u64()?;
            Op::SetRange { db, key, offset, data: r.bytes()?.to_vec() }
        }
        TAG_FLUSH_DB => {
            let db = r.u8()?;
            Op::FlushDb { db: (db != ALL_DBS).then_some(db) }
        }
        TAG_FLUSH_SLOT => Op::FlushSlot { slot: r.u16()? },
        TAG_NOOP => Op::Noop,
        _ => return Err(DecodeError),
    })
}

/// Visits the `(db, key)` of every keyed op of one group body, and reports partition-wide ops
/// (`FlushDb`, `FlushSlot`) through `wide`.
fn visit_keys<'a>(body: &'a [u8], f: &mut dyn FnMut(u8, &'a [u8]), wide: &mut dyn FnMut(&Op)) -> Result<(), DecodeError> {
    let mut r = Rd::new(body);
    let _seq = r.u64()?;
    let n = r.u16()?;
    for _ in 0..n {
        match r.u8()? {
            TAG_PUT => {
                let db = r.u8()?;
                r.take(1 + 8)?;
                let key = r.bytes()?;
                r.bytes()?;
                f(db, key);
            }
            TAG_DEL | TAG_EXPIRED => {
                let db = r.u8()?;
                f(db, r.bytes()?);
            }
            TAG_EXPIRE => {
                let db = r.u8()?;
                let key = r.bytes()?;
                r.take(8)?;
                f(db, key);
            }
            TAG_APPEND => {
                let db = r.u8()?;
                let key = r.bytes()?;
                r.bytes()?;
                f(db, key);
            }
            TAG_SET_RANGE => {
                let db = r.u8()?;
                let key = r.bytes()?;
                r.take(8)?;
                r.bytes()?;
                f(db, key);
            }
            TAG_FLUSH_DB => {
                let db = r.u8()?;
                wide(&Op::FlushDb { db: (db != ALL_DBS).then_some(db) });
            }
            TAG_FLUSH_SLOT => {
                let slot = r.u16()?;
                wide(&Op::FlushSlot { slot });
            }
            TAG_NOOP => {}
            _ => return Err(DecodeError),
        }
    }
    if !r.done() {
        return Err(DecodeError);
    }
    Ok(())
}

/// `(db, key)` pairs (Repair key sets).
pub type KeyList = Vec<(u8, Box<[u8]>)>;

/// The `(db, key)` pairs touched by the encoded groups in `b` (each pair once, in first-touch
/// order).
pub fn group_keys(b: &[u8]) -> Result<KeyList, DecodeError> {
    let mut out = KeySet::default();
    let mut at = 0;
    while at < b.len() {
        let (body, used) = split_group(&b[at..])?;
        visit_keys(body, &mut |db, k| out.insert(db, k), &mut |_| {})?;
        at += used;
    }
    Ok(out.into_vec())
}

/// Like [`group_keys`], and also whether any group holds a partition-wide op (`FlushDb`,
/// `FlushSlot`), whose effect a key set cannot describe.
pub fn group_keys_and_wide(b: &[u8], out: &mut KeySet) -> Result<bool, DecodeError> {
    let mut wide = false;
    let mut at = 0;
    while at < b.len() {
        let (body, used) = split_group(&b[at..])?;
        visit_keys(body, &mut |db, k| out.insert(db, k), &mut |_| wide = true)?;
        at += used;
    }
    Ok(wide)
}

/// Seq of the encoded group at the start of `b` without decoding its ops.
pub fn peek_seq(b: &[u8]) -> Option<Seq> {
    let (body, _) = split_group(b).ok()?;
    let s = body.get(..8)?;
    let mut a = [0u8; 8];
    a.copy_from_slice(s);
    Some(Seq(u64::from_le_bytes(a)))
}

/// Total bytes of the encoded group at the start of `b` (`None` if the prefix is incomplete or
/// invalid).
pub fn peek_len(b: &[u8]) -> Option<usize> {
    let len = b.get(..GROUP_LEN_PREFIX)?;
    let len = u32::from_le_bytes([len[0], len[1], len[2], len[3]]) as usize;
    (GROUP_HDR..=MAX_GROUP_BYTES).contains(&len).then_some(GROUP_LEN_PREFIX + len)
}

/// Iterates `(seq, encoded group)` over a buffer of whole encoded groups; stops at the first
/// malformed group.
pub fn iter_groups(b: &[u8]) -> impl Iterator<Item = (Seq, &[u8])> + '_ {
    let mut at = 0;
    core::iter::from_fn(move || {
        let rest = b.get(at..)?;
        if rest.is_empty() {
            return None;
        }
        let n = peek_len(rest)?;
        let g = rest.get(..n)?;
        let seq = peek_seq(g)?;
        at += n;
        Some((seq, g))
    })
}

/// An insertion-ordered set of `(db, key)` pairs (Repair key sets; each key once).
#[derive(Default)]
pub struct KeySet {
    seen: hashbrown::HashSet<(u8, Box<[u8]>), foldhash::fast::RandomState>,
    order: Vec<(u8, Box<[u8]>)>,
}

impl KeySet {
    pub fn insert(&mut self, db: u8, key: &[u8]) {
        let k = (db, Box::<[u8]>::from(key));
        if !self.seen.contains(&k) {
            self.seen.insert(k.clone());
            self.order.push(k);
        }
    }

    pub fn extend(&mut self, keys: impl IntoIterator<Item = (u8, Box<[u8]>)>) {
        for (db, k) in keys {
            self.insert(db, &k);
        }
    }

    pub fn contains(&self, db: u8, key: &[u8]) -> bool {
        self.seen.contains(&(db, Box::<[u8]>::from(key)))
    }

    pub fn len(&self) -> usize {
        self.order.len()
    }

    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    pub fn into_vec(self) -> Vec<(u8, Box<[u8]>)> {
        self.order
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn bytes() -> impl Strategy<Value = Vec<u8>> {
        proptest::collection::vec(any::<u8>(), 0..300)
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            (any::<u8>(), 1u8..=2, any::<u64>(), bytes(), bytes())
                .prop_map(|(db, vtype, expire_at, k, value)| Op::Put { db, vtype, expire_at, key: k.into(), value }),
            (any::<u8>(), bytes()).prop_map(|(db, k)| Op::Del { db, key: k.into() }),
            (any::<u8>(), bytes()).prop_map(|(db, k)| Op::Expired { db, key: k.into() }),
            (any::<u8>(), bytes(), any::<u64>()).prop_map(|(db, k, e)| Op::Expire { db, key: k.into(), expire_at: e }),
            (any::<u8>(), bytes(), bytes()).prop_map(|(db, k, d)| Op::Append { db, key: k.into(), data: d }),
            (any::<u8>(), bytes(), any::<u64>(), bytes())
                .prop_map(|(db, k, o, d)| Op::SetRange { db, key: k.into(), offset: o, data: d }),
            proptest::option::of(0u8..16).prop_map(|db| Op::FlushDb { db }),
            any::<u16>().prop_map(|slot| Op::FlushSlot { slot }),
            Just(Op::Noop),
        ]
    }

    fn group() -> impl Strategy<Value = Group> {
        (any::<u64>(), proptest::collection::vec(op(), 0..8)).prop_map(|(s, ops)| Group { seq: Seq(s), ops })
    }

    /// The payload bytes an op must carry in full (LC-29).
    fn payloads(op: &Op) -> Vec<&[u8]> {
        match op {
            Op::Put { key, value, .. } => vec![key, value],
            Op::Del { key, .. } | Op::Expired { key, .. } | Op::Expire { key, .. } => vec![key],
            Op::Append { key, data, .. } | Op::SetRange { key, data, .. } => vec![key, data],
            _ => vec![],
        }
    }

    fn find(hay: &[u8], needle: &[u8]) -> bool {
        needle.is_empty() || hay.windows(needle.len()).any(|w| w == needle)
    }

    proptest! {
        #[test]
        fn group_round_trip(g in group()) {
            let mut b = Vec::new();
            encode_group(&g, &mut b);
            prop_assert_eq!(b.len(), encoded_len(&g));
            let (d, n) = decode_group(&b).unwrap();
            prop_assert_eq!(n, b.len());
            prop_assert_eq!(&d, &g);
            prop_assert_eq!(peek_seq(&b), Some(g.seq));
            prop_assert_eq!(peek_len(&b), Some(b.len()));
        }

        /// LC-29: every op of an encoded group carries its full key and value bytes; the
        /// encoding is never shorter than the payloads it describes.
        #[test]
        fn every_op_encodes_its_full_payload(o in op()) {
            let g = Group { seq: Seq(7), ops: vec![o.clone()] };
            let mut b = Vec::new();
            encode_group(&g, &mut b);
            let total: usize = payloads(&o).iter().map(|p| p.len()).sum();
            prop_assert!(b.len() >= GROUP_LEN_PREFIX + GROUP_HDR + total);
            for p in payloads(&o) {
                prop_assert!(find(&b, p));
            }
        }

        #[test]
        fn concatenated_groups_iterate_and_list_keys(gs in proptest::collection::vec(group(), 0..6)) {
            let mut b = Vec::new();
            for g in &gs {
                encode_group(g, &mut b);
            }
            let seqs: Vec<Seq> = iter_groups(&b).map(|(s, _)| s).collect();
            prop_assert_eq!(seqs, gs.iter().map(|g| g.seq).collect::<Vec<_>>());
            let mut want = KeySet::default();
            let mut wide = false;
            for g in &gs {
                for o in &g.ops {
                    match o {
                        Op::Put { db, key, .. } | Op::Del { db, key } | Op::Expired { db, key }
                        | Op::Expire { db, key, .. } | Op::Append { db, key, .. } | Op::SetRange { db, key, .. } => want.insert(*db, key),
                        Op::FlushDb { .. } | Op::FlushSlot { .. } => wide = true,
                        Op::Noop => {}
                    }
                }
            }
            prop_assert_eq!(group_keys(&b).unwrap(), want.into_vec());
            let mut ks = KeySet::default();
            prop_assert_eq!(group_keys_and_wide(&b, &mut ks).unwrap(), wide);
        }

        #[test]
        fn decoder_never_panics(b in proptest::collection::vec(any::<u8>(), 0..200)) {
            let _ = decode_group(&b);
            let _ = group_keys(&b);
            let _ = peek_seq(&b);
            let _ = iter_groups(&b).count();
        }
    }

    #[test]
    fn truncated_and_trailing_bytes_are_malformed() {
        let g = Group { seq: Seq::new(3, 9), ops: vec![Op::Put { db: 0, vtype: 1, expire_at: 0, key: b"k".as_slice().into(), value: b"v".to_vec() }] };
        let mut b = Vec::new();
        encode_group(&g, &mut b);
        for cut in 0..b.len() {
            assert!(decode_group(&b[..cut]).is_err(), "cut {cut}");
        }
        // a length prefix that covers a byte the ops do not consume
        let mut long = b.clone();
        let len = u32::from_le_bytes([b[0], b[1], b[2], b[3]]) + 1;
        long[..4].copy_from_slice(&len.to_le_bytes());
        long.push(0);
        assert!(decode_group(&long).is_err());
        // unknown op tag
        let mut bad = b.clone();
        bad[GROUP_LEN_PREFIX + GROUP_HDR] = 0x55;
        assert!(decode_group(&bad).is_err());
    }

    #[test]
    fn large_values_round_trip() {
        let value = vec![0xA5u8; 5 << 20];
        let g = Group { seq: Seq::new(1, 1), ops: vec![Op::Put { db: 0, vtype: 1, expire_at: 9, key: b"big".as_slice().into(), value: value.clone() }] };
        let mut b = Vec::new();
        encode_group(&g, &mut b);
        let (d, _) = decode_group(&b).unwrap();
        assert_eq!(d, g);
    }
}
