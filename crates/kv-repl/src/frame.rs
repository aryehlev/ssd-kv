//! Replication frames and handshake (DESIGN §23.7, §23.9, §23.13).
//!
//! Frame header (24 B): `u16 magic 0x524B ("KR")`, `u8 version`, `u8 type`, `u16 flags`,
//! `u16 partition`, `u32 term`, `u32 payload_len (<= 8 MiB)`, `u32 crc32c(payload)`, `u32 zero`.
//! Handshake: member -> `Hello`; primary -> `Accept { term, mode }` or `Reject(reason)`. Both
//! sides reject peers whose term is below their fence: every frame carries the sender's term.
//! Types holding user bytes or the auth token have no `Debug` (LC-32).
//!
//! A logical frame whose payload exceeds [`FRAME_MAX_PAYLOAD`] (a value above a few MiB in a
//! `GROUPS`, `SNAP_REC` or `IMAGE` frame) travels as consecutive `PUT_PART` fragments of its
//! encoding, at most [`PART_BYTES`] each; [`Reassembly`] rebuilds it on the receiving side.
//!
//! Payload layouts (little endian):
//!
//! ```text
//! HELLO       [16] cluster_id, u32 auth_len, auth, [20] node_id, u16 shard, u16 partition,
//!             u32 fence_term, u32 last_term, u64 applied, u8 recovered, u8 learner,
//!             u8 has_migration, u64 migration
//! ACCEPT      u32 term, u8 mode (1 continue, 2 repair, 3 full), u64 seq
//! REJECT      u8 reason
//! GROUPS      encoded groups (codec), back to back
//! COMMIT      u64 commit_seq          HEARTBEAT  u64 commit_seq
//! ACK         u64 durable, u64 applied
//! SNAP_BEGIN  u16 slot                SNAP_END   u16 slot, u64 count
//! SNAP_REC    u8 db, u8 vtype, u64 expire_at, u32 klen, key, u32 vlen, value
//! SYNC_DONE   u64 at
//! REPAIR_KEYS u32 n, n x (u8 db, u32 klen, key)
//! IMAGE       u8 db, u32 klen, key, u8 present [, u8 vtype, u64 expire_at, u32 vlen, value]
//! SEAL        u64 migration, u64 last_src_seq
//! PUT_PART    u64 seq, u8 db, u32 klen, key, u64 offset, u64 total, u32 dlen, data
//! ```

use crate::Seq;
use crate::codec::{DecodeError, Rd};

/// `"KR"`
pub const FRAME_MAGIC: u16 = 0x524B;
pub const FRAME_VERSION: u8 = 1;
pub const FRAME_HDR: usize = 24;
pub const FRAME_MAX_PAYLOAD: usize = 8 << 20;
/// Data bytes of one `PUT_PART` fragment (values above 4 MiB are split, §23.7).
pub const PART_BYTES: usize = 4 << 20;
/// Largest logical frame [`Reassembly`] rebuilds (a 512 MiB value with its headers).
pub const MAX_REASSEMBLED: usize = (512 << 20) + (2 << 20);

const T_HELLO: u8 = 1;
const T_ACCEPT: u8 = 2;
const T_REJECT: u8 = 3;
const T_GROUPS: u8 = 4;
const T_COMMIT: u8 = 5;
const T_HEARTBEAT: u8 = 6;
const T_ACK: u8 = 7;
const T_SNAP_BEGIN: u8 = 8;
const T_SNAP_REC: u8 = 9;
const T_SNAP_END: u8 = 10;
const T_SYNC_DONE: u8 = 11;
const T_REPAIR_KEYS: u8 = 12;
const T_IMAGE: u8 = 13;
const T_SEAL: u8 = 14;
const T_PUT_PART: u8 = 15;

/// Member -> primary handshake.
pub struct Hello {
    pub cluster_id: [u8; 16],
    pub auth: Vec<u8>,
    pub node_id: [u8; 20],
    pub shard: u16,
    pub partition: u16,
    pub fence_term: u32,
    pub last_term: u32,
    /// = persisted `repl_durable_seq`, a lower bound
    pub applied: Seq,
    pub recovered: Recovered,
    pub learner: bool,
    /// set when a migration destination partition dials the source, §23.13
    pub migration: Option<u64>,
}

/// How the member's partition came up.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Recovered {
    Clean,
    Scanned,
    SyncInterrupted,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AcceptMode {
    Continue { from: Seq },
    Repair { at: Seq },
    Full { at: Seq },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RejectReason {
    StaleTerm,
    NotMember,
    NotPrimary,
    NeedOperatorResync,
    BadAuth,
}

/// One live record of a full-sync or migration snapshot walk.
pub struct SnapRec {
    pub db: u8,
    pub vtype: u8,
    pub expire_at: u64,
    pub key: Box<[u8]>,
    pub value: Vec<u8>,
}

/// The primary's current state of one key (Repair); `None` = absent.
pub struct ImageRec {
    pub db: u8,
    pub key: Box<[u8]>,
    /// `(vtype, expire_at, value)`
    pub image: Option<(u8, u64, Vec<u8>)>,
}

pub enum Frame {
    Hello(Hello),
    Accept { term: u32, mode: AcceptMode },
    Reject(RejectReason),
    /// A sequence of encoded groups.
    Groups(Vec<u8>),
    Commit(Seq),
    Heartbeat(Seq),
    Ack { durable: Seq, applied: Seq },
    SnapBegin { slot: u16 },
    SnapRec(SnapRec),
    SnapEnd { slot: u16, count: u64 },
    SyncDone { at: Seq },
    RepairKeys(Vec<(u8, Box<[u8]>)>),
    Image(ImageRec),
    Seal { mig: u64, last: Seq },
    PutPart { seq: Seq, db: u8, key: Box<[u8]>, offset: u64, total: u64, data: Vec<u8> },
}

impl Frame {
    /// Short name for diagnostics (no contents).
    pub fn kind(&self) -> &'static str {
        match self {
            Frame::Hello(_) => "hello",
            Frame::Accept { .. } => "accept",
            Frame::Reject(_) => "reject",
            Frame::Groups(_) => "groups",
            Frame::Commit(_) => "commit",
            Frame::Heartbeat(_) => "heartbeat",
            Frame::Ack { .. } => "ack",
            Frame::SnapBegin { .. } => "snap-begin",
            Frame::SnapRec(_) => "snap-rec",
            Frame::SnapEnd { .. } => "snap-end",
            Frame::SyncDone { .. } => "sync-done",
            Frame::RepairKeys(_) => "repair-keys",
            Frame::Image(_) => "image",
            Frame::Seal { .. } => "seal",
            Frame::PutPart { .. } => "put-part",
        }
    }

    fn type_code(&self) -> u8 {
        match self {
            Frame::Hello(_) => T_HELLO,
            Frame::Accept { .. } => T_ACCEPT,
            Frame::Reject(_) => T_REJECT,
            Frame::Groups(_) => T_GROUPS,
            Frame::Commit(_) => T_COMMIT,
            Frame::Heartbeat(_) => T_HEARTBEAT,
            Frame::Ack { .. } => T_ACK,
            Frame::SnapBegin { .. } => T_SNAP_BEGIN,
            Frame::SnapRec(_) => T_SNAP_REC,
            Frame::SnapEnd { .. } => T_SNAP_END,
            Frame::SyncDone { .. } => T_SYNC_DONE,
            Frame::RepairKeys(_) => T_REPAIR_KEYS,
            Frame::Image(_) => T_IMAGE,
            Frame::Seal { .. } => T_SEAL,
            Frame::PutPart { .. } => T_PUT_PART,
        }
    }
}

fn put_u16(o: &mut Vec<u8>, v: u16) {
    o.extend_from_slice(&v.to_le_bytes());
}
fn put_u32(o: &mut Vec<u8>, v: u32) {
    o.extend_from_slice(&v.to_le_bytes());
}
fn put_u64(o: &mut Vec<u8>, v: u64) {
    o.extend_from_slice(&v.to_le_bytes());
}
fn put_bytes(o: &mut Vec<u8>, b: &[u8]) {
    put_u32(o, b.len() as u32);
    o.extend_from_slice(b);
}

fn recovered_code(r: Recovered) -> u8 {
    match r {
        Recovered::Clean => 0,
        Recovered::Scanned => 1,
        Recovered::SyncInterrupted => 2,
    }
}

fn reason_code(r: RejectReason) -> u8 {
    match r {
        RejectReason::StaleTerm => 1,
        RejectReason::NotMember => 2,
        RejectReason::NotPrimary => 3,
        RejectReason::NeedOperatorResync => 4,
        RejectReason::BadAuth => 5,
    }
}

/// The payload of `f` (without the header).
fn payload(f: &Frame, o: &mut Vec<u8>) {
    match f {
        Frame::Hello(h) => {
            o.extend_from_slice(&h.cluster_id);
            put_bytes(o, &h.auth);
            o.extend_from_slice(&h.node_id);
            put_u16(o, h.shard);
            put_u16(o, h.partition);
            put_u32(o, h.fence_term);
            put_u32(o, h.last_term);
            put_u64(o, h.applied.0);
            o.push(recovered_code(h.recovered));
            o.push(u8::from(h.learner));
            o.push(u8::from(h.migration.is_some()));
            put_u64(o, h.migration.unwrap_or(0));
        }
        Frame::Accept { term, mode } => {
            put_u32(o, *term);
            let (m, s) = match mode {
                AcceptMode::Continue { from } => (1u8, *from),
                AcceptMode::Repair { at } => (2, *at),
                AcceptMode::Full { at } => (3, *at),
            };
            o.push(m);
            put_u64(o, s.0);
        }
        Frame::Reject(r) => o.push(reason_code(*r)),
        Frame::Groups(g) => o.extend_from_slice(g),
        Frame::Commit(s) | Frame::Heartbeat(s) => put_u64(o, s.0),
        Frame::Ack { durable, applied } => {
            put_u64(o, durable.0);
            put_u64(o, applied.0);
        }
        Frame::SnapBegin { slot } => put_u16(o, *slot),
        Frame::SnapRec(r) => {
            o.push(r.db);
            o.push(r.vtype);
            put_u64(o, r.expire_at);
            put_bytes(o, &r.key);
            put_bytes(o, &r.value);
        }
        Frame::SnapEnd { slot, count } => {
            put_u16(o, *slot);
            put_u64(o, *count);
        }
        Frame::SyncDone { at } => put_u64(o, at.0),
        Frame::RepairKeys(keys) => {
            put_u32(o, keys.len() as u32);
            for (db, k) in keys {
                o.push(*db);
                put_bytes(o, k);
            }
        }
        Frame::Image(i) => {
            o.push(i.db);
            put_bytes(o, &i.key);
            match &i.image {
                None => o.push(0),
                Some((vtype, expire_at, value)) => {
                    o.push(1);
                    o.push(*vtype);
                    put_u64(o, *expire_at);
                    put_bytes(o, value);
                }
            }
        }
        Frame::Seal { mig, last } => {
            put_u64(o, *mig);
            put_u64(o, last.0);
        }
        Frame::PutPart { seq, db, key, offset, total, data } => {
            put_u64(o, seq.0);
            o.push(*db);
            put_bytes(o, key);
            put_u64(o, *offset);
            put_u64(o, *total);
            put_bytes(o, data);
        }
    }
}

/// Appends one header + payload.
fn emit(code: u8, partition: u16, term: u32, body: &[u8], out: &mut Vec<u8>) {
    debug_assert!(body.len() <= FRAME_MAX_PAYLOAD);
    out.reserve(FRAME_HDR + body.len());
    put_u16(out, FRAME_MAGIC);
    out.push(FRAME_VERSION);
    out.push(code);
    put_u16(out, 0);
    put_u16(out, partition);
    put_u32(out, term);
    put_u32(out, body.len() as u32);
    put_u32(out, kv_common::crc::crc32c(body));
    put_u32(out, 0);
    out.extend_from_slice(body);
}

/// Appends `f` as one frame of `partition` stamped with the sender's `term`. A payload above
/// [`FRAME_MAX_PAYLOAD`] is split into `PUT_PART` fragments of the logical frame's encoding
/// (several frames are appended then).
pub fn encode(f: &Frame, partition: u16, term: u32, out: &mut Vec<u8>) {
    let mut body = Vec::new();
    payload(f, &mut body);
    if body.len() <= FRAME_MAX_PAYLOAD {
        emit(f.type_code(), partition, term, &body, out);
        return;
    }
    // the logical frame (header + payload) in fragments; `db` carries the inner frame type
    let mut logical = Vec::with_capacity(FRAME_HDR + body.len());
    emit_unchecked(f.type_code(), partition, term, &body, &mut logical);
    let seq = match f {
        Frame::Groups(g) => crate::codec::peek_seq(g).unwrap_or(Seq::ZERO),
        _ => Seq::ZERO,
    };
    let total = logical.len() as u64;
    let mut frag = Vec::with_capacity(PART_BYTES + 64);
    for (i, chunk) in logical.chunks(PART_BYTES).enumerate() {
        frag.clear();
        payload(
            &Frame::PutPart {
                seq,
                db: f.type_code(),
                key: Box::default(),
                offset: (i * PART_BYTES) as u64,
                total,
                data: chunk.to_vec(),
            },
            &mut frag,
        );
        emit(T_PUT_PART, partition, term, &frag, out);
    }
}

/// [`emit`] without the payload limit (the logical frame of a fragmented payload).
fn emit_unchecked(code: u8, partition: u16, term: u32, body: &[u8], out: &mut Vec<u8>) {
    put_u16(out, FRAME_MAGIC);
    out.push(FRAME_VERSION);
    out.push(code);
    put_u16(out, 0);
    put_u16(out, partition);
    put_u32(out, term);
    put_u32(out, body.len().min(u32::MAX as usize) as u32);
    put_u32(out, kv_common::crc::crc32c(body));
    put_u32(out, 0);
    out.extend_from_slice(body);
}

/// Incremental frame decoder for one stream.
pub struct FrameDecoder {
    /// Header of the frame being received, once complete.
    hdr: Option<[u8; FRAME_HDR]>,
}

/// A validated frame header.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Header {
    pub code: u8,
    pub flags: u16,
    pub partition: u16,
    pub term: u32,
    pub len: usize,
    pub crc: u32,
}

/// Parses and validates a header (`limit`: the largest payload accepted).
pub fn parse_header(h: &[u8; FRAME_HDR], limit: usize) -> Result<Header, FrameError> {
    let u16_at = |i: usize| u16::from_le_bytes([h[i], h[i + 1]]);
    let u32_at = |i: usize| u32::from_le_bytes([h[i], h[i + 1], h[i + 2], h[i + 3]]);
    if u16_at(0) != FRAME_MAGIC || h[2] != FRAME_VERSION || u32_at(20) != 0 {
        return Err(FrameError::BadMagic);
    }
    let len = u32_at(16) as usize;
    if len > limit {
        return Err(FrameError::TooLarge);
    }
    Ok(Header { code: h[3], flags: u16_at(4), partition: u16_at(6), term: u32_at(8), len, crc: u32_at(12) })
}

impl FrameDecoder {
    pub fn new() -> Self {
        FrameDecoder { hdr: None }
    }

    /// Decodes one frame from `buf`: `(partition, term, frame, consumed)`, or `Ok(None)` when
    /// more bytes are needed. `buf` must start where the previous call's `consumed` ended (or
    /// at the same place again after `Ok(None)`).
    pub fn feed(&mut self, buf: &[u8]) -> Result<Option<(u16, u32, Frame, usize)>, FrameError> {
        let Some(h) = buf.get(..FRAME_HDR) else { return Ok(None) };
        let mut hb = [0u8; FRAME_HDR];
        hb.copy_from_slice(h);
        let hdr = parse_header(&hb, FRAME_MAX_PAYLOAD)?;
        self.hdr = Some(hb);
        let end = FRAME_HDR + hdr.len;
        let Some(body) = buf.get(FRAME_HDR..end) else { return Ok(None) };
        self.hdr = None;
        if kv_common::crc::crc32c(body) != hdr.crc {
            return Err(FrameError::BadCrc);
        }
        let f = decode_payload(hdr.code, body).map_err(|_| FrameError::Malformed)?;
        Ok(Some((hdr.partition, hdr.term, f, end)))
    }

    /// Bytes still needed to complete the frame at the start of `buf` (0 when one is
    /// complete); used to size socket reads.
    pub fn want(&self, buf: &[u8]) -> usize {
        match buf.get(..FRAME_HDR) {
            None => FRAME_HDR - buf.len(),
            Some(h) => {
                let len = u32::from_le_bytes([h[16], h[17], h[18], h[19]]) as usize;
                (FRAME_HDR + len.min(FRAME_MAX_PAYLOAD)).saturating_sub(buf.len())
            }
        }
    }
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self::new()
    }
}

fn decode_payload(code: u8, b: &[u8]) -> Result<Frame, DecodeError> {
    let mut r = Rd::new(b);
    let f = match code {
        T_HELLO => {
            let mut cluster_id = [0u8; 16];
            cluster_id.copy_from_slice(r.take(16)?);
            let auth = r.bytes()?.to_vec();
            let mut node_id = [0u8; 20];
            node_id.copy_from_slice(r.take(20)?);
            let shard = r.u16()?;
            let partition = r.u16()?;
            let fence_term = r.u32()?;
            let last_term = r.u32()?;
            let applied = Seq(r.u64()?);
            let recovered = match r.u8()? {
                0 => Recovered::Clean,
                1 => Recovered::Scanned,
                2 => Recovered::SyncInterrupted,
                _ => return Err(DecodeError),
            };
            let learner = r.u8()? != 0;
            let has_mig = r.u8()? != 0;
            let mig = r.u64()?;
            Frame::Hello(Hello {
                cluster_id,
                auth,
                node_id,
                shard,
                partition,
                fence_term,
                last_term,
                applied,
                recovered,
                learner,
                migration: has_mig.then_some(mig),
            })
        }
        T_ACCEPT => {
            let term = r.u32()?;
            let m = r.u8()?;
            let s = Seq(r.u64()?);
            let mode = match m {
                1 => AcceptMode::Continue { from: s },
                2 => AcceptMode::Repair { at: s },
                3 => AcceptMode::Full { at: s },
                _ => return Err(DecodeError),
            };
            Frame::Accept { term, mode }
        }
        T_REJECT => Frame::Reject(match r.u8()? {
            1 => RejectReason::StaleTerm,
            2 => RejectReason::NotMember,
            3 => RejectReason::NotPrimary,
            4 => RejectReason::NeedOperatorResync,
            5 => RejectReason::BadAuth,
            _ => return Err(DecodeError),
        }),
        T_GROUPS => {
            // every group must be whole
            let mut at = 0;
            while at < b.len() {
                at += crate::codec::peek_len(&b[at..]).ok_or(DecodeError)?;
            }
            if at != b.len() {
                return Err(DecodeError);
            }
            return Ok(Frame::Groups(b.to_vec()));
        }
        T_COMMIT => Frame::Commit(Seq(r.u64()?)),
        T_HEARTBEAT => Frame::Heartbeat(Seq(r.u64()?)),
        T_ACK => {
            let durable = Seq(r.u64()?);
            Frame::Ack { durable, applied: Seq(r.u64()?) }
        }
        T_SNAP_BEGIN => Frame::SnapBegin { slot: r.u16()? },
        T_SNAP_REC => {
            let db = r.u8()?;
            let vtype = r.u8()?;
            let expire_at = r.u64()?;
            let key = r.bytes()?.into();
            let value = r.bytes()?.to_vec();
            Frame::SnapRec(SnapRec { db, vtype, expire_at, key, value })
        }
        T_SNAP_END => {
            let slot = r.u16()?;
            Frame::SnapEnd { slot, count: r.u64()? }
        }
        T_SYNC_DONE => Frame::SyncDone { at: Seq(r.u64()?) },
        T_REPAIR_KEYS => {
            let n = r.u32()? as usize;
            // each key needs at least 5 bytes: bound the allocation by the payload
            if n > b.len() / 5 {
                return Err(DecodeError);
            }
            let mut keys = Vec::with_capacity(n);
            for _ in 0..n {
                let db = r.u8()?;
                keys.push((db, r.bytes()?.into()));
            }
            Frame::RepairKeys(keys)
        }
        T_IMAGE => {
            let db = r.u8()?;
            let key = r.bytes()?.into();
            let image = match r.u8()? {
                0 => None,
                1 => {
                    let vtype = r.u8()?;
                    let expire_at = r.u64()?;
                    Some((vtype, expire_at, r.bytes()?.to_vec()))
                }
                _ => return Err(DecodeError),
            };
            Frame::Image(ImageRec { db, key, image })
        }
        T_SEAL => {
            let mig = r.u64()?;
            Frame::Seal { mig, last: Seq(r.u64()?) }
        }
        T_PUT_PART => {
            let seq = Seq(r.u64()?);
            let db = r.u8()?;
            let key = r.bytes()?.into();
            let offset = r.u64()?;
            let total = r.u64()?;
            let data = r.bytes()?.to_vec();
            Frame::PutPart { seq, db, key, offset, total, data }
        }
        _ => return Err(DecodeError),
    };
    if !r.done() {
        return Err(DecodeError);
    }
    Ok(f)
}

/// Rebuilds logical frames sent as `PUT_PART` fragments.
#[derive(Default)]
pub struct Reassembly {
    buf: Vec<u8>,
    total: u64,
}

impl Reassembly {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feeds one fragment; returns the logical frame `(partition, term, frame)` once complete.
    pub fn push(&mut self, offset: u64, total: u64, data: &[u8]) -> Result<Option<(u16, u32, Frame)>, FrameError> {
        if total as usize > MAX_REASSEMBLED || (total as usize) < FRAME_HDR {
            return Err(FrameError::TooLarge);
        }
        if offset == 0 {
            self.buf.clear();
            self.total = total;
        }
        if total != self.total || offset != self.buf.len() as u64 || self.buf.len() + data.len() > total as usize {
            self.buf.clear();
            return Err(FrameError::Malformed);
        }
        self.buf.extend_from_slice(data);
        if (self.buf.len() as u64) < total {
            return Ok(None);
        }
        let logical = std::mem::take(&mut self.buf);
        let mut hb = [0u8; FRAME_HDR];
        hb.copy_from_slice(&logical[..FRAME_HDR]);
        let hdr = parse_header(&hb, MAX_REASSEMBLED)?;
        let body = &logical[FRAME_HDR..];
        if body.len() != hdr.len {
            return Err(FrameError::Malformed);
        }
        if kv_common::crc::crc32c(body) != hdr.crc {
            return Err(FrameError::BadCrc);
        }
        if hdr.code == T_PUT_PART {
            return Err(FrameError::Malformed);
        }
        let f = decode_payload(hdr.code, body).map_err(|_| FrameError::Malformed)?;
        Ok(Some((hdr.partition, hdr.term, f)))
    }

    /// Bytes of a logical frame received so far.
    pub fn pending(&self) -> usize {
        self.buf.len()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FrameError {
    BadMagic,
    BadCrc,
    TooLarge,
    Malformed,
}

impl core::fmt::Display for FrameError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            FrameError::BadMagic => "bad replication frame magic or version",
            FrameError::BadCrc => "replication frame checksum mismatch",
            FrameError::TooLarge => "replication frame too large",
            FrameError::Malformed => "malformed replication frame",
        })
    }
}

impl std::error::Error for FrameError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Group, Op};
    use proptest::prelude::*;

    /// Encodes `f`, decodes it again (through fragments when needed) and re-encodes the result.
    fn round_trip(f: &Frame, partition: u16, term: u32) -> Frame {
        let mut b = Vec::new();
        encode(f, partition, term, &mut b);
        let mut d = FrameDecoder::new();
        let mut at = 0;
        let mut ra = Reassembly::new();
        loop {
            let (p, t, g, n) = d.feed(&b[at..]).unwrap().expect("complete");
            at += n;
            assert_eq!((p, t), (partition, term));
            match g {
                Frame::PutPart { offset, total, data, .. } => {
                    if let Some((p, t, g)) = ra.push(offset, total, &data).unwrap() {
                        assert_eq!((p, t), (partition, term));
                        assert_eq!(at, b.len());
                        return g;
                    }
                }
                g => {
                    assert_eq!(at, b.len());
                    return g;
                }
            }
        }
    }

    fn bytes_of(f: &Frame) -> Vec<u8> {
        let mut b = Vec::new();
        payload(f, &mut b);
        b
    }

    fn arb_frame() -> impl Strategy<Value = Frame> {
        let v = || proptest::collection::vec(any::<u8>(), 0..64);
        let seq = || any::<u64>().prop_map(Seq);
        prop_oneof![
            (any::<[u8; 16]>(), v(), any::<[u8; 20]>(), any::<(u16, u16, u32, u32)>(), seq(), 0u8..3, any::<bool>(), proptest::option::of(any::<u64>()))
                .prop_map(|(cluster_id, auth, node_id, (shard, partition, fence_term, last_term), applied, r, learner, migration)| {
                    Frame::Hello(Hello {
                        cluster_id,
                        auth,
                        node_id,
                        shard,
                        partition,
                        fence_term,
                        last_term,
                        applied,
                        recovered: [Recovered::Clean, Recovered::Scanned, Recovered::SyncInterrupted][usize::from(r)],
                        learner,
                        migration,
                    })
                }),
            (any::<u32>(), 0u8..3, seq()).prop_map(|(term, m, s)| Frame::Accept {
                term,
                mode: match m {
                    0 => AcceptMode::Continue { from: s },
                    1 => AcceptMode::Repair { at: s },
                    _ => AcceptMode::Full { at: s },
                }
            }),
            (0usize..5).prop_map(|i| Frame::Reject(
                [RejectReason::StaleTerm, RejectReason::NotMember, RejectReason::NotPrimary, RejectReason::NeedOperatorResync, RejectReason::BadAuth][i]
            )),
            (seq(), v(), v()).prop_map(|(s, k, val)| {
                let mut g = Vec::new();
                crate::codec::encode_group(&Group { seq: s, ops: vec![Op::Put { db: 0, vtype: 1, expire_at: 0, key: k.into(), value: val }] }, &mut g);
                Frame::Groups(g)
            }),
            seq().prop_map(Frame::Commit),
            seq().prop_map(Frame::Heartbeat),
            (seq(), seq()).prop_map(|(durable, applied)| Frame::Ack { durable, applied }),
            any::<u16>().prop_map(|slot| Frame::SnapBegin { slot }),
            (any::<u8>(), 1u8..3, any::<u64>(), v(), v())
                .prop_map(|(db, vtype, expire_at, k, value)| Frame::SnapRec(SnapRec { db, vtype, expire_at, key: k.into(), value })),
            (any::<u16>(), any::<u64>()).prop_map(|(slot, count)| Frame::SnapEnd { slot, count }),
            seq().prop_map(|at| Frame::SyncDone { at }),
            proptest::collection::vec((any::<u8>(), v()), 0..8)
                .prop_map(|ks| Frame::RepairKeys(ks.into_iter().map(|(d, k)| (d, k.into())).collect())),
            (any::<u8>(), v(), proptest::option::of((1u8..3, any::<u64>(), v())))
                .prop_map(|(db, k, image)| Frame::Image(ImageRec { db, key: k.into(), image })),
            (any::<u64>(), seq()).prop_map(|(mig, last)| Frame::Seal { mig, last }),
        ]
    }

    proptest! {
        #[test]
        fn frames_round_trip(f in arb_frame(), partition in any::<u16>(), term in any::<u32>()) {
            let g = round_trip(&f, partition, term);
            prop_assert_eq!(f.kind(), g.kind());
            prop_assert_eq!(bytes_of(&f), bytes_of(&g));
        }

        #[test]
        fn decoder_never_panics(b in proptest::collection::vec(any::<u8>(), 0..256)) {
            let mut d = FrameDecoder::new();
            let _ = d.feed(&b);
            let _ = d.want(&b);
        }

        #[test]
        fn split_feeds_wait_for_whole_frames(f in arb_frame(), cut in 0usize..400) {
            let mut b = Vec::new();
            encode(&f, 3, 9, &mut b);
            let cut = cut.min(b.len());
            let mut d = FrameDecoder::new();
            if cut < b.len() {
                prop_assert!(d.feed(&b[..cut]).unwrap().is_none());
                prop_assert!(d.want(&b[..cut]) > 0);
            }
            let (_, _, g, n) = d.feed(&b).unwrap().unwrap();
            prop_assert_eq!(n, b.len());
            prop_assert_eq!(bytes_of(&g), bytes_of(&f));
        }
    }

    #[test]
    fn header_layout_and_errors() {
        let mut b = Vec::new();
        encode(&Frame::Commit(Seq::new(2, 5)), 7, 3, &mut b);
        assert_eq!(b.len(), FRAME_HDR + 8);
        assert_eq!(&b[..2], &FRAME_MAGIC.to_le_bytes());
        assert_eq!(b[2], FRAME_VERSION);
        assert_eq!(u16::from_le_bytes([b[6], b[7]]), 7);
        assert_eq!(u32::from_le_bytes([b[8], b[9], b[10], b[11]]), 3);
        let mut d = FrameDecoder::new();
        let mut bad = b.clone();
        bad[0] ^= 1;
        assert_eq!(d.feed(&bad).err(), Some(FrameError::BadMagic));
        let mut bad = b.clone();
        *bad.last_mut().unwrap() ^= 1;
        assert_eq!(d.feed(&bad).err(), Some(FrameError::BadCrc));
        let mut bad = b.clone();
        bad[16..20].copy_from_slice(&((FRAME_MAX_PAYLOAD + 1) as u32).to_le_bytes());
        assert_eq!(d.feed(&bad).err(), Some(FrameError::TooLarge));
    }

    #[test]
    fn large_values_travel_in_fragments() {
        let value = (0..(9u32 << 20)).map(|i| (i % 251) as u8).collect::<Vec<u8>>();
        let f = Frame::SnapRec(SnapRec { db: 0, vtype: 1, expire_at: 5, key: b"big".as_slice().into(), value: value.clone() });
        let mut b = Vec::new();
        encode(&f, 1, 2, &mut b);
        // every physical frame respects the payload limit
        let mut d = FrameDecoder::new();
        let mut at = 0;
        let mut parts = 0;
        while at < b.len() {
            let (_, _, g, n) = d.feed(&b[at..]).unwrap().unwrap();
            assert!(n <= FRAME_HDR + FRAME_MAX_PAYLOAD);
            assert!(matches!(g, Frame::PutPart { .. }));
            parts += 1;
            at += n;
        }
        assert!(parts >= 3);
        let Frame::SnapRec(r) = round_trip(&f, 1, 2) else { panic!("snap rec") };
        assert_eq!(r.value, value);
        // a fragment out of order is refused
        let mut ra = Reassembly::new();
        assert_eq!(ra.push(5, 100, b"x").err(), Some(FrameError::Malformed));
    }
}
