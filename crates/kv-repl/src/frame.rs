//! Replication frames and handshake (DESIGN §23.7, §23.9, §23.13).
//!
//! Frame header (24 B): `u16 magic 0x524B ("KR")`, `u8 version`, `u8 type`, `u16 flags`,
//! `u16 partition`, `u32 term`, `u32 payload_len (<= 8 MiB)`, `u32 crc32c(payload)`, `u32 zero`.
//! Handshake: member -> `Hello`; primary -> `Accept { term, mode }` or `Reject(reason)`. Both
//! sides reject peers whose term is below their fence. Values above 4 MiB travel as `PutPart`
//! fragments. Types holding user bytes or the auth token have no `Debug` (LC-32).

use crate::Seq;

/// `"KR"`
pub const FRAME_MAGIC: u16 = 0x524B;
pub const FRAME_VERSION: u8 = 1;
pub const FRAME_HDR: usize = 24;
pub const FRAME_MAX_PAYLOAD: usize = 8 << 20;

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

/// Appends `f` as one frame of `partition` stamped with the sender's `term`.
pub fn encode(f: &Frame, partition: u16, term: u32, out: &mut Vec<u8>) {
    todo!("DESIGN §23.7: frame encode")
}

/// Incremental frame decoder for one stream.
pub struct FrameDecoder {
    /// Header of the frame being received, once complete.
    hdr: Option<[u8; FRAME_HDR]>,
}

impl FrameDecoder {
    pub fn new() -> Self {
        FrameDecoder { hdr: None }
    }

    /// Decodes one frame from `buf`: `(partition, term, frame, consumed)`, or `Ok(None)` when
    /// more bytes are needed.
    pub fn feed(&mut self, buf: &[u8]) -> Result<Option<(u16, u32, Frame, usize)>, FrameError> {
        todo!("DESIGN §23.7: frame decode")
    }
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FrameError {
    BadMagic,
    BadCrc,
    TooLarge,
    Malformed,
}
