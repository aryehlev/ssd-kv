//! Replication-visible effects (DESIGN §23.7).
//!
//! One [`Group`] per client command (or EXEC), stamped with the command's seq; its [`Op`]s carry
//! full payloads, never digests (LC-29). Non-deterministic or float commands are shipped as
//! effects (INCRBYFLOAT -> `Put` of the result; EXPIRE -> `Expire` with an absolute time; SET EX
//! -> `Put` with an absolute time). `kv-repl` encodes groups; members apply them through
//! `Keyspace::apply_replicated`. `Debug` goes through `Redacted` (LC-32).

use core::fmt;

use kv_common::Redacted;

use crate::layout::Seq;

#[derive(Clone, PartialEq, Eq)]
pub enum Op {
    Put { db: u8, vtype: u8, expire_at: u64, key: Box<[u8]>, value: Vec<u8> },
    Del { db: u8, key: Box<[u8]> },
    Expired { db: u8, key: Box<[u8]> },
    /// `expire_at` 0 = persist.
    Expire { db: u8, key: Box<[u8]>, expire_at: u64 },
    Append { db: u8, key: Box<[u8]>, data: Vec<u8> },
    SetRange { db: u8, key: Box<[u8]>, offset: u64, data: Vec<u8> },
    /// `None` = all databases (0xFF on the wire).
    FlushDb { db: Option<u8> },
    FlushSlot { slot: u16 },
    Noop,
}

/// Debug for Op/Group/Pending/SnapRec/ImageRec goes through Redacted (LC-32)
#[derive(Clone, PartialEq, Eq)]
pub struct Group {
    pub seq: Seq,
    pub ops: Vec<Op>,
}

impl fmt::Debug for Op {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Op::Put { db, vtype, expire_at, key, value } => f
                .debug_struct("Put")
                .field("db", db)
                .field("vtype", vtype)
                .field("expire_at", expire_at)
                .field("key", &Redacted(key))
                .field("value", &Redacted(value))
                .finish(),
            Op::Del { db, key } => f.debug_struct("Del").field("db", db).field("key", &Redacted(key)).finish(),
            Op::Expired { db, key } => f.debug_struct("Expired").field("db", db).field("key", &Redacted(key)).finish(),
            Op::Expire { db, key, expire_at } => f
                .debug_struct("Expire")
                .field("db", db)
                .field("key", &Redacted(key))
                .field("expire_at", expire_at)
                .finish(),
            Op::Append { db, key, data } => f
                .debug_struct("Append")
                .field("db", db)
                .field("key", &Redacted(key))
                .field("data", &Redacted(data))
                .finish(),
            Op::SetRange { db, key, offset, data } => f
                .debug_struct("SetRange")
                .field("db", db)
                .field("key", &Redacted(key))
                .field("offset", offset)
                .field("data", &Redacted(data))
                .finish(),
            Op::FlushDb { db } => f.debug_struct("FlushDb").field("db", db).finish(),
            Op::FlushSlot { slot } => f.debug_struct("FlushSlot").field("slot", slot).finish(),
            Op::Noop => f.write_str("Noop"),
        }
    }
}

impl fmt::Debug for Group {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Group").field("seq", &self.seq).field("ops", &self.ops).finish()
    }
}
