//! The command-facing storage API (DESIGN §6.5, §14, §15, §16, §23.7).
//!
//! A [`Keyspace`] is handed to a command's `exec` closure for one partition request. Every
//! method is staged and committed only if `exec` returns `Ok`, so commands are re-runnable when
//! `exec` needs a page that was not prefetched (`StoreError::NeedMore`, §6.5). Lookups verify the
//! full key against the bytes read from the page or extent (LC-07); a key whose
//! `expire_at <= now_ms` is treated as absent (lazy expiry, §16.4). Members apply replicated
//! effects through [`Keyspace::apply_replicated`]: `Ordered` skips an op whose key already has a
//! stored seq >= the op's seq; `Image` installs a repair image; `SnapshotMerge` confirms an
//! identical record (sets its `sync_mark`, writes nothing) or writes the differing one (§23.7).

use core::marker::PhantomData;

use kv_index::VType;
use smallvec::SmallVec;

use crate::effect::Op;
use crate::layout::{PageId, Seq};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StoreError {
    /// A page that was not prefetched is needed; the partition loads it and re-runs `exec`.
    NeedMore,
    /// `-OOM` at the fill limit (§11.6).
    OutOfSpace,
    /// Value beyond 512 MiB (or `hash-max-bytes`).
    TooLarge,
    WrongType,
    /// `-ERR storage corruption detected` (§15); names the page only.
    Corrupt(PageId),
    /// `-ERR storage I/O error` (§22).
    Io(i32),
    /// The partition is read-only after a write error (§22).
    ReadOnly,
    /// `-LOADING` (§18.1).
    Loading,
}

/// Header-level facts about a key.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct KeyMeta {
    pub vtype: VType,
    pub expire_at_ms: Option<u64>,
    pub len: u64,
}

/// A value: contiguous (small record or VALUE extent) or chunked (CHUNKMAP). Holds user bytes:
/// no `Debug` (LC-32).
pub enum Value<'a> {
    Contig(&'a [u8]),
    Chunks(SmallVec<[&'a [u8]; 4]>),
}

impl<'a> Value<'a> {
    pub fn len(&self) -> u64 {
        match self {
            Value::Contig(b) => b.len() as u64,
            Value::Chunks(cs) => cs.iter().map(|c| c.len() as u64).sum(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn to_vec(&self) -> Vec<u8> {
        match self {
            Value::Contig(b) => b.to_vec(),
            Value::Chunks(cs) => cs.concat(),
        }
    }

    /// Bytes `[start, end)` of the value (clamped by the caller to the value length).
    pub fn slice(&self, start: u64, end: u64) -> Vec<u8> {
        todo!("DESIGN §8.3: value range")
    }
}

/// How a replicated op is applied on a member.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ApplyMode {
    /// skip if stored seq >= op seq
    Ordered,
    Image,
    /// §23.7: identical -> mark only
    SnapshotMerge,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ApplyOutcome {
    Applied,
    Skipped,
    /// identical record kept, sync_mark set, nothing written
    Confirmed,
}

/// Staged access to one partition for one request.
pub struct Keyspace<'p> {
    db: u8,
    now_ms: u64,
    part: PhantomData<&'p mut ()>,
}

impl<'p> Keyspace<'p> {
    pub fn db(&self) -> u8 {
        self.db
    }

    pub fn select(&mut self, db: u8) {
        self.db = db;
    }

    /// Wall-clock time of the request (TTL decisions).
    pub fn now_ms(&self) -> u64 {
        self.now_ms
    }

    pub fn meta(&mut self, key: &[u8]) -> Result<Option<KeyMeta>, StoreError> {
        todo!("DESIGN §15: meta access")
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<(KeyMeta, Value<'_>)>, StoreError> {
        todo!("DESIGN §15: read access")
    }

    pub fn put(&mut self, key: &[u8], value: &[u8], vtype: VType, expire_at_ms: Option<u64>) -> Result<(), StoreError> {
        todo!("DESIGN §14.1: staged put")
    }

    /// Returns the new length.
    pub fn append(&mut self, key: &[u8], data: &[u8]) -> Result<u64, StoreError> {
        todo!("DESIGN §14.2, §14.5: append")
    }

    /// Returns the new length.
    pub fn set_range(&mut self, key: &[u8], off: u64, data: &[u8]) -> Result<u64, StoreError> {
        todo!("DESIGN §14.2, §14.5: set range")
    }

    /// Sets (`Some`) or removes (`None`) the expiry; returns whether the key exists.
    pub fn set_expire(&mut self, key: &[u8], expire_at_ms: Option<u64>) -> Result<bool, StoreError> {
        todo!("DESIGN §16.3: set expiry")
    }

    /// Returns whether the key existed.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool, StoreError> {
        todo!("DESIGN §16.1: delete")
    }

    /// member / image / snapshot
    pub fn apply_replicated(&mut self, op: &Op, seq: Seq, mode: ApplyMode) -> Result<ApplyOutcome, StoreError> {
        todo!("DESIGN §23.7: apply replicated op")
    }
}
