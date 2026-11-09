//! The storage surface that keyed command handlers run against (DESIGN §6.5, §24.7).
//!
//! Handlers are written once, generically over [`Store`], and registered for the partition's
//! [`Keyspace`] (see `cmd::handler`). The trait mirrors the command-facing `Keyspace` methods
//! one to one, so the production instantiation is a thin, inlined delegation; tests and the
//! simulator can drive the very same handler code against a reference model.
//!
//! Contract (the `Keyspace` contract of §24.7): every mutating call is staged and committed
//! only if the handler returns `Ok`; a handler may therefore be run again from the start after
//! `Err(StoreError::NeedMore)`. Lookups treat a key whose `expire_at <= now_ms` as absent (lazy
//! expiry, §16.4) and verify the full key bytes (LC-07). `put` replaces a key of any type.

use kv_index::VType;
use kv_storage::keyspace::{KeyMeta, Keyspace, StoreError, Value};

/// Staged, command-scoped access to one partition's keys (the [`Keyspace`] surface).
pub trait Store {
    /// The selected database.
    fn db(&self) -> u8;
    /// Selects the database for the following calls (COPY ... DB n).
    fn select(&mut self, db: u8);
    /// Wall-clock time of the request (all TTL decisions of one command use it).
    fn now_ms(&self) -> u64;
    /// Header facts of a live key (meta access, §8.1).
    fn meta(&mut self, key: &[u8]) -> Result<Option<KeyMeta>, StoreError>;
    /// Header facts and value of a live key, of any type.
    fn get(&mut self, key: &[u8]) -> Result<Option<(KeyMeta, Value<'_>)>, StoreError>;
    /// Creates or replaces `key` (any previous type) with the given value and expiry.
    fn put(
        &mut self,
        key: &[u8],
        value: &[u8],
        vtype: VType,
        expire_at_ms: Option<u64>,
    ) -> Result<(), StoreError>;
    /// Appends to an existing string, keeping its expiry; returns the new length.
    fn append(&mut self, key: &[u8], data: &[u8]) -> Result<u64, StoreError>;
    /// Overwrites `data` at `off` of an existing string (zero padding), keeping its expiry;
    /// returns the new length.
    fn set_range(&mut self, key: &[u8], off: u64, data: &[u8]) -> Result<u64, StoreError>;
    /// Sets (`Some`) or removes (`None`) the expiry; returns whether the key exists.
    fn set_expire(&mut self, key: &[u8], expire_at_ms: Option<u64>) -> Result<bool, StoreError>;
    /// Deletes the key; returns whether it existed.
    fn delete(&mut self, key: &[u8]) -> Result<bool, StoreError>;
}

impl Store for Keyspace<'_> {
    #[inline]
    fn db(&self) -> u8 {
        Keyspace::db(self)
    }

    #[inline]
    fn select(&mut self, db: u8) {
        Keyspace::select(self, db)
    }

    #[inline]
    fn now_ms(&self) -> u64 {
        Keyspace::now_ms(self)
    }

    #[inline]
    fn meta(&mut self, key: &[u8]) -> Result<Option<KeyMeta>, StoreError> {
        Keyspace::meta(self, key)
    }

    #[inline]
    fn get(&mut self, key: &[u8]) -> Result<Option<(KeyMeta, Value<'_>)>, StoreError> {
        Keyspace::get(self, key)
    }

    #[inline]
    fn put(
        &mut self,
        key: &[u8],
        value: &[u8],
        vtype: VType,
        expire_at_ms: Option<u64>,
    ) -> Result<(), StoreError> {
        Keyspace::put(self, key, value, vtype, expire_at_ms)
    }

    #[inline]
    fn append(&mut self, key: &[u8], data: &[u8]) -> Result<u64, StoreError> {
        Keyspace::append(self, key, data)
    }

    #[inline]
    fn set_range(&mut self, key: &[u8], off: u64, data: &[u8]) -> Result<u64, StoreError> {
        Keyspace::set_range(self, key, off, data)
    }

    #[inline]
    fn set_expire(&mut self, key: &[u8], expire_at_ms: Option<u64>) -> Result<bool, StoreError> {
        Keyspace::set_expire(self, key, expire_at_ms)
    }

    #[inline]
    fn delete(&mut self, key: &[u8]) -> Result<bool, StoreError> {
        Keyspace::delete(self, key)
    }
}

/// Total length of a value.
#[inline]
pub(crate) fn value_len(v: &Value<'_>) -> usize {
    match v {
        Value::Contig(b) => b.len(),
        Value::Chunks(cs) => cs.iter().map(|c| c.len()).sum(),
    }
}

/// The value as one slice when it is contiguous (every small record and VALUE extent).
#[inline]
pub(crate) fn contig<'v>(v: &'v Value<'_>) -> Option<&'v [u8]> {
    match v {
        Value::Contig(b) => Some(b),
        Value::Chunks(cs) if cs.len() == 1 => Some(cs[0]),
        Value::Chunks(_) => None,
    }
}

/// Copies bytes `[start, end)` of the value into `out` (no intermediate buffer).
pub(crate) fn copy_range(v: &Value<'_>, start: usize, end: usize, out: &mut Vec<u8>) {
    match v {
        Value::Contig(b) => out.extend_from_slice(&b[start..end]),
        Value::Chunks(cs) => {
            let mut base = 0usize;
            for c in cs {
                let (lo, hi) = (base, base + c.len());
                base = hi;
                if hi <= start {
                    continue;
                }
                if lo >= end {
                    break;
                }
                out.extend_from_slice(&c[start.max(lo) - lo..end.min(hi) - lo]);
            }
        }
    }
}

/// The whole value as an owned buffer (RENAME/COPY payloads, hash decoding of chunked values).
pub(crate) fn to_vec(v: &Value<'_>) -> Vec<u8> {
    let mut out = Vec::with_capacity(value_len(v));
    copy_range(v, 0, value_len(v), &mut out);
    out
}
