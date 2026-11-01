//! SCAN, KEYS and RANDOMKEY driving (DESIGN §16.6, LC-17, LC-18).
//!
//! These commands are registered as `Handler::Scan`: the runtime runs them, with the command
//! side (argument rules, step requests, KEYS walk, replies) from `cmd::scan`. The connection's
//! home worker sends one step at a time ([`ScanReq`]) to the worker owning the step's
//! partition, which runs it with `PartitionStore::scan_keys` on that one thread and answers with
//! the keys and the next cursor ([`ScanOutcome`]). SCAN is one step; KEYS runs steps over all
//! partitions strictly one after another, at most one step in flight per call (test
//! `keys_sequential`); RANDOMKEY is one step on one partition at a time. No SCAN-family request
//! is fanned out across workers, and only key names travel (MATCH and TYPE are fixed built-in
//! filters).

use kv_storage::keyspace::StoreError;
use kv_storage::partition::Request;

/// What a step is for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StepKind {
    Scan,
    Keys,
    RandomKey,
}

/// One step on one partition (key bytes only; holds a user pattern: no `Debug`, LC-32).
pub struct ScanReq {
    pub kind: StepKind,
    pub partition: u16,
    /// The partition request of the step (`ReqKind::ScanStep` / `ReqKind::RandomKey`).
    pub request: Request,
    /// MATCH pattern; `None` = every key.
    pub pattern: Option<Box<[u8]>>,
}

/// The answer to a step: the next cursor and the keys found.
pub struct ScanOutcome {
    pub res: Result<(u64, Vec<Box<[u8]>>), StoreError>,
}

/// The state of a SCAN-family call on its home worker.
pub enum ScanCall {
    Scan,
    Keys { walk: crate::cmd::scan::KeysWalk, keys: Vec<Box<[u8]>>, pattern: Option<Box<[u8]>> },
    RandomKey { db: u8, attempts: u32 },
}

/// The step's key filter (MATCH with Redis's `*` rule).
pub fn matcher(pattern: Option<&[u8]>) -> impl Fn(&[u8]) -> bool + '_ {
    crate::cmd::scan::key_matcher(pattern)
}
