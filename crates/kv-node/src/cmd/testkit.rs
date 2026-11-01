//! Test support for the command handlers: a reference model of the `Keyspace` contract and
//! helpers that run one command and return its raw reply bytes.
//!
//! The model keeps committed state in a map and applies a command's staged changes only when
//! the handler returns `Ok` (a failed handler leaves the state as it was, like the partition's
//! staged effects). It treats expired keys as absent on every access (lazy expiry) but keeps
//! them in the map until touched, so a "DBSIZE" over the model counts them as Redis does. With
//! `chunk` set it hands values out as several chunks, exercising the CHUNKMAP code paths of
//! handlers.

use kv_index::VType;
use kv_resp::{Proto, ReplyWriter};
use kv_storage::keyspace::{KeyMeta, StoreError, Value};
use smallvec::SmallVec;

use super::store::Store;
use super::{Args, GenericKeyedFn};
use crate::runtime::xmsg::ExecEnv;

/// One stored record (holds user bytes: no `Debug`, LC-32).
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Rec {
    pub vtype: VType,
    pub value: Vec<u8>,
    pub expire: Option<u64>,
}

#[derive(Clone, Default)]
pub(crate) struct Model {
    pub db: u8,
    pub now: u64,
    pub data: hashbrown::HashMap<(u8, Vec<u8>), Rec>,
    /// Hand values out in chunks of this many bytes (0 = contiguous).
    pub chunk: usize,
    /// Number of mutating calls (to assert that failed or no-op commands write nothing).
    pub writes: u64,
}

impl Model {
    pub fn new(now: u64) -> Self {
        Model {
            now,
            ..Default::default()
        }
    }

    fn live(&mut self, key: &[u8]) -> Option<&mut Rec> {
        let k = (self.db, key.to_vec());
        let now = self.now;
        if self
            .data
            .get(&k)
            .is_some_and(|r| r.expire.is_some_and(|e| e <= now))
        {
            self.data.remove(&k);
        }
        self.data.get_mut(&k)
    }

    /// Raw record (no expiry filtering).
    pub fn rec(&self, db: u8, key: &[u8]) -> Option<&Rec> {
        self.data.get(&(db, key.to_vec()))
    }

    pub fn set_str(&mut self, key: &[u8], value: &[u8], expire: Option<u64>) {
        self.data.insert(
            (self.db, key.to_vec()),
            Rec {
                vtype: VType::String,
                value: value.to_vec(),
                expire,
            },
        );
    }

    /// Keys of db (including logically expired ones, like DBSIZE).
    pub fn dbsize(&self, db: u8) -> usize {
        self.data.keys().filter(|(d, _)| *d == db).count()
    }

    /// Runs one keyed handler with staging semantics; returns the reply bytes.
    pub fn run(&mut self, f: GenericKeyedFn<Model>, argv: &[&[u8]], proto: Proto) -> Vec<u8> {
        let saved = (self.data.clone(), self.db, self.writes);
        let args = Args {
            argv: argv.iter().copied().collect(),
        };
        let env = ExecEnv {
            proto,
            db: self.db,
            now_ms: self.now,
            readonly_conn: false,
            conn_id: 1,
        };
        let mut buf = Vec::new();
        let r = f(self, &args, &mut ReplyWriter::new(&mut buf, proto), &env);
        match r {
            Ok(()) => buf,
            Err(e) => {
                (self.data, self.db, self.writes) = saved;
                let mut out = Vec::new();
                super::write_store_error(e, &mut ReplyWriter::new(&mut out, proto));
                out
            }
        }
    }
}

impl Store for Model {
    fn db(&self) -> u8 {
        self.db
    }

    fn select(&mut self, db: u8) {
        self.db = db;
    }

    fn now_ms(&self) -> u64 {
        self.now
    }

    fn meta(&mut self, key: &[u8]) -> Result<Option<KeyMeta>, StoreError> {
        Ok(self.live(key).map(|r| KeyMeta {
            vtype: r.vtype,
            expire_at_ms: r.expire,
            len: r.value.len() as u64,
        }))
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<(KeyMeta, Value<'_>)>, StoreError> {
        let chunk = self.chunk;
        Ok(self.live(key).map(|r| {
            let m = KeyMeta {
                vtype: r.vtype,
                expire_at_ms: r.expire,
                len: r.value.len() as u64,
            };
            let v = if chunk == 0 {
                Value::Contig(&r.value)
            } else {
                Value::Chunks(r.value.chunks(chunk).collect::<SmallVec<_>>())
            };
            (m, v)
        }))
    }

    fn put(
        &mut self,
        key: &[u8],
        value: &[u8],
        vtype: VType,
        expire_at_ms: Option<u64>,
    ) -> Result<(), StoreError> {
        if value.len() > 512 << 20 {
            return Err(StoreError::TooLarge);
        }
        self.writes += 1;
        self.data.insert(
            (self.db, key.to_vec()),
            Rec {
                vtype,
                value: value.to_vec(),
                expire: expire_at_ms,
            },
        );
        Ok(())
    }

    fn append(&mut self, key: &[u8], data: &[u8]) -> Result<u64, StoreError> {
        self.writes += 1;
        match self.live(key) {
            Some(r) if r.vtype != VType::String => Err(StoreError::WrongType),
            Some(r) => {
                r.value.extend_from_slice(data);
                Ok(r.value.len() as u64)
            }
            None => {
                self.put(key, data, VType::String, None)?;
                Ok(data.len() as u64)
            }
        }
    }

    fn set_range(&mut self, key: &[u8], off: u64, data: &[u8]) -> Result<u64, StoreError> {
        self.writes += 1;
        let off = off as usize;
        let r = match self.live(key) {
            Some(r) if r.vtype != VType::String => return Err(StoreError::WrongType),
            Some(r) => r,
            None => {
                self.put(key, b"", VType::String, None)?;
                self.live(key).expect("just created")
            }
        };
        if r.value.len() < off + data.len() {
            r.value.resize(off + data.len(), 0);
        }
        r.value[off..off + data.len()].copy_from_slice(data);
        Ok(r.value.len() as u64)
    }

    fn set_expire(&mut self, key: &[u8], expire_at_ms: Option<u64>) -> Result<bool, StoreError> {
        self.writes += 1;
        Ok(match self.live(key) {
            Some(r) => {
                r.expire = expire_at_ms;
                true
            }
            None => false,
        })
    }

    fn delete(&mut self, key: &[u8]) -> Result<bool, StoreError> {
        self.writes += 1;
        let existed = self.live(key).is_some();
        self.data.remove(&(self.db, key.to_vec()));
        Ok(existed)
    }
}

/// Splits a whitespace-separated command line into arguments (no quoting; tests only).
pub(crate) fn words(line: &str) -> Vec<&[u8]> {
    line.split_ascii_whitespace().map(str::as_bytes).collect()
}

/// Runs `line` through the generic keyed registry against `m`; returns the reply bytes.
pub(crate) fn run(m: &mut Model, line: &str) -> Vec<u8> {
    run_argv(m, &words(line), Proto::Resp2)
}

pub(crate) fn run3(m: &mut Model, line: &str) -> Vec<u8> {
    run_argv(m, &words(line), Proto::Resp3)
}

pub(crate) fn run_argv(m: &mut Model, argv: &[&[u8]], proto: Proto) -> Vec<u8> {
    let spec = match kv_cmd::resolve(argv) {
        Ok(spec) => spec,
        Err(e) => {
            let mut out = Vec::new();
            e.write(argv, &mut ReplyWriter::new(&mut out, proto));
            return out;
        }
    };
    let f = super::keyed::<Model>(spec.id)
        .unwrap_or_else(|| panic!("{} is not a keyed command", spec.name));
    m.run(f, argv, proto)
}

/// Reply bytes as a printable string (for assertion messages).
pub(crate) fn show(b: &[u8]) -> String {
    String::from_utf8_lossy(b).replace("\r\n", "\\r\\n")
}

/// Asserts that `line` replies exactly `want` (RESP2).
#[track_caller]
pub(crate) fn check(m: &mut Model, line: &str, want: &str) {
    let got = run(m, line);
    assert_eq!(show(&got), show(want.as_bytes()), "reply to `{line}`");
}

/// Asserts that `line` replies exactly `want` in RESP3.
#[track_caller]
pub(crate) fn check3(m: &mut Model, line: &str, want: &str) {
    let got = run3(m, line);
    assert_eq!(show(&got), show(want.as_bytes()), "RESP3 reply to `{line}`");
}
