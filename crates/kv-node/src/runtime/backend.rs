//! What a worker drives its partitions through (DESIGN §6.4, §6.5, §24.7).
//!
//! [`Backend`] is the worker's view of the storage engine plus the keyed command handlers: it
//! submits partition requests, executes ready ones ([`Work`]), runs SCAN steps, reports
//! durability tickets, and moves device I/O between the partitions and the ring. The
//! production implementation is [`PageBackend`] (the `PartitionStore`s of one worker, its page
//! cache, flush trackers and the meta client); runtime tests use an in-RAM implementation. Both
//! run the same handler code: every [`Work`] is interpreted by [`run_work`] over any
//! `cmd::Store`.
//!
//! The trait exists so that the event loop, routing, ordering and protocol machinery can be
//! exercised without devices; it is not a second storage engine (there is exactly one, the
//! page store, LC-03).

use std::os::fd::RawFd;

use kv_cmd::CmdId;
use kv_common::Now;
use kv_index::VType;
use kv_resp::ReplyWriter;
use kv_storage::cache::PageCache;
use kv_storage::effect::Op;
use kv_storage::flush::FlushTracker;
use kv_storage::io::{IoDone, IoReq};
use kv_storage::keyspace::{ApplyMode, ApplyOutcome, SharedValue, StoreError};
use kv_storage::layout::Seq;
use smallvec::SmallVec;
use kv_storage::metastore::{MetaClient, MetaDone, MetaReq};
use kv_storage::partition::{ExecStatus, PartState, PartitionCounters, PartitionStore, ReqId, Request, StorageCtx};
use kv_storage::recovery::{RecoveryMode, RecoveryProgress};
use kv_storage::ticket::{TicketId, TicketState};

use crate::cmd::{Args, Store};
use crate::dispatch::xtxn::{KeyRead, LockKey, ReadWant};
use crate::runtime::ring::DiskKind;
use crate::runtime::xmsg::{ExecEnv, OwnedCmd};

/// What an executed request runs.
#[derive(Clone, Copy)]
pub enum Work<'a> {
    /// One keyed command (its registered handler).
    Cmd(&'a OwnedCmd),
    /// An EXEC body whose keys share this partition (`cmd::txn::run_queued`).
    Multi { cmds: &'a [OwnedCmd], databases: u8 },
    /// The reads of a cross-partition transaction lock (§6.8 step 3).
    Reads(&'a [LockKey]),
    /// The staged writes of a cross-partition transaction (§6.8 step 4).
    Ops(&'a [Op]),
    /// Nothing: the request's kind does the work (FLUSHDB, barriers).
    Nothing,
    /// Replication, sync and migration work of the cluster glue (page store only).
    Repl(&'a ReplWork),
}

/// The result of a [`Work`] besides its reply bytes.
pub enum WorkOut {
    None,
    /// [`Work::Reads`]: one read per lock key.
    Reads(Vec<KeyRead>),
    /// [`Work::Repl`].
    Repl(ReplOut),
}

/// What a replication request does with the partition's keyspace (DESIGN §23.7, §23.13).
pub enum ReplWork {
    /// Replicated ops applied at `seq`: a member's groups (`Ordered`), repair images (`Image`),
    /// full-sync records (`SnapshotMerge`: identical records are confirmed, nothing written).
    Apply { ops: Vec<Op>, seq: Seq, mode: ApplyMode },
    /// Ops applied as this node's own writes (a migration destination's copy, slot purges):
    /// new seqs, replicated to this shard's members like any client write.
    Own { ops: Vec<Op> },
    /// The current image `(vtype, expire_at, value)` of each key (full-sync records, repair
    /// images, migration copies).
    Read { db: u8, keys: Vec<Box<[u8]>> },
    /// The stored version of each key (`Seq::ZERO` when absent).
    Seqs { db: u8, keys: Vec<Box<[u8]>> },
    /// Merge-sync prune: deletes each key whose stored version is below `below`, at `at`.
    Prune { db: u8, keys: Vec<Box<[u8]>>, below: Seq, at: Seq },
    /// A `SlotIter` walk step (run through the partition's key walk, not a keyspace closure).
    Walk,
}

/// The result of a [`ReplWork`].
pub enum ReplOut {
    None,
    Applied(SmallVec<[ApplyOutcome; 2]>),
    Images(Vec<Option<(u8, u64, Vec<u8>)>>),
    Seqs(Vec<Seq>),
    /// A walk step: the next fingerprint (`2^32` once the slot is done) and the keys listed.
    Listed { next: u64, keys: Vec<Box<[u8]>> },
}

/// Runs a [`ReplWork`] against the partition keyspace.
pub fn run_repl(ks: &mut kv_storage::keyspace::Keyspace<'_>, w: &ReplWork) -> Result<ReplOut, StoreError> {
    match w {
        ReplWork::Apply { ops, seq, mode } => {
            let mut out = SmallVec::new();
            for op in ops {
                out.push(ks.apply_replicated(op, *seq, *mode)?);
            }
            Ok(ReplOut::Applied(out))
        }
        ReplWork::Own { ops } => {
            for op in ops {
                match op {
                    Op::Put { db, vtype, expire_at, key, value } => {
                        ks.select(*db);
                        let vt = if *vtype == kv_storage::extent::EXT_VTYPE_HASH { VType::Hash } else { VType::String };
                        ks.put(key, value, vt, (*expire_at != 0).then_some(*expire_at))?;
                    }
                    Op::Del { db, key } | Op::Expired { db, key } => {
                        ks.select(*db);
                        ks.delete(key)?;
                    }
                    Op::Expire { db, key, expire_at } => {
                        ks.select(*db);
                        ks.set_expire(key, (*expire_at != 0).then_some(*expire_at))?;
                    }
                    Op::Append { db, key, data } => {
                        ks.select(*db);
                        ks.append(key, data)?;
                    }
                    Op::SetRange { db, key, offset, data } => {
                        ks.select(*db);
                        ks.set_range(key, *offset, data)?;
                    }
                    Op::FlushDb { .. } | Op::FlushSlot { .. } | Op::Noop => {}
                }
            }
            Ok(ReplOut::None)
        }
        ReplWork::Read { db, keys } => {
            ks.select(*db);
            let mut out = Vec::with_capacity(keys.len());
            for k in keys {
                out.push(match ks.get(k)? {
                    Some((m, v)) => {
                        let vtype = match m.vtype {
                            VType::String => kv_storage::extent::EXT_VTYPE_STRING,
                            VType::Hash => kv_storage::extent::EXT_VTYPE_HASH,
                        };
                        Some((vtype, m.expire_at_ms.unwrap_or(0), v.to_vec()))
                    }
                    None => None,
                });
            }
            Ok(ReplOut::Images(out))
        }
        ReplWork::Seqs { db, keys } => {
            ks.select(*db);
            let mut out = Vec::with_capacity(keys.len());
            for k in keys {
                out.push(ks.stored_seq(k)?);
            }
            Ok(ReplOut::Seqs(out))
        }
        ReplWork::Prune { db, keys, below, at } => {
            ks.select(*db);
            for k in keys {
                if ks.stored_seq(k)? < *below {
                    ks.apply_replicated(&Op::Del { db: *db, key: k.clone() }, *at, ApplyMode::Image)?;
                }
            }
            Ok(ReplOut::None)
        }
        ReplWork::Walk => Ok(ReplOut::None),
    }
}

/// The full state `(vtype, expire_at, value)` of a key after a request (`None`: absent).
pub type KeyImage = Option<(u8, u64, Vec<u8>)>;

/// The key a relative op (APPEND, SETRANGE, EXPIRE/PERSIST) changes: `(db, key)`.
pub fn relative_op_key(op: &Op) -> Option<(u8, &[u8])> {
    match op {
        Op::Append { db, key, .. } | Op::SetRange { db, key, .. } | Op::Expire { db, key, .. } => Some((*db, key)),
        _ => None,
    }
}

/// A [`Store`] that remembers the keys its relative changes (APPEND, SETRANGE, EXPIRE/PERSIST)
/// touched, so that their full state after the request can be read in the same execution
/// ([`read_images`]). A cluster primary sends that state, not the relative op, to a member
/// whose sync is in progress or to a migration destination: an APPEND applied to a copy that
/// the sync has not replaced yet would build a value the primary never held (DESIGN §23.7).
pub struct Imaging<'s, S: Store> {
    pub s: &'s mut S,
    pub on: bool,
    pub touched: SmallVec<[(u8, Box<[u8]>); 2]>,
}

impl<'s, S: Store> Imaging<'s, S> {
    pub fn new(s: &'s mut S, on: bool) -> Self {
        Imaging { s, on, touched: SmallVec::new() }
    }

    fn note(&mut self, key: &[u8]) {
        if self.on {
            let db = self.s.db();
            if !self.touched.iter().any(|(d, k)| *d == db && **k == *key) {
                self.touched.push((db, key.into()));
            }
        }
    }
}

impl<S: Store> Store for Imaging<'_, S> {
    #[inline]
    fn db(&self) -> u8 {
        self.s.db()
    }
    #[inline]
    fn select(&mut self, db: u8) {
        self.s.select(db)
    }
    #[inline]
    fn now_ms(&self) -> u64 {
        self.s.now_ms()
    }
    #[inline]
    fn meta(&mut self, key: &[u8]) -> Result<Option<kv_storage::keyspace::KeyMeta>, StoreError> {
        self.s.meta(key)
    }
    #[inline]
    fn get(&mut self, key: &[u8]) -> Result<Option<(kv_storage::keyspace::KeyMeta, kv_storage::keyspace::Value<'_>)>, StoreError> {
        self.s.get(key)
    }
    #[inline]
    fn put(&mut self, key: &[u8], value: &[u8], vtype: VType, expire_at_ms: Option<u64>) -> Result<(), StoreError> {
        self.s.put(key, value, vtype, expire_at_ms)
    }
    #[inline]
    fn append(&mut self, key: &[u8], data: &[u8]) -> Result<u64, StoreError> {
        let n = self.s.append(key, data)?;
        self.note(key);
        Ok(n)
    }
    #[inline]
    fn set_range(&mut self, key: &[u8], off: u64, data: &[u8]) -> Result<u64, StoreError> {
        let n = self.s.set_range(key, off, data)?;
        self.note(key);
        Ok(n)
    }
    #[inline]
    fn set_expire(&mut self, key: &[u8], expire_at_ms: Option<u64>) -> Result<bool, StoreError> {
        let r = self.s.set_expire(key, expire_at_ms)?;
        self.note(key);
        Ok(r)
    }
    #[inline]
    fn delete(&mut self, key: &[u8]) -> Result<bool, StoreError> {
        self.s.delete(key)
    }
}

/// Reads the state of each `(db, key)` after the request's changes (staged ones included) into
/// `out`; the selected database is restored.
pub fn read_images<S: Store>(s: &mut S, keys: &[(u8, Box<[u8]>)], out: &mut Vec<(u8, Box<[u8]>, KeyImage)>) -> Result<(), StoreError> {
    let db0 = s.db();
    for (db, k) in keys {
        if out.iter().any(|(d, kk, _)| d == db && kk == k) {
            continue;
        }
        s.select(*db);
        let img = match s.get(k)? {
            Some((m, v)) => {
                let vtype = match m.vtype {
                    VType::String => kv_storage::extent::EXT_VTYPE_STRING,
                    VType::Hash => kv_storage::extent::EXT_VTYPE_HASH,
                };
                Some((vtype, m.expire_at_ms.unwrap_or(0), v.to_vec()))
            }
            None => None,
        };
        out.push((*db, k.clone(), img));
    }
    s.select(db0);
    Ok(())
}

/// `g` with every relative op replaced by the full state of its key from `images` (a `Put`, or
/// a `Del` of a key absent afterwards); `None` when a relative op's key has no image.
pub fn with_full_images(g: &kv_storage::effect::Group, images: &[(u8, Box<[u8]>, KeyImage)]) -> Option<kv_storage::effect::Group> {
    let mut ops = Vec::with_capacity(g.ops.len());
    for op in &g.ops {
        match relative_op_key(op) {
            None => ops.push(op.clone()),
            Some((db, key)) => {
                let (_, _, img) = images.iter().find(|(d, k, _)| *d == db && **k == *key)?;
                ops.push(match img {
                    Some((vtype, expire_at, value)) => Op::Put {
                        db,
                        vtype: *vtype,
                        expire_at: *expire_at,
                        key: key.into(),
                        value: value.clone(),
                    },
                    None => Op::Del { db, key: key.into() },
                });
            }
        }
    }
    Some(kv_storage::effect::Group { seq: g.seq, ops })
}

/// Runs `work` against a store, writing its reply to `reply` (the caller truncates it before a
/// re-run). Keyed handlers come from `cmd::keyed`, so the production `Keyspace` and test
/// stores execute the same code.
pub fn run_work<S: Store>(s: &mut S, work: Work<'_>, env: &ExecEnv, reply: &mut Vec<u8>) -> Result<WorkOut, StoreError> {
    let mut w = ReplyWriter::new(reply, env.proto);
    match work {
        Work::Cmd(cmd) => {
            let args = Args { argv: cmd.argv() };
            match crate::cmd::keyed::<S>(cmd.id) {
                Some(f) => f(s, &args, &mut w, env).map(|()| WorkOut::None),
                None => {
                    w.error(b"ERR internal: command is not a keyed command");
                    Ok(WorkOut::None)
                }
            }
        }
        // the connection's database after the body is known at EXEC (its valid SELECTs)
        Work::Multi { cmds, databases } => crate::cmd::txn::run_queued(s, cmds, &mut w, env, databases).map(|_| WorkOut::None),
        Work::Reads(keys) => {
            let mut out = Vec::with_capacity(keys.len());
            for k in keys {
                s.select(k.db);
                let read = match k.read {
                    ReadWant::Nothing => KeyRead { exists: false, value: None },
                    ReadWant::Exists => KeyRead { exists: s.meta(&k.key)?.is_some(), value: None },
                    ReadWant::Value => match s.get(&k.key)? {
                        Some((m, v)) => {
                            let vtype = match m.vtype {
                                VType::String => kv_storage::extent::EXT_VTYPE_STRING,
                                VType::Hash => kv_storage::extent::EXT_VTYPE_HASH,
                            };
                            KeyRead { exists: true, value: Some((vtype, m.expire_at_ms, v.to_vec())) }
                        }
                        None => KeyRead { exists: false, value: None },
                    },
                };
                out.push(read);
            }
            Ok(WorkOut::Reads(out))
        }
        Work::Ops(ops) => {
            for op in ops {
                match op {
                    Op::Put { db, vtype, expire_at, key, value } => {
                        s.select(*db);
                        let vt = if *vtype == kv_storage::extent::EXT_VTYPE_HASH { VType::Hash } else { VType::String };
                        let exp = (*expire_at != 0).then_some(*expire_at);
                        s.put(key, value, vt, exp)?;
                    }
                    Op::Del { db, key } => {
                        s.select(*db);
                        s.delete(key)?;
                    }
                    _ => {}
                }
            }
            Ok(WorkOut::None)
        }
        Work::Nothing => Ok(WorkOut::None),
        // replication work runs only on the page store's keyspace (see `PageBackend::exec`)
        Work::Repl(_) => Err(StoreError::Io(-libc::ENOSYS)),
    }
}

/// The worker's storage surface for the partitions it owns (`p` is always one of them).
pub trait Backend {
    /// Partitions this worker owns.
    fn owned(&self) -> &[u16];
    fn state(&self, p: u16) -> PartState;
    fn submit(&mut self, p: u16, now: Now, req: Request) -> ReqId;
    fn take_ready(&mut self, p: u16, out: &mut Vec<ReqId>);
    fn exec(&mut self, p: u16, now: Now, id: ReqId, work: Work<'_>, env: &ExecEnv, reply: &mut Vec<u8>) -> ExecStatus<WorkOut>;
    /// One SCAN/KEYS/RANDOMKEY step of a submitted step request.
    fn scan(&mut self, p: u16, now: Now, id: ReqId, matcher: &dyn Fn(&[u8]) -> bool, out: &mut Vec<Box<[u8]>>) -> ExecStatus<u64>;
    fn take_tickets(&mut self, p: u16, out: &mut Vec<(TicketId, TicketState)>);
    fn on_meta(&mut self, p: u16, now: Now, done: MetaDone);
    /// Page writer and background work of every owned partition, then flush submission.
    fn tick(&mut self, now: Now);
    /// Drives recovery of `p` (§18).
    fn recover(&mut self, p: u16, now: Now, mode: RecoveryMode) -> RecoveryProgress;
    /// Device requests to submit, with the device's file descriptor.
    fn take_io(&mut self, out: &mut Vec<(RawFd, u8, IoReq)>);
    fn io_done(&mut self, now: Now, kind: DiskKind, dev: u8, done: IoDone);
    fn counters(&self, p: u16) -> &PartitionCounters;
    /// A meta request on behalf of the worker (cross-partition commits, §14.6).
    fn meta_request(&self, p: u16, token: u64, r: MetaReq);
    /// Whether background work is pending (arms the 1 ms timer, §6.4 step 7).
    fn busy(&self) -> bool;
    /// A fresh txn id for a cross-partition transaction, `None` when the engine cannot provide
    /// one.
    fn next_txn_id(&mut self) -> Option<u64>;
    /// Undoes partition `p`'s part of cross-partition txn `txn`, which its coordinator gave up
    /// (`PartitionStore::abort_txn`, §14.6 step 6): the part's keys show their previous
    /// versions at once, and a part not applied yet is refused when it runs. Returns the
    /// ticket that is `Durable` once nothing of the txn is left on media or in flight in `p`
    /// (`None`: nothing was staged).
    fn abort_txn(&mut self, _p: u16, _now: Now, _txn: u64) -> Option<TicketId> {
        None
    }
    /// The commit of cross-partition txn `txn` failed with `errno`
    /// (`PartitionStore::txn_commit_failed`): `p` turns read-only and fails what waits on it.
    fn txn_commit_failed(&mut self, _p: u16, _txn: u64, _errno: i32) {}
    /// How many multi-key txns partition `p` has opened (monotonic): a request whose exec
    /// moved it spent its txn credit (§14.6 step 0).
    fn txns_opened(&self, p: u16) -> u64;
    /// Bytes of staged records of this worker's partitions not yet in a page image (the
    /// `max-dirty-bytes` limit, §6.7).
    fn staged_bytes(&self) -> u64;
    /// Shutdown: hands every drained partition's clean-shutdown section to the snapshot
    /// writer (§10.10); `true` once every owned partition handed its section in (keep ticking
    /// until then).
    fn snapshot_sections(&mut self, now: Now) -> bool;
    /// Cluster role of partition `p`: a primary produces replication groups and starts `term`
    /// (seq counter restarts, §10.11); a member or idle partition produces none.
    fn set_cluster_role(&mut self, p: u16, primary: bool, term: u32);
    /// The seq the next write of `p` takes.
    fn next_seq(&self, p: u16) -> Seq;
    /// The node's key-hash seed (fingerprints of the walk position).
    fn hash_seed(&self, p: u16) -> u64;
    /// Index entries of `(db, slot)` in `p` (walks skip empty tables).
    fn slot_len(&self, p: u16, db: u8, slot: u16) -> usize;
    /// Registers one watcher of `(db, key)` with partition `p` (WATCH, §8.5).
    fn watch_key(&mut self, _p: u16, _db: u8, _key: &[u8]) {}
    /// Removes one watcher of `(db, key)` from partition `p`.
    fn unwatch_key(&mut self, _p: u16, _db: u8, _key: &[u8]) {}
    /// The watched keys of partition `p` changed since the last call (see
    /// `PartitionStore::take_touched`).
    fn take_touched(&mut self, _p: u16, _out: &mut Vec<(u8, Box<[u8]>)>) {}
    /// Whether the following `exec`s read the full state of every key a relative op changes
    /// (see [`Imaging`]); the images of the last completed `exec` are taken by `take_images`.
    fn set_imaging(&mut self, _on: bool) {}
    /// The key images the last `exec` read (empty unless imaging was on).
    fn take_images(&mut self, _out: &mut Vec<(u8, Box<[u8]>, KeyImage)>) {}
    /// The shared value parts of the last `exec`'s reply: when not empty, the reply bytes are a
    /// bulk header and the value follows in these parts (`ReplyBody::Ext`, GET of a value read
    /// from extents, sent without a copy).
    fn take_shared(&mut self) -> SmallVec<[SharedValue; 4]> {
        SmallVec::new()
    }
}

/// Storage resources of one worker for the page store.
pub struct PageBackend {
    owned: Vec<u16>,
    /// Indexed by partition id (`None` for partitions of other workers).
    parts: Vec<Option<PartitionStore>>,
    cache: PageCache,
    /// One per device.
    flush: Vec<FlushTracker>,
    meta: MetaClient,
    /// Device file descriptors, by device index.
    dev_fds: Vec<RawFd>,
    io: Vec<IoReq>,
    txn_ids: Option<std::sync::Arc<kv_storage::intent::TxnIds>>,
    /// Clean-shutdown sections go here (`None`: no snapshot is written).
    snapshot: Option<kv_storage::snapshot::SnapshotWriter>,
    /// Owned partitions whose section was handed in.
    sections_done: Vec<bool>,
    durable: Vec<kv_storage::flush::WriteId>,
    /// By partition id: something reached the partition since its last tick (a request, an
    /// I/O or meta completion, durable writes). Idle partitions with no background work are
    /// not ticked: their tick would find nothing to build.
    touched: Vec<bool>,
    /// `exec` reads the full state of keys relative ops change (`Backend::set_imaging`).
    imaging: bool,
    /// The key images of the last `exec`.
    images: Vec<(u8, Box<[u8]>, KeyImage)>,
    /// The shared value parts of the last `exec`'s reply (`Backend::take_shared`).
    shared: SmallVec<[SharedValue; 4]>,
}

impl PageBackend {
    /// `parts`: this worker's partitions (by id); `dev_fds`/`flush`: one per device.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        parts: Vec<(u16, PartitionStore)>,
        partitions: u16,
        cache: PageCache,
        flush: Vec<FlushTracker>,
        meta: MetaClient,
        dev_fds: Vec<RawFd>,
        txn_ids: Option<std::sync::Arc<kv_storage::intent::TxnIds>>,
        snapshot: Option<kv_storage::snapshot::SnapshotWriter>,
    ) -> Self {
        let mut slots: Vec<Option<PartitionStore>> = (0..partitions).map(|_| None).collect();
        let mut owned = Vec::with_capacity(parts.len());
        for (id, p) in parts {
            owned.push(id);
            slots[usize::from(id)] = Some(p);
        }
        let sections_done = vec![false; owned.len()];
        PageBackend {
            owned,
            parts: slots,
            cache,
            flush,
            meta,
            dev_fds,
            io: Vec::new(),
            txn_ids,
            snapshot,
            sections_done,
            durable: Vec::new(),
            touched: vec![true; usize::from(partitions)],
            imaging: false,
            images: Vec::new(),
            shared: SmallVec::new(),
        }
    }

    #[inline]
    fn split(&mut self, p: u16, now: Now) -> (&mut PartitionStore, StorageCtx<'_>) {
        self.touched[usize::from(p)] = true;
        let part = self.parts[usize::from(p)].as_mut().expect("partition owned by this worker");
        let cx = StorageCtx { cache: &mut self.cache, io: &mut self.io, flush: &mut self.flush, meta: &self.meta, now };
        (part, cx)
    }
}

impl Backend for PageBackend {
    fn owned(&self) -> &[u16] {
        &self.owned
    }

    fn watch_key(&mut self, p: u16, db: u8, key: &[u8]) {
        if let Some(part) = self.parts.get_mut(usize::from(p)).and_then(Option::as_mut) {
            part.watch_key(db, key);
        }
    }

    fn unwatch_key(&mut self, p: u16, db: u8, key: &[u8]) {
        if let Some(part) = self.parts.get_mut(usize::from(p)).and_then(Option::as_mut) {
            part.unwatch_key(db, key);
        }
    }

    fn take_touched(&mut self, p: u16, out: &mut Vec<(u8, Box<[u8]>)>) {
        if let Some(part) = self.parts.get_mut(usize::from(p)).and_then(Option::as_mut) {
            part.take_touched(out);
        }
    }

    fn state(&self, p: u16) -> PartState {
        self.parts[usize::from(p)].as_ref().map_or(PartState::Loading, PartitionStore::state)
    }

    fn submit(&mut self, p: u16, now: Now, req: Request) -> ReqId {
        let (part, mut cx) = self.split(p, now);
        part.submit(&mut cx, req)
    }

    fn take_ready(&mut self, p: u16, out: &mut Vec<ReqId>) {
        if let Some(part) = self.parts[usize::from(p)].as_mut() {
            part.take_ready(out);
        }
    }

    fn exec(&mut self, p: u16, now: Now, id: ReqId, work: Work<'_>, env: &ExecEnv, reply: &mut Vec<u8>) -> ExecStatus<WorkOut> {
        let base = reply.len();
        let imaging = self.imaging;
        let mut images = std::mem::take(&mut self.images);
        images.clear();
        let mut shared = std::mem::take(&mut self.shared);
        shared.clear();
        let (part, mut cx) = self.split(p, now);
        let status = part.exec(&mut cx, id, &mut |ks| {
            reply.truncate(base);
            images.clear();
            shared.clear();
            match work {
                Work::Cmd(c) if c.id == CmdId::Get && c.argc() == 2 && base == 0 => {
                    // GET: a value read from extents is sent from its read buffers
                    let mut w = ReplyWriter::new(reply, env.proto);
                    match ks.get_shared(c.arg(1))? {
                        None => w.null(),
                        Some((m, _, _)) if m.vtype != VType::String => w.error(crate::cmd::err::WRONGTYPE),
                        Some((m, v, parts)) => {
                            if parts.is_empty() {
                                crate::cmd::write_value(&mut w, &v);
                            } else {
                                w.bulk_len(m.len as usize);
                                shared.extend(parts);
                            }
                        }
                    }
                    Ok(WorkOut::None)
                }
                Work::Repl(w) => {
                    let out = run_repl(ks, w)?;
                    if imaging && let ReplWork::Own { ops } = w {
                        let keys: SmallVec<[(u8, Box<[u8]>); 2]> =
                            ops.iter().filter_map(relative_op_key).map(|(d, k)| (d, k.into())).collect();
                        read_images(ks, &keys, &mut images)?;
                    }
                    Ok(WorkOut::Repl(out))
                }
                _ => {
                    let mut im = Imaging::new(ks, imaging);
                    let out = run_work(&mut im, work, env, reply)?;
                    let touched = std::mem::take(&mut im.touched);
                    if !touched.is_empty() {
                        read_images(ks, &touched, &mut images)?;
                    }
                    Ok(out)
                }
            }
        });
        if !matches!(status, ExecStatus::Done(..)) {
            reply.truncate(base);
            images.clear();
            shared.clear();
        }
        self.images = images;
        self.shared = shared;
        status
    }

    fn set_imaging(&mut self, on: bool) {
        self.imaging = on;
    }

    fn take_images(&mut self, out: &mut Vec<(u8, Box<[u8]>, KeyImage)>) {
        out.append(&mut self.images);
    }

    fn take_shared(&mut self) -> SmallVec<[SharedValue; 4]> {
        std::mem::take(&mut self.shared)
    }

    fn scan(&mut self, p: u16, now: Now, id: ReqId, matcher: &dyn Fn(&[u8]) -> bool, out: &mut Vec<Box<[u8]>>) -> ExecStatus<u64> {
        let (part, mut cx) = self.split(p, now);
        part.scan_keys(&mut cx, id, matcher, out)
    }

    fn take_tickets(&mut self, p: u16, out: &mut Vec<(TicketId, TicketState)>) {
        if let Some(part) = self.parts[usize::from(p)].as_mut() {
            part.take_tickets(out);
        }
    }

    fn on_meta(&mut self, p: u16, now: Now, done: MetaDone) {
        if self.parts[usize::from(p)].is_some() {
            let (part, mut cx) = self.split(p, now);
            part.on_meta_done(&mut cx, done);
        }
    }

    fn tick(&mut self, now: Now) {
        for i in 0..self.owned.len() {
            let p = self.owned[i];
            let pi = usize::from(p);
            if !self.touched[pi] && !self.parts[pi].as_ref().is_some_and(PartitionStore::busy) {
                continue;
            }
            let (part, mut cx) = self.split(p, now);
            part.tick(&mut cx);
            self.touched[pi] = false;
        }
        for f in &mut self.flush {
            f.poll(&mut self.io);
        }
    }

    fn recover(&mut self, p: u16, now: Now, mode: RecoveryMode) -> RecoveryProgress {
        let (part, mut cx) = self.split(p, now);
        part.recover(&mut cx, mode)
    }

    fn take_io(&mut self, out: &mut Vec<(RawFd, u8, IoReq)>) {
        for r in self.io.drain(..) {
            let fd = self.dev_fds.get(usize::from(r.dev)).copied().unwrap_or(-1);
            out.push((fd, r.dev, r));
        }
    }

    fn io_done(&mut self, now: Now, kind: DiskKind, dev: u8, done: IoDone) {
        if kind == DiskKind::Flush {
            // a flush covers every write completed before it was submitted (§13.8); the
            // writes it made durable (or lost) belong to any partition of this worker
            let Some(tracker) = self.flush.get_mut(usize::from(dev)) else { return };
            match done.res {
                Ok(_) => {
                    self.durable.clear();
                    tracker.on_flush_done_into(&mut self.durable);
                    if self.durable.is_empty() {
                        return;
                    }
                    for i in 0..self.owned.len() {
                        let pi = usize::from(self.owned[i]);
                        if let Some(part) = self.parts[pi].as_mut() {
                            part.on_durable(&self.durable);
                            self.touched[pi] = true;
                        }
                    }
                }
                Err(errno) => {
                    tracing::error!(dev, errno = -errno, "device flush failed");
                    let lost = tracker.on_flush_failed();
                    for i in 0..self.owned.len() {
                        let pi = usize::from(self.owned[i]);
                        if let Some(part) = self.parts[pi].as_mut() {
                            part.on_flush_failed(&lost, errno);
                            self.touched[pi] = true;
                        }
                    }
                }
            }
            return;
        }
        let p = done.tag.partition;
        if self.parts.get(usize::from(p)).is_some_and(Option::is_some) {
            let (part, mut cx) = self.split(p, now);
            part.on_io_done(&mut cx, done);
        }
    }

    fn counters(&self, p: u16) -> &PartitionCounters {
        self.parts[usize::from(p)].as_ref().expect("owned partition").counters()
    }

    fn meta_request(&self, p: u16, token: u64, r: MetaReq) {
        self.meta.request(p, token, r);
    }

    fn busy(&self) -> bool {
        self.owned.iter().any(|&p| self.parts[usize::from(p)].as_ref().is_some_and(PartitionStore::busy))
    }

    fn next_txn_id(&mut self) -> Option<u64> {
        self.txn_ids.as_ref().and_then(|t| t.try_next())
    }

    fn abort_txn(&mut self, p: u16, now: Now, txn: u64) -> Option<TicketId> {
        self.parts[usize::from(p)].as_ref()?;
        let (part, mut cx) = self.split(p, now);
        part.abort_txn(&mut cx, txn)
    }

    fn txn_commit_failed(&mut self, p: u16, txn: u64, errno: i32) {
        self.touched[usize::from(p)] = true;
        if let Some(part) = self.parts[usize::from(p)].as_mut() {
            part.txn_commit_failed(txn, errno);
        }
    }

    fn txns_opened(&self, p: u16) -> u64 {
        // The engine reports no per-request txn flag (ExecOut has none): every opened txn
        // stages TXN-flagged records, which `page_writes_txn` counts once per txn at exec.
        self.parts[usize::from(p)].as_ref().map_or(0, |part| part.counters().page_writes_txn)
    }

    fn staged_bytes(&self) -> u64 {
        let cache = &self.cache;
        self.owned.iter().filter_map(|&p| self.parts[usize::from(p)].as_ref()).map(|part| part.staged_bytes(cache) as u64).sum()
    }

    fn snapshot_sections(&mut self, now: Now) -> bool {
        let Some(mut w) = self.snapshot.take() else { return true };
        let mut all = true;
        for i in 0..self.owned.len() {
            if self.sections_done[i] {
                continue;
            }
            let p = self.owned[i];
            let (part, mut cx) = self.split(p, now);
            if part.state() == PartState::Loading {
                // never recovered: no section, the next start takes the full scan
                self.sections_done[i] = true;
                continue;
            }
            self.sections_done[i] = part.snapshot_section(&mut cx, &mut w);
            all &= self.sections_done[i];
        }
        self.snapshot = Some(w);
        all
    }

    fn set_cluster_role(&mut self, p: u16, primary: bool, term: u32) {
        if let Some(part) = self.parts[usize::from(p)].as_mut() {
            if primary {
                part.set_term(term);
            }
            part.set_produce_groups(primary);
            self.touched[usize::from(p)] = true;
        }
    }

    fn next_seq(&self, p: u16) -> Seq {
        self.parts[usize::from(p)].as_ref().map_or(Seq::ZERO, PartitionStore::next_seq)
    }

    fn hash_seed(&self, p: u16) -> u64 {
        self.parts[usize::from(p)].as_ref().map_or(0, |part| part.cfg().hash_seed)
    }

    fn slot_len(&self, p: u16, db: u8, slot: u16) -> usize {
        self.parts[usize::from(p)].as_ref().and_then(|part| part.index().table(db, slot)).map_or(0, |t| t.len())
    }
}

/// The partitions of worker `w` among `partitions` (`{p : p % W == w}`, §5).
pub fn partitions_of(w: u16, workers: u16, partitions: u16) -> impl Iterator<Item = u16> {
    (0..partitions).filter(move |p| p % workers == w)
}

/// Whether `id` deletes keys (its reply waits for `Written` even in `async` mode, §14.7).
#[inline]
pub fn is_delete(id: CmdId) -> bool {
    id.spec().flags.contains(kv_cmd::CmdFlags::DELETE)
}

#[cfg(test)]
pub mod mem {
    //! An in-RAM backend for runtime tests: requests are ready at once (no I/O), keys live in a
    //! map per partition, tickets are not used (every effect is final at exec).

    use super::*;
    use kv_storage::effect::Group;
    use kv_storage::keyspace::{KeyMeta, Value};
    use kv_storage::partition::{ExecOut, ReqKind};

    #[derive(Clone)]
    struct Rec {
        vtype: VType,
        value: Vec<u8>,
        expire: Option<u64>,
    }

    /// One partition's keys and its staged view for one exec.
    #[derive(Default)]
    pub struct MemPart {
        data: hashbrown::HashMap<(u8, Vec<u8>), Rec>,
        reqs: hashbrown::HashMap<u32, Request>,
        ready: Vec<ReqId>,
        next: u32,
        counters: PartitionCounters,
    }

    /// The staged store handed to handlers: changes apply only when the handler succeeds.
    pub struct MemStore<'a> {
        base: &'a hashbrown::HashMap<(u8, Vec<u8>), Rec>,
        staged: hashbrown::HashMap<(u8, Vec<u8>), Option<Rec>>,
        db: u8,
        now: u64,
    }

    impl MemStore<'_> {
        /// Lazy expiry: an expired key touched by a command is deleted (as the page store does).
        fn reap(&mut self, key: &[u8]) {
            let k = (self.db, key.to_vec());
            if !self.staged.contains_key(&k) && self.base.get(&k).is_some_and(|r| r.expire.is_some_and(|e| e <= self.now)) {
                self.staged.insert(k, None);
            }
        }

        fn live(&self, key: &[u8]) -> Option<&Rec> {
            let k = (self.db, key.to_vec());
            let r = match self.staged.get(&k) {
                Some(s) => s.as_ref(),
                None => self.base.get(&k),
            };
            r.filter(|r| r.expire.is_none_or(|e| e > self.now))
        }
    }

    impl Store for MemStore<'_> {
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
            self.reap(key);
            Ok(self.live(key).map(|r| KeyMeta { vtype: r.vtype, expire_at_ms: r.expire, len: r.value.len() as u64 }))
        }
        fn get(&mut self, key: &[u8]) -> Result<Option<(KeyMeta, Value<'_>)>, StoreError> {
            self.reap(key);
            Ok(self.live(key).map(|r| {
                (KeyMeta { vtype: r.vtype, expire_at_ms: r.expire, len: r.value.len() as u64 }, Value::Contig(&r.value))
            }))
        }
        fn put(&mut self, key: &[u8], value: &[u8], vtype: VType, expire_at_ms: Option<u64>) -> Result<(), StoreError> {
            self.reap(key);
            self.staged.insert((self.db, key.to_vec()), Some(Rec { vtype, value: value.to_vec(), expire: expire_at_ms }));
            Ok(())
        }
        fn append(&mut self, key: &[u8], data: &[u8]) -> Result<u64, StoreError> {
            self.reap(key);
            let mut r = self.live(key).cloned().unwrap_or(Rec { vtype: VType::String, value: Vec::new(), expire: None });
            r.value.extend_from_slice(data);
            let n = r.value.len() as u64;
            self.staged.insert((self.db, key.to_vec()), Some(r));
            Ok(n)
        }
        fn set_range(&mut self, key: &[u8], off: u64, data: &[u8]) -> Result<u64, StoreError> {
            self.reap(key);
            let mut r = self.live(key).cloned().unwrap_or(Rec { vtype: VType::String, value: Vec::new(), expire: None });
            let end = off as usize + data.len();
            if r.value.len() < end {
                r.value.resize(end, 0);
            }
            r.value[off as usize..end].copy_from_slice(data);
            let n = r.value.len() as u64;
            self.staged.insert((self.db, key.to_vec()), Some(r));
            Ok(n)
        }
        fn set_expire(&mut self, key: &[u8], expire_at_ms: Option<u64>) -> Result<bool, StoreError> {
            self.reap(key);
            match self.live(key).cloned() {
                Some(mut r) => {
                    r.expire = expire_at_ms;
                    self.staged.insert((self.db, key.to_vec()), Some(r));
                    Ok(true)
                }
                None => Ok(false),
            }
        }
        fn delete(&mut self, key: &[u8]) -> Result<bool, StoreError> {
            self.reap(key);
            let existed = self.live(key).is_some();
            self.staged.insert((self.db, key.to_vec()), None);
            Ok(existed)
        }
    }

    /// Where the in-RAM backend's meta requests go (cross-partition commits).
    pub type MetaSink = Box<dyn Fn(u16, u64, MetaReq) + Send>;

    /// The in-RAM backend of one worker.
    pub struct MemBackend {
        owned: Vec<u16>,
        parts: Vec<Option<MemPart>>,
        next_txn: u64,
        meta_sink: Option<MetaSink>,
    }

    impl MemBackend {
        pub fn new(w: u16, workers: u16, partitions: u16) -> Self {
            let owned: Vec<u16> = partitions_of(w, workers, partitions).collect();
            let mut parts: Vec<Option<MemPart>> = (0..partitions).map(|_| None).collect();
            for &p in &owned {
                parts[usize::from(p)] = Some(MemPart::default());
            }
            MemBackend { owned, parts, next_txn: 1, meta_sink: None }
        }

        /// Routes meta requests (cross-partition commits) to `f`.
        pub fn with_meta(mut self, f: MetaSink) -> Self {
            self.meta_sink = Some(f);
            self
        }

        fn part(&mut self, p: u16) -> &mut MemPart {
            self.parts[usize::from(p)].as_mut().expect("owned")
        }

        fn recount(part: &mut MemPart, now: u64) {
            let mut c = PartitionCounters::default();
            for ((db, _), r) in &part.data {
                let _ = now;
                c.keys[usize::from(*db)] += 1;
                if r.expire.is_some() {
                    c.expires[usize::from(*db)] += 1;
                }
            }
            part.counters = c;
        }
    }

    fn done<R>(r: R) -> ExecStatus<R> {
        ExecStatus::Done(r, ExecOut::new(None, None::<Group>, None))
    }

    impl Backend for MemBackend {
        fn owned(&self) -> &[u16] {
            &self.owned
        }
        fn state(&self, _p: u16) -> PartState {
            PartState::Ready
        }
        fn submit(&mut self, p: u16, _now: Now, req: Request) -> ReqId {
            let part = self.part(p);
            let id = part.next;
            part.next += 1;
            part.reqs.insert(id, req);
            part.ready.push(ReqId(id));
            ReqId(id)
        }
        fn take_ready(&mut self, p: u16, out: &mut Vec<ReqId>) {
            out.append(&mut self.part(p).ready);
        }
        fn exec(&mut self, p: u16, now: Now, id: ReqId, work: Work<'_>, env: &ExecEnv, reply: &mut Vec<u8>) -> ExecStatus<WorkOut> {
            let part = self.part(p);
            let Some(req) = part.reqs.remove(&id.0) else { return ExecStatus::Failed(StoreError::Io(-libc::ENOENT)) };
            if let ReqKind::FlushDb { db } = req.kind {
                part.data.retain(|(d, _), _| db.is_some_and(|x| x != *d));
                Self::recount(part, now.unix_ms);
                return done(WorkOut::None);
            }
            let base = reply.len();
            let mut st = MemStore { base: &part.data, staged: hashbrown::HashMap::new(), db: req.db, now: now.unix_ms };
            match run_work(&mut st, work, env, reply) {
                Ok(out) => {
                    let staged = std::mem::take(&mut st.staged);
                    for (k, v) in staged {
                        match v {
                            Some(r) => {
                                part.data.insert(k, r);
                            }
                            None => {
                                part.data.remove(&k);
                            }
                        }
                    }
                    Self::recount(part, now.unix_ms);
                    done(out)
                }
                Err(e) => {
                    reply.truncate(base);
                    ExecStatus::Failed(e)
                }
            }
        }
        fn scan(&mut self, p: u16, now: Now, id: ReqId, matcher: &dyn Fn(&[u8]) -> bool, out: &mut Vec<Box<[u8]>>) -> ExecStatus<u64> {
            let partitions = self.parts.len() as u16;
            let part = self.part(p);
            let Some(req) = part.reqs.remove(&id.0) else { return ExecStatus::Failed(StoreError::Io(-libc::ENOENT)) };
            let live = |r: &Rec| r.expire.is_none_or(|e| e > now.unix_ms);
            match req.kind {
                ReqKind::ScanStep { db, vtype, .. } => {
                    for ((d, k), r) in &part.data {
                        if *d == db && live(r) && vtype.is_none_or(|t| t == r.vtype) && matcher(k) {
                            out.push(k.clone().into_boxed_slice());
                        }
                    }
                    // one step covers the whole partition
                    let next = if p + 1 >= partitions { u64::from(partitions) << 48 } else { u64::from(p + 1) << 48 };
                    done(next)
                }
                ReqKind::RandomKey { db, r } => {
                    let keys: Vec<&Vec<u8>> = part.data.iter().filter(|((d, _), rec)| *d == db && live(rec)).map(|((_, k), _)| k).collect();
                    if !keys.is_empty() {
                        out.push(keys[(r % keys.len() as u64) as usize].clone().into_boxed_slice());
                    }
                    done(0)
                }
                _ => ExecStatus::Failed(StoreError::Io(-libc::EINVAL)),
            }
        }
        fn take_tickets(&mut self, _p: u16, _out: &mut Vec<(TicketId, TicketState)>) {}
        fn on_meta(&mut self, _p: u16, _now: Now, _done: MetaDone) {}
        fn tick(&mut self, now: Now) {
            // active expiry: keys past their TTL go even when nothing touches them
            for part in self.parts.iter_mut().flatten() {
                let before = part.data.len();
                part.data.retain(|_, r| r.expire.is_none_or(|e| e > now.unix_ms));
                if part.data.len() != before {
                    Self::recount(part, now.unix_ms);
                }
            }
        }
        fn recover(&mut self, _p: u16, _now: Now, _mode: RecoveryMode) -> RecoveryProgress {
            RecoveryProgress::Done { bytes_scanned: 0 }
        }
        fn take_io(&mut self, _out: &mut Vec<(RawFd, u8, IoReq)>) {}
        fn io_done(&mut self, _now: Now, _kind: DiskKind, _dev: u8, _done: IoDone) {}
        fn counters(&self, p: u16) -> &PartitionCounters {
            &self.parts[usize::from(p)].as_ref().expect("owned").counters
        }
        fn meta_request(&self, p: u16, token: u64, r: MetaReq) {
            if let Some(f) = &self.meta_sink {
                f(p, token, r);
            }
        }
        fn busy(&self) -> bool {
            // keys with a TTL need the periodic tick of active expiry
            self.parts.iter().flatten().any(|p| p.data.values().any(|r| r.expire.is_some()))
        }
        fn next_txn_id(&mut self) -> Option<u64> {
            self.next_txn += 1;
            Some(self.next_txn)
        }
        fn txns_opened(&self, _p: u16) -> u64 {
            // every effect is final at exec: no txn is ever left open
            0
        }
        fn staged_bytes(&self) -> u64 {
            0
        }
        fn snapshot_sections(&mut self, _now: Now) -> bool {
            true
        }
        fn set_cluster_role(&mut self, _p: u16, _primary: bool, _term: u32) {}
        fn next_seq(&self, _p: u16) -> Seq {
            Seq::ZERO
        }
        fn hash_seed(&self, _p: u16) -> u64 {
            0
        }
        fn slot_len(&self, _p: u16, _db: u8, _slot: u16) -> usize {
            0
        }
    }
}

#[cfg(test)]
mod imaging_tests {
    use super::*;
    use kv_storage::effect::Group;

    #[test]
    fn relative_ops_become_full_state() {
        let g = Group {
            seq: Seq::new(3, 9),
            ops: vec![
                Op::Append { db: 0, key: b"a".as_slice().into(), data: b"x".to_vec() },
                Op::Put { db: 0, vtype: 1, expire_at: 0, key: b"p".as_slice().into(), value: b"v".to_vec() },
                Op::Expire { db: 1, key: b"e".as_slice().into(), expire_at: 77 },
                Op::SetRange { db: 0, key: b"gone".as_slice().into(), offset: 2, data: b"y".to_vec() },
            ],
        };
        let images: Vec<(u8, Box<[u8]>, KeyImage)> = vec![
            (0, b"a".as_slice().into(), Some((1, 5, b"abcx".to_vec()))),
            (1, b"e".as_slice().into(), Some((2, 77, b"h".to_vec()))),
            (0, b"gone".as_slice().into(), None),
        ];
        let f = with_full_images(&g, &images).expect("every relative op has an image");
        assert_eq!(f.seq, g.seq);
        assert_eq!(
            f.ops,
            vec![
                Op::Put { db: 0, vtype: 1, expire_at: 5, key: b"a".as_slice().into(), value: b"abcx".to_vec() },
                g.ops[1].clone(),
                Op::Put { db: 1, vtype: 2, expire_at: 77, key: b"e".as_slice().into(), value: b"h".to_vec() },
                Op::Del { db: 0, key: b"gone".as_slice().into() },
            ]
        );
        // a relative op without an image (of its db) cannot be converted
        assert!(with_full_images(&g, &images[1..]).is_none());
        let wrong_db: Vec<(u8, Box<[u8]>, KeyImage)> =
            images.iter().map(|(d, k, i)| (d ^ 1, k.clone(), i.clone())).collect();
        assert!(with_full_images(&g, &wrong_db).is_none());
    }

    #[test]
    fn the_imaging_store_reads_the_state_after_the_request() {
        let mut m = crate::cmd::testkit::Model::default();
        let mut im = Imaging::new(&mut m, true);
        im.put(b"k", b"ab", VType::String, None).unwrap();
        im.append(b"k", b"cd").unwrap();
        im.select(2);
        im.put(b"t", b"v", VType::String, None).unwrap();
        im.set_expire(b"t", Some(u64::MAX / 2)).unwrap();
        im.set_expire(b"t", Some(u64::MAX / 2)).unwrap();
        let touched = std::mem::take(&mut im.touched);
        assert_eq!(touched.len(), 2, "each key once");
        let mut out = Vec::new();
        read_images(&mut m, &touched, &mut out).unwrap();
        assert_eq!(out[0], (0, b"k".as_slice().into(), Some((kv_storage::extent::EXT_VTYPE_STRING, 0, b"abcd".to_vec()))));
        assert_eq!(out[1].0, 2);
        assert_eq!(out[1].2.as_ref().map(|i| i.1), Some(u64::MAX / 2));
        // off: nothing is remembered
        let mut im = Imaging::new(&mut m, false);
        im.append(b"k", b"e").unwrap();
        assert!(im.touched.is_empty());
    }
}
