//! Standalone cross-partition transactions (DESIGN §6.8, §14.6, LC-37).
//!
//! 1. The coordinating worker (the one that received the command) takes the node-global
//!    `XTxnGate` (a conflict lock, not load control): one worker's batch of XTxns (those
//!    waiting in its gate queue at its turn, started in queue order) enters the partitions'
//!    lock order at a time, and the worker gives the gate up once every part of the batch
//!    answered `Registered` (its `Lock` holds its keys or waits in FIFO order behind what
//!    conflicts with it). Every
//!    partition then orders any two conflicting transactions the same way (the order in which
//!    they held the gate), so none waits for another that waits for it, and transactions on
//!    disjoint keys read, write and commit concurrently (the meta thread batches their
//!    commits). Holding the gate to the end serialized every cross-partition write of the
//!    node (about 1.8 ms each): a few hundred concurrent MSETs waited for seconds. The txn
//!    credit was reserved before the command was consumed (§6.7).
//! 2. `Lock { keys }` (key bytes only) to each involved partition: the transaction holds those
//!    keys (requests touching one of them, keyless requests, and what queues behind them wait
//!    in FIFO order; requests on other keys go on), the partition prefetches the keys and
//!    replies `Locked`.
//! 3. `Read`: values and metas the command needs (MSETNX existence, RENAME source value).
//! 4. A txn id is taken and `Apply { txn_id, ops }` is sent; partitions stage TXN-flagged
//!    records and report when their writes are durable.
//! 5. When every part is durable the commit is requested from the meta thread; `Commit` is sent,
//!    partitions unlock (queued requests resume in FIFO order) and the reply is released.
//!
//! Cluster mode never uses XTxn (cross-slot -> `-CROSSSLOT`). No message carries a key hash.
//!
//! Implementation notes:
//! * The gate never blocks a thread (a worker must only ever block in `io_uring_enter`): it is
//!   a FIFO ticket gate. A command takes a ticket when it is consumed and proceeds when the
//!   gate serves its ticket; until then it waits in its worker's gate queue (a conflict wait,
//!   like an earlier command on the same key, LC-15) and nothing else on the worker stops.
//!   Releasing serves the next ticket and rings every parked worker.
//! * The read of step 3 travels with the lock: a partition answers `Locked` with the values
//!   the command needs, which saves one round trip per part.
//! * [`XTxnCoord`] is the coordinator's state machine; it is pure (no I/O, no clocks) and the
//!   worker carries out the [`Step`]s it returns.

use std::sync::atomic::{AtomicU64, Ordering};

use kv_cmd::{Access, CmdId};
use kv_storage::effect::Op;
use kv_storage::keyspace::StoreError;
use smallvec::SmallVec;

use crate::runtime::xmsg::{ExecEnv, OwnedCmd};

/// The node-global conflict gate: a FIFO ticket gate taken without blocking.
pub struct XTxnGate {
    next: AtomicU64,
    serving: AtomicU64,
}

impl XTxnGate {
    pub fn new() -> Self {
        XTxnGate { next: AtomicU64::new(0), serving: AtomicU64::new(0) }
    }

    /// Takes the next ticket (when a cross-partition write is consumed).
    pub fn ticket(&self) -> u64 {
        self.next.fetch_add(1, Ordering::AcqRel)
    }

    /// Whether ticket `t` holds the gate now.
    #[inline]
    pub fn is_turn(&self, t: u64) -> bool {
        self.serving.load(Ordering::Acquire) == t
    }

    /// Releases the gate held by ticket `t` (serves `t + 1`). The caller then wakes the
    /// workers, one of which holds the next ticket.
    pub fn release(&self, t: u64) {
        let prev = self.serving.swap(t + 1, Ordering::AcqRel);
        debug_assert_eq!(prev, t, "the gate is released by its holder only");
    }
}

impl Default for XTxnGate {
    fn default() -> Self {
        Self::new()
    }
}

/// What a partition reads for the coordinator while it locks.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReadWant {
    /// Nothing (blind write).
    Nothing,
    /// Whether the key exists (MSETNX, RENAMENX / COPY destination).
    Exists,
    /// The whole value with its type and expiry (RENAME / COPY source).
    Value,
}

/// A key read under the lock (holds user bytes: no `Debug`, LC-32).
pub struct KeyRead {
    pub exists: bool,
    /// `(vtype, expire_at_ms, value)` when `ReadWant::Value` was asked and the key exists
    /// (`vtype` in the storage encoding of `Op::Put`: 1 string, 2 hash).
    pub value: Option<(u8, Option<u64>, Vec<u8>)>,
}

/// One key a partition locks for a transaction.
pub struct LockKey {
    pub db: u8,
    pub key: Box<[u8]>,
    pub access: Access,
    pub read: ReadWant,
}

pub enum XTxnMsg {
    /// Lock the partition for transaction `xtxn` of worker `from` and read what `keys` ask.
    Lock { xtxn: u64, from: u16, partition: u16, db: u8, keys: SmallVec<[LockKey; 4]> },
    /// The partition took the `Lock` in its order of transactions (its keys are held, or it
    /// waits behind what holds or waits for them): from now on no later transaction gets
    /// ahead of this one on those keys.
    Registered { xtxn: u64 },
    /// The partition is locked; one read per key of the `Lock`, in order.
    Locked { xtxn: u64, partition: u16, reads: Result<Vec<KeyRead>, StoreError> },
    /// Stage `ops` as TXN-flagged records of `txn_id`.
    Apply { xtxn: u64, txn_id: u64, partition: u16, ops: Vec<Op> },
    /// The partition's TXN-flagged writes are durable (or failed).
    Applied { xtxn: u64, partition: u16, res: Result<(), StoreError> },
    /// `txn_id` is committed: resolve and unlock.
    Commit { xtxn: u64, txn_id: u64, partition: u16 },
    /// Unlock without applying (the command failed or wrote nothing).
    Unlock { xtxn: u64, partition: u16 },
    /// The transaction was given up after `Apply` was sent (another part failed, §14.6 step
    /// 6): undo what the part staged (or refuse its `Apply` if it has not run), then unlock.
    Abort { xtxn: u64, txn_id: u64, partition: u16 },
    /// The commit of `txn_id` failed (`errno`): it may or may not have reached media, so the
    /// part is neither committed nor undone; the partition turns read-only and fails what
    /// waits on it, then unlocks.
    CommitFailed { xtxn: u64, txn_id: u64, partition: u16, errno: i32 },
    /// Nothing of the aborted part is left on media or in flight in `partition` (`ok`), or
    /// its undo failed (an I/O error: some of it may be left).
    Aborted { xtxn: u64, partition: u16, ok: bool },
}

/// The transaction kinds (multi-key writes whose keys span partitions).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    Mset,
    Msetnx,
    Rename { nx: bool },
    Copy { dst_db: u8, replace: bool },
}

/// One involved partition.
struct Part {
    partition: u16,
    db: u8,
    /// `(argument index of the key, read wanted)` in lock order.
    keys: SmallVec<[(u16, Access, ReadWant); 4]>,
    reads: Option<Vec<KeyRead>>,
    applied: bool,
    has_ops: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Phase {
    Locking,
    Applying,
    Committing,
    Done,
}

/// What the coordinating worker does next.
pub enum Step {
    /// Send these messages (`(partition, message)`; the worker routes by owner).
    Send(Vec<(u16, XTxnMsg)>),
    /// Take a txn id, then call [`XTxnCoord::apply`].
    NeedTxnId,
    /// Ask the meta thread to commit `txn_id`; call [`XTxnCoord::on_committed`] when done.
    Commit { txn_id: u64 },
    /// Send the messages, then deliver `reply` and release the gate: the transaction is over.
    Finish { msgs: Vec<(u16, XTxnMsg)>, reply: Vec<u8> },
    /// Like `Finish`, for a transaction given up after its parts were sent `Apply`: `msgs`
    /// carries an `Abort` to each of the `parts` such parts. Once every one answered `Aborted`
    /// the worker reports `TxnAborted { txn_id }` to the meta thread, whose resolution returns
    /// the txn credit (a txn id left unresolved stops `resolved_w`, and with it the return of
    /// every later credit).
    FinishAborted { msgs: Vec<(u16, XTxnMsg)>, reply: Vec<u8>, txn_id: u64, parts: usize },
    /// Nothing to do until the next message.
    Wait,
}

/// The coordinator side of one cross-partition transaction.
pub struct XTxnCoord {
    pub xtxn: u64,
    /// Unused: the gate is taken per batch of a worker (`ExecState::gate_ticket`).
    pub ticket: u64,
    pub cmd: OwnedCmd,
    pub env: ExecEnv,
    kind: Kind,
    parts: SmallVec<[Part; 4]>,
    phase: Phase,
    txn_id: u64,
    /// The reply decided at apply time (sent once committed).
    reply: Vec<u8>,
}

/// Whether `id` is a multi-key write that becomes an XTxn when its keys span partitions.
pub fn is_xtxn_command(id: CmdId) -> bool {
    matches!(id, CmdId::Mset | CmdId::Msetnx | CmdId::Rename | CmdId::Renamenx | CmdId::Copy)
}

/// COPY's destination db and REPLACE flag (arguments were validated by `cmd::precheck`).
fn copy_options(argv: &[&[u8]], db: u8) -> (u8, bool) {
    let mut dst_db = db;
    let mut replace = false;
    let mut i = 3;
    while i < argv.len() {
        if argv[i].eq_ignore_ascii_case(b"REPLACE") {
            replace = true;
            i += 1;
        } else if argv[i].eq_ignore_ascii_case(b"DB") && i + 1 < argv.len() {
            dst_db = kv_common::num::string2ll(argv[i + 1]).and_then(|v| u8::try_from(v).ok()).unwrap_or(db);
            i += 2;
        } else {
            i += 1;
        }
    }
    (dst_db, replace)
}

impl XTxnCoord {
    /// Plans the transaction of `cmd`; `partition_of` maps a key to its partition. Returns
    /// `None` when `cmd` is not an XTxn command.
    pub fn new(xtxn: u64, ticket: u64, cmd: OwnedCmd, env: ExecEnv, partition_of: impl Fn(&[u8]) -> u16) -> Option<Self> {
        let argv = cmd.argv();
        let kind = match cmd.id {
            CmdId::Mset => Kind::Mset,
            CmdId::Msetnx => Kind::Msetnx,
            CmdId::Rename => Kind::Rename { nx: false },
            CmdId::Renamenx => Kind::Rename { nx: true },
            CmdId::Copy => {
                let (dst_db, replace) = copy_options(&argv, env.db);
                Kind::Copy { dst_db, replace }
            }
            _ => return None,
        };
        let mut keys: SmallVec<[(u16, u8, Access, ReadWant); 8]> = SmallVec::new();
        match kind {
            Kind::Mset | Kind::Msetnx => {
                let read = if kind == Kind::Msetnx { ReadWant::Exists } else { ReadWant::Nothing };
                let mut i = 1;
                while i + 1 < argv.len() {
                    keys.push((i as u16, env.db, Access::Write, read));
                    i += 2;
                }
            }
            Kind::Rename { nx } => {
                keys.push((1, env.db, Access::ReadWrite, ReadWant::Value));
                keys.push((2, env.db, Access::Write, if nx { ReadWant::Exists } else { ReadWant::Nothing }));
            }
            Kind::Copy { dst_db, replace } => {
                keys.push((1, env.db, Access::Read, ReadWant::Value));
                keys.push((2, dst_db, Access::Write, if replace { ReadWant::Nothing } else { ReadWant::Exists }));
            }
        }
        let mut parts: SmallVec<[Part; 4]> = SmallVec::new();
        for (i, db, access, read) in keys {
            let p = partition_of(argv[usize::from(i)]);
            match parts.iter_mut().find(|x| x.partition == p && x.db == db) {
                Some(part) => part.keys.push((i, access, read)),
                None => parts.push(Part {
                    partition: p,
                    db,
                    keys: smallvec::smallvec![(i, access, read)],
                    reads: None,
                    applied: false,
                    has_ops: false,
                }),
            }
        }
        drop(argv);
        Some(XTxnCoord { xtxn, ticket, cmd, env, kind, parts, phase: Phase::Locking, txn_id: 0, reply: Vec::new() })
    }

    /// Distinct partitions involved (the fan-out width, LC-37).
    pub fn partitions(&self) -> impl Iterator<Item = u16> + '_ {
        self.parts.iter().map(|p| p.partition)
    }

    /// The `Lock` messages that start the transaction (sent once the gate is held). `from` is
    /// the coordinating worker.
    pub fn start(&self, from: u16) -> Step {
        let argv = self.cmd.argv();
        let msgs = self
            .parts
            .iter()
            .map(|p| {
                let keys = p
                    .keys
                    .iter()
                    .map(|&(i, access, read)| LockKey { db: p.db, key: argv[usize::from(i)].into(), access, read })
                    .collect();
                (p.partition, XTxnMsg::Lock { xtxn: self.xtxn, from, partition: p.partition, db: p.db, keys })
            })
            .collect();
        Step::Send(msgs)
    }

    fn unlock_all(&self) -> Vec<(u16, XTxnMsg)> {
        self.parts.iter().map(|p| (p.partition, XTxnMsg::Unlock { xtxn: self.xtxn, partition: p.partition })).collect()
    }

    fn finish_unlocked(&mut self, reply: Vec<u8>) -> Step {
        self.phase = Phase::Done;
        Step::Finish { msgs: self.unlock_all(), reply }
    }

    fn error_reply(e: StoreError, proto: kv_resp::Proto) -> Vec<u8> {
        let mut out = Vec::new();
        super::write_store_error(e, &mut kv_resp::ReplyWriter::new(&mut out, proto));
        out
    }

    /// A partition answered `Locked`.
    pub fn on_locked(&mut self, partition: u16, reads: Result<Vec<KeyRead>, StoreError>) -> Step {
        if self.phase != Phase::Locking {
            return Step::Wait;
        }
        let reads = match reads {
            Ok(r) => r,
            Err(e) => {
                let reply = Self::error_reply(e, self.env.proto);
                return self.finish_unlocked(reply);
            }
        };
        if let Some(p) = self.parts.iter_mut().find(|p| p.partition == partition && p.reads.is_none()) {
            p.reads = Some(reads);
        }
        if self.parts.iter().all(|p| p.reads.is_some()) { self.decide() } else { Step::Wait }
    }

    /// The read for argument `i`.
    fn read_of(&self, i: u16) -> Option<&KeyRead> {
        self.parts.iter().find_map(|p| {
            let at = p.keys.iter().position(|&(k, ..)| k == i)?;
            p.reads.as_ref()?.get(at)
        })
    }

    fn write_reply(&mut self, f: impl FnOnce(&mut kv_resp::ReplyWriter<'_>)) {
        self.reply.clear();
        f(&mut kv_resp::ReplyWriter::new(&mut self.reply, self.env.proto));
    }

    /// Every part is locked: decide whether anything is written.
    fn decide(&mut self) -> Step {
        match self.kind {
            Kind::Mset => self.write_reply(|w| w.ok()),
            Kind::Msetnx => {
                let any = self.parts.iter().any(|p| p.reads.as_ref().is_some_and(|r| r.iter().any(|k| k.exists)));
                if any {
                    let mut out = Vec::new();
                    kv_resp::ReplyWriter::new(&mut out, self.env.proto).int(0);
                    return self.finish_unlocked(out);
                }
                self.write_reply(|w| w.int(1));
            }
            Kind::Rename { nx } => {
                if !self.read_of(1).is_some_and(|r| r.exists) {
                    let mut out = Vec::new();
                    kv_resp::ReplyWriter::new(&mut out, self.env.proto).error(b"ERR no such key");
                    return self.finish_unlocked(out);
                }
                if nx && self.read_of(2).is_some_and(|r| r.exists) {
                    let mut out = Vec::new();
                    kv_resp::ReplyWriter::new(&mut out, self.env.proto).int(0);
                    return self.finish_unlocked(out);
                }
                if nx { self.write_reply(|w| w.int(1)) } else { self.write_reply(|w| w.ok()) }
            }
            Kind::Copy { replace, .. } => {
                let src = self.read_of(1).is_some_and(|r| r.exists);
                let blocked = !replace && self.read_of(2).is_some_and(|r| r.exists);
                if !src || blocked {
                    let mut out = Vec::new();
                    kv_resp::ReplyWriter::new(&mut out, self.env.proto).int(0);
                    return self.finish_unlocked(out);
                }
                self.write_reply(|w| w.int(1));
            }
        }
        Step::NeedTxnId
    }

    /// The ops of every part, in partition order of `self.parts`.
    fn ops(&self) -> Vec<Vec<Op>> {
        let argv = self.cmd.argv();
        let mut out: Vec<Vec<Op>> = self.parts.iter().map(|_| Vec::new()).collect();
        let part_of = |i: u16| self.parts.iter().position(|p| p.keys.iter().any(|&(k, ..)| k == i));
        match self.kind {
            Kind::Mset | Kind::Msetnx => {
                for (pi, p) in self.parts.iter().enumerate() {
                    for &(i, ..) in &p.keys {
                        let key = argv[usize::from(i)];
                        let value = argv[usize::from(i) + 1];
                        // a later duplicate of the same key wins (Redis MSET order)
                        out[pi].retain(|op| !matches!(op, Op::Put { key: k, .. } if &**k == key));
                        out[pi].push(Op::Put { db: p.db, vtype: kv_storage::extent::EXT_VTYPE_STRING, expire_at: 0, key: key.into(), value: value.to_vec() });
                    }
                }
            }
            Kind::Rename { .. } | Kind::Copy { .. } => {
                let Some(src) = self.read_of(1) else { return out };
                let Some((vtype, expire_at, value)) = src.value.as_ref() else { return out };
                let (Some(sp), Some(dp)) = (part_of(1), part_of(2)) else { return out };
                out[dp].push(Op::Put {
                    db: self.parts[dp].db,
                    vtype: *vtype,
                    expire_at: expire_at.unwrap_or(0),
                    key: argv[2].into(),
                    value: value.clone(),
                });
                if matches!(self.kind, Kind::Rename { .. }) {
                    out[sp].push(Op::Del { db: self.parts[sp].db, key: argv[1].into() });
                }
            }
        }
        out
    }

    /// The txn id was taken: the `Apply` messages (parts without ops are unlocked at the end).
    pub fn apply(&mut self, txn_id: u64) -> Step {
        self.txn_id = txn_id;
        self.phase = Phase::Applying;
        let ops = self.ops();
        let mut msgs = Vec::new();
        for (p, ops) in self.parts.iter_mut().zip(ops) {
            p.has_ops = !ops.is_empty();
            if p.has_ops {
                msgs.push((p.partition, XTxnMsg::Apply { xtxn: self.xtxn, txn_id, partition: p.partition, ops }));
            } else {
                p.applied = true;
            }
        }
        if msgs.is_empty() {
            return self.on_all_applied();
        }
        Step::Send(msgs)
    }

    fn on_all_applied(&mut self) -> Step {
        self.phase = Phase::Committing;
        Step::Commit { txn_id: self.txn_id }
    }

    /// A partition's writes are durable (or failed).
    pub fn on_applied(&mut self, partition: u16, res: Result<(), StoreError>) -> Step {
        if self.phase != Phase::Applying {
            return Step::Wait;
        }
        if let Err(e) = res {
            // Not committed: every part that was sent ops undoes them (§14.6 step 6). An
            // `Unlock` alone left the other parts' staged records open for good: their keys
            // showed the uncommitted versions (a failed RENAME had deleted its source), later
            // writes of those keys never completed, and the txn id never resolved.
            let reply = Self::error_reply(e, self.env.proto);
            return self.finish_aborted(reply);
        }
        if let Some(p) = self.parts.iter_mut().find(|p| p.partition == partition && !p.applied) {
            p.applied = true;
        }
        if self.parts.iter().all(|p| p.applied) { self.on_all_applied() } else { Step::Wait }
    }

    /// Ends a transaction whose parts were sent `Apply`: `Abort` to those, `Unlock` to the
    /// others.
    fn finish_aborted(&mut self, reply: Vec<u8>) -> Step {
        self.phase = Phase::Done;
        let mut parts = 0;
        let msgs = self
            .parts
            .iter()
            .map(|p| {
                let m = if p.has_ops {
                    parts += 1;
                    XTxnMsg::Abort { xtxn: self.xtxn, txn_id: self.txn_id, partition: p.partition }
                } else {
                    XTxnMsg::Unlock { xtxn: self.xtxn, partition: p.partition }
                };
                (p.partition, m)
            })
            .collect();
        if parts == 0 {
            return Step::Finish { msgs, reply };
        }
        Step::FinishAborted { msgs, reply, txn_id: self.txn_id, parts }
    }

    /// The commit of the txn id is durable (or failed).
    pub fn on_committed(&mut self, res: Result<(), StoreError>) -> Step {
        if self.phase != Phase::Committing {
            return Step::Wait;
        }
        self.phase = Phase::Done;
        let (reply, errno) = match res {
            Ok(()) => (std::mem::take(&mut self.reply), None),
            Err(e) => (Self::error_reply(e, self.env.proto), Some(if let StoreError::Io(n) = e { n } else { -libc::EIO })),
        };
        let msgs = self
            .parts
            .iter()
            .map(|p| {
                let m = match (p.has_ops, errno) {
                    (true, None) => XTxnMsg::Commit { xtxn: self.xtxn, txn_id: self.txn_id, partition: p.partition },
                    // an Unlock alone left the part open: later writes of its keys, and
                    // barriers, waited for its commit for good
                    (true, Some(errno)) => XTxnMsg::CommitFailed { xtxn: self.xtxn, txn_id: self.txn_id, partition: p.partition, errno },
                    (false, _) => XTxnMsg::Unlock { xtxn: self.xtxn, partition: p.partition },
                };
                (p.partition, m)
            })
            .collect();
        Step::Finish { msgs, reply }
    }

    /// Ends the transaction without writing (e.g. no txn id could be taken): unlock every part
    /// and answer `-<msg>`.
    pub fn abort(&mut self, msg: &[u8]) -> Step {
        let mut out = Vec::new();
        kv_resp::ReplyWriter::new(&mut out, self.env.proto).error(msg);
        self.finish_unlocked(out)
    }

    /// Every part is locked (or the transaction ended).
    pub fn locked(&self) -> bool {
        self.phase != Phase::Locking
    }

    /// Number of `Lock` messages the transaction sends (one per part).
    pub fn lock_count(&self) -> usize {
        self.parts.len()
    }

    /// Whether the transaction finished.
    #[cfg(test)]
    pub fn done(&self) -> bool {
        self.phase == Phase::Done
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kv_resp::Proto;

    fn env() -> ExecEnv {
        ExecEnv { proto: Proto::Resp2, db: 0, now_ms: 0, readonly_conn: false, conn_id: 1, watch: 0 }
    }

    /// keys starting with 'a' live on partition 1, others on 2
    fn part(k: &[u8]) -> u16 {
        if k.first() == Some(&b'a') { 1 } else { 2 }
    }

    fn sent(s: Step) -> Vec<(u16, XTxnMsg)> {
        match s {
            Step::Send(m) => m,
            _ => panic!("expected Send"),
        }
    }

    fn reads(n: usize, exists: bool) -> Vec<KeyRead> {
        (0..n).map(|_| KeyRead { exists, value: None }).collect()
    }

    #[test]
    fn ticket_gate_serves_in_order_without_blocking() {
        let g = XTxnGate::new();
        let (a, b) = (g.ticket(), g.ticket());
        assert!(g.is_turn(a) && !g.is_turn(b));
        g.release(a);
        assert!(g.is_turn(b));
        g.release(b);
        assert!(g.is_turn(g.ticket()));
    }

    #[test]
    fn mset_locks_each_partition_once_then_commits() {
        let cmd = OwnedCmd::new(CmdId::Mset, &[b"MSET", b"a1", b"x", b"b1", b"y", b"a2", b"z", b"a1", b"w"]);
        let mut t = XTxnCoord::new(9, 0, cmd, env(), part).unwrap();
        assert_eq!(t.partitions().collect::<Vec<_>>(), vec![1, 2], "fan-out width = distinct partitions");
        let locks = sent(t.start(4));
        assert_eq!(locks.len(), 2);
        for (p, m) in &locks {
            let XTxnMsg::Lock { from, partition, keys, .. } = m else { panic!() };
            assert_eq!((*from, partition), (4, p));
            assert!(keys.iter().all(|k| k.read == ReadWant::Nothing));
        }
        assert!(matches!(t.on_locked(1, Ok(reads(3, false))), Step::Wait));
        assert!(matches!(t.on_locked(2, Ok(reads(1, false))), Step::NeedTxnId));
        let applies = sent(t.apply(77));
        assert_eq!(applies.len(), 2);
        let XTxnMsg::Apply { ops, txn_id, .. } = &applies[0].1 else { panic!() };
        assert_eq!(*txn_id, 77);
        assert_eq!(ops.len(), 2, "duplicate key a1: the later value wins");
        assert!(ops.iter().any(|o| matches!(o, Op::Put { key, value, .. } if &**key == b"a1" && value == b"w")));
        assert!(matches!(t.on_applied(1, Ok(())), Step::Wait));
        let Step::Commit { txn_id } = t.on_applied(2, Ok(())) else { panic!() };
        assert_eq!(txn_id, 77);
        let Step::Finish { msgs, reply } = t.on_committed(Ok(())) else { panic!() };
        assert_eq!(reply, b"+OK\r\n");
        assert!(msgs.iter().all(|(_, m)| matches!(m, XTxnMsg::Commit { txn_id: 77, .. })));
        assert!(t.done());
    }

    #[test]
    fn msetnx_with_an_existing_key_writes_nothing() {
        let cmd = OwnedCmd::new(CmdId::Msetnx, &[b"MSETNX", b"a1", b"x", b"b1", b"y"]);
        let mut t = XTxnCoord::new(1, 0, cmd, env(), part).unwrap();
        let _ = t.start(0);
        let _ = t.on_locked(1, Ok(reads(1, false)));
        let Step::Finish { msgs, reply } = t.on_locked(2, Ok(reads(1, true))) else { panic!() };
        assert_eq!(reply, b":0\r\n");
        assert!(msgs.iter().all(|(_, m)| matches!(m, XTxnMsg::Unlock { .. })));
    }

    #[test]
    fn rename_moves_value_and_ttl_across_partitions() {
        let cmd = OwnedCmd::new(CmdId::Rename, &[b"RENAME", b"a-src", b"b-dst"]);
        let mut t = XTxnCoord::new(1, 0, cmd, env(), part).unwrap();
        let _ = t.start(0);
        let src = vec![KeyRead { exists: true, value: Some((0, Some(5000), b"v".to_vec())) }];
        assert!(matches!(t.on_locked(1, Ok(src)), Step::Wait));
        assert!(matches!(t.on_locked(2, Ok(reads(1, false))), Step::NeedTxnId));
        let applies = sent(t.apply(3));
        let put = applies.iter().find(|(p, _)| *p == 2).unwrap();
        let XTxnMsg::Apply { ops, .. } = &put.1 else { panic!() };
        assert!(matches!(&ops[0], Op::Put { key, expire_at: 5000, value, .. } if &**key == b"b-dst" && value == b"v"));
        let del = applies.iter().find(|(p, _)| *p == 1).unwrap();
        let XTxnMsg::Apply { ops, .. } = &del.1 else { panic!() };
        assert!(matches!(&ops[0], Op::Del { key, .. } if &**key == b"a-src"));
    }

    #[test]
    fn rename_of_a_missing_key_and_failures_unlock() {
        let cmd = OwnedCmd::new(CmdId::Rename, &[b"RENAME", b"a-src", b"b-dst"]);
        let mut t = XTxnCoord::new(1, 0, cmd, env(), part).unwrap();
        let _ = t.start(0);
        let _ = t.on_locked(1, Ok(reads(1, false)));
        let Step::Finish { reply, msgs } = t.on_locked(2, Ok(reads(1, false))) else { panic!() };
        assert_eq!(reply, b"-ERR no such key\r\n");
        assert_eq!(msgs.len(), 2);

        let cmd = OwnedCmd::new(CmdId::Mset, &[b"MSET", b"a", b"1", b"b", b"2"]);
        let mut t = XTxnCoord::new(2, 0, cmd, env(), part).unwrap();
        let _ = t.start(0);
        let Step::Finish { reply, .. } = t.on_locked(1, Err(StoreError::Loading)) else { panic!() };
        assert!(reply.starts_with(b"-LOADING"));

        let cmd = OwnedCmd::new(CmdId::Mset, &[b"MSET", b"a", b"1", b"b", b"2"]);
        let mut t = XTxnCoord::new(3, 0, cmd, env(), part).unwrap();
        let _ = t.start(0);
        let _ = t.on_locked(1, Ok(reads(1, false)));
        let _ = t.on_locked(2, Ok(reads(1, false)));
        let _ = t.apply(5);
        let Step::FinishAborted { reply, msgs, txn_id, parts } = t.on_applied(2, Err(StoreError::OutOfSpace)) else { panic!() };
        assert!(reply.starts_with(b"-OOM"));
        assert_eq!((txn_id, parts), (5, 2));
        assert!(msgs.iter().all(|(_, m)| matches!(m, XTxnMsg::Abort { txn_id: 5, .. })), "not committed: every part with ops undoes them");
        assert!(matches!(t.on_applied(1, Ok(())), Step::Wait), "a late Applied of an aborted txn is ignored");

        // RENAME to a key of another partition: the source's part is aborted with the rest
        let cmd = OwnedCmd::new(CmdId::Rename, &[b"RENAME", b"a-src", b"b-dst"]);
        let mut t = XTxnCoord::new(4, 0, cmd, env(), part).unwrap();
        let _ = t.start(0);
        let _ = t.on_locked(1, Ok(vec![KeyRead { exists: true, value: Some((1, None, b"v".to_vec())) }]));
        let _ = t.on_locked(2, Ok(reads(1, false)));
        let _ = t.apply(6);
        assert!(matches!(t.on_applied(1, Ok(())), Step::Wait));
        let Step::FinishAborted { msgs, parts: 2, .. } = t.on_applied(2, Err(StoreError::OutOfSpace)) else { panic!() };
        assert!(msgs.iter().any(|(p, m)| *p == 1 && matches!(m, XTxnMsg::Abort { txn_id: 6, .. })), "the applied source delete is undone");
    }

    #[test]
    fn a_failed_commit_fails_the_parts_with_ops() {
        let cmd = OwnedCmd::new(CmdId::Rename, &[b"RENAME", b"a-src", b"b-dst"]);
        let mut t = XTxnCoord::new(1, 0, cmd, env(), part).unwrap();
        let _ = t.start(0);
        let _ = t.on_locked(1, Ok(vec![KeyRead { exists: true, value: Some((1, None, b"v".to_vec())) }]));
        let _ = t.on_locked(2, Ok(reads(1, false)));
        let _ = t.apply(8);
        let _ = t.on_applied(1, Ok(()));
        let Step::Commit { txn_id: 8 } = t.on_applied(2, Ok(())) else { panic!() };
        let Step::Finish { msgs, reply } = t.on_committed(Err(StoreError::Io(-5))) else { panic!() };
        assert!(reply.starts_with(b"-"));
        assert_eq!(msgs.len(), 2);
        assert!(msgs.iter().all(|(_, m)| matches!(m, XTxnMsg::CommitFailed { txn_id: 8, errno: -5, .. })), "an unlock alone leaves the parts open");
    }

    #[test]
    fn copy_respects_replace_and_db() {
        let cmd = OwnedCmd::new(CmdId::Copy, &[b"COPY", b"a", b"b", b"DB", b"3"]);
        let mut t = XTxnCoord::new(1, 0, cmd, env(), part).unwrap();
        let locks = sent(t.start(0));
        let dst = locks.iter().find(|(p, _)| *p == 2).unwrap();
        let XTxnMsg::Lock { db, keys, .. } = &dst.1 else { panic!() };
        assert_eq!((*db, keys[0].read), (3, ReadWant::Exists));
        let _ = t.on_locked(1, Ok(vec![KeyRead { exists: true, value: Some((0, None, b"v".to_vec())) }]));
        let Step::Finish { reply, .. } = t.on_locked(2, Ok(reads(1, true))) else { panic!() };
        assert_eq!(reply, b":0\r\n");
        assert!(XTxnCoord::new(1, 0, OwnedCmd::new(CmdId::Get, &[b"GET", b"a"]), env(), part).is_none());
    }
}
