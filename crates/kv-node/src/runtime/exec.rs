//! Command dispatch and execution on a worker (DESIGN §6.4-§6.8, §7.5, §8, §14.7, §16.5,
//! §16.6).
//!
//! The home worker of a connection resolves each parsed command (lookup, arity, NOAUTH, MULTI
//! queueing, `-LOADING`), then hands it to its handler kind: connection commands run here at
//! once; keyed commands become partition requests on the owner worker (inline when local, an
//! `XMsg::Cmd` otherwise); standalone multi-partition reads/deletes are split per partition and
//! gathered; multi-key writes spanning partitions become cross-partition transactions;
//! SCAN-family calls run one step at a time on the step's owner; FLUSH*, barriers, SHUTDOWN and
//! CLIENT LIST/KILL are node operations gathered over every worker.
//!
//! On the owner, a ready request runs its [`Work`] synchronously; the reply is released per the
//! durability mode (§14.7): at once for reads, after `Written` (async deletes, written mode) or
//! `Durable` (flushed mode). A command is never parked because of load (LC-14/LC-15): the only
//! waits are for its own I/O and tickets, for earlier commands on its keys (the partition's
//! FIFOs), and for the cross-partition transaction gate (a conflict lock).

use std::collections::VecDeque;
use std::sync::Arc;

use kv_cmd::{CmdFlags, CmdId, CommandSpec};
use kv_resp::ReplyWriter;
use kv_storage::effect::Op;
use kv_storage::keyspace::StoreError;
use kv_storage::metastore::{MetaDone, MetaReq};
use kv_storage::partition::{Durability, ExecStatus, Origin, ReqId, ReqKind, Request};
use kv_storage::ticket::TicketState;
use smallvec::SmallVec;

use crate::cmd::server::{NodeOp, NodeOpDone};
use crate::cmd::{Args, ConnCtx, ConnOutcome, FanOutSplit, Handler};
use crate::dispatch::router::{self, Route};
use crate::dispatch::scan::{ScanCall, ScanOutcome, ScanReq, StepKind};
use crate::dispatch::xtxn::{LockKey, Step, XTxnCoord, XTxnMsg};
use crate::dispatch::{multikey, write_store_error};
use crate::net::budget::{Reservation, is_large, rounded};
use crate::net::conn::{Conn, GatherSlot, Hold, flags};
use crate::runtime::backend::{Backend, Work, WorkOut, is_delete};
use crate::runtime::worker::Worker;
use crate::runtime::xmsg::{ConnRef, ExecEnv, GatherRef, OwnedCmd, ReplyBody, XMsg};
use crate::shared::NodeShared;

/// Meta tokens of the worker's own requests (cross-partition commits) carry this bit;
/// partitions' tokens never do.
pub const WORKER_TOKEN_BIT: u64 = 1 << 63;

/// Gather id of the shutdown drain (no reply).
const DRAIN_GATHER: u32 = u32::MAX;

/// Where a result goes.
#[derive(Clone, Copy)]
pub struct ReplyTo {
    pub conn: ConnRef,
    pub seq: u64,
    /// Fan-out part index.
    pub part: Option<u16>,
}

/// What a submitted request is for.
pub enum JobKind {
    Cmd(OwnedCmd),
    Multi(Vec<OwnedCmd>),
    /// Cross-partition transaction lock (participant side).
    Lock { xtxn: u64, coord: u16, keys: SmallVec<[LockKey; 4]> },
    /// Cross-partition transaction writes (participant side).
    Apply { xtxn: u64, coord: u16, ops: Vec<Op> },
    /// A SCAN-family step for `home`.
    Scan { pattern: Option<Box<[u8]>>, home: ReplyTo },
    /// One partition's part of a FLUSH or barrier.
    Node { gather: GatherRef },
    /// Replication, sync and migration work of the cluster glue (§23.7, §23.13).
    Repl(crate::cluster::repl_glue::ReplJob),
}

/// A submitted partition request and its continuation.
pub struct Job {
    pub to: Option<ReplyTo>,
    pub kind: JobKind,
    pub env: ExecEnv,
    pub id: CmdId,
    pub start_ns: u64,
    pub _lv: Reservation,
    /// The txn credit of a multi-key write (§14.6 step 0): spent when the partition opened a
    /// txn for it (the meta thread returns it on `Resolved`), given back otherwise.
    pub credit: Option<CreditGuard>,
}

/// What a durability wait continues with.
enum WaitFor {
    /// `commit`: in cluster mode the reply also waits for the shard commit of that seq.
    Reply { to: ReplyTo, body: ReplyBody, commit: Option<(u16, kv_storage::layout::Seq)> },
    Node { gather: GatherRef, commit: Option<(u16, kv_storage::layout::Seq)> },
    Apply { xtxn: u64, coord: u16, partition: u16 },
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Need {
    Written,
    Durable,
}

struct Waiter {
    what: WaitFor,
    need: Need,
}

/// What waits for a shard commit (cluster mode, §23.8 step 3).
pub enum CommitWaiter {
    Reply(ReplyTo, ReplyBody),
    Node(GatherRef),
}

/// A cross-partition transaction this worker coordinates.
struct Coord {
    coord: XTxnCoord,
    to: ReplyTo,
    start_ns: u64,
    /// Spent once the transaction takes a txn id; given back when it ends without one.
    credit: Option<CreditGuard>,
    /// The `Lock` messages went out (the gate served the transaction).
    started: bool,
    /// The connection stopped parsing until then (`Hold::Order`).
    holds_conn: bool,
}

/// A result produced by an executed request, released after the partition ticks of the same
/// loop turn (the engine updates its key counts there).
enum Held {
    Reply(ReplyTo, ReplyBody),
    NodePart(GatherRef),
}

/// A partition locked by a cross-partition transaction (participant side): new requests wait
/// in FIFO order until it ends.
struct PartHold {
    xtxn: u64,
    coord: u16,
    queue: VecDeque<HoldItem>,
}

/// What waits behind a partition hold, in arrival order.
enum HoldItem {
    Job(Request, Job),
    /// The `Lock` of the next transaction (it arrived before the current one's `Commit` or
    /// `Unlock`): it takes the partition when its turn in the queue comes.
    Lock(XTxnMsg),
}

/// A node operation on its home worker.
struct HomeGather {
    op: NodeOp,
    to: Option<ReplyTo>,
    remaining: u16,
    err: Option<StoreError>,
    lines: Vec<u8>,
    killed: u64,
}

/// One worker's share of a FLUSH / barrier.
struct NodePart {
    remaining: usize,
    err: Option<StoreError>,
}

/// The mutable execution state of a worker.
pub struct ExecState {
    jobs: hashbrown::HashMap<(u16, u32), Job, foldhash::fast::RandomState>,
    ready: Vec<ReqId>,
    waits: hashbrown::HashMap<(u16, u64), SmallVec<[Waiter; 1]>, foldhash::fast::RandomState>,
    tickets: Vec<(kv_storage::ticket::TicketId, TicketState)>,
    /// Messages for other workers that found the queue full, FIFO per destination.
    pub outbox: Vec<VecDeque<XMsg>>,
    /// Messages to this worker itself (handled in the next drain, never re-entrantly).
    pub local: VecDeque<XMsg>,
    holds: hashbrown::HashMap<u16, PartHold, foldhash::fast::RandomState>,
    coords: hashbrown::HashMap<u64, Coord, foldhash::fast::RandomState>,
    gate_q: VecDeque<u64>,
    next_xtxn: u64,
    node_parts: hashbrown::HashMap<GatherRef, NodePart, foldhash::fast::RandomState>,
    gathers: Vec<Option<HomeGather>>,
    free_gathers: Vec<u32>,
    /// WATCH replies waiting for their keys' owners to register them: `(reply, protocol,
    /// owners left)` by id.
    watch_waits: hashbrown::HashMap<u32, (ReplyTo, kv_resp::Proto, usize), foldhash::fast::RandomState>,
    next_watch_wait: u32,
    /// Scratch for the watched keys a partition reports changed.
    touched: Vec<(u8, Box<[u8]>)>,
    scans: hashbrown::HashMap<(u32, u32, u64), ScanCall, foldhash::fast::RandomState>,
    lat: Vec<(CmdId, u32)>,
    reply: Vec<u8>,
    rng: u64,
    /// Partitions that executed a request this loop turn.
    executed: SmallVec<[u16; 8]>,
    held: Vec<Held>,
}

impl ExecState {
    pub fn new(workers: u16) -> Self {
        ExecState {
            jobs: Default::default(),
            ready: Vec::new(),
            waits: Default::default(),
            tickets: Vec::new(),
            outbox: crate::runtime::worker::outboxes(workers),
            local: VecDeque::new(),
            holds: Default::default(),
            coords: Default::default(),
            gate_q: VecDeque::new(),
            next_xtxn: 0,
            node_parts: Default::default(),
            gathers: Vec::new(),
            free_gathers: Vec::new(),
            watch_waits: Default::default(),
            next_watch_wait: 0,
            touched: Vec::new(),
            scans: Default::default(),
            lat: Vec::new(),
            reply: Vec::new(),
            rng: 0x9e37_79b9_7f4a_7c15 ^ u64::from(workers),
            executed: SmallVec::new(),
            held: Vec::new(),
        }
    }

    /// Nothing in flight on this worker's side.
    pub fn idle(&self) -> bool {
        self.held.is_empty()
            && self.jobs.is_empty()
            && self.waits.is_empty()
            && self.coords.is_empty()
            && self.holds.is_empty()
            && self.local.is_empty()
            && self.outbox.iter().all(VecDeque::is_empty)
    }

    /// Work that the next tick can do without waiting for a completion.
    pub fn runnable(&self) -> bool {
        !self.local.is_empty()
    }

    /// Something only time can unblock (the transaction gate, a full destination queue).
    pub fn waiting_on_gate(&self) -> bool {
        !self.gate_q.is_empty() || self.outbox.iter().any(|q| !q.is_empty())
    }

    /// Folds the tick's latency samples into the shared histograms.
    pub fn flush_latency(&mut self, shared: &NodeShared, worker: u16) {
        shared.latency[usize::from(worker)].record_batch(&self.lat);
        self.lat.clear();
    }

    fn next_random(&mut self) -> u64 {
        // xorshift64*: RANDOMKEY draws (not security relevant)
        self.rng ^= self.rng >> 12;
        self.rng ^= self.rng << 25;
        self.rng ^= self.rng >> 27;
        self.rng.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    /// The pre-consume checks of §6.7 for a complete command at `args` (offsets relative to
    /// `conn.read_buf[base..]`): the large-value soft limit and the txn credit. On success the
    /// command's large-value reservation is taken (unconditionally, §6.9) and the credit, if
    /// one was needed, is held by the returned value.
    pub fn pre_consume(&mut self, shared: &Arc<NodeShared>, conn: &mut Conn, args: &[(u32, u32)], base: usize, now_boot_ns: u64) -> Result<PreConsume, Hold> {
        let large: u64 = args.iter().filter(|&&(_, l)| is_large(l as usize)).map(|&(_, l)| rounded(l as usize)).sum();
        if large > 0 && !conn.lv_admitted && !shared.budget.may_start() {
            return Err(Hold::LargeValue);
        }
        let src = &conn.read_buf[base..];
        let arg = |i: usize| -> &[u8] {
            let (o, l) = args[i];
            &src[o as usize..(o + l) as usize]
        };
        if gate_holds(shared, conn, args, src, now_boot_ns) {
            conn.gate_hold_since.get_or_insert(now_boot_ns);
            return Err(Hold::Gate);
        }
        conn.gate_hold_since = None;
        let needs_credit = !args.is_empty() && {
            let name = arg(0);
            let multi = conn.multi.is_some();
            match kv_cmd::lookup(name).map(|s| s.id) {
                Some(CmdId::Mset | CmdId::Msetnx) => !multi && args.len() >= 5,
                Some(CmdId::Rename | CmdId::Renamenx | CmdId::Copy) => !multi && args.len() >= 3,
                Some(CmdId::Exec) => conn.multi.as_ref().is_some_and(|m| written_keys(&m.queued) >= 2),
                _ => false,
            }
        };
        let credit = if needs_credit {
            if !shared.take_txn_credit() {
                return Err(Hold::TxnCredit);
            }
            Some(CreditGuard(Some(shared.clone())))
        } else {
            None
        };
        Ok(PreConsume { lv: Reservation::take(shared, large), credit })
    }
}

/// How long a cluster write waits on a passing TRYAGAIN state of the routing gate before it is
/// answered TRYAGAIN after all (a term raise takes tens of milliseconds; a handoff drains the
/// writes in flight first).
pub(crate) const GATE_HOLD_NS: u64 = 500_000_000;

/// Whether the complete command at `args` (in `src`) is a write the cluster routing gate would
/// answer TRYAGAIN right now (a write in MULTI is checked when queued, an EXEC for its queued
/// writes), and the connection has not waited for that longer than `GATE_HOLD_NS`.
fn gate_holds(shared: &NodeShared, conn: &Conn, args: &[(u32, u32)], src: &[u8], now_boot_ns: u64) -> bool {
    use crate::cluster::ownership::Gate;
    if args.is_empty() || shared.cluster().is_none() {
        return false;
    }
    if conn.gate_hold_since.is_some_and(|t| now_boot_ns.saturating_sub(t) >= GATE_HOLD_NS) {
        return false;
    }
    let own = shared.ownership.read().clone();
    if !own.may_try_again() {
        return false;
    }
    let argv: SmallVec<[&[u8]; 8]> = args.iter().map(|&(o, l)| &src[o as usize..(o + l) as usize]).collect();
    let Some(spec) = kv_cmd::lookup(argv[0]) else { return false };
    let lease_ok = shared.lease.valid(now_boot_ns);
    let try_again = |spec: &'static CommandSpec, argv: &[&[u8]]| {
        if !spec.flags.contains(CmdFlags::WRITE) || !spec.has_keys() {
            return false;
        }
        let pos = kv_cmd::key_positions(spec, argv);
        match kv_cmd::common_slot(argv, &pos) {
            Ok(slot) => own.gate(slot, spec.flags, conn.readonly, lease_ok, conn.id) == Gate::TryAgain,
            Err(_) => false,
        }
    };
    if spec.id == CmdId::Exec {
        return conn.multi.as_ref().is_some_and(|m| {
            m.queued.iter().any(|c| try_again(c.id.spec(), &c.argv()))
        });
    }
    try_again(spec, &argv)
}

/// Keys written by a list of queued commands (the EXEC txn-credit rule, §6.7).
fn written_keys(cmds: &[OwnedCmd]) -> usize {
    cmds.iter()
        .filter(|c| c.id.spec().flags.contains(CmdFlags::WRITE))
        .map(|c| kv_cmd::key_positions(c.id.spec(), &c.argv()).len())
        .sum()
}

/// A txn credit taken before consuming a multi-key write; returned unless spent.
pub struct CreditGuard(Option<Arc<NodeShared>>);

impl CreditGuard {
    /// Takes a credit from the node pool for a replication request (`None`: none free now).
    pub(crate) fn try_take(shared: &Arc<NodeShared>) -> Option<Self> {
        shared.take_txn_credit().then(|| CreditGuard(Some(shared.clone())))
    }

    /// Takes over a credit that travelled with a command to its owner worker.
    fn adopt(shared: &Arc<NodeShared>) -> Self {
        CreditGuard(Some(shared.clone()))
    }

    /// A txn was opened with this credit: it returns when the meta thread reports the txn
    /// resolved (`MetaDone::Resolved`).
    fn spend(mut self) {
        self.0 = None;
    }

    /// The credit moves with its command to another worker, which adopts it.
    fn transfer(mut self) -> bool {
        self.0 = None;
        true
    }
}

impl Drop for CreditGuard {
    fn drop(&mut self) {
        if let Some(s) = self.0.take() {
            s.give_back_txn_credits(1);
        }
    }
}

/// What the pre-consume checks hand to dispatch.
pub struct PreConsume {
    pub lv: Reservation,
    pub credit: Option<CreditGuard>,
}

/// The partition request of a keyed command: its keys with their access modes (key bytes only;
/// the partition computes the key hashes, LC-37).
fn keyed_request(spec: &CommandSpec, argv: &[&[u8]], db: u8) -> Request {
    let pos = kv_cmd::key_positions(spec, argv);
    let keys = pos.iter().map(|&i| (Box::<[u8]>::from(argv[usize::from(i)]), kv_cmd::key_access(spec, argv, i))).collect();
    Request { db, keys, kind: ReqKind::Keyed, origin: Origin::Client }
}

/// The partition request of an EXEC body.
fn multi_request(cmds: &[OwnedCmd], db: u8) -> Request {
    let mut keys: SmallVec<[(Box<[u8]>, kv_cmd::Access); 2]> = SmallVec::new();
    for c in cmds {
        let argv = c.argv();
        let spec = c.id.spec();
        for i in kv_cmd::key_positions(spec, &argv) {
            let k = argv[usize::from(i)];
            let a = kv_cmd::key_access(spec, &argv, i);
            match keys.iter_mut().find(|(x, _)| &**x == k) {
                Some(e) => e.1 = merge_access(e.1, a),
                None => keys.push((k.into(), a)),
            }
        }
    }
    Request { db, keys, kind: ReqKind::Keyed, origin: Origin::Client }
}

fn merge_access(a: kv_cmd::Access, b: kv_cmd::Access) -> kv_cmd::Access {
    use kv_cmd::Access::*;
    match (a, b) {
        (Meta, x) | (x, Meta) => x,
        (Read, Read) => Read,
        (Write, Write) => Write,
        _ => ReadWrite,
    }
}

/// The partition of every key of an EXEC body; `None` when they span partitions.
fn multi_partition(cmds: &[OwnedCmd], partitions: u16) -> Result<Option<u16>, ()> {
    let mut p = None;
    for c in cmds {
        let argv = c.argv();
        for i in kv_cmd::key_positions(c.id.spec(), &argv) {
            let q = kv_common::key_slot(argv[usize::from(i)]) % partitions;
            match p {
                None => p = Some(q),
                Some(x) if x != q => return Err(()),
                Some(_) => {}
            }
        }
    }
    Ok(p)
}

/// The database an EXEC body leaves the connection in (its valid SELECTs).
fn db_after(cmds: &[OwnedCmd], db: u8, databases: u8) -> u8 {
    cmds.iter()
        .filter(|c| c.id == CmdId::Select && c.argc() == 2)
        .filter_map(|c| kv_common::num::string2ll(c.arg(1)))
        .rfind(|&n| n >= 0 && n < i64::from(databases))
        .map_or(db, |n| n as u8)
}

/// How long a reply waits for its ticket (§14.7): `None` = release at exec.
fn ticket_need(mode: Durability, id: CmdId, has_ticket: bool) -> Option<Need> {
    if !has_ticket || id.spec().flags.contains(CmdFlags::READONLY) {
        return None;
    }
    match mode {
        Durability::Async if is_delete(id) => Some(Need::Written),
        Durability::Async => None,
        Durability::Written => Some(Need::Written),
        Durability::Flushed => Some(Need::Durable),
    }
}

fn satisfied(need: Need, st: TicketState) -> bool {
    match st {
        TicketState::Executed => false,
        TicketState::Written => need == Need::Written,
        TicketState::Durable | TicketState::Failed(_) => true,
    }
}

fn err_body(e: StoreError, proto: kv_resp::Proto) -> ReplyBody {
    let mut out = Vec::with_capacity(64);
    write_store_error(e, &mut ReplyWriter::new(&mut out, proto));
    ReplyBody::Bytes(out)
}

impl<B: Backend> Worker<B> {
    // -----------------------------------------------------------------------------------------
    // messaging

    /// Sends to worker `dst` (itself: next drain) keeping FIFO order behind earlier messages
    /// that found the queue full. Returns whether the message left at once.
    pub(super) fn send(&mut self, dst: u16, msg: XMsg) -> bool {
        if dst == self.id {
            self.x.local.push_back(msg);
            return true;
        }
        let q = &mut self.x.outbox[usize::from(dst)];
        if !q.is_empty() {
            q.push_back(msg);
            return false;
        }
        match self.shared.workers[usize::from(dst)].try_send(self.id, msg) {
            Ok(()) => {
                self.counters.xmsg_hops += 1;
                true
            }
            Err(m) => {
                self.x.outbox[usize::from(dst)].push_back(m);
                false
            }
        }
    }

    /// Sends a command parsed from `conn`; a full destination queue stops parsing that
    /// connection until the queue drains (§6.3, §6.7).
    fn send_cmd(&mut self, conn: &mut Conn, dst: u16, msg: XMsg) {
        if !self.send(dst, msg) {
            conn.hold = Some(Hold::Queue);
        }
    }

    /// Delivers a result to its connection (here or on its home worker).
    pub(crate) fn deliver(&mut self, to: ReplyTo, body: ReplyBody) {
        if to.conn.worker != self.id {
            let msg = match to.part {
                None => XMsg::Reply { conn: to.conn, req_seq: to.seq, reply: body },
                Some(part) => XMsg::Part { conn: to.conn, req_seq: to.seq, part, reply: body },
            };
            self.send(to.conn.worker, msg);
            return;
        }
        let slot = to.conn.slot;
        if self.checked_out == Some(slot) {
            // the connection is being parsed (a result produced during its own dispatch)
            if self.gens.get(slot as usize) == Some(&to.conn.generation) {
                self.deferred.push((to, body));
            }
            return;
        }
        let Some(conn) = self.conn_mut(slot, to.conn.generation) else { return };
        let proto = conn.proto;
        let ready = match to.part {
            None => conn.deliver(to.seq, body),
            Some(part) => conn.deliver_part(to.seq, part, body, |g| merge_gather(g, proto)),
        };
        if ready {
            self.queue_send(slot);
        }
        self.reconsider(slot);
        self.maybe_close(slot);
    }

    /// Handles a message from another worker (or this one).
    pub(super) fn handle_message(&mut self, m: XMsg) {
        match m {
            XMsg::Cmd { from, conn, req_seq, partition, cmd, env, part, credit, lv } => {
                let to = ReplyTo { conn, seq: req_seq, part };
                let _ = from;
                let spec = cmd.id.spec();
                let req = keyed_request(spec, &cmd.argv(), env.db);
                let credit = credit.then(|| CreditGuard::adopt(&self.shared));
                let job = Job { to: Some(to), id: cmd.id, kind: JobKind::Cmd(cmd), env, start_ns: self.now.mono_ns, _lv: lv, credit };
                self.submit_job(partition, req, job);
            }
            XMsg::Exec { from, conn, req_seq, partition, cmds, env, credit } => {
                let _ = from;
                let req = multi_request(&cmds, env.db);
                let job = Job {
                    to: Some(ReplyTo { conn, seq: req_seq, part: None }),
                    id: CmdId::Exec,
                    kind: JobKind::Multi(cmds),
                    env,
                    start_ns: self.now.mono_ns,
                    _lv: Reservation::NONE,
                    credit: credit.then(|| CreditGuard::adopt(&self.shared)),
                };
                self.submit_job(partition, req, job);
            }
            XMsg::Watch { keys, add, ack } => {
                for (p, db, key) in &keys {
                    if add {
                        self.backend.watch_key(*p, *db, key);
                    } else {
                        self.backend.unwatch_key(*p, *db, key);
                    }
                }
                if let Some(ack) = ack {
                    self.send(ack.worker, XMsg::WatchDone { ack });
                }
            }
            XMsg::WatchDone { ack } => self.watch_registered(ack.id),
            XMsg::WatchRelease { token } => self.release_watch(token),
            XMsg::Reply { conn, req_seq, reply } => self.deliver(ReplyTo { conn, seq: req_seq, part: None }, reply),
            XMsg::Part { conn, req_seq, part, reply } => self.deliver(ReplyTo { conn, seq: req_seq, part: Some(part) }, reply),
            XMsg::Flush { db, gather } => self.node_part_start(gather, ReqKind::FlushDb { db }),
            XMsg::Barrier { gather } => self.node_part_start(gather, ReqKind::Barrier),
            XMsg::GatherDone { gather, from, res } => {
                let _ = from;
                self.gather_done(gather, res.err(), &[], 0);
            }
            XMsg::Clients { gather, op, caller } => self.clients_part(gather, &op, caller),
            XMsg::ClientsDone { gather, lines, killed } => self.gather_done(gather, None, &lines, killed),
            XMsg::XTxn(t) => self.on_xtxn(t),
            XMsg::Meta { partition, token, done } => self.on_meta(partition, token, done),
            XMsg::Scan { from, conn, req_seq, req } => {
                let _ = from;
                let ScanReq { kind: _, partition, request, pattern } = req;
                let job = Job {
                    to: None,
                    id: CmdId::Scan,
                    kind: JobKind::Scan { pattern, home: ReplyTo { conn, seq: req_seq, part: None } },
                    env: ExecEnv { proto: kv_resp::Proto::Resp2, db: request.db, now_ms: self.now.unix_ms, readonly_conn: false, conn_id: 0, watch: 0 },
                    start_ns: self.now.mono_ns,
                    _lv: Reservation::NONE,
                    credit: None,
                };
                self.submit_job(partition, request, job);
            }
            XMsg::ScanDone { conn, req_seq, out } => self.scan_done(conn, req_seq, out),
            XMsg::Ctrl(ev) => {
                if self.cluster.is_some() {
                    self.on_ctrl(ev);
                } else {
                    tracing::debug!(worker = self.id, "control event ignored in standalone mode");
                }
            }
            XMsg::Repl(ev) => {
                if self.cluster.is_some() {
                    self.on_repl(ev);
                } else if let crate::cluster::net::ReplEvent::Inbound { stream, .. } | crate::cluster::net::ReplEvent::Dialed { stream: Some(stream), .. } = ev {
                    stream.abort();
                }
            }
            XMsg::ReplSocket { fd, partition, .. } => {
                tracing::debug!(worker = self.id, partition, "replication stream refused: not in cluster mode");
                crate::runtime::ring::sys::close(fd);
            }
            XMsg::Shutdown => {}
        }
    }

    fn on_meta(&mut self, partition: u16, token: u64, done: MetaDone) {
        if token & crate::cluster::AGENT_TOKEN_BIT != 0 && token & WORKER_TOKEN_BIT == 0 {
            if let Some(cs) = self.shared.cluster() {
                cs.tell_agent(crate::cluster::AgentMsg::Meta { token, done });
            }
            return;
        }
        if token & crate::cluster::REPL_TOKEN_BIT != 0 && token & WORKER_TOKEN_BIT == 0 {
            self.repl_meta_done(partition, token, done);
            return;
        }
        if let MetaDone::Resolved { credits } = done {
            // the txns resolved: their credits return (whoever asked for the commit)
            self.shared.give_back_txn_credits(credits);
            if token & WORKER_TOKEN_BIT != 0 {
                return;
            }
        }
        if token & WORKER_TOKEN_BIT != 0 {
            let xtxn = token & !WORKER_TOKEN_BIT;
            let res = match done {
                MetaDone::Committed { .. } | MetaDone::Ok => Ok(()),
                MetaDone::Failed(e) => Err(StoreError::Io(e)),
                _ => return,
            };
            self.xtxn_step(xtxn, |c| c.on_committed(res));
            return;
        }
        if self.backend.owned().contains(&partition) {
            self.backend.on_meta(partition, self.now, done);
        }
    }

    // -----------------------------------------------------------------------------------------
    // dispatch of one parsed command (home worker)

    /// Dispatches the command whose arguments `args` lie in `conn.read_buf[base..]`.
    pub(super) fn dispatch(&mut self, slot: u32, conn: &mut Conn, args: &[(u32, u32)], base: usize, pc: PreConsume) {
        let PreConsume { lv, mut credit } = pc;
        let mut out = std::mem::take(&mut self.x.reply);
        out.clear();
        let resolved = {
            let src = &conn.read_buf[base..];
            let argv: SmallVec<[&[u8]; 8]> = args.iter().map(|&(o, l)| &src[o as usize..(o + l) as usize]).collect();
            match kv_cmd::resolve(&argv) {
                Ok(spec) => Ok((spec, OwnedCmd::new(spec.id, &argv))),
                Err(e) => {
                    e.write(&argv, &mut ReplyWriter::new(&mut out, conn.proto));
                    Err(e)
                }
            }
        };
        let (spec, cmd) = match resolved {
            Ok(x) => x,
            Err(e) => {
                if let kv_cmd::ResolveError::WrongArity(s) = e {
                    self.shared.command_stats[usize::from(self.id)].reject(s.id);
                }
                crate::cmd::txn::flag_queue_error(conn);
                self.reply_now(conn, &mut out);
                self.x.reply = out;
                return;
            }
        };
        conn.last_cmd_id = Some(spec.id);
        let mut w = ReplyWriter::new(&mut out, conn.proto);
        let requirepass = !self.cfg.requirepass.is_empty();
        if requirepass && !conn.authed && !router::allowed_before_auth(spec) {
            w.error(router::NOAUTH);
            crate::cmd::txn::flag_queue_error(conn);
            self.shared.command_stats[usize::from(self.id)].reject(spec.id);
            self.reply_now(conn, &mut out);
            self.x.reply = out;
            return;
        }
        // MULTI: everything but the transaction and connection-control commands is queued
        if conn.multi.is_some() && crate::cmd::txn::is_queued(spec.id) {
            if spec.flags.contains(CmdFlags::NO_MULTI) {
                crate::cmd::txn::reject_no_multi(conn, &mut w);
            } else if let Some(g) = self.cluster_gate(spec, &cmd, conn) {
                // the gate again at queue time inside MULTI (§23.6): the EXEC aborts
                router::write_gate(g, &mut w);
                crate::cmd::txn::flag_queue_error(conn);
            } else {
                crate::cmd::txn::queue(conn, cmd, &mut w);
            }
            self.reply_now(conn, &mut out);
            self.x.reply = out;
            return;
        }
        if !router::allowed_while_loading(spec) && !spec.has_keys() && self.shared.loading() {
            w.error(router::LOADING);
            self.shared.command_stats[usize::from(self.id)].reject(spec.id);
            self.reply_now(conn, &mut out);
            self.x.reply = out;
            return;
        }
        if self.router.cluster && !spec.has_keys() && spec.flags.contains(CmdFlags::WRITE) {
            // keyless writes (FLUSHDB/FLUSHALL) need the primary role and a valid lease
            let lease_ok = self.shared.lease.valid(self.now.boot_ns);
            let g = self.shared.ownership.read().gate(None, spec.flags, conn.readonly, lease_ok, conn.id);
            if g != crate::cluster::ownership::Gate::Serve {
                router::write_gate(g, &mut w);
                self.shared.command_stats[usize::from(self.id)].reject(spec.id);
                self.reply_now(conn, &mut out);
                self.x.reply = out;
                return;
            }
        }
        let started = self.now.mono_ns;
        match crate::cmd::handler(spec.id) {
            Handler::Keyed(_) => self.dispatch_keyed(slot, conn, spec, cmd, &mut credit, lv, &mut out),
            Handler::Conn(f) | Handler::Cluster(f) => {
                self.run_conn_cmd(slot, conn, spec, &cmd, f, &mut out);
                self.record(spec.id, started, out.first() == Some(&b'-'));
            }
            Handler::Scan => self.dispatch_scan(slot, conn, spec, &cmd, &mut out),
            Handler::Txn => self.dispatch_txn(slot, conn, spec, cmd, &mut credit, &mut out),
        }
        self.x.reply = out;
    }

    /// Answers the current command at once with the bytes in `out` (which is left empty).
    fn reply_now(&mut self, conn: &mut Conn, out: &mut Vec<u8>) {
        if let Some(direct) = conn.direct_out() {
            direct.extend_from_slice(out);
            let n = out.len();
            conn.direct_done(n);
        } else {
            conn.reply_now(ReplyBody::Bytes(out.clone()));
        }
        out.clear();
    }

    fn record(&mut self, id: CmdId, started_ns: u64, failed: bool) {
        let us = self.now.mono_ns.saturating_sub(started_ns) / 1000;
        self.shared.command_stats[usize::from(self.id)].record(id, us, failed);
        self.x.lat.push((id, u32::try_from(us).unwrap_or(u32::MAX)));
    }

    fn env_of(&self, conn: &Conn) -> ExecEnv {
        ExecEnv { proto: conn.proto, db: conn.db, now_ms: self.now.unix_ms, readonly_conn: conn.readonly, conn_id: conn.id, watch: 0 }
    }

    #[allow(clippy::too_many_arguments)]
    fn dispatch_keyed(
        &mut self,
        slot: u32,
        conn: &mut Conn,
        spec: &'static CommandSpec,
        cmd: OwnedCmd,
        credit: &mut Option<CreditGuard>,
        lv: Reservation,
        out: &mut Vec<u8>,
    ) {
        let argv = cmd.argv();
        {
            let mut w = ReplyWriter::new(out, conn.proto);
            if !crate::cmd::precheck(spec.id, &argv, &self.cfg, &mut w) {
                drop(argv);
                self.shared.command_stats[usize::from(self.id)].reject(spec.id);
                self.reply_now(conn, out);
                return;
            }
        }
        let pos = kv_cmd::key_positions(spec, &argv);
        let env = self.env_of(conn);
        let lease_ok = self.shared.lease.valid(self.now.boot_ns);
        let ownership = if self.router.cluster { Some(self.shared.ownership.read().clone()) } else { None };
        let route = self.router.route(spec, &argv, &pos, &env, |slot| match &ownership {
            Some(o) => o.gate(slot, spec.flags, conn.readonly, lease_ok, conn.id),
            None => crate::cluster::ownership::Gate::Serve,
        });
        let shared = self.shared.clone();
        let loading = |p: u16| shared.partition_state(p) == kv_storage::partition::PartState::Loading;
        match route {
            Route::Local { partition } | Route::Remote { partition, .. } if loading(partition) => {
                drop(argv);
                ReplyWriter::new(out, conn.proto).error(router::LOADING);
                self.reply_now(conn, out);
            }
            Route::Local { partition } => {
                let req = keyed_request(spec, &argv, env.db);
                drop(argv);
                let seq = conn.reserve_slot();
                let to = ReplyTo { conn: self.conn_ref(slot), seq, part: None };
                let job = Job { to: Some(to), id: spec.id, kind: JobKind::Cmd(cmd), env, start_ns: self.now.mono_ns, _lv: lv, credit: credit.take() };
                self.submit_job(partition, req, job);
            }
            Route::Remote { worker, partition } => {
                drop(argv);
                let seq = conn.reserve_slot();
                let credit = credit.take().is_some_and(CreditGuard::transfer);
                // the large-value reservation travels with the command's buffer (§6.9)
                let msg = XMsg::Cmd { from: self.id, conn: self.conn_ref(slot), req_seq: seq, partition, cmd, env, part: None, credit, lv };
                self.send_cmd(conn, worker, msg);
            }
            Route::FanOut(split) => {
                if split.parts.iter().any(|&(p, _)| loading(p)) {
                    drop(argv);
                    ReplyWriter::new(out, conn.proto).error(router::LOADING);
                    self.reply_now(conn, out);
                    return;
                }
                let subs: Vec<(u16, OwnedCmd)> =
                    (0..split.parts.len()).map(|i| (split.parts[i].0, OwnedCmd::new(spec.id, &multikey::sub_argv(&split, i, &argv)))).collect();
                let argc = argv.len();
                drop(argv);
                self.fan_out(slot, conn, spec.id, split, argc, subs, env);
            }
            Route::XTxn => {
                drop(argv);
                let seq = conn.reserve_slot();
                let to = ReplyTo { conn: self.conn_ref(slot), seq, part: None };
                if !self.start_xtxn(to, cmd, env, credit.take()) {
                    conn.hold = Some(Hold::Order);
                }
            }
            Route::Home => {
                drop(argv);
                ReplyWriter::new(out, conn.proto).error(b"ERR internal: keyed command without a partition");
                self.reply_now(conn, out);
            }
            Route::Refused(g) => {
                drop(argv);
                router::write_gate(g, &mut ReplyWriter::new(out, conn.proto));
                self.shared.command_stats[usize::from(self.id)].reject(spec.id);
                self.reply_now(conn, out);
            }
        }
    }

    /// Sends one sub-request per partition and installs the gather (§6.8).
    #[allow(clippy::too_many_arguments)]
    fn fan_out(&mut self, slot: u32, conn: &mut Conn, id: CmdId, split: FanOutSplit, argc: usize, subs: Vec<(u16, OwnedCmd)>, env: ExecEnv) {
        let seq = conn.reserve_slot();
        let n = subs.len();
        conn.set_gather(seq, GatherSlot { id, split, argc, parts: (0..n).map(|_| None).collect(), remaining: n });
        let cref = self.conn_ref(slot);
        for (i, (partition, sub)) in subs.into_iter().enumerate() {
            let to = ReplyTo { conn: cref, seq, part: Some(i as u16) };
            let worker = self.router.workers;
            let owner = partition % worker;
            if owner == self.id {
                let req = keyed_request(id.spec(), &sub.argv(), env.db);
                let job = Job { to: Some(to), id, kind: JobKind::Cmd(sub), env, start_ns: self.now.mono_ns, _lv: Reservation::NONE, credit: None };
                self.submit_job(partition, req, job);
            } else {
                let msg = XMsg::Cmd { from: self.id, conn: cref, req_seq: seq, partition, cmd: sub, env, part: Some(i as u16), credit: false, lv: Reservation::NONE };
                self.send_cmd(conn, owner, msg);
            }
        }
    }

    fn run_conn_cmd(&mut self, slot: u32, conn: &mut Conn, spec: &'static CommandSpec, cmd: &OwnedCmd, f: crate::cmd::ConnFn, out: &mut Vec<u8>) {
        let args = Args { argv: cmd.argv() };
        let shared = self.shared.clone();
        let outcome = {
            let proto = conn.proto;
            let mut cx = ConnCtx { conn: &mut *conn, shared: &shared, worker: self.id };
            let mut w = ReplyWriter::new(out, proto);
            f(&mut cx, &args, &mut w)
        };
        conn.parser.set_auth_required(!conn.authed && !self.cfg.requirepass.is_empty());
        if conn.watch_token != 0 && conn.watch.is_empty() {
            // RESET ended the WATCH list
            self.end_watch(conn);
        }
        match outcome {
            ConnOutcome::Done => self.reply_now(conn, out),
            ConnOutcome::Close => {
                self.reply_now(conn, out);
                conn.flags |= flags::CLOSE_AFTER_REPLY;
            }
            ConnOutcome::FanOut(split) => {
                let subs: Vec<(u16, OwnedCmd)> = (0..split.parts.len())
                    .map(|i| (split.parts[i].0, OwnedCmd::new(spec.id, &multikey::sub_argv(&split, i, &args.argv))))
                    .collect();
                let env = self.env_of(conn);
                self.fan_out(slot, conn, spec.id, split, args.argv.len(), subs, env);
            }
            ConnOutcome::Async => match crate::cmd::node_op(spec.id, conn, &args.argv) {
                Some(op) => {
                    let seq = conn.reserve_slot();
                    let to = ReplyTo { conn: self.conn_ref(slot), seq, part: None };
                    self.start_node_op(Some(to), op, conn.proto, conn.id);
                }
                None => {
                    ReplyWriter::new(out, conn.proto).error(b"ERR internal: asynchronous command without an operation");
                    self.reply_now(conn, out);
                }
            },
        }
    }

    // -----------------------------------------------------------------------------------------
    // MULTI / EXEC (Handler::Txn)

    fn dispatch_txn(&mut self, slot: u32, conn: &mut Conn, spec: &'static CommandSpec, cmd: OwnedCmd, credit: &mut Option<CreditGuard>, out: &mut Vec<u8>) {
        let proto = conn.proto;
        match spec.id {
            CmdId::Multi => crate::cmd::txn::multi(conn, &mut ReplyWriter::new(out, proto)),
            CmdId::Discard => {
                crate::cmd::txn::discard(conn, &mut ReplyWriter::new(out, proto));
                self.end_watch(conn);
            }
            CmdId::Watch => {
                self.dispatch_watch(slot, conn, &cmd, out);
                return;
            }
            CmdId::Unwatch => {
                crate::cmd::txn::unwatch(conn, &mut ReplyWriter::new(out, proto));
                self.end_watch(conn);
            }
            CmdId::Exec => {
                // the WATCH session's dirty state as of now (checked again where the body runs)
                if self.shared.watch.is_dirty(conn.watch_token) {
                    conn.flags |= flags::WATCH_DIRTY;
                }
                let token = std::mem::take(&mut conn.watch_token);
                match crate::cmd::txn::exec_begin(conn, &mut ReplyWriter::new(out, proto)) {
                    crate::cmd::txn::ExecStart::Replied => self.release_watch(token),
                    crate::cmd::txn::ExecStart::Run(cmds) => {
                        // the session goes with the body: released where it runs, after its
                        // last check
                        self.exec_transaction(slot, conn, cmds, token, credit, out);
                        return;
                    }
                }
            }
            _ => ReplyWriter::new(out, proto).error(b"ERR internal: not a transaction command"),
        }
        self.reply_now(conn, out);
    }

    #[allow(clippy::too_many_arguments)]
    fn exec_transaction(&mut self, slot: u32, conn: &mut Conn, cmds: Vec<OwnedCmd>, watch: u64, credit: &mut Option<CreditGuard>, out: &mut Vec<u8>) {
        let proto = conn.proto;
        if self.router.cluster
            && let Some(g) = self.multi_gate(&cmds, conn.readonly, conn.id)
        {
            self.release_watch(watch);
            router::write_gate(g, &mut ReplyWriter::new(out, proto));
            self.reply_now(conn, out);
            return;
        }
        let partition = match multi_partition(&cmds, self.router.partitions) {
            Ok(Some(p)) => p,
            // a keyless transaction (PING, ECHO, SELECT) runs on any partition: one of ours
            Ok(None) => self.backend.owned().first().copied().unwrap_or(0),
            Err(()) => {
                self.release_watch(watch);
                ReplyWriter::new(out, proto)
                    .error(b"ERR EXEC whose keys span partitions is not supported yet (use a common hashtag)");
                self.reply_now(conn, out);
                return;
            }
        };
        if self.shared.partition_state(partition) == kv_storage::partition::PartState::Loading {
            self.release_watch(watch);
            ReplyWriter::new(out, proto).error(router::LOADING);
            self.reply_now(conn, out);
            return;
        }
        let mut env = self.env_of(conn);
        env.watch = watch;
        conn.db = db_after(&cmds, conn.db, self.cfg.databases);
        let seq = conn.reserve_slot();
        let to = ReplyTo { conn: self.conn_ref(slot), seq, part: None };
        let owner = partition % self.router.workers;
        if owner == self.id {
            let req = multi_request(&cmds, env.db);
            let job = Job { to: Some(to), id: CmdId::Exec, kind: JobKind::Multi(cmds), env, start_ns: self.now.mono_ns, _lv: Reservation::NONE, credit: credit.take() };
            self.submit_job(partition, req, job);
        } else {
            let credit = credit.take().is_some_and(CreditGuard::transfer);
            let msg = XMsg::Exec { from: self.id, conn: to.conn, req_seq: seq, partition, cmds, env, credit };
            self.send_cmd(conn, owner, msg);
        }
    }

    // -----------------------------------------------------------------------------------------
    // WATCH sessions (`runtime::watch`, §8.5)

    /// WATCH key [key ...]: each new key joins the connection's session and is registered
    /// with the partition that owns it; the reply waits until every owner registered its keys.
    fn dispatch_watch(&mut self, slot: u32, conn: &mut Conn, cmd: &OwnedCmd, out: &mut Vec<u8>) {
        let proto = conn.proto;
        let argv = cmd.argv();
        if conn.multi.is_some() {
            ReplyWriter::new(out, proto).error(b"ERR WATCH inside MULTI is not allowed");
            drop(argv);
            self.reply_now(conn, out);
            return;
        }
        if self.router.cluster {
            // the keys must be served here, in one slot (a cluster client sends WATCH where the
            // transaction will run)
            let mut slot_of = None;
            for k in &argv[1..] {
                let s = kv_common::key_slot(k);
                if slot_of.is_some_and(|x| x != s) {
                    router::write_gate(crate::cluster::ownership::Gate::CrossSlot, &mut ReplyWriter::new(out, proto));
                    drop(argv);
                    self.reply_now(conn, out);
                    return;
                }
                slot_of = Some(s);
            }
            let lease_ok = self.shared.lease.valid(self.now.boot_ns);
            let g = self.shared.ownership.read().gate(slot_of, CmdFlags::READONLY, conn.readonly, lease_ok, conn.id);
            if g != crate::cluster::ownership::Gate::Serve {
                router::write_gate(g, &mut ReplyWriter::new(out, proto));
                drop(argv);
                self.reply_now(conn, out);
                return;
            }
        }
        if conn.watch_token == 0 {
            conn.watch_token = self.shared.watch.token(self.id);
        }
        let token = conn.watch_token;
        let workers = self.router.workers;
        let mut remote: SmallVec<[(u16, Vec<crate::runtime::watch::WatchedKey>); 4]> = SmallVec::new();
        for &key in &argv[1..] {
            if conn.watch.iter().any(|k| k.db == conn.db && *k.key == *key) {
                continue;
            }
            conn.watch.push(crate::net::conn::WatchKey { db: conn.db, key: key.into() });
            let p = self.router.partition_of_key(key);
            let wk: crate::runtime::watch::WatchedKey = (p, conn.db, key.into());
            if !self.shared.watch.add(token, wk.clone()) {
                continue;
            }
            let owner = p % workers;
            if owner == self.id {
                self.backend.watch_key(p, conn.db, key);
            } else {
                match remote.iter_mut().find(|(w, _)| *w == owner) {
                    Some((_, v)) => v.push(wk),
                    None => remote.push((owner, vec![wk])),
                }
            }
        }
        drop(argv);
        if remote.is_empty() {
            ReplyWriter::new(out, proto).ok();
            self.reply_now(conn, out);
            return;
        }
        let seq = conn.reserve_slot();
        let to = ReplyTo { conn: self.conn_ref(slot), seq, part: None };
        self.x.next_watch_wait = self.x.next_watch_wait.wrapping_add(1);
        let id = self.x.next_watch_wait;
        self.x.watch_waits.insert(id, (to, proto, remote.len()));
        let ack = GatherRef { worker: self.id, id };
        for (owner, keys) in remote {
            self.send(owner, XMsg::Watch { keys, add: true, ack: Some(ack) });
        }
    }

    /// One owner registered the keys of WATCH reply `id`.
    fn watch_registered(&mut self, id: u32) {
        let Some(e) = self.x.watch_waits.get_mut(&id) else { return };
        e.2 = e.2.saturating_sub(1);
        if e.2 > 0 {
            return;
        }
        let (to, proto, _) = self.x.watch_waits.remove(&id).expect("present");
        let mut b = Vec::new();
        ReplyWriter::new(&mut b, proto).ok();
        self.deliver(to, ReplyBody::Bytes(b));
    }

    /// The connection's WATCH list ended (DISCARD, UNWATCH, RESET, close): its session goes.
    pub(super) fn end_watch(&mut self, conn: &mut Conn) {
        let token = std::mem::take(&mut conn.watch_token);
        conn.watch.clear();
        self.release_watch(token);
    }

    /// Ends WATCH session `token` and unregisters its keys with their owners (on the
    /// session's home worker: its removals follow its registrations in every queue).
    pub(super) fn release_watch(&mut self, token: u64) {
        if token == 0 {
            return;
        }
        let home = crate::runtime::watch::WatchTable::home(token);
        if home != self.id {
            self.send(home, XMsg::WatchRelease { token });
            return;
        }
        let workers = self.router.workers;
        let mut remote: SmallVec<[(u16, Vec<crate::runtime::watch::WatchedKey>); 4]> = SmallVec::new();
        for wk in self.shared.watch.release(token) {
            let owner = wk.0 % workers;
            if owner == self.id {
                self.backend.unwatch_key(wk.0, wk.1, &wk.2);
            } else {
                match remote.iter_mut().find(|(w, _)| *w == owner) {
                    Some((_, v)) => v.push(wk),
                    None => remote.push((owner, vec![wk])),
                }
            }
        }
        for (owner, keys) in remote {
            self.send(owner, XMsg::Watch { keys, add: false, ack: None });
        }
    }

    /// The watched keys partition `p` reports changed make their sessions dirty.
    pub(super) fn drain_touched(&mut self, p: u16) {
        let mut buf = std::mem::take(&mut self.x.touched);
        buf.clear();
        self.backend.take_touched(p, &mut buf);
        for (db, key) in buf.drain(..) {
            self.shared.watch.touched(p, db, &key);
        }
        self.x.touched = buf;
    }

    /// [`Worker::drain_touched`] for every owned partition (after the partitions' ticks:
    /// expiries).
    pub(super) fn drain_all_touched(&mut self) {
        let owned: SmallVec<[u16; 32]> = self.backend.owned().iter().copied().collect();
        for p in owned {
            self.drain_touched(p);
        }
    }

    // -----------------------------------------------------------------------------------------
    // SCAN / KEYS / RANDOMKEY (Handler::Scan)

    fn dispatch_scan(&mut self, slot: u32, conn: &mut Conn, spec: &'static CommandSpec, cmd: &OwnedCmd, out: &mut Vec<u8>) {
        let argv = cmd.argv();
        let proto = conn.proto;
        let partitions = self.router.partitions;
        let db = conn.db;
        let (call, step) = match spec.id {
            CmdId::Scan => {
                let mut w = ReplyWriter::new(out, proto);
                let Some(a) = crate::cmd::scan::parse_scan(&argv, &mut w) else {
                    drop(argv);
                    self.reply_now(conn, out);
                    return;
                };
                match crate::cmd::scan::plan_scan(db, &a, partitions) {
                    crate::cmd::scan::ScanPlan::Reply { next } => {
                        crate::cmd::scan::write_scan_reply(next, &[], &mut w);
                        drop(argv);
                        self.reply_now(conn, out);
                        return;
                    }
                    crate::cmd::scan::ScanPlan::Step { partition, request, pattern } => {
                        (ScanCall::Scan, (StepKind::Scan, partition, request, pattern.map(Box::from)))
                    }
                }
            }
            CmdId::Keys => {
                let walk = crate::cmd::scan::KeysWalk::new(db, partitions);
                let pattern: Option<Box<[u8]>> = Some(argv[1].into());
                let Some((partition, request)) = walk.next_step() else {
                    crate::cmd::scan::write_keys_reply(&[], &mut ReplyWriter::new(out, proto));
                    drop(argv);
                    self.reply_now(conn, out);
                    return;
                };
                let step = (StepKind::Keys, partition, request, pattern.clone());
                (ScanCall::Keys { walk, keys: Vec::new(), pattern }, step)
            }
            _ => {
                let r = self.x.next_random();
                match crate::cmd::scan::random_partition(&self.shared, db, r) {
                    None => {
                        crate::cmd::scan::write_randomkey_reply(None, &mut ReplyWriter::new(out, proto));
                        drop(argv);
                        self.reply_now(conn, out);
                        return;
                    }
                    Some(partition) => {
                        (ScanCall::RandomKey { db, attempts: 1 }, (StepKind::RandomKey, partition, crate::cmd::scan::randomkey_request(db, r), None))
                    }
                }
            }
        };
        drop(argv);
        let seq = conn.reserve_slot();
        let cref = self.conn_ref(slot);
        self.x.scans.insert((cref.slot, cref.generation, seq), call);
        let (kind, partition, request, pattern) = step;
        self.scan_step(ReplyTo { conn: cref, seq, part: None }, kind, partition, request, pattern);
    }

    /// Runs one step on the partition's owner (LC-18: one step at a time, on one thread).
    fn scan_step(&mut self, home: ReplyTo, kind: StepKind, partition: u16, request: Request, pattern: Option<Box<[u8]>>) {
        let owner = partition % self.router.workers;
        if owner == self.id {
            let env = ExecEnv { proto: kv_resp::Proto::Resp2, db: request.db, now_ms: self.now.unix_ms, readonly_conn: false, conn_id: 0, watch: 0 };
            let job = Job { to: None, id: CmdId::Scan, kind: JobKind::Scan { pattern, home }, env, start_ns: self.now.mono_ns, _lv: Reservation::NONE, credit: None };
            self.submit_job(partition, request, job);
        } else {
            let req = ScanReq { kind, partition, request, pattern };
            self.send(owner, XMsg::Scan { from: self.id, conn: home.conn, req_seq: home.seq, req });
        }
    }

    /// A step finished (home side): reply, or run the next step.
    fn scan_done(&mut self, conn: ConnRef, seq: u64, out: ScanOutcome) {
        let key = (conn.slot, conn.generation, seq);
        let Some(call) = self.x.scans.remove(&key) else { return };
        let to = ReplyTo { conn, seq, part: None };
        let proto = self.conn_mut(conn.slot, conn.generation).map_or(kv_resp::Proto::Resp2, |c| c.proto);
        let (next, mut keys) = match out.res {
            Ok(x) => x,
            Err(e) => {
                self.deliver(to, err_body(e, proto));
                return;
            }
        };
        let mut body = Vec::new();
        match call {
            ScanCall::Scan => {
                let next = crate::cmd::scan::client_cursor(next, self.router.partitions);
                crate::cmd::scan::write_scan_reply(next, &keys, &mut ReplyWriter::new(&mut body, proto));
            }
            ScanCall::Keys { mut walk, keys: mut acc, pattern } => {
                walk.on_step_done(next);
                acc.append(&mut keys);
                if let Some((partition, request)) = walk.next_step() {
                    self.x.scans.insert(key, ScanCall::Keys { walk, keys: acc, pattern: pattern.clone() });
                    self.scan_step(to, StepKind::Keys, partition, request, pattern);
                    return;
                }
                crate::cmd::scan::write_keys_reply(&acc, &mut ReplyWriter::new(&mut body, proto));
            }
            ScanCall::RandomKey { db, attempts } => {
                if keys.is_empty() && attempts < crate::cmd::scan::RANDOMKEY_ATTEMPTS {
                    let r = self.x.next_random();
                    if let Some(partition) = crate::cmd::scan::random_partition(&self.shared, db, r) {
                        self.x.scans.insert(key, ScanCall::RandomKey { db, attempts: attempts + 1 });
                        self.scan_step(to, StepKind::RandomKey, partition, crate::cmd::scan::randomkey_request(db, r), None);
                        return;
                    }
                }
                crate::cmd::scan::write_randomkey_reply(keys.first().map(|k| &**k), &mut ReplyWriter::new(&mut body, proto));
            }
        }
        self.deliver(to, ReplyBody::Bytes(body));
    }

    // -----------------------------------------------------------------------------------------
    // partition requests (owner side)

    /// Submits `req` to owned partition `p` with its continuation; while a cross-partition
    /// transaction holds `p`, new requests wait in FIFO order (§6.8 step 2).
    pub(crate) fn submit_job(&mut self, p: u16, req: Request, job: Job) {
        if !self.backend.owned().contains(&p) {
            tracing::error!(worker = self.id, partition = p, "request for a partition of another worker");
            if let Some(to) = job.to {
                let mut b = Vec::new();
                ReplyWriter::new(&mut b, job.env.proto).error(b"ERR internal: misrouted request");
                self.deliver(to, ReplyBody::Bytes(b));
            }
            return;
        }
        if let Some(h) = self.x.holds.get_mut(&p) {
            let own = match &job.kind {
                JobKind::Lock { xtxn, .. } | JobKind::Apply { xtxn, .. } => *xtxn == h.xtxn,
                _ => false,
            };
            if !own {
                h.queue.push_back(HoldItem::Job(req, job));
                return;
            }
        }
        let id = self.backend.submit(p, self.now, req);
        self.x.jobs.insert((p, id.0), job);
    }

    /// Step 4 of the tick: executes every ready request of every owned partition, until no
    /// partition has a ready request left.
    pub(super) fn execute_ready(&mut self) {
        let owned: SmallVec<[u16; 32]> = self.backend.owned().iter().copied().collect();
        self.poll_gate();
        for _round in 0..64 {
            let mut any = false;
            for &p in &owned {
                let mut ready = std::mem::take(&mut self.x.ready);
                ready.clear();
                self.backend.take_ready(p, &mut ready);
                any |= !ready.is_empty();
                for &id in &ready {
                    self.run_job(p, id);
                }
                self.x.ready = ready;
            }
            if !any {
                break;
            }
        }
    }

    fn run_job(&mut self, p: u16, id: ReqId) {
        let Some(job) = self.x.jobs.remove(&(p, id.0)) else {
            tracing::error!(worker = self.id, partition = p, "ready request without a job");
            return;
        };
        if let JobKind::Scan { .. } = job.kind {
            return self.run_scan(p, id, job);
        }
        if let JobKind::Repl(rj) = &job.kind
            && matches!(rj.work, crate::runtime::backend::ReplWork::Walk)
        {
            return self.run_repl_walk(p, id, job);
        }
        if self.router.cluster
            && let Some(g) = self.owner_gate(&job)
        {
            // the role or the slot changed since dispatch: the command does not run here
            let mut reply = std::mem::take(&mut self.x.reply);
            reply.clear();
            let st = self.backend.exec(p, self.now, id, Work::Nothing, &job.env, &mut reply);
            if matches!(st, ExecStatus::Requeued) {
                self.x.jobs.insert((p, id.0), job);
                self.x.reply = reply;
                return;
            }
            reply.clear();
            if let JobKind::Multi(_) = job.kind {
                self.release_watch(job.env.watch);
            }
            router::write_gate(g, &mut ReplyWriter::new(&mut reply, job.env.proto));
            if let Some(to) = job.to {
                self.deliver(to, ReplyBody::Bytes(std::mem::take(&mut reply)));
            }
            self.x.reply = reply;
            return;
        }
        let watch = match &job.kind {
            JobKind::Multi(_) => job.env.watch,
            _ => 0,
        };
        if watch != 0 && self.shared.watch.is_dirty(watch) {
            // a watched key changed: the body never runs (EXEC answers a null array)
            let mut reply = std::mem::take(&mut self.x.reply);
            reply.clear();
            let st = self.backend.exec(p, self.now, id, Work::Nothing, &job.env, &mut reply);
            if matches!(st, ExecStatus::Requeued) {
                self.x.jobs.insert((p, id.0), job);
                self.x.reply = reply;
                return;
            }
            self.release_watch(watch);
            reply.clear();
            ReplyWriter::new(&mut reply, job.env.proto).null_array();
            self.record(job.id, job.start_ns, false);
            if let Some(to) = job.to {
                self.deliver(to, ReplyBody::Bytes(std::mem::take(&mut reply)));
            }
            self.x.reply = reply;
            return;
        }
        let mut reply = std::mem::take(&mut self.x.reply);
        reply.clear();
        let databases = self.cfg.databases;
        let txns_before = self.backend.txns_opened(p);
        if self.router.cluster {
            let imaging = self.cluster_imaging(p);
            self.backend.set_imaging(imaging);
        }
        let status = {
            let work = match &job.kind {
                JobKind::Cmd(c) => Work::Cmd(c),
                JobKind::Multi(cmds) => Work::Multi { cmds, databases },
                JobKind::Lock { keys, .. } => Work::Reads(keys),
                JobKind::Apply { ops, .. } => Work::Ops(ops),
                JobKind::Repl(rj) => Work::Repl(&rj.work),
                JobKind::Node { .. } | JobKind::Scan { .. } => Work::Nothing,
            };
            self.backend.exec(p, self.now, id, work, &job.env, &mut reply)
        };
        if !matches!(status, ExecStatus::Requeued) {
            // the changes it committed reach the WATCH sessions before any reply is released
            self.drain_touched(p);
            self.release_watch(watch);
        }
        match status {
            ExecStatus::Requeued => {
                self.x.jobs.insert((p, id.0), job);
            }
            ExecStatus::Done(out, eo) => {
                let mut job = job;
                if self.backend.txns_opened(p) != txns_before {
                    // the partition opened a txn: its resolution returns the credit
                    if let Some(c) = job.credit.take() {
                        c.spend();
                    }
                }
                self.job_done(p, job, out, &eo, &mut reply);
            }
            ExecStatus::Failed(e) => self.job_failed(p, job, e),
        }
        self.x.reply = reply;
    }

    fn job_done(&mut self, p: u16, job: Job, out: WorkOut, eo: &kv_storage::partition::ExecOut, reply: &mut Vec<u8>) {
        let mode = self.cfg.effective_durability();
        let ticket = eo.ticket;
        if !self.x.executed.contains(&p) {
            self.x.executed.push(p);
        }
        if let JobKind::Repl(rj) = job.kind {
            let res = match out {
                WorkOut::Repl(o) => Ok(o),
                _ => Ok(crate::runtime::backend::ReplOut::None),
            };
            self.repl_job_done(p, rj, res, Some(eo));
            return;
        }
        // cluster primary: the group goes to the members; replies wait for the shard commit
        let commit = match (&eo.group, self.cluster.is_some()) {
            (Some(g), true) => self.cluster_group(p, g, ticket).map(|s| (p, s)),
            _ => None,
        };
        match job.kind {
            JobKind::Cmd(_) | JobKind::Multi(_) => {
                self.record(job.id, job.start_ns, reply.first() == Some(&b'-'));
                let Some(to) = job.to else { return };
                let body = ReplyBody::Bytes(std::mem::take(reply));
                match (ticket_need(mode, job.id, ticket.is_some()), ticket) {
                    (Some(need), Some(t)) => self.wait(p, t.0, Waiter { what: WaitFor::Reply { to, body, commit }, need }),
                    _ => match commit {
                        Some((cp, s)) => self.commit_wait(cp, s, CommitWaiter::Reply(to, body)),
                        // released after this tick's partition ticks (see `release_executed`)
                        None => self.x.held.push(Held::Reply(to, body)),
                    },
                }
            }
            JobKind::Lock { xtxn, coord, .. } => {
                let reads = match out {
                    WorkOut::Reads(r) => Ok(r),
                    _ => Ok(Vec::new()),
                };
                self.send(coord, XMsg::XTxn(XTxnMsg::Locked { xtxn, partition: p, reads }));
            }
            JobKind::Apply { xtxn, coord, .. } => match ticket {
                // the commit needs every TXN record written first (§14.6 step 3)
                Some(t) => self.wait(p, t.0, Waiter { what: WaitFor::Apply { xtxn, coord, partition: p }, need: Need::Written }),
                None => {
                    self.send(coord, XMsg::XTxn(XTxnMsg::Applied { xtxn, partition: p, res: Ok(()) }));
                }
            },
            JobKind::Node { gather } => match ticket {
                // FLUSH replies after the cutoff is durable (E5); barriers after every effect is
                Some(t) => self.wait(p, t.0, Waiter { what: WaitFor::Node { gather, commit }, need: Need::Durable }),
                None => match commit {
                    Some((cp, s)) => self.commit_wait(cp, s, CommitWaiter::Node(gather)),
                    None => self.x.held.push(Held::NodePart(gather)),
                },
            },
            JobKind::Scan { .. } | JobKind::Repl(_) => {}
        }
    }

    /// A commit waiter is released: `refused` replaces the reply with that error (the write
    /// was not acknowledged).
    pub(crate) fn commit_released(&mut self, w: CommitWaiter, refused: Option<&[u8]>) {
        match w {
            CommitWaiter::Reply(to, body) => {
                let body = match refused {
                    Some(e) => {
                        let mut b = Vec::with_capacity(e.len() + 3);
                        ReplyWriter::new(&mut b, kv_resp::Proto::Resp2).error(e);
                        ReplyBody::Bytes(b)
                    }
                    None => body,
                };
                self.deliver(to, body);
            }
            CommitWaiter::Node(gather) => {
                let err = refused.map(|_| StoreError::ReadOnly);
                self.node_part_done(gather, err);
            }
        }
    }

    /// The cluster gate of a command at queue time inside MULTI (`None`: queue it).
    fn cluster_gate(&self, spec: &'static CommandSpec, cmd: &OwnedCmd, conn: &Conn) -> Option<crate::cluster::ownership::Gate> {
        if !self.router.cluster || !spec.has_keys() {
            return None;
        }
        let argv = cmd.argv();
        let pos = kv_cmd::key_positions(spec, &argv);
        if pos.is_empty() {
            return None;
        }
        let slot = match kv_cmd::common_slot(&argv, &pos) {
            Ok(s) => s,
            Err(_) => return Some(crate::cluster::ownership::Gate::CrossSlot),
        };
        let lease_ok = self.shared.lease.valid(self.now.boot_ns);
        let g = self.shared.ownership.read().gate(slot, spec.flags, conn.readonly, lease_ok, conn.id);
        (g != crate::cluster::ownership::Gate::Serve).then_some(g)
    }

    /// The slot and flags of an EXEC body: one slot (else CROSSSLOT), WRITE if any command
    /// writes.
    fn multi_slot(cmds: &[OwnedCmd]) -> Result<(Option<u16>, CmdFlags), ()> {
        let mut slot = None;
        let mut flags = CmdFlags::READONLY;
        for c in cmds {
            let spec = c.id.spec();
            if spec.flags.contains(CmdFlags::WRITE) {
                flags = CmdFlags::WRITE;
            }
            let argv = c.argv();
            for i in kv_cmd::key_positions(spec, &argv) {
                let s = kv_common::key_slot(argv[usize::from(i)]);
                match slot {
                    None => slot = Some(s),
                    Some(x) if x != s => return Err(()),
                    Some(_) => {}
                }
            }
        }
        Ok((slot, flags))
    }

    /// The cluster gate of an EXEC body at EXEC time.
    fn multi_gate(&self, cmds: &[OwnedCmd], readonly_conn: bool, conn_id: u64) -> Option<crate::cluster::ownership::Gate> {
        let (slot, flags) = match Self::multi_slot(cmds) {
            Ok(x) => x,
            Err(()) => return Some(crate::cluster::ownership::Gate::CrossSlot),
        };
        slot?;
        let lease_ok = self.shared.lease.valid(self.now.boot_ns);
        let g = self.shared.ownership.read().gate(slot, flags, readonly_conn, lease_ok, conn_id);
        (g != crate::cluster::ownership::Gate::Serve).then_some(g)
    }

    /// The gate again on the owner, right before a client command executes (cluster mode).
    fn owner_gate(&self, job: &Job) -> Option<crate::cluster::ownership::Gate> {
        let (slot, flags) = match &job.kind {
            JobKind::Cmd(c) => {
                let spec = c.id.spec();
                let argv = c.argv();
                let pos = kv_cmd::key_positions(spec, &argv);
                let slot = pos.first().map(|&i| kv_common::key_slot(argv[usize::from(i)]));
                (slot, spec.flags)
            }
            JobKind::Multi(cmds) => match Self::multi_slot(cmds) {
                Ok(x) => x,
                Err(()) => return Some(crate::cluster::ownership::Gate::CrossSlot),
            },
            _ => return None,
        };
        slot?;
        self.owner_refuses(flags, slot, &job.env)
    }

    /// A walk step (`SlotIter`) of the cluster glue.
    fn run_repl_walk(&mut self, p: u16, id: ReqId, job: Job) {
        let mut keys = Vec::new();
        let status = self.backend.scan(p, self.now, id, &|_| true, &mut keys);
        match status {
            ExecStatus::Requeued => {
                self.x.jobs.insert((p, id.0), job);
            }
            ExecStatus::Done(next, _) => {
                if let JobKind::Repl(rj) = job.kind {
                    self.repl_job_done(p, rj, Ok(crate::runtime::backend::ReplOut::Listed { next, keys }), None);
                }
            }
            ExecStatus::Failed(e) => {
                if let JobKind::Repl(rj) = job.kind {
                    self.repl_job_done(p, rj, Err(e), None);
                }
            }
        }
    }

    /// After the partitions' ticks of this loop turn (§6.4 step 4): publishes the key counts of
    /// every partition that executed a request, then releases the results held for it, so
    /// that a DBSIZE issued after a reply always counts that reply's write.
    pub(super) fn release_executed(&mut self) {
        for i in 0..self.x.executed.len() {
            let p = self.x.executed[i];
            self.shared.counters[usize::from(p)].publish_keys(self.backend.counters(p));
        }
        self.x.executed.clear();
        let mut held = std::mem::take(&mut self.x.held);
        for h in held.drain(..) {
            match h {
                Held::Reply(to, body) => self.deliver(to, body),
                Held::NodePart(gather) => self.node_part_done(gather, None),
            }
        }
        if self.x.held.is_empty() {
            self.x.held = held;
        }
    }

    fn job_failed(&mut self, p: u16, job: Job, e: StoreError) {
        match job.kind {
            JobKind::Cmd(_) | JobKind::Multi(_) => {
                self.record(job.id, job.start_ns, true);
                if let Some(to) = job.to {
                    self.deliver(to, err_body(e, job.env.proto));
                }
            }
            JobKind::Lock { xtxn, coord, .. } => {
                self.send(coord, XMsg::XTxn(XTxnMsg::Locked { xtxn, partition: p, reads: Err(e) }));
            }
            JobKind::Apply { xtxn, coord, .. } => {
                self.send(coord, XMsg::XTxn(XTxnMsg::Applied { xtxn, partition: p, res: Err(e) }));
            }
            JobKind::Node { gather } => self.node_part_done(gather, Some(e)),
            JobKind::Scan { home, .. } => self.send_scan_result(home, ScanOutcome { res: Err(e) }),
            JobKind::Repl(rj) => self.repl_job_done(p, rj, Err(e), None),
        }
    }

    fn run_scan(&mut self, p: u16, id: ReqId, job: Job) {
        let JobKind::Scan { pattern, home, .. } = &job.kind else { return };
        let home = *home;
        let mut keys = Vec::new();
        let status = {
            let m = crate::dispatch::scan::matcher(pattern.as_deref());
            // cluster mode: only the keys of slots this node serves (an inert migration copy
            // or slots being purged are never listed, §23.13 step 5, §23.14)
            let filter = self.router.cluster.then(|| self.shared.ownership.read().slot_filter());
            let f = |k: &[u8]| filter.as_ref().is_none_or(|f| f.serves(kv_common::key_slot(k))) && m(k);
            self.backend.scan(p, self.now, id, &f, &mut keys)
        };
        match status {
            ExecStatus::Requeued => {
                self.x.jobs.insert((p, id.0), job);
            }
            ExecStatus::Done(next, _) => self.send_scan_result(home, ScanOutcome { res: Ok((next, keys)) }),
            ExecStatus::Failed(e) => self.send_scan_result(home, ScanOutcome { res: Err(e) }),
        }
    }

    fn send_scan_result(&mut self, home: ReplyTo, out: ScanOutcome) {
        if home.conn.worker == self.id {
            self.scan_done(home.conn, home.seq, out);
        } else {
            self.send(home.conn.worker, XMsg::ScanDone { conn: home.conn, req_seq: home.seq, out });
        }
    }

    fn wait(&mut self, p: u16, ticket: u64, w: Waiter) {
        self.x.waits.entry((p, ticket)).or_default().push(w);
    }

    /// Step 6: releases every wait whose ticket reached the state it needs.
    pub(super) fn release_tickets(&mut self) {
        let owned: SmallVec<[u16; 32]> = self.backend.owned().iter().copied().collect();
        for p in owned {
            let mut tickets = std::mem::take(&mut self.x.tickets);
            tickets.clear();
            self.backend.take_tickets(p, &mut tickets);
            if self.cluster.is_some() {
                for &(t, st) in &tickets {
                    self.cluster_ticket(p, t, st);
                }
            }
            for &(t, st) in &tickets {
                let Some(ws) = self.x.waits.get_mut(&(p, t.0)) else { continue };
                let mut done: SmallVec<[Waiter; 1]> = SmallVec::new();
                let mut i = 0;
                while i < ws.len() {
                    if satisfied(ws[i].need, st) {
                        done.push(ws.swap_remove(i));
                    } else {
                        i += 1;
                    }
                }
                if ws.is_empty() {
                    self.x.waits.remove(&(p, t.0));
                }
                let failed = match st {
                    TicketState::Failed(errno) => Some(StoreError::Io(errno)),
                    _ => None,
                };
                for w in done {
                    self.wait_done(w, failed);
                }
            }
            self.x.tickets = tickets;
        }
    }

    fn wait_done(&mut self, w: Waiter, failed: Option<StoreError>) {
        match w.what {
            WaitFor::Reply { to, body, commit } => match (failed, commit) {
                (Some(e), _) => self.deliver(to, err_body(e, kv_resp::Proto::Resp2)),
                (None, Some((p, s))) => self.commit_wait(p, s, CommitWaiter::Reply(to, body)),
                (None, None) => self.deliver(to, body),
            },
            WaitFor::Node { gather, commit } => match (failed, commit) {
                (None, Some((p, s))) => self.commit_wait(p, s, CommitWaiter::Node(gather)),
                (f, _) => self.node_part_done(gather, f),
            },
            WaitFor::Apply { xtxn, coord, partition } => {
                let res = failed.map_or(Ok(()), Err);
                self.send(coord, XMsg::XTxn(XTxnMsg::Applied { xtxn, partition, res }));
            }
        }
    }

    // -----------------------------------------------------------------------------------------
    // cross-partition transactions (§6.8)

    /// Plans and queues a cross-partition transaction at the gate. Returns whether its `Lock`
    /// messages already went out (or it ended); otherwise the caller holds the connection
    /// (`Hold::Order`) until they do, so that no later command of the connection reaches one
    /// of its partitions first (§6.6).
    fn start_xtxn(&mut self, to: ReplyTo, cmd: OwnedCmd, env: ExecEnv, credit: Option<CreditGuard>) -> bool {
        self.x.next_xtxn += 1;
        let xtxn = (u64::from(self.id) << 40) | self.x.next_xtxn;
        let router = self.router;
        let id = cmd.id;
        let Some(mut coord) = XTxnCoord::new(xtxn, 0, cmd, env, |k| router.partition_of_key(k)) else {
            let mut b = Vec::new();
            ReplyWriter::new(&mut b, env.proto).error(b"ERR internal: not a cross-partition transaction command");
            self.record(id, self.now.mono_ns, true);
            self.deliver(to, ReplyBody::Bytes(b));
            return true;
        };
        // the gate ticket is taken only for a transaction that will run (it must be released
        // in turn)
        coord.ticket = self.shared.xtxn.ticket();
        self.x.coords.insert(xtxn, Coord { coord, to, start_ns: self.now.mono_ns, credit, started: false, holds_conn: false });
        self.x.gate_q.push_back(xtxn);
        self.poll_gate();
        match self.x.coords.get_mut(&xtxn) {
            Some(c) if !c.started => {
                c.holds_conn = true;
                false
            }
            _ => true,
        }
    }

    /// Starts the transaction at the head of this worker's gate queue when the gate serves it.
    fn poll_gate(&mut self) {
        while let Some(&xtxn) = self.x.gate_q.front() {
            let Some(c) = self.x.coords.get(&xtxn) else {
                self.x.gate_q.pop_front();
                continue;
            };
            if !self.shared.xtxn.is_turn(c.coord.ticket) {
                return;
            }
            self.x.gate_q.pop_front();
            let me = self.id;
            if c.coord.partitions().next().is_none() {
                self.xtxn_step(xtxn, |c| c.abort(b"ERR internal: empty transaction"));
                continue;
            }
            self.xtxn_step(xtxn, |c| c.start(me));
            // the locks are on their way: the connection's later commands queue behind them
            if let Some(c) = self.x.coords.get_mut(&xtxn) {
                c.started = true;
                if std::mem::take(&mut c.holds_conn) {
                    let conn = c.to.conn;
                    self.lift_hold(conn, Hold::Order);
                }
            }
        }
    }

    /// Lifts hold `h` of a connection of this worker (if it still holds it).
    fn lift_hold(&mut self, conn: ConnRef, h: Hold) {
        let slot = conn.slot;
        let Some(c) = self.conn_mut(slot, conn.generation) else { return };
        if c.hold == Some(h) {
            c.hold = None;
            self.queue_parse(slot);
        }
    }

    /// Carries a participant-side transaction message to partition `p`: at once when this
    /// worker owns `p` (so that the partition is locked before any later command of the
    /// coordinating connection is submitted to it), otherwise through the worker's queue.
    fn xtxn_send(&mut self, p: u16, m: XTxnMsg) {
        let dst = p % self.router.workers;
        if dst == self.id {
            self.on_xtxn(m);
        } else {
            self.send(dst, XMsg::XTxn(m));
        }
    }

    /// Advances transaction `xtxn` with `f` and carries out the steps it asks for.
    fn xtxn_step(&mut self, xtxn: u64, f: impl FnOnce(&mut XTxnCoord) -> Step) {
        let Some(c) = self.x.coords.get_mut(&xtxn) else { return };
        let mut step = f(&mut c.coord);
        loop {
            match step {
                Step::Wait => return,
                Step::Send(msgs) => {
                    for (p, m) in msgs {
                        self.xtxn_send(p, m);
                    }
                    return;
                }
                Step::NeedTxnId => {
                    let id = self.backend.next_txn_id();
                    let Some(c) = self.x.coords.get_mut(&xtxn) else { return };
                    step = match id {
                        Some(t) => {
                            // the id's resolution returns the credit
                            if let Some(cr) = c.credit.take() {
                                cr.spend();
                            }
                            c.coord.apply(t)
                        }
                        None => c.coord.abort(b"ERR cross-partition transactions are not available: no transaction id (use a common hashtag)"),
                    };
                }
                Step::Commit { txn_id } => {
                    let p = self.backend.owned().first().copied().unwrap_or(0);
                    self.backend.meta_request(p, WORKER_TOKEN_BIT | xtxn, MetaReq::CommitTxn { txn: txn_id });
                    return;
                }
                Step::Finish { msgs, reply } => {
                    for (p, m) in msgs {
                        self.xtxn_send(p, m);
                    }
                    let Some(c) = self.x.coords.remove(&xtxn) else { return };
                    self.shared.xtxn.release(c.coord.ticket);
                    self.shared.wake_all();
                    self.record(c.coord.cmd.id, c.start_ns, reply.first() == Some(&b'-'));
                    self.deliver(c.to, ReplyBody::Bytes(reply));
                    return;
                }
            }
        }
    }

    fn on_xtxn(&mut self, m: XTxnMsg) {
        match m {
            XTxnMsg::Lock { xtxn, from, partition, db, keys } => {
                if let Some(h) = self.x.holds.get_mut(&partition)
                    && h.xtxn != xtxn
                {
                    // the previous transaction still holds the partition
                    h.queue.push_back(HoldItem::Lock(XTxnMsg::Lock { xtxn, from, partition, db, keys }));
                    return;
                }
                self.x.holds.insert(partition, PartHold { xtxn, coord: from, queue: VecDeque::new() });
                let req = Request {
                    db,
                    keys: keys.iter().map(|k| (k.key.clone(), k.access)).collect(),
                    kind: ReqKind::Keyed,
                    origin: Origin::Client,
                };
                let env = ExecEnv { proto: kv_resp::Proto::Resp2, db, now_ms: self.now.unix_ms, readonly_conn: false, conn_id: 0, watch: 0 };
                let job = Job { to: None, id: CmdId::Mset, kind: JobKind::Lock { xtxn, coord: from, keys }, env, start_ns: self.now.mono_ns, _lv: Reservation::NONE, credit: None };
                self.submit_job(partition, req, job);
            }
            XTxnMsg::Apply { xtxn, txn_id, partition, ops } => {
                let coord = self.x.holds.get(&partition).map_or(xtxn_worker(xtxn), |h| h.coord);
                let db = ops.iter().find_map(op_db).unwrap_or(0);
                let keys = ops.iter().filter_map(op_key).map(|k| (Box::<[u8]>::from(k), kv_cmd::Access::Write)).collect();
                let req = Request { db, keys, kind: ReqKind::Txn { txn_id }, origin: Origin::Client };
                let env = ExecEnv { proto: kv_resp::Proto::Resp2, db, now_ms: self.now.unix_ms, readonly_conn: false, conn_id: 0, watch: 0 };
                let job = Job { to: None, id: CmdId::Mset, kind: JobKind::Apply { xtxn, coord, ops }, env, start_ns: self.now.mono_ns, _lv: Reservation::NONE, credit: None };
                self.submit_job(partition, req, job);
            }
            XTxnMsg::Commit { xtxn, txn_id, partition } => {
                self.backend.on_meta(partition, self.now, MetaDone::Committed { txn: txn_id });
                self.release_hold(partition, xtxn);
            }
            XTxnMsg::Unlock { xtxn, partition } => self.release_hold(partition, xtxn),
            XTxnMsg::Locked { xtxn, partition, reads } => self.xtxn_step(xtxn, |c| c.on_locked(partition, reads)),
            XTxnMsg::Applied { xtxn, partition, res } => self.xtxn_step(xtxn, |c| c.on_applied(partition, res)),
        }
    }

    /// Ends transaction `xtxn`'s hold of `p`: what arrived meanwhile goes in, in order (a
    /// later transaction's `Lock` takes the partition at its place in that order).
    fn release_hold(&mut self, p: u16, xtxn: u64) {
        if self.x.holds.get(&p).is_none_or(|h| h.xtxn != xtxn) {
            return;
        }
        let Some(h) = self.x.holds.remove(&p) else { return };
        for item in h.queue {
            match item {
                HoldItem::Job(req, job) => self.submit_job(p, req, job),
                HoldItem::Lock(m) => self.on_xtxn(m),
            }
        }
    }

    // -----------------------------------------------------------------------------------------
    // node operations (FLUSH*, barriers, SHUTDOWN, CLIENT LIST/KILL)

    fn start_node_op(&mut self, to: Option<ReplyTo>, op: NodeOp, proto: kv_resp::Proto, caller: u64) {
        let mut to = to;
        if op.replies_at_start()
            && let Some(t) = to.take()
        {
            let mut b = Vec::new();
            crate::cmd::server::write_node_op_reply(&op, NodeOpDone::Ok, &mut ReplyWriter::new(&mut b, proto));
            self.deliver(t, ReplyBody::Bytes(b));
        }
        let workers = self.router.workers;
        let id = match self.x.free_gathers.pop() {
            Some(i) => i,
            None => {
                self.x.gathers.push(None);
                (self.x.gathers.len() - 1) as u32
            }
        };
        let gather = GatherRef { worker: self.id, id };
        let shared_op = Arc::new(op.clone());
        self.x.gathers[id as usize] = Some(HomeGather { op, to, remaining: workers, err: None, lines: Vec::new(), killed: 0 });
        for w in 0..workers {
            let (msg, ordered) = match &*shared_op {
                NodeOp::Flush { db } => (XMsg::Flush { db: *db, gather }, true),
                NodeOp::Barrier(_) | NodeOp::Shutdown { .. } => (XMsg::Barrier { gather }, true),
                NodeOp::ClientList(_) | NodeOp::ClientKill { .. } => (XMsg::Clients { gather, op: shared_op.clone(), caller }, false),
            };
            if w == self.id && ordered {
                // this worker's partitions take their part before any later command of the
                // connection (other workers receive it ahead of them in their queues, §6.6)
                self.handle_message(msg);
            } else {
                self.send(w, msg);
            }
        }
    }

    /// This worker's share of a FLUSH / barrier: one request per owned partition.
    fn node_part_start(&mut self, gather: GatherRef, kind: ReqKind) {
        let owned: SmallVec<[u16; 32]> = self.backend.owned().iter().copied().collect();
        if owned.is_empty() {
            self.node_part_reply(gather, None);
            return;
        }
        self.x.node_parts.insert(gather, NodePart { remaining: owned.len(), err: None });
        for p in owned {
            let db = match kind {
                ReqKind::FlushDb { db } => db.unwrap_or(0),
                _ => 0,
            };
            let req = Request { db, keys: SmallVec::new(), kind, origin: Origin::Client };
            let env = ExecEnv { proto: kv_resp::Proto::Resp2, db, now_ms: self.now.unix_ms, readonly_conn: false, conn_id: 0, watch: 0 };
            let id = if matches!(kind, ReqKind::FlushDb { .. }) { CmdId::Flushdb } else { CmdId::Save };
            let job = Job { to: None, id, kind: JobKind::Node { gather }, env, start_ns: self.now.mono_ns, _lv: Reservation::NONE, credit: None };
            self.submit_job(p, req, job);
        }
    }

    fn node_part_done(&mut self, gather: GatherRef, err: Option<StoreError>) {
        let Some(np) = self.x.node_parts.get_mut(&gather) else { return };
        np.remaining -= 1;
        if np.err.is_none() {
            np.err = err;
        }
        if np.remaining > 0 {
            return;
        }
        let np = self.x.node_parts.remove(&gather).expect("present");
        self.node_part_reply(gather, np.err);
    }

    fn node_part_reply(&mut self, gather: GatherRef, err: Option<StoreError>) {
        if gather.id == DRAIN_GATHER {
            return;
        }
        let res = err.map_or(Ok(()), Err);
        self.send(gather.worker, XMsg::GatherDone { gather, from: self.id, res });
    }

    /// CLIENT LIST / KILL over this worker's connections.
    fn clients_part(&mut self, gather: GatherRef, op: &NodeOp, caller: u64) {
        let mut lines = Vec::new();
        let mut killed = 0u64;
        let now = self.now.boot_ns;
        let mut kill: SmallVec<[u32; 4]> = SmallVec::new();
        for (slot, c) in self.conns.iter().enumerate() {
            let Some(c) = c else { continue };
            match op {
                NodeOp::ClientList(f) if f.matches(c.id) => {
                    let current = (c.id == caller).then_some(CmdId::ClientList);
                    crate::cmd::conn_cmds::write_client_line(c, current, now, &mut lines);
                }
                NodeOp::ClientKill { filter, caller, .. } if filter.matches_conn(c, *caller) => {
                    kill.push(slot as u32);
                    killed += 1;
                }
                _ => {}
            }
        }
        for slot in kill {
            if let Some(c) = self.conns[slot as usize].as_mut() {
                // a killed connection still gets the replies already produced
                c.flags |= flags::CLOSE_AFTER_REPLY;
                c.consumed = c.filled();
            }
            self.maybe_close(slot);
        }
        self.send(gather.worker, XMsg::ClientsDone { gather, lines, killed });
    }

    /// One worker answered a node operation (home side).
    fn gather_done(&mut self, gather: GatherRef, err: Option<StoreError>, lines: &[u8], killed: u64) {
        let Some(Some(g)) = self.x.gathers.get_mut(gather.id as usize) else { return };
        g.remaining -= 1;
        if g.err.is_none() {
            g.err = err;
        }
        g.lines.extend_from_slice(lines);
        g.killed += killed;
        if g.remaining > 0 {
            return;
        }
        let g = self.x.gathers[gather.id as usize].take().expect("present");
        self.x.free_gathers.push(gather.id);
        if matches!(g.op, NodeOp::Barrier(_) | NodeOp::Shutdown { .. }) && g.err.is_none() {
            crate::cmd::server::note_barrier_done(&self.shared, self.now.unix_ms);
        }
        let shutdown = matches!(g.op, NodeOp::Shutdown { .. });
        if let Some(to) = g.to {
            let proto = self.conn_mut(to.conn.slot, to.conn.generation).map_or(kv_resp::Proto::Resp2, |c| c.proto);
            let done = match (&g.op, g.err) {
                (_, Some(e)) => NodeOpDone::Failed(e),
                (NodeOp::ClientList(_), None) => NodeOpDone::Clients(&g.lines),
                (NodeOp::ClientKill { .. }, None) => NodeOpDone::Killed(g.killed),
                _ => NodeOpDone::Ok,
            };
            let mut b = Vec::new();
            crate::cmd::server::write_node_op_reply(&g.op, done, &mut ReplyWriter::new(&mut b, proto));
            if !(shutdown && g.err.is_none()) {
                self.deliver(to, ReplyBody::Bytes(b));
            }
        }
        if shutdown && g.err.is_none() {
            // SAVE / NOSAVE override `snapshot-on-shutdown`
            if let NodeOp::Shutdown { save: Some(save), .. } = g.op {
                self.shared.shutdown_snapshot.store(save, std::sync::atomic::Ordering::Release);
            }
            self.shared.request_shutdown();
        }
    }

    /// The shutdown drain: a barrier on every owned partition (no reply).
    pub(super) fn start_local_barrier_for_shutdown(&mut self) {
        let gather = GatherRef { worker: self.id, id: DRAIN_GATHER };
        self.node_part_start(gather, ReqKind::Barrier);
    }
}

/// The coordinating worker of a transaction id.
#[inline]
fn xtxn_worker(xtxn: u64) -> u16 {
    (xtxn >> 40) as u16
}

fn op_db(op: &Op) -> Option<u8> {
    match op {
        Op::Put { db, .. } | Op::Del { db, .. } => Some(*db),
        _ => None,
    }
}

fn op_key(op: &Op) -> Option<&[u8]> {
    match op {
        Op::Put { key, .. } | Op::Del { key, .. } => Some(key),
        _ => None,
    }
}

/// Merges the parts of a fan-out with `cmd::gather` (§6.8).
fn merge_gather(g: &GatherSlot, proto: kv_resp::Proto) -> Vec<u8> {
    let owned: SmallVec<[Vec<u8>; 4]> = g.parts.iter().map(|p| p.as_ref().map_or_else(Vec::new, ReplyBody::to_vec)).collect();
    let parts: SmallVec<[&[u8]; 4]> = owned.iter().map(Vec::as_slice).collect();
    let mut out = Vec::new();
    crate::cmd::gather(g.id, &g.split, g.argc, &parts, &mut ReplyWriter::new(&mut out, proto));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd(words: &[&[u8]]) -> OwnedCmd {
        let spec = kv_cmd::resolve(words).unwrap();
        OwnedCmd::new(spec.id, words)
    }

    #[test]
    fn durability_modes_release_replies_at_the_right_state() {
        assert_eq!(ticket_need(Durability::Written, CmdId::Set, true), Some(Need::Written));
        assert_eq!(ticket_need(Durability::Flushed, CmdId::Set, true), Some(Need::Durable));
        assert_eq!(ticket_need(Durability::Async, CmdId::Set, true), None);
        assert_eq!(ticket_need(Durability::Async, CmdId::Del, true), Some(Need::Written), "async deletes wait for Written");
        assert_eq!(ticket_need(Durability::Flushed, CmdId::Get, true), None, "reads never wait");
        assert_eq!(ticket_need(Durability::Flushed, CmdId::Set, false), None);
        assert!(satisfied(Need::Written, TicketState::Written));
        assert!(!satisfied(Need::Durable, TicketState::Written));
        assert!(satisfied(Need::Durable, TicketState::Durable));
        assert!(satisfied(Need::Durable, TicketState::Failed(-5)));
        assert!(!satisfied(Need::Written, TicketState::Executed));
    }

    #[test]
    fn exec_bodies_map_to_one_partition_or_none() {
        let a = cmd(&[b"SET", b"{a}1", b"x"]);
        let b = cmd(&[b"GET", b"{a}2"]);
        let c = cmd(&[b"GET", b"{b}1"]);
        let ping = cmd(&[b"PING"]);
        let pa = kv_common::key_slot(b"a") % 64;
        assert_eq!(multi_partition(&[a, b], 64), Ok(Some(pa)));
        let a = cmd(&[b"SET", b"{a}1", b"x"]);
        assert_eq!(multi_partition(&[a, c], 64), Err(()));
        assert_eq!(multi_partition(&[ping], 64), Ok(None));
        let req = multi_request(&[cmd(&[b"SET", b"k", b"v"]), cmd(&[b"GET", b"k"]), cmd(&[b"DEL", b"j"])], 2);
        assert_eq!(req.db, 2);
        assert_eq!(req.keys.len(), 2, "each key once");
        assert_eq!(req.keys[0].1, kv_cmd::Access::ReadWrite, "write then read merges");
        assert_eq!(written_keys(&[cmd(&[b"SET", b"k", b"v"]), cmd(&[b"GET", b"k"]), cmd(&[b"DEL", b"j", b"i"])]), 3);
    }

    #[test]
    fn select_inside_exec_sets_the_connection_database() {
        let body = [cmd(&[b"SELECT", b"3"]), cmd(&[b"SET", b"k", b"v"]), cmd(&[b"SELECT", b"99"])];
        assert_eq!(db_after(&body, 0, 16), 3);
        assert_eq!(db_after(&[cmd(&[b"PING"])], 5, 16), 5);
    }

    #[test]
    fn keyed_requests_carry_key_bytes_and_access_modes() {
        let argv: [&[u8]; 4] = [b"SET", b"k", b"v", b"GET"];
        let r = keyed_request(CmdId::Set.spec(), &argv, 1);
        assert_eq!(r.keys.len(), 1);
        assert_eq!(&*r.keys[0].0, b"k");
        assert_eq!(r.keys[0].1, kv_cmd::Access::ReadWrite);
        assert!(matches!(r.kind, ReqKind::Keyed));
        assert_eq!(merge_access(kv_cmd::Access::Meta, kv_cmd::Access::Read), kv_cmd::Access::Read);
        assert_eq!(xtxn_worker((3u64 << 40) | 9), 3);
    }
}
