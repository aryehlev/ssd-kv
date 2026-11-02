//! Replication streams of the node's partitions (DESIGN §23.7, §23.8).
//!
//! Primary: every executed command's group is encoded into the partition's `ReplSender` and
//! shipped to connected members in parallel with the page writes; replies wait for the shard
//! commit (`commit_seq >= seq`) and a valid lease. Member: each partition dials the primary's
//! replication port with `Hello`; the accepting worker hands the socket to the worker owning the
//! partition (`XMsg::ReplSocket`); frames become partition requests with `Origin::Replicated`,
//! `Image` or `Snapshot`; acks report the durable prefix. Replication positions are persisted to
//! partition meta every 50 ms. Streams read their sockets under the same pre-decode checks as
//! clients (§6.7).
//!
//! Everything here runs on the worker owning the partition, inside its tick loop: the transport
//! ([`super::net`]) only moves bytes. Per partition the state is one of
//! * idle (no shard role, or the partition is still loading),
//! * primary: a [`ReplSender`] (backlog, acks, commit point), the ring of groups this node
//!   received in earlier terms (Repair key sets), one stream per connected member, and for
//!   members in Repair or full sync a direct feed written in the partition's execution order;
//! * member: a [`MemberStream`] to the primary, the apply pipeline and its durable prefix.
//!
//! Positions only (LC-36): the commit point, the durable prefixes, coverage tests of the rings
//! and the walk position. The send window of a walk is the transport's unsent-byte count of
//! that one socket, a flow-control fact, not a load comparison of replication state.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use kv_cmd::{CmdFlags, CmdId};
use kv_common::Access;
use kv_common::pace::TokenBucket;
use kv_ctrl::{MemberState, MigPhase, MigrationTask, NodeAssignment, NodeId};
use kv_repl::backlog::Backlog;
use kv_repl::codec::{self, KeySet};
use kv_repl::frame::{self, AcceptMode, Frame, Hello, ImageRec, Recovered, RejectReason, SnapRec};
use kv_repl::member::{MemberAction, MemberStream};
use kv_repl::primary::{MemberId, ReplSender};
use kv_repl::sync::{self, PrimaryView, SyncDecision};
use kv_repl::walk::{self, WalkPos};
use kv_storage::effect::{Group, Op};
use kv_storage::keyspace::{ApplyMode, ApplyOutcome, StoreError};
use kv_storage::layout::Seq;
use kv_storage::metastore::{MetaDone, MetaReq};
use kv_storage::partition::{Durability, ExecOut, Origin, PartState, ReqKind, Request};
use kv_storage::pmeta::{PMETA_SYNCING, PartitionMeta};
use kv_storage::ticket::{TicketId, TicketState};
use smallvec::SmallVec;

use super::agent::CtrlEvent;
use super::migrate_glue::{MigIn, MigOut};
use super::net::{ReplEvent, StreamIo};
use super::track::Prefix;
use super::{AgentMsg, ClusterShared, REPL_TOKEN_BIT, before, same_token};
use crate::runtime::backend::{Backend, ReplOut, ReplWork};
use crate::runtime::exec::{CommitWaiter, Job, JobKind};
use crate::runtime::worker::Worker;
use crate::runtime::xmsg::ExecEnv;

/// Keys per walk step (one `SlotIter` request, then one read of those keys).
pub(super) const WALK_KEYS: u32 = 64;
/// Unsent bytes of one socket at which a walk or an image feed waits for the socket to drain.
pub(super) const SEND_WINDOW: usize = 4 << 20;
/// Interval of the replication-position persistence (§23.7).
const PERSIST_EVERY_NS: u64 = 50_000_000;
/// Pause before redialing a primary after a closed stream.
const REDIAL_NS: u64 = 200_000_000;
/// Pause before redialing after a refusal (the operator or a new assignment must act).
const REDIAL_REJECTED_NS: u64 = 1_000_000_000;
/// Longest wait of a reply for the shard commit before it is answered as not acknowledged.
const COMMIT_WAIT_LIMIT_NS: u64 = 10_000_000_000;
/// A member stream that delivered no frame for this long is closed and redialed (the primary
/// sends a heartbeat every `repl-heartbeat-ms`).
const MEMBER_IDLE_NS: u64 = 3_000_000_000;
/// Ring kept by an idle or member partition for the groups it receives.
const SMALL_RING: usize = 64 << 10;

/// What a submitted replication request continues with.
pub enum Then {
    /// Member: a part of group `seq`.
    MemberGroup { epoch: u64, seq: Seq },
    /// Member: a repair image or a full-sync record (`key` for the merge confirmations).
    MemberImage {
        epoch: u64,
        confirm: Option<(u8, Box<[u8]>)>,
    },
    /// Member: deletes of the merge-sync prune.
    MemberPrune { epoch: u64 },
    /// Member: stored seqs of a divergent-key walk step (Repair key set after a restart).
    MemberSeqs { epoch: u64, above: Seq },
    /// A walk step's listing (`SlotIter`) of `walker`.
    Listed { walker: Walker },
    /// Primary: images of keys for a member (Repair) or a full-sync step.
    PrimaryRead {
        member: MemberId,
        epoch: u64,
        snap: bool,
    },
    /// Migration source: values of a walk step's keys.
    MigOutRead { id: u64, epoch: u64 },
    /// Migration destination: item `item` of the stream applied.
    MigInApply { id: u64, epoch: u64, item: u64 },
    /// Slot purge deletes.
    Purged,
}

/// Who a walk step belongs to.
#[derive(Clone, Copy)]
pub enum Walker {
    /// Primary: the full sync of a member.
    Full { member: MemberId, epoch: u64 },
    /// Member: the merge-sync prune walk.
    Prune { epoch: u64 },
    /// Member: the divergent-key walk.
    Divergent { epoch: u64 },
    /// Migration source.
    MigOut { id: u64, epoch: u64 },
    /// Migration destination: the prune of keys the copy did not confirm.
    MigInPrune { id: u64, epoch: u64 },
    /// Purge of slots this primary no longer serves.
    Purge,
}

/// A replication request and its continuation.
pub struct ReplJob {
    pub work: ReplWork,
    pub then: Then,
}

/// A walk over slot tables in slot order through `SlotIter` steps (page references, then
/// real keys; LC-18).
pub(super) struct KeyWalk {
    pub slots: Vec<u16>,
    pub idx: usize,
    pub fp_from: u64,
    pub busy: bool,
}

impl KeyWalk {
    pub fn new(slots: Vec<u16>) -> Self {
        KeyWalk {
            slots,
            idx: 0,
            fp_from: 0,
            busy: false,
        }
    }

    pub fn done(&self) -> bool {
        self.idx >= self.slots.len()
    }

    /// The position everything below which was listed.
    pub fn pos(&self) -> WalkPos {
        if self.done() {
            WalkPos(u64::MAX)
        } else {
            WalkPos(((self.idx as u64) << 32) | self.fp_from)
        }
    }

    /// A listing step returned `next`: moves on (to the next slot once the table is done).
    pub fn listed(&mut self, next: u64) {
        if next >= 1 << 32 {
            self.idx += 1;
            self.fp_from = 0;
        } else {
            self.fp_from = next;
        }
    }

    /// Skips slots whose table is empty (`len(slot) == 0`).
    pub fn skip_empty(&mut self, len: &dyn Fn(u16) -> usize) {
        while self.fp_from == 0 && !self.done() && len(self.slots[self.idx]) == 0 {
            self.idx += 1;
        }
    }

    /// Has the walk passed the key with fingerprint `fp` in `slot`? One scalar comparison.
    pub fn passed(&self, slot: u16, fp: u32) -> bool {
        match self.slots.binary_search(&slot) {
            Ok(i) => walk::passed(self.pos(), i as u16, fp),
            Err(_) => self.done(),
        }
    }
}

/// A member's state at its primary.
struct Peer {
    node: NodeId,
    state: MemberState,
    in_isr: bool,
    present: bool,
    stream: Option<StreamIo>,
    epoch: u64,
    /// Counted although not in the committed ISR yet (its catch-up barriers are done; its
    /// `IsrExpand` follows once it holds every committed write, see `ExpandGate`).
    expanding: bool,
    /// Last readiness reported to the expand gate for this partition.
    reported_ready: bool,
    direct: Option<Direct>,
    last_shrink_ns: u64,
}

/// A member fed directly in execution order.
enum Direct {
    Repair {
        at: Seq,
        keys: VecDeque<(u8, Box<[u8]>)>,
        member_keys: bool,
        reading: bool,
    },
    Full {
        at: Seq,
        walk: KeyWalk,
        reading: bool,
        begun: bool,
        count: u64,
        bucket: TokenBucket,
    },
}

struct PrimaryPart {
    shard: u16,
    term: u32,
    sender: ReplSender,
    /// Groups received as a member in earlier terms (Repair key sets (b)/(c)).
    received: Backlog,
    peers: Vec<Peer>,
    history: Vec<(u32, Vec<u64>)>,
    handoff: Option<u64>,
}

/// A member's sync with its primary.
enum MemberSync {
    None,
    Repair {
        at: Seq,
        end: Option<Seq>,
        open: usize,
    },
    Full {
        at: Seq,
        end: Option<Seq>,
        open: usize,
        confirmed: KeySet,
        prune: Option<KeyWalk>,
        held: VecDeque<MemberAction>,
        flag_durable: bool,
    },
}

struct MemberPart {
    shard: u16,
    term: u32,
    /// The primary's replication address; `None` while the shard has no serving primary
    /// (a failover is pending): the member keeps its rings and waits for the next assignment.
    primary: Option<SocketAddr>,
    stream: MemberStream,
    io: Option<StreamIo>,
    dial_tag: Option<u64>,
    next_dial_ns: u64,
    epoch: u64,
    sync: MemberSync,
    /// Group seq -> its requests not durable yet.
    groups: hashbrown::HashMap<u64, u32, foldhash::fast::RandomState>,
    /// The ring of this node's own groups when it was primary (Repair key set (a)).
    own_groups: Option<Backlog>,
    divergent: Option<(KeyWalk, KeySet)>,
    ready_reported: Option<bool>,
    /// When the stream last delivered a frame (boot ns).
    last_rx_ns: u64,
    /// The primary's last refusal (repeated refusals are logged once).
    last_reject: Option<RejectReason>,
}

enum Role {
    Idle,
    Primary(Box<PrimaryPart>),
    Member(Box<MemberPart>),
}

/// What a ticket of a partition stands for.
#[derive(Clone, Copy)]
enum TicketFor {
    /// Primary: a produced group (local durability).
    Produced(Seq),
    /// Member: a part of an applied group.
    Group { epoch: u64, seq: Seq },
    /// Member: an image, snapshot record or prune delete of the current sync.
    Sync { epoch: u64 },
}

pub(super) struct PartRepl {
    pub p: u16,
    role: Role,
    last_term: u32,
    /// Primary: produced groups durable locally; member: applied groups durable.
    durable: Prefix,
    tickets: hashbrown::HashMap<u64, TicketFor, foldhash::fast::RandomState>,
    /// `(seq, since boot ns, waiter)` of replies waiting for the shard commit.
    commit_waits: Vec<(Seq, u64, CommitWaiter)>,
    syncing: bool,
    persisted: (Seq, Seq, u32, u32),
    pub(super) mig_out: Vec<MigOut>,
    pub(super) mig_in: Vec<MigIn>,
    purge: Option<(KeyWalk, bool)>,
    meta_wait: Option<u64>,
    /// Replication requests waiting for a txn credit, and every later one (stream order).
    queued: VecDeque<(Request, ReplJob, bool)>,
}

/// Which stream an id belongs to.
#[derive(Clone, Copy)]
pub(super) enum StreamRef {
    Member { p: u16 },
    Primary { p: u16, m: MemberId },
    MigOut { p: u16, id: u64 },
    MigIn { p: u16, id: u64 },
}

/// A worker's cluster state.
pub struct ClusterWorker {
    pub(super) cs: Arc<ClusterShared>,
    pub(super) parts: Vec<Option<PartRepl>>,
    pub(super) streams: hashbrown::HashMap<u64, StreamRef, foldhash::fast::RandomState>,
    assignment: Option<Arc<NodeAssignment>>,
    /// Partitions whose role still waits for recovery.
    unapplied: Vec<u16>,
    pub(super) next_tag: u64,
    last_persist_ns: u64,
    handoff: Option<u64>,
    pub(super) backlog_bytes: usize,
    heartbeat_ns: u64,
    ack_timeout_ns: u64,
    pub(super) sync_iops: u64,
    /// The worker is shutting down: no stream is accepted, dialed or driven any more.
    stopped: bool,
}

impl ClusterWorker {
    pub fn new(
        cs: Arc<ClusterShared>,
        owned: &[u16],
        partitions: u16,
        cfg: &crate::config::Config,
    ) -> Self {
        let mut parts: Vec<Option<PartRepl>> = (0..partitions).map(|_| None).collect();
        for &p in owned {
            let boot = cs.boot_part(p);
            let durable = boot.map_or(Seq::ZERO, |m| m.repl_durable);
            let last_term = boot.map_or(0, |m| m.term);
            let syncing = boot.is_some_and(|m| m.flags & PMETA_SYNCING != 0);
            if syncing {
                cs.syncing[usize::from(p)].store(true, Ordering::Release);
            }
            let commit = boot.map_or(Seq::ZERO, |m| m.repl_commit);
            parts[usize::from(p)] = Some(PartRepl {
                p,
                role: Role::Idle,
                last_term,
                durable: Prefix::new(durable),
                tickets: Default::default(),
                commit_waits: Vec::new(),
                syncing,
                persisted: (
                    durable,
                    commit,
                    if syncing { PMETA_SYNCING } else { 0 },
                    last_term,
                ),
                mig_out: Vec::new(),
                mig_in: Vec::new(),
                purge: None,
                meta_wait: None,
                queued: VecDeque::new(),
            });
        }
        ClusterWorker {
            cs,
            parts,
            streams: Default::default(),
            assignment: None,
            unapplied: Vec::new(),
            next_tag: 1,
            last_persist_ns: 0,
            handoff: None,
            backlog_bytes: usize::try_from(cfg.repl_backlog_size).unwrap_or(usize::MAX),
            heartbeat_ns: cfg.repl_heartbeat_ms.saturating_mul(1_000_000),
            ack_timeout_ns: cfg.repl_ack_timeout_ms.saturating_mul(1_000_000),
            sync_iops: cfg.sync_iops,
            stopped: false,
        }
    }

    /// Something needs the next tick soon (1 ms timer): walks, feeds, commit waits.
    pub fn busy(&self) -> bool {
        self.parts.iter().flatten().any(|pr| {
            !pr.commit_waits.is_empty()
                || pr.purge.is_some()
                || match &pr.role {
                    Role::Primary(pp) => pp.peers.iter().any(|x| x.direct.is_some()),
                    Role::Member(m) => {
                        !matches!(m.sync, MemberSync::None)
                            || m.divergent.is_some()
                            || (m.io.is_some() && m.stream.ack_pending())
                    }
                    Role::Idle => false,
                }
                || !pr.mig_out.is_empty()
                || !pr.mig_in.is_empty()
                || !pr.queued.is_empty()
        }) || !self.unapplied.is_empty()
    }

    /// Streams exist (heartbeats, acks and persistence need a periodic tick).
    pub fn active(&self) -> bool {
        !self.streams.is_empty()
            || self
                .parts
                .iter()
                .flatten()
                .any(|pr| matches!(pr.role, Role::Member(_)))
    }

    pub(super) fn part(&mut self, p: u16) -> Option<&mut PartRepl> {
        self.parts.get_mut(usize::from(p)).and_then(Option::as_mut)
    }

    /// The shard role of the current assignment.
    pub(super) fn assignment(&self) -> Option<&Arc<NodeAssignment>> {
        self.assignment.as_ref()
    }
}

fn frame_bytes(f: &Frame, p: u16, term: u32) -> Vec<u8> {
    let mut out = Vec::new();
    frame::encode(f, p, term, &mut out);
    out
}

/// Keys and access modes of ops (one partition request).
fn op_keys(ops: &[Op], merge: bool) -> SmallVec<[(Box<[u8]>, Access); 2]> {
    let mut keys: SmallVec<[(Box<[u8]>, Access); 2]> = SmallVec::new();
    for op in ops {
        let (k, a) = match op {
            Op::Put { key, .. } if merge => (key, Access::ReadWrite),
            Op::Put { key, .. } | Op::Del { key, .. } | Op::Expired { key, .. } => {
                (key, Access::Write)
            }
            Op::Expire { key, .. } | Op::Append { key, .. } | Op::SetRange { key, .. } => {
                (key, Access::ReadWrite)
            }
            _ => continue,
        };
        if !keys.iter().any(|(x, _)| x == k) {
            keys.push((k.clone(), a));
        }
    }
    keys
}

fn op_db(ops: &[Op]) -> u8 {
    ops.iter()
        .find_map(|o| match o {
            Op::Put { db, .. }
            | Op::Del { db, .. }
            | Op::Expired { db, .. }
            | Op::Expire { db, .. }
            | Op::Append { db, .. }
            | Op::SetRange { db, .. } => Some(*db),
            _ => None,
        })
        .unwrap_or(0)
}

/// The slot of a group's keyed ops (cluster commands touch one slot), if any.
pub(super) fn group_slot(g: &Group) -> Option<u16> {
    g.ops.iter().find_map(|o| match o {
        Op::Put { key, .. }
        | Op::Del { key, .. }
        | Op::Expired { key, .. }
        | Op::Expire { key, .. }
        | Op::Append { key, .. }
        | Op::SetRange { key, .. } => Some(kv_common::key_slot(key)),
        _ => None,
    })
}

/// Fingerprints of a group's keys.
pub(super) fn group_fps(g: &Group, seed: u64) -> SmallVec<[(u16, u32); 2]> {
    g.ops
        .iter()
        .filter_map(|o| match o {
            Op::Put { key, .. }
            | Op::Del { key, .. }
            | Op::Expired { key, .. }
            | Op::Expire { key, .. }
            | Op::Append { key, .. }
            | Op::SetRange { key, .. } => Some((
                kv_common::key_slot(key),
                kv_common::KeyHash::of(seed, key).fp32(),
            )),
            _ => None,
        })
        .collect()
}

fn is_wide(g: &Group) -> bool {
    g.ops
        .iter()
        .any(|o| matches!(o, Op::FlushDb { .. } | Op::FlushSlot { .. }))
}

fn repl_env(now_ms: u64) -> ExecEnv {
    ExecEnv {
        proto: kv_resp::Proto::Resp2,
        db: 0,
        now_ms,
        readonly_conn: false,
        conn_id: 0,
    }
}

/// The slots of partition `p` among `partitions` (`s % P == p`), in slot order.
pub(super) fn slots_of(p: u16, partitions: u16) -> Vec<u16> {
    (0..kv_common::SLOTS as u16)
        .filter(|s| s % partitions.max(1) == p)
        .collect()
}

impl<B: Backend> Worker<B> {
    /// Runs `f` with the worker's cluster state taken out (put back afterwards).
    pub(crate) fn with_cluster<R>(
        &mut self,
        f: impl FnOnce(&mut Self, &mut ClusterWorker) -> R,
    ) -> Option<R> {
        let mut cw = self.cluster.take()?;
        let r = f(self, &mut cw);
        self.cluster = Some(cw);
        Some(r)
    }

    fn local_need(&self) -> Option<TicketState> {
        match self.cfg.effective_durability() {
            Durability::Async => None,
            Durability::Written => Some(TicketState::Written),
            Durability::Flushed => Some(TicketState::Durable),
        }
    }

    fn ticket_satisfies(&self, st: TicketState) -> bool {
        matches!(
            (self.local_need(), st),
            (None, _)
                | (_, TicketState::Durable | TicketState::Failed(_))
                | (Some(TicketState::Written), TicketState::Written)
        )
    }

    /// Submits a replication request of partition `p`, in order with the partition's earlier
    /// ones. A write of several keys is crash-atomic like any local multi-key write (§14.6) and
    /// takes a txn credit first; while none is free it waits in the partition's queue, and so
    /// does everything submitted after it (applies must keep the stream's order).
    pub(crate) fn submit_repl(
        &mut self,
        cw: &mut ClusterWorker,
        p: u16,
        req: Request,
        rj: ReplJob,
    ) {
        let needs_credit = req.keys.len() >= 2
            && matches!(
                rj.work,
                ReplWork::Apply { .. } | ReplWork::Own { .. } | ReplWork::Prune { .. }
            );
        let Some(pr) = cw.part(p) else { return };
        if !pr.queued.is_empty() {
            pr.queued.push_back((req, rj, needs_credit));
            return;
        }
        let credit = if needs_credit {
            match crate::runtime::exec::CreditGuard::try_take(&self.shared) {
                Some(c) => Some(c),
                None => {
                    pr.queued.push_back((req, rj, needs_credit));
                    return;
                }
            }
        } else {
            None
        };
        self.submit_repl_now(p, req, rj, credit);
    }

    fn submit_repl_now(
        &mut self,
        p: u16,
        req: Request,
        rj: ReplJob,
        credit: Option<crate::runtime::exec::CreditGuard>,
    ) {
        let env = repl_env(self.now.unix_ms);
        let job = Job {
            to: None,
            kind: JobKind::Repl(rj),
            env,
            id: CmdId::Ping,
            start_ns: self.now.mono_ns,
            _lv: crate::net::budget::Reservation::NONE,
            credit,
        };
        self.submit_job(p, req, job);
    }

    /// Submits the queued replication requests of `p` whose credits are now free, in order.
    fn drain_queued(&mut self, cw: &mut ClusterWorker, p: u16) {
        loop {
            let Some(pr) = cw.part(p) else { return };
            let Some((_, _, needs)) = pr.queued.front() else {
                return;
            };
            let credit = if *needs {
                match crate::runtime::exec::CreditGuard::try_take(&self.shared) {
                    Some(c) => Some(c),
                    None => return,
                }
            } else {
                None
            };
            let Some((req, rj, _)) = pr.queued.pop_front() else {
                return;
            };
            self.submit_repl_now(p, req, rj, credit);
        }
    }

    // -----------------------------------------------------------------------------------------
    // produced groups (primary)

    /// A primary partition executed a request that produced `g`: backlog, direct feeds,
    /// migration forwards and local durability tracking. Returns the seq the reply waits for
    /// (`None`: no shard commit applies here).
    pub(crate) fn cluster_group(
        &mut self,
        p: u16,
        g: &Group,
        ticket: Option<TicketId>,
    ) -> Option<Seq> {
        let seed = self.backend.hash_seed(p);
        let done_now = ticket.is_none() || self.local_need().is_none();
        self.with_cluster(|_, cw| {
            let pr = cw.part(p)?;
            let Role::Primary(pp) = &mut pr.role else {
                return None;
            };
            let mut enc = Vec::with_capacity(codec::encoded_len(g));
            codec::encode_group(g, &mut enc);
            pp.sender.on_group(&enc, g.seq);
            let term = pp.term;
            let fps = group_fps(g, seed);
            let wide = is_wide(g);
            for peer in pp.peers.iter() {
                let (Some(io), Some(d)) = (&peer.stream, &peer.direct) else {
                    continue;
                };
                let forward = match d {
                    Direct::Repair { .. } => true,
                    Direct::Full { walk, .. } => {
                        wide || fps.iter().any(|&(s, fp)| walk.passed(s, fp))
                    }
                };
                if forward {
                    io.send(frame_bytes(&Frame::Groups(enc.clone()), p, term));
                }
            }
            let slot = group_slot(g);
            for mo in pr.mig_out.iter_mut() {
                mo.on_group(g, &enc, slot, &fps, p, term);
            }
            pr.durable.push(g.seq, done_now);
            if !done_now && let Some(t) = ticket {
                pr.tickets.insert(t.0, TicketFor::Produced(g.seq));
            }
            Some(g.seq)
        })
        .flatten()
    }

    /// A ticket of an owned partition changed state.
    pub(crate) fn cluster_ticket(&mut self, p: u16, t: TicketId, st: TicketState) {
        if !self.ticket_satisfies(st) {
            return;
        }
        let failed = matches!(st, TicketState::Failed(_));
        if failed {
            tracing::error!(
                partition = p,
                "a replicated write failed on this node's device"
            );
        }
        self.with_cluster(|w, cw| {
            let Some(pr) = cw.part(p) else { return };
            let Some(what) = pr.tickets.remove(&t.0) else {
                return;
            };
            if failed
                && matches!(what, TicketFor::Group { .. } | TicketFor::Sync { .. })
                && let Role::Member(m) = &mut pr.role
            {
                // a member never acknowledges a write its device did not take: the durable
                // prefix stays where it is and the stream restarts from it
                if let Some(io) = m.io.take() {
                    io.abort();
                }
                m.epoch += 1;
                m.sync = MemberSync::None;
                m.divergent = None;
                m.groups.clear();
                return;
            }
            match what {
                TicketFor::Produced(s) => pr.durable.done(s),
                TicketFor::Group { epoch, seq } => {
                    if let Role::Member(m) = &mut pr.role
                        && m.epoch == epoch
                    {
                        group_part_done(m, &mut pr.durable, seq);
                    }
                }
                TicketFor::Sync { epoch } => {
                    if let Role::Member(m) = &mut pr.role
                        && m.epoch == epoch
                    {
                        match &mut m.sync {
                            MemberSync::Repair { open, .. } | MemberSync::Full { open, .. } => {
                                *open = open.saturating_sub(1)
                            }
                            MemberSync::None => {}
                        }
                    }
                    w.member_sync_progress(cw, p);
                }
            }
        });
    }

    /// A commit waiter for partition `p` (released by the tick once `commit_seq >= seq`).
    pub(crate) fn commit_wait(&mut self, p: u16, seq: Seq, w: CommitWaiter) {
        let now = self.now.boot_ns;
        let mut slot = Some(w);
        self.with_cluster(|_, cw| {
            if let Some(pr) = cw.part(p)
                && matches!(pr.role, Role::Primary(_))
                && let Some(w) = slot.take()
            {
                pr.commit_waits.push((seq, now, w));
            }
        });
        if let Some(w) = slot {
            // the partition is no longer the shard primary: the write is not acknowledged
            self.commit_released(w, Some(b"TRYAGAIN the shard role of this node changed"));
        }
    }

    // -----------------------------------------------------------------------------------------
    // control events

    pub(crate) fn on_ctrl(&mut self, ev: CtrlEvent) {
        match ev {
            CtrlEvent::Assign(a) => {
                self.with_cluster(|w, cw| {
                    if cw.stopped {
                        return;
                    }
                    cw.assignment = Some(a);
                    cw.unapplied = w.backend.owned().to_vec();
                    w.apply_roles(cw);
                });
            }
            CtrlEvent::Fence {
                req,
                shard,
                new_term,
            } => {
                let _ = shard;
                let applied = self
                    .with_cluster(|_, cw| {
                        let mut out = Vec::new();
                        let mut closed = Vec::new();
                        let ids: Vec<u16> = cw.parts.iter().flatten().map(|pr| pr.p).collect();
                        for p in ids {
                            let Some(pr) = cw.part(p) else { continue };
                            let applied = match &mut pr.role {
                                Role::Member(m) => {
                                    let a = m.stream.applied().max(pr.durable.upto());
                                    if m.term < new_term {
                                        // close the streams of the fenced term
                                        if let Some(io) = m.io.take() {
                                            closed.push(io.id);
                                            io.abort();
                                        }
                                        m.epoch += 1;
                                        m.sync = MemberSync::None;
                                        m.divergent = None;
                                        m.groups.clear();
                                    }
                                    a
                                }
                                Role::Primary(pp) => pp.sender.last(),
                                Role::Idle => pr.durable.upto(),
                            };
                            out.push((p, applied.0));
                        }
                        for id in closed {
                            cw.streams.remove(&id);
                        }
                        out
                    })
                    .unwrap_or_default();
                if let Some(cs) = self.shared.cluster() {
                    cs.tell_agent(AgentMsg::FenceApplied { req, applied });
                }
            }
            CtrlEvent::Handoff { req, .. } => {
                self.with_cluster(|_, cw| {
                    cw.handoff = Some(req);
                    for pr in cw.parts.iter_mut().flatten() {
                        if let Role::Primary(pp) = &mut pr.role {
                            pp.handoff = Some(req);
                        }
                    }
                });
            }
            CtrlEvent::MigrationPhase { .. } => {}
        }
    }

    /// Brings every owned, recovered partition to the role of the current assignment.
    fn apply_roles(&mut self, cw: &mut ClusterWorker) {
        let Some(a) = cw.assignment.clone() else {
            return;
        };
        let pending = std::mem::take(&mut cw.unapplied);
        for p in pending {
            if self.backend.state(p) == PartState::Loading {
                cw.unapplied.push(p);
                continue;
            }
            self.apply_role(cw, p, &a);
        }
    }

    fn apply_role(&mut self, cw: &mut ClusterWorker, p: u16, a: &NodeAssignment) {
        let backlog_bytes = cw.backlog_bytes;
        let heartbeat_ns = cw.heartbeat_ns;
        let cs = cw.cs.clone();
        let Some(pr) = cw.part(p) else { return };
        let want = a.role.as_ref();
        let same = match (&pr.role, want) {
            (Role::Idle, None) => true,
            (Role::Primary(pp), Some(r)) => {
                r.is_primary && pp.term == r.term && pp.shard == r.shard.0
            }
            (Role::Member(m), Some(r)) => {
                !r.is_primary
                    && m.term == r.term
                    && m.shard == r.shard.0
                    && r.primary_repl.map(|x| x.0) == m.primary
            }
            _ => false,
        };
        if !same {
            let old = std::mem::replace(&mut pr.role, Role::Idle);
            let mut carried_ring: Option<Backlog> = None;
            let mut own_groups: Option<Backlog> = None;
            // replies of writes the old role executed: they cannot commit any more; answered
            // now, before anything below can return early (a dropped waiter is a reply that
            // never comes)
            let waits = std::mem::take(&mut pr.commit_waits);
            for (_, _, w) in waits {
                self.commit_released(w, Some(b"TRYAGAIN the shard role of this node changed"));
            }
            match old {
                Role::Primary(pp) => {
                    for peer in &pp.peers {
                        if let Some(io) = &peer.stream {
                            io.abort();
                        }
                    }
                    own_groups = Some(pp.sender.into_backlog());
                }
                Role::Member(m) => {
                    if let Some(io) = &m.io {
                        io.abort();
                    }
                    let m = *m;
                    // a member that moves to a newer term (or is promoted) keeps the groups
                    // it received: its own Repair key set (a), or the new primary's key set (b)
                    own_groups = m.own_groups;
                    carried_ring = Some(m.stream.into_received());
                }
                Role::Idle => {}
            }
            for mo in pr.mig_out.drain(..) {
                mo.abort();
            }
            for mi in pr.mig_in.drain(..) {
                mi.abort();
            }
            pr.purge = None;
            pr.queued.clear();
            pr.tickets
                .retain(|_, t| matches!(t, TicketFor::Produced(_)));
            cw.streams.retain(|_, r| match r {
                StreamRef::Member { p: x }
                | StreamRef::Primary { p: x, .. }
                | StreamRef::MigOut { p: x, .. }
                | StreamRef::MigIn { p: x, .. } => *x != p,
            });
            let Some(pr) = cw.part(p) else { return };
            match want {
                None => {
                    self.backend.set_cluster_role(p, false, 0);
                }
                Some(r) if r.is_primary => {
                    self.backend.set_cluster_role(p, true, r.term);
                    let ring = match Backlog::try_new(backlog_bytes) {
                        Ok(b) => b,
                        Err(e) => {
                            tracing::error!(
                                partition = p,
                                "replication backlog memory cannot be locked: {e}"
                            );
                            self.shared.request_shutdown();
                            return;
                        }
                    };
                    let mut sender = ReplSender::with_backlog(p, r.term, ring);
                    sender.set_heartbeat_ns(heartbeat_ns);
                    sender.start_at(before(self.backend.next_seq(p)));
                    // the groups of the term taken over (Repair key sets (b)): received as its
                    // member, or produced as its primary (re-promoted after a NoPrimary
                    // period); without either (a restart) a ring that was never started: it
                    // covers nothing, so members behind the takeover point are refused rather
                    // than repaired with an empty key set
                    let received = carried_ring.or(own_groups).unwrap_or_else(|| {
                        Backlog::try_new(SMALL_RING).unwrap_or_else(|_| Backlog::new(4096))
                    });
                    pr.durable.reset(Seq::new(r.term, 0));
                    pr.tickets.clear();
                    pr.last_term = r.term;
                    pr.role = Role::Primary(Box::new(PrimaryPart {
                        shard: r.shard.0,
                        term: r.term,
                        sender,
                        received,
                        peers: Vec::new(),
                        history: Vec::new(),
                        handoff: None,
                    }));
                    tracing::info!(
                        partition = p,
                        term = r.term,
                        "partition is the shard primary"
                    );
                }
                Some(r) => {
                    self.backend.set_cluster_role(p, false, r.term);
                    // without a primary address (the shard waits for a promotion) the member
                    // dials nobody but keeps its rings for the next term's Repair
                    let primary = r.primary_repl.map(|x| x.0);
                    // the groups received in the previous term stay with the member (Repair
                    // key set (a)); a fresh ring covers nothing until a stream starts it
                    let ring =
                        match carried_ring.map_or_else(|| Backlog::try_new(backlog_bytes), Ok) {
                            Ok(b) => b,
                            Err(e) => {
                                tracing::error!(
                                    partition = p,
                                    "replication ring memory cannot be locked: {e}"
                                );
                                self.shared.request_shutdown();
                                return;
                            }
                        };
                    pr.role = Role::Member(Box::new(MemberPart {
                        shard: r.shard.0,
                        term: r.term,
                        primary,
                        stream: MemberStream::with_backlog(p, ring),
                        io: None,
                        dial_tag: None,
                        next_dial_ns: 0,
                        epoch: 0,
                        sync: MemberSync::None,
                        groups: Default::default(),
                        own_groups,
                        divergent: None,
                        ready_reported: None,
                        last_rx_ns: 0,
                        last_reject: None,
                    }));
                }
            }
            let _ = cs;
        }
        // role facts that change without a role change
        let Some(pr) = cw.part(p) else { return };
        if let (Role::Primary(pp), Some(r)) = (&mut pr.role, want) {
            pp.history = r
                .history
                .iter()
                .map(|t| (t.term, t.takeover.clone()))
                .collect();
            for x in pp.peers.iter_mut() {
                x.present = false;
            }
            for (node, state, in_isr) in &r.members {
                match pp.peers.iter_mut().find(|x| x.node == *node) {
                    Some(x) => {
                        x.present = true;
                        x.state = *state;
                        x.in_isr = *in_isr;
                        if *in_isr {
                            x.expanding = false;
                            x.reported_ready = false;
                        }
                    }
                    None => pp.peers.push(Peer {
                        node: *node,
                        state: *state,
                        in_isr: *in_isr,
                        present: true,
                        stream: None,
                        epoch: 0,
                        expanding: false,
                        reported_ready: false,
                        direct: None,
                        last_shrink_ns: 0,
                    }),
                }
            }
            for (i, x) in pp.peers.iter_mut().enumerate() {
                if !x.present {
                    if let Some(io) = x.stream.take() {
                        io.abort();
                    }
                    x.direct = None;
                    x.expanding = false;
                    pp.sender.detach(i as MemberId);
                }
            }
            let counted: Vec<MemberId> = pp
                .peers
                .iter()
                .enumerate()
                .filter(|(_, x)| x.present && (x.in_isr || x.expanding))
                .map(|(i, _)| i as MemberId)
                .collect();
            pp.sender.set_isr(&counted);
            let tasks = r.migrations.clone();
            self.sync_migrations(cw, p, &tasks);
        }
        self.plan_purge(cw, p);
    }

    // -----------------------------------------------------------------------------------------
    // transport events

    pub(crate) fn on_repl(&mut self, ev: ReplEvent) {
        self.with_cluster(|w, cw| match ev {
            ReplEvent::Inbound { stream, .. }
            | ReplEvent::Dialed {
                stream: Some(stream),
                ..
            } if cw.stopped => stream.abort(),
            ReplEvent::Inbound {
                stream,
                hello,
                term,
            } => w.on_inbound(cw, stream, *hello, term),
            ReplEvent::Dialed {
                tag,
                partition,
                stream,
            } => w.on_dialed(cw, tag, partition, stream),
            ReplEvent::Frame {
                stream,
                term,
                frame,
                bytes,
            } => {
                let Some(r) = cw.streams.get(&stream).copied() else {
                    return;
                };
                match r {
                    StreamRef::Member { p } => w.member_frame(cw, p, stream, term, frame),
                    StreamRef::Primary { p, m } => w.primary_frame(cw, p, m, stream, term, frame),
                    StreamRef::MigOut { p, id } => w.mig_out_frame(cw, p, id, stream, frame),
                    StreamRef::MigIn { p, id } => w.mig_in_frame(cw, p, id, stream, term, frame),
                }
                w.consumed(cw, stream, bytes);
            }
            ReplEvent::Closed { stream } => w.on_closed(cw, stream),
        });
    }

    fn consumed(&self, cw: &mut ClusterWorker, stream: u64, bytes: usize) {
        let Some(r) = cw.streams.get(&stream).copied() else {
            return;
        };
        let io = match r {
            StreamRef::Member { p } => cw.part(p).and_then(|pr| match &pr.role {
                Role::Member(m) => m.io.as_ref().filter(|i| i.id == stream),
                _ => None,
            }),
            StreamRef::Primary { p, m } => cw.part(p).and_then(|pr| match &pr.role {
                Role::Primary(pp) => pp
                    .peers
                    .get(usize::from(m))
                    .and_then(|x| x.stream.as_ref())
                    .filter(|i| i.id == stream),
                _ => None,
            }),
            StreamRef::MigOut { p, id } => cw
                .part(p)
                .and_then(|pr| pr.mig_out.iter().find(|x| x.id == id))
                .and_then(|x| x.io.as_ref()),
            StreamRef::MigIn { p, id } => cw
                .part(p)
                .and_then(|pr| pr.mig_in.iter().find(|x| x.id == id))
                .and_then(|x| x.io.as_ref()),
        };
        if let Some(io) = io {
            io.consumed(bytes);
        }
    }

    fn on_closed(&mut self, cw: &mut ClusterWorker, stream: u64) {
        let Some(r) = cw.streams.remove(&stream) else {
            return;
        };
        let now = self.now.boot_ns;
        match r {
            StreamRef::Member { p } => {
                if let Some(pr) = cw.part(p)
                    && let Role::Member(m) = &mut pr.role
                    && m.io.as_ref().is_some_and(|i| i.id == stream)
                {
                    m.io = None;
                    m.epoch += 1;
                    m.next_dial_ns = m.next_dial_ns.max(now + REDIAL_NS);
                    m.sync = MemberSync::None;
                    m.divergent = None;
                    m.groups.clear();
                    tracing::debug!(partition = p, "replication stream to the primary closed");
                }
            }
            StreamRef::Primary { p, m } => {
                if let Some(pr) = cw.part(p)
                    && let Role::Primary(pp) = &mut pr.role
                    && let Some(x) = pp.peers.get_mut(usize::from(m))
                    && x.stream.as_ref().is_some_and(|i| i.id == stream)
                {
                    x.stream = None;
                    x.direct = None;
                    x.epoch += 1;
                    pp.sender.detach(m);
                }
            }
            StreamRef::MigOut { p, id } => {
                if let Some(pr) = cw.part(p)
                    && let Some(mo) = pr.mig_out.iter_mut().find(|x| x.id == id)
                {
                    mo.closed(stream);
                }
            }
            StreamRef::MigIn { p, id } => {
                if let Some(pr) = cw.part(p)
                    && let Some(mi) = pr.mig_in.iter_mut().find(|x| x.id == id)
                {
                    mi.closed(stream, now);
                }
            }
        }
    }

    fn on_dialed(&mut self, cw: &mut ClusterWorker, tag: u64, p: u16, stream: Option<StreamIo>) {
        let now = self.now.boot_ns;
        let Some(pr) = cw.part(p) else {
            if let Some(io) = stream {
                io.abort();
            }
            return;
        };
        if let Some(mi) = pr.mig_in.iter_mut().find(|x| x.dial_tag == Some(tag)) {
            let id = mi.id;
            if let Some(io) = stream {
                let sid = io.id;
                mi.connected(io, now);
                cw.streams.insert(sid, StreamRef::MigIn { p, id });
            } else {
                mi.dial_failed(now);
            }
            return;
        }
        let durable = pr.durable.upto();
        let Role::Member(m) = &mut pr.role else {
            if let Some(io) = stream {
                io.abort();
            }
            return;
        };
        if m.dial_tag != Some(tag) {
            if let Some(io) = stream {
                io.abort();
            }
            return;
        }
        m.dial_tag = None;
        match stream {
            Some(io) => {
                let sid = io.id;
                m.io = Some(io);
                m.epoch += 1;
                m.stream.restart(durable, durable);
                m.sync = MemberSync::None;
                m.groups.clear();
                m.ready_reported = None;
                m.last_rx_ns = now;
                cw.streams.insert(sid, StreamRef::Member { p });
            }
            None => m.next_dial_ns = now + REDIAL_NS,
        }
    }

    // -----------------------------------------------------------------------------------------
    // primary side

    fn on_inbound(&mut self, cw: &mut ClusterWorker, io: StreamIo, h: Hello, _term: u32) {
        let p = h.partition;
        if let Some(mig) = h.migration {
            return self.mig_out_hello(cw, p, mig, io, h);
        }
        let ident = cw.cs.identity();
        let reject = |io: StreamIo, r: RejectReason, term: u32| {
            io.send(frame_bytes(&Frame::Reject(r), p, term));
            drop(io);
        };
        if !same_token(&h.auth, &cw.cs.auth) || ident.cluster_id != Some(h.cluster_id) {
            return reject(io, RejectReason::BadAuth, 0);
        }
        let last_executed = before(self.backend.next_seq(p));
        let loading = self.backend.state(p) == PartState::Loading;
        let rate = cw.sync_iops.max(1);
        let ClusterWorker { parts, streams, .. } = cw;
        let Some(pr) = parts.get_mut(usize::from(p)).and_then(Option::as_mut) else {
            return;
        };
        let Role::Primary(pp) = &mut pr.role else {
            return reject(io, RejectReason::NotPrimary, 0);
        };
        if loading || pp.shard != h.shard {
            return reject(io, RejectReason::NotPrimary, pp.term);
        }
        let Some(mi) = pp
            .peers
            .iter()
            .position(|x| x.present && x.node.0 == h.node_id)
        else {
            return reject(io, RejectReason::NotMember, pp.term);
        };
        let m = mi as MemberId;
        let term = pp.term;
        let view = PrimaryView {
            term,
            history: &pp.history,
            backlog: pp.sender.backlog(),
            received: &pp.received,
            is_member: true,
            operator_full_sync: pp.peers[mi].state == MemberState::Syncing,
            last_executed: last_executed.max(pp.sender.last()),
        };
        let d = sync::decide(&h, &view);
        tracing::info!(partition = p, member = %pp.peers[mi].node.hex40(), decision = d.kind(), "replication handshake");
        let mode = match d {
            SyncDecision::Reject(r) => {
                if r == RejectReason::NeedOperatorResync {
                    // only `replica resync` can bring this member back: the coordinator records
                    // it (NeedsFullSync, shown by `kvadm status`); nothing starts a sync here
                    cw.cs.tell_agent(AgentMsg::NeedsResync {
                        shard: pp.shard,
                        term,
                        member: pp.peers[mi].node,
                    });
                }
                return reject(io, r, term);
            }
            SyncDecision::Continue { from } => AcceptMode::Continue { from },
            SyncDecision::Repair {
                at,
                primary_keys,
                want_member_keys,
            } => {
                let x = &mut pp.peers[mi];
                x.direct = Some(Direct::Repair {
                    at,
                    keys: primary_keys.into_iter().collect(),
                    member_keys: !want_member_keys,
                    reading: false,
                });
                AcceptMode::Repair { at }
            }
            SyncDecision::Full { at } => {
                let slots = slots_of(p, self.shared.partitions);
                let x = &mut pp.peers[mi];
                x.direct = Some(Direct::Full {
                    at,
                    walk: KeyWalk::new(slots),
                    reading: false,
                    begun: false,
                    count: 0,
                    bucket: TokenBucket::new(rate, rate.max(256)),
                });
                AcceptMode::Full { at }
            }
        };
        let x = &mut pp.peers[mi];
        if let Some(old) = x.stream.take() {
            streams.remove(&old.id);
            old.abort();
        }
        x.epoch += 1;
        io.send(frame_bytes(&Frame::Accept { term, mode }, p, term));
        pp.sender.attach(m, &mode);
        streams.insert(io.id, StreamRef::Primary { p, m });
        let x = &mut pp.peers[mi];
        x.stream = Some(io);
    }

    fn primary_frame(
        &mut self,
        cw: &mut ClusterWorker,
        p: u16,
        m: MemberId,
        stream: u64,
        term: u32,
        f: Frame,
    ) {
        let now = self.now.boot_ns;
        let Some(pr) = cw.part(p) else { return };
        let Role::Primary(pp) = &mut pr.role else {
            return;
        };
        if term > pp.term {
            // the member saw a newer term: this primary is superseded
            if let Some(x) = pp.peers.get_mut(usize::from(m))
                && let Some(io) = x.stream.take()
            {
                io.abort();
            }
            return;
        }
        let Some(x) = pp.peers.get_mut(usize::from(m)) else {
            return;
        };
        if x.stream.as_ref().is_none_or(|i| i.id != stream) {
            return;
        }
        match f {
            Frame::Ack { durable, .. } => pp.sender.on_ack(m, durable, now),
            Frame::RepairKeys(keys) => {
                if let Some(Direct::Repair {
                    keys: q,
                    member_keys,
                    ..
                }) = &mut x.direct
                {
                    q.extend(keys);
                    *member_keys = true;
                }
            }
            other => tracing::debug!(
                partition = p,
                kind = other.kind(),
                "unexpected frame from a member"
            ),
        }
    }

    /// Drives the direct feeds (repair images, full-sync walks) of partition `p`'s members.
    fn drive_direct(&mut self, cw: &mut ClusterWorker, p: u16) {
        let now_ns = self.now.mono_ns;
        let last = before(self.backend.next_seq(p));
        let lens = |w: &Self, s: u16| w.backend.slot_len(p, 0, s);
        let mut submits: Vec<(Request, ReplJob)> = Vec::new();
        {
            let Some(pr) = cw.part(p) else { return };
            let Role::Primary(pp) = &mut pr.role else {
                return;
            };
            let term = pp.term;
            for (i, x) in pp.peers.iter_mut().enumerate() {
                let m = i as MemberId;
                let (Some(io), Some(d)) = (&x.stream, &mut x.direct) else {
                    continue;
                };
                if io.unsent() >= SEND_WINDOW {
                    continue;
                }
                let epoch = x.epoch;
                match d {
                    Direct::Repair {
                        at,
                        keys,
                        member_keys,
                        reading,
                    } => {
                        if *reading {
                            continue;
                        }
                        if keys.is_empty() {
                            if *member_keys {
                                // every image is out: the stream continues from the backlog
                                let end = (*at).max(last).max(pp.sender.last());
                                io.send(frame_bytes(&Frame::SyncDone { at: end }, p, term));
                                pp.sender.resume(m, end);
                                x.direct = None;
                            }
                            continue;
                        }
                        let n = keys.len().min(WALK_KEYS as usize);
                        let batch: Vec<(u8, Box<[u8]>)> = keys.drain(..n).collect();
                        let db = batch[0].0;
                        let ks: Vec<Box<[u8]>> = batch.into_iter().map(|(_, k)| k).collect();
                        let req = Request {
                            db,
                            keys: ks.iter().map(|k| (k.clone(), Access::Read)).collect(),
                            kind: ReqKind::Keyed,
                            origin: Origin::Client,
                        };
                        *reading = true;
                        submits.push((
                            req,
                            ReplJob {
                                work: ReplWork::Read { db, keys: ks },
                                then: Then::PrimaryRead {
                                    member: m,
                                    epoch,
                                    snap: false,
                                },
                            },
                        ));
                    }
                    Direct::Full {
                        walk,
                        reading,
                        begun,
                        count,
                        bucket,
                        ..
                    } => {
                        if *reading || walk.busy {
                            continue;
                        }
                        if !*begun {
                            walk.skip_empty(&|s| lens(self, s));
                        }
                        if walk.done() {
                            let end = last.max(pp.sender.last());
                            io.send(frame_bytes(&Frame::SyncDone { at: end }, p, term));
                            pp.sender.resume(m, end);
                            x.direct = None;
                            continue;
                        }
                        if !bucket.try_take(now_ns, u64::from(WALK_KEYS)) {
                            continue;
                        }
                        let slot = walk.slots[walk.idx];
                        if !*begun {
                            io.send(frame_bytes(&Frame::SnapBegin { slot }, p, term));
                            *begun = true;
                            *count = 0;
                        }
                        walk.busy = true;
                        let req = Request {
                            db: 0,
                            keys: SmallVec::new(),
                            kind: ReqKind::SlotIter {
                                slot,
                                fp_from: walk.fp_from as u32,
                                max_entries: WALK_KEYS,
                            },
                            origin: Origin::Client,
                        };
                        submits.push((
                            req,
                            ReplJob {
                                work: ReplWork::Walk,
                                then: Then::Listed {
                                    walker: Walker::Full { member: m, epoch },
                                },
                            },
                        ));
                    }
                }
            }
        }
        for (req, rj) in submits {
            self.submit_repl(cw, p, req, rj);
        }
    }

    // -----------------------------------------------------------------------------------------
    // member side

    fn member_hello(
        &self,
        cw: &ClusterWorker,
        p: u16,
        pr: &PartRepl,
        m: &MemberPart,
    ) -> Option<Vec<u8>> {
        let ident = cw.cs.identity();
        let h = Hello {
            cluster_id: ident.cluster_id?,
            auth: cw.cs.auth.clone(),
            node_id: ident.node_id?.0,
            shard: m.shard,
            partition: p,
            fence_term: ident.fence_term,
            last_term: pr.last_term,
            applied: pr.durable.upto(),
            recovered: if pr.syncing {
                Recovered::SyncInterrupted
            } else if cw.cs.boot.clean {
                Recovered::Clean
            } else {
                Recovered::Scanned
            },
            learner: false,
            migration: None,
        };
        Some(frame_bytes(&Frame::Hello(h), p, pr.last_term))
    }

    fn member_dial(&mut self, cw: &mut ClusterWorker, p: u16) {
        let now = self.now.boot_ns;
        if self.backend.state(p) == PartState::Loading {
            return;
        }
        let tag = cw.next_tag;
        let hello = {
            let Some(pr) = cw.parts.get(usize::from(p)).and_then(Option::as_ref) else {
                return;
            };
            let Role::Member(m) = &pr.role else { return };
            if m.primary.is_none() || m.io.is_some() || m.dial_tag.is_some() || now < m.next_dial_ns
            {
                return;
            }
            match self.member_hello(cw, p, pr, m) {
                Some(h) => h,
                None => return,
            }
        };
        let Some(net) = cw.cs.net.get() else { return };
        let Some(pr) = cw.parts.get_mut(usize::from(p)).and_then(Option::as_mut) else {
            return;
        };
        let Role::Member(m) = &mut pr.role else {
            return;
        };
        let Some(primary) = m.primary else { return };
        cw.next_tag += 1;
        m.dial_tag = Some(tag);
        m.next_dial_ns = now + REDIAL_NS;
        net.dial(self.id, p, tag, primary, hello);
    }

    fn member_frame(&mut self, cw: &mut ClusterWorker, p: u16, stream: u64, term: u32, f: Frame) {
        let fence = cw.cs.fence_term();
        let now = self.now.boot_ns;
        let actions = {
            let Some(pr) = cw.part(p) else { return };
            let Role::Member(m) = &mut pr.role else {
                return;
            };
            if m.io.as_ref().is_none_or(|i| i.id != stream) {
                return;
            }
            m.last_rx_ns = now;
            if term < fence {
                // a frame of a fenced term
                if let Some(io) = m.io.take() {
                    io.abort();
                }
                return;
            }
            if let Frame::Reject(r) = &f {
                if m.last_reject != Some(*r) {
                    tracing::info!(partition = p, "the primary refused the stream: {r:?}");
                } else {
                    tracing::debug!(partition = p, "the primary refused the stream again: {r:?}");
                }
                m.last_reject = Some(*r);
                m.next_dial_ns = now + REDIAL_REJECTED_NS;
                if let Some(io) = m.io.take() {
                    io.abort();
                }
                return;
            }
            let mut acts = Vec::new();
            match m.stream.on_frame(f) {
                Ok(a) => acts.push(a),
                Err(e) => {
                    tracing::warn!(partition = p, "replication stream error: {e}");
                    if let Some(io) = m.io.take() {
                        io.abort();
                    }
                    return;
                }
            }
            while let Some(a) = m.stream.next_action() {
                acts.push(a);
            }
            acts
        };
        for a in actions {
            self.member_action(cw, p, a);
        }
    }

    fn member_action(&mut self, cw: &mut ClusterWorker, p: u16, a: MemberAction) {
        // actions after SyncBegin wait until the SYNCING flag is durable
        {
            let Some(pr) = cw.part(p) else { return };
            let Role::Member(m) = &mut pr.role else {
                return;
            };
            if let MemberSync::Full {
                held,
                flag_durable: false,
                ..
            } = &mut m.sync
            {
                held.push_back(a);
                return;
            }
        }
        match a {
            MemberAction::Nothing | MemberAction::SnapBegin(_) | MemberAction::SnapEnd { .. } => {}
            MemberAction::Commit(_) => {}
            MemberAction::Apply(g) => self.member_apply(cw, p, g),
            MemberAction::Image(rec, at) => self.member_image(cw, p, rec, at),
            MemberAction::Snap(rec, stamp) => self.member_snap(cw, p, rec, stamp),
            MemberAction::SyncBegin(at) => {
                let token = cw.cs.token(REPL_TOKEN_BIT);
                let (shard, term) = {
                    let Some(pr) = cw.part(p) else { return };
                    let Role::Member(m) = &mut pr.role else {
                        return;
                    };
                    m.sync = MemberSync::Full {
                        at,
                        end: None,
                        open: 0,
                        confirmed: KeySet::default(),
                        prune: None,
                        held: VecDeque::new(),
                        flag_durable: false,
                    };
                    m.ready_reported = Some(false);
                    let st = (m.shard, m.term);
                    pr.syncing = true;
                    let up = pr.durable.upto().min(before(at));
                    pr.durable.reset(up);
                    pr.durable.push(at, false);
                    pr.meta_wait = Some(token);
                    st
                };
                cw.cs.syncing[usize::from(p)].store(true, Ordering::Release);
                // SYNCING is durable before the first record is applied (§23.7)
                let pm = self.part_meta(cw, p);
                self.backend
                    .meta_request(p, token, MetaReq::PartitionMeta { m: pm });
                cw.cs.tell_agent(AgentMsg::PartReady {
                    shard,
                    term,
                    partition: p,
                    ready: false,
                });
                tracing::info!(partition = p, "merge full sync started");
            }
            MemberAction::SendRepairKeys => self.member_repair_keys(cw, p),
            MemberAction::RepairDone(end) => {
                let Some(pr) = cw.part(p) else { return };
                let Role::Member(m) = &mut pr.role else {
                    return;
                };
                if let MemberSync::Repair { end: e, .. } = &mut m.sync {
                    *e = Some(end);
                }
                self.member_sync_progress(cw, p);
            }
            MemberAction::SyncDone(end) => {
                let slots = slots_of(p, self.shared.partitions);
                let Some(pr) = cw.part(p) else { return };
                let Role::Member(m) = &mut pr.role else {
                    return;
                };
                if let MemberSync::Full { end: e, prune, .. } = &mut m.sync {
                    *e = Some(end);
                    *prune = Some(KeyWalk::new(slots));
                }
                self.member_sync_progress(cw, p);
            }
            MemberAction::Seal { .. } => {}
        }
    }

    fn member_apply(&mut self, cw: &mut ClusterWorker, p: u16, g: Group) {
        let Some(pr) = cw.part(p) else { return };
        let Role::Member(m) = &mut pr.role else {
            return;
        };
        let epoch = m.epoch;
        let seq = g.seq;
        pr.durable.push(seq, false);
        // keyed runs and partition-wide ops, in order
        let mut reqs: Vec<(Request, ReplJob)> = Vec::new();
        let mut run: Vec<Op> = Vec::new();
        let flush_run = |run: &mut Vec<Op>, reqs: &mut Vec<(Request, ReplJob)>| {
            if run.is_empty() {
                return;
            }
            let ops = std::mem::take(run);
            let req = Request {
                db: op_db(&ops),
                keys: op_keys(&ops, false),
                kind: ReqKind::Keyed,
                origin: Origin::Replicated(seq),
            };
            reqs.push((
                req,
                ReplJob {
                    work: ReplWork::Apply {
                        ops,
                        seq,
                        mode: ApplyMode::Ordered,
                    },
                    then: Then::MemberGroup { epoch, seq },
                },
            ));
        };
        for op in g.ops {
            match op {
                Op::FlushDb { db } => {
                    flush_run(&mut run, &mut reqs);
                    let req = Request {
                        db: db.unwrap_or(0),
                        keys: SmallVec::new(),
                        kind: ReqKind::FlushDb { db },
                        origin: Origin::Replicated(seq),
                    };
                    reqs.push((
                        req,
                        ReplJob {
                            work: ReplWork::Apply {
                                ops: Vec::new(),
                                seq,
                                mode: ApplyMode::Ordered,
                            },
                            then: Then::MemberGroup { epoch, seq },
                        },
                    ));
                }
                Op::FlushSlot { .. } | Op::Noop => {}
                op => run.push(op),
            }
        }
        flush_run(&mut run, &mut reqs);
        if reqs.is_empty() {
            pr.durable.done(seq);
            return;
        }
        m.groups.insert(seq.0, reqs.len() as u32);
        for (req, rj) in reqs {
            self.submit_repl(cw, p, req, rj);
        }
    }

    fn member_image(&mut self, cw: &mut ClusterWorker, p: u16, rec: ImageRec, at: Seq) {
        let Some(pr) = cw.part(p) else { return };
        let Role::Member(m) = &mut pr.role else {
            return;
        };
        let epoch = m.epoch;
        if let MemberSync::Repair { open, .. } = &mut m.sync {
            *open += 1;
        }
        let op = match rec.image {
            Some((vtype, expire_at, value)) => Op::Put {
                db: rec.db,
                vtype,
                expire_at,
                key: rec.key,
                value,
            },
            None => Op::Del {
                db: rec.db,
                key: rec.key,
            },
        };
        let ops = vec![op];
        let req = Request {
            db: op_db(&ops),
            keys: op_keys(&ops, false),
            kind: ReqKind::Keyed,
            origin: Origin::Image(at),
        };
        self.submit_repl(
            cw,
            p,
            req,
            ReplJob {
                work: ReplWork::Apply {
                    ops,
                    seq: at,
                    mode: ApplyMode::Image,
                },
                then: Then::MemberImage {
                    epoch,
                    confirm: None,
                },
            },
        );
    }

    fn member_snap(&mut self, cw: &mut ClusterWorker, p: u16, rec: SnapRec, stamp: Seq) {
        let Some(pr) = cw.part(p) else { return };
        let Role::Member(m) = &mut pr.role else {
            return;
        };
        let epoch = m.epoch;
        if let MemberSync::Full { open, .. } = &mut m.sync {
            *open += 1;
        }
        let confirm = Some((rec.db, rec.key.clone()));
        let ops = vec![Op::Put {
            db: rec.db,
            vtype: rec.vtype,
            expire_at: rec.expire_at,
            key: rec.key,
            value: rec.value,
        }];
        let req = Request {
            db: op_db(&ops),
            keys: op_keys(&ops, true),
            kind: ReqKind::Keyed,
            origin: Origin::Snapshot(stamp),
        };
        self.submit_repl(
            cw,
            p,
            req,
            ReplJob {
                work: ReplWork::Apply {
                    ops,
                    seq: stamp,
                    mode: ApplyMode::SnapshotMerge,
                },
                then: Then::MemberImage { epoch, confirm },
            },
        );
    }

    /// `REPAIR_KEYS`: the keys of this member's own groups after its term's takeover point.
    fn member_repair_keys(&mut self, cw: &mut ClusterWorker, p: u16) {
        let history: Vec<(u32, Vec<u64>)> = cw
            .assignment()
            .and_then(|a| a.role.as_ref())
            .map(|r| {
                r.history
                    .iter()
                    .map(|t| (t.term, t.takeover.clone()))
                    .collect()
            })
            .unwrap_or_default();
        let slots = slots_of(p, self.shared.partitions);
        // the highest position this partition holds: a ring is a complete key source only if
        // it reaches it (after a restart the previous process may have applied groups past
        // what this process received)
        let upto = before(self.backend.next_seq(p));
        let cs = cw.cs.clone();
        let Some(pr) = cw.part(p) else { return };
        let last_term = pr.last_term;
        let Role::Member(m) = &mut pr.role else {
            return;
        };
        let Some(io) = m.io.as_ref() else { return };
        let Some((_, AcceptMode::Repair { at })) = m.stream.accepted() else {
            return;
        };
        let takeover = sync::takeover(&history, last_term, p);
        let from_ring = takeover.and_then(|t| {
            let mut rings: SmallVec<[&Backlog; 2]> = SmallVec::new();
            if let Some(b) = m.own_groups.as_ref() {
                rings.push(b);
            }
            rings.push(m.stream.received());
            kv_repl::member::own_keys_after(&rings, t, upto)
        });
        m.sync = MemberSync::Repair {
            at,
            end: None,
            open: 0,
        };
        let up = pr.durable.upto().min(before(at));
        pr.durable.reset(up);
        pr.durable.push(at, false);
        let shard = m.shard;
        let term = m.term;
        cs.tell_agent(AgentMsg::PartReady {
            shard,
            term,
            partition: p,
            ready: false,
        });
        if let Some(ks) = from_ring {
            io.send(frame_bytes(&Frame::RepairKeys(ks.into_vec()), p, term));
            m.stream.reset_received(at);
            return;
        }
        // not covered by a ring (the node restarted): walk this partition's keys for every key
        // whose stored version is newer than the takeover point
        m.divergent = Some((KeyWalk::new(slots), KeySet::default()));
        let _ = takeover;
    }

    fn drive_divergent(&mut self, cw: &mut ClusterWorker, p: u16) {
        let history: Vec<(u32, Vec<u64>)> = cw
            .assignment()
            .and_then(|a| a.role.as_ref())
            .map(|r| {
                r.history
                    .iter()
                    .map(|t| (t.term, t.takeover.clone()))
                    .collect()
            })
            .unwrap_or_default();
        let lens = |w: &Self, s: u16| w.backend.slot_len(p, 0, s);
        let submit;
        {
            let Some(pr) = cw.part(p) else { return };
            let last_term = pr.last_term;
            let Role::Member(m) = &mut pr.role else {
                return;
            };
            let epoch = m.epoch;
            let Some((walk, ks)) = &mut m.divergent else {
                return;
            };
            if walk.busy {
                return;
            }
            walk.skip_empty(&|s| lens(self, s));
            if walk.done() {
                let keys = std::mem::take(ks).into_vec();
                m.divergent = None;
                if let (Some(io), Some((_, AcceptMode::Repair { at }))) =
                    (m.io.as_ref(), m.stream.accepted())
                {
                    tracing::info!(
                        partition = p,
                        keys = keys.len(),
                        "repair keys from the partition walk"
                    );
                    io.send(frame_bytes(&Frame::RepairKeys(keys), p, m.term));
                    m.stream.reset_received(at);
                }
                return;
            }
            let _ = sync::takeover(&history, last_term, p);
            walk.busy = true;
            let slot = walk.slots[walk.idx];
            let req = Request {
                db: 0,
                keys: SmallVec::new(),
                kind: ReqKind::SlotIter {
                    slot,
                    fp_from: walk.fp_from as u32,
                    max_entries: WALK_KEYS,
                },
                origin: Origin::Client,
            };
            submit = Some((
                req,
                ReplJob {
                    work: ReplWork::Walk,
                    then: Then::Listed {
                        walker: Walker::Divergent { epoch },
                    },
                },
            ));
        }
        if let Some((req, rj)) = submit {
            self.submit_repl(cw, p, req, rj);
        }
    }

    /// Advances a member's repair or full sync (prune walk, completion).
    fn member_sync_progress(&mut self, cw: &mut ClusterWorker, p: u16) {
        let lens = |w: &Self, s: u16| w.backend.slot_len(p, 0, s);
        let mut submit = None;
        let mut finished = None;
        {
            let Some(pr) = cw.part(p) else { return };
            let Role::Member(m) = &mut pr.role else {
                return;
            };
            let epoch = m.epoch;
            match &mut m.sync {
                MemberSync::None => return,
                MemberSync::Repair {
                    at,
                    end: Some(end),
                    open: 0,
                } => finished = Some((*at, *end, false)),
                MemberSync::Repair { .. } => {}
                MemberSync::Full {
                    at,
                    end: Some(end),
                    open,
                    prune: Some(walk),
                    ..
                } => {
                    if walk.busy {
                        return;
                    }
                    walk.skip_empty(&|s| lens(self, s));
                    if walk.done() {
                        if *open == 0 {
                            finished = Some((*at, *end, true));
                        }
                    } else {
                        walk.busy = true;
                        let slot = walk.slots[walk.idx];
                        let req = Request {
                            db: 0,
                            keys: SmallVec::new(),
                            kind: ReqKind::SlotIter {
                                slot,
                                fp_from: walk.fp_from as u32,
                                max_entries: WALK_KEYS,
                            },
                            origin: Origin::Client,
                        };
                        submit = Some((
                            req,
                            ReplJob {
                                work: ReplWork::Walk,
                                then: Then::Listed {
                                    walker: Walker::Prune { epoch },
                                },
                            },
                        ));
                    }
                }
                MemberSync::Full { .. } => {}
            }
        }
        if let Some((req, rj)) = submit {
            self.submit_repl(cw, p, req, rj);
        }
        if let Some((at, end, full)) = finished {
            let token = cw.cs.token(REPL_TOKEN_BIT);
            let (shard, term) = {
                let Some(pr) = cw.part(p) else { return };
                let Role::Member(m) = &mut pr.role else {
                    return;
                };
                m.sync = MemberSync::None;
                m.ready_reported = Some(true);
                // caught up in the current term: the backlog of a term this node served as
                // primary is no Repair key source any more (its locked memory goes)
                m.own_groups = None;
                let st = (m.shard, m.term);
                if let Some((t, _)) = m.stream.accepted() {
                    pr.last_term = pr.last_term.max(t);
                }
                pr.durable.done(at);
                pr.durable.push(end, true);
                if full {
                    pr.syncing = false;
                    pr.meta_wait = Some(token);
                }
                st
            };
            if full {
                cw.cs.syncing[usize::from(p)].store(false, Ordering::Release);
                let pm = self.part_meta(cw, p);
                self.backend
                    .meta_request(p, token, MetaReq::PartitionMeta { m: pm });
                tracing::info!(partition = p, "merge full sync complete");
            } else {
                tracing::info!(partition = p, "repair complete");
            }
            cw.cs.tell_agent(AgentMsg::PartReady {
                shard,
                term,
                partition: p,
                ready: true,
            });
        }
    }

    // -----------------------------------------------------------------------------------------
    // job completions

    /// A replication request finished (`res`: its output, or the error it failed with).
    pub(crate) fn repl_job_done(
        &mut self,
        p: u16,
        rj: ReplJob,
        res: Result<ReplOut, StoreError>,
        eo: Option<&ExecOut>,
    ) {
        if let Some(eo) = eo
            && let Some(g) = &eo.group
        {
            // own writes of a primary (migration destination, purge) replicate like any other
            let _ = self.cluster_group(p, g, eo.ticket);
        }
        let ticket = eo.and_then(|e| e.ticket);
        let wrote = eo.and_then(|e| e.seq);
        self.with_cluster(|w, cw| w.repl_continue(cw, p, rj, res, ticket, wrote));
    }

    fn repl_continue(
        &mut self,
        cw: &mut ClusterWorker,
        p: u16,
        rj: ReplJob,
        res: Result<ReplOut, StoreError>,
        ticket: Option<TicketId>,
        wrote: Option<Seq>,
    ) {
        let need = self.local_need();
        let durable_now = ticket.is_none() || need.is_none();
        if let Err(e) = &res {
            tracing::warn!(partition = p, "replication request failed: {e:?}");
        }
        match rj.then {
            Then::MemberGroup { epoch, seq } => {
                let Some(pr) = cw.part(p) else { return };
                let Role::Member(m) = &mut pr.role else {
                    return;
                };
                if m.epoch != epoch {
                    return;
                }
                if res.is_err() {
                    // the stream restarts from the durable prefix
                    if let Some(io) = m.io.take() {
                        io.abort();
                    }
                    return;
                }
                match (durable_now, ticket) {
                    (false, Some(t)) => {
                        pr.tickets.insert(t.0, TicketFor::Group { epoch, seq });
                    }
                    _ => group_part_done(m, &mut pr.durable, seq),
                }
            }
            Then::MemberImage { epoch, confirm } => {
                let Some(pr) = cw.part(p) else { return };
                let Role::Member(m) = &mut pr.role else {
                    return;
                };
                if m.epoch != epoch {
                    return;
                }
                if let (Ok(ReplOut::Applied(o)), Some(k), MemberSync::Full { confirmed, .. }) =
                    (&res, confirm, &mut m.sync)
                    && o.first() == Some(&ApplyOutcome::Confirmed)
                {
                    confirmed.insert(k.0, &k.1);
                }
                if res.is_err() {
                    if let Some(io) = m.io.take() {
                        io.abort();
                    }
                    return;
                }
                match (durable_now, ticket) {
                    (false, Some(t)) => {
                        pr.tickets.insert(t.0, TicketFor::Sync { epoch });
                    }
                    _ => {
                        if let MemberSync::Repair { open, .. } | MemberSync::Full { open, .. } =
                            &mut m.sync
                        {
                            *open = open.saturating_sub(1);
                        }
                    }
                }
                self.member_sync_progress(cw, p);
            }
            Then::MemberPrune { epoch } => {
                let Some(pr) = cw.part(p) else { return };
                let Role::Member(m) = &mut pr.role else {
                    return;
                };
                if m.epoch != epoch {
                    return;
                }
                if let MemberSync::Full {
                    open,
                    prune: Some(walk),
                    ..
                } = &mut m.sync
                {
                    walk.busy = false;
                    if let (false, Some(t)) = (durable_now, ticket) {
                        *open += 1;
                        pr.tickets.insert(t.0, TicketFor::Sync { epoch });
                    }
                }
                self.member_sync_progress(cw, p);
            }
            Then::MemberSeqs { epoch, above } => {
                let Some(pr) = cw.part(p) else { return };
                let Role::Member(m) = &mut pr.role else {
                    return;
                };
                if m.epoch != epoch {
                    return;
                }
                if let (Some((walk, ks)), ReplWork::Seqs { db, keys }) =
                    (&mut m.divergent, &rj.work)
                {
                    walk.busy = false;
                    if let Ok(ReplOut::Seqs(seqs)) = &res {
                        for (k, s) in keys.iter().zip(seqs) {
                            if *s > above {
                                ks.insert(*db, k);
                            }
                        }
                    }
                }
            }
            Then::Listed { walker } => self.walk_listed(cw, p, walker, res),
            Then::PrimaryRead {
                member,
                epoch,
                snap,
            } => {
                let Some(pr) = cw.part(p) else { return };
                let Role::Primary(pp) = &mut pr.role else {
                    return;
                };
                let term = pp.term;
                let Some(x) = pp.peers.get_mut(usize::from(member)) else {
                    return;
                };
                if x.epoch != epoch {
                    return;
                }
                let (Some(io), ReplWork::Read { db, keys }) = (&x.stream, &rj.work) else {
                    return;
                };
                let images = match res {
                    Ok(ReplOut::Images(v)) => v,
                    _ => {
                        // the member restarts its stream (and the decision) from scratch
                        io.abort();
                        return;
                    }
                };
                let mut out = Vec::new();
                let mut n = 0u64;
                for (k, img) in keys.iter().zip(images) {
                    if snap {
                        if let Some((vtype, expire_at, value)) = img {
                            frame::encode(
                                &Frame::SnapRec(SnapRec {
                                    db: *db,
                                    vtype,
                                    expire_at,
                                    key: k.clone(),
                                    value,
                                }),
                                p,
                                term,
                                &mut out,
                            );
                            n += 1;
                        }
                    } else {
                        frame::encode(
                            &Frame::Image(ImageRec {
                                db: *db,
                                key: k.clone(),
                                image: img,
                            }),
                            p,
                            term,
                            &mut out,
                        );
                    }
                }
                io.send(out);
                match &mut x.direct {
                    Some(Direct::Repair { reading, .. }) => *reading = false,
                    Some(Direct::Full {
                        reading,
                        walk,
                        begun,
                        count,
                        ..
                    }) => {
                        *reading = false;
                        *count += n;
                        if walk.fp_from == 0 && *begun {
                            // the walk moved past this slot when it was listed
                            let slot = walk.slots[walk.idx.saturating_sub(1)];
                            io.send(frame_bytes(
                                &Frame::SnapEnd {
                                    slot,
                                    count: *count,
                                },
                                p,
                                term,
                            ));
                            *begun = false;
                        }
                    }
                    None => {}
                }
            }
            Then::MigOutRead { id, epoch } => self.mig_out_read(cw, p, id, epoch, &rj.work, res),
            Then::MigInApply { id, epoch, item } => {
                self.mig_in_applied(cw, p, id, epoch, item, wrote, res.is_ok())
            }
            Then::Purged => {
                if let Some(pr) = cw.part(p)
                    && let Some((walk, _)) = &mut pr.purge
                {
                    walk.busy = false;
                }
            }
        }
    }

    fn walk_listed(
        &mut self,
        cw: &mut ClusterWorker,
        p: u16,
        walker: Walker,
        res: Result<ReplOut, StoreError>,
    ) {
        let (next, keys) = match res {
            Ok(ReplOut::Listed { next, keys }) => (next, keys),
            Ok(_) | Err(_) => (u64::MAX, Vec::new()),
        };
        let failed = next == u64::MAX && keys.is_empty();
        match walker {
            Walker::Full { member, epoch } => {
                let mut submit = None;
                {
                    let Some(pr) = cw.part(p) else { return };
                    let Role::Primary(pp) = &mut pr.role else {
                        return;
                    };
                    let term = pp.term;
                    let Some(x) = pp.peers.get_mut(usize::from(member)) else {
                        return;
                    };
                    if x.epoch != epoch {
                        return;
                    }
                    let Some(Direct::Full {
                        walk,
                        reading,
                        begun,
                        count,
                        ..
                    }) = &mut x.direct
                    else {
                        return;
                    };
                    walk.busy = false;
                    if failed {
                        if let Some(io) = &x.stream {
                            io.abort();
                        }
                        return;
                    }
                    let slot = walk.slots[walk.idx];
                    walk.listed(next);
                    if keys.is_empty() {
                        if walk.fp_from == 0
                            && let Some(io) = &x.stream
                        {
                            io.send(frame_bytes(
                                &Frame::SnapEnd {
                                    slot,
                                    count: *count,
                                },
                                p,
                                term,
                            ));
                            *begun = false;
                        }
                    } else {
                        *reading = true;
                        let req = Request {
                            db: 0,
                            keys: keys.iter().map(|k| (k.clone(), Access::Read)).collect(),
                            kind: ReqKind::Keyed,
                            origin: Origin::Client,
                        };
                        submit = Some((
                            req,
                            ReplJob {
                                work: ReplWork::Read { db: 0, keys },
                                then: Then::PrimaryRead {
                                    member,
                                    epoch,
                                    snap: true,
                                },
                            },
                        ));
                    }
                }
                if let Some((req, rj)) = submit {
                    self.submit_repl(cw, p, req, rj);
                }
            }
            Walker::Prune { epoch } => {
                let mut submit = None;
                {
                    let Some(pr) = cw.part(p) else { return };
                    let Role::Member(m) = &mut pr.role else {
                        return;
                    };
                    if m.epoch != epoch {
                        return;
                    }
                    let MemberSync::Full {
                        at,
                        confirmed,
                        prune: Some(walk),
                        ..
                    } = &mut m.sync
                    else {
                        return;
                    };
                    walk.busy = false;
                    if failed {
                        return;
                    }
                    walk.listed(next);
                    let doomed: Vec<Box<[u8]>> = keys
                        .into_iter()
                        .filter(|k| !confirmed.contains(0, k))
                        .collect();
                    if !doomed.is_empty() {
                        walk.busy = true;
                        let at = *at;
                        let req = Request {
                            db: 0,
                            keys: doomed.iter().map(|k| (k.clone(), Access::Write)).collect(),
                            kind: ReqKind::Keyed,
                            origin: Origin::Snapshot(at),
                        };
                        submit = Some((
                            req,
                            ReplJob {
                                work: ReplWork::Prune {
                                    db: 0,
                                    keys: doomed,
                                    below: at,
                                    at,
                                },
                                then: Then::MemberPrune { epoch },
                            },
                        ));
                    }
                }
                if let Some((req, rj)) = submit {
                    self.submit_repl(cw, p, req, rj);
                } else {
                    self.member_sync_progress(cw, p);
                }
            }
            Walker::Divergent { epoch } => {
                let history: Vec<(u32, Vec<u64>)> = cw
                    .assignment()
                    .and_then(|a| a.role.as_ref())
                    .map(|r| {
                        r.history
                            .iter()
                            .map(|t| (t.term, t.takeover.clone()))
                            .collect()
                    })
                    .unwrap_or_default();
                let mut submit = None;
                {
                    let Some(pr) = cw.part(p) else { return };
                    let above = sync::takeover(&history, pr.last_term, p).unwrap_or(Seq::ZERO);
                    let Role::Member(m) = &mut pr.role else {
                        return;
                    };
                    if m.epoch != epoch {
                        return;
                    }
                    let Some((walk, _)) = &mut m.divergent else {
                        return;
                    };
                    walk.busy = false;
                    if failed {
                        return;
                    }
                    walk.listed(next);
                    if !keys.is_empty() {
                        walk.busy = true;
                        let req = Request {
                            db: 0,
                            keys: keys.iter().map(|k| (k.clone(), Access::Meta)).collect(),
                            kind: ReqKind::Keyed,
                            origin: Origin::Client,
                        };
                        submit = Some((
                            req,
                            ReplJob {
                                work: ReplWork::Seqs { db: 0, keys },
                                then: Then::MemberSeqs { epoch, above },
                            },
                        ));
                    }
                }
                if let Some((req, rj)) = submit {
                    self.submit_repl(cw, p, req, rj);
                }
            }
            Walker::MigInPrune { id, epoch } => {
                self.mig_in_prune_listed(cw, p, id, epoch, next, keys, failed)
            }
            Walker::MigOut { id, epoch } => {
                self.mig_out_listed(cw, p, id, epoch, next, keys, failed)
            }
            Walker::Purge => {
                let mut submit = None;
                if let Some(pr) = cw.part(p)
                    && let Some((walk, _)) = &mut pr.purge
                {
                    walk.busy = false;
                    if !failed {
                        walk.listed(next);
                        if !keys.is_empty() {
                            walk.busy = true;
                            let ops: Vec<Op> = keys
                                .iter()
                                .map(|k| Op::Del {
                                    db: 0,
                                    key: k.clone(),
                                })
                                .collect();
                            let req = Request {
                                db: 0,
                                keys: keys.iter().map(|k| (k.clone(), Access::Write)).collect(),
                                kind: ReqKind::Keyed,
                                origin: Origin::Client,
                            };
                            submit = Some((
                                req,
                                ReplJob {
                                    work: ReplWork::Own { ops },
                                    then: Then::Purged,
                                },
                            ));
                        }
                    }
                }
                if let Some((req, rj)) = submit {
                    self.submit_repl(cw, p, req, rj);
                }
            }
        }
    }

    /// A meta reply for the replication glue of partition `p`.
    pub(crate) fn repl_meta_done(&mut self, p: u16, token: u64, done: MetaDone) {
        let failed = matches!(done, MetaDone::Failed(_));
        if let MetaDone::Failed(e) = done {
            tracing::error!(partition = p, errno = e, "partition meta write failed");
        }
        let held = self
            .with_cluster(|_, cw| {
                let pr = cw.part(p)?;
                if pr.meta_wait != Some(token) {
                    return None;
                }
                pr.meta_wait = None;
                let Role::Member(m) = &mut pr.role else {
                    return None;
                };
                if failed
                    && matches!(
                        m.sync,
                        MemberSync::Full {
                            flag_durable: false,
                            ..
                        }
                    )
                {
                    // SYNCING is not durable: no record of the merge may be applied. The
                    // stream ends; the next handshake starts the operator's sync again.
                    if let Some(io) = m.io.take() {
                        io.abort();
                    }
                    m.epoch += 1;
                    m.sync = MemberSync::None;
                    return None;
                }
                match &mut m.sync {
                    MemberSync::Full {
                        held, flag_durable, ..
                    } if !*flag_durable => {
                        *flag_durable = true;
                        Some(std::mem::take(held))
                    }
                    _ => None,
                }
            })
            .flatten();
        if let Some(held) = held {
            for a in held {
                self.with_cluster(|w, cw| w.member_action(cw, p, a));
            }
        }
    }

    /// The partition meta this node persists for `p` (positions are lower bounds).
    fn part_meta(&self, cw: &mut ClusterWorker, p: u16) -> PartitionMeta {
        let history: Vec<(u32, u64)> = cw
            .assignment()
            .and_then(|a| a.role.as_ref())
            .map(|r| {
                r.history
                    .iter()
                    .rev()
                    .take(4)
                    .map(|t| (t.term, t.takeover.get(usize::from(p)).copied().unwrap_or(0)))
                    .collect()
            })
            .unwrap_or_default();
        let Some(pr) = cw.part(p) else {
            return PartitionMeta::initial(p);
        };
        let commit = match &pr.role {
            Role::Primary(pp) => pp.sender.commit_seq(),
            Role::Member(m) => m.stream.commit().min(pr.durable.upto()),
            Role::Idle => pr.persisted.1,
        };
        let mut m = PartitionMeta::initial(p);
        m.flags = if pr.syncing { PMETA_SYNCING } else { 0 };
        m.term = pr.last_term;
        m.repl_durable = pr.durable.upto();
        m.repl_commit = commit;
        for (i, h) in history.into_iter().enumerate() {
            m.history[i] = h;
        }
        pr.persisted = (m.repl_durable, m.repl_commit, m.flags, m.term);
        m
    }

    // -----------------------------------------------------------------------------------------
    // the tick

    /// Step 4 of the tick for the cluster glue: applies pending roles, drives streams, walks
    /// and feeds, releases committed replies, acks, persists positions, reports to the agent.
    pub(crate) fn cluster_tick(&mut self) {
        self.with_cluster(|w, cw| {
            if !cw.stopped {
                w.cluster_tick_inner(cw);
            }
        });
    }

    /// Shutdown: every stream ends, background sync and migration work stops, and replies
    /// still waiting for the shard commit are answered as not acknowledged (the drain then
    /// waits only for the partitions' own writes).
    pub(crate) fn cluster_stop(&mut self) {
        let waits = self
            .with_cluster(|_, cw| {
                cw.stopped = true;
                let mut out = Vec::new();
                for pr in cw.parts.iter_mut().flatten() {
                    match &mut pr.role {
                        Role::Primary(pp) => {
                            for x in pp.peers.iter_mut() {
                                if let Some(io) = x.stream.take() {
                                    io.abort();
                                }
                                x.direct = None;
                            }
                        }
                        Role::Member(m) => {
                            if let Some(io) = m.io.take() {
                                io.abort();
                            }
                            m.sync = MemberSync::None;
                            m.divergent = None;
                            m.next_dial_ns = u64::MAX;
                        }
                        Role::Idle => {}
                    }
                    for mo in pr.mig_out.drain(..) {
                        mo.abort();
                    }
                    for mi in pr.mig_in.drain(..) {
                        mi.abort();
                    }
                    pr.purge = None;
                    pr.queued.clear();
                    out.extend(pr.commit_waits.drain(..).map(|(_, _, w)| w));
                }
                cw.streams.clear();
                cw.unapplied.clear();
                out
            })
            .unwrap_or_default();
        for w in waits {
            self.commit_released(w, Some(b"TRYAGAIN the node is shutting down"));
        }
    }

    fn cluster_tick_inner(&mut self, cw: &mut ClusterWorker) {
        if !cw.unapplied.is_empty() {
            self.apply_roles(cw);
        }
        let now = self.now.boot_ns;
        let partitions = self.shared.partitions;
        let lease_ok = self.shared.lease.valid(now);
        let below_min = self.shared.ownership.read().below_min_isr();
        let persist = now.saturating_sub(cw.last_persist_ns) >= PERSIST_EVERY_NS;
        if persist {
            cw.last_persist_ns = now;
        }
        // keys in slots this node does not serve (DBSIZE leaves them out), every 50 ms
        let filter = persist.then(|| self.shared.ownership.read().slot_filter());
        let ids: Vec<u16> = cw.parts.iter().flatten().map(|pr| pr.p).collect();
        let cs = cw.cs.clone();
        let worker_handoff = cw.handoff;
        let mut handoff_last: Vec<(u16, u64)> = Vec::new();
        let mut handoff_open = false;
        for p in ids {
            self.drain_queued(cw, p);
            let is_primary = matches!(cw.part(p).map(|pr| &pr.role), Some(Role::Primary(_)));
            if is_primary {
                self.drive_direct(cw, p);
                self.drive_mig_out(cw, p);
                self.drive_purge(cw, p);
                let mut released: Vec<(CommitWaiter, Option<&'static [u8]>)> = Vec::new();
                let ack_timeout = cw.ack_timeout_ns;
                let mut reports: Vec<AgentMsg> = Vec::new();
                if let Some(pr) = cw.part(p)
                    && let Role::Primary(pp) = &mut pr.role
                {
                    pp.sender.set_local_durable(pr.durable.upto());
                    let term = pp.term;
                    let shard = pp.shard;
                    // backlog streams
                    for (i, x) in pp.peers.iter_mut().enumerate() {
                        let m = i as MemberId;
                        let Some(io) = &x.stream else { continue };
                        if pp.sender.overrun(m) {
                            tracing::info!(
                                partition = p,
                                "a member fell behind the backlog: its stream restarts"
                            );
                            io.abort();
                            continue;
                        }
                        if io.unsent() < SEND_WINDOW {
                            let mut out = Vec::new();
                            if pp.sender.fill_send_at(m, &mut out, SEND_WINDOW, now) {
                                io.send(out);
                            }
                        }
                    }
                    // ISR shrink requests and expands (age tests and position tests only)
                    let lag = pp.sender.lagging(now, ack_timeout);
                    let mut recount = false;
                    let gate = &cs.expand_gate;
                    for m in lag {
                        let Some(x) = pp.peers.get_mut(usize::from(m)) else {
                            continue;
                        };
                        if x.in_isr {
                            if now.saturating_sub(x.last_shrink_ns) >= 100_000_000 {
                                x.last_shrink_ns = now;
                                reports.push(AgentMsg::Shrink {
                                    shard,
                                    term,
                                    member: x.node,
                                });
                            }
                        } else if x.expanding && gate.set(x.node, term, p, partitions, false) {
                            // its expand was not requested yet: it stops counting and catches
                            // up again (once requested it counts until the committed ISR shows
                            // it, and then leaves through the shrink path)
                            x.expanding = false;
                            x.reported_ready = false;
                            recount = true;
                        }
                    }
                    for (i, x) in pp.peers.iter_mut().enumerate() {
                        let m = i as MemberId;
                        let eligible = x.present
                            && !x.in_isr
                            && !matches!(
                                x.state,
                                MemberState::Syncing | MemberState::NeedsFullSync
                            );
                        // 1. its catch-up barriers are done: count its acks
                        if eligible && !x.expanding && pp.sender.caught_up(m) {
                            x.expanding = true;
                            recount = true;
                        }
                        if x.expanding
                            && !pp.sender.is_attached(m)
                            && gate.set(x.node, term, p, partitions, false)
                        {
                            x.expanding = false;
                            x.reported_ready = false;
                            recount = true;
                        }
                        // 2. it holds every write committed before it counted: ready for its
                        // IsrExpand (the agent asks once every partition is ready)
                        let ready = x.expanding && !x.in_isr && pp.sender.holds_committed(m);
                        if ready != x.reported_ready
                            && !x.in_isr
                            && gate.set(x.node, term, p, partitions, ready)
                        {
                            x.reported_ready = ready;
                            if ready {
                                reports.push(AgentMsg::Expand {
                                    shard,
                                    term,
                                    member: x.node,
                                    partition: p,
                                    counted: true,
                                });
                            }
                        }
                    }
                    if recount {
                        let counted: Vec<MemberId> = pp
                            .peers
                            .iter()
                            .enumerate()
                            .filter(|(_, x)| x.present && (x.in_isr || x.expanding))
                            .map(|(i, _)| i as MemberId)
                            .collect();
                        pp.sender.set_isr(&counted);
                    }
                    // committed replies
                    let commit = pp.sender.commit_seq();
                    if !pr.commit_waits.is_empty() {
                        let mut i = 0;
                        while i < pr.commit_waits.len() {
                            let (seq, since) = (pr.commit_waits[i].0, pr.commit_waits[i].1);
                            if lease_ok && seq <= commit {
                                released.push((pr.commit_waits.swap_remove(i).2, None));
                            } else if now.saturating_sub(since) >= COMMIT_WAIT_LIMIT_NS {
                                // no commit and no committed ISR change within the bound (the
                                // coordinator is unreachable or the lease is gone): the write
                                // is answered as not acknowledged rather than held forever
                                released.push((pr.commit_waits.swap_remove(i).2, Some(&b"TRYAGAIN the write was not acknowledged by the shard in time"[..])));
                            } else {
                                i += 1;
                            }
                        }
                    }
                    if let Some(req) = pp.handoff
                        && Some(req) == worker_handoff
                    {
                        if commit >= pp.sender.last() {
                            handoff_last.push((p, pp.sender.last().0));
                        } else {
                            handoff_open = true;
                        }
                    }
                }
                for m in reports {
                    cw.cs.tell_agent(m);
                }
                for (w, refused) in released {
                    if refused.is_some() {
                        self.commit_released(w, refused);
                    } else if below_min {
                        self.commit_released(
                            w,
                            Some(b"NOREPLICAS Not enough good replicas to write."),
                        );
                    } else {
                        self.commit_released(w, None);
                    }
                }
            } else {
                self.member_dial(cw, p);
                self.drive_divergent(cw, p);
                self.member_sync_progress(cw, p);
                if let Some(pr) = cw.part(p)
                    && let Role::Member(m) = &mut pr.role
                {
                    if m.io.is_some() && now.saturating_sub(m.last_rx_ns) >= MEMBER_IDLE_NS {
                        tracing::info!(
                            partition = p,
                            "no frame from the primary: the stream is redialed"
                        );
                        if let Some(io) = &m.io {
                            io.abort();
                        }
                    }
                    m.stream.note_durable(pr.durable.upto());
                    if let Some(io) = &m.io
                        && let Some(f) = m.stream.ack_due(self.now.mono_ns)
                    {
                        io.send(frame_bytes(&f, p, m.stream.accepted().map_or(0, |a| a.0)));
                    }
                    // a plain Continue on the current term is a ready partition too
                    if matches!(m.sync, MemberSync::None)
                        && m.divergent.is_none()
                        && m.ready_reported != Some(true)
                        && let Some((t, AcceptMode::Continue { .. })) = m.stream.accepted()
                    {
                        m.ready_reported = Some(true);
                        m.own_groups = None;
                        pr.last_term = pr.last_term.max(t);
                        cs.tell_agent(AgentMsg::PartReady {
                            shard: m.shard,
                            term: m.term,
                            partition: p,
                            ready: true,
                        });
                    }
                }
            }
            self.drive_mig_in(cw, p);
            // replication links for INFO replication (no ids or positions leave the node)
            let links = cw.part(p).map_or(0, |pr| match &pr.role {
                Role::Primary(pp) => pp
                    .peers
                    .iter()
                    .enumerate()
                    .filter(|(i, x)| {
                        *i < 63 && x.stream.is_some() && pp.sender.is_attached(*i as MemberId)
                    })
                    .fold(0u64, |acc, (i, _)| acc | (1u64 << i)),
                Role::Member(m) => {
                    if m.io.is_some() && m.stream.accepted().is_some() {
                        super::LINK_UP
                    } else {
                        0
                    }
                }
                Role::Idle => 0,
            });
            cs.set_links(p, links);
            if let Some(f) = &filter {
                let step = usize::from(self.shared.partitions.max(1));
                let n: u64 = (p..kv_common::SLOTS as u16)
                    .step_by(step)
                    .filter(|&s| !f.serves(s))
                    .map(|s| self.backend.slot_len(p, 0, s) as u64)
                    .sum();
                cs.set_unserved_keys(p, n);
            }
            if persist {
                let changed = cw.part(p).is_some_and(|pr| {
                    let commit = match &pr.role {
                        Role::Primary(pp) => pp.sender.commit_seq(),
                        Role::Member(m) => m.stream.commit().min(pr.durable.upto()),
                        Role::Idle => pr.persisted.1,
                    };
                    let flags = if pr.syncing { PMETA_SYNCING } else { 0 };
                    pr.meta_wait.is_none()
                        && (pr.durable.upto(), commit, flags, pr.last_term) != pr.persisted
                });
                if changed {
                    let token = cw.cs.token(REPL_TOKEN_BIT);
                    let pm = self.part_meta(cw, p);
                    self.backend
                        .meta_request(p, token, MetaReq::PartitionMeta { m: pm });
                }
            }
        }
        if let Some(req) = cw.handoff
            && !handoff_open
        {
            cw.handoff = None;
            cw.cs.tell_agent(AgentMsg::HandoffReached {
                req,
                last: handoff_last,
            });
        }
    }

    // -----------------------------------------------------------------------------------------
    // purge of slots this primary does not serve

    fn plan_purge(&mut self, cw: &mut ClusterWorker, p: u16) {
        let Some(a) = cw.assignment().cloned() else {
            return;
        };
        let Some(r) = a.role.as_ref().filter(|r| r.is_primary) else {
            return;
        };
        let mut keep = r.owned_slots.clone();
        for t in &r.migrations {
            let active = match t {
                MigrationTask::Out { phase, .. } => *phase != MigPhase::Done,
                MigrationTask::In { .. } => true,
            };
            if active {
                for s in t.slots().iter_slots() {
                    keep.insert(s);
                }
            }
        }
        let doomed: Vec<u16> = slots_of(p, self.shared.partitions)
            .into_iter()
            .filter(|&s| !keep.contains(s) && self.backend.slot_len(p, 0, s) > 0)
            .collect();
        let Some(pr) = cw.part(p) else { return };
        if doomed.is_empty() {
            pr.purge = None;
        } else if pr.purge.is_none() {
            tracing::info!(
                partition = p,
                slots = doomed.len(),
                "purging keys of slots this shard no longer serves"
            );
            pr.purge = Some((KeyWalk::new(doomed), false));
        }
    }

    fn drive_purge(&mut self, cw: &mut ClusterWorker, p: u16) {
        let submit;
        {
            let Some(pr) = cw.part(p) else { return };
            let Some((walk, _)) = &mut pr.purge else {
                return;
            };
            if walk.busy {
                return;
            }
            if walk.done() {
                pr.purge = None;
                return;
            }
            walk.busy = true;
            let slot = walk.slots[walk.idx];
            let req = Request {
                db: 0,
                keys: SmallVec::new(),
                kind: ReqKind::SlotIter {
                    slot,
                    fp_from: walk.fp_from as u32,
                    max_entries: WALK_KEYS,
                },
                origin: Origin::Client,
            };
            submit = Some((
                req,
                ReplJob {
                    work: ReplWork::Walk,
                    then: Then::Listed {
                        walker: Walker::Purge,
                    },
                },
            ));
        }
        if let Some((req, rj)) = submit {
            self.submit_repl(cw, p, req, rj);
        }
    }

    /// Whether a write on `slot` may still execute here as the shard primary (the owner-side
    /// re-check of the gate at execution time).
    pub(crate) fn owner_refuses(
        &self,
        flags: CmdFlags,
        slot: Option<u16>,
        env: &ExecEnv,
    ) -> Option<super::ownership::Gate> {
        let lease_ok = self.shared.lease.valid(self.now.boot_ns);
        let g = self.shared.ownership.read().gate(
            slot,
            flags,
            env.readonly_conn,
            lease_ok,
            env.conn_id,
        );
        (g != super::ownership::Gate::Serve).then_some(g)
    }
}

impl PartRepl {
    /// The shard commit point when this partition is the primary.
    pub(super) fn primary_commit(&self) -> Option<Seq> {
        match &self.role {
            Role::Primary(pp) => Some(pp.sender.commit_seq()),
            _ => None,
        }
    }
}

/// One request of group `seq` is durable: the group is done when all are.
fn group_part_done(m: &mut MemberPart, durable: &mut Prefix, seq: Seq) {
    let left = match m.groups.get_mut(&seq.0) {
        Some(n) => {
            *n = n.saturating_sub(1);
            *n
        }
        None => 0,
    };
    if left == 0 {
        m.groups.remove(&seq.0);
        durable.done(seq);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_walk_moves_in_slot_order_and_tests_one_position() {
        let mut w = KeyWalk::new(vec![3, 19, 35]);
        assert!(!w.passed(3, 0));
        w.listed(500);
        assert!(w.passed(3, 499) && !w.passed(3, 500) && !w.passed(19, 0));
        w.listed(1 << 32);
        assert_eq!(w.idx, 1);
        assert!(w.passed(3, u32::MAX) && !w.passed(19, 0));
        w.skip_empty(&|s| if s == 19 { 0 } else { 5 });
        assert_eq!(w.idx, 2);
        assert!(
            w.passed(19, u32::MAX),
            "an empty slot is passed when skipped"
        );
        w.listed(1 << 32);
        assert!(w.done());
        assert!(w.passed(35, u32::MAX));
        assert_eq!(slots_of(3, 16).len(), 1024);
        assert!(slots_of(3, 16).iter().all(|s| s % 16 == 3));
    }

    #[test]
    fn group_facts() {
        let g = Group {
            seq: Seq(9),
            ops: vec![Op::Put {
                db: 0,
                vtype: 1,
                expire_at: 0,
                key: b"{a}1".as_slice().into(),
                value: vec![1],
            }],
        };
        assert_eq!(group_slot(&g), Some(kv_common::key_slot(b"a")));
        assert!(!is_wide(&g));
        assert_eq!(group_fps(&g, 7).len(), 1);
        let f = Group {
            seq: Seq(10),
            ops: vec![Op::FlushDb { db: None }],
        };
        assert!(is_wide(&f) && group_slot(&f).is_none());
        let ops = vec![
            Op::Put {
                db: 0,
                vtype: 1,
                expire_at: 0,
                key: b"k".as_slice().into(),
                value: vec![],
            },
            Op::Append {
                db: 0,
                key: b"k".as_slice().into(),
                data: vec![],
            },
        ];
        assert_eq!(op_keys(&ops, false).len(), 1, "one entry per key");
    }
}
