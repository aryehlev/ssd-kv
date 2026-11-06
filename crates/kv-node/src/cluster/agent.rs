//! The agent thread (DESIGN §6.1, §23.4, §23.9, §23.10, §23.12).
//!
//! A tokio current-thread runtime. It accepts the coordinator leader's control connection on
//! `cluster-control-port` (data nodes never dial anyone for topology), answers `Hello` with
//! identity and readiness only, applies `Assign` in `(coord_term, version)` order after
//! persisting role, shard, fence term and version in node meta, answers older assignments with
//! `Error(StaleAssignment)`, sends `Renew` every `renew_ms` and sets the lease cell from each
//! `LeaseGrant` (`t_send + lease_ms * 0.95 - 10 ms`), persists the fence term before
//! acknowledging a `Fence`, runs graceful handoffs, and serves `/metrics`. It talks to
//! partitions only through `XMsg::Ctrl`.
//!
//! The agent's own loop never waits for anything but its inputs: node-meta writes go to the
//! meta thread and their completion comes back as a message, so assignments and fences are a
//! small queue of persist-then-act steps processed in arrival order. Node-initiated reports
//! (ISR changes, readiness, migration progress) are gathered from the workers' per-partition
//! reports and sent when every partition they cover agrees; they are repeated at a fixed period
//! while the committed assignment does not reflect them yet (a report sent to a coordinator that
//! stopped leading is otherwise lost).

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use smallvec::SmallVec;

use kv_ctrl::{
    CoordToNode, Health, MemberState, MigPhase, MigrationTask, NodeAssignment, NodeError, NodeId,
    NodeToCoord, ShardId,
};
use kv_storage::metastore::{MetaDone, MetaReq};
use kv_storage::partition::PartState;
use kv_storage::pmeta::{NodeMeta, ROLE_NONE, ROLE_PRIMARY, ROLE_REPLICA, SHARD_NONE};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use super::ownership::{GateFlags, Ownership};
use super::{AGENT_TOKEN_BIT, AgentMsg, ClusterShared, same_token};
use crate::runtime::xmsg::XMsg;
use crate::shared::NodeShared;

/// Control-plane events delivered to workers.
pub enum CtrlEvent {
    /// A newly applied assignment (each worker rebuilds what it needs).
    Assign(Arc<NodeAssignment>),
    /// Fence to `new_term`: close streams of older terms, report applied positions.
    Fence {
        req: u64,
        shard: ShardId,
        new_term: u32,
    },
    /// Graceful handoff to `to`: refuse new writes with TRYAGAIN, drain, report last positions.
    Handoff {
        req: u64,
        shard: ShardId,
        to: NodeId,
    },
    MigrationPhase {
        id: u64,
        phase: MigPhase,
    },
}

/// Time a coordinator has to send its `Hello` after connecting.
const HELLO_TIMEOUT: Duration = Duration::from_secs(5);
/// A leading coordinator answers every renewal (every `renew_ms`): a control connection that
/// delivered nothing for this long is dead (a silent partition, or a coordinator that stopped
/// leading without closing it) and is closed; a leader dials again.
const SESSION_IDLE: Duration = Duration::from_secs(10);
/// Longest graceful handoff before writes are accepted again.
const HANDOFF_LIMIT: Duration = Duration::from_secs(6);
/// Longest node-meta write before its step is given up (unacknowledged).
const PERSIST_LIMIT: Duration = Duration::from_secs(10);
/// Period of repeated node-initiated reports not yet reflected by an assignment.
const REPORT_REPEAT: Duration = Duration::from_millis(1000);
/// A primary that relinquished its lease for a handoff and still holds its role after this
/// long (the coordinator never committed the promotion, e.g. it lost leadership meanwhile)
/// reports `health.failed`: the normal failover path then moves the shard on (fencing another
/// member and waiting for this node's lease by the coordinator's rule), so the shard never
/// stays without a serving primary. The node itself never takes a lease back for that role.
const HANDOFF_STUCK: Duration = Duration::from_secs(10);
/// Longest wait of a primary that handed off to itself (a term raise) for the assignment of the
/// new term; it serves writes again after it.
const TERM_RAISE_LIMIT: Duration = Duration::from_secs(2);
/// How long a partition's lag report of a member counts for the node-wide shrink choice (each
/// lagging partition reports every 100 ms).
const LAG_REPORT_WINDOW: Duration = Duration::from_millis(150);
/// How long a requested ISR shrink counts as done while the committed ISR does not show it (a
/// commit takes milliseconds; a request the coordinator dropped stops counting after this).
const SHRINK_PENDING: Duration = Duration::from_millis(1000);
/// Longest wait of a shutting-down primary for its handoff (the coordinator's handoff bound is
/// 5 s): after it the node shuts down anyway and its shard fails over by lease expiry.
const SHUTDOWN_HANDOFF_LIMIT: Duration = Duration::from_secs(8);

/// Runs the agent until shutdown.
pub fn run(shared: Arc<NodeShared>) -> std::io::Result<()> {
    let Some(cs) = shared.cluster() else {
        return Err(std::io::Error::other("cluster state missing"));
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async move {
        let cfg = shared.config();
        let addr = SocketAddr::new(super::bind_ip(&cfg), super::control_port(&cfg));
        let l = TcpListener::bind(addr).await?;
        tracing::info!(%addr, "cluster control port listening");
        let rx = cs
            .take_agent_rx()
            .ok_or_else(|| std::io::Error::other("the agent runs once"))?;
        Agent::new(shared.clone(), cs).serve(l, rx).await
    })
}

/// One coordinator connection.
struct Session {
    id: u64,
    cluster_id: [u8; 16],
    tx: mpsc::UnboundedSender<NodeToCoord>,
}

/// What a session task reports.
enum SessionMsg {
    Open {
        id: u64,
        cluster_id: [u8; 16],
        tx: mpsc::UnboundedSender<NodeToCoord>,
    },
    Frame {
        id: u64,
        msg: CoordToNode,
    },
    Closed {
        id: u64,
    },
}

/// A step that needs node meta durable first.
enum Persist {
    Assign {
        session: u64,
        a: Box<NodeAssignment>,
    },
    Fence {
        session: u64,
        req: u64,
        shard: ShardId,
        new_term: u32,
    },
}

/// Per-partition reports gathered for one node-initiated message.
struct Gather {
    bits: Vec<bool>,
    last_sent: Option<std::time::Instant>,
}

impl Gather {
    fn new(n: usize) -> Self {
        Gather {
            bits: vec![false; n],
            last_sent: None,
        }
    }

    fn set(&mut self, i: usize, on: bool) {
        if let Some(b) = self.bits.get_mut(i) {
            if *b && !on {
                self.last_sent = None;
            }
            *b = on;
        }
    }

    fn complete_over(&self, want: &[u16]) -> bool {
        !want.is_empty()
            && want
                .iter()
                .all(|&p| self.bits.get(usize::from(p)).copied().unwrap_or(false))
    }

    /// Whether a (repeated) report is due now.
    fn due(&mut self) -> bool {
        if self.last_sent.is_some_and(|t| t.elapsed() < REPORT_REPEAT) {
            return false;
        }
        self.last_sent = Some(std::time::Instant::now());
        true
    }
}

/// A fence or handoff answer gathered from every worker.
struct WorkerGather {
    session: u64,
    workers_left: usize,
    values: Vec<u64>,
    started: std::time::Instant,
    /// A handoff to this node itself (a term raise, §23.12): it stays the primary, so its
    /// lease is kept.
    keep_lease: bool,
}

struct Agent {
    shared: Arc<NodeShared>,
    cs: Arc<ClusterShared>,
    sessions: Vec<Session>,
    next_session: u64,
    /// `(coord_term, version)` of the applied assignment.
    current: Option<(u64, u64)>,
    persist_q: VecDeque<Persist>,
    /// `(token, step, started)` of the node-meta write in flight.
    persist_inflight: Option<(u64, Persist, std::time::Instant)>,
    renew_seq: u64,
    sent_renewals: VecDeque<(u64, u64)>,
    /// After a completed handoff the lease stays relinquished until the demoting assignment.
    lease_held_back: Option<std::time::Instant>,
    /// A term raise reached its barrier: writes wait for the assignment of the new term.
    term_raise_since: Option<std::time::Instant>,
    /// When each `TermRaise` `(shard, term)` was last sent.
    raise_sent: Option<((u16, u32), std::time::Instant)>,
    fences: hashbrown::HashMap<u64, WorkerGather, foldhash::fast::RandomState>,
    handoffs: hashbrown::HashMap<u64, WorkerGather, foldhash::fast::RandomState>,
    /// When each member's `IsrExpand` was last sent (repeated every `REPORT_REPEAT` while the
    /// committed ISR does not show it; readiness itself is in `ClusterShared::expand_gate`).
    expand_sent: hashbrown::HashMap<NodeId, std::time::Instant, foldhash::fast::RandomState>,
    shrink_sent: hashbrown::HashMap<NodeId, std::time::Instant, foldhash::fast::RandomState>,
    /// Lagging ISR members as the partitions reported them lately: `(oldest age, when)`.
    lagging: hashbrown::HashMap<NodeId, (u64, std::time::Instant), foldhash::fast::RandomState>,
    /// When the last `HandoffRequest` of a shutting-down primary was sent.
    handoff_requested: Option<std::time::Instant>,
    resync_sent: hashbrown::HashMap<NodeId, std::time::Instant, foldhash::fast::RandomState>,
    ready: (u32, Gather),
    mig_ready: hashbrown::HashMap<u64, Gather, foldhash::fast::RandomState>,
    sealed: hashbrown::HashMap<u64, Gather, foldhash::fast::RandomState>,
}

impl Agent {
    fn new(shared: Arc<NodeShared>, cs: Arc<ClusterShared>) -> Self {
        let p = usize::from(shared.partitions);
        Agent {
            shared,
            cs,
            sessions: Vec::new(),
            next_session: 1,
            current: None,
            persist_q: VecDeque::new(),
            persist_inflight: None,
            renew_seq: 0,
            sent_renewals: VecDeque::new(),
            lease_held_back: None,
            term_raise_since: None,
            raise_sent: None,
            fences: Default::default(),
            handoffs: Default::default(),
            expand_sent: Default::default(),
            shrink_sent: Default::default(),
            lagging: Default::default(),
            handoff_requested: None,
            resync_sent: Default::default(),
            ready: (0, Gather::new(p)),
            mig_ready: Default::default(),
            sealed: Default::default(),
        }
    }

    fn partitions(&self) -> usize {
        usize::from(self.shared.partitions)
    }

    async fn serve(
        mut self,
        l: TcpListener,
        mut rx: mpsc::UnboundedReceiver<AgentMsg>,
    ) -> std::io::Result<()> {
        let (stx, mut srx) = mpsc::unbounded_channel::<SessionMsg>();
        let mut tick = tokio::time::interval(Duration::from_millis(self.renew_ms()));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut renew_ms = self.renew_ms();
        loop {
            if self.shared.shutdown_requested() {
                return Ok(());
            }
            tokio::select! {
                acc = l.accept() => {
                    if let Ok((sock, peer)) = acc {
                        let _ = sock.set_nodelay(true);
                        let id = self.next_session;
                        self.next_session += 1;
                        tokio::spawn(session(sock, peer, id, self.shared.clone(), self.cs.clone(), stx.clone()));
                    }
                }
                m = srx.recv() => if let Some(m) = m { self.on_session(m) },
                m = rx.recv() => match m {
                    Some(m) => self.on_worker(m),
                    None => return Ok(()),
                },
                _ = tick.tick() => {
                    self.on_tick();
                    let want = self.renew_ms();
                    if want != renew_ms {
                        renew_ms = want;
                        tick = tokio::time::interval(Duration::from_millis(want));
                        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    }
                }
            }
        }
    }

    fn renew_ms(&self) -> u64 {
        self.cs
            .assignment()
            .map_or(100, |a| u64::from(a.params.renew_ms.max(10)))
    }

    fn send(&self, session: u64, m: NodeToCoord) {
        if let Some(s) = self.sessions.iter().find(|s| s.id == session) {
            let _ = s.tx.send(m);
        }
    }

    fn send_all(&self, m: &NodeToCoord) {
        for s in &self.sessions {
            let _ = s.tx.send(m.clone());
        }
    }

    fn broadcast(&self, ev: impl Fn() -> super::agent::CtrlEvent) {
        for w in &self.shared.workers {
            w.send_side(XMsg::Ctrl(ev()));
        }
    }

    // -------------------------------------------------------------------------------------
    // coordinator messages

    fn on_session(&mut self, m: SessionMsg) {
        match m {
            SessionMsg::Open { id, cluster_id, tx } => {
                self.sessions.push(Session { id, cluster_id, tx });
                // reports go to the new leader too
                self.ready.1.last_sent = None;
            }
            SessionMsg::Closed { id } => self.sessions.retain(|s| s.id != id),
            SessionMsg::Frame { id, msg } => match msg {
                CoordToNode::Hello { .. } => {
                    self.send(id, NodeToCoord::Error(NodeError::Malformed))
                }
                CoordToNode::Assign(a) => {
                    self.persist_q.push_back(Persist::Assign {
                        session: id,
                        a: Box::new(a),
                    });
                    self.next_persist();
                }
                CoordToNode::LeaseGrant { seq, lease_ms } => self.on_grant(seq, lease_ms),
                CoordToNode::Fence {
                    req,
                    shard,
                    new_term,
                } => {
                    self.persist_q.push_back(Persist::Fence {
                        session: id,
                        req,
                        shard,
                        new_term,
                    });
                    self.next_persist();
                }
                CoordToNode::Handoff { req, shard, to } => self.on_handoff(id, req, shard, to),
                CoordToNode::MigrationPhase { id: mid, phase } => {
                    self.broadcast(|| CtrlEvent::MigrationPhase { id: mid, phase })
                }
                CoordToNode::Ping => {}
            },
        }
    }

    fn on_grant(&mut self, seq: u64, lease_ms: u32) {
        let Some(&(_, t_send)) = self.sent_renewals.iter().find(|(s, _)| *s == seq) else {
            return;
        };
        if self.lease_held_back.is_some() {
            return;
        }
        // node rule: t_send + lease_ms * 0.95 - 10 ms (CLOCK_BOOTTIME)
        let span = (u64::from(lease_ms) * 950_000).saturating_sub(10_000_000);
        let expiry = t_send.saturating_add(span);
        let cur = self.shared.lease.expiry();
        if expiry > cur {
            self.shared.lease.set(expiry);
        }
    }

    /// Starts the next persist step unless one is running.
    fn next_persist(&mut self) {
        while self.persist_inflight.is_none() {
            let Some(step) = self.persist_q.pop_front() else {
                return;
            };
            let id = self.cs.identity();
            let meta = match &step {
                Persist::Assign { session, a } => {
                    let key = a.order_key();
                    if self.current.is_some_and(|c| key < c)
                        || (self.current.is_none() && a.version < id.version)
                    {
                        self.send(*session, NodeToCoord::Error(NodeError::StaleAssignment));
                        continue;
                    }
                    // the same state version from the same leader term is acknowledged as it
                    // is, unless its coordinator list differs: an operator peer-list change
                    // moves no state version, and the new list must reach the MOVED targets
                    // (a removed coordinator there would redirect clients to a closed port)
                    if self.current == Some(key)
                        && self
                            .cs
                            .assignment()
                            .is_none_or(|c| c.coordinators == a.coordinators)
                    {
                        self.send(*session, NodeToCoord::AssignAck { version: a.version });
                        continue;
                    }
                    let cluster_id = self
                        .sessions
                        .iter()
                        .find(|s| s.id == *session)
                        .map(|s| s.cluster_id);
                    let Some(cluster_id) = cluster_id else {
                        continue;
                    };
                    let (shard, role) = match &a.role {
                        None => (SHARD_NONE, ROLE_NONE),
                        Some(r) => (
                            r.shard.0,
                            if r.is_primary {
                                ROLE_PRIMARY
                            } else {
                                ROLE_REPLICA
                            },
                        ),
                    };
                    let (_, fence) =
                        super::fence_after(id.fence_shard, id.fence_term, a.role.as_ref());
                    NodeMeta {
                        cluster_id,
                        node_id: a.node_id.0,
                        shard,
                        role,
                        fence_term: fence,
                        assignment_version: a.version,
                    }
                }
                Persist::Fence { shard, .. } if shard.0 != id.fence_shard => {
                    // a fence of a shard this node's persisted assignment is not in (a leader
                    // whose committed state this node has moved past): not acknowledged, so
                    // that leader takes its next candidate after the fence timeout; a term of
                    // another shard never becomes this node's fence
                    tracing::warn!(
                        shard = shard.0,
                        "fence for a shard this node is not assigned to: ignored"
                    );
                    continue;
                }
                Persist::Fence { new_term, .. } => {
                    let mut m = self.node_meta_now();
                    m.fence_term = m.fence_term.max(*new_term);
                    m
                }
            };
            let token = self.cs.token(AGENT_TOKEN_BIT);
            if !self
                .cs
                .meta_request(0, token, MetaReq::NodeMeta { m: meta })
            {
                // shutting down: nothing is persisted or acknowledged any more
                return;
            }
            self.persist_inflight = Some((token, step, std::time::Instant::now()));
        }
    }

    /// The node meta as currently applied.
    fn node_meta_now(&self) -> NodeMeta {
        let id = self.cs.identity();
        let (shard, role) = match self.cs.assignment().as_ref().and_then(|a| a.role.clone()) {
            None => (SHARD_NONE, ROLE_NONE),
            Some(r) => (
                r.shard.0,
                if r.is_primary {
                    ROLE_PRIMARY
                } else {
                    ROLE_REPLICA
                },
            ),
        };
        NodeMeta {
            cluster_id: id.cluster_id.unwrap_or([0; 16]),
            node_id: id.node_id.map_or([0; 20], |n| n.0),
            shard,
            role,
            fence_term: id.fence_term,
            assignment_version: id.version,
        }
    }

    fn on_persisted(&mut self, token: u64, done: MetaDone) {
        let Some((t, step, started)) = self.persist_inflight.take() else {
            return;
        };
        if t != token {
            self.persist_inflight = Some((t, step, started));
            return;
        }
        if let MetaDone::Failed(errno) = done {
            tracing::error!(
                errno,
                "node meta write failed: the step is not acknowledged"
            );
            self.next_persist();
            return;
        }
        match step {
            Persist::Assign { session, a } => self.apply_assignment(session, a),
            Persist::Fence {
                session,
                req,
                shard,
                new_term,
            } => {
                {
                    let mut id = self.cs.identity.write();
                    id.fence_term = id.fence_term.max(new_term);
                }
                tracing::info!(shard = shard.0, new_term, "fenced");
                self.fences.insert(
                    req,
                    WorkerGather {
                        session,
                        workers_left: self.shared.workers.len(),
                        values: vec![0; self.partitions()],
                        started: std::time::Instant::now(),
                        keep_lease: false,
                    },
                );
                self.broadcast(|| CtrlEvent::Fence {
                    req,
                    shard,
                    new_term,
                });
            }
        }
        self.next_persist();
    }

    fn apply_assignment(&mut self, session: u64, a: Box<NodeAssignment>) {
        let cluster_id = self
            .sessions
            .iter()
            .find(|s| s.id == session)
            .map(|s| s.cluster_id);
        let old = self.cs.assignment();
        {
            let mut id = self.cs.identity.write();
            id.node_id = Some(a.node_id);
            if let Some(c) = cluster_id {
                id.cluster_id = Some(c);
            }
            id.version = a.version;
            (id.fence_shard, id.fence_term) =
                super::fence_after(id.fence_shard, id.fence_term, a.role.as_ref());
        }
        let role_key =
            |x: &NodeAssignment| x.role.as_ref().map(|r| (r.shard, r.term, r.is_primary));
        if old.as_ref().is_none_or(|o| role_key(o) != role_key(&a)) {
            // a handoff ends with the assignment that demotes this node (or any role change)
            self.cs.handoff.store(false, Ordering::Release);
            self.lease_held_back = None;
            self.term_raise_since = None;
            self.handoffs.clear();
        }
        // after an unclean restart a primary role of the term served before it is not served
        // again: the node applies it as a member without a primary (it serves nothing for the
        // shard) and reports `health.failed` until the coordinator moves the shard to a newer
        // term (the normal failover path, which may promote this node again at that term)
        let refuse = self.cs.refuses_primary(&a);
        if refuse != self.cs.refusing_primary.swap(refuse, Ordering::AcqRel) || refuse {
            if refuse {
                tracing::warn!(
                    version = a.version,
                    "restarted after a crash as a shard primary: that term is not served again; reporting failed until a newer term is assigned"
                );
            } else {
                tracing::info!("a newer assignment arrived: the restart hold ends");
            }
        }
        let mut a = a;
        if refuse && let Some(r) = a.role.as_mut() {
            r.is_primary = false;
            r.primary_repl = None;
            r.members.clear();
            r.migrations.clear();
        }
        let a: Arc<NodeAssignment> = Arc::from(a);
        // before any worker sees it: expand rounds end only by what the committed state shows
        self.cs.expand_gate.on_assignment(a.role.as_ref());
        self.current = Some(a.order_key());
        *self.cs.assignment.write() = Some(a.clone());
        let flags = GateFlags {
            syncing: self.cs.syncing.clone(),
            handoff: self.cs.handoff.clone(),
        };
        *self.shared.ownership.write() = Arc::new(Ownership::from_assignment_with(&a, Some(flags)));
        if let Some(pw) = a.client_password.as_ref()
            && self.shared.config().requirepass != *pw
        {
            let pw = pw.clone();
            if let Err(e) = self.shared.update_config(|c| c.set("requirepass", &pw)) {
                tracing::warn!("cannot apply the cluster's client password: {e}");
            }
        }
        self.broadcast(|| CtrlEvent::Assign(a.clone()));
        self.send(session, NodeToCoord::AssignAck { version: a.version });
        tracing::debug!(version = a.version, "assignment applied");
    }

    fn on_handoff(&mut self, session: u64, req: u64, shard: ShardId, to: NodeId) {
        let to_self = self.cs.identity().node_id == Some(to);
        self.cs.handoff.store(true, Ordering::Release);
        self.handoffs.insert(
            req,
            WorkerGather {
                session,
                workers_left: self.shared.workers.len(),
                values: vec![0; self.partitions()],
                started: std::time::Instant::now(),
                keep_lease: to_self,
            },
        );
        if to_self {
            tracing::info!(shard = shard.0, "term raise: handoff to this primary itself");
        }
        self.broadcast(|| CtrlEvent::Handoff { req, shard, to });
    }

    // -------------------------------------------------------------------------------------
    // worker reports

    fn on_worker(&mut self, m: AgentMsg) {
        match m {
            AgentMsg::Meta { token, done } => self.on_persisted(token, done),
            AgentMsg::FenceApplied { req, applied } => {
                let done = match self.fences.get_mut(&req) {
                    Some(g) => {
                        for (p, s) in applied {
                            if let Some(v) = g.values.get_mut(usize::from(p)) {
                                *v = s;
                            }
                        }
                        g.workers_left = g.workers_left.saturating_sub(1);
                        g.workers_left == 0
                    }
                    None => false,
                };
                if done && let Some(g) = self.fences.remove(&req) {
                    self.send(
                        g.session,
                        NodeToCoord::FenceAck {
                            req,
                            applied: g.values,
                        },
                    );
                }
            }
            AgentMsg::HandoffReached { req, last } => {
                let done = match self.handoffs.get_mut(&req) {
                    Some(g) => {
                        for (p, s) in last {
                            if let Some(v) = g.values.get_mut(usize::from(p)) {
                                *v = s;
                            }
                        }
                        g.workers_left = g.workers_left.saturating_sub(1);
                        g.workers_left == 0
                    }
                    None => false,
                };
                if done && let Some(g) = self.handoffs.remove(&req) {
                    if g.keep_lease {
                        // a term raise: this node stays the primary; writes wait (TRYAGAIN)
                        // for the assignment of the new term, at most `TERM_RAISE_LIMIT`
                        self.term_raise_since = Some(std::time::Instant::now());
                    } else {
                        // relinquish the lease before answering (§23.12)
                        self.lease_held_back = Some(std::time::Instant::now());
                        self.shared.lease.set(0);
                    }
                    self.send(
                        g.session,
                        NodeToCoord::HandoffDone {
                            req,
                            last: g.values,
                        },
                    );
                }
            }
            AgentMsg::Expand {
                shard,
                term,
                member,
                partition,
                counted,
            } => {
                // readiness is in the expand gate; this only says it changed
                let _ = (term, partition, counted);
                self.report_expand(shard, member);
            }
            AgentMsg::Shrink {
                shard,
                term,
                member,
                age_ns,
                keep_age_ns,
            } => {
                let a = self.cs.assignment();
                let Some(role) = a.as_ref().and_then(|a| a.role.as_ref()) else {
                    return;
                };
                if !role.is_primary || role.shard.0 != shard || role.term != term {
                    return;
                }
                // node-wide: the oldest age any partition reported lately for each member, so
                // that every partition's report leads to the same choice
                let now = std::time::Instant::now();
                self.lagging
                    .retain(|_, (_, at)| now.duration_since(*at) < LAG_REPORT_WINDOW);
                let e = self.lagging.entry(member).or_insert((0, now));
                if age_ns >= e.0 {
                    *e = (age_ns, now);
                }
                // a shrink requested lately that the committed ISR does not show yet counts as
                // done: it takes the spare budget first, so that a stall of every member never
                // gets two of them dropped (one per report, before the first commit arrives)
                // when only one may go
                let lagging: Vec<Lagging> = role
                    .members
                    .iter()
                    .enumerate()
                    .filter(|(_, (_, _, in_isr))| *in_isr)
                    .map(|(i, (n, _, _))| Lagging {
                        member: i as u16,
                        age_ns: self.lagging.get(n).map(|(age, _)| *age),
                        pending: self
                            .shrink_sent
                            .get(n)
                            .is_some_and(|t| now.duration_since(*t) < SHRINK_PENDING),
                    })
                    .collect();
                let chosen = shrink_with_pending(&lagging, usize::from(role.min_isr.max(1)), keep_age_ns);
                for i in chosen {
                    let Some((member, _, _)) = role.members.get(usize::from(i)) else {
                        continue;
                    };
                    let member = *member;
                    let due = self
                        .shrink_sent
                        .get(&member)
                        .is_none_or(|t| t.elapsed() >= Duration::from_millis(200));
                    if due {
                        self.shrink_sent.insert(member, now);
                        tracing::info!(member = %member.hex40(), "replica is not acknowledging: ISR shrink requested");
                        self.send_all(&NodeToCoord::IsrShrink {
                            shard: ShardId(shard),
                            term,
                            member,
                        });
                    }
                }
            }
            AgentMsg::NeedsResync {
                shard,
                term,
                member,
            } => {
                let a = self.cs.assignment();
                let Some(role) = a.as_ref().and_then(|a| a.role.as_ref()) else {
                    return;
                };
                let recorded = role.members.iter().any(|(n, st, _)| {
                    *n == member && matches!(st, MemberState::NeedsFullSync | MemberState::Syncing)
                });
                if !role.is_primary || role.shard.0 != shard || role.term != term || recorded {
                    return;
                }
                // repeated (the member redials) until the committed assignment shows it
                let due = self
                    .resync_sent
                    .get(&member)
                    .is_none_or(|t| t.elapsed() >= REPORT_REPEAT);
                if due {
                    self.resync_sent.insert(member, std::time::Instant::now());
                    tracing::warn!(member = %member.hex40(), "replica cannot catch up by continue or repair: needs an operator resync");
                    self.send_all(&NodeToCoord::MemberNeedsResync {
                        shard: ShardId(shard),
                        term,
                        member,
                    });
                }
            }
            AgentMsg::TermRaise { shard, term, above } => {
                let a = self.cs.assignment();
                let Some(role) = a.as_ref().and_then(|a| a.role.as_ref()) else {
                    return;
                };
                if !role.is_primary || role.shard.0 != shard || role.term != term {
                    return;
                }
                // repeated (the member redials) until the assignment of a newer term arrives
                let due = self
                    .raise_sent
                    .is_none_or(|(k, t)| k != (shard, term) || t.elapsed() >= REPORT_REPEAT);
                if due {
                    self.raise_sent = Some(((shard, term), std::time::Instant::now()));
                    tracing::info!(shard, term, above, "term raise requested");
                    self.send_all(&NodeToCoord::TermRaise {
                        shard: ShardId(shard),
                        term,
                        above,
                    });
                }
            }
            AgentMsg::PartReady {
                shard,
                term,
                partition,
                ready,
            } => {
                if self.ready.0 != term {
                    self.ready = (term, Gather::new(self.partitions()));
                }
                self.ready.1.set(usize::from(partition), ready);
                self.report_ready(shard, term);
            }
            AgentMsg::MigReady { id, partition } => {
                let n = self.partitions();
                self.mig_ready
                    .entry(id)
                    .or_insert_with(|| Gather::new(n))
                    .set(usize::from(partition), true);
                self.report_migrations();
            }
            AgentMsg::Sealed { id, partition } => {
                let n = self.partitions();
                self.sealed
                    .entry(id)
                    .or_insert_with(|| Gather::new(n))
                    .set(usize::from(partition), true);
                self.report_migrations();
            }
        }
    }

    fn all_partitions(&self) -> Vec<u16> {
        (0..self.shared.partitions).collect()
    }

    fn report_expand(&mut self, shard: u16, member: NodeId) {
        let a = self.cs.assignment();
        let Some(role) = a.as_ref().and_then(|a| a.role.as_ref()) else {
            return;
        };
        if !role.is_primary || role.shard.0 != shard {
            return;
        }
        let in_isr = role.members.iter().any(|(n, _, isr)| *n == member && *isr);
        let term = role.term;
        // every partition counts it and it holds every committed write: from this request on
        // no partition stops counting it (see `ExpandGate`)
        if in_isr || !self.cs.expand_gate.request(&member, term) {
            return;
        }
        let due = self
            .expand_sent
            .get(&member)
            .is_none_or(|t| t.elapsed() >= REPORT_REPEAT);
        if !due {
            return;
        }
        self.expand_sent.insert(member, std::time::Instant::now());
        tracing::info!(member = %member.hex40(), "replica caught up: ISR expand requested");
        self.send_all(&NodeToCoord::IsrExpand {
            shard: ShardId(shard),
            term,
            member,
        });
    }

    fn report_ready(&mut self, shard: u16, term: u32) {
        let all = self.all_partitions();
        let Some(me) = self.cs.identity().node_id else {
            return;
        };
        let a = self.cs.assignment();
        let Some(role) = a.as_ref().and_then(|a| a.role.as_ref()) else {
            return;
        };
        if role.is_primary || role.term != term || role.shard.0 != shard {
            return;
        }
        if self.ready.0 == term && self.ready.1.complete_over(&all) && self.ready.1.due() {
            self.send_all(&NodeToCoord::MemberReady {
                shard: ShardId(shard),
                term,
                member: me,
            });
        }
    }

    fn report_migrations(&mut self) {
        let a = self.cs.assignment();
        let Some(role) = a.as_ref().and_then(|a| a.role.as_ref()) else {
            return;
        };
        if !role.is_primary {
            return;
        }
        let parts = |t: &MigrationTask| -> Vec<u16> {
            let mut v: Vec<u16> = t
                .slots()
                .iter_slots()
                .map(|s| s % self.shared.partitions.max(1))
                .collect();
            v.sort_unstable();
            v.dedup();
            v
        };
        let mut out = Vec::new();
        for t in &role.migrations {
            match t {
                MigrationTask::Out {
                    id,
                    phase: MigPhase::Copy | MigPhase::Ready,
                    ..
                } => {
                    if let Some(g) = self.mig_ready.get_mut(id)
                        && g.complete_over(&parts(t))
                        && g.due()
                    {
                        out.push(NodeToCoord::MigrationReady { id: *id });
                    }
                }
                MigrationTask::In {
                    id,
                    phase: MigPhase::Cutover,
                    ..
                } => {
                    if let Some(g) = self.sealed.get_mut(id)
                        && g.complete_over(&parts(t))
                        && g.due()
                    {
                        out.push(NodeToCoord::SealDone { id: *id });
                    }
                }
                _ => {}
            }
        }
        let live: Vec<u64> = role.migrations.iter().map(MigrationTask::id).collect();
        self.mig_ready.retain(|id, _| live.contains(id));
        self.sealed.retain(|id, _| live.contains(id));
        for m in out {
            tracing::info!("migration report: {}", m.kind());
            self.send_all(&m);
        }
    }

    // -------------------------------------------------------------------------------------
    // periodic work

    /// SIGTERM on a primary: ask for a handoff (repeated every second) and shut down once the
    /// node is no longer the primary, when no coordinator is connected, or after the bound.
    fn shutdown_handoff_tick(&mut self) {
        let Some(since) = self.cs.shutdown_handoff_since() else {
            return;
        };
        let a = self.cs.assignment();
        let primary = a
            .as_ref()
            .and_then(|a| a.role.as_ref())
            .filter(|r| r.is_primary)
            .map(|r| (r.shard, r.term));
        let Some((shard, term)) = primary else {
            tracing::info!("handed off: shutting down");
            self.shared.request_shutdown();
            return;
        };
        if self.sessions.is_empty() || since.elapsed() >= SHUTDOWN_HANDOFF_LIMIT {
            tracing::warn!("the shard was not handed off in time: shutting down (it fails over)");
            self.shared.request_shutdown();
            return;
        }
        if self
            .handoff_requested
            .is_none_or(|t| t.elapsed() >= REPORT_REPEAT)
        {
            self.handoff_requested = Some(std::time::Instant::now());
            self.send_all(&NodeToCoord::HandoffRequest { shard, term });
        }
    }

    fn on_tick(&mut self) {
        self.shutdown_handoff_tick();
        // renewals: one per period on every coordinator connection
        if !self.sessions.is_empty() {
            self.renew_seq += 1;
            let now = kv_common::clock::boot_ns();
            self.sent_renewals.push_back((self.renew_seq, now));
            while self.sent_renewals.len() > 64 {
                self.sent_renewals.pop_front();
            }
            let health = self.health();
            let m = NodeToCoord::Renew {
                seq: self.renew_seq,
                incarnation: self.cs.incarnation,
                version_seen: self.cs.identity().version,
                health,
            };
            self.send_all(&m);
        }
        // a handoff that did not complete in time gives writes back
        let late: Vec<u64> = self
            .handoffs
            .iter()
            .filter(|(_, g)| g.started.elapsed() > HANDOFF_LIMIT)
            .map(|(r, _)| *r)
            .collect();
        for r in late {
            tracing::warn!("graceful handoff did not complete in time: writes are accepted again");
            self.handoffs.remove(&r);
            self.cs.handoff.store(false, Ordering::Release);
        }
        // a term raise whose assignment never came gives writes back: this node is still the
        // primary of the term it serves (if the raise is committed later, the members repair
        // from the takeover point it reported, which this node's state covers)
        if self
            .term_raise_since
            .is_some_and(|t| t.elapsed() >= TERM_RAISE_LIMIT)
        {
            tracing::warn!("term raise not committed in time: writes are accepted again");
            self.term_raise_since = None;
            self.cs.handoff.store(false, Ordering::Release);
        }
        self.fences
            .retain(|_, g| g.started.elapsed() < Duration::from_secs(10));
        // a node-meta write that never completes does not hold every later step forever: the
        // step is dropped unacknowledged (the coordinator pushes again) and the queue moves on
        if self
            .persist_inflight
            .as_ref()
            .is_some_and(|(_, _, t)| t.elapsed() > PERSIST_LIMIT)
        {
            tracing::error!("node meta write did not complete: the step is not acknowledged");
            self.persist_inflight = None;
            self.next_persist();
        }
        // repeated reports
        let a = self.cs.assignment();
        if let Some(role) = a.as_ref().and_then(|a| a.role.as_ref()) {
            let shard = role.shard.0;
            let term = role.term;
            let members: Vec<NodeId> = self.cs.expand_gate.members();
            self.expand_sent.retain(|n, _| members.contains(n));
            for m in members {
                self.report_expand(shard, m);
            }
            self.report_ready(shard, term);
            self.report_migrations();
        }
    }

    fn health(&self) -> Health {
        let mut read_only = 0u16;
        let mut ready = true;
        for p in 0..self.shared.partitions {
            match self.shared.partition_state(p) {
                PartState::Loading => ready = false,
                PartState::ReadOnly => read_only += 1,
                PartState::Ready => {}
            }
        }
        let a = self.cs.assignment();
        Health {
            ready,
            // a restarted primary asks for a newer term (see `ClusterShared::restart_term`); a
            // handoff whose promotion never came asks for a failover (`HANDOFF_STUCK`); a
            // device that failed a write asks for one too (`device_failed_reports`)
            failed: self.cs.refusing_primary.load(Ordering::Acquire)
                || self
                    .lease_held_back
                    .is_some_and(|t| t.elapsed() >= HANDOFF_STUCK)
                || device_failed_reports(read_only, a.as_ref().and_then(|a| a.role.as_ref())),
            read_only_partitions: read_only,
            corrupt_pages: 0,
        }
    }
}

/// A node with a partition its device failed a write of (the partition is read-only, §22)
/// reports `health.failed`, so that the coordinator flags it and promotes a healthy member of
/// its shard: as a primary it can make no write of that partition durable any more (its
/// local durable position stops before the failed group, so nothing after it commits), and as
/// a member it can apply none. A primary of a shard with no other ISR member keeps serving
/// what it can (reads; writes of its other partitions): flagging it would only turn the shard
/// down.
pub(super) fn device_failed_reports(read_only: u16, role: Option<&kv_ctrl::ShardRole>) -> bool {
    read_only > 0
        && role.is_some_and(|r| !r.is_primary || r.members.iter().any(|(_, _, in_isr)| *in_isr))
}

/// One coordinator connection: handshake, then frames both ways until it closes.
async fn session(
    mut sock: TcpStream,
    peer: SocketAddr,
    id: u64,
    shared: Arc<NodeShared>,
    cs: Arc<ClusterShared>,
    to_agent: mpsc::UnboundedSender<SessionMsg>,
) {
    let first = tokio::time::timeout(HELLO_TIMEOUT, read_frame(&mut sock)).await;
    let Ok(Ok(body)) = first else { return };
    let Ok(CoordToNode::Hello {
        cluster_id,
        coord: _,
        coord_term: _,
        auth,
    }) = kv_ctrl::decode_body::<CoordToNode>(&body)
    else {
        let _ = write_msg(&mut sock, &NodeToCoord::Error(NodeError::Malformed)).await;
        return;
    };
    let ident = cs.identity();
    if !same_token(&auth, &cs.auth) || ident.cluster_id.is_some_and(|c| c != cluster_id) {
        tracing::warn!(%peer, "control connection refused: bad token or another cluster");
        let _ = write_msg(&mut sock, &NodeToCoord::Error(NodeError::BadAuth)).await;
        return;
    }
    let ready = !shared.loading();
    let ack = NodeToCoord::HelloAck {
        node: ident.node_id,
        incarnation: cs.incarnation,
        set_uuid: cs.boot.set_uuid,
        partitions: shared.partitions,
        ready,
    };
    if write_msg(&mut sock, &ack).await.is_err() {
        return;
    }
    tracing::info!(%peer, "coordinator control connection up");
    let (tx, mut rx) = mpsc::unbounded_channel::<NodeToCoord>();
    let _ = to_agent.send(SessionMsg::Open { id, cluster_id, tx });
    let (mut rd, mut wr) = sock.into_split();
    let writer = tokio::spawn(async move {
        while let Some(m) = rx.recv().await {
            let mut f = Vec::new();
            kv_ctrl::encode(&m, &mut f);
            if wr.write_all(&f).await.is_err() {
                return;
            }
        }
    });
    loop {
        let body = match tokio::time::timeout(SESSION_IDLE, read_frame_half(&mut rd)).await {
            Ok(Ok(b)) => b,
            Ok(Err(_)) => break,
            Err(_) => {
                tracing::info!(%peer, "no frame from the coordinator for {} s: closing the control connection", SESSION_IDLE.as_secs());
                break;
            }
        };
        match kv_ctrl::decode_body::<CoordToNode>(&body) {
            Ok(msg) => {
                if to_agent.send(SessionMsg::Frame { id, msg }).is_err() {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    writer.abort();
    let _ = to_agent.send(SessionMsg::Closed { id });
    tracing::info!(%peer, "coordinator control connection closed");
}

async fn read_frame(s: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut h = [0u8; 4];
    s.read_exact(&mut h).await?;
    let len = kv_ctrl::frame_len(h)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    let mut b = vec![0u8; len];
    s.read_exact(&mut b).await?;
    Ok(b)
}

async fn read_frame_half(s: &mut tokio::net::tcp::OwnedReadHalf) -> std::io::Result<Vec<u8>> {
    let mut h = [0u8; 4];
    s.read_exact(&mut h).await?;
    let len = kv_ctrl::frame_len(h)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    let mut b = vec![0u8; len];
    s.read_exact(&mut b).await?;
    Ok(b)
}

async fn write_msg(s: &mut TcpStream, m: &NodeToCoord) -> std::io::Result<()> {
    let mut f = Vec::new();
    kv_ctrl::encode(m, &mut f);
    s.write_all(&f).await
}


/// An ISR member of a primary's shard as the shrink choice sees it: its lag age if it lags
/// (`None`: it acknowledges), and whether a shrink of it was requested lately and is not
/// committed yet.
#[derive(Clone, Copy, Debug)]
struct Lagging {
    member: u16,
    age_ns: Option<u64>,
    pending: bool,
}

/// The members to ask the coordinator to drop (see `kv_repl::primary::shrink_choice`), with the
/// shrinks requested and not committed yet counted as done: they take the spare budget (the
/// committed ISR with the primary above `min_isr`) first, a pending one that still lags is
/// asked again, and the others are chosen oldest first within what is left or past the keep
/// bound.
fn shrink_with_pending(isr: &[Lagging], min_isr: usize, keep_age_ns: u64) -> SmallVec<[u16; 2]> {
    let spare = (1 + isr.len()).saturating_sub(min_isr.max(1));
    let mut out = SmallVec::new();
    let mut left = spare.saturating_sub(isr.iter().filter(|l| l.pending && l.age_ns.is_none()).count());
    for l in isr.iter().filter(|l| l.pending && l.age_ns.is_some()) {
        let age = l.age_ns.unwrap_or(0);
        if left > 0 || age > keep_age_ns {
            out.push(l.member);
        }
        left = left.saturating_sub(1);
    }
    let ages: Vec<(u16, u64)> = isr
        .iter()
        .filter(|l| !l.pending)
        .filter_map(|l| l.age_ns.map(|a| (l.member, a)))
        .collect();
    // `shrink_choice` with an ISR of `left` spare members over `min_isr`
    out.extend(kv_repl::primary::shrink_choice(&ages, left + min_isr.max(1), min_isr.max(1), keep_age_ns));
    out
}

#[cfg(test)]
mod tests {
    use kv_common::slot::SlotSet;
    use kv_ctrl::{MemberState, NodeId, ShardId, ShardRole};

    use super::{Lagging, device_failed_reports, shrink_with_pending};

    const MS: u64 = 1_000_000;

    fn l(member: u16, age_ms: Option<u64>, pending: bool) -> Lagging {
        Lagging { member, age_ns: age_ms.map(|a| a * MS), pending }
    }

    #[test]
    fn a_requested_shrink_takes_the_spare_budget_until_it_commits() {
        let keep = 875 * MS;
        // RF3, min ISR 2, both members stalled: the older one goes
        assert_eq!(shrink_with_pending(&[l(0, Some(600), false), l(1, Some(560), false)], 2, keep).as_slice(), &[0]);
        // its shrink is requested and not committed: the other one stays (it used to be picked
        // as soon as its age passed the first one's, and both were dropped: NOREPLICAS)
        assert_eq!(shrink_with_pending(&[l(0, Some(600), true), l(1, Some(640), false)], 2, keep).as_slice(), &[0]);
        assert_eq!(shrink_with_pending(&[l(0, None, true), l(1, Some(700), false)], 2, keep).as_slice(), &[] as &[u16]);
        // past the keep bound it goes as well (a dead member still leaves within a second)
        assert_eq!(shrink_with_pending(&[l(0, Some(900), true), l(1, Some(880), false)], 2, keep).as_slice(), &[0, 1]);
        // RF2, min ISR 2: a member lately shrunk and back is still kept until the keep bound
        assert_eq!(shrink_with_pending(&[l(0, Some(600), true)], 2, keep).as_slice(), &[] as &[u16]);
        assert_eq!(shrink_with_pending(&[l(0, Some(900), true)], 2, keep).as_slice(), &[0]);
        // min ISR 1: every lagging member may go
        assert_eq!(shrink_with_pending(&[l(0, Some(600), true), l(1, Some(560), false)], 1, keep).as_slice(), &[0, 1]);
    }

    fn role(primary: bool, isr_members: usize) -> ShardRole {
        ShardRole {
            shard: ShardId(1),
            term: 2,
            is_primary: primary,
            owned_slots: SlotSet::new(),
            primary_repl: None,
            members: (0..2u8)
                .map(|i| (NodeId([i + 5; 20]), MemberState::InSync, usize::from(i) < isr_members))
                .collect(),
            min_isr: 1,
            history: Vec::new(),
            migrations: Vec::new(),
        }
    }

    /// (a): a failed device write makes a partition read-only; the node then asks for a
    /// failover whenever a healthy member can take over, and never without a failed device.
    #[test]
    fn a_failed_device_asks_for_a_failover_when_a_member_can_take_over() {
        assert!(device_failed_reports(1, Some(&role(true, 1))));
        assert!(device_failed_reports(3, Some(&role(false, 0))), "a member is not usable either");
        assert!(
            !device_failed_reports(1, Some(&role(true, 0))),
            "a primary alone keeps serving what it can"
        );
        assert!(!device_failed_reports(0, Some(&role(true, 2))));
        assert!(!device_failed_reports(2, None), "no role: nothing to fail over");
    }
}
