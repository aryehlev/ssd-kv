//! Control connections to data nodes (DESIGN §23.4).
//!
//! The leader dials every registered node's control port (nodes never dial anyone for
//! topology), performs the `Hello`/`HelloAck` handshake, pushes `Assign` on every committed
//! change of a node's projection and answers renewals. Requests carry a `req` id; replies echo
//! it. A protocol violation closes the connection (§22).
//!
//! * One task per registered node holds its connection while this coordinator leads; all of
//!   them stop when it stops leading. The node's projection comes only from committed state
//!   (`ClusterState::assignment_for`, stamped with the leader's term); it is pushed on connect
//!   and whenever it changes, and pushed again when no `AssignAck` arrives within a second.
//! * Cutover ordering (§23.13 step 3): an assignment that moves a migration into Cutover reaches
//!   the source shard's primary first; the destination's is held until the source acknowledged
//!   it (for at most one second: the destination serves nothing before the source's seal
//!   anyway).
//! * Renewals feed the [`LeaseTable`]; a `LeaseGrant` is sent only while this coordinator's own
//!   check-quorum lease is valid and the node is not flagged (or about to be flagged) FAIL.
//! * Node-initiated messages (ISR changes, readiness, migration events) go to the controllers
//!   as [`NodeEvent`]s; replies to requests go to the waiting [`Pusher::request`].

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use kv_ctrl::{
    CoordId, CoordToNode, MigPhase, MigrationTask, NodeAssignment, NodeError, NodeId, NodeToCoord,
};
use kv_topo::ClusterState;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::{Notify, mpsc, oneshot};

use crate::detector::Detector;
use crate::lease::{LeaseTable, RenewOutcome};
use crate::wsr::Wsr;
use crate::wsr::rpc::{encode_frame, read_frame};

#[derive(Debug)]
pub enum PushError {
    NotConnected,
    Timeout,
    Closed,
    Io(std::io::Error),
}

impl PushError {
    /// Short name for diagnostics.
    pub fn kind(&self) -> &'static str {
        match self {
            PushError::NotConnected => "not connected",
            PushError::Timeout => "timeout",
            PushError::Closed => "connection closed",
            PushError::Io(_) => "i/o error",
        }
    }
}

impl std::fmt::Display for PushError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PushError::Io(e) => write!(f, "i/o error: {e}"),
            other => f.write_str(other.kind()),
        }
    }
}

impl std::error::Error for PushError {}

/// Something a node told the leader on its own initiative.
pub enum NodeEvent {
    /// `IsrShrink`, `IsrExpand`, `MemberReady`, `MigrationReady`, `SealDone` from `from`.
    Msg { from: NodeId, msg: NodeToCoord },
    /// A renewal (or handshake) came from a new process of `node`.
    NewIncarnation(NodeId),
    /// `node` reported `health.failed`.
    ReportsFailed(NodeId),
}

/// Settings of the control connections.
#[derive(Clone)]
pub struct PushCfg {
    pub coord: CoordId,
    /// The cluster token (no `Debug` on this type).
    pub auth: Vec<u8>,
    pub connect_timeout: Duration,
}

impl PushCfg {
    pub fn new(coord: CoordId, auth: Vec<u8>) -> Self {
        PushCfg {
            coord,
            auth,
            connect_timeout: Duration::from_millis(1000),
        }
    }
}

/// Leases and failure suspicion, kept together: grants depend on both.
pub struct LeaseState {
    pub leases: LeaseTable,
    pub detector: Detector,
}

struct ConnHandle {
    ctrl: SocketAddr,
    tx: mpsc::Sender<CoordToNode>,
    task: tokio::task::JoinHandle<()>,
    connected: Arc<AtomicBool>,
}

type Waiters = hashbrown::HashMap<(NodeId, u64), oneshot::Sender<NodeToCoord>, foldhash::fast::RandomState>;

pub struct Pusher {
    conns: Mutex<hashbrown::HashMap<NodeId, ConnHandle, foldhash::fast::RandomState>>,
    wsr: Arc<Wsr>,
    cfg: PushCfg,
    waiters: Mutex<Waiters>,
    lease: Mutex<LeaseState>,
    /// Highest assignment version each node acknowledged under this leadership.
    acked: Mutex<hashbrown::HashMap<NodeId, u64, foldhash::fast::RandomState>>,
    acked_changed: Notify,
    events: mpsc::UnboundedSender<NodeEvent>,
    next_req: AtomicU64,
    leader_since_ns: AtomicU64,
}

/// Frames queued per node before `send` waits.
const NODE_QUEUE: usize = 256;
/// Pause between connection attempts.
const RECONNECT_PAUSE: Duration = Duration::from_millis(200);
/// An assignment not acknowledged within this time is pushed again.
const ACK_TIMEOUT: Duration = Duration::from_secs(1);
/// Longest wait of a destination's cutover assignment for the source's acknowledgement.
const CUTOVER_ORDER_WAIT: Duration = Duration::from_secs(1);

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Pusher {
    /// A pusher for the coordinator `wsr` runs; node events arrive on the returned receiver.
    pub fn new(wsr: Arc<Wsr>, cfg: PushCfg) -> (Arc<Self>, mpsc::UnboundedReceiver<NodeEvent>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let lease_ms = wsr.committed().params.lease_ms;
        let p = Arc::new(Pusher {
            conns: Mutex::new(hashbrown::HashMap::default()),
            wsr,
            cfg,
            waiters: Mutex::new(hashbrown::HashMap::default()),
            lease: Mutex::new(LeaseState {
                leases: LeaseTable::new(lease_ms),
                detector: Detector::new(),
            }),
            acked: Mutex::new(hashbrown::HashMap::default()),
            acked_changed: Notify::new(),
            events: tx,
            next_req: AtomicU64::new(rand::random::<u64>() >> 2),
            leader_since_ns: AtomicU64::new(u64::MAX),
        });
        (p, rx)
    }

    pub async fn send(&self, n: &NodeId, m: CoordToNode) -> Result<(), PushError> {
        let tx = {
            let conns = lock(&self.conns);
            let c = conns.get(n).ok_or(PushError::NotConnected)?;
            if !c.connected.load(Ordering::Acquire) {
                return Err(PushError::NotConnected);
            }
            c.tx.clone()
        };
        tx.send(m).await.map_err(|_| PushError::Closed)
    }

    pub async fn request(&self, n: &NodeId, m: CoordToNode, timeout: Duration) -> Result<NodeToCoord, PushError> {
        let Some(req) = m.req() else {
            // Not a request: nothing echoes it.
            self.send(n, m).await?;
            return Err(PushError::Closed);
        };
        let (tx, rx) = oneshot::channel();
        lock(&self.waiters).insert((*n, req), tx);
        let r = async {
            self.send(n, m).await?;
            match tokio::time::timeout(timeout, rx).await {
                Ok(Ok(reply)) => Ok(reply),
                Ok(Err(_)) => Err(PushError::Closed),
                Err(_) => Err(PushError::Timeout),
            }
        }
        .await;
        lock(&self.waiters).remove(&(*n, req));
        r
    }

    /// A fresh request id.
    pub fn next_req(&self) -> u64 {
        self.next_req.fetch_add(1, Ordering::Relaxed)
    }

    /// When this coordinator last became leader (CLOCK_BOOTTIME ns; `u64::MAX` while it does
    /// not lead).
    pub fn leader_since_ns(&self) -> u64 {
        self.leader_since_ns.load(Ordering::Acquire)
    }

    /// The leader holds `n`'s lease at `now_ns`.
    pub fn lease_held(&self, n: &NodeId, now_ns: u64) -> bool {
        lock(&self.lease).leases.held(n, now_ns)
    }

    /// When `n`'s current lease certainly ends (`None`: none granted to its current process).
    pub fn lease_expiry(&self, n: &NodeId) -> Option<u64> {
        lock(&self.lease).leases.expiry_ns(n)
    }

    /// `n` is suspected by the detector (PFAIL, or a FAIL about to be committed).
    pub fn suspected(&self, n: &NodeId) -> bool {
        lock(&self.lease).detector.suspected(n)
    }

    /// Runs `f` on the lease table and the detector.
    pub fn with_lease<R>(&self, f: impl FnOnce(&mut LeaseState) -> R) -> R {
        f(&mut lock(&self.lease))
    }

    /// `n`'s control connection is up (handshake done).
    pub fn connected(&self, n: &NodeId) -> bool {
        lock(&self.conns)
            .get(n)
            .is_some_and(|c| c.connected.load(Ordering::Acquire))
    }

    fn acked_version(&self, n: &NodeId) -> u64 {
        lock(&self.acked).get(n).copied().unwrap_or(0)
    }

    fn stop_all(&self) {
        let mut conns = lock(&self.conns);
        for (_, c) in conns.drain() {
            c.task.abort();
        }
        lock(&self.waiters).clear();
        lock(&self.acked).clear();
    }

    /// Keeps one connection task per registered node while this coordinator leads. Returns
    /// when the coordinator stops.
    pub async fn run(self: Arc<Self>) {
        let mut state_rx = self.wsr.subscribe();
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut leading = false;
        loop {
            tokio::select! {
                r = state_rx.changed() => if r.is_err() { break },
                _ = tick.tick() => {}
            }
            if !self.wsr.is_leader() {
                if leading {
                    tracing::info!("stopped leading: closing node control connections");
                    self.stop_all();
                    self.leader_since_ns.store(u64::MAX, Ordering::Release);
                    lock(&self.lease).detector.clear();
                }
                leading = false;
                continue;
            }
            let s = self.wsr.committed();
            if !leading {
                leading = true;
                self.leader_since_ns
                    .store(kv_common::clock::boot_ns(), Ordering::Release);
                lock(&self.lease).detector.clear();
            }
            {
                let mut ls = lock(&self.lease);
                ls.leases.set_lease_ms(s.params.lease_ms);
                ls.leases.retain(&|n| s.node(n).is_some());
            }
            self.reconcile(&s);
        }
        self.stop_all();
    }

    fn reconcile(self: &Arc<Self>, s: &ClusterState) {
        let mut conns = lock(&self.conns);
        conns.retain(|id, c| {
            let keep = s.node(id).is_some_and(|r| r.ctrl == c.ctrl) && !c.task.is_finished();
            if !keep {
                c.task.abort();
            }
            keep
        });
        for rec in &s.nodes {
            if conns.contains_key(&rec.id) {
                continue;
            }
            let (tx, rx) = mpsc::channel(NODE_QUEUE);
            let connected = Arc::new(AtomicBool::new(false));
            let task = tokio::spawn(self.clone().node_task(rec.id, rec.ctrl, rx, connected.clone()));
            conns.insert(
                rec.id,
                ConnHandle {
                    ctrl: rec.ctrl,
                    tx,
                    task,
                    connected,
                },
            );
        }
    }

    async fn node_task(
        self: Arc<Self>,
        node: NodeId,
        ctrl: SocketAddr,
        mut out_rx: mpsc::Receiver<CoordToNode>,
        connected: Arc<AtomicBool>,
    ) {
        loop {
            if !self.wsr.is_leader() {
                return;
            }
            match self.session(node, ctrl, &mut out_rx, &connected).await {
                Ok(()) => return,
                Err(e) => tracing::debug!(node = %node.hex40(), %ctrl, "control connection: {e}"),
            }
            connected.store(false, Ordering::Release);
            // Messages queued for a lost connection are not delivered later.
            while out_rx.try_recv().is_ok() {}
            tokio::time::sleep(RECONNECT_PAUSE).await;
        }
    }

    /// One control connection: handshake, then pushes, renewals and replies until it breaks.
    /// `Ok(())` means stop for good (leadership lost or node removed).
    async fn session(
        &self,
        node: NodeId,
        ctrl: SocketAddr,
        out_rx: &mut mpsc::Receiver<CoordToNode>,
        connected: &AtomicBool,
    ) -> std::io::Result<()> {
        let to = self.cfg.connect_timeout;
        let stream = tokio::time::timeout(to, TcpStream::connect(ctrl))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "connect"))??;
        let _ = stream.set_nodelay(true);
        let (mut rd, mut wr) = stream.into_split();
        let s = self.wsr.committed();
        let term = self.wsr.term();
        write_msg(
            &mut wr,
            &CoordToNode::Hello {
                cluster_id: s.cluster_id,
                coord: self.cfg.coord,
                coord_term: term,
                auth: self.cfg.auth.clone(),
            },
        )
        .await?;
        let body = tokio::time::timeout(to, read_frame(&mut rd))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "no hello ack"))??;
        let ack: NodeToCoord = kv_ctrl::decode_body(&body).map_err(invalid)?;
        match ack {
            NodeToCoord::HelloAck { node: id, incarnation, .. } => {
                if id.is_some_and(|i| i != node) {
                    return Err(invalid("the control address answers with another node id"));
                }
                let known = lock(&self.lease).leases.incarnation(&node);
                if known.is_some_and(|k| k != incarnation) {
                    lock(&self.lease).detector.on_new_incarnation(node);
                    let _ = self.events.send(NodeEvent::NewIncarnation(node));
                }
            }
            NodeToCoord::Error(NodeError::BadAuth) => {
                tracing::warn!(node = %node.hex40(), "node refused the cluster token");
                tokio::time::sleep(Duration::from_secs(1)).await;
                return Err(invalid("bad auth"));
            }
            other => return Err(invalid(format!("unexpected {} before hello ack", other.kind()))),
        }
        // Reader task: decoded frames into a channel (reads are not cancel-safe).
        let (in_tx, mut in_rx) = mpsc::channel::<NodeToCoord>(256);
        let reader = tokio::spawn(async move {
            loop {
                let Ok(body) = read_frame(&mut rd).await else { return };
                let Ok(m) = kv_ctrl::decode_body::<NodeToCoord>(&body) else { return };
                if in_tx.send(m).await.is_err() {
                    return;
                }
            }
        });
        struct AbortOnDrop(tokio::task::JoinHandle<()>);
        impl Drop for AbortOnDrop {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let _reader = AbortOnDrop(reader);
        connected.store(true, Ordering::Release);
        tracing::info!(node = %node.hex40(), %ctrl, "control connection up");

        let mut state_rx = self.wsr.subscribe();
        let mut last: Option<NodeAssignment> = None;
        let mut pending_ack: Option<(u64, Instant)> = None;
        let mut deferred_since: Option<Instant> = None;
        // (state version, leader term, peer generation) the last projection was computed for
        let mut computed_for: Option<(u64, u64, u64)> = None;
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            if !self.wsr.is_leader() {
                return Ok(());
            }
            let s = self.wsr.committed();
            if s.node(&node).is_none() {
                return Ok(());
            }
            let term = self.wsr.term();
            let peers = self.wsr.peers();
            let key = (s.version, term, peers.r#gen);
            let overdue = pending_ack.is_some_and(|(_, t)| t.elapsed() >= ACK_TIMEOUT);
            if computed_for != Some(key) || overdue || deferred_since.is_some() {
                computed_for = Some(key);
                // Push the projection when it changed, or again when it is unacknowledged.
                let want = s.assignment_for(&node, term, &peers.endpoints());
                let changed = last.as_ref().is_none_or(|l| {
                    !l.same_projection(&want) || l.coord_term != want.coord_term
                });
                if changed || overdue {
                    let hold = self.cutover_source(&s, &node, last.as_ref()).filter(|src| {
                        self.acked_version(src) < s.version
                            && deferred_since.is_none_or(|t| t.elapsed() < CUTOVER_ORDER_WAIT)
                    });
                    if hold.is_some() {
                        deferred_since.get_or_insert_with(Instant::now);
                    } else {
                        deferred_since = None;
                        let v = want.version;
                        write_msg(&mut wr, &CoordToNode::Assign(want.clone())).await?;
                        pending_ack = Some((v, Instant::now()));
                        last = Some(want);
                    }
                } else {
                    deferred_since = None;
                }
            }
            tokio::select! {
                r = state_rx.changed() => if r.is_err() { return Ok(()) },
                m = out_rx.recv() => match m {
                    None => return Ok(()),
                    Some(m) => write_msg(&mut wr, &m).await?,
                },
                m = in_rx.recv() => match m {
                    None => return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "closed by the node")),
                    Some(m) => {
                        if let Some(reply) = self.on_inbound(node, m, &mut pending_ack, &s) {
                            write_msg(&mut wr, &reply).await?;
                        }
                    }
                },
                _ = tick.tick() => {}
                _ = self.acked_changed.notified(), if deferred_since.is_some() => {}
                _ = tokio::time::sleep(Duration::from_millis(20)), if deferred_since.is_some() => {}
            }
        }
    }

    /// `node` is the destination primary of a migration that the new state puts in Cutover
    /// while its last assignment did not show it: the source primary must get it first.
    fn cutover_source(&self, s: &ClusterState, node: &NodeId, last: Option<&NodeAssignment>) -> Option<NodeId> {
        for m in &s.migrations {
            if m.phase != MigPhase::Cutover {
                continue;
            }
            let dst = s.shard(m.dst)?;
            if dst.primary != Some(*node) {
                continue;
            }
            let shown = last
                .and_then(|a| a.role.as_ref())
                .is_some_and(|r| {
                    r.migrations
                        .iter()
                        .any(|t| matches!(t, MigrationTask::In { id, phase: MigPhase::Cutover, .. } if *id == m.id))
                });
            if !shown {
                return s.shard(m.src).and_then(|sh| sh.primary);
            }
        }
        None
    }

    /// Handles one message from `node`; returns the immediate answer, if any.
    fn on_inbound(
        &self,
        node: NodeId,
        m: NodeToCoord,
        pending_ack: &mut Option<(u64, Instant)>,
        s: &ClusterState,
    ) -> Option<CoordToNode> {
        match m {
            NodeToCoord::Renew {
                seq,
                incarnation,
                version_seen: _,
                health,
            } => {
                let now = kv_common::clock::boot_ns();
                let mut ls = lock(&self.lease);
                let out = ls.leases.on_renew(node, incarnation, now);
                if out == RenewOutcome::Ignored {
                    return None;
                }
                ls.leases.set_health(&node, health);
                if out == RenewOutcome::NewIncarnation {
                    ls.detector.on_new_incarnation(node);
                    let _ = self.events.send(NodeEvent::NewIncarnation(node));
                }
                if health.failed {
                    ls.detector.on_reported_failed(node);
                    let _ = self.events.send(NodeEvent::ReportsFailed(node));
                }
                if self.wsr.lease_valid() && ls.detector.may_grant(s, &node) {
                    ls.leases.grant(&node, now);
                    return Some(CoordToNode::LeaseGrant {
                        seq,
                        lease_ms: s.params.lease_ms,
                    });
                }
                None
            }
            NodeToCoord::AssignAck { version } => {
                {
                    let mut a = lock(&self.acked);
                    let e = a.entry(node).or_insert(0);
                    *e = (*e).max(version);
                }
                if pending_ack.is_some_and(|(v, _)| version >= v) {
                    *pending_ack = None;
                }
                self.acked_changed.notify_waiters();
                None
            }
            NodeToCoord::Error(NodeError::StaleAssignment) => {
                // The node applied a newer assignment (from a later coordinator term).
                tracing::warn!(node = %node.hex40(), "node reports a newer assignment than this leader's");
                *pending_ack = None;
                None
            }
            NodeToCoord::Error(e) => {
                tracing::warn!(node = %node.hex40(), "node error: {e:?}");
                None
            }
            m @ (NodeToCoord::FenceAck { .. } | NodeToCoord::HandoffDone { .. }) => {
                let req = m.req().unwrap_or(0);
                if let Some(w) = lock(&self.waiters).remove(&(node, req)) {
                    let _ = w.send(m);
                }
                None
            }
            NodeToCoord::HelloAck { .. } => None,
            m @ (NodeToCoord::IsrShrink { .. }
            | NodeToCoord::IsrExpand { .. }
            | NodeToCoord::MemberReady { .. }
            | NodeToCoord::MigrationReady { .. }
            | NodeToCoord::SealDone { .. }) => {
                let _ = self.events.send(NodeEvent::Msg { from: node, msg: m });
                None
            }
        }
    }
}

fn invalid(e: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
}

async fn write_msg(w: &mut OwnedWriteHalf, m: &CoordToNode) -> std::io::Result<()> {
    let f = encode_frame(m)?;
    w.write_all(&f).await
}

/// Dials a node's control port for `kvadm node add`: handshake only, returning the
/// `HelloAck` (identity and readiness; the node's partition count).
pub async fn probe_node(
    ctrl: SocketAddr,
    cluster_id: [u8; 16],
    coord: CoordId,
    coord_term: u64,
    auth: &[u8],
    timeout: Duration,
) -> std::io::Result<NodeToCoord> {
    let mut s = tokio::time::timeout(timeout, TcpStream::connect(ctrl))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "connect"))??;
    let f = encode_frame(&CoordToNode::Hello {
        cluster_id,
        coord,
        coord_term,
        auth: auth.to_vec(),
    })?;
    s.write_all(&f).await?;
    let body = tokio::time::timeout(timeout, read_frame(&mut s))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "no hello ack"))??;
    kv_ctrl::decode_body(&body).map_err(invalid)
}
