//! Whole-state replication among coordinators (DESIGN §23.3).
//!
//! Rules: (1) persist `coord.meta` before granting a vote or adopting a higher term; (2) a
//! follower persists `accepted` before acknowledging a proposal; (3) depth one: version v+1 is
//! proposed only after v is committed, and queued transitions are batched into the next
//! proposal; (4) a vote is granted only if the candidate's `(last_term, last_version)` is at
//! least the voter's, first come first served within a term, with pre-vote and check-quorum;
//! (5) a new leader first re-proposes its own accepted value under its new term; (6) side
//! effects come only from committed state. Election timeout uniform 500-1000 ms; leader beat
//! every 100 ms among coordinators only. Messages carry `peer_gen`; votes and proposals are
//! accepted only from peers of the coordinator's own generation, and majorities are counted
//! over its own list. Ids are never compared.
//!
//! * [`core`]: the pure state machine; [`rpc`]: peer messages and transport; [`persist`]: the
//!   fixed A/B state files; [`peers`]: the operator-set peer list.
//!
//! [`Wsr`] runs one core in a tokio task: it carries out the core's persistence outputs (on the
//! blocking pool) before sending the step's messages, applies queued transition batches to the
//! committed `ClusterState` to build the next proposal, publishes every committed state on a
//! watch channel and tracks the leader's check-quorum lease (from beat rounds answered by a
//! majority) and the followers' last contact with a leader. A persistence failure stops the
//! member ([`Wsr::is_failed`]); `kvc` then exits.
//!
//! Scheduling delays and slow state writes (a loaded host, a shared disk) never cost the
//! group its leader on their own:
//! * while a state write is in progress the leader keeps beating (the core steps only its
//!   beat timer then, and only the beats are sent: no message of a step whose writes are
//!   pending goes out), so its followers keep hearing from it and its check-quorum lease is
//!   renewed by the rounds they answer;
//! * check-quorum steps the leader down only when no majority answered during a whole window
//!   of twice the minimum election timeout, and a check or an election timer that fires
//!   more than [`LATE_TIMER`] late (this member did not run) is postponed by one beat, so the
//!   replies and beats waiting to be read are handled first.

pub mod core;
pub mod peers;
pub mod persist;
pub mod rpc;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use kv_ctrl::CoordId;
use kv_topo::{ClusterState, Reject, Transition};
use rand::Rng;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::sleep_until;

use self::core::{Core, Entry, Input, Output, Role};
use self::peers::PeerList;
use self::persist::StateFiles;
use self::rpc::{Blob, PeerNet, WsrMsg};

/// Configuration of the local WSR member.
#[derive(Clone)]
pub struct WsrCfg {
    pub id: CoordId,
    pub peer_listen: std::net::SocketAddr,
    pub election_timeout_ms: (u32, u32),
    pub beat_ms: u32,
    /// The cluster token that authenticates peer connections.
    pub auth: Vec<u8>,
    /// Followers serve topology while they heard from a leader within this time (2 s).
    pub topology_fresh_ms: u32,
}

impl Default for WsrCfg {
    fn default() -> Self {
        WsrCfg {
            id: CoordId([0; 20]),
            peer_listen: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
            election_timeout_ms: (500, 1000),
            beat_ms: 100,
            auth: Vec::new(),
            topology_fresh_ms: 2000,
        }
    }
}

impl std::fmt::Debug for WsrCfg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WsrCfg")
            .field("id", &self.id)
            .field("peer_listen", &self.peer_listen)
            .field("election_timeout_ms", &self.election_timeout_ms)
            .field("beat_ms", &self.beat_ms)
            .field("auth", &"<redacted>")
            .field("topology_fresh_ms", &self.topology_fresh_ms)
            .finish()
    }
}

#[derive(Debug)]
pub enum ProposeError {
    NotLeader,
    Rejected(Reject),
    StateTooLarge,
    /// Leadership was lost while the batch was in flight: it may or may not commit (a later
    /// leader re-proposes the value if a majority persisted it).
    LostLeadership,
    Io(std::io::Error),
}

impl std::fmt::Display for ProposeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProposeError::NotLeader => f.write_str("this coordinator is not the leader"),
            ProposeError::Rejected(r) => write!(f, "{r}"),
            ProposeError::StateTooLarge => f.write_str("cluster state too large"),
            ProposeError::LostLeadership => {
                f.write_str("leadership lost before the change committed (outcome unknown)")
            }
            ProposeError::Io(e) => write!(f, "coordinator state I/O error: {e}"),
        }
    }
}

impl std::error::Error for ProposeError {}

type Reply = oneshot::Sender<Result<u64, ProposeError>>;

/// A timer that fires this much later than due means this member did not run meanwhile (a
/// stopped or descheduled process, a long state write): the decision waits one beat for the
/// input that queued up.
pub const LATE_TIMER: Duration = Duration::from_millis(100);

enum Cmd {
    Propose(Vec<Transition>, Reply),
    SetPeers(PeerList, oneshot::Sender<Result<(), String>>),
    Shutdown,
}

/// Snapshot of the member's view shared with the services.
struct Shared {
    peers: Arc<PeerList>,
    leader_id: Option<CoordId>,
    last_leader_contact: Option<Instant>,
    lease_until: Option<Instant>,
    leader_since: Option<Instant>,
    /// Committed entry: `(version, term of the value, value)`.
    committed_entry: (u64, u64, Blob),
}

/// core state machine + persistence + peers
pub struct Wsr {
    id: CoordId,
    cfg: WsrCfg,
    committed: watch::Sender<Arc<ClusterState>>,
    leader: AtomicBool,
    term: AtomicU64,
    failed: AtomicBool,
    tx: mpsc::UnboundedSender<Cmd>,
    shared: Mutex<Shared>,
    net: Arc<PeerNet>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    /// Fault injection for tests: milliseconds added to every state-file write.
    write_delay_ms: AtomicU64,
}

/// The next peer message (`None` once the channel closed or while the receiver is out).
async fn recv_peer(rx: &mut Option<mpsc::Receiver<(CoordId, WsrMsg<Blob>)>>) -> Option<(CoordId, WsrMsg<Blob>)> {
    match rx {
        Some(r) => r.recv().await,
        None => std::future::pending().await,
    }
}

fn io_err(msg: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg.into())
}

impl Wsr {
    pub async fn start(cfg: WsrCfg, persist: persist::StateFiles) -> std::io::Result<Arc<Self>> {
        let l = tokio::net::TcpListener::bind(cfg.peer_listen).await?;
        Self::start_with_listener(cfg, persist, l).await
    }

    /// [`Wsr::start`] on an already bound peer listener.
    pub async fn start_with_listener(
        cfg: WsrCfg,
        mut persist: persist::StateFiles,
        listener: tokio::net::TcpListener,
    ) -> std::io::Result<Arc<Self>> {
        let (persist, p) = tokio::task::spawn_blocking(move || {
            let p = persist.load();
            (persist, p)
        })
        .await
        .map_err(|e| io_err(format!("state file reader stopped: {e}")))?;
        let p = p?;
        if p.committed.version == 0 {
            return Err(io_err(
                "the state files hold no committed state (run `kvc init` first)",
            ));
        }
        let state = ClusterState::decode(&p.committed.bytes)
            .map_err(|e| io_err(format!("committed state does not decode: {e}")))?;
        let committed = Entry {
            version: p.committed.version,
            term: p.committed.term,
            value: Blob::new(p.committed.bytes),
        };
        let accepted = p.accepted.map(|a| Entry {
            version: a.version,
            term: a.term,
            value: Blob::new(a.bytes),
        });
        let peers = Arc::new(p.peers);
        let core = Core::recover(cfg.id, p.term, p.voted_for, peers.clone(), committed.clone(), accepted);
        let net = PeerNet::new(cfg.id, cfg.auth.clone());
        net.set_peers(&peers);
        let (tx, cmd_rx) = mpsc::unbounded_channel();
        let (peer_tx, peer_rx) = mpsc::channel(1024);
        let (committed_tx, _) = watch::channel(Arc::new(state));
        let wsr = Arc::new(Wsr {
            id: cfg.id,
            cfg: cfg.clone(),
            committed: committed_tx,
            leader: AtomicBool::new(false),
            term: AtomicU64::new(p.term),
            failed: AtomicBool::new(false),
            tx,
            shared: Mutex::new(Shared {
                peers,
                leader_id: None,
                last_leader_contact: None,
                lease_until: None,
                leader_since: None,
                committed_entry: (committed.version, committed.term, committed.value.clone()),
            }),
            net: net.clone(),
            tasks: Mutex::new(Vec::new()),
            write_delay_ms: AtomicU64::new(0),
        });
        let serve = tokio::spawn({
            let auth = cfg.auth.clone();
            async move {
                if let Err(e) = rpc::serve_peers(listener, auth, peer_tx).await {
                    tracing::error!("peer listener stopped: {e}");
                }
            }
        });
        let driver = Driver::new(wsr.clone(), core, persist);
        let run = tokio::spawn(driver.run(cmd_rx, peer_rx));
        wsr.tasks_lock().extend([serve, run]);
        Ok(wsr)
    }

    /// Fault injection for tests: every later state-file write of this member takes `ms`
    /// milliseconds longer (a slow or contended disk).
    #[doc(hidden)]
    pub fn set_write_delay_ms(&self, ms: u64) {
        self.write_delay_ms.store(ms, Ordering::Release);
    }

    fn tasks_lock(&self) -> std::sync::MutexGuard<'_, Vec<tokio::task::JoinHandle<()>>> {
        self.tasks.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn shared(&self) -> std::sync::MutexGuard<'_, Shared> {
        self.shared.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Proposes `batch` (applied as one all-or-nothing unit, possibly batched with others into
    /// the next version) and waits until it commits.
    pub async fn propose(&self, batch: Vec<Transition>) -> Result<u64 /*committed version*/, ProposeError> {
        let rx = self.submit(batch)?;
        rx.await.unwrap_or(Err(ProposeError::LostLeadership))
    }

    /// Queues `batch` at once (batches queue in call order) and returns the receiver of its
    /// outcome; [`Wsr::propose`] is `submit` plus the wait.
    pub fn submit(
        &self,
        batch: Vec<Transition>,
    ) -> Result<oneshot::Receiver<Result<u64, ProposeError>>, ProposeError> {
        if !self.is_leader() {
            return Err(ProposeError::NotLeader);
        }
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Cmd::Propose(batch, tx))
            .map_err(|_| ProposeError::NotLeader)?;
        Ok(rx)
    }

    pub fn committed(&self) -> Arc<ClusterState> {
        self.committed.borrow().clone()
    }

    pub fn is_leader(&self) -> bool {
        self.leader.load(Ordering::Acquire)
    }

    pub fn term(&self) -> u64 {
        self.term.load(Ordering::Acquire)
    }

    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<Arc<ClusterState>> {
        self.committed.subscribe()
    }

    pub fn id(&self) -> CoordId {
        self.id
    }

    pub fn cfg(&self) -> &WsrCfg {
        &self.cfg
    }

    /// The operator-set peer list in force.
    pub fn peers(&self) -> Arc<PeerList> {
        self.shared().peers.clone()
    }

    /// The leader this coordinator follows (itself when leader), if known.
    pub fn leader_id(&self) -> Option<CoordId> {
        let s = self.shared();
        if self.is_leader() { Some(self.id) } else { s.leader_id }
    }

    /// Leader: its check-quorum lease is valid (a majority answered a beat round that started
    /// less than 90% of the minimum election timeout ago). Node leases are granted only then.
    pub fn lease_valid(&self) -> bool {
        self.is_leader()
            && self
                .shared()
                .lease_until
                .is_some_and(|t| Instant::now() < t)
    }

    /// When this coordinator last won an election (leader only).
    pub fn leader_since(&self) -> Option<Instant> {
        if self.is_leader() { self.shared().leader_since } else { None }
    }

    /// Topology may be served: leader, or a follower that heard from a leader within
    /// `topology_fresh_ms` (§23.3).
    pub fn serving_topology(&self) -> bool {
        if self.is_leader() {
            return true;
        }
        let fresh = Duration::from_millis(u64::from(self.cfg.topology_fresh_ms));
        self.shared()
            .last_leader_contact
            .is_some_and(|t| t.elapsed() < fresh)
    }

    /// The committed entry: `(version, term of the value, encoded state)`.
    pub fn committed_entry(&self) -> (u64, u64, Blob) {
        self.shared().committed_entry.clone()
    }

    /// `KVADM COORD SETPEERS`: validates the single-server change against the local list,
    /// persists it in `coord.meta` and switches the transport.
    pub async fn set_peers(&self, new: PeerList) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Cmd::SetPeers(new, tx))
            .map_err(|_| "coordinator is stopping".to_owned())?;
        rx.await.unwrap_or_else(|_| Err("coordinator is stopping".into()))
    }

    /// The member stopped after a persistence failure.
    pub fn is_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    /// Stops the member (its state files stay as last written).
    pub async fn shutdown(&self) {
        let _ = self.tx.send(Cmd::Shutdown);
        let tasks: Vec<_> = self.tasks_lock().drain(..).collect();
        for t in tasks {
            t.abort();
            let _ = t.await;
        }
        self.net.shutdown();
        self.leader.store(false, Ordering::Release);
    }
}

struct InFlight {
    version: u64,
    term: u64,
    waiters: Vec<Reply>,
    state: Arc<ClusterState>,
}

struct Driver {
    wsr: Arc<Wsr>,
    core: Core<Blob>,
    files: Option<StateFiles>,
    state: Arc<ClusterState>,
    pending: Vec<(Vec<Transition>, Option<Reply>)>,
    inflight: Option<InFlight>,
    election_at: tokio::time::Instant,
    next_beat: tokio::time::Instant,
    next_check: tokio::time::Instant,
    round: u64,
    round_starts: std::collections::VecDeque<(u64, Instant)>,
    /// Peer messages (taken out while a state write runs, see [`Driver::persist`]).
    peer_rx: Option<mpsc::Receiver<(CoordId, WsrMsg<Blob>)>>,
    /// Peer messages that arrived during a state write, handled in order after it.
    deferred: std::collections::VecDeque<(CoordId, WsrMsg<Blob>)>,
    /// The last check-quorum or election timer fired late and was postponed by one beat.
    postponed: bool,
}

impl Driver {
    fn new(wsr: Arc<Wsr>, core: Core<Blob>, files: StateFiles) -> Self {
        let state = wsr.committed();
        let now = tokio::time::Instant::now();
        let mut d = Driver {
            wsr,
            core,
            files: Some(files),
            state,
            pending: Vec::new(),
            inflight: None,
            election_at: now,
            next_beat: now,
            next_check: now,
            round: 0,
            round_starts: std::collections::VecDeque::new(),
            peer_rx: None,
            deferred: std::collections::VecDeque::new(),
            postponed: false,
        };
        d.reset_election();
        d
    }

    fn beat_every(&self) -> Duration {
        Duration::from_millis(u64::from(self.wsr.cfg.beat_ms.max(1)))
    }

    /// Check-quorum window: twice the minimum election timeout.
    fn check_window(&self) -> Duration {
        Duration::from_millis(self.election_range().0 * 2)
    }

    /// A beat round starts: the core's beat step (its outputs are returned, not yet carried
    /// out).
    fn beat(&mut self, now: tokio::time::Instant) -> Vec<Output<Blob>> {
        self.round += 1;
        self.round_starts.push_back((self.round, Instant::now()));
        while self.round_starts.len() > 64 {
            self.round_starts.pop_front();
        }
        self.next_beat = now + self.beat_every();
        let mut out = Vec::new();
        self.core.step(Input::BeatTimeout { round: self.round }, &mut out);
        out
    }

    /// The effects of a beat step taken while a state write runs: beats are sent, the
    /// check-quorum lease is renewed; anything else waits for the next beat after the write.
    fn beat_effects(&mut self, out: &[Output<Blob>]) {
        for o in out {
            match o {
                Output::Send {
                    to,
                    msg: msg @ WsrMsg::Beat { .. },
                } => self.wsr.net.send(to, msg),
                Output::QuorumHeard { .. } => self.absorb(std::slice::from_ref(o)),
                _ => {}
            }
        }
    }

    fn election_range(&self) -> (u64, u64) {
        let (a, b) = self.wsr.cfg.election_timeout_ms;
        (u64::from(a), u64::from(b.max(a + 1)))
    }

    fn reset_election(&mut self) {
        let (a, b) = self.election_range();
        let ms = rand::rng().random_range(a..b);
        self.election_at = tokio::time::Instant::now() + Duration::from_millis(ms);
    }

    async fn run(mut self, mut cmd_rx: mpsc::UnboundedReceiver<Cmd>, peer_rx: mpsc::Receiver<(CoordId, WsrMsg<Blob>)>) {
        self.peer_rx = Some(peer_rx);
        let result: std::io::Result<()> = async {
            loop {
                // what arrived during a state write first, in arrival order
                if let Some((from, msg)) = self.deferred.pop_front() {
                    self.process(Some(Input::Msg { from, msg })).await?;
                    continue;
                }
                let leader = self.core.role() == Role::Leader;
                let deadline = if leader {
                    self.next_beat.min(self.next_check)
                } else {
                    self.election_at
                };
                tokio::select! {
                    biased;
                    cmd = cmd_rx.recv() => match cmd {
                        None | Some(Cmd::Shutdown) => return Ok(()),
                        Some(Cmd::Propose(batch, tx)) => {
                            if leader {
                                self.pending.push((batch, Some(tx)));
                                self.process(None).await?;
                            } else {
                                let _ = tx.send(Err(ProposeError::NotLeader));
                            }
                        }
                        Some(Cmd::SetPeers(new, tx)) => {
                            let r = self.set_peers(new).await;
                            let _ = tx.send(r);
                        }
                    },
                    m = recv_peer(&mut self.peer_rx) => {
                        let Some((from, msg)) = m else { return Ok(()) };
                        self.process(Some(Input::Msg { from, msg })).await?;
                    }
                    _ = sleep_until(deadline) => {
                        let now = tokio::time::Instant::now();
                        if leader {
                            if now >= self.next_beat {
                                let out = self.beat(now);
                                self.process_out(out).await?;
                            }
                            if now >= self.next_check {
                                if !self.postponed && now.saturating_duration_since(self.next_check) > LATE_TIMER {
                                    // this member did not run: the replies that waited first
                                    // (once: a member that is always late still decides)
                                    self.postponed = true;
                                    self.next_check = now + self.beat_every();
                                } else {
                                    self.postponed = false;
                                    self.next_check = now + self.check_window();
                                    self.process(Some(Input::CheckQuorum)).await?;
                                }
                            }
                        } else if !self.postponed && now.saturating_duration_since(self.election_at) > LATE_TIMER {
                            // this member did not run: the beats that waited first (once)
                            self.postponed = true;
                            self.election_at = now + self.beat_every();
                        } else {
                            self.postponed = false;
                            self.reset_election();
                            self.process(Some(Input::ElectionTimeout)).await?;
                        }
                    }
                }
            }
        }
        .await;
        if let Err(e) = result {
            tracing::error!("coordinator state could not be persisted, stopping: {e}");
            self.wsr.failed.store(true, Ordering::Release);
        }
        self.wsr.leader.store(false, Ordering::Release);
        self.fail_all(true);
        self.wsr.net.shutdown();
    }

    fn fail_all(&mut self, stopping: bool) {
        if let Some(f) = self.inflight.take() {
            for w in f.waiters {
                let _ = w.send(Err(ProposeError::LostLeadership));
            }
        }
        for (_, w) in self.pending.drain(..) {
            if let Some(w) = w {
                let _ = w.send(Err(if stopping {
                    ProposeError::LostLeadership
                } else {
                    ProposeError::NotLeader
                }));
            }
        }
    }

    async fn set_peers(&mut self, new: PeerList) -> Result<(), String> {
        let cur = self.core.peers().clone();
        if cur == new {
            return Ok(()); // a repeated operator call (kvadm retries) changes nothing
        }
        peers::set_peers(&cur, &new).map_err(|e| e.to_string())?;
        if !new.contains(&self.core.id()) && !cur.contains(&self.core.id()) {
            return Err("this coordinator is in neither the current nor the new list".into());
        }
        let new = Arc::new(new);
        self.process(Some(Input::SetPeers(new.clone())))
            .await
            .map_err(|e| format!("coord.meta write failed: {e}"))?;
        tracing::info!(peer_gen = new.r#gen, peers = new.peers.len(), "peer list changed by the operator");
        self.wsr.net.set_peers(&new);
        self.wsr.shared().peers = new;
        Ok(())
    }

    /// One core step (or none, to flush queued proposals), then its effects in the required
    /// order: in-memory commits, the next proposal, persistence, then messages.
    async fn process(&mut self, input: Option<Input<Blob>>) -> std::io::Result<()> {
        let mut out = Vec::new();
        if let Some(i) = input {
            self.core.step(i, &mut out);
        }
        self.process_out(out).await
    }

    /// The effects of a step's outputs `out` (see [`Driver::process`]).
    async fn process_out(&mut self, mut out: Vec<Output<Blob>>) -> std::io::Result<()> {
        self.absorb(&out);
        if self.core.can_propose() && !self.pending.is_empty()
            && let Some(value) = self.build_proposal() {
                let mut more = Vec::new();
                self.core.step(Input::Propose { value }, &mut more);
                self.absorb(&more);
                out.extend(more);
            }
        let meta = out.iter().any(|o| matches!(o, Output::PersistMeta));
        let state = out.iter().any(|o| matches!(o, Output::PersistState));
        if meta || state {
            self.persist(meta, state).await?;
        }
        for o in out {
            if let Output::Send { to, msg } = o {
                self.wsr.net.send(&to, &msg);
            }
        }
        self.wsr.term.store(self.core.term(), Ordering::Release);
        Ok(())
    }

    async fn persist(&mut self, meta: bool, state: bool) -> std::io::Result<()> {
        let Some(mut files) = self.files.take() else {
            return Err(io_err("state files unavailable"));
        };
        let (term, voted_for, _) = self.core.meta();
        let peers = self.core.peers_arc();
        let c = self.core.committed().clone();
        let a = self.core.accepted().cloned();
        let delay = self.wsr.write_delay_ms.load(Ordering::Acquire);
        let mut task = tokio::task::spawn_blocking(move || {
            if delay > 0 {
                std::thread::sleep(Duration::from_millis(delay));
            }
            let r = (|| {
                if meta {
                    files.write_meta(term, voted_for, &peers)?;
                }
                if state {
                    files.write_state_raw(
                        (c.version, c.term, c.value.as_slice()),
                        a.as_ref().map(|a| (a.version, a.term, a.value.as_slice())),
                    )?;
                }
                Ok(())
            })();
            (files, r)
        });
        // A leader keeps beating while the files are written (unless its term or vote is
        // being written: nothing of the new term goes out before it is durable). Only the
        // beat step and the beat replies of the current term are stepped meanwhile (they touch
        // the round counters alone), and only the beats are sent: the step that asked for this
        // write sends nothing before it completes, no proposal acknowledgement is counted
        // before it, every other message waits for it in arrival order, and the in-flight
        // proposal is re-sent at the next beat after it.
        //
        // A follower likewise keeps answering the beats of its leader's term while it writes
        // a state value (never its term or vote): the reply says only that it follows that
        // leader in that term, which stays true, since every message that could change it
        // (a vote request, a higher term) waits for the write, and the beat itself is stepped
        // after the write, ahead of anything that arrived after it, so the election timer is
        // reset before any later vote request is seen. Beats are answered so only while no
        // message of another term or sender waits (`answer_beats`). Without this, followers
        // that persist every
        // proposal on a slow disk answered no beat for the length of each write, and the
        // leader's check-quorum lease lapsed (no node lease granted) or it stepped down.
        let mut rx = self.peer_rx.take();
        let mut closed = false;
        let mut answer_beats = {
            let (term, leader) = (self.core.term(), self.core.leader());
            self.deferred
                .iter()
                .all(|(f, m)| m.term() == term && Some(*f) == leader)
        };
        let joined = loop {
            let beating = !meta && self.core.role() == Role::Leader;
            let following = !meta
                && self.core.role() != Role::Leader
                && self.core.leader().is_some()
                && answer_beats;
            tokio::select! {
                biased;
                r = &mut task => break r,
                _ = sleep_until(self.next_beat), if beating => {
                    let now = tokio::time::Instant::now();
                    let out = self.beat(now);
                    self.beat_effects(&out);
                }
                m = recv_peer(&mut rx), if (beating || following) && !closed => {
                    // a closed channel is seen again by the main loop after the write
                    closed = m.is_none();
                    if let Some((from, msg)) = m {
                        let term = self.core.term();
                        if beating {
                            let current = matches!(&msg, WsrMsg::BeatReply { term: t, .. } if *t == term);
                            if current && self.deferred.is_empty() {
                                let mut out = Vec::new();
                                self.core.step(Input::Msg { from, msg }, &mut out);
                                self.beat_effects(&out);
                            } else {
                                self.deferred.push_back((from, msg));
                            }
                            continue;
                        }
                        // messages of the leader's own term from the leader (beats, the
                        // proposal it re-sends at each beat) change neither the term nor whom
                        // this member follows; anything else stops the answers
                        if msg.term() != term || self.core.leader() != Some(from) {
                            answer_beats = false;
                        } else if let WsrMsg::Beat { round, .. } = &msg {
                            let reply = WsrMsg::BeatReply {
                                peer_gen: self.core.peers().r#gen,
                                term,
                                round: *round,
                                need: false,
                            };
                            self.wsr.net.send(&from, &reply);
                        }
                        self.deferred.push_back((from, msg));
                    }
                }
            }
        };
        self.peer_rx = rx;
        let (files, r) = joined.map_err(|e| io_err(format!("state file writer stopped: {e}")))?;
        self.files = Some(files);
        r
    }

    /// Effects of core outputs that need no persistence: commits, role changes, timers.
    fn absorb(&mut self, out: &[Output<Blob>]) {
        for o in out {
            match o {
                Output::Commit { version } => self.on_commit(*version),
                Output::BecameLeader { term } => {
                    tracing::info!(term, "elected coordinator leader");
                    let now = tokio::time::Instant::now();
                    self.next_beat = now;
                    self.next_check = now + self.check_window();
                    {
                        let mut s = self.wsr.shared();
                        s.leader_id = Some(self.core.id());
                        s.leader_since = Some(Instant::now());
                        s.lease_until = None;
                    }
                    self.wsr.term.store(*term, Ordering::Release);
                    self.wsr.leader.store(true, Ordering::Release);
                    // A bootstrap state (term 0, never replicated) is replaced by one value of
                    // the group at once, so every member serves the same cluster identity.
                    if self.core.committed().term == 0 && self.core.accepted().is_none() {
                        self.pending.push((Vec::new(), None));
                    }
                }
                Output::SteppedDown { term } => {
                    tracing::info!(term, "stepped down as coordinator leader");
                    self.wsr.leader.store(false, Ordering::Release);
                    {
                        let mut s = self.wsr.shared();
                        s.leader_since = None;
                        s.lease_until = None;
                        s.leader_id = None;
                    }
                    self.fail_all(false);
                    self.reset_election();
                }
                Output::LeaderContact { leader } => {
                    {
                        let mut s = self.wsr.shared();
                        s.leader_id = Some(*leader);
                        s.last_leader_contact = Some(Instant::now());
                    }
                    self.reset_election();
                }
                Output::VoteGranted => self.reset_election(),
                Output::QuorumHeard { round } => {
                    if let Some(&(_, start)) = self.round_starts.iter().find(|(r, _)| r == round) {
                        // 90% of the minimum election timeout: margin for clock rate drift.
                        let lease = Duration::from_millis(self.election_range().0 * 9 / 10);
                        let until = start + lease;
                        let mut s = self.wsr.shared();
                        if s.lease_until.is_none_or(|t| until > t) {
                            s.lease_until = Some(until);
                        }
                    }
                }
                Output::Send { .. } | Output::PersistMeta | Output::PersistState => {}
            }
        }
    }

    fn on_commit(&mut self, version: u64) {
        let entry = self.core.committed().clone();
        debug_assert_eq!(entry.version, version);
        let state = match self.inflight.take() {
            Some(f) if f.version == version && f.term == entry.term => {
                for w in f.waiters {
                    let _ = w.send(Ok(version));
                }
                Some(f.state)
            }
            other => {
                if let Some(f) = other {
                    for w in f.waiters {
                        let _ = w.send(Err(ProposeError::LostLeadership));
                    }
                }
                match ClusterState::decode(entry.value.as_slice()) {
                    Ok(s) => Some(Arc::new(s)),
                    Err(e) => {
                        tracing::error!(version, "committed state does not decode: {e}");
                        None
                    }
                }
            }
        };
        self.wsr.shared().committed_entry = (entry.version, entry.term, entry.value.clone());
        if let Some(s) = state {
            if s.version != version {
                tracing::error!(version, "committed state carries version {}", s.version);
            }
            self.state = s.clone();
            self.wsr.committed.send_replace(s);
        }
    }

    /// Applies every queued batch (each all-or-nothing) on top of the committed state; the
    /// result is the next version's value.
    fn build_proposal(&mut self) -> Option<Blob> {
        let base = self.state.clone();
        let mut acc: Option<(ClusterState, Vec<u8>)> = None;
        let mut waiters = Vec::new();
        let max = self.files.as_ref().map_or(persist::STATE_MAX_BYTES, |f| {
            f.half_bytes()
                .saturating_sub(4096 + self.core.committed().value.as_slice().len())
                .min(persist::STATE_MAX_BYTES)
        });
        for (batch, tx) in self.pending.drain(..) {
            let mut b = acc.as_ref().map_or_else(|| (*base).clone(), |(s, _)| s.clone());
            b.version = base.version;
            match b.apply_with_encoding(&batch) {
                Ok((s, bytes)) if bytes.len() <= max => {
                    acc = Some((s, bytes));
                    waiters.extend(tx);
                }
                Ok(_) | Err(Reject::StateTooLarge) => {
                    if let Some(tx) = tx {
                        let _ = tx.send(Err(ProposeError::StateTooLarge));
                    }
                }
                Err(e) => {
                    if let Some(tx) = tx {
                        let _ = tx.send(Err(ProposeError::Rejected(e)));
                    }
                }
            }
        }
        let (state, bytes) = acc?;
        self.inflight = Some(InFlight {
            version: state.version,
            term: self.core.term(),
            waiters,
            state: Arc::new(state),
        });
        Some(Blob::new(bytes))
    }
}
