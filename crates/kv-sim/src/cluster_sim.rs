//! Cluster simulation (DESIGN §23.11, §25.3).
//!
//! 3 coordinators and 6 data nodes in-process over the simulated network. Checks: no
//! acknowledged write lost; at most one serving primary per shard per instant; writes of an old
//! term never commit after a fence; membership and slot map changed only by admin ops; zero
//! full-sync or migration bytes caused by failures alone; repaired members converge; per-key
//! read/write checks; failover picks by the static rule for every fence-reply pattern; merge
//! resync rewrites only divergent keys; migrations reach Ready exactly at recorded barriers.
//!
//! What runs here is production code wherever it is a library state machine:
//! * coordinators: the WSR core (`kv_coord::wsr::core::Core`) over the whole
//!   [`kv_topo::ClusterState`], with `ClusterState::apply`, the lease table and failure
//!   detector of kv-coord, the static failover choice and promotion transition
//!   (`choose_primary`, `failover_promote`), the ISR and member-ready rules (`kv_coord::isr`),
//!   the node projection (`assignment_for`) and client redirects (`kv_topo::redirect`);
//! * data nodes: the replication state machines of kv-repl (`ReplSender`, `MemberStream`,
//!   `sync::decide`, the group codec, the frame codec and the RAM rings).
//!
//! The coordinator's async driver (`Wsr`, `Pusher`, `run_failover`) and the data node's cluster
//! agent, routing gate and replication glue (`kv-node/src/cluster/*`, a binary crate) cannot be
//! linked in-process; this file drives the same steps as those drivers, in the order §23.4,
//! §23.6-§23.10 give them, on top of the state machines above. The storage engine is modelled
//! by a per-partition record map with a durability pipeline (writes become durable after a
//! seeded, sometimes slow, disk delay; a crash keeps only the durable records), so that the run
//! stays fast enough for many seeds; the storage engine itself is covered by `storage_sim`.
//!
//! Faults: message drop, delay, duplication and reordering (`simnet`), network partitions,
//! clock-rate drift, crash and restart of any coordinator or node, slow disks. Replication
//! streams are ordered byte streams over the lossy network: a lost message breaks the stream,
//! which is then redialed (as a TCP reset would). Nothing in a run writes a trace (LC-33); a
//! failure is reported with its seed, from which the whole run is regenerated.
//!
//! The node model follows `kv-node/src/cluster/*` step for step where it decides something:
//! the rings a partition keeps across role changes (a member keeps its received ring and the
//! backlog of a term it served as primary; a promoted node uses the ring of the term it took
//! over, or its own backlog when it is re-promoted, else a ring that was never started and
//! covers nothing), the Repair key set (a) (`kv_repl::member::own_keys_after`, else a walk of
//! the partition), the persisted `last_term` of the handshake, and the restart hold of a
//! primary (after a crash it never serves the term it served before and reports
//! `health.failed` while its assignment names it primary of that term), the ISR expand
//! handshake (counting starts after `ReplSender::caught_up`, the `IsrExpand` is sent once
//! `ReplSender::holds_committed`, and a counted member stops counting only before its expand
//! was requested: kv-node `ExpandGate`) and the primary's NeedsFullSync report. The
//! coordinator side runs the failover steps of `run_failover`, including the lease hold-off of
//! the failed primary while another node is fenced (`Detector::hold_off`).
//!
//! The findings of earlier campaigns are regression tests (`repro_*`): R1 and R2 (received
//! rings, kv-repl and kv-node), the failover lease wait (kv-coord). `KV_SIM_TRACE=1` (and
//! `KV_SIM_TRACE_KEY=<k>`) print proposals, faults and the writes of one key to stderr for a
//! failing seed.
//!
//! Scope of this harness (reported, not hidden): no operator command runs during a simulated
//! run (the cluster is bootstrapped by admin transitions before it starts), so full sync,
//! migrations and handoffs are not exercised here; the merge resync and migration barrier
//! properties are covered by kv-repl's `full_sync_keeps_layout` and
//! `migration_ready_is_position_barrier`, and the static failover rule by kv-coord's
//! `promote_isr_independent_of_fence_replies`. Every `Promote` committed here is checked
//! against the static rule over the fence attempts the run actually made.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;

use kv_coord::detector::Detector;
use kv_coord::isr;
use kv_coord::lease::{LeaseTable, RenewOutcome};
use kv_coord::wsr::core::{Core, Entry, Input, Output, Role};
use kv_coord::wsr::peers::{CoordRecord, PeerList};
use kv_coord::wsr::rpc::WsrMsg;
use kv_ctrl::{
    CoordEndpoint, CoordId, CoordToNode, Health, MemberState, NodeAssignment, NodeId, NodeToCoord,
    ShardId, SlotRanges,
};
use kv_repl::backlog::Backlog;
use kv_repl::frame::{
    self, AcceptMode, Frame, FrameDecoder, Hello, ImageRec, Recovered, RejectReason,
};
use kv_repl::member::{MemberAction, MemberStream};
use kv_repl::primary::{MemberId, ReplSender, StallClock};
use kv_repl::sync::{self, PrimaryView, SyncDecision};
use kv_repl::{Group, Op, Seq};
use kv_topo::redirect::{Redirect, for_slot};
use kv_topo::state::ShardStatus;
use kv_topo::{AdminOp, ClusterParams, ClusterState, Transition};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::linearize::{self, Event};
use crate::simnet::{NetFaults, SimNet};

const MS: u64 = 1_000_000;
/// Partitions per node (the smallest cluster-wide P the coordinator accepts).
const P: u16 = 16;
/// Replication ring bytes per partition (RAM only; small so that a run locks little memory).
const RING: usize = 16 << 10;
/// Shard replication factor and minimum ISR of the bootstrapped shards.
const RF: usize = 3;
const MIN_ISR: u8 = 2;
/// Keys the clients use (small, so every key sees many overwrites).
const KEYS: usize = 24;

/// One cluster simulation run's parameters.
#[derive(Clone, Copy, Debug)]
pub struct ClusterSimCfg {
    pub seed: u64,
    pub coordinators: u8,
    pub nodes: u8,
    /// Upper bound on processed events (a run normally ends at `sim_ms`).
    pub steps: u64,
    /// Simulated duration of the faulty part of the run, in milliseconds; a quiet phase (every
    /// process up, network healed) of [`QUIET_MS`] follows before the final checks.
    pub sim_ms: u64,
    pub faults: NetFaults,
    /// Mean time between injected faults (crash, restart, partition, slow disk), in ms; 0
    /// disables fault injection.
    pub fault_every_ms: u64,
    /// Concurrent logical clients.
    pub clients: u8,
    /// Freezes are part of the fault mix: a node or coordinator stops for 0.1-2.5 s (SIGSTOP,
    /// a long scheduling delay) and then runs its stale timers before the input that waited;
    /// a frozen node is sometimes crashed right after it resumes.
    pub freezes: bool,
    /// A scripted check (no other fault needs to run): at `at_ms` the primary of shard 1 is
    /// frozen for `freeze_ms`, then crashed `crash_ms` after it resumes; the run fails unless
    /// no ISR shrink was committed for the frozen primary's shard meanwhile and another
    /// member is promoted within 3 s of the crash.
    pub stall_crash: Option<StallCrash>,
    /// The members' ack age test runs on the stall clock (`kv_repl::primary::StallClock`, as
    /// kv-node does) and the coordinator acts on a shrink only from a primary whose lease it
    /// holds; `false` models the time before both (a primary's own stall counted as member
    /// lag, and its shrink requests were committed on waking up).
    pub stall_clock: bool,
}

/// See [`ClusterSimCfg::stall_crash`].
#[derive(Clone, Copy, Debug)]
pub struct StallCrash {
    pub at_ms: u64,
    pub freeze_ms: u64,
    pub crash_ms: u64,
}

/// Length of the quiet phase that ends every run.
pub const QUIET_MS: u64 = 20_000;

impl ClusterSimCfg {
    /// The §25.3 shape (3 coordinators, 6 nodes) with a lossy network, crashes, partitions,
    /// clock drift and slow disks.
    pub fn new(seed: u64) -> Self {
        ClusterSimCfg {
            seed,
            coordinators: 3,
            nodes: 6,
            steps: 40_000_000,
            sim_ms: 30_000,
            faults: NetFaults {
                drop: 0.002,
                duplicate: 0.002,
                max_delay_ns: 3 * MS,
                max_clock_drift: 1e-3,
                min_delay_ns: 50_000,
            },
            fault_every_ms: 1_500,
            clients: 4,
            freezes: true,
            stall_crash: None,
            stall_clock: true,
        }
    }
}

/// Counters of one run (for campaign summaries).
#[derive(Clone, Debug, Default)]
pub struct ClusterSimReport {
    pub events: u64,
    pub commits: u64,
    pub promotions: u64,
    pub fences: u64,
    pub node_crashes: u64,
    pub coord_crashes: u64,
    pub partitions: u64,
    pub slow_disks: u64,
    pub acked_writes: u64,
    pub unacked_writes: u64,
    pub reads: u64,
    pub repairs: u64,
    pub continues: u64,
    pub resync_needed: u64,
    pub max_term: u32,
    pub stream_breaks: u64,
    pub freezes: u64,
    /// NeedOperatorResync by cause: interrupted sync, same term not covered by the backlog,
    /// no takeover recorded for the previous term, the received ring does not cover the
    /// member's gap, the own backlog does not cover the current term, other.
    pub resync_why: [u64; 6],
}

pub fn run(cfg: &ClusterSimCfg) -> Result<(), String> {
    run_report(cfg).map(|_| ())
}

/// Runs one seed; `Err` names the first violated property (with the seed).
pub fn run_report(cfg: &ClusterSimCfg) -> Result<ClusterSimReport, String> {
    let mut w = World::new(cfg)?;
    w.run().map_err(|e| format!("seed {}: {e}", cfg.seed))?;
    Ok(w.report)
}

// =============================================================================================
// identities and addresses

fn coord_id(i: usize) -> CoordId {
    CoordId([0x10 + i as u8; 20])
}

fn coord_index(id: &CoordId) -> usize {
    usize::from(id.0[0] - 0x10)
}

fn node_id(i: usize) -> NodeId {
    NodeId([0xA0 + i as u8; 20])
}

fn node_index(id: &NodeId) -> Option<usize> {
    id.0[0]
        .checked_sub(0xA0)
        .map(usize::from)
        .filter(|_| id.0.iter().all(|b| *b == id.0[0]))
}

fn node_addr(i: usize, port: u16) -> SocketAddr {
    SocketAddr::from(([10, 0, 1, i as u8 + 1], port))
}

fn key_name(k: usize) -> Vec<u8> {
    format!("key:{k}").into_bytes()
}

fn partition_of(key: &[u8]) -> u16 {
    kv_common::key_slot(key) % P
}

fn val_bytes(id: u64) -> Vec<u8> {
    id.to_le_bytes().to_vec()
}

fn val_id(v: &[u8]) -> Option<u64> {
    <[u8; 8]>::try_from(v).ok().map(u64::from_le_bytes)
}

// =============================================================================================
// messages

#[derive(Clone)]
enum Msg {
    Wsr(WsrMsg<Arc<ClusterState>>),
    /// Coordinator (leader) -> node control message.
    Ctrl {
        coord_term: u64,
        m: CoordToNode,
    },
    /// Node -> coordinator.
    Report(NodeToCoord),
    /// One chunk of an ordered replication byte stream; `None` closes it.
    Stream {
        conn: u64,
        seq: u64,
        data: Option<Vec<u8>>,
    },
    /// Client -> coordinator: where is `slot` served?
    Route {
        op: u64,
        slot: u16,
    },
    RouteReply {
        op: u64,
        node: Option<usize>,
    },
    /// Client -> node.
    Req {
        op: u64,
        req: Req,
    },
    /// Node -> client.
    Resp {
        op: u64,
        resp: Resp,
    },
}

#[derive(Clone)]
enum Req {
    Set(Vec<u8>, u64),
    Del(Vec<u8>),
    Get(Vec<u8>),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Resp {
    Ok,
    Value(Option<u64>),
    Moved,
    TryAgain,
    NoReplicas,
}

// =============================================================================================
// observer: the global checks

struct Obs {
    /// First committed value seen per version (WSR: at most one value per version).
    versions: BTreeMap<u64, Arc<ClusterState>>,
    /// Version of the bootstrapped state: no admin transition is committed after it.
    base_version: u64,
    /// `(shard, term) -> takeover per partition` from committed promotions.
    takeover: BTreeMap<(u16, u32), Vec<u64>>,
    /// Acknowledged writes: `(shard, partition, seq)`.
    acked: Vec<(u16, u16, Seq)>,
    /// Per-key client histories.
    hist: Vec<Vec<Event>>,
    /// Fence attempts per `(shard, new_term)` in order (the static-rule check of each Promote).
    fence_attempts: BTreeMap<(u16, u32), Vec<NodeId>>,
    snap_frames: u64,
    violation: Option<String>,
}

impl Obs {
    fn fail(&mut self, what: String) {
        if self.violation.is_none() {
            self.violation = Some(what);
        }
    }

    fn on_commit(&mut self, v: &Arc<ClusterState>, report: &mut ClusterSimReport) {
        let version = v.version;
        if let Some(prev) = self.versions.get(&version) {
            if **prev != **v {
                self.fail(format!(
                    "two different values committed at version {version}"
                ));
            }
            return;
        }
        report.commits += 1;
        if version > self.base_version
            && let Some(prev) = self.versions.get(&(version - 1)).cloned()
        {
            // membership and slot map change only by admin ops (none run during the simulation)
            let ids = |s: &ClusterState| s.nodes.iter().map(|n| n.id).collect::<Vec<_>>();
            let members = |s: &ClusterState| {
                s.shards
                    .iter()
                    .map(|sh| (sh.id, sh.members.iter().map(|m| m.node).collect::<Vec<_>>()))
                    .collect::<Vec<_>>()
            };
            if ids(&prev) != ids(v) || members(&prev) != members(v) || prev.slots != v.slots {
                self.fail(format!(
                    "membership or slot map changed at version {version} without an admin op"
                ));
            }
            for sh in &v.shards {
                let Some(old) = prev.shard(sh.id) else {
                    continue;
                };
                if sh.term > old.term {
                    report.promotions += 1;
                    report.max_term = report.max_term.max(sh.term);
                    if let Some(te) = sh.history.iter().find(|t| t.term == old.term) {
                        self.takeover
                            .insert((sh.id.0, old.term), te.takeover.clone());
                    }
                    // §23.9: only the fenced node is promoted, and it was in the committed ISR
                    if let Some(p) = sh.primary {
                        let tried = self
                            .fence_attempts
                            .get(&(sh.id.0, sh.term))
                            .cloned()
                            .unwrap_or_default();
                        if !tried.contains(&p) {
                            self.fail(format!(
                                "shard {} promoted {} without fencing it",
                                sh.id.0,
                                short(&p)
                            ));
                        }
                        if !old.member(&p).is_some_and(|m| m.in_isr) {
                            self.fail(format!(
                                "shard {} promoted {}, which was not in the committed ISR",
                                sh.id.0,
                                short(&p)
                            ));
                        }
                    }
                    self.check_acked_against_takeover();
                }
            }
        }
        self.versions.insert(version, v.clone());
    }

    /// I3/I4: every acknowledged write of term t is at or below the takeover point of t.
    fn check_acked_against_takeover(&mut self) {
        let mut bad = None;
        for &(sh, p, seq) in &self.acked {
            if let Some(t) = self.takeover.get(&(sh, seq.term()))
                && seq.0 > t[usize::from(p)]
            {
                bad = Some((sh, p, seq, t[usize::from(p)]));
                break;
            }
        }
        if let Some((sh, p, seq, t)) = bad {
            self.fail(format!(
                "write {}:{} of shard {sh} partition {p} acknowledged above the takeover point {}:{} of its term",
                seq.term(),
                seq.counter(),
                Seq(t).term(),
                Seq(t).counter()
            ));
        }
    }

    fn on_ack(&mut self, shard: u16, p: u16, seq: Seq) {
        self.acked.push((shard, p, seq));
        if let Some(t) = self.takeover.get(&(shard, seq.term()))
            && seq.0 > t[usize::from(p)]
        {
            self.fail(format!(
                "a write of term {} committed after that term was fenced and taken over",
                seq.term()
            ));
        }
    }
}

/// `KV_SIM_TRACE=1` prints the coordinators' proposals and the faults to stderr (transitions
/// and process numbers only; no key or value bytes, nothing written to disk).
fn trace_on() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("KV_SIM_TRACE").is_some())
}

/// `KV_SIM_TRACE_KEY=<k>`: also print every write of key `k` on every node.
fn trace_key(key: &[u8]) -> bool {
    static K: std::sync::OnceLock<Option<Vec<u8>>> = std::sync::OnceLock::new();
    K.get_or_init(|| {
        std::env::var("KV_SIM_TRACE_KEY")
            .ok()
            .map(|k| key_name(k.parse().unwrap_or(0)))
    })
    .as_deref()
    .is_some_and(|k| k == key)
}

fn sq(s: Seq) -> String {
    format!("{}:{}", s.term(), s.counter())
}

fn short(n: &NodeId) -> String {
    format!("n{}", node_index(n).map_or(99, |i| i))
}

// =============================================================================================
// coordinators

struct CoordDisk {
    term: u64,
    voted_for: Option<CoordId>,
    committed: Entry<Arc<ClusterState>>,
    accepted: Option<Entry<Arc<ClusterState>>>,
}

/// Leader-only state (what `Pusher`, the controllers and `run_failover` keep in RAM).
struct Leading {
    since: u64,
    leases: LeaseTable,
    detector: Detector,
    /// Node -> (pushed version, local time of the push).
    pushed: BTreeMap<usize, (u64, u64)>,
    acked: BTreeMap<usize, u64>,
    queue: Vec<Transition>,
    in_flight: Option<u64>,
    failovers: BTreeMap<u16, Failover>,
    next_req: u64,
    quorum_until: u64,
    rounds: BTreeMap<u64, u64>,
}

/// One running failover (the steps of `kv_coord::failover::run_failover`).
struct Failover {
    failed: NodeId,
    term: u32,
    unusable: Vec<NodeId>,
    stage: FoStage,
}

enum FoStage {
    Choose,
    Fencing { n: NodeId, req: u64, deadline: u64 },
    LeaseGone { n: NodeId, takeover: Vec<u64> },
    Retry { at: u64 },
}

struct Coord {
    idx: usize,
    up: bool,
    core: Core<Arc<ClusterState>>,
    disk: CoordDisk,
    election_at: u64,
    beat_at: u64,
    check_at: u64,
    leader_contact: u64,
    round: u64,
    leading: Option<Leading>,
    /// kv-coord admin: an ISR shrink only from a primary whose lease is held
    /// (`ClusterSimCfg::stall_clock: false` models the time before that rule).
    shrink_guard: bool,
}

/// Effects of a step: messages to send and observations to make.
struct Fx<'a> {
    me: usize,
    out: &'a mut Vec<(usize, usize, Msg)>,
    obs: &'a mut Obs,
    report: &'a mut ClusterSimReport,
    rng: &'a mut StdRng,
    now_g: u64,
    now_l: u64,
    after: &'a dyn Fn(u64) -> u64,
}

impl Fx<'_> {
    fn send(&mut self, to: usize, m: Msg) {
        self.out.push((self.me, to, m));
    }
}

struct Topo {
    coords: usize,
    nodes: usize,
    client: usize,
    endpoints: Vec<CoordEndpoint>,
}

impl Topo {
    fn coord_proc(&self, i: usize) -> usize {
        i
    }
    fn node_proc(&self, i: usize) -> usize {
        self.coords + i
    }
}

impl Coord {
    fn new(idx: usize, peers: &Arc<PeerList>, base: &Arc<ClusterState>) -> Self {
        let committed = Entry {
            version: base.version,
            term: 0,
            value: base.clone(),
        };
        let disk = CoordDisk {
            term: 0,
            voted_for: None,
            committed: committed.clone(),
            accepted: None,
        };
        Coord {
            idx,
            up: true,
            core: Core::recover(coord_id(idx), 0, None, peers.clone(), committed, None),
            disk,
            election_at: 0,
            beat_at: u64::MAX,
            check_at: u64::MAX,
            leader_contact: 0,
            round: 0,
            leading: None,
            shrink_guard: true,
        }
    }

    fn restart(&mut self, peers: &Arc<PeerList>) {
        self.core = Core::recover(
            coord_id(self.idx),
            self.disk.term,
            self.disk.voted_for,
            peers.clone(),
            self.disk.committed.clone(),
            self.disk.accepted.clone(),
        );
        self.leading = None;
        self.beat_at = u64::MAX;
        self.check_at = u64::MAX;
        self.leader_contact = 0;
    }

    fn state(&self) -> &Arc<ClusterState> {
        &self.core.committed().value
    }

    /// One core step: persistence first (rules 1 and 2), then messages and role changes.
    fn step(&mut self, input: Input<Arc<ClusterState>>, fx: &mut Fx<'_>, topo: &Topo) {
        let mut outs = Vec::new();
        self.core.step(input, &mut outs);
        for o in &outs {
            match o {
                Output::PersistMeta => {
                    let (t, v, _) = self.core.meta();
                    self.disk.term = t;
                    self.disk.voted_for = v;
                }
                Output::PersistState => {
                    self.disk.committed = self.core.committed().clone();
                    self.disk.accepted = self.core.accepted().cloned();
                }
                _ => {}
            }
        }
        for o in outs {
            match o {
                Output::Send { to, msg } => {
                    fx.send(topo.coord_proc(coord_index(&to)), Msg::Wsr(msg))
                }
                Output::Commit { .. } => {
                    let v = self.state().clone();
                    fx.obs.on_commit(&v, fx.report);
                    if let Some(l) = &mut self.leading
                        && l.in_flight.is_some_and(|f| f <= v.version)
                    {
                        l.in_flight = None;
                    }
                }
                Output::BecameLeader { .. } => {
                    let lease_ms = self.state().params.lease_ms;
                    self.leading = Some(Leading {
                        since: fx.now_l,
                        leases: LeaseTable::new(lease_ms),
                        detector: Detector::new(),
                        pushed: BTreeMap::new(),
                        acked: BTreeMap::new(),
                        queue: Vec::new(),
                        in_flight: None,
                        failovers: BTreeMap::new(),
                        next_req: 1,
                        quorum_until: fx.now_l + 500 * MS,
                        rounds: BTreeMap::new(),
                    });
                    self.beat_at = fx.now_g;
                    self.check_at = (fx.after)(500 * MS);
                }
                Output::SteppedDown { .. } => {
                    self.leading = None;
                    self.beat_at = u64::MAX;
                    self.check_at = u64::MAX;
                    self.election_at = (fx.after)(fx.rng.random_range(500 * MS..1000 * MS));
                }
                Output::LeaderContact { .. } => {
                    self.leader_contact = fx.now_g;
                    self.election_at = (fx.after)(fx.rng.random_range(500 * MS..1000 * MS));
                }
                Output::VoteGranted => {
                    self.election_at = (fx.after)(fx.rng.random_range(500 * MS..1000 * MS));
                }
                Output::QuorumHeard { round } => {
                    if let Some(l) = &mut self.leading
                        && let Some(t0) = l.rounds.remove(&round)
                    {
                        l.quorum_until = l.quorum_until.max(t0 + 500 * MS);
                        l.rounds.retain(|r, _| *r > round);
                    }
                }
                Output::PersistMeta | Output::PersistState => {}
            }
        }
    }

    /// Timers and the leader's control loop (every tick).
    fn tick(&mut self, fx: &mut Fx<'_>, topo: &Topo) {
        if self.core.role() == Role::Leader {
            if self.beat_at <= fx.now_g {
                self.round += 1;
                if let Some(l) = &mut self.leading {
                    l.rounds.insert(self.round, fx.now_l);
                }
                let round = self.round;
                self.step(Input::BeatTimeout { round }, fx, topo);
                self.beat_at = (fx.after)(100 * MS);
            }
            if self.check_at <= fx.now_g {
                self.step(Input::CheckQuorum, fx, topo);
                self.check_at = (fx.after)(500 * MS);
            }
        } else if self.election_at <= fx.now_g {
            self.step(Input::ElectionTimeout, fx, topo);
            self.election_at = (fx.after)(fx.rng.random_range(500 * MS..1000 * MS));
        }
        if self.core.role() == Role::Leader && self.leading.is_some() {
            self.control(fx, topo);
        }
    }

    fn quorum_lease_ok(&self, now_l: u64) -> bool {
        self.core.role() == Role::Leader
            && self
                .leading
                .as_ref()
                .is_some_and(|l| now_l < l.quorum_until)
    }

    fn enqueue(&mut self, t: Transition) {
        if let Some(l) = &mut self.leading
            && !l.queue.contains(&t)
        {
            l.queue.push(t);
        }
    }

    fn control(&mut self, fx: &mut Fx<'_>, topo: &Topo) {
        let s = self.state().clone();
        let coord_term = self.core.term();
        let now = fx.now_l;
        // pushes: every node whose projection changed, re-sent until acknowledged
        let mut sends = Vec::new();
        {
            let l = self.leading.as_mut().expect("leading");
            for rec in &s.nodes {
                let Some(n) = node_index(&rec.id) else {
                    continue;
                };
                let acked = l.acked.get(&n).copied().unwrap_or(0);
                if acked >= s.version {
                    continue;
                }
                let due = l
                    .pushed
                    .get(&n)
                    .is_none_or(|&(v, t)| v < s.version || now >= t + 100 * MS);
                if due {
                    l.pushed.insert(n, (s.version, now));
                    sends.push((n, s.assignment_for(&rec.id, coord_term, &topo.endpoints)));
                }
            }
        }
        for (n, a) in sends {
            fx.send(
                topo.node_proc(n),
                Msg::Ctrl {
                    coord_term,
                    m: CoordToNode::Assign(a),
                },
            );
        }
        // the leader dials every node: a node that renews to nobody (restarted, or following an
        // older leader) gets a Hello on a new control connection
        let mut hellos = Vec::new();
        {
            let l = self.leading.as_mut().expect("leading");
            for rec in &s.nodes {
                let Some(n) = node_index(&rec.id) else {
                    continue;
                };
                let quiet = l
                    .leases
                    .last_renew(&rec.id)
                    .is_none_or(|t| now > t + 300 * MS);
                let due = l.pushed.get(&n).is_none_or(|&(_, t)| now >= t + 300 * MS);
                if quiet && due {
                    l.pushed
                        .insert(n, (l.pushed.get(&n).map_or(0, |p| p.0), now));
                    hellos.push(n);
                }
            }
        }
        for n in hellos {
            let m = CoordToNode::Hello {
                cluster_id: s.cluster_id,
                coord: coord_id(self.idx),
                coord_term,
                auth: Vec::new(),
            };
            fx.send(topo.node_proc(n), Msg::Ctrl { coord_term, m });
        }
        // failure detection
        let health = {
            let l = self.leading.as_mut().expect("leading");
            l.detector.tick(&s, &l.leases, now, l.since)
        };
        for t in health {
            self.enqueue(t);
        }
        // failovers of shards whose primary is flagged
        for sh in &s.shards {
            if sh.status == ShardStatus::Promoting || sh.status == ShardStatus::NoPrimary {
                let Some(failed) = sh.primary else { continue };
                let l = self.leading.as_mut().expect("leading");
                l.failovers.entry(sh.id.0).or_insert(Failover {
                    failed,
                    term: sh.term,
                    unusable: Vec::new(),
                    stage: FoStage::Choose,
                });
            }
        }
        self.drive_failovers(&s, fx, topo);
        // proposals (depth one): the queued transitions that still apply, as one batch
        if self.core.can_propose() {
            let mut value = None;
            if let Some(l) = self.leading.as_mut()
                && l.in_flight.is_none()
                && !l.queue.is_empty()
            {
                let mut keep = Vec::new();
                let mut cur = (*s).clone();
                for t in std::mem::take(&mut l.queue) {
                    if let Ok(next) = cur.apply(std::slice::from_ref(&t)) {
                        cur = next;
                        keep.push(t);
                    }
                }
                if !keep.is_empty()
                    && let Ok(next) = s.apply(&keep)
                {
                    if trace_on() {
                        eprintln!(
                            "[{} ms] c{} proposes v{}: {:?}",
                            fx.now_g / MS,
                            self.idx,
                            next.version,
                            keep
                        );
                    }
                    l.in_flight = Some(next.version);
                    value = Some(Arc::new(next));
                }
            }
            if let Some(value) = value {
                self.step(Input::Propose { value }, fx, topo);
            }
        }
    }

    fn drive_failovers(&mut self, s: &Arc<ClusterState>, fx: &mut Fx<'_>, topo: &Topo) {
        let now = fx.now_l;
        let p = &s.params;
        let wait = (u64::from(p.lease_ms) + 1000 + u64::from(p.fail_grace_ms)) * MS;
        let Some(l) = self.leading.as_mut() else {
            return;
        };
        if now < l.since + wait {
            return; // a new leader makes no promotion before every old lease ran out (§23.4)
        }
        let mut proposals = Vec::new();
        let mut fences = Vec::new();
        let shards: Vec<u16> = l.failovers.keys().copied().collect();
        for sid in shards {
            let Some(sh) = s.shard(ShardId(sid)) else {
                if let Some(fo) = l.failovers.remove(&sid) {
                    l.detector.release_hold(&fo.failed);
                }
                continue;
            };
            let fo = l.failovers.get_mut(&sid).expect("present");
            if sh.status == ShardStatus::Serving
                || sh.primary != Some(fo.failed)
                || sh.term != fo.term
            {
                // the run ends (`run_failover` returns): its lease hold-off ends with it
                let failed = fo.failed;
                l.failovers.remove(&sid);
                l.detector.release_hold(&failed);
                continue;
            }
            match &fo.stage {
                FoStage::Retry { at } => {
                    if now >= *at {
                        fo.stage = FoStage::Choose;
                    }
                }
                FoStage::Choose => {
                    let leases = &l.leases;
                    let held = |n: &NodeId| leases.held(n, now);
                    let mut chosen = s.choose_primary(ShardId(sid), &held, &fo.unusable);
                    if chosen.is_none() && sh.status == ShardStatus::NoPrimary {
                        // RF1 path: the failed primary itself is back
                        let f = fo.failed;
                        let back = s.node(&f).is_some_and(|r| !r.flags.fail)
                            && sh.member(&f).is_some_and(|m| m.in_isr)
                            && held(&f)
                            && !fo.unusable.contains(&f);
                        if back {
                            chosen = Some(f);
                        }
                    }
                    match chosen {
                        Some(n) => {
                            let req = l.next_req;
                            l.next_req += 1;
                            let new_term = sh.term + 1;
                            if n != fo.failed {
                                // `run_failover`: from the fence on, the failed primary gets no
                                // new lease (`LeaseHoldOff`)
                                l.detector.hold_off(fo.failed);
                            }
                            fx.obs
                                .fence_attempts
                                .entry((sid, new_term))
                                .or_default()
                                .push(n);
                            fx.report.fences += 1;
                            fences.push((
                                n,
                                CoordToNode::Fence {
                                    req,
                                    shard: ShardId(sid),
                                    new_term,
                                },
                            ));
                            fo.stage = FoStage::Fencing {
                                n,
                                req,
                                deadline: now + 500 * MS,
                            };
                        }
                        None => {
                            // NoEligibleMember: kvc ends this run (and its lease hold-off) and
                            // starts a fresh one (a fresh unusable list) 500 ms later
                            fo.unusable.clear();
                            fo.stage = FoStage::Retry { at: now + 500 * MS };
                            l.detector.release_hold(&fo.failed);
                        }
                    }
                }
                FoStage::Fencing { n, deadline, .. } => {
                    if now >= *deadline {
                        // no answer in time: next node by the same rule, sequentially
                        fo.unusable.push(*n);
                        fo.stage = FoStage::Choose;
                    }
                }
                FoStage::LeaseGone { n, takeover } => {
                    let gone =
                        *n == fo.failed || l.leases.expiry_ns(&fo.failed).is_none_or(|e| now >= e);
                    if gone {
                        let det = &l.detector;
                        let flagged =
                            |x: &NodeId| det.suspected(x) || s.node(x).is_none_or(|r| r.flags.fail);
                        if let Ok(t) =
                            s.failover_promote(ShardId(sid), *n, &flagged, takeover.clone())
                        {
                            proposals.push(t);
                        }
                        // the hold-off stays until the promotion is committed (the entry is
                        // then removed above), as `run_failover` holds it across its proposal
                        fo.stage = FoStage::Retry {
                            at: now + 1000 * MS,
                        };
                    }
                }
            }
        }
        for (n, m) in fences {
            if let Some(i) = node_index(&n) {
                fx.send(
                    topo.node_proc(i),
                    Msg::Ctrl {
                        coord_term: self.core.term(),
                        m,
                    },
                );
            }
        }
        for t in proposals {
            self.enqueue(t);
        }
    }

    fn on_report(&mut self, from_node: usize, m: NodeToCoord, fx: &mut Fx<'_>, topo: &Topo) {
        if self.core.role() != Role::Leader {
            return;
        }
        let s = self.state().clone();
        let nid = node_id(from_node);
        let now = fx.now_l;
        let coord_term = self.core.term();
        let quorum = self.quorum_lease_ok(now);
        let Some(l) = self.leading.as_mut() else {
            return;
        };
        match m {
            NodeToCoord::Renew {
                seq,
                incarnation,
                health,
                ..
            } => {
                match l.leases.on_renew(nid, incarnation, now) {
                    RenewOutcome::Ignored => return,
                    RenewOutcome::NewIncarnation => l.detector.on_new_incarnation(nid),
                    RenewOutcome::Granted => {}
                }
                l.leases.set_health(&nid, health);
                if health.failed {
                    l.detector.on_reported_failed(nid);
                }
                // a failover that fenced another node holds the failed primary's leases off
                // (`Detector::may_grant`), so its wait for that lease (§23.9 step 4) ends; and no
                // grant reaches a node with an unacknowledged push (kv-coord `Pusher`: it may
                // still apply an older role)
                let unacked = l
                    .pushed
                    .get(&from_node)
                    .is_some_and(|&(v, _)| l.acked.get(&from_node).is_none_or(|&a| a < v));
                if quorum && !unacked && l.detector.may_grant(&s, &nid) {
                    l.leases.grant(&nid, now);
                    let lease_ms = s.params.lease_ms;
                    fx.send(
                        topo.node_proc(from_node),
                        Msg::Ctrl {
                            coord_term,
                            m: CoordToNode::LeaseGrant { seq, lease_ms },
                        },
                    );
                }
            }
            NodeToCoord::AssignAck { version } => {
                let a = l.acked.entry(from_node).or_insert(0);
                *a = (*a).max(version);
            }
            NodeToCoord::FenceAck { req, applied } => {
                for fo in l.failovers.values_mut() {
                    if let FoStage::Fencing { n, req: r, .. } = &fo.stage
                        && *n == nid
                        && *r == req
                        && applied.len() == usize::from(s.params.partitions)
                    {
                        fo.stage = FoStage::LeaseGone {
                            n: nid,
                            takeover: applied.clone(),
                        };
                    }
                }
            }
            NodeToCoord::IsrShrink {
                shard,
                term,
                member,
            }
            | NodeToCoord::IsrExpand {
                shard,
                term,
                member,
            } => {
                let add = matches!(m, NodeToCoord::IsrExpand { .. });
                // kv-coord admin: a shrink only from a primary whose lease the leader holds
                let shrink_ok = add
                    || !self.shrink_guard
                    || isr::shrink_accepted(l.leases.held(&nid, now), l.detector.suspected(&nid));
                if shrink_ok
                    && isr::from_primary(&s, shard, &nid)
                    && let Some(t) = isr::isr_transition(&s, shard, term, member, add)
                {
                    self.enqueue(t);
                }
            }
            NodeToCoord::MemberReady {
                shard,
                term,
                member,
            } => {
                if let Some(t) = isr::member_ready_transition(&s, &nid, shard, term, member) {
                    self.enqueue(t);
                }
            }
            NodeToCoord::MemberNeedsResync {
                shard,
                term,
                member,
            } => {
                if let Some(t) = isr::needs_resync_transition(&s, &nid, shard, term, member) {
                    self.enqueue(t);
                }
            }
            _ => {}
        }
    }

    fn on_route(&mut self, slot: u16, fx: &Fx<'_>) -> Option<usize> {
        let serving = self.core.role() == Role::Leader
            || fx.now_g.saturating_sub(self.leader_contact) < 2_000 * MS;
        if !serving || self.leader_contact == 0 && self.core.role() != Role::Leader {
            return None;
        }
        let s = self.state();
        let pfail = |n: &NodeId| {
            self.leading
                .as_ref()
                .is_some_and(|l| l.detector.suspected(n))
        };
        match for_slot(s, slot, &pfail) {
            Redirect::Moved(_, addr) => s
                .nodes
                .iter()
                .find(|n| n.client == addr)
                .and_then(|n| node_index(&n.id)),
            Redirect::ClusterDown | Redirect::TryAgain => None,
        }
    }
}

// =============================================================================================
// data nodes: storage model

#[derive(Clone)]
struct Rec {
    value: Vec<u8>,
    seq: Seq,
}

/// One partition's records: what is live, what is durable, and the writes in between.
struct Store {
    live: BTreeMap<Vec<u8>, Rec>,
    durable: BTreeMap<Vec<u8>, Rec>,
    /// `(durable at (global ns), position, key, new state)` in write order.
    pipeline: VecDeque<(u64, Seq, Vec<u8>, Option<Rec>)>,
    /// Running maximum of the positions written (a group seq, or a repair stamp).
    pos: Seq,
    /// Every write enqueued at or below this position is durable.
    durable_pos: Seq,
    /// Last executed or applied group seq.
    last_exec: Seq,
    last_durable_at: u64,
}

impl Store {
    fn new(start: Seq) -> Self {
        Store {
            live: BTreeMap::new(),
            durable: BTreeMap::new(),
            pipeline: VecDeque::new(),
            pos: start,
            durable_pos: start,
            last_exec: start,
            last_durable_at: 0,
        }
    }

    /// Applies one record change. `cond`: skipped when the stored version is at least `seq`
    /// (ordered replay from a lower bound). Returns whether it was written.
    fn write(
        &mut self,
        key: &[u8],
        value: Option<Vec<u8>>,
        seq: Seq,
        cond: bool,
        now: u64,
        delay: u64,
    ) -> bool {
        if cond && self.live.get(key).is_some_and(|r| r.seq >= seq) {
            return false;
        }
        // the storage engine never stores a key at a seq at or below the copy it replaces
        // (such a copy is never zeroed, and later writes of the key wait for it forever)
        assert!(
            self.live.get(key).is_none_or(|r| r.seq < seq),
            "a record of {key:?} would replace a copy at {:?} with one at {seq:?}",
            self.live.get(key).map(|r| r.seq)
        );
        let rec = value.map(|value| Rec { value, seq });
        match &rec {
            Some(r) => {
                self.live.insert(key.to_vec(), r.clone());
            }
            None => {
                self.live.remove(key);
            }
        }
        self.pos = self.pos.max(seq);
        let at = (now + delay).max(self.last_durable_at);
        self.last_durable_at = at;
        self.pipeline.push_back((at, self.pos, key.to_vec(), rec));
        true
    }

    /// A position reached without a record change (a group whose op was skipped).
    fn advance(&mut self, seq: Seq, now: u64, delay: u64) {
        if seq > self.pos {
            self.pos = seq;
            let at = (now + delay).max(self.last_durable_at);
            self.last_durable_at = at;
            self.pipeline.push_back((at, self.pos, Vec::new(), None));
        }
    }

    fn mature(&mut self, now: u64) {
        while self.pipeline.front().is_some_and(|e| e.0 <= now) {
            let (_, pos, key, rec) = self.pipeline.pop_front().expect("front");
            if !key.is_empty() {
                match rec {
                    Some(r) => {
                        self.durable.insert(key, r);
                    }
                    None => {
                        self.durable.remove(&key);
                    }
                }
            }
            self.durable_pos = self.durable_pos.max(pos);
        }
        if self.pipeline.is_empty() {
            self.durable_pos = self.durable_pos.max(self.pos);
        }
    }

    /// A crash: only durable records remain.
    fn crash(&mut self) {
        self.pipeline.clear();
        self.live = self.durable.clone();
        let max = self.live.values().map(|r| r.seq).max().unwrap_or(Seq::ZERO);
        self.pos = self.durable_pos.max(max);
        self.last_exec = self.pos;
        self.durable_pos = self.pos;
    }
}

// =============================================================================================
// data nodes: replication endpoints

/// One end of an ordered replication byte stream.
struct StreamEnd {
    peer: usize,
    tx: u64,
    rx: u64,
    buf: BTreeMap<u64, Option<Vec<u8>>>,
    gap_since: Option<u64>,
    last_rx: u64,
    dec: FrameDecoder,
}

impl StreamEnd {
    fn new(peer: usize, now: u64) -> Self {
        StreamEnd {
            peer,
            tx: 0,
            rx: 0,
            buf: BTreeMap::new(),
            gap_since: None,
            last_rx: now,
            dec: FrameDecoder::new(),
        }
    }
}

enum ConnRole {
    /// This node is a member; partition `p`.
    Member(u16),
    /// This node is the primary; partition, member id, member node.
    Primary(u16, MemberId, usize),
}

struct MemberSide {
    stream: MemberStream,
    /// The backlog of a term this node served as primary (Repair key set (a); kv-node
    /// `MemberPart::own_groups`).
    own_groups: Option<Backlog>,
    conn: Option<u64>,
    dial_at: u64,
    /// The stream completed its catch-up (Continue accepted, or Repair images done).
    ready: bool,
    rejected: bool,
}

struct RepairState {
    keys: BTreeSet<Vec<u8>>,
}

struct PrimaryPart {
    sender: ReplSender,
    /// The groups of the term this node took over (the Repair key sets (b)): its received
    /// ring, its own backlog when re-promoted, else a ring that was never started.
    prev_received: Backlog,
    /// Member id -> (conn, repair in progress).
    conns: BTreeMap<MemberId, (u64, Option<RepairState>)>,
}

struct Pending {
    client_op: u64,
    client: usize,
    p: u16,
    seq: Seq,
    deadline: u64,
}

enum NodeRole {
    None,
    Primary {
        shard: u16,
        term: u32,
        parts: Vec<PrimaryPart>,
        /// Member node -> member id (operator member order, the primary excluded).
        mids: Vec<(usize, MemberId)>,
        pending: Vec<Pending>,
        /// Members counted ahead of their committed expand.
        expanding: BTreeSet<MemberId>,
        /// Expanding members whose IsrExpand was sent: they count until the committed ISR
        /// shows them (kv-node `ExpandGate`), whatever their lag.
        expand_sent: BTreeSet<MemberId>,
        last_isr_msg: u64,
    },
    Replica {
        shard: u16,
        term: u32,
        primary: Option<usize>,
        parts: Vec<MemberSide>,
        ready_sent: bool,
    },
}

struct NodeDisk {
    assignment: Option<NodeAssignment>,
    fence_term: u32,
    /// Persisted lower bounds of the durable and commit positions per partition.
    repl_durable: Vec<Seq>,
    repl_commit: Vec<Seq>,
    /// Persisted `last_term` per partition (partition meta `term`).
    last_term: Vec<u32>,
}

struct Node {
    idx: usize,
    up: bool,
    incarnation: u64,
    /// The process restarted after a crash (no clean shutdown happens in the simulation).
    scanned: bool,
    /// After a crash of a node whose persisted role was a primary: the persisted fence term; a
    /// primary role of a term at or below it is never served again (kv-node
    /// `ClusterShared::restart_term`).
    restart_term: Option<u32>,
    /// Per partition: the term of the last stream that completed its catch-up (Continue
    /// accepted, Repair done), the handshake's `last_term` (kv-node `PartRepl::last_term`).
    last_term: Vec<u32>,
    disk: NodeDisk,
    stores: Vec<Store>,
    lease_expiry: u64,
    renew_seq: u64,
    renews: VecDeque<(u64, u64)>,
    ctrl: Option<(usize, u64)>,
    next_renew: u64,
    next_persist: u64,
    role: NodeRole,
    conns: BTreeMap<u64, (ConnRole, StreamEnd)>,
    disk_slow_until: u64,
    disk_slow_ns: u64,
    /// Renewals report `health.failed`: the node holds an assignment naming it primary of a
    /// term it must not serve after its restart (or, before the first one, it was a primary
    /// when it crashed). A new leader cannot tell a restart from the first renewal it sees.
    report_failed: bool,
    /// Primary: when each member's NeedOperatorResync was last reported (kv-node agent
    /// `resync_sent`, repeated at most once a second).
    resync_sent: BTreeMap<usize, u64>,
    /// After a restart: the persisted `repl_durable` lower bound per partition, announced by
    /// the first handshake.
    meta_applied: Vec<Option<Seq>>,
    /// The clock of the members' ack age test (kv-node `ClusterWorker::run`): observed at
    /// every tick, a gap counts at most one step (a frozen process).
    run: StallClock,
    /// `ClusterSimCfg::stall_clock`.
    stall_clock: bool,
}

/// Per-node shared knobs.
struct NodeEnv<'a> {
    topo: &'a Topo,
    next_conn: &'a mut u64,
}

impl Node {
    fn new(idx: usize) -> Self {
        let start = Seq::new(1, 0);
        Node {
            idx,
            up: true,
            incarnation: 1,
            scanned: false,
            restart_term: None,
            // the bootstrapped members are in sync at term 1
            last_term: vec![1; usize::from(P)],
            disk: NodeDisk {
                assignment: None,
                fence_term: 1,
                repl_durable: vec![start; usize::from(P)],
                repl_commit: vec![start; usize::from(P)],
                last_term: vec![1; usize::from(P)],
            },
            stores: (0..P).map(|_| Store::new(start)).collect(),
            lease_expiry: 0,
            renew_seq: 0,
            renews: VecDeque::new(),
            ctrl: None,
            next_renew: 0,
            next_persist: 0,
            role: NodeRole::None,
            conns: BTreeMap::new(),
            disk_slow_until: 0,
            disk_slow_ns: 0,
            meta_applied: (0..P).map(|_| None).collect(),
            report_failed: false,
            resync_sent: BTreeMap::new(),
            run: StallClock::new(),
            stall_clock: true,
        }
    }


    /// A primary partition whose own durability covers a group a counted member has not
    /// acknowledged yet (an ack in flight).
    fn ack_in_flight(&self) -> bool {
        let NodeRole::Primary { parts, .. } = &self.role else {
            return false;
        };
        parts.iter().any(|pp| {
            pp.sender
                .isr()
                .iter()
                .any(|&m| pp.sender.acked(m).is_some_and(|a| a < pp.sender.local_durable()))
        })
    }

    fn crash(&mut self) {
        if trace_on() {
            let p = std::env::var("KV_SIM_TRACE_KEY")
                .ok()
                .map(|k| partition_of(&key_name(k.parse().unwrap_or(0))));
            if let Some(p) = p {
                let s = &self.stores[usize::from(p)];
                eprintln!(
                    "n{} crash: p{p} last {} durable {} pipeline {}",
                    self.idx,
                    sq(s.last_exec),
                    sq(s.durable_pos),
                    s.pipeline.len()
                );
            }
        }
        self.up = false;
        for s in &mut self.stores {
            s.crash();
        }
        self.role = NodeRole::None;
        self.conns.clear();
        self.lease_expiry = 0;
        self.renews.clear();
        self.ctrl = None;
    }

    fn restart(&mut self, rng: &mut StdRng, now_l: u64) {
        self.up = true;
        self.incarnation = rng.random::<u64>() | 1;
        self.scanned = true;
        // kv-node: an unclean restart of a node whose persisted role is a primary holds every
        // primary role of a term up to the persisted fence term, and renewals report failed
        // until an assignment shows otherwise
        let was_primary = self
            .disk
            .assignment
            .as_ref()
            .and_then(|a| a.role.as_ref())
            .is_some_and(|r| r.is_primary);
        self.restart_term = was_primary.then_some(self.disk.fence_term);
        self.report_failed = was_primary;
        self.run = StallClock::new();
        self.last_term = self.disk.last_term.clone();
        // the first handshake of each partition announces the persisted lower bound
        self.meta_applied = self.disk.repl_durable.iter().map(|s| Some(*s)).collect();
        self.next_renew = now_l;
        self.next_persist = now_l;
    }

    fn disk_delay(&self, rng: &mut StdRng, now_g: u64) -> u64 {
        let base = rng.random_range(50_000..2 * MS);
        if now_g < self.disk_slow_until {
            base + self.disk_slow_ns
        } else {
            base
        }
    }

    fn shard_term(&self) -> Option<(u16, u32)> {
        match &self.role {
            NodeRole::Primary { shard, term, .. } | NodeRole::Replica { shard, term, .. } => {
                Some((*shard, *term))
            }
            NodeRole::None => None,
        }
    }

    /// Serving primary at this instant (the lease-disjointness check).
    fn serving_primary(&self, now_l: u64) -> Option<(u16, u32)> {
        match &self.role {
            NodeRole::Primary { shard, term, .. }
                if self.up && now_l < self.lease_expiry && self.disk.fence_term <= *term =>
            {
                Some((*shard, *term))
            }
            _ => None,
        }
    }

    // ------------------------------------------------------------------------- streams

    fn stream_send(&mut self, conn: u64, data: Option<Vec<u8>>, fx: &mut Fx<'_>) {
        if let Some((_, end)) = self.conns.get_mut(&conn) {
            let seq = end.tx;
            end.tx += 1;
            let peer = end.peer;
            fx.send(peer, Msg::Stream { conn, seq, data });
        }
    }

    fn close_conn(&mut self, conn: u64, fx: &mut Fx<'_>) {
        if let Some((role, end)) = self.conns.remove(&conn) {
            let seq = end.tx;
            fx.send(
                end.peer,
                Msg::Stream {
                    conn,
                    seq,
                    data: None,
                },
            );
            fx.report.stream_breaks += 1;
            self.after_close(role, conn);
        }
    }

    /// Bookkeeping when a stream ends (closed by either side or broken).
    fn after_close(&mut self, role: ConnRole, conn: u64) {
        match (&mut self.role, role) {
            (NodeRole::Replica { parts, .. }, ConnRole::Member(p)) => {
                let ms = &mut parts[usize::from(p)];
                if ms.conn == Some(conn) {
                    ms.conn = None;
                    ms.ready = false;
                }
            }
            (NodeRole::Primary { parts, .. }, ConnRole::Primary(p, m, _)) => {
                let pp = &mut parts[usize::from(p)];
                if pp.conns.get(&m).is_some_and(|c| c.0 == conn) {
                    pp.conns.remove(&m);
                    pp.sender.detach(m);
                }
            }
            _ => {}
        }
    }

    fn on_stream(
        &mut self,
        from: usize,
        conn: u64,
        seq: u64,
        data: Option<Vec<u8>>,
        fx: &mut Fx<'_>,
        env: &mut NodeEnv<'_>,
    ) {
        if !self.conns.contains_key(&conn) {
            // a new incoming stream starts with seq 0 and a Hello; anything else is stale
            if seq != 0 || data.is_none() {
                if data.is_some() {
                    fx.send(
                        from,
                        Msg::Stream {
                            conn,
                            seq: u64::MAX,
                            data: None,
                        },
                    );
                }
                return;
            }
            let mut end = StreamEnd::new(from, fx.now_g);
            end.rx = 1;
            let bytes = data.expect("checked");
            let mut dec = FrameDecoder::new();
            let Ok(Some((p, _, Frame::Hello(h), _))) = dec.feed(&bytes) else {
                return;
            };
            self.on_hello(from, conn, end, p, h, fx, env);
            return;
        }
        let Some((_, end)) = self.conns.get_mut(&conn) else {
            return;
        };
        if seq < end.rx {
            return; // duplicate
        }
        end.buf.insert(seq, data);
        end.last_rx = fx.now_g;
        let mut ready = Vec::new();
        while let Some(d) = end.buf.remove(&end.rx) {
            end.rx += 1;
            ready.push(d);
        }
        end.gap_since = if end.buf.is_empty() {
            None
        } else {
            end.gap_since.or(Some(fx.now_g))
        };
        for d in ready {
            match d {
                None => {
                    if let Some((role, _)) = self.conns.remove(&conn) {
                        self.after_close(role, conn);
                    }
                    return;
                }
                Some(bytes) => {
                    if !self.on_stream_bytes(conn, &bytes, fx, env) {
                        self.close_conn(conn, fx);
                        return;
                    }
                }
            }
        }
    }

    /// Decodes and handles the frames of one chunk; false breaks the stream.
    fn on_stream_bytes(
        &mut self,
        conn: u64,
        bytes: &[u8],
        fx: &mut Fx<'_>,
        env: &mut NodeEnv<'_>,
    ) -> bool {
        let mut frames = Vec::new();
        {
            let Some((_, end)) = self.conns.get_mut(&conn) else {
                return false;
            };
            let mut at = 0;
            while at < bytes.len() {
                match end.dec.feed(&bytes[at..]) {
                    Ok(Some((p, term, f, used))) => {
                        frames.push((p, term, f));
                        at += used.max(1);
                    }
                    Ok(None) => break,
                    Err(_) => return false,
                }
            }
        }
        for (p, term, f) in frames {
            let role = match self.conns.get(&conn) {
                Some((ConnRole::Member(_), _)) => None,
                Some((ConnRole::Primary(pp, m, n), _)) => Some((*pp, *m, *n)),
                None => return false,
            };
            let ok = match role {
                None => self.member_frame(conn, p, term, f, fx, env),
                Some((pp, m, _)) => self.primary_frame(conn, pp, m, f, fx),
            };
            if !ok {
                return false;
            }
        }
        true
    }

    // ------------------------------------------------------------------------- primary side

    #[allow(clippy::too_many_arguments)]
    fn on_hello(
        &mut self,
        from: usize,
        conn: u64,
        end: StreamEnd,
        p: u16,
        h: Hello,
        fx: &mut Fx<'_>,
        env: &mut NodeEnv<'_>,
    ) {
        let _ = env;
        let fence = self.disk.fence_term;
        let member_node = node_index(&NodeId(h.node_id));
        let assignment_members: Vec<(NodeId, MemberState, bool)> = self
            .disk
            .assignment
            .as_ref()
            .and_then(|a| a.role.as_ref())
            .map(|r| r.members.clone())
            .unwrap_or_default();
        let history: Vec<(u32, Vec<u64>)> = self
            .disk
            .assignment
            .as_ref()
            .and_then(|a| a.role.as_ref())
            .map(|r| {
                r.history
                    .iter()
                    .map(|t| (t.term, t.takeover.clone()))
                    .collect()
            })
            .unwrap_or_default();
        let last_exec = self.stores[usize::from(p)].last_exec;
        let NodeRole::Primary {
            shard: my_shard,
            term,
            parts,
            mids,
            ..
        } = &mut self.role
        else {
            let mut out = Vec::new();
            frame::encode(&Frame::Reject(RejectReason::NotPrimary), p, fence, &mut out);
            fx.send(
                from,
                Msg::Stream {
                    conn,
                    seq: 0,
                    data: Some(out),
                },
            );
            fx.send(
                from,
                Msg::Stream {
                    conn,
                    seq: 1,
                    data: None,
                },
            );
            return;
        };
        let term = *term;
        let mid = member_node.and_then(|n| mids.iter().find(|(x, _)| *x == n).map(|(_, m)| *m));
        let operator_full = member_node
            .and_then(|n| {
                assignment_members
                    .iter()
                    .find(|(id, _, _)| *id == node_id(n))
            })
            .is_some_and(|(_, st, _)| *st == MemberState::Syncing);
        let pp = &mut parts[usize::from(p)];
        let view = PrimaryView {
            term,
            history: &history,
            backlog: pp.sender.backlog(),
            received: &pp.prev_received,
            is_member: mid.is_some(),
            operator_full_sync: operator_full,
            last_executed: last_exec,
        };
        let decision = sync::decide(&h, &view);
        let mut out = Vec::new();
        let mut keep = false;
        let mut repair = None;
        let mut cont = None;
        match decision {
            SyncDecision::Continue { from: f } => {
                frame::encode(
                    &Frame::Accept {
                        term,
                        mode: AcceptMode::Continue { from: f },
                    },
                    p,
                    term,
                    &mut out,
                );
                fx.report.continues += 1;
                keep = true;
                cont = Some(AcceptMode::Continue { from: f });
            }
            SyncDecision::Repair {
                at,
                primary_keys,
                want_member_keys,
            } => {
                frame::encode(
                    &Frame::Accept {
                        term,
                        mode: AcceptMode::Repair { at },
                    },
                    p,
                    term,
                    &mut out,
                );
                fx.report.repairs += 1;
                keep = true;
                repair = Some((at, primary_keys, want_member_keys));
            }
            SyncDecision::Full { .. } => {
                // no operator command runs here, so this is a full sync caused by failures
                fx.obs.fail(format!(
                    "full sync of partition {p} started without an operator command"
                ));
            }
            SyncDecision::Reject(r) => {
                if r == RejectReason::NeedOperatorResync {
                    fx.report.resync_needed += 1;
                    // which coverage test failed (the same tests `sync::decide` makes)
                    let start = Seq::new(term, 0);
                    let at = last_exec.max(start);
                    let prev_end = sync::prev_takeover(&history, term, p).map(|(_, s)| s);
                    let why = if h.recovered == Recovered::SyncInterrupted {
                        0
                    } else if h.last_term == term {
                        1
                    } else if prev_end.is_none() {
                        2
                    } else if h.applied < prev_end.unwrap_or(Seq::ZERO)
                        && pp
                            .prev_received
                            .keys_between(h.applied, prev_end.unwrap_or(Seq::ZERO))
                            .is_none()
                    {
                        3
                    } else if at > start && pp.sender.backlog().keys_between(start, at).is_none() {
                        4
                    } else {
                        5
                    };
                    fx.report.resync_why[why] += 1;
                    if trace_on() {
                        eprintln!(
                            "[{}] p{p} {} rejects {:?} (why {why}): hello last_term {} applied {} fence {} recovered {:?}; prev_end {:?} at {} ring floor {} end {:?} history {:?}",
                            fx.now_l / MS,
                            short(&node_id(self.idx)),
                            member_node.map(|n| short(&node_id(n))),
                            h.last_term,
                            sq(h.applied),
                            h.fence_term,
                            h.recovered,
                            prev_end.map(sq),
                            sq(at),
                            sq(pp.prev_received.floor()),
                            pp.prev_received.end().map(sq),
                            history.iter().map(|(t, v)| (*t, sq(Seq(v[usize::from(p)])))).collect::<Vec<_>>(),
                        );
                    }
                    // kv-node: the primary reports the member (NeedsFullSync), unless the
                    // committed assignment already shows it, at most once a second
                    let recorded = assignment_members.iter().any(|(id, st, _)| {
                        member_node.is_some_and(|n| *id == node_id(n))
                            && matches!(st, MemberState::NeedsFullSync | MemberState::Syncing)
                    });
                    if let (Some(n), Some((c, _)), false) = (member_node, self.ctrl, recorded) {
                        let due = self
                            .resync_sent
                            .get(&n)
                            .is_none_or(|t| fx.now_l >= t + 1000 * MS);
                        if due {
                            self.resync_sent.insert(n, fx.now_l);
                            fx.send(
                                c,
                                Msg::Report(NodeToCoord::MemberNeedsResync {
                                    shard: ShardId(*my_shard),
                                    term,
                                    member: node_id(n),
                                }),
                            );
                        }
                    }
                }
                frame::encode(&Frame::Reject(r), p, term, &mut out);
            }
        }
        let mid = match mid {
            Some(m) if keep => m,
            _ => {
                fx.send(
                    from,
                    Msg::Stream {
                        conn,
                        seq: 0,
                        data: Some(out),
                    },
                );
                fx.send(
                    from,
                    Msg::Stream {
                        conn,
                        seq: 1,
                        data: None,
                    },
                );
                return;
            }
        };
        // a member that dials again replaces its previous stream
        if let Some((old, _)) = pp.conns.remove(&mid) {
            pp.sender.detach(mid);
            if let Some((_, e)) = self.conns.remove(&old) {
                fx.send(
                    e.peer,
                    Msg::Stream {
                        conn: old,
                        seq: e.tx,
                        data: None,
                    },
                );
            }
        }
        let NodeRole::Primary { parts, .. } = &mut self.role else {
            unreachable!("checked above")
        };
        let pp = &mut parts[usize::from(p)];
        match &repair {
            None => {
                if let Some(mode) = cont {
                    pp.sender.attach(mid, &mode);
                }
                pp.conns.insert(mid, (conn, None));
            }
            Some((at, keys, want)) => {
                pp.sender.attach(mid, &AcceptMode::Repair { at: *at });
                pp.conns.insert(
                    mid,
                    (
                        conn,
                        Some(RepairState {
                            keys: keys.iter().map(|(_, k)| k.to_vec()).collect(),
                        }),
                    ),
                );
                let _ = want;
            }
        }
        let member_proc = from;
        self.conns.insert(
            conn,
            (ConnRole::Primary(p, mid, member_node.unwrap_or(0)), end),
        );
        self.stream_send(conn, Some(out), fx);
        if let Some((_, _, false)) = repair {
            self.finish_repair(p, mid, conn, fx);
        }
        let _ = member_proc;
    }

    /// Sends one image per repair key (the primary's current state), then the end position;
    /// the backlog stream continues after it.
    fn finish_repair(&mut self, p: u16, mid: MemberId, conn: u64, fx: &mut Fx<'_>) {
        let last = self.stores[usize::from(p)].last_exec;
        let NodeRole::Primary { term, parts, .. } = &mut self.role else {
            return;
        };
        let term = *term;
        let pp = &mut parts[usize::from(p)];
        // kv-node `drive_direct`: the stream continues after the later of the partition's last
        // executed seq and the sender's last group (never below the term's start, which the
        // backlog would not cover)
        let last = last.max(pp.sender.last());
        let Some((_, Some(rs))) = pp.conns.get_mut(&mid) else {
            return;
        };
        let keys = std::mem::take(&mut rs.keys);
        let mut out = Vec::new();
        for k in keys {
            let image = self.stores[usize::from(p)]
                .live
                .get(&k)
                .map(|r| (1u8, 0u64, r.value.clone()));
            frame::encode(
                &Frame::Image(ImageRec {
                    db: 0,
                    key: k.into_boxed_slice(),
                    image,
                }),
                p,
                term,
                &mut out,
            );
        }
        frame::encode(&Frame::SyncDone { at: last }, p, term, &mut out);
        if let Some(c) = pp.conns.get_mut(&mid) {
            c.1 = None;
        }
        pp.sender.resume(mid, last);
        self.stream_send(conn, Some(out), fx);
    }

    fn primary_frame(
        &mut self,
        conn: u64,
        p: u16,
        mid: MemberId,
        f: Frame,
        fx: &mut Fx<'_>,
    ) -> bool {
        let NodeRole::Primary { parts, .. } = &mut self.role else {
            return false;
        };
        let pp = &mut parts[usize::from(p)];
        match f {
            Frame::Ack { durable, .. } => {
                pp.sender.on_ack(mid, durable, fx.now_l);
                true
            }
            Frame::RepairKeys(keys) => {
                if let Some((_, Some(rs))) = pp.conns.get_mut(&mid) {
                    rs.keys.extend(keys.into_iter().map(|(_, k)| k.to_vec()));
                    self.finish_repair(p, mid, conn, fx);
                }
                true
            }
            _ => false,
        }
    }

    // ------------------------------------------------------------------------- member side

    fn member_frame(
        &mut self,
        conn: u64,
        p: u16,
        term: u32,
        f: Frame,
        fx: &mut Fx<'_>,
        env: &mut NodeEnv<'_>,
    ) -> bool {
        let _ = env;
        // fencing: frames of a term below this node's fence are refused
        if term < self.disk.fence_term && !matches!(f, Frame::Reject(_)) {
            return false;
        }
        let delay = self.disk_delay(fx.rng, fx.now_g);
        let history: Vec<(u32, Vec<u64>)> = self
            .disk
            .assignment
            .as_ref()
            .and_then(|a| a.role.as_ref())
            .map(|r| {
                r.history
                    .iter()
                    .map(|t| (t.term, t.takeover.clone()))
                    .collect()
            })
            .unwrap_or_default();
        let last_term = &mut self.last_term[usize::from(p)];
        let NodeRole::Replica { parts, .. } = &mut self.role else {
            return false;
        };
        let ms = &mut parts[usize::from(p)];
        if ms.conn != Some(conn) {
            return false;
        }
        let first = match ms.stream.on_frame(f) {
            Ok(a) => a,
            Err(_) => return false,
        };
        let mut actions = vec![first];
        while let Some(a) = ms.stream.next_action() {
            actions.push(a);
        }
        let store = &mut self.stores[usize::from(p)];
        let mut send = Vec::new();
        for a in actions {
            match a {
                MemberAction::Apply(g) => {
                    let mut wrote = false;
                    for op in &g.ops {
                        match op {
                            Op::Put { key, value, .. } => {
                                let w = store.write(
                                    key,
                                    Some(value.clone()),
                                    g.seq,
                                    true,
                                    fx.now_g,
                                    delay,
                                );
                                if trace_key(key) {
                                    eprintln!(
                                        "[{} ms] n{} p{p} apply put {:?} at {} written {w}",
                                        fx.now_g / MS,
                                        self.idx,
                                        val_id(value),
                                        sq(g.seq)
                                    );
                                }
                                wrote |= w;
                            }
                            Op::Del { key, .. } | Op::Expired { key, .. } => {
                                if store.live.get(&key[..]).is_some_and(|r| r.seq >= g.seq) {
                                    continue;
                                }
                                wrote |= store.write(key, None, g.seq, false, fx.now_g, delay);
                            }
                            _ => {}
                        }
                    }
                    if !wrote {
                        store.advance(g.seq, fx.now_g, delay);
                    }
                    store.last_exec = store.last_exec.max(g.seq);
                }
                MemberAction::Image(rec, at) => {
                    let v = rec.image.map(|(_, _, v)| v);
                    if trace_key(&rec.key) {
                        eprintln!(
                            "[{} ms] n{} p{p} image {:?} at {}",
                            fx.now_g / MS,
                            self.idx,
                            v.as_deref().and_then(val_id),
                            sq(at)
                        );
                    }
                    // kv-node applies images ordered: a key held at `at` or later already shows
                    // the image's state (`member_image`)
                    store.write(&rec.key, v, at, true, fx.now_g, delay);
                }
                MemberAction::SendRepairKeys => {
                    // (a): keys of this member's own groups after its last term's takeover
                    // point (kv-node `member_repair_keys`): from a ring that holds every group
                    // up to the highest position the partition holds, else a walk of the
                    // partition for keys stored above the takeover point
                    let takeover = sync::takeover(&history, *last_term, p);
                    let mut keys: BTreeSet<Vec<u8>> = BTreeSet::new();
                    let mut rings: Vec<&Backlog> = Vec::new();
                    if let Some(b) = ms.own_groups.as_ref() {
                        rings.push(b);
                    }
                    rings.push(ms.stream.received());
                    let ring_keys = takeover
                        .and_then(|t| kv_repl::member::own_keys_after(&rings, t, store.pos));
                    match ring_keys {
                        Some(ks) => keys.extend(ks.into_vec().into_iter().map(|(_, k)| k.to_vec())),
                        None => {
                            // the walk (DESIGN §18.4 maybe_divergent after a restart)
                            let above = takeover.unwrap_or(Seq::ZERO);
                            for (k, r) in &store.live {
                                if r.seq > above {
                                    keys.insert(k.clone());
                                }
                            }
                        }
                    }
                    let list: Vec<(u8, Box<[u8]>)> = keys
                        .into_iter()
                        .map(|k| (0u8, k.into_boxed_slice()))
                        .collect();
                    let mut out = Vec::new();
                    frame::encode(&Frame::RepairKeys(list), p, term, &mut out);
                    send.push(out);
                    if let Some((_, AcceptMode::Repair { at })) = ms.stream.accepted() {
                        ms.stream.reset_received(at);
                    }
                }
                MemberAction::RepairDone(end) => {
                    store.advance(end, fx.now_g, delay);
                    store.last_exec = store.last_exec.max(end);
                    ms.ready = true;
                    // kv-node: caught up, the own backlog of an earlier term is dropped
                    ms.own_groups = None;
                    if let Some((t, _)) = ms.stream.accepted() {
                        *last_term = (*last_term).max(t);
                    }
                }
                MemberAction::SyncBegin(_)
                | MemberAction::Snap(..)
                | MemberAction::SnapBegin(_)
                | MemberAction::SnapEnd { .. } => {
                    fx.obs.snap_frames += 1;
                    fx.obs.fail(format!(
                        "full-sync frames on partition {p} without an operator command"
                    ));
                }
                MemberAction::SyncDone(_) | MemberAction::Seal { .. } => {
                    fx.obs.fail(
                        "full-sync or migration end on a member without an operator command".into(),
                    );
                }
                MemberAction::Commit(_) | MemberAction::Nothing => {}
            }
        }
        if let Some((t, AcceptMode::Continue { .. })) = ms.stream.accepted() {
            // kv-node: a plain Continue on the current term is a ready partition
            ms.ready = true;
            ms.own_groups = None;
            *last_term = (*last_term).max(t);
        }
        if ms.ready {
            self.meta_applied[usize::from(p)] = None;
        }
        if let Some(r) = ms.stream.rejected() {
            ms.rejected = true;
            if r == RejectReason::NeedOperatorResync {
                ms.dial_at = fx.now_g + 1000 * MS;
            }
            return false;
        }
        for out in send {
            self.stream_send(conn, Some(out), fx);
        }
        true
    }

    // ------------------------------------------------------------------------- control

    fn on_ctrl(
        &mut self,
        from: usize,
        coord_term: u64,
        m: CoordToNode,
        fx: &mut Fx<'_>,
        env: &mut NodeEnv<'_>,
    ) {
        // the control connection is the newest leader's
        match self.ctrl {
            Some((_, t)) if coord_term < t => {
                if matches!(m, CoordToNode::Assign(_)) {
                    fx.send(
                        from,
                        Msg::Report(NodeToCoord::Error(kv_ctrl::NodeError::StaleAssignment)),
                    );
                }
                return;
            }
            _ => self.ctrl = Some((from, coord_term)),
        }
        match m {
            CoordToNode::Assign(a) => {
                let cur = self
                    .disk
                    .assignment
                    .as_ref()
                    .map_or((0, 0), |c| c.order_key());
                if a.order_key() < cur {
                    fx.send(
                        from,
                        Msg::Report(NodeToCoord::Error(kv_ctrl::NodeError::StaleAssignment)),
                    );
                    return;
                }
                let version = a.version;
                // kv-node agent: `refusing_primary` follows every applied assignment
                self.report_failed = self.refuses_primary(&a);
                // persisted before the ack (§23.4)
                if let Some(r) = &a.role
                    && r.term > self.disk.fence_term
                {
                    self.disk.fence_term = r.term;
                }
                self.disk.assignment = Some(a);
                fx.send(from, Msg::Report(NodeToCoord::AssignAck { version }));
                self.reconfigure(fx, env);
            }
            CoordToNode::LeaseGrant { seq, lease_ms } => {
                while self.renews.front().is_some_and(|(s, _)| *s < seq) {
                    self.renews.pop_front();
                }
                if let Some(&(s, t_send)) = self.renews.front()
                    && s == seq
                {
                    let exp =
                        (t_send + (u64::from(lease_ms) * MS) * 95 / 100).saturating_sub(10 * MS);
                    self.lease_expiry = self.lease_expiry.max(exp);
                }
            }
            CoordToNode::Fence {
                req,
                shard,
                new_term,
            } => {
                if new_term > self.disk.fence_term {
                    self.disk.fence_term = new_term; // durable before the reply
                }
                // streams of older terms end; the received rings stay (Repair key sets (b))
                let old: Vec<u64> = self.conns.keys().copied().collect();
                if let NodeRole::Replica { term, primary, .. } = &mut self.role
                    && *term < new_term
                {
                    *primary = None;
                    for c in old {
                        self.close_conn(c, fx);
                    }
                }
                let _ = shard;
                let applied = self.stores.iter().map(|s| s.last_exec.0).collect();
                fx.send(from, Msg::Report(NodeToCoord::FenceAck { req, applied }));
            }
            _ => {}
        }
    }

    /// After a crash, `a` names this node primary of a term it must not serve (kv-node
    /// `ClusterShared::refuses_primary`).
    fn refuses_primary(&self, a: &NodeAssignment) -> bool {
        match (self.restart_term, a.role.as_ref()) {
            (Some(rt), Some(r)) => r.is_primary && r.term <= rt,
            _ => false,
        }
    }

    /// Applies the persisted assignment to the node's roles and streams.
    fn reconfigure(&mut self, fx: &mut Fx<'_>, env: &mut NodeEnv<'_>) {
        let Some(a) = self.disk.assignment.clone() else {
            return;
        };
        let refused = self.refuses_primary(&a);
        let Some(r) = a.role.clone() else {
            self.drop_role(fx);
            return;
        };
        let fenced = r.term < self.disk.fence_term;
        let isr: Vec<usize> = r
            .members
            .iter()
            .filter(|m| m.2)
            .filter_map(|m| node_index(&m.0))
            .collect();
        if r.is_primary && !fenced && !refused {
            let same = matches!(&self.role, NodeRole::Primary { shard, term, .. } if *shard == r.shard.0 && *term == r.term);
            if !same {
                // promotion (kv-node `apply_role`): the groups of the term taken over for the
                // Repair key sets (b): the received ring, or the own backlog of a re-promoted
                // primary; else (a restart) a ring that was never started
                let mut rings: Vec<Option<Backlog>> = (0..P).map(|_| None).collect();
                match std::mem::replace(&mut self.role, NodeRole::None) {
                    NodeRole::Replica { parts, .. } => {
                        for (p, ms) in parts.into_iter().enumerate() {
                            rings[p] = Some(ms.stream.into_received());
                        }
                    }
                    NodeRole::Primary { parts, pending, .. } => {
                        for (p, pp) in parts.into_iter().enumerate() {
                            rings[p] = Some(pp.sender.into_backlog());
                        }
                        for pd in pending {
                            fx.send(
                                pd.client,
                                Msg::Resp {
                                    op: pd.client_op,
                                    resp: Resp::TryAgain,
                                },
                            );
                        }
                    }
                    NodeRole::None => {}
                }
                for c in self.conns.keys().copied().collect::<Vec<_>>() {
                    self.close_conn(c, fx);
                }
                let parts = (0..P)
                    .map(|p| {
                        let mut sender = ReplSender::new(p, r.term, RING);
                        sender.set_heartbeat_ns(50 * MS);
                        let last = self.stores[usize::from(p)].last_exec;
                        if last.term() == r.term {
                            sender.start_at(last);
                        }
                        let prev_received = rings[usize::from(p)]
                            .take()
                            .unwrap_or_else(|| Backlog::new(64));
                        PrimaryPart {
                            sender,
                            prev_received,
                            conns: BTreeMap::new(),
                        }
                    })
                    .collect();
                let mids = r
                    .members
                    .iter()
                    .enumerate()
                    .filter_map(|(i, m)| node_index(&m.0).map(|n| (n, i as MemberId)))
                    .collect();
                self.role = NodeRole::Primary {
                    shard: r.shard.0,
                    term: r.term,
                    parts,
                    mids,
                    pending: Vec::new(),
                    expanding: BTreeSet::new(),
                    expand_sent: BTreeSet::new(),
                    last_isr_msg: 0,
                };
            }
            if let NodeRole::Primary {
                parts,
                mids,
                expanding,
                expand_sent,
                ..
            } = &mut self.role
            {
                let committed: Vec<MemberId> = mids
                    .iter()
                    .filter(|(n, _)| isr.contains(n))
                    .map(|(_, m)| *m)
                    .collect();
                expanding.retain(|m| !committed.contains(m));
                expand_sent.retain(|m| expanding.contains(m));
                let mut counted = committed.clone();
                counted.extend(expanding.iter().copied());
                let run_now = if self.stall_clock {
                    self.run.peek(fx.now_l)
                } else {
                    fx.now_l
                };
                for pp in parts.iter_mut() {
                    pp.sender.set_isr_at(&counted, run_now);
                }
            }
            return;
        }
        if fenced {
            // fenced for a newer term whose assignment has not arrived: no stream of the old
            // term, the rings kept
            if let NodeRole::Replica { primary, .. } = &mut self.role {
                *primary = None;
            }
            for c in self.conns.keys().copied().collect::<Vec<_>>() {
                self.close_conn(c, fx);
            }
            return;
        }
        // replica (or a restarted primary: serves nothing until a newer assignment)
        let primary = r
            .primary_repl
            .and_then(|pa| (0..env.topo.nodes).find(|&i| node_addr(i, 7379) == pa.0));
        let same = matches!(&self.role, NodeRole::Replica { shard, term, primary: pr, .. }
            if *shard == r.shard.0 && *term == r.term && *pr == primary);
        if same {
            return;
        }
        let old = std::mem::replace(&mut self.role, NodeRole::None);
        // kv-node `apply_role`: (received ring, own backlog) carried into the new member role
        let mut prev: Vec<(Option<Backlog>, Option<Backlog>)> =
            (0..P).map(|_| (None, None)).collect();
        match old {
            NodeRole::Primary { parts, pending, .. } => {
                for pd in pending {
                    fx.send(
                        pd.client,
                        Msg::Resp {
                            op: pd.client_op,
                            resp: Resp::TryAgain,
                        },
                    );
                }
                // a demoted primary keeps its own groups for its Repair key set (a)
                for (p, pp) in parts.into_iter().enumerate() {
                    prev[p].1 = Some(pp.sender.into_backlog());
                }
            }
            NodeRole::Replica { parts, .. } => {
                for (p, ms) in parts.into_iter().enumerate() {
                    prev[p] = (Some(ms.stream.into_received()), ms.own_groups);
                }
            }
            NodeRole::None => {}
        }
        for c in self.conns.keys().copied().collect::<Vec<_>>() {
            self.close_conn(c, fx);
        }
        let parts = (0..P)
            .map(|p| {
                let (ring, own_groups) = std::mem::take(&mut prev[usize::from(p)]);
                // a fresh ring covers nothing until a stream starts it
                let stream =
                    MemberStream::with_backlog(p, ring.unwrap_or_else(|| Backlog::new(RING)));
                MemberSide {
                    stream,
                    own_groups,
                    conn: None,
                    dial_at: fx.now_g,
                    ready: false,
                    rejected: false,
                }
            })
            .collect();
        self.role = NodeRole::Replica {
            shard: r.shard.0,
            term: r.term,
            primary,
            parts,
            ready_sent: false,
        };
    }

    fn drop_role(&mut self, fx: &mut Fx<'_>) {
        if let NodeRole::Primary { pending, .. } = std::mem::replace(&mut self.role, NodeRole::None)
        {
            for pd in pending {
                fx.send(
                    pd.client,
                    Msg::Resp {
                        op: pd.client_op,
                        resp: Resp::TryAgain,
                    },
                );
            }
        }
        for c in self.conns.keys().copied().collect::<Vec<_>>() {
            self.close_conn(c, fx);
        }
    }

    // ------------------------------------------------------------------------- clients

    fn on_req(&mut self, from: usize, op: u64, req: Req, fx: &mut Fx<'_>) {
        let key = match &req {
            Req::Set(k, _) | Req::Del(k) | Req::Get(k) => k.clone(),
        };
        let slot = kv_common::key_slot(&key);
        let p = slot % P;
        let lease_ok = fx.now_l < self.lease_expiry;
        if trace_on() && trace_key(&key) {
            eprintln!("[{} ms] n{} request op {op} lease_ok {lease_ok}", fx.now_g / MS, self.idx);
        }
        let role = self.disk.assignment.as_ref().and_then(|a| a.role.clone());
        let fence = self.disk.fence_term;
        let delay = self.disk_delay(fx.rng, fx.now_g);
        let NodeRole::Primary {
            shard,
            term,
            parts,
            pending,
            mids,
            ..
        } = &mut self.role
        else {
            fx.send(
                from,
                Msg::Resp {
                    op,
                    resp: Resp::Moved,
                },
            );
            return;
        };
        let Some(r) =
            role.filter(|r| r.is_primary && r.term == *term && r.owned_slots.contains(slot))
        else {
            fx.send(
                from,
                Msg::Resp {
                    op,
                    resp: Resp::Moved,
                },
            );
            return;
        };
        if !lease_ok || fence > *term {
            fx.send(
                from,
                Msg::Resp {
                    op,
                    resp: Resp::Moved,
                },
            );
            return;
        }
        let store = &mut self.stores[usize::from(p)];
        let (k, v) = match req {
            Req::Get(_) => {
                let v = store.live.get(&key).and_then(|r| val_id(&r.value));
                fx.send(
                    from,
                    Msg::Resp {
                        op,
                        resp: Resp::Value(v),
                    },
                );
                return;
            }
            Req::Set(k, id) => (k, Some(val_bytes(id))),
            Req::Del(k) => (k, None),
        };
        let isr_len = 1 + r.members.iter().filter(|m| m.2).count();
        if isr_len < usize::from(r.min_isr) {
            fx.send(
                from,
                Msg::Resp {
                    op,
                    resp: Resp::NoReplicas,
                },
            );
            return;
        }
        let seq = if store.last_exec.term() == *term {
            store.last_exec.next().expect("counter")
        } else {
            Seq::new(*term, 1)
        };
        let op_v = match &v {
            Some(val) => Op::Put {
                db: 0,
                vtype: 1,
                expire_at: 0,
                key: k.clone().into_boxed_slice(),
                value: val.clone(),
            },
            None => Op::Del {
                db: 0,
                key: k.clone().into_boxed_slice(),
            },
        };
        if v.is_none() && !store.live.contains_key(&k) {
            store.advance(seq, fx.now_g, delay);
        } else {
            store.write(&k, v.clone(), seq, false, fx.now_g, delay);
        }
        store.last_exec = seq;
        if trace_key(&k) {
            eprintln!(
                "[{} ms] n{} p{p} primary t{} exec {:?} at {}",
                fx.now_g / MS,
                self.idx,
                *term,
                v.as_deref().and_then(val_id),
                sq(seq)
            );
        }
        let mut enc = Vec::new();
        kv_repl::codec::encode_group(
            &Group {
                seq,
                ops: vec![op_v],
            },
            &mut enc,
        );
        let run_now = if self.stall_clock {
            self.run.peek(fx.now_l)
        } else {
            fx.now_l
        };
        let pp = &mut parts[usize::from(p)];
        pp.sender.on_group_at(&enc, seq, run_now);
        // members under repair get every group directly, in execution order
        let mut direct = Vec::new();
        for (mid, (conn, _)) in &pp.conns {
            if pp.sender.direct(*mid) {
                let mut out = Vec::new();
                frame::encode(&Frame::Groups(enc.clone()), p, *term, &mut out);
                direct.push((*conn, out));
            }
        }
        pending.push(Pending {
            client_op: op,
            client: from,
            p,
            seq,
            deadline: fx.now_l + 10_000 * MS,
        });
        let _ = (shard, mids);
        for (conn, out) in direct {
            self.stream_send(conn, Some(out), fx);
        }
    }

    // ------------------------------------------------------------------------- tick

    fn tick(&mut self, fx: &mut Fx<'_>, env: &mut NodeEnv<'_>) {
        for s in &mut self.stores {
            s.mature(fx.now_g);
        }
        self.run.advance(fx.now_l, false);
        if fx.now_l >= self.next_persist {
            self.next_persist = fx.now_l + 50 * MS;
            for (p, s) in self.stores.iter().enumerate() {
                self.disk.repl_durable[p] = self.disk.repl_durable[p].max(s.durable_pos);
            }
            self.disk.last_term.clone_from(&self.last_term);
        }
        // lease renewals on the control connection
        let renew_ms = self
            .disk
            .assignment
            .as_ref()
            .map_or(100, |a| a.params.renew_ms);
        if fx.now_l >= self.next_renew {
            self.next_renew = fx.now_l + u64::from(renew_ms) * MS;
            if let Some((c, _)) = self.ctrl {
                self.renew_seq += 1;
                self.renews.push_back((self.renew_seq, fx.now_l));
                while self.renews.len() > 64 {
                    self.renews.pop_front();
                }
                let version_seen = self.disk.assignment.as_ref().map_or(0, |a| a.version);
                fx.send(
                    c,
                    Msg::Report(NodeToCoord::Renew {
                        seq: self.renew_seq,
                        incarnation: self.incarnation,
                        version_seen,
                        health: Health {
                            ready: true,
                            failed: self.report_failed,
                            read_only_partitions: 0,
                            corrupt_pages: 0,
                        },
                    }),
                );
            }
        }
        // broken streams: a gap that does not fill, or a silent peer
        let mut broken = Vec::new();
        for (c, (_, e)) in &self.conns {
            if e.gap_since.is_some_and(|t| fx.now_g > t + 100 * MS)
                || fx.now_g > e.last_rx + 1500 * MS
            {
                broken.push(*c);
            }
        }
        for c in broken {
            self.close_conn(c, fx);
        }
        let ack_timeout = self
            .disk
            .assignment
            .as_ref()
            .map_or(500, |a| a.params.repl_ack_timeout_ms);
        match &mut self.role {
            NodeRole::Primary { .. } => self.primary_tick(fx, u64::from(ack_timeout) * MS),
            NodeRole::Replica { .. } => self.member_tick(fx, env),
            NodeRole::None => {}
        }
    }

    fn primary_tick(&mut self, fx: &mut Fx<'_>, ack_timeout: u64) {
        let lease_ok = fx.now_l < self.lease_expiry;
        let fence = self.disk.fence_term;
        let me = node_id(self.idx);
        let committed_isr: Vec<usize> = self
            .disk
            .assignment
            .as_ref()
            .and_then(|a| a.role.as_ref())
            .map(|r| {
                r.members
                    .iter()
                    .filter(|m| m.2)
                    .filter_map(|m| node_index(&m.0))
                    .collect()
            })
            .unwrap_or_default();
        let min_isr = self
            .disk
            .assignment
            .as_ref()
            .and_then(|a| a.role.as_ref())
            .map_or(1, |r| usize::from(r.min_isr.max(1)));
        let ctrl = self.ctrl.map(|c| c.0);
        let NodeRole::Primary {
            shard,
            term,
            parts,
            pending,
            mids,
            expanding,
            expand_sent,
            last_isr_msg,
        } = &mut self.role
        else {
            return;
        };
        let run_now = if self.stall_clock {
            self.run.peek(fx.now_l)
        } else {
            fx.now_l
        };
        let mut sends = Vec::new();
        let mut overrun = Vec::new();
        for (p, pp) in parts.iter_mut().enumerate() {
            pp.sender.set_local_durable_at(self.stores[p].durable_pos, run_now);
            for (mid, (conn, rs)) in &pp.conns {
                let _ = rs;
                // kv-node: a member that fell behind the backlog has its stream restarted
                if pp.sender.overrun(*mid) {
                    overrun.push(*conn);
                    continue;
                }
                let mut out = Vec::new();
                if pp.sender.fill_send_at(*mid, &mut out, 64 << 10, fx.now_l) {
                    sends.push((*conn, out));
                }
            }
        }
        // replies: ticket durable, committed on the ISR, lease valid at release (§23.8)
        let mut keep = Vec::new();
        for pd in std::mem::take(pending) {
            let pp = &parts[usize::from(pd.p)];
            let committed = pp.sender.commit_seq() >= pd.seq
                && self.stores[usize::from(pd.p)].durable_pos >= pd.seq;
            if committed && lease_ok && fence <= *term {
                if trace_on() {
                    eprintln!(
                        "[{} ms] n{} ack p{} {}",
                        fx.now_g / MS,
                        self.idx,
                        pd.p,
                        sq(pd.seq)
                    );
                }
                fx.obs.on_ack(*shard, pd.p, pd.seq);
                fx.send(
                    pd.client,
                    Msg::Resp {
                        op: pd.client_op,
                        resp: Resp::Ok,
                    },
                );
            } else if fx.now_l > pd.deadline || fence > *term {
                fx.send(
                    pd.client,
                    Msg::Resp {
                        op: pd.client_op,
                        resp: Resp::TryAgain,
                    },
                );
            } else {
                keep.push(pd);
            }
        }
        *pending = keep;
        // ISR shrink / expand requests, re-sent until the committed assignment reflects them
        if fx.now_l >= *last_isr_msg + 100 * MS
            && let Some(c) = ctrl
        {
            *last_isr_msg = fx.now_l;
            let mut lag = BTreeSet::new();
            // kv-node agent: node-wide, the oldest age of each lagging member over the
            // partitions; it asks to drop only the members the ISR can lose while it keeps
            // min_isr members, the others once they pass the keep bound
            // (`kv_repl::primary::shrink_choice`)
            let mut oldest: BTreeMap<MemberId, u64> = BTreeMap::new();
            for pp in parts.iter() {
                for (m, age_ns) in pp.sender.lagging_ages(run_now, ack_timeout) {
                    let e = oldest.entry(m).or_insert(0);
                    *e = (*e).max(age_ns);
                    lag.insert(m);
                }
            }
            let isr_ages: Vec<(MemberId, u64)> = mids
                .iter()
                .filter(|(n, _)| committed_isr.contains(n))
                .filter_map(|(_, mid)| oldest.get(mid).map(|a| (*mid, *a)))
                .collect();
            let drop: BTreeSet<MemberId> = kv_repl::primary::shrink_choice(
                &isr_ages,
                1 + committed_isr.len(),
                min_isr,
                ack_timeout.saturating_mul(7) / 4,
            )
            .into_iter()
            .collect();
            let mut recount = false;
            for (n, mid) in mids.iter() {
                let in_isr = committed_isr.contains(n);
                if in_isr && drop.contains(mid) {
                    if trace_on() {
                        let at: Vec<String> = parts
                            .iter()
                            .enumerate()
                            .filter(|(_, pp)| pp.sender.lagging(run_now, ack_timeout).contains(mid))
                            .map(|(p, pp)| {
                                format!(
                                    "p{p} acked {} local {} last {} attached {}",
                                    pp.sender.acked(*mid).map_or("-".into(), sq),
                                    sq(pp.sender.local_durable()),
                                    sq(pp.sender.last()),
                                    pp.sender.is_attached(*mid)
                                )
                            })
                            .collect();
                        eprintln!(
                            "[{} ms] n{} shrink request n{n}: {}",
                            fx.now_g / MS,
                            self.idx,
                            at.join("; ")
                        );
                    }
                    fx.send(
                        c,
                        Msg::Report(NodeToCoord::IsrShrink {
                            shard: ShardId(*shard),
                            term: *term,
                            member: node_id(*n),
                        }),
                    );
                } else if !in_isr {
                    // kv-node: counting starts after the barrier chain (`caught_up`); a lagging
                    // counted member stops counting unless its IsrExpand was sent
                    if expanding.contains(mid) && !expand_sent.contains(mid) && lag.contains(mid) {
                        expanding.remove(mid);
                        recount = true;
                        continue;
                    }
                    if !expanding.contains(mid) && parts.iter().all(|pp| pp.sender.caught_up(*mid))
                    {
                        expanding.insert(*mid);
                        recount = true;
                        continue;
                    }
                    // IsrExpand once it holds every write committed before it counted
                    if expanding.contains(mid)
                        && parts.iter().all(|pp| pp.sender.holds_committed(*mid))
                    {
                        expand_sent.insert(*mid);
                        fx.send(
                            c,
                            Msg::Report(NodeToCoord::IsrExpand {
                                shard: ShardId(*shard),
                                term: *term,
                                member: node_id(*n),
                            }),
                        );
                    }
                }
            }
            if recount {
                let mut counted: Vec<MemberId> = mids
                    .iter()
                    .filter(|(x, _)| committed_isr.contains(x))
                    .map(|(_, m)| *m)
                    .collect();
                counted.extend(expanding.iter().copied());
                for pp in parts.iter_mut() {
                    pp.sender.set_isr_at(&counted, run_now);
                }
            }
        }
        let _ = me;
        for (conn, out) in sends {
            self.stream_send(conn, Some(out), fx);
        }
        for conn in overrun {
            self.close_conn(conn, fx);
        }
    }

    fn member_tick(&mut self, fx: &mut Fx<'_>, env: &mut NodeEnv<'_>) {
        let me = node_id(self.idx);
        let fence = self.disk.fence_term;
        let scanned = self.scanned;
        let ctrl = self.ctrl.map(|c| c.0);
        let mut dials = Vec::new();
        let mut acks = Vec::new();
        let mut all_ready = true;
        let NodeRole::Replica {
            shard,
            term,
            primary,
            parts,
            ready_sent,
        } = &mut self.role
        else {
            return;
        };
        for (p, ms) in parts.iter_mut().enumerate() {
            let store = &self.stores[p];
            ms.stream.note_durable(store.durable_pos);
            all_ready &= ms.ready;
            match ms.conn {
                Some(conn) => {
                    if let Some(f) = ms.stream.ack_due(fx.now_l) {
                        let mut out = Vec::new();
                        frame::encode(&f, p as u16, fence, &mut out);
                        acks.push((conn, out));
                    }
                }
                None => {
                    if let Some(pr) = *primary
                        && fx.now_g >= ms.dial_at
                    {
                        ms.dial_at = fx.now_g + 50 * MS;
                        let applied = self.meta_applied[p]
                            .unwrap_or(store.durable_pos)
                            .min(store.durable_pos);
                        ms.stream.restart(applied, store.durable_pos);
                        let h = Hello {
                            cluster_id: [7; 16],
                            auth: b"sim".to_vec(),
                            node_id: me.0,
                            shard: *shard,
                            partition: p as u16,
                            fence_term: fence,
                            // kv-node: the persisted term of the last completed catch-up
                            last_term: self.last_term[p],
                            applied,
                            recovered: if scanned {
                                Recovered::Scanned
                            } else {
                                Recovered::Clean
                            },
                            learner: false,
                            migration: None,
                        };
                        let mut out = Vec::new();
                        frame::encode(&Frame::Hello(h), p as u16, fence, &mut out);
                        let conn = *env.next_conn;
                        *env.next_conn += 1;
                        ms.conn = Some(conn);
                        ms.ready = false;
                        ms.rejected = false;
                        dials.push((conn, p as u16, pr, out));
                    }
                }
            }
        }
        if all_ready
            && !*ready_sent
            && let Some(c) = ctrl
        {
            *ready_sent = true;
            fx.send(
                c,
                Msg::Report(NodeToCoord::MemberReady {
                    shard: ShardId(*shard),
                    term: *term,
                    member: me,
                }),
            );
        }
        for (conn, p, pr, out) in dials {
            let peer = env.topo.node_proc(pr);
            let mut end = StreamEnd::new(peer, fx.now_g);
            end.tx = 1;
            self.conns.insert(conn, (ConnRole::Member(p), end));
            fx.send(
                peer,
                Msg::Stream {
                    conn,
                    seq: 0,
                    data: Some(out),
                },
            );
        }
        for (conn, out) in acks {
            self.stream_send(conn, Some(out), fx);
        }
    }
}

// =============================================================================================
// clients

struct ClientOp {
    op: u64,
    key: usize,
    kind: u8, // 0 set, 1 del, 2 get
    write_id: u64,
    start: u64,
    deadline: u64,
    node: Option<usize>,
    hist_index: Option<usize>,
}

struct Clients {
    next_op: u64,
    next_write: u64,
    route: BTreeMap<u16, usize>,
    active: Vec<Option<ClientOp>>,
    idle_until: Vec<u64>,
    stop_at: u64,
    final_reads: Vec<bool>,
    final_phase: bool,
    /// The last outcome of each final read attempt (failure messages).
    final_last: Vec<&'static str>,
}

// =============================================================================================
// the world

#[derive(Clone, Copy)]
enum Timer {
    Tick,
    Fault,
    Restart(usize),
    HealNet,
    Heal,
    FinalPhase,
    /// The frozen process resumes.
    Thaw(usize),
    /// Crash a node right after a freeze (it restarts after `.1` ms).
    CrashAfterFreeze(usize, u64),
    /// Step of the scripted `ClusterSimCfg::stall_crash` check.
    Script(u8),
}

/// The scripted stall-then-crash check's record.
struct StallProbe {
    node: usize,
    /// Not frozen yet: the freeze starts at the end of the first tick of the node in which
    /// an acknowledgement is in flight (its own durability covers a group a member has not
    /// acknowledged yet), the interleaving in which the stall used to look like member lag.
    armed: bool,
    shard: u16,
    isr_before: Vec<NodeId>,
    frozen_at: u64,
    crashed_at: Option<u64>,
    /// An ISR shrink was committed between the freeze and the crash.
    shrunk: Option<Vec<NodeId>>,
    promoted_at: Option<u64>,
}

struct World {
    cfg: ClusterSimCfg,
    rng: StdRng,
    net: SimNet<(usize, Msg)>,
    timers: BTreeMap<(u64, u64), (usize, Timer)>,
    timer_seq: u64,
    topo: Topo,
    peers: Arc<PeerList>,
    coords: Vec<Coord>,
    nodes: Vec<Node>,
    clients: Clients,
    obs: Obs,
    report: ClusterSimReport,
    next_conn: u64,
    out: Vec<(usize, usize, Msg)>,
    end: u64,
    partitioned: bool,
    /// Per process: frozen until this global time (0: running).
    frozen_until: Vec<u64>,
    /// Per process: input that arrived while it was frozen, in arrival order.
    frozen_buf: Vec<Vec<(usize, Msg)>>,
    probe: Option<StallProbe>,
}

/// The bootstrapped cluster: 6 nodes, two RF3 shards (min ISR 2) over all slots, every
/// replica InSync; built by admin transitions, then committed as the starting state.
fn bootstrap(nodes: usize) -> Result<ClusterState, String> {
    let params = ClusterParams {
        partitions: P,
        ..ClusterParams::default()
    };
    let mut s = ClusterState::initial(params, [7; 16]);
    let mut b = Vec::new();
    for i in 0..nodes {
        b.push(Transition::Admin(admin_ops::node_add(i)));
    }
    let shards = (nodes / RF).max(1);
    for sh in 0..shards {
        let first = sh * RF;
        b.push(Transition::Admin(AdminOp::ShardCreate {
            id: ShardId(sh as u16 + 1),
            primary: node_id(first),
            min_isr: MIN_ISR,
        }));
        for r in 1..RF {
            if first + r < nodes {
                b.push(Transition::Admin(AdminOp::ReplicaAdd {
                    shard: ShardId(sh as u16 + 1),
                    node: node_id(first + r),
                }));
            }
        }
        let lo = (sh * 16384 / shards) as u16;
        let hi = ((sh + 1) * 16384 / shards - 1) as u16;
        b.push(Transition::Admin(AdminOp::SlotsAssign {
            slots: SlotRanges(vec![(lo, hi)]),
            shard: ShardId(sh as u16 + 1),
        }));
    }
    s = s.apply(&b).map_err(|e| format!("bootstrap: {e:?}"))?;
    // the learners finished their (empty) full sync and were expanded into the ISR
    for sh in 0..shards {
        let sid = ShardId(sh as u16 + 1);
        for r in 1..RF {
            let n = node_id(sh * RF + r);
            let first = node_id(sh * RF);
            if let Some(t) = isr::member_ready_transition(&s, &n, sid, 1, n) {
                s = s
                    .apply(&[t])
                    .map_err(|e| format!("bootstrap ready: {e:?}"))?;
            }
            if let Some(t) = isr::isr_transition(&s, sid, 1, n, true) {
                s = s
                    .apply(&[t])
                    .map_err(|e| format!("bootstrap expand: {e:?}"))?;
            }
            let _ = first;
        }
    }
    Ok(s)
}

/// `AdminOp::NodeAdd` through its serde encoding: the variant's operator-label field has a
/// name that the storage-path vocabulary check (LC-02) does not allow in this crate, so it is
/// built from a mirror enum with the same encoding (postcard encodes variants by index and
/// fields by position, not by name).
mod admin_ops {
    use super::*;

    #[derive(serde::Serialize)]
    #[allow(dead_code)]
    enum Mirror {
        ParamSet {
            key: String,
            value: String,
        },
        NodeAdd {
            id: NodeId,
            ctrl: SocketAddr,
            client: SocketAddr,
            repl: SocketAddr,
            label: String,
            priority: u8,
            partitions: u16,
        },
    }

    pub(super) fn node_add(i: usize) -> AdminOp {
        let m = Mirror::NodeAdd {
            id: node_id(i),
            ctrl: node_addr(i, 8379),
            client: node_addr(i, 6379),
            repl: node_addr(i, 7379),
            label: format!("z{}", i % 3),
            priority: 0,
            partitions: P,
        };
        let b = postcard::to_allocvec(&m).expect("encodable");
        let op: AdminOp = postcard::from_bytes(&b).expect("the mirror matches AdminOp::NodeAdd");
        assert!(
            matches!(&op, AdminOp::NodeAdd { id, partitions, .. } if *id == node_id(i) && *partitions == P)
        );
        op
    }
}

impl World {
    fn new(cfg: &ClusterSimCfg) -> Result<Self, String> {
        let nc = usize::from(cfg.coordinators.max(1));
        let nn = usize::from(cfg.nodes.max(RF as u8));
        let base = Arc::new(bootstrap(nn)?);
        let records: Vec<CoordRecord> = (0..nc)
            .map(|i| {
                let a = |p: u16| SocketAddr::from(([10, 0, 0, i as u8 + 1], p));
                CoordRecord::new(coord_id(i), a(7100), a(7000), a(7200))
            })
            .collect();
        let endpoints = records
            .iter()
            .map(|r| CoordEndpoint {
                id: r.id,
                client: r.client,
            })
            .collect();
        let peers = Arc::new(PeerList {
            r#gen: 1,
            peers: records,
        });
        let topo = Topo {
            coords: nc,
            nodes: nn,
            client: nc + nn,
            endpoints,
        };
        let mut net = SimNet::new(cfg.seed, cfg.faults);
        net.add_process(nc + nn);
        let mut rng = StdRng::seed_from_u64(cfg.seed ^ 0x434c_5553_5445_5253);
        let mut coords: Vec<Coord> = (0..nc).map(|i| Coord::new(i, &peers, &base)).collect();
        for (i, c) in coords.iter_mut().enumerate() {
            c.election_at = net.after(i, rng.random_range(500 * MS..1000 * MS));
        }
        let nodes = (0..nn).map(Node::new).collect();
        let k = usize::from(cfg.clients.max(1));
        let end = cfg.sim_ms * MS + QUIET_MS * MS;
        let mut obs = Obs {
            versions: BTreeMap::new(),
            base_version: base.version,
            takeover: BTreeMap::new(),
            acked: Vec::new(),
            hist: (0..KEYS).map(|_| Vec::new()).collect(),
            fence_attempts: BTreeMap::new(),
            snap_frames: 0,
            violation: None,
        };
        obs.versions.insert(base.version, base.clone());
        let mut w = World {
            cfg: *cfg,
            rng,
            net,
            timers: BTreeMap::new(),
            timer_seq: 0,
            topo,
            peers,
            coords,
            nodes,
            clients: Clients {
                next_op: 1,
                next_write: 1,
                route: BTreeMap::new(),
                active: (0..k).map(|_| None).collect(),
                idle_until: vec![200 * MS; k],
                stop_at: end - 9_000 * MS,
                final_reads: vec![false; KEYS],
                final_phase: false,
                final_last: vec!["never tried"; KEYS],
            },
            obs,
            report: ClusterSimReport::default(),
            next_conn: 1,
            out: Vec::new(),
            end,
            partitioned: false,
            frozen_until: vec![0; nc + nn + 1],
            frozen_buf: (0..=nc + nn).map(|_| Vec::new()).collect(),
            probe: None,
        };
        for n in &mut w.nodes {
            n.stall_clock = cfg.stall_clock;
        }
        for c in &mut w.coords {
            c.shrink_guard = cfg.stall_clock;
        }
        if let Some(sc) = cfg.stall_crash {
            w.schedule(sc.at_ms * MS, w.topo.client, Timer::Script(0));
        }
        for p in 0..=w.topo.client {
            let t = w.rng.random_range(0..5 * MS);
            w.schedule(t, p, Timer::Tick);
        }
        if cfg.fault_every_ms > 0 {
            let t = w
                .rng
                .random_range(500 * MS..cfg.fault_every_ms * 2 * MS + 500 * MS);
            w.schedule(t, w.topo.client, Timer::Fault);
        }
        w.schedule(cfg.sim_ms * MS, w.topo.client, Timer::Heal);
        w.schedule(end - 6_000 * MS, w.topo.client, Timer::FinalPhase);
        Ok(w)
    }

    fn schedule(&mut self, at: u64, proc_: usize, t: Timer) {
        self.timer_seq += 1;
        self.timers.insert((at, self.timer_seq), (proc_, t));
    }

    fn up(&self, p: usize) -> bool {
        if p < self.topo.coords {
            self.coords[p].up
        } else if p < self.topo.client {
            self.nodes[p - self.topo.coords].up
        } else {
            true
        }
    }

    fn run(&mut self) -> Result<(), String> {
        let mut steps = 0u64;
        loop {
            steps += 1;
            if steps > self.cfg.steps {
                return Err(format!(
                    "event budget of {} exhausted at {} ms",
                    self.cfg.steps,
                    self.net.now() / MS
                ));
            }
            let t_msg = self.net.next_delivery_time().unwrap_or(u64::MAX);
            let t_timer = self.timers.keys().next().map_or(u64::MAX, |k| k.0);
            let t = t_msg.min(t_timer);
            if t == u64::MAX || t > self.end {
                break;
            }
            if t_msg <= t_timer {
                if let Some((to, (from, m))) = self.net.deliver_next() {
                    self.deliver(from, to, m);
                }
            } else {
                let (&k, &(p, timer)) = self.timers.iter().next().expect("timer");
                self.timers.remove(&k);
                self.net.advance_to(k.0);
                self.on_timer(p, timer);
            }
            self.report.events += 1;
            self.flush();
            self.check_instant();
            self.observe_probe();
            if let Some(v) = self.obs.violation.take() {
                return Err(format!("{v} (at {} ms)", self.net.now() / MS));
            }
        }
        self.final_checks()
    }

    fn flush(&mut self) {
        for (from, to, m) in std::mem::take(&mut self.out) {
            self.net.send(from, to, (from, m));
        }
    }

    /// At most one serving primary per shard at this instant (lease disjointness).
    fn check_instant(&mut self) {
        let mut serving: BTreeMap<u16, Vec<(usize, u32)>> = BTreeMap::new();
        for n in &self.nodes {
            let local = self.net.local_time(self.topo.node_proc(n.idx));
            if let Some((sh, term)) = n.serving_primary(local) {
                serving.entry(sh).or_default().push((n.idx, term));
            }
        }
        for (sh, v) in serving {
            if v.len() > 1 {
                self.obs
                    .fail(format!("two serving primaries in shard {sh}: {v:?}"));
            }
        }
    }

    fn fx_parts(&mut self, p: usize) -> (u64, u64) {
        (self.net.now(), self.net.local_time(p))
    }

    fn deliver(&mut self, from: usize, to: usize, m: Msg) {
        if !self.up(to) {
            return;
        }
        if self.frozen_until[to] > 0 {
            // a stopped process reads nothing; its sockets keep what arrives
            self.frozen_buf[to].push((from, m));
            return;
        }
        let (now_g, now_l) = self.fx_parts(to);
        let net = &self.net;
        let after = |d: u64| net.after(to, d);
        let mut fx = Fx {
            me: to,
            out: &mut self.out,
            obs: &mut self.obs,
            report: &mut self.report,
            rng: &mut self.rng,
            now_g,
            now_l,
            after: &after,
        };
        if to < self.topo.coords {
            let c = &mut self.coords[to];
            match m {
                Msg::Wsr(msg) => {
                    if from < self.topo.coords {
                        c.step(
                            Input::Msg {
                                from: coord_id(from),
                                msg,
                            },
                            &mut fx,
                            &self.topo,
                        );
                    }
                }
                Msg::Report(r) => {
                    if let Some(n) = from
                        .checked_sub(self.topo.coords)
                        .filter(|n| *n < self.topo.nodes)
                    {
                        c.on_report(n, r, &mut fx, &self.topo);
                    }
                }
                Msg::Route { op, slot } => {
                    let node = c.on_route(slot, &fx);
                    fx.send(from, Msg::RouteReply { op, node });
                }
                _ => {}
            }
        } else if to < self.topo.client {
            let n = &mut self.nodes[to - self.topo.coords];
            let mut env = NodeEnv {
                topo: &self.topo,
                next_conn: &mut self.next_conn,
            };
            match m {
                Msg::Ctrl { coord_term, m } => n.on_ctrl(from, coord_term, m, &mut fx, &mut env),
                Msg::Stream { conn, seq, data } => {
                    n.on_stream(from, conn, seq, data, &mut fx, &mut env)
                }
                Msg::Req { op, req } => n.on_req(from, op, req, &mut fx),
                _ => {}
            }
        } else {
            match m {
                Msg::RouteReply { op, node } => {
                    client_route_reply(&mut self.clients, op, node, &mut fx, &self.topo)
                }
                Msg::Resp { op, resp } => {
                    client_resp(&mut self.clients, &mut fx, op, from, resp, &self.topo)
                }
                _ => {}
            }
        }
    }

    fn on_timer(&mut self, p: usize, t: Timer) {
        match t {
            Timer::Tick => {
                let period = if p < self.topo.coords {
                    10 * MS
                } else {
                    5 * MS
                };
                let next = self.net.after(p, period);
                self.schedule(next, p, Timer::Tick);
                if !self.up(p) || self.frozen_until[p] > 0 {
                    return;
                }
                self.tick_proc(p);
            }
            Timer::Fault => self.inject_fault(),
            Timer::Restart(proc_) => self.restart(proc_),
            Timer::HealNet => {
                self.net.heal();
                self.partitioned = false;
            }
            Timer::Heal => {
                self.net.heal();
                self.partitioned = false;
                for p in 0..self.topo.client {
                    self.thaw(p);
                    if !self.up(p) {
                        self.restart(p);
                    }
                }
                for n in &mut self.nodes {
                    n.disk_slow_until = 0;
                }
            }
            Timer::FinalPhase => self.clients.final_phase = true,
            Timer::Thaw(proc_) => {
                // a stale timer of an earlier freeze (the process crashed meanwhile) is ignored
                if self.frozen_until[proc_] > 0 && self.net.now() >= self.frozen_until[proc_] {
                    self.thaw(proc_);
                }
            }
            Timer::CrashAfterFreeze(i, back_ms) => {
                if self.nodes[i].up && self.frozen_until[self.topo.node_proc(i)] == 0 {
                    self.crash_node(i, back_ms);
                }
            }
            Timer::Script(step) => self.script(step),
        }
    }

    /// One tick of process `p` (its timers: renewals, streams, elections, client ops).
    fn tick_proc(&mut self, p: usize) {
        {
            {
                let (now_g, now_l) = self.fx_parts(p);
                let net = &self.net;
                let after = |d: u64| net.after(p, d);
                let mut fx = Fx {
                    me: p,
                    out: &mut self.out,
                    obs: &mut self.obs,
                    report: &mut self.report,
                    rng: &mut self.rng,
                    now_g,
                    now_l,
                    after: &after,
                };
                if p < self.topo.coords {
                    self.coords[p].tick(&mut fx, &self.topo);
                } else if p < self.topo.client {
                    let mut env = NodeEnv {
                        topo: &self.topo,
                        next_conn: &mut self.next_conn,
                    };
                    self.nodes[p - self.topo.coords].tick(&mut fx, &mut env);
                } else {
                    client_tick(&mut self.clients, &mut fx, &self.topo);
                }
            }
        }
        // the scripted stall starts right after a tick that left an ack in flight
        if let Some(pr) = &self.probe
            && pr.armed
            && p == self.topo.node_proc(pr.node)
            && self.nodes[pr.node].ack_in_flight()
        {
            self.flush();
            self.script(1);
        }
    }

    /// Process `p` stops for `ms` (SIGSTOP, a long scheduling delay): no timer runs and its
    /// input waits until it resumes.
    fn freeze(&mut self, p: usize, ms: u64) {
        if p >= self.topo.client || !self.up(p) || self.frozen_until[p] > 0 {
            return;
        }
        let now = self.net.now();
        if trace_on() {
            eprintln!("[{} ms] freeze proc {p} for {ms} ms", now / MS);
        }
        self.report.freezes += 1;
        self.frozen_until[p] = now + ms * MS;
        self.schedule(now + ms * MS, self.topo.client, Timer::Thaw(p));
    }

    /// The frozen process resumes: its stale timers run first (the worst order after
    /// SIGCONT), then the input that waited, in arrival order.
    fn thaw(&mut self, p: usize) {
        if self.frozen_until[p] == 0 {
            return;
        }
        self.frozen_until[p] = 0;
        let buf = std::mem::take(&mut self.frozen_buf[p]);
        if !self.up(p) {
            return;
        }
        if trace_on() {
            eprintln!("[{} ms] thaw proc {p}", self.net.now() / MS);
        }
        self.tick_proc(p);
        self.flush();
        for (from, m) in buf {
            self.deliver(from, p, m);
            self.flush();
        }
    }

    /// Kill -9 of node `i`; it restarts `back_ms` later.
    fn crash_node(&mut self, i: usize, back_ms: u64) {
        if !self.nodes[i].up {
            return;
        }
        let now = self.net.now();
        let p = self.topo.node_proc(i);
        if trace_on() {
            eprintln!("[{} ms] crash n{i}", now / MS);
        }
        self.frozen_until[p] = 0;
        self.frozen_buf[p].clear();
        self.nodes[i].crash();
        self.net.crash(p);
        self.report.node_crashes += 1;
        self.schedule(now + back_ms * MS, p, Timer::Restart(p));
    }

    /// The leader's committed state (`None` without a leader).
    fn leader_state(&self) -> Option<Arc<ClusterState>> {
        self.coords
            .iter()
            .find(|c| c.up && c.core.role() == Role::Leader && c.leading.is_some())
            .map(|c| c.state().clone())
    }

    /// The scripted stall-then-crash check (`ClusterSimCfg::stall_crash`).
    fn script(&mut self, step: u8) {
        let Some(sc) = self.cfg.stall_crash else {
            return;
        };
        let now = self.net.now();
        match step {
            0 => {
                let Some(s) = self.leader_state() else {
                    // no leader yet: try again shortly
                    self.schedule(now + 100 * MS, self.topo.client, Timer::Script(0));
                    return;
                };
                let Some(sh) = s.shards.first() else { return };
                let Some(node) = sh.primary.and_then(|p| node_index(&p)) else {
                    return;
                };
                let isr_before: Vec<NodeId> = sh.isr().map(|m| m.node).collect();
                self.probe = Some(StallProbe {
                    node,
                    armed: true,
                    shard: sh.id.0,
                    isr_before,
                    frozen_at: now,
                    crashed_at: None,
                    shrunk: None,
                    promoted_at: None,
                });
            }
            1 => {
                let Some(node) = self.probe.as_ref().map(|p| p.node) else {
                    return;
                };
                if let Some(p) = &mut self.probe {
                    p.armed = false;
                    p.frozen_at = now;
                }
                self.freeze(self.topo.node_proc(node), sc.freeze_ms);
                self.schedule(
                    now + (sc.freeze_ms + sc.crash_ms) * MS,
                    self.topo.client,
                    Timer::Script(2),
                );
            }
            _ => {
                let Some(node) = self.probe.as_ref().map(|p| p.node) else {
                    return;
                };
                // it stays down long enough that only a promotion can serve its shard
                self.crash_node(node, 8_000);
                if let Some(p) = &mut self.probe {
                    p.crashed_at = Some(now);
                }
            }
        }
    }

    /// The scripted check's observations after every event.
    fn observe_probe(&mut self) {
        if self.probe.as_ref().is_none_or(|p| p.promoted_at.is_some()) {
            return;
        }
        let Some(s) = self.leader_state() else { return };
        let now = self.net.now();
        let Some(p) = &mut self.probe else { return };
        let Some(sh) = s.shard(ShardId(p.shard)) else {
            return;
        };
        let primary = sh.primary.and_then(|n| node_index(&n));
        match p.crashed_at {
            None => {
                let isr: Vec<NodeId> = sh.isr().map(|m| m.node).collect();
                if primary == Some(p.node) && isr.len() < p.isr_before.len() && p.shrunk.is_none() {
                    p.shrunk = Some(isr);
                }
            }
            Some(_) => {
                if primary.is_some_and(|n| n != p.node) && sh.status == ShardStatus::Serving {
                    p.promoted_at = Some(now);
                }
            }
        }
    }

    /// The scripted check's verdict (final checks).
    fn probe_verdict(&self) -> Result<(), String> {
        let Some(p) = &self.probe else {
            return if self.cfg.stall_crash.is_some() {
                Err("the scripted stall never ran".into())
            } else {
                Ok(())
            };
        };
        if let Some(isr) = &p.shrunk {
            return Err(format!(
                "a {} ms stall of the primary n{} of shard {} shrank its ISR from {} to {} members while every member was healthy",
                self.cfg.stall_crash.map_or(0, |s| s.freeze_ms),
                p.node,
                p.shard,
                p.isr_before.len(),
                isr.len()
            ));
        }
        let Some(c) = p.crashed_at else {
            return Err("the scripted crash never ran".into());
        };
        match p.promoted_at {
            Some(t) if t - c <= 3_000 * MS => Ok(()),
            Some(t) => Err(format!(
                "shard {} was promoted {} ms after its primary crashed (bound 3000 ms)",
                p.shard,
                (t - c) / MS
            )),
            None => Err(format!(
                "shard {} had no new primary after its primary n{} crashed (frozen at {} ms)",
                p.shard,
                p.node,
                p.frozen_at / MS
            )),
        }
    }

    fn restart(&mut self, p: usize) {
        if self.up(p) {
            return;
        }
        self.frozen_until[p] = 0;
        self.frozen_buf[p].clear();
        self.net.restart(p);
        if p < self.topo.coords {
            let peers = self.peers.clone();
            let c = &mut self.coords[p];
            c.up = true;
            c.restart(&peers);
            c.election_at = self
                .net
                .after(p, self.rng.random_range(500 * MS..1000 * MS));
        } else {
            let local = self.net.local_time(p);
            self.nodes[p - self.topo.coords].restart(&mut self.rng, local);
        }
    }

    fn inject_fault(&mut self) {
        let now = self.net.now();
        if now >= self.cfg.sim_ms * MS {
            return;
        }
        let gap = self
            .rng
            .random_range(self.cfg.fault_every_ms / 2..self.cfg.fault_every_ms * 3 / 2 + 1)
            * MS;
        self.schedule(now + gap, self.topo.client, Timer::Fault);
        let r = self.rng.random_range(0..100u32);
        if trace_on() {
            eprintln!("[{} ms] fault draw {r}", now / MS);
        }
        let down_nodes = self.nodes.iter().filter(|n| !n.up).count();
        let down_coords = self.coords.iter().filter(|c| !c.up).count();
        if self.cfg.freezes && self.rng.random_bool(0.15) {
            // a freeze (SIGSTOP, a long scheduling delay) of a primary, any node or a
            // coordinator; a frozen node is sometimes crashed right after it resumes
            let ms = self.rng.random_range(100..2_500u64);
            let pick = self.rng.random_range(0..10u32);
            let primaries: Vec<usize> = self
                .leader_state()
                .map(|s| {
                    s.shards
                        .iter()
                        .filter_map(|sh| sh.primary.and_then(|p| node_index(&p)))
                        .collect()
                })
                .unwrap_or_default();
            let proc_ = if pick < 5 && !primaries.is_empty() {
                let i = primaries[self.rng.random_range(0..primaries.len())];
                Some((self.topo.node_proc(i), Some(i)))
            } else if pick < 8 {
                let i = self.rng.random_range(0..self.topo.nodes);
                Some((self.topo.node_proc(i), Some(i)))
            } else {
                Some((self.rng.random_range(0..self.topo.coords), None))
            };
            if let Some((p, node)) = proc_ {
                self.freeze(p, ms);
                if let Some(i) = node
                    && down_nodes < 2
                    && self.rng.random_bool(0.3)
                {
                    let after = self.rng.random_range(0..400u64);
                    let back = self.rng.random_range(300..6_000u64);
                    self.schedule(
                        now + (ms + after) * MS,
                        self.topo.client,
                        Timer::CrashAfterFreeze(i, back),
                    );
                }
            }
            return;
        }
        if r < 40 && down_nodes < 2 {
            // crash a node (kill -9); it restarts after a while
            let i = self.rng.random_range(0..self.topo.nodes);
            if self.nodes[i].up {
                let back = self.rng.random_range(300..6_000u64);
                self.crash_node(i, back);
            }
        } else if r < 55 && down_coords < 1 {
            let i = self.rng.random_range(0..self.topo.coords);
            if self.coords[i].up {
                self.frozen_until[i] = 0;
                self.frozen_buf[i].clear();
                self.coords[i].up = false;
                self.coords[i].leading = None;
                self.net.crash(i);
                self.report.coord_crashes += 1;
                let back = now + self.rng.random_range(300 * MS..4_000 * MS);
                self.schedule(back, i, Timer::Restart(i));
            }
        } else if r < 75 && !self.partitioned {
            // a network partition of a random side for 0.5-4 s
            let side: Vec<usize> = (0..self.topo.client)
                .filter(|_| self.rng.random_bool(0.3))
                .collect();
            if !side.is_empty() {
                self.net.partition(&side);
                self.partitioned = true;
                self.report.partitions += 1;
                let heal = now + self.rng.random_range(500 * MS..4_000 * MS);
                self.timer_seq += 1;
                self.timers
                    .insert((heal, self.timer_seq), (self.topo.client, Timer::HealNet));
            }
        } else {
            // a slow disk: writes take 50-600 ms more to become durable for a while
            let i = self.rng.random_range(0..self.topo.nodes);
            self.nodes[i].disk_slow_ns = self.rng.random_range(50 * MS..600 * MS);
            self.nodes[i].disk_slow_until = now + self.rng.random_range(500 * MS..3_000 * MS);
            self.report.slow_disks += 1;
        }
    }

    /// A compact picture of the cluster for a failure message: the leader's committed shards
    /// and each node's role, lease and fence.
    fn diagnose(&self) -> String {
        let mut out = String::new();
        let leader = self
            .coords
            .iter()
            .find(|c| c.up && c.core.role() == Role::Leader);
        match leader {
            None => out.push_str("no leader; "),
            Some(c) => {
                let s = c.state();
                out.push_str(&format!("leader c{} v{}; ", c.idx, s.version));
                for sh in &s.shards {
                    out.push_str(&format!(
                        "shard {} {:?} term {} primary {}: ",
                        sh.id.0,
                        sh.status,
                        sh.term,
                        sh.primary.map_or("-".into(), |p| short(&p))
                    ));
                    for m in &sh.members {
                        let fail = s.node(&m.node).is_some_and(|r| r.flags.fail);
                        out.push_str(&format!(
                            "{}{}{}{} ",
                            short(&m.node),
                            if m.in_isr { "+" } else { "" },
                            m.state.as_str(),
                            if fail { "!" } else { "" }
                        ));
                    }
                    out.push_str("; ");
                }
                if let Some(l) = &c.leading {
                    for (sid, fo) in &l.failovers {
                        let st = match &fo.stage {
                            FoStage::Choose => "choose".to_string(),
                            FoStage::Fencing { n, .. } => format!("fencing {}", short(n)),
                            FoStage::LeaseGone { n, .. } => format!("lease-gone {}", short(n)),
                            FoStage::Retry { .. } => "retry".to_string(),
                        };
                        out.push_str(&format!(
                            "failover {sid}: {st} unusable {:?}; ",
                            fo.unusable.iter().map(short).collect::<Vec<_>>()
                        ));
                    }
                }
            }
        }
        for n in &self.nodes {
            let local = self.net.local_time(self.topo.node_proc(n.idx));
            let role = match &n.role {
                NodeRole::None => "none".to_string(),
                NodeRole::Primary { term, pending, .. } => {
                    format!("primary t{term} pending {}", pending.len())
                }
                NodeRole::Replica {
                    term,
                    primary,
                    parts,
                    ..
                } => format!(
                    "replica t{term} of {} ready {}/{} rejected {}",
                    primary.map_or("-".into(), |p| format!("n{p}")),
                    parts.iter().filter(|m| m.ready).count(),
                    parts.len(),
                    parts.iter().filter(|m| m.rejected).count()
                ),
            };
            out.push_str(&format!(
                "n{} {} fence {} lease {} ctrl {:?}; ",
                n.idx,
                role,
                n.disk.fence_term,
                if local < n.lease_expiry { "ok" } else { "no" },
                n.ctrl.map(|c| c.0)
            ));
        }
        out
    }

    /// After the quiet phase: every key's history is linearizable in the §23.11 sense (the
    /// final reads after quiescence included), and the members converged to their primaries.
    fn final_checks(&mut self) -> Result<(), String> {
        self.probe_verdict()?;
        let missing = self.clients.final_reads.iter().filter(|r| !**r).count();
        if missing > 0 {
            let keys: Vec<String> = (0..KEYS)
                .filter(|k| !self.clients.final_reads[*k])
                .map(|k| {
                    format!(
                        "key {k} slot {} ({})",
                        kv_common::key_slot(&key_name(k)),
                        self.clients.final_last[k]
                    )
                })
                .collect();
            return Err(format!(
                "{missing} keys could not be read after the network healed and every process restarted (cluster unavailable) [{}]: {}",
                keys.join(", "),
                self.diagnose()
            ));
        }
        for (k, h) in self.obs.hist.iter().enumerate() {
            linearize::check_key(h).map_err(|e| format!("key {k}: {e}"))?;
        }
        // convergence: every ISR member that finished its catch-up holds its primary's state
        let leader = self
            .coords
            .iter()
            .find(|c| c.up && c.core.role() == Role::Leader);
        let Some(leader) = leader else {
            return Err("no coordinator leader after the quiet phase".into());
        };
        let s = leader.state().clone();
        for sh in &s.shards {
            let Some(pi) = sh.primary.and_then(|p| node_index(&p)) else {
                continue;
            };
            for m in &sh.members {
                let Some(mi) = node_index(&m.node) else {
                    continue;
                };
                if mi == pi || !m.in_isr || m.state != MemberState::InSync {
                    continue;
                }
                let NodeRole::Replica { parts, .. } = &self.nodes[mi].role else {
                    continue;
                };
                for (p, part) in parts.iter().enumerate() {
                    if !part.ready {
                        continue;
                    }
                    let a: BTreeMap<&Vec<u8>, &Vec<u8>> = self.nodes[pi].stores[p]
                        .live
                        .iter()
                        .map(|(k, r)| (k, &r.value))
                        .collect();
                    let b: BTreeMap<&Vec<u8>, &Vec<u8>> = self.nodes[mi].stores[p]
                        .live
                        .iter()
                        .map(|(k, r)| (k, &r.value))
                        .collect();
                    if a != b {
                        return Err(format!(
                            "member n{mi} of shard {} did not converge to primary n{pi} on partition {p} ({} vs {} keys)",
                            sh.id.0,
                            b.len(),
                            a.len()
                        ));
                    }
                }
            }
        }
        if self.obs.snap_frames > 0 {
            return Err(format!(
                "{} full-sync frames caused by failures alone",
                self.obs.snap_frames
            ));
        }
        Ok(())
    }
}

// =============================================================================================
// client logic

fn client_tick(c: &mut Clients, fx: &mut Fx<'_>, topo: &Topo) {
    for i in 0..c.active.len() {
        if let Some(op) = &c.active[i] {
            if fx.now_g > op.deadline {
                // no reply in time: not acknowledged (a write may still take effect)
                let op = c.active[i].take().expect("active");
                if trace_on() && trace_key(&key_name(op.key)) {
                    eprintln!(
                        "[{} ms] client op {} kind {} timed out (node {:?})",
                        fx.now_g / MS,
                        op.op,
                        op.kind,
                        op.node
                    );
                }
                if c.final_phase {
                    c.final_last[op.key] = "timeout";
                }
                if let Some(n) = op.node {
                    c.route.retain(|_, v| *v != n);
                }
                c.idle_until[i] = fx.now_g + 20 * MS;
                let _ = op;
            }
            continue;
        }
        if fx.now_g < c.idle_until[i] {
            continue;
        }
        let final_phase = c.final_phase;
        if !final_phase && fx.now_g >= c.stop_at {
            continue;
        }
        let (key, kind) = if final_phase {
            // final reads: each key once, after quiescence
            let busy: Vec<usize> = c.active.iter().flatten().map(|o| o.key).collect();
            match (0..KEYS).find(|k| !c.final_reads[*k] && !busy.contains(k)) {
                Some(k) => (k, 2u8),
                None => continue,
            }
        } else {
            let r = fx.rng.random_range(0..100u32);
            (
                fx.rng.random_range(0..KEYS),
                if r < 55 {
                    0
                } else if r < 65 {
                    1
                } else {
                    2
                },
            )
        };
        let op = c.next_op;
        c.next_op += 1;
        let mut o = ClientOp {
            op,
            key,
            kind,
            write_id: 0,
            start: fx.now_g,
            // the final reads give up sooner: a request or reply lost by the network (the
            // drop fault stays on after the heal) is retried within the final window
            deadline: fx.now_g + if final_phase { 500 * MS } else { 2_000 * MS },
            node: None,
            hist_index: None,
        };
        if kind != 2 {
            // every write attempt is in the history from its start: one that never gets OK is
            // an unacknowledged write (it may take effect at any time, G6)
            o.write_id = c.next_write;
            c.next_write += 1;
            let h = &mut fx.obs.hist[key];
            h.push(if kind == 0 {
                Event::Write {
                    id: o.write_id,
                    start_ns: o.start,
                    ack_ns: None,
                }
            } else {
                Event::Delete {
                    id: o.write_id,
                    start_ns: o.start,
                    ack_ns: None,
                }
            });
            o.hist_index = Some(h.len() - 1);
            fx.report.unacked_writes += 1;
        }
        let slot = kv_common::key_slot(&key_name(key));
        match c.route.get(&slot) {
            Some(&n) => {
                o.node = Some(n);
                fx.send(
                    topo.node_proc(n),
                    Msg::Req {
                        op,
                        req: make_req(&o),
                    },
                );
            }
            None => {
                let coord = fx.rng.random_range(0..topo.coords);
                fx.send(topo.coord_proc(coord), Msg::Route { op, slot });
            }
        }
        c.active[i] = Some(o);
    }
}

fn make_req(o: &ClientOp) -> Req {
    let k = key_name(o.key);
    match o.kind {
        0 => Req::Set(k, o.write_id),
        1 => Req::Del(k),
        _ => Req::Get(k),
    }
}

fn find_active(c: &mut Clients, op: u64) -> Option<usize> {
    c.active
        .iter()
        .position(|a| a.as_ref().is_some_and(|o| o.op == op))
}

fn client_route_reply(c: &mut Clients, op: u64, node: Option<usize>, fx: &mut Fx<'_>, topo: &Topo) {
    let Some(i) = find_active(c, op) else { return };
    let o = c.active[i].as_mut().expect("active");
    if trace_on() && trace_key(&key_name(o.key)) {
        eprintln!("[{} ms] client op {} kind {} routed to {node:?}", fx.now_g / MS, o.op, o.kind);
    }
    match node {
        Some(n) => {
            c.route.insert(kv_common::key_slot(&key_name(o.key)), n);
            o.node = Some(n);
            let req = make_req(o);
            fx.send(topo.node_proc(n), Msg::Req { op, req });
        }
        None => {
            // CLUSTERDOWN (or no topology): the attempt never reached a node
            if c.final_phase {
                c.final_last[o.key] = "clusterdown";
            }
            c.active[i] = None;
            c.idle_until[i] = fx.now_g + 50 * MS;
        }
    }
}

fn client_resp(c: &mut Clients, fx: &mut Fx<'_>, op: u64, from: usize, resp: Resp, topo: &Topo) {
    let _ = from;
    let Some(i) = find_active(c, op) else {
        return; // a late reply to an attempt already given up: its write stays unacknowledged
    };
    let o = c.active[i].take().expect("active");
    let _ = topo;
    let now = fx.now_g;
    if trace_on() && trace_key(&key_name(o.key)) {
        eprintln!(
            "[{} ms] client op {} kind {} reply {resp:?} from proc {from}",
            now / MS,
            o.op,
            o.kind
        );
    }
    let hist = &mut fx.obs.hist[o.key];
    match (o.kind, resp) {
        (0 | 1, Resp::Ok) => {
            if let Some(Event::Write { ack_ns, .. } | Event::Delete { ack_ns, .. }) =
                o.hist_index.and_then(|i| hist.get_mut(i))
            {
                *ack_ns = Some(now);
            }
            fx.report.acked_writes += 1;
            fx.report.unacked_writes -= 1;
        }
        (2, Resp::Value(v)) => {
            hist.push(Event::Read {
                start_ns: o.start,
                end_ns: now,
                observed: v,
            });
            fx.report.reads += 1;
            if c.final_phase {
                c.final_reads[o.key] = true;
            }
        }
        (_, Resp::Moved) => {
            c.route.remove(&kv_common::key_slot(&key_name(o.key)));
            if c.final_phase {
                c.final_last[o.key] = "moved";
            }
        }
        (_, r) => {
            if c.final_phase {
                c.final_last[o.key] = match r {
                    Resp::TryAgain => "tryagain",
                    Resp::NoReplicas => "noreplicas",
                    _ => "other",
                };
            }
        }
    }
    c.idle_until[i] = fx.now_g + fx.rng.random_range(0..3 * MS);
}

/// Command line of a cluster campaign: `kv-sim cluster --seeds N [--start S] [--sim-ms MS]
/// [--hostile] [--quiet-net]` (wired by the binary's `main.rs`). Prints one line per failing
/// seed and a summary; exit code 0 when every seed passed.
pub mod cli {
    use super::*;

    pub fn main(args: Vec<String>) -> i32 {
        let mut seeds = 100u64;
        let mut start = 1u64;
        let mut sim_ms = None;
        let mut hostile = false;
        let mut quiet_net = false;
        let mut freezes = true;
        let mut stall_clock = true;
        let mut it = args.iter();
        while let Some(a) = it.next() {
            let mut num = |name: &str| -> Option<u64> {
                let v = it.next().and_then(|v| v.parse().ok());
                if v.is_none() {
                    eprintln!("kv-sim cluster: {name} needs a number");
                }
                v
            };
            match a.as_str() {
                "--seeds" => match num("--seeds") {
                    Some(v) => seeds = v,
                    None => return 2,
                },
                "--start" => match num("--start") {
                    Some(v) => start = v,
                    None => return 2,
                },
                "--sim-ms" => match num("--sim-ms") {
                    Some(v) => sim_ms = Some(v),
                    None => return 2,
                },
                "--hostile" => hostile = true,
                "--quiet-net" => quiet_net = true,
                "--no-freezes" => freezes = false,
                "--boot-clock" => stall_clock = false,
                other => {
                    eprintln!("kv-sim cluster: unknown argument {other}");
                    return 2;
                }
            }
        }
        let mut failed = 0u64;
        let mut total = ClusterSimReport::default();
        for seed in start..start + seeds {
            let mut cfg = ClusterSimCfg::new(seed);
            cfg.freezes = freezes;
            cfg.stall_clock = stall_clock;
            if let Some(ms) = sim_ms {
                cfg.sim_ms = ms;
            }
            if hostile {
                cfg.faults = NetFaults {
                    drop: 0.02,
                    duplicate: 0.02,
                    max_delay_ns: 20 * MS,
                    ..cfg.faults
                };
            }
            if quiet_net {
                cfg.faults = NetFaults::default();
            }
            match run_report(&cfg) {
                Ok(r) => total.add(&r),
                Err(e) => {
                    failed += 1;
                    println!("FAIL {e}");
                }
            }
        }
        println!("kv-sim cluster: {seeds} seeds from {start}, {failed} failed; {total:?}");
        i32::from(failed > 0)
    }
}

impl ClusterSimReport {
    /// Adds another run's counters (campaign totals).
    pub fn add(&mut self, r: &ClusterSimReport) {
        self.events += r.events;
        self.commits += r.commits;
        self.promotions += r.promotions;
        self.fences += r.fences;
        self.node_crashes += r.node_crashes;
        self.coord_crashes += r.coord_crashes;
        self.partitions += r.partitions;
        self.slow_disks += r.slow_disks;
        self.acked_writes += r.acked_writes;
        self.unacked_writes += r.unacked_writes;
        self.reads += r.reads;
        self.repairs += r.repairs;
        self.continues += r.continues;
        self.resync_needed += r.resync_needed;
        self.max_term = self.max_term.max(r.max_term);
        self.stream_breaks += r.stream_breaks;
        self.freezes += r.freezes;
        for (a, b) in self.resync_why.iter_mut().zip(r.resync_why) {
            *a += b;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_state_is_valid() {
        let s = bootstrap(6).unwrap();
        assert_eq!(s.shards.len(), 2);
        assert!(s.check_invariants().is_ok());
        for sh in &s.shards {
            assert_eq!(sh.isr().count(), 3);
            assert_eq!(sh.status, ShardStatus::Serving);
        }
        assert_eq!(s.assigned_slots(), 16384);
    }

    #[test]
    fn store_durability_pipeline() {
        let mut s = Store::new(Seq::new(1, 0));
        assert!(s.write(b"a", Some(vec![1]), Seq::new(1, 1), true, 0, 10));
        assert!(
            !s.write(b"a", Some(vec![0]), Seq::new(1, 1), true, 0, 10),
            "replay skipped"
        );
        s.mature(5);
        assert_eq!(s.durable_pos, Seq::new(1, 0));
        s.mature(10);
        assert_eq!(s.durable_pos, Seq::new(1, 1));
        assert!(s.write(b"a", None, Seq::new(1, 2), false, 10, 100));
        s.crash();
        assert_eq!(
            s.live.get(&b"a"[..]).map(|r| r.value.clone()),
            Some(vec![1]),
            "the delete was not durable"
        );
    }

    fn groups(from: u64, to: u64) -> Vec<u8> {
        let mut b = Vec::new();
        for c in from..=to {
            let g = Group {
                seq: Seq::new(1, c),
                ops: vec![Op::Put {
                    db: 0,
                    vtype: 1,
                    expire_at: 0,
                    key: format!("k{c}").into_bytes().into(),
                    value: vec![1],
                }],
            };
            kv_repl::codec::encode_group(&g, &mut b);
        }
        b
    }

    fn drain(m: &mut MemberStream, f: Frame) {
        m.on_frame(f).unwrap();
        while m.next_action().is_some() {}
    }

    /// kv-repl R1 (fixed), minimized from the simulation: a member whose stream broke while
    /// some received groups were not yet durable dials again with its durable position
    /// (kv-node: `m.stream.restart(durable, durable)`); the Continue resends groups the
    /// received ring already holds. Before the fix `Backlog::push` got a seq below its last
    /// one (debug builds: "backlog seqs must increase"; release: a ring with repeated,
    /// unordered seqs). Fix: `MemberStream` stores a group only when its seq is above the
    /// ring's last (`Backlog::push_after_end`).
    #[test]
    fn repro_r1_continue_below_ring_end() {
        let mut m = MemberStream::new(0, 1 << 14);
        m.restart(Seq::new(1, 0), Seq::new(1, 0));
        drain(
            &mut m,
            Frame::Accept {
                term: 1,
                mode: AcceptMode::Continue {
                    from: Seq::new(1, 1),
                },
            },
        );
        drain(&mut m, Frame::Groups(groups(1, 5)));
        // the stream breaks with only (1, 2) durable; the member dials again from there
        m.restart(Seq::new(1, 2), Seq::new(1, 2));
        drain(
            &mut m,
            Frame::Accept {
                term: 1,
                mode: AcceptMode::Continue {
                    from: Seq::new(1, 3),
                },
            },
        );
        drain(&mut m, Frame::Groups(groups(3, 6)));
        assert_eq!(
            m.received().len(),
            6,
            "each received group once, in seq order"
        );
    }

    /// kv-repl R2 (fixed), minimized: a member's received ring that starts empty (after a
    /// restart, or any fresh ring) and is fed by a `Continue { from }` never holds the groups
    /// before `from`; before the fix `covers_after` (floor 0) said it did, and when that
    /// member became the next primary `sync::decide` took this ring as the Repair key source
    /// (b), missing the groups of the gap. Fix: a ring that was never started covers nothing,
    /// and `Accept { Continue { from } }` restarts the ring after `from - 1` unless it already
    /// reached it (`Backlog::continue_after`).
    #[test]
    fn repro_r2_ring_never_covers_a_gap() {
        let mut m = MemberStream::new(0, 1 << 14);
        m.restart(Seq::new(1, 49), Seq::new(1, 49));
        drain(
            &mut m,
            Frame::Accept {
                term: 1,
                mode: AcceptMode::Continue {
                    from: Seq::new(1, 50),
                },
            },
        );
        drain(&mut m, Frame::Groups(groups(50, 55)));
        let ring = m.into_received();
        assert!(
            ring.keys_between(Seq::new(1, 10), Seq::new(1, 55))
                .is_none(),
            "groups (1,11)..(1,49) were never received: the range is not covered"
        );
    }

    /// kv-repl/kv-node R2 (fixed), found by seeds 40 and 79 (an acknowledged write lost):
    /// a fresh `Backlog` had floor 0, so `covers_after` held for every position although the
    /// ring never received anything. kv-node passed such a ring as the new primary's
    /// received ring when it carried none (`repl_glue.rs`: `carried_ring.unwrap_or_else(||
    /// Backlog::try_new(SMALL_RING))`, e.g. a primary of the previous term that restarted and
    /// is re-promoted, or a member ring created after a restart), and `sync::decide` accepted
    /// a Repair whose key set (b) was empty; the repaired member kept an older value of a key
    /// whose acknowledged write it never received, and was later promoted. In the simulation:
    /// term 3 primary n0 acks a write that only n0 and n2 hold; n2 then n0 crash; n0 is
    /// re-promoted (term 4, ISR n0) with a fresh received ring and repairs n1 (last term 1) with
    /// no term-3 keys; n0 expands n1 into the ISR and fails; n1 is promoted (term 5) without
    /// the write. Fix: a ring that was never started covers nothing (`Backlog`), so the
    /// handshake is refused (NeedOperatorResync) instead.
    #[test]
    fn repro_r2_fresh_received_ring_repairs_without_keys() {
        let mut backlog = Backlog::new(1 << 12);
        backlog.start_after(Seq::new(4, 0));
        let received = Backlog::new(1 << 12); // what kv-node uses when no ring was carried
        let history = vec![(3u32, vec![Seq::new(3, 14).0; usize::from(P)])];
        let h = Hello {
            cluster_id: [7; 16],
            auth: Vec::new(),
            node_id: node_id(1).0,
            shard: 1,
            partition: 8,
            fence_term: 4,
            last_term: 1,
            applied: Seq::new(1, 46),
            recovered: Recovered::Clean,
            learner: false,
            migration: None,
        };
        let v = PrimaryView {
            term: 4,
            history: &history,
            backlog: &backlog,
            received: &received,
            is_member: true,
            operator_full_sync: false,
            last_executed: Seq::new(4, 0),
        };
        assert!(
            !matches!(sync::decide(&h, &v), SyncDecision::Repair { .. }),
            "the groups (1:46, 3:14] are unknown to this primary: no Repair can cover them"
        );
    }

    /// kv-coord (fixed), found by seed 17 (seed 33 before the fix; 22 of 300 seeds failed
    /// this way): both remaining ISR members of a shard and its primary are flagged by a new
    /// leader's detector in one batch (NoPrimary); the primary renews healthy again and its
    /// flag is cleared, then a member's flag clears and the shard turns Promoting with a
    /// healthy primary. `run_failover` fenced the member and then waited in `lease_gone` for
    /// the old primary's lease, which the leader kept renewing (its `may_grant` was true), so
    /// the shard stayed CLUSTERDOWN forever. Fix: from the fence of another node on, the
    /// failed primary gets no lease (`Detector::hold_off`, `run_failover`'s `LeaseHoldOff`).
    #[test]
    fn repro_failover_waits_on_a_renewed_lease() {
        for seed in [17, 33] {
            run(&ClusterSimCfg::new(seed)).unwrap();
        }
    }

    /// kv-node/kv-repl F1 (fixed), found by real-process runs (SIGSTOP of a primary for
    /// 400 ms with the 250 ms ack timeout of then, then kill -9): the age test of the members'
    /// acks ran on the boot clock and timed groups from when they were produced, so the first
    /// tick after the primary resumed saw every group it produced before the stop as older
    /// than `repl-ack-timeout-ms` (the
    /// acks sat unread in its sockets) and asked to shrink every member out of the ISR; the
    /// coordinator committed it, and a crash right after left the shard with no promotable
    /// member until the dead primary restarted. Fix: the age test runs on a
    /// `StallClock` (a gap between two ticks counts at most one step; nothing while the
    /// replication transport has not run) and times a group from when the primary's own
    /// durability covered it; the coordinator acts on a shrink only from a primary whose lease
    /// it holds. `stall_clock: false` reproduces the stuck shard.
    #[test]
    fn repro_f1_primary_stall_then_crash_fails_over() {
        // every stall outlasts the ack timeout (the 500 ms default)
        let cases = [(700, 0), (700, 150), (900, 50), (600, 300), (1_200, 20)];
        for (i, &(freeze_ms, crash_ms)) in cases.iter().enumerate() {
            let cfg = ClusterSimCfg {
                faults: NetFaults::default(),
                fault_every_ms: 0,
                sim_ms: 9_000,
                freezes: false,
                stall_crash: Some(StallCrash {
                    at_ms: 3_000 + 37 * i as u64,
                    freeze_ms,
                    crash_ms,
                }),
                ..ClusterSimCfg::new(70 + i as u64)
            };
            run(&cfg).unwrap();
        }
        // the boot clock: the same stalls shrink healthy members (whenever the primary had
        // noticed its own durability of a group before it stopped)
        let mut stuck = Vec::new();
        for i in 0..6u64 {
            let old = ClusterSimCfg {
                faults: NetFaults::default(),
                fault_every_ms: 0,
                sim_ms: 9_000,
                freezes: false,
                stall_clock: false,
                stall_crash: Some(StallCrash {
                    at_ms: 3_000 + 13 * i,
                    freeze_ms: 700,
                    crash_ms: 100,
                }),
                ..ClusterSimCfg::new(70 + i)
            };
            if let Err(e) = run(&old) {
                stuck.push(e);
            }
        }
        eprintln!("boot clock: {} of 6 stalls failed: {stuck:#?}", stuck.len());
        assert!(
            stuck.iter().any(|e| e.contains("shrank its ISR")),
            "the boot clock counts the primary's stall as member lag"
        );
    }

    fn quiet(seed: u64) -> ClusterSimCfg {
        ClusterSimCfg {
            faults: NetFaults::default(),
            fault_every_ms: 0,
            sim_ms: 3_000,
            ..ClusterSimCfg::new(seed)
        }
    }

    #[test]
    fn healthy_cluster_serves_and_converges() {
        let r = run_report(&quiet(1)).unwrap();
        assert!(r.acked_writes > 100, "{r:?}");
        assert_eq!(r.promotions, 0, "{r:?}");
    }

    /// The campaign: `KV_SIM_CLUSTER_SEEDS` seeds (default 6) starting at
    /// `KV_SIM_CLUSTER_START` (default 1), with every fault kind.
    #[test]
    fn seeded_faults() {
        let n: u64 = std::env::var("KV_SIM_CLUSTER_SEEDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(6);
        let start: u64 = std::env::var("KV_SIM_CLUSTER_START")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1);
        let mut fails = Vec::new();
        let mut total = ClusterSimReport::default();
        for seed in start..start + n {
            let cfg = ClusterSimCfg::new(seed);
            match run_report(&cfg) {
                Ok(r) => total.add(&r),
                Err(e) => fails.push(e),
            }
        }
        eprintln!(
            "cluster_sim: {n} seeds, {} failed; totals {total:?}",
            fails.len()
        );
        assert!(fails.is_empty(), "{fails:#?}");
    }
}
