//! Stateright model of whole-state replication (DESIGN §23.3, §25.3).
//!
//! The model drives the coordinator's real state machine, `kv_coord::wsr::core::Core`, with
//! abstract state values: a value is the list of the terms that produced each version on top of
//! the proposer's initial value, so "value B extends value A" is "A is a prefix of B" and a lost
//! commit shows up as a broken prefix. Each member has a disk (its `coord.meta` and
//! `coord.state`) that is updated only by the core's persistence outputs; a crash rebuilds the
//! core from the disk, so a missing persistence step is caught as a lost vote or acceptance.
//!
//! Explored behaviours: 3 and 5 members; message loss (any message may be dropped), reordering
//! (any message may be delivered next), optional duplication (delivery keeps the message);
//! crash/restart with the A/B files (atomic writes: a torn half recovers the previous value,
//! which is a crash before the write); election timeouts, beats, check-quorum step-downs and
//! proposals at any moment; and operator SETPEERS sequences of single-server deltas applied to
//! the members of the old and the new list in any order, the next change starting only when
//! every member of the current list has reported the current generation, a leader of the current
//! generation has committed a value, and every member staying in the list has committed it
//! ([`WsrState::may_start`]; kvadm's sequence, a strengthening of §23.3 that this model showed
//! necessary).
//!
//! Properties: at most one value committed per version; committed state never lost (every
//! committed value extends every committed value of a lower version); leader completeness (a
//! leader of term T holds every value committed in a term below T); at most one leader per term
//! across peer generations; plus the structural invariant `accepted.version > committed.version`.
//!
//! [`simulate`] runs the same members over [`crate::simnet::SimNet`] with seeded faults for many
//! more steps than the exhaustive checker reaches, checking the same properties after every
//! step.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use kv_coord::wsr::core::{Core, Entry, Input, Output, Role};
use kv_coord::wsr::peers::{CoordRecord, PeerList};
use kv_coord::wsr::rpc::WsrMsg;
use kv_ctrl::CoordId;
use stateright::{Model, Property};

/// An abstract state value: the proposer's initial marker followed by one term per version.
pub type Val = Vec<u8>;

/// Model parameters.
#[derive(Clone, Debug)]
pub struct WsrModel {
    /// Members of generation 1 (`0..members`).
    pub members: u8,
    /// Further members that start as joiners (generation 0, empty list), numbered after the
    /// generation-1 members.
    pub joiners: u8,
    /// The member lists of generations 2, 3, ...: each differs from the previous one by
    /// exactly one member.
    pub generations: Vec<Vec<u8>>,
    /// No election beyond this term.
    pub max_term: u64,
    /// No proposal beyond this version.
    pub max_version: u64,
    /// Crash/restart budget over a whole run.
    pub max_crashes: u8,
    /// Delivery keeps the message (duplication) when true.
    pub duplicate: bool,
    /// Messages in flight; further sends are lost.
    pub max_net: usize,
    /// Explore leader beats and check-quorum step-downs.
    pub beats: bool,
    /// Explore dropping any message in flight (without it, loss is a message never
    /// delivered).
    pub drops: bool,
    /// The operator's sync commit before each peer-list change ([`WsrState::may_start`]);
    /// `false` models the generation-only rule, which the checker shows unsafe.
    pub sync_rule: bool,
}

impl WsrModel {
    /// `members` members, no peer-list change.
    pub fn new(members: u8, max_term: u64, max_version: u64) -> Self {
        WsrModel {
            members,
            joiners: 0,
            generations: Vec::new(),
            max_term,
            max_version,
            max_crashes: 1,
            duplicate: false,
            max_net: 8,
            beats: true,
            drops: true,
            sync_rule: true,
        }
    }

    fn total(&self) -> usize {
        usize::from(self.members + self.joiners)
    }

    /// The list of generation `g` (1-based).
    fn list(&self, g: u64) -> Vec<u8> {
        if g <= 1 {
            (0..self.members).collect()
        } else {
            self.generations[(g - 2) as usize].clone()
        }
    }

    fn peer_list(&self, g: u64) -> Arc<PeerList> {
        Arc::new(PeerList {
            r#gen: g,
            peers: self.list(g).into_iter().map(record).collect(),
        })
    }

    fn max_gen(&self) -> u64 {
        1 + self.generations.len() as u64
    }
}

/// The id of member `i` (ids are opaque bytes; they are never compared for order).
pub fn coord_id(i: u8) -> CoordId {
    CoordId([i + 1; 20])
}

fn index_of(id: &CoordId) -> u8 {
    id.0[0] - 1
}

fn record(i: u8) -> CoordRecord {
    let a = |p: u16| SocketAddr::from(([127, 0, 0, 1], p + u16::from(i)));
    CoordRecord::new(coord_id(i), a(7100), a(7000), a(7200))
}

/// What a member's state files hold.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Disk {
    pub term: u64,
    pub voted_for: Option<CoordId>,
    pub peers: Arc<PeerList>,
    pub committed: Entry<Val>,
    pub accepted: Option<Entry<Val>>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Member {
    pub core: Core<Val>,
    pub disk: Disk,
}

impl Member {
    fn new(i: u8, peers: Arc<PeerList>) -> Self {
        let committed = Entry {
            version: 1,
            term: 0,
            value: vec![100 + i],
        };
        let disk = Disk {
            term: 0,
            voted_for: None,
            peers: peers.clone(),
            committed: committed.clone(),
            accepted: None,
        };
        Member {
            core: Core::recover(coord_id(i), 0, None, peers, committed, None),
            disk,
        }
    }

    fn restart(&mut self) {
        let d = self.disk.clone();
        self.core = Core::recover(
            self.core.id(),
            d.term,
            d.voted_for,
            d.peers,
            d.committed,
            d.accepted,
        );
    }

    /// The value of the member's `last` entry.
    fn last_value(&self) -> &Val {
        match self.core.accepted() {
            Some(a) if (a.term, a.version) > (self.core.committed().term, self.core.committed().version) => &a.value,
            _ => &self.core.committed().value,
        }
    }
}

/// One message in flight.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Envelope {
    pub from: u8,
    pub to: u8,
    pub msg: WsrMsg<Val>,
}

impl fmt::Debug for Envelope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}->{} {:?}", self.from, self.to, self.msg)
    }
}

/// Global state of the modelled group, with ghost records for the properties.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct WsrState {
    pub members: Vec<Member>,
    pub net: BTreeSet<Envelope>,
    /// Every commit any member made: `(version, value, term of the committing proposal)`.
    pub chosen: BTreeSet<(u64, Val, u64)>,
    /// Every election won: `(term, member)`.
    pub leaders: BTreeSet<(u64, u8)>,
    /// Per peer generation: the highest version a leader of that generation committed (the
    /// operator's sync commit before the next peer-list change, see [`WsrState::may_start`]).
    pub gen_commits: BTreeMap<u64, u64>,
    pub crashes: u8,
}

/// One modelled step.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum WsrAction {
    Timeout(u8),
    Beat(u8),
    CheckQuorum(u8),
    Propose(u8),
    Deliver(Envelope),
    Drop(Envelope),
    Crash(u8),
    /// The operator gives member `.0` the list of generation `.1`.
    SetPeers(u8, u64),
}

impl WsrState {
    /// Steps member `i`, applies its persistence outputs to its disk first, then queues its
    /// messages and records ghost commits and elections. Returns whether the step restarts the
    /// member's election timer (leader contact or a granted vote).
    fn step(&mut self, m: &WsrModel, i: u8, input: Input<Val>) -> bool {
        self.step_collect(m, i, input, &mut Vec::new())
    }

    /// [`WsrState::step`] that also appends the commits the step made to `fresh`.
    fn step_collect(
        &mut self,
        m: &WsrModel,
        i: u8,
        input: Input<Val>,
        fresh: &mut Vec<(u64, Val, u64)>,
    ) -> bool {
        let mut out = Vec::new();
        let mut reset = false;
        let member = &mut self.members[usize::from(i)];
        member.core.step(input, &mut out);
        // Persistence first (the driver's rule), then everything else.
        for o in &out {
            match o {
                Output::PersistMeta => {
                    let (term, voted_for, _) = member.core.meta();
                    member.disk.term = term;
                    member.disk.voted_for = voted_for;
                    member.disk.peers = member.core.peers_arc();
                }
                Output::PersistState => {
                    member.disk.committed = member.core.committed().clone();
                    member.disk.accepted = member.core.accepted().cloned();
                }
                _ => {}
            }
        }
        for o in out {
            match o {
                Output::Send { to, msg } => {
                    if self.net.len() < m.max_net {
                        self.net.insert(Envelope {
                            from: i,
                            to: index_of(&to),
                            msg,
                        });
                    }
                }
                Output::Commit { version } => {
                    let c = self.members[usize::from(i)].core.committed();
                    debug_assert_eq!(c.version, version);
                    fresh.push((c.version, c.value.clone(), c.term));
                    self.chosen.insert((c.version, c.value.clone(), c.term));
                    let me = &self.members[usize::from(i)].core;
                    if me.role() == Role::Leader {
                        let e = self.gen_commits.entry(me.peers().r#gen).or_insert(0);
                        *e = (*e).max(version);
                    }
                }
                Output::BecameLeader { term } => {
                    self.leaders.insert((term, i));
                }
                Output::LeaderContact { .. } | Output::VoteGranted => reset = true,
                _ => {}
            }
        }
        reset
    }

    fn gen_of(&self, i: usize) -> u64 {
        self.members[i].core.peers().r#gen
    }

    /// The operator precondition for starting peer generation `g` (kvadm's SETPEERS sequence):
    /// every member of list `g - 1` reports generation `g - 1`, a leader of generation `g - 1`
    /// committed a value (kvadm's sync commit), and every member of both lists has committed at
    /// least that version. Generation alone is not enough: without the sync, the lists of
    /// generations `g - 2` and `g` (two servers apart) can both assemble a majority of members
    /// that never saw each other's commits (found by this model: 3 -> 4 -> 3 elects two leaders
    /// in one term).
    fn may_start(&self, m: &WsrModel, g: u64) -> bool {
        let prev = m.list(g - 1);
        let next = m.list(g);
        if !prev.iter().all(|&p| self.gen_of(usize::from(p)) >= g - 1) {
            return false;
        }
        if !m.sync_rule {
            return true;
        }
        let Some(&v) = self.gen_commits.get(&(g - 1)) else {
            return false;
        };
        prev.iter()
            .filter(|p| next.contains(p))
            .all(|&p| self.members[usize::from(p)].core.committed().version >= v)
    }
}

fn is_prefix(a: &Val, b: &Val) -> bool {
    a.len() <= b.len() && b[..a.len()] == a[..]
}

/// At most one leader per term (across generations).
fn one_leader_per_term(_: &WsrModel, s: &WsrState) -> bool {
    let v: Vec<&(u64, u8)> = s.leaders.iter().collect();
    v.windows(2).all(|w| w[0].0 != w[1].0)
}

/// At most one value committed per version.
fn one_value_per_version(_: &WsrModel, s: &WsrState) -> bool {
    let v: Vec<&(u64, Val, u64)> = s.chosen.iter().collect();
    v.windows(2).all(|w| w[0].0 != w[1].0 || w[0].1 == w[1].1)
}

/// Committed state is never lost: every committed value extends every committed value of a
/// lower version, and every member's committed value (from version 2 on) is one of them.
fn committed_never_lost(_: &WsrModel, s: &WsrState) -> bool {
    for (va, a, _) in &s.chosen {
        for (vb, b, _) in &s.chosen {
            if va <= vb && !is_prefix(a, b) {
                return false;
            }
        }
    }
    s.members.iter().all(|m| {
        let c = &m.disk.committed;
        c.version < 2 || s.chosen.iter().any(|(v, val, _)| *v == c.version && *val == c.value)
    })
}

/// A leader of term T holds every value committed in a term below T.
fn leader_completeness(_: &WsrModel, s: &WsrState) -> bool {
    s.members.iter().all(|m| {
        m.core.role() != Role::Leader
            || s.chosen
                .iter()
                .filter(|(_, _, t)| *t < m.core.term())
                .all(|(_, val, _)| is_prefix(val, m.last_value()))
    })
}

/// `accepted.version > committed.version` on every member, in RAM and on disk.
fn accepted_above_committed(_: &WsrModel, s: &WsrState) -> bool {
    s.members.iter().all(|m| {
        m.core
            .accepted()
            .is_none_or(|a| a.version > m.core.committed().version)
            && m.disk
                .accepted
                .as_ref()
                .is_none_or(|a| a.version > m.disk.committed.version)
    })
}

impl Model for WsrModel {
    type State = WsrState;
    type Action = WsrAction;

    fn init_states(&self) -> Vec<Self::State> {
        let mut members = Vec::new();
        for i in 0..self.members {
            members.push(Member::new(i, self.peer_list(1)));
        }
        for j in 0..self.joiners {
            members.push(Member::new(self.members + j, Arc::new(PeerList::joining())));
        }
        vec![WsrState {
            members,
            net: BTreeSet::new(),
            chosen: BTreeSet::new(),
            leaders: BTreeSet::new(),
            gen_commits: BTreeMap::new(),
            crashes: 0,
        }]
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        for (i, m) in s.members.iter().enumerate() {
            let i = i as u8;
            let c = &m.core;
            match c.role() {
                Role::Leader => {
                    if self.beats {
                        actions.push(WsrAction::Beat(i));
                        actions.push(WsrAction::CheckQuorum(i));
                    }
                    if c.can_propose() && c.committed().version < self.max_version {
                        actions.push(WsrAction::Propose(i));
                    }
                }
                _ => {
                    if c.is_voter() && c.term() < self.max_term {
                        actions.push(WsrAction::Timeout(i));
                    }
                }
            }
            if s.crashes < self.max_crashes {
                actions.push(WsrAction::Crash(i));
            }
        }
        for e in &s.net {
            actions.push(WsrAction::Deliver(e.clone()));
            if self.drops {
                actions.push(WsrAction::Drop(e.clone()));
            }
        }
        // SETPEERS: generation g starts only once its precondition holds.
        for g in 2..=self.max_gen() {
            let prev = self.list(g - 1);
            if !s.may_start(self, g) {
                continue;
            }
            let next = self.list(g);
            for i in 0..self.total() {
                let i8 = i as u8;
                let cur = s.gen_of(i);
                let in_lists = prev.contains(&i8) || next.contains(&i8);
                let eligible = cur == g - 1 || (s.members[i].core.peers().is_joining() && next.contains(&i8));
                if in_lists && eligible {
                    actions.push(WsrAction::SetPeers(i8, g));
                }
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match action {
            WsrAction::Timeout(i) => {
                s.step(self, i, Input::ElectionTimeout);
            }
            WsrAction::Beat(i) => {
                s.step(self, i, Input::BeatTimeout { round: 0 });
            }
            WsrAction::CheckQuorum(i) => {
                s.step(self, i, Input::CheckQuorum);
            }
            WsrAction::Propose(i) => {
                let c = &s.members[usize::from(i)].core;
                let mut value = c.committed().value.clone();
                value.push(c.term() as u8);
                s.step(self, i, Input::Propose { value });
            }
            WsrAction::Deliver(e) => {
                if !self.duplicate {
                    s.net.remove(&e);
                }
                s.step(
                    self,
                    e.to,
                    Input::Msg {
                        from: coord_id(e.from),
                        msg: e.msg,
                    },
                );
            }
            WsrAction::Drop(e) => {
                s.net.remove(&e);
            }
            WsrAction::Crash(i) => {
                s.crashes += 1;
                s.members[usize::from(i)].restart();
            }
            WsrAction::SetPeers(i, g) => {
                let list = self.peer_list(g);
                s.step(self, i, Input::SetPeers(list));
            }
        }
        if s == *last { None } else { Some(s) }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("at most one leader per term", one_leader_per_term),
            Property::always("at most one value committed per version", one_value_per_version),
            Property::always("committed state never lost", committed_never_lost),
            Property::always("leader completeness", leader_completeness),
            Property::always("accepted above committed", accepted_above_committed),
            Property::sometimes("a value is committed", some_commit),
            Property::sometimes("a second term commits", second_term_commits),
            Property::sometimes("the last generation commits", last_generation_commits),
        ]
    }
}

fn some_commit(_: &WsrModel, s: &WsrState) -> bool {
    !s.chosen.is_empty()
}

fn second_term_commits(_: &WsrModel, s: &WsrState) -> bool {
    s.chosen.iter().any(|(_, _, t)| *t >= 2)
}

/// A leader whose list is the last generation commits in its own term.
fn last_generation_commits(m: &WsrModel, s: &WsrState) -> bool {
    let g = m.max_gen();
    g > 1
        && s.members.iter().any(|x| {
            x.core.peers().r#gen == g
                && x.core.role() == Role::Leader
                && s.chosen.iter().any(|(_, _, t)| *t == x.core.term())
        })
}

/// Checks every `always` property of the model on one state; the name of the first violated
/// one.
pub fn violated(m: &WsrModel, s: &WsrState) -> Option<&'static str> {
    m.properties()
        .into_iter()
        .filter(|p| matches!(p.expectation, stateright::Expectation::Always))
        .find(|p| !(p.condition)(m, s))
        .map(|p| p.name)
}

// ---------------------------------------------------------------------------------------------
// Deterministic simulation over the simulated network.

/// The `always` properties checked incrementally, for runs with thousands of commits (the
/// exhaustive checker's whole-state functions are quadratic in the number of commits).
#[derive(Default)]
struct Incremental {
    /// The value committed at each version.
    by_version: BTreeMap<u64, Val>,
    /// Per proposal term: the highest version committed by a proposal of that term, and its value.
    by_term: BTreeMap<u64, (u64, Val)>,
}

impl Incremental {
    fn add(&mut self, version: u64, val: &Val, term: u64) -> Option<&'static str> {
        match self.by_version.get(&version) {
            Some(x) if x != val => return Some("at most one value committed per version"),
            Some(_) => {}
            None => {
                // Prefix order is transitive: the neighbours decide.
                if let Some((_, lo)) = self.by_version.range(..version).next_back()
                    && !is_prefix(lo, val)
                {
                    return Some("committed state never lost");
                }
                if let Some((_, hi)) = self.by_version.range(version + 1..).next()
                    && !is_prefix(val, hi)
                {
                    return Some("committed state never lost");
                }
                self.by_version.insert(version, val.clone());
            }
        }
        let e = self.by_term.entry(term).or_insert_with(|| (version, val.clone()));
        if version > e.0 {
            *e = (version, val.clone());
        }
        None
    }

    fn check(&self, s: &WsrState) -> Option<&'static str> {
        if !one_leader_per_term(&WsrModel::new(1, 0, 0), s) {
            return Some("at most one leader per term");
        }
        for m in &s.members {
            let c = &m.disk.committed;
            if c.version >= 2 && self.by_version.get(&c.version) != Some(&c.value) {
                return Some("committed state never lost");
            }
            if m.core.role() == Role::Leader {
                // The newest value committed below the leader's term must be in its last value.
                let newest = self
                    .by_term
                    .range(..m.core.term())
                    .map(|(_, e)| e)
                    .max_by_key(|e| e.0);
                if newest.is_some_and(|(_, v)| !is_prefix(v, m.last_value())) {
                    return Some("leader completeness");
                }
            }
        }
        if !accepted_above_committed(&WsrModel::new(1, 0, 0), s) {
            return Some("accepted above committed");
        }
        None
    }
}

/// Parameters of one simulated run.
#[derive(Clone, Debug)]
pub struct SimCfg {
    pub seed: u64,
    pub members: u8,
    pub steps: u64,
    pub faults: crate::simnet::NetFaults,
    /// Per-step probability of a crash/restart of a random member.
    pub crash: f64,
    /// Peer-list generations to walk through (as in [`WsrModel::generations`]); joiners are
    /// the members named in them beyond `members`.
    pub generations: Vec<Vec<u8>>,
}

/// Outcome of a simulated run.
#[derive(Clone, Debug, Default)]
pub struct SimReport {
    pub commits: u64,
    pub max_version: u64,
    pub max_term: u64,
    pub elections: u64,
    pub crashes: u64,
    pub messages: u64,
    pub generation: u64,
}

/// Runs the real core over the simulated network with seeded faults, checking the model's
/// properties after every step. Timers fire at simulated times drawn like the real ones
/// (election 500-1000 ms, beats every 100 ms, check-quorum every 500 ms).
pub fn simulate(cfg: &SimCfg) -> Result<SimReport, String> {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use crate::simnet::SimNet;

    const MS: u64 = 1_000_000;
    let joiners = cfg
        .generations
        .iter()
        .flatten()
        .map(|&x| x + 1)
        .max()
        .unwrap_or(0)
        .saturating_sub(cfg.members);
    let model = WsrModel {
        members: cfg.members,
        joiners,
        generations: cfg.generations.clone(),
        max_term: u64::MAX,
        max_version: u64::MAX,
        max_crashes: u8::MAX,
        duplicate: false,
        max_net: usize::MAX,
        beats: true,
        drops: true,
        sync_rule: true,
    };
    let mut s = model.init_states().remove(0);
    let n = model.total();
    let mut rng = StdRng::seed_from_u64(cfg.seed ^ 0x5753_525f_5349_4d00);
    let mut net: SimNet<(u8, WsrMsg<Val>)> = SimNet::new(cfg.seed, cfg.faults);
    net.add_process(n - 1);
    let mut election_at: Vec<u64> = (0..n)
        .map(|i| net.after(i, rng.random_range(500 * MS..1000 * MS)))
        .collect();
    let mut beat_at = vec![0u64; n];
    let mut check_at = vec![0u64; n];
    let mut was_leader = vec![false; n];
    let mut round = 0u64;
    let mut report = SimReport::default();
    let mut next_gen = 2u64;
    let mut inc = Incremental::default();
    let mut fresh = Vec::new();

    // Hands the messages a step queued to the simulated network.
    fn flush(s: &mut WsrState, net: &mut SimNet<(u8, WsrMsg<Val>)>) {
        for e in std::mem::take(&mut s.net) {
            net.send(usize::from(e.from), usize::from(e.to), (e.from, e.msg));
        }
    }

    for _ in 0..cfg.steps {
        let mut next_timer = u64::MAX;
        for i in 0..n {
            let t = if s.members[i].core.role() == Role::Leader {
                beat_at[i].min(check_at[i])
            } else {
                election_at[i]
            };
            next_timer = next_timer.min(t);
        }
        let next_msg = net.next_delivery_time().unwrap_or(u64::MAX);
        let before = (s.chosen.len(), s.leaders.len());
        let mut resets = Vec::new();
        if next_msg <= next_timer {
            if let Some((to, (from, msg))) = net.deliver_next() {
                report.messages += 1;
                let to8 = to as u8;
                if s.step_collect(&model, to8, Input::Msg { from: coord_id(from), msg }, &mut fresh) {
                    resets.push(to);
                }
            }
        } else {
            net.advance_to(next_timer);
            let now = net.now();
            for i in 0..n {
                let i8 = i as u8;
                if s.members[i].core.role() == Role::Leader {
                    if beat_at[i] <= now {
                        round += 1;
                        s.step_collect(&model, i8, Input::BeatTimeout { round }, &mut fresh);
                        beat_at[i] = net.after(i, 100 * MS);
                    }
                    if check_at[i] <= now {
                        s.step_collect(&model, i8, Input::CheckQuorum, &mut fresh);
                        check_at[i] = net.after(i, 500 * MS);
                    }
                } else if election_at[i] <= now {
                    s.step_collect(&model, i8, Input::ElectionTimeout, &mut fresh);
                    election_at[i] = net.after(i, rng.random_range(500 * MS..1000 * MS));
                }
            }
        }
        flush(&mut s, &mut net);
        // Proposals, crashes and operator peer changes.
        for i in 0..n {
            let c = &s.members[i].core;
            if c.can_propose() && rng.random_bool(0.3) {
                let mut value = c.committed().value.clone();
                value.push((c.term() % 251) as u8);
                s.step_collect(&model, i as u8, Input::Propose { value }, &mut fresh);
            }
        }
        if rng.random_bool(cfg.crash) {
            let i = rng.random_range(0..n);
            s.members[i].restart();
            net.crash(i);
            net.restart(i);
            resets.push(i);
            report.crashes += 1;
        }
        if next_gen <= model.max_gen() && rng.random_bool(0.01) {
            let prev = model.list(next_gen - 1);
            let list = model.list(next_gen);
            if s.may_start(&model, next_gen) {
                let cands: Vec<usize> = (0..n)
                    .filter(|&i| {
                        let i8 = i as u8;
                        (prev.contains(&i8) || list.contains(&i8))
                            && (s.gen_of(i) == next_gen - 1
                                || (s.members[i].core.peers().is_joining() && list.contains(&i8)))
                    })
                    .collect();
                if !cands.is_empty() {
                    let i = cands[rng.random_range(0..cands.len())];
                    s.step_collect(&model, i as u8, Input::SetPeers(model.peer_list(next_gen)), &mut fresh);
                }
                if list.iter().all(|&p| s.gen_of(usize::from(p)) >= next_gen) {
                    report.generation = next_gen;
                    next_gen += 1;
                }
            }
        }
        flush(&mut s, &mut net);
        for i in resets {
            election_at[i] = net.after(i, rng.random_range(500 * MS..1000 * MS));
        }
        for i in 0..n {
            let leader = s.members[i].core.role() == Role::Leader;
            if leader && !was_leader[i] {
                beat_at[i] = net.now();
                check_at[i] = net.after(i, 500 * MS);
            } else if !leader && was_leader[i] {
                election_at[i] = net.after(i, rng.random_range(500 * MS..1000 * MS));
            }
            was_leader[i] = leader;
        }
        report.commits += (s.chosen.len() - before.0) as u64;
        report.elections += (s.leaders.len() - before.1) as u64;
        for (v, val, t) in fresh.drain(..) {
            if let Some(p) = inc.add(v, &val, t) {
                return Err(format!("seed {}: property violated: {p}", cfg.seed));
            }
        }
        if let Some(p) = inc.check(&s) {
            return Err(format!("seed {}: property violated: {p}", cfg.seed));
        }
    }
    report.max_version = s.chosen.iter().map(|c| c.0).max().unwrap_or(1);
    report.max_term = s.leaders.iter().map(|l| l.0).max().unwrap_or(0);
    Ok(report)
}

/// Result of one model-checking run.
#[derive(Clone, Copy, Debug)]
pub struct CheckReport {
    pub unique_states: usize,
    pub states: usize,
    pub max_depth: usize,
    /// The run ended before its time limit: every reachable state within the bounds (and the
    /// depth limit, if any) was visited.
    pub complete: bool,
    /// The `sometimes` properties for which an example was found.
    pub examples: usize,
}

/// Model-checks `m` depth first (fingerprints only, so memory stays small) for at most
/// `timeout`, panicking with the counterexample path if an `always` property fails.
pub fn check(m: &WsrModel, timeout: std::time::Duration, depth: Option<usize>) -> CheckReport {
    use stateright::Checker;
    let threads = std::thread::available_parallelism().map_or(1, |n| n.get());
    let mut b = m.clone().checker().threads(threads).timeout(timeout);
    if let Some(d) = depth {
        b = b.target_max_depth(d);
    }
    let start = std::time::Instant::now();
    let c = b.spawn_dfs().join();
    let complete = start.elapsed() < timeout;
    let mut examples = 0;
    for p in m.properties() {
        let found = c.discovery(p.name);
        match p.expectation {
            stateright::Expectation::Always => {
                if let Some(path) = found {
                    panic!(
                        "wsr_model {}: counterexample:\n{:#?}",
                        p.name,
                        path.into_actions()
                    );
                }
            }
            _ => examples += usize::from(found.is_some()),
        }
    }
    CheckReport {
        unique_states: c.unique_state_count(),
        states: c.state_count(),
        max_depth: c.max_depth(),
        complete,
        examples,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::simnet::NetFaults;

    /// Debug builds explore smaller bounds (the checker is ~20x slower there).
    fn release() -> bool {
        !cfg!(debug_assertions)
    }

    fn cfg(n: u8, t: u64, v: u64, beats: bool, crashes: u8, net: usize, drops: bool) -> WsrModel {
        let mut m = WsrModel::new(n, t, v);
        m.beats = beats;
        m.max_crashes = crashes;
        m.max_net = net;
        m.drops = drops;
        m
    }

    fn run(name: &str, m: &WsrModel, secs: u64) -> CheckReport {
        let t = std::time::Instant::now();
        let r = check(m, Duration::from_secs(secs), None);
        eprintln!(
            "wsr_model {name}: {} unique states ({} generated), max depth {}, {} in {:.1?}, {} examples",
            r.unique_states,
            r.states,
            r.max_depth,
            if r.complete { "complete" } else { "time-bounded" },
            t.elapsed(),
            r.examples
        );
        r
    }

    /// Three members, two terms, two versions: the whole state space within the bounds.
    #[test]
    fn three_members_complete() {
        let m = if release() {
            cfg(3, 2, 2, false, 0, 3, false)
        } else {
            cfg(3, 2, 2, false, 0, 2, false)
        };
        let r = run("3 members (complete)", &m, 600);
        assert!(r.complete);
        assert!(r.examples >= 2, "commits in two terms must be reachable");
    }

    /// Three members with crash/restart from the disk, message loss, duplication, beats and
    /// check-quorum step-downs, three terms: time-bounded.
    #[test]
    fn three_members_crash_loss_duplication() {
        let mut m = cfg(3, 3, 3, true, 2, 5, true);
        m.duplicate = true;
        let r = run("3 members crash/loss/dup", &m, if release() { 60 } else { 20 });
        assert!(r.unique_states > 10_000);
    }

    /// Five members, two terms (one in debug builds), two versions, at most two messages in
    /// flight: the whole state space within the bounds.
    #[test]
    fn five_members_complete() {
        let m = cfg(5, if release() { 2 } else { 1 }, 2, false, 0, 2, false);
        let r = run("5 members (complete)", &m, 900);
        assert!(r.complete);
        assert!(r.examples >= 1);
    }

    /// SETPEERS 3 -> 4 -> 3 with small bounds: the whole state space.
    #[test]
    fn setpeers_complete() {
        let mut m = cfg(3, 2, 4, false, 0, if release() { 2 } else { 1 }, false);
        m.joiners = 1;
        m.generations = vec![vec![0, 1, 2, 3], vec![1, 2, 3]];
        let r = run("SETPEERS 3 -> 4 -> 3 (complete)", &m, 900);
        assert!(r.complete);
    }

    /// The generation-only rule of the original design (start the next change once every member
    /// reports the current generation) is unsafe: 3 -> 4 -> 3 makes the lists of generations 1
    /// and 3 disjoint majorities of members that never saw each other's commits, and the checker
    /// finds two leaders in one term. The sync commit of [`WsrState::may_start`] prevents it.
    #[test]
    fn generation_only_rule_is_unsafe() {
        use stateright::Checker;
        let mut m = cfg(3, 2, 2, false, 0, 1, false);
        m.joiners = 1;
        m.generations = vec![vec![0, 1, 2, 3], vec![1, 2, 3]];
        m.sync_rule = false;
        let c = m.checker().spawn_bfs().join();
        assert!(c.discovery("at most one leader per term").is_some());
    }

    /// Five members: time-bounded.
    #[test]
    fn five_members() {
        let m = cfg(5, 2, 3, false, 1, 5, true);
        let r = run("5 members", &m, if release() { 60 } else { 20 });
        assert!(r.unique_states > 10_000);
    }

    /// SETPEERS: add a fourth coordinator (a joiner), then remove the first one, applied to
    /// the members in any order; elections and commits in every generation.
    #[test]
    fn setpeers_single_server_deltas() {
        let mut m = cfg(3, 3, 4, false, 1, 4, true);
        m.joiners = 1;
        m.generations = vec![vec![0, 1, 2, 3], vec![1, 2, 3]];
        let r = run("SETPEERS 3 -> 4 -> 3", &m, if release() { 60 } else { 20 });
        assert!(r.unique_states > 10_000);
    }

    /// The same deltas from five members: add a sixth, remove the first.
    #[test]
    fn setpeers_from_five_members() {
        let mut m = cfg(5, 3, 4, false, 0, 4, false);
        m.joiners = 1;
        m.generations = vec![vec![0, 1, 2, 3, 4, 5], vec![1, 2, 3, 4, 5]];
        let r = run("SETPEERS 5 -> 6 -> 5", &m, if release() { 30 } else { 10 });
        assert!(r.unique_states > 10_000);
    }

    fn simulate_seeds(members: u8, generations: Vec<Vec<u8>>, seeds: u64, steps: u64) {
        let mut total = SimReport::default();
        for seed in 0..seeds {
            let r = simulate(&SimCfg {
                seed,
                members,
                steps,
                faults: NetFaults::hostile(),
                crash: 0.002,
                generations: generations.clone(),
            })
            .unwrap_or_else(|e| panic!("{e}"));
            total.commits += r.commits;
            total.elections += r.elections;
            total.crashes += r.crashes;
            total.messages += r.messages;
            total.max_term = total.max_term.max(r.max_term);
            total.max_version = total.max_version.max(r.max_version);
            total.generation = total.generation.max(r.generation);
        }
        eprintln!("wsr simulation {members} members, {seeds} seeds x {steps} steps: {total:?}");
        assert!(total.commits > seeds, "the simulated group must make progress");
        assert!(total.elections >= seeds);
    }

    #[test]
    fn simulation_three_members() {
        simulate_seeds(3, Vec::new(), if release() { 200 } else { 20 }, 20_000);
    }

    #[test]
    fn simulation_five_members() {
        simulate_seeds(5, Vec::new(), if release() { 100 } else { 10 }, 20_000);
    }

    #[test]
    fn simulation_setpeers() {
        // 3 -> add 3 -> remove 0 -> add 4 -> remove 1
        let gens = vec![vec![0, 1, 2, 3], vec![1, 2, 3], vec![1, 2, 3, 4], vec![2, 3, 4]];
        simulate_seeds(3, gens, if release() { 100 } else { 10 }, 40_000);
    }

    #[test]
    #[ignore = "long model-checking runs for the report; run with --release --ignored --nocapture"]
    fn long_runs() {
        run("3 members t2 v2 drops (complete)", &cfg(3, 2, 2, false, 0, 3, true), 900);
        run("3 members t2 v2 crash (complete)", &cfg(3, 2, 2, false, 1, 3, false), 900);
        let mut m = cfg(3, 3, 3, true, 2, 6, true);
        m.duplicate = true;
        run("3 members t3 v3 beats crash dup", &m, 300);
        run("5 members t3 v3", &cfg(5, 3, 3, true, 1, 6, true), 300);
        let mut m = cfg(3, 3, 4, true, 1, 5, true);
        m.joiners = 1;
        m.generations = vec![vec![0, 1, 2, 3], vec![1, 2, 3]];
        run("SETPEERS 3 -> 4 -> 3", &m, 300);
    }
}
