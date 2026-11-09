//! The WSR state machine (DESIGN §23.3), free of I/O so that the deterministic simulator and
//! the model checker drive the same code (`kv-sim/src/wsr_model.rs`, §25.3).
//!
//! Inputs are [`Input`]s (timer expiries, peer messages, local proposals, operator peer-list
//! changes); outputs are [`Output`]s (send, persist meta, persist state, commit, role changes).
//! A driver MUST carry out the persistence outputs of one step before it sends any message of
//! that step (rules 1 and 2): every message a step emits may depend on what the step persisted.
//!
//! The value `V` is opaque: the encoded `ClusterState` in `kvc`, a small stand-in in the model.
//! The persistent part is `coord.meta` (`term`, `voted_for`, `peers`) and `coord.state`
//! (`committed`, `accepted`); everything else is volatile and reset by [`Core::recover`].
//!
//! Protocol (Raft-style election, a "log" of at most one uncommitted entry, the whole state as
//! the entry):
//! * An entry is identified by `(term, version)`: the leader of a term proposes at most one
//!   value per version. A member's `last` is the larger (term first) of its committed and
//!   accepted entries; it never decreases.
//! * Election: on its timeout a member asks for pre-votes for `term + 1`; with a majority of its
//!   own list it becomes candidate (term + 1, votes for itself, persists meta) and asks for
//!   votes. A vote is granted at most once per term (persisted before the reply), only to a
//!   candidate whose `last` is at least the voter's (rule 4). Members that heard from a leader
//!   since their last election timeout neither pre-vote nor vote (stickiness, which also gives
//!   the leader its check-quorum lease). Ids are never compared (LC-27).
//! * A new leader re-proposes its own accepted entry under its term, same version and value
//!   (rule 5), and proposes a new version only once nothing is in flight (rule 3).
//! * A follower persists a proposal before acknowledging it (rule 2). It accepts a proposal only
//!   if `(term, version)` is not below its `last`, so reordered old proposals never replace a
//!   newer acknowledged one.
//! * The leader commits its in-flight entry once a majority of its list persisted it (itself
//!   included) and tells the others; a follower that missed values gets the committed value
//!   through `Install` after a beat.
//! * Messages are accepted only from coordinators of the member's own list and generation;
//!   majorities are counted over its own list (peer lists are operator-set, §23.3).

use std::sync::Arc;

use kv_ctrl::CoordId;

use super::peers::PeerList;
use super::rpc::WsrMsg;

/// Role of the local member.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Role {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

/// One state value with the term of the proposal that carried it.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Entry<V> {
    pub version: u64,
    pub term: u64,
    pub value: V,
}

/// `Debug` shows the value through its own `Debug` (a [`super::rpc::Blob`] prints its length
/// only).
impl<V: core::fmt::Debug> core::fmt::Debug for Entry<V> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Entry(v{} t{} {:?})", self.version, self.term, self.value)
    }
}

impl<V> Entry<V> {
    fn tv(&self) -> (u64, u64) {
        (self.term, self.version)
    }
}

pub enum Input<V> {
    /// The election timer fired (followers and candidates; a leader ignores it).
    ElectionTimeout,
    /// Leader beat timer: send beats tagged `round` (any increasing number) and re-send the
    /// in-flight proposal to members that have not acknowledged it.
    BeatTimeout { round: u64 },
    /// Leader check-quorum timer (one minimum election timeout): step down unless a majority
    /// was heard from since the previous check.
    CheckQuorum,
    Msg { from: CoordId, msg: WsrMsg<V> },
    /// The leader's next whole state (only when [`Core::can_propose`]); its version is the
    /// committed version + 1.
    Propose { value: V },
    /// `KVADM COORD SETPEERS`, already validated with `peers::set_peers`.
    SetPeers(Arc<PeerList>),
}

pub enum Output<V> {
    Send { to: CoordId, msg: WsrMsg<V> },
    /// Write `coord.meta` from [`Core::meta`].
    PersistMeta,
    /// Write `coord.state` from [`Core::committed`] / [`Core::accepted`].
    PersistState,
    /// [`Core::committed`] now holds `version`.
    Commit { version: u64 },
    BecameLeader { term: u64 },
    SteppedDown { term: u64 },
    /// A message from the current leader was accepted: restart the election timer.
    LeaderContact { leader: CoordId },
    /// A vote was granted: restart the election timer.
    VoteGranted,
    /// A majority (the leader included) answered the beats of `round`.
    QuorumHeard { round: u64 },
}

impl<V> Output<V> {
    pub fn is_persist(&self) -> bool {
        matches!(self, Output::PersistMeta | Output::PersistState)
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Core<V> {
    id: CoordId,
    // persistent: coord.meta
    term: u64,
    voted_for: Option<CoordId>,
    peers: Arc<PeerList>,
    // persistent: coord.state
    committed: Entry<V>,
    accepted: Option<Entry<V>>,
    // volatile
    role: Role,
    leader: Option<CoordId>,
    /// Heard from a current leader since the last election timeout (or since restart).
    leader_recent: bool,
    /// Pre-votes or votes received, by index in `peers`.
    votes: u8,
    /// Leader: members that persisted the in-flight entry (`accepted`), by index.
    acks: u8,
    /// Leader: members heard from since the last check-quorum.
    heard: u8,
    /// Leader: current beat round and the members that answered it.
    round: u64,
    round_acks: u8,
    round_reported: bool,
}

fn bit(i: usize) -> u8 {
    1u8 << i
}

impl<V: Clone + PartialEq> Core<V> {
    /// A member restarting from its persisted state (also the first start). Volatile state
    /// starts as a follower that counts as having just heard from a leader, so a restarted
    /// member waits one election timeout before it supports a challenger.
    pub fn recover(
        id: CoordId,
        term: u64,
        voted_for: Option<CoordId>,
        peers: impl Into<Arc<PeerList>>,
        committed: Entry<V>,
        accepted: Option<Entry<V>>,
    ) -> Self {
        // An accepted entry at or below the committed version carries no information.
        let accepted = accepted.filter(|a| a.version > committed.version);
        Core {
            id,
            term,
            voted_for,
            peers: peers.into(),
            committed,
            accepted,
            role: Role::Follower,
            leader: None,
            leader_recent: true,
            votes: 0,
            acks: 0,
            heard: 0,
            round: 0,
            round_reported: false,
            round_acks: 0,
        }
    }

    pub fn id(&self) -> CoordId {
        self.id
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn term(&self) -> u64 {
        self.term
    }

    pub fn voted_for(&self) -> Option<CoordId> {
        self.voted_for
    }

    pub fn peers(&self) -> &PeerList {
        &self.peers
    }

    /// The shared peer list (cheap to clone).
    pub fn peers_arc(&self) -> Arc<PeerList> {
        self.peers.clone()
    }

    /// The leader this member currently follows (itself when leader).
    pub fn leader(&self) -> Option<CoordId> {
        self.leader
    }

    pub fn committed(&self) -> &Entry<V> {
        &self.committed
    }

    pub fn accepted(&self) -> Option<&Entry<V>> {
        self.accepted.as_ref()
    }

    /// `(term, version, voted_for)`-side persistent state: what `coord.meta` must hold.
    pub fn meta(&self) -> (u64, Option<CoordId>, &PeerList) {
        (self.term, self.voted_for, &self.peers)
    }

    /// The larger of the committed and accepted entries, as `(term, version)`.
    pub fn last(&self) -> (u64, u64) {
        let c = self.committed.tv();
        match &self.accepted {
            Some(a) if a.tv() > c => a.tv(),
            _ => c,
        }
    }

    /// This member is in its own list (a removed or still-joining coordinator is not).
    pub fn is_voter(&self) -> bool {
        self.peers.contains(&self.id)
    }

    /// Leader with nothing in flight: [`Input::Propose`] is accepted.
    pub fn can_propose(&self) -> bool {
        self.role == Role::Leader && self.accepted.is_none() && self.is_voter()
    }

    fn my_index(&self) -> Option<usize> {
        self.peers.index_of(&self.id)
    }

    fn self_bit(&self) -> u8 {
        self.my_index().map_or(0, bit)
    }

    fn majority(&self, mask: u8) -> bool {
        let n = self.peers.peers.len();
        n > 0 && (mask.count_ones() as usize) * 2 > n
    }

    fn others(&self) -> Vec<CoordId> {
        self.peers
            .peers
            .iter()
            .map(|p| p.id)
            .filter(|id| *id != self.id)
            .collect()
    }

    fn broadcast(&self, msg: WsrMsg<V>, out: &mut Vec<Output<V>>) {
        for to in self.others() {
            out.push(Output::Send {
                to,
                msg: msg.clone(),
            });
        }
    }

    fn peer_gen(&self) -> u64 {
        self.peers.r#gen
    }

    /// Adopts a newer term: no vote yet, follower, no known leader (persisted, rule 1).
    fn adopt_term(&mut self, term: u64, out: &mut Vec<Output<V>>) {
        debug_assert!(term > self.term);
        self.term = term;
        self.voted_for = None;
        self.become_follower(None, out);
        out.push(Output::PersistMeta);
    }

    fn become_follower(&mut self, leader: Option<CoordId>, out: &mut Vec<Output<V>>) {
        if self.role == Role::Leader {
            out.push(Output::SteppedDown { term: self.term });
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.votes = 0;
        self.acks = 0;
        self.heard = 0;
        self.round_acks = 0;
    }

    /// A message from the leader of the current term was accepted.
    fn follow(&mut self, leader: CoordId, out: &mut Vec<Output<V>>) {
        if self.role != Role::Follower || self.leader != Some(leader) {
            self.become_follower(Some(leader), out);
        }
        self.leader_recent = true;
        out.push(Output::LeaderContact { leader });
    }

    pub fn step(&mut self, input: Input<V>, out: &mut Vec<Output<V>>) {
        match input {
            Input::ElectionTimeout => self.on_election_timeout(out),
            Input::BeatTimeout { round } => self.on_beat_timeout(round, out),
            Input::CheckQuorum => self.on_check_quorum(out),
            Input::Msg { from, msg } => self.on_msg(from, msg, out),
            Input::Propose { value } => self.on_propose(value, out),
            Input::SetPeers(peers) => self.on_set_peers(peers, out),
        }
    }

    fn on_election_timeout(&mut self, out: &mut Vec<Output<V>>) {
        if self.role == Role::Leader {
            return;
        }
        self.leader_recent = false;
        self.leader = None;
        if !self.is_voter() {
            self.role = Role::Follower;
            return;
        }
        // Pre-vote round for term + 1; changes nothing persistent.
        self.role = Role::PreCandidate;
        self.votes = self.self_bit();
        let (last_term, last_version) = self.last();
        self.broadcast(
            WsrMsg::PreVote {
                peer_gen: self.peer_gen(),
                term: self.term + 1,
                last_term,
                last_version,
            },
            out,
        );
        if self.majority(self.votes) {
            self.become_candidate(out);
        }
    }

    fn become_candidate(&mut self, out: &mut Vec<Output<V>>) {
        self.term += 1;
        self.voted_for = Some(self.id);
        self.role = Role::Candidate;
        self.leader = None;
        self.votes = self.self_bit();
        out.push(Output::PersistMeta);
        let (last_term, last_version) = self.last();
        self.broadcast(
            WsrMsg::RequestVote {
                peer_gen: self.peer_gen(),
                term: self.term,
                last_term,
                last_version,
            },
            out,
        );
        if self.majority(self.votes) {
            self.become_leader(out);
        }
    }

    fn become_leader(&mut self, out: &mut Vec<Output<V>>) {
        self.role = Role::Leader;
        self.leader = Some(self.id);
        self.leader_recent = true;
        self.votes = 0;
        self.heard = 0;
        self.round_acks = 0;
        self.round_reported = false;
        out.push(Output::BecameLeader { term: self.term });
        // Rule 5: re-propose the own accepted value under the new term before anything else.
        if let Some(acc) = &mut self.accepted {
            acc.term = self.term;
            out.push(Output::PersistState);
            self.start_inflight(out);
        }
    }

    /// Sends the in-flight entry (`accepted`, of the current term) to every other member.
    fn start_inflight(&mut self, out: &mut Vec<Output<V>>) {
        self.acks = self.self_bit();
        if let Some(acc) = &self.accepted {
            let msg = WsrMsg::Proposal {
                peer_gen: self.peer_gen(),
                term: self.term,
                version: acc.version,
                value: acc.value.clone(),
            };
            self.broadcast(msg, out);
        }
        if self.majority(self.acks) {
            self.commit_inflight(out);
        }
    }

    fn commit_inflight(&mut self, out: &mut Vec<Output<V>>) {
        let Some(e) = self.accepted.take() else {
            return;
        };
        debug_assert_eq!(e.term, self.term);
        let (term, version) = e.tv();
        self.committed = e;
        self.acks = 0;
        out.push(Output::PersistState);
        out.push(Output::Commit { version });
        self.broadcast(
            WsrMsg::Commit {
                peer_gen: self.peer_gen(),
                term,
                version,
            },
            out,
        );
    }

    fn on_beat_timeout(&mut self, round: u64, out: &mut Vec<Output<V>>) {
        if self.role != Role::Leader {
            return;
        }
        self.round = round;
        self.round_acks = self.self_bit();
        self.round_reported = false;
        self.broadcast(
            WsrMsg::Beat {
                peer_gen: self.peer_gen(),
                term: self.term,
                round,
                committed_version: self.committed.version,
                committed_term: self.committed.term,
            },
            out,
        );
        // Re-send the in-flight proposal to members that have not persisted it.
        if let Some(acc) = &self.accepted {
            for (i, p) in self.peers.peers.iter().enumerate() {
                if p.id != self.id && self.acks & bit(i) == 0 {
                    out.push(Output::Send {
                        to: p.id,
                        msg: WsrMsg::Proposal {
                            peer_gen: self.peers.r#gen,
                            term: self.term,
                            version: acc.version,
                            value: acc.value.clone(),
                        },
                    });
                }
            }
        }
        self.check_round(out);
    }

    fn check_round(&mut self, out: &mut Vec<Output<V>>) {
        if !self.round_reported && self.majority(self.round_acks) {
            self.round_reported = true;
            out.push(Output::QuorumHeard { round: self.round });
        }
    }

    fn on_check_quorum(&mut self, out: &mut Vec<Output<V>>) {
        if self.role != Role::Leader {
            return;
        }
        if !self.majority(self.heard | self.self_bit()) {
            self.become_follower(None, out);
        }
        self.heard = 0;
    }

    fn on_propose(&mut self, value: V, out: &mut Vec<Output<V>>) {
        if !self.can_propose() {
            return;
        }
        self.accepted = Some(Entry {
            version: self.committed.version + 1,
            term: self.term,
            value,
        });
        out.push(Output::PersistState);
        self.start_inflight(out);
    }

    fn on_set_peers(&mut self, peers: Arc<PeerList>, out: &mut Vec<Output<V>>) {
        self.peers = peers;
        out.push(Output::PersistMeta);
        // Bit masks index the old list: start counting afresh.
        self.votes = 0;
        self.heard = 0;
        self.round_acks = 0;
        self.round_reported = false;
        match self.role {
            Role::Leader if self.is_voter() => {
                if self.accepted.is_some() {
                    self.start_inflight(out);
                } else {
                    self.acks = 0;
                }
            }
            Role::Follower => {
                if !self.is_voter() {
                    self.leader = None;
                }
            }
            _ => self.become_follower(None, out),
        }
    }

    fn on_msg(&mut self, from: CoordId, msg: WsrMsg<V>, out: &mut Vec<Output<V>>) {
        // Only coordinators of the own list and generation count; a member outside its own
        // list (removed, or still joining) takes no part.
        let Some(fi) = self.peers.index_of(&from) else {
            return;
        };
        if from == self.id || msg.peer_gen() != self.peer_gen() || !self.is_voter() {
            return;
        }
        match msg {
            WsrMsg::PreVote {
                term,
                last_term,
                last_version,
                ..
            } => {
                let granted = term > self.term
                    && !self.leader_recent
                    && self.role != Role::Leader
                    && (last_term, last_version) >= self.last();
                out.push(Output::Send {
                    to: from,
                    msg: WsrMsg::PreVoteReply {
                        peer_gen: self.peer_gen(),
                        term: if granted { term } else { self.term },
                        granted,
                    },
                });
            }
            WsrMsg::PreVoteReply { term, granted, .. } => {
                if !granted {
                    if term > self.term {
                        self.adopt_term(term, out);
                    }
                    return;
                }
                if self.role == Role::PreCandidate && term == self.term + 1 {
                    self.votes |= bit(fi);
                    if self.majority(self.votes) {
                        self.become_candidate(out);
                    }
                }
            }
            WsrMsg::RequestVote {
                term,
                last_term,
                last_version,
                ..
            } => {
                // Stickiness: until its election timer fires after the last contact with a
                // leader (or after a restart), a member ignores the request entirely (no term
                // change): a partitioned member cannot disrupt a live leader, and the leader's
                // check-quorum lease holds.
                if self.leader_recent && self.leader != Some(from) {
                    return;
                }
                if term > self.term {
                    self.adopt_term(term, out);
                }
                let up_to_date = (last_term, last_version) >= self.last();
                let granted = term == self.term
                    && self.role == Role::Follower
                    && self.voted_for.is_none_or(|v| v == from)
                    && up_to_date;
                if granted && self.voted_for.is_none() {
                    self.voted_for = Some(from);
                    out.push(Output::PersistMeta);
                }
                if granted {
                    out.push(Output::VoteGranted);
                }
                out.push(Output::Send {
                    to: from,
                    msg: WsrMsg::VoteReply {
                        peer_gen: self.peer_gen(),
                        term: self.term,
                        granted,
                    },
                });
            }
            WsrMsg::VoteReply { term, granted, .. } => {
                if term > self.term {
                    self.adopt_term(term, out);
                    return;
                }
                if granted && term == self.term && self.role == Role::Candidate {
                    self.votes |= bit(fi);
                    if self.majority(self.votes) {
                        self.become_leader(out);
                    }
                }
            }
            WsrMsg::Proposal {
                term,
                version,
                value,
                ..
            } => {
                if term < self.term {
                    out.push(Output::Send {
                        to: from,
                        msg: WsrMsg::Accepted {
                            peer_gen: self.peer_gen(),
                            term: self.term,
                            version,
                            ok: false,
                        },
                    });
                    return;
                }
                if term > self.term {
                    self.adopt_term(term, out);
                }
                if self.role == Role::Leader {
                    return; // one leader per term: cannot happen
                }
                self.follow(from, out);
                if self.accept(term, version, value, out) {
                    out.push(Output::Send {
                        to: from,
                        msg: WsrMsg::Accepted {
                            peer_gen: self.peer_gen(),
                            term,
                            version,
                            ok: true,
                        },
                    });
                }
            }
            WsrMsg::Accepted {
                term, version, ok, ..
            } => {
                if term > self.term {
                    self.adopt_term(term, out);
                    return;
                }
                if term != self.term || self.role != Role::Leader || !ok {
                    return;
                }
                self.heard |= bit(fi);
                if self
                    .accepted
                    .as_ref()
                    .is_some_and(|a| a.version == version && a.term == term)
                {
                    self.acks |= bit(fi);
                    if self.majority(self.acks) {
                        self.commit_inflight(out);
                    }
                }
            }
            WsrMsg::Commit { term, version, .. } => {
                if term < self.term {
                    return;
                }
                if term > self.term {
                    self.adopt_term(term, out);
                }
                if self.role == Role::Leader {
                    return;
                }
                self.follow(from, out);
                self.commit_accepted(term, version, out);
            }
            WsrMsg::Beat {
                term,
                round,
                committed_version,
                committed_term,
                ..
            } => {
                if term < self.term {
                    out.push(Output::Send {
                        to: from,
                        msg: WsrMsg::BeatReply {
                            peer_gen: self.peer_gen(),
                            term: self.term,
                            round,
                            need: false,
                        },
                    });
                    return;
                }
                if term > self.term {
                    self.adopt_term(term, out);
                }
                if self.role == Role::Leader {
                    return;
                }
                self.follow(from, out);
                self.commit_accepted(committed_term, committed_version, out);
                let need = self.committed.version < committed_version;
                out.push(Output::Send {
                    to: from,
                    msg: WsrMsg::BeatReply {
                        peer_gen: self.peer_gen(),
                        term: self.term,
                        round,
                        need,
                    },
                });
            }
            WsrMsg::BeatReply {
                term, round, need, ..
            } => {
                if term > self.term {
                    self.adopt_term(term, out);
                    return;
                }
                if term != self.term || self.role != Role::Leader {
                    return;
                }
                self.heard |= bit(fi);
                if round == self.round {
                    self.round_acks |= bit(fi);
                    self.check_round(out);
                }
                if need {
                    out.push(Output::Send {
                        to: from,
                        msg: WsrMsg::Install {
                            peer_gen: self.peer_gen(),
                            term: self.term,
                            version: self.committed.version,
                            value_term: self.committed.term,
                            value: self.committed.value.clone(),
                        },
                    });
                }
            }
            WsrMsg::Install {
                term,
                version,
                value_term,
                value,
                ..
            } => {
                if term < self.term {
                    return;
                }
                if term > self.term {
                    self.adopt_term(term, out);
                }
                if self.role == Role::Leader {
                    return;
                }
                self.follow(from, out);
                self.install(version, value_term, value, out);
            }
        }
    }

    /// Persists the proposal `(term, version)` unless it is below `last` (a reordered older
    /// message). Returns whether the member now holds it (and may acknowledge).
    fn accept(&mut self, term: u64, version: u64, value: V, out: &mut Vec<Output<V>>) -> bool {
        if (term, version) < self.last() {
            return false;
        }
        let cv = self.committed.version;
        if version > cv {
            let same = self
                .accepted
                .as_ref()
                .is_some_and(|a| a.version == version && a.term == term);
            if !same {
                self.accepted = Some(Entry {
                    version,
                    term,
                    value,
                });
                out.push(Output::PersistState);
            }
            true
        } else if version == cv {
            // The committed value re-proposed under a newer term: same value (it is
            // committed), now carried by `term`; any accepted entry of an older term above it
            // is superseded by the leader's.
            if self.committed.term < term || self.accepted.is_some() {
                self.committed.term = self.committed.term.max(term);
                self.accepted = None;
                out.push(Output::PersistState);
            }
            true
        } else {
            false
        }
    }

    /// Commits the accepted entry if it is exactly `(term, version)`.
    fn commit_accepted(&mut self, term: u64, version: u64, out: &mut Vec<Output<V>>) {
        if self
            .accepted
            .as_ref()
            .is_some_and(|a| a.version == version && a.term == term)
            && let Some(e) = self.accepted.take() {
                self.committed = e;
                out.push(Output::PersistState);
                out.push(Output::Commit { version });
            }
    }

    /// Adopts the leader's committed value when it is newer than the local committed one.
    fn install(&mut self, version: u64, value_term: u64, value: V, out: &mut Vec<Output<V>>) {
        if version <= self.committed.version {
            return;
        }
        let mut term = value_term;
        if let Some(a) = &self.accepted
            && a.version == version {
                term = term.max(a.term);
            }
        self.committed = Entry {
            version,
            term,
            value,
        };
        if self.accepted.as_ref().is_some_and(|a| a.version <= version) {
            self.accepted = None;
        }
        out.push(Output::PersistState);
        out.push(Output::Commit { version });
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::super::peers::CoordRecord;
    use super::*;

    fn id(i: u8) -> CoordId {
        CoordId([i + 1; 20])
    }

    fn peers(n: u8) -> PeerList {
        PeerList {
            r#gen: 1,
            peers: (0..n)
                .map(|i| {
                    let a = |p: u16| SocketAddr::from(([127, 0, 0, 1], p + u16::from(i)));
                    CoordRecord::new(id(i), a(7100), a(7000), a(7200))
                })
                .collect(),
        }
    }

    type C = Core<u32>;

    fn group(n: u8) -> Vec<C> {
        (0..n)
            .map(|i| {
                Core::recover(
                    id(i),
                    0,
                    None,
                    peers(n),
                    Entry {
                        version: 1,
                        term: 0,
                        value: 100 + u32::from(i),
                    },
                    None,
                )
            })
            .collect()
    }

    /// Delivers every message until quiescence (no loss), returning the outputs seen.
    fn run(g: &mut [C], mut inbox: Vec<(usize, CoordId, WsrMsg<u32>)>) -> Vec<(usize, Output<u32>)> {
        let mut seen = Vec::new();
        while let Some((to, from, msg)) = (!inbox.is_empty()).then(|| inbox.remove(0)) {
            let mut out = Vec::new();
            g[to].step(Input::Msg { from, msg }, &mut out);
            for o in out {
                if let Output::Send { to: t, msg } = &o {
                    let ti = g.iter().position(|c| c.id() == *t).unwrap();
                    inbox.push((ti, g[to].id(), msg.clone()));
                }
                seen.push((to, o));
            }
        }
        seen
    }

    fn fire(g: &mut [C], i: usize, input: Input<u32>) -> Vec<(usize, Output<u32>)> {
        let mut out = Vec::new();
        g[i].step(input, &mut out);
        let mut inbox = Vec::new();
        let mut seen = Vec::new();
        for o in out {
            if let Output::Send { to, msg } = &o {
                let ti = g.iter().position(|c| c.id() == *to).unwrap();
                inbox.push((ti, g[i].id(), msg.clone()));
            }
            seen.push((i, o));
        }
        seen.extend(run(g, inbox));
        seen
    }

    /// Clears stickiness everywhere (as after a restart plus one election timeout).
    fn unstick(g: &mut [C]) {
        for c in g.iter_mut() {
            c.leader_recent = false;
        }
    }

    #[test]
    fn elect_propose_commit() {
        let mut g = group(3);
        unstick(&mut g);
        let seen = fire(&mut g, 0, Input::ElectionTimeout);
        assert_eq!(g[0].role(), Role::Leader);
        assert_eq!(g[0].term(), 1);
        assert!(seen.iter().any(|(i, o)| *i == 0 && matches!(o, Output::BecameLeader { term: 1 })));
        // voters persisted their votes
        assert_eq!(g[1].voted_for(), Some(id(0)));
        assert!(g[0].can_propose());
        let seen = fire(&mut g, 0, Input::Propose { value: 7 });
        for c in &g {
            assert_eq!(c.committed().version, 2);
            assert_eq!(c.committed().value, 7);
            assert!(c.accepted().is_none());
        }
        // persistence of the accepted value precedes the acknowledgement of every follower
        for f in 1..3 {
            let pos_persist = seen
                .iter()
                .position(|(i, o)| *i == f && matches!(o, Output::PersistState))
                .unwrap();
            let pos_ack = seen
                .iter()
                .position(|(i, o)| {
                    *i == f && matches!(o, Output::Send { msg: WsrMsg::Accepted { ok: true, .. }, .. })
                })
                .unwrap();
            assert!(pos_persist < pos_ack);
        }
    }

    #[test]
    fn sticky_followers_refuse_pre_votes() {
        let mut g = group(3);
        unstick(&mut g);
        fire(&mut g, 0, Input::ElectionTimeout);
        assert_eq!(g[0].role(), Role::Leader);
        fire(&mut g, 0, Input::BeatTimeout { round: 1 });
        // member 1 has heard from the leader; member 2's election timer fires anyway
        let mut out = Vec::new();
        g[2].step(Input::ElectionTimeout, &mut out);
        assert_eq!(g[2].role(), Role::PreCandidate);
        let mut inbox = Vec::new();
        for o in out {
            if let Output::Send { to, msg } = o {
                let ti = g.iter().position(|c| c.id() == to).unwrap();
                inbox.push((ti, id(2), msg));
            }
        }
        run(&mut g, inbox);
        assert_eq!(g[0].role(), Role::Leader);
        assert_eq!(g[0].term(), 1);
        assert_ne!(g[2].role(), Role::Leader);
    }

    #[test]
    fn new_leader_reproposes_its_accepted_value() {
        let mut g = group(3);
        unstick(&mut g);
        fire(&mut g, 0, Input::ElectionTimeout);
        // the proposal reaches member 1 only
        let mut out = Vec::new();
        g[0].step(Input::Propose { value: 9 }, &mut out);
        let to1 = out.iter().find_map(|o| match o {
            Output::Send { to, msg } if *to == id(1) => Some(msg.clone()),
            _ => None,
        });
        let mut out1 = Vec::new();
        g[1].step(Input::Msg { from: id(0), msg: to1.unwrap() }, &mut out1);
        assert_eq!(g[1].accepted().map(|a| a.value), Some(9));
        // leader 0 dies; member 1 times out and wins with member 2's vote
        unstick(&mut g);
        let mut out = Vec::new();
        g[1].step(Input::ElectionTimeout, &mut out);
        let mut inbox = Vec::new();
        for o in out {
            if let Output::Send { to, msg } = o
                && to == id(2) {
                    inbox.push((2usize, id(1), msg));
                }
        }
        // deliver only between 1 and 2
        let mut queue = inbox;
        while let Some((to, from, msg)) = (!queue.is_empty()).then(|| queue.remove(0)) {
            let mut o = Vec::new();
            g[to].step(Input::Msg { from, msg }, &mut o);
            for x in o {
                if let Output::Send { to: t, msg } = x {
                    let ti = g.iter().position(|c| c.id() == t).unwrap();
                    if ti != 0 {
                        queue.push((ti, g[to].id(), msg));
                    }
                }
            }
        }
        assert_eq!(g[1].role(), Role::Leader);
        assert_eq!(g[1].term(), 2);
        assert_eq!(g[1].committed().value, 9);
        assert_eq!(g[2].committed().value, 9);
        assert_eq!(g[2].committed().version, 2);
    }

    #[test]
    fn stale_reordered_proposal_does_not_replace_newer() {
        let mut c: C = group(3).remove(1);
        let mut out = Vec::new();
        let p = |version, value| WsrMsg::Proposal {
            peer_gen: 1,
            term: 1,
            version,
            value,
        };
        c.step(Input::Msg { from: id(0), msg: p(3, 30) }, &mut out);
        assert_eq!(c.accepted().map(|a| a.value), Some(30));
        out.clear();
        c.step(Input::Msg { from: id(0), msg: p(2, 20) }, &mut out);
        assert_eq!(c.accepted().map(|a| a.value), Some(30));
        assert!(!out.iter().any(|o| matches!(o, Output::Send { msg: WsrMsg::Accepted { .. }, .. })));
    }

    #[test]
    fn other_generations_and_strangers_are_ignored() {
        let mut c: C = group(3).remove(1);
        let mut out = Vec::new();
        let p = |peer_gen| WsrMsg::Proposal {
            peer_gen,
            term: 5,
            version: 2,
            value: 1,
        };
        c.step(Input::Msg { from: id(0), msg: p(2) }, &mut out);
        c.step(Input::Msg { from: id(9), msg: p(1) }, &mut out);
        assert!(out.is_empty());
        assert_eq!(c.term(), 0);
    }

    #[test]
    fn single_member_commits_alone() {
        let mut g = group(1);
        g[0].leader_recent = false;
        let mut out = Vec::new();
        g[0].step(Input::ElectionTimeout, &mut out);
        assert_eq!(g[0].role(), Role::Leader);
        out.clear();
        g[0].step(Input::Propose { value: 5 }, &mut out);
        assert_eq!(g[0].committed().version, 2);
        let persist = out.iter().position(Output::is_persist).unwrap();
        let commit = out.iter().position(|o| matches!(o, Output::Commit { .. })).unwrap();
        assert!(persist < commit);
    }

    #[test]
    fn lagging_follower_gets_install() {
        let mut g = group(3);
        unstick(&mut g);
        fire(&mut g, 0, Input::ElectionTimeout);
        // commit two values with member 2 cut off
        for v in [11, 12] {
            let mut out = Vec::new();
            g[0].step(Input::Propose { value: v }, &mut out);
            let mut inbox = Vec::new();
            for o in out {
                if let Output::Send { to, msg } = o
                    && to == id(1) {
                        inbox.push((1usize, id(0), msg));
                    }
            }
            let mut q = inbox;
            while let Some((to, from, msg)) = (!q.is_empty()).then(|| q.remove(0)) {
                let mut o = Vec::new();
                g[to].step(Input::Msg { from, msg }, &mut o);
                for x in o {
                    if let Output::Send { to: t, msg } = x {
                        let ti = g.iter().position(|c| c.id() == t).unwrap();
                        if ti != 2 {
                            q.push((ti, g[to].id(), msg));
                        }
                    }
                }
            }
        }
        assert_eq!(g[0].committed().version, 3);
        assert_eq!(g[2].committed().version, 1);
        fire(&mut g, 0, Input::BeatTimeout { round: 1 });
        assert_eq!(g[2].committed().version, 3);
        assert_eq!(g[2].committed().value, 12);
    }

    #[test]
    fn check_quorum_steps_down_without_majority() {
        let mut g = group(3);
        unstick(&mut g);
        fire(&mut g, 0, Input::ElectionTimeout);
        let mut out = Vec::new();
        g[0].step(Input::CheckQuorum, &mut out);
        assert_eq!(g[0].role(), Role::Follower);
        assert!(out.iter().any(|o| matches!(o, Output::SteppedDown { .. })));
    }
}
