//! The WSR state machine (DESIGN §23.3), free of I/O so that the deterministic simulator and
//! the model checker drive the same code (`kv-sim/src/wsr_model.rs`, §25.3).
//!
//! Inputs are [`Event`]s (time, peer messages, local proposals, persistence completions);
//! outputs are [`Action`]s (send, persist meta, persist state, commit). Persistence actions
//! complete before the messages that depend on them are sent (rules 1 and 2).

use kv_ctrl::CoordId;

use super::peers::PeerList;
use super::rpc::WsrMsg;

/// Role of the local member.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

pub enum Event {
    /// Boot-clock time advanced.
    Tick { now_ns: u64 },
    Msg { from: CoordId, msg: WsrMsg },
    /// The leader's next batch, already applied to the committed state (encoded).
    Propose { version: u64, state: Vec<u8> },
    MetaPersisted,
    StatePersisted,
}

pub enum Action {
    Send { to: CoordId, msg: WsrMsg },
    PersistMeta { term: u64, voted_for: Option<CoordId> },
    PersistState,
    Commit { version: u64 },
    BecameLeader { term: u64 },
    SteppedDown,
}

pub struct Core {
    id: CoordId,
    peers: PeerList,
    role: Role,
    term: u64,
    voted_for: Option<CoordId>,
    committed_version: u64,
    accepted: Option<(u64, u64)>,
}

impl Core {
    pub fn new(id: CoordId, peers: PeerList, term: u64, voted_for: Option<CoordId>, committed_version: u64) -> Self {
        todo!("DESIGN §23.3: WSR core")
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn step(&mut self, ev: Event, out: &mut Vec<Action>) {
        todo!("DESIGN §23.3: WSR step")
    }
}
