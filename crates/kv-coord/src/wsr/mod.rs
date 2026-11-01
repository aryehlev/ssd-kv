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

pub mod core;
pub mod peers;
pub mod persist;
pub mod rpc;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use kv_ctrl::CoordId;
use kv_topo::{ClusterState, Reject, Transition};

/// Configuration of the local WSR member.
#[derive(Clone, Debug)]
pub struct WsrCfg {
    pub id: CoordId,
    pub peer_listen: std::net::SocketAddr,
    pub election_timeout_ms: (u32, u32),
    pub beat_ms: u32,
}

impl Default for WsrCfg {
    fn default() -> Self {
        WsrCfg {
            id: CoordId([0; 20]),
            peer_listen: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
            election_timeout_ms: (500, 1000),
            beat_ms: 100,
        }
    }
}

#[derive(Debug)]
pub enum ProposeError {
    NotLeader,
    Rejected(Reject),
    StateTooLarge,
    LostLeadership,
    Io(std::io::Error),
}

/// core state machine + persistence + peers
pub struct Wsr {
    committed: tokio::sync::watch::Sender<Arc<ClusterState>>,
    leader: AtomicBool,
    term: AtomicU64,
}

impl Wsr {
    pub async fn start(cfg: WsrCfg, persist: persist::StateFiles) -> std::io::Result<Arc<Self>> {
        todo!("DESIGN §23.3: start WSR")
    }

    pub async fn propose(&self, batch: Vec<Transition>) -> Result<u64 /*committed version*/, ProposeError> {
        todo!("DESIGN §23.3: propose")
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
}
