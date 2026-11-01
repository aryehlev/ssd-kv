//! WSR peer messages and transport (DESIGN §23.3).
//!
//! Frames are `u32 len || postcard(WsrMsg)` over the coordinators' peer port, authenticated
//! with the cluster token in a constant-time compared handshake. Every message carries the
//! sender's `peer_gen`; a beat carries no state value.

use kv_ctrl::CoordId;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub enum WsrMsg {
    PreVote { term: u64, peer_gen: u64, last_term: u64, last_version: u64 },
    PreVoteReply { term: u64, granted: bool },
    RequestVote { term: u64, peer_gen: u64, last_term: u64, last_version: u64 },
    VoteReply { term: u64, granted: bool },
    /// The whole next state (encoded `ClusterState`) at `version`.
    Proposal { term: u64, leader: CoordId, peer_gen: u64, version: u64, state: Vec<u8> },
    Accepted { term: u64, version: u64 },
    Committed { term: u64, version: u64 },
    Beat { term: u64, leader: CoordId, peer_gen: u64, committed_version: u64 },
}

/// Accepts peer connections on the peer port and feeds messages to the core.
pub async fn serve_peers(l: tokio::net::TcpListener, auth: Vec<u8>) -> std::io::Result<()> {
    todo!("DESIGN §23.3: peer transport")
}
