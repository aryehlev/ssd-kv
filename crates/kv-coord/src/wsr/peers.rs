//! the operator-set voter list, never part of ClusterState (DESIGN §23.3, LC-38)
//!
//! Each coordinator keeps `{peer_gen, peers}` in its own `coord.meta`, written only by the
//! operator call `KVADM COORD SETPEERS <gen> <list>`, which kvadm sends to each coordinator in
//! turn. A change adds or removes exactly one coordinator relative to generation `gen - 1`;
//! kvadm refuses the next change until every coordinator of the new list reports the new
//! generation. No list is ever derived from who answered.

use std::net::SocketAddr;

use kv_ctrl::CoordId;

/// Most coordinators in a peer list.
pub const MAX_PEERS: usize = 7;

/// One coordinator of the peer list.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CoordRecord {
    pub id: CoordId,
    /// Id shown in CLUSTER NODES coordinator lines (40 hex digits when rendered).
    pub id40: [u8; 20],
    pub peer: SocketAddr,
    pub client: SocketAddr,
    pub admin: SocketAddr,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PeerList {
    /// Generation (`gen` is reserved in edition 2024, hence the raw identifier).
    pub r#gen: u64,
    pub peers: Vec<CoordRecord>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PeerChangeError {
    /// `new.gen != cur.gen + 1`.
    Generation,
    /// Not exactly one coordinator added or removed.
    NotSingleServer,
    TooMany,
    Empty,
}

/// gen = cur.gen + 1, exactly one server added or removed
pub fn set_peers(cur: &PeerList, new: &PeerList) -> Result<(), PeerChangeError> {
    todo!("DESIGN §23.3: single-server peer change")
}
