//! Redirects for keyed commands sent to a coordinator (DESIGN §23.5).
//!
//! `-MOVED <slot> <primary ip:port>` from the committed map; an unassigned slot, or a shard that
//! is `NoPrimary`/`Promoting` (or whose primary is suspected failed by the leader), gives
//! `-CLUSTERDOWN Hash slot not served`. ASK is never emitted.

use std::net::SocketAddr;

use kv_ctrl::NodeId;

use crate::state::ClusterState;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Redirect {
    Moved(u16, SocketAddr),
    ClusterDown,
    TryAgain,
}

pub fn for_slot(s: &ClusterState, slot: u16, now_pfail: &dyn Fn(&NodeId) -> bool) -> Redirect {
    todo!("DESIGN §23.5: redirect")
}
