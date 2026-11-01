//! ISR changes (DESIGN §23.8).
//!
//! `IsrShrink { shard, term, member }` from the shard's primary is committed even when the ISR
//! falls below `min_isr`; the primary stops waiting for that member only after the committed
//! assignment arrives. `IsrExpand` is sent by the primary once it counts a caught-up member's
//! acks; after the commit the member is promotable and advertised. Requests of a stale term are
//! ignored. Membership itself never changes here (LC-25).

use kv_ctrl::{NodeId, ShardId};
use kv_topo::{ClusterState, Transition};

/// The transition for an ISR request, or `None` when it is stale or already applied.
pub fn isr_transition(s: &ClusterState, shard: ShardId, term: u32, member: NodeId, add: bool) -> Option<Transition> {
    todo!("DESIGN §23.8: ISR request")
}
