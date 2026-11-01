//! Topology replies of the coordinator's client service (DESIGN §23.5).
//!
//! * CLUSTER SLOTS: Redis 7 format per contiguous range of one shard, primary first, then InSync
//!   members; coordinators are not listed.
//! * CLUSTER SHARDS: per shard `slots` and `nodes` with `replication-offset: 0` and health from
//!   replication state only.
//! * CLUSTER NODES: data-node lines from the state, plus one slotless line per coordinator
//!   rendered from the static peer list (never from the state), `myself` only on the answering
//!   coordinator, never `noaddr`. Coordinator line style R1 (`slave` with master-id `-`) or R2
//!   (`master` with no slots); config-epoch columns are 0.
//! * CLUSTER INFO: `cluster_state`, slot counts, `cluster_current_epoch:<version>`,
//!   `cluster_my_epoch:0`.
//!
//! Rendered replies are cached per `version`.

use kv_ctrl::{CoordEndpoint, CoordId};

use crate::state::ClusterState;

/// How coordinators appear in CLUSTER NODES (decided at M2.1).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CoordLineStyle {
    /// slotless `slave` entry with master-id `-`
    R1,
    /// `master` with no slots
    R2,
}

pub fn cluster_slots(s: &ClusterState, w: &mut kv_resp::ReplyWriter<'_>) {
    todo!("DESIGN §23.5: CLUSTER SLOTS")
}

pub fn cluster_shards(s: &ClusterState, w: &mut kv_resp::ReplyWriter<'_>) {
    todo!("DESIGN §23.5: CLUSTER SHARDS")
}

pub fn cluster_nodes(
    s: &ClusterState,
    peers: &[CoordEndpoint], /* static list */
    myself: &CoordId,
    style: CoordLineStyle, /* R1 | R2 */
) -> Vec<u8> {
    todo!("DESIGN §23.5: CLUSTER NODES")
}

pub fn cluster_info(s: &ClusterState) -> Vec<u8> {
    todo!("DESIGN §23.5: CLUSTER INFO")
}
