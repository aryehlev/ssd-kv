//! `kv-topo`: the versioned cluster state and its views (DESIGN §23.2, §23.5, §24.10).
//!
//! [`ClusterState`] is the one versioned map object owned by the coordinators: nodes in operator
//! order, shards with their members and terms, the slot assignment and migrations. It holds no
//! coordinator list: each coordinator's peer list lives in its own `coord.meta`, outside the
//! voted value (LC-38). [`ClusterState::apply`] is pure and deterministic; only `Admin`
//! transitions change membership, replica lists or the assignment of unassigned slots (LC-25).
//! Versions and terms are integer counters (LC-27). The static failover choice
//! ([`ClusterState::choose_primary`], [`state::static_choice`]) uses operator priority and
//! member order only. [`render`] produces CLUSTER SLOTS/SHARDS/NODES/INFO/MYID and HELLO for
//! the coordinator's client service; [`redirect`] computes MOVED/CLUSTERDOWN/CROSSSLOT for keyed
//! commands sent to a coordinator.
#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(unsafe_code)]

pub mod redirect;
pub mod render;
pub mod state;
pub mod transition;

// The state holds no coordinator records (LC-38).
pub use state::{
    ClusterParams, ClusterState, Member, MigPhase, Migration, NodeRecord, ShardRecord,
};
pub use transition::{AdminOp, Reject, Transition};
