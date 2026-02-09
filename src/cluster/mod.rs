//! Cluster mode: partitioning, routing, replication, and failover.

pub mod node;
pub mod topology;
pub mod router;
pub mod peer_pool;
pub mod replication;
pub mod health;
pub mod rebalance;

pub use node::{NodeId, NodeInfo, NodeStatus};
pub use topology::{ClusterTopology, ShardAssignment, ShardState};
pub use router::{ClusterRouter, RouteDecision};
