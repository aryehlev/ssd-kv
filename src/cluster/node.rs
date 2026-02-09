//! Node identity and metadata for cluster members.

use std::fmt;
use std::net::SocketAddr;

/// Unique identifier for a node in the cluster.
pub type NodeId = u32;

/// Status of a node as seen by the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    /// Node is healthy and serving requests.
    Active,
    /// Node is suspected of being down (missed heartbeats).
    Suspect,
    /// Node is confirmed down.
    Dead,
    /// Node is joining/leaving the cluster and migrating shards.
    Migrating,
}

/// Information about a single cluster node.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    /// Unique node identifier (derived from StatefulSet ordinal in K8s).
    pub id: NodeId,
    /// Address for client-facing Redis protocol.
    pub redis_addr: SocketAddr,
    /// Address for inter-node gRPC communication.
    pub cluster_addr: SocketAddr,
    /// Current status of the node.
    pub status: NodeStatus,
    /// Monotonically increasing generation for leader election.
    pub generation: u64,
}

impl NodeInfo {
    pub fn new(id: NodeId, redis_addr: SocketAddr, cluster_addr: SocketAddr) -> Self {
        Self {
            id,
            redis_addr,
            cluster_addr,
            status: NodeStatus::Active,
            generation: 0,
        }
    }

    /// Returns true if this node can serve requests.
    pub fn is_available(&self) -> bool {
        matches!(self.status, NodeStatus::Active)
    }
}

impl fmt::Display for NodeInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Node(id={}, redis={}, cluster={}, status={:?})",
            self.id, self.redis_addr, self.cluster_addr, self.status
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_info_creation() {
        let node = NodeInfo::new(
            0,
            "127.0.0.1:7777".parse().unwrap(),
            "127.0.0.1:7780".parse().unwrap(),
        );
        assert_eq!(node.id, 0);
        assert!(node.is_available());
        assert_eq!(node.status, NodeStatus::Active);
    }

    #[test]
    fn test_node_status() {
        let mut node = NodeInfo::new(
            1,
            "127.0.0.1:7778".parse().unwrap(),
            "127.0.0.1:7781".parse().unwrap(),
        );
        assert!(node.is_available());

        node.status = NodeStatus::Dead;
        assert!(!node.is_available());

        node.status = NodeStatus::Suspect;
        assert!(!node.is_available());
    }
}
