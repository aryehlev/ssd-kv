//! Shard-to-node mapping and cluster topology.
//!
//! The cluster uses the existing 256-shard architecture. Each shard is
//! assigned to a primary node and optionally replicated to other nodes.
//! Routing is O(1): `shard_id = (key_hash >> 56) % 256`, then look up
//! the owning node in the shard map.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::cluster::node::{NodeId, NodeInfo, NodeStatus};
use crate::engine::index::NUM_SHARDS;

/// Assignment of a single shard to a primary and replica nodes.
#[derive(Debug, Clone)]
pub struct ShardAssignment {
    /// The primary node that owns this shard.
    pub primary: NodeId,
    /// Replica nodes for this shard (in priority order for failover).
    pub replicas: Vec<NodeId>,
    /// Current state of the shard.
    pub state: ShardState,
}

/// State of a shard during normal operation and migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardState {
    /// Shard is serving normally.
    Active,
    /// Shard is being migrated to a new node.
    Migrating {
        /// Node the shard is migrating from.
        source: NodeId,
        /// Node the shard is migrating to.
        target: NodeId,
    },
}

impl Default for ShardAssignment {
    fn default() -> Self {
        Self {
            primary: 0,
            replicas: Vec::new(),
            state: ShardState::Active,
        }
    }
}

/// The full cluster topology: maps each of the 256 shards to nodes.
pub struct ClusterTopology {
    /// All known nodes in the cluster.
    pub nodes: Vec<NodeInfo>,
    /// Shard assignment map: shard_id -> assignment.
    pub shard_map: Vec<ShardAssignment>,
    /// This node's ID.
    pub local_node_id: NodeId,
    /// Replication factor (number of copies including primary).
    pub replication_factor: u8,
    /// Topology version, incremented on every change.
    pub version: AtomicU64,
}

impl ClusterTopology {
    /// Creates a new topology with shards evenly distributed across `num_nodes` nodes.
    pub fn new(
        local_node_id: NodeId,
        nodes: Vec<NodeInfo>,
        replication_factor: u8,
    ) -> Self {
        let num_nodes = nodes.len();
        let mut shard_map = Vec::with_capacity(NUM_SHARDS);

        for shard_id in 0..NUM_SHARDS {
            let primary = (shard_id / Self::shards_per_node(num_nodes)) as NodeId;
            // Clamp to last node if rounding pushes past end
            let primary = primary.min((num_nodes - 1) as NodeId);

            let mut replicas = Vec::new();
            for r in 1..replication_factor {
                let replica_id = ((primary as usize + r as usize) % num_nodes) as NodeId;
                replicas.push(replica_id);
            }

            shard_map.push(ShardAssignment {
                primary,
                replicas,
                state: ShardState::Active,
            });
        }

        Self {
            nodes,
            shard_map,
            local_node_id,
            replication_factor,
            version: AtomicU64::new(1),
        }
    }

    /// Creates a single-node topology (standalone mode equivalent).
    pub fn single_node(local_node_id: NodeId, node: NodeInfo) -> Self {
        let mut shard_map = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            shard_map.push(ShardAssignment {
                primary: local_node_id,
                replicas: Vec::new(),
                state: ShardState::Active,
            });
        }

        Self {
            nodes: vec![node],
            shard_map,
            local_node_id,
            replication_factor: 1,
            version: AtomicU64::new(1),
        }
    }

    /// Returns the number of shards each node should own (approximately).
    fn shards_per_node(num_nodes: usize) -> usize {
        if num_nodes == 0 {
            NUM_SHARDS
        } else {
            // Ceiling division to ensure all shards are covered
            (NUM_SHARDS + num_nodes - 1) / num_nodes
        }
    }

    /// O(1) lookup: returns the primary node for a given shard.
    #[inline]
    pub fn primary_for_shard(&self, shard_id: u16) -> NodeId {
        self.shard_map[shard_id as usize].primary
    }

    /// Returns the primary + replica nodes for a shard.
    pub fn nodes_for_shard(&self, shard_id: u16) -> Vec<NodeId> {
        let assignment = &self.shard_map[shard_id as usize];
        let mut nodes = vec![assignment.primary];
        nodes.extend_from_slice(&assignment.replicas);
        nodes
    }

    /// Returns true if this node is the primary for the given shard.
    #[inline]
    pub fn is_local_primary(&self, shard_id: u16) -> bool {
        self.shard_map[shard_id as usize].primary == self.local_node_id
    }

    /// Returns true if this node is a replica for the given shard.
    pub fn is_local_replica(&self, shard_id: u16) -> bool {
        self.shard_map[shard_id as usize]
            .replicas
            .contains(&self.local_node_id)
    }

    /// Returns true if this node owns (primary or replica) the given shard.
    pub fn is_local_shard(&self, shard_id: u16) -> bool {
        self.is_local_primary(shard_id) || self.is_local_replica(shard_id)
    }

    /// Returns all shard IDs owned by the given node as primary.
    pub fn shards_for_node(&self, node_id: NodeId) -> Vec<u16> {
        self.shard_map
            .iter()
            .enumerate()
            .filter(|(_, a)| a.primary == node_id)
            .map(|(i, _)| i as u16)
            .collect()
    }

    /// Returns a NodeInfo by ID, if it exists.
    pub fn get_node(&self, node_id: NodeId) -> Option<&NodeInfo> {
        self.nodes.iter().find(|n| n.id == node_id)
    }

    /// Returns a mutable NodeInfo by ID.
    pub fn get_node_mut(&mut self, node_id: NodeId) -> Option<&mut NodeInfo> {
        self.nodes.iter_mut().find(|n| n.id == node_id)
    }

    /// Returns the number of active nodes.
    pub fn active_node_count(&self) -> usize {
        self.nodes
            .iter()
            .filter(|n| n.status == NodeStatus::Active)
            .count()
    }

    /// Recalculates shard assignments after node changes.
    /// Returns a list of (shard_id, old_primary, new_primary) for shards that moved.
    pub fn rebalance(&mut self) -> Vec<(u16, NodeId, NodeId)> {
        let active_nodes: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|n| n.status == NodeStatus::Active)
            .map(|n| n.id)
            .collect();

        if active_nodes.is_empty() {
            return Vec::new();
        }

        let num_active = active_nodes.len();
        let spn = Self::shards_per_node(num_active);
        let mut moves = Vec::new();

        for (shard_id, assignment) in self.shard_map.iter_mut().enumerate() {
            let ideal_owner_idx = (shard_id / spn).min(num_active - 1);
            let ideal_owner = active_nodes[ideal_owner_idx];

            if assignment.primary != ideal_owner {
                let old = assignment.primary;
                assignment.primary = ideal_owner;
                moves.push((shard_id as u16, old, ideal_owner));
            }

            // Update replicas
            assignment.replicas.clear();
            for r in 1..self.replication_factor as usize {
                let replica_idx = (ideal_owner_idx + r) % num_active;
                assignment.replicas.push(active_nodes[replica_idx]);
            }
        }

        if !moves.is_empty() {
            self.version.fetch_add(1, Ordering::SeqCst);
        }

        moves
    }

    /// Computes the shard ID for a given key hash.
    #[inline]
    pub fn shard_for_key(key_hash: u64) -> u16 {
        ((key_hash >> 56) as u16) % NUM_SHARDS as u16
    }

    /// Returns the current topology version.
    pub fn current_version(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::node::NodeInfo;

    fn make_nodes(n: usize) -> Vec<NodeInfo> {
        (0..n)
            .map(|i| {
                NodeInfo::new(
                    i as NodeId,
                    format!("127.0.0.1:{}", 7777 + i).parse().unwrap(),
                    format!("127.0.0.1:{}", 7780 + i).parse().unwrap(),
                )
            })
            .collect()
    }

    #[test]
    fn test_single_node_topology() {
        let nodes = make_nodes(1);
        let topo = ClusterTopology::single_node(0, nodes[0].clone());

        // All 256 shards should belong to node 0
        for shard_id in 0..NUM_SHARDS as u16 {
            assert_eq!(topo.primary_for_shard(shard_id), 0);
            assert!(topo.is_local_primary(shard_id));
        }
        assert_eq!(topo.shards_for_node(0).len(), NUM_SHARDS);
    }

    #[test]
    fn test_three_node_topology() {
        let nodes = make_nodes(3);
        let topo = ClusterTopology::new(0, nodes, 2);

        // All 256 shards should have a primary
        let mut shard_counts = [0u32; 3];
        for shard_id in 0..NUM_SHARDS as u16 {
            let primary = topo.primary_for_shard(shard_id);
            assert!(primary < 3, "Primary {} out of range for shard {}", primary, shard_id);
            shard_counts[primary as usize] += 1;
        }

        // Each node should own roughly 256/3 ≈ 85-86 shards
        for (node, count) in shard_counts.iter().enumerate() {
            assert!(
                *count >= 80 && *count <= 90,
                "Node {} has {} shards, expected ~85",
                node,
                count
            );
        }

        // Each shard with replication_factor=2 should have 1 replica
        for shard_id in 0..NUM_SHARDS as u16 {
            let nodes = topo.nodes_for_shard(shard_id);
            assert_eq!(
                nodes.len(),
                2,
                "Shard {} has {} nodes, expected 2",
                shard_id,
                nodes.len()
            );
            // Primary and replica should be different
            assert_ne!(nodes[0], nodes[1], "Shard {} has same primary and replica", shard_id);
        }
    }

    #[test]
    fn test_shard_for_key() {
        // Deterministic: same hash -> same shard
        let shard1 = ClusterTopology::shard_for_key(0xFF00000000000000);
        let shard2 = ClusterTopology::shard_for_key(0xFF00000000000000);
        assert_eq!(shard1, shard2);

        // shard_id is always < 256
        for hash in [0u64, 1, u64::MAX, 0xAB00000000000000] {
            let shard = ClusterTopology::shard_for_key(hash);
            assert!(shard < 256, "Shard {} >= 256 for hash {:#x}", shard, hash);
        }
    }

    #[test]
    fn test_rebalance_on_node_failure() {
        let mut nodes = make_nodes(3);
        let mut topo = ClusterTopology::new(0, nodes.clone(), 2);

        // Mark node 1 as dead
        topo.nodes[1].status = NodeStatus::Dead;

        let moves = topo.rebalance();

        // Some shards should have moved away from node 1
        assert!(!moves.is_empty(), "Expected shard moves after node failure");

        // No shard should be assigned to dead node 1
        for shard_id in 0..NUM_SHARDS as u16 {
            assert_ne!(
                topo.primary_for_shard(shard_id),
                1,
                "Shard {} still assigned to dead node 1",
                shard_id
            );
        }
    }

    #[test]
    fn test_local_shard_checks() {
        let nodes = make_nodes(3);
        let topo = ClusterTopology::new(0, nodes, 2);

        let local_shards = topo.shards_for_node(0);
        for &shard_id in &local_shards {
            assert!(topo.is_local_primary(shard_id));
            assert!(topo.is_local_shard(shard_id));
        }
    }

    #[test]
    fn test_topology_version() {
        let mut nodes = make_nodes(3);
        let mut topo = ClusterTopology::new(0, nodes, 2);
        let v1 = topo.current_version();

        topo.nodes[1].status = NodeStatus::Dead;
        topo.rebalance();
        let v2 = topo.current_version();
        assert!(v2 > v1, "Version should increase after rebalance");
    }
}
