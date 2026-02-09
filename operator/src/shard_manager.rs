//! Shard assignment computation for the Kubernetes operator.
//!
//! Computes how the 256 shards are distributed across N nodes,
//! including replica assignments.

use serde::{Deserialize, Serialize};

const NUM_SHARDS: u32 = 256;

/// A shard assignment entry: which node is primary and which are replicas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardAssignmentEntry {
    pub shard_id: u32,
    pub primary: u32,
    pub replicas: Vec<u32>,
}

/// Complete shard map for the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardMap {
    pub assignments: Vec<ShardAssignmentEntry>,
    pub num_nodes: u32,
    pub replication_factor: u32,
}

/// Computes the shard map for `num_nodes` nodes with the given replication factor.
pub fn compute_shard_map(num_nodes: u32, replication_factor: u32) -> ShardMap {
    let rf = replication_factor.min(num_nodes);
    let shards_per_node = (NUM_SHARDS + num_nodes - 1) / num_nodes;

    let mut assignments = Vec::with_capacity(NUM_SHARDS as usize);

    for shard_id in 0..NUM_SHARDS {
        let primary = (shard_id / shards_per_node).min(num_nodes - 1);

        let mut replicas = Vec::new();
        for r in 1..rf {
            let replica = (primary + r) % num_nodes;
            replicas.push(replica);
        }

        assignments.push(ShardAssignmentEntry {
            shard_id,
            primary,
            replicas,
        });
    }

    ShardMap {
        assignments,
        num_nodes,
        replication_factor: rf,
    }
}

/// Computes the diff between old and new shard maps.
/// Returns (shard_id, old_primary, new_primary) for moved shards.
pub fn compute_migration_plan(old: &ShardMap, new: &ShardMap) -> Vec<(u32, u32, u32)> {
    let mut moves = Vec::new();
    for (old_entry, new_entry) in old.assignments.iter().zip(new.assignments.iter()) {
        if old_entry.primary != new_entry.primary {
            moves.push((old_entry.shard_id, old_entry.primary, new_entry.primary));
        }
    }
    moves
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_node_shard_map() {
        let map = compute_shard_map(1, 1);
        assert_eq!(map.assignments.len(), 256);
        for entry in &map.assignments {
            assert_eq!(entry.primary, 0);
            assert!(entry.replicas.is_empty());
        }
    }

    #[test]
    fn test_three_node_shard_map() {
        let map = compute_shard_map(3, 2);
        assert_eq!(map.assignments.len(), 256);

        let mut counts = [0u32; 3];
        for entry in &map.assignments {
            assert!(entry.primary < 3);
            counts[entry.primary as usize] += 1;
            // With RF=2, each shard should have 1 replica
            assert_eq!(entry.replicas.len(), 1);
            assert_ne!(entry.primary, entry.replicas[0]);
        }

        // Each node should own roughly 85-86 shards
        for (node, count) in counts.iter().enumerate() {
            assert!(
                *count >= 80 && *count <= 90,
                "Node {} has {} shards",
                node,
                count
            );
        }
    }

    #[test]
    fn test_replication_factor_capped() {
        // RF > num_nodes should be capped
        let map = compute_shard_map(2, 5);
        assert_eq!(map.replication_factor, 2);
        for entry in &map.assignments {
            assert_eq!(entry.replicas.len(), 1); // RF=2: 1 replica
        }
    }

    #[test]
    fn test_migration_plan() {
        let old = compute_shard_map(3, 2);
        let new = compute_shard_map(4, 2);

        let moves = compute_migration_plan(&old, &new);
        // Some shards should move to the new node (node 3)
        assert!(
            !moves.is_empty(),
            "Scaling 3->4 should produce shard migrations"
        );

        // At least some moves should target node 3
        let moves_to_3: Vec<_> = moves.iter().filter(|(_, _, target)| *target == 3).collect();
        assert!(
            !moves_to_3.is_empty(),
            "Some shards should migrate to the new node 3"
        );
    }

    #[test]
    fn test_no_migration_same_config() {
        let map1 = compute_shard_map(3, 2);
        let map2 = compute_shard_map(3, 2);
        let moves = compute_migration_plan(&map1, &map2);
        assert!(moves.is_empty(), "Same config should produce no migrations");
    }
}
