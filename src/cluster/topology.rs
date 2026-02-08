//! Slot-to-node mapping and cluster topology.
//!
//! The cluster uses 16,384 hash slots (matching Redis Cluster protocol).
//! Slot assignment: `slot = CRC16(key) % 16384`, with hash tag support
//! for `{tag}` extraction. Routing is O(1) via the slot map.
//!
//! The internal storage engine still uses its 256-shard index independently.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::cluster::node::{NodeId, NodeInfo, NodeStatus};

/// Total number of hash slots in a Redis Cluster.
pub const NUM_SLOTS: usize = 16384;

/// CRC16 lookup table (CCITT variant used by Redis Cluster).
static CRC16_TAB: [u16; 256] = {
    let mut tab = [0u16; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = (i as u16) << 8;
        let mut j = 0;
        while j < 8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
            j += 1;
        }
        tab[i] = crc;
        i += 1;
    }
    tab
};

/// Computes the CRC16 hash of a byte slice (CCITT variant, same as Redis).
#[inline]
pub fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &b in data {
        let idx = ((crc >> 8) ^ (b as u16)) as usize;
        crc = (crc << 8) ^ CRC16_TAB[idx & 0xFF];
    }
    crc
}

/// Extracts the hash tag from a key, following Redis Cluster rules.
/// If the key contains `{...}` with a non-empty substring between the
/// first `{` and the first subsequent `}`, that substring is used.
/// Otherwise the whole key is used.
#[inline]
pub fn extract_hash_tag(key: &[u8]) -> &[u8] {
    if let Some(start) = key.iter().position(|&b| b == b'{') {
        if let Some(end) = key[start + 1..].iter().position(|&b| b == b'}') {
            if end > 0 {
                return &key[start + 1..start + 1 + end];
            }
        }
    }
    key
}

/// Computes the Redis Cluster hash slot for a key.
/// Supports `{hashtag}` syntax.
#[inline]
pub fn key_hash_slot(key: &[u8]) -> u16 {
    let tag = extract_hash_tag(key);
    crc16(tag) % NUM_SLOTS as u16
}

/// Assignment of a single slot to a primary and replica nodes.
#[derive(Debug, Clone)]
pub struct ShardAssignment {
    /// The primary node that owns this slot.
    pub primary: NodeId,
    /// Replica nodes for this slot (in priority order for failover).
    pub replicas: Vec<NodeId>,
    /// Current state of the slot.
    pub state: ShardState,
}

/// State of a slot during normal operation and migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardState {
    /// Slot is serving normally.
    Active,
    /// Slot is being migrated to a new node.
    Migrating {
        /// Node the slot is migrating from.
        source: NodeId,
        /// Node the slot is migrating to.
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

/// The full cluster topology: maps each of the 16384 hash slots to nodes.
pub struct ClusterTopology {
    /// All known nodes in the cluster.
    pub nodes: Vec<NodeInfo>,
    /// Slot assignment map: slot_id -> assignment.
    pub shard_map: Vec<ShardAssignment>,
    /// This node's ID.
    pub local_node_id: NodeId,
    /// Replication factor (number of copies including primary).
    pub replication_factor: u8,
    /// Topology version, incremented on every change.
    pub version: AtomicU64,
}

impl ClusterTopology {
    /// Creates a new topology with slots evenly distributed across `num_nodes` nodes.
    pub fn new(
        local_node_id: NodeId,
        nodes: Vec<NodeInfo>,
        replication_factor: u8,
    ) -> Self {
        let num_nodes = nodes.len();
        let mut shard_map = Vec::with_capacity(NUM_SLOTS);

        let slots_per = Self::slots_per_node(num_nodes);

        for slot_id in 0..NUM_SLOTS {
            let primary = (slot_id / slots_per) as NodeId;
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
        let mut shard_map = Vec::with_capacity(NUM_SLOTS);
        for _ in 0..NUM_SLOTS {
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

    /// Returns the number of slots each node should own (approximately).
    fn slots_per_node(num_nodes: usize) -> usize {
        if num_nodes == 0 {
            NUM_SLOTS
        } else {
            (NUM_SLOTS + num_nodes - 1) / num_nodes
        }
    }

    /// O(1) lookup: returns the primary node for a given slot.
    #[inline]
    pub fn primary_for_shard(&self, slot_id: u16) -> NodeId {
        self.shard_map[slot_id as usize].primary
    }

    /// Returns the primary + replica nodes for a slot.
    pub fn nodes_for_shard(&self, slot_id: u16) -> Vec<NodeId> {
        let assignment = &self.shard_map[slot_id as usize];
        let mut nodes = vec![assignment.primary];
        nodes.extend_from_slice(&assignment.replicas);
        nodes
    }

    /// Returns true if this node is the primary for the given slot.
    #[inline]
    pub fn is_local_primary(&self, slot_id: u16) -> bool {
        self.shard_map[slot_id as usize].primary == self.local_node_id
    }

    /// Returns true if this node is a replica for the given slot.
    pub fn is_local_replica(&self, slot_id: u16) -> bool {
        self.shard_map[slot_id as usize]
            .replicas
            .contains(&self.local_node_id)
    }

    /// Returns true if this node owns (primary or replica) the given slot.
    pub fn is_local_shard(&self, slot_id: u16) -> bool {
        self.is_local_primary(slot_id) || self.is_local_replica(slot_id)
    }

    /// Returns all slot IDs owned by the given node as primary.
    pub fn shards_for_node(&self, node_id: NodeId) -> Vec<u16> {
        self.shard_map
            .iter()
            .enumerate()
            .filter(|(_, a)| a.primary == node_id)
            .map(|(i, _)| i as u16)
            .collect()
    }

    /// Returns contiguous slot ranges owned by a node as primary.
    /// Each entry is (start_slot, end_slot) inclusive.
    pub fn slot_ranges_for_node(&self, node_id: NodeId) -> Vec<(u16, u16)> {
        let mut ranges = Vec::new();
        let mut start: Option<u16> = None;

        for (slot_id, assignment) in self.shard_map.iter().enumerate() {
            if assignment.primary == node_id {
                if start.is_none() {
                    start = Some(slot_id as u16);
                }
            } else if let Some(s) = start {
                ranges.push((s, (slot_id - 1) as u16));
                start = None;
            }
        }
        if let Some(s) = start {
            ranges.push((s, (NUM_SLOTS - 1) as u16));
        }
        ranges
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

    /// Recalculates slot assignments after node changes.
    /// Returns a list of (slot_id, old_primary, new_primary) for slots that moved.
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
        let spn = Self::slots_per_node(num_active);
        let mut moves = Vec::new();

        for (slot_id, assignment) in self.shard_map.iter_mut().enumerate() {
            let ideal_owner_idx = (slot_id / spn).min(num_active - 1);
            let ideal_owner = active_nodes[ideal_owner_idx];

            if assignment.primary != ideal_owner {
                let old = assignment.primary;
                assignment.primary = ideal_owner;
                moves.push((slot_id as u16, old, ideal_owner));
            }

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

    /// Computes the hash slot for a key using CRC16 with hash tag support.
    #[inline]
    pub fn slot_for_key(key: &[u8]) -> u16 {
        key_hash_slot(key)
    }

    /// Maps an xxHash3 key hash to an internal index shard (0..255).
    /// Used by rebalance.rs which operates on the 256-shard storage index.
    #[inline]
    pub fn shard_for_key(key_hash: u64) -> u16 {
        (key_hash >> 56) as u16 % 256
    }

    /// Returns the current topology version.
    pub fn current_version(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }

    /// Returns the Redis address of the primary for a given slot.
    pub fn redis_addr_for_slot(&self, slot_id: u16) -> Option<SocketAddr> {
        let node_id = self.primary_for_shard(slot_id);
        self.get_node(node_id).map(|n| n.redis_addr)
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
    fn test_crc16_known_values() {
        // Redis uses CRC16/CCITT. Known test vectors:
        assert_eq!(crc16(b""), 0x0000);
        assert_eq!(crc16(b"123456789"), 0x31C3);
    }

    #[test]
    fn test_hash_tag_extraction() {
        assert_eq!(extract_hash_tag(b"foo{bar}baz"), b"bar");
        assert_eq!(extract_hash_tag(b"{bar}"), b"bar");
        assert_eq!(extract_hash_tag(b"foo{bar}{zap}"), b"bar"); // first tag
        assert_eq!(extract_hash_tag(b"foo{}bar"), b"foo{}bar"); // empty tag -> whole key
        assert_eq!(extract_hash_tag(b"foobar"), b"foobar"); // no braces
        assert_eq!(extract_hash_tag(b"foo{bar"), b"foo{bar"); // no closing brace
        assert_eq!(extract_hash_tag(b"foo}bar{"), b"foo}bar{"); // wrong order
    }

    #[test]
    fn test_hash_tag_same_slot() {
        // Keys with same hash tag should map to same slot
        let slot1 = key_hash_slot(b"user:{12345}:name");
        let slot2 = key_hash_slot(b"user:{12345}:email");
        let slot3 = key_hash_slot(b"order:{12345}:items");
        assert_eq!(slot1, slot2);
        assert_eq!(slot2, slot3);
    }

    #[test]
    fn test_key_hash_slot_range() {
        for i in 0..10_000 {
            let key = format!("test_key_{}", i);
            let slot = key_hash_slot(key.as_bytes());
            assert!(slot < NUM_SLOTS as u16, "Slot {} >= {} for key {}", slot, NUM_SLOTS, key);
        }
    }

    #[test]
    fn test_single_node_topology() {
        let nodes = make_nodes(1);
        let topo = ClusterTopology::single_node(0, nodes[0].clone());

        for slot_id in 0..NUM_SLOTS as u16 {
            assert_eq!(topo.primary_for_shard(slot_id), 0);
            assert!(topo.is_local_primary(slot_id));
        }
        assert_eq!(topo.shards_for_node(0).len(), NUM_SLOTS);
    }

    #[test]
    fn test_three_node_topology() {
        let nodes = make_nodes(3);
        let topo = ClusterTopology::new(0, nodes, 2);

        let mut counts = [0u32; 3];
        for slot_id in 0..NUM_SLOTS as u16 {
            let primary = topo.primary_for_shard(slot_id);
            assert!(primary < 3);
            counts[primary as usize] += 1;
        }

        // Each node should own roughly 16384/3 ≈ 5461
        for (node, count) in counts.iter().enumerate() {
            assert!(
                *count >= 5000 && *count <= 5600,
                "Node {} has {} slots, expected ~5461",
                node,
                count
            );
        }

        // With RF=2, each slot has 1 replica
        for slot_id in 0..NUM_SLOTS as u16 {
            let nodes = topo.nodes_for_shard(slot_id);
            assert_eq!(nodes.len(), 2);
            assert_ne!(nodes[0], nodes[1]);
        }
    }

    #[test]
    fn test_slot_ranges_for_node() {
        let nodes = make_nodes(3);
        let topo = ClusterTopology::new(0, nodes, 1);

        // Each node should have one contiguous range
        for node_id in 0..3u32 {
            let ranges = topo.slot_ranges_for_node(node_id);
            assert!(!ranges.is_empty(), "Node {} has no ranges", node_id);

            // Total slots across ranges should match shards_for_node count
            let total: usize = ranges.iter().map(|(s, e)| (*e as usize - *s as usize + 1)).sum();
            assert_eq!(total, topo.shards_for_node(node_id).len());
        }
    }

    #[test]
    fn test_rebalance_on_node_failure() {
        let nodes = make_nodes(3);
        let mut topo = ClusterTopology::new(0, nodes, 2);

        topo.nodes[1].status = NodeStatus::Dead;
        let moves = topo.rebalance();
        assert!(!moves.is_empty());

        for slot_id in 0..NUM_SLOTS as u16 {
            assert_ne!(topo.primary_for_shard(slot_id), 1);
        }
    }

    #[test]
    fn test_local_shard_checks() {
        let nodes = make_nodes(3);
        let topo = ClusterTopology::new(0, nodes, 2);

        let local_slots = topo.shards_for_node(0);
        for &slot_id in &local_slots {
            assert!(topo.is_local_primary(slot_id));
            assert!(topo.is_local_shard(slot_id));
        }
    }

    #[test]
    fn test_topology_version() {
        let nodes = make_nodes(3);
        let mut topo = ClusterTopology::new(0, nodes, 2);
        let v1 = topo.current_version();

        topo.nodes[1].status = NodeStatus::Dead;
        topo.rebalance();
        let v2 = topo.current_version();
        assert!(v2 > v1);
    }

    #[test]
    fn test_redis_addr_for_slot() {
        let nodes = make_nodes(3);
        let topo = ClusterTopology::new(0, nodes, 1);

        let addr = topo.redis_addr_for_slot(0).unwrap();
        assert_eq!(addr.port(), 7777); // Node 0

        // Last slot should go to node 2
        let last_slot = (NUM_SLOTS - 1) as u16;
        let addr = topo.redis_addr_for_slot(last_slot).unwrap();
        assert_eq!(addr.port(), 7779); // Node 2
    }

    #[test]
    fn test_slot_distribution_well_spread() {
        let mut slot_counts = vec![0u32; NUM_SLOTS];
        for i in 0..100_000 {
            let key = format!("distrib_key_{}", i);
            let slot = key_hash_slot(key.as_bytes()) as usize;
            slot_counts[slot] += 1;
        }
        let non_empty = slot_counts.iter().filter(|c| **c > 0).count();
        assert!(
            non_empty > 14000,
            "Only {} of {} slots hit with 100k keys",
            non_empty,
            NUM_SLOTS,
        );
    }
}
