//! Shard rebalancing: migrate data when nodes join or leave the cluster.
//!
//! When the topology changes (scale up/down or failover), shards may need
//! to be migrated between nodes. This module handles the migration process:
//! 1. Source node streams all data for migrating shards to the target node.
//! 2. Target node applies the data to its local handler.
//! 3. Once complete, the shard assignment is updated.

use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tracing::{info, warn};

use crate::cluster::node::NodeId;
use crate::cluster::peer_pool::{PeerConnectionPool, PeerMessage, PeerOp};
use crate::cluster::topology::{ClusterTopology, ShardState};
use crate::engine::index::Index;
use crate::server::handler::Handler;

/// Statistics for an ongoing rebalance operation.
#[derive(Debug, Default)]
pub struct RebalanceStats {
    /// Total keys migrated.
    pub keys_migrated: AtomicU64,
    /// Total shards migrated.
    pub shards_completed: AtomicU64,
    /// Total errors during migration.
    pub errors: AtomicU64,
    /// Whether rebalancing is in progress.
    pub in_progress: AtomicBool,
}

/// Manages shard rebalancing for the cluster.
pub struct RebalanceManager {
    topology: Arc<RwLock<ClusterTopology>>,
    local_handler: Arc<Handler>,
    index: Arc<Index>,
    peers: Arc<PeerConnectionPool>,
    stats: Arc<RebalanceStats>,
}

impl RebalanceManager {
    pub fn new(
        topology: Arc<RwLock<ClusterTopology>>,
        local_handler: Arc<Handler>,
        index: Arc<Index>,
        peers: Arc<PeerConnectionPool>,
    ) -> Self {
        Self {
            topology,
            local_handler,
            index,
            peers,
            stats: Arc::new(RebalanceStats::default()),
        }
    }

    /// Returns the rebalance stats.
    pub fn stats(&self) -> Arc<RebalanceStats> {
        Arc::clone(&self.stats)
    }

    /// Triggers a rebalance and returns the list of shard moves.
    pub fn compute_rebalance(&self) -> Vec<(u16, NodeId, NodeId)> {
        let mut topo = self.topology.write();
        topo.rebalance()
    }

    /// Exports all keys from a local shard by iterating the index.
    /// Returns a list of (key, value) pairs for the given shard.
    pub fn export_shard(&self, shard_id: u16) -> Vec<(Vec<u8>, Vec<u8>)> {
        let entries = self.index.iter_shard(shard_id as usize);
        let mut kv_pairs = Vec::new();

        for entry in entries {
            if !entry.is_live() {
                continue;
            }
            let key = entry.key.as_bytes().to_vec();
            if let Some(value) = self.local_handler.get_value(&key) {
                kv_pairs.push((key, value));
            }
        }

        kv_pairs
    }

    /// Sends shard data to a target node.
    pub async fn send_shard_to_node(
        &self,
        shard_id: u16,
        target_node: NodeId,
    ) -> io::Result<u64> {
        let kv_pairs = self.export_shard(shard_id);
        let total = kv_pairs.len() as u64;

        info!(
            "Migrating shard {} to node {} ({} keys)",
            shard_id, target_node, total
        );

        // Mark shard as migrating
        {
            let mut topo = self.topology.write();
            let local = topo.local_node_id;
            topo.shard_map[shard_id as usize].state = ShardState::Migrating {
                source: local,
                target: target_node,
            };
        }

        let mut migrated = 0u64;
        for (key, value) in kv_pairs {
            let msg = PeerMessage {
                op: PeerOp::ReplicatePut,
                key,
                value,
                ttl: 0, // TTL not preserved in migration (simplification)
                shard_id,
            };

            match self.peers.send_async(target_node, &msg).await {
                Ok(()) => {
                    migrated += 1;
                    self.stats.keys_migrated.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    warn!(
                        "Failed to migrate key to node {} for shard {}: {}",
                        target_node, shard_id, e
                    );
                    self.stats.errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        // Mark shard migration as complete
        {
            let mut topo = self.topology.write();
            topo.shard_map[shard_id as usize].state = ShardState::Active;
        }
        self.stats.shards_completed.fetch_add(1, Ordering::Relaxed);

        info!(
            "Shard {} migration complete: {}/{} keys sent to node {}",
            shard_id, migrated, total, target_node
        );

        Ok(migrated)
    }

    /// Executes all pending shard moves (sends shards this node is losing).
    pub async fn execute_rebalance(&self, moves: Vec<(u16, NodeId, NodeId)>) -> io::Result<()> {
        let local_id = {
            self.topology.read().local_node_id
        };

        self.stats.in_progress.store(true, Ordering::SeqCst);

        let outgoing_moves: Vec<_> = moves
            .iter()
            .filter(|(_, old_owner, _)| *old_owner == local_id)
            .collect();

        if outgoing_moves.is_empty() {
            info!("No outgoing shard migrations for this node");
            self.stats.in_progress.store(false, Ordering::SeqCst);
            return Ok(());
        }

        info!(
            "Starting rebalance: {} shards to migrate out",
            outgoing_moves.len()
        );

        for (shard_id, _, new_owner) in &outgoing_moves {
            if let Err(e) = self.send_shard_to_node(*shard_id, *new_owner).await {
                warn!("Failed to migrate shard {}: {}", shard_id, e);
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
            }
        }

        self.stats.in_progress.store(false, Ordering::SeqCst);
        info!("Rebalance complete");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::node::NodeInfo;
    use crate::cluster::topology::ClusterTopology;
    use crate::storage::file_manager::FileManager;
    use crate::storage::write_buffer::WriteBuffer;
    use tempfile::tempdir;

    fn create_rebalance_setup() -> (RebalanceManager, Arc<Handler>, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let fm = Arc::new(FileManager::new(dir.path()).unwrap());
        fm.create_file().unwrap();
        let index = Arc::new(Index::new());
        let wb = Arc::new(WriteBuffer::new(0, 1023));
        let handler = Arc::new(Handler::new(Arc::clone(&index), fm, wb));

        let node = NodeInfo::new(
            0,
            "127.0.0.1:7777".parse().unwrap(),
            "127.0.0.1:7780".parse().unwrap(),
        );
        let topo = Arc::new(RwLock::new(ClusterTopology::single_node(0, node)));
        let peers = Arc::new(PeerConnectionPool::new());

        let mgr = RebalanceManager::new(topo, Arc::clone(&handler), index, peers);
        (mgr, handler, dir)
    }

    #[test]
    fn test_export_shard() {
        let (mgr, handler, _dir) = create_rebalance_setup();

        // Insert some keys
        for i in 0..100 {
            let key = format!("export_key_{}", i);
            let value = format!("export_val_{}", i);
            handler.put_sync(key.as_bytes(), value.as_bytes(), 0).unwrap();
        }

        // Export all 256 shards and check total
        let mut total_exported = 0;
        for shard_id in 0..256u16 {
            let pairs = mgr.export_shard(shard_id);
            total_exported += pairs.len();
        }

        assert_eq!(total_exported, 100, "Should export all 100 keys across all shards");
    }

    #[test]
    fn test_compute_rebalance_single_node() {
        let (mgr, _, _dir) = create_rebalance_setup();
        // Single node: rebalance should produce no moves
        let moves = mgr.compute_rebalance();
        assert!(moves.is_empty(), "Single node should have no rebalance moves");
    }

    #[test]
    fn test_rebalance_stats_initial() {
        let (mgr, _, _dir) = create_rebalance_setup();
        let stats = mgr.stats();
        assert_eq!(stats.keys_migrated.load(Ordering::Relaxed), 0);
        assert_eq!(stats.shards_completed.load(Ordering::Relaxed), 0);
        assert!(!stats.in_progress.load(Ordering::Relaxed));
    }
}
