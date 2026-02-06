//! Health checking: periodic heartbeats to detect node failures.
//!
//! The health checker pings all peer nodes at a configurable interval.
//! After N missed heartbeats, the node is marked as suspect, then dead.
//! When a node is marked dead, failover is triggered.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tracing::{debug, info, warn};

use crate::cluster::node::{NodeId, NodeStatus};
use crate::cluster::peer_pool::{PeerConnectionPool, PeerMessage, PeerOp};
use crate::cluster::topology::ClusterTopology;

/// Tracks the health state of a single peer node.
#[derive(Debug, Clone)]
pub struct PeerHealth {
    /// Number of consecutive missed heartbeats.
    pub missed_heartbeats: u32,
    /// Time of last successful heartbeat.
    pub last_heartbeat: Instant,
    /// Current status.
    pub status: NodeStatus,
}

impl PeerHealth {
    fn new() -> Self {
        Self {
            missed_heartbeats: 0,
            last_heartbeat: Instant::now(),
            status: NodeStatus::Active,
        }
    }
}

/// Configuration for the health checker.
#[derive(Debug, Clone)]
pub struct HealthConfig {
    /// Interval between health checks.
    pub check_interval: Duration,
    /// Number of missed heartbeats before marking suspect.
    pub suspect_threshold: u32,
    /// Number of missed heartbeats before marking dead.
    pub dead_threshold: u32,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(1),
            suspect_threshold: 2,
            dead_threshold: 3,
        }
    }
}

/// Health checker that runs in a background task.
pub struct HealthChecker {
    topology: Arc<RwLock<ClusterTopology>>,
    peers: Arc<PeerConnectionPool>,
    config: HealthConfig,
    peer_health: Arc<RwLock<HashMap<NodeId, PeerHealth>>>,
    stop: Arc<AtomicBool>,
}

impl HealthChecker {
    pub fn new(
        topology: Arc<RwLock<ClusterTopology>>,
        peers: Arc<PeerConnectionPool>,
        config: HealthConfig,
    ) -> Self {
        let peer_health = {
            let topo = topology.read();
            let mut health = HashMap::new();
            for node in &topo.nodes {
                if node.id != topo.local_node_id {
                    health.insert(node.id, PeerHealth::new());
                }
            }
            Arc::new(RwLock::new(health))
        };

        Self {
            topology,
            peers,
            config,
            peer_health,
            stop: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Returns a handle to stop the health checker.
    pub fn stop_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.stop)
    }

    /// Returns the current health status of all peers.
    pub fn peer_health(&self) -> Arc<RwLock<HashMap<NodeId, PeerHealth>>> {
        Arc::clone(&self.peer_health)
    }

    /// Runs the health check loop. Call this in a tokio::spawn.
    pub async fn run(&self) {
        info!("Health checker started (interval={}ms, dead_threshold={})",
            self.config.check_interval.as_millis(),
            self.config.dead_threshold,
        );

        while !self.stop.load(Ordering::Relaxed) {
            self.check_all_peers().await;
            tokio::time::sleep(self.config.check_interval).await;
        }

        info!("Health checker stopped");
    }

    /// Checks all peer nodes once.
    async fn check_all_peers(&self) {
        let peer_ids = self.peers.peer_ids();

        for node_id in peer_ids {
            let heartbeat_msg = PeerMessage {
                op: PeerOp::Heartbeat,
                key: Vec::new(),
                value: Vec::new(),
                ttl: 0,
                shard_id: 0,
            };

            let timeout = self.config.check_interval / 2;
            let result = tokio::time::timeout(
                timeout,
                self.peers.send_request(node_id, &heartbeat_msg),
            )
            .await;

            let mut health = self.peer_health.write();
            let entry = health.entry(node_id).or_insert_with(PeerHealth::new);

            match result {
                Ok(Ok(resp)) if resp.op == PeerOp::HeartbeatAck => {
                    // Node is alive
                    if entry.missed_heartbeats > 0 {
                        debug!("Node {} recovered (was {} missed)", node_id, entry.missed_heartbeats);
                    }
                    entry.missed_heartbeats = 0;
                    entry.last_heartbeat = Instant::now();

                    if entry.status != NodeStatus::Active {
                        entry.status = NodeStatus::Active;
                        self.update_node_status(node_id, NodeStatus::Active);
                    }
                }
                _ => {
                    // Heartbeat failed
                    entry.missed_heartbeats += 1;
                    debug!(
                        "Node {} missed heartbeat #{}", node_id, entry.missed_heartbeats
                    );

                    if entry.missed_heartbeats >= self.config.dead_threshold {
                        if entry.status != NodeStatus::Dead {
                            warn!("Node {} marked as DEAD after {} missed heartbeats",
                                node_id, entry.missed_heartbeats);
                            entry.status = NodeStatus::Dead;
                            self.update_node_status(node_id, NodeStatus::Dead);
                            self.trigger_failover(node_id);
                        }
                    } else if entry.missed_heartbeats >= self.config.suspect_threshold {
                        if entry.status != NodeStatus::Suspect {
                            warn!("Node {} marked as SUSPECT after {} missed heartbeats",
                                node_id, entry.missed_heartbeats);
                            entry.status = NodeStatus::Suspect;
                            self.update_node_status(node_id, NodeStatus::Suspect);
                        }
                    }
                }
            }
        }
    }

    /// Updates the node status in the topology.
    fn update_node_status(&self, node_id: NodeId, status: NodeStatus) {
        let mut topo = self.topology.write();
        if let Some(node) = topo.get_node_mut(node_id) {
            node.status = status;
        }
    }

    /// Triggers failover for a dead node by rebalancing shard assignments.
    fn trigger_failover(&self, dead_node_id: NodeId) {
        info!("Triggering failover for dead node {}", dead_node_id);
        let mut topo = self.topology.write();
        let moves = topo.rebalance();
        if !moves.is_empty() {
            info!(
                "Failover complete: {} shards reassigned (version {})",
                moves.len(),
                topo.current_version()
            );
            for (shard, old, new) in &moves {
                debug!("  Shard {} moved: node {} -> node {}", shard, old, new);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::node::NodeInfo;

    #[test]
    fn test_peer_health_initial() {
        let h = PeerHealth::new();
        assert_eq!(h.missed_heartbeats, 0);
        assert_eq!(h.status, NodeStatus::Active);
    }

    #[test]
    fn test_health_config_default() {
        let config = HealthConfig::default();
        assert_eq!(config.check_interval, Duration::from_secs(1));
        assert_eq!(config.suspect_threshold, 2);
        assert_eq!(config.dead_threshold, 3);
    }

    #[test]
    fn test_health_checker_creation() {
        let nodes = vec![
            NodeInfo::new(0, "127.0.0.1:7777".parse().unwrap(), "127.0.0.1:7780".parse().unwrap()),
            NodeInfo::new(1, "127.0.0.1:7778".parse().unwrap(), "127.0.0.1:7781".parse().unwrap()),
            NodeInfo::new(2, "127.0.0.1:7779".parse().unwrap(), "127.0.0.1:7782".parse().unwrap()),
        ];
        let topo = Arc::new(RwLock::new(ClusterTopology::new(0, nodes, 2)));
        let peers = Arc::new(PeerConnectionPool::new());
        let checker = HealthChecker::new(topo, peers, HealthConfig::default());

        // Should track 2 peers (not self)
        let health = checker.peer_health();
        let h = health.read();
        assert_eq!(h.len(), 2);
        assert!(h.contains_key(&1));
        assert!(h.contains_key(&2));
        assert!(!h.contains_key(&0)); // Local node not tracked
    }
}
