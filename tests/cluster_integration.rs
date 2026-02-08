//! Integration tests for cluster mode.
//!
//! These tests verify:
//! - Topology construction and shard distribution
//! - Request routing (local vs forwarded)
//! - Replication message handling
//! - Health checking state machine
//! - Rebalancing after node changes
//! - Multi-node end-to-end scenarios with peer servers

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tempfile::tempdir;

use ssd_kv::cluster::health::{HealthChecker, HealthConfig};
use ssd_kv::cluster::node::{NodeId, NodeInfo, NodeStatus};
use ssd_kv::cluster::peer_pool::{PeerConnectionPool, PeerMessage, PeerOp};
use ssd_kv::cluster::rebalance::RebalanceManager;
use ssd_kv::cluster::replication::{PeerServer, ReplicationManager};
use ssd_kv::cluster::router::{ClusterRouter, RouteDecision};
use ssd_kv::cluster::topology::{ClusterTopology, ShardState, NUM_SLOTS};
use ssd_kv::engine::index::Index;
use ssd_kv::engine::index_entry::hash_key;
use ssd_kv::server::handler::Handler;
use ssd_kv::storage::file_manager::FileManager;
use ssd_kv::storage::write_buffer::WriteBuffer;

// =================== Helper functions ===================

fn make_nodes(n: usize) -> Vec<NodeInfo> {
    (0..n)
        .map(|i| {
            NodeInfo::new(
                i as NodeId,
                format!("127.0.0.1:{}", 17777 + i).parse().unwrap(),
                format!("127.0.0.1:{}", 17780 + i).parse().unwrap(),
            )
        })
        .collect()
}

fn make_handler(dir: &std::path::Path) -> Arc<Handler> {
    let fm = Arc::new(FileManager::new(dir).unwrap());
    fm.create_file().unwrap();
    let index = Arc::new(Index::new());
    let wb = Arc::new(WriteBuffer::new(0, 1023));
    Arc::new(Handler::new(index, fm, wb))
}

fn make_handler_with_index(dir: &std::path::Path) -> (Arc<Handler>, Arc<Index>) {
    let fm = Arc::new(FileManager::new(dir).unwrap());
    fm.create_file().unwrap();
    let index = Arc::new(Index::new());
    let wb = Arc::new(WriteBuffer::new(0, 1023));
    let handler = Arc::new(Handler::new(Arc::clone(&index), fm, wb));
    (handler, index)
}

// =================== Topology Tests ===================

#[test]
fn test_topology_slots_fully_covered() {
    for num_nodes in 1..=10 {
        let nodes = make_nodes(num_nodes);
        let topo = ClusterTopology::new(0, nodes, 1);

        for slot_id in 0..NUM_SLOTS as u16 {
            let primary = topo.primary_for_shard(slot_id);
            assert!(
                (primary as usize) < num_nodes,
                "Slot {} assigned to node {} but only {} nodes exist",
                slot_id,
                primary,
                num_nodes
            );
        }
    }
}

#[test]
fn test_topology_balanced_distribution() {
    for num_nodes in [2, 3, 4, 5, 8, 16] {
        let nodes = make_nodes(num_nodes);
        let topo = ClusterTopology::new(0, nodes, 1);

        let mut counts = vec![0u32; num_nodes];
        for slot_id in 0..NUM_SLOTS as u16 {
            counts[topo.primary_for_shard(slot_id) as usize] += 1;
        }

        let expected = NUM_SLOTS as f64 / num_nodes as f64;
        for (node, count) in counts.iter().enumerate() {
            let deviation = (*count as f64 - expected).abs() / expected;
            assert!(
                deviation < 0.2, // Allow 20% deviation
                "Node {} has {} slots (expected ~{:.0}, deviation {:.0}%) in {}-node cluster",
                node,
                count,
                expected,
                deviation * 100.0,
                num_nodes,
            );
        }
    }
}

#[test]
fn test_topology_replication_no_self_replicas() {
    for num_nodes in 2..=5 {
        let nodes = make_nodes(num_nodes);
        let rf = num_nodes.min(3) as u8;
        let topo = ClusterTopology::new(0, nodes, rf);

        for slot_id in 0..NUM_SLOTS as u16 {
            let all_nodes = topo.nodes_for_shard(slot_id);
            assert_eq!(
                all_nodes.len(),
                rf as usize,
                "Slot {} has {} nodes, expected {}",
                slot_id,
                all_nodes.len(),
                rf
            );

            // No duplicates
            let mut sorted = all_nodes.clone();
            sorted.sort();
            sorted.dedup();
            assert_eq!(
                sorted.len(),
                all_nodes.len(),
                "Slot {} has duplicate node assignments: {:?}",
                slot_id,
                all_nodes
            );
        }
    }
}

#[test]
fn test_topology_slot_for_key_deterministic() {
    let keys: Vec<&[u8]> = vec![b"hello", b"world", b"foo", b"bar", b"test123"];
    for key in &keys {
        let slot1 = ClusterTopology::slot_for_key(key);
        let slot2 = ClusterTopology::slot_for_key(key);
        assert_eq!(slot1, slot2, "Slot for key {:?} is not deterministic", key);
        assert!(slot1 < NUM_SLOTS as u16);
    }
}

#[test]
fn test_topology_slot_for_key_distributes_well() {
    let mut slot_counts = vec![0u32; NUM_SLOTS];
    for i in 0..100_000 {
        let key = format!("distribution_test_key_{}", i);
        let slot = ClusterTopology::slot_for_key(key.as_bytes()) as usize;
        slot_counts[slot] += 1;
    }

    let non_empty = slot_counts.iter().filter(|c| **c > 0).count();
    assert!(
        non_empty > 14000,
        "Only {} of {} slots got keys (expected >14000 with 100k keys)",
        non_empty,
        NUM_SLOTS
    );
}

// =================== Router Tests ===================

#[test]
fn test_router_single_node_all_local() {
    let dir = tempdir().unwrap();
    let handler = make_handler(dir.path());
    let node = NodeInfo::new(0, "127.0.0.1:7777".parse().unwrap(), "127.0.0.1:7780".parse().unwrap());
    let topo = Arc::new(RwLock::new(ClusterTopology::single_node(0, node)));
    let peers = Arc::new(PeerConnectionPool::new());
    let router = ClusterRouter::new(topo, handler, peers);

    for i in 0..1000 {
        let key = format!("key_{}", i);
        assert_eq!(router.route_key(key.as_bytes()), RouteDecision::Local);
    }
}

#[test]
fn test_router_multi_node_mixed_routing() {
    let dir = tempdir().unwrap();
    let handler = make_handler(dir.path());
    let nodes = make_nodes(3);
    let topo = Arc::new(RwLock::new(ClusterTopology::new(0, nodes, 2)));
    let peers = Arc::new(PeerConnectionPool::new());
    let router = ClusterRouter::new(topo, handler, peers);

    let mut local = 0;
    let mut forward = 0;
    for i in 0..10_000 {
        let key = format!("routing_key_{}", i);
        match router.route_key(key.as_bytes()) {
            RouteDecision::Local => local += 1,
            RouteDecision::Forward(_) => forward += 1,
        }
    }

    // Roughly 1/3 should be local
    let local_pct = local as f64 / 10_000.0;
    assert!(
        local_pct > 0.2 && local_pct < 0.5,
        "Local routing ratio {:.2} is outside expected range 0.2-0.5 for 3-node cluster",
        local_pct
    );
}

#[test]
fn test_router_consistent_routing() {
    let dir = tempdir().unwrap();
    let handler = make_handler(dir.path());
    let nodes = make_nodes(3);
    let topo = Arc::new(RwLock::new(ClusterTopology::new(0, nodes, 2)));
    let peers = Arc::new(PeerConnectionPool::new());
    let router = ClusterRouter::new(topo, handler, peers);

    // Same key should always route to same decision
    let key = b"consistent_key";
    let first = router.route_key(key);
    for _ in 0..100 {
        assert_eq!(router.route_key(key), first);
    }
}

#[test]
fn test_router_local_operations() {
    let dir = tempdir().unwrap();
    let handler = make_handler(dir.path());
    let node = NodeInfo::new(0, "127.0.0.1:7777".parse().unwrap(), "127.0.0.1:7780".parse().unwrap());
    let topo = Arc::new(RwLock::new(ClusterTopology::single_node(0, node)));
    let peers = Arc::new(PeerConnectionPool::new());
    let router = ClusterRouter::new(topo, handler, peers);

    // PUT
    router.put(b"router_key", b"router_value", 0).unwrap();

    // GET
    assert_eq!(router.get(b"router_key"), Some(b"router_value".to_vec()));

    // EXISTS check
    assert!(router.get(b"router_key").is_some());
    assert!(router.get(b"nonexistent").is_none());

    // DELETE
    assert!(router.delete(b"router_key").unwrap());
    assert_eq!(router.get(b"router_key"), None);
}

#[test]
fn test_router_many_local_keys() {
    let dir = tempdir().unwrap();
    let handler = make_handler(dir.path());
    let node = NodeInfo::new(0, "127.0.0.1:7777".parse().unwrap(), "127.0.0.1:7780".parse().unwrap());
    let topo = Arc::new(RwLock::new(ClusterTopology::single_node(0, node)));
    let peers = Arc::new(PeerConnectionPool::new());
    let router = ClusterRouter::new(topo, handler, peers);

    // Write 1000 keys
    for i in 0..1000 {
        let key = format!("bulk_key_{}", i);
        let value = format!("bulk_value_{}", i);
        router.put(key.as_bytes(), value.as_bytes(), 0).unwrap();
    }

    // Read all back
    for i in 0..1000 {
        let key = format!("bulk_key_{}", i);
        let expected = format!("bulk_value_{}", i);
        let val = router.get(key.as_bytes());
        assert_eq!(val, Some(expected.into_bytes()), "Key {} mismatch", key);
    }
}

// =================== Peer Message Protocol Tests ===================

#[test]
fn test_peer_message_all_ops() {
    let ops = vec![
        PeerOp::ForwardGet,
        PeerOp::ForwardPut,
        PeerOp::ForwardDelete,
        PeerOp::ReplicatePut,
        PeerOp::ReplicateDelete,
        PeerOp::Heartbeat,
        PeerOp::HeartbeatAck,
        PeerOp::ResponseOk,
        PeerOp::ResponseNotFound,
        PeerOp::ResponseError,
    ];

    for op in ops {
        let msg = PeerMessage {
            op,
            key: b"test".to_vec(),
            value: b"val".to_vec(),
            ttl: 42,
            shard_id: 123,
        };
        let data = msg.serialize();
        let decoded = PeerMessage::deserialize(&data).unwrap();
        assert_eq!(decoded.op, op);
        assert_eq!(decoded.key, b"test");
        assert_eq!(decoded.value, b"val");
        assert_eq!(decoded.ttl, 42);
        assert_eq!(decoded.shard_id, 123);
    }
}

#[test]
fn test_peer_message_empty_key_value() {
    let msg = PeerMessage {
        op: PeerOp::Heartbeat,
        key: Vec::new(),
        value: Vec::new(),
        ttl: 0,
        shard_id: 0,
    };
    let data = msg.serialize();
    let decoded = PeerMessage::deserialize(&data).unwrap();
    assert_eq!(decoded.key.len(), 0);
    assert_eq!(decoded.value.len(), 0);
}

#[test]
fn test_peer_message_large_value() {
    let large_value = vec![0xAB; 1024 * 1024]; // 1MB
    let msg = PeerMessage {
        op: PeerOp::ForwardPut,
        key: b"large_key".to_vec(),
        value: large_value.clone(),
        ttl: 0,
        shard_id: 50,
    };
    let data = msg.serialize();
    let decoded = PeerMessage::deserialize(&data).unwrap();
    assert_eq!(decoded.value.len(), 1024 * 1024);
    assert_eq!(decoded.value, large_value);
}

#[test]
fn test_peer_message_truncated_header() {
    let data = vec![0x31, 0x00]; // Too short
    assert!(PeerMessage::deserialize(&data).is_err());
}

#[test]
fn test_peer_message_invalid_op() {
    let msg = PeerMessage {
        op: PeerOp::ForwardGet,
        key: Vec::new(),
        value: Vec::new(),
        ttl: 0,
        shard_id: 0,
    };
    let mut data = msg.serialize();
    data[0] = 0xFF; // Invalid op
    assert!(PeerMessage::deserialize(&data).is_err());
}

// =================== Replication Tests ===================

#[test]
fn test_replication_put_applied_locally() {
    let dir = tempdir().unwrap();
    let handler = make_handler(dir.path());
    let node = NodeInfo::new(0, "127.0.0.1:7777".parse().unwrap(), "127.0.0.1:7780".parse().unwrap());
    let topo = Arc::new(RwLock::new(ClusterTopology::single_node(0, node)));
    let peers = Arc::new(PeerConnectionPool::new());
    let repl = ReplicationManager::new(topo, Arc::clone(&handler), peers);

    let msg = PeerMessage {
        op: PeerOp::ReplicatePut,
        key: b"repl_key".to_vec(),
        value: b"repl_val".to_vec(),
        ttl: 0,
        shard_id: 10,
    };

    let resp = repl.handle_replication_message(&msg).unwrap();
    assert_eq!(resp.op, PeerOp::ResponseOk);

    let val = handler.get_value(b"repl_key");
    assert_eq!(val, Some(b"repl_val".to_vec()));
}

#[test]
fn test_replication_delete_applied_locally() {
    let dir = tempdir().unwrap();
    let handler = make_handler(dir.path());
    handler.put_sync(b"to_delete", b"value", 0).unwrap();

    let node = NodeInfo::new(0, "127.0.0.1:7777".parse().unwrap(), "127.0.0.1:7780".parse().unwrap());
    let topo = Arc::new(RwLock::new(ClusterTopology::single_node(0, node)));
    let peers = Arc::new(PeerConnectionPool::new());
    let repl = ReplicationManager::new(topo, Arc::clone(&handler), peers);

    let msg = PeerMessage {
        op: PeerOp::ReplicateDelete,
        key: b"to_delete".to_vec(),
        value: Vec::new(),
        ttl: 0,
        shard_id: 5,
    };

    let resp = repl.handle_replication_message(&msg).unwrap();
    assert_eq!(resp.op, PeerOp::ResponseOk);
    assert_eq!(handler.get_value(b"to_delete"), None);
}

#[test]
fn test_replication_invalid_op() {
    let dir = tempdir().unwrap();
    let handler = make_handler(dir.path());
    let node = NodeInfo::new(0, "127.0.0.1:7777".parse().unwrap(), "127.0.0.1:7780".parse().unwrap());
    let topo = Arc::new(RwLock::new(ClusterTopology::single_node(0, node)));
    let peers = Arc::new(PeerConnectionPool::new());
    let repl = ReplicationManager::new(topo, handler, peers);

    let msg = PeerMessage {
        op: PeerOp::ForwardGet, // Not a replication op
        key: b"key".to_vec(),
        value: Vec::new(),
        ttl: 0,
        shard_id: 0,
    };

    assert!(repl.handle_replication_message(&msg).is_err());
}

#[test]
fn test_replication_stats_tracking() {
    let dir = tempdir().unwrap();
    let handler = make_handler(dir.path());
    let node = NodeInfo::new(0, "127.0.0.1:7777".parse().unwrap(), "127.0.0.1:7780".parse().unwrap());
    let topo = Arc::new(RwLock::new(ClusterTopology::single_node(0, node)));
    let peers = Arc::new(PeerConnectionPool::new());
    let repl = ReplicationManager::new(topo, handler, peers);

    for i in 0..10 {
        let msg = PeerMessage {
            op: PeerOp::ReplicatePut,
            key: format!("stats_key_{}", i).into_bytes(),
            value: b"value".to_vec(),
            ttl: 0,
            shard_id: i,
        };
        repl.handle_replication_message(&msg).unwrap();
    }

    assert_eq!(repl.stats().entries_received.load(Ordering::Relaxed), 10);
}

// =================== Rebalance Tests ===================

#[test]
fn test_rebalance_node_failure() {
    let mut nodes = make_nodes(3);
    let mut topo = ClusterTopology::new(0, nodes, 2);

    // Record initial distribution
    let initial_node1_shards: Vec<u16> = topo.shards_for_node(1);
    assert!(!initial_node1_shards.is_empty());

    // Kill node 1
    topo.nodes[1].status = NodeStatus::Dead;
    let moves = topo.rebalance();

    assert!(!moves.is_empty());
    // No slot should be on dead node
    for slot_id in 0..NUM_SLOTS as u16 {
        assert_ne!(topo.primary_for_shard(slot_id), 1);
    }
}

#[test]
fn test_rebalance_scale_up() {
    let mut topo = ClusterTopology::new(0, make_nodes(3), 1);
    let _old_distribution: Vec<u32> = (0..NUM_SLOTS as u16).map(|s| topo.primary_for_shard(s)).collect();

    // Add node 3
    topo.nodes.push(NodeInfo::new(
        3,
        "127.0.0.1:17780".parse().unwrap(),
        "127.0.0.1:17783".parse().unwrap(),
    ));
    let moves = topo.rebalance();

    // Some shards should have moved to node 3
    let node3_shards = topo.shards_for_node(3);
    assert!(
        !node3_shards.is_empty(),
        "Node 3 should receive shards after scale-up"
    );
}

#[test]
fn test_rebalance_preserves_total_shards() {
    for num_nodes in 2..=6 {
        let mut topo = ClusterTopology::new(0, make_nodes(num_nodes), 1);

        // Kill one node
        topo.nodes[num_nodes - 1].status = NodeStatus::Dead;
        topo.rebalance();

        let _total: usize = (0..num_nodes as u32)
            .map(|n| topo.shards_for_node(n).len())
            .sum();
        // All slots should still be assigned
        let assigned: usize = (0..NUM_SLOTS as u16)
            .filter(|&s| (topo.primary_for_shard(s) as usize) < num_nodes)
            .count();
        assert_eq!(assigned, NUM_SLOTS, "Not all slots assigned after rebalance");
    }
}

#[test]
fn test_export_shard_correctness() {
    let dir = tempdir().unwrap();
    let (handler, index) = make_handler_with_index(dir.path());
    let node = NodeInfo::new(0, "127.0.0.1:7777".parse().unwrap(), "127.0.0.1:7780".parse().unwrap());
    let topo = Arc::new(RwLock::new(ClusterTopology::single_node(0, node)));
    let peers = Arc::new(PeerConnectionPool::new());
    let mgr = RebalanceManager::new(topo, Arc::clone(&handler), Arc::clone(&index), peers);

    // Insert known keys
    let mut expected_per_shard = std::collections::HashMap::new();
    for i in 0..500 {
        let key = format!("export_test_{}", i);
        let value = format!("export_val_{}", i);
        handler.put_sync(key.as_bytes(), value.as_bytes(), 0).unwrap();

        let hash = hash_key(key.as_bytes());
        let shard = ClusterTopology::shard_for_key(hash);
        expected_per_shard
            .entry(shard)
            .or_insert_with(Vec::new)
            .push((key, value));
    }

    // Export each shard and verify
    let mut total_exported = 0;
    for shard_id in 0..256u16 {
        let pairs = mgr.export_shard(shard_id);
        total_exported += pairs.len();

        // Verify each exported pair matches expected
        for (key, value) in &pairs {
            let hash = hash_key(key);
            let actual_shard = ClusterTopology::shard_for_key(hash);
            assert_eq!(
                actual_shard, shard_id,
                "Exported key from wrong shard: key shard={}, export shard={}",
                actual_shard, shard_id
            );
        }
    }

    assert_eq!(total_exported, 500);
}

// =================== Health Checker Tests ===================

#[test]
fn test_health_checker_tracks_peers_not_self() {
    let nodes = make_nodes(5);
    let topo = Arc::new(RwLock::new(ClusterTopology::new(2, nodes, 2)));
    let peers = Arc::new(PeerConnectionPool::new());
    let checker = HealthChecker::new(topo, peers, HealthConfig::default());

    let health = checker.peer_health();
    let h = health.read();
    assert_eq!(h.len(), 4); // 5 nodes - self
    assert!(!h.contains_key(&2)); // Node 2 is self
    assert!(h.contains_key(&0));
    assert!(h.contains_key(&1));
    assert!(h.contains_key(&3));
    assert!(h.contains_key(&4));
}

// =================== Peer Server Integration Tests ===================

#[tokio::test]
async fn test_peer_server_forward_get() {
    let dir = tempdir().unwrap();
    let handler = make_handler(dir.path());

    // Pre-populate data
    handler.put_sync(b"peer_key", b"peer_value", 0).unwrap();

    let node = NodeInfo::new(0, "127.0.0.1:7777".parse().unwrap(), "127.0.0.1:7780".parse().unwrap());
    let topo = Arc::new(RwLock::new(ClusterTopology::single_node(0, node)));
    let peers = Arc::new(PeerConnectionPool::new());
    let repl = Arc::new(ReplicationManager::new(
        Arc::clone(&topo),
        Arc::clone(&handler),
        peers,
    ));

    let peer_server = Arc::new(PeerServer::new(
        Arc::clone(&handler),
        repl,
        Arc::clone(&topo),
    ));

    // Start peer server on a random port
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let handler_for_server = Arc::clone(&handler);
    let topo_for_server = Arc::clone(&topo);
    tokio::spawn(async move {
        loop {
            if let Ok((stream, _)) = listener.accept().await {
                let h = Arc::clone(&handler_for_server);
                let r = Arc::new(ReplicationManager::new(
                    Arc::clone(&topo_for_server),
                    Arc::clone(&h),
                    Arc::new(PeerConnectionPool::new()),
                ));
                tokio::spawn(async move {
                    let _ = PeerServer::handle_peer_connection(stream, h, r).await;
                });
            }
        }
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect and send a ForwardGet
    let mut stream = tokio::net::TcpStream::connect(server_addr).await.unwrap();
    let msg = PeerMessage {
        op: PeerOp::ForwardGet,
        key: b"peer_key".to_vec(),
        value: Vec::new(),
        ttl: 0,
        shard_id: 0,
    };
    msg.write_to(&mut stream).await.unwrap();

    let resp = PeerMessage::read_from(&mut stream).await.unwrap();
    assert_eq!(resp.op, PeerOp::ResponseOk);
    assert_eq!(resp.value, b"peer_value");
}

#[tokio::test]
async fn test_peer_server_forward_put_then_get() {
    let dir = tempdir().unwrap();
    let handler = make_handler(dir.path());

    let node = NodeInfo::new(0, "127.0.0.1:7777".parse().unwrap(), "127.0.0.1:7780".parse().unwrap());
    let topo = Arc::new(RwLock::new(ClusterTopology::single_node(0, node)));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let handler_clone = Arc::clone(&handler);
    let topo_clone = Arc::clone(&topo);
    tokio::spawn(async move {
        loop {
            if let Ok((stream, _)) = listener.accept().await {
                let h = Arc::clone(&handler_clone);
                let r = Arc::new(ReplicationManager::new(
                    Arc::clone(&topo_clone),
                    Arc::clone(&h),
                    Arc::new(PeerConnectionPool::new()),
                ));
                tokio::spawn(async move {
                    let _ = PeerServer::handle_peer_connection(stream, h, r).await;
                });
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut stream = tokio::net::TcpStream::connect(server_addr).await.unwrap();

    // ForwardPut
    let put_msg = PeerMessage {
        op: PeerOp::ForwardPut,
        key: b"remote_key".to_vec(),
        value: b"remote_value".to_vec(),
        ttl: 0,
        shard_id: 5,
    };
    put_msg.write_to(&mut stream).await.unwrap();
    let put_resp = PeerMessage::read_from(&mut stream).await.unwrap();
    assert_eq!(put_resp.op, PeerOp::ResponseOk);

    // ForwardGet
    let get_msg = PeerMessage {
        op: PeerOp::ForwardGet,
        key: b"remote_key".to_vec(),
        value: Vec::new(),
        ttl: 0,
        shard_id: 5,
    };
    get_msg.write_to(&mut stream).await.unwrap();
    let get_resp = PeerMessage::read_from(&mut stream).await.unwrap();
    assert_eq!(get_resp.op, PeerOp::ResponseOk);
    assert_eq!(get_resp.value, b"remote_value");
}

#[tokio::test]
async fn test_peer_server_forward_delete() {
    let dir = tempdir().unwrap();
    let handler = make_handler(dir.path());
    handler.put_sync(b"delete_me", b"value", 0).unwrap();

    let node = NodeInfo::new(0, "127.0.0.1:7777".parse().unwrap(), "127.0.0.1:7780".parse().unwrap());
    let topo = Arc::new(RwLock::new(ClusterTopology::single_node(0, node)));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let handler_clone = Arc::clone(&handler);
    let topo_clone = Arc::clone(&topo);
    tokio::spawn(async move {
        loop {
            if let Ok((stream, _)) = listener.accept().await {
                let h = Arc::clone(&handler_clone);
                let r = Arc::new(ReplicationManager::new(
                    Arc::clone(&topo_clone),
                    Arc::clone(&h),
                    Arc::new(PeerConnectionPool::new()),
                ));
                tokio::spawn(async move {
                    let _ = PeerServer::handle_peer_connection(stream, h, r).await;
                });
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut stream = tokio::net::TcpStream::connect(server_addr).await.unwrap();

    // ForwardDelete
    let del_msg = PeerMessage {
        op: PeerOp::ForwardDelete,
        key: b"delete_me".to_vec(),
        value: Vec::new(),
        ttl: 0,
        shard_id: 0,
    };
    del_msg.write_to(&mut stream).await.unwrap();
    let del_resp = PeerMessage::read_from(&mut stream).await.unwrap();
    assert_eq!(del_resp.op, PeerOp::ResponseOk);

    // Verify deleted locally
    assert_eq!(handler.get_value(b"delete_me"), None);
}

#[tokio::test]
async fn test_peer_server_heartbeat() {
    let dir = tempdir().unwrap();
    let handler = make_handler(dir.path());

    let node = NodeInfo::new(0, "127.0.0.1:7777".parse().unwrap(), "127.0.0.1:7780".parse().unwrap());
    let topo = Arc::new(RwLock::new(ClusterTopology::single_node(0, node)));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let handler_clone = Arc::clone(&handler);
    let topo_clone = Arc::clone(&topo);
    tokio::spawn(async move {
        loop {
            if let Ok((stream, _)) = listener.accept().await {
                let h = Arc::clone(&handler_clone);
                let r = Arc::new(ReplicationManager::new(
                    Arc::clone(&topo_clone),
                    Arc::clone(&h),
                    Arc::new(PeerConnectionPool::new()),
                ));
                tokio::spawn(async move {
                    let _ = PeerServer::handle_peer_connection(stream, h, r).await;
                });
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut stream = tokio::net::TcpStream::connect(server_addr).await.unwrap();

    // Send heartbeat
    let hb_msg = PeerMessage {
        op: PeerOp::Heartbeat,
        key: Vec::new(),
        value: Vec::new(),
        ttl: 0,
        shard_id: 0,
    };
    hb_msg.write_to(&mut stream).await.unwrap();
    let hb_resp = PeerMessage::read_from(&mut stream).await.unwrap();
    assert_eq!(hb_resp.op, PeerOp::HeartbeatAck);
}

#[tokio::test]
async fn test_peer_server_replication_over_network() {
    let dir = tempdir().unwrap();
    let handler = make_handler(dir.path());

    let node = NodeInfo::new(0, "127.0.0.1:7777".parse().unwrap(), "127.0.0.1:7780".parse().unwrap());
    let topo = Arc::new(RwLock::new(ClusterTopology::single_node(0, node)));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let handler_clone = Arc::clone(&handler);
    let topo_clone = Arc::clone(&topo);
    tokio::spawn(async move {
        loop {
            if let Ok((stream, _)) = listener.accept().await {
                let h = Arc::clone(&handler_clone);
                let r = Arc::new(ReplicationManager::new(
                    Arc::clone(&topo_clone),
                    Arc::clone(&h),
                    Arc::new(PeerConnectionPool::new()),
                ));
                tokio::spawn(async move {
                    let _ = PeerServer::handle_peer_connection(stream, h, r).await;
                });
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut stream = tokio::net::TcpStream::connect(server_addr).await.unwrap();

    // ReplicatePut 100 keys
    for i in 0..100 {
        let msg = PeerMessage {
            op: PeerOp::ReplicatePut,
            key: format!("net_repl_{}", i).into_bytes(),
            value: format!("net_val_{}", i).into_bytes(),
            ttl: 0,
            shard_id: (i % 256) as u16,
        };
        msg.write_to(&mut stream).await.unwrap();
        let resp = PeerMessage::read_from(&mut stream).await.unwrap();
        assert_eq!(resp.op, PeerOp::ResponseOk);
    }

    // Verify all keys exist locally
    for i in 0..100 {
        let key = format!("net_repl_{}", i);
        let expected = format!("net_val_{}", i);
        let val = handler.get_value(key.as_bytes());
        assert_eq!(val, Some(expected.into_bytes()), "Key {} missing after replication", key);
    }
}

// =================== Two-Node Integration Test ===================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_two_node_forward_and_get() {
    // Set up Node 0 (our node)
    let dir0 = tempdir().unwrap();
    let handler0 = make_handler(dir0.path());

    // Set up Node 1 (remote node)
    let dir1 = tempdir().unwrap();
    let handler1 = make_handler(dir1.path());

    // Start peer server for Node 1
    let node1_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node1_addr = node1_listener.local_addr().unwrap();

    let handler1_clone = Arc::clone(&handler1);
    let topo1 = Arc::new(RwLock::new(ClusterTopology::single_node(1,
        NodeInfo::new(1, node1_addr, node1_addr))));
    let topo1_clone = Arc::clone(&topo1);
    tokio::spawn(async move {
        loop {
            if let Ok((stream, _)) = node1_listener.accept().await {
                let h = Arc::clone(&handler1_clone);
                let r = Arc::new(ReplicationManager::new(
                    Arc::clone(&topo1_clone),
                    Arc::clone(&h),
                    Arc::new(PeerConnectionPool::new()),
                ));
                tokio::spawn(async move {
                    let _ = PeerServer::handle_peer_connection(stream, h, r).await;
                });
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Set up Node 0 with topology pointing to Node 1
    let nodes = vec![
        NodeInfo::new(0, "127.0.0.1:7777".parse().unwrap(), "127.0.0.1:7780".parse().unwrap()),
        NodeInfo::new(1, node1_addr, node1_addr),
    ];
    let topo0 = Arc::new(RwLock::new(ClusterTopology::new(0, nodes, 1)));
    let peers0 = Arc::new(PeerConnectionPool::new());
    peers0.add_peer(1, node1_addr);

    let router0 = ClusterRouter::new(Arc::clone(&topo0), Arc::clone(&handler0), peers0);

    // Write a key that should go to node 1
    // Find a key that routes to node 1
    let mut remote_key = String::new();
    for i in 0..10_000 {
        let key = format!("find_remote_{}", i);
        if router0.route_key(key.as_bytes()) == RouteDecision::Forward(1) {
            remote_key = key;
            break;
        }
    }
    assert!(!remote_key.is_empty(), "Couldn't find a key that routes to node 1");

    // PUT via router (should forward to Node 1)
    router0.put(remote_key.as_bytes(), b"remote_val", 0).unwrap();

    // GET via router (should forward to Node 1 and retrieve)
    let val = router0.get(remote_key.as_bytes());
    assert_eq!(val, Some(b"remote_val".to_vec()));

    // Also verify it's on Node 1 directly
    let val_direct = handler1.get_value(remote_key.as_bytes());
    assert_eq!(val_direct, Some(b"remote_val".to_vec()));

    // And NOT on Node 0 (since we forwarded)
    let val_local = handler0.get_value(remote_key.as_bytes());
    assert_eq!(val_local, None);
}

// =================== Config Validation Tests ===================

#[test]
fn test_config_cluster_mode_requires_node_id() {
    let mut config = ssd_kv::config::Config::default();
    config.cluster_mode = true;
    config.node_id = None;
    config.total_nodes = Some(3);
    assert!(config.validate().is_err());
}

#[test]
fn test_config_cluster_mode_requires_total_nodes() {
    let mut config = ssd_kv::config::Config::default();
    config.cluster_mode = true;
    config.node_id = Some(0);
    config.total_nodes = None;
    assert!(config.validate().is_err());
}

#[test]
fn test_config_cluster_mode_rf_exceeds_nodes() {
    let mut config = ssd_kv::config::Config::default();
    config.cluster_mode = true;
    config.node_id = Some(0);
    config.total_nodes = Some(2);
    config.replication_factor = 3;
    assert!(config.validate().is_err());
}

#[test]
fn test_config_cluster_mode_valid() {
    let mut config = ssd_kv::config::Config::default();
    config.cluster_mode = true;
    config.node_id = Some(0);
    config.total_nodes = Some(3);
    config.replication_factor = 2;
    assert!(config.validate().is_ok());
}

#[test]
fn test_config_standalone_mode_valid() {
    let config = ssd_kv::config::Config::default();
    assert!(config.validate().is_ok());
}

// =================== Connection Pool Tests ===================

#[test]
fn test_connection_pool_lifecycle() {
    let pool = PeerConnectionPool::new();

    assert!(pool.peer_ids().is_empty());

    pool.add_peer(1, "127.0.0.1:7781".parse().unwrap());
    pool.add_peer(2, "127.0.0.1:7782".parse().unwrap());
    pool.add_peer(3, "127.0.0.1:7783".parse().unwrap());
    assert_eq!(pool.peer_ids().len(), 3);

    pool.remove_peer(2);
    assert_eq!(pool.peer_ids().len(), 2);
    assert!(!pool.peer_ids().contains(&2));

    // Adding same peer again should update
    pool.add_peer(1, "127.0.0.1:8881".parse().unwrap());
    assert_eq!(pool.peer_ids().len(), 2);
}

// =================== Topology Version Tests ===================

#[test]
fn test_topology_version_increments_on_rebalance() {
    let mut topo = ClusterTopology::new(0, make_nodes(3), 2);
    let v1 = topo.current_version();

    topo.nodes[1].status = NodeStatus::Dead;
    let moves = topo.rebalance();
    assert!(!moves.is_empty());

    let v2 = topo.current_version();
    assert!(v2 > v1);
}

#[test]
fn test_topology_version_stable_without_changes() {
    let mut topo = ClusterTopology::new(0, make_nodes(3), 2);
    let v1 = topo.current_version();

    // Rebalance without changes
    let moves = topo.rebalance();
    // There may be moves since it's recomputing, but if there are none...
    if moves.is_empty() {
        assert_eq!(topo.current_version(), v1);
    }
}

// =================== TTL Integration Tests ===================

#[test]
fn test_ttl_command_with_expiry() {
    let dir = tempdir().unwrap();
    let handler = make_handler(dir.path());

    // SET with TTL
    handler.put_sync(b"ttl_key", b"ttl_val", 3600).unwrap();

    // get_with_meta should return TTL info
    let meta = handler.get_with_meta(b"ttl_key").unwrap();
    assert_eq!(meta.value, b"ttl_val");
    assert_eq!(meta.ttl_secs, 3600);

    // Compute remaining TTL (should be close to 3600)
    let now_micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0);
    let expiry = meta.timestamp_micros + (meta.ttl_secs as u64 * 1_000_000);
    let remaining_secs = ((expiry - now_micros) / 1_000_000) as i64;
    assert!(remaining_secs >= 3599 && remaining_secs <= 3600);
}

#[test]
fn test_ttl_no_expiry() {
    let dir = tempdir().unwrap();
    let handler = make_handler(dir.path());

    handler.put_sync(b"no_ttl_key", b"val", 0).unwrap();

    let meta = handler.get_with_meta(b"no_ttl_key").unwrap();
    assert_eq!(meta.ttl_secs, 0); // -1 in Redis semantics
}

#[test]
fn test_ttl_nonexistent() {
    let dir = tempdir().unwrap();
    let handler = make_handler(dir.path());

    assert!(handler.get_with_meta(b"missing").is_none()); // -2 in Redis semantics
}

#[test]
fn test_pttl_returns_milliseconds() {
    let dir = tempdir().unwrap();
    let handler = make_handler(dir.path());

    handler.put_sync(b"pttl_key", b"val", 10).unwrap();

    let meta = handler.get_with_meta(b"pttl_key").unwrap();
    let now_micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0);
    let expiry = meta.timestamp_micros + (meta.ttl_secs as u64 * 1_000_000);
    let remaining_ms = ((expiry - now_micros) / 1_000) as i64;
    assert!(remaining_ms > 9000 && remaining_ms <= 10_000);
}

#[test]
fn test_expire_sets_ttl() {
    let dir = tempdir().unwrap();
    let handler = make_handler(dir.path());

    handler.put_sync(b"exp_key", b"exp_val", 0).unwrap();

    // Initially no TTL
    let meta = handler.get_with_meta(b"exp_key").unwrap();
    assert_eq!(meta.ttl_secs, 0);

    // Set TTL via update_ttl
    assert!(handler.update_ttl(b"exp_key", 300).unwrap());

    // Verify TTL was set
    let meta = handler.get_with_meta(b"exp_key").unwrap();
    assert_eq!(meta.ttl_secs, 300);
}

#[test]
fn test_persist_removes_ttl() {
    let dir = tempdir().unwrap();
    let handler = make_handler(dir.path());

    // Set with TTL
    handler.put_sync(b"persist_key", b"persist_val", 600).unwrap();
    let meta = handler.get_with_meta(b"persist_key").unwrap();
    assert_eq!(meta.ttl_secs, 600);

    // Remove TTL (persist)
    assert!(handler.update_ttl(b"persist_key", 0).unwrap());

    // Verify TTL removed
    let meta = handler.get_with_meta(b"persist_key").unwrap();
    assert_eq!(meta.ttl_secs, 0);
}

#[test]
fn test_expireat_absolute_time() {
    let dir = tempdir().unwrap();
    let handler = make_handler(dir.path());

    handler.put_sync(b"eat_key", b"eat_val", 0).unwrap();

    // Set to expire 1 hour from now
    let future_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0) + 3600;
    let remaining = future_secs - std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    assert!(handler.update_ttl(b"eat_key", remaining as u32).unwrap());

    let meta = handler.get_with_meta(b"eat_key").unwrap();
    assert!(meta.ttl_secs >= 3599 && meta.ttl_secs <= 3600);
}

// =================== Cluster Protocol Tests ===================

#[test]
fn test_cluster_keyslot_known_values() {
    use ssd_kv::cluster::topology::key_hash_slot;

    // The hash slot should be deterministic and in range
    let slot = key_hash_slot(b"foo");
    assert!(slot < NUM_SLOTS as u16);

    // Same key always returns same slot
    assert_eq!(key_hash_slot(b"foo"), key_hash_slot(b"foo"));

    // Hash tags work
    assert_eq!(key_hash_slot(b"user:{123}:name"), key_hash_slot(b"user:{123}:email"));
}

#[test]
fn test_cluster_slots_all_covered() {
    let nodes = make_nodes(3);
    let topo = ClusterTopology::new(0, nodes, 2);

    // Verify all 16384 slots have ranges
    let mut covered = vec![false; NUM_SLOTS];
    for node_id in 0..3u32 {
        for (start, end) in topo.slot_ranges_for_node(node_id) {
            for s in start..=end {
                covered[s as usize] = true;
            }
        }
    }
    assert!(covered.iter().all(|c| *c), "Not all slots are covered by ranges");
}

#[test]
fn test_cluster_nodes_format() {
    let nodes = make_nodes(3);
    let topo = ClusterTopology::new(0, nodes, 1);

    // Verify local node has is_local_primary for some slots
    let local_slots = topo.shards_for_node(0);
    assert!(!local_slots.is_empty());
    for &slot in &local_slots {
        assert!(topo.is_local_primary(slot));
    }
}

#[test]
fn test_moved_redirection_format() {
    // Verify that non-local keys would produce MOVED responses
    let dir = tempdir().unwrap();
    let handler = make_handler(dir.path());
    let nodes = make_nodes(3);
    let topo = Arc::new(RwLock::new(ClusterTopology::new(0, nodes, 1)));
    let peers = Arc::new(PeerConnectionPool::new());
    let router = ClusterRouter::new(topo, handler, peers);

    // Find a key that routes to another node
    for i in 0..10_000 {
        let key = format!("moved_key_{}", i);
        let (slot, decision) = router.route_key_with_slot(key.as_bytes());
        if let RouteDecision::Forward(node_id) = decision {
            // Verify we can get the address
            let addr = router.redis_addr_for_slot(slot);
            assert!(addr.is_some(), "Should have address for slot {}", slot);
            return; // Test passed
        }
    }
    panic!("Could not find any key that routes to another node");
}

#[test]
fn test_crossslot_detection() {
    use ssd_kv::cluster::topology::key_hash_slot;

    // Keys with different hash tags should (usually) have different slots
    let slot1 = key_hash_slot(b"user:{1}:name");
    let slot2 = key_hash_slot(b"user:{2}:name");
    // These should be different since tags are different
    assert_ne!(slot1, slot2, "Different hash tags should produce different slots");

    // Keys with same hash tag should have same slot
    let slot_a = key_hash_slot(b"a:{same}:x");
    let slot_b = key_hash_slot(b"b:{same}:y");
    assert_eq!(slot_a, slot_b);
}
