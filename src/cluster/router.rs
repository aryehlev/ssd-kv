//! Request routing: determines whether a request should be handled locally
//! or forwarded to another node in the cluster.

use std::io;
use std::sync::Arc;

use parking_lot::RwLock;
use tracing::{debug, warn};

use std::net::SocketAddr;

use crate::cluster::node::NodeId;
use crate::cluster::peer_pool::{PeerConnectionPool, PeerMessage, PeerOp};
use crate::cluster::topology::ClusterTopology;
use crate::server::handler::Handler;

/// Result of routing a key to a node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteDecision {
    /// Handle locally — this node owns the shard.
    Local,
    /// Forward to another node.
    Forward(NodeId),
}

/// Routes requests to the correct node based on key hashing and shard ownership.
pub struct ClusterRouter {
    topology: Arc<RwLock<ClusterTopology>>,
    local_handler: Arc<Handler>,
    peers: Arc<PeerConnectionPool>,
}

impl ClusterRouter {
    pub fn new(
        topology: Arc<RwLock<ClusterTopology>>,
        local_handler: Arc<Handler>,
        peers: Arc<PeerConnectionPool>,
    ) -> Self {
        Self {
            topology,
            local_handler,
            peers,
        }
    }

    /// Determines where a key should be routed using CRC16 hash slots.
    #[inline]
    pub fn route_key(&self, key: &[u8]) -> RouteDecision {
        let slot = ClusterTopology::slot_for_key(key);
        let topo = self.topology.read();
        let owner = topo.primary_for_shard(slot);
        if owner == topo.local_node_id {
            RouteDecision::Local
        } else {
            RouteDecision::Forward(owner)
        }
    }

    /// Returns the slot + route decision for a key (used for MOVED responses).
    #[inline]
    pub fn route_key_with_slot(&self, key: &[u8]) -> (u16, RouteDecision) {
        let slot = ClusterTopology::slot_for_key(key);
        let topo = self.topology.read();
        let owner = topo.primary_for_shard(slot);
        if owner == topo.local_node_id {
            (slot, RouteDecision::Local)
        } else {
            (slot, RouteDecision::Forward(owner))
        }
    }

    /// Returns the Redis address for a given slot's primary node.
    pub fn redis_addr_for_slot(&self, slot: u16) -> Option<SocketAddr> {
        let topo = self.topology.read();
        topo.redis_addr_for_slot(slot)
    }

    /// Returns the hash slot for a key (CRC16-based).
    #[inline]
    pub fn slot_for_key(&self, key: &[u8]) -> u16 {
        ClusterTopology::slot_for_key(key)
    }

    /// Handles a GET request, forwarding if necessary.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self.route_key(key) {
            RouteDecision::Local => self.local_handler.get_value(key),
            RouteDecision::Forward(node_id) => {
                // Forward via blocking runtime since Redis handler is sync
                let shard = self.slot_for_key(key);
                let msg = PeerMessage {
                    op: PeerOp::ForwardGet,
                    key: key.to_vec(),
                    value: Vec::new(),
                    ttl: 0,
                    shard_id: shard,
                };
                match self.forward_sync(node_id, &msg) {
                    Ok(resp) if resp.op == PeerOp::ResponseOk => Some(resp.value),
                    Ok(_) => None,
                    Err(e) => {
                        warn!("Failed to forward GET to node {}: {}", node_id, e);
                        None
                    }
                }
            }
        }
    }

    /// Handles a PUT request, forwarding if necessary.
    pub fn put(&self, key: &[u8], value: &[u8], ttl: u32) -> io::Result<()> {
        match self.route_key(key) {
            RouteDecision::Local => {
                self.local_handler.put_sync(key, value, ttl)?;
                // Trigger async replication to replicas
                self.replicate_put(key, value, ttl);
                Ok(())
            }
            RouteDecision::Forward(node_id) => {
                let shard = self.slot_for_key(key);
                let msg = PeerMessage {
                    op: PeerOp::ForwardPut,
                    key: key.to_vec(),
                    value: value.to_vec(),
                    ttl,
                    shard_id: shard,
                };
                match self.forward_sync(node_id, &msg) {
                    Ok(resp) if resp.op == PeerOp::ResponseOk => Ok(()),
                    Ok(resp) => Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("Remote PUT failed: {:?}", resp.op),
                    )),
                    Err(e) => Err(e),
                }
            }
        }
    }

    /// Handles a DELETE request, forwarding if necessary.
    pub fn delete(&self, key: &[u8]) -> io::Result<bool> {
        match self.route_key(key) {
            RouteDecision::Local => {
                let deleted = self.local_handler.delete_sync(key)?;
                // Trigger async replication to replicas
                self.replicate_delete(key);
                Ok(deleted)
            }
            RouteDecision::Forward(node_id) => {
                let shard = self.slot_for_key(key);
                let msg = PeerMessage {
                    op: PeerOp::ForwardDelete,
                    key: key.to_vec(),
                    value: Vec::new(),
                    ttl: 0,
                    shard_id: shard,
                };
                match self.forward_sync(node_id, &msg) {
                    Ok(resp) if resp.op == PeerOp::ResponseOk => Ok(true),
                    Ok(_) => Ok(false),
                    Err(e) => Err(e),
                }
            }
        }
    }

    /// Asynchronously replicates a PUT to replica nodes (fire-and-forget).
    fn replicate_put(&self, key: &[u8], value: &[u8], ttl: u32) {
        let shard = self.slot_for_key(key);
        let replicas = {
            let topo = self.topology.read();
            let assignment = &topo.shard_map[shard as usize];
            assignment.replicas.clone()
        };

        if replicas.is_empty() {
            return;
        }

        let msg = PeerMessage {
            op: PeerOp::ReplicatePut,
            key: key.to_vec(),
            value: value.to_vec(),
            ttl,
            shard_id: shard,
        };

        let peers = Arc::clone(&self.peers);
        tokio::spawn(async move {
            for replica_id in replicas {
                if let Err(e) = peers.send_async(replica_id, &msg).await {
                    debug!("Replication to node {} failed: {}", replica_id, e);
                }
            }
        });
    }

    /// Asynchronously replicates a DELETE to replica nodes (fire-and-forget).
    fn replicate_delete(&self, key: &[u8]) {
        let shard = self.slot_for_key(key);
        let replicas = {
            let topo = self.topology.read();
            let assignment = &topo.shard_map[shard as usize];
            assignment.replicas.clone()
        };

        if replicas.is_empty() {
            return;
        }

        let msg = PeerMessage {
            op: PeerOp::ReplicateDelete,
            key: key.to_vec(),
            value: Vec::new(),
            ttl: 0,
            shard_id: shard,
        };

        let peers = Arc::clone(&self.peers);
        tokio::spawn(async move {
            for replica_id in replicas {
                if let Err(e) = peers.send_async(replica_id, &msg).await {
                    debug!("Replication DELETE to node {} failed: {}", replica_id, e);
                }
            }
        });
    }

    /// Forward a request synchronously (blocking the current thread).
    /// Creates a fresh TCP connection per forward to avoid cross-runtime issues.
    fn forward_sync(&self, node_id: NodeId, msg: &PeerMessage) -> io::Result<PeerMessage> {
        // Look up the peer's address from the topology
        let addr = {
            let topo = self.topology.read();
            topo.get_node(node_id)
                .map(|n| n.cluster_addr)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("Unknown node {}", node_id)))?
        };

        // Use std::net TCP for synchronous forwarding (no tokio dependency)
        let mut stream = std::net::TcpStream::connect_timeout(&addr.into(), std::time::Duration::from_secs(5))?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(std::time::Duration::from_secs(10)))?;
        stream.set_write_timeout(Some(std::time::Duration::from_secs(10)))?;

        // Send message
        let data = msg.serialize();
        std::io::Write::write_all(&mut stream, &data)?;

        // Read response header
        let mut header = [0u8; 15]; // HEADER_SIZE = 15
        std::io::Read::read_exact(&mut stream, &mut header)?;

        let key_len = u32::from_be_bytes([header[7], header[8], header[9], header[10]]) as usize;
        let value_len = u32::from_be_bytes([header[11], header[12], header[13], header[14]]) as usize;

        let mut payload = vec![0u8; key_len + value_len];
        if !payload.is_empty() {
            std::io::Read::read_exact(&mut stream, &mut payload)?;
        }

        let mut full = Vec::with_capacity(15 + payload.len());
        full.extend_from_slice(&header);
        full.extend_from_slice(&payload);

        PeerMessage::deserialize(&full)
    }

    /// Returns a reference to the local handler.
    pub fn local_handler(&self) -> &Handler {
        &self.local_handler
    }

    /// Returns a reference to the topology.
    pub fn topology(&self) -> &RwLock<ClusterTopology> {
        &self.topology
    }

    /// Returns a reference to the peer connection pool.
    pub fn peers(&self) -> &PeerConnectionPool {
        &self.peers
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::node::NodeInfo;
    use crate::cluster::topology::ClusterTopology;
    use crate::engine::index::Index;
    use crate::storage::file_manager::FileManager;
    use crate::storage::write_buffer::WriteBuffer;
    use tempfile::tempdir;

    fn create_test_setup() -> (Arc<RwLock<ClusterTopology>>, Arc<Handler>, Arc<PeerConnectionPool>, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let fm = Arc::new(FileManager::new(dir.path()).unwrap());
        fm.create_file().unwrap();
        let index = Arc::new(Index::new());
        let wb = Arc::new(WriteBuffer::new(0, 1023));
        let handler = Arc::new(Handler::new(index, fm, wb));

        let node = NodeInfo::new(
            0,
            "127.0.0.1:7777".parse().unwrap(),
            "127.0.0.1:7780".parse().unwrap(),
        );
        let topo = Arc::new(RwLock::new(ClusterTopology::single_node(0, node)));
        let peers = Arc::new(PeerConnectionPool::new());

        (topo, handler, peers, dir)
    }

    #[test]
    fn test_single_node_all_local() {
        let (topo, handler, peers, _dir) = create_test_setup();
        let router = ClusterRouter::new(topo, handler, peers);

        // In single-node mode, all keys should route locally
        for i in 0..100 {
            let key = format!("key_{}", i);
            assert_eq!(
                router.route_key(key.as_bytes()),
                RouteDecision::Local,
                "Key {} should be local",
                key
            );
        }
    }

    #[test]
    fn test_multi_node_routing() {
        let dir = tempdir().unwrap();
        let fm = Arc::new(FileManager::new(dir.path()).unwrap());
        fm.create_file().unwrap();
        let index = Arc::new(Index::new());
        let wb = Arc::new(WriteBuffer::new(0, 1023));
        let handler = Arc::new(Handler::new(index, fm, wb));

        let nodes = vec![
            NodeInfo::new(0, "127.0.0.1:7777".parse().unwrap(), "127.0.0.1:7780".parse().unwrap()),
            NodeInfo::new(1, "127.0.0.1:7778".parse().unwrap(), "127.0.0.1:7781".parse().unwrap()),
            NodeInfo::new(2, "127.0.0.1:7779".parse().unwrap(), "127.0.0.1:7782".parse().unwrap()),
        ];
        let topo = Arc::new(RwLock::new(ClusterTopology::new(0, nodes, 2)));
        let peers = Arc::new(PeerConnectionPool::new());
        let router = ClusterRouter::new(topo, handler, peers);

        // Some keys should be local, some should be forwarded
        let mut local_count = 0;
        let mut forward_count = 0;
        for i in 0..1000 {
            let key = format!("key_{}", i);
            match router.route_key(key.as_bytes()) {
                RouteDecision::Local => local_count += 1,
                RouteDecision::Forward(_) => forward_count += 1,
            }
        }

        // With 3 nodes, roughly 1/3 should be local
        assert!(local_count > 200, "Too few local keys: {}", local_count);
        assert!(forward_count > 400, "Too few forwarded keys: {}", forward_count);
    }

    #[test]
    fn test_local_put_get_delete() {
        let (topo, handler, peers, _dir) = create_test_setup();
        let router = ClusterRouter::new(topo, handler, peers);

        // PUT
        router.put(b"hello", b"world", 0).unwrap();

        // GET
        let val = router.get(b"hello");
        assert_eq!(val, Some(b"world".to_vec()));

        // DELETE
        let deleted = router.delete(b"hello").unwrap();
        assert!(deleted);

        // GET should be None now
        let val = router.get(b"hello");
        assert_eq!(val, None);
    }
}
