//! WAL-based replication manager.
//!
//! The primary node streams write operations (PUT/DELETE) to replica nodes
//! asynchronously using the peer protocol. Replicas apply operations to
//! their local handler to maintain a copy of the data.

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

use crate::cluster::peer_pool::{PeerConnectionPool, PeerMessage, PeerOp};
use crate::cluster::topology::ClusterTopology;
use crate::server::handler::Handler;

/// Per-replica send counters. Replication is async (primary doesn't
/// wait for the ack before returning to the client), but we still
/// track per-replica acks so divergence is observable. If a replica's
/// `acked` falls behind `sent` for long enough, something is wrong
/// — either the network to that peer is lossy or the peer itself is
/// failing to apply.
#[derive(Debug, Default)]
pub struct ReplicaStats {
    pub sent: AtomicU64,
    pub acked: AtomicU64,
    pub failed: AtomicU64,
    /// Monotonic timestamp (ms since startup) of the last successful
    /// ack from this replica. Zero = never acked.
    pub last_ack_ms: AtomicU64,
    /// Monotonic timestamp of the last failure. Useful for "when did
    /// this replica start diverging?"
    pub last_failure_ms: AtomicU64,
}

impl ReplicaStats {
    /// A replica is considered "diverged" when it has sent items
    /// that never acked. `sent - acked - failed` == in-flight; any
    /// non-zero `failed` is a hard divergence signal.
    pub fn is_diverged(&self) -> bool {
        self.failed.load(Ordering::Relaxed) > 0
    }

    pub fn lag(&self) -> u64 {
        let sent = self.sent.load(Ordering::Relaxed);
        let acked = self.acked.load(Ordering::Relaxed);
        sent.saturating_sub(acked)
    }
}

/// Statistics for replication.
#[derive(Debug, Default)]
pub struct ReplicationStats {
    /// Total entries sent to replicas (summed across replicas).
    pub entries_sent: AtomicU64,
    /// Total entries received from primary (for replica-side counts).
    pub entries_received: AtomicU64,
    /// Total replication errors (summed across replicas).
    pub errors: AtomicU64,
    /// Per-replica stats keyed by `NodeId`. Populated lazily as we
    /// see each replica. Read-mostly: the hot path uses the RwLock
    /// in shared mode and only takes the exclusive lock on first
    /// encounter of a replica id.
    per_replica: RwLock<HashMap<u32, Arc<ReplicaStats>>>,
}

impl ReplicationStats {
    /// Returns (and lazily creates) the per-replica counter block.
    pub fn replica(&self, node_id: u32) -> Arc<ReplicaStats> {
        if let Some(s) = self.per_replica.read().get(&node_id) {
            return Arc::clone(s);
        }
        let mut w = self.per_replica.write();
        Arc::clone(w.entry(node_id).or_insert_with(|| Arc::new(ReplicaStats::default())))
    }

    /// Snapshot of all per-replica counters for observability.
    pub fn per_replica_snapshot(&self) -> Vec<(u32, Arc<ReplicaStats>)> {
        let r = self.per_replica.read();
        let mut v: Vec<_> = r.iter().map(|(k, v)| (*k, Arc::clone(v))).collect();
        v.sort_by_key(|(id, _)| *id);
        v
    }
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Manages replication for this node.
///
/// - As a **primary**: sends writes to replicas via the peer pool.
/// - As a **replica**: accepts incoming writes from the primary via the peer listener.
pub struct ReplicationManager {
    topology: Arc<RwLock<ClusterTopology>>,
    local_handler: Arc<Handler>,
    peers: Arc<PeerConnectionPool>,
    stats: Arc<ReplicationStats>,
}

impl ReplicationManager {
    pub fn new(
        topology: Arc<RwLock<ClusterTopology>>,
        local_handler: Arc<Handler>,
        peers: Arc<PeerConnectionPool>,
    ) -> Self {
        Self {
            topology,
            local_handler,
            peers,
            stats: Arc::new(ReplicationStats::default()),
        }
    }

    /// Returns the replication stats.
    pub fn stats(&self) -> Arc<ReplicationStats> {
        Arc::clone(&self.stats)
    }

    /// Replicate a PUT operation to all replica nodes for the given shard.
    pub fn replicate_put(&self, key: &[u8], value: &[u8], ttl: u32, shard_id: u16) {
        let replicas = {
            let topo = self.topology.read();
            topo.shard_map[shard_id as usize].replicas.clone()
        };

        if replicas.is_empty() {
            return;
        }

        let msg = PeerMessage {
            op: PeerOp::ReplicatePut,
            key: key.to_vec(),
            value: value.to_vec(),
            ttl,
            shard_id,
        };

        let peers = Arc::clone(&self.peers);
        let stats = Arc::clone(&self.stats);
        tokio::spawn(async move {
            for replica_id in replicas {
                let replica_stats = stats.replica(replica_id);
                replica_stats.sent.fetch_add(1, Ordering::Relaxed);
                stats.entries_sent.fetch_add(1, Ordering::Relaxed);
                match peers.send_async(replica_id, &msg).await {
                    Ok(()) => {
                        replica_stats.acked.fetch_add(1, Ordering::Relaxed);
                        replica_stats.last_ack_ms.store(now_ms(), Ordering::Relaxed);
                    }
                    Err(e) => {
                        warn!("Replication PUT to node {} failed: {} — replica now diverged", replica_id, e);
                        replica_stats.failed.fetch_add(1, Ordering::Relaxed);
                        replica_stats.last_failure_ms.store(now_ms(), Ordering::Relaxed);
                        stats.errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
    }

    /// Replicate a DELETE operation to all replica nodes for the given shard.
    pub fn replicate_delete(&self, key: &[u8], shard_id: u16) {
        let replicas = {
            let topo = self.topology.read();
            topo.shard_map[shard_id as usize].replicas.clone()
        };

        if replicas.is_empty() {
            return;
        }

        let msg = PeerMessage {
            op: PeerOp::ReplicateDelete,
            key: key.to_vec(),
            value: Vec::new(),
            ttl: 0,
            shard_id,
        };

        let peers = Arc::clone(&self.peers);
        let stats = Arc::clone(&self.stats);
        tokio::spawn(async move {
            for replica_id in replicas {
                let replica_stats = stats.replica(replica_id);
                replica_stats.sent.fetch_add(1, Ordering::Relaxed);
                stats.entries_sent.fetch_add(1, Ordering::Relaxed);
                match peers.send_async(replica_id, &msg).await {
                    Ok(()) => {
                        replica_stats.acked.fetch_add(1, Ordering::Relaxed);
                        replica_stats.last_ack_ms.store(now_ms(), Ordering::Relaxed);
                    }
                    Err(e) => {
                        warn!("Replication DELETE to node {} failed: {} — replica now diverged", replica_id, e);
                        replica_stats.failed.fetch_add(1, Ordering::Relaxed);
                        replica_stats.last_failure_ms.store(now_ms(), Ordering::Relaxed);
                        stats.errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
    }

    /// Handles an incoming replication message (called on replica nodes).
    pub fn handle_replication_message(&self, msg: &PeerMessage) -> io::Result<PeerMessage> {
        match msg.op {
            PeerOp::ReplicatePut => {
                self.local_handler.put_sync(&msg.key, &msg.value, msg.ttl)?;
                self.stats.entries_received.fetch_add(1, Ordering::Relaxed);
                Ok(PeerMessage {
                    op: PeerOp::ResponseOk,
                    key: Vec::new(),
                    value: Vec::new(),
                    ttl: 0,
                    shard_id: msg.shard_id,
                })
            }
            PeerOp::ReplicateDelete => {
                self.local_handler.delete_sync(&msg.key)?;
                self.stats.entries_received.fetch_add(1, Ordering::Relaxed);
                Ok(PeerMessage {
                    op: PeerOp::ResponseOk,
                    key: Vec::new(),
                    value: Vec::new(),
                    ttl: 0,
                    shard_id: msg.shard_id,
                })
            }
            _ => {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Unexpected replication op: {:?}", msg.op),
                ))
            }
        }
    }
}

/// The cluster peer server that listens for inter-node messages.
/// Handles forwarded requests, replication, and health checks.
pub struct PeerServer {
    local_handler: Arc<Handler>,
    replication: Arc<ReplicationManager>,
    topology: Arc<RwLock<ClusterTopology>>,
}

impl PeerServer {
    pub fn new(
        local_handler: Arc<Handler>,
        replication: Arc<ReplicationManager>,
        topology: Arc<RwLock<ClusterTopology>>,
    ) -> Self {
        Self {
            local_handler,
            replication,
            topology,
        }
    }

    /// Starts listening for peer connections on the given address.
    pub async fn run(&self, addr: std::net::SocketAddr) -> io::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        info!("Cluster peer server listening on {}", addr);

        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    debug!("Peer connected: {}", peer);
                    let handler = Arc::clone(&self.local_handler);
                    let replication = Arc::clone(&self.replication);
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_peer_connection(stream, handler, replication).await {
                            debug!("Peer connection error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Peer accept error: {}", e);
                }
            }
        }
    }

    /// Handles a single peer connection.
    pub async fn handle_peer_connection(
        mut stream: TcpStream,
        handler: Arc<Handler>,
        replication: Arc<ReplicationManager>,
    ) -> io::Result<()> {
        stream.set_nodelay(true)?;

        loop {
            let msg = match PeerMessage::read_from(&mut stream).await {
                Ok(msg) => msg,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            };

            let response = match msg.op {
                PeerOp::ForwardGet => {
                    match handler.get_value(&msg.key) {
                        Some(value) => PeerMessage {
                            op: PeerOp::ResponseOk,
                            key: Vec::new(),
                            value,
                            ttl: 0,
                            shard_id: msg.shard_id,
                        },
                        None => PeerMessage {
                            op: PeerOp::ResponseNotFound,
                            key: Vec::new(),
                            value: Vec::new(),
                            ttl: 0,
                            shard_id: msg.shard_id,
                        },
                    }
                }
                PeerOp::ForwardPut => {
                    match handler.put_sync(&msg.key, &msg.value, msg.ttl) {
                        Ok(()) => PeerMessage {
                            op: PeerOp::ResponseOk,
                            key: Vec::new(),
                            value: Vec::new(),
                            ttl: 0,
                            shard_id: msg.shard_id,
                        },
                        Err(e) => PeerMessage {
                            op: PeerOp::ResponseError,
                            key: Vec::new(),
                            value: e.to_string().into_bytes(),
                            ttl: 0,
                            shard_id: msg.shard_id,
                        },
                    }
                }
                PeerOp::ForwardDelete => {
                    match handler.delete_sync(&msg.key) {
                        Ok(true) => PeerMessage {
                            op: PeerOp::ResponseOk,
                            key: Vec::new(),
                            value: Vec::new(),
                            ttl: 0,
                            shard_id: msg.shard_id,
                        },
                        Ok(false) => PeerMessage {
                            op: PeerOp::ResponseNotFound,
                            key: Vec::new(),
                            value: Vec::new(),
                            ttl: 0,
                            shard_id: msg.shard_id,
                        },
                        Err(e) => PeerMessage {
                            op: PeerOp::ResponseError,
                            key: Vec::new(),
                            value: e.to_string().into_bytes(),
                            ttl: 0,
                            shard_id: msg.shard_id,
                        },
                    }
                }
                PeerOp::ReplicatePut | PeerOp::ReplicateDelete => {
                    match replication.handle_replication_message(&msg) {
                        Ok(resp) => resp,
                        Err(e) => PeerMessage {
                            op: PeerOp::ResponseError,
                            key: Vec::new(),
                            value: e.to_string().into_bytes(),
                            ttl: 0,
                            shard_id: msg.shard_id,
                        },
                    }
                }
                PeerOp::Heartbeat => {
                    PeerMessage {
                        op: PeerOp::HeartbeatAck,
                        key: Vec::new(),
                        value: Vec::new(),
                        ttl: 0,
                        shard_id: 0,
                    }
                }
                _ => {
                    PeerMessage {
                        op: PeerOp::ResponseError,
                        key: Vec::new(),
                        value: b"Unexpected op".to_vec(),
                        ttl: 0,
                        shard_id: msg.shard_id,
                    }
                }
            };

            response.write_to(&mut stream).await?;
        }

        Ok(())
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

    fn create_replication_setup() -> (Arc<ReplicationManager>, Arc<Handler>, tempfile::TempDir) {
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
        let repl = Arc::new(ReplicationManager::new(topo, Arc::clone(&handler), peers));

        (repl, handler, dir)
    }

    #[test]
    fn test_handle_replicate_put() {
        let (repl, handler, _dir) = create_replication_setup();

        let msg = PeerMessage {
            op: PeerOp::ReplicatePut,
            key: b"repl_key".to_vec(),
            value: b"repl_value".to_vec(),
            ttl: 0,
            shard_id: 10,
        };

        let resp = repl.handle_replication_message(&msg).unwrap();
        assert_eq!(resp.op, PeerOp::ResponseOk);

        // Verify the data was written locally
        let val = handler.get_value(b"repl_key");
        assert_eq!(val, Some(b"repl_value".to_vec()));
    }

    #[test]
    fn test_handle_replicate_delete() {
        let (repl, handler, _dir) = create_replication_setup();

        // First put a key
        handler.put_sync(b"del_key", b"del_value", 0).unwrap();

        let msg = PeerMessage {
            op: PeerOp::ReplicateDelete,
            key: b"del_key".to_vec(),
            value: Vec::new(),
            ttl: 0,
            shard_id: 10,
        };

        let resp = repl.handle_replication_message(&msg).unwrap();
        assert_eq!(resp.op, PeerOp::ResponseOk);

        // Key should be deleted
        let val = handler.get_value(b"del_key");
        assert_eq!(val, None);
    }

    #[test]
    fn test_replication_stats() {
        let (repl, _, _dir) = create_replication_setup();

        let msg = PeerMessage {
            op: PeerOp::ReplicatePut,
            key: b"stats_key".to_vec(),
            value: b"stats_value".to_vec(),
            ttl: 0,
            shard_id: 5,
        };

        repl.handle_replication_message(&msg).unwrap();
        assert_eq!(repl.stats().entries_received.load(Ordering::Relaxed), 1);
    }
}
