//! Connection pool for inter-node gRPC communication.
//!
//! Each node maintains a pool of connections to every other node.
//! Connections are lazily established and reconnected on failure.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex as TokioMutex;

use crate::cluster::node::NodeId;

/// A message sent between cluster nodes over the peer protocol.
#[derive(Debug, Clone)]
pub struct PeerMessage {
    /// The operation type.
    pub op: PeerOp,
    /// The key (for key-based operations).
    pub key: Vec<u8>,
    /// The value (for PUT operations).
    pub value: Vec<u8>,
    /// TTL in seconds (for PUT operations).
    pub ttl: u32,
    /// Shard ID for routing context.
    pub shard_id: u16,
}

/// Peer operation types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PeerOp {
    /// Forward a GET request.
    ForwardGet = 0x31,
    /// Forward a PUT request.
    ForwardPut = 0x32,
    /// Forward a DELETE request.
    ForwardDelete = 0x33,
    /// Replicate a PUT to a replica.
    ReplicatePut = 0x41,
    /// Replicate a DELETE to a replica.
    ReplicateDelete = 0x42,
    /// Heartbeat/health check.
    Heartbeat = 0x50,
    /// Heartbeat response.
    HeartbeatAck = 0x51,
    /// Response with value.
    ResponseOk = 0x60,
    /// Response: key not found.
    ResponseNotFound = 0x61,
    /// Response: error.
    ResponseError = 0x62,
}

impl PeerOp {
    pub fn from_u8(b: u8) -> Option<Self> {
        match b {
            0x31 => Some(Self::ForwardGet),
            0x32 => Some(Self::ForwardPut),
            0x33 => Some(Self::ForwardDelete),
            0x41 => Some(Self::ReplicatePut),
            0x42 => Some(Self::ReplicateDelete),
            0x50 => Some(Self::Heartbeat),
            0x51 => Some(Self::HeartbeatAck),
            0x60 => Some(Self::ResponseOk),
            0x61 => Some(Self::ResponseNotFound),
            0x62 => Some(Self::ResponseError),
            _ => None,
        }
    }
}

/// Wire format for peer messages:
///   [op:1][shard_id:2][ttl:4][key_len:4][value_len:4][key:N][value:M]
const HEADER_SIZE: usize = 1 + 2 + 4 + 4 + 4; // 15 bytes

impl PeerMessage {
    /// Serializes the message into bytes for wire transmission.
    pub fn serialize(&self) -> Vec<u8> {
        let total = HEADER_SIZE + self.key.len() + self.value.len();
        let mut buf = Vec::with_capacity(total);
        buf.push(self.op as u8);
        buf.extend_from_slice(&self.shard_id.to_be_bytes());
        buf.extend_from_slice(&self.ttl.to_be_bytes());
        buf.extend_from_slice(&(self.key.len() as u32).to_be_bytes());
        buf.extend_from_slice(&(self.value.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);
        buf
    }

    /// Deserializes a message from bytes.
    pub fn deserialize(data: &[u8]) -> io::Result<Self> {
        if data.len() < HEADER_SIZE {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Message too short"));
        }
        let op = PeerOp::from_u8(data[0])
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Unknown op"))?;
        let shard_id = u16::from_be_bytes([data[1], data[2]]);
        let ttl = u32::from_be_bytes([data[3], data[4], data[5], data[6]]);
        let key_len = u32::from_be_bytes([data[7], data[8], data[9], data[10]]) as usize;
        let value_len = u32::from_be_bytes([data[11], data[12], data[13], data[14]]) as usize;

        if data.len() < HEADER_SIZE + key_len + value_len {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Message truncated"));
        }

        let key = data[HEADER_SIZE..HEADER_SIZE + key_len].to_vec();
        let value = data[HEADER_SIZE + key_len..HEADER_SIZE + key_len + value_len].to_vec();

        Ok(Self {
            op,
            key,
            value,
            ttl,
            shard_id,
        })
    }

    /// Reads a complete message from an async stream.
    pub async fn read_from(stream: &mut TcpStream) -> io::Result<Self> {
        let mut header = [0u8; HEADER_SIZE];
        stream.read_exact(&mut header).await?;

        let key_len = u32::from_be_bytes([header[7], header[8], header[9], header[10]]) as usize;
        let value_len = u32::from_be_bytes([header[11], header[12], header[13], header[14]]) as usize;

        let mut payload = vec![0u8; key_len + value_len];
        if !payload.is_empty() {
            stream.read_exact(&mut payload).await?;
        }

        let mut full = Vec::with_capacity(HEADER_SIZE + payload.len());
        full.extend_from_slice(&header);
        full.extend_from_slice(&payload);

        Self::deserialize(&full)
    }

    /// Writes the message to an async stream.
    pub async fn write_to(&self, stream: &mut TcpStream) -> io::Result<()> {
        let data = self.serialize();
        stream.write_all(&data).await
    }
}

/// A connection to a single peer node.
struct PeerConnection {
    stream: TokioMutex<Option<TcpStream>>,
    addr: SocketAddr,
}

impl PeerConnection {
    fn new(addr: SocketAddr) -> Self {
        Self {
            stream: TokioMutex::new(None),
            addr,
        }
    }

    /// Gets or establishes a connection.
    async fn get_stream(&self) -> io::Result<tokio::sync::MutexGuard<'_, Option<TcpStream>>> {
        let mut guard = self.stream.lock().await;
        if guard.is_none() {
            let stream = tokio::time::timeout(
                Duration::from_secs(5),
                TcpStream::connect(self.addr),
            )
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Connection timeout"))?
            ?;
            stream.set_nodelay(true)?;
            *guard = Some(stream);
        }
        Ok(guard)
    }

    /// Sends a message and waits for a response.
    async fn send_recv(&self, msg: &PeerMessage) -> io::Result<PeerMessage> {
        let mut guard = self.get_stream().await?;
        let stream = guard.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotConnected, "No connection")
        })?;

        // Send
        msg.write_to(stream).await?;

        // Receive response
        match PeerMessage::read_from(stream).await {
            Ok(resp) => Ok(resp),
            Err(e) => {
                // Connection is broken, drop it
                *guard = None;
                Err(e)
            }
        }
    }

    /// Sends a message without waiting for a response (fire-and-forget).
    async fn send_only(&self, msg: &PeerMessage) -> io::Result<()> {
        let mut guard = self.get_stream().await?;
        let stream = guard.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotConnected, "No connection")
        })?;

        match msg.write_to(stream).await {
            Ok(()) => Ok(()),
            Err(e) => {
                *guard = None;
                Err(e)
            }
        }
    }

    /// Closes the connection.
    async fn close(&self) {
        let mut guard = self.stream.lock().await;
        *guard = None;
    }
}

/// Pool of connections to all peer nodes.
pub struct PeerConnectionPool {
    /// Map from node ID to connection.
    peers: RwLock<HashMap<NodeId, Arc<PeerConnection>>>,
}

impl PeerConnectionPool {
    pub fn new() -> Self {
        Self {
            peers: RwLock::new(HashMap::new()),
        }
    }

    /// Adds or updates a peer address.
    pub fn add_peer(&self, node_id: NodeId, addr: SocketAddr) {
        let mut peers = self.peers.write();
        peers.insert(node_id, Arc::new(PeerConnection::new(addr)));
    }

    /// Removes a peer.
    pub fn remove_peer(&self, node_id: NodeId) {
        let mut peers = self.peers.write();
        peers.remove(&node_id);
    }

    /// Sends a request to a peer and waits for a response.
    pub async fn send_request(&self, node_id: NodeId, msg: &PeerMessage) -> io::Result<PeerMessage> {
        let conn = {
            let peers = self.peers.read();
            peers.get(&node_id).cloned().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("No connection to node {}", node_id),
                )
            })?
        };
        conn.send_recv(msg).await
    }

    /// Sends a message to a peer without waiting for a response (replication).
    pub async fn send_async(&self, node_id: NodeId, msg: &PeerMessage) -> io::Result<()> {
        let conn = {
            let peers = self.peers.read();
            peers.get(&node_id).cloned().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("No connection to node {}", node_id),
                )
            })?
        };
        conn.send_only(msg).await
    }

    /// Returns the list of connected peer node IDs.
    pub fn peer_ids(&self) -> Vec<NodeId> {
        self.peers.read().keys().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_message_serialize_roundtrip() {
        let msg = PeerMessage {
            op: PeerOp::ForwardGet,
            key: b"test_key".to_vec(),
            value: Vec::new(),
            ttl: 0,
            shard_id: 42,
        };
        let data = msg.serialize();
        let decoded = PeerMessage::deserialize(&data).unwrap();
        assert_eq!(decoded.op, PeerOp::ForwardGet);
        assert_eq!(decoded.key, b"test_key");
        assert_eq!(decoded.shard_id, 42);
    }

    #[test]
    fn test_peer_message_with_value() {
        let msg = PeerMessage {
            op: PeerOp::ForwardPut,
            key: b"key".to_vec(),
            value: b"some_value".to_vec(),
            ttl: 300,
            shard_id: 100,
        };
        let data = msg.serialize();
        let decoded = PeerMessage::deserialize(&data).unwrap();
        assert_eq!(decoded.op, PeerOp::ForwardPut);
        assert_eq!(decoded.key, b"key");
        assert_eq!(decoded.value, b"some_value");
        assert_eq!(decoded.ttl, 300);
        assert_eq!(decoded.shard_id, 100);
    }

    #[test]
    fn test_peer_op_roundtrip() {
        for op_byte in [0x31, 0x32, 0x33, 0x41, 0x42, 0x50, 0x51, 0x60, 0x61, 0x62] {
            let op = PeerOp::from_u8(op_byte).unwrap();
            assert_eq!(op as u8, op_byte);
        }
        assert!(PeerOp::from_u8(0x00).is_none());
        assert!(PeerOp::from_u8(0xFF).is_none());
    }

    #[test]
    fn test_pool_add_remove() {
        let pool = PeerConnectionPool::new();
        pool.add_peer(1, "127.0.0.1:7781".parse().unwrap());
        pool.add_peer(2, "127.0.0.1:7782".parse().unwrap());
        assert_eq!(pool.peer_ids().len(), 2);

        pool.remove_peer(1);
        assert_eq!(pool.peer_ids().len(), 1);
        assert!(pool.peer_ids().contains(&2));
    }
}
