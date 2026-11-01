//! Control connections to data nodes (DESIGN §23.4).
//!
//! The leader dials every registered node's control port (nodes never dial anyone for
//! topology), performs the `Hello`/`HelloAck` handshake, pushes `Assign` on every committed
//! change of a node's projection and answers renewals. Requests carry a `req` id; replies echo
//! it. A protocol violation closes the connection (§22).

use std::time::Duration;

use kv_ctrl::{CoordToNode, NodeId, NodeToCoord};

#[derive(Debug)]
pub enum PushError {
    NotConnected,
    Timeout,
    Closed,
    Io(std::io::Error),
}

pub struct Pusher {
    conns: tokio::sync::Mutex<hashbrown::HashMap<NodeId, tokio::sync::mpsc::Sender<CoordToNode>, foldhash::fast::RandomState>>,
}

impl Pusher {
    pub async fn send(&self, n: &NodeId, m: CoordToNode) -> Result<(), PushError> {
        todo!("DESIGN §23.4: push")
    }

    pub async fn request(&self, n: &NodeId, m: CoordToNode, timeout: Duration) -> Result<NodeToCoord, PushError> {
        todo!("DESIGN §23.4: request/reply")
    }
}
