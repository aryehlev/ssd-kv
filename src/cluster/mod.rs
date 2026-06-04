//! Ready protocol for cluster mode.
//!
//! Each node listens on `--cluster-port` for peer connections and also dials
//! every address in `--cluster-peers`. Both sides exchange:
//!
//!   `READY <node_id> <total_nodes>\n`
//!
//! Once a majority of peers (`total_nodes / 2 + 1`, counting self) have
//! completed the handshake, `wait_for_quorum` returns `true`.

use std::collections::HashSet;
use std::io::{BufRead, BufReader, Write as IoWrite};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tracing::{info, warn};

use crate::config::Config;

pub struct ClusterManager {
    ready_nodes: Arc<Mutex<HashSet<u32>>>,
    node_id: u32,
    total_nodes: u32,
}

impl ClusterManager {
    /// Start the cluster listener and peer dialers; returns immediately.
    pub fn start(config: &Config) -> Self {
        let node_id = config.node_id.expect("node_id required in cluster mode");
        let total_nodes = config.total_nodes.expect("total_nodes required in cluster mode");
        let ready_nodes: Arc<Mutex<HashSet<u32>>> = Arc::new(Mutex::new(HashSet::new()));

        // ── Listener ─────────────────────────────────────────────────────────
        {
            let ready = Arc::clone(&ready_nodes);
            let bind_addr = format!("0.0.0.0:{}", config.cluster_port);
            let listener = TcpListener::bind(&bind_addr)
                .unwrap_or_else(|e| panic!("cluster: failed to bind {bind_addr}: {e}"));
            info!("cluster listener started on {bind_addr}");

            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    match stream {
                        Ok(s) => {
                            let ready2 = Arc::clone(&ready);
                            std::thread::spawn(move || {
                                handle_incoming(s, node_id, total_nodes, ready2);
                            });
                        }
                        Err(e) => warn!("cluster: accept error: {e}"),
                    }
                }
            });
        }

        // ── Dialers (one per peer) ────────────────────────────────────────────
        for peer_addr in config.cluster_peer_list() {
            let ready = Arc::clone(&ready_nodes);
            std::thread::spawn(move || {
                dial_peer(peer_addr, node_id, total_nodes, ready);
            });
        }

        ClusterManager { ready_nodes, node_id, total_nodes }
    }

    /// Block until a majority of nodes are ready, or until `timeout` elapses.
    pub fn wait_for_quorum(&self, timeout: Duration) -> bool {
        let quorum = self.total_nodes / 2 + 1; // majority including self
        let deadline = Instant::now() + timeout;

        loop {
            let ready_count = {
                let set = self.ready_nodes.lock().unwrap();
                set.len() as u32 + 1 // +1 for self
            };

            if ready_count >= quorum {
                info!(
                    "cluster quorum reached: {}/{} nodes ready",
                    ready_count, self.total_nodes
                );
                return true;
            }

            if Instant::now() >= deadline {
                warn!(
                    "cluster quorum not reached within timeout ({}/{} nodes ready)",
                    ready_count, self.total_nodes
                );
                return false;
            }

            std::thread::sleep(Duration::from_millis(200));
        }
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

fn ready_line(node_id: u32, total_nodes: u32) -> String {
    format!("READY {node_id} {total_nodes}\n")
}

/// Parse `READY <id> <total>\n`; returns `Some(id)` on success.
fn parse_ready(line: &str) -> Option<u32> {
    let mut parts = line.trim().split_ascii_whitespace();
    if parts.next()? != "READY" {
        return None;
    }
    let id: u32 = parts.next()?.parse().ok()?;
    let _total: u32 = parts.next()?.parse().ok()?;
    Some(id)
}

/// Handle one accepted connection: read READY, reply READY, record peer.
fn handle_incoming(
    stream: TcpStream,
    my_id: u32,
    total_nodes: u32,
    ready_nodes: Arc<Mutex<HashSet<u32>>>,
) {
    let peer_addr = stream.peer_addr().map(|a| a.to_string()).unwrap_or_default();
    let mut reader = BufReader::new(&stream);
    let mut line = String::new();

    if reader.read_line(&mut line).is_err() {
        return;
    }
    let Some(peer_id) = parse_ready(&line) else { return };

    // Reply
    if stream_write(&stream, &ready_line(my_id, total_nodes)).is_ok() {
        ready_nodes.lock().unwrap().insert(peer_id);
        info!("cluster: peer {peer_id} connected from {peer_addr}");
    }
}

/// Dial a peer with exponential backoff; send READY and record the response.
fn dial_peer(
    addr: String,
    my_id: u32,
    total_nodes: u32,
    ready_nodes: Arc<Mutex<HashSet<u32>>>,
) {
    let mut delay = Duration::from_millis(100);
    let max_delay = Duration::from_secs(5);

    loop {
        match TcpStream::connect(&addr) {
            Ok(stream) => {
                if stream_write(&stream, &ready_line(my_id, total_nodes)).is_err() {
                    std::thread::sleep(delay);
                    delay = (delay * 2).min(max_delay);
                    continue;
                }
                let mut reader = BufReader::new(&stream);
                let mut line = String::new();
                if reader.read_line(&mut line).is_ok() {
                    if let Some(peer_id) = parse_ready(&line) {
                        ready_nodes.lock().unwrap().insert(peer_id);
                        info!("cluster: connected to peer {peer_id} at {addr}");
                        return;
                    }
                }
            }
            Err(_) => {
                std::thread::sleep(delay);
                delay = (delay * 2).min(max_delay);
            }
        }
    }
}

fn stream_write(mut stream: &TcpStream, msg: &str) -> std::io::Result<()> {
    stream.write_all(msg.as_bytes())
}
