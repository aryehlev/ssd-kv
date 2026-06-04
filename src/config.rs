//! Configuration for the SIndex KV server.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;

/// SIndex: Trillion-scale SSD-based KV store with deterministic latency.
///
/// Implements the design from:
/// "The Design of Trillion-scale SSD-based Indexing with Deterministic
/// Latency for Cloud Block Storage", ACM TOS 2024 (DOI 10.1145/3789205).
#[derive(Parser, Debug, Clone)]
#[command(name = "ssd-kv")]
#[command(version = "0.2.0")]
#[command(about = "SIndex KV: trillion-scale SSD indexing with deterministic latency")]
pub struct Config {
    /// Directory for segment files and value log.
    #[arg(short, long, default_value = "./data")]
    pub data_dir: PathBuf,

    /// Server bind address (Redis-compatible RESP protocol).
    #[arg(short, long, default_value = "127.0.0.1:7777")]
    pub bind: SocketAddr,

    /// Maximum concurrent client connections.
    #[arg(long, default_value = "10000")]
    pub max_connections: usize,

    /// Enable verbose debug logging.
    #[arg(short, long)]
    pub verbose: bool,

    /// Log level (trace, debug, info, warn, error).
    #[arg(long, default_value = "info")]
    pub log_level: String,

    /// Number of RESP reactor threads. Each shares the port via SO_REUSEPORT.
    #[arg(long, default_value = "1")]
    pub reactor_threads: usize,

    /// Read buffer size in KB per connection.
    #[arg(long, default_value = "64")]
    pub read_buffer_kb: usize,

    /// Write buffer size in KB per connection.
    #[arg(long, default_value = "64")]
    pub write_buffer_kb: usize,

    /// Number of logical databases (SELECT 0..N-1).
    #[arg(long, default_value = "16")]
    pub num_dbs: u8,

    // ── Cluster ──────────────────────────────────────────────────────────────

    /// Run as a cluster member (enables the ready-protocol handshake).
    #[arg(long)]
    pub cluster_mode: bool,

    /// This node's ordinal ID (required in cluster mode).
    #[arg(long)]
    pub node_id: Option<u32>,

    /// Total number of nodes in the cluster (required in cluster mode).
    #[arg(long)]
    pub total_nodes: Option<u32>,

    /// Inter-node port for the ready-protocol handshake.
    #[arg(long, default_value = "7780")]
    pub cluster_port: u16,

    /// Comma-separated `host:port` list of peer cluster-port addresses.
    #[arg(long, default_value = "")]
    pub cluster_peers: String,

    /// Copies per key including primary.
    #[arg(long, default_value = "2")]
    pub replication_factor: u32,

    /// Heartbeat interval in milliseconds.
    #[arg(long, default_value = "1000")]
    pub health_check_interval_ms: u64,

    /// Missed heartbeats before a node is marked dead.
    #[arg(long, default_value = "3")]
    pub health_check_threshold: u32,

    /// Allow reads from replica nodes.
    #[arg(long)]
    pub replica_read: bool,
}

impl Config {
    pub fn validate(&self) -> Result<(), String> {
        if self.num_dbs == 0 || self.num_dbs > 16 {
            return Err("--num-dbs must be between 1 and 16".to_string());
        }
        if self.reactor_threads == 0 {
            return Err("--reactor-threads must be at least 1".to_string());
        }
        if self.cluster_mode {
            if self.node_id.is_none() {
                return Err("--node-id is required in cluster mode".to_string());
            }
            if self.total_nodes.is_none() {
                return Err("--total-nodes is required in cluster mode".to_string());
            }
        }
        Ok(())
    }

    /// Parse `--cluster-peers` into a list of `host:port` strings.
    pub fn cluster_peer_list(&self) -> Vec<String> {
        self.cluster_peers
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect()
    }

    pub fn read_buffer_bytes(&self) -> usize {
        self.read_buffer_kb * 1024
    }

    pub fn write_buffer_bytes(&self) -> usize {
        self.write_buffer_kb * 1024
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            bind: "127.0.0.1:7777".parse().unwrap(),
            max_connections: 10_000,
            verbose: false,
            log_level: "info".to_string(),
            reactor_threads: 1,
            read_buffer_kb: 64,
            write_buffer_kb: 64,
            num_dbs: 16,
            cluster_mode: false,
            node_id: None,
            total_nodes: None,
            cluster_port: 7780,
            cluster_peers: String::new(),
            replication_factor: 2,
            health_check_interval_ms: 1000,
            health_check_threshold: 3,
            replica_read: false,
        }
    }
}
