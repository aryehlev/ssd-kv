//! Configuration for the SSD-KV server.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;

/// SSD-KV: High-Performance Key-Value Store
#[derive(Parser, Debug, Clone)]
#[command(name = "ssd-kv")]
#[command(author = "SSD-KV Team")]
#[command(version = "0.1.0")]
#[command(about = "Aerospike-inspired KV store: index in RAM, data on SSD")]
pub struct Config {
    /// Data directory for storage files
    #[arg(short, long, default_value = "./data")]
    pub data_dir: PathBuf,

    /// Server bind address
    #[arg(short, long, default_value = "127.0.0.1:7777")]
    pub bind: SocketAddr,

    /// Maximum number of client connections
    #[arg(long, default_value = "10000")]
    pub max_connections: usize,

    /// Enable verbose logging
    #[arg(short, long)]
    pub verbose: bool,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    pub log_level: String,

    /// Disable compaction
    #[arg(long)]
    pub no_compaction: bool,

    /// Compaction utilization threshold (0.0-1.0)
    #[arg(long, default_value = "0.5")]
    pub compaction_threshold: f32,

    /// Compaction check interval in seconds
    #[arg(long, default_value = "60")]
    pub compaction_interval: u64,

    /// Read buffer size in KB
    #[arg(long, default_value = "64")]
    pub read_buffer_kb: usize,

    /// Write buffer size in KB
    #[arg(long, default_value = "64")]
    pub write_buffer_kb: usize,

    /// Number of WBlocks per file (1 WBlock = 1MB)
    #[arg(long, default_value = "1023")]
    pub wblocks_per_file: u32,

    /// Number of worker threads (0 = auto-detect based on CPU count)
    #[arg(long, default_value = "0")]
    pub workers: usize,

    // --- Cluster mode options ---

    /// Enable cluster mode
    #[arg(long)]
    pub cluster_mode: bool,

    /// This node's ID (required in cluster mode, typically the StatefulSet ordinal)
    #[arg(long)]
    pub node_id: Option<u32>,

    /// Total number of nodes in the cluster
    #[arg(long)]
    pub total_nodes: Option<u32>,

    /// Replication factor (number of copies including primary)
    #[arg(long, default_value = "2")]
    pub replication_factor: u8,

    /// Port for inter-node gRPC communication
    #[arg(long, default_value = "7780")]
    pub cluster_port: u16,

    /// Comma-separated list of peer addresses (host:cluster_port)
    /// e.g. "ssdkv-0:7780,ssdkv-1:7780,ssdkv-2:7780"
    #[arg(long)]
    pub cluster_peers: Option<String>,

    /// Health check interval in milliseconds
    #[arg(long, default_value = "1000")]
    pub health_check_interval_ms: u64,

    /// Number of missed heartbeats before marking a node as dead
    #[arg(long, default_value = "3")]
    pub health_check_threshold: u32,
}

impl Config {
    /// Returns the number of worker threads.
    pub fn num_workers(&self) -> usize {
        if self.workers == 0 {
            std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(4)
        } else {
            self.workers
        }
    }

    /// Validates the configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.compaction_threshold < 0.0 || self.compaction_threshold > 1.0 {
            return Err("Compaction threshold must be between 0.0 and 1.0".to_string());
        }

        if self.wblocks_per_file == 0 || self.wblocks_per_file > 1023 {
            return Err("WBlocks per file must be between 1 and 1023".to_string());
        }

        if self.read_buffer_kb == 0 {
            return Err("Read buffer size must be positive".to_string());
        }

        if self.write_buffer_kb == 0 {
            return Err("Write buffer size must be positive".to_string());
        }

        if self.cluster_mode {
            if self.node_id.is_none() {
                return Err("--node-id is required in cluster mode".to_string());
            }
            if self.total_nodes.is_none() {
                return Err("--total-nodes is required in cluster mode".to_string());
            }
            if self.total_nodes.unwrap() == 0 {
                return Err("--total-nodes must be at least 1".to_string());
            }
            if self.replication_factor as u32 > self.total_nodes.unwrap() {
                return Err("Replication factor cannot exceed total nodes".to_string());
            }
        }

        Ok(())
    }

    /// Returns the read buffer size in bytes.
    pub fn read_buffer_size(&self) -> usize {
        self.read_buffer_kb * 1024
    }

    /// Returns the write buffer size in bytes.
    pub fn write_buffer_size(&self) -> usize {
        self.write_buffer_kb * 1024
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            bind: "127.0.0.1:7777".parse().unwrap(),
            max_connections: 10000,
            verbose: false,
            log_level: "info".to_string(),
            no_compaction: false,
            compaction_threshold: 0.5,
            compaction_interval: 60,
            read_buffer_kb: 64,
            write_buffer_kb: 64,
            wblocks_per_file: 1023,
            workers: 0,
            cluster_mode: false,
            node_id: None,
            total_nodes: None,
            replication_factor: 2,
            cluster_port: 7780,
            cluster_peers: None,
            health_check_interval_ms: 1000,
            health_check_threshold: 3,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation() {
        let mut config = Config::default();

        config.compaction_threshold = 1.5;
        assert!(config.validate().is_err());

        config.compaction_threshold = 0.5;
        config.wblocks_per_file = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_buffer_sizes() {
        let config = Config::default();
        assert_eq!(config.read_buffer_size(), 64 * 1024);
        assert_eq!(config.write_buffer_size(), 64 * 1024);
    }
}
