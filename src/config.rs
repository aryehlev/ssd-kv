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
    #[arg(short, long, default_value = "127.0.0.1:6379")]
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
}

impl Config {
    pub fn validate(&self) -> Result<(), String> {
        if self.num_dbs == 0 || self.num_dbs > 16 {
            return Err("--num-dbs must be between 1 and 16".to_string());
        }
        if self.reactor_threads == 0 {
            return Err("--reactor-threads must be at least 1".to_string());
        }
        Ok(())
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
            bind: "127.0.0.1:6379".parse().unwrap(),
            max_connections: 10_000,
            verbose: false,
            log_level: "info".to_string(),
            reactor_threads: 1,
            read_buffer_kb: 64,
            write_buffer_kb: 64,
            num_dbs: 16,
        }
    }
}
