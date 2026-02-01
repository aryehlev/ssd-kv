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
}

impl Config {
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
