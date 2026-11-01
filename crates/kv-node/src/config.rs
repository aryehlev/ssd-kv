//! Configuration and command line of `kvd` (DESIGN §20).
//!
//! ```text
//! kvd [CONFIG_FILE] [--<name> <value>...]      Redis-style: file first, then overrides
//! kvd format --device PATH[:SIZE] [--device ...] [--partitions 64] [--cluster]
//!            [--snapshot-region SIZE|PCT] [--force]
//! kvd inspect --device PATH [--ag N] [--page ID] [--show-data]
//! kvd check --device PATH... [--placement-report]
//! ```
//! File syntax: `name value ...` lines, `#` comments, `include path`. Unknown names, bad values
//! or a format mismatch exit with code 1 and a one-line reason before listening (§22).
//! `kvd --port {port} --data-dir {dir}` (the tests/compat launch template) serves
//! `{dir}/kvd.data` of `data-size` bytes, formatting it on first start (`auto-format` applies to
//! `data-dir` files only, never to `device` entries). Parameters marked R in §20.2 can be changed
//! with CONFIG SET. The schema has no per-device, concurrency or stream-count keys and no
//! queue-depth key other than `recovery-qd` (LC-37), and no default-TTL key (LC-13). `threads`
//! defaults from `std::thread::available_parallelism` only, clamped to `[1, P]`.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use kv_storage::engine::{DeviceCachePolicy, FlushMethod, GranularityPolicy, PageUpdatePolicy};
use kv_storage::format::SnapshotRegionSize;
use kv_storage::partition::Durability;

#[derive(Parser)]
#[command(
    name = "kvd",
    version,
    about = "kvssd data node: Redis-protocol compatible key-value server with a RAM index and an in-place SSD page store",
    args_conflicts_with_subcommands = true
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,
    #[command(flatten)]
    pub serve: ServeArgs,
}

#[derive(Subcommand)]
pub enum Command {
    /// Format data devices or files (zero-fills files; refuses unsupported substrates).
    Format(FormatArgs),
    /// Dump superblock, AG table, metadata or one page (read-only; record bytes redacted).
    Inspect(InspectArgs),
    /// Offline full scan and invariant check (exit 0 or 1).
    Check(CheckArgs),
}

#[derive(Args, Debug)]
pub struct FormatArgs {
    /// `PATH[:SIZE]`; repeat for several devices.
    #[arg(long = "device", value_name = "PATH[:SIZE]", required = true)]
    pub devices: Vec<String>,
    /// Partitions P (16, 32, 64 or 128); a cluster-wide constant.
    #[arg(long, default_value_t = 64)]
    pub partitions: u16,
    #[arg(long)]
    pub cluster: bool,
    /// Per region, per device: a size (e.g. 512mb) or a percentage (e.g. 3%); 0 = none.
    #[arg(long, value_name = "SIZE|PCT")]
    pub snapshot_region: Option<String>,
    /// Overwrite an existing kvssd format.
    #[arg(long)]
    pub force: bool,
}

#[derive(Args, Debug)]
pub struct InspectArgs {
    #[arg(long)]
    pub device: PathBuf,
    #[arg(long, value_name = "N")]
    pub ag: Option<u32>,
    #[arg(long, value_name = "ID")]
    pub page: Option<u64>,
    /// Print record keys and values instead of redacted placeholders.
    #[arg(long)]
    pub show_data: bool,
}

#[derive(Args, Debug)]
pub struct CheckArgs {
    #[arg(long = "device", value_name = "PATH", num_args = 1.., required = true)]
    pub devices: Vec<PathBuf>,
    /// Report placement statistics (seq vs device address correlation, AG acquisition runs).
    #[arg(long)]
    pub placement_report: bool,
}

/// `kvd [CONFIG_FILE] [--<name> <value>...]`: every §20.2 parameter as an override. Values are
/// validated by [`Config::load`] with the same rules as the file.
#[derive(Args, Default)]
pub struct ServeArgs {
    /// Configuration file (`name value` lines).
    #[arg(value_name = "CONFIG_FILE")]
    pub config_file: Option<PathBuf>,

    // network
    #[arg(long, value_name = "ADDR", num_args = 1..)]
    pub bind: Option<Vec<String>>,
    #[arg(long, value_name = "PORT")]
    pub port: Option<String>,
    #[arg(long, value_name = "PATH")]
    pub unixsocket: Option<String>,
    #[arg(long, value_name = "N")]
    pub tcp_backlog: Option<String>,
    #[arg(long, value_name = "SECONDS")]
    pub tcp_keepalive: Option<String>,
    #[arg(long, value_name = "SECONDS")]
    pub timeout: Option<String>,
    #[arg(long, value_name = "N")]
    pub maxclients: Option<String>,
    #[arg(long, value_name = "PASSWORD")]
    pub requirepass: Option<String>,
    #[arg(long, value_name = "N")]
    pub databases: Option<String>,
    #[arg(long, value_name = "N|auto")]
    pub threads: Option<String>,

    // storage
    #[arg(long, value_name = "DIR")]
    pub data_dir: Option<String>,
    #[arg(long, value_name = "SIZE")]
    pub data_size: Option<String>,
    #[arg(long, value_name = "yes|no")]
    pub auto_format: Option<String>,
    /// `PATH [SIZE]`; repeatable.
    #[arg(long, value_name = "PATH [SIZE]", num_args = 1..=2, action = clap::ArgAction::Append)]
    pub device: Option<Vec<String>>,
    #[arg(long, value_name = "P")]
    pub format_partitions: Option<String>,
    #[arg(long, value_name = "SIZE|auto")]
    pub cache_size: Option<String>,
    #[arg(long, value_name = "async|written|flushed")]
    pub durability: Option<String>,
    #[arg(long, value_name = "auto|fdatasync|dsync|none")]
    pub flush_method: Option<String>,
    #[arg(long, value_name = "auto|write-through|write-back")]
    pub device_cache: Option<String>,
    #[arg(long, value_name = "auto|page|lba")]
    pub write_granularity: Option<String>,
    #[arg(long, value_name = "auto|two-image|atomic-overwrite")]
    pub page_update: Option<String>,
    #[arg(long, value_name = "BYTES")]
    pub page_slack_cap: Option<String>,
    #[arg(long, value_name = "BYTES")]
    pub page_slack_atomic: Option<String>,
    #[arg(long, value_name = "FRACTION")]
    pub max_storage_fill: Option<String>,
    #[arg(long, value_name = "yes|no")]
    pub snapshot_on_shutdown: Option<String>,
    #[arg(long, value_name = "SIZE|PCT")]
    pub snapshot_region: Option<String>,
    #[arg(long, value_name = "yes|no")]
    pub uring_fixed_buffers: Option<String>,

    // limits
    #[arg(long, value_name = "N")]
    pub max_pending_per_conn: Option<String>,
    #[arg(long, value_name = "SIZE")]
    pub client_output_buffer_limit: Option<String>,
    #[arg(long, value_name = "SIZE")]
    pub proto_max_bulk_len: Option<String>,
    #[arg(long, value_name = "SIZE")]
    pub max_dirty_bytes: Option<String>,
    #[arg(long, value_name = "SIZE")]
    pub large_value_memory: Option<String>,

    // background rates
    #[arg(long, value_name = "N")]
    pub active_expire_reads_per_sec: Option<String>,
    #[arg(long, value_name = "N")]
    pub scrub_iops: Option<String>,
    #[arg(long, value_name = "N")]
    pub warm_iops: Option<String>,
    #[arg(long, value_name = "N")]
    pub reclaim_iops: Option<String>,
    #[arg(long, value_name = "N")]
    pub purge_iops: Option<String>,
    #[arg(long, value_name = "yes|no")]
    pub discard: Option<String>,
    #[arg(long, value_name = "N")]
    pub discard_mbps: Option<String>,
    #[arg(long, value_name = "N")]
    pub ag_release_per_sec: Option<String>,
    #[arg(long, value_name = "yes|no")]
    pub latency_controller: Option<String>,
    #[arg(long, value_name = "US")]
    pub latency_target_p99_us: Option<String>,
    #[arg(long, value_name = "N")]
    pub recovery_qd: Option<String>,
    #[arg(long, value_name = "MS")]
    pub clock_floor_interval_ms: Option<String>,
    #[arg(long, value_name = "SIZE")]
    pub hash_max_bytes: Option<String>,
    #[arg(long, value_name = "SIZE")]
    pub extent_cache_max: Option<String>,

    // diagnostics
    #[arg(long, value_name = "debug|verbose|notice|warning")]
    pub loglevel: Option<String>,
    #[arg(long, value_name = "PATH")]
    pub logfile: Option<String>,
    #[arg(long, value_name = "yes|no")]
    pub core_dumps: Option<String>,
    #[arg(long, value_name = "PORT")]
    pub metrics_port: Option<String>,

    // cluster
    #[arg(long, value_name = "yes|no")]
    pub cluster_enabled: Option<String>,
    #[arg(long, value_name = "PORT")]
    pub cluster_control_port: Option<String>,
    #[arg(long, value_name = "PORT")]
    pub cluster_repl_port: Option<String>,
    #[arg(long, value_name = "IP")]
    pub cluster_announce_ip: Option<String>,
    #[arg(long, value_name = "PORT")]
    pub cluster_announce_port: Option<String>,
    #[arg(long, value_name = "TOKEN")]
    pub cluster_auth_token: Option<String>,
    #[arg(long, value_name = "SIZE")]
    pub repl_backlog_size: Option<String>,
    #[arg(long, value_name = "MS")]
    pub repl_ack_timeout_ms: Option<String>,
    #[arg(long, value_name = "MS")]
    pub repl_heartbeat_ms: Option<String>,
    #[arg(long, value_name = "N")]
    pub sync_mbps: Option<String>,
    #[arg(long, value_name = "N")]
    pub sync_iops: Option<String>,
}

/// Redis log levels.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LogLevel {
    Debug,
    Verbose,
    Notice,
    Warning,
}

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;
const GIB: u64 = 1024 * MIB;

/// Effective configuration (§20.2 defaults in `Default`).
/// No `Debug`: holds `requirepass` and the cluster token.
#[derive(Clone)]
pub struct Config {
    pub config_file: Option<PathBuf>,
    pub bind: Vec<String>,
    pub port: u16,
    pub unixsocket: String,
    pub tcp_backlog: u32,
    pub tcp_keepalive: u32,
    /// Idle client seconds (0 = never).
    pub timeout: u64,
    pub maxclients: u32,
    pub requirepass: String,
    /// 16 standalone; forced to 1 in cluster mode.
    pub databases: u8,
    /// `None` = auto (online CPUs, clamped to `[1, P]`).
    pub threads: Option<u16>,
    pub data_dir: PathBuf,
    pub data_size: u64,
    pub auto_format: bool,
    /// `device PATH [SIZE]` entries.
    pub device: Vec<(PathBuf, Option<u64>)>,
    pub format_partitions: u16,
    /// `None` = auto (60% of MemAvailable minus the index estimate).
    pub cache_size: Option<u64>,
    /// `None` = the mode default (written standalone, flushed cluster).
    pub durability: Option<Durability>,
    pub flush_method: FlushMethod,
    pub device_cache: DeviceCachePolicy,
    pub write_granularity: GranularityPolicy,
    pub page_update: PageUpdatePolicy,
    pub page_slack_cap: u16,
    pub page_slack_atomic: u16,
    pub max_storage_fill: f32,
    pub snapshot_on_shutdown: bool,
    pub snapshot_region: SnapshotRegionSize,
    /// Register page-cache frames as fixed io_uring buffers (§12).
    pub uring_fixed_buffers: bool,
    pub max_pending_per_conn: u32,
    pub client_output_buffer_limit: u64,
    pub proto_max_bulk_len: u64,
    pub max_dirty_bytes: u64,
    pub large_value_memory: u64,
    pub active_expire_reads_per_sec: u64,
    pub scrub_iops: u64,
    pub warm_iops: u64,
    pub reclaim_iops: u64,
    pub purge_iops: u64,
    pub discard: bool,
    pub discard_mbps: u64,
    pub ag_release_per_sec: u64,
    pub latency_controller: bool,
    pub latency_target_p99_us: u32,
    pub recovery_qd: u32,
    pub clock_floor_interval_ms: u64,
    pub hash_max_bytes: u64,
    pub extent_cache_max: u64,
    pub loglevel: LogLevel,
    /// "" = stderr.
    pub logfile: String,
    pub core_dumps: bool,
    /// 0 = off.
    pub metrics_port: u16,
    pub cluster_enabled: bool,
    /// `None` = port + 10000.
    pub cluster_control_port: Option<u16>,
    /// `None` = port + 20000.
    pub cluster_repl_port: Option<u16>,
    pub cluster_announce_ip: String,
    pub cluster_announce_port: u16,
    pub cluster_auth_token: String,
    pub repl_backlog_size: u64,
    pub repl_ack_timeout_ms: u64,
    pub repl_heartbeat_ms: u64,
    pub sync_mbps: u64,
    pub sync_iops: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            config_file: None,
            bind: vec!["127.0.0.1".to_owned()],
            port: 6379,
            unixsocket: String::new(),
            tcp_backlog: 511,
            tcp_keepalive: 300,
            timeout: 0,
            maxclients: 10000,
            requirepass: String::new(),
            databases: 16,
            threads: None,
            data_dir: PathBuf::from("."),
            data_size: 4 * GIB,
            auto_format: true,
            device: Vec::new(),
            format_partitions: 64,
            cache_size: None,
            durability: None,
            flush_method: FlushMethod::Auto,
            device_cache: DeviceCachePolicy::Auto,
            write_granularity: GranularityPolicy::Auto,
            page_update: PageUpdatePolicy::Auto,
            page_slack_cap: 1024,
            page_slack_atomic: 128,
            max_storage_fill: 0.95,
            snapshot_on_shutdown: true,
            snapshot_region: SnapshotRegionSize::Percent(3.0),
            uring_fixed_buffers: true,
            max_pending_per_conn: 4096,
            client_output_buffer_limit: 64 * MIB,
            proto_max_bulk_len: 512 * MIB,
            max_dirty_bytes: 256 * MIB,
            large_value_memory: 2 * GIB,
            active_expire_reads_per_sec: 20000,
            scrub_iops: 4000,
            warm_iops: 8000,
            reclaim_iops: 4000,
            purge_iops: 4000,
            discard: true,
            discard_mbps: 200,
            ag_release_per_sec: 20,
            latency_controller: false,
            latency_target_p99_us: 1000,
            recovery_qd: 16,
            clock_floor_interval_ms: 1000,
            hash_max_bytes: 16 * MIB,
            extent_cache_max: 0,
            loglevel: LogLevel::Notice,
            logfile: String::new(),
            core_dumps: false,
            metrics_port: 0,
            cluster_enabled: false,
            cluster_control_port: None,
            cluster_repl_port: None,
            cluster_announce_ip: String::new(),
            cluster_announce_port: 0,
            cluster_auth_token: String::new(),
            repl_backlog_size: 32 * MIB,
            repl_ack_timeout_ms: 250,
            repl_heartbeat_ms: 10,
            sync_mbps: 200,
            sync_iops: 5000,
        }
    }
}

/// A configuration error: printed as one line, exit code 1.
#[derive(Debug)]
pub struct ConfigError(pub String);

impl Config {
    /// Applies the config file (with includes), then the command-line overrides, then validates.
    pub fn load(args: &ServeArgs) -> Result<Config, ConfigError> {
        todo!("DESIGN §20: configuration loading")
    }

    /// CONFIG SET of one parameter (R parameters only).
    pub fn set(&mut self, name: &str, value: &str) -> Result<(), ConfigError> {
        todo!("DESIGN §20.2: CONFIG SET")
    }

    /// CONFIG GET: `(name, value)` pairs matching a glob, including the compatibility keys.
    pub fn get(&self, pattern: &[u8]) -> Vec<(String, String)> {
        todo!("DESIGN §20.2: CONFIG GET")
    }
}
