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
//!
//! Values follow Redis conventions: booleans are `yes`/`no`; sizes are bytes with an optional
//! `k`/`kb`/`m`/`mb`/`g`/`gb` suffix (`k` = 1000, `kb` = 1024, as Redis `memtoll`); a quoted
//! token (`"..."` with `\n \r \t \\ \" \xHH` escapes, `'...'`) may contain spaces. Settings are
//! applied in order (file lines, includes in place, then command-line overrides), so the last
//! one wins; `device` lines accumulate.

use std::path::{Path, PathBuf};

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
    /// `PATH[:SIZE]`; repeatable (the file form is `device PATH [SIZE]`).
    #[arg(long, value_name = "PATH[:SIZE]", action = clap::ArgAction::Append)]
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

impl ServeArgs {
    /// The command-line overrides as `(parameter name, values)` in §20.2 order.
    pub fn overrides(&self) -> Vec<(&'static str, Vec<String>)> {
        let mut out: Vec<(&'static str, Vec<String>)> = Vec::new();
        macro_rules! one {
            ($($field:ident => $name:literal),* $(,)?) => {
                $( if let Some(v) = &self.$field { out.push(($name, vec![v.clone()])); } )*
            };
        }
        if let Some(b) = &self.bind {
            out.push(("bind", b.clone()));
        }
        one!(port => "port", unixsocket => "unixsocket", tcp_backlog => "tcp-backlog",
            tcp_keepalive => "tcp-keepalive", timeout => "timeout", maxclients => "maxclients",
            requirepass => "requirepass", databases => "databases", threads => "threads",
            data_dir => "data-dir", data_size => "data-size", auto_format => "auto-format");
        if let Some(devs) = &self.device {
            for d in devs {
                // `PATH[:SIZE]` becomes the file form `PATH [SIZE]`
                let v = match parse_device_spec(d) {
                    Ok((path, Some(_))) => {
                        let (_, size) = d.rsplit_once(':').unwrap_or((d, ""));
                        vec![path.display().to_string(), size.to_owned()]
                    }
                    _ => vec![d.clone()],
                };
                out.push(("device", v));
            }
        }
        one!(format_partitions => "format-partitions", cache_size => "cache-size",
            durability => "durability", flush_method => "flush-method",
            device_cache => "device-cache", write_granularity => "write-granularity",
            page_update => "page-update", page_slack_cap => "page-slack-cap",
            page_slack_atomic => "page-slack-atomic", max_storage_fill => "max-storage-fill",
            snapshot_on_shutdown => "snapshot-on-shutdown", snapshot_region => "snapshot-region",
            uring_fixed_buffers => "uring-fixed-buffers",
            max_pending_per_conn => "max-pending-per-conn",
            client_output_buffer_limit => "client-output-buffer-limit",
            proto_max_bulk_len => "proto-max-bulk-len", max_dirty_bytes => "max-dirty-bytes",
            large_value_memory => "large-value-memory",
            active_expire_reads_per_sec => "active-expire-reads-per-sec",
            scrub_iops => "scrub-iops", warm_iops => "warm-iops", reclaim_iops => "reclaim-iops",
            purge_iops => "purge-iops", discard => "discard", discard_mbps => "discard-mbps",
            ag_release_per_sec => "ag-release-per-sec", latency_controller => "latency-controller",
            latency_target_p99_us => "latency-target-p99-us", recovery_qd => "recovery-qd",
            clock_floor_interval_ms => "clock-floor-interval-ms",
            hash_max_bytes => "hash-max-bytes", extent_cache_max => "extent-cache-max",
            loglevel => "loglevel", logfile => "logfile", core_dumps => "core-dumps",
            metrics_port => "metrics-port", cluster_enabled => "cluster-enabled",
            cluster_control_port => "cluster-control-port",
            cluster_repl_port => "cluster-repl-port",
            cluster_announce_ip => "cluster-announce-ip",
            cluster_announce_port => "cluster-announce-port",
            cluster_auth_token => "cluster-auth-token", repl_backlog_size => "repl-backlog-size",
            repl_ack_timeout_ms => "repl-ack-timeout-ms", repl_heartbeat_ms => "repl-heartbeat-ms",
            sync_mbps => "sync-mbps", sync_iops => "sync-iops");
        out
    }
}

/// Redis log levels.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LogLevel {
    Debug,
    Verbose,
    Notice,
    Warning,
}

impl LogLevel {
    pub fn name(self) -> &'static str {
        match self {
            LogLevel::Debug => "debug",
            LogLevel::Verbose => "verbose",
            LogLevel::Notice => "notice",
            LogLevel::Warning => "warning",
        }
    }
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
///
/// For [`Config::set`] the message is the Redis 7.0 error text without the leading `ERR `
/// (`Unknown option or number of arguments for CONFIG SET - 'x'`, `CONFIG SET failed (possibly
/// related to argument 'x') - reason`), ready for `-ERR <message>`.
#[derive(Debug)]
pub struct ConfigError(pub String);

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

/// One parameter of §20.2: its name and whether CONFIG SET may change it (R).
struct Param {
    name: &'static str,
    runtime: bool,
}

const fn p(name: &'static str, runtime: bool) -> Param {
    Param { name, runtime }
}

/// Every parameter, in §20.2 order (CONFIG GET order).
const PARAMS: &[Param] = &[
    p("bind", false),
    p("port", false),
    p("unixsocket", false),
    p("tcp-backlog", false),
    p("tcp-keepalive", false),
    p("timeout", true),
    p("maxclients", true),
    p("requirepass", true),
    p("databases", false),
    p("threads", false),
    p("data-dir", false),
    p("data-size", false),
    p("auto-format", false),
    p("device", false),
    p("format-partitions", false),
    p("cache-size", true),
    p("durability", true),
    p("flush-method", false),
    p("device-cache", false),
    p("write-granularity", false),
    p("page-update", false),
    p("page-slack-cap", true),
    p("page-slack-atomic", true),
    p("max-storage-fill", true),
    p("snapshot-on-shutdown", true),
    p("snapshot-region", false),
    p("uring-fixed-buffers", false),
    p("max-pending-per-conn", true),
    p("client-output-buffer-limit", true),
    p("proto-max-bulk-len", true),
    p("max-dirty-bytes", true),
    p("large-value-memory", true),
    p("active-expire-reads-per-sec", true),
    p("scrub-iops", true),
    p("warm-iops", true),
    p("reclaim-iops", true),
    p("purge-iops", true),
    p("discard", true),
    p("discard-mbps", true),
    p("ag-release-per-sec", true),
    p("latency-controller", true),
    p("latency-target-p99-us", true),
    p("recovery-qd", false),
    p("clock-floor-interval-ms", false),
    p("hash-max-bytes", true),
    p("extent-cache-max", true),
    p("loglevel", true),
    p("logfile", false),
    p("core-dumps", false),
    p("metrics-port", false),
    p("cluster-enabled", false),
    p("cluster-control-port", false),
    p("cluster-repl-port", false),
    p("cluster-announce-ip", false),
    p("cluster-announce-port", false),
    p("cluster-auth-token", false),
    p("repl-backlog-size", false),
    p("repl-ack-timeout-ms", true),
    p("repl-heartbeat-ms", true),
    p("sync-mbps", true),
    p("sync-iops", true),
];

/// Keys CONFIG GET answers for client compatibility (constant values; not settable).
const COMPAT: &[(&str, &str)] =
    &[("save", ""), ("appendonly", "no"), ("maxmemory", "0"), ("maxmemory-policy", "noeviction")];

/// Nesting limit of `include` (guards against include cycles).
const MAX_INCLUDE_DEPTH: usize = 16;

fn param(name: &str) -> Option<&'static Param> {
    PARAMS.iter().find(|p| p.name == name)
}

/// Why one value was refused (rendered by the caller with the parameter name).
type ValueResult<T> = Result<T, String>;

fn parse_bool(v: &str) -> ValueResult<bool> {
    if v.eq_ignore_ascii_case("yes") {
        Ok(true)
    } else if v.eq_ignore_ascii_case("no") {
        Ok(false)
    } else {
        Err("argument must be 'yes' or 'no'".to_owned())
    }
}

/// Decimal integer without sign, spaces or suffix.
fn parse_uint(v: &str) -> ValueResult<u64> {
    if v.is_empty() || !v.bytes().all(|b| b.is_ascii_digit()) {
        return Err("argument couldn't be parsed into an integer".to_owned());
    }
    v.parse::<u64>().map_err(|_| "argument couldn't be parsed into an integer".to_owned())
}

fn parse_in<T: TryFrom<u64>>(v: &str, lo: u64, hi: u64) -> ValueResult<T> {
    let n = parse_uint(v)?;
    if n < lo || n > hi {
        return Err(format!("argument must be between {lo} and {hi} inclusive"));
    }
    T::try_from(n).map_err(|_| "argument out of range".to_owned())
}

/// Redis `memtoll`: bytes with an optional `k`/`kb`/`m`/`mb`/`g`/`gb` suffix (case-insensitive;
/// `k` = 1000, `kb` = 1024).
pub fn parse_size(v: &str) -> ValueResult<u64> {
    let lower = v.to_ascii_lowercase();
    let (num, mul) = [("kb", KIB), ("mb", MIB), ("gb", GIB), ("k", 1000), ("m", 1_000_000), ("g", 1_000_000_000)]
        .iter()
        .find_map(|&(suf, mul)| lower.strip_suffix(suf).map(|n| (n, mul)))
        .unwrap_or((lower.as_str(), 1));
    let n = parse_uint(num).map_err(|_| "argument must be a memory value".to_owned())?;
    n.checked_mul(mul).ok_or_else(|| "argument must be a memory value".to_owned())
}

/// `PATH[:SIZE]` as used by `kvd format --device`.
pub fn parse_device_spec(s: &str) -> ValueResult<(PathBuf, Option<u64>)> {
    match s.rsplit_once(':') {
        Some((path, size)) if !path.is_empty() && parse_size(size).is_ok() => {
            Ok((PathBuf::from(path), Some(parse_size(size)?)))
        }
        _ if s.is_empty() => Err("empty device path".to_owned()),
        _ => Ok((PathBuf::from(s), None)),
    }
}

fn render_bool(b: bool) -> String {
    if b { "yes" } else { "no" }.to_owned()
}

fn durability_name(d: Durability) -> &'static str {
    match d {
        Durability::Async => "async",
        Durability::Written => "written",
        Durability::Flushed => "flushed",
    }
}

fn render_region(r: SnapshotRegionSize) -> String {
    match r {
        SnapshotRegionSize::Bytes(b) => b.to_string(),
        SnapshotRegionSize::Percent(p) => format!("{p}%"),
    }
}

/// Splits one config line into tokens (`sdssplitargs` rules: blanks separate, `"..."` with
/// escapes, `'...'` with `\'`; a closing quote must be followed by a blank or the line end).
fn split_line(line: &str) -> Result<Vec<String>, String> {
    let b = line.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    loop {
        while i < b.len() && b[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= b.len() {
            return Ok(out);
        }
        let mut cur: Vec<u8> = Vec::new();
        let mut in_dq = false;
        let mut in_sq = false;
        loop {
            if in_dq {
                if i >= b.len() {
                    return Err("unbalanced quotes".to_owned());
                }
                if b[i] == b'\\' && i + 3 < b.len() && b[i + 1] == b'x' && b[i + 2].is_ascii_hexdigit() && b[i + 3].is_ascii_hexdigit() {
                    let hex = std::str::from_utf8(&b[i + 2..i + 4]).map_err(|e| e.to_string())?;
                    cur.push(u8::from_str_radix(hex, 16).map_err(|e| e.to_string())?);
                    i += 4;
                } else if b[i] == b'\\' && i + 1 < b.len() {
                    cur.push(match b[i + 1] {
                        b'n' => b'\n',
                        b'r' => b'\r',
                        b't' => b'\t',
                        b'b' => 0x08,
                        b'a' => 0x07,
                        c => c,
                    });
                    i += 2;
                } else if b[i] == b'"' {
                    if i + 1 < b.len() && !b[i + 1].is_ascii_whitespace() {
                        return Err("closing quote must be followed by a space".to_owned());
                    }
                    i += 1;
                    break;
                } else {
                    cur.push(b[i]);
                    i += 1;
                }
            } else if in_sq {
                if i >= b.len() {
                    return Err("unbalanced quotes".to_owned());
                }
                if b[i] == b'\\' && i + 1 < b.len() && b[i + 1] == b'\'' {
                    cur.push(b'\'');
                    i += 2;
                } else if b[i] == b'\'' {
                    if i + 1 < b.len() && !b[i + 1].is_ascii_whitespace() {
                        return Err("closing quote must be followed by a space".to_owned());
                    }
                    i += 1;
                    break;
                } else {
                    cur.push(b[i]);
                    i += 1;
                }
            } else {
                if i >= b.len() || b[i].is_ascii_whitespace() {
                    break;
                }
                match b[i] {
                    b'"' => in_dq = true,
                    b'\'' => in_sq = true,
                    c => cur.push(c),
                }
                i += 1;
            }
        }
        out.push(String::from_utf8(cur).map_err(|_| "configuration values must be UTF-8".to_owned())?);
    }
}

/// Where a setting came from (for one-line error messages).
enum Origin<'a> {
    File { path: &'a Path, line: usize },
    Cli,
}

impl Origin<'_> {
    fn describe(&self, name: &str) -> String {
        match self {
            Origin::File { path, line } => format!("{}:{line}: '{name}'", path.display()),
            Origin::Cli => format!("--{name}"),
        }
    }
}

impl Config {
    /// Applies the config file (with includes), then the command-line overrides, then validates.
    pub fn load(args: &ServeArgs) -> Result<Config, ConfigError> {
        let mut cfg = Config::default();
        if let Some(path) = &args.config_file {
            cfg.config_file = Some(std::fs::canonicalize(path).unwrap_or_else(|_| path.clone()));
            cfg.apply_file(path, 0)?;
        }
        for (name, values) in args.overrides() {
            let refs: Vec<&str> = values.iter().map(String::as_str).collect();
            cfg.apply(name, &refs).map_err(|e| ConfigError(format!("{}: {e}", Origin::Cli.describe(name))))?;
        }
        cfg.finish()?;
        Ok(cfg)
    }

    /// Applies the `name value ...` lines of `text` (as read from `path`).
    pub fn apply_text(&mut self, path: &Path, text: &str, depth: usize) -> Result<(), ConfigError> {
        for (i, raw) in text.lines().enumerate() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let origin = Origin::File { path, line: i + 1 };
            let toks = split_line(line).map_err(|e| ConfigError(format!("{}:{}: {e}", path.display(), i + 1)))?;
            let Some((name, values)) = toks.split_first() else { continue };
            let name = name.to_ascii_lowercase();
            if name == "include" {
                let [inc] = values else {
                    return Err(ConfigError(format!("{}: exactly one path expected", origin.describe("include"))));
                };
                self.apply_file(Path::new(inc), depth + 1)?;
                continue;
            }
            let refs: Vec<&str> = values.iter().map(String::as_str).collect();
            self.apply(&name, &refs).map_err(|e| ConfigError(format!("{}: {e}", origin.describe(&name))))?;
        }
        Ok(())
    }

    fn apply_file(&mut self, path: &Path, depth: usize) -> Result<(), ConfigError> {
        if depth > MAX_INCLUDE_DEPTH {
            return Err(ConfigError(format!("{}: include nesting deeper than {MAX_INCLUDE_DEPTH}", path.display())));
        }
        let text = std::fs::read_to_string(path)
            .map_err(|e| ConfigError(format!("cannot read config file {}: {e}", path.display())))?;
        self.apply_text(path, &text, depth)
    }

    /// Sets one parameter from its values (file line, CLI override or CONFIG SET).
    fn apply(&mut self, name: &str, v: &[&str]) -> ValueResult<()> {
        if param(name).is_none() {
            return Err("unknown configuration parameter".to_owned());
        }
        if name == "bind" {
            if v.is_empty() {
                return Err("at least one address expected".to_owned());
            }
            self.bind = v.iter().map(|s| (*s).to_owned()).collect();
            return Ok(());
        }
        if name == "device" {
            let dev = match v {
                [path] => (PathBuf::from(path), None),
                [path, size] => (PathBuf::from(path), Some(parse_size(size)?)),
                _ => return Err("expected 'device PATH [SIZE]'".to_owned()),
            };
            if dev.0.as_os_str().is_empty() {
                return Err("empty device path".to_owned());
            }
            self.device.push(dev);
            return Ok(());
        }
        let [s] = v else {
            return Err("wrong number of arguments".to_owned());
        };
        let s = *s;
        match name {
            "port" => self.port = parse_in(s, 0, 65535)?,
            "unixsocket" => self.unixsocket = s.to_owned(),
            "tcp-backlog" => self.tcp_backlog = parse_in(s, 1, i32::MAX as u64)?,
            "tcp-keepalive" => self.tcp_keepalive = parse_in(s, 0, i32::MAX as u64)?,
            "timeout" => self.timeout = parse_in(s, 0, i32::MAX as u64)?,
            "maxclients" => self.maxclients = parse_in(s, 1, u32::MAX as u64)?,
            "requirepass" => self.requirepass = s.to_owned(),
            "databases" => self.databases = parse_in(s, 1, 16)?,
            "threads" => {
                self.threads = if s.eq_ignore_ascii_case("auto") { None } else { Some(parse_in(s, 1, 1024)?) }
            }
            "data-dir" => {
                if s.is_empty() {
                    return Err("empty directory".to_owned());
                }
                self.data_dir = PathBuf::from(s);
            }
            "data-size" => {
                let n = parse_size(s)?;
                if n < 16 * MIB {
                    return Err("data-size must be at least 16mb".to_owned());
                }
                self.data_size = n;
            }
            "auto-format" => self.auto_format = parse_bool(s)?,
            "format-partitions" => {
                let n: u16 = parse_in(s, 1, 128)?;
                if !matches!(n, 16 | 32 | 64 | 128) {
                    return Err("argument must be 16, 32, 64 or 128".to_owned());
                }
                self.format_partitions = n;
            }
            "cache-size" => self.cache_size = if s.eq_ignore_ascii_case("auto") { None } else { Some(parse_size(s)?) },
            "durability" => {
                self.durability = Some(match s.to_ascii_lowercase().as_str() {
                    "async" => Durability::Async,
                    "written" => Durability::Written,
                    "flushed" => Durability::Flushed,
                    _ => return Err("argument must be one of async, written, flushed".to_owned()),
                })
            }
            "flush-method" => {
                self.flush_method = match s.to_ascii_lowercase().as_str() {
                    "auto" => FlushMethod::Auto,
                    "fdatasync" => FlushMethod::Fdatasync,
                    "dsync" => FlushMethod::Dsync,
                    "none" => FlushMethod::None,
                    _ => return Err("argument must be one of auto, fdatasync, dsync, none".to_owned()),
                }
            }
            "device-cache" => {
                self.device_cache = match s.to_ascii_lowercase().as_str() {
                    "auto" => DeviceCachePolicy::Auto,
                    "write-through" => DeviceCachePolicy::WriteThrough,
                    "write-back" => DeviceCachePolicy::WriteBack,
                    _ => return Err("argument must be one of auto, write-through, write-back".to_owned()),
                }
            }
            "write-granularity" => {
                self.write_granularity = match s.to_ascii_lowercase().as_str() {
                    "auto" => GranularityPolicy::Auto,
                    "page" => GranularityPolicy::Page,
                    "lba" => GranularityPolicy::Lba,
                    _ => return Err("argument must be one of auto, page, lba".to_owned()),
                }
            }
            "page-update" => {
                self.page_update = match s.to_ascii_lowercase().as_str() {
                    "auto" => PageUpdatePolicy::Auto,
                    "two-image" => PageUpdatePolicy::TwoImage,
                    "atomic-overwrite" => PageUpdatePolicy::AtomicOverwrite,
                    _ => return Err("argument must be one of auto, two-image, atomic-overwrite".to_owned()),
                }
            }
            "page-slack-cap" => self.page_slack_cap = parse_in(s, 0, 2048)?,
            "page-slack-atomic" => self.page_slack_atomic = parse_in(s, 0, 2048)?,
            "max-storage-fill" => {
                let f: f32 = s.parse().map_err(|_| "argument must be a fraction".to_owned())?;
                if !(f > 0.0 && f <= 1.0) {
                    return Err("argument must be in (0, 1]".to_owned());
                }
                self.max_storage_fill = f;
            }
            "snapshot-on-shutdown" => self.snapshot_on_shutdown = parse_bool(s)?,
            "snapshot-region" => {
                self.snapshot_region =
                    SnapshotRegionSize::parse(s).ok_or_else(|| "argument must be a size or a percentage below 50%".to_owned())?
            }
            "uring-fixed-buffers" => self.uring_fixed_buffers = parse_bool(s)?,
            "max-pending-per-conn" => self.max_pending_per_conn = parse_in(s, 1, u32::MAX as u64)?,
            "client-output-buffer-limit" => self.client_output_buffer_limit = parse_size(s)?,
            "proto-max-bulk-len" => {
                let n = parse_size(s)?;
                if !(1024 * 1024..=512 * MIB).contains(&n) {
                    return Err("argument must be between 1mb and 512mb".to_owned());
                }
                self.proto_max_bulk_len = n;
            }
            "max-dirty-bytes" => self.max_dirty_bytes = parse_size(s)?,
            "large-value-memory" => {
                let n = parse_size(s)?;
                if n > 2 * GIB {
                    return Err("argument must be at most 2gb".to_owned());
                }
                self.large_value_memory = n;
            }
            "active-expire-reads-per-sec" => self.active_expire_reads_per_sec = parse_uint(s)?,
            "scrub-iops" => self.scrub_iops = parse_uint(s)?,
            "warm-iops" => self.warm_iops = parse_uint(s)?,
            "reclaim-iops" => self.reclaim_iops = parse_uint(s)?,
            "purge-iops" => self.purge_iops = parse_uint(s)?,
            "discard" => self.discard = parse_bool(s)?,
            "discard-mbps" => self.discard_mbps = parse_uint(s)?,
            "ag-release-per-sec" => self.ag_release_per_sec = parse_uint(s)?,
            "latency-controller" => self.latency_controller = parse_bool(s)?,
            "latency-target-p99-us" => self.latency_target_p99_us = parse_in(s, 1, u32::MAX as u64)?,
            "recovery-qd" => self.recovery_qd = parse_in(s, 1, 4096)?,
            "clock-floor-interval-ms" => self.clock_floor_interval_ms = parse_in(s, 1, 3_600_000)?,
            "hash-max-bytes" => {
                let n = parse_size(s)?;
                if n == 0 || n > 512 * MIB {
                    return Err("argument must be between 1 and 512mb".to_owned());
                }
                self.hash_max_bytes = n;
            }
            "extent-cache-max" => self.extent_cache_max = parse_size(s)?,
            "loglevel" => {
                self.loglevel = match s.to_ascii_lowercase().as_str() {
                    "debug" => LogLevel::Debug,
                    "verbose" => LogLevel::Verbose,
                    "notice" => LogLevel::Notice,
                    "warning" => LogLevel::Warning,
                    _ => return Err("argument must be one of debug, verbose, notice, warning".to_owned()),
                }
            }
            "logfile" => self.logfile = s.to_owned(),
            "core-dumps" => self.core_dumps = parse_bool(s)?,
            "metrics-port" => self.metrics_port = parse_in(s, 0, 65535)?,
            "cluster-enabled" => self.cluster_enabled = parse_bool(s)?,
            "cluster-control-port" => self.cluster_control_port = Some(parse_in(s, 1, 65535)?),
            "cluster-repl-port" => self.cluster_repl_port = Some(parse_in(s, 1, 65535)?),
            "cluster-announce-ip" => self.cluster_announce_ip = s.to_owned(),
            "cluster-announce-port" => self.cluster_announce_port = parse_in(s, 0, 65535)?,
            "cluster-auth-token" => self.cluster_auth_token = s.to_owned(),
            "repl-backlog-size" => {
                let n = parse_size(s)?;
                if n < MIB {
                    return Err("argument must be at least 1mb".to_owned());
                }
                self.repl_backlog_size = n;
            }
            "repl-ack-timeout-ms" => self.repl_ack_timeout_ms = parse_in(s, 1, 3_600_000)?,
            "repl-heartbeat-ms" => self.repl_heartbeat_ms = parse_in(s, 1, 60_000)?,
            "sync-mbps" => self.sync_mbps = parse_in(s, 1, u32::MAX as u64)?,
            "sync-iops" => self.sync_iops = parse_in(s, 1, u32::MAX as u64)?,
            _ => return Err("unknown configuration parameter".to_owned()),
        }
        Ok(())
    }

    /// Cross-parameter rules and mode-dependent defaults, applied once after loading.
    fn finish(&mut self) -> Result<(), ConfigError> {
        if self.cluster_enabled {
            self.databases = 1;
        }
        self.validate()
    }

    /// Rules that must hold after every change.
    fn validate(&self) -> Result<(), ConfigError> {
        if self.cluster_enabled {
            if self.durability == Some(Durability::Async) {
                return Err(ConfigError("durability async is not allowed in cluster mode (use written or flushed)".to_owned()));
            }
            if self.cluster_auth_token.is_empty() {
                return Err(ConfigError("cluster-auth-token is required in cluster mode".to_owned()));
            }
            if self.port > 65535 - 20000 && (self.cluster_control_port.is_none() || self.cluster_repl_port.is_none()) {
                return Err(ConfigError("port + 20000 exceeds 65535: set cluster-control-port and cluster-repl-port".to_owned()));
            }
        }
        if self.port == 0 && self.unixsocket.is_empty() {
            return Err(ConfigError("port 0 disables TCP: a unixsocket is then required".to_owned()));
        }
        if self.bind.iter().any(|b| b.trim_start_matches('-').parse::<std::net::IpAddr>().is_err()) {
            return Err(ConfigError("bind: every address must be an IPv4 or IPv6 address".to_owned()));
        }
        Ok(())
    }

    /// CONFIG SET of one parameter (R parameters only).
    pub fn set(&mut self, name: &str, value: &str) -> Result<(), ConfigError> {
        let lname = name.to_ascii_lowercase();
        let Some(p) = param(&lname) else {
            return Err(ConfigError(format!("Unknown option or number of arguments for CONFIG SET - '{name}'")));
        };
        if !p.runtime {
            return Err(ConfigError(format!("CONFIG SET failed (possibly related to argument '{lname}') - can't set immutable config")));
        }
        let mut next = self.clone();
        next.apply(&lname, &[value])
            .and_then(|()| next.validate().map_err(|e| e.0))
            .map_err(|e| ConfigError(format!("CONFIG SET failed (possibly related to argument '{lname}') - {e}")))?;
        *self = next;
        Ok(())
    }

    /// CONFIG GET: `(name, value)` pairs matching a glob, including the compatibility keys.
    pub fn get(&self, pattern: &[u8]) -> Vec<(String, String)> {
        let matches = |name: &str| kv_common::glob::glob_match(pattern, name.as_bytes(), true);
        let mut out = Vec::new();
        for p in PARAMS {
            if matches(p.name) {
                out.push((p.name.to_owned(), self.value_of(p.name)));
            }
        }
        for &(k, v) in COMPAT {
            if matches(k) {
                out.push((k.to_owned(), v.to_owned()));
            }
        }
        out
    }

    /// The CONFIG GET rendering of one parameter (effective values: mode defaults resolved).
    fn value_of(&self, name: &str) -> String {
        match name {
            "bind" => self.bind.join(" "),
            "port" => self.port.to_string(),
            "unixsocket" => self.unixsocket.clone(),
            "tcp-backlog" => self.tcp_backlog.to_string(),
            "tcp-keepalive" => self.tcp_keepalive.to_string(),
            "timeout" => self.timeout.to_string(),
            "maxclients" => self.maxclients.to_string(),
            "requirepass" => self.requirepass.clone(),
            "databases" => self.databases.to_string(),
            "threads" => self.threads.map_or_else(|| "auto".to_owned(), |t| t.to_string()),
            "data-dir" => self.data_dir.display().to_string(),
            "data-size" => self.data_size.to_string(),
            "auto-format" => render_bool(self.auto_format),
            "device" => self
                .device
                .iter()
                .map(|(p, s)| match s {
                    Some(s) => format!("{}:{s}", p.display()),
                    None => p.display().to_string(),
                })
                .collect::<Vec<_>>()
                .join(" "),
            "format-partitions" => self.format_partitions.to_string(),
            "cache-size" => self.cache_size.map_or_else(|| "auto".to_owned(), |c| c.to_string()),
            "durability" => durability_name(self.effective_durability()).to_owned(),
            "flush-method" => match self.flush_method {
                FlushMethod::Auto => "auto",
                FlushMethod::Fdatasync => "fdatasync",
                FlushMethod::Dsync => "dsync",
                FlushMethod::None => "none",
            }
            .to_owned(),
            "device-cache" => match self.device_cache {
                DeviceCachePolicy::Auto => "auto",
                DeviceCachePolicy::WriteThrough => "write-through",
                DeviceCachePolicy::WriteBack => "write-back",
            }
            .to_owned(),
            "write-granularity" => match self.write_granularity {
                GranularityPolicy::Auto => "auto",
                GranularityPolicy::Page => "page",
                GranularityPolicy::Lba => "lba",
            }
            .to_owned(),
            "page-update" => match self.page_update {
                PageUpdatePolicy::Auto => "auto",
                PageUpdatePolicy::TwoImage => "two-image",
                PageUpdatePolicy::AtomicOverwrite => "atomic-overwrite",
            }
            .to_owned(),
            "page-slack-cap" => self.page_slack_cap.to_string(),
            "page-slack-atomic" => self.page_slack_atomic.to_string(),
            "max-storage-fill" => self.max_storage_fill.to_string(),
            "snapshot-on-shutdown" => render_bool(self.snapshot_on_shutdown),
            "snapshot-region" => render_region(self.snapshot_region),
            "uring-fixed-buffers" => render_bool(self.uring_fixed_buffers),
            "max-pending-per-conn" => self.max_pending_per_conn.to_string(),
            "client-output-buffer-limit" => self.client_output_buffer_limit.to_string(),
            "proto-max-bulk-len" => self.proto_max_bulk_len.to_string(),
            "max-dirty-bytes" => self.max_dirty_bytes.to_string(),
            "large-value-memory" => self.large_value_memory.to_string(),
            "active-expire-reads-per-sec" => self.active_expire_reads_per_sec.to_string(),
            "scrub-iops" => self.scrub_iops.to_string(),
            "warm-iops" => self.warm_iops.to_string(),
            "reclaim-iops" => self.reclaim_iops.to_string(),
            "purge-iops" => self.purge_iops.to_string(),
            "discard" => render_bool(self.discard),
            "discard-mbps" => self.discard_mbps.to_string(),
            "ag-release-per-sec" => self.ag_release_per_sec.to_string(),
            "latency-controller" => render_bool(self.latency_controller),
            "latency-target-p99-us" => self.latency_target_p99_us.to_string(),
            "recovery-qd" => self.recovery_qd.to_string(),
            "clock-floor-interval-ms" => self.clock_floor_interval_ms.to_string(),
            "hash-max-bytes" => self.hash_max_bytes.to_string(),
            "extent-cache-max" => self.extent_cache_max.to_string(),
            "loglevel" => self.loglevel.name().to_owned(),
            "logfile" => self.logfile.clone(),
            "core-dumps" => render_bool(self.core_dumps),
            "metrics-port" => self.metrics_port.to_string(),
            "cluster-enabled" => render_bool(self.cluster_enabled),
            "cluster-control-port" => self.control_port().to_string(),
            "cluster-repl-port" => self.repl_port().to_string(),
            "cluster-announce-ip" => self.cluster_announce_ip.clone(),
            "cluster-announce-port" => self.cluster_announce_port.to_string(),
            "cluster-auth-token" => self.cluster_auth_token.clone(),
            "repl-backlog-size" => self.repl_backlog_size.to_string(),
            "repl-ack-timeout-ms" => self.repl_ack_timeout_ms.to_string(),
            "repl-heartbeat-ms" => self.repl_heartbeat_ms.to_string(),
            "sync-mbps" => self.sync_mbps.to_string(),
            "sync-iops" => self.sync_iops.to_string(),
            _ => String::new(),
        }
    }

    /// `durability` with the mode default applied (written standalone, flushed cluster).
    pub fn effective_durability(&self) -> Durability {
        self.durability.unwrap_or(if self.cluster_enabled { Durability::Flushed } else { Durability::Written })
    }

    /// `cluster-control-port` with its default (port + 10000).
    pub fn control_port(&self) -> u16 {
        self.cluster_control_port.unwrap_or(self.port.saturating_add(10000))
    }

    /// `cluster-repl-port` with its default (port + 20000).
    pub fn repl_port(&self) -> u16 {
        self.cluster_repl_port.unwrap_or(self.port.saturating_add(20000))
    }

    /// Worker count: `threads`, or the online CPU count (`available_parallelism` only, LC-37),
    /// clamped to `[1, partitions]`.
    pub fn worker_count(&self, partitions: u16) -> u16 {
        let want = self.threads.unwrap_or_else(|| {
            std::thread::available_parallelism().map_or(1, |n| u16::try_from(n.get()).unwrap_or(u16::MAX))
        });
        want.clamp(1, partitions.max(1))
    }

    /// The data devices: the `device` entries, or `{data-dir}/kvd.data` of `data-size` bytes.
    /// The flag tells whether the files may be formatted on first start (`data-dir` only).
    pub fn data_devices(&self) -> (Vec<(PathBuf, Option<u64>)>, bool) {
        if self.device.is_empty() {
            (vec![(self.data_dir.join("kvd.data"), Some(self.data_size))], self.auto_format)
        } else {
            (self.device.clone(), false)
        }
    }

    /// Whether `name` is a known parameter.
    pub fn is_parameter(name: &str) -> bool {
        param(&name.to_ascii_lowercase()).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cli(args: &[&str]) -> Result<Config, ConfigError> {
        let mut argv = vec!["kvd"];
        argv.extend_from_slice(args);
        let c = Cli::try_parse_from(argv).map_err(|e| ConfigError(e.to_string()))?;
        Config::load(&c.serve)
    }

    #[test]
    fn defaults_match_the_table() {
        let c = cli(&[]).unwrap();
        assert_eq!(c.port, 6379);
        assert_eq!(c.bind, vec!["127.0.0.1"]);
        assert_eq!(c.tcp_backlog, 511);
        assert_eq!(c.tcp_keepalive, 300);
        assert_eq!(c.maxclients, 10000);
        assert_eq!(c.databases, 16);
        assert_eq!(c.data_size, 4 * GIB);
        assert_eq!(c.max_pending_per_conn, 4096);
        assert_eq!(c.client_output_buffer_limit, 64 * MIB);
        assert_eq!(c.proto_max_bulk_len, 512 * MIB);
        assert_eq!(c.large_value_memory, 2 * GIB);
        assert_eq!(c.effective_durability(), Durability::Written);
        assert_eq!(c.control_port(), 16379);
        assert_eq!(c.repl_port(), 26379);
        assert_eq!(c.snapshot_region, SnapshotRegionSize::Percent(3.0));
        let (devs, fmt) = c.data_devices();
        assert_eq!(devs, vec![(PathBuf::from("./kvd.data"), Some(4 * GIB))]);
        assert!(fmt);
    }

    #[test]
    fn compat_launch_template() {
        let c = cli(&["--port", "7777", "--data-dir", "/tmp/x"]).unwrap();
        assert_eq!(c.port, 7777);
        assert_eq!(c.data_devices().0[0].0, PathBuf::from("/tmp/x/kvd.data"));
    }

    #[test]
    fn sizes_follow_memtoll() {
        assert_eq!(parse_size("1k").unwrap(), 1000);
        assert_eq!(parse_size("1kb").unwrap(), 1024);
        assert_eq!(parse_size("4GB").unwrap(), 4 * GIB);
        assert_eq!(parse_size("64mb").unwrap(), 64 * MIB);
        assert_eq!(parse_size("12").unwrap(), 12);
        assert!(parse_size("-1").is_err());
        assert!(parse_size("1.5mb").is_err());
        assert!(parse_size("mb").is_err());
        assert!(parse_size("99999999999999999999gb").is_err());
    }

    #[test]
    fn file_then_cli_overrides_and_includes() {
        let dir = tempfile::tempdir().unwrap();
        let inc = dir.path().join("inc.conf");
        std::fs::write(&inc, "maxclients 77\n").unwrap();
        let main = dir.path().join("kvd.conf");
        std::fs::write(
            &main,
            format!(
                "# comment\nport 7000\n  bind 127.0.0.1 ::1\nrequirepass \"a b\\x41\"\ninclude {}\ndevice /d1 1gb\ndevice /d2\nloglevel WARNING\n",
                inc.display()
            ),
        )
        .unwrap();
        let mut argv = vec!["kvd".to_owned(), main.display().to_string()];
        argv.extend(["--port", "7001", "--device", "/d3:2mb"].map(str::to_owned));
        let c = Cli::try_parse_from(argv).unwrap();
        let c = Config::load(&c.serve).unwrap();
        assert_eq!(c.port, 7001);
        assert_eq!(c.bind, vec!["127.0.0.1", "::1"]);
        assert_eq!(c.requirepass, "a bA");
        assert_eq!(c.maxclients, 77);
        assert_eq!(c.loglevel, LogLevel::Warning);
        assert_eq!(
            c.device,
            vec![
                (PathBuf::from("/d1"), Some(GIB)),
                (PathBuf::from("/d2"), None),
                (PathBuf::from("/d3"), Some(2 * MIB))
            ]
        );
        assert!(!c.data_devices().1, "device entries are never auto-formatted");
    }

    #[test]
    fn errors_are_one_line_and_name_the_parameter() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("bad.conf");
        std::fs::write(&f, "port 7000\nsave \"\"\n").unwrap();
        let c = Cli::try_parse_from(["kvd".to_owned(), f.display().to_string()]).unwrap();
        let e = Config::load(&c.serve).err().unwrap().0;
        assert!(e.contains(":2: 'save'"), "{e}");
        assert!(!e.contains('\n'));
        assert!(cli(&["--port", "70000"]).is_err());
        assert!(cli(&["--databases", "17"]).is_err());
        assert!(cli(&["--format-partitions", "48"]).is_err());
        assert!(cli(&["--durability", "sometimes"]).is_err());
        assert!(cli(&["--bind", "localhost"]).is_err());
        assert!(cli(&["--threads", "0"]).is_err());
        assert!(cli(&["--no-such-option", "1"]).is_err());
    }

    #[test]
    fn include_cycles_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("loop.conf");
        std::fs::write(&f, format!("include {}\n", f.display())).unwrap();
        let c = Cli::try_parse_from(["kvd".to_owned(), f.display().to_string()]).unwrap();
        assert!(Config::load(&c.serve).err().unwrap().0.contains("nesting"));
    }

    #[test]
    fn cluster_mode_rules() {
        assert!(cli(&["--cluster-enabled", "yes"]).is_err(), "token required");
        let c = cli(&["--cluster-enabled", "yes", "--cluster-auth-token", "t"]).unwrap();
        assert_eq!(c.databases, 1);
        assert_eq!(c.effective_durability(), Durability::Flushed);
        assert!(cli(&["--cluster-enabled", "yes", "--cluster-auth-token", "t", "--durability", "async"]).is_err());
    }

    #[test]
    fn config_set_respects_runtime_flag() {
        let mut c = Config::default();
        c.set("maxclients", "5").unwrap();
        assert_eq!(c.maxclients, 5);
        c.set("LogLevel", "debug").unwrap();
        assert_eq!(c.loglevel, LogLevel::Debug);
        let e = c.set("port", "1").unwrap_err().0;
        assert_eq!(e, "CONFIG SET failed (possibly related to argument 'port') - can't set immutable config");
        let e = c.set("nope", "1").unwrap_err().0;
        assert_eq!(e, "Unknown option or number of arguments for CONFIG SET - 'nope'");
        let e = c.set("maxclients", "x").unwrap_err().0;
        assert!(e.starts_with("CONFIG SET failed (possibly related to argument 'maxclients') - "), "{e}");
        assert_eq!(c.maxclients, 5, "a refused value changes nothing");
    }

    #[test]
    fn config_get_globs_and_compat_keys() {
        let c = Config::default();
        let all = c.get(b"*");
        assert_eq!(all.len(), PARAMS.len() + COMPAT.len());
        assert_eq!(c.get(b"maxmemory"), vec![("maxmemory".to_owned(), "0".to_owned())]);
        assert_eq!(c.get(b"save"), vec![("save".to_owned(), String::new())]);
        assert_eq!(c.get(b"APPENDONLY"), vec![("appendonly".to_owned(), "no".to_owned())]);
        let dbs = c.get(b"databases");
        assert_eq!(dbs, vec![("databases".to_owned(), "16".to_owned())]);
        let ce = c.get(b"cluster-enabled");
        assert_eq!(ce, vec![("cluster-enabled".to_owned(), "no".to_owned())]);
        assert!(c.get(b"no-such-*").is_empty());
        let ports: Vec<_> = c.get(b"*port").into_iter().map(|(k, _)| k).collect();
        assert!(ports.contains(&"port".to_owned()) && ports.contains(&"metrics-port".to_owned()));
        assert_eq!(c.get(b"durability")[0].1, "written");
        assert_eq!(c.get(b"cache-size")[0].1, "auto");
    }

    #[test]
    fn every_parameter_renders_and_parses_back() {
        let c = Config::default();
        for p in PARAMS {
            let v = c.value_of(p.name);
            if matches!(p.name, "device" | "unixsocket" | "requirepass" | "logfile" | "cluster-announce-ip" | "cluster-auth-token") {
                continue;
            }
            let mut d = Config::default();
            let vals: Vec<&str> = if p.name == "bind" { v.split(' ').collect() } else { vec![v.as_str()] };
            d.apply(p.name, &vals).unwrap_or_else(|e| panic!("{}: {v:?}: {e}", p.name));
            assert_eq!(d.value_of(p.name), v, "{}", p.name);
        }
    }

    #[test]
    fn worker_count_is_clamped_to_partitions() {
        let mut c = Config { threads: Some(200), ..Config::default() };
        assert_eq!(c.worker_count(64), 64);
        c.threads = None;
        let n = c.worker_count(128);
        assert!((1..=128).contains(&n));
        assert_eq!(c.worker_count(1), 1);
    }

    #[test]
    fn line_splitting_follows_sdssplitargs() {
        assert_eq!(split_line("a  b\tc").unwrap(), vec!["a", "b", "c"]);
        assert_eq!(split_line(r#"x "a\"b" 'c\'d'"#).unwrap(), vec!["x", "a\"b", "c'd"]);
        assert_eq!(split_line(r#"x """#).unwrap(), vec!["x", ""]);
        assert!(split_line(r#"x "abc"#).is_err());
        assert!(split_line(r#"x "a"b"#).is_err());
    }

    #[test]
    fn device_specs() {
        assert_eq!(parse_device_spec("/a/b:1gb").unwrap(), (PathBuf::from("/a/b"), Some(GIB)));
        assert_eq!(parse_device_spec("/a/b").unwrap(), (PathBuf::from("/a/b"), None));
        assert_eq!(parse_device_spec("/a:b/c").unwrap(), (PathBuf::from("/a:b/c"), None));
        assert!(parse_device_spec("").is_err());
    }
}
