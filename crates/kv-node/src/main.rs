//! `kvd`: the kvssd data node (DESIGN §4.2, §6, §18.1, §20, §24.11).
//!
//! Threads (§6.1): `threads` pinned io_uring workers, each owning its client connections, the
//! partitions `{p : p % W == w}`, a page cache and one flush tracker per device; one meta thread
//! for fixed-location metadata; one agent thread (tokio current-thread) for the coordinator
//! control channel, leases and `/metrics`. Startup (§18.1): probe and open devices, start workers
//! and listeners at once (commands needing a partition reply `-LOADING` until it is Ready),
//! recover each partition (snapshot or full scan), write the startup superblock before any data
//! write.
//!
//! `unsafe` is allowed only in `runtime/ring.rs` (§24).
#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(unsafe_code)]
// Parts of the §24.11 interface (cluster mode, shared reply bodies, control events) are wired
// only from M2 on; the node cluster agent modules are still stubs.
#![allow(dead_code)]

#[allow(unused_variables)]
mod cluster;
mod cmd;
mod config;
mod dispatch;
mod log;
mod metrics;
mod net;
mod runtime;
mod shared;

use clap::Parser;

use crate::config::{CheckArgs, Cli, Command, Config, FormatArgs, InspectArgs};

/// Whether `cluster::agent` and `cluster::ownership` are implemented (M2); until then cluster
/// mode is refused at startup with a one-line reason (§22).
const CLUSTER_AGENT_AVAILABLE: bool = false;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    let cli = match config::Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) => {
            use clap::error::ErrorKind;
            if matches!(e.kind(), ErrorKind::DisplayHelp | ErrorKind::DisplayVersion) {
                e.exit();
            }
            // Bad configuration: exit code 1 with a one-line reason before listening (§22).
            let _ = e.print();
            std::process::exit(1);
        }
    };
    std::process::exit(run(cli));
}

fn run(cli: Cli) -> i32 {
    match cli.command {
        Some(Command::Format(a)) => format_cmd(&a),
        Some(Command::Inspect(a)) => inspect_cmd(&a),
        Some(Command::Check(a)) => check_cmd(&a),
        None => serve(&cli.serve),
    }
}

/// `kvd [CONFIG_FILE] [--name value ...]`.
fn serve(args: &config::ServeArgs) -> i32 {
    let cfg = match Config::load(args) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("kvd: configuration error: {e}");
            return 1;
        }
    };
    if let Err(e) = log::init(cfg.loglevel, &cfg.logfile) {
        eprintln!("kvd: cannot open the log file {}: {e}", cfg.logfile);
        return 1;
    }
    if let Err(e) = log::set_core_dumps(cfg.core_dumps) {
        eprintln!("kvd: cannot set the core dump policy: {e}");
        return 1;
    }
    let cluster = cfg.cluster_enabled;
    if cluster && !CLUSTER_AGENT_AVAILABLE {
        // The node cluster agent and the ownership gate (src/cluster/*) are M2 work that this
        // build does not contain yet: refuse cluster mode instead of starting half of it.
        eprintln!("kvd: cluster-enabled yes needs the node cluster agent (M2), which this build does not include");
        return 1;
    }
    let node = match runtime::node::start(cfg) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("kvd: {e}");
            return 1;
        }
    };
    if cluster {
        // the node cluster agent (§23.4): coordinator control channel and leases
        let shared = node.shared.clone();
        let spawned = std::thread::Builder::new().name("kvd-agent".to_owned()).spawn(move || {
            if let Err(e) = cluster::agent::run(shared.clone()) {
                tracing::error!("cluster agent stopped: {e}");
                shared.request_shutdown();
            }
        });
        if let Err(e) = spawned {
            eprintln!("kvd: cannot start the cluster agent: {e}");
            node.shared.request_shutdown();
        }
    }
    node.wait()
}

/// `kvd format --device PATH[:SIZE] ... [--partitions P] [--cluster] [--snapshot-region R]
/// [--force]`.
fn format_cmd(a: &FormatArgs) -> i32 {
    let mut devices = Vec::with_capacity(a.devices.len());
    for d in &a.devices {
        match config::parse_device_spec(d) {
            Ok(x) => devices.push(x),
            Err(e) => {
                eprintln!("kvd format: --device {d}: {e}");
                return 1;
            }
        }
    }
    let snapshot_region = match &a.snapshot_region {
        None => kv_storage::format::SnapshotRegionSize::default(),
        Some(s) => match kv_storage::format::SnapshotRegionSize::parse(s) {
            Some(r) => r,
            None => {
                eprintln!("kvd format: --snapshot-region {s}: a size or a percentage below 50% expected");
                return 1;
            }
        },
    };
    if !a.force {
        for (path, _) in &devices {
            if let Ok(Some(_)) = runtime::node::read_superblock(path) {
                eprintln!("kvd format: {} already holds a kvssd format (use --force to overwrite)", path.display());
                return 1;
            }
        }
    }
    let opts = kv_storage::format::FormatOpts { devices, partitions: a.partitions, cluster: a.cluster, snapshot_region };
    match kv_storage::format::format(&opts) {
        Ok(()) => {
            println!("kvd format: {} device(s) formatted with {} partitions", opts.devices.len(), opts.partitions);
            0
        }
        Err(e) => {
            eprintln!("kvd format: {e}");
            1
        }
    }
}

/// `kvd inspect --device PATH`: the superblock (secrets and record bytes are never printed).
fn inspect_cmd(a: &InspectArgs) -> i32 {
    if a.ag.is_some() || a.page.is_some() || a.show_data {
        eprintln!("kvd inspect: --ag, --page and --show-data need the storage inspection tools (M1.3)");
        return 1;
    }
    match runtime::node::read_superblock(&a.device) {
        Ok(Some(sb)) => {
            println!("device          {}", a.device.display());
            println!("device_index    {} of {}", sb.device_index, sb.device_count);
            println!("partitions      {}", sb.partitions);
            println!("flags           {:#06x}", sb.flags);
            println!("lba             {}", sb.lba);
            println!("device_pages    {}", sb.device_pages);
            println!("n_ags           {}", sb.n_ags);
            println!("boot_gen        {}", sb.boot_gen);
            println!("databases       {}", sb.databases);
            println!("snap_region     {} ({} pages per region)", sb.snap_region, sb.snap_region_pages);
            0
        }
        Ok(None) => {
            eprintln!("kvd inspect: {} holds no valid superblock", a.device.display());
            1
        }
        Err(e) => {
            eprintln!("kvd inspect: {}: {e}", a.device.display());
            1
        }
    }
}

/// `kvd check --device PATH...`.
fn check_cmd(a: &CheckArgs) -> i32 {
    let _ = (&a.devices, a.placement_report);
    eprintln!("kvd check: the offline full scan needs the storage engine's verification pass (M1.3)");
    1
}
