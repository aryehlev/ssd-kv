//! `kvadm`: the kvssd operator CLI (DESIGN §23.12, §24.12).
//!
//! Sends RESP `KVADM <verb> ...` commands (after `AUTH <admin-token>`) to a coordinator's admin
//! port and prints the reply; a follower relays to the leader. It never talks to data nodes and
//! never touches storage (LC-37). `coord setpeers` changes one coordinator at a time and is sent
//! to each coordinator in turn; the next change is refused until every listed coordinator reports
//! the new generation (§23.3).
#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(unsafe_code)]
#![allow(dead_code, unused_variables)]

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(name = "kvadm", version, about = "kvssd operator CLI (talks to a coordinator's admin port)")]
struct Cli {
    /// Coordinator admin address(es), `host:port`; the first reachable one is used.
    #[arg(long = "coord", value_name = "HOST:PORT", global = true)]
    coords: Vec<String>,
    /// Admin token.
    #[arg(long, value_name = "TOKEN", global = true)]
    token: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Cluster parameters.
    #[command(subcommand)]
    Cluster(ClusterCmd),
    /// Data node registration and state.
    #[command(subcommand)]
    Node(NodeCmd),
    /// Shards.
    #[command(subcommand)]
    Shard(ShardCmd),
    /// Slot assignment.
    #[command(subcommand)]
    Slots(SlotsCmd),
    /// Shard members.
    #[command(subcommand)]
    Replica(ReplicaCmd),
    /// Graceful primary handoff.
    Failover {
        shard: u16,
        /// Target member; without it the static priority rule picks one.
        #[arg(long, value_name = "NODE")]
        to: Option<String>,
    },
    /// Operator-initiated slot migration.
    Migrate(MigrateArgs),
    /// List migrations.
    Migrations,
    /// Slot-count rebalancing plans.
    #[command(subcommand)]
    Rebalance(RebalanceCmd),
    /// Coordinator peer lists and disaster recovery exports.
    #[command(subcommand)]
    Coord(CoordCmd),
    /// Read-only cluster summary (shards, ISR, member states, migrations).
    Status {
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum ClusterCmd {
    /// `cluster params set <k> <v>`
    #[command(subcommand)]
    Params(ParamsCmd),
}

#[derive(Subcommand)]
enum ParamsCmd {
    Set { key: String, value: String },
}

#[derive(Subcommand)]
enum NodeCmd {
    /// Register a formatted node by its control address.
    Add {
        ctrl_addr: String,
        #[arg(long, value_name = "IP:PORT")]
        client: String,
        #[arg(long, value_name = "IP:PORT")]
        repl: String,
        #[arg(long)]
        zone: String,
        /// Static failover preference (higher first).
        #[arg(long, default_value_t = 0)]
        priority: u8,
    },
    Remove {
        id: String,
        #[arg(long)]
        force: bool,
    },
    Maintenance {
        id: String,
        #[arg(value_parser = ["on", "off"])]
        state: String,
    },
    ClearFail {
        id: String,
    },
}

#[derive(Subcommand)]
enum ShardCmd {
    Create {
        shard: u16,
        #[arg(long, value_name = "NODE")]
        primary: String,
        #[arg(long, value_name = "N")]
        min_isr: Option<u8>,
    },
    Delete {
        shard: u16,
    },
}

#[derive(Subcommand)]
enum SlotsCmd {
    /// Assign unassigned slots, e.g. `0-5460,6000`.
    Assign { ranges: String, shard: u16 },
}

#[derive(Subcommand)]
enum ReplicaCmd {
    /// Add a node with no role as a learner of the shard (full sync, then ISR).
    Add { shard: u16, node: String },
    Remove { shard: u16, node: String },
    /// Merge resync of a member that needs a full sync or reports corrupt pages.
    Resync { node: String },
}

#[derive(Args)]
#[command(args_conflicts_with_subcommands = true)]
struct MigrateArgs {
    #[command(subcommand)]
    control: Option<MigrateControl>,
    /// Slot ranges owned by one source shard, e.g. `100-199`.
    ranges: Option<String>,
    #[arg(long, value_name = "SHARD")]
    to: Option<u16>,
    /// Static token-bucket rate in MB/s.
    #[arg(long, value_name = "MBPS")]
    rate: Option<u32>,
    /// Further position barriers before cutover.
    #[arg(long, value_name = "K")]
    barriers: Option<u8>,
}

#[derive(Subcommand)]
enum MigrateControl {
    Pause { id: u64 },
    Resume { id: u64 },
    Abort { id: u64 },
}

#[derive(Subcommand)]
enum RebalanceCmd {
    /// Compute a deterministic plan from the committed state.
    Plan {
        #[arg(long, default_value = "slots", value_parser = ["slots"])]
        by: String,
    },
    /// Run a plan one migration at a time; stops at the first failover or health change.
    Apply { plan: PathBuf },
}

#[derive(Subcommand)]
enum CoordCmd {
    /// Set generation `gen` of the peer list (exactly one coordinator added or removed).
    Setpeers { r#gen: u64, list: String },
    /// Add one coordinator (runs the setpeers sequence).
    Add { peer: String },
    /// Remove one coordinator (runs the setpeers sequence).
    Remove { id: String },
    /// Show each coordinator's peer list generation.
    Peers,
    /// Write the committed state to a file (or alternate `<file>.a`/`<file>.b`).
    Export { file: PathBuf },
}

fn main() {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) => {
            use clap::error::ErrorKind;
            if matches!(e.kind(), ErrorKind::DisplayHelp | ErrorKind::DisplayVersion) {
                e.exit();
            }
            let _ = e.print();
            std::process::exit(1);
        }
    };
    let _ = cli;
    eprintln!("kvadm: not yet implemented");
    std::process::exit(2);
}
