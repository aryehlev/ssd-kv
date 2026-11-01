//! `kvadm`: the kvssd operator CLI (DESIGN §23.12, §24.12).
//!
//! Sends RESP `KVADM <verb> ...` commands (after `AUTH <admin-token>`) to a coordinator's admin
//! port and prints the reply; a follower relays to the leader. It never talks to data nodes and
//! never touches storage (LC-37). `coord setpeers` changes one coordinator at a time and is sent
//! to each coordinator in turn; the next change is refused until every listed coordinator reports
//! the new generation (§23.3).
#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(unsafe_code)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use clap::{Args, Parser, Subcommand};
use kv_resp::Value;

/// `println!` that ignores a closed stdout (e.g. `kvadm status | head`).
macro_rules! outln {
    ($($t:tt)*) => {{
        let _ = writeln!(std::io::stdout(), $($t)*);
    }};
}

/// `print!` that ignores a closed stdout.
macro_rules! out {
    ($($t:tt)*) => {{
        let _ = write!(std::io::stdout(), $($t)*);
    }};
}

#[derive(Parser)]
#[command(name = "kvadm", version, about = "kvssd operator CLI (talks to a coordinator's admin port)")]
struct Cli {
    /// Coordinator admin address(es), `host:port`; the first reachable one is used.
    #[arg(long = "coord", value_name = "HOST:PORT", global = true)]
    coords: Vec<String>,
    /// Admin token (default: the KVADM_TOKEN environment variable).
    #[arg(long, value_name = "TOKEN", global = true)]
    token: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// First cluster parameters (`key value` pairs), before any node is registered.
    Init { params: Vec<String> },
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
    Get,
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
        #[arg(long, default_value = "")]
        zone: String,
        /// Static failover preference (higher first).
        #[arg(long, default_value_t = 0)]
        priority: u8,
        /// Register without the handshake (the node is formatted with this partition count).
        #[arg(long, value_name = "P")]
        partitions: Option<u16>,
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
    /// Progress of the plan being applied.
    Status,
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
    Export {
        file: PathBuf,
        /// Overwrite the older of `<file>.a` and `<file>.b` instead of `<file>` itself.
        #[arg(long)]
        ab: bool,
    },
}

/// One RESP connection to a coordinator's admin port, authenticated.
struct Admin {
    s: TcpStream,
    buf: Vec<u8>,
}

impl Admin {
    fn connect(addr: SocketAddr, token: &str) -> Result<Self, String> {
        let s = TcpStream::connect_timeout(&addr, Duration::from_secs(2)).map_err(|e| format!("{addr}: {e}"))?;
        s.set_read_timeout(Some(Duration::from_secs(120))).map_err(|e| e.to_string())?;
        let _ = s.set_nodelay(true);
        let mut a = Admin { s, buf: Vec::new() };
        match a.call(&["AUTH", token])? {
            Value::Simple(_) => Ok(a),
            Value::Error(e) => Err(format!("{addr}: {}", String::from_utf8_lossy(&e))),
            _ => Err(format!("{addr}: unexpected AUTH reply")),
        }
    }

    fn call(&mut self, args: &[&str]) -> Result<Value, String> {
        let a: Vec<&[u8]> = args.iter().map(|x| x.as_bytes()).collect();
        let mut out = Vec::new();
        kv_resp::encode_command(&a, &mut out);
        self.s.write_all(&out).map_err(|e| e.to_string())?;
        loop {
            if let Some((v, n)) = kv_resp::decode_value(&self.buf).map_err(|_| "malformed reply".to_owned())? {
                self.buf.drain(..n);
                return Ok(v);
            }
            let mut chunk = [0u8; 64 * 1024];
            let n = self.s.read(&mut chunk).map_err(|e| e.to_string())?;
            if n == 0 {
                return Err("connection closed by the coordinator".into());
            }
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }

    /// `KVADM <words...>`
    fn kvadm(&mut self, words: &[String]) -> Result<Value, String> {
        let mut args: Vec<&str> = vec!["KVADM"];
        args.extend(words.iter().map(String::as_str));
        self.call(&args)
    }
}

fn resolve(s: &str) -> Result<SocketAddr, String> {
    s.to_socket_addrs()
        .map_err(|e| format!("cannot resolve '{s}': {e}"))?
        .next()
        .ok_or_else(|| format!("'{s}' resolves to no address"))
}

/// The first reachable coordinator of `coords`.
fn connect_any(coords: &[String], token: &str) -> Result<Admin, String> {
    let mut last = String::from("no coordinator given");
    for c in coords {
        match resolve(c).and_then(|a| Admin::connect(a, token)) {
            Ok(a) => return Ok(a),
            Err(e) => last = e,
        }
    }
    Err(last)
}

/// Prints a reply; `Err` for an error reply.
fn print(v: &Value) -> Result<(), String> {
    match v {
        Value::Error(e) => Err(String::from_utf8_lossy(e).into_owned()),
        Value::Simple(s) => {
            outln!("{}", String::from_utf8_lossy(s));
            Ok(())
        }
        Value::Bulk(Some(b)) => {
            let t = String::from_utf8_lossy(b);
            out!("{t}");
            if !t.ends_with('\n') {
                outln!();
            }
            Ok(())
        }
        Value::Bulk(None) | Value::Null => {
            outln!("(nil)");
            Ok(())
        }
        Value::Int(i) => {
            outln!("{i}");
            Ok(())
        }
        Value::Array(Some(items)) => items.iter().try_for_each(print),
        other => {
            outln!("{other:?}");
            Ok(())
        }
    }
}

fn text(v: Value) -> Result<String, String> {
    match v {
        Value::Bulk(Some(b)) | Value::Simple(b) => Ok(String::from_utf8_lossy(&b).into_owned()),
        Value::Error(e) => Err(String::from_utf8_lossy(&e).into_owned()),
        _ => Err("unexpected reply".into()),
    }
}

/// One peer-list entry `id=host:peer/client/admin` (the canonical form `coord peers` prints).
struct Entry {
    id: String,
    admin: SocketAddr,
    raw: String,
}

fn parse_entry(e: &str) -> Result<Entry, String> {
    let (id, rest) = e
        .split_once('=')
        .ok_or_else(|| format!("peer entry '{e}' must be ID=HOST:PEER/CLIENT/ADMIN"))?;
    let parts: Vec<&str> = rest.split('/').collect();
    let [peer, _client, admin] = parts.as_slice() else {
        return Err(format!("peer entry '{e}' needs HOST:PEER/CLIENT/ADMIN"));
    };
    let peer = resolve(peer)?;
    let admin: u16 = admin.parse().map_err(|_| format!("invalid admin port in '{e}'"))?;
    Ok(Entry {
        id: id.to_owned(),
        admin: SocketAddr::new(peer.ip(), admin),
        raw: e.to_owned(),
    })
}

fn parse_list(list: &str) -> Result<Vec<Entry>, String> {
    list.split(',')
        .map(str::trim)
        .filter(|x| !x.is_empty())
        .map(parse_entry)
        .collect()
}

/// `(generation, list)` as one coordinator reports it.
fn peers_of(addr: SocketAddr, token: &str) -> Result<(u64, String), String> {
    let mut a = Admin::connect(addr, token)?;
    let t = text(a.kvadm(&["COORD".into(), "PEERS".into()])?)?;
    let (g, list) = t.trim().split_once(' ').unwrap_or((t.trim(), ""));
    let g = g.parse().map_err(|_| format!("{addr}: malformed peer list reply"))?;
    Ok((g, list.to_owned()))
}

/// The SETPEERS sequence (§23.3). Before anything is sent: every coordinator of the current
/// list reports the current generation (one that leaves the list may be unreachable: removing a
/// dead coordinator is the usual reason for a removal), and every coordinator of the new list
/// answers (a coordinator being added reports the joining generation 0 or the current one). The
/// new list then goes to each reachable coordinator of both lists in turn, and every
/// coordinator of the new list must report the new generation.
fn setpeers(coords: &[String], token: &str, new_gen: Option<u64>, make: &dyn Fn(&[Entry]) -> Result<String, String>) -> Result<(), String> {
    let mut a = connect_any(coords, token)?;
    let t = text(a.kvadm(&["COORD".into(), "PEERS".into()])?)?;
    let (g, cur) = t.trim().split_once(' ').unwrap_or((t.trim(), ""));
    let g: u64 = g.parse().map_err(|_| "malformed peer list reply".to_owned())?;
    let target = new_gen.unwrap_or(g + 1);
    if target != g + 1 {
        return Err(format!("the next generation is {}, not {target}", g + 1));
    }
    let cur_list = parse_list(cur)?;
    let new = make(&cur_list)?;
    let new_list = parse_list(&new)?;
    let stays = |id: &str| new_list.iter().any(|x| x.id == id);
    let mut reachable: Vec<&Entry> = Vec::new();
    for e in &cur_list {
        match peers_of(e.admin, token) {
            Ok((eg, _)) if eg == g => reachable.push(e),
            Ok((eg, _)) => {
                return Err(format!(
                    "coordinator {} reports peer generation {eg}, not {g}: finish the previous change first",
                    e.id
                ));
            }
            Err(err) if stays(&e.id) => return Err(format!("coordinator {}: {err}", e.id)),
            Err(err) => outln!("coordinator {} (leaving the list) is unreachable: {err}", e.id),
        }
    }
    for e in new_list.iter().filter(|e| !cur_list.iter().any(|x| x.id == e.id)) {
        match peers_of(e.admin, token) {
            Ok((0, _)) => reachable.push(e),
            Ok((eg, _)) if eg == g => reachable.push(e),
            Ok((eg, _)) => {
                return Err(format!("coordinator {} to add reports peer generation {eg}", e.id));
            }
            Err(err) => return Err(format!("coordinator {} to add: {err}", e.id)),
        }
    }
    for e in &reachable {
        let mut c = Admin::connect(e.admin, token)?;
        let r = c.kvadm(&["COORD".into(), "SETPEERS".into(), target.to_string(), new.clone()])?;
        if let Value::Error(m) = r {
            return Err(format!("coordinator {}: {}", e.id, String::from_utf8_lossy(&m)));
        }
        outln!("coordinator {}: peer generation {target}", e.id);
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    for e in &new_list {
        loop {
            match peers_of(e.admin, token) {
                Ok((eg, _)) if eg == target => break,
                _ if Instant::now() > deadline => {
                    return Err(format!("coordinator {} does not report generation {target}", e.id));
                }
                _ => std::thread::sleep(Duration::from_millis(200)),
            }
        }
    }
    outln!("peer generation {target} in force: {new}");
    Ok(())
}

/// Writes an export over `file`, or over the older (or invalid) of `file.a` / `file.b`.
fn write_export(file: &Path, ab: bool, bytes: &[u8]) -> Result<PathBuf, String> {
    let path = if ab {
        let a = file.with_extension(append_ext(file, "a"));
        let b = file.with_extension(append_ext(file, "b"));
        // An export starts with an 8-byte magic and the version (u64 LE); keep the newer one.
        let version = |p: &Path| -> Option<u64> {
            let d = std::fs::read(p).ok()?;
            (d.len() >= 16).then(|| u64::from_le_bytes(d[8..16].try_into().unwrap_or([0; 8])))
        };
        match (version(&a), version(&b)) {
            (Some(va), Some(vb)) if va <= vb => a,
            (Some(_), Some(_)) => b,
            (None, _) => a,
            (Some(_), None) => b,
        }
    } else {
        file.to_path_buf()
    };
    let mut f = std::fs::File::create(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    f.write_all(bytes).map_err(|e| e.to_string())?;
    f.sync_all().map_err(|e| e.to_string())?;
    Ok(path)
}

fn append_ext(file: &Path, x: &str) -> String {
    match file.extension() {
        Some(e) => format!("{}.{x}", e.to_string_lossy()),
        None => x.to_owned(),
    }
}

/// The `KVADM` words of a command that is a single request.
fn words(c: &Command) -> Option<Vec<String>> {
    let w = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<String>>();
    let mut out = match c {
        Command::Init { params } => {
            let mut v = w(&["INIT"]);
            v.extend(params.iter().cloned());
            return Some(v);
        }
        Command::Cluster(ClusterCmd::Params(ParamsCmd::Set { key, value })) => {
            w(&["PARAMS", "SET", key, value])
        }
        Command::Cluster(ClusterCmd::Params(ParamsCmd::Get)) => w(&["PARAMS", "GET"]),
        Command::Node(n) => match n {
            NodeCmd::Add {
                ctrl_addr,
                client,
                repl,
                zone,
                priority,
                partitions,
            } => {
                let mut v = w(&["NODE", "ADD", ctrl_addr, "--client", client, "--repl", repl]);
                if !zone.is_empty() {
                    v.extend(w(&["--zone", zone]));
                }
                v.extend(w(&["--priority", &priority.to_string()]));
                if let Some(p) = partitions {
                    v.extend(w(&["--partitions", &p.to_string()]));
                }
                v
            }
            NodeCmd::Remove { id, force } => {
                let mut v = w(&["NODE", "REMOVE", id]);
                if *force {
                    v.push("--force".into());
                }
                v
            }
            NodeCmd::Maintenance { id, state } => w(&["NODE", "MAINTENANCE", id, state]),
            NodeCmd::ClearFail { id } => w(&["NODE", "CLEAR-FAIL", id]),
        },
        Command::Shard(ShardCmd::Create { shard, primary, min_isr }) => {
            let mut v = w(&["SHARD", "CREATE", &shard.to_string(), "--primary", primary]);
            if let Some(m) = min_isr {
                v.extend(w(&["--min-isr", &m.to_string()]));
            }
            v
        }
        Command::Shard(ShardCmd::Delete { shard }) => w(&["SHARD", "DELETE", &shard.to_string()]),
        Command::Slots(SlotsCmd::Assign { ranges, shard }) => {
            w(&["SLOTS", "ASSIGN", ranges, &shard.to_string()])
        }
        Command::Replica(r) => match r {
            ReplicaCmd::Add { shard, node } => w(&["REPLICA", "ADD", &shard.to_string(), node]),
            ReplicaCmd::Remove { shard, node } => {
                w(&["REPLICA", "REMOVE", &shard.to_string(), node])
            }
            ReplicaCmd::Resync { node } => w(&["REPLICA", "RESYNC", node]),
        },
        Command::Failover { shard, to } => {
            let mut v = w(&["FAILOVER", &shard.to_string()]);
            if let Some(t) = to {
                v.extend(w(&["--to", t]));
            }
            v
        }
        Command::Migrate(m) => match &m.control {
            Some(MigrateControl::Pause { id }) => w(&["MIGRATE", "PAUSE", &id.to_string()]),
            Some(MigrateControl::Resume { id }) => w(&["MIGRATE", "RESUME", &id.to_string()]),
            Some(MigrateControl::Abort { id }) => w(&["MIGRATE", "ABORT", &id.to_string()]),
            None => {
                let mut v = w(&["MIGRATE", m.ranges.as_deref().unwrap_or("")]);
                if let Some(t) = m.to {
                    v.extend(w(&["--to", &t.to_string()]));
                }
                if let Some(r) = m.rate {
                    v.extend(w(&["--rate", &r.to_string()]));
                }
                if let Some(b) = m.barriers {
                    v.extend(w(&["--barriers", &b.to_string()]));
                }
                v
            }
        },
        Command::Migrations => w(&["MIGRATIONS"]),
        Command::Rebalance(RebalanceCmd::Plan { by }) => w(&["REBALANCE", "PLAN", "--by", by]),
        Command::Rebalance(RebalanceCmd::Status) => w(&["REBALANCE", "STATUS"]),
        Command::Status { json } => {
            let mut v = w(&["STATUS"]);
            if *json {
                v.push("--json".into());
            }
            v
        }
        Command::Rebalance(RebalanceCmd::Apply { .. }) | Command::Coord(_) => return None,
    };
    out.shrink_to_fit();
    Some(out)
}

fn run(cli: Cli) -> Result<(), String> {
    let token = match cli.token.or_else(|| std::env::var("KVADM_TOKEN").ok()) {
        Some(t) => t,
        None => return Err("give the admin token with --token or KVADM_TOKEN".into()),
    };
    let coords = if cli.coords.is_empty() {
        vec!["127.0.0.1:7200".to_owned()]
    } else {
        cli.coords
    };
    if let Some(w) = words(&cli.command) {
        let mut a = connect_any(&coords, &token)?;
        return print(&a.kvadm(&w)?);
    }
    match cli.command {
        Command::Rebalance(RebalanceCmd::Apply { plan }) => {
            let t = std::fs::read_to_string(&plan).map_err(|e| format!("{}: {e}", plan.display()))?;
            let mut a = connect_any(&coords, &token)?;
            print(&a.kvadm(&["REBALANCE".into(), "APPLY".into(), t])?)
        }
        Command::Coord(CoordCmd::Peers) => {
            let mut a = connect_any(&coords, &token)?;
            let t = text(a.kvadm(&["COORD".into(), "PEERS".into()])?)?;
            let (g, list) = t.trim().split_once(' ').unwrap_or((t.trim(), ""));
            outln!("generation {g}: {list}");
            for e in parse_list(list)? {
                match peers_of(e.admin, &token) {
                    Ok((eg, _)) => outln!("  {} admin {} reports generation {eg}", e.id, e.admin),
                    Err(err) => outln!("  {} admin {} unreachable: {err}", e.id, e.admin),
                }
            }
            Ok(())
        }
        Command::Coord(CoordCmd::Setpeers { r#gen, list }) => {
            setpeers(&coords, &token, Some(r#gen), &|_| Ok(list.clone()))
        }
        Command::Coord(CoordCmd::Add { peer }) => {
            let e = parse_entry(&peer)?;
            setpeers(&coords, &token, None, &|cur| {
                if cur.iter().any(|x| x.id == e.id) {
                    return Err(format!("coordinator {} is already listed", e.id));
                }
                let mut v: Vec<&str> = cur.iter().map(|x| x.raw.as_str()).collect();
                v.push(&e.raw);
                Ok(v.join(","))
            })
        }
        Command::Coord(CoordCmd::Remove { id }) => setpeers(&coords, &token, None, &|cur| {
            if !cur.iter().any(|x| x.id == id) {
                return Err(format!("coordinator {id} is not listed"));
            }
            Ok(cur
                .iter()
                .filter(|x| x.id != id)
                .map(|x| x.raw.as_str())
                .collect::<Vec<_>>()
                .join(","))
        }),
        Command::Coord(CoordCmd::Export { file, ab }) => {
            let mut a = connect_any(&coords, &token)?;
            match a.kvadm(&["COORD".into(), "EXPORT".into()])? {
                Value::Bulk(Some(b)) => {
                    let p = write_export(&file, ab, &b)?;
                    outln!("exported {} bytes to {}", b.len(), p.display());
                    Ok(())
                }
                Value::Error(e) => Err(String::from_utf8_lossy(&e).into_owned()),
                _ => Err("unexpected reply".into()),
            }
        }
        _ => Err("unsupported command".into()),
    }
}

fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) => {
            use clap::error::ErrorKind;
            if matches!(e.kind(), ErrorKind::DisplayHelp | ErrorKind::DisplayVersion) {
                e.exit();
            }
            let _ = e.print();
            return ExitCode::from(1);
        }
    };
    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("kvadm: {e}");
            ExitCode::from(1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entries_and_words() {
        let l = parse_list("c1=127.0.0.1:7100/7000/7200,c2=127.0.0.1:7101/7001/7201").unwrap();
        assert_eq!(l.len(), 2);
        assert_eq!(l[1].admin, "127.0.0.1:7201".parse().unwrap());
        assert!(parse_entry("c1=127.0.0.1:7100").is_err());
        let c = Cli::try_parse_from(["kvadm", "--token", "t", "migrate", "1-2", "--to", "3"]).unwrap();
        assert_eq!(words(&c.command).unwrap(), vec!["MIGRATE", "1-2", "--to", "3"]);
        let c = Cli::try_parse_from(["kvadm", "migrate", "pause", "7"]).unwrap();
        assert_eq!(words(&c.command).unwrap(), vec!["MIGRATE", "PAUSE", "7"]);
        let c = Cli::try_parse_from(["kvadm", "cluster", "params", "set", "lease_ms", "900"]).unwrap();
        assert_eq!(words(&c.command).unwrap(), vec!["PARAMS", "SET", "lease_ms", "900"]);
        let c = Cli::try_parse_from(["kvadm", "node", "maintenance", "abcd", "on"]).unwrap();
        assert_eq!(words(&c.command).unwrap(), vec!["NODE", "MAINTENANCE", "abcd", "on"]);
        let c = Cli::try_parse_from(["kvadm", "coord", "export", "x"]).unwrap();
        assert!(words(&c.command).is_none());
    }

    #[test]
    fn export_ab_alternates() {
        let dir = std::env::temp_dir().join(format!("kvadm-export-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let f = dir.join("state.export");
        let blob = |v: u64| {
            let mut b = vec![0u8; 16];
            b[8..16].copy_from_slice(&v.to_le_bytes());
            b
        };
        let p1 = write_export(&f, true, &blob(5)).unwrap();
        let p2 = write_export(&f, true, &blob(6)).unwrap();
        let p3 = write_export(&f, true, &blob(7)).unwrap();
        assert_ne!(p1, p2);
        assert_eq!(p1, p3);
        assert_eq!(write_export(&f, false, &blob(8)).unwrap(), f);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
