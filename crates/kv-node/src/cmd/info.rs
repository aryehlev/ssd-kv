//! INFO (DESIGN §21): sections server, clients, memory, persistence, stats, replication (only
//! `role`, `connected_slaves`, `master_link_status`: no shard id, term, seq, lag position or peer
//! identity, LC-24, LC-28), keyspace, cluster, commandstats, latencystats and kvd. Bulk reply
//! (RESP3: verbatim `txt`).
//!
//! `INFO` alone (or `default`) renders server, clients, memory, persistence, stats, replication,
//! cluster, keyspace and kvd; `all` and `everything` add commandstats and latencystats. Section
//! names are case-insensitive, unknown ones contribute nothing, and sections always appear in
//! the fixed order above, as in Redis 7.0. Every value is read from the node's published
//! mirrors (atomics), so INFO never waits for a worker. Fields whose source is not published to
//! the command layer are left out rather than reported as invented zeros.

use std::fmt::Write as _;
use std::sync::atomic::Ordering;

use kv_resp::ReplyWriter;

use super::{Args, ConnCtx, ConnOutcome};
use crate::metrics::{CommandStat, WorkerCounters, sum_partitions};
use crate::shared::NodeShared;

/// One INFO section.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u16)]
enum Section {
    Server,
    Clients,
    Memory,
    Persistence,
    Stats,
    Replication,
    Cluster,
    Keyspace,
    Commandstats,
    Latencystats,
    Kvd,
}

/// Output order and names.
const SECTIONS: [(Section, &str, &str); 11] = [
    (Section::Server, "server", "Server"),
    (Section::Clients, "clients", "Clients"),
    (Section::Memory, "memory", "Memory"),
    (Section::Persistence, "persistence", "Persistence"),
    (Section::Stats, "stats", "Stats"),
    (Section::Replication, "replication", "Replication"),
    (Section::Cluster, "cluster", "Cluster"),
    (Section::Keyspace, "keyspace", "Keyspace"),
    (Section::Commandstats, "commandstats", "Commandstats"),
    (Section::Latencystats, "latencystats", "Latencystats"),
    (Section::Kvd, "kvd", "Kvd"),
];

/// A set of sections (bit `1 << Section`).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
struct Selection(u32);

impl Selection {
    const DEFAULT: Selection = Selection(
        1 << Section::Server as u32
            | 1 << Section::Clients as u32
            | 1 << Section::Memory as u32
            | 1 << Section::Persistence as u32
            | 1 << Section::Stats as u32
            | 1 << Section::Replication as u32
            | 1 << Section::Cluster as u32
            | 1 << Section::Keyspace as u32
            | 1 << Section::Kvd as u32,
    );
    const ALL: Selection = Selection((1 << SECTIONS.len()) - 1);

    fn has(self, s: Section) -> bool {
        self.0 & (1 << s as u32) != 0
    }

    /// The selection named by INFO's arguments.
    fn parse(args: &[&[u8]]) -> Selection {
        if args.is_empty() {
            return Selection::DEFAULT;
        }
        let mut sel = Selection(0);
        for &a in args {
            let a = super::c_str(a);
            if a.eq_ignore_ascii_case(b"default") {
                sel.0 |= Selection::DEFAULT.0;
            } else if a.eq_ignore_ascii_case(b"all") || a.eq_ignore_ascii_case(b"everything") {
                sel.0 |= Selection::ALL.0;
            } else if let Some((s, _, _)) = SECTIONS
                .iter()
                .find(|(_, n, _)| a.eq_ignore_ascii_case(n.as_bytes()))
            {
                sel.0 |= 1 << *s as u32;
            }
        }
        sel
    }
}

/// INFO [section ...].
pub fn info(cx: &mut ConnCtx<'_>, a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    let text = render(cx.shared, Selection::parse(&a.argv[1..]));
    w.verbatim(b"txt", text.as_bytes());
    ConnOutcome::Done
}

/// Redis `bytesToHuman`.
fn human(n: u64) -> String {
    const UNITS: [(u64, char); 5] = [
        (1 << 10, 'K'),
        (1 << 20, 'M'),
        (1 << 30, 'G'),
        (1 << 40, 'T'),
        (1 << 50, 'P'),
    ];
    if n < 1024 {
        return format!("{n}B");
    }
    let (div, unit) = UNITS
        .iter()
        .rev()
        .find(|(d, _)| n >= *d)
        .copied()
        .unwrap_or((1024, 'K'));
    format!("{:.2}{unit}", n as f64 / div as f64)
}

/// Resident set size of this process (`/proc/self/statm`), 0 if unavailable.
fn rss_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_ascii_whitespace().nth(1)?.parse::<u64>().ok())
        .map_or(0, |pages| pages * 4096)
}

/// `Linux <release> <arch>` as Redis prints `os` (from uname).
fn os_line() -> String {
    let release = std::fs::read_to_string("/proc/sys/kernel/osrelease").unwrap_or_default();
    let os = std::env::consts::OS;
    let mut name = os.to_owned();
    if let Some(first) = name.get_mut(0..1) {
        first.make_ascii_uppercase();
    }
    format!("{name} {} {}", release.trim(), std::env::consts::ARCH)
}

/// Renders the selected sections.
fn render(shared: &NodeShared, sel: Selection) -> String {
    let cfg = shared.config();
    let mut s = String::with_capacity(4096);
    let mut first = true;
    let mut workers = WorkerCounters::default();
    for wc in &shared.worker_counters {
        workers.add(&wc.snapshot());
    }
    let parts = sum_partitions(&shared.counters);
    let now = kv_common::Now::read();
    for (sec, _, title) in SECTIONS {
        if !sel.has(sec) {
            continue;
        }
        if !first {
            s.push_str("\r\n");
        }
        first = false;
        let _ = write!(s, "# {title}\r\n");
        // `field:value\r\n`
        macro_rules! f {
            ($k:expr, $v:expr) => {{
                let _ = write!(s, "{}:{}\r\n", $k, $v);
            }};
        }
        match sec {
            Section::Server => {
                let uptime = now.mono_ns.saturating_sub(shared.start.mono_ns) / 1_000_000_000;
                f!("redis_version", "7.2.0");
                f!(
                    "redis_mode",
                    if cfg.cluster_enabled {
                        "cluster"
                    } else {
                        "standalone"
                    }
                );
                f!("kvd_version", env!("CARGO_PKG_VERSION"));
                f!("os", os_line());
                f!("arch_bits", usize::BITS);
                f!("process_id", std::process::id());
                f!("run_id", shared.run_id);
                f!("tcp_port", cfg.port);
                f!("uptime_in_seconds", uptime);
                f!("uptime_in_days", uptime / 86_400);
                f!(
                    "executable",
                    std::env::current_exe()
                        .map(|p| p.display().to_string())
                        .unwrap_or_default()
                );
                f!(
                    "config_file",
                    cfg.config_file
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_default()
                );
                f!("io_threads_active", 0);
            }
            Section::Clients => {
                f!("connected_clients", workers.connected_clients);
                f!("maxclients", cfg.maxclients);
                f!("blocked_clients", 0);
                f!("backpressured_clients", workers.backpressured_clients);
            }
            Section::Memory => {
                let rss = rss_bytes();
                let keys: u64 = parts.keys.iter().sum();
                f!("used_memory", rss);
                f!("used_memory_human", human(rss));
                f!("maxmemory", 0);
                f!("index_bytes", parts.index_bytes);
                f!(
                    "index_bytes_per_key",
                    format!(
                        "{:.2}",
                        if keys == 0 {
                            0.0
                        } else {
                            parts.index_bytes as f64 / keys as f64
                        }
                    )
                );
                f!("alloc_state_bytes", parts.alloc_state_bytes);
                if let Some(c) = cfg.cache_size {
                    f!("cache_bytes", c);
                }
                f!("dirty_bytes", parts.dirty_bytes);
                f!("large_value_bytes", shared.budget.used());
            }
            Section::Persistence => {
                let r = &shared.recovery;
                f!("loading", u8::from(shared.loading()));
                f!(
                    "recovery_mode",
                    if r.scanned.load(Ordering::Relaxed) {
                        "scan"
                    } else {
                        "snapshot"
                    }
                );
                f!(
                    "recovery_duration_ms",
                    r.duration_ms.load(Ordering::Relaxed)
                );
                f!(
                    "recovery_bytes_scanned",
                    r.bytes_scanned.load(Ordering::Relaxed)
                );
                f!(
                    "last_shutdown_clean",
                    u8::from(r.last_shutdown_clean.load(Ordering::Relaxed))
                );
                f!(
                    "rdb_last_save_time",
                    shared.lastsave_unix.load(Ordering::Relaxed)
                );
                f!("rdb_last_bgsave_status", "ok");
                f!("aof_enabled", 0);
                f!(
                    "durability",
                    match cfg.effective_durability() {
                        kv_storage::Durability::Async => "async",
                        kv_storage::Durability::Written => "written",
                        kv_storage::Durability::Flushed => "flushed",
                    }
                );
            }
            Section::Stats => {
                f!(
                    "total_connections_received",
                    workers.total_connections_received
                );
                f!("total_commands_processed", workers.total_commands_processed);
                f!(
                    "instantaneous_ops_per_sec",
                    shared.ops_per_sec.load(Ordering::Relaxed)
                );
                f!("keyspace_hits", parts.keyspace_hits);
                f!("keyspace_misses", parts.keyspace_misses);
                f!("expired_keys", parts.expired_keys);
                f!("evicted_keys", 0);
                f!("total_net_input_bytes", workers.total_net_input_bytes);
                f!("total_net_output_bytes", workers.total_net_output_bytes);
                f!("xmsg_hops", workers.xmsg_hops);
                f!("rejected_connections", workers.rejected_connections);
            }
            Section::Replication => {
                // role, member count and link state only (LC-24, LC-28): no shard id, term,
                // seq, lag position or peer identity
                match shared.cluster() {
                    Some(cs) if shared.ownership.read().is_replica() => {
                        f!("role", "slave");
                        f!("connected_slaves", 0);
                        f!(
                            "master_link_status",
                            if cs.member_links_up() { "up" } else { "down" }
                        );
                    }
                    Some(cs) => {
                        f!("role", "master");
                        f!("connected_slaves", cs.connected_members());
                    }
                    None => {
                        f!("role", "master");
                        f!("connected_slaves", 0);
                    }
                }
            }
            Section::Cluster => f!("cluster_enabled", u8::from(cfg.cluster_enabled)),
            Section::Keyspace => {
                for db in 0..cfg.databases.min(16) {
                    let keys = parts.keys[usize::from(db)];
                    if keys > 0 {
                        let _ = write!(
                            s,
                            "db{db}:keys={keys},expires={},avg_ttl=0\r\n",
                            parts.expires[usize::from(db)]
                        );
                    }
                }
            }
            Section::Commandstats => {
                for spec in kv_cmd::all_with_subcommands() {
                    let mut t = CommandStat::default();
                    for cs in &shared.command_stats {
                        let c = cs.get(spec.id);
                        t.calls += c.calls;
                        t.usec += c.usec;
                        t.rejected_calls += c.rejected_calls;
                        t.failed_calls += c.failed_calls;
                    }
                    if t.calls + t.rejected_calls + t.failed_calls == 0 {
                        continue;
                    }
                    let per = if t.calls == 0 {
                        0.0
                    } else {
                        t.usec as f64 / t.calls as f64
                    };
                    let _ = write!(
                        s,
                        "cmdstat_{}:calls={},usec={},usec_per_call={per:.2},rejected_calls={},failed_calls={}\r\n",
                        spec.name, t.calls, t.usec, t.rejected_calls, t.failed_calls
                    );
                }
            }
            Section::Latencystats => {
                for spec in kv_cmd::all_with_subcommands() {
                    let mut h = kv_common::metrics::Hist::new();
                    let mut any = false;
                    for l in &shared.latency {
                        any |= l.merge_into(spec.id, &mut h);
                    }
                    if !any || h.is_empty() {
                        continue;
                    }
                    let _ = write!(
                        s,
                        "latency_percentiles_usec_{}:p50={:.3},p99={:.3},p99.9={:.3},p99.99={:.3}\r\n",
                        spec.name,
                        h.percentile(50.0) as f64,
                        h.percentile(99.0) as f64,
                        h.percentile(99.9) as f64,
                        h.percentile(99.99) as f64,
                    );
                }
            }
            Section::Kvd => {
                let ratio = |num: u64, den: u64| {
                    if den == 0 {
                        0.0
                    } else {
                        num as f64 / den as f64
                    }
                };
                f!("partitions", shared.partitions);
                f!("page_reads", parts.page_reads);
                for (n, v) in [
                    ("update", parts.page_writes_update),
                    ("insert", parts.page_writes_insert),
                    ("delete", parts.page_writes_delete),
                    ("move", parts.page_writes_move),
                    ("scrub", parts.page_writes_scrub),
                    ("txn", parts.page_writes_txn),
                ] {
                    let _ = write!(s, "page_writes_{n}:{v}\r\n");
                }
                f!("extent_writes", parts.extent_writes);
                f!("host_bytes_written", parts.host_bytes_written);
                f!("user_bytes_written", parts.user_bytes_written);
                f!(
                    "write_amp",
                    format!(
                        "{:.2}",
                        ratio(parts.host_bytes_written, parts.user_bytes_written)
                    )
                );
                f!("sector_writes", parts.sector_writes);
                f!("full_page_writes", parts.full_page_writes);
                f!("moves", parts.moves);
                f!("fp_false_positive_reads", parts.fp_false_positive_reads);
                f!("stale_copies", parts.stale_copies);
                f!("foreground_scrub_writes", parts.foreground_scrub_writes);
                f!("txn_markers", parts.txn_markers);
                f!("txn_deletes_split", parts.txn_deletes_split);
                f!("space_wait_timeouts", parts.space_wait_timeouts);
                f!("moves_without_room", parts.moves_without_room);
                f!(
                    "txn_credits_in_use",
                    crate::shared::TXN_CREDITS
                        .saturating_sub(shared.txn_credits.load(Ordering::Relaxed))
                );
                f!(
                    "large_value_backpressured_conns",
                    workers.large_value_backpressured_conns
                );
                f!("client_reply_bytes_held", crate::net::conn::node_reply_bytes());
                f!("client_output_limit_disconnections", crate::net::conn::evicted());
                f!(
                    "flush_reclaim_pending_pages",
                    parts.flush_reclaim_pending_pages
                );
                f!("corrupt_pages", parts.corrupt_pages);
                f!("unexpected_orphans", parts.unexpected_orphans);
            }
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::conn_cmds::tests::Fixture;

    #[test]
    fn section_selection() {
        assert_eq!(Selection::parse(&[]), Selection::DEFAULT);
        assert_eq!(Selection::parse(&[b"DEFAULT"]), Selection::DEFAULT);
        assert_eq!(Selection::parse(&[b"everything"]), Selection::ALL);
        let s = Selection::parse(&[b"Server", b"keyspace", b"nosuch"]);
        assert!(s.has(Section::Server) && s.has(Section::Keyspace) && !s.has(Section::Memory));
        assert_eq!(Selection::parse(&[b"nosuch"]), Selection(0));
        assert!(!Selection::DEFAULT.has(Section::Commandstats));
    }

    #[test]
    fn human_sizes() {
        assert_eq!(human(0), "0B");
        assert_eq!(human(1023), "1023B");
        assert_eq!(human(1024), "1.00K");
        assert_eq!(human(1536), "1.50K");
        assert_eq!(human(3 << 20), "3.00M");
        assert_eq!(human(5 << 30), "5.00G");
    }

    #[test]
    fn info_replies() {
        let mut f = Fixture::new();
        let r = f.run_conn(info, "INFO");
        assert!(
            r.starts_with('$')
                && r.contains("# Server\r\nredis_version:7.2.0\r\nredis_mode:standalone\r\n"),
            "{r}"
        );
        assert!(
            r.contains("\r\n\r\n# Clients\r\n")
                && r.contains("# Keyspace\r\n")
                && !r.contains("# Commandstats")
        );
        let r = f.run_conn(info, "INFO nosuch");
        assert_eq!(r, "$0\r\n\r\n");
        f.shared.counters[0].keys[0].store(3, Ordering::Relaxed);
        f.shared.counters[1].keys[0].store(4, Ordering::Relaxed);
        f.shared.counters[1].expires[0].store(1, Ordering::Relaxed);
        f.shared.counters[1].keys[5].store(2, Ordering::Relaxed);
        let r = f.run_conn(info, "INFO keyspace");
        assert_eq!(
            r,
            "$76\r\n# Keyspace\r\ndb0:keys=7,expires=1,avg_ttl=0\r\ndb5:keys=2,expires=0,avg_ttl=0\r\n\r\n"
        );
        f.conn.proto = kv_resp::Proto::Resp3;
        let r = f.run_conn(info, "INFO keyspace");
        assert!(r.starts_with("=80\r\ntxt:# Keyspace\r\n"), "{r}");
        f.shared.command_stats[0].record(kv_cmd::CmdId::Get, 30, false);
        f.shared.command_stats[0].record(kv_cmd::CmdId::Get, 10, true);
        f.shared.latency[0].record_batch(&[(kv_cmd::CmdId::Get, 30)]);
        let r = f.run_conn(info, "INFO commandstats latencystats");
        assert!(r.contains("cmdstat_get:calls=2,usec=40,usec_per_call=20.00,rejected_calls=0,failed_calls=1\r\n"), "{r}");
        assert!(
            r.contains("latency_percentiles_usec_get:p50=30.000,"),
            "{r}"
        );
        assert!(f.shared.take_txn_credit());
        let r = f.run_conn(info, "INFO kvd");
        assert!(
            r.contains("\r\ntxn_credits_in_use:1\r\n")
                && r.contains("\r\nunexpected_orphans:0\r\n"),
            "{r}"
        );
        let r = f.run_conn(info, "INFO replication cluster");
        assert!(r.contains("# Replication\r\nrole:master\r\nconnected_slaves:0\r\n\r\n# Cluster\r\ncluster_enabled:0\r\n"), "{r}");
    }
}
