//! Operator API on `admin-port` (DESIGN §23.12).
//!
//! RESP `AUTH <admin-token>` then `KVADM <verb> ...`. Verbs map to `AdminOp` transitions
//! (params, node add/remove/maintenance/clear-fail, shard create/delete, slots assign, replica
//! add/remove/resync, migrate/pause/resume/abort, rebalance plan/apply) or to local operations
//! (`COORD SETPEERS`, `COORD PEERS`, export, status). On a follower, an admin command is relayed
//! to the leader and only the leader's reply text is returned.
//!
//! [`Controllers`] also holds the leader's event loop ([`Controllers::run`]): failure detection
//! ticks, failovers by the static rule, maintenance handoffs, ISR and member-state commits from
//! the primaries' reports and migration progress. Every action reads committed state only and
//! changes it only through `Wsr::propose` (§23.3 rule 6); membership and slot ownership change
//! only through the operator verbs here (LC-25).
//!
//! Verbs (arguments are words; options are `--name value`):
//! ```text
//! INIT [key value ...]                       first parameters, before any node is registered
//! PARAMS SET key value | PARAMS GET
//! NODE ADD ctrl --client ip:port --repl ip:port [--zone z] [--priority n] [--partitions P]
//! NODE REMOVE id [--force] | NODE MAINTENANCE id on|off | NODE CLEAR-FAIL id
//! SHARD CREATE sid --primary node [--min-isr n] | SHARD DELETE sid
//! SLOTS ASSIGN ranges sid
//! REPLICA ADD sid node | REPLICA REMOVE sid node | REPLICA RESYNC node
//! FAILOVER sid [--to node]
//! MIGRATE ranges --to sid [--rate mbps] [--barriers k] | MIGRATE PAUSE|RESUME|ABORT id | MIGRATIONS
//! REBALANCE PLAN [--by slots] | REBALANCE APPLY plan-text | REBALANCE STATUS
//! COORD SETPEERS gen list | COORD PEERS | COORD STATE | COORD SYNC | COORD EXPORT
//! STATUS [--json]
//! ```
//! Node ids are 40 hex digits or a unique prefix of at least 4.

use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use kv_ctrl::{MemberState, MigPhase, NodeId, NodeToCoord, ShardId, SlotRanges};
use kv_resp::{Limits, Parsed, Parser, Proto, ReplyWriter};
use kv_topo::state::ShardStatus;
use kv_topo::{AdminOp, ClusterState, Transition};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::failover::{self, FailoverError};
use crate::push::{NodeEvent, Pusher};
use crate::rebalance::{MigrateCmd, rebalance_plan};
use crate::wsr::peers::{PeerList, coord_name};
use crate::wsr::rpc::ct_eq;
use crate::wsr::{ProposeError, Wsr, persist};
use crate::{isr, migration};

/// Settings of the controllers and the admin API. No `Debug` (it holds the tokens).
#[derive(Clone)]
pub struct CtlCfg {
    /// `admin-token` (kvadm authenticates with it; followers relay with it).
    pub admin_token: Vec<u8>,
    /// `cluster-auth-token` (node handshakes for `node add`).
    pub auth: Vec<u8>,
}

/// Retry bookkeeping of one per-shard job (failover or handoff).
#[derive(Clone, Copy)]
struct Job {
    running: bool,
    retry_at: Option<Instant>,
}

#[derive(Default)]
struct Jobs {
    failover: hashbrown::HashMap<ShardId, Job, foldhash::fast::RandomState>,
    handoff: hashbrown::HashMap<ShardId, Job, foldhash::fast::RandomState>,
    /// Health transitions submitted and not yet answered.
    health: hashbrown::HashSet<(NodeId, bool), foldhash::fast::RandomState>,
}

/// Progress of `rebalance apply`.
#[derive(Clone, Default)]
struct RebalanceStatus {
    running: bool,
    done: usize,
    total: usize,
    outcome: String,
}

/// The leader-side controllers the admin API drives.
pub struct Controllers {
    pub pusher: Arc<Pusher>,
    pub wsr: Arc<Wsr>,
    cfg: CtlCfg,
    jobs: Arc<Mutex<Jobs>>,
    last_mig_id: Mutex<u64>,
    rebalance: Mutex<RebalanceStatus>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Committed facts whose change stops `rebalance apply`: node FAIL flags, shard terms and
/// statuses.
fn health_and_terms(s: &ClusterState) -> Vec<(u64, u8)> {
    let mut v: Vec<(u64, u8)> = s
        .nodes
        .iter()
        .map(|n| (u64::from_le_bytes(n.id.0[..8].try_into().unwrap_or([0; 8])), u8::from(n.flags.fail)))
        .collect();
    v.extend(s.shards.iter().map(|sh| {
        (
            u64::from(sh.id.0) << 32 | u64::from(sh.term),
            match sh.status {
                ShardStatus::Serving => 0,
                ShardStatus::Promoting => 1,
                ShardStatus::NoPrimary => 2,
            },
        )
    }));
    v
}

impl Controllers {
    pub fn new(wsr: Arc<Wsr>, pusher: Arc<Pusher>, cfg: CtlCfg) -> Arc<Self> {
        Arc::new(Controllers {
            pusher,
            wsr,
            cfg,
            jobs: Arc::new(Mutex::new(Jobs::default())),
            last_mig_id: Mutex::new(0),
            rebalance: Mutex::new(RebalanceStatus::default()),
        })
    }

    /// The leader's PFAIL view (for the client service's redirects).
    pub fn suspected(&self, n: &NodeId) -> bool {
        self.wsr.is_leader() && self.pusher.suspected(n)
    }

    /// The leader's event loop: node events and a detection tick every 100 ms. Returns when
    /// the pusher's event channel closes.
    pub async fn run(self: Arc<Self>, mut events: mpsc::UnboundedReceiver<NodeEvent>) {
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                ev = events.recv() => match ev {
                    None => return,
                    Some(ev) => self.on_event(ev),
                },
                _ = tick.tick() => self.on_tick(),
            }
        }
    }

    /// Submits `batch` in order with the other submissions and logs its outcome.
    fn submit(&self, what: &'static str, batch: Vec<Transition>, done: Option<(NodeId, bool)>) {
        let rx = match self.wsr.submit(batch) {
            Ok(rx) => rx,
            Err(_) => {
                if let Some(k) = done {
                    lock(&self.jobs).health.remove(&k);
                }
                return;
            }
        };
        let jobs = self.jobs.clone();
        tokio::spawn(async move {
            match rx.await {
                Ok(Ok(v)) => tracing::debug!(version = v, "{what} committed"),
                Ok(Err(e)) => tracing::debug!("{what} not committed: {e}"),
                Err(_) => {}
            }
            if let Some(k) = done {
                lock(&jobs).health.remove(&k);
            }
        });
    }

    fn on_event(self: &Arc<Self>, ev: NodeEvent) {
        if !self.wsr.is_leader() {
            return;
        }
        let s = self.wsr.committed();
        match ev {
            NodeEvent::NewIncarnation(_) | NodeEvent::ReportsFailed(_) => self.on_tick(),
            NodeEvent::Msg { from, msg } => match msg {
                NodeToCoord::IsrShrink { shard, term, member } | NodeToCoord::IsrExpand { shard, term, member } => {
                    let add = matches!(msg, NodeToCoord::IsrExpand { .. });
                    if !isr::from_primary(&s, shard, &from) {
                        return;
                    }
                    if let Some(t) = isr::isr_transition(&s, shard, term, member, add) {
                        self.submit(if add { "ISR expand" } else { "ISR shrink" }, vec![t], None);
                    }
                }
                NodeToCoord::MemberReady { shard, term, member } => {
                    if let Some(t) = isr::member_ready_transition(&s, &from, shard, term, member) {
                        self.submit("member ready", vec![t], None);
                    }
                }
                NodeToCoord::MigrationReady { id } => {
                    if let Some(b) = migration::on_ready(&s, &from, id) {
                        self.submit("migration cutover", b, None);
                    }
                }
                NodeToCoord::SealDone { id } => {
                    if let Some(t) = migration::on_seal_done(&s, &from, id) {
                        self.submit("migration done", vec![t], None);
                    }
                }
                NodeToCoord::MemberNeedsResync { shard, term, member } => {
                    if let Some(t) = isr::needs_resync_transition(&s, &from, shard, term, member) {
                        tracing::warn!(shard = shard.0, member = %member.hex40(),
                            "member cannot catch up by continue or repair: needs-full-sync (kvadm replica resync)");
                        self.submit("member needs resync", vec![t], None);
                    }
                }
                NodeToCoord::HandoffRequest { shard, term } => {
                    // a primary that is shutting down asks to hand its shard off first (§23.12)
                    let ok = s
                        .shard(shard)
                        .is_some_and(|sh| sh.term == term && sh.primary == Some(from) && sh.status == ShardStatus::Serving);
                    if ok && self.take_job(false, shard, Instant::now()) {
                        tracing::info!(shard = shard.0, "handoff requested by the primary (shutdown)");
                        let me = self.clone();
                        tokio::spawn(async move {
                            let r = failover::run_handoff(me.wsr.clone(), me.pusher.clone(), shard, None).await;
                            if let Err(e) = &r {
                                tracing::info!(shard = shard.0, "shutdown handoff: {e}");
                            }
                            me.release(false, shard, Duration::from_millis(500));
                        });
                    }
                }
                _ => {}
            },
        }
    }

    fn on_tick(self: &Arc<Self>) {
        if !self.wsr.is_leader() {
            let mut j = lock(&self.jobs);
            j.failover.clear();
            j.handoff.clear();
            return;
        }
        let since = self.pusher.leader_since_ns();
        if since == u64::MAX {
            return;
        }
        let s = self.wsr.committed();
        let now_ns = kv_common::clock::boot_ns();
        let health = self
            .pusher
            .with_lease(|ls| ls.detector.tick(&s, &ls.leases, now_ns, since));
        for t in health {
            if let Transition::Health { node, fail } = t
                && lock(&self.jobs).health.insert((node, fail)) {
                    tracing::info!(node = %node.hex40(), fail, "node health change");
                    self.submit("health", vec![t], Some((node, fail)));
                }
        }
        let now = Instant::now();
        for sh in &s.shards {
            let Some(p) = sh.primary else { continue };
            if sh.status != ShardStatus::Serving {
                if self.take_job(true, sh.id, now) {
                    let me = self.clone();
                    let shard = sh.id;
                    tokio::spawn(async move {
                        let r = failover::run_failover(me.wsr.clone(), me.pusher.clone(), shard, p).await;
                        let retry = match &r {
                            Ok(()) | Err(FailoverError::Superseded) => Duration::ZERO,
                            Err(FailoverError::NoEligibleMember) => Duration::from_millis(500),
                            Err(e) => {
                                tracing::warn!(shard = shard.0, "failover: {e}");
                                Duration::from_secs(1)
                            }
                        };
                        me.release(true, shard, retry);
                    });
                }
            } else if s.node(&p).is_some_and(|r| r.flags.maint) && self.take_job(false, sh.id, now) {
                let me = self.clone();
                let shard = sh.id;
                tokio::spawn(async move {
                    let r = failover::run_handoff(me.wsr.clone(), me.pusher.clone(), shard, None).await;
                    if let Err(e) = &r {
                        tracing::debug!(shard = shard.0, "maintenance handoff: {e}");
                    }
                    me.release(false, shard, Duration::from_secs(2));
                });
            }
        }
    }

    /// Marks a job running unless it runs or waits for its retry time.
    fn take_job(&self, failover: bool, shard: ShardId, now: Instant) -> bool {
        let mut j = lock(&self.jobs);
        let map = if failover { &mut j.failover } else { &mut j.handoff };
        let e = map.entry(shard).or_insert(Job {
            running: false,
            retry_at: None,
        });
        if e.running || e.retry_at.is_some_and(|t| now < t) {
            return false;
        }
        e.running = true;
        true
    }

    fn release(&self, failover: bool, shard: ShardId, retry: Duration) {
        let mut j = lock(&self.jobs);
        let map = if failover { &mut j.failover } else { &mut j.handoff };
        map.insert(
            shard,
            Job {
                running: false,
                retry_at: Some(Instant::now() + retry),
            },
        );
    }

    fn next_mig_id(&self, s: &ClusterState) -> u64 {
        let mut last = lock(&self.last_mig_id);
        let id = migration::next_id(s, *last);
        *last = id;
        id
    }

    /// `rebalance apply`: one migration at a time, stopping at the first failover or health
    /// change (never re-planning).
    async fn rebalance_apply(self: Arc<Self>, cmds: Vec<MigrateCmd>) {
        let total = cmds.len();
        let set = |done: usize, running: bool, outcome: &str| {
            *lock(&self.rebalance) = RebalanceStatus {
                running,
                done,
                total,
                outcome: outcome.to_owned(),
            };
        };
        let base = health_and_terms(&self.wsr.committed());
        let mut rx = self.wsr.subscribe();
        for (i, c) in cmds.into_iter().enumerate() {
            set(i, true, "running");
            if !self.wsr.is_leader() {
                return set(i, false, "stopped: leadership lost");
            }
            let s = self.wsr.committed();
            if health_and_terms(&s) != base {
                return set(i, false, "stopped: a failover or health change happened");
            }
            let id = self.next_mig_id(&s);
            let op = AdminOp::MigrateStart {
                id,
                slots: c.slots.clone(),
                dst: c.to,
                rate_mbps: None,
                barriers: 2,
            };
            if let Err(e) = self.wsr.propose(vec![Transition::Admin(op)]).await {
                return set(i, false, &format!("stopped: {c}: {e}"));
            }
            loop {
                let _ = tokio::time::timeout(Duration::from_millis(500), rx.changed()).await;
                let s = self.wsr.committed();
                if health_and_terms(&s) != base {
                    return set(i, false, "stopped: a failover or health change happened");
                }
                match s.migrations.iter().find(|m| m.id == id).map(|m| m.phase) {
                    Some(MigPhase::Done) => break,
                    Some(MigPhase::Paused) => return set(i, false, &format!("stopped: migration {id} paused")),
                    None => return set(i, false, &format!("stopped: migration {id} aborted")),
                    Some(_) => {}
                }
                if !self.wsr.is_leader() {
                    return set(i, false, "stopped: leadership lost");
                }
            }
        }
        set(total, false, "finished");
    }
}

// ---------------------------------------------------------------------------------------------
// Admin connections.

pub async fn serve_admin(l: tokio::net::TcpListener, wsr: Arc<Wsr>, ctl: Arc<Controllers>) -> std::io::Result<()> {
    let _ = &wsr;
    loop {
        let (s, peer) = l.accept().await?;
        let ctl = ctl.clone();
        tokio::spawn(async move {
            let _ = s.set_nodelay(true);
            if let Err(e) = admin_conn(ctl, s).await {
                tracing::debug!(%peer, "admin connection closed: {e}");
            }
        });
    }
}

async fn admin_conn(ctl: Arc<Controllers>, mut sock: TcpStream) -> std::io::Result<()> {
    let mut parser = Parser::new(Limits {
        max_bulk: 64 << 20,
        ..Limits::default()
    });
    let mut buf = Vec::with_capacity(16 * 1024);
    let mut args = Vec::new();
    let mut authed = false;
    loop {
        let start = buf.len();
        buf.resize(start + 16 * 1024, 0);
        let n = sock.read(&mut buf[start..]).await?;
        buf.truncate(start + n);
        if n == 0 {
            return Ok(());
        }
        let mut pos = 0;
        loop {
            args.clear();
            match parser.parse(&buf[pos..], &mut args) {
                Ok(Parsed::Incomplete) => break,
                Ok(Parsed::Empty { consumed }) => pos += consumed,
                Ok(Parsed::Command { consumed }) => {
                    let src = parser.arg_source(&buf[pos..]);
                    let argv: Vec<Vec<u8>> = args
                        .iter()
                        .map(|&(o, l)| src[o as usize..(o + l) as usize].to_vec())
                        .collect();
                    pos += consumed;
                    let reply = command(&ctl, &mut authed, &argv).await;
                    sock.write_all(&reply).await?;
                }
                Err(e) => {
                    let mut out = Vec::new();
                    e.write_reply(&mut out);
                    sock.write_all(&out).await?;
                    return Ok(());
                }
            }
        }
        buf.drain(..pos);
    }
}

fn err(msg: impl AsRef<str>) -> Vec<u8> {
    let mut out = Vec::new();
    ReplyWriter::new(&mut out, Proto::Resp2).error(msg.as_ref().as_bytes());
    out
}

fn simple(msg: impl AsRef<str>) -> Vec<u8> {
    let mut out = Vec::new();
    ReplyWriter::new(&mut out, Proto::Resp2).simple(msg.as_ref().as_bytes());
    out
}

fn bulk(b: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    ReplyWriter::new(&mut out, Proto::Resp2).bulk(b);
    out
}

async fn command(ctl: &Arc<Controllers>, authed: &mut bool, argv: &[Vec<u8>]) -> Vec<u8> {
    let Some(name) = argv.first() else {
        return err("ERR empty command");
    };
    if name.eq_ignore_ascii_case(b"AUTH") {
        let pass = argv.last().map(Vec::as_slice).unwrap_or(b"");
        if argv.len() >= 2 && ct_eq(pass, &ctl.cfg.admin_token) {
            *authed = true;
            return simple("OK");
        }
        return err("WRONGPASS invalid admin token");
    }
    if name.eq_ignore_ascii_case(b"QUIT") {
        return simple("OK");
    }
    if !*authed {
        return err("NOAUTH Authentication required.");
    }
    if name.eq_ignore_ascii_case(b"PING") {
        return simple("PONG");
    }
    if !name.eq_ignore_ascii_case(b"KVADM") {
        return err("ERR the admin port accepts AUTH, PING and KVADM commands only");
    }
    let mut words = Vec::with_capacity(argv.len() - 1);
    for a in &argv[1..] {
        match std::str::from_utf8(a) {
            Ok(s) => words.push(s.to_owned()),
            Err(_) => return err("ERR arguments must be UTF-8 text"),
        }
    }
    if words.is_empty() {
        return err("ERR KVADM needs a verb");
    }
    let verb = words[0].to_ascii_uppercase();
    let sub = words.get(1).map(|w| w.to_ascii_uppercase()).unwrap_or_default();
    // Per-coordinator operations, never relayed.
    let local = matches!(
        (verb.as_str(), sub.as_str()),
        ("COORD", "SETPEERS" | "PEERS" | "STATE" | "EXPORT")
    );
    if !local && !ctl.wsr.is_leader() {
        return relay(ctl, argv, &verb).await;
    }
    match run_verb(ctl, &verb, &sub, &words).await {
        Ok(reply) => reply,
        Err(e) => err(format!("ERR {e}")),
    }
}

/// Forwards one admin command to the leader and returns its reply bytes unchanged.
async fn relay(ctl: &Arc<Controllers>, argv: &[Vec<u8>], verb: &str) -> Vec<u8> {
    let wsr = &ctl.wsr;
    let leader = wsr.leader_id().filter(|l| *l != wsr.id() && wsr.serving_topology());
    let Some(addr) = leader.and_then(|l| wsr.peers().record(&l).map(|r| r.admin)) else {
        if verb == "STATUS" {
            return bulk(status_text(ctl, false).as_bytes());
        }
        return err("CLUSTERDOWN no coordinator leader is known (try again)");
    };
    match relay_to(addr, &ctl.cfg.admin_token, argv).await {
        Ok(r) => r,
        Err(e) => err(format!("CLUSTERDOWN the leader's admin port did not answer: {e}")),
    }
}

async fn relay_to(addr: SocketAddr, token: &[u8], argv: &[Vec<u8>]) -> std::io::Result<Vec<u8>> {
    let mut s = tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(addr))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "connect"))??;
    let mut out = Vec::new();
    kv_resp::encode_command(&[b"AUTH", token], &mut out);
    let a: Vec<&[u8]> = argv.iter().map(Vec::as_slice).collect();
    kv_resp::encode_command(&a, &mut out);
    s.write_all(&out).await?;
    let mut buf = Vec::new();
    let mut replies = Vec::new();
    while replies.len() < 2 {
        let mut chunk = vec![0u8; 16 * 1024];
        let n = tokio::time::timeout(Duration::from_secs(60), s.read(&mut chunk))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "reply"))??;
        if n == 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "closed"));
        }
        buf.extend_from_slice(&chunk[..n]);
        while let Some((_, used)) = kv_resp::decode_value(&buf)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "bad reply"))?
        {
            replies.push(buf.drain(..used).collect::<Vec<u8>>());
            if replies.len() == 2 {
                break;
            }
        }
    }
    if !replies[0].starts_with(b"+OK") {
        return Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, "admin token refused"));
    }
    Ok(replies.swap_remove(1))
}

/// Options `--name value` (and bare `--flag`) after the positional words.
struct Opts {
    pos: Vec<String>,
    opts: Vec<(String, Option<String>)>,
}

impl Opts {
    fn parse(words: &[String], flags: &[&str]) -> Self {
        let mut pos = Vec::new();
        let mut opts = Vec::new();
        let mut i = 0;
        while i < words.len() {
            let w = &words[i];
            if let Some(name) = w.strip_prefix("--") {
                let name = name.to_ascii_lowercase();
                if flags.contains(&name.as_str()) || i + 1 >= words.len() {
                    opts.push((name, None));
                    i += 1;
                } else {
                    opts.push((name, Some(words[i + 1].clone())));
                    i += 2;
                }
            } else {
                pos.push(w.clone());
                i += 1;
            }
        }
        Opts { pos, opts }
    }

    fn get(&self, name: &str) -> Option<&str> {
        self.opts
            .iter()
            .find(|(n, _)| n == name)
            .and_then(|(_, v)| v.as_deref())
    }

    fn flag(&self, name: &str) -> bool {
        self.opts.iter().any(|(n, _)| n == name)
    }

    fn pos(&self, i: usize, what: &str) -> Result<&str, String> {
        self.pos
            .get(i)
            .map(String::as_str)
            .ok_or_else(|| format!("missing {what}"))
    }

    fn check(&self, allowed: &[&str], max_pos: usize) -> Result<(), String> {
        if let Some((n, _)) = self.opts.iter().find(|(n, _)| !allowed.contains(&n.as_str())) {
            return Err(format!("unknown option --{n}"));
        }
        if self.pos.len() > max_pos {
            return Err(format!("unexpected argument '{}'", self.pos[max_pos]));
        }
        Ok(())
    }
}

fn resolve_addr(s: &str) -> Result<SocketAddr, String> {
    if let Ok(a) = s.parse() {
        return Ok(a);
    }
    s.to_socket_addrs()
        .map_err(|e| format!("cannot resolve '{s}': {e}"))?
        .next()
        .ok_or_else(|| format!("'{s}' resolves to no address"))
}

fn parse_num<T: std::str::FromStr>(s: &str, what: &str) -> Result<T, String> {
    s.parse::<T>().map_err(|_| format!("invalid {what} '{s}'"))
}

fn parse_shard(s: &str) -> Result<ShardId, String> {
    parse_num::<u16>(s, "shard id").map(ShardId)
}

/// A registered node by 40 hex digits or a unique prefix of at least 4.
fn find_node(s: &ClusterState, id: &str) -> Result<NodeId, String> {
    let id = id.to_ascii_lowercase();
    if let Some(n) = NodeId::parse_hex40(&id) {
        return Ok(n);
    }
    if id.len() < 4 || !id.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(format!("invalid node id '{id}'"));
    }
    let hits: Vec<NodeId> = s
        .nodes
        .iter()
        .map(|n| n.id)
        .filter(|n| n.hex40().starts_with(&id))
        .collect();
    match hits.as_slice() {
        [one] => Ok(*one),
        [] => Err(format!("no registered node matches '{id}'")),
        _ => Err(format!("node id prefix '{id}' is ambiguous")),
    }
}

fn ranges(s: &str) -> Result<SlotRanges, String> {
    SlotRanges::parse(s)
        .and_then(|r| r.normalized())
        .filter(|r| !r.is_empty())
        .ok_or_else(|| format!("invalid slot ranges '{s}'"))
}

async fn commit(ctl: &Controllers, batch: Vec<Transition>) -> Result<Vec<u8>, String> {
    match ctl.wsr.propose(batch).await {
        Ok(v) => Ok(simple(format!("OK version {v}"))),
        Err(ProposeError::Rejected(r)) => Err(r.to_string()),
        Err(e) => Err(e.to_string()),
    }
}

async fn admin(ctl: &Controllers, op: AdminOp) -> Result<Vec<u8>, String> {
    commit(ctl, vec![Transition::Admin(op)]).await
}

async fn run_verb(ctl: &Arc<Controllers>, verb: &str, sub: &str, words: &[String]) -> Result<Vec<u8>, String> {
    let s = ctl.wsr.committed();
    match (verb, sub) {
        ("INIT", _) => {
            if !s.nodes.is_empty() {
                return Err("the cluster already has registered nodes".into());
            }
            let kv = &words[1..];
            if !kv.len().is_multiple_of(2) {
                return Err("INIT takes key value pairs".into());
            }
            let batch = kv
                .chunks(2)
                .map(|p| {
                    Transition::Admin(AdminOp::ParamSet {
                        key: p[0].clone(),
                        value: p[1].clone(),
                    })
                })
                .collect();
            let v = ctl.wsr.propose(batch).await.map_err(|e| e.to_string())?;
            Ok(simple(format!(
                "OK cluster {} version {v}",
                hex(&ctl.wsr.committed().cluster_id)
            )))
        }
        ("PARAMS", "SET") => {
            let o = Opts::parse(&words[2..], &[]);
            o.check(&[], 2)?;
            admin(
                ctl,
                AdminOp::ParamSet {
                    key: o.pos(0, "parameter name")?.to_owned(),
                    value: o.pos(1, "parameter value")?.to_owned(),
                },
            )
            .await
        }
        ("PARAMS", "GET") => {
            let p = &s.params;
            Ok(bulk(
                format!(
                    "partitions {}\nlease_ms {}\nrenew_ms {}\nfail_grace_ms {}\nrepl_ack_timeout_ms {}\nclient_password {}\n",
                    p.partitions,
                    p.lease_ms,
                    p.renew_ms,
                    p.fail_grace_ms,
                    p.repl_ack_timeout_ms,
                    if p.client_password.is_some() { "(set)" } else { "(none)" }
                )
                .as_bytes(),
            ))
        }
        ("NODE", "ADD") => node_add(ctl, &s, &words[2..]).await,
        ("NODE", "REMOVE") => {
            let o = Opts::parse(&words[2..], &["force"]);
            o.check(&["force"], 1)?;
            let id = find_node(&s, o.pos(0, "node id")?)?;
            admin(ctl, AdminOp::NodeRemove { id, force: o.flag("force") }).await
        }
        ("NODE", "MAINTENANCE") => {
            let o = Opts::parse(&words[2..], &[]);
            o.check(&[], 2)?;
            let id = find_node(&s, o.pos(0, "node id")?)?;
            let on = match o.pos(1, "on|off")?.to_ascii_lowercase().as_str() {
                "on" => true,
                "off" => false,
                x => return Err(format!("expected on or off, got '{x}'")),
            };
            // With `on`, the controllers hand the node's primaries over (static rule).
            admin(ctl, AdminOp::NodeMaint { id, on }).await
        }
        ("NODE", "CLEAR-FAIL") => {
            let o = Opts::parse(&words[2..], &[]);
            o.check(&[], 1)?;
            let node = find_node(&s, o.pos(0, "node id")?)?;
            commit(ctl, vec![Transition::Health { node, fail: false }]).await
        }
        ("SHARD", "CREATE") => {
            let o = Opts::parse(&words[2..], &[]);
            o.check(&["primary", "min-isr"], 1)?;
            let id = parse_shard(o.pos(0, "shard id")?)?;
            let primary = find_node(&s, o.get("primary").ok_or("missing --primary")?)?;
            let min_isr = o.get("min-isr").map_or(Ok(1), |v| parse_num(v, "min-isr"))?;
            admin(ctl, AdminOp::ShardCreate { id, primary, min_isr }).await
        }
        ("SHARD", "DELETE") => {
            let o = Opts::parse(&words[2..], &[]);
            o.check(&[], 1)?;
            admin(ctl, AdminOp::ShardDelete { id: parse_shard(o.pos(0, "shard id")?)? }).await
        }
        ("SLOTS", "ASSIGN") => {
            let o = Opts::parse(&words[2..], &[]);
            o.check(&[], 2)?;
            admin(
                ctl,
                AdminOp::SlotsAssign {
                    slots: ranges(o.pos(0, "slot ranges")?)?,
                    shard: parse_shard(o.pos(1, "shard id")?)?,
                },
            )
            .await
        }
        ("REPLICA", "ADD" | "REMOVE") => {
            let o = Opts::parse(&words[2..], &[]);
            o.check(&[], 2)?;
            let shard = parse_shard(o.pos(0, "shard id")?)?;
            let node = find_node(&s, o.pos(1, "node id")?)?;
            let op = if sub == "ADD" {
                AdminOp::ReplicaAdd { shard, node }
            } else {
                AdminOp::ReplicaRemove { shard, node }
            };
            admin(ctl, op).await
        }
        ("REPLICA", "RESYNC") => {
            let o = Opts::parse(&words[2..], &[]);
            o.check(&[], 1)?;
            let node = find_node(&s, o.pos(0, "node id")?)?;
            let corrupt = ctl
                .pusher
                .with_lease(|ls| ls.leases.health(&node))
                .is_some_and(|h| h.corrupt_pages > 0);
            s.resync_allowed(&node, corrupt).map_err(|e| e.to_string())?;
            admin(ctl, AdminOp::ReplicaResync { node }).await
        }
        ("FAILOVER", _) => {
            let o = Opts::parse(&words[1..], &[]);
            o.check(&["to"], 1)?;
            let shard = parse_shard(o.pos(0, "shard id")?)?;
            let to = o.get("to").map(|t| find_node(&s, t)).transpose()?;
            let n = failover::run_handoff(ctl.wsr.clone(), ctl.pusher.clone(), shard, to)
                .await
                .map_err(|e| e.to_string())?;
            Ok(simple(format!("OK primary {}", n.hex40())))
        }
        ("MIGRATE", "PAUSE" | "RESUME" | "ABORT") => {
            let o = Opts::parse(&words[2..], &[]);
            o.check(&[], 1)?;
            let id: u64 = parse_num(o.pos(0, "migration id")?, "migration id")?;
            let op = match sub {
                "PAUSE" => AdminOp::MigratePause { id },
                "RESUME" => AdminOp::MigrateResume { id },
                _ => AdminOp::MigrateAbort { id },
            };
            admin(ctl, op).await
        }
        ("MIGRATE", _) => {
            let o = Opts::parse(&words[1..], &[]);
            o.check(&["to", "rate", "barriers"], 1)?;
            let slots = ranges(o.pos(0, "slot ranges")?)?;
            let dst = parse_shard(o.get("to").ok_or("missing --to")?)?;
            let rate_mbps = o.get("rate").map(|r| parse_num::<u32>(r, "rate")).transpose()?;
            let barriers = o.get("barriers").map_or(Ok(2), |b| parse_num::<u8>(b, "barriers"))?;
            let id = ctl.next_mig_id(&s);
            let op = AdminOp::MigrateStart {
                id,
                slots,
                dst,
                rate_mbps,
                barriers,
            };
            let v = ctl
                .wsr
                .propose(vec![Transition::Admin(op)])
                .await
                .map_err(|e| e.to_string())?;
            Ok(simple(format!("OK migration {id} version {v}")))
        }
        ("MIGRATIONS", _) => {
            let mut t = String::new();
            for m in &s.migrations {
                t.push_str(&migration::describe(&s, m));
                t.push('\n');
            }
            Ok(bulk(t.as_bytes()))
        }
        ("REBALANCE", "PLAN") => {
            let o = Opts::parse(&words[2..], &[]);
            o.check(&["by"], 0)?;
            match o.get("by").unwrap_or("slots") {
                "slots" => {}
                other => return Err(format!("--by {other} is not supported (slots only)")),
            }
            let t: String = rebalance_plan(&s).iter().map(|c| format!("{c}\n")).collect();
            Ok(bulk(t.as_bytes()))
        }
        ("REBALANCE", "APPLY") => {
            let text = words[2..].join(" ").replace(" migrate ", "\nmigrate ");
            let cmds = MigrateCmd::parse_plan(&text)?;
            {
                let r = lock(&ctl.rebalance);
                if r.running {
                    return Err("a rebalance is already running".into());
                }
            }
            *lock(&ctl.rebalance) = RebalanceStatus {
                running: true,
                done: 0,
                total: cmds.len(),
                outcome: "running".into(),
            };
            let n = cmds.len();
            tokio::spawn(ctl.clone().rebalance_apply(cmds));
            Ok(simple(format!("OK applying {n} migrations one at a time")))
        }
        ("REBALANCE", "STATUS") => {
            let r = lock(&ctl.rebalance).clone();
            Ok(bulk(
                format!("running {}\ndone {}/{}\noutcome {}\n", r.running, r.done, r.total, r.outcome).as_bytes(),
            ))
        }
        ("COORD", "SETPEERS") => {
            let g: u64 = parse_num(words.get(2).ok_or("missing generation")?, "generation")?;
            let list = words.get(3).ok_or("missing peer list")?;
            let new = PeerList::parse(g, list, None)?;
            ctl.wsr.set_peers(new).await?;
            Ok(simple(format!("OK peer generation {g}")))
        }
        ("COORD", "PEERS") => {
            let p = ctl.wsr.peers();
            Ok(bulk(format!("{} {}", p.r#gen, p).as_bytes()))
        }
        ("COORD", "STATE") => {
            // `<peer generation> <term> <committed version> <leader|follower>`
            let w = &ctl.wsr;
            Ok(bulk(
                format!(
                    "{} {} {} {}",
                    w.peers().r#gen,
                    w.term(),
                    w.committed().version,
                    if w.is_leader() { "leader" } else { "follower" }
                )
                .as_bytes(),
            ))
        }
        ("COORD", "SYNC") => {
            // The sync commit of the SETPEERS sequence: a value committed by this leader under
            // its current peer generation (the next peer-list change waits until every staying
            // coordinator has committed it; see kv-sim wsr_model `may_start`).
            let g = ctl.wsr.peers().r#gen;
            let v = ctl.wsr.propose(Vec::new()).await.map_err(|e| e.to_string())?;
            if ctl.wsr.peers().r#gen != g {
                return Err("the peer generation changed during the sync commit".into());
            }
            Ok(simple(format!("OK version {v} generation {g}")))
        }
        ("COORD", "EXPORT") => {
            let (version, value_term, blob) = ctl.wsr.committed_entry();
            let x = persist::Export {
                version,
                coord_term: ctl.wsr.term(),
                value_term,
                peer_gen: ctl.wsr.peers().r#gen,
                bytes: blob.as_slice().to_vec(),
            };
            Ok(bulk(&x.encode()))
        }
        ("STATUS", _) => {
            let o = Opts::parse(&words[1..], &["json"]);
            o.check(&["json"], 0)?;
            Ok(bulk(status_text(ctl, o.flag("json")).as_bytes()))
        }
        _ => Err(format!("unknown verb '{}'", words.join(" "))),
    }
}

async fn node_add(ctl: &Controllers, s: &ClusterState, words: &[String]) -> Result<Vec<u8>, String> {
    let o = Opts::parse(words, &[]);
    o.check(&["client", "repl", "zone", "priority", "partitions", "id"], 1)?;
    let ctrl = resolve_addr(o.pos(0, "control address")?)?;
    let client = resolve_addr(o.get("client").ok_or("missing --client")?)?;
    let repl = resolve_addr(o.get("repl").ok_or("missing --repl")?)?;
    let zone = o.get("zone").unwrap_or("").to_owned();
    let priority = o.get("priority").map_or(Ok(0), |p| parse_num::<u8>(p, "priority"))?;
    let given_id = o
        .get("id")
        .map(|i| NodeId::parse_hex40(&i.to_ascii_lowercase()).ok_or_else(|| format!("invalid node id '{i}'")))
        .transpose()?;
    let (id, partitions) = match o.get("partitions") {
        Some(p) => (given_id.unwrap_or_else(NodeId::random), parse_num::<u16>(p, "partitions")?),
        None => {
            // The handshake tells the node's identity (if it ever joined) and its format.
            let ack = crate::push::probe_node(
                ctrl,
                s.cluster_id,
                ctl.wsr.id(),
                ctl.wsr.term(),
                &ctl.cfg.auth,
                Duration::from_secs(2),
            )
            .await
            .map_err(|e| format!("node at {ctrl} did not answer the handshake ({e}); give --partitions to register it offline"))?;
            match ack {
                NodeToCoord::HelloAck { node, partitions, .. } => {
                    (node.or(given_id).unwrap_or_else(NodeId::random), partitions)
                }
                NodeToCoord::Error(e) => return Err(format!("node refused the handshake: {e:?}")),
                other => return Err(format!("unexpected {} from the node", other.kind())),
            }
        }
    };
    let op = AdminOp::NodeAdd {
        id,
        ctrl,
        client,
        repl,
        zone,
        priority,
        partitions,
    };
    let v = ctl
        .wsr
        .propose(vec![Transition::Admin(op)])
        .await
        .map_err(|e| e.to_string())?;
    Ok(simple(format!("OK node {} version {v}", id.hex40())))
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Minimal JSON string escaping.
fn js(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 2);
    o.push('"');
    for c in s.chars() {
        match c {
            '"' => o.push_str("\\\""),
            '\\' => o.push_str("\\\\"),
            c if (c as u32) < 0x20 => o.push_str(&format!("\\u{:04x}", c as u32)),
            c => o.push(c),
        }
    }
    o.push('"');
    o
}

/// `kvadm status`: a read-only summary of the committed state and this coordinator's view.
/// States that wait for the operator (no automatic path leaves them, LC-25): members that need
/// a resync, paused migrations, shards without a serving primary. One line each.
fn attention(s: &ClusterState) -> Vec<String> {
    let mut out = Vec::new();
    for sh in &s.shards {
        if sh.status == ShardStatus::NoPrimary {
            out.push(format!(
                "shard {} has no eligible member to promote (CLUSTERDOWN for its slots): it serves again when its primary returns, or after operator action",
                sh.id.0
            ));
        }
        for m in &sh.members {
            if m.state == MemberState::NeedsFullSync {
                out.push(format!(
                    "member {} of shard {} cannot catch up by continue or repair: kvadm replica resync {}",
                    m.node.hex40(),
                    sh.id.0,
                    m.node.hex40()
                ));
            }
        }
    }
    for m in &s.migrations {
        if m.phase == MigPhase::Paused {
            out.push(format!(
                "migration {} is paused (a failover of its source or destination): kvadm migrate resume {} or kvadm migrate abort {}",
                m.id, m.id, m.id
            ));
        }
    }
    out
}

fn status_text(ctl: &Controllers, json: bool) -> String {
    let wsr = &ctl.wsr;
    let s = wsr.committed();
    let peers = wsr.peers();
    let leader = wsr.leader_id();
    let now = kv_common::clock::boot_ns();
    let leading = wsr.is_leader();
    let lease = |n: &NodeId| leading && ctl.pusher.lease_held(n, now);
    let conn = |n: &NodeId| leading && ctl.pusher.connected(n);
    let reb = lock(&ctl.rebalance).clone();
    if json {
        let mut o = String::from("{");
        o.push_str(&format!(
            "\"cluster_id\":{},\"version\":{},\"coordinator\":{},\"role\":{},\"term\":{},\"leader\":{},",
            js(&hex(&s.cluster_id)),
            s.version,
            js(&coord_name(&wsr.id())),
            js(if leading { "leader" } else { "follower" }),
            wsr.term(),
            leader.map_or("null".to_owned(), |l| js(&coord_name(&l)))
        ));
        o.push_str(&format!("\"peer_gen\":{},\"peers\":[", peers.r#gen));
        for (i, p) in peers.peers.iter().enumerate() {
            if i > 0 {
                o.push(',');
            }
            o.push_str(&format!(
                "{{\"id\":{},\"peer\":{},\"client\":{},\"admin\":{}}}",
                js(&coord_name(&p.id)),
                js(&p.peer.to_string()),
                js(&p.client.to_string()),
                js(&p.admin.to_string())
            ));
        }
        let p = &s.params;
        o.push_str(&format!(
            "],\"params\":{{\"partitions\":{},\"lease_ms\":{},\"renew_ms\":{},\"fail_grace_ms\":{},\"repl_ack_timeout_ms\":{},\"client_password_set\":{}}},\"nodes\":[",
            p.partitions, p.lease_ms, p.renew_ms, p.fail_grace_ms, p.repl_ack_timeout_ms, p.client_password.is_some()
        ));
        for (i, n) in s.nodes.iter().enumerate() {
            if i > 0 {
                o.push(',');
            }
            o.push_str(&format!(
                "{{\"id\":{},\"ctrl\":{},\"client\":{},\"repl\":{},\"zone\":{},\"priority\":{},\"fail\":{},\"maint\":{},\"lease\":{},\"connected\":{}}}",
                js(&n.id.hex40()),
                js(&n.ctrl.to_string()),
                js(&n.client.to_string()),
                js(&n.repl.to_string()),
                js(&n.zone),
                n.priority,
                n.flags.fail,
                n.flags.maint,
                lease(&n.id),
                conn(&n.id)
            ));
        }
        o.push_str("],\"shards\":[");
        for (i, sh) in s.shards.iter().enumerate() {
            if i > 0 {
                o.push(',');
            }
            o.push_str(&format!(
                "{{\"id\":{},\"status\":{},\"term\":{},\"primary\":{},\"min_isr\":{},\"slots\":{},\"members\":[",
                sh.id.0,
                js(sh.status.as_str()),
                sh.term,
                sh.primary.map_or("null".to_owned(), |p| js(&p.hex40())),
                sh.min_isr,
                js(&s.slot_ranges_of(sh.id).to_string())
            ));
            for (j, m) in sh.members.iter().enumerate() {
                if j > 0 {
                    o.push(',');
                }
                o.push_str(&format!(
                    "{{\"node\":{},\"state\":{},\"in_isr\":{}}}",
                    js(&m.node.hex40()),
                    js(m.state.as_str()),
                    m.in_isr
                ));
            }
            o.push_str("]}");
        }
        o.push_str("],\"migrations\":[");
        for (i, m) in s.migrations.iter().enumerate() {
            if i > 0 {
                o.push(',');
            }
            o.push_str(&format!(
                "{{\"id\":{},\"slots\":{},\"src\":{},\"dst\":{},\"phase\":{},\"rate_mbps\":{},\"barriers\":{}}}",
                m.id,
                js(&m.slots.to_string()),
                m.src.0,
                m.dst.0,
                js(m.phase.as_str()),
                m.rate_mbps.map_or("null".to_owned(), |r| r.to_string()),
                m.barriers
            ));
        }
        o.push_str("],\"attention\":[");
        for (i, a) in attention(&s).iter().enumerate() {
            if i > 0 {
                o.push(',');
            }
            o.push_str(&js(a));
        }
        o.push_str(&format!(
            "],\"assigned_slots\":{},\"rebalance\":{{\"running\":{},\"done\":{},\"total\":{},\"outcome\":{}}}}}",
            s.assigned_slots(),
            reb.running,
            reb.done,
            reb.total,
            js(&reb.outcome)
        ));
        return o;
    }
    let mut t = String::new();
    t.push_str(&format!("cluster {}  version {}\n", hex(&s.cluster_id), s.version));
    t.push_str(&format!(
        "coordinator {} ({}, term {}), leader {}\n",
        coord_name(&wsr.id()),
        if leading { "leader" } else { "follower" },
        wsr.term(),
        leader.map_or("unknown".to_owned(), |l| coord_name(&l))
    ));
    t.push_str(&format!("peers (generation {}): {}\n", peers.r#gen, peers));
    let p = &s.params;
    t.push_str(&format!(
        "params: partitions {} lease_ms {} renew_ms {} fail_grace_ms {} repl_ack_timeout_ms {} client_password {}\n",
        p.partitions,
        p.lease_ms,
        p.renew_ms,
        p.fail_grace_ms,
        p.repl_ack_timeout_ms,
        if p.client_password.is_some() { "set" } else { "none" }
    ));
    t.push_str(&format!("slots assigned {}/16384\n", s.assigned_slots()));
    t.push_str(&format!("nodes ({}):\n", s.nodes.len()));
    for n in &s.nodes {
        let mut flags = Vec::new();
        if n.flags.fail {
            flags.push("fail");
        }
        if n.flags.maint {
            flags.push("maint");
        }
        t.push_str(&format!(
            "  {} ctrl {} client {} repl {} zone '{}' priority {} flags {} lease {} control {}\n",
            n.id.hex40(),
            n.ctrl,
            n.client,
            n.repl,
            n.zone,
            n.priority,
            if flags.is_empty() { "-".to_owned() } else { flags.join(",") },
            if !leading { "n/a" } else if lease(&n.id) { "held" } else { "none" },
            if !leading { "n/a" } else if conn(&n.id) { "up" } else { "down" },
        ));
    }
    t.push_str(&format!("shards ({}):\n", s.shards.len()));
    for sh in &s.shards {
        t.push_str(&format!(
            "  {} {} term {} primary {} min-isr {} slots {}\n",
            sh.id.0,
            sh.status.as_str(),
            sh.term,
            sh.primary.map_or("-".to_owned(), |p| p.hex40()),
            sh.min_isr,
            s.slot_ranges_of(sh.id)
        ));
        for m in &sh.members {
            let role = if sh.primary == Some(m.node) { "primary" } else { "replica" };
            t.push_str(&format!(
                "    {} {} {}{}\n",
                m.node.hex40(),
                role,
                m.state.as_str(),
                if m.in_isr { " isr" } else { "" }
            ));
            debug_assert!(m.state != MemberState::Syncing || !m.in_isr);
        }
    }
    if !s.migrations.is_empty() {
        t.push_str(&format!("migrations ({}):\n", s.migrations.len()));
        for m in &s.migrations {
            t.push_str(&format!("  {}\n", migration::describe(&s, m)));
        }
    }
    if reb.total > 0 || reb.running {
        t.push_str(&format!("rebalance: {} ({}/{})\n", reb.outcome, reb.done, reb.total));
    }
    let att = attention(&s);
    if !att.is_empty() {
        t.push_str(&format!("operator action needed ({}):\n", att.len()));
        for a in att {
            t.push_str(&format!("  {a}\n"));
        }
    }
    t
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options() {
        let w: Vec<String> = ["1-2", "--to", "3", "--force", "x"].iter().map(|s| s.to_string()).collect();
        let o = Opts::parse(&w, &["force"]);
        assert_eq!(o.pos, vec!["1-2", "x"]);
        assert_eq!(o.get("to"), Some("3"));
        assert!(o.flag("force"));
        assert!(o.check(&["to", "force"], 2).is_ok());
        assert!(o.check(&["to"], 2).is_err());
        assert!(o.check(&["to", "force"], 1).is_err());
        assert_eq!(js("a\"b\\c\n"), "\"a\\\"b\\\\c\\u000a\"");
        assert_eq!(hex(&[0xab, 1]), "ab01");
    }
}
