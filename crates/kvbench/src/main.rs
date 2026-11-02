//! `kvbench`: the kvssd benchmark client (DESIGN §25.8, §27).
//!
//! A pipelined RESP load generator. One OS thread per connection with blocking sockets: each
//! thread writes a batch of `--pipeline` commands in one buffer and then reads the batch's
//! replies with a small allocation-free reply scanner, so the client itself stays far from
//! being the bottleneck on cached workloads.
//!
//! * `--load` inserts the whole key space once (`<prefix>%016d` keys, each connection a stripe)
//!   and reports the insert rate.
//! * Otherwise a `--ratio R:W` mix of GET and SET over `--keys` keys, `--dist uniform` or
//!   `--dist zipf:<theta>` (scrambled, so hot keys spread over slots and partitions), for
//!   `--warmup` + `--duration` seconds; only the measured window is recorded.
//! * Closed loop by default; `--rate N` makes it open loop at `N` ops/s in total: every batch has
//!   an intended start time and latency is measured from that time, so a stalled server shows
//!   up in the tail instead of being hidden by the client waiting (coordinated omission).
//! * `--cluster` routes every key by slot like a Redis-Cluster client: the slot map comes from
//!   `CLUSTER SLOTS` at the seed (a coordinator), MOVED refreshes it and the command is resent,
//!   TRYAGAIN / CLUSTERDOWN are retried after a short pause (counted, never hidden).
//! * Reports ops/s and p50/p90/p99/p99.9/p99.99/max per command type, plus errors and misses;
//!   `--json` prints one JSON object instead; `--hdr-out PATH` writes the merged percentile
//!   distributions (a text table, one row per percentile step).
//!
//! Every socket has read and write timeouts and every thread watches one shared deadline, so
//! a stalled or dead server ends the run with errors instead of hanging it.
#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(unsafe_code)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;
use kv_common::metrics::Hist;

#[derive(Parser, Clone, Debug)]
#[command(
    name = "kvbench",
    version,
    about = "kvssd benchmark client (pipelined RESP load generator)"
)]
struct Cli {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value_t = 6379)]
    port: u16,
    /// Route keys by slot through the cluster map served at --host:--port (a coordinator).
    #[arg(long)]
    cluster: bool,
    /// Client password (AUTH) sent on every connection.
    #[arg(long)]
    auth: Option<String>,
    /// Insert the whole keyspace once, then exit.
    #[arg(long)]
    load: bool,
    /// Number of keys.
    #[arg(long, default_value_t = 20_000_000)]
    keys: u64,
    /// Key prefix; keys are `<prefix>%016d`.
    #[arg(long, default_value = "key:")]
    prefix: String,
    /// Value size in bytes.
    #[arg(long, default_value_t = 300)]
    value_size: usize,
    /// Read:write ratio, e.g. 90:10.
    #[arg(long, default_value = "90:10")]
    ratio: String,
    /// Key distribution: uniform or zipf:<theta> (0 < theta < 1, e.g. zipf:0.99).
    #[arg(long, default_value = "uniform")]
    dist: String,
    /// Connections (one thread each).
    #[arg(long, default_value_t = 64)]
    conns: u32,
    /// Commands per batch on each connection.
    #[arg(long, default_value_t = 1)]
    pipeline: u32,
    /// Open-loop target rate in ops/s over all connections (closed loop when absent).
    #[arg(long)]
    rate: Option<u64>,
    /// Measured duration in seconds.
    #[arg(long, default_value_t = 300)]
    duration: u64,
    /// Warm-up seconds before measuring.
    #[arg(long, default_value_t = 60)]
    warmup: u64,
    /// Socket read/write timeout in milliseconds (a reply slower than this is an error).
    #[arg(long, default_value_t = 5000)]
    timeout_ms: u64,
    /// Seed of the key and value generators.
    #[arg(long, default_value_t = 1)]
    seed: u64,
    /// Print one JSON object instead of the text report.
    #[arg(long)]
    json: bool,
    /// Write the merged latency histograms to this file.
    #[arg(long, value_name = "PATH")]
    hdr_out: Option<PathBuf>,
}

// ---------------------------------------------------------------------------------------------
// generators

/// splitmix64: small, fast, seedable; good enough for key choice.
#[derive(Clone)]
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed ^ 0x9E37_79B9_7F4A_7C15)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, n)` (Lemire's multiply-shift; n > 0).
    fn below(&mut self, n: u64) -> u64 {
        ((u128::from(self.next_u64()) * u128::from(n)) >> 64) as u64
    }

    /// Uniform in `[0, 1)`.
    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }
}

fn fnv64(mut x: u64) -> u64 {
    let mut h: u64 = 0xCBF2_9CE4_8422_2325;
    for _ in 0..8 {
        h ^= x & 0xFF;
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
        x >>= 8;
    }
    h
}

/// Key index distribution.
#[derive(Clone)]
enum Dist {
    Uniform,
    /// YCSB-style zipfian over ranks, scrambled with FNV so hot ranks are not neighbours.
    Zipf {
        theta: f64,
        zetan: f64,
        alpha: f64,
        eta: f64,
    },
}

fn zeta(n: u64, theta: f64) -> f64 {
    (1..=n).map(|i| 1.0 / (i as f64).powf(theta)).sum()
}

impl Dist {
    fn parse(s: &str, n: u64) -> Result<Self, String> {
        if s == "uniform" {
            return Ok(Dist::Uniform);
        }
        let t = s
            .strip_prefix("zipf:")
            .ok_or_else(|| format!("--dist: expected uniform or zipf:<theta>, got {s}"))?;
        let theta: f64 = t.parse().map_err(|_| format!("--dist: bad theta {t}"))?;
        if !(theta > 0.0 && theta < 1.0) {
            return Err("--dist: theta must be in (0, 1)".into());
        }
        let zetan = zeta(n, theta);
        let zeta2 = zeta(2.min(n), theta);
        let alpha = 1.0 / (1.0 - theta);
        let eta = (1.0 - (2.0 / n as f64).powf(1.0 - theta)) / (1.0 - zeta2 / zetan);
        Ok(Dist::Zipf {
            theta,
            zetan,
            alpha,
            eta,
        })
    }

    fn next(&self, rng: &mut Rng, n: u64) -> u64 {
        match *self {
            Dist::Uniform => rng.below(n),
            Dist::Zipf {
                theta,
                zetan,
                alpha,
                eta,
            } => {
                let u = rng.unit();
                let uz = u * zetan;
                let rank = if uz < 1.0 {
                    0
                } else if uz < 1.0 + 0.5f64.powf(theta) {
                    1
                } else {
                    ((n as f64) * (eta * u - eta + 1.0).powf(alpha)) as u64
                };
                fnv64(rank.min(n - 1)) % n
            }
        }
    }
}

fn key_of(prefix: &str, i: u64, out: &mut Vec<u8>) {
    out.clear();
    out.extend_from_slice(prefix.as_bytes());
    let mut d = [b'0'; 16];
    let mut v = i;
    for slot in d.iter_mut().rev() {
        *slot = b'0' + (v % 10) as u8;
        v /= 10;
    }
    out.extend_from_slice(&d);
}

// ---------------------------------------------------------------------------------------------
// replies

/// What one reply was, as far as the benchmark cares.
#[derive(Debug, PartialEq)]
enum Reply {
    Ok,
    /// A nil bulk (GET miss).
    Nil,
    /// An error reply; its text when it is a redirect or a retryable condition.
    Err(Vec<u8>),
}

/// Scans one reply at the start of `buf` without allocating for the common cases. Returns the
/// reply and its length, `Ok(None)` when incomplete.
fn scan_reply(buf: &[u8]) -> Result<Option<(Reply, usize)>, String> {
    let Some(&t) = buf.first() else {
        return Ok(None);
    };
    let line_end = |from: usize| {
        buf[from..]
            .windows(2)
            .position(|w| w == b"\r\n")
            .map(|p| from + p)
    };
    match t {
        b'+' | b':' => Ok(line_end(1).map(|e| (Reply::Ok, e + 2))),
        b'-' => Ok(line_end(1).map(|e| (Reply::Err(buf[1..e].to_vec()), e + 2))),
        b'$' => {
            let Some(e) = line_end(1) else {
                return Ok(None);
            };
            let len: i64 = std::str::from_utf8(&buf[1..e])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or("bad bulk length")?;
            if len < 0 {
                return Ok(Some((Reply::Nil, e + 2)));
            }
            let total = e + 2 + len as usize + 2;
            Ok((buf.len() >= total).then_some((Reply::Ok, total)))
        }
        b'_' => Ok(line_end(1).map(|e| (Reply::Nil, e + 2))),
        _ => match kv_resp::decode_value(buf) {
            Ok(Some((kv_resp::Value::Error(e), n))) => Ok(Some((Reply::Err(e), n))),
            Ok(Some((kv_resp::Value::Null | kv_resp::Value::Bulk(None), n))) => {
                Ok(Some((Reply::Nil, n)))
            }
            Ok(Some((_, n))) => Ok(Some((Reply::Ok, n))),
            Ok(None) => Ok(None),
            Err(_) => Err("malformed reply".into()),
        },
    }
}

// ---------------------------------------------------------------------------------------------
// connections and routing

struct Conn {
    s: TcpStream,
    rbuf: Vec<u8>,
    rpos: usize,
}

impl Conn {
    fn open(addr: SocketAddr, timeout: Duration, auth: Option<&str>) -> std::io::Result<Self> {
        let s = TcpStream::connect_timeout(&addr, timeout)?;
        s.set_read_timeout(Some(timeout))?;
        s.set_write_timeout(Some(timeout))?;
        let _ = s.set_nodelay(true);
        let mut c = Conn {
            s,
            rbuf: Vec::with_capacity(1 << 16),
            rpos: 0,
        };
        if let Some(pw) = auth {
            let mut out = Vec::new();
            kv_resp::encode_command(&[b"AUTH", pw.as_bytes()], &mut out);
            c.s.write_all(&out)?;
            if let Reply::Err(e) = c.read_reply()? {
                return Err(std::io::Error::other(format!(
                    "AUTH: {}",
                    String::from_utf8_lossy(&e)
                )));
            }
        }
        Ok(c)
    }

    fn read_reply(&mut self) -> std::io::Result<Reply> {
        loop {
            match scan_reply(&self.rbuf[self.rpos..]) {
                Ok(Some((r, n))) => {
                    self.rpos += n;
                    if self.rpos == self.rbuf.len() {
                        self.rbuf.clear();
                        self.rpos = 0;
                    }
                    return Ok(r);
                }
                Ok(None) => {}
                Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
            }
            if self.rpos > 0 {
                self.rbuf.drain(..self.rpos);
                self.rpos = 0;
            }
            let old = self.rbuf.len();
            self.rbuf.resize(old + 65536, 0);
            let n = match self.s.read(&mut self.rbuf[old..]) {
                Ok(n) => n,
                Err(e) => {
                    self.rbuf.truncate(old);
                    return Err(e);
                }
            };
            self.rbuf.truncate(old + n);
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed",
                ));
            }
        }
    }

    /// One command with a full RESP value back (setup traffic only).
    fn call(&mut self, args: &[&[u8]]) -> std::io::Result<kv_resp::Value> {
        let mut out = Vec::new();
        kv_resp::encode_command(args, &mut out);
        self.s.write_all(&out)?;
        let mut buf = std::mem::take(&mut self.rbuf);
        buf.drain(..self.rpos);
        self.rpos = 0;
        loop {
            if let Some((v, n)) = kv_resp::decode_value(&buf)
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "bad reply"))?
            {
                buf.drain(..n);
                self.rbuf = buf;
                return Ok(v);
            }
            let mut chunk = [0u8; 16384];
            let n = self.s.read(&mut chunk)?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed",
                ));
            }
            buf.extend_from_slice(&chunk[..n]);
        }
    }
}

fn resolve(host: &str, port: u16) -> Result<SocketAddr, String> {
    (host, port)
        .to_socket_addrs()
        .map_err(|e| format!("{host}:{port}: {e}"))?
        .next()
        .ok_or_else(|| format!("{host}:{port}: no address"))
}

/// The slot -> primary address map of a cluster, read with `CLUSTER SLOTS` at a seed.
fn fetch_slots(
    seed: SocketAddr,
    timeout: Duration,
    auth: Option<&str>,
) -> Result<Vec<Option<SocketAddr>>, String> {
    let mut c = Conn::open(seed, timeout, auth).map_err(|e| format!("{seed}: {e}"))?;
    let v = c
        .call(&[b"CLUSTER", b"SLOTS"])
        .map_err(|e| format!("CLUSTER SLOTS at {seed}: {e}"))?;
    let mut map = vec![None; kv_common::SLOTS];
    let kv_resp::Value::Array(Some(ranges)) = v else {
        return Err(format!("CLUSTER SLOTS at {seed}: unexpected reply"));
    };
    for r in ranges {
        let kv_resp::Value::Array(Some(f)) = r else {
            continue;
        };
        let (
            Some(kv_resp::Value::Int(a)),
            Some(kv_resp::Value::Int(b)),
            Some(kv_resp::Value::Array(Some(node))),
        ) = (f.first(), f.get(1), f.get(2))
        else {
            continue;
        };
        let ip = match node.first() {
            Some(kv_resp::Value::Bulk(Some(ip))) | Some(kv_resp::Value::Simple(ip)) => {
                String::from_utf8_lossy(ip).into_owned()
            }
            _ => continue,
        };
        let Some(kv_resp::Value::Int(port)) = node.get(1) else {
            continue;
        };
        let Ok(addr) = resolve(&ip, *port as u16) else {
            continue;
        };
        for s in (*a).max(0)..=(*b).min(16383) {
            map[s as usize] = Some(addr);
        }
    }
    Ok(map)
}

/// Where the commands of one connection thread go: one server, or the slot owners.
struct Route {
    seed: SocketAddr,
    cluster: bool,
    slots: Vec<Option<SocketAddr>>,
    conns: Vec<(SocketAddr, Conn)>,
    timeout: Duration,
    auth: Option<String>,
}

impl Route {
    fn target(&self, key: &[u8]) -> SocketAddr {
        if !self.cluster {
            return self.seed;
        }
        self.slots[usize::from(kv_common::key_slot(key))].unwrap_or(self.seed)
    }

    fn conn(&mut self, addr: SocketAddr) -> std::io::Result<usize> {
        if let Some(i) = self.conns.iter().position(|(a, _)| *a == addr) {
            return Ok(i);
        }
        let c = Conn::open(addr, self.timeout, self.auth.as_deref())?;
        self.conns.push((addr, c));
        Ok(self.conns.len() - 1)
    }

    fn drop_conn(&mut self, addr: SocketAddr) {
        self.conns.retain(|(a, _)| *a != addr);
    }

    fn refresh(&mut self) {
        if self.cluster
            && let Ok(m) = fetch_slots(self.seed, self.timeout, self.auth.as_deref())
        {
            self.slots = m;
        }
    }
}

// ---------------------------------------------------------------------------------------------
// workers

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Get,
    Set,
}

/// What one connection thread measured.
struct Stats {
    get: Hist,
    set: Hist,
    gets: u64,
    sets: u64,
    misses: u64,
    errors: u64,
    retries: u64,
    first_error: Option<String>,
}

impl Stats {
    fn new() -> Self {
        Stats {
            get: Hist::new(),
            set: Hist::new(),
            gets: 0,
            sets: 0,
            misses: 0,
            errors: 0,
            retries: 0,
            first_error: None,
        }
    }

    fn merge(&mut self, o: &Stats) {
        self.get.merge(&o.get);
        self.set.merge(&o.set);
        self.gets += o.gets;
        self.sets += o.sets;
        self.misses += o.misses;
        self.errors += o.errors;
        self.retries += o.retries;
        if self.first_error.is_none() {
            self.first_error.clone_from(&o.first_error);
        }
    }

    fn error(&mut self, e: impl FnOnce() -> String) {
        self.errors += 1;
        if self.first_error.is_none() {
            self.first_error = Some(e());
        }
    }
}

struct Plan {
    cli: Cli,
    dist: Dist,
    read_pct: u32,
    seed_addr: SocketAddr,
    slots: Vec<Option<SocketAddr>>,
    stop: AtomicBool,
    /// Load mode: next key index to insert.
    next_load: AtomicU64,
}

fn parse_ratio(s: &str) -> Result<u32, String> {
    let (r, w) = s.split_once(':').ok_or("--ratio: expected R:W")?;
    let r: u32 = r.trim().parse().map_err(|_| "--ratio: bad read share")?;
    let w: u32 = w.trim().parse().map_err(|_| "--ratio: bad write share")?;
    if r + w == 0 {
        return Err("--ratio: both shares are zero".into());
    }
    Ok(r * 100 / (r + w))
}

/// One batch entry: command type, key index, intended start.
struct Op {
    kind: Kind,
    key: u64,
}

fn worker(plan: Arc<Plan>, idx: u32, measure_from: Instant, end: Instant) -> Stats {
    let cli = &plan.cli;
    let timeout = Duration::from_millis(cli.timeout_ms.max(1));
    let mut route = Route {
        seed: plan.seed_addr,
        cluster: cli.cluster,
        slots: plan.slots.clone(),
        conns: Vec::new(),
        timeout,
        auth: cli.auth.clone(),
    };
    let mut rng = Rng::new(cli.seed.wrapping_mul(0x2545_F491_4F6C_DD1D) ^ u64::from(idx));
    let mut value = vec![0u8; cli.value_size];
    for b in &mut value {
        *b = b'a' + (rng.below(26) as u8);
    }
    let mut st = Stats::new();
    let depth = cli.pipeline.max(1) as usize;
    // open loop: this connection's share of the rate, in batches per second
    let batch_interval = cli.rate.filter(|r| *r > 0).map(|r| {
        let per_conn = r as f64 / f64::from(cli.conns.max(1));
        Duration::from_secs_f64(depth as f64 / per_conn)
    });
    let mut next_start = Instant::now();
    let mut ops: Vec<Op> = Vec::with_capacity(depth);
    let mut key = Vec::with_capacity(64);
    let mut groups: Vec<(SocketAddr, Vec<usize>, Vec<u8>)> = Vec::new();
    while !plan.stop.load(Ordering::Relaxed) && Instant::now() < end {
        ops.clear();
        if cli.load {
            for _ in 0..depth {
                let k = plan.next_load.fetch_add(1, Ordering::Relaxed);
                if k >= cli.keys {
                    break;
                }
                ops.push(Op {
                    kind: Kind::Set,
                    key: k,
                });
            }
            if ops.is_empty() {
                break;
            }
        } else {
            for _ in 0..depth {
                let kind = if (rng.below(100) as u32) < plan.read_pct {
                    Kind::Get
                } else {
                    Kind::Set
                };
                ops.push(Op {
                    kind,
                    key: plan.dist.next(&mut rng, cli.keys.max(1)),
                });
            }
        }
        let intended = match batch_interval {
            Some(iv) => {
                let now = Instant::now();
                if next_start > now {
                    std::thread::sleep(next_start - now);
                }
                let t = next_start;
                next_start += iv;
                t
            }
            None => Instant::now(),
        };
        // group the batch by target server, one buffer each
        for g in &mut groups {
            g.1.clear();
            g.2.clear();
        }
        for (i, op) in ops.iter().enumerate() {
            key_of(&cli.prefix, op.key, &mut key);
            let t = route.target(&key);
            let gi = match groups.iter().position(|g| g.0 == t) {
                Some(gi) => gi,
                None => {
                    groups.push((t, Vec::new(), Vec::new()));
                    groups.len() - 1
                }
            };
            let g = &mut groups[gi];
            g.1.push(i);
            match op.kind {
                Kind::Get => kv_resp::encode_command(&[b"GET", &key], &mut g.2),
                Kind::Set => kv_resp::encode_command(&[b"SET", &key, &value], &mut g.2),
            }
        }
        let mut redo: Vec<usize> = Vec::new();
        let mut refresh = false;
        for g in groups.iter().filter(|g| !g.1.is_empty()) {
            let ci = match route.conn(g.0) {
                Ok(ci) => ci,
                Err(e) => {
                    for _ in &g.1 {
                        st.error(|| format!("connect {}: {e}", g.0));
                    }
                    refresh = true;
                    std::thread::sleep(Duration::from_millis(20));
                    continue;
                }
            };
            let c = &mut route.conns[ci].1;
            if let Err(e) = c.s.write_all(&g.2) {
                for _ in &g.1 {
                    st.error(|| format!("write {}: {e}", g.0));
                }
                route.drop_conn(g.0);
                refresh = true;
                continue;
            }
            let mut failed = false;
            for &i in &g.1 {
                let r = match route.conns.iter_mut().find(|(a, _)| *a == g.0) {
                    Some((_, c)) => c.read_reply(),
                    None => Err(std::io::Error::other("connection dropped")),
                };
                let done = Instant::now();
                match r {
                    Ok(Reply::Err(e))
                        if cli.cluster && (e.starts_with(b"MOVED") || e.starts_with(b"ASK")) =>
                    {
                        st.retries += 1;
                        refresh = true;
                        redo.push(i);
                    }
                    Ok(Reply::Err(e))
                        if e.starts_with(b"TRYAGAIN")
                            || e.starts_with(b"CLUSTERDOWN")
                            || e.starts_with(b"LOADING") =>
                    {
                        st.retries += 1;
                        redo.push(i);
                    }
                    Ok(Reply::Err(e)) => st.error(|| String::from_utf8_lossy(&e).into_owned()),
                    Ok(r) => {
                        if done >= measure_from {
                            let us = done.saturating_duration_since(intended).as_micros() as u64;
                            match ops[i].kind {
                                Kind::Get => {
                                    st.get.record_us(us);
                                    st.gets += 1;
                                    if r == Reply::Nil {
                                        st.misses += 1;
                                    }
                                }
                                Kind::Set => {
                                    st.set.record_us(us);
                                    st.sets += 1;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        st.error(|| format!("read {}: {e}", g.0));
                        failed = true;
                        break;
                    }
                }
            }
            if failed {
                // the rest of this group's replies are lost with the connection
                route.drop_conn(g.0);
                refresh = true;
            }
        }
        if refresh {
            route.refresh();
        }
        if !redo.is_empty() {
            std::thread::sleep(Duration::from_millis(5));
            // resend once, synchronously, with the refreshed map
            for i in redo {
                key_of(&cli.prefix, ops[i].key, &mut key);
                let t = route.target(&key);
                let r = route.conn(t).and_then(|ci| {
                    let c = &mut route.conns[ci].1;
                    let mut out = Vec::new();
                    match ops[i].kind {
                        Kind::Get => kv_resp::encode_command(&[b"GET", &key], &mut out),
                        Kind::Set => kv_resp::encode_command(&[b"SET", &key, &value], &mut out),
                    }
                    c.s.write_all(&out)?;
                    c.read_reply()
                });
                match r {
                    Ok(Reply::Err(e)) => st.error(|| String::from_utf8_lossy(&e).into_owned()),
                    Ok(_) => {
                        let done = Instant::now();
                        if done >= measure_from {
                            let us = done.saturating_duration_since(intended).as_micros() as u64;
                            match ops[i].kind {
                                Kind::Get => {
                                    st.get.record_us(us);
                                    st.gets += 1;
                                }
                                Kind::Set => {
                                    st.set.record_us(us);
                                    st.sets += 1;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        st.error(|| format!("{t}: {e}"));
                        route.drop_conn(t);
                    }
                }
            }
        }
    }
    st
}

// ---------------------------------------------------------------------------------------------
// report

const PCTS: [f64; 6] = [50.0, 90.0, 99.0, 99.9, 99.99, 100.0];

fn pct_line(h: &Hist) -> String {
    let v: Vec<String> = PCTS
        .iter()
        .map(|p| {
            let us = if *p >= 100.0 {
                h.max()
            } else {
                h.percentile(*p)
            };
            format!(
                "p{}={:.3}ms",
                if *p >= 100.0 {
                    "max".into()
                } else {
                    p.to_string()
                },
                us as f64 / 1000.0
            )
        })
        .collect();
    v.join(" ")
}

fn pct_json(h: &Hist) -> String {
    let v: Vec<String> = PCTS
        .iter()
        .map(|p| {
            let us = if *p >= 100.0 {
                h.max()
            } else {
                h.percentile(*p)
            };
            let name = if *p >= 100.0 {
                "max".to_string()
            } else {
                format!("p{p}")
            };
            format!("\"{name}_us\":{us}")
        })
        .collect();
    v.join(",")
}

fn hdr_table(name: &str, h: &Hist, out: &mut String) {
    use std::fmt::Write as _;
    let _ = writeln!(
        out,
        "# {name}: {} samples, mean {:.1} us",
        h.count(),
        h.mean()
    );
    let _ = writeln!(
        out,
        "{:>12} {:>12} {:>14}",
        "value_us", "percentile", "1/(1-p)"
    );
    // halving steps toward 100%: 0, 50, 75, 87.5, ... (the usual HDR percentile spacing)
    let mut p = 0.0f64;
    let mut step = 50.0f64;
    while p < 99.9999 {
        let v = h.percentile(p);
        let inv = if p < 100.0 {
            1.0 / (1.0 - p / 100.0)
        } else {
            f64::INFINITY
        };
        let _ = writeln!(out, "{v:>12} {:>12.6} {inv:>14.2}", p / 100.0);
        p += step;
        step /= 2.0;
    }
    let _ = writeln!(out, "{:>12} {:>12.6} {:>14}", h.max(), 1.0, "inf");
}

fn run(cli: Cli) -> Result<i32, String> {
    if cli.keys == 0 {
        return Err("--keys must be > 0".into());
    }
    if cli.conns == 0 {
        return Err("--conns must be > 0".into());
    }
    let read_pct = parse_ratio(&cli.ratio)?;
    let dist = if cli.load {
        Dist::Uniform
    } else {
        Dist::parse(&cli.dist, cli.keys)?
    };
    let seed_addr = resolve(&cli.host, cli.port)?;
    let timeout = Duration::from_millis(cli.timeout_ms.max(1));
    let slots = if cli.cluster {
        let m = fetch_slots(seed_addr, timeout, cli.auth.as_deref())?;
        if m.iter().all(Option::is_none) {
            return Err("the cluster map has no assigned slot".into());
        }
        m
    } else {
        // one probe so a wrong address fails fast with a clear message
        let mut c = Conn::open(seed_addr, timeout, cli.auth.as_deref())
            .map_err(|e| format!("{seed_addr}: {e}"))?;
        c.call(&[b"PING"])
            .map_err(|e| format!("{seed_addr}: {e}"))?;
        Vec::new()
    };
    let plan = Arc::new(Plan {
        cli: cli.clone(),
        dist,
        read_pct,
        seed_addr,
        slots,
        stop: AtomicBool::new(false),
        next_load: AtomicU64::new(0),
    });
    let start = Instant::now();
    let (measure_from, end) = if cli.load {
        // a load has no warm-up and ends when every key is written (bounded by a generous cap)
        (
            start,
            start + Duration::from_secs(cli.duration.max(1).saturating_mul(1000)),
        )
    } else {
        let m = start + Duration::from_secs(cli.warmup);
        (m, m + Duration::from_secs(cli.duration.max(1)))
    };
    let handles: Vec<_> = (0..cli.conns)
        .map(|i| {
            let p = plan.clone();
            std::thread::Builder::new()
                .name(format!("kvbench-{i}"))
                .spawn(move || worker(p, i, measure_from, end))
                .map_err(|e| format!("spawn: {e}"))
        })
        .collect::<Result<_, _>>()?;
    // the main thread is the watchdog: workers end at `end`; a worker stuck past it (a socket
    // timeout is the most it can wait) is not waited for beyond the slack
    let slack = timeout * 3 + Duration::from_secs(2);
    let mut total = Stats::new();
    let mut stuck = 0;
    for h in handles {
        loop {
            if h.is_finished() {
                match h.join() {
                    Ok(s) => total.merge(&s),
                    Err(_) => total.error(|| "a worker thread panicked".into()),
                }
                break;
            }
            if Instant::now() > end + slack {
                plan.stop.store(true, Ordering::Relaxed);
                stuck += 1;
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    let elapsed = if cli.load {
        start.elapsed()
    } else {
        Instant::now()
            .min(end)
            .saturating_duration_since(measure_from)
    };
    let secs = elapsed.as_secs_f64().max(1e-9);
    let ops = total.gets + total.sets;
    let mut all = total.get.clone();
    all.merge(&total.set);
    if cli.json {
        println!(
            "{{\"mode\":\"{}\",\"conns\":{},\"pipeline\":{},\"seconds\":{:.3},\"ops\":{},\"ops_per_s\":{:.1},\"gets\":{},\"sets\":{},\"misses\":{},\"errors\":{},\"retries\":{},\"stuck_threads\":{},\"all\":{{{}}},\"get\":{{{}}},\"set\":{{{}}}}}",
            if cli.load {
                "load"
            } else if cli.rate.is_some() {
                "open"
            } else {
                "closed"
            },
            cli.conns,
            cli.pipeline,
            secs,
            ops,
            ops as f64 / secs,
            total.gets,
            total.sets,
            total.misses,
            total.errors,
            total.retries,
            stuck,
            pct_json(&all),
            pct_json(&total.get),
            pct_json(&total.set)
        );
    } else {
        println!(
            "kvbench {}: {} conns x pipeline {}, {:.1} s measured, {} ops, {:.0} ops/s",
            if cli.load {
                "load"
            } else if cli.rate.is_some() {
                "open loop"
            } else {
                "closed loop"
            },
            cli.conns,
            cli.pipeline,
            secs,
            ops,
            ops as f64 / secs
        );
        if !cli.load {
            println!("  all  {}", pct_line(&all));
            println!(
                "  GET  {:>10} ops ({:.0}/s, {} misses)  {}",
                total.gets,
                total.gets as f64 / secs,
                total.misses,
                pct_line(&total.get)
            );
        }
        println!(
            "  SET  {:>10} ops ({:.0}/s)  {}",
            total.sets,
            total.sets as f64 / secs,
            pct_line(&total.set)
        );
        println!(
            "  errors {} retries {} stuck threads {}",
            total.errors, total.retries, stuck
        );
        if let Some(e) = &total.first_error {
            println!("  first error: {e}");
        }
    }
    if let Some(path) = &cli.hdr_out {
        let mut s = String::new();
        hdr_table("all", &all, &mut s);
        hdr_table("GET", &total.get, &mut s);
        hdr_table("SET", &total.set, &mut s);
        std::fs::write(path, s).map_err(|e| format!("{}: {e}", path.display()))?;
    }
    Ok(if total.errors > 0 || stuck > 0 { 3 } else { 0 })
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
    match run(cli) {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            eprintln!("kvbench: {e}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reply_scanner() {
        assert_eq!(scan_reply(b"+OK\r\n").unwrap(), Some((Reply::Ok, 5)));
        assert_eq!(scan_reply(b"+OK\r").unwrap(), None);
        assert_eq!(scan_reply(b"$-1\r\n").unwrap(), Some((Reply::Nil, 5)));
        assert_eq!(
            scan_reply(b"$3\r\nabc\r\n+OK\r\n").unwrap(),
            Some((Reply::Ok, 9))
        );
        assert_eq!(scan_reply(b"$3\r\nab").unwrap(), None);
        assert_eq!(scan_reply(b"_\r\n").unwrap(), Some((Reply::Nil, 3)));
        assert_eq!(scan_reply(b":12\r\n").unwrap(), Some((Reply::Ok, 5)));
        assert_eq!(
            scan_reply(b"-MOVED 1 127.0.0.1:7000\r\n").unwrap(),
            Some((Reply::Err(b"MOVED 1 127.0.0.1:7000".to_vec()), 25))
        );
        assert_eq!(
            scan_reply(b"*1\r\n$1\r\na\r\n").unwrap(),
            Some((Reply::Ok, 11))
        );
        assert!(scan_reply(b"$x\r\n").is_err());
    }

    #[test]
    fn keys_and_ratio() {
        let mut k = Vec::new();
        key_of("key:", 42, &mut k);
        assert_eq!(k, b"key:0000000000000042");
        assert_eq!(parse_ratio("90:10").unwrap(), 90);
        assert_eq!(parse_ratio("100:0").unwrap(), 100);
        assert_eq!(parse_ratio("1:1").unwrap(), 50);
        assert!(parse_ratio("0:0").is_err());
        assert!(parse_ratio("x").is_err());
    }

    #[test]
    fn zipf_is_skewed_and_in_range() {
        let n = 10_000;
        let d = Dist::parse("zipf:0.99", n).unwrap();
        let mut r = Rng::new(7);
        let mut counts = vec![0u32; n as usize];
        for _ in 0..200_000 {
            let k = d.next(&mut r, n);
            assert!(k < n);
            counts[k as usize] += 1;
        }
        counts.sort_unstable_by(|a, b| b.cmp(a));
        let top: u32 = counts[..100].iter().sum();
        // the hottest 1% of keys take far more than 1% of the accesses
        assert!(top > 200_000 / 5, "top 1% = {top}");
        let u = Dist::parse("uniform", n).unwrap();
        let mut c = vec![0u32; n as usize];
        for _ in 0..200_000 {
            c[u.next(&mut r, n) as usize] += 1;
        }
        c.sort_unstable_by(|a, b| b.cmp(a));
        let top: u32 = c[..100].iter().sum();
        assert!(top < 200_000 / 50, "uniform top 1% = {top}");
        assert!(Dist::parse("zipf:1.5", n).is_err());
    }

    /// A tiny in-process RESP server answering SET/GET/PING, enough to drive a short run.
    #[test]
    fn short_run_against_a_stub_server() {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        std::thread::spawn(move || {
            for s in l.incoming().flatten() {
                std::thread::spawn(move || {
                    let mut s = s;
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 65536];
                    loop {
                        let n = match s.read(&mut chunk) {
                            Ok(0) | Err(_) => return,
                            Ok(n) => n,
                        };
                        buf.extend_from_slice(&chunk[..n]);
                        let mut out = Vec::new();
                        let mut p = kv_resp::Parser::new(kv_resp::Limits {
                            max_bulk: 1 << 20,
                            max_args: 16,
                            max_inline: 1024,
                        });
                        loop {
                            let mut args = Vec::new();
                            match p.parse(&buf, &mut args) {
                                Ok(kv_resp::Parsed::Command { consumed }) => {
                                    let (o, l) = args[0];
                                    let name =
                                        buf[o as usize..(o + l) as usize].to_ascii_uppercase();
                                    match name.as_slice() {
                                        b"GET" => out.extend_from_slice(b"$-1\r\n"),
                                        _ => out.extend_from_slice(b"+OK\r\n"),
                                    }
                                    buf.drain(..consumed);
                                }
                                Ok(kv_resp::Parsed::Empty { consumed }) => {
                                    buf.drain(..consumed);
                                }
                                _ => break,
                            }
                        }
                        if s.write_all(&out).is_err() {
                            return;
                        }
                    }
                });
            }
        });
        let cli = Cli::try_parse_from([
            "kvbench",
            "--port",
            &port.to_string(),
            "--keys",
            "1000",
            "--conns",
            "2",
            "--pipeline",
            "8",
            "--duration",
            "1",
            "--warmup",
            "0",
            "--value-size",
            "16",
        ])
        .unwrap();
        assert_eq!(run(cli).unwrap(), 0);
        let cli = Cli::try_parse_from([
            "kvbench",
            "--port",
            &port.to_string(),
            "--keys",
            "500",
            "--conns",
            "2",
            "--pipeline",
            "4",
            "--load",
        ])
        .unwrap();
        assert_eq!(run(cli).unwrap(), 0);
        let cli = Cli::try_parse_from([
            "kvbench",
            "--port",
            &port.to_string(),
            "--keys",
            "1000",
            "--conns",
            "1",
            "--rate",
            "2000",
            "--duration",
            "1",
            "--warmup",
            "0",
            "--json",
        ])
        .unwrap();
        assert_eq!(run(cli).unwrap(), 0);
    }
}
