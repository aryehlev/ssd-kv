//! Multi-process cluster tests (DESIGN §23.7-§23.13, §25.6): the built `kvc` and `kvd`
//! binaries on localhost, each data node on its own pre-allocated data file.
//!
//! * `replication_failover_and_returning_member`: writes replicate to the replicas (a learner
//!   added after data exists catches up by a merge full sync); killing the primary (kill -9)
//!   promotes a replica with no acknowledged write lost, checked by a writer that records
//!   every acknowledged write; the old primary restarts and returns as a member (repair
//!   against the new primary) and serves the same data on READONLY reads.
//! * `slot_migration_under_load`: an operator migration of half the slots to a second shard
//!   while a writer runs; zero acknowledged writes lost, routing follows the cutover, the
//!   source purges the moved slots.
//!
//! The binaries are taken from `KVD_BIN` / `KVC_BIN`, else from the target directory of this
//! test (`cargo build -p kv-node -p kv-coord` first). Every wait has a deadline and a watchdog
//! ends the process if a test runs far longer than it should: a hang fails, it never blocks.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use kv_resp::Value;

const TOKEN: &str = "cluster-test-token";
const ADMIN: &str = "admin-test-token";
const PARTITIONS: u16 = 16;

/// Tests run one at a time (each starts several processes on a 4-CPU box).
static SERIAL: Mutex<()> = Mutex::new(());

fn bin(name: &str, env: &str) -> PathBuf {
    if let Ok(p) = std::env::var(env) {
        return PathBuf::from(p);
    }
    // target/<profile>/deps/cluster-<hash> -> target/<profile>/<name>
    let exe = std::env::current_exe().expect("test executable path");
    let dir = exe.parent().and_then(Path::parent).expect("target profile directory");
    let p = dir.join(name);
    assert!(p.exists(), "{} not found: build it first (cargo build -p kv-node -p kv-coord) or set {env}", p.display());
    p
}

/// A port below the ephemeral range, unique in this process and free right now.
fn free_port() -> u16 {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let start = (std::process::id() % 300) * 37;
    loop {
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let p = (20_000 + (start + n) % 12_000) as u16;
        if std::net::TcpListener::bind(("127.0.0.1", p)).is_ok() {
            return p;
        }
    }
}

fn wait_until(what: &str, secs: u64, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "timed out after {secs} s waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Ends the whole test process if a test runs past its limit (children are killed by the
/// harness's Drop on the normal path; here the process group goes down with us).
fn watchdog(limit: Duration, done: Arc<AtomicBool>, pids: Arc<Mutex<Vec<u32>>>) {
    std::thread::spawn(move || {
        let start = Instant::now();
        while start.elapsed() < limit {
            if done.load(Ordering::Acquire) {
                return;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        eprintln!("watchdog: the test exceeded {limit:?}; killing its processes");
        for pid in pids.lock().unwrap_or_else(|e| e.into_inner()).iter() {
            let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
        }
        std::process::exit(101);
    });
}

// ---------------------------------------------------------------------------------------------
// RESP client with timeouts

struct Conn {
    s: TcpStream,
    buf: Vec<u8>,
}

impl Conn {
    fn open(addr: SocketAddr) -> std::io::Result<Self> {
        let s = TcpStream::connect_timeout(&addr, Duration::from_millis(500))?;
        s.set_read_timeout(Some(Duration::from_secs(3)))?;
        s.set_write_timeout(Some(Duration::from_secs(3)))?;
        let _ = s.set_nodelay(true);
        Ok(Conn { s, buf: Vec::new() })
    }

    fn call(&mut self, args: &[&[u8]]) -> std::io::Result<Value> {
        let mut out = Vec::new();
        kv_resp::encode_command(args, &mut out);
        self.s.write_all(&out)?;
        loop {
            match kv_resp::decode_value(&self.buf) {
                Ok(Some((v, n))) => {
                    self.buf.drain(..n);
                    return Ok(v);
                }
                Ok(None) => {}
                Err(_) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "bad reply")),
            }
            let mut chunk = [0u8; 65536];
            let n = self.s.read(&mut chunk)?;
            if n == 0 {
                return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "closed"));
            }
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }

    fn call_str(&mut self, args: &[&str]) -> std::io::Result<Value> {
        let a: Vec<&[u8]> = args.iter().map(|x| x.as_bytes()).collect();
        self.call(&a)
    }
}

fn text(v: &Value) -> String {
    match v {
        Value::Simple(b) | Value::Bulk(Some(b)) | Value::Error(b) => String::from_utf8_lossy(b).into_owned(),
        Value::Int(i) => i.to_string(),
        Value::Bulk(None) | Value::Null => "(nil)".into(),
        Value::Array(Some(a)) => a.iter().map(text).collect::<Vec<_>>().join(" "),
        _ => String::new(),
    }
}

/// A client that routes keyed commands like a Redis-Cluster client: it starts at a
/// coordinator, follows MOVED, and retries TRYAGAIN / CLUSTERDOWN / connection errors a few
/// times within a short budget.
struct Routed {
    coords: Vec<SocketAddr>,
    route: hashbrown::HashMap<u16, SocketAddr>,
    conns: hashbrown::HashMap<SocketAddr, Conn>,
    next_coord: usize,
}

enum Outcome {
    Ok(Value),
    /// Not acknowledged (an error reply or no reply).
    Failed(String),
}

impl Routed {
    fn new(coords: Vec<SocketAddr>) -> Self {
        Routed { coords, route: hashbrown::HashMap::new(), conns: hashbrown::HashMap::new(), next_coord: 0 }
    }

    fn coord(&mut self) -> SocketAddr {
        self.next_coord = (self.next_coord + 1) % self.coords.len();
        self.coords[self.next_coord]
    }

    fn exec(&mut self, key: &[u8], args: &[&[u8]]) -> Outcome {
        let slot = kv_common::key_slot(key);
        let mut last = String::new();
        for attempt in 0..12 {
            let target = match self.route.get(&slot) {
                Some(a) => *a,
                None => self.coord(),
            };
            let conn = match self.conns.entry(target) {
                hashbrown::hash_map::Entry::Occupied(e) => e.into_mut(),
                hashbrown::hash_map::Entry::Vacant(e) => match Conn::open(target) {
                    Ok(c) => e.insert(c),
                    Err(err) => {
                        last = format!("connect {target}: {err}");
                        self.route.remove(&slot);
                        std::thread::sleep(Duration::from_millis(50));
                        continue;
                    }
                },
            };
            match conn.call(args) {
                Ok(Value::Error(e)) => {
                    let e = String::from_utf8_lossy(&e).into_owned();
                    if let Some(rest) = e.strip_prefix("MOVED ") {
                        if let Some(addr) = rest.split_whitespace().nth(1).and_then(|a| a.parse().ok()) {
                            self.route.insert(slot, addr);
                        }
                        last = e;
                        continue;
                    }
                    if e.starts_with("TRYAGAIN") || e.starts_with("CLUSTERDOWN") || e.starts_with("LOADING") {
                        // during a seal or a failover window: retry later at the same place
                        if e.starts_with("CLUSTERDOWN") {
                            self.route.remove(&slot);
                        }
                        last = e;
                        std::thread::sleep(Duration::from_millis(20 * (attempt + 1)));
                        continue;
                    }
                    return Outcome::Failed(e);
                }
                Ok(v) => return Outcome::Ok(v),
                Err(err) => {
                    // no reply: the write may or may not have taken effect (not acknowledged)
                    self.conns.remove(&target);
                    self.route.remove(&slot);
                    return Outcome::Failed(format!("{target}: {err}"));
                }
            }
        }
        Outcome::Failed(format!("too many redirects: {last}"))
    }
}

// ---------------------------------------------------------------------------------------------
// processes

struct Coord {
    child: Option<Child>,
    client: u16,
    admin: u16,
}

struct Node {
    child: Option<Child>,
    port: u16,
    ctrl: u16,
    repl: u16,
    dir: PathBuf,
    id: String,
}

struct Cluster {
    root: PathBuf,
    coords: Vec<Coord>,
    nodes: Vec<Node>,
    kvd: PathBuf,
    kvc: PathBuf,
    pids: Arc<Mutex<Vec<u32>>>,
    done: Arc<AtomicBool>,
    ok: bool,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for n in &mut self.nodes {
            if let Some(mut c) = n.child.take() {
                let _ = c.kill();
                let _ = c.wait();
            }
        }
        for c in &mut self.coords {
            if let Some(mut ch) = c.child.take() {
                let _ = ch.kill();
                let _ = ch.wait();
            }
        }
        self.done.store(true, Ordering::Release);
        if self.ok {
            let _ = std::fs::remove_dir_all(&self.root);
        } else {
            eprintln!("cluster test failed: logs kept in {}", self.root.display());
        }
    }
}

impl Cluster {
    fn start(name: &str, n_nodes: usize, limit: Duration) -> Self {
        let root = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("cluster-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let kvd = bin("kvd", "KVD_BIN");
        let kvc = bin("kvc", "KVC_BIN");
        let pids = Arc::new(Mutex::new(Vec::new()));
        let done = Arc::new(AtomicBool::new(false));
        watchdog(limit, done.clone(), pids.clone());
        let ports: Vec<(u16, u16, u16)> = (0..3).map(|_| (free_port(), free_port(), free_port())).collect();
        let list = ports.iter().enumerate().map(|(i, (p, c, a))| format!("c{i}=127.0.0.1:{p}/{c}/{a}")).collect::<Vec<_>>().join(",");
        let mut c = Cluster { root: root.clone(), coords: Vec::new(), nodes: Vec::new(), kvd, kvc, pids, done, ok: false };
        for (i, (_, client, admin)) in ports.iter().enumerate() {
            let dir = root.join(format!("c{i}"));
            let st = Command::new(&c.kvc)
                .args(["init", "--dir", dir.to_str().unwrap(), "--id", &format!("c{i}"), "--peers", &list, "--half-bytes", "1048576"])
                .stdout(Stdio::null())
                .status()
                .unwrap();
            assert!(st.success(), "kvc init");
            let log = root.join(format!("c{i}.log"));
            let child = Command::new(&c.kvc)
                .args(["--dir", dir.to_str().unwrap(), "--cluster-auth-token", TOKEN, "--admin-token", ADMIN, "--logfile", log.to_str().unwrap(), "--loglevel", "debug"])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::from(std::fs::File::create(root.join(format!("c{i}.err"))).unwrap()))
                .spawn()
                .unwrap();
            c.pids.lock().unwrap().push(child.id());
            c.coords.push(Coord { child: Some(child), client: *client, admin: *admin });
        }
        for i in 0..n_nodes {
            let dir = root.join(format!("n{i}"));
            c.nodes.push(Node { child: None, port: free_port(), ctrl: free_port(), repl: free_port(), dir, id: String::new() });
            c.spawn_node(i);
        }
        wait_until("a coordinator leader", 30, || c.admin_try(&["STATUS"]).is_ok_and(|s| s.contains("leader c")));
        for (k, v) in [("partitions", "16"), ("renew_ms", "50"), ("lease_ms", "600"), ("fail_grace_ms", "100")] {
            c.admin(&["PARAMS", "SET", k, v]);
        }
        for i in 0..n_nodes {
            c.wait_serving(i);
            c.register(i);
        }
        c
    }

    fn spawn_node(&mut self, i: usize) {
        let n = &self.nodes[i];
        let log = self.root.join(format!("n{i}.log"));
        let err = std::fs::OpenOptions::new().create(true).write(true).truncate(false).open(self.root.join(format!("n{i}.err"))).unwrap();
        let child = Command::new(&self.kvd)
            .args([
                "--bind",
                "127.0.0.1",
                "--port",
                &n.port.to_string(),
                "--data-dir",
                n.dir.to_str().unwrap(),
                "--data-size",
                "640mb",
                "--format-partitions",
                &PARTITIONS.to_string(),
                "--cluster-enabled",
                "yes",
                "--cluster-auth-token",
                TOKEN,
                "--cluster-control-port",
                &n.ctrl.to_string(),
                "--cluster-repl-port",
                &n.repl.to_string(),
                "--threads",
                "2",
                "--repl-backlog-size",
                "4mb",
                "--cache-size",
                "64mb",
                "--snapshot-region",
                "0",
                "--logfile",
                log.to_str().unwrap(),
                "--loglevel",
                "verbose",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(err))
            .spawn()
            .unwrap();
        self.pids.lock().unwrap().push(child.id());
        self.nodes[i].child = Some(child);
    }

    /// Whether node `i`'s diagnostics log holds `what`.
    fn log_contains(&self, i: usize, what: &str) -> bool {
        std::fs::read_to_string(self.root.join(format!("n{i}.log"))).is_ok_and(|s| s.contains(what))
    }

    fn wait_serving(&mut self, i: usize) {
        let addr = self.node_addr(i);
        let err = self.root.join(format!("n{i}.err"));
        let child = &mut self.nodes[i].child;
        wait_until(&format!("node {i} to listen"), 60, || {
            if let Some(c) = child.as_mut()
                && let Ok(Some(st)) = c.try_wait()
            {
                panic!("node {i} exited ({st}): {}", std::fs::read_to_string(&err).unwrap_or_default());
            }
            Conn::open(addr).and_then(|mut c| c.call_str(&["PING"])).is_ok()
        });
    }

    fn register(&mut self, i: usize) {
        let n = &self.nodes[i];
        let r = self.admin(&[
            "NODE",
            "ADD",
            &format!("127.0.0.1:{}", n.ctrl),
            "--client",
            &format!("127.0.0.1:{}", n.port),
            "--repl",
            &format!("127.0.0.1:{}", n.repl),
        ]);
        let id = r.split_whitespace().nth(2).expect("node id in the reply").to_owned();
        self.nodes[i].id = id;
    }

    fn kill_node(&mut self, i: usize) {
        if let Some(mut c) = self.nodes[i].child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    fn node_addr(&self, i: usize) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], self.nodes[i].port))
    }

    fn coord_clients(&self) -> Vec<SocketAddr> {
        self.coords.iter().map(|c| SocketAddr::from(([127, 0, 0, 1], c.client))).collect()
    }

    /// One admin command on any coordinator (followers relay to the leader).
    fn admin_try(&self, words: &[&str]) -> Result<String, String> {
        let mut last = String::new();
        for c in &self.coords {
            let addr = SocketAddr::from(([127, 0, 0, 1], c.admin));
            let Ok(mut conn) = Conn::open(addr) else { continue };
            if conn.call_str(&["AUTH", ADMIN]).is_err() {
                continue;
            }
            let mut args = vec!["KVADM"];
            args.extend_from_slice(words);
            match conn.call_str(&args) {
                Ok(Value::Error(e)) => last = String::from_utf8_lossy(&e).into_owned(),
                Ok(v) => return Ok(text(&v)),
                Err(e) => last = e.to_string(),
            }
        }
        Err(last)
    }

    /// An admin command that must succeed (retried while the cluster settles).
    fn admin(&self, words: &[&str]) -> String {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            match self.admin_try(words) {
                Ok(s) => return s,
                Err(e) => {
                    assert!(Instant::now() < deadline, "kvadm {words:?}: {e}");
                    std::thread::sleep(Duration::from_millis(200));
                }
            }
        }
    }

    fn status(&self) -> String {
        self.admin(&["STATUS"])
    }

    /// The `kvadm status` member line of node `i`.
    fn member_line(&self, i: usize) -> String {
        let s = self.status();
        let id = &self.nodes[i].id;
        s.lines().find(|l| l.trim_start().starts_with(id.as_str()) && (l.contains("primary") || l.contains("replica")) && !l.contains("ctrl")).unwrap_or("").to_owned()
    }

    fn wait_member(&self, i: usize, want: &str, secs: u64) {
        wait_until(&format!("node {i} to be '{want}'"), secs, || self.member_line(i).contains(want));
    }

    fn wait_primary(&self, i: usize, secs: u64) {
        wait_until(&format!("node {i} to be the shard primary"), secs, || {
            let l = self.member_line(i);
            l.contains(" primary ") && l.contains("isr")
        });
    }

    /// A READONLY read of `key` on node `i`.
    fn replica_get(&self, i: usize, key: &str) -> Option<String> {
        let mut c = Conn::open(self.node_addr(i)).ok()?;
        c.call_str(&["READONLY"]).ok()?;
        match c.call_str(&["GET", key]).ok()? {
            Value::Bulk(Some(b)) => Some(String::from_utf8_lossy(&b).into_owned()),
            Value::Bulk(None) | Value::Null => None,
            other => Some(format!("<{}>", text(&other))),
        }
    }
}

/// Writes unique keys until stopped and records every acknowledged write.
struct Writer {
    stop: Arc<AtomicBool>,
    acks: Arc<AtomicU32>,
    handle: Option<std::thread::JoinHandle<WriterResult>>,
}

/// Acknowledged `(key, value)` writes and the number of writes not acknowledged.
type WriterResult = (Vec<(String, String)>, usize);

/// Keys whose write was not acknowledged, per writer prefix (they may or may not exist, G6).
static UNACKED: Mutex<Vec<String>> = Mutex::new(Vec::new());

impl Writer {
    fn start(coords: Vec<SocketAddr>, prefix: &str, pause: Duration) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let acks = Arc::new(AtomicU32::new(0));
        let (s, n) = (stop.clone(), acks.clone());
        let prefix = prefix.to_owned();
        let handle = std::thread::spawn(move || {
            let mut r = Routed::new(coords);
            let mut acked = Vec::new();
            let mut failed = 0usize;
            let mut i = 0u64;
            while !s.load(Ordering::Acquire) {
                i += 1;
                let k = format!("{prefix}:{i}");
                let v = format!("value-{i}-{}", "x".repeat((i % 50) as usize));
                match r.exec(k.as_bytes(), &[b"SET", k.as_bytes(), v.as_bytes()]) {
                    Outcome::Ok(Value::Simple(ok)) if ok == b"OK" => {
                        acked.push((k, v));
                        n.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {
                        failed += 1;
                        UNACKED.lock().unwrap_or_else(|e| e.into_inner()).push(k);
                    }
                }
                if !pause.is_zero() {
                    std::thread::sleep(pause);
                }
            }
            (acked, failed)
        });
        Writer { stop, acks, handle: Some(handle) }
    }

    /// Lets the writer run until it has `min` acknowledged writes (the box may be busy).
    fn at_least(&self, min: u32, secs: u64) {
        wait_until(&format!("{min} acknowledged writes"), secs, || self.acks.load(Ordering::Relaxed) >= min);
    }

    fn finish(mut self) -> (Vec<(String, String)>, usize) {
        self.stop.store(true, Ordering::Release);
        self.handle.take().unwrap().join().unwrap()
    }
}

/// A setup write retried until it is acknowledged (the cluster may still settle: a node that
/// has not renewed yet is flagged failed when it is added and cleared at its first renewal).
fn set_settled(r: &mut Routed, k: &str, v: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match r.exec(k.as_bytes(), &[b"SET", k.as_bytes(), v.as_bytes()]) {
            Outcome::Ok(Value::Simple(ok)) if ok == b"OK" => return,
            Outcome::Ok(other) => panic!("SET {k}: {}", text(&other)),
            Outcome::Failed(e) => {
                assert!(Instant::now() < deadline, "SET {k} not acknowledged: {e}");
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

/// Every acknowledged write is readable through the cluster's routing.
fn verify_acked(coords: &[SocketAddr], acked: &[(String, String)]) {
    let mut r = Routed::new(coords.to_vec());
    let mut lost = Vec::new();
    for (k, v) in acked {
        match r.exec(k.as_bytes(), &[b"GET", k.as_bytes()]) {
            Outcome::Ok(Value::Bulk(Some(b))) if b == v.as_bytes() => {}
            Outcome::Ok(other) => lost.push(format!("{k}: {}", text(&other))),
            Outcome::Failed(e) => lost.push(format!("{k}: {e}")),
        }
    }
    assert!(lost.is_empty(), "{} of {} acknowledged writes lost, e.g. {:?}", lost.len(), acked.len(), &lost[..lost.len().min(5)]);
}

#[test]
fn replication_failover_and_returning_member() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut c = Cluster::start("failover", 3, Duration::from_secs(420));
    let coords = c.coord_clients();
    let (n0, n1) = (c.nodes[0].id.clone(), c.nodes[1].id.clone());
    // shard 1: n0 primary, all slots; n1 a replica (learner: full sync of an empty partition)
    c.admin(&["SHARD", "CREATE", "1", "--primary", &n0]);
    c.admin(&["SLOTS", "ASSIGN", "0-16383", "1"]);
    c.wait_primary(0, 30);
    c.admin(&["REPLICA", "ADD", "1", &n1]);
    c.wait_member(1, "in-sync isr", 60);

    // writes replicate to the replica
    let mut r = Routed::new(coords.clone());
    for i in 0..300 {
        set_settled(&mut r, &format!("base:{i}"), &format!("b{i}"));
    }
    wait_until("the base writes on the replica", 20, || (0..300).step_by(37).all(|i| c.replica_get(1, &format!("base:{i}")) == Some(format!("b{i}"))));

    // a learner added after the data exists catches up by a merge full sync, under writes
    let w = Writer::start(coords.clone(), "w1", Duration::from_millis(2));
    std::thread::sleep(Duration::from_millis(300));
    let n2 = c.nodes[2].id.clone();
    c.admin(&["REPLICA", "ADD", "1", &n2]);
    c.wait_member(2, "in-sync isr", 90);
    w.at_least(100, 60);
    let (acked1, failed1) = w.finish();
    assert!(acked1.len() > 50, "the writer made progress ({} acked, {failed1} failed)", acked1.len());
    for (k, v) in acked1.iter().step_by(17) {
        wait_until("an acknowledged write on the learner", 20, || c.replica_get(2, k).as_deref() == Some(v.as_str()));
    }
    assert_eq!(c.replica_get(2, "base:123").as_deref(), Some("b123"), "snapshot data on the learner");
    assert!(c.log_contains(2, "merge full sync complete"), "the learner caught up by a merge full sync");

    // failover: kill -9 the primary under load; no acknowledged write is lost
    let w = Writer::start(coords.clone(), "w2", Duration::from_millis(1));
    std::thread::sleep(Duration::from_millis(500));
    let t0 = Instant::now();
    c.kill_node(0);
    wait_until("a promotion", 60, || {
        let s = c.status();
        s.lines().any(|l| (l.contains(n1.as_str()) || l.contains(n2.as_str())) && l.contains(" primary ") && l.contains("isr") && !l.contains("ctrl"))
    });
    let promoted = if c.member_line(1).contains(" primary ") { 1 } else { 2 };
    eprintln!("promotion of node {promoted} committed {:?} after the kill", t0.elapsed());
    std::thread::sleep(Duration::from_millis(1500));
    w.at_least(100, 60);
    let (acked2, failed2) = w.finish();
    eprintln!("writer during failover: {} acked, {failed2} not acknowledged", acked2.len());
    assert!(!acked2.is_empty());
    verify_acked(&coords, &acked1);
    verify_acked(&coords, &acked2);
    // the writes acknowledged after the promotion reached the other ISR member too
    let other = 3 - promoted;
    let last = acked2.last().unwrap().clone();
    wait_until("the other replica follows the new primary", 30, || c.replica_get(other, &last.0).as_deref() == Some(last.1.as_str()));

    // the old primary returns as a member (repair against the new primary) and converges
    c.spawn_node(0);
    c.wait_serving(0);
    c.wait_member(0, "in-sync isr", 90);
    let mut r = Routed::new(coords.clone());
    let k = "after-return";
    set_settled(&mut r, k, "yes");
    wait_until("the returning member applies new writes", 20, || c.replica_get(0, k).as_deref() == Some("yes"));
    for (k, v) in acked2.iter().step_by(13).chain(acked1.iter().step_by(29)) {
        assert_eq!(c.replica_get(0, k).as_deref(), Some(v.as_str()), "the returning member holds {k}");
    }
    assert_eq!(c.replica_get(0, "base:7").as_deref(), Some("b7"));
    // writes that were never acknowledged converge: the returning member holds exactly what
    // the new primary holds for them (present or absent), never a divergent copy
    let unacked: Vec<String> = UNACKED.lock().unwrap_or_else(|e| e.into_inner()).iter().filter(|k| k.starts_with("w2:")).cloned().collect();
    let mut r = Routed::new(coords.clone());
    for k in &unacked {
        let at_primary = match r.exec(k.as_bytes(), &[b"GET", k.as_bytes()]) {
            Outcome::Ok(Value::Bulk(b)) => b.map(|b| String::from_utf8_lossy(&b).into_owned()),
            Outcome::Ok(other) => panic!("GET {k}: {}", text(&other)),
            Outcome::Failed(e) => panic!("GET {k}: {e}"),
        };
        assert_eq!(c.replica_get(0, k), at_primary, "unacknowledged write {k} converged");
    }
    assert!(c.log_contains(0, "repair complete"), "the returning member repaired against the new primary");
    assert!(!c.log_contains(0, "merge full sync started"), "no full sync for a returning member");

    // graceful handoff back to node 0 under writes (§23.12): the old primary drains to its
    // own barrier, relinquishes its lease and stays an in-sync member; nothing acknowledged
    // is lost
    let w = Writer::start(coords.clone(), "w3", Duration::from_millis(1));
    w.at_least(50, 60);
    let n0 = c.nodes[0].id.clone();
    c.admin(&["FAILOVER", "1", "--to", &n0]);
    c.wait_primary(0, 60);
    c.wait_member(promoted, "in-sync isr", 60);
    w.at_least(150, 60);
    let (acked3, failed3) = w.finish();
    eprintln!("writer during the handoff: {} acked, {failed3} not acknowledged", acked3.len());
    verify_acked(&coords, &acked3);
    verify_acked(&coords, &acked2);
    let last = acked3.last().unwrap().clone();
    wait_until("the demoted primary follows", 30, || c.replica_get(promoted, &last.0).as_deref() == Some(last.1.as_str()));
    c.ok = true;
}

#[test]
fn slot_migration_under_load() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut c = Cluster::start("migrate", 2, Duration::from_secs(300));
    let (n0, n1) = (c.nodes[0].id.clone(), c.nodes[1].id.clone());
    c.admin(&["SHARD", "CREATE", "1", "--primary", &n0]);
    c.admin(&["SLOTS", "ASSIGN", "0-16383", "1"]);
    c.admin(&["SHARD", "CREATE", "2", "--primary", &n1]);
    c.wait_primary(0, 30);
    c.wait_primary(1, 30);
    let coords = c.coord_clients();
    let mut r = Routed::new(coords.clone());
    for i in 0..400 {
        set_settled(&mut r, &format!("pre:{i}"), &format!("p{i}"));
    }
    let w = Writer::start(coords.clone(), "mig", Duration::from_millis(1));
    std::thread::sleep(Duration::from_millis(800));
    let out = c.admin(&["MIGRATE", "0-8191", "--to", "2"]);
    let id: String = out.split_whitespace().nth(2).unwrap_or("").to_owned();
    wait_until("the migration to finish", 120, || {
        c.admin_try(&["MIGRATIONS"]).is_ok_and(|m| m.lines().any(|l| l.starts_with(&format!("{id} ")) && l.contains("phase=done")))
    });
    // writes after the cutover go to the destination
    std::thread::sleep(Duration::from_millis(1000));
    w.at_least(200, 60);
    let (acked, failed) = w.finish();
    eprintln!("writer during migration: {} acked, {failed} not acknowledged", acked.len());
    assert!(acked.len() > 200, "the writer made progress");
    let pre: Vec<(String, String)> = (0..400).map(|i| (format!("pre:{i}"), format!("p{i}"))).collect();
    verify_acked(&coords, &pre);
    verify_acked(&coords, &acked);
    // the moved slots are served by the destination; the source purged them
    let moved: Vec<&(String, String)> = pre.iter().filter(|(k, _)| kv_common::key_slot(k.as_bytes()) <= 8191).collect();
    assert!(!moved.is_empty());
    let mut src = Conn::open(c.node_addr(0)).unwrap();
    let mut dst = Conn::open(c.node_addr(1)).unwrap();
    for (k, v) in moved.iter().take(40) {
        assert_eq!(text(&dst.call_str(&["GET", k]).unwrap()), *v, "destination serves {k}");
        let e = text(&src.call_str(&["GET", k]).unwrap());
        assert!(e.starts_with("MOVED"), "source redirects {k}: {e}");
    }
    // the source keeps only the slots it still owns (plus writes that were never acknowledged)
    let kept = pre.iter().chain(acked.iter()).filter(|(k, _)| kv_common::key_slot(k.as_bytes()) > 8191).count();
    wait_until("the source to purge the moved slots", 60, || {
        Conn::open(c.node_addr(0)).and_then(|mut cs| cs.call_str(&["DBSIZE"])).is_ok_and(|v| matches!(v, Value::Int(n) if (n as usize) <= kept + failed))
    });
    c.ok = true;
}
