//! A whole coordinator group in process: three `kvc` stacks (WSR, pusher, controllers, client
//! service, admin API) over real TCP, with scripted data nodes that speak the control protocol
//! (DESIGN §23.3-§23.5, §23.8-§23.13). Exercises registration, shard and replica setup, ISR
//! commits, assignment pushes, topology replies and redirects, failover by the static rule,
//! migration with cutover ordering, rebalance apply, admin relay from followers, leader loss,
//! export and recovery.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use kv_coord::config::{CoordConfig, ServeConfig};
use kv_coord::wsr::WsrCfg;
use kv_coord::wsr::persist::MIN_HALF_BYTES;
use kv_coord::{Coordinator, init_dir, recover_dir, start_with};
use kv_ctrl::{CoordToNode, Health, MigPhase, MigrationTask, NodeAssignment, NodeId, NodeToCoord, ShardId};
use kv_resp::Value;
use kv_topo::render::CoordLineStyle;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

const TOKEN: &str = "cluster-token";
const ADMIN: &str = "admin-token";

/// A port below the kernel's ephemeral range (so no `:0` bind can take it meanwhile), unique
/// in this process and free right now.
fn free_port() -> u16 {
    static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let start = (std::process::id() % 400) * 30;
    loop {
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let p = (20_000 + (start + n) % 12_000) as u16;
        if std::net::TcpListener::bind(("127.0.0.1", p)).is_ok() {
            return p;
        }
    }
}

fn base(name: &str) -> PathBuf {
    let d = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("e2e-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn serve_cfg(dir: &Path, style: CoordLineStyle) -> ServeConfig {
    ServeConfig {
        base: CoordConfig {
            dir: dir.to_path_buf(),
            ..CoordConfig::default()
        },
        bind_set: false,
        client_port_set: false,
        peer_port_set: false,
        admin_port_set: false,
        cluster_auth_token: TOKEN.into(),
        admin_token: ADMIN.into(),
        style,
    }
}

fn fast() -> WsrCfg {
    WsrCfg {
        election_timeout_ms: (150, 300),
        beat_ms: 30,
        ..WsrCfg::default()
    }
}

/// `kvc init` for `n` coordinators; returns their directories.
fn init_group(name: &str, n: usize) -> Vec<PathBuf> {
    let b = base(name);
    let list: Vec<String> = (0..n)
        .map(|i| format!("c{i}=127.0.0.1:{}/{}/{}", free_port(), free_port(), free_port()))
        .collect();
    let list = list.join(",");
    (0..n)
        .map(|i| {
            let dir = b.join(format!("c{i}"));
            init_dir(&dir, &format!("c{i}"), &list, (7000, 7200), false, Some(MIN_HALF_BYTES * 4)).unwrap();
            dir
        })
        .collect()
}

async fn wait_for<F: FnMut() -> bool>(what: &str, secs: u64, mut f: F) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn leader_of(cs: &[Option<Coordinator>]) -> Option<usize> {
    let l: Vec<usize> = cs
        .iter()
        .enumerate()
        .filter(|(_, c)| c.as_ref().is_some_and(|c| c.wsr.is_leader()))
        .map(|(i, _)| i)
        .collect();
    (l.len() == 1).then(|| l[0])
}

// ---------------------------------------------------------------------------------------------
// RESP helpers

async fn call(s: &mut TcpStream, buf: &mut Vec<u8>, args: &[&[u8]]) -> Value {
    let mut out = Vec::new();
    kv_resp::encode_command(args, &mut out);
    s.write_all(&out).await.unwrap();
    loop {
        if let Some((v, n)) = kv_resp::decode_value(buf).unwrap() {
            buf.drain(..n);
            return v;
        }
        let mut chunk = vec![0u8; 65536];
        let n = s.read(&mut chunk).await.unwrap();
        assert!(n > 0, "connection closed");
        buf.extend_from_slice(&chunk[..n]);
    }
}

async fn admin(addr: SocketAddr, words: &[&str]) -> Value {
    let mut s = TcpStream::connect(addr).await.unwrap();
    let mut buf = Vec::new();
    assert!(matches!(call(&mut s, &mut buf, &[b"AUTH", ADMIN.as_bytes()]).await, Value::Simple(_)));
    let mut args: Vec<&[u8]> = vec![b"KVADM"];
    args.extend(words.iter().map(|w| w.as_bytes()));
    call(&mut s, &mut buf, &args).await
}

fn text(v: &Value) -> String {
    match v {
        Value::Simple(b) | Value::Bulk(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        Value::Error(e) => panic!("error reply: {}", String::from_utf8_lossy(e)),
        other => panic!("unexpected reply {other:?}"),
    }
}

fn error(v: &Value) -> String {
    match v {
        Value::Error(e) => String::from_utf8_lossy(e).into_owned(),
        other => panic!("expected an error, got {other:?}"),
    }
}

async fn admin_ok(addr: SocketAddr, words: &[&str]) -> String {
    text(&admin(addr, words).await)
}

async fn client(addr: SocketAddr, args: &[&str]) -> Value {
    let mut s = TcpStream::connect(addr).await.unwrap();
    let mut buf = Vec::new();
    let a: Vec<&[u8]> = args.iter().map(|x| x.as_bytes()).collect();
    call(&mut s, &mut buf, &a).await
}

// ---------------------------------------------------------------------------------------------
// Scripted data node: control protocol only. Reports migration readiness as soon as its
// assignment gives it an outgoing task, and the seal as soon as an incoming task is in cutover.

#[derive(Default)]
struct NodeState {
    id: Option<NodeId>,
    incarnation: u64,
    assign: Option<NodeAssignment>,
    /// `(version, when)` of every assignment applied.
    applied: Vec<(u64, Instant)>,
    out: Option<mpsc::UnboundedSender<NodeToCoord>>,
    fences: Vec<u32>,
    grants: u64,
    reported: Vec<(u64, MigPhase)>,
    /// When an assignment first showed migration `id` in Cutover.
    cutover_at: Vec<(u64, Instant)>,
}

struct FakeNode {
    ctrl: SocketAddr,
    client: SocketAddr,
    repl: SocketAddr,
    st: Arc<Mutex<NodeState>>,
    tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

async fn read_frame(s: &mut tokio::net::tcp::OwnedReadHalf) -> Option<Vec<u8>> {
    let mut h = [0u8; 4];
    s.read_exact(&mut h).await.ok()?;
    let len = kv_ctrl::frame_len(h).ok()?;
    let mut b = vec![0u8; len];
    s.read_exact(&mut b).await.ok()?;
    Some(b)
}

/// Aborts the task when dropped (a killed connection takes its helpers with it).
struct Guard(tokio::task::JoinHandle<()>);

impl Drop for Guard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl FakeNode {
    async fn start() -> Self {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ctrl = l.local_addr().unwrap();
        let st = Arc::new(Mutex::new(NodeState {
            incarnation: rand::random(),
            ..NodeState::default()
        }));
        let tasks = Arc::new(Mutex::new(Vec::new()));
        let (st2, tasks2) = (st.clone(), tasks.clone());
        let accept = tokio::spawn(async move {
            loop {
                let Ok((s, _)) = l.accept().await else { return };
                let t = tokio::spawn(Self::conn(s, st2.clone()));
                tasks2.lock().unwrap().push(t);
            }
        });
        tasks.lock().unwrap().push(accept);
        FakeNode {
            ctrl,
            client: SocketAddr::from(([127, 0, 0, 1], free_port())),
            repl: SocketAddr::from(([127, 0, 0, 1], free_port())),
            st,
            tasks,
        }
    }

    async fn conn(s: TcpStream, st: Arc<Mutex<NodeState>>) {
        let (mut rd, mut wr) = s.into_split();
        let Some(body) = read_frame(&mut rd).await else { return };
        let Ok(CoordToNode::Hello { auth, .. }) = kv_ctrl::decode_body::<CoordToNode>(&body) else { return };
        let send = |m: &NodeToCoord| {
            let mut f = Vec::new();
            kv_ctrl::encode(m, &mut f);
            f
        };
        if auth != TOKEN.as_bytes() {
            let _ = wr.write_all(&send(&NodeToCoord::Error(kv_ctrl::NodeError::BadAuth))).await;
            return;
        }
        let (id, inc) = {
            let g = st.lock().unwrap();
            (g.id, g.incarnation)
        };
        let ack = NodeToCoord::HelloAck {
            node: id,
            incarnation: inc,
            set_uuid: [7; 16],
            partitions: 64,
            ready: true,
        };
        if wr.write_all(&send(&ack)).await.is_err() {
            return;
        }
        let (tx, mut rx) = mpsc::unbounded_channel::<NodeToCoord>();
        st.lock().unwrap().out = Some(tx.clone());
        let _writer = Guard(tokio::spawn(async move {
            while let Some(m) = rx.recv().await {
                if wr.write_all(&send(&m)).await.is_err() {
                    return;
                }
            }
        }));
        let st_r = st.clone();
        let tx_r = tx.clone();
        let _renew = Guard(tokio::spawn(async move {
            let mut seq = 0u64;
            loop {
                tokio::time::sleep(Duration::from_millis(50)).await;
                seq += 1;
                let (inc, seen) = {
                    let g = st_r.lock().unwrap();
                    (g.incarnation, g.assign.as_ref().map_or(0, |a| a.version))
                };
                let m = NodeToCoord::Renew {
                    seq,
                    incarnation: inc,
                    version_seen: seen,
                    health: Health {
                        ready: true,
                        ..Health::default()
                    },
                };
                if tx_r.send(m).is_err() {
                    return;
                }
            }
        }));
        while let Some(body) = read_frame(&mut rd).await {
            let Ok(m) = kv_ctrl::decode_body::<CoordToNode>(&body) else { break };
            let reply = {
                let mut g = st.lock().unwrap();
                match m {
                    CoordToNode::Assign(a) => {
                        let newer = g.assign.as_ref().is_none_or(|o| a.order_key() > o.order_key());
                        if newer {
                            g.id = Some(a.node_id);
                            g.applied.push((a.version, Instant::now()));
                            let v = a.version;
                            let mut reports = Vec::new();
                            if let Some(r) = &a.role {
                                for t in &r.migrations {
                                    if t.phase() == MigPhase::Cutover && !g.cutover_at.iter().any(|(i, _)| *i == t.id()) {
                                        g.cutover_at.push((t.id(), Instant::now()));
                                    }
                                    match t {
                                        MigrationTask::Out { id, phase: MigPhase::Copy, .. } => {
                                            reports.push((*id, MigPhase::Copy))
                                        }
                                        MigrationTask::In { id, phase: MigPhase::Cutover, .. } => {
                                            reports.push((*id, MigPhase::Cutover))
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            g.assign = Some(a);
                            for (id, ph) in reports {
                                if !g.reported.contains(&(id, ph)) {
                                    g.reported.push((id, ph));
                                    let m = if ph == MigPhase::Copy {
                                        NodeToCoord::MigrationReady { id }
                                    } else {
                                        NodeToCoord::SealDone { id }
                                    };
                                    let _ = tx.send(m);
                                }
                            }
                            Some(NodeToCoord::AssignAck { version: v })
                        } else {
                            Some(NodeToCoord::Error(kv_ctrl::NodeError::StaleAssignment))
                        }
                    }
                    CoordToNode::LeaseGrant { .. } => {
                        g.grants += 1;
                        None
                    }
                    CoordToNode::Fence { req, new_term, .. } => {
                        g.fences.push(new_term);
                        Some(NodeToCoord::FenceAck { req, applied: vec![9; 64] })
                    }
                    CoordToNode::Handoff { req, .. } => Some(NodeToCoord::HandoffDone { req, last: vec![9; 64] }),
                    _ => None,
                }
            };
            if let Some(r) = reply {
                let _ = tx.send(r);
            }
        }
    }

    fn send(&self, m: NodeToCoord) {
        let tx = self.st.lock().unwrap().out.clone();
        tx.expect("connected").send(m).unwrap();
    }

    fn assignment<R>(&self, f: impl FnOnce(&NodeAssignment) -> R) -> Option<R> {
        self.st.lock().unwrap().assign.as_ref().map(f)
    }

    fn is_primary(&self) -> bool {
        self.assignment(|a| a.role.as_ref().is_some_and(|r| r.is_primary)).unwrap_or(false)
    }

    /// The process dies: connections drop, renewals stop.
    fn kill(&self) {
        for t in self.tasks.lock().unwrap().drain(..) {
            t.abort();
        }
    }

    async fn add(&self, coord_admin: SocketAddr) -> NodeId {
        let r = admin_ok(
            coord_admin,
            &[
                "NODE",
                "ADD",
                &self.ctrl.to_string(),
                "--client",
                &self.client.to_string(),
                "--repl",
                &self.repl.to_string(),
                "--zone",
                "z1",
            ],
        )
        .await;
        let id = r.split_whitespace().nth(2).unwrap();
        NodeId::parse_hex40(id).unwrap()
    }
}

fn slots_reply_primary_port(v: &Value, slot: i64) -> Option<i64> {
    let Value::Array(Some(ranges)) = v else { return None };
    for r in ranges {
        let Value::Array(Some(e)) = r else { continue };
        let (Value::Int(a), Value::Int(b)) = (&e[0], &e[1]) else { continue };
        if (*a..=*b).contains(&slot) {
            let Value::Array(Some(p)) = &e[2] else { return None };
            if let Value::Int(port) = p[1] {
                return Some(port);
            }
        }
    }
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_group_end_to_end() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let dirs = init_group("main", 3);
    let mut cs: Vec<Option<Coordinator>> = Vec::new();
    for d in &dirs {
        cs.push(Some(start_with(&serve_cfg(d, CoordLineStyle::R1), fast()).await.unwrap()));
    }
    wait_for("a leader", 15, || leader_of(&cs).is_some()).await;
    let l = leader_of(&cs).unwrap();
    let f = (l + 1) % 3;
    let fa = cs[f].as_ref().unwrap().admin_addr;
    let fc = cs[f].as_ref().unwrap().client_addr;
    wait_for("follower contact", 10, || cs[f].as_ref().unwrap().wsr.serving_topology()).await;

    // Parameters through a follower (relayed to the leader).
    for (k, v) in [("renew_ms", "50"), ("lease_ms", "500"), ("fail_grace_ms", "100")] {
        assert!(admin_ok(fa, &["PARAMS", "SET", k, v]).await.starts_with("OK version"));
    }
    let n1 = FakeNode::start().await;
    let n2 = FakeNode::start().await;
    let n3 = FakeNode::start().await;
    let id1 = n1.add(fa).await;
    let id2 = n2.add(fa).await;
    let id3 = n3.add(fa).await;
    admin_ok(fa, &["SHARD", "CREATE", "1", "--primary", &id1.hex40()]).await;
    admin_ok(fa, &["SLOTS", "ASSIGN", "0-16383", "1"]).await;
    admin_ok(fa, &["REPLICA", "ADD", "1", &id2.hex40()[..8]]).await;
    let e = error(&admin(fa, &["REPLICA", "ADD", "1", "zz"]).await);
    assert!(e.contains("invalid node id"), "{e}");

    // Every node is dialed and gets its projection; leases are granted.
    wait_for("assignments", 10, || {
        n1.is_primary()
            && n2.assignment(|a| a.role.as_ref().is_some_and(|r| r.shard == ShardId(1) && !r.is_primary)) == Some(true)
            && n3.assignment(|a| a.role.is_none()) == Some(true)
    })
    .await;
    wait_for("lease grants", 10, || n1.st.lock().unwrap().grants > 0).await;

    // The learner finishes its sync (member ready) and the primary expands the ISR.
    n2.send(NodeToCoord::MemberReady { shard: ShardId(1), term: 1, member: id2 });
    let wsr = cs[l].as_ref().unwrap().wsr.clone();
    wait_for("catching up", 10, || {
        wsr.committed().shard(ShardId(1)).unwrap().member(&id2).unwrap().state == kv_ctrl::MemberState::CatchingUp
    })
    .await;
    // an ISR change from a non-primary is ignored; from the primary it commits
    n2.send(NodeToCoord::IsrExpand { shard: ShardId(1), term: 1, member: id2 });
    n1.send(NodeToCoord::IsrExpand { shard: ShardId(1), term: 1, member: id2 });
    wait_for("ISR expand", 10, || wsr.committed().shard(ShardId(1)).unwrap().member(&id2).unwrap().in_isr).await;

    // Topology from a follower: one range, primary then the in-sync replica.
    let mut slots = Value::Null;
    wait_for("follower topology", 10, || {
        cs[f].as_ref().unwrap().wsr.committed().version == wsr.committed().version
    })
    .await;
    for _ in 0..50 {
        slots = client(fc, &["CLUSTER", "SLOTS"]).await;
        if let Value::Array(Some(r)) = &slots {
            if r.len() == 1 && matches!(&r[0], Value::Array(Some(e)) if e.len() == 4) {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(slots_reply_primary_port(&slots, 0), Some(i64::from(n1.client.port())));
    let moved = error(&client(fc, &["GET", "foo"]).await);
    assert_eq!(moved, format!("MOVED 12182 127.0.0.1:{}", n1.client.port()));
    assert!(error(&client(fc, &["MGET", "a", "b"]).await).starts_with("CROSSSLOT"));
    let nodes = text(&client(fc, &["CLUSTER", "NODES"]).await);
    assert_eq!(nodes.lines().count(), 5, "{nodes}");
    assert_eq!(nodes.lines().filter(|x| x.contains("myself")).count(), 1);
    assert!(error(&client(fc, &["DBSIZE"]).await).starts_with("ERR unknown command"));
    let info = text(&client(fc, &["CLUSTER", "INFO"]).await);
    assert!(info.contains("cluster_state:ok"), "{info}");

    // Failover: the primary dies; the in-sync replica is fenced and promoted.
    let t0 = Instant::now();
    n1.kill();
    wait_for("promotion", 15, || {
        let s = wsr.committed();
        let sh = s.shard(ShardId(1)).unwrap();
        sh.primary == Some(id2) && sh.term == 2
    })
    .await;
    let took = t0.elapsed();
    assert_eq!(n2.st.lock().unwrap().fences, vec![2], "only the chosen node was fenced");
    assert!(n3.st.lock().unwrap().fences.is_empty());
    wait_for("new primary's assignment", 10, || n2.is_primary()).await;
    let s = wsr.committed();
    assert!(s.node(&id1).unwrap().flags.fail);
    assert_eq!(s.shard(ShardId(1)).unwrap().history.last().unwrap().takeover, vec![9; 64]);
    eprintln!("failover committed {took:?} after the primary died");
    let moved = error(&client(cs[l].as_ref().unwrap().client_addr, &["GET", "foo"]).await);
    assert_eq!(moved, format!("MOVED 12182 127.0.0.1:{}", n2.client.port()));

    // Migration: copy -> ready -> cutover (source first) -> seal -> done.
    admin_ok(fa, &["SHARD", "CREATE", "2", "--primary", &id3.hex40()]).await;
    let r = admin_ok(fa, &["MIGRATE", "0-99", "--to", "2", "--barriers", "1"]).await;
    let mig: u64 = r.split_whitespace().nth(2).unwrap().parse().unwrap();
    wait_for("migration done", 15, || {
        wsr.committed().migrations.iter().any(|m| m.id == mig && m.phase == MigPhase::Done)
    })
    .await;
    let s = wsr.committed();
    assert_eq!(s.slot_owner(0), Some(ShardId(2)));
    assert_eq!(s.slot_owner(100), Some(ShardId(1)));
    // The cutover reached the source primary before the destination's (§23.13 step 3).
    let at = |n: &FakeNode| n.st.lock().unwrap().cutover_at.iter().find(|(i, _)| *i == mig).map(|(_, t)| *t);
    let (src_t, dst_t) = (at(&n2).expect("source saw the cutover"), at(&n3).expect("destination saw it"));
    assert!(src_t <= dst_t, "cutover pushed to the destination first");
    let listing = admin_ok(fa, &["MIGRATIONS"]).await;
    assert!(listing.contains(&format!("{mig} slots=0-99")) && listing.contains("phase=done"), "{listing}");

    // Rebalance: plan from the state alone, applied one migration at a time.
    let plan = admin_ok(fa, &["REBALANCE", "PLAN"]).await;
    assert_eq!(plan.lines().count(), 1, "{plan}");
    assert!(admin_ok(fa, &["REBALANCE", "APPLY", &plan]).await.starts_with("OK applying 1"));
    wait_for("rebalance finished", 20, || {
        let s = wsr.committed();
        s.slot_ranges_of(ShardId(1)).slot_count() == 8192 && s.slot_ranges_of(ShardId(2)).slot_count() == 8192
    })
    .await;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let st = admin_ok(fa, &["REBALANCE", "STATUS"]).await;
        if st.contains("outcome finished") {
            assert!(st.contains("done 1/1"), "{st}");
            break;
        }
        assert!(Instant::now() < deadline, "{st}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Status (text and JSON) through the follower.
    let status = admin_ok(fa, &["STATUS"]).await;
    assert!(status.contains("shards (2)") && status.contains(&id2.hex40()), "{status}");
    let json = admin_ok(fa, &["STATUS", "--json"]).await;
    assert!(json.starts_with('{') && json.contains("\"assigned_slots\":16384"), "{json}");

    // Leader loss: a new leader takes over, re-dials the nodes and keeps serving.
    let old = l;
    let old_term = cs[old].as_ref().unwrap().wsr.term();
    let c = cs[old].take().unwrap();
    c.shutdown().await;
    wait_for("a new leader", 15, || leader_of(&cs).is_some()).await;
    let l2 = leader_of(&cs).unwrap();
    assert_ne!(l2, old);
    let wsr2 = cs[l2].as_ref().unwrap().wsr.clone();
    wait_for("nodes follow the new leader", 15, || {
        n2.assignment(|a| a.coord_term).unwrap() == wsr2.term() && wsr2.term() > old_term
    })
    .await;
    let a2 = cs[l2].as_ref().unwrap().admin_addr;
    let other = 3 - old - l2;
    let oa = cs[other].as_ref().unwrap().admin_addr;
    assert!(admin_ok(oa, &["NODE", "MAINTENANCE", &id3.hex40(), "on"]).await.starts_with("OK"));

    // Export from one coordinator; recover a new single-member group from it.
    let exp = match admin(a2, &["COORD", "EXPORT"]).await {
        Value::Bulk(Some(b)) => b,
        other => panic!("{other:?}"),
    };
    let rec_dir = base("recover");
    let file = rec_dir.join("state.export");
    std::fs::create_dir_all(&rec_dir).unwrap();
    std::fs::write(&file, &exp).unwrap();
    let entry = format!("r0=127.0.0.1:{}/{}/{}", free_port(), free_port(), free_port());
    let msg = recover_dir(&rec_dir.join("r0"), &file, Some("r0"), Some(&entry)).unwrap();
    assert!(msg.contains("single-member group"), "{msg}");
    let exported = kv_coord::wsr::persist::Export::decode(&exp).unwrap().version;
    for c in cs.iter_mut() {
        if let Some(c) = c.take() {
            c.shutdown().await;
        }
    }
    n2.kill();
    n3.kill();
    let r = start_with(&serve_cfg(&rec_dir.join("r0"), CoordLineStyle::R1), fast()).await.unwrap();
    wait_for("recovered leader", 15, || r.wsr.is_leader()).await;
    let s = r.wsr.committed();
    assert!(s.version > exported);
    assert!(r.wsr.term() > 1 << 32);
    assert_eq!(s.slot_ranges_of(ShardId(2)).slot_count(), 8192);
    let peers = admin_ok(r.admin_addr, &["COORD", "PEERS"]).await;
    assert!(peers.starts_with("2 r0="), "{peers}");
    r.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_service_details() {
    // A single-member group with the R2 line style.
    let dirs = init_group("r2", 1);
    let c = start_with(&serve_cfg(&dirs[0], CoordLineStyle::R2), fast()).await.unwrap();
    wait_for("leader", 15, || c.wsr.is_leader() && c.wsr.committed().version >= 2).await;
    let ca = c.client_addr;
    assert_eq!(client(ca, &["DBSIZE"]).await, Value::Int(0));
    assert_eq!(client(ca, &["KEYS", "*"]).await, Value::Array(Some(vec![])));
    assert_eq!(client(ca, &["FLUSHALL"]).await, Value::Simple(b"OK".to_vec()));
    // no slots assigned: keyed commands are refused
    assert_eq!(error(&client(ca, &["GET", "x"]).await), "CLUSTERDOWN Hash slot not served");
    assert!(text(&client(ca, &["CLUSTER", "INFO"]).await).contains("cluster_state:fail"));
    assert_eq!(client(ca, &["CLUSTER", "KEYSLOT", "123456789"]).await, Value::Int(12739));
    assert!(error(&client(ca, &["CLUSTER", "COUNTKEYSINSLOT", "1"]).await).contains("stores no keys"));
    assert!(text(&client(ca, &["INFO", "server"]).await).contains("server_kind:coordinator"));
    let myid = text(&client(ca, &["CLUSTER", "MYID"]).await);
    assert_eq!(myid, c.wsr.id().hex40());
    assert!(text(&client(ca, &["CLUSTER", "NODES"]).await).contains("myself,master"));

    // One connection: HELLO 3, MULTI/EXEC, SELECT, pipelining.
    let mut s = TcpStream::connect(ca).await.unwrap();
    let mut buf = Vec::new();
    let hello = call(&mut s, &mut buf, &[b"HELLO", b"3"]).await;
    let Value::Map(m) = hello else { panic!("{hello:?}") };
    assert!(m.iter().any(|(k, v)| k == &Value::Bulk(Some(b"mode".to_vec())) && v == &Value::Bulk(Some(b"cluster".to_vec()))));
    assert!(m.iter().any(|(k, v)| k == &Value::Bulk(Some(b"role".to_vec())) && v == &Value::Bulk(Some(b"master".to_vec()))));
    assert_eq!(call(&mut s, &mut buf, &[b"MULTI"]).await, Value::Simple(b"OK".to_vec()));
    assert_eq!(call(&mut s, &mut buf, &[b"PING"]).await, Value::Simple(b"QUEUED".to_vec()));
    assert!(matches!(call(&mut s, &mut buf, &[b"SET", b"k", b"v"]).await, Value::Error(_)));
    assert!(error(&call(&mut s, &mut buf, &[b"EXEC"]).await).starts_with("EXECABORT"));
    assert!(error(&call(&mut s, &mut buf, &[b"SELECT", b"1"]).await).contains("out of range"));
    assert_eq!(call(&mut s, &mut buf, &[b"SELECT", b"0"]).await, Value::Simple(b"OK".to_vec()));
    assert_eq!(call(&mut s, &mut buf, &[b"RANDOMKEY"]).await, Value::Null);
    let mut out = Vec::new();
    kv_resp::encode_command(&[b"PING"], &mut out);
    kv_resp::encode_command(&[b"ECHO", b"hi"], &mut out);
    s.write_all(&out).await.unwrap();
    let mut got = Vec::new();
    while got.len() < 2 {
        if let Some((v, n)) = kv_resp::decode_value(&buf).unwrap() {
            buf.drain(..n);
            got.push(v);
            continue;
        }
        let mut chunk = vec![0u8; 1024];
        let n = s.read(&mut chunk).await.unwrap();
        buf.extend_from_slice(&chunk[..n]);
    }
    assert_eq!(got[1], Value::Bulk(Some(b"hi".to_vec())));

    // Client password: the coordinator asks for it like a data node.
    admin_ok(c.admin_addr, &["PARAMS", "SET", "requirepass", "s3cret"]).await;
    assert_eq!(error(&client(ca, &["CLUSTER", "SLOTS"]).await), "NOAUTH Authentication required.");
    let mut s = TcpStream::connect(ca).await.unwrap();
    let mut buf = Vec::new();
    assert!(error(&call(&mut s, &mut buf, &[b"AUTH", b"nope"]).await).starts_with("WRONGPASS"));
    assert_eq!(call(&mut s, &mut buf, &[b"AUTH", b"s3cret"]).await, Value::Simple(b"OK".to_vec()));
    assert!(matches!(call(&mut s, &mut buf, &[b"CLUSTER", b"SLOTS"]).await, Value::Array(_)));
    // admin errors and auth
    assert!(error(&admin(c.admin_addr, &["NODE", "REMOVE", "abcd"]).await).contains("no registered node"));
    let mut s = TcpStream::connect(c.admin_addr).await.unwrap();
    let mut buf = Vec::new();
    assert!(error(&call(&mut s, &mut buf, &[b"KVADM", b"STATUS"]).await).starts_with("NOAUTH"));
    assert!(error(&call(&mut s, &mut buf, &[b"AUTH", b"bad"]).await).starts_with("WRONGPASS"));
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn followers_without_a_leader_answer_clusterdown() {
    let dirs = init_group("down", 3);
    let mut cs: Vec<Option<Coordinator>> = Vec::new();
    for d in &dirs {
        cs.push(Some(start_with(&serve_cfg(d, CoordLineStyle::R1), fast()).await.unwrap()));
    }
    wait_for("a leader", 15, || leader_of(&cs).is_some()).await;
    let l = leader_of(&cs).unwrap();
    let f = (l + 1) % 3;
    wait_for("follower contact", 10, || cs[f].as_ref().unwrap().wsr.serving_topology()).await;
    for i in 0..3 {
        if i != f {
            cs[i].take().unwrap().shutdown().await;
        }
    }
    let c = cs[f].take().unwrap();
    wait_for("staleness", 10, || !c.wsr.serving_topology()).await;
    assert_eq!(error(&client(c.client_addr, &["CLUSTER", "SLOTS"]).await), "CLUSTERDOWN The cluster is down");
    assert_eq!(error(&client(c.client_addr, &["GET", "a"]).await), "CLUSTERDOWN The cluster is down");
    assert_eq!(client(c.client_addr, &["PING"]).await, Value::Simple(b"PONG".to_vec()));
    // admin verbs that need the leader say so; local ones still work
    assert!(error(&admin(c.admin_addr, &["SHARD", "DELETE", "1"]).await).starts_with("CLUSTERDOWN"));
    assert!(admin_ok(c.admin_addr, &["COORD", "PEERS"]).await.starts_with("1 c0="));
    assert!(admin_ok(c.admin_addr, &["STATUS"]).await.contains("follower"));
    c.shutdown().await;
}
