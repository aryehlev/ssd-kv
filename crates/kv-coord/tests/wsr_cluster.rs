//! In-process 3-coordinator WSR group over real TCP and real state files (DESIGN §23.3):
//! election, commit, leader kill, restart from the A/B files, torn A/B write recovery.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use kv_coord::wsr::peers::{CoordRecord, PeerList, parse_coord_id};
use kv_coord::wsr::persist::{MIN_HALF_BYTES, STATE_FILE, StateFiles, Versioned};
use kv_coord::wsr::{Wsr, WsrCfg};
use kv_topo::{AdminOp, ClusterParams, ClusterState, Transition};

struct Member {
    dir: PathBuf,
    rec: CoordRecord,
    wsr: Option<Arc<Wsr>>,
}

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

fn group(name: &str, n: usize) -> Vec<Member> {
    let base = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("wsr-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let recs: Vec<CoordRecord> = (0..n)
        .map(|i| {
            let a = |p: u16| SocketAddr::from(([127, 0, 0, 1], p));
            CoordRecord::new(
                parse_coord_id(&format!("c{i}")).unwrap(),
                a(free_port()),
                a(free_port()),
                a(free_port()),
            )
        })
        .collect();
    let peers = PeerList {
        r#gen: 1,
        peers: recs.clone(),
    };
    recs.into_iter()
        .enumerate()
        .map(|(i, rec)| {
            let dir = base.join(format!("c{i}"));
            StateFiles::create_with_identity(&dir, MIN_HALF_BYTES * 4, Some(&rec), &peers).unwrap();
            let (mut f, _) = StateFiles::open(&dir).unwrap();
            // Each member starts from its own `kvc init` state (its own cluster id).
            let s = ClusterState::initial(ClusterParams::default(), [i as u8 + 1; 16]);
            f.write_state(
                &Versioned {
                    version: 1,
                    term: 0,
                    bytes: s.encode(),
                },
                None,
            )
            .unwrap();
            Member { dir, rec, wsr: None }
        })
        .collect()
}

fn cfg(rec: &CoordRecord) -> WsrCfg {
    WsrCfg {
        id: rec.id,
        peer_listen: rec.peer,
        election_timeout_ms: (150, 300),
        beat_ms: 30,
        auth: b"cluster-token".to_vec(),
        topology_fresh_ms: 2000,
    }
}

async fn start(m: &mut Member) {
    let dir = m.dir.clone();
    let (files, _) = tokio::task::spawn_blocking(move || StateFiles::open(&dir))
        .await
        .unwrap()
        .unwrap();
    let l = tokio::net::TcpListener::bind(m.rec.peer).await.unwrap();
    m.wsr = Some(Wsr::start_with_listener(cfg(&m.rec), files, l).await.unwrap());
}

async fn stop(m: &mut Member) {
    if let Some(w) = m.wsr.take() {
        w.shutdown().await;
    }
}

/// Waits for a single leader among the running members; returns its index.
async fn leader(ms: &[Member]) -> usize {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let leaders: Vec<usize> = ms
            .iter()
            .enumerate()
            .filter(|(_, m)| m.wsr.as_ref().is_some_and(|w| w.is_leader()))
            .map(|(i, _)| i)
            .collect();
        if let [one] = leaders.as_slice() {
            // It must also be able to commit (its bootstrap value is committed).
            if ms[*one].wsr.as_ref().unwrap().committed().version >= 2 {
                return *one;
            }
        }
        assert!(Instant::now() < deadline, "no single leader: {leaders:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn propose_lease(ms: &[Member], lease_ms: u32) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let l = leader(ms).await;
        let w = ms[l].wsr.as_ref().unwrap();
        let t = Transition::Admin(AdminOp::ParamSet {
            key: "lease_ms".into(),
            value: lease_ms.to_string(),
        });
        match w.propose(vec![t]).await {
            Ok(v) => return v,
            Err(e) => {
                assert!(Instant::now() < deadline, "proposal failed: {e}");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

/// Waits until every running member has committed at least `version`.
async fn converge(ms: &[Member], version: u64) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let done = ms
            .iter()
            .filter_map(|m| m.wsr.as_ref())
            .all(|w| w.committed().version >= version);
        if done {
            return;
        }
        assert!(Instant::now() < deadline, "members did not converge on version {version}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn state_of(m: &Member) -> Arc<ClusterState> {
    m.wsr.as_ref().unwrap().committed()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn elect_and_commit() {
    let mut ms = group("elect", 3);
    for m in &mut ms {
        start(m).await;
    }
    let l = leader(&ms).await;
    let v = propose_lease(&ms, 1600).await;
    converge(&ms, v).await;
    let ids: Vec<[u8; 16]> = ms.iter().map(|m| state_of(m).cluster_id).collect();
    assert!(ids.windows(2).all(|w| w[0] == w[1]), "one cluster identity after the first commit");
    for m in &ms {
        let s = state_of(m);
        assert_eq!(s.params.lease_ms, 1600);
        assert_eq!(s.version, v);
    }
    // followers know the leader and serve topology
    for (i, m) in ms.iter().enumerate() {
        let w = m.wsr.as_ref().unwrap();
        assert_eq!(w.leader_id(), Some(ms[l].rec.id));
        assert!(w.serving_topology());
        assert_eq!(w.is_leader(), i == l);
    }
    // a follower refuses proposals
    let f = (l + 1) % 3;
    assert!(ms[f].wsr.as_ref().unwrap().propose(vec![]).await.is_err());
    // batches proposed together commit in one or more versions, in order
    let w = ms[l].wsr.as_ref().unwrap().clone();
    let mk = |v: u32| {
        vec![Transition::Admin(AdminOp::ParamSet {
            key: "lease_ms".into(),
            value: v.to_string(),
        })]
    };
    let (a, b) = tokio::join!(w.propose(mk(1700)), w.propose(mk(1800)));
    let (a, b) = (a.unwrap(), b.unwrap());
    assert!(a <= b);
    converge(&ms, b).await;
    assert_eq!(state_of(&ms[f]).params.lease_ms, 1800);
    for m in &mut ms {
        stop(m).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_kill_and_restart_from_files() {
    let mut ms = group("kill", 3);
    for m in &mut ms {
        start(m).await;
    }
    let v1 = propose_lease(&ms, 1600).await;
    converge(&ms, v1).await;
    let old = leader(&ms).await;
    let old_term = ms[old].wsr.as_ref().unwrap().term();
    stop(&mut ms[old]).await;
    // the other two elect a new leader of a higher term and keep committing
    let v2 = propose_lease(&ms, 1700).await;
    let l = leader(&ms).await;
    assert_ne!(l, old);
    assert!(ms[l].wsr.as_ref().unwrap().term() > old_term);
    // the killed leader restarts from its A/B files and catches up as a follower
    start(&mut ms[old]).await;
    assert!(state_of(&ms[old]).version >= v1, "its committed state survived the restart");
    converge(&ms, v2).await;
    assert_eq!(state_of(&ms[old]).params.lease_ms, 1700);
    // stop everything, restart everything: the committed state is never lost
    for m in &mut ms {
        stop(m).await;
    }
    for m in &mut ms {
        start(m).await;
    }
    let v3 = propose_lease(&ms, 1900).await;
    assert!(v3 > v2);
    converge(&ms, v3).await;
    for m in &ms {
        assert_eq!(state_of(m).params.lease_ms, 1900);
    }
    for m in &mut ms {
        stop(m).await;
    }
}

/// Byte offset of the half holding the newest valid value.
fn newest_half(dir: &Path) -> (usize, usize) {
    let data = std::fs::read(dir.join(STATE_FILE)).unwrap();
    let half = data.len() / 2;
    let seq = |i: usize| u64::from_le_bytes(data[i * half + 0x10..i * half + 0x18].try_into().unwrap());
    (if seq(0) >= seq(1) { 0 } else { half }, half)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn torn_state_write_recovery() {
    let mut ms = group("torn", 3);
    for m in &mut ms {
        start(m).await;
    }
    let v1 = propose_lease(&ms, 1600).await;
    converge(&ms, v1).await;
    let l = leader(&ms).await;
    let f = (l + 1) % 3;
    stop(&mut ms[f]).await;
    let v2 = propose_lease(&ms, 1700).await;
    let v3 = propose_lease(&ms, 1800).await;
    assert!(v3 > v2);
    // Tear the follower's newest half (a power cut in the middle of its last state write).
    let (off, _) = newest_half(&ms[f].dir);
    let path = ms[f].dir.join(STATE_FILE);
    let mut data = std::fs::read(&path).unwrap();
    for b in &mut data[off + 0x20..off + 0x20 + 512] {
        *b = !*b;
    }
    std::fs::write(&path, data).unwrap();
    let (_, p) = StateFiles::open(&ms[f].dir).unwrap();
    assert!(p.committed.version >= 1 && p.committed.version <= v1, "the previous half is used");
    // It restarts from the surviving half and the leader brings it up to date.
    start(&mut ms[f]).await;
    converge(&ms, v3).await;
    assert_eq!(state_of(&ms[f]).params.lease_ms, 1800);
    assert_eq!(state_of(&ms[f]).encode(), state_of(&ms[l]).encode());
    for m in &mut ms {
        stop(m).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn setpeers_add_then_remove() {
    // Start with two of three records listed (generation 1 = {c0, c1}); add c2, then remove c0.
    let mut ms = group("peers", 3);
    let all: Vec<CoordRecord> = ms.iter().map(|m| m.rec.clone()).collect();
    for (i, m) in ms.iter_mut().enumerate() {
        let (mut f, p) = StateFiles::open(&m.dir).unwrap();
        let list = if i < 2 {
            PeerList {
                r#gen: 1,
                peers: all[..2].to_vec(),
            }
        } else {
            PeerList::joining()
        };
        f.write_meta(p.term, None, &list).unwrap();
        start(m).await;
    }
    let v = propose_lease(&ms, 1600).await;
    converge(&ms[..2], v).await;
    let g2 = PeerList {
        r#gen: 2,
        peers: all.clone(),
    };
    for m in &ms {
        m.wsr.as_ref().unwrap().set_peers(g2.clone()).await.unwrap();
    }
    // the joiner receives the committed state and votes from now on
    let v = propose_lease(&ms, 1700).await;
    converge(&ms, v).await;
    assert_eq!(state_of(&ms[2]).params.lease_ms, 1700);
    // a two-server change is refused
    let bad = PeerList {
        r#gen: 3,
        peers: all[..1].to_vec(),
    };
    assert!(ms[0].wsr.as_ref().unwrap().set_peers(bad).await.is_err());
    let g3 = PeerList {
        r#gen: 3,
        peers: all[1..].to_vec(),
    };
    for m in &ms {
        m.wsr.as_ref().unwrap().set_peers(g3.clone()).await.unwrap();
    }
    stop(&mut ms[0]).await;
    let v = propose_lease(&ms, 1800).await;
    converge(&ms, v).await;
    assert_eq!(state_of(&ms[2]).params.lease_ms, 1800);
    for m in &mut ms {
        stop(m).await;
    }
}
