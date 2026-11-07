//! End-to-end tests of the runtime: a node with several io_uring workers over the in-RAM
//! backend, driven through real sockets with the `redis` client (and raw RESP where the client
//! would hide the wire format). They exercise accept/recv/send, parsing and pipelining,
//! routing to other workers, fan-out gathers, cross-partition transactions, SCAN-family steps,
//! node operations, backpressure-free shutdown and connection limits.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use kv_storage::metastore::{MetaDone, MetaReq};

use crate::config::Config;
use crate::runtime::backend::mem::MemBackend;
use crate::runtime::node::{NodeHandle, start_with};
use crate::runtime::xmsg::XMsg;
use crate::shared::NodeShared;

const PARTITIONS: u16 = 16;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

struct TestNode {
    node: Option<NodeHandle>,
    port: u16,
}

impl TestNode {
    fn start(threads: u16, tweak: impl FnOnce(&mut Config)) -> Self {
        let port = free_port();
        let mut cfg = Config { port, threads: Some(threads), ..Config::default() };
        tweak(&mut cfg);
        let node = start_with(cfg, PARTITIONS, move |w, shared: &Arc<NodeShared>| {
            let s = shared.clone();
            MemBackend::new(w, threads, PARTITIONS).with_meta(Box::new(move |p, token, r| {
                if let MetaReq::CommitTxn { txn } = r {
                    // the meta thread answers a commit, then resolves it (credits return)
                    let me = &s.workers[usize::from(w)];
                    me.send_side(XMsg::Meta { partition: p, token, done: MetaDone::Committed { txn } });
                    me.send_side(XMsg::Meta { partition: p, token, done: MetaDone::Resolved { credits: 1 } });
                }
            }))
        })
        .expect("node starts");
        // every partition of the in-RAM backend is ready at once
        for _ in 0..200 {
            if !node.shared.loading() {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        TestNode { node: Some(node), port }
    }

    fn client(&self) -> redis::Connection {
        let c = redis::Client::open(format!("redis://127.0.0.1:{}/", self.port)).unwrap();
        c.get_connection().unwrap()
    }

    fn raw(&self) -> TcpStream {
        let s = TcpStream::connect(("127.0.0.1", self.port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        s
    }

    fn stop(mut self) -> i32 {
        let n = self.node.take().unwrap();
        n.shared.request_shutdown();
        n.wait()
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        if let Some(n) = self.node.take() {
            n.shared.request_shutdown();
            n.wait();
        }
    }
}

/// Reads until `want` bytes arrived (or the peer closed).
fn read_exact_or_eof(s: &mut TcpStream, want: usize) -> Vec<u8> {
    let mut out = Vec::new();
    let mut buf = [0u8; 65536];
    while out.len() < want {
        match s.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => out.extend_from_slice(&buf[..n]),
        }
    }
    out
}

/// Two keys whose partitions are owned by different workers (W = 2).
fn keys_on_two_workers() -> (Vec<u8>, Vec<u8>) {
    let owner = |k: &[u8]| (kv_common::key_slot(k) % PARTITIONS) % 2;
    let a = (0..).map(|i| format!("ka{i}").into_bytes()).find(|k| owner(k) == 0).unwrap();
    let b = (0..).map(|i| format!("kb{i}").into_bytes()).find(|k| owner(k) == 1).unwrap();
    (a, b)
}

#[test]
fn ping_set_get_over_two_workers() {
    let n = TestNode::start(2, |_| {});
    let mut c = n.client();
    let pong: String = redis::cmd("PING").query(&mut c).unwrap();
    assert_eq!(pong, "PONG");
    let (a, b) = keys_on_two_workers();
    let _: () = redis::cmd("SET").arg(&a).arg("1").query(&mut c).unwrap();
    let _: () = redis::cmd("SET").arg(&b).arg("2").query(&mut c).unwrap();
    let va: String = redis::cmd("GET").arg(&a).query(&mut c).unwrap();
    let vb: String = redis::cmd("GET").arg(&b).query(&mut c).unwrap();
    assert_eq!((va.as_str(), vb.as_str()), ("1", "2"));
    let missing: Option<String> = redis::cmd("GET").arg("nope").query(&mut c).unwrap();
    assert_eq!(missing, None);
    let e: redis::RedisResult<()> = redis::cmd("NOSUCHCMD").arg("x").query(&mut c);
    assert!(e.unwrap_err().to_string().contains("unknown command"));
    assert_eq!(n.stop(), 0);
}

#[test]
fn pipelined_replies_keep_request_order_across_workers() {
    let n = TestNode::start(3, |_| {});
    let mut c = n.client();
    let mut p = redis::pipe();
    for i in 0..2000 {
        p.cmd("SET").arg(format!("key:{i}")).arg(i).ignore();
    }
    for i in 0..2000 {
        p.cmd("GET").arg(format!("key:{i}"));
    }
    let got: Vec<i64> = p.query(&mut c).unwrap();
    assert_eq!(got, (0..2000).collect::<Vec<i64>>());
    let dbsize: i64 = redis::cmd("DBSIZE").query(&mut c).unwrap();
    assert_eq!(dbsize, 2000);
}

#[test]
fn byte_by_byte_and_inline_input() {
    let n = TestNode::start(2, |_| {});
    let mut s = n.raw();
    for b in b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n" {
        s.write_all(&[*b]).unwrap();
        s.flush().unwrap();
    }
    assert_eq!(read_exact_or_eof(&mut s, 16), b"+OK\r\n$5\r\nhello\r\n");
    s.write_all(b"PING\r\nECHO \"a b\"\r\n").unwrap();
    assert_eq!(read_exact_or_eof(&mut s, 16), b"+PONG\r\n$3\r\na b\r\n");
}

#[test]
fn protocol_errors_reply_then_close() {
    let n = TestNode::start(1, |_| {});
    let mut s = n.raw();
    s.write_all(b"*1\r\n$x\r\n").unwrap();
    let got = read_exact_or_eof(&mut s, usize::MAX);
    assert_eq!(got, b"-ERR Protocol error: invalid bulk length\r\n");
}

#[test]
fn fan_out_reads_and_deletes_are_gathered_in_order() {
    let n = TestNode::start(2, |_| {});
    let mut c = n.client();
    let (a, b) = keys_on_two_workers();
    let _: () = redis::cmd("SET").arg(&a).arg("va").query(&mut c).unwrap();
    let _: () = redis::cmd("SET").arg(&b).arg("vb").query(&mut c).unwrap();
    let got: Vec<Option<String>> = redis::cmd("MGET").arg(&b).arg("missing").arg(&a).arg(&b).query(&mut c).unwrap();
    assert_eq!(got, vec![Some("vb".into()), None, Some("va".into()), Some("vb".into())]);
    let ex: i64 = redis::cmd("EXISTS").arg(&a).arg(&b).arg(&a).arg("missing").query(&mut c).unwrap();
    assert_eq!(ex, 3, "EXISTS counts duplicates");
    let del: i64 = redis::cmd("DEL").arg(&a).arg(&b).arg(&a).query(&mut c).unwrap();
    assert_eq!(del, 2);
    let ex: i64 = redis::cmd("EXISTS").arg(&a).arg(&b).query(&mut c).unwrap();
    assert_eq!(ex, 0);
}

#[test]
fn cross_partition_writes_run_as_transactions() {
    let n = TestNode::start(2, |_| {});
    let mut c = n.client();
    let (a, b) = keys_on_two_workers();
    let _: () = redis::cmd("MSET").arg(&a).arg("1").arg(&b).arg("2").query(&mut c).unwrap();
    let got: Vec<String> = redis::cmd("MGET").arg(&a).arg(&b).query(&mut c).unwrap();
    assert_eq!(got, vec!["1", "2"]);
    let nx: i64 = redis::cmd("MSETNX").arg(&a).arg("x").arg("fresh").arg("y").query(&mut c).unwrap();
    assert_eq!(nx, 0);
    let _: () = redis::cmd("RENAME").arg(&a).arg(&b).query(&mut c).unwrap();
    let vb: String = redis::cmd("GET").arg(&b).query(&mut c).unwrap();
    assert_eq!(vb, "1");
    let va: Option<String> = redis::cmd("GET").arg(&a).query(&mut c).unwrap();
    assert_eq!(va, None);
    let e: redis::RedisResult<()> = redis::cmd("RENAME").arg(&a).arg(&b).query(&mut c);
    assert!(e.unwrap_err().to_string().contains("no such key"));
    // concurrent transactions from several connections serialize through the gate
    let port = n.port;
    let threads: Vec<_> = (0..4)
        .map(|t| {
            let (a, b) = (a.clone(), b.clone());
            std::thread::spawn(move || {
                let mut c = redis::Client::open(format!("redis://127.0.0.1:{port}/")).unwrap().get_connection().unwrap();
                for i in 0..50 {
                    let v = format!("{t}-{i}");
                    let _: () = redis::cmd("MSET").arg(&a).arg(&v).arg(&b).arg(&v).query(&mut c).unwrap();
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    let got: Vec<String> = redis::cmd("MGET").arg(&a).arg(&b).query(&mut c).unwrap();
    assert_eq!(got[0], got[1], "every MSET applied both keys together");
}

/// Commands of one connection that reach the same partition execute in request order (§6.6),
/// also behind a cross-partition transaction still waiting for the gate and behind a FLUSHDB.
#[test]
fn pipelined_commands_keep_order_behind_transactions_and_flushes() {
    let n = TestNode::start(2, |_| {});
    let mut c = n.client();
    let (a, b) = keys_on_two_workers();
    // a second connection keeps the transaction gate busy
    let port = n.port;
    let (a2, b2) = (a.clone(), b.clone());
    let noise = std::thread::spawn(move || {
        let mut c = redis::Client::open(format!("redis://127.0.0.1:{port}/")).unwrap().get_connection().unwrap();
        for i in 0..300 {
            let _: () = redis::cmd("MSET").arg(&a2).arg(i).arg(&b2).arg(i).query(&mut c).unwrap();
        }
    });
    for round in 0..20 {
        let mut p = redis::pipe();
        for i in 0..50 {
            p.cmd("MSET").arg(&a).arg(i).arg(&b).arg(i).ignore();
            p.cmd("DEL").arg(&a);
            p.cmd("GET").arg(&b);
        }
        p.cmd("FLUSHDB").ignore();
        p.cmd("SET").arg(&a).arg("after").ignore();
        p.cmd("GET").arg(&a);
        let got: Vec<redis::Value> = p.query(&mut c).unwrap();
        assert_eq!(got.len(), 101);
        for i in 0..50 {
            let del: i64 = redis::from_redis_value(got[2 * i].clone()).unwrap();
            assert_eq!(del, 1, "round {round}: DEL {i} ran before the MSET it follows");
            let v: Option<String> = redis::from_redis_value(got[2 * i + 1].clone()).unwrap();
            // the other connection's MSETs may interleave, never an older or missing value
            assert!(v.is_some(), "round {round}: GET {i} ran before the MSET it follows");
        }
        let last: String = redis::from_redis_value(got[100].clone()).unwrap();
        assert_eq!(last, "after", "round {round}: the SET after FLUSHDB was flushed");
    }
    noise.join().unwrap();
}

#[test]
fn scan_keys_and_randomkey_walk_every_partition() {
    let n = TestNode::start(2, |_| {});
    let mut c = n.client();
    for i in 0..100 {
        let _: () = redis::cmd("SET").arg(format!("s:{i}")).arg(i).query(&mut c).unwrap();
    }
    let _: () = redis::cmd("SET").arg("other").arg(1).query(&mut c).unwrap();
    let mut keys: Vec<String> = redis::cmd("KEYS").arg("s:*").query(&mut c).unwrap();
    keys.sort();
    assert_eq!(keys.len(), 100);
    let mut cursor = 0u64;
    let mut seen = std::collections::BTreeSet::new();
    loop {
        let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN").arg(cursor).arg("MATCH").arg("s:*").query(&mut c).unwrap();
        seen.extend(batch);
        if next == 0 {
            break;
        }
        cursor = next;
    }
    assert_eq!(seen.len(), 100);
    let rk: Option<String> = redis::cmd("RANDOMKEY").query(&mut c).unwrap();
    assert!(rk.is_some());
    let e: redis::RedisResult<()> = redis::cmd("SCAN").arg("notanumber").query(&mut c);
    assert!(e.unwrap_err().to_string().contains("invalid cursor"));
}

#[test]
fn flush_and_save_are_node_operations() {
    let n = TestNode::start(2, |_| {});
    let mut c = n.client();
    for i in 0..20 {
        let _: () = redis::cmd("SET").arg(format!("f:{i}")).arg(i).query(&mut c).unwrap();
    }
    let _: () = redis::cmd("SELECT").arg(1).query(&mut c).unwrap();
    let _: () = redis::cmd("SET").arg("in-db1").arg(1).query(&mut c).unwrap();
    let _: () = redis::cmd("SELECT").arg(0).query(&mut c).unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut c).unwrap();
    // the counters are published within a millisecond
    std::thread::sleep(Duration::from_millis(20));
    let size: i64 = redis::cmd("DBSIZE").query(&mut c).unwrap();
    assert_eq!(size, 0);
    let _: () = redis::cmd("SELECT").arg(1).query(&mut c).unwrap();
    let v: Option<String> = redis::cmd("GET").arg("in-db1").query(&mut c).unwrap();
    assert_eq!(v.as_deref(), Some("1"), "FLUSHDB clears one database");
    let _: () = redis::cmd("FLUSHALL").query(&mut c).unwrap();
    let v: Option<String> = redis::cmd("GET").arg("in-db1").query(&mut c).unwrap();
    assert_eq!(v, None);
    let ok: String = redis::cmd("SAVE").query(&mut c).unwrap();
    assert_eq!(ok, "OK");
    let started: String = redis::cmd("BGSAVE").query(&mut c).unwrap();
    assert_eq!(started, "Background saving started");
}

#[test]
fn multi_exec_runs_in_one_partition() {
    let n = TestNode::start(2, |_| {});
    let mut s = n.raw();
    s.write_all(b"MULTI\r\nSET {t}a 1\r\nINCR {t}a\r\nGET {t}a\r\nEXEC\r\n").unwrap();
    let want = b"+OK\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n*3\r\n+OK\r\n:2\r\n$1\r\n2\r\n";
    assert_eq!(read_exact_or_eof(&mut s, want.len()), want);
    s.write_all(b"EXEC\r\n").unwrap();
    let want = b"-ERR EXEC without MULTI\r\n";
    assert_eq!(read_exact_or_eof(&mut s, want.len()), want);
}

#[test]
fn hello_resp3_and_client_list() {
    let n = TestNode::start(2, |_| {});
    let mut s = n.raw();
    s.write_all(b"HELLO 3\r\n").unwrap();
    let got = read_exact_or_eof(&mut s, 1);
    assert_eq!(got[0], b'%', "HELLO 3 replies with a RESP3 map");
    let mut a = n.raw();
    a.write_all(b"CLIENT SETNAME other\r\n").unwrap();
    assert_eq!(read_exact_or_eof(&mut a, 5), b"+OK\r\n");
    let mut c = n.client();
    let list: String = redis::cmd("CLIENT").arg("LIST").query(&mut c).unwrap();
    assert!(list.lines().count() >= 3, "every worker's connections are listed: {list}");
    assert!(list.contains("name=other"));
    assert!(list.contains("cmd=client|list"));
}

#[test]
fn requirepass_and_maxclients() {
    let n = TestNode::start(1, |c| {
        c.requirepass = "secret".to_owned();
        c.maxclients = 2;
    });
    let mut s = n.raw();
    s.write_all(b"GET k\r\nAUTH secret\r\nPING\r\n").unwrap();
    let want = b"-NOAUTH Authentication required.\r\n+OK\r\n+PONG\r\n";
    assert_eq!(read_exact_or_eof(&mut s, want.len()), want);
    let _second = n.raw();
    std::thread::sleep(Duration::from_millis(50));
    let mut third = n.raw();
    let got = read_exact_or_eof(&mut third, usize::MAX);
    assert_eq!(got, b"-ERR max number of clients reached\r\n");
}

#[test]
fn quit_closes_after_the_reply_and_shutdown_stops_the_node() {
    let n = TestNode::start(2, |_| {});
    let mut s = n.raw();
    s.write_all(b"PING\r\nQUIT\r\nPING\r\n").unwrap();
    assert_eq!(read_exact_or_eof(&mut s, usize::MAX), b"+PONG\r\n+OK\r\n");
    let mut c = n.client();
    let _: redis::RedisResult<()> = redis::cmd("SHUTDOWN").query(&mut c);
    let node = { n }.node.take().unwrap();
    assert_eq!(node.wait(), 0);
}

#[test]
fn large_values_round_trip() {
    let n = TestNode::start(2, |_| {});
    let mut c = n.client();
    let big = vec![b'x'; 3 << 20];
    let _: () = redis::cmd("SET").arg("big").arg(&big).query(&mut c).unwrap();
    let back: Vec<u8> = redis::cmd("GET").arg("big").query(&mut c).unwrap();
    assert_eq!(back.len(), big.len());
    assert!(back == big);
    assert_eq!(n.node.as_ref().unwrap().shared.budget.used(), 0, "large-value reservations are released");
}

/// A node over the in-RAM backend for the black-box harness in `tests/compat` (the runtime,
/// protocol and handlers under test; no devices):
/// `KVD_MEM_PORT=7777 cargo test -p kv-node serve_mem_node -- --ignored --nocapture`.
#[test]
#[ignore = "serves RESP on $KVD_MEM_PORT until killed (run with --ignored)"]
fn serve_mem_node() {
    let port: u16 = std::env::var("KVD_MEM_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(7777);
    let threads: u16 = std::env::var("KVD_MEM_THREADS").ok().and_then(|p| p.parse().ok()).unwrap_or(2);
    let cfg = Config { port, threads: Some(threads), ..Config::default() };
    let node = start_with(cfg, PARTITIONS, move |w, shared: &Arc<NodeShared>| {
        let s = shared.clone();
        MemBackend::new(w, threads, PARTITIONS).with_meta(Box::new(move |p, token, r| {
            if let MetaReq::CommitTxn { txn } = r {
                // the meta thread answers a commit, then resolves it (credits return)
                let me = &s.workers[usize::from(w)];
                me.send_side(XMsg::Meta { partition: p, token, done: MetaDone::Committed { txn } });
                me.send_side(XMsg::Meta { partition: p, token, done: MetaDone::Resolved { credits: 1 } });
            }
        }))
    })
    .expect("node starts");
    eprintln!("serving the in-RAM node on 127.0.0.1:{port}");
    node.wait();
}

/// `kvd` over the page store on a fresh data directory (formatted on first start, §20.1):
/// PING/SET/GET and friends through the `redis` client, a clean SHUTDOWN, and a restart that
/// serves the same data from the snapshot (§18.2).
#[test]
fn kvd_on_a_temp_dir_serves_and_restarts() {
    use crate::runtime::node;
    use std::sync::atomic::Ordering;

    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let mut cfg = Config { port, threads: Some(2), ..Config::default() };
    cfg.data_dir = dir.path().to_path_buf();
    cfg.data_size = 1 << 30;
    cfg.format_partitions = 16;
    cfg.cache_size = Some(64 << 20);

    let wait_ready = |n: &NodeHandle| {
        for _ in 0..2000 {
            if !n.shared.loading() {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!("partitions did not become ready");
    };
    let client = || redis::Client::open(format!("redis://127.0.0.1:{port}/")).unwrap().get_connection().unwrap();

    let n = node::start(cfg.clone()).expect("kvd starts on a new data directory");
    assert!(dir.path().join("kvd.data").exists(), "the data file was formatted");
    wait_ready(&n);
    let mut c = client();
    let pong: String = redis::cmd("PING").query(&mut c).unwrap();
    assert_eq!(pong, "PONG");
    let _: () = redis::cmd("SET").arg("k").arg("v1").query(&mut c).unwrap();
    let v: String = redis::cmd("GET").arg("k").query(&mut c).unwrap();
    assert_eq!(v, "v1");
    let missing: Option<String> = redis::cmd("GET").arg("missing").query(&mut c).unwrap();
    assert_eq!(missing, None);
    // keys on many partitions (cross-partition MSET is a transaction, MGET a fan-out)
    let mut m = redis::cmd("MSET");
    for i in 0..50 {
        m.arg(format!("m:{i}")).arg(i);
    }
    let _: () = m.query(&mut c).unwrap();
    let got: Vec<i64> = redis::cmd("MGET").arg("m:0").arg("m:17").arg("m:49").query(&mut c).unwrap();
    assert_eq!(got, vec![0, 17, 49]);
    let big = vec![b'z'; 300 << 10];
    let _: () = redis::cmd("SET").arg("big").arg(&big).query(&mut c).unwrap();
    let n_incr: i64 = redis::cmd("INCRBY").arg("counter").arg(41).query(&mut c).unwrap();
    assert_eq!(n_incr, 41);
    let del: i64 = redis::cmd("DEL").arg("m:0").arg("m:1").query(&mut c).unwrap();
    assert_eq!(del, 2);
    let size: i64 = redis::cmd("DBSIZE").query(&mut c).unwrap();
    assert_eq!(size, 1 + 48 + 1 + 1);
    let _: redis::RedisResult<()> = redis::cmd("SHUTDOWN").query(&mut c);
    assert_eq!(n.wait(), 0, "clean shutdown");

    let n = node::start(cfg).expect("kvd restarts on its data directory");
    wait_ready(&n);
    assert!(!n.shared.recovery.scanned.load(Ordering::Relaxed), "a clean restart uses the snapshot");
    let mut c = client();
    let v: String = redis::cmd("GET").arg("k").query(&mut c).unwrap();
    assert_eq!(v, "v1");
    let back: Vec<u8> = redis::cmd("GET").arg("big").query(&mut c).unwrap();
    assert!(back == big, "the extent value survives the restart");
    let got: Vec<Option<i64>> = redis::cmd("MGET").arg("m:0").arg("m:2").arg("counter").query(&mut c).unwrap();
    assert_eq!(got, vec![None, Some(2), Some(41)]);
    let size: i64 = redis::cmd("DBSIZE").query(&mut c).unwrap();
    assert_eq!(size, 51);
    n.shared.request_shutdown();
    assert_eq!(n.wait(), 0);
}

/// (e) WATCH on the page store: EXEC answers a null array when a watched key changed after
/// WATCH (a write or delete by any connection, on any worker, FLUSHDB, an expiry), and runs
/// otherwise; UNWATCH, DISCARD and EXEC end the session.
#[test]
fn watch_aborts_exec_when_a_watched_key_changed() {
    use crate::runtime::node;

    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let mut cfg = Config { port, threads: Some(2), ..Config::default() };
    cfg.data_dir = dir.path().to_path_buf();
    cfg.data_size = 1 << 30;
    cfg.format_partitions = 16;
    cfg.cache_size = Some(64 << 20);
    let n = node::start(cfg).expect("kvd starts");
    for _ in 0..2000 {
        if !n.shared.loading() {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let client = || redis::Client::open(format!("redis://127.0.0.1:{port}/")).unwrap().get_connection().unwrap();
    let (mut a, mut b) = (client(), client());
    // keys whose partitions belong to different workers (and a body on a third key)
    let (ka, kb) = keys_on_two_workers();
    let (ka, kb) = (String::from_utf8(ka).unwrap(), String::from_utf8(kb).unwrap());
    let exec = |c: &mut redis::Connection, key: &str, v: &str| -> Option<Vec<String>> {
        let _: () = redis::cmd("MULTI").query(c).unwrap();
        let q: String = redis::cmd("SET").arg(key).arg(v).query(c).unwrap();
        assert_eq!(q, "QUEUED");
        redis::cmd("EXEC").query(c).unwrap()
    };
    let watch = |c: &mut redis::Connection, keys: &[&str]| {
        let ok: String = redis::cmd("WATCH").arg(keys).query(c).unwrap();
        assert_eq!(ok, "OK");
    };
    for k in [&ka, &kb] {
        let _: () = redis::cmd("SET").arg(k).arg("0").query(&mut a).unwrap();
    }
    // untouched: the transaction runs
    watch(&mut a, &[&ka, &kb]);
    assert_eq!(exec(&mut a, &ka, "1"), Some(vec!["OK".to_owned()]));
    // another connection writes a watched key (on either worker): EXEC aborts, nothing runs
    for (changed, body) in [(&ka, &ka), (&kb, &ka), (&ka, &kb)] {
        watch(&mut a, &[&ka, &kb]);
        let _: () = redis::cmd("SET").arg(changed).arg("by-b").query(&mut b).unwrap();
        assert_eq!(exec(&mut a, body, "from-a"), None, "{changed} changed after WATCH");
        let v: String = redis::cmd("GET").arg(changed).query(&mut a).unwrap();
        assert_eq!(v, "by-b");
    }
    // a delete, the connection's own write, FLUSHDB
    watch(&mut a, &[&kb]);
    let _: i64 = redis::cmd("DEL").arg(&kb).query(&mut b).unwrap();
    assert_eq!(exec(&mut a, &ka, "x"), None, "delete");
    watch(&mut a, &[&ka]);
    let _: () = redis::cmd("SET").arg(&ka).arg("mine").query(&mut a).unwrap();
    assert_eq!(exec(&mut a, &ka, "x"), None, "own write");
    watch(&mut a, &[&ka]);
    let _: () = redis::cmd("FLUSHDB").query(&mut b).unwrap();
    assert_eq!(exec(&mut a, &kb, "x"), None, "flushdb");
    // an expiry (a read makes the expired key go)
    let _: () = redis::cmd("SET").arg(&kb).arg("e").arg("PX").arg(40).query(&mut b).unwrap();
    watch(&mut a, &[&kb]);
    std::thread::sleep(Duration::from_millis(80));
    let gone: Option<String> = redis::cmd("GET").arg(&kb).query(&mut b).unwrap();
    assert_eq!(gone, None);
    assert_eq!(exec(&mut a, &ka, "x"), None, "expiry");
    // reads and other keys do not count; UNWATCH and DISCARD end the session
    watch(&mut a, &[&ka]);
    let _: Option<String> = redis::cmd("GET").arg(&ka).query(&mut b).unwrap();
    let _: () = redis::cmd("SET").arg("unrelated").arg("1").query(&mut b).unwrap();
    assert_eq!(exec(&mut a, &ka, "r"), Some(vec!["OK".to_owned()]), "reads and other keys");
    watch(&mut a, &[&ka]);
    let _: () = redis::cmd("UNWATCH").query(&mut a).unwrap();
    let _: () = redis::cmd("SET").arg(&ka).arg("after-unwatch").query(&mut b).unwrap();
    assert_eq!(exec(&mut a, &ka, "u"), Some(vec!["OK".to_owned()]), "unwatched");
    watch(&mut a, &[&ka]);
    let _: () = redis::cmd("MULTI").query(&mut a).unwrap();
    let _: () = redis::cmd("DISCARD").query(&mut a).unwrap();
    let _: () = redis::cmd("SET").arg(&ka).arg("after-discard").query(&mut b).unwrap();
    assert_eq!(exec(&mut a, &ka, "d"), Some(vec!["OK".to_owned()]), "discarded");
    let v: String = redis::cmd("GET").arg(&ka).query(&mut b).unwrap();
    assert_eq!(v, "d");
    // a closed connection's session goes with it
    {
        let mut c = client();
        watch(&mut c, &[&ka, &kb]);
    }
    std::thread::sleep(Duration::from_millis(50));
    let _: () = redis::cmd("SET").arg(&ka).arg("z").query(&mut b).unwrap();
    assert!(!n.shared.watch.any(), "no watched key is left behind");
    n.shared.request_shutdown();
    assert_eq!(n.wait(), 0);
}

/// Cross-partition RENAMEs and MSETs on a full page store: a part refused for space (`-OOM`,
/// storage full) aborts the transaction's other parts (§14.6 step 6). Before, they were only
/// unlocked: a failed RENAME had deleted its source (the source's staged delete stayed
/// visible), the parts' txns stayed open so later writes of their keys and SHUTDOWN never
/// completed, and the txn ids never resolved, so the txn credits leaked until every
/// multi-key write waited for one.
#[test]
fn failed_cross_partition_writes_on_a_full_device_undo_every_part() {
    use crate::runtime::node;

    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let mut cfg = Config { port, threads: Some(2), ..Config::default() };
    cfg.data_dir = dir.path().to_path_buf();
    cfg.data_size = 1 << 30;
    cfg.format_partitions = 16;
    cfg.cache_size = Some(64 << 20);
    let n = node::start(cfg).expect("kvd starts");
    for _ in 0..2000 {
        if !n.shared.loading() {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let mut c = redis::Client::open(format!("redis://127.0.0.1:{port}/")).unwrap().get_connection().unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let big = vec![b'x'; 100_000];
    let (mut i, mut refused) = (0u32, 0u32);
    while refused < 20 && i < 30_000 {
        if redis::cmd("SET").arg(format!("fill{i}")).arg(&big).query::<()>(&mut c).is_err() {
            refused += 1;
        }
        i += 1;
    }
    assert_eq!(refused, 20, "the device fills up");
    let (mut renamed, mut failed) = (0, 0);
    for j in 0..300 {
        let src = format!("fill{j}");
        let exists: i64 = redis::cmd("EXISTS").arg(&src).query(&mut c).unwrap();
        if exists == 0 {
            continue;
        }
        match redis::cmd("RENAME").arg(&src).arg(format!("dst{j}")).query::<()>(&mut c) {
            Ok(()) => renamed += 1,
            Err(e) => {
                assert!(!e.is_timeout(), "RENAME {src} hangs");
                failed += 1;
                let back: i64 = redis::cmd("EXISTS").arg(&src).query(&mut c).unwrap();
                assert_eq!(back, 1, "a failed RENAME keeps its source ({e})");
            }
        }
    }
    assert!(failed > 0, "some RENAMEs were refused for space ({renamed} renamed)");
    for j in 0..100 {
        let mut m = redis::cmd("MSET");
        for k in 0..4 {
            m.arg(format!("m{}", (j * 7 + k * 13) % 97)).arg(vec![b'y'; 5000 * (k + 1)]);
        }
        if let Err(e) = m.query::<()>(&mut c) {
            assert!(!e.is_timeout(), "MSET #{j} hangs");
        }
    }
    // every aborted txn id resolves and gives its credit back
    let mut in_use = String::new();
    for _ in 0..200 {
        let info: String = redis::cmd("INFO").arg("all").query(&mut c).unwrap();
        in_use = info.lines().find_map(|l| l.strip_prefix("txn_credits_in_use:")).unwrap_or("?").trim().to_string();
        if in_use == "0" {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(in_use, "0", "txn credits leaked");
    let _: redis::RedisResult<()> = redis::cmd("SHUTDOWN").query(&mut c);
    assert_eq!(n.wait(), 0, "SHUTDOWN completes");
}

/// Txn ids that never resolve (a failed device) leave no credit for further multi-key
/// writes: such a write is refused after `CREDIT_HOLD_NS` instead of holding its connection
/// forever (an EXEC is answered EXECABORT).
#[test]
fn multi_key_writes_without_a_credit_are_refused_in_bounded_time() {
    use crate::runtime::exec::CREDIT_HOLD_NS;
    let port = free_port();
    let threads = 2;
    let cfg = Config { port, threads: Some(threads), ..Config::default() };
    let node = start_with(cfg, PARTITIONS, move |w, shared: &Arc<NodeShared>| {
        let s = shared.clone();
        MemBackend::new(w, threads, PARTITIONS).with_meta(Box::new(move |p, token, r| {
            if let MetaReq::CommitTxn { txn } = r {
                // committed, but the ids never resolve: no credit comes back
                s.workers[usize::from(w)].send_side(XMsg::Meta { partition: p, token, done: MetaDone::Committed { txn } });
            }
        }))
    })
    .expect("node starts");
    let n = TestNode { node: Some(node), port };
    let mut c = n.client();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let (a, b) = keys_on_two_workers();
    for i in 0..crate::shared::TXN_CREDITS {
        let _: () = redis::cmd("MSET").arg(&a).arg(i).arg(&b).arg(i).query(&mut c).unwrap();
    }
    let t = std::time::Instant::now();
    let e = redis::cmd("MSET").arg(&a).arg("x").arg(&b).arg("y").query::<()>(&mut c).unwrap_err();
    assert!(!e.is_timeout() && e.to_string().contains("no transaction credit"), "{e}");
    assert!(t.elapsed() >= Duration::from_nanos(CREDIT_HOLD_NS) && t.elapsed() < Duration::from_secs(5));
    let _: () = redis::cmd("MULTI").query(&mut c).unwrap();
    let _: () = redis::cmd("SET").arg(&a).arg("1").query(&mut c).unwrap();
    let _: () = redis::cmd("SET").arg(&b).arg("2").query(&mut c).unwrap();
    let e = redis::cmd("EXEC").query::<redis::Value>(&mut c).unwrap_err();
    assert!(e.to_string().to_ascii_uppercase().contains("EXECABORT"), "{e}");
    // single-key writes go on
    let _: () = redis::cmd("SET").arg(&a).arg("z").query(&mut c).unwrap();
    let v: String = redis::cmd("GET").arg(&a).query(&mut c).unwrap();
    assert_eq!(v, "z");
}
