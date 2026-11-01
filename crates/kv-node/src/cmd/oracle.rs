//! A RESP server over the command handlers and the reference model (tests only).
//!
//! `serve_model` (ignored by default) listens on `127.0.0.1:$KVD_MODEL_PORT` and answers every
//! connection with the real handlers of this module tree, the registry, the RESP parser and
//! writer, and `testkit::Model` as the storage. It lets the black-box harness of
//! `tests/compat/` (differential runs against Redis 7.0.15) exercise the command layer before
//! the storage engine and the io_uring runtime exist:
//!
//! ```text
//! KVD_MODEL_PORT=7790 cargo test -p kv-node serve_model -- --ignored --nocapture &
//! tests/compat/.venv/bin/python tests/compat/differential.py --target 127.0.0.1:7790
//! ```
//!
//! Commands are executed one at a time under one lock; the SCAN cursor is a position in the
//! sorted key list (COUNT keys per step). Only the commands the harness and the scenarios use beyond the handlers
//! themselves (DBSIZE, FLUSHDB/FLUSHALL, SCAN, KEYS, RANDOMKEY, MULTI/EXEC) are emulated here.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use kv_cmd::CmdId;
use kv_resp::{Parsed, Parser, Proto, ReplyWriter};

use super::conn_cmds::tests::new_conn;
use super::server::{NodeOp, NodeOpDone, write_node_op_reply};
use super::testkit::Model;
use super::{Args, ConnCtx, ConnOutcome, Handler, scan, txn};
use crate::config::Config;
use crate::net::conn::Conn;
use crate::runtime::xmsg::{ExecEnv, OwnedCmd};
use crate::shared::NodeShared;

struct Node {
    model: Mutex<Model>,
    shared: NodeShared,
}

fn unix_ms() -> u64 {
    super::expire::unix_time_us() / 1000
}

/// Live keys of `db` in the model (expired keys are dropped on the way, like a lookup).
fn live_keys(m: &mut Model, db: u8) -> Vec<Box<[u8]>> {
    let now = m.now;
    m.data
        .retain(|(d, _), r| *d != db || r.expire.is_none_or(|e| e > now));
    m.data
        .keys()
        .filter(|(d, _)| *d == db)
        .map(|(_, k)| k.clone().into_boxed_slice())
        .collect()
}

fn exec_one(node: &Node, conn: &mut Conn, argv: &[&[u8]], out: &mut Vec<u8>) -> bool {
    let mut w = ReplyWriter::new(out, conn.proto);
    let spec = match kv_cmd::resolve(argv) {
        Ok(s) => s,
        Err(e) => {
            e.write(argv, &mut w);
            txn::flag_queue_error(conn);
            return true;
        }
    };
    let cfg = node.shared.config();
    if conn.multi.is_some() && txn::is_queued(spec.id) {
        txn::queue(conn, OwnedCmd::new(spec.id, argv), &mut w);
        return true;
    }
    if !super::precheck(spec.id, argv, &cfg, &mut w) {
        return true;
    }
    let mut m = node.model.lock().unwrap();
    m.now = unix_ms();
    m.db = conn.db;
    match super::handler(spec.id) {
        Handler::Keyed(_) => {
            let f = super::keyed::<Model>(spec.id).expect("keyed");
            let reply = m.run(f, argv, conn.proto);
            out.extend_from_slice(&reply);
        }
        Handler::Conn(f) | Handler::Cluster(f) => {
            if spec.id == CmdId::Dbsize {
                let n = live_keys(&mut m, conn.db).len();
                w.int(n as i64);
                return true;
            }
            let args = Args::new(argv);
            let mut cx = ConnCtx {
                conn,
                shared: &node.shared,
                worker: 0,
            };
            match f(&mut cx, &args, &mut w) {
                ConnOutcome::Done | ConnOutcome::FanOut(_) => {}
                ConnOutcome::Close => return false,
                ConnOutcome::Async => {
                    let op = super::node_op(spec.id, conn, argv).expect("node op");
                    let mut lines = Vec::new();
                    let done = match &op {
                        NodeOp::Flush { db } => {
                            m.data.retain(|(d, _), _| db.is_some_and(|x| x != *d));
                            NodeOpDone::Ok
                        }
                        NodeOp::ClientList(filter) => {
                            if filter.matches(conn.id) {
                                super::conn_cmds::write_client_line(
                                    conn,
                                    Some(spec.id),
                                    kv_common::clock::boot_ns(),
                                    &mut lines,
                                );
                            }
                            NodeOpDone::Clients(&lines)
                        }
                        NodeOp::ClientKill { .. } => NodeOpDone::Killed(0),
                        NodeOp::Shutdown { .. } | NodeOp::Barrier(_) => NodeOpDone::Ok,
                    };
                    let mut w = ReplyWriter::new(out, conn.proto);
                    write_node_op_reply(&op, done, &mut w);
                }
            }
        }
        Handler::Scan => match spec.id {
            CmdId::Scan => {
                if let Some(a) = scan::parse_scan(argv, &mut w) {
                    // the model's cursor is a position in the sorted key list
                    let matcher = scan::key_matcher(a.pattern);
                    let mut all = live_keys(&mut m, conn.db);
                    all.sort();
                    let from = usize::try_from(a.cursor)
                        .unwrap_or(usize::MAX)
                        .min(all.len());
                    let to = from.saturating_add(a.count as usize).min(all.len());
                    let keys: Vec<_> = match a.type_filter {
                        scan::TypeFilter::Nothing => Vec::new(),
                        tf => all[from..to]
                            .iter()
                            .filter(|k| {
                                matcher(k)
                                    && match tf {
                                        scan::TypeFilter::Only(t) => {
                                            m.rec(conn.db, k).is_some_and(|r| r.vtype == t)
                                        }
                                        _ => true,
                                    }
                            })
                            .cloned()
                            .collect(),
                    };
                    let next = if to >= all.len() || a.type_filter == scan::TypeFilter::Nothing {
                        0
                    } else {
                        to as u64
                    };
                    scan::write_scan_reply(next, &keys, &mut w);
                }
            }
            CmdId::Keys => {
                let matcher = scan::key_matcher(Some(argv[1]));
                let keys: Vec<_> = live_keys(&mut m, conn.db)
                    .into_iter()
                    .filter(|k| matcher(k))
                    .collect();
                scan::write_keys_reply(&keys, &mut w);
            }
            _ => {
                let keys = live_keys(&mut m, conn.db);
                scan::write_randomkey_reply(keys.first().map(|k| &k[..]), &mut w);
            }
        },
        Handler::Txn => {
            let args = Args::new(argv);
            match spec.id {
                CmdId::Multi => txn::multi(conn, &mut w),
                CmdId::Discard => txn::discard(conn, &mut w),
                CmdId::Watch => txn::watch(conn, &args, &mut w),
                CmdId::Unwatch => txn::unwatch(conn, &mut w),
                _ => {
                    if let txn::ExecStart::Run(cmds) = txn::exec_begin(conn, &mut w) {
                        let env = ExecEnv {
                            proto: conn.proto,
                            db: conn.db,
                            now_ms: m.now,
                            readonly_conn: false,
                            conn_id: conn.id,
                        };
                        let saved = m.data.clone();
                        let mut buf = Vec::new();
                        match txn::run_queued(
                            &mut *m,
                            &cmds,
                            &mut ReplyWriter::new(&mut buf, conn.proto),
                            &env,
                            cfg.databases,
                        ) {
                            Ok(db) => {
                                conn.db = db;
                                out.extend_from_slice(&buf);
                            }
                            Err(e) => {
                                m.data = saved;
                                super::write_store_error(e, &mut ReplyWriter::new(out, conn.proto));
                            }
                        }
                    }
                }
            }
        }
    }
    true
}

fn serve_conn(node: Arc<Node>, mut s: TcpStream, id: u64) {
    let _ = s.set_nodelay(true);
    let mut conn = new_conn(id);
    let mut parser = Parser::new(kv_resp::Limits::default());
    let mut buf: Vec<u8> = Vec::with_capacity(1 << 16);
    let mut chunk = vec![0u8; 1 << 16];
    let mut args = Vec::new();
    let mut out = Vec::new();
    loop {
        let n = match s.read(&mut chunk) {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        buf.extend_from_slice(&chunk[..n]);
        let mut open = true;
        loop {
            args.clear();
            match parser.parse_in_place(&mut buf, &mut args) {
                Ok(Parsed::Incomplete) => break,
                Ok(Parsed::Empty { consumed }) => {
                    buf.drain(..consumed);
                }
                Ok(Parsed::Command { consumed }) => {
                    let argv: Vec<&[u8]> = args
                        .iter()
                        .map(|&(o, l)| &buf[o as usize..(o + l) as usize])
                        .collect();
                    open = exec_one(&node, &mut conn, &argv, &mut out);
                    buf.drain(..consumed);
                    if !open {
                        break;
                    }
                }
                Err(e) => {
                    e.write_reply(&mut out);
                    open = false;
                    break;
                }
            }
        }
        if s.write_all(&out).is_err() || !open {
            return;
        }
        out.clear();
    }
}

/// Accepts connections on `listener` forever, one thread per connection.
fn serve(listener: TcpListener) {
    let node = Arc::new(Node {
        model: Mutex::new(Model::new(unix_ms())),
        shared: NodeShared::new(Config::default(), 4, 1).expect("node state"),
    });
    for (id, s) in listener.incoming().enumerate() {
        let Ok(s) = s else { continue };
        let node = node.clone();
        std::thread::spawn(move || serve_conn(node, s, id as u64 + 1));
    }
}

#[test]
#[ignore = "serves RESP on $KVD_MODEL_PORT until killed (run with --ignored)"]
fn serve_model() {
    let port: u16 = std::env::var("KVD_MODEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7790);
    let listener = TcpListener::bind(("127.0.0.1", port)).expect("bind");
    eprintln!("model server on 127.0.0.1:{port}");
    serve(listener);
}

/// A client library (the `redis` crate, RESP2 and RESP3) drives every command module through
/// the model server and decodes the replies into typed values (§25.5).
#[test]
fn redis_client_session() {
    use redis::{Commands, RedisResult, Value};
    use std::collections::BTreeMap;

    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind");
    let port = listener.local_addr().expect("address").port();
    std::thread::spawn(move || serve(listener));

    for (db, proto) in [(2u8, "2"), (3, "resp3")] {
        let url = format!("redis://127.0.0.1:{port}/{db}?protocol={proto}");
        let client = redis::Client::open(url).expect("client");
        let mut c = client.get_connection().expect("connection");
        let _: () = redis::cmd("FLUSHDB").query(&mut c).unwrap();

        // connection and server commands
        let pong: String = redis::cmd("PING").query(&mut c).unwrap();
        assert_eq!(pong, "PONG");
        let echo: Vec<u8> = redis::cmd("ECHO").arg(b"a\0b").query(&mut c).unwrap();
        assert_eq!(echo, b"a\0b");
        let _: () = redis::cmd("CLIENT")
            .arg("SETNAME")
            .arg("tester")
            .query(&mut c)
            .unwrap();
        let name: Option<String> = redis::cmd("CLIENT").arg("GETNAME").query(&mut c).unwrap();
        assert_eq!(name.as_deref(), Some("tester"));
        let n: i64 = redis::cmd("COMMAND").arg("COUNT").query(&mut c).unwrap();
        assert!(n > 50);
        let time: (String, String) = redis::cmd("TIME").query(&mut c).unwrap();
        assert!(time.0.parse::<u64>().unwrap() > 1_600_000_000);
        let cfg: BTreeMap<String, String> = redis::cmd("CONFIG")
            .arg("GET")
            .arg("databases")
            .query(&mut c)
            .unwrap();
        assert_eq!(cfg.get("databases").map(String::as_str), Some("16"));
        let info: String = redis::cmd("INFO").arg("server").query(&mut c).unwrap();
        assert!(info.contains("redis_version:7.2.0"), "{info}");

        // strings
        let _: () = c.set("s", "10").unwrap();
        assert_eq!(c.incr::<_, _, i64>("s", 5).unwrap(), 15);
        let f: String = redis::cmd("INCRBYFLOAT")
            .arg("s")
            .arg("0.5")
            .query(&mut c)
            .unwrap();
        assert_eq!(f, "15.5");
        assert_eq!(c.append::<_, _, i64>("s", "x").unwrap(), 5);
        assert_eq!(c.strlen::<_, i64>("s").unwrap(), 5);
        assert_eq!(c.getrange::<_, String>("s", 1, -2).unwrap(), "5.5");
        assert_eq!(c.setrange::<_, _, i64>("s", 6, "z").unwrap(), 7);
        let _: () = c.mset(&[("a", "1"), ("b", "2")]).unwrap();
        let v: Vec<Option<String>> = c.mget(&["a", "nokey", "b"]).unwrap();
        assert_eq!(v, [Some("1".into()), None, Some("2".into())]);
        let old: Option<String> = c.getset("a", "3").unwrap();
        assert_eq!(old.as_deref(), Some("1"));
        let r: RedisResult<i64> = c.incr("b", i64::MAX);
        let e = r.expect_err("overflow");
        assert!(
            e.to_string()
                .contains("increment or decrement would overflow"),
            "{e}"
        );

        // keys and expiry
        assert!(c.expire::<_, bool>("a", 100).unwrap());
        assert_eq!(c.ttl::<_, i64>("a").unwrap(), 100);
        assert!(c.persist::<_, bool>("a").unwrap());
        assert_eq!(c.ttl::<_, i64>("a").unwrap(), -1);
        assert_eq!(c.exists::<_, i64>(&["a", "b", "a", "nokey"]).unwrap(), 3);
        let t: String = redis::cmd("TYPE").arg("a").query(&mut c).unwrap();
        assert_eq!(t, "string");
        let _: () = c.rename("a", "a2").unwrap();
        assert_eq!(c.del::<_, i64>(&["a2", "a2", "nokey"]).unwrap(), 1);

        // hashes (RESP3: HGETALL is a map)
        let _: () = c.hset_multiple("h", &[("f1", "v1"), ("f2", "v2")]).unwrap();
        let all: BTreeMap<String, String> = c.hgetall("h").unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all["f2"], "v2");
        assert_eq!(c.hincr::<_, _, _, i64>("h", "n", 7).unwrap(), 7);
        assert_eq!(c.hlen::<_, i64>("h").unwrap(), 3);
        let keys: Vec<String> = c.hkeys("h").unwrap();
        assert_eq!(keys, ["f1", "f2", "n"]);
        let t: String = redis::cmd("TYPE").arg("h").query(&mut c).unwrap();
        assert_eq!(t, "hash");

        // iteration
        let mut seen: Vec<String> = c.scan().unwrap().collect::<RedisResult<_>>().unwrap();
        seen.sort();
        assert_eq!(seen, ["b", "h", "s"]);
        let hashes: Vec<String> = redis::cmd("SCAN")
            .cursor_arg(0)
            .arg("TYPE")
            .arg("hash")
            .clone()
            .iter(&mut c)
            .unwrap()
            .collect::<RedisResult<_>>()
            .unwrap();
        assert_eq!(hashes, ["h"]);
        let mut k: Vec<String> = c.keys("[bh]*").unwrap();
        k.sort();
        assert_eq!(k, ["b", "h"]);
        let n: i64 = redis::cmd("DBSIZE").query(&mut c).unwrap();
        assert_eq!(n, 3);

        // transactions
        let (x, y): (i64, String) = redis::pipe()
            .atomic()
            .cmd("INCR")
            .arg("ctr")
            .cmd("GET")
            .arg("ctr")
            .query(&mut c)
            .unwrap();
        assert_eq!((x, y.as_str()), (1, "1"));
        let e = redis::cmd("EXEC")
            .query::<Value>(&mut c)
            .expect_err("EXEC without MULTI");
        assert!(e.to_string().contains("EXEC without MULTI"), "{e}");
        let _: () = redis::cmd("FLUSHDB").query(&mut c).unwrap();
        let n: i64 = redis::cmd("DBSIZE").query(&mut c).unwrap();
        assert_eq!(n, 0);
    }
}

#[test]
fn model_server_round_trip() {
    let node = Arc::new(Node {
        model: Mutex::new(Model::new(unix_ms())),
        shared: NodeShared::new(Config::default(), 4, 1).expect("node state"),
    });
    let mut conn = new_conn(1);
    let mut out = Vec::new();
    for line in [
        "SET k v",
        "GET k",
        "HELLO 3",
        "GET nokey",
        "SCAN 0 MATCH k*",
        "DBSIZE",
        "FLUSHDB",
        "DBSIZE",
        "MULTI",
        "SET a 1",
        "EXEC",
    ] {
        let argv: Vec<&[u8]> = line.split(' ').map(str::as_bytes).collect();
        assert!(exec_one(&node, &mut conn, &argv, &mut out));
    }
    let s = String::from_utf8_lossy(&out);
    assert!(s.starts_with("+OK\r\n$1\r\nv\r\n%7\r\n"), "{s}");
    assert!(s.ends_with("_\r\n*2\r\n$1\r\n0\r\n*1\r\n$1\r\nk\r\n:1\r\n+OK\r\n:0\r\n+OK\r\n+QUEUED\r\n*1\r\n+OK\r\n"), "{s}");
    let _ = Proto::Resp2;
}
