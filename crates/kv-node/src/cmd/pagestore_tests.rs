//! Handler tests over the real page store (DESIGN §14-§16, §25.1): the keyed handlers run inside
//! `PartitionStore::exec` of a single-threaded `LocalEngine` on a formatted, `O_DIRECT` data file,
//! exactly as the workers run them (`handler()` registry, partition request built from the
//! command's key specs and access modes, reply after the command's ticket is `Written`).
//!
//! * `page_store_matches_model`: seeded random command streams (strings, keys, expiry, hashes;
//!   small values, 2-16 KiB extents and multi-MiB values) give byte-identical replies on the page
//!   store and on the reference model of `testkit`, in RESP2 and RESP3;
//! * `restart_keeps_acknowledged_state`: after clean shutdown and after a stop without shutdown
//!   (every reply was released at `Written`), a reopened store holds exactly the model's keys,
//!   values and TTLs;
//! * `scan_and_randomkey_on_page_store`: SCAN steps over every partition return every key once,
//!   MATCH and TYPE filter inside the step, RANDOMKEY returns a live key.

use std::path::PathBuf;

use kv_common::Now;
use kv_index::VType;
use kv_resp::{Proto, ReplyWriter};
use kv_storage::engine::{EngineCfg, LocalEngine, format_devices};
use kv_storage::io::{DevInfo, SyncFileDevice};
use kv_storage::keyspace::Keyspace;
use kv_storage::partition::{ExecStatus, Origin, ReqKind, Request};
use kv_storage::ticket::TicketState;
use smallvec::SmallVec;

use super::testkit::{Model, show};
use super::{Args, Handler, scan};
use crate::runtime::xmsg::ExecEnv;

/// Sparse data file size: room for every partition's first AGs, extents and chunked values.
const FILE_BYTES: u64 = 1 << 30;
const PARTITIONS: u16 = 16;
/// Driver iterations a request may take to become ready or written (a hang fails the test).
const PUMP_LIMIT: usize = 20_000;
/// The start of a test's fixed wall clock: the current time in whole seconds (never before the
/// durable clock floor the format recorded, §16.4), advanced explicitly by the tests.
fn t0() -> u64 {
    Now::read().unix_ms / 1000 * 1000
}

/// One page store and its file.
struct Store {
    eng: Option<LocalEngine<SyncFileDevice>>,
    path: PathBuf,
    _dir: tempfile::TempDir,
    now_ms: u64,
    /// The last commands run (for failure messages).
    history: std::collections::VecDeque<String>,
}

/// A write-back, two-image file device (its size taken from the file).
fn dev_info() -> DevInfo {
    DevInfo::new(0, 4096)
}

fn engine_cfg(path: &std::path::Path) -> EngineCfg {
    let mut cfg = EngineCfg::defaults(vec![path.to_path_buf()]);
    cfg.cache_bytes = 32 << 20;
    cfg
}

/// The clock of a request at `ms`.
fn clock(ms: u64) -> Now {
    let mut n = Now::read();
    n.unix_ms = ms;
    n
}

impl Store {
    fn new(seed: u64) -> Store {
        let dir = tempfile::tempdir().expect("temporary directory");
        let path = dir.path().join("data0");
        std::fs::File::create(&path)
            .and_then(|f| f.set_len(FILE_BYTES))
            .expect("data file");
        let mut dev = SyncFileDevice::open(&path, dev_info()).expect("open data file");
        format_devices(&mut [&mut dev], PARTITIONS, false, 0, seed).expect("format");
        let mut s = Store {
            eng: None,
            path,
            _dir: dir,
            now_ms: t0(),
            history: std::collections::VecDeque::new(),
        };
        s.open(vec![dev]);
        s
    }

    fn open(&mut self, devs: Vec<SyncFileDevice>) {
        let mut eng = LocalEngine::open(devs, &engine_cfg(&self.path)).expect("open engine");
        eng.set_now(clock(self.now_ms));
        eng.recover().expect("recovery");
        self.eng = Some(eng);
    }

    fn eng(&mut self) -> &mut LocalEngine<SyncFileDevice> {
        self.eng.as_mut().expect("engine open")
    }

    fn set_now(&mut self, ms: u64) {
        self.now_ms = ms;
        self.eng().set_now(clock(ms));
    }

    /// Clean shutdown (snapshot and CLEAN superblock), then open again.
    fn restart_clean(&mut self) {
        let devs = self.eng.take().expect("engine").shutdown(true).expect("shutdown");
        self.open(devs);
    }

    /// Stop without shutdown (as after kill -9 once every reply was released), then open again.
    fn restart_unclean(&mut self) {
        let devs = self.eng.take().expect("engine").into_devices();
        drop(devs);
        let dev = SyncFileDevice::open(&self.path, dev_info()).expect("reopen data file");
        self.open(vec![dev]);
    }

    /// The recent commands, oldest first.
    fn recent(&self) -> String {
        self.history.iter().cloned().collect::<Vec<_>>().join("\n")
    }

    /// Runs one keyed command as a worker does; returns its reply.
    fn run(&mut self, argv: &[&[u8]], db: u8, proto: Proto) -> Vec<u8> {
        if self.history.len() == 24 {
            self.history.pop_front();
        }
        self.history.push_back(format!("db{db} {}", line_of(argv)));
        let mut out = Vec::new();
        let spec = match kv_cmd::resolve(argv) {
            Ok(s) => s,
            Err(e) => {
                e.write(argv, &mut ReplyWriter::new(&mut out, proto));
                return out;
            }
        };
        let cfg = crate::config::Config::default();
        if !super::precheck(spec.id, argv, &cfg, &mut ReplyWriter::new(&mut out, proto)) {
            return out;
        }
        let Handler::Keyed(f) = super::handler(spec.id) else {
            panic!("{} is not a keyed command", spec.name);
        };
        let pos = kv_cmd::key_positions(spec, argv);
        let keys: SmallVec<[(Box<[u8]>, kv_cmd::Access); 2]> = pos
            .iter()
            .map(|&i| {
                (
                    Box::<[u8]>::from(argv[usize::from(i)]),
                    kv_cmd::key_access(spec, argv, i),
                )
            })
            .collect();
        let p = keys
            .first()
            .map_or(0, |(k, _)| kv_common::key_slot(k) % PARTITIONS);
        assert!(
            keys.iter()
                .all(|(k, _)| kv_common::key_slot(k) % PARTITIONS == p),
            "test commands keep their keys in one partition"
        );
        let req = Request {
            db,
            keys,
            kind: ReqKind::Keyed,
            origin: Origin::Client,
        };
        let args = Args::new(argv);
        let env = ExecEnv {
            proto,
            db,
            now_ms: self.now_ms,
            readonly_conn: false,
            conn_id: 1,
        };
        let line = format!("{}\nrecent commands:\n{}", line_of(argv), self.recent());
        let eng = self.eng.as_mut().expect("engine open");
        let id = eng.submit(p, req);
        let mut ready = Vec::new();
        let mut status = None;
        for _ in 0..PUMP_LIMIT {
            ready.clear();
            eng.ready(&mut ready);
            if ready.contains(&(p, id)) {
                match eng.exec(p, id, &mut |ks: &mut Keyspace<'_>| {
                    out.clear();
                    f(ks, &args, &mut ReplyWriter::new(&mut out, proto), &env)
                }) {
                    ExecStatus::Done((), exec) => status = Some(Ok(exec)),
                    ExecStatus::Failed(e) => status = Some(Err(e)),
                    ExecStatus::Requeued => {}
                }
            }
            if status.is_some() {
                break;
            }
            eng.pump();
        }
        match status.unwrap_or_else(|| panic!("{line} never became ready")) {
            Ok(exec) => {
                if let Some(t) = exec.ticket {
                    // the reply is released once the command's effects are written (§14.7)
                    for _ in 0..PUMP_LIMIT {
                        match eng.ticket_state(p, t) {
                            Some(TicketState::Written | TicketState::Durable) => return out,
                            Some(TicketState::Failed(e)) => panic!("{line}: ticket failed ({e})"),
                            _ => eng.pump(),
                        }
                    }
                    panic!("{line}: ticket never written");
                }
                out
            }
            Err(e) => {
                let mut err = Vec::new();
                super::write_store_error(e, &mut ReplyWriter::new(&mut err, proto));
                err
            }
        }
    }

    /// One SCAN call (`cursor` as returned to the client).
    fn scan(&mut self, db: u8, line: &str) -> (u64, Vec<Box<[u8]>>) {
        let argv: Vec<&[u8]> = line.split_ascii_whitespace().map(str::as_bytes).collect();
        let mut sink = Vec::new();
        let a = scan::parse_scan(&argv, &mut ReplyWriter::new(&mut sink, Proto::Resp2))
            .expect("valid SCAN");
        match scan::plan_scan(db, &a, PARTITIONS) {
            scan::ScanPlan::Reply { next } => (next, Vec::new()),
            scan::ScanPlan::Step {
                partition,
                request,
                pattern,
            } => {
                let matcher = scan::key_matcher(pattern);
                let (next, keys) = self
                    .eng()
                    .run_scan(partition, request, &matcher)
                    .expect("scan step");
                (scan::client_cursor(next, PARTITIONS), keys)
            }
        }
    }
}

/// The model's reply to the same command.
fn model_run(m: &mut Model, argv: &[&[u8]], db: u8, proto: Proto) -> Vec<u8> {
    m.db = db;
    let spec = match kv_cmd::resolve(argv) {
        Ok(s) => s,
        Err(e) => {
            let mut out = Vec::new();
            e.write(argv, &mut ReplyWriter::new(&mut out, proto));
            return out;
        }
    };
    let mut out = Vec::new();
    let cfg = crate::config::Config::default();
    if !super::precheck(spec.id, argv, &cfg, &mut ReplyWriter::new(&mut out, proto)) {
        return out;
    }
    let f = super::keyed::<Model>(spec.id).expect("keyed command");
    m.run(f, argv, proto)
}

/// A small deterministic generator (xorshift*), so failures are reproducible from the seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn pick<'a>(&mut self, xs: &[&'a str]) -> &'a str {
        xs[self.below(xs.len())]
    }
}

/// Keys sharing one hashtag (one partition) so that multi-key commands stay single-partition.
const KEYS: &[&str] = &["{t}a", "{t}b", "{t}c", "{t}h", "{t}n"];

/// A value of a size class chosen by `r`: small, extent-sized, or (rarely) several MiB.
fn value(r: &mut Rng) -> Vec<u8> {
    let len = match r.below(100) {
        0..=59 => r.below(40),
        60..=79 => 100 + r.below(1900),
        80..=97 => 2048 + r.below(14 * 1024),
        _ => (1 << 20) + r.below(9 << 20),
    };
    let b = b'a' + (r.below(26) as u8);
    (0..len).map(|i| b.wrapping_add((i % 7) as u8)).collect()
}

/// One random keyed command.
fn command(r: &mut Rng, now_s: u64) -> Vec<Vec<u8>> {
    let k = |r: &mut Rng| r.pick(KEYS).as_bytes().to_vec();
    let n = |r: &mut Rng| r.pick(&["0", "1", "-1", "7", "100", "abc"]).as_bytes().to_vec();
    let v = |s: &str| s.as_bytes().to_vec();
    match r.below(30) {
        0..=3 => vec![v("SET"), k(r), value(r)],
        4 => {
            let opts = [
                "NX", "XX", "GET", "KEEPTTL", "EX", "PX", "EXAT", "PXAT", "XX GET",
            ];
            let mut c = vec![v("SET"), k(r), value(r)];
            for o in r.pick(&opts).split(' ') {
                c.push(v(o));
                match o {
                    "EX" => c.push(v(&format!("{}", 1 + r.below(100)))),
                    "PX" => c.push(v(&format!("{}", 1 + r.below(100_000)))),
                    "EXAT" => c.push(v(&format!("{}", now_s + 1 + r.below(100) as u64))),
                    "PXAT" => c.push(v(&format!("{}", now_s * 1000 + r.below(100_000) as u64))),
                    _ => {}
                }
            }
            c
        }
        5 | 6 => vec![v("GET"), k(r)],
        7 => vec![v("APPEND"), k(r), value(r)],
        8 => vec![v("SETRANGE"), k(r), v(&format!("{}", r.below(5000))), value(r)],
        9 => {
            let (a, b) = (r.below(40) as i64 - 20, r.below(40) as i64 - 20);
            vec![v("GETRANGE"), k(r), v(&a.to_string()), v(&b.to_string())]
        }
        10 => vec![v("STRLEN"), k(r)],
        11 => vec![v("INCRBY"), k(r), n(r)],
        12 => vec![v("INCRBYFLOAT"), k(r), v(r.pick(&["1.5", "-0.25", "1e3", "x"]))],
        13 => vec![v("DEL"), k(r), k(r)],
        14 => vec![v("EXISTS"), k(r), k(r), k(r)],
        15 => vec![v("TYPE"), k(r)],
        16 => vec![v(r.pick(&["EXPIRE", "PEXPIRE"])), k(r), n(r)],
        17 => {
            let cond = r.pick(&["NX", "XX", "GT", "LT"]);
            vec![v("EXPIRE"), k(r), v(&format!("{}", 1 + r.below(200))), v(cond)]
        }
        18 => vec![v(r.pick(&["TTL", "PTTL", "EXPIRETIME", "PEXPIRETIME"])), k(r)],
        19 => vec![v("PERSIST"), k(r)],
        20 => vec![v(r.pick(&["RENAME", "RENAMENX", "COPY"])), k(r), k(r)],
        21 => vec![v("MSET"), k(r), value(r), k(r), value(r)],
        22 => vec![v("MSETNX"), k(r), value(r), k(r), value(r)],
        23 => vec![v("MGET"), k(r), k(r), k(r)],
        24 => vec![v(r.pick(&["GETDEL", "GETSET"])), k(r), value(r)]
            .into_iter()
            .take(2 + usize::from(r.below(2) == 0))
            .collect(),
        25 => {
            let o = r.pick(&["", "PERSIST", "EX 50", "PXAT 1"]);
            let mut c = vec![v("GETEX"), k(r)];
            c.extend(o.split_ascii_whitespace().map(v));
            c
        }
        26 => vec![v("HSET"), k(r), v(r.pick(&["f", "g"])), value(r)],
        27 => vec![v(r.pick(&["HGETALL", "HLEN", "HKEYS"])), k(r)],
        28 => vec![v("HINCRBY"), k(r), v("ctr"), n(r)],
        _ => vec![v(r.pick(&["HGET", "HDEL", "HEXISTS", "HSTRLEN"])), k(r), v("f")],
    }
}

/// A command line for assertion messages (long arguments shortened).
fn line_of(argv: &[&[u8]]) -> String {
    argv.iter()
        .map(|a| {
            if a.len() > 24 {
                format!("<{} bytes>", a.len())
            } else {
                String::from_utf8_lossy(a).into_owned()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Reply text for assertion messages (large bulks shortened).
fn brief(b: &[u8]) -> String {
    let s = show(b);
    if s.len() > 200 {
        format!("{}... ({} bytes)", &s[..200], b.len())
    } else {
        s
    }
}

#[test]
fn page_store_matches_model() {
    for seed in [1u64, 2, 3] {
        let mut st = Store::new(seed);
        let mut m = Model::new(st.now_ms);
        let mut r = Rng(0x9e37_79b9_7f4a_7c15 ^ seed);
        for i in 0..600 {
            // time moves forward in whole seconds now and then, so TTLs expire for real
            if r.below(40) == 0 {
                let ms = st.now_ms + 1000 * (1 + r.below(30) as u64);
                st.set_now(ms);
                m.now = ms;
            }
            let db = if r.below(10) == 0 { 1 } else { 0 };
            let proto = if r.below(3) == 0 {
                Proto::Resp3
            } else {
                Proto::Resp2
            };
            let cmd = command(&mut r, st.now_ms / 1000);
            let argv: Vec<&[u8]> = cmd.iter().map(Vec::as_slice).collect();
            let want = model_run(&mut m, &argv, db, proto);
            let got = st.run(&argv, db, proto);
            assert!(
                got == want,
                "seed {seed} step {i}: {}\n  page store: {}\n  model:      {}\nrecent commands:\n{}",
                line_of(&argv),
                brief(&got),
                brief(&want),
                st.recent()
            );
        }
    }
}

/// Every live key of the model, read back through GET/HGETALL and PTTL on the store.
fn assert_same_contents(st: &mut Store, m: &mut Model) {
    let now = m.now;
    let live: Vec<((u8, Vec<u8>), VType, Option<u64>)> = m
        .data
        .iter()
        .filter(|(_, r)| r.expire.is_none_or(|e| e > now))
        .map(|(k, r)| (k.clone(), r.vtype, r.expire))
        .collect();
    for ((db, key), vtype, _) in &live {
        let read: &[u8] = if *vtype == VType::Hash {
            b"HGETALL"
        } else {
            b"GET"
        };
        for c in [read, b"PTTL", b"TYPE"] {
            let argv: [&[u8]; 2] = [c, key];
            let want = model_run(m, &argv, *db, Proto::Resp2);
            let got = st.run(&argv, *db, Proto::Resp2);
            assert!(
                got == want,
                "{} after restart: {} vs {}",
                String::from_utf8_lossy(c),
                brief(&got),
                brief(&want)
            );
        }
    }
    for k in KEYS {
        for db in [0u8, 1] {
            let argv: [&[u8]; 2] = [b"EXISTS", k.as_bytes()];
            assert_eq!(
                st.run(&argv, db, Proto::Resp2),
                model_run(m, &argv, db, Proto::Resp2),
                "EXISTS after restart"
            );
        }
    }
}

#[test]
fn restart_keeps_acknowledged_state() {
    let mut st = Store::new(7);
    let mut m = Model::new(st.now_ms);
    let mut r = Rng(0x51ed_270b_d1b6_5b3d);
    for round in 0..4 {
        for _ in 0..150 {
            let db = if r.below(8) == 0 { 1 } else { 0 };
            let cmd = command(&mut r, st.now_ms / 1000);
            let argv: Vec<&[u8]> = cmd.iter().map(Vec::as_slice).collect();
            let want = model_run(&mut m, &argv, db, Proto::Resp2);
            assert_eq!(
                brief(&st.run(&argv, db, Proto::Resp2)),
                brief(&want),
                "round {round}"
            );
        }
        if round % 2 == 0 {
            st.restart_clean();
        } else {
            st.restart_unclean();
        }
        assert_same_contents(&mut st, &mut m);
    }
}

#[test]
fn scan_and_randomkey_on_page_store() {
    let mut st = Store::new(11);
    let mut expect = Vec::new();
    for i in 0..300 {
        let key = format!("key:{i}");
        let argv: [&[u8]; 3] = [b"SET", key.as_bytes(), b"v"];
        assert_eq!(st.run(&argv, 0, Proto::Resp2), b"+OK\r\n");
        expect.push(key);
    }
    let argv: [&[u8]; 4] = [b"HSET", b"hash:1", b"f", b"1"];
    assert_eq!(st.run(&argv, 0, Proto::Resp2), b":1\r\n");
    let argv: [&[u8]; 3] = [b"SET", b"other-db", b"v"];
    assert_eq!(st.run(&argv, 3, Proto::Resp2), b"+OK\r\n");

    // a full iteration returns every key once
    let mut seen: Vec<String> = Vec::new();
    let mut cursor = 0u64;
    loop {
        let (next, keys) = st.scan(0, &format!("SCAN {cursor} COUNT 25"));
        seen.extend(keys.iter().map(|k| String::from_utf8_lossy(k).into_owned()));
        if next == 0 {
            break;
        }
        cursor = next;
    }
    seen.sort();
    let mut want = expect.clone();
    want.push("hash:1".into());
    want.sort();
    assert_eq!(seen, want);

    // MATCH and TYPE are applied inside the steps
    let collect = |st: &mut Store, opts: &str| {
        let mut out = Vec::new();
        let mut cursor = 0u64;
        loop {
            let (next, keys) = st.scan(0, &format!("SCAN {cursor} {opts}"));
            out.extend(keys.iter().map(|k| String::from_utf8_lossy(k).into_owned()));
            if next == 0 {
                break out;
            }
            cursor = next;
        }
    };
    let mut m1 = collect(&mut st, "MATCH key:1?");
    m1.sort();
    let want: Vec<String> = (10..20).map(|i| format!("key:{i}")).collect();
    assert_eq!(m1, want);
    assert_eq!(collect(&mut st, "TYPE hash COUNT 1000"), ["hash:1"]);
    assert!(collect(&mut st, "TYPE zset").is_empty());

    // RANDOMKEY: a live key of the database
    for r in [1u64, 99, 12345] {
        let p = (r % u64::from(PARTITIONS)) as u16;
        let (_, keys) = st
            .eng()
            .run_scan(p, scan::randomkey_request(3, r), &|_| true)
            .expect("randomkey step");
        assert!(keys.is_empty() || &*keys[0] == b"other-db");
    }
}

/// Multi-key writes inside one partition request (§14.6): MSET, MSETNX, RENAME, RENAMENX,
/// COPY, and a delete next to a write.
#[test]
fn multi_key_writes_on_page_store() {
    let mut st = Store::new(13);
    let mut m = Model::new(st.now_ms);
    for line in [
        "SET {t}a v1",
        "RENAME {t}a {t}n",
        "GET {t}n",
        "GET {t}a",
        "MSET {t}p 1 {t}q 2",
        "MGET {t}p {t}q",
        "MSETNX {t}p 9 {t}r 9",
        "MSETNX {t}r 3 {t}s 4",
        "MGET {t}r {t}s",
        "RENAMENX {t}r {t}s",
        "RENAMENX {t}r {t}u",
        "COPY {t}u {t}w",
        "COPY {t}u {t}w REPLACE",
        "MGET {t}r {t}u {t}w",
        "DEL {t}p {t}q {t}zz",
        "MGET {t}p {t}q",
    ] {
        let argv: Vec<&[u8]> = line.split(' ').map(str::as_bytes).collect();
        let want = model_run(&mut m, &argv, 0, Proto::Resp2);
        assert_eq!(
            show(&st.run(&argv, 0, Proto::Resp2)),
            show(&want),
            "{line}"
        );
    }
    st.restart_unclean();
    for line in ["MGET {t}a {t}n {t}p {t}q {t}r {t}s {t}u {t}w"] {
        let argv: Vec<&[u8]> = line.split(' ').map(str::as_bytes).collect();
        let want = model_run(&mut m, &argv, 0, Proto::Resp2);
        assert_eq!(show(&st.run(&argv, 0, Proto::Resp2)), show(&want), "{line} after restart");
    }
}
