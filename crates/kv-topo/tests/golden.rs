//! Golden tests: the coordinator's topology replies against replies captured from a real
//! redis-server 7.0.15 cluster (`tests/golden/*.bin`, 3 primaries + 3 replicas + 1 slotless
//! primary on 127.0.0.1:30001-30007; `degraded_*` after the replica on :30006 was killed and
//! flagged FAIL).
//!
//! The kvssd state below mirrors that cluster (same node ids, addresses, slot ranges and shard
//! order). Differences that are deliberate (LC-28: replication offsets, config epochs and
//! ping/pong times are 0; no cluster bus, so `@0`; `cluster_enabled:1` and zero message
//! counters in CLUSTER INFO) are normalized in the Redis capture before comparing. Everything
//! else must match byte for byte.

mod common;

use std::collections::BTreeSet;
use std::net::SocketAddr;

use common::{V, admin, apply_ok, parse_all};
use kv_ctrl::{CoordEndpoint, CoordId, NodeId, ShardId, SlotRanges};
use kv_resp::{Proto, ReplyWriter};
use kv_topo::redirect::{self, Redirect};
use kv_topo::render::{self, CoordLineStyle, RenderedTopology};
use kv_topo::{AdminOp, ClusterParams, ClusterState, Transition};

const IDS: [&str; 7] = [
    "9b62676aa8ea5a3ec5d8aa3b69ef571f8c7de9fc", // :30001 primary 0-5460
    "107b243c329a66916d76d8d1c862a45d013bafb3", // :30002 primary 5461-10922
    "eab3ad63a6852e28da50d9e784bc33903f4db156", // :30003 primary 10923-16383
    "fd840bdd70de5175dd8c3b1dabb2ca8dd6b34258", // :30004 replica of :30001
    "a20f41d2ec30c406a42f5261fcbb74bed8ca0674", // :30005 replica of :30002
    "ee3c67a8b02514fcbf6082616065c0637b33cb41", // :30006 replica of :30003
    "54fc289025e2cce17a07edaf02064a0beb83820d", // :30007 primary without slots
];

fn id(i: usize) -> NodeId {
    NodeId::parse_hex40(IDS[i]).unwrap()
}

fn at(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

/// The mirrored healthy cluster. Shards in the order Redis listed them in CLUSTER SHARDS.
fn healthy() -> ClusterState {
    let mut s = ClusterState::initial(ClusterParams::default(), [9; 16]);
    let mut batch = Vec::new();
    for i in 0..7 {
        let port = 30001 + i as u16;
        batch.push(admin(AdminOp::NodeAdd {
            id: id(i),
            ctrl: at(port + 11000),
            client: at(port),
            repl: at(port + 12000),
            zone: "a".into(),
            priority: 0,
            partitions: 64,
        }));
    }
    s = apply_ok(&s, &batch);
    // (shard id, primary, replica, slots)
    type Layout = (u16, usize, Option<usize>, Option<(u16, u16)>);
    let layout: [Layout; 4] = [
        (3, 2, Some(5), Some((10923, 16383))),
        (1, 0, Some(3), Some((0, 5460))),
        (2, 1, Some(4), Some((5461, 10922))),
        (4, 6, None, None),
    ];
    for (sid, p, r, slots) in layout {
        let sid = ShardId(sid);
        let mut b = vec![admin(AdminOp::ShardCreate {
            id: sid,
            primary: id(p),
            min_isr: 1,
        })];
        if let Some((a, z)) = slots {
            b.push(admin(AdminOp::SlotsAssign {
                slots: SlotRanges(vec![(a, z)]),
                shard: sid,
            }));
        }
        if let Some(r) = r {
            b.push(admin(AdminOp::ReplicaAdd {
                shard: sid,
                node: id(r),
            }));
            b.push(Transition::Isr {
                shard: sid,
                term: 1,
                node: id(r),
                add: true,
            });
        }
        s = apply_ok(&s, &b);
    }
    s
}

fn degraded() -> ClusterState {
    apply_ok(
        &healthy(),
        &[Transition::Health {
            node: id(5),
            fail: true,
        }],
    )
}

fn render(f: impl Fn(&mut ReplyWriter<'_>), p: Proto) -> Vec<u8> {
    let mut out = Vec::new();
    f(&mut ReplyWriter::new(&mut out, p));
    out
}

fn golden(name: &str) -> Vec<u8> {
    let path = format!("{}/tests/golden/{name}.bin", env!("CARGO_MANIFEST_DIR"));
    std::fs::read(&path).unwrap_or_else(|e| panic!("{path}: {e}"))
}

fn show(b: &[u8]) -> String {
    String::from_utf8_lossy(b).replace("\r\n", "\\r\\n\n")
}

fn assert_bytes(ours: &[u8], redis: &[u8], what: &str) {
    assert!(
        ours == redis,
        "{what} differs\n--- ours ---\n{}\n--- redis 7.0.15 ---\n{}",
        show(ours),
        show(redis)
    );
}

/// Replaces every `replication-offset` value by 0 (LC-28: clients never receive seqs).
fn zero_offsets(b: &[u8]) -> Vec<u8> {
    let key = b"$18\r\nreplication-offset\r\n:";
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i..].starts_with(key) {
            out.extend_from_slice(key);
            i += key.len();
            while b[i].is_ascii_digit() {
                i += 1;
            }
            out.push(b'0');
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    out
}

#[test]
fn cluster_slots_matches_redis() {
    for (tag, s) in [("healthy", healthy()), ("degraded", degraded())] {
        for (p, suffix) in [(Proto::Resp2, "resp2"), (Proto::Resp3, "resp3")] {
            let ours = render(|w| render::cluster_slots(&s, w), p);
            assert_bytes(
                &ours,
                &golden(&format!("{tag}_slots_{suffix}")),
                &format!("CLUSTER SLOTS {tag} {suffix}"),
            );
        }
    }
}

#[test]
fn cluster_shards_matches_redis() {
    for (tag, s) in [("healthy", healthy()), ("degraded", degraded())] {
        for (p, suffix) in [(Proto::Resp2, "resp2"), (Proto::Resp3, "resp3")] {
            let ours = render(|w| render::cluster_shards(&s, w), p);
            let redis = zero_offsets(&golden(&format!("{tag}_shards_{suffix}")));
            assert_bytes(&ours, &redis, &format!("CLUSTER SHARDS {tag} {suffix}"));
        }
    }
}

/// Redis line -> our conventions: bus port 0, ping/pong/config-epoch 0, no `myself` (the
/// answering server is a coordinator here).
fn normalize_redis_line(l: &str) -> String {
    let mut f: Vec<String> = l.split(' ').map(str::to_owned).collect();
    let (addr, _) = f[1].split_once('@').unwrap();
    f[1] = format!("{addr}@0");
    f[2] = f[2].replace("myself,", "");
    for c in &mut f[4..7] {
        *c = "0".into();
    }
    f.join(" ")
}

fn text_of(v: V) -> Vec<u8> {
    match v {
        V::Bulk(b) => b,
        V::Verbatim(fmt, b) => {
            assert_eq!(fmt, "txt");
            b
        }
        other => panic!("not a text reply: {other:?}"),
    }
}

fn coords() -> Vec<CoordEndpoint> {
    vec![
        CoordEndpoint {
            id: CoordId([0xc1; 20]),
            client: at(7000),
        },
        CoordEndpoint {
            id: CoordId([0xc2; 20]),
            client: at(7001),
        },
        CoordEndpoint {
            id: CoordId([0xc3; 20]),
            client: "[::1]:7002".parse().unwrap(),
        },
    ]
}

#[test]
fn cluster_nodes_matches_redis() {
    let peers = coords();
    for (tag, s) in [("healthy", healthy()), ("degraded", degraded())] {
        for (p, suffix) in [(Proto::Resp2, "resp2"), (Proto::Resp3, "resp3")] {
            let redis_text = text_of(parse_all(&golden(&format!("{tag}_nodes_{suffix}"))));
            let redis_lines: BTreeSet<String> = String::from_utf8(redis_text)
                .unwrap()
                .lines()
                .map(normalize_redis_line)
                .collect();
            let text = render::cluster_nodes(&s, &peers, &peers[1].id, CoordLineStyle::R1);
            assert!(text.ends_with(b"\n"));
            let ours = String::from_utf8(text.clone()).unwrap();
            let (data, coord): (Vec<&str>, Vec<&str>) = ours
                .lines()
                .partition(|l| IDS.iter().any(|i| l.starts_with(i)));
            let data: BTreeSet<String> = data.into_iter().map(str::to_owned).collect();
            assert_eq!(data, redis_lines, "CLUSTER NODES {tag} {suffix}");
            assert_eq!(coord.len(), 3);
            // framing: bulk (RESP2) / txt verbatim (RESP3), exactly as Redis frames NODES
            let framed = render(|w| render::write_text(w, &text), p);
            let expect = match p {
                Proto::Resp2 => format!("${}\r\n{}\r\n", text.len(), ours),
                Proto::Resp3 => format!("={}\r\ntxt:{}\r\n", text.len() + 4, ours),
            };
            assert_bytes(&framed, expect.as_bytes(), "NODES framing");
            let g = golden(&format!("{tag}_nodes_{suffix}"));
            assert_eq!(g[0], framed[0], "same RESP type as Redis");
        }
    }
}

#[test]
fn coordinator_lines_r1_r2() {
    let s = healthy();
    let peers = coords();
    let c = |i: usize| CoordId([0xc1 + i as u8; 20]).hex40();
    let r1 = String::from_utf8(render::cluster_nodes(
        &s,
        &peers,
        &peers[1].id,
        CoordLineStyle::R1,
    ))
    .unwrap();
    let r1_coord: Vec<&str> = r1
        .lines()
        .filter(|l| !IDS.iter().any(|i| l.starts_with(i)))
        .collect();
    assert_eq!(
        r1_coord,
        [
            format!("{} 127.0.0.1:7000@0 slave - 0 0 0 connected", c(0)),
            format!("{} 127.0.0.1:7001@0 myself,slave - 0 0 0 connected", c(1)),
            format!("{} ::1:7002@0 slave - 0 0 0 connected", c(2)),
        ]
    );
    let r2 = String::from_utf8(render::cluster_nodes(
        &s,
        &peers,
        &peers[0].id,
        CoordLineStyle::R2,
    ))
    .unwrap();
    let r2_coord: Vec<&str> = r2
        .lines()
        .filter(|l| !IDS.iter().any(|i| l.starts_with(i)))
        .collect();
    assert_eq!(
        r2_coord,
        [
            format!("{} 127.0.0.1:7000@0 myself,master - 0 0 0 connected", c(0)),
            format!("{} 127.0.0.1:7001@0 master - 0 0 0 connected", c(1)),
            format!("{} ::1:7002@0 master - 0 0 0 connected", c(2)),
        ]
    );
    for text in [&r1, &r2] {
        assert!(
            !text.contains("noaddr"),
            "never noaddr (Lettuce drops such nodes)"
        );
        assert_eq!(text.matches("myself").count(), 1);
    }
    // the coordinator list comes from the peer list only, never from the state
    let none = render::cluster_nodes(&s, &[], &peers[0].id, CoordLineStyle::R1);
    assert_eq!(String::from_utf8(none).unwrap().lines().count(), 7);
}

fn info_map(text: &[u8]) -> Vec<(String, String)> {
    String::from_utf8(text.to_vec())
        .unwrap()
        .split("\r\n")
        .filter(|l| !l.is_empty())
        .map(|l| {
            let (k, v) = l.split_once(':').unwrap();
            (k.to_owned(), v.to_owned())
        })
        .collect()
}

#[test]
fn cluster_info_matches_redis() {
    for (tag, s) in [("healthy", healthy()), ("degraded", degraded())] {
        let ours_text = render::cluster_info(&s);
        assert!(ours_text.ends_with(b"\r\n"));
        let ours = info_map(&ours_text);
        let redis = info_map(&text_of(parse_all(&golden(&format!("{tag}_info_resp2")))));
        // Redis prints per-type bus message counters only when non-zero; there is no bus here.
        let is_per_type = |k: &str| {
            k.starts_with("cluster_stats_messages_")
                && k != "cluster_stats_messages_sent"
                && k != "cluster_stats_messages_received"
        };
        let redis_keys: Vec<&str> = redis
            .iter()
            .map(|(k, _)| k.as_str())
            .filter(|k| !is_per_type(k))
            .collect();
        let our_keys: Vec<&str> = ours.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(our_keys[0], "cluster_enabled");
        assert_eq!(&our_keys[1..], &redis_keys[..], "same fields, same order");
        let get =
            |m: &[(String, String)], k: &str| m.iter().find(|(x, _)| x == k).unwrap().1.clone();
        for k in [
            "cluster_state",
            "cluster_slots_assigned",
            "cluster_slots_ok",
            "cluster_slots_pfail",
            "cluster_slots_fail",
            "cluster_known_nodes",
            "cluster_size",
            "total_cluster_links_buffer_limit_exceeded",
        ] {
            assert_eq!(get(&ours, k), get(&redis, k), "{tag} {k}");
        }
        assert_eq!(
            get(&ours, "cluster_current_epoch"),
            s.version.to_string(),
            "one global map counter"
        );
        assert_eq!(get(&ours, "cluster_my_epoch"), "0");
        // framing as Redis: bulk / txt verbatim
        for (p, suffix) in [(Proto::Resp2, "resp2"), (Proto::Resp3, "resp3")] {
            let framed = render(|w| render::write_text(w, &ours_text), p);
            assert_eq!(framed[0], golden(&format!("{tag}_info_{suffix}"))[0]);
            assert_eq!(text_of(parse_all(&framed)), ours_text);
        }
    }
}

#[test]
fn cluster_info_failure_states() {
    let s = healthy();
    let f = apply_ok(
        &s,
        &[Transition::Health {
            node: id(0),
            fail: true,
        }],
    );
    let m = info_map(&render::cluster_info(&f));
    let get = |k: &str| m.iter().find(|(x, _)| x == k).unwrap().1.clone();
    assert_eq!(get("cluster_state"), "fail");
    assert_eq!(get("cluster_slots_fail"), "5461");
    assert_eq!(get("cluster_slots_ok"), (16384 - 5461).to_string());
    let empty = ClusterState::initial(ClusterParams::default(), [0; 16]);
    let m = info_map(&render::cluster_info(&empty));
    assert!(m.contains(&("cluster_state".into(), "fail".into())));
    assert!(m.contains(&("cluster_size".into(), "0".into())));
    assert_eq!(
        render(|w| render::cluster_shards(&empty, w), Proto::Resp2),
        b"*0\r\n"
    );
    assert_eq!(
        render(|w| render::cluster_slots(&empty, w), Proto::Resp3),
        b"*0\r\n"
    );
}

fn map_keys(v: &V) -> Vec<String> {
    let pairs: Vec<(V, V)> = match v {
        V::Map(p) => p.clone(),
        V::Array(a) => a.chunks(2).map(|c| (c[0].clone(), c[1].clone())).collect(),
        other => panic!("not a map: {other:?}"),
    };
    pairs
        .iter()
        .map(|(k, _)| match k {
            V::Bulk(b) => String::from_utf8(b.clone()).unwrap(),
            other => panic!("{other:?}"),
        })
        .collect()
}

#[test]
fn hello_matches_redis_shape() {
    let r3 = golden("hello_master_resp3");
    let ours3 = render(
        |w| render::coordinator_hello(w, 15, CoordLineStyle::R2),
        Proto::Resp3,
    );
    assert_eq!(ours3[0], r3[0], "RESP3 map");
    let (rv, ov) = (parse_all(&r3), parse_all(&ours3));
    assert_eq!(map_keys(&ov), map_keys(&rv));
    let r2 = golden("hello_replica_resp2");
    let ours2 = render(
        |w| render::coordinator_hello(w, 8, CoordLineStyle::R1),
        Proto::Resp2,
    );
    assert_eq!(ours2[0], r2[0], "RESP2 flat array");
    assert_eq!(map_keys(&parse_all(&ours2)), map_keys(&parse_all(&r2)));
    // field values: mode cluster, role per line style, proto per connection, empty modules
    let text = String::from_utf8_lossy(&ours2).into_owned();
    assert!(text.contains("$4\r\nmode\r\n$7\r\ncluster\r\n"));
    assert!(text.contains("$4\r\nrole\r\n$7\r\nreplica\r\n"));
    assert!(text.contains("$5\r\nproto\r\n:2\r\n"));
    assert!(text.ends_with("$7\r\nmodules\r\n*0\r\n"));
    assert!(String::from_utf8_lossy(&ours3).contains("$4\r\nrole\r\n$6\r\nmaster\r\n"));
    assert!(String::from_utf8_lossy(&ours3).contains("$7\r\nversion\r\n$5\r\n7.2.0\r\n"));
}

#[test]
fn moved_and_crossslot_match_redis() {
    let s = healthy();
    let r = redirect::for_slot(&s, 12182, &|_| false);
    assert_eq!(r, Redirect::Moved(12182, at(30003)));
    let ours = render(|w| r.write(w), Proto::Resp2);
    assert_bytes(&ours, &golden("moved"), "MOVED");
    let ours = render(|w| redirect::KeysRedirect::CrossSlot.write(w), Proto::Resp3);
    assert_bytes(&ours, &golden("crossslot"), "CROSSSLOT");
}

#[test]
fn myid_and_rendered_cache() {
    let s = healthy();
    let me = CoordId([0xc2; 20]);
    assert_eq!(
        render(|w| render::cluster_myid(&me, w), Proto::Resp2),
        format!("$40\r\n{}\r\n", me.hex40()).as_bytes()
    );
    assert_eq!(
        render(render::cluster_myshardid, Proto::Resp3),
        format!("$40\r\n{}\r\n", "0".repeat(40)).as_bytes()
    );
    let peers = coords();
    let t = RenderedTopology::build(&s, &peers, &me, CoordLineStyle::R1);
    assert_eq!(t.version, s.version);
    for p in [Proto::Resp2, Proto::Resp3] {
        assert_eq!(t.slots(p), render(|w| render::cluster_slots(&s, w), p));
        assert_eq!(t.shards(p), render(|w| render::cluster_shards(&s, w), p));
        assert_eq!(
            text_of(parse_all(t.nodes(p))),
            render::cluster_nodes(&s, &peers, &me, CoordLineStyle::R1)
        );
        assert_eq!(text_of(parse_all(t.info(p))), render::cluster_info(&s));
    }
}

/// Members that are not in sync are not advertised (SLOTS, NODES) but are listed as `loading`
/// in SHARDS; failed members as `fail`.
#[test]
fn unadvertised_members() {
    let s = healthy();
    let s = apply_ok(
        &s,
        &[Transition::Isr {
            shard: ShardId(1),
            term: 1,
            node: id(3),
            add: false,
        }],
    );
    let slots = render(|w| render::cluster_slots(&s, w), Proto::Resp2);
    assert!(!String::from_utf8_lossy(&slots).contains(IDS[3]));
    let nodes = String::from_utf8(render::cluster_nodes(
        &s,
        &[],
        &CoordId([0; 20]),
        CoordLineStyle::R1,
    ))
    .unwrap();
    assert!(!nodes.contains(IDS[3]));
    let shards = String::from_utf8_lossy(&render(|w| render::cluster_shards(&s, w), Proto::Resp2))
        .into_owned();
    let pos = shards.find(IDS[3]).unwrap();
    assert!(shards[pos..].contains("$6\r\nhealth\r\n$7\r\nloading\r\n"));
    // the failed primary keeps its slots in SLOTS (full coverage for clients) with fail flags in NODES
    let f = apply_ok(
        &healthy(),
        &[Transition::Health {
            node: id(0),
            fail: true,
        }],
    );
    let slots = String::from_utf8_lossy(&render(|w| render::cluster_slots(&f, w), Proto::Resp2))
        .into_owned();
    assert!(slots.contains(IDS[0]));
    let nodes = String::from_utf8(render::cluster_nodes(
        &f,
        &[],
        &CoordId([0; 20]),
        CoordLineStyle::R1,
    ))
    .unwrap();
    assert!(nodes.contains(&format!(
        "{} 127.0.0.1:30001@0 master,fail - 0 0 0 disconnected 0-5460\n",
        IDS[0]
    )));
}
