//! Shared builders for kv-topo tests.
#![allow(dead_code)]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use kv_ctrl::{MemberState, NodeId, ShardId, SlotRanges};
use kv_topo::{AdminOp, ClusterParams, ClusterState, Transition};

pub fn nid(i: u8) -> NodeId {
    let mut b = [0u8; 20];
    b[0] = 0xA0;
    b[19] = i;
    NodeId(b)
}

/// Distinct client / ctrl / repl addresses for node index `i`.
pub fn client_addr(i: u8) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, i)), 7000)
}

pub fn node_add_at(id: NodeId, client: SocketAddr, priority: u8) -> Transition {
    let base = client.port();
    Transition::Admin(AdminOp::NodeAdd {
        id,
        ctrl: SocketAddr::new(client.ip(), base.wrapping_add(10_000)),
        client,
        repl: SocketAddr::new(client.ip(), base.wrapping_add(20_000)),
        zone: format!("z{}", client.port() % 3),
        priority,
        partitions: 64,
    })
}

pub fn node_add(i: u8, priority: u8) -> Transition {
    node_add_at(nid(i), client_addr(i), priority)
}

pub fn admin(op: AdminOp) -> Transition {
    Transition::Admin(op)
}

pub fn ranges(a: u16, b: u16) -> SlotRanges {
    SlotRanges(vec![(a, b)])
}

pub fn apply_ok(s: &ClusterState, batch: &[Transition]) -> ClusterState {
    match s.apply(batch) {
        Ok(n) => n,
        Err(e) => panic!("batch {batch:?} rejected: {e}"),
    }
}

/// Makes `node` an InSync ISR replica of `shard` (replica add, then the primary's ISR expand).
pub fn add_insync_replica(s: &ClusterState, shard: ShardId, node: NodeId) -> ClusterState {
    let term = s.shard(shard).expect("shard").term;
    apply_ok(
        s,
        &[
            admin(AdminOp::ReplicaAdd { shard, node }),
            Transition::Isr {
                shard,
                term,
                node,
                add: true,
            },
        ],
    )
}

/// 3 shards (ids 1, 2, 3) x (primary + one InSync replica), all slots assigned:
/// shard 1 = nodes 1 (primary), 4; shard 2 = 2, 5; shard 3 = 3, 6. Node i has priority i.
pub fn three_by_two() -> ClusterState {
    let mut s = ClusterState::initial(ClusterParams::default(), [7; 16]);
    let adds: Vec<_> = (1..=6).map(|i| node_add(i, i)).collect();
    s = apply_ok(&s, &adds);
    let spans = [(0u16, 5460u16), (5461, 10922), (10923, 16383)];
    for (k, &(a, b)) in spans.iter().enumerate() {
        let sid = ShardId(k as u16 + 1);
        s = apply_ok(
            &s,
            &[
                admin(AdminOp::ShardCreate {
                    id: sid,
                    primary: nid(k as u8 + 1),
                    min_isr: 1,
                }),
                admin(AdminOp::SlotsAssign {
                    slots: ranges(a, b),
                    shard: sid,
                }),
            ],
        );
        s = add_insync_replica(&s, sid, nid(k as u8 + 4));
    }
    assert!(s.shards.iter().all(|sh| {
        sh.members
            .iter()
            .all(|m| m.state == MemberState::InSync && m.in_isr)
    }));
    s
}

// ---------------------------------------------------------------------------------------------
// A minimal RESP2/RESP3 reader for comparing replies structurally.

#[derive(Clone, PartialEq, Debug)]
pub enum V {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    Verbatim(String, Vec<u8>),
    Null,
    Array(Vec<V>),
    Map(Vec<(V, V)>),
}

fn line(b: &[u8]) -> (&[u8], &[u8]) {
    let p = b.windows(2).position(|w| w == b"\r\n").expect("CRLF");
    (&b[..p], &b[p + 2..])
}

fn num(b: &[u8]) -> i64 {
    std::str::from_utf8(b).unwrap().parse().unwrap()
}

/// Parses one value; returns it and the rest.
pub fn parse(b: &[u8]) -> (V, &[u8]) {
    let (tag, rest) = (b[0], &b[1..]);
    let (l, rest) = line(rest);
    match tag {
        b'+' => (V::Simple(String::from_utf8(l.to_vec()).unwrap()), rest),
        b'-' => (V::Error(String::from_utf8(l.to_vec()).unwrap()), rest),
        b':' => (V::Int(num(l)), rest),
        b'_' => (V::Null, rest),
        b'$' | b'=' => {
            let n = num(l);
            if n < 0 {
                return (V::Null, rest);
            }
            let n = n as usize;
            let body = rest[..n].to_vec();
            assert_eq!(&rest[n..n + 2], b"\r\n");
            let rest = &rest[n + 2..];
            if tag == b'=' {
                (
                    V::Verbatim(
                        String::from_utf8(body[..3].to_vec()).unwrap(),
                        body[4..].to_vec(),
                    ),
                    rest,
                )
            } else {
                (V::Bulk(body), rest)
            }
        }
        b'*' | b'~' => {
            let n = num(l);
            if n < 0 {
                return (V::Null, rest);
            }
            let mut rest = rest;
            let mut v = Vec::new();
            for _ in 0..n {
                let (x, r) = parse(rest);
                v.push(x);
                rest = r;
            }
            (V::Array(v), rest)
        }
        b'%' => {
            let n = num(l);
            let mut rest = rest;
            let mut v = Vec::new();
            for _ in 0..n {
                let (k, r) = parse(rest);
                let (x, r) = parse(r);
                v.push((k, x));
                rest = r;
            }
            (V::Map(v), rest)
        }
        t => panic!("unexpected RESP tag {t}"),
    }
}

/// Parses exactly one complete value.
pub fn parse_all(b: &[u8]) -> V {
    let (v, rest) = parse(b);
    assert!(rest.is_empty(), "trailing bytes after the reply");
    v
}
