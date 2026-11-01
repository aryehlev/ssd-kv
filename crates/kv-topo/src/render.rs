//! Topology replies of the coordinator's client service (DESIGN §23.5).
//!
//! * CLUSTER SLOTS: Redis 7 format `[start, end, [ip, port, id40, {}], replicas...]` per
//!   contiguous range of one shard (ascending), primary first, then the advertised replicas:
//!   ISR members in state InSync without a FAIL flag. Coordinators are not listed.
//! * CLUSTER SHARDS: per shard a 2-entry map `slots` (flat start/end integer pairs) and `nodes`
//!   (primary first, then the other members), each node a 7-entry map `id, port, ip, endpoint,
//!   role, replication-offset: 0, health`. Health comes from replication state only (LC-30):
//!   `fail` (FAIL flag or member state Failed; the word Redis 7.0.15 sends), `online` (serving
//!   primary, or InSync ISR replica), else `loading`.
//! * CLUSTER NODES: data-node lines from the state (primary, then advertised replicas), plus one
//!   slotless line per coordinator rendered from the static peer list (never from the state),
//!   `myself` only on the answering coordinator, never `noaddr`. Coordinator line style R1
//!   (`slave` with master-id `-`) or R2 (`master` with no slots). Ping, pong and config-epoch
//!   columns are 0 and the bus port is `@0` (LC-28).
//! * CLUSTER INFO: `cluster_enabled:1`, `cluster_state`, slot counts, `cluster_known_nodes`
//!   (registered data nodes), `cluster_size`, `cluster_current_epoch:<version>`,
//!   `cluster_my_epoch:0`, zero message counters.
//!
//! The byte formats follow redis-server 7.0.15 (golden tests in `tests/golden.rs` compare them
//! with replies captured from a real cluster). NODES and INFO texts are sent as a bulk string
//! (RESP2) or a `txt` verbatim string (RESP3) with [`write_text`]. Rendered replies are cached
//! per `version` ([`RenderedTopology`]).

use std::fmt::Write as _;
use std::net::SocketAddr;

use kv_ctrl::{CoordEndpoint, CoordId, MemberState};
use kv_resp::{Proto, ReplyWriter};

use crate::state::{ClusterState, NodeRecord, ShardRecord, ShardStatus};

/// How coordinators appear in CLUSTER NODES (decided at M2.1).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CoordLineStyle {
    /// slotless `slave` entry with master-id `-`
    R1,
    /// `master` with no slots
    R2,
}

/// Compatibility version string reported by HELLO (§7.4, §28).
pub const COMPAT_VERSION: &str = "7.2.0";

/// `server` field of the coordinator's HELLO reply.
pub const SERVER_NAME: &str = "kvc";

/// CLUSTER MYSHARDID at a coordinator: forty zeros.
pub const ZERO_SHARD_ID: &str = "0000000000000000000000000000000000000000";

fn ip_string(a: &SocketAddr) -> String {
    a.ip().to_string()
}

/// A replica that clients may use: in the committed ISR, InSync, not flagged FAIL.
fn advertised(sh: &ShardRecord, node: &NodeRecord) -> bool {
    !sh.is_primary(&node.id)
        && sh
            .member(&node.id)
            .is_some_and(|m| m.in_isr && m.state == MemberState::InSync)
}

/// The shard's primary record and its replicas (records of the other members, member order)
/// for which `keep` holds.
fn shard_nodes<'a>(
    s: &'a ClusterState,
    sh: &'a ShardRecord,
    keep: impl Fn(&NodeRecord) -> bool,
) -> (Option<&'a NodeRecord>, Vec<&'a NodeRecord>) {
    let primary = sh.primary.and_then(|p| s.node(&p));
    let replicas = sh
        .members
        .iter()
        .filter(|m| !sh.is_primary(&m.node))
        .filter_map(|m| s.node(&m.node))
        .filter(|n| keep(n))
        .collect();
    (primary, replicas)
}

fn write_slots_node(w: &mut ReplyWriter<'_>, n: &NodeRecord) {
    w.array(4);
    w.bulk(ip_string(&n.client).as_bytes());
    w.int(i64::from(n.client.port()));
    w.bulk(n.id.hex40().as_bytes());
    // networking metadata (hostname): none
    w.map(0);
}

/// CLUSTER SLOTS.
pub fn cluster_slots(s: &ClusterState, w: &mut ReplyWriter<'_>) {
    let mut entries = Vec::new();
    for (a, b, id) in s.slot_runs() {
        let Some(sh) = s.shard(id) else { continue };
        let (Some(p), replicas) = shard_nodes(s, sh, |n| advertised(sh, n) && !n.flags.fail) else {
            continue;
        };
        entries.push((a, b, p, replicas));
    }
    w.array(entries.len());
    for (a, b, p, replicas) in entries {
        w.array(3 + replicas.len());
        w.int(i64::from(a));
        w.int(i64::from(b));
        write_slots_node(w, p);
        for r in replicas {
            write_slots_node(w, r);
        }
    }
}

fn shard_health(sh: &ShardRecord, n: &NodeRecord) -> &'static [u8] {
    let member = sh.member(&n.id);
    if n.flags.fail || member.is_some_and(|m| m.state == MemberState::Failed) {
        b"fail"
    } else if sh.is_primary(&n.id) {
        if sh.status == ShardStatus::Serving {
            b"online"
        } else {
            b"loading"
        }
    } else if advertised(sh, n) {
        b"online"
    } else {
        b"loading"
    }
}

fn write_shards_node(w: &mut ReplyWriter<'_>, sh: &ShardRecord, n: &NodeRecord) {
    let ip = ip_string(&n.client);
    w.map(7);
    w.bulk(b"id");
    w.bulk(n.id.hex40().as_bytes());
    w.bulk(b"port");
    w.int(i64::from(n.client.port()));
    w.bulk(b"ip");
    w.bulk(ip.as_bytes());
    w.bulk(b"endpoint");
    w.bulk(ip.as_bytes());
    w.bulk(b"role");
    w.bulk(if sh.is_primary(&n.id) {
        b"master"
    } else {
        b"replica"
    });
    w.bulk(b"replication-offset");
    // Clients never receive seqs (LC-28).
    w.int(0);
    w.bulk(b"health");
    w.bulk(shard_health(sh, n));
}

/// CLUSTER SHARDS. Empty when no shard owns a slot (as Redis does for a cluster of size 0).
pub fn cluster_shards(s: &ClusterState, w: &mut ReplyWriter<'_>) {
    if s.assigned_slots() == 0 {
        w.array(0);
        return;
    }
    w.array(s.shards.len());
    for sh in &s.shards {
        let ranges = s.slot_ranges_of(sh.id);
        let (primary, replicas) = shard_nodes(s, sh, |_| true);
        w.map(2);
        w.bulk(b"slots");
        w.array(2 * ranges.0.len());
        for &(a, b) in &ranges.0 {
            w.int(i64::from(a));
            w.int(i64::from(b));
        }
        w.bulk(b"nodes");
        w.array(usize::from(primary.is_some()) + replicas.len());
        if let Some(p) = primary {
            write_shards_node(w, sh, p);
        }
        for r in replicas {
            write_shards_node(w, sh, r);
        }
    }
}

fn push_addr(out: &mut String, a: &SocketAddr) {
    // Redis prints `ip:port@cport` without brackets, IPv6 included; clients split at the last
    // colon. There is no cluster bus: the bus port is 0.
    // Writing to a String cannot fail.
    let _ = write!(out, "{}:{}@0", a.ip(), a.port());
}

fn push_data_line(
    out: &mut String,
    n: &NodeRecord,
    master_of: Option<&NodeRecord>,
    slots: &[(u16, u16)],
) {
    out.push_str(&n.id.hex40());
    out.push(' ');
    push_addr(out, &n.client);
    out.push_str(if master_of.is_some() {
        " slave"
    } else {
        " master"
    });
    if n.flags.fail {
        out.push_str(",fail");
    }
    out.push(' ');
    match master_of {
        Some(m) => out.push_str(&m.id.hex40()),
        None => out.push('-'),
    }
    out.push_str(" 0 0 0 ");
    out.push_str(if n.flags.fail {
        "disconnected"
    } else {
        "connected"
    });
    for &(a, b) in slots {
        let _ = if a == b {
            write!(out, " {a}")
        } else {
            write!(out, " {a}-{b}")
        };
    }
    out.push('\n');
}

fn push_coord_line(out: &mut String, c: &CoordEndpoint, me: bool, style: CoordLineStyle) {
    out.push_str(&c.id.hex40());
    out.push(' ');
    push_addr(out, &c.client);
    out.push(' ');
    // Redis flag order: myself first.
    if me {
        out.push_str("myself,");
    }
    out.push_str(match style {
        CoordLineStyle::R1 => "slave",
        CoordLineStyle::R2 => "master",
    });
    // R1: master-id `-` matches no master, so no client fans out to it or reads from it.
    out.push_str(" - 0 0 0 connected\n");
}

/// CLUSTER NODES text (send with [`write_text`]).
pub fn cluster_nodes(
    s: &ClusterState,
    peers: &[CoordEndpoint], /* static list */
    myself: &CoordId,
    style: CoordLineStyle, /* R1 | R2 */
) -> Vec<u8> {
    let mut out = String::new();
    for sh in &s.shards {
        let (Some(p), replicas) = shard_nodes(s, sh, |n| advertised(sh, n)) else {
            continue;
        };
        push_data_line(&mut out, p, None, &s.slot_ranges_of(sh.id).0);
        for r in replicas {
            push_data_line(&mut out, r, Some(p), &[]);
        }
    }
    // Coordinators: from the operator-set peer list only, never from the state (LC-38).
    for c in peers {
        push_coord_line(&mut out, c, c.id == *myself, style);
    }
    out.into_bytes()
}

/// CLUSTER INFO text (send with [`write_text`]).
pub fn cluster_info(s: &ClusterState) -> Vec<u8> {
    let assigned = s.assigned_slots();
    let mut ok = 0usize;
    let runs = s.slot_runs();
    for &(a, b, id) in &runs {
        if s.shard(id).is_some_and(|sh| s.shard_serving(sh)) {
            ok += usize::from(b - a) + 1;
        }
    }
    let fail = assigned - ok;
    let size = s
        .shards
        .iter()
        .filter(|sh| runs.iter().any(|r| r.2 == sh.id))
        .count();
    let state_ok = assigned == kv_common::SLOTS && fail == 0;
    let mut out = String::with_capacity(512);
    let mut line = |k: &str, v: &dyn core::fmt::Display| {
        // Writing to a String cannot fail.
        let _ = write!(out, "{k}:{v}\r\n");
    };
    line("cluster_enabled", &1);
    line("cluster_state", &if state_ok { "ok" } else { "fail" });
    line("cluster_slots_assigned", &assigned);
    line("cluster_slots_ok", &ok);
    line("cluster_slots_pfail", &0);
    line("cluster_slots_fail", &fail);
    line("cluster_known_nodes", &s.nodes.len());
    line("cluster_size", &size);
    // One global map counter; never a shard term (LC-28).
    line("cluster_current_epoch", &s.version);
    line("cluster_my_epoch", &0);
    line("cluster_stats_messages_sent", &0);
    line("cluster_stats_messages_received", &0);
    line("total_cluster_links_buffer_limit_exceeded", &0);
    out.into_bytes()
}

/// Writes a CLUSTER NODES / CLUSTER INFO text: a bulk string in RESP2, a `txt` verbatim string
/// in RESP3 (as Redis 7.0 does).
pub fn write_text(w: &mut ReplyWriter<'_>, text: &[u8]) {
    w.verbatim(b"txt", text);
}

/// CLUSTER MYID at a coordinator: its own id.
pub fn cluster_myid(myself: &CoordId, w: &mut ReplyWriter<'_>) {
    w.bulk(myself.hex40().as_bytes());
}

/// CLUSTER MYSHARDID at a coordinator: forty zeros (it belongs to no shard).
pub fn cluster_myshardid(w: &mut ReplyWriter<'_>) {
    w.bulk(ZERO_SHARD_ID.as_bytes());
}

/// The coordinator's HELLO reply (map of 7, in the writer's protocol): `server`, `version`
/// (the compatibility string), `proto`, `id`, `mode: cluster`, `role` (`replica` with R1,
/// `master` with R2, matching its CLUSTER NODES line), `modules: []`.
pub fn coordinator_hello(w: &mut ReplyWriter<'_>, client_id: u64, style: CoordLineStyle) {
    let proto = match w.proto {
        Proto::Resp2 => 2,
        Proto::Resp3 => 3,
    };
    w.map(7);
    w.bulk(b"server");
    w.bulk(SERVER_NAME.as_bytes());
    w.bulk(b"version");
    w.bulk(COMPAT_VERSION.as_bytes());
    w.bulk(b"proto");
    w.int(proto);
    w.bulk(b"id");
    w.int(i64::try_from(client_id).unwrap_or(i64::MAX));
    w.bulk(b"mode");
    w.bulk(b"cluster");
    w.bulk(b"role");
    w.bulk(match style {
        CoordLineStyle::R1 => b"replica",
        CoordLineStyle::R2 => b"master",
    });
    w.bulk(b"modules");
    w.array(0);
}

/// Complete replies (RESP bytes, both protocols) of CLUSTER SLOTS/SHARDS/NODES/INFO for one
/// committed version: built once per version and shared by every client connection.
#[derive(Clone)]
pub struct RenderedTopology {
    pub version: u64,
    slots: [Vec<u8>; 2],
    shards: [Vec<u8>; 2],
    nodes: [Vec<u8>; 2],
    info: [Vec<u8>; 2],
}

fn pi(p: Proto) -> usize {
    match p {
        Proto::Resp2 => 0,
        Proto::Resp3 => 1,
    }
}

impl RenderedTopology {
    pub fn build(
        s: &ClusterState,
        peers: &[CoordEndpoint],
        myself: &CoordId,
        style: CoordLineStyle,
    ) -> Self {
        let nodes_text = cluster_nodes(s, peers, myself, style);
        let info_text = cluster_info(s);
        let both = |f: &dyn Fn(&mut ReplyWriter<'_>)| {
            [Proto::Resp2, Proto::Resp3].map(|p| {
                let mut buf = Vec::new();
                f(&mut ReplyWriter::new(&mut buf, p));
                buf
            })
        };
        RenderedTopology {
            version: s.version,
            slots: both(&|w| cluster_slots(s, w)),
            shards: both(&|w| cluster_shards(s, w)),
            nodes: both(&|w| write_text(w, &nodes_text)),
            info: both(&|w| write_text(w, &info_text)),
        }
    }

    pub fn slots(&self, p: Proto) -> &[u8] {
        &self.slots[pi(p)]
    }

    pub fn shards(&self, p: Proto) -> &[u8] {
        &self.shards[pi(p)]
    }

    pub fn nodes(&self, p: Proto) -> &[u8] {
        &self.nodes[pi(p)]
    }

    pub fn info(&self, p: Proto) -> &[u8] {
        &self.info[pi(p)]
    }
}
