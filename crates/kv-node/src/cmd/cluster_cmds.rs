//! CLUSTER and related commands on a data node (DESIGN §8.7, LC-24).
//!
//! Standalone: every CLUSTER command, READONLY, READWRITE and ASKING reply
//! `-ERR This instance has cluster support disabled`. Cluster: CLUSTER KEYSLOT, CLUSTER MYID (own
//! id only), CLUSTER INFO with constant fields (`cluster_state` from the lease, 0 for every slot,
//! epoch and node-count field); topology requests reply
//! `-ERR cluster topology is served by the coordinator`; membership and slot management replies
//! `-ERR cluster membership and slots are managed by kvadm`; ASKING is a no-op `+OK`; READONLY /
//! READWRITE set the connection flag. A data node never names another data node.
//!
//! Every reply here is a constant or a function of the request and the node's own lease; no
//! handler reads routing state (LC-24).

use kv_resp::ReplyWriter;

use super::{Args, ConnCtx, ConnOutcome, err, write_arity_error};

/// Replies the standalone refusal and returns `false` when cluster mode is off.
fn cluster_mode(cx: &ConnCtx<'_>, w: &mut ReplyWriter<'_>) -> bool {
    if cx.shared.config().cluster_enabled {
        return true;
    }
    w.error(err::CLUSTER_DISABLED);
    false
}

/// `CLUSTER` without a subcommand (normally answered by command resolution).
pub fn cluster_container(
    cx: &mut ConnCtx<'_>,
    _a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
) -> ConnOutcome {
    if cluster_mode(cx, w) {
        write_arity_error("cluster", w);
    }
    ConnOutcome::Done
}

/// CLUSTER KEYSLOT key: `CRC16(tag or key) % 16384`.
pub fn cluster_keyslot(cx: &mut ConnCtx<'_>, a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    if cluster_mode(cx, w) {
        w.int(i64::from(kv_common::key_slot(a.arg(2))));
    }
    ConnOutcome::Done
}

/// CLUSTER MYID: the node's own identity (identity only, never another node's). The identity
/// is assigned when the coordinator registers the node; before the first assignment this
/// replies an error instead of an invented id.
pub fn cluster_myid(cx: &mut ConnCtx<'_>, _a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    if cluster_mode(cx, w) {
        match cx.shared.cluster().and_then(|c| c.identity().node_id) {
            Some(id) => w.bulk(id.hex40().as_bytes()),
            None => w.error(b"ERR node identity is not available yet"),
        }
    }
    ConnOutcome::Done
}

/// The CLUSTER INFO text: `cluster_state` from the lease, constant 0 for every slot, epoch and
/// node-count field (LC-24, LC-28).
pub fn cluster_info_text(lease_ok: bool) -> Vec<u8> {
    let state: &[u8] = if lease_ok { b"ok" } else { b"fail" };
    let mut s = Vec::with_capacity(400);
    s.extend_from_slice(b"cluster_enabled:1\r\ncluster_state:");
    s.extend_from_slice(state);
    s.extend_from_slice(b"\r\n");
    for f in [
        "cluster_slots_assigned",
        "cluster_slots_ok",
        "cluster_slots_pfail",
        "cluster_slots_fail",
        "cluster_known_nodes",
        "cluster_size",
        "cluster_current_epoch",
        "cluster_my_epoch",
        "cluster_stats_messages_sent",
        "cluster_stats_messages_received",
        "total_cluster_links_buffer_limit_exceeded",
    ] {
        s.extend_from_slice(f.as_bytes());
        s.extend_from_slice(b":0\r\n");
    }
    s
}

/// CLUSTER INFO.
pub fn cluster_info(cx: &mut ConnCtx<'_>, _a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    if cluster_mode(cx, w) {
        let ok = cx.shared.lease.valid(kv_common::clock::boot_ns());
        w.verbatim(b"txt", &cluster_info_text(ok));
    }
    ConnOutcome::Done
}

/// CLUSTER SLOTS, SHARDS, NODES, REPLICAS, SLAVES, LINKS, MYSHARDID, COUNTKEYSINSLOT,
/// GETKEYSINSLOT: the coordinators serve topology (LC-24).
pub fn cluster_topology(
    cx: &mut ConnCtx<'_>,
    _a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
) -> ConnOutcome {
    if cluster_mode(cx, w) {
        w.error(b"ERR cluster topology is served by the coordinator");
    }
    ConnOutcome::Done
}

/// CLUSTER MEET, ADDSLOTS[RANGE], DELSLOTS[RANGE], SETSLOT, FAILOVER, FORGET, REPLICATE, RESET,
/// FLUSHSLOTS, BUMPEPOCH, SAVECONFIG, SET-CONFIG-EPOCH, COUNT-FAILURE-REPORTS.
pub fn cluster_managed(
    cx: &mut ConnCtx<'_>,
    _a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
) -> ConnOutcome {
    if cluster_mode(cx, w) {
        w.error(b"ERR cluster membership and slots are managed by kvadm");
    }
    ConnOutcome::Done
}

/// ASKING: a no-op (data nodes never send ASK redirects).
pub fn asking(cx: &mut ConnCtx<'_>, _a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    if cluster_mode(cx, w) {
        w.ok();
    }
    ConnOutcome::Done
}

/// READONLY: replica reads allowed on this connection (§23.8).
pub fn readonly(cx: &mut ConnCtx<'_>, _a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    if cluster_mode(cx, w) {
        cx.conn.readonly = true;
        w.ok();
    }
    ConnOutcome::Done
}

/// READWRITE: clears READONLY.
pub fn readwrite(cx: &mut ConnCtx<'_>, _a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    if cluster_mode(cx, w) {
        cx.conn.readonly = false;
        w.ok();
    }
    ConnOutcome::Done
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::conn_cmds::tests::Fixture;
    use crate::config::Config;

    #[test]
    fn standalone_refuses_everything() {
        let mut f = Fixture::new();
        let disabled = "-ERR This instance has cluster support disabled\r\n";
        for (h, line) in [
            (cluster_keyslot as crate::cmd::ConnFn, "CLUSTER KEYSLOT k"),
            (cluster_info, "CLUSTER INFO"),
            (cluster_myid, "CLUSTER MYID"),
            (cluster_topology, "CLUSTER SLOTS"),
            (cluster_managed, "CLUSTER MEET a 1"),
            (asking, "ASKING"),
            (readonly, "READONLY"),
            (readwrite, "READWRITE"),
        ] {
            assert_eq!(f.run_conn(h, line), disabled, "{line}");
        }
        assert!(!f.conn.readonly);
    }

    #[test]
    fn cluster_mode_replies() {
        let mut f = Fixture::with_config(Config {
            cluster_enabled: true,
            databases: 1,
            ..Config::default()
        });
        assert_eq!(
            f.run_conn(cluster_keyslot, "CLUSTER KEYSLOT 123456789"),
            format!(":{}\r\n", 0x31C3)
        );
        assert_eq!(
            f.run_conn(cluster_keyslot, "CLUSTER KEYSLOT {user1}:a"),
            format!(":{}\r\n", kv_common::key_slot(b"user1"))
        );
        assert_eq!(
            f.run_conn(cluster_topology, "CLUSTER NODES"),
            "-ERR cluster topology is served by the coordinator\r\n"
        );
        assert_eq!(
            f.run_conn(cluster_managed, "CLUSTER ADDSLOTS 1"),
            "-ERR cluster membership and slots are managed by kvadm\r\n"
        );
        assert_eq!(f.run_conn(asking, "ASKING"), "+OK\r\n");
        assert_eq!(f.run_conn(readonly, "READONLY"), "+OK\r\n");
        assert!(f.conn.readonly);
        assert_eq!(f.run_conn(readwrite, "READWRITE"), "+OK\r\n");
        assert!(!f.conn.readonly);
        let info = f.run_conn(cluster_info, "CLUSTER INFO");
        assert!(
            info.contains("cluster_state:fail\r\n"),
            "no lease yet: {info}"
        );
        f.shared.lease.set(u64::MAX);
        let info = f.run_conn(cluster_info, "CLUSTER INFO");
        assert!(
            info.contains("cluster_enabled:1\r\ncluster_state:ok\r\ncluster_slots_assigned:0\r\n"),
            "{info}"
        );
        assert!(
            info.contains("cluster_known_nodes:0\r\n") && info.contains("cluster_my_epoch:0\r\n")
        );
    }
}
