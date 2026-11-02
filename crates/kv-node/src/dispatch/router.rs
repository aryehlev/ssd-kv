//! Routing of one parsed command (DESIGN §6.2, §7.5, §23.6).
//!
//! Unknown command -> `-ERR unknown command 'x', with args beginning with: ...` (Redis 7.0
//! format); arity error -> `-ERR wrong number of arguments for 'x' command`; NOAUTH before AUTH;
//! inside MULTI, `+QUEUED` (queue-time errors abort EXEC). Cluster mode: `Ownership::gate()`
//! decides Serve / MOVED to a coordinator / CROSSSLOT / NOREPLICAS / TRYAGAIN / READONLY /
//! CLUSTERDOWN before execution (and again at queue time inside MULTI). Then
//! `slot = key_slot(key)`, `partition = slot % P`, `worker = partition % W`.
//!
//! The admission order is Redis 7.0's `processCommand`: lookup and arity, then authentication,
//! then the cluster gate, then `-LOADING`, then MULTI queueing. Nothing here waits or reads a
//! clock (LC-14): a command is either routed or answered at once.

use kv_cmd::{CmdFlags, CommandSpec};

use crate::cluster::ownership::Gate;
use crate::cmd::FanOutSplit;
use crate::dispatch::{multikey, xtxn};
use crate::runtime::xmsg::ExecEnv;

/// `-NOAUTH` text (§7.6).
pub const NOAUTH: &[u8] = b"NOAUTH Authentication required.";
/// `-LOADING` text (§18.1).
pub const LOADING: &[u8] = b"LOADING Redis is loading the dataset in memory";

/// Where a command goes.
pub enum Route {
    /// Execute on a partition of this worker.
    Local { partition: u16 },
    /// Send to the owner worker.
    Remote { worker: u16, partition: u16 },
    /// Split across partitions (standalone multi-key reads/deletes).
    FanOut(FanOutSplit),
    /// Cross-partition transaction (standalone).
    XTxn,
    /// Keyless command handled on the home worker.
    Home,
    /// Answered by the gate without execution.
    Refused(Gate),
}

/// Partition of a slot.
pub fn partition_of(slot: u16, partitions: u16) -> u16 {
    slot % partitions
}

/// Worker owning a partition.
pub fn worker_of(partition: u16, workers: u16) -> u16 {
    partition % workers
}

/// The routing facts of a node, fixed for a worker's lifetime.
#[derive(Clone, Copy, Debug)]
pub struct Router {
    /// P.
    pub partitions: u16,
    /// W.
    pub workers: u16,
    /// This worker.
    pub me: u16,
    pub cluster: bool,
}

impl Router {
    /// Partition of a key (`key_slot(key) % P`).
    #[inline]
    pub fn partition_of_key(&self, key: &[u8]) -> u16 {
        partition_of(kv_common::key_slot(key), self.partitions)
    }

    #[inline]
    fn place(&self, partition: u16) -> Route {
        let worker = worker_of(partition, self.workers);
        if worker == self.me { Route::Local { partition } } else { Route::Remote { worker, partition } }
    }

    /// Routes a keyed command whose keys are at `pos`. In cluster mode `gate` is asked about
    /// the keys' common slot (the ownership gate, §23.6); standalone never asks it.
    pub fn route(&self, spec: &CommandSpec, argv: &[&[u8]], pos: &[u16], env: &ExecEnv, gate: impl FnOnce(Option<u16>) -> Gate) -> Route {
        let _ = env;
        if pos.is_empty() {
            return Route::Home;
        }
        if self.cluster {
            let slot = match kv_cmd::common_slot(argv, pos) {
                Ok(s) => s,
                Err(_) => return Route::Refused(Gate::CrossSlot),
            };
            return match gate(slot) {
                Gate::Serve => self.place(partition_of(slot.unwrap_or(0), self.partitions)),
                g => Route::Refused(g),
            };
        }
        let first = self.partition_of_key(argv[usize::from(pos[0])]);
        if pos[1..].iter().all(|&i| self.partition_of_key(argv[usize::from(i)]) == first) {
            return self.place(first);
        }
        if multikey::is_fanout_command(spec.id) {
            Route::FanOut(multikey::split(argv, pos, self.partitions))
        } else if xtxn::is_xtxn_command(spec.id) {
            Route::XTxn
        } else {
            // other multi-key commands (WATCH) are handled by their own drivers
            Route::Home
        }
    }
}

/// Whether a connection that has not authenticated may run `spec` (§7.6).
#[inline]
pub fn allowed_before_auth(spec: &CommandSpec) -> bool {
    spec.flags.contains(CmdFlags::NO_AUTH)
}

/// Whether `spec` is served while partitions are loading (§18.1: INFO, CONFIG GET, CLIENT,
/// HELLO, AUTH, SELECT, QUIT, RESET, COMMAND, ... carry `LOADING_OK`).
#[inline]
pub fn allowed_while_loading(spec: &CommandSpec) -> bool {
    spec.flags.contains(CmdFlags::LOADING_OK)
}

/// Writes the reply of a gate refusal.
pub fn write_gate(g: Gate, w: &mut kv_resp::ReplyWriter<'_>) {
    match g {
        Gate::Serve => {}
        Gate::Moved(slot, addr) => {
            let msg = format!("MOVED {slot} {addr}");
            w.error(msg.as_bytes());
        }
        Gate::CrossSlot => w.error(kv_cmd::CrossSlot::MESSAGE),
        Gate::NoReplicas => w.error(b"NOREPLICAS Not enough good replicas to write."),
        Gate::TryAgain => w.error(b"TRYAGAIN Multiple keys request during rehashing of slot"),
        Gate::ReadOnlyReplica => w.error(b"READONLY You can't write against a read only replica."),
        Gate::ClusterDown => w.error(b"CLUSTERDOWN The cluster is down"),
        Gate::MigrationRunning => w.error(
            b"ERR FLUSHDB and FLUSHALL are refused while a slot migration of this shard runs (see kvadm migrations; retry after it is done, paused or aborted)",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kv_cmd::CmdId;
    use kv_resp::Proto;

    fn env() -> ExecEnv {
        ExecEnv { proto: Proto::Resp2, db: 0, now_ms: 0, readonly_conn: false, conn_id: 1 }
    }

    fn keys(spec: &CommandSpec, argv: &[&[u8]]) -> smallvec::SmallVec<[u16; 4]> {
        kv_cmd::key_positions(spec, argv)
    }

    #[test]
    fn single_key_goes_to_its_owner() {
        let r = Router { partitions: 64, workers: 4, me: 1, cluster: false };
        let argv: [&[u8]; 2] = [b"GET", b"foo"];
        let spec = CmdId::Get.spec();
        let p = kv_common::key_slot(b"foo") % 64;
        match r.route(spec, &argv, &keys(spec, &argv), &env(), |_| panic!("no gate in standalone")) {
            Route::Local { partition } => assert!(p % 4 == 1 && partition == p),
            Route::Remote { worker, partition } => assert_eq!((worker, partition), (p % 4, p)),
            _ => panic!("single partition"),
        }
        assert_eq!(partition_of(16383, 64), 16383 % 64);
        assert_eq!(worker_of(13, 4), 1);
    }

    #[test]
    fn multi_partition_commands_split_or_become_transactions() {
        let r = Router { partitions: 64, workers: 2, me: 0, cluster: false };
        let (a, b) = (&b"{a}x"[..], &b"{b}x"[..]);
        assert_ne!(r.partition_of_key(a), r.partition_of_key(b));
        let mget: [&[u8]; 3] = [b"MGET", a, b];
        let spec = CmdId::Mget.spec();
        let Route::FanOut(s) = r.route(spec, &mget, &keys(spec, &mget), &env(), |_| Gate::Serve) else { panic!() };
        assert_eq!(s.parts.len(), 2);
        let mset: [&[u8]; 5] = [b"MSET", a, b"1", b, b"2"];
        let spec = CmdId::Mset.spec();
        assert!(matches!(r.route(spec, &mset, &keys(spec, &mset), &env(), |_| Gate::Serve), Route::XTxn));
        let same: [&[u8]; 5] = [b"MSET", b"{a}1", b"1", b"{a}2", b"2"];
        assert!(matches!(r.route(spec, &same, &keys(spec, &same), &env(), |_| Gate::Serve), Route::Local { .. } | Route::Remote { .. }));
        let ping: [&[u8]; 1] = [b"PING"];
        assert!(matches!(r.route(CmdId::Ping.spec(), &ping, &[], &env(), |_| Gate::Serve), Route::Home));
    }

    #[test]
    fn cluster_mode_asks_the_gate_and_refuses_cross_slot() {
        let r = Router { partitions: 64, workers: 1, me: 0, cluster: true };
        let mget: [&[u8]; 3] = [b"MGET", b"{a}x", b"{b}x"];
        let spec = CmdId::Mget.spec();
        assert!(matches!(r.route(spec, &mget, &keys(spec, &mget), &env(), |_| Gate::Serve), Route::Refused(Gate::CrossSlot)));
        let get: [&[u8]; 2] = [b"GET", b"k"];
        let spec = CmdId::Get.spec();
        let addr: std::net::SocketAddr = "10.0.0.1:7000".parse().unwrap();
        let slot = kv_common::key_slot(b"k");
        let route = r.route(spec, &get, &keys(spec, &get), &env(), |s| {
            assert_eq!(s, Some(slot));
            Gate::Moved(slot, addr)
        });
        let Route::Refused(g) = route else { panic!() };
        let mut out = Vec::new();
        write_gate(g, &mut kv_resp::ReplyWriter::new(&mut out, Proto::Resp2));
        assert_eq!(out, format!("-MOVED {slot} 10.0.0.1:7000\r\n").into_bytes());
    }

    #[test]
    fn admission_flags() {
        assert!(allowed_before_auth(CmdId::Auth.spec()) && allowed_before_auth(CmdId::Hello.spec()));
        assert!(!allowed_before_auth(CmdId::Get.spec()));
        assert!(allowed_while_loading(CmdId::Info.spec()) && !allowed_while_loading(CmdId::Get.spec()));
    }
}
