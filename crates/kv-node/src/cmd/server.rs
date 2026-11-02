//! Server commands (DESIGN §8.2): COMMAND (from the kv-cmd table), DBSIZE (sum of the
//! per-partition counters; counts logically expired but unreclaimed keys like Redis), FLUSHDB /
//! FLUSHALL (per-partition seq cutoffs, reply after the cutoff metadata is durable, §16.5), TIME,
//! SAVE / BGSAVE / LASTSAVE (durability barriers; no dump file exists), SHUTDOWN (drain, snapshot
//! when it fits, CLEAN), WAIT / WAITAOF, TOUCH, and the REPLICAOF/SLAVEOF refusal
//! `-ERR replication is managed by kvadm`.
//!
//! Commands that need every partition or every worker (FLUSH*, SAVE, BGSAVE, WAITAOF with a
//! local count, SHUTDOWN, CLIENT LIST / KILL) validate their arguments here, write nothing and
//! return `ConnOutcome::Async`; [`node_op`] then names the [`NodeOp`] the dispatcher runs, and
//! [`write_node_op_reply`] answers once it is done.

use kv_cmd::CmdId;
use kv_resp::ReplyWriter;
use kv_storage::keyspace::StoreError;

use super::conn_cmds::{ClientFilter, KillFilter};
use super::store::Store;
use super::{Args, ConnCtx, ConnOutcome, err, is_opt, parse_i64};
use crate::net::conn::Conn;
use crate::runtime::xmsg::ExecEnv;

/// A node-level operation started by a connection command (see the module docs).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum NodeOp {
    /// FLUSHDB (`Some(db)`) or FLUSHALL (`None`) on every partition (§16.5). Both ASYNC and
    /// SYNC are synchronous; the reply waits until every partition's cutoff is durable (E5).
    Flush { db: Option<u8> },
    /// A durability barrier: every executed effect written and flushed (§8.2).
    Barrier(BarrierKind),
    /// SHUTDOWN (§8.2): stop accepting, drain, snapshot unless `save == Some(false)`, CLEAN.
    Shutdown {
        save: Option<bool>,
        now: bool,
        force: bool,
    },
    /// CLIENT LIST over every worker's connections.
    ClientList(ClientFilter),
    /// CLIENT KILL over every worker's connections (`old_form`: `CLIENT KILL addr:port`).
    ClientKill {
        filter: KillFilter,
        old_form: bool,
        caller: u64,
    },
}

/// Which command a barrier answers.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BarrierKind {
    /// SAVE: `+OK` when the barrier completes.
    Save,
    /// BGSAVE: `+Background saving started` at once; the barrier runs in the background.
    Bgsave,
    /// WAITAOF: `[1, replicas]` when the barrier completes.
    Waitaof,
}

impl NodeOp {
    /// Whether the reply is written as soon as the operation is started (BGSAVE) rather than
    /// when it completes.
    pub fn replies_at_start(&self) -> bool {
        matches!(self, NodeOp::Barrier(BarrierKind::Bgsave))
    }
}

/// The result of a completed [`NodeOp`].
pub enum NodeOpDone<'a> {
    /// Flush or barrier completed (or BGSAVE started).
    Ok,
    /// The operation failed on some partition.
    Failed(StoreError),
    /// CLIENT LIST: the concatenated lines of every matching connection
    /// (`conn_cmds::write_client_line`), in any worker order.
    Clients(&'a [u8]),
    /// CLIENT KILL: connections closed.
    Killed(u64),
}

/// The node operation `id` asked for (after its handler returned `ConnOutcome::Async`).
pub fn node_op(id: CmdId, conn: &Conn, argv: &[&[u8]]) -> Option<NodeOp> {
    match id {
        CmdId::Flushdb => Some(NodeOp::Flush { db: Some(conn.db) }),
        CmdId::Flushall => Some(NodeOp::Flush { db: None }),
        CmdId::Save => Some(NodeOp::Barrier(BarrierKind::Save)),
        CmdId::Bgsave => Some(NodeOp::Barrier(BarrierKind::Bgsave)),
        CmdId::Waitaof => Some(NodeOp::Barrier(BarrierKind::Waitaof)),
        CmdId::Shutdown => parse_shutdown(argv).ok().flatten(),
        CmdId::ClientList => super::conn_cmds::parse_client_list(argv)
            .ok()
            .map(NodeOp::ClientList),
        CmdId::ClientKill => {
            super::conn_cmds::parse_client_kill(argv)
                .ok()
                .map(|(filter, old_form)| NodeOp::ClientKill {
                    filter,
                    old_form,
                    caller: conn.id,
                })
        }
        _ => None,
    }
}

/// Writes the reply of a completed (or, for BGSAVE, started) node operation.
pub fn write_node_op_reply(op: &NodeOp, done: NodeOpDone<'_>, w: &mut ReplyWriter<'_>) {
    if let NodeOpDone::Failed(e) = done {
        return match op {
            NodeOp::Shutdown { .. } => w.error(b"ERR Errors trying to SHUTDOWN. Check logs."),
            _ => super::write_store_error(e, w),
        };
    }
    match op {
        NodeOp::Flush { .. } | NodeOp::Barrier(BarrierKind::Save) | NodeOp::Shutdown { .. } => {
            w.ok()
        }
        NodeOp::Barrier(BarrierKind::Bgsave) => w.simple(b"Background saving started"),
        NodeOp::Barrier(BarrierKind::Waitaof) => {
            // the local copy is durable; replicas acknowledge only in cluster mode
            w.array(2);
            w.int(1);
            w.int(0);
        }
        NodeOp::ClientList(_) => {
            let lines = match done {
                NodeOpDone::Clients(l) => l,
                _ => b"",
            };
            w.verbatim(b"txt", lines);
        }
        NodeOp::ClientKill { old_form, .. } => {
            let n = match done {
                NodeOpDone::Killed(n) => n,
                _ => 0,
            };
            if !*old_form {
                w.int(n as i64);
            } else if n == 0 {
                w.error(b"ERR No such client");
            } else {
                w.ok();
            }
        }
    }
}

/// Records a completed durability barrier (SAVE, BGSAVE, SHUTDOWN): LASTSAVE reports it.
pub fn note_barrier_done(shared: &crate::shared::NodeShared, unix_ms: u64) {
    shared
        .lastsave_unix
        .store(unix_ms / 1000, std::sync::atomic::Ordering::Relaxed);
}

// ---------------------------------------------------------------------------------------------
// COMMAND (thin wrappers over the kv-cmd table)

/// COMMAND: the COMMAND INFO entry of every command.
pub fn command(_cx: &mut ConnCtx<'_>, _a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    kv_cmd::write_command_all(w);
    ConnOutcome::Done
}

pub fn command_count(_cx: &mut ConnCtx<'_>, _a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    w.int(kv_cmd::command_count() as i64);
    ConnOutcome::Done
}

pub fn command_info(_cx: &mut ConnCtx<'_>, a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    kv_cmd::write_command_info_reply(&a.argv[2..], w);
    ConnOutcome::Done
}

pub fn command_docs(_cx: &mut ConnCtx<'_>, a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    kv_cmd::write_command_docs_reply(&a.argv[2..], w);
    ConnOutcome::Done
}

pub fn command_list(_cx: &mut ConnCtx<'_>, a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    kv_cmd::write_command_list_reply(&a.argv[2..], w);
    ConnOutcome::Done
}

pub fn command_getkeys(
    _cx: &mut ConnCtx<'_>,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
) -> ConnOutcome {
    kv_cmd::write_command_getkeys(&a.argv[2..], w);
    ConnOutcome::Done
}

// ---------------------------------------------------------------------------------------------
// keyspace-wide commands

/// DBSIZE: the sum of the partitions' published key counts of the connection's database (in
/// cluster mode only the keys of the slots this node serves).
pub fn dbsize(cx: &mut ConnCtx<'_>, _a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    w.int(cx.shared.served_dbsize(cx.conn.db) as i64);
    ConnOutcome::Done
}

/// Redis 7.0 `getFlushCommandFlags`: no argument, or one of SYNC / ASYNC.
fn flush_args_ok(a: &Args<'_>) -> bool {
    match a.len() {
        1 => true,
        2 => is_opt(a.arg(1), b"sync") || is_opt(a.arg(1), b"async"),
        _ => false,
    }
}

/// FLUSHDB [ASYNC|SYNC] (§16.5): node operation [`NodeOp::Flush`].
pub fn flushdb(_cx: &mut ConnCtx<'_>, a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    if !flush_args_ok(a) {
        w.error(err::SYNTAX);
        return ConnOutcome::Done;
    }
    ConnOutcome::Async
}

/// FLUSHALL [ASYNC|SYNC] (§16.5): node operation [`NodeOp::Flush`].
pub fn flushall(cx: &mut ConnCtx<'_>, a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    flushdb(cx, a, w)
}

/// TOUCH k..: the number of existing keys (meta access; no write).
pub fn touch<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    let mut n = 0i64;
    for &key in &a.argv[1..] {
        n += i64::from(ks.meta(key)?.is_some());
    }
    w.int(n);
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// time, durability barriers, shutdown

/// TIME: `[unix seconds, microseconds]` as two bulks.
pub fn time(_cx: &mut ConnCtx<'_>, _a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    write_time(w);
    ConnOutcome::Done
}

/// The TIME reply: `[unix seconds, microseconds]` as bulks.
pub(crate) fn write_time(w: &mut ReplyWriter<'_>) {
    let us = super::expire::unix_time_us();
    w.array(2);
    w.bulk_i64((us / 1_000_000) as i64);
    w.bulk_i64((us % 1_000_000) as i64);
}

/// SAVE: a durability barrier; `+OK` when every executed effect is written and flushed.
pub fn save(_cx: &mut ConnCtx<'_>, _a: &Args<'_>, _w: &mut ReplyWriter<'_>) -> ConnOutcome {
    ConnOutcome::Async
}

/// BGSAVE [SCHEDULE]: starts a barrier, replies `+Background saving started`.
pub fn bgsave(_cx: &mut ConnCtx<'_>, a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    if a.len() > 1 && !(a.len() == 2 && is_opt(a.arg(1), b"schedule")) {
        w.error(err::SYNTAX);
        return ConnOutcome::Done;
    }
    ConnOutcome::Async
}

/// LASTSAVE: Unix seconds of the last completed barrier (the startup time at first).
pub fn lastsave(cx: &mut ConnCtx<'_>, _a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    w.int(
        cx.shared
            .lastsave_unix
            .load(std::sync::atomic::Ordering::Relaxed) as i64,
    );
    ConnOutcome::Done
}

/// Redis 7.0 SHUTDOWN arguments: `Ok(None)` is a valid ABORT (nothing to abort).
fn parse_shutdown(argv: &[&[u8]]) -> Result<Option<NodeOp>, ()> {
    let (mut save, mut nosave, mut now, mut force, mut abort) = (false, false, false, false, false);
    for &arg in &argv[1..] {
        if is_opt(arg, b"nosave") {
            nosave = true;
        } else if is_opt(arg, b"save") {
            save = true;
        } else if is_opt(arg, b"now") {
            now = true;
        } else if is_opt(arg, b"force") {
            force = true;
        } else if is_opt(arg, b"abort") {
            abort = true;
        } else {
            return Err(());
        }
    }
    if (abort && (save || nosave || now || force)) || (save && nosave) {
        return Err(());
    }
    if abort {
        return Ok(None);
    }
    let save = if save {
        Some(true)
    } else if nosave {
        Some(false)
    } else {
        None
    };
    Ok(Some(NodeOp::Shutdown { save, now, force }))
}

/// SHUTDOWN [NOSAVE|SAVE] [NOW] [FORCE] [ABORT].
pub fn shutdown(_cx: &mut ConnCtx<'_>, a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    match parse_shutdown(&a.argv) {
        Err(()) => w.error(err::SYNTAX),
        // shutdown runs to completion once started, so there is never one to abort
        Ok(None) => w.error(b"ERR No shutdown in progress."),
        Ok(Some(_)) => return ConnOutcome::Async,
    }
    ConnOutcome::Done
}

/// Redis `getTimeoutFromObjectOrReply` for millisecond timeouts.
fn timeout_ok(arg: &[u8], w: &mut ReplyWriter<'_>) -> bool {
    match parse_i64(arg) {
        None => w.error(b"ERR timeout is not an integer or out of range"),
        Some(t) if t < 0 => w.error(b"ERR timeout is negative"),
        Some(_) => return true,
    }
    false
}

/// WAIT numreplicas timeout: standalone has no replicas (`:0` at once); in cluster mode an
/// acknowledged write is already replicated to the ISR (§23.8), so nothing is left to wait for.
pub fn wait(_cx: &mut ConnCtx<'_>, a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    if parse_i64(a.arg(1)).is_none() {
        w.error(err::NOT_INTEGER);
    } else if timeout_ok(a.arg(2), w) {
        w.int(0);
    }
    ConnOutcome::Done
}

/// WAITAOF numlocal numreplicas timeout (Redis 7.2 arguments): with `numlocal` 1 a barrier
/// makes the connection's writes durable, then `[1, 0]`.
pub fn waitaof(_cx: &mut ConnCtx<'_>, a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    let Some(numlocal) = parse_i64(a.arg(1)) else {
        w.error(err::NOT_INTEGER);
        return ConnOutcome::Done;
    };
    if !(0..=1).contains(&numlocal) {
        w.error(b"ERR value is out of range, value must between 0 and 1");
        return ConnOutcome::Done;
    }
    match parse_i64(a.arg(2)) {
        None => {
            w.error(err::NOT_INTEGER);
            return ConnOutcome::Done;
        }
        Some(n) if n < 0 => {
            w.error(b"ERR value is out of range, must be positive");
            return ConnOutcome::Done;
        }
        Some(_) => {}
    }
    if !timeout_ok(a.arg(3), w) {
        return ConnOutcome::Done;
    }
    if numlocal == 0 {
        w.array(2);
        w.int(1);
        w.int(0);
        return ConnOutcome::Done;
    }
    ConnOutcome::Async
}

/// REPLICAOF / SLAVEOF: replication is configured through the operator tool only.
pub fn replicaof(_cx: &mut ConnCtx<'_>, _a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    w.error(b"ERR replication is managed by kvadm");
    ConnOutcome::Done
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::conn_cmds::tests::{Fixture, words_owned};
    use crate::cmd::testkit::{Model, check};
    use kv_resp::Proto;

    #[test]
    fn touch_counts_existing() {
        let mut m = Model::new(1000);
        m.set_str(b"a", b"1", None);
        m.set_str(b"x", b"1", Some(1000));
        check(&mut m, "TOUCH a a b x", ":2\r\n");
        assert_eq!(m.writes, 0);
    }

    #[test]
    fn flush_and_barrier_arguments() {
        let mut f = Fixture::new();
        assert_eq!(f.run_conn(flushdb, "FLUSHDB a b"), "-ERR syntax error\r\n");
        assert_eq!(
            f.run_conn(flushdb, "FLUSHDB bogus"),
            "-ERR syntax error\r\n"
        );
        assert!(
            f.async_op("FLUSHDB async")
                .is_some_and(|op| op == NodeOp::Flush { db: Some(0) })
        );
        f.conn.db = 3;
        assert!(
            f.async_op("FLUSHDB")
                .is_some_and(|op| op == NodeOp::Flush { db: Some(3) })
        );
        assert!(
            f.async_op("FLUSHALL SYNC")
                .is_some_and(|op| op == NodeOp::Flush { db: None })
        );
        assert_eq!(f.run_conn(bgsave, "BGSAVE x"), "-ERR syntax error\r\n");
        assert_eq!(
            f.run_conn(bgsave, "BGSAVE SCHEDULE x"),
            "-ERR syntax error\r\n"
        );
        let op = f.async_op("BGSAVE schedule").unwrap();
        assert!(op.replies_at_start());
        assert_eq!(reply(&op, NodeOpDone::Ok), "+Background saving started\r\n");
        let op = f.async_op("SAVE").unwrap();
        assert_eq!(reply(&op, NodeOpDone::Ok), "+OK\r\n");
        assert_eq!(
            reply(&op, NodeOpDone::Failed(StoreError::Io(5))),
            "-ERR storage I/O error\r\n"
        );
    }

    fn reply(op: &NodeOp, done: NodeOpDone<'_>) -> String {
        let mut out = Vec::new();
        write_node_op_reply(op, done, &mut ReplyWriter::new(&mut out, Proto::Resp2));
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn shutdown_arguments() {
        let mut f = Fixture::new();
        assert_eq!(
            f.run_conn(shutdown, "SHUTDOWN SAVE NOSAVE"),
            "-ERR syntax error\r\n"
        );
        assert_eq!(
            f.run_conn(shutdown, "SHUTDOWN ABORT"),
            "-ERR No shutdown in progress.\r\n"
        );
        assert_eq!(
            f.run_conn(shutdown, "SHUTDOWN ABORT NOW"),
            "-ERR syntax error\r\n"
        );
        assert_eq!(
            f.run_conn(shutdown, "SHUTDOWN foo"),
            "-ERR syntax error\r\n"
        );
        assert_eq!(
            f.async_op("SHUTDOWN nosave NOW"),
            Some(NodeOp::Shutdown {
                save: Some(false),
                now: true,
                force: false
            })
        );
        assert_eq!(
            f.async_op("SHUTDOWN"),
            Some(NodeOp::Shutdown {
                save: None,
                now: false,
                force: false
            })
        );
        let op = f.async_op("SHUTDOWN FORCE SAVE").unwrap();
        assert_eq!(
            reply(&op, NodeOpDone::Failed(StoreError::Io(5))),
            "-ERR Errors trying to SHUTDOWN. Check logs.\r\n"
        );
    }

    #[test]
    fn wait_family() {
        let mut f = Fixture::new();
        assert_eq!(
            f.run_conn(wait, "WAIT abc 0"),
            "-ERR value is not an integer or out of range\r\n"
        );
        assert_eq!(
            f.run_conn(wait, "WAIT 0 abc"),
            "-ERR timeout is not an integer or out of range\r\n"
        );
        assert_eq!(
            f.run_conn(wait, "WAIT 0 -1"),
            "-ERR timeout is negative\r\n"
        );
        assert_eq!(f.run_conn(wait, "WAIT 1 1"), ":0\r\n");
        assert_eq!(f.run_conn(wait, "WAIT -1 0"), ":0\r\n");
        assert_eq!(
            f.run_conn(waitaof, "WAITAOF 2 0 0"),
            "-ERR value is out of range, value must between 0 and 1\r\n"
        );
        assert_eq!(
            f.run_conn(waitaof, "WAITAOF 0 -1 0"),
            "-ERR value is out of range, must be positive\r\n"
        );
        assert_eq!(
            f.run_conn(waitaof, "WAITAOF 0 0 -1"),
            "-ERR timeout is negative\r\n"
        );
        assert_eq!(f.run_conn(waitaof, "WAITAOF 0 0 0"), "*2\r\n:1\r\n:0\r\n");
        let op = f.async_op("WAITAOF 1 0 0").unwrap();
        assert_eq!(reply(&op, NodeOpDone::Ok), "*2\r\n:1\r\n:0\r\n");
    }

    #[test]
    fn misc_server_commands() {
        let mut f = Fixture::new();
        assert_eq!(
            f.run_conn(replicaof, "REPLICAOF a b"),
            "-ERR replication is managed by kvadm\r\n"
        );
        assert_eq!(f.run_conn(dbsize, "DBSIZE"), ":0\r\n");
        f.shared.counters[1].keys[0].store(5, std::sync::atomic::Ordering::Relaxed);
        f.shared.counters[2].keys[0].store(2, std::sync::atomic::Ordering::Relaxed);
        f.shared.counters[2].keys[1].store(9, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(f.run_conn(dbsize, "DBSIZE"), ":7\r\n");
        f.conn.db = 1;
        assert_eq!(f.run_conn(dbsize, "DBSIZE"), ":9\r\n");
        let t = f.run_conn(time, "TIME");
        assert!(t.starts_with("*2\r\n$10\r\n"), "{t}");
        note_barrier_done(&f.shared, 1_234_567_000);
        assert_eq!(f.run_conn(lastsave, "LASTSAVE"), ":1234567\r\n");
        assert!(f.run_conn(command_count, "COMMAND COUNT").starts_with(':'));
        assert_eq!(
            f.run_conn(command_getkeys, "COMMAND GETKEYS GET k"),
            "*1\r\n$1\r\nk\r\n"
        );
        assert_eq!(
            f.run_conn(command_info, "COMMAND INFO nosuch"),
            "*1\r\n$-1\r\n"
        );
        let _ = words_owned("x");
    }
}
