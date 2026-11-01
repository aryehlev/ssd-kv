//! Transactions (DESIGN §8.5): MULTI / EXEC / DISCARD (queued per connection; EXEC runs as one
//! partition request, or one XTxn in standalone mode when partitions differ; crash-atomic through
//! the intent table) and WATCH / UNWATCH (M1.4; per-partition watchers flagged dirty by executed
//! writes and FLUSH*).
//!
//! The connection side lives here: MULTI state, queueing with `+QUEUED`, queue-time rejections
//! (which make EXEC answer EXECABORT), the WATCH list and EXEC's start ([`exec_begin`]). The
//! body of an EXEC whose keys share a partition runs as one partition request through
//! [`run_queued`]: every queued command executes in order inside one staged `exec`, its replies
//! form the EXEC array, and everything commits together or not at all. A storage error (`-OOM`,
//! I/O) therefore fails the whole EXEC with that error instead of one element of it, which keeps
//! the transaction all-or-nothing on media. Per-partition watcher tables and the dirty flag
//! (`flags::WATCH_DIRTY`) are maintained by the partitions and the dispatcher.

use kv_cmd::CmdId;
use kv_resp::ReplyWriter;
use kv_storage::keyspace::StoreError;

use super::store::Store;
use super::{Args, Handler, err};
use crate::net::conn::{Conn, MultiState, WatchKey, flags};
use crate::runtime::xmsg::{ExecEnv, OwnedCmd};

/// Whether a command received inside MULTI is queued (everything but the transaction and
/// connection-control commands, which run at once).
pub fn is_queued(id: CmdId) -> bool {
    !matches!(
        id,
        CmdId::Exec | CmdId::Discard | CmdId::Multi | CmdId::Watch | CmdId::Quit | CmdId::Reset
    )
}

/// MULTI.
pub fn multi(conn: &mut Conn, w: &mut ReplyWriter<'_>) {
    if conn.multi.is_some() {
        w.error(b"ERR MULTI calls can not be nested");
    } else {
        conn.multi = Some(MultiState {
            queued: Vec::new(),
            errored: false,
        });
        w.ok();
    }
}

/// Queues a command inside MULTI (`+QUEUED`).
pub fn queue(conn: &mut Conn, cmd: OwnedCmd, w: &mut ReplyWriter<'_>) {
    match conn.multi.as_mut() {
        Some(m) => {
            m.queued.push(cmd);
            w.simple(b"QUEUED");
        }
        None => w.error(b"ERR internal: QUEUED outside MULTI"),
    }
}

/// A command refused at queue time (unknown command, arity, NO_MULTI, gate refusals): the
/// caller has written its error reply; EXEC will answer EXECABORT.
pub fn flag_queue_error(conn: &mut Conn) {
    if let Some(m) = conn.multi.as_mut() {
        m.errored = true;
    }
}

/// The reply to a NO_MULTI command inside MULTI (also flags the transaction).
pub fn reject_no_multi(conn: &mut Conn, w: &mut ReplyWriter<'_>) {
    w.error(b"ERR Command not allowed inside a transaction");
    flag_queue_error(conn);
}

/// Ends the transaction state and the WATCH list (after EXEC, DISCARD and RESET).
fn clear(conn: &mut Conn) {
    conn.multi = None;
    unwatch_all(conn);
}

fn unwatch_all(conn: &mut Conn) {
    conn.watch.clear();
    conn.flags &= !flags::WATCH_DIRTY;
}

/// DISCARD.
pub fn discard(conn: &mut Conn, w: &mut ReplyWriter<'_>) {
    if conn.multi.is_none() {
        w.error(b"ERR DISCARD without MULTI");
    } else {
        clear(conn);
        w.ok();
    }
}

/// WATCH key [key ...]: adds the keys of the connection's database (each `(db, key)` once).
/// The dispatcher registers new entries with their partitions' watcher tables.
pub fn watch(conn: &mut Conn, a: &Args<'_>, w: &mut ReplyWriter<'_>) {
    if conn.multi.is_some() {
        w.error(b"ERR WATCH inside MULTI is not allowed");
        return;
    }
    for &key in &a.argv[1..] {
        if !conn.watch.iter().any(|k| k.db == conn.db && *k.key == *key) {
            conn.watch.push(WatchKey {
                db: conn.db,
                key: key.into(),
            });
        }
    }
    w.ok();
}

/// UNWATCH.
pub fn unwatch(conn: &mut Conn, w: &mut ReplyWriter<'_>) {
    unwatch_all(conn);
    w.ok();
}

/// The outcome of EXEC's checks.
pub enum ExecStart {
    /// Answered (`EXEC without MULTI`, EXECABORT, or a null array for a dirty WATCH).
    Replied,
    /// Run these commands as one transaction and reply with their array (`run_queued`).
    Run(Vec<OwnedCmd>),
}

/// EXEC: validates the transaction state and takes the queued commands; the MULTI state and
/// the WATCH list end here in every case.
pub fn exec_begin(conn: &mut Conn, w: &mut ReplyWriter<'_>) -> ExecStart {
    let Some(m) = conn.multi.take() else {
        w.error(b"ERR EXEC without MULTI");
        return ExecStart::Replied;
    };
    let dirty = conn.flags & flags::WATCH_DIRTY != 0;
    unwatch_all(conn);
    if m.errored {
        w.error(b"EXECABORT Transaction discarded because of previous errors.");
        return ExecStart::Replied;
    }
    if dirty {
        w.null_array();
        return ExecStart::Replied;
    }
    ExecStart::Run(m.queued)
}

/// The argument slices of a queued command.
pub fn argv_of(cmd: &OwnedCmd) -> smallvec::SmallVec<[&[u8]; 8]> {
    cmd.args
        .iter()
        .map(|&(off, len)| &cmd.buf[off as usize..(off + len) as usize])
        .collect()
}

/// Whether every queued command can run inside one partition request (keyed commands and the
/// connection commands [`run_queued`] executes inline). Others need the home worker.
pub fn runs_in_partition(id: CmdId) -> bool {
    matches!(super::handler(id), Handler::Keyed(_)) || inline_conn_cmd(id)
}

/// The connection commands an EXEC body answers inline, from the request alone: they read no
/// node state and change nothing but the body's database.
fn inline_conn_cmd(id: CmdId) -> bool {
    matches!(
        id,
        CmdId::Ping
            | CmdId::Echo
            | CmdId::Select
            | CmdId::Unwatch
            | CmdId::Time
            | CmdId::ClientId
            | CmdId::Command
            | CmdId::CommandCount
            | CmdId::CommandInfo
            | CmdId::CommandDocs
            | CmdId::CommandList
            | CmdId::CommandGetkeys
    )
}

/// The reply of a queued command that needs the home worker or node-wide state (INFO, DBSIZE,
/// FLUSH*, CONFIG, HELLO, SCAN, ...): such commands are not executed inside a transaction.
const NOT_IN_TXN: &[u8] = b"ERR command not supported inside a single-partition transaction";

/// Runs an EXEC body inside one partition request: the EXEC array header, then each queued
/// command's reply in order. SELECT inside the transaction switches the database of the
/// following commands; the returned value is the connection's database afterwards.
/// `databases` bounds SELECT.
pub fn run_queued<S: Store>(
    ks: &mut S,
    cmds: &[OwnedCmd],
    w: &mut ReplyWriter<'_>,
    env: &ExecEnv,
    databases: u8,
) -> Result<u8, StoreError> {
    w.array(cmds.len());
    let mut env = *env;
    for cmd in cmds {
        let args = Args { argv: argv_of(cmd) };
        match cmd.id {
            CmdId::Ping => match args.len() {
                1 => w.simple(b"PONG"),
                2 => w.bulk(args.arg(1)),
                _ => super::write_arity_error("ping", w),
            },
            CmdId::Echo => w.bulk(args.arg(1)),
            CmdId::Select => match super::parse_i64(args.arg(1)) {
                None => w.error(err::NOT_INTEGER),
                Some(id) if i32::try_from(id).is_err() => w.error(err::INT_RANGE),
                Some(id) if id < 0 || id >= i64::from(databases) => w.error(err::DB_RANGE),
                Some(id) => {
                    ks.select(id as u8);
                    env.db = id as u8;
                    w.ok();
                }
            },
            // EXEC dropped the WATCH list before the body runs
            CmdId::Unwatch => w.ok(),
            CmdId::Time => super::server::write_time(w),
            CmdId::ClientId => w.int(env.conn_id as i64),
            // the command table is static
            CmdId::Command => kv_cmd::write_command_all(w),
            CmdId::CommandCount => w.int(kv_cmd::command_count() as i64),
            CmdId::CommandInfo => kv_cmd::write_command_info_reply(&args.argv[2..], w),
            CmdId::CommandDocs => kv_cmd::write_command_docs_reply(&args.argv[2..], w),
            CmdId::CommandList => kv_cmd::write_command_list_reply(&args.argv[2..], w),
            CmdId::CommandGetkeys => kv_cmd::write_command_getkeys(&args.argv[2..], w),
            id => match super::keyed::<S>(id) {
                Some(f) => f(ks, &args, w, &env)?,
                None => w.error(NOT_IN_TXN),
            },
        }
    }
    Ok(env.db)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::conn_cmds::tests::new_conn;
    use crate::cmd::testkit::{Model, show};
    use kv_resp::Proto;

    fn owned(line: &str) -> OwnedCmd {
        let mut buf = Vec::new();
        let mut args = smallvec::SmallVec::new();
        for wd in line.split_ascii_whitespace() {
            args.push((buf.len() as u32, wd.len() as u32));
            buf.extend_from_slice(wd.as_bytes());
        }
        let refs: Vec<&[u8]> = line.split_ascii_whitespace().map(str::as_bytes).collect();
        let id = kv_cmd::resolve(&refs).map(|s| s.id).unwrap_or(CmdId::Ping);
        OwnedCmd {
            id,
            buf: buf.into_boxed_slice(),
            args,
        }
    }

    fn reply(f: impl FnOnce(&mut ReplyWriter<'_>)) -> String {
        let mut out = Vec::new();
        f(&mut ReplyWriter::new(&mut out, Proto::Resp2));
        show(&out)
    }

    #[test]
    fn multi_discard_watch() {
        let mut c = new_conn(1);
        assert_eq!(
            reply(|w| discard(&mut c, w)),
            "-ERR DISCARD without MULTI\\r\\n"
        );
        assert_eq!(reply(|w| multi(&mut c, w)), "+OK\\r\\n");
        assert_eq!(
            reply(|w| multi(&mut c, w)),
            "-ERR MULTI calls can not be nested\\r\\n"
        );
        let args = Args {
            argv: [&b"WATCH"[..], b"a"].into_iter().collect(),
        };
        assert_eq!(
            reply(|w| watch(&mut c, &args, w)),
            "-ERR WATCH inside MULTI is not allowed\\r\\n"
        );
        assert_eq!(
            reply(|w| queue(&mut c, owned("SET x 1"), w)),
            "+QUEUED\\r\\n"
        );
        assert_eq!(reply(|w| discard(&mut c, w)), "+OK\\r\\n");
        assert!(c.multi.is_none());
        let args = Args {
            argv: [&b"WATCH"[..], b"a", b"b", b"a"].into_iter().collect(),
        };
        assert_eq!(reply(|w| watch(&mut c, &args, w)), "+OK\\r\\n");
        assert_eq!(c.watch.len(), 2);
        c.db = 1;
        assert_eq!(reply(|w| watch(&mut c, &args, w)), "+OK\\r\\n");
        assert_eq!(c.watch.len(), 4, "keys are per database");
        c.flags |= flags::WATCH_DIRTY;
        assert_eq!(reply(|w| unwatch(&mut c, w)), "+OK\\r\\n");
        assert!(c.watch.is_empty() && c.flags & flags::WATCH_DIRTY == 0);
        assert!(
            !is_queued(CmdId::Exec)
                && !is_queued(CmdId::Watch)
                && is_queued(CmdId::Get)
                && is_queued(CmdId::Select)
        );
    }

    #[test]
    fn exec_outcomes() {
        let mut c = new_conn(1);
        assert_eq!(
            reply(|w| assert!(matches!(exec_begin(&mut c, w), ExecStart::Replied))),
            "-ERR EXEC without MULTI\\r\\n"
        );
        multi(&mut c, &mut ReplyWriter::new(&mut Vec::new(), Proto::Resp2));
        flag_queue_error(&mut c);
        assert_eq!(
            reply(|w| assert!(matches!(exec_begin(&mut c, w), ExecStart::Replied))),
            "-EXECABORT Transaction discarded because of previous errors.\\r\\n"
        );
        assert!(c.multi.is_none());
        multi(&mut c, &mut ReplyWriter::new(&mut Vec::new(), Proto::Resp2));
        c.watch.push(WatchKey {
            db: 0,
            key: b"k".to_vec().into(),
        });
        c.flags |= flags::WATCH_DIRTY;
        assert_eq!(
            reply(|w| assert!(matches!(exec_begin(&mut c, w), ExecStart::Replied))),
            "*-1\\r\\n"
        );
        assert!(c.watch.is_empty(), "EXEC unwatches");
        multi(&mut c, &mut ReplyWriter::new(&mut Vec::new(), Proto::Resp2));
        queue(
            &mut c,
            owned("SET k v"),
            &mut ReplyWriter::new(&mut Vec::new(), Proto::Resp2),
        );
        let mut out = Vec::new();
        let ExecStart::Run(cmds) =
            exec_begin(&mut c, &mut ReplyWriter::new(&mut out, Proto::Resp2))
        else {
            panic!()
        };
        assert!(out.is_empty());
        assert_eq!(cmds.len(), 1);
        assert_eq!(argv_of(&cmds[0]).as_slice(), &[&b"SET"[..], b"k", b"v"]);
        let mut out = Vec::new();
        reject_no_multi(&mut c, &mut ReplyWriter::new(&mut out, Proto::Resp2));
    }

    #[test]
    fn run_queued_body() {
        let mut m = Model::new(1_000);
        let cmds = [
            owned("SELECT 3"),
            owned("SET k v"),
            owned("PING"),
            owned("MSET a b c"),
            owned("INCR k"),
            owned("GET k"),
            owned("UNWATCH"),
            owned("CLIENT ID"),
            owned("DBSIZE"),
            owned("COMMAND GETKEYS GET k"),
        ];
        let env = ExecEnv {
            proto: Proto::Resp2,
            db: 0,
            now_ms: 1_000,
            readonly_conn: false,
            conn_id: 1,
        };
        let mut out = Vec::new();
        let db = run_queued(
            &mut m,
            &cmds,
            &mut ReplyWriter::new(&mut out, Proto::Resp2),
            &env,
            16,
        )
        .unwrap();
        assert_eq!(db, 3);
        assert_eq!(
            show(&out),
            "*10\\r\\n+OK\\r\\n+OK\\r\\n+PONG\\r\\n-ERR wrong number of arguments for 'mset' command\\r\\n\
             -ERR value is not an integer or out of range\\r\\n$1\\r\\nv\\r\\n+OK\\r\\n:1\\r\\n\
             -ERR command not supported inside a single-partition transaction\\r\\n*1\\r\\n$1\\r\\nk\\r\\n"
        );
        assert!(m.rec(3, b"k").is_some() && m.rec(0, b"k").is_none());
        let cmds = [owned("SELECT 16")];
        let mut out = Vec::new();
        assert_eq!(
            run_queued(
                &mut m,
                &cmds,
                &mut ReplyWriter::new(&mut out, Proto::Resp2),
                &env,
                16
            )
            .unwrap(),
            0
        );
        assert_eq!(show(&out), "*1\\r\\n-ERR DB index is out of range\\r\\n");
        assert!(
            runs_in_partition(CmdId::Set)
                && runs_in_partition(CmdId::Select)
                && runs_in_partition(CmdId::Unwatch)
                && !runs_in_partition(CmdId::Info)
                && !runs_in_partition(CmdId::Dbsize)
        );
        let mut out = Vec::new();
        run_queued(
            &mut m,
            &[owned("TIME")],
            &mut ReplyWriter::new(&mut out, Proto::Resp2),
            &env,
            16,
        )
        .unwrap();
        assert!(
            show(&out).starts_with("*1\\r\\n*2\\r\\n$10\\r\\n"),
            "{}",
            show(&out)
        );
    }
}
