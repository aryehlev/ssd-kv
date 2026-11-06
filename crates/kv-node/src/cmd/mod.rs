//! Command handlers and their registry (DESIGN §8, §24.11).
//!
//! Every command id maps to one [`Handler`]: `Keyed` handlers run inside a partition's `exec`
//! against a `Keyspace` (re-runnable: they mutate only through staged `Keyspace` calls); `Conn`
//! handlers run on the connection's home worker for keyless and connection commands; `Scan` and
//! `Txn` are the SCAN/KEYS step machinery and MULTI/EXEC; `Cluster` handles CLUSTER and related
//! commands. Handlers never read `Ownership` fields; they call `gate()` and `slot_filter()`
//! (LC-24). Error texts are Redis 7.0.15's (§8.1).
//!
//! # Contracts with the dispatcher
//!
//! * **Before routing** a command, the dispatcher calls [`precheck`]: it answers the argument
//!   errors Redis reports before touching any key (MSET's even argument count, COPY's options),
//!   so that a command split across partitions (§6.8) never starts with bad arguments.
//! * **Keyed handlers** are written once over [`store::Store`] and registered for
//!   `Keyspace`. They validate everything before their first mutating call and write the whole
//!   reply themselves. When one returns `Err`, its staged effects are dropped and the bytes it
//!   wrote are discarded by the caller (truncate the reply buffer to its length before the
//!   call); `NeedMore` re-runs it from the start, any other error is answered with
//!   [`write_store_error`].
//! * **Fan-out** (§6.8): a sub-request runs the same keyed handler on the argv
//!   `[name, keys of that partition...]` ([`FanOutSplit::sub_argv`]); [`gather`] merges the
//!   per-partition replies into the client's reply.
//! * **Connection handlers** return [`ConnOutcome`]. `Async` means the handler validated its
//!   arguments, wrote nothing, and needs node-level work: [`node_op`] says which (FLUSH*, SAVE
//!   barriers, SHUTDOWN, CLIENT LIST/KILL across workers), and `server::write_node_op_reply`
//!   writes the reply once it is done.

pub mod cluster_cmds;
pub mod config_cmd;
pub mod conn_cmds;
pub mod expire;
pub mod hash;
pub mod info;
pub mod keys;
pub mod scan;
pub mod server;
pub mod store;
pub mod strings;
pub mod txn;

#[cfg(test)]
mod oracle;
#[cfg(test)]
mod pagestore_tests;
#[cfg(test)]
mod props;
#[cfg(test)]
pub(crate) mod testkit;

use std::sync::atomic::{AtomicU64, Ordering};

use kv_cmd::CmdId;
use kv_resp::ReplyWriter;
use kv_storage::keyspace::{Keyspace, StoreError};
use smallvec::SmallVec;

use crate::config::Config;
use crate::net::conn::Conn;
use crate::runtime::xmsg::ExecEnv;
use crate::shared::NodeShared;

pub use server::NodeOp;
pub use store::Store;

pub type KeyedFn = fn(
    &mut kv_storage::keyspace::Keyspace<'_>,
    &Args<'_>,
    &mut kv_resp::ReplyWriter<'_>,
    &ExecEnv,
) -> Result<(), kv_storage::keyspace::StoreError>;

/// A keyed handler over any [`Store`] (the generic form every `KeyedFn` is built from).
pub type GenericKeyedFn<S> =
    fn(&mut S, &Args<'_>, &mut ReplyWriter<'_>, &ExecEnv) -> Result<(), StoreError>;

/// keyless/connection commands on the home worker
pub type ConnFn = fn(&mut ConnCtx<'_>, &Args<'_>, &mut kv_resp::ReplyWriter<'_>) -> ConnOutcome;

/// Arguments of one command (index 0 = command name).
pub struct Args<'a> {
    pub argv: smallvec::SmallVec<[&'a [u8]; 8]>,
}

impl<'a> Args<'a> {
    pub fn new(argv: &[&'a [u8]]) -> Self {
        Args {
            argv: argv.iter().copied().collect(),
        }
    }

    /// Number of arguments including the command name.
    #[inline]
    pub fn len(&self) -> usize {
        self.argv.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.argv.is_empty()
    }

    /// Argument `i` (0 = command name).
    #[inline]
    pub fn arg(&self, i: usize) -> &'a [u8] {
        self.argv[i]
    }
}

/// What a connection-level handler wants next.
pub enum ConnOutcome {
    Done,
    /// Close after the reply is flushed (QUIT).
    Close,
    FanOut(FanOutSplit),
    /// The reply arrives later (gathers, barriers, SHUTDOWN).
    Async,
}

/// fixed per-command split: one sub-request per distinct partition of the keys, key bytes only
/// (LC-37)
pub struct FanOutSplit {
    /// `(partition, argument indexes of its keys)`, one entry per distinct partition.
    pub parts: smallvec::SmallVec<[(u16, smallvec::SmallVec<[u16; 4]>); 4]>,
}

impl FanOutSplit {
    /// The argv of sub-request `part`: the command name, then that partition's key arguments
    /// in their original order.
    pub fn sub_argv<'a>(&self, part: usize, argv: &[&'a [u8]]) -> SmallVec<[&'a [u8]; 8]> {
        let mut out = SmallVec::with_capacity(self.parts[part].1.len() + 1);
        out.push(argv[0]);
        out.extend(self.parts[part].1.iter().map(|&i| argv[usize::from(i)]));
        out
    }
}

/// Context of a connection-level handler.
pub struct ConnCtx<'a> {
    pub conn: &'a mut Conn,
    pub shared: &'a NodeShared,
    pub worker: u16,
}

#[derive(Clone, Copy)]
pub enum Handler {
    Keyed(KeyedFn),
    Conn(ConnFn),
    Scan,
    Txn,
    Cluster(ConnFn),
}

/// Declares the keyed handlers once: the `Keyspace` registry ([`handler`]) and the generic one
/// ([`keyed`]) are generated from the same list.
macro_rules! keyed_handlers {
    ($($id:ident => $m:ident :: $f:ident),* $(,)?) => {
        /// The keyed handler of `id` over any [`Store`] (tests, simulator), or `None` when `id`
        /// is not a keyed command.
        pub fn keyed<S: Store>(id: CmdId) -> Option<GenericKeyedFn<S>> {
            match id {
                $(CmdId::$id => Some($m::$f::<S>),)*
                _ => None,
            }
        }

        fn keyed_keyspace(id: CmdId) -> Option<KeyedFn> {
            match id {
                $(CmdId::$id => {
                    let f: KeyedFn = |ks: &mut Keyspace<'_>, a, w, e| $m::$f(ks, a, w, e);
                    Some(f)
                })*
                _ => None,
            }
        }
    };
}

keyed_handlers! {
    Touch => server::touch,
    // strings (§8.3)
    Get => strings::get,
    Set => strings::set,
    Setnx => strings::setnx,
    Setex => strings::setex,
    Psetex => strings::psetex,
    Getset => strings::getset,
    Getdel => strings::getdel,
    Getex => strings::getex,
    Mget => strings::mget,
    Mset => strings::mset,
    Msetnx => strings::msetnx,
    Append => strings::append,
    Strlen => strings::strlen,
    Getrange => strings::getrange,
    Substr => strings::getrange,
    Setrange => strings::setrange,
    Incr => strings::incr,
    Decr => strings::decr,
    Incrby => strings::incrby,
    Decrby => strings::decrby,
    Incrbyfloat => strings::incrbyfloat,
    // keys and expiry (§8.4)
    Del => keys::del,
    Unlink => keys::del,
    Exists => keys::exists,
    Type => keys::type_,
    Rename => keys::rename,
    Renamenx => keys::renamenx,
    Copy => keys::copy,
    Expire => expire::expire,
    Pexpire => expire::pexpire,
    Expireat => expire::expireat,
    Pexpireat => expire::pexpireat,
    Ttl => expire::ttl,
    Pttl => expire::pttl,
    Expiretime => expire::expiretime,
    Pexpiretime => expire::pexpiretime,
    Persist => expire::persist,
    // hashes (§8.6)
    Hset => hash::hset,
    Hsetnx => hash::hsetnx,
    Hget => hash::hget,
    Hmget => hash::hmget,
    Hmset => hash::hmset,
    Hdel => hash::hdel,
    Hgetall => hash::hgetall,
    Hkeys => hash::hkeys,
    Hvals => hash::hvals,
    Hlen => hash::hlen,
    Hexists => hash::hexists,
    Hincrby => hash::hincrby,
    Hincrbyfloat => hash::hincrbyfloat,
    Hstrlen => hash::hstrlen,
}

/// The handler registered for a command id.
pub fn handler(id: kv_cmd::CmdId) -> Handler {
    if let Some(f) = keyed_keyspace(id) {
        return Handler::Keyed(f);
    }
    use CmdId::*;
    match id {
        // connection (§7.4, §7.6, §8.2)
        Ping => Handler::Conn(conn_cmds::ping),
        Echo => Handler::Conn(conn_cmds::echo),
        Hello => Handler::Conn(conn_cmds::hello),
        Auth => Handler::Conn(conn_cmds::auth),
        Select => Handler::Conn(conn_cmds::select),
        Quit => Handler::Conn(conn_cmds::quit),
        Reset => Handler::Conn(conn_cmds::reset),
        Client => Handler::Conn(conn_cmds::client_container),
        ClientId => Handler::Conn(conn_cmds::client_id),
        ClientGetname => Handler::Conn(conn_cmds::client_getname),
        ClientSetname => Handler::Conn(conn_cmds::client_setname),
        ClientInfo => Handler::Conn(conn_cmds::client_info),
        ClientList => Handler::Conn(conn_cmds::client_list),
        ClientSetinfo => Handler::Conn(conn_cmds::client_setinfo),
        ClientNoEvict => Handler::Conn(conn_cmds::client_no_evict),
        ClientNoTouch => Handler::Conn(conn_cmds::client_no_touch),
        ClientKill => Handler::Conn(conn_cmds::client_kill),
        ClientReply => Handler::Conn(conn_cmds::client_reply),
        // server (§8.2)
        Command => Handler::Conn(server::command),
        CommandCount => Handler::Conn(server::command_count),
        CommandInfo => Handler::Conn(server::command_info),
        CommandDocs => Handler::Conn(server::command_docs),
        CommandList => Handler::Conn(server::command_list),
        CommandGetkeys => Handler::Conn(server::command_getkeys),
        Info => Handler::Conn(info::info),
        Config => Handler::Conn(config_cmd::config_container),
        ConfigGet => Handler::Conn(config_cmd::config_get),
        ConfigSet => Handler::Conn(config_cmd::config_set),
        ConfigResetstat => Handler::Conn(config_cmd::config_resetstat),
        ConfigRewrite => Handler::Conn(config_cmd::config_rewrite),
        Dbsize => Handler::Conn(server::dbsize),
        Flushdb => Handler::Conn(server::flushdb),
        Flushall => Handler::Conn(server::flushall),
        Time => Handler::Conn(server::time),
        Save => Handler::Conn(server::save),
        Bgsave => Handler::Conn(server::bgsave),
        Lastsave => Handler::Conn(server::lastsave),
        Shutdown => Handler::Conn(server::shutdown),
        Wait => Handler::Conn(server::wait),
        Waitaof => Handler::Conn(server::waitaof),
        Replicaof | Slaveof => Handler::Conn(server::replicaof),
        // iteration (§16.6)
        Randomkey | Keys | Scan => Handler::Scan,
        // transactions (§8.5)
        Multi | Exec | Discard | Watch | Unwatch => Handler::Txn,
        // cluster (§8.7)
        Cluster => Handler::Cluster(cluster_cmds::cluster_container),
        ClusterKeyslot => Handler::Cluster(cluster_cmds::cluster_keyslot),
        ClusterMyid => Handler::Cluster(cluster_cmds::cluster_myid),
        ClusterInfo => Handler::Cluster(cluster_cmds::cluster_info),
        ClusterSlots
        | ClusterShards
        | ClusterNodes
        | ClusterReplicas
        | ClusterSlaves
        | ClusterLinks
        | ClusterMyshardid
        | ClusterCountkeysinslot
        | ClusterGetkeysinslot => Handler::Cluster(cluster_cmds::cluster_topology),
        ClusterMeet
        | ClusterAddslots
        | ClusterAddslotsrange
        | ClusterDelslots
        | ClusterDelslotsrange
        | ClusterSetslot
        | ClusterFailover
        | ClusterForget
        | ClusterReplicate
        | ClusterReset
        | ClusterFlushslots
        | ClusterBumpepoch
        | ClusterSaveconfig
        | ClusterSetConfigEpoch
        | ClusterCountFailureReports => Handler::Cluster(cluster_cmds::cluster_managed),
        Readonly => Handler::Cluster(cluster_cmds::readonly),
        Readwrite => Handler::Cluster(cluster_cmds::readwrite),
        Asking => Handler::Cluster(cluster_cmds::asking),
        // keyed commands were answered above
        Touch | Get | Set | Setnx | Setex | Psetex | Getset | Getdel | Getex | Mget | Mset
        | Msetnx | Append | Strlen | Getrange | Substr | Setrange | Incr | Decr | Incrby
        | Decrby | Incrbyfloat | Del | Unlink | Exists | Type | Rename | Renamenx | Copy
        | Expire | Pexpire | Expireat | Pexpireat | Ttl | Pttl | Expiretime | Pexpiretime
        | Persist | Hset | Hsetnx | Hget | Hmget | Hmset | Hdel | Hgetall | Hkeys | Hvals
        | Hlen | Hexists | Hincrby | Hincrbyfloat | Hstrlen => {
            unreachable!("keyed handler {id:?} missing from keyed_handlers!")
        }
    }
}

/// The node-level operation a connection handler asked for by returning
/// [`ConnOutcome::Async`] (`None` for every other command).
pub fn node_op(id: CmdId, conn: &Conn, argv: &[&[u8]]) -> Option<NodeOp> {
    server::node_op(id, conn, argv)
}

/// Argument checks that Redis performs before looking at any key, run by the dispatcher before
/// a command is routed or split across partitions (§6.8). Returns `false` after writing the
/// error reply. Commands whose arguments can only be judged against data return `true`.
pub fn precheck(id: CmdId, argv: &[&[u8]], cfg: &Config, w: &mut ReplyWriter<'_>) -> bool {
    match id {
        CmdId::Mset | CmdId::Msetnx => strings::precheck_mset(id, argv, w),
        CmdId::Copy => keys::precheck_copy(argv, cfg.databases, cfg.cluster_enabled, w),
        _ => true,
    }
}

/// The reply to a storage error (§11.6, §15, §18.1, §22). Corruption replies name nothing but
/// the condition; the page id goes to diagnostics only (LC-32).
pub fn write_store_error(e: StoreError, w: &mut ReplyWriter<'_>) {
    w.error(match e {
        StoreError::WrongType => err::WRONGTYPE,
        StoreError::OutOfSpace => {
            b"OOM storage full: command not allowed when device fill > 'max-storage-fill'"
        }
        StoreError::TooLarge => err::STRING_TOO_LONG,
        StoreError::Corrupt(_) => b"ERR storage corruption detected",
        StoreError::Io(_) | StoreError::ReadOnly => b"ERR storage I/O error",
        StoreError::Loading => b"LOADING Redis is loading the dataset in memory",
        // never answered to a client: the partition re-runs the command (§6.5)
        StoreError::NeedMore => b"ERR storage page not loaded",
    });
}

/// Merges the per-partition replies of a command split by [`FanOutSplit`] (§6.8): the sum of
/// the integer replies (DEL, UNLINK, EXISTS, TOUCH) or the MGET elements back in argument
/// order. `parts[i]` is the reply of `split.parts[i]`; the first error reply, if any, is the
/// command's reply. `argc` is the original argument count.
pub fn gather(
    id: CmdId,
    split: &FanOutSplit,
    argc: usize,
    parts: &[&[u8]],
    w: &mut ReplyWriter<'_>,
) {
    if let Some(e) = parts.iter().find(|p| p.first() == Some(&b'-')) {
        w.buf.extend_from_slice(e);
        return;
    }
    if id == CmdId::Mget {
        // every part is `*n` followed by n bulk or null elements in its argument order
        let mut slots: SmallVec<[&[u8]; 16]> = SmallVec::from_elem(&b""[..], argc);
        for (part, reply) in split.parts.iter().zip(parts) {
            let Some(elems) = resp_elements(reply) else {
                w.error(b"ERR internal: malformed partial reply");
                return;
            };
            for (&i, e) in part.1.iter().zip(elems) {
                slots[usize::from(i)] = e;
            }
        }
        w.array(argc - 1);
        for e in &slots[1..] {
            w.buf.extend_from_slice(e);
        }
        return;
    }
    let mut sum = 0i64;
    for p in parts {
        match p
            .strip_prefix(b":")
            .and_then(|r| r.strip_suffix(b"\r\n"))
            .and_then(kv_common::num::string2ll)
        {
            Some(n) => sum += n,
            None => {
                w.error(b"ERR internal: malformed partial reply");
                return;
            }
        }
    }
    w.int(sum);
}

/// Splits an array reply of bulk or null elements into the element encodings.
fn resp_elements(reply: &[u8]) -> Option<SmallVec<[&[u8]; 8]>> {
    let line_end = |from: usize| memchr_crlf(&reply[from..]).map(|n| from + n);
    let rest = reply.strip_prefix(b"*")?;
    let e = line_end(1)?;
    let n = usize::try_from(kv_common::num::string2ll(&rest[..e - 1])?).ok()?;
    let mut pos = e + 2;
    let mut out = SmallVec::with_capacity(n);
    for _ in 0..n {
        let start = pos;
        match reply.get(pos)? {
            b'_' => pos += 3,
            b'$' => {
                let le = line_end(pos)?;
                let len = kv_common::num::string2ll(&reply[pos + 1..le])?;
                pos = le + 2;
                if len >= 0 {
                    pos += usize::try_from(len).ok()? + 2;
                }
            }
            _ => return None,
        }
        out.push(reply.get(start..pos)?);
    }
    (pos == reply.len()).then_some(out)
}

fn memchr_crlf(s: &[u8]) -> Option<usize> {
    s.windows(2).position(|w| w == b"\r\n")
}

/// Redis 7.0.15 error texts shared by several handlers (§8.1).
pub(crate) mod err {
    pub const SYNTAX: &[u8] = b"ERR syntax error";
    pub const NOT_INTEGER: &[u8] = b"ERR value is not an integer or out of range";
    /// `getIntFromObjectOrReply` outside the 32-bit range.
    pub const INT_RANGE: &[u8] =
        b"ERR value is out of range, value must between -2147483648 and 2147483647";
    pub const WRONGTYPE: &[u8] =
        b"WRONGTYPE Operation against a key holding the wrong kind of value";
    pub const NOT_FLOAT: &[u8] = b"ERR value is not a valid float";
    pub const OVERFLOW: &[u8] = b"ERR increment or decrement would overflow";
    pub const NAN_OR_INF: &[u8] = b"ERR increment would produce NaN or Infinity";
    pub const STRING_TOO_LONG: &[u8] =
        b"ERR string exceeds maximum allowed size (proto-max-bulk-len)";
    pub const NO_SUCH_KEY: &[u8] = b"ERR no such key";
    pub const DB_RANGE: &[u8] = b"ERR DB index is out of range";
    pub const INVALID_CURSOR: &[u8] = b"ERR invalid cursor";
    pub const CLUSTER_DISABLED: &[u8] = b"ERR This instance has cluster support disabled";
}

/// The longest string value (§8.3: 512 MiB), the default and the ceiling of
/// `proto-max-bulk-len`.
pub(crate) const MAX_STRING_LEN: u64 = 512 * 1024 * 1024;

/// `proto-max-bulk-len` as the keyed handlers apply it (Redis `checkStringLength`).
static STRING_LIMIT: AtomicU64 = AtomicU64::new(MAX_STRING_LEN);
/// `hash-max-bytes` as the hash handlers apply it.
static HASH_LIMIT: AtomicU64 = AtomicU64::new(hash::HASH_MAX_BYTES as u64);

/// Publishes the value limits of `cfg` (`proto-max-bulk-len`, `hash-max-bytes`; both settable at
/// run time, §20.2) to the keyed handlers. Those run inside partitions without access to the
/// configuration, so the node calls this once the configuration is loaded and CONFIG SET calls
/// it after every change. The string limit never exceeds 512 MiB (§8.3).
pub fn apply_value_limits(cfg: &Config) {
    STRING_LIMIT.store(
        cfg.proto_max_bulk_len.min(MAX_STRING_LEN),
        Ordering::Relaxed,
    );
    HASH_LIMIT.store(cfg.hash_max_bytes, Ordering::Relaxed);
}

/// The longest string a write may produce now.
#[inline]
pub(crate) fn string_limit() -> u64 {
    STRING_LIMIT.load(Ordering::Relaxed)
}

/// The largest encoded hash a write may produce now.
#[inline]
pub(crate) fn hash_limit() -> u64 {
    HASH_LIMIT.load(Ordering::Relaxed)
}

/// The bytes before the first NUL: Redis compares option names as C strings.
#[inline]
pub(crate) fn c_str(s: &[u8]) -> &[u8] {
    match s.iter().position(|&b| b == 0) {
        Some(n) => &s[..n],
        None => s,
    }
}

/// `strcasecmp(arg, name) == 0` (C-string semantics; `name` is lowercase ASCII).
#[inline]
pub(crate) fn is_opt(arg: &[u8], name: &[u8]) -> bool {
    c_str(arg).eq_ignore_ascii_case(name)
}

/// Redis `getLongLongFromObject` on an argument (`string2ll`).
#[inline]
pub(crate) fn parse_i64(arg: &[u8]) -> Option<i64> {
    kv_common::num::string2ll(arg)
}

/// `-ERR wrong number of arguments for '<name>' command` for the full command name.
pub(crate) fn write_arity_error(name: &str, w: &mut ReplyWriter<'_>) {
    let mut m = Vec::with_capacity(48 + name.len());
    m.extend_from_slice(b"ERR wrong number of arguments for '");
    m.extend_from_slice(name.as_bytes());
    m.extend_from_slice(b"' command");
    w.error(&m);
}

/// The decimal rendering of an integer, on the stack.
pub(crate) struct Decimal {
    buf: [u8; 20],
    len: usize,
}

impl Decimal {
    pub(crate) fn i64(v: i64) -> Self {
        let mut buf = [0u8; 20];
        let len = kv_common::num::ll2string(v, &mut buf);
        Decimal { buf, len }
    }

    pub(crate) fn u64(mut v: u64) -> Self {
        let mut tmp = [0u8; 20];
        let mut n = 0;
        loop {
            tmp[19 - n] = b'0' + (v % 10) as u8;
            n += 1;
            v /= 10;
            if v == 0 {
                break;
            }
        }
        let mut buf = [0u8; 20];
        buf[..n].copy_from_slice(&tmp[20 - n..]);
        Decimal { buf, len: n }
    }

    #[inline]
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

/// Writes a value as one bulk reply (chunked values are copied chunk by chunk).
#[inline]
pub(crate) fn write_value(w: &mut ReplyWriter<'_>, v: &kv_storage::keyspace::Value<'_>) {
    match v {
        kv_storage::keyspace::Value::Contig(b) => w.bulk(b),
        kv_storage::keyspace::Value::Chunks(cs) => {
            w.bulk_len(cs.iter().map(|c| c.len()).sum());
            for c in cs {
                w.buf.extend_from_slice(c);
            }
            w.crlf();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kv_resp::Proto;

    #[test]
    fn every_command_has_a_handler() {
        for spec in kv_cmd::all_with_subcommands() {
            // must not panic
            let h = handler(spec.id);
            if spec.has_keys() && !matches!(spec.id, CmdId::Watch | CmdId::ClusterKeyslot) {
                assert!(
                    matches!(h, Handler::Keyed(_))
                        || matches!(spec.id, CmdId::ClusterCountkeysinslot),
                    "{} has keys but no keyed handler",
                    spec.name
                );
            }
        }
    }

    #[test]
    fn store_errors() {
        let mut out = Vec::new();
        write_store_error(
            StoreError::OutOfSpace,
            &mut ReplyWriter::new(&mut out, Proto::Resp2),
        );
        assert_eq!(
            out,
            b"-OOM storage full: command not allowed when device fill > 'max-storage-fill'\r\n"
        );
        out.clear();
        write_store_error(
            StoreError::Loading,
            &mut ReplyWriter::new(&mut out, Proto::Resp3),
        );
        assert_eq!(out, b"-LOADING Redis is loading the dataset in memory\r\n");
    }

    fn split(parts: &[(u16, &[u16])]) -> FanOutSplit {
        FanOutSplit {
            parts: parts
                .iter()
                .map(|(p, ix)| (*p, ix.iter().copied().collect()))
                .collect(),
        }
    }

    #[test]
    fn gather_counts_and_mget() {
        let s = split(&[(3, &[1, 3]), (7, &[2])]);
        let argv: [&[u8]; 4] = [b"MGET", b"a", b"b", b"c"];
        assert_eq!(s.sub_argv(0, &argv).as_slice(), &[&b"MGET"[..], b"a", b"c"]);
        assert_eq!(s.sub_argv(1, &argv).as_slice(), &[&b"MGET"[..], b"b"]);

        let mut out = Vec::new();
        gather(
            CmdId::Del,
            &s,
            4,
            &[b":2\r\n", b":1\r\n"],
            &mut ReplyWriter::new(&mut out, Proto::Resp2),
        );
        assert_eq!(out, b":3\r\n");

        out.clear();
        let p0: &[u8] = b"*2\r\n$2\r\nva\r\n$-1\r\n";
        let p1: &[u8] = b"*1\r\n$0\r\n\r\n";
        gather(
            CmdId::Mget,
            &s,
            4,
            &[p0, p1],
            &mut ReplyWriter::new(&mut out, Proto::Resp2),
        );
        assert_eq!(out, b"*3\r\n$2\r\nva\r\n$0\r\n\r\n$-1\r\n");

        out.clear();
        let p0: &[u8] = b"*2\r\n_\r\n$1\r\nx\r\n";
        let p1: &[u8] = b"*1\r\n_\r\n";
        gather(
            CmdId::Mget,
            &s,
            4,
            &[p0, p1],
            &mut ReplyWriter::new(&mut out, Proto::Resp3),
        );
        assert_eq!(out, b"*3\r\n_\r\n_\r\n$1\r\nx\r\n");

        out.clear();
        gather(
            CmdId::Exists,
            &s,
            4,
            &[b":1\r\n", b"-LOADING x\r\n"],
            &mut ReplyWriter::new(&mut out, Proto::Resp2),
        );
        assert_eq!(out, b"-LOADING x\r\n");
    }

    #[test]
    fn value_limits_follow_config() {
        // the defaults (other tests run concurrently and expect them): a configured string
        // limit above 512 MiB is clamped to it
        let cfg = Config {
            proto_max_bulk_len: 4 * MAX_STRING_LEN,
            ..Config::default()
        };
        apply_value_limits(&cfg);
        assert_eq!(string_limit(), MAX_STRING_LEN);
        assert_eq!(hash_limit(), hash::HASH_MAX_BYTES as u64);
        assert_eq!(cfg.hash_max_bytes, hash::HASH_MAX_BYTES as u64);
    }

    #[test]
    fn c_string_options() {
        assert!(is_opt(b"Nx", b"nx"));
        assert!(is_opt(b"nx\0zz", b"nx"));
        assert!(!is_opt(b"nxx", b"nx"));
        assert!(!is_opt(b"\0nx", b"nx"));
    }
}
