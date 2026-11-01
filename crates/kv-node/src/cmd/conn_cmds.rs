//! Connection commands (DESIGN §7.4, §7.6, §8.2): PING, ECHO, HELLO (protocol switch, reply
//! in the new protocol with `server: kvd`, `version: 7.2.0`), AUTH (user `default` only),
//! SELECT (standalone `0..databases-1`; cluster: 0 only), QUIT, RESET and the CLIENT
//! subcommands (ID, GETNAME, SETNAME, INFO, LIST, SETINFO, NO-EVICT, NO-TOUCH, KILL, REPLY ON) in
//! Redis 7.0 formats.
//!
//! The only user is `default`; its password is `requirepass` (none: every password is
//! accepted for `default`, as for Redis's `nopass` default user). Passwords are compared in
//! time independent of where they differ. CLIENT LIST and CLIENT KILL span every worker's
//! connections, so they are node operations (`server::NodeOp`); the per-connection rendering
//! ([`write_client_line`]) and the filters live here.

use kv_resp::{Proto, ReplyWriter};

use super::{Args, ConnCtx, ConnOutcome, err, is_opt, parse_i64};
use crate::config::Config;
use crate::net::conn::{Conn, flags};

/// `server` field of the HELLO reply (§7.4).
pub const HELLO_SERVER: &[u8] = b"kvd";
/// `version` field of the HELLO reply: the compatibility version clients parse (§7.4).
pub const HELLO_VERSION: &[u8] = b"7.2.0";

const WRONGPASS: &[u8] = b"WRONGPASS invalid username-password pair or user is disabled.";

/// PING [msg]: `+PONG` or the message as a bulk.
pub fn ping(_cx: &mut ConnCtx<'_>, a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    match a.len() {
        1 => w.simple(b"PONG"),
        2 => w.bulk(a.arg(1)),
        _ => super::write_arity_error("ping", w),
    }
    ConnOutcome::Done
}

/// ECHO msg.
pub fn echo(_cx: &mut ConnCtx<'_>, a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    w.bulk(a.arg(1));
    ConnOutcome::Done
}

/// `a == b` in time that depends only on the lengths (Redis `time_independent_strcmp`).
fn same_secret(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Whether `user`/`pass` authenticate as the default user.
fn credentials_ok(cfg: &Config, user: &[u8], pass: &[u8]) -> bool {
    user == b"default"
        && (cfg.requirepass.is_empty() || same_secret(pass, cfg.requirepass.as_bytes()))
}

/// Whether the connection may run commands (authenticated, or no password configured).
fn authenticated(cx: &ConnCtx<'_>, cfg: &Config) -> bool {
    cx.conn.authed || cfg.requirepass.is_empty()
}

/// AUTH [user] pass (Redis 7.0 `authCommand`).
pub fn auth(cx: &mut ConnCtx<'_>, a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    let cfg = cx.shared.config();
    if a.len() > 3 {
        w.error(err::SYNTAX);
        return ConnOutcome::Done;
    }
    let (user, pass) = if a.len() == 2 {
        if cfg.requirepass.is_empty() {
            w.error(
                b"ERR AUTH <password> called without any password configured for the default user. \
                  Are you sure your configuration is correct?",
            );
            return ConnOutcome::Done;
        }
        (&b"default"[..], a.arg(1))
    } else {
        (a.arg(1), a.arg(2))
    };
    if credentials_ok(&cfg, user, pass) {
        cx.conn.authed = true;
        w.ok();
    } else {
        w.error(WRONGPASS);
    }
    ConnOutcome::Done
}

/// HELLO [protover [AUTH user pass] [SETNAME name]] (§7.4, Redis 7.0 `helloCommand`): the
/// arguments are checked first, then the credentials, then the connection must be
/// authenticated; the name is set and the protocol switched, and the reply is written in the
/// new protocol.
pub fn hello(cx: &mut ConnCtx<'_>, a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    let h = match kv_resp::parse_hello(&a.argv) {
        Ok(h) => h,
        Err(e) => {
            e.write(&a.argv, w);
            return ConnOutcome::Done;
        }
    };
    let cfg = cx.shared.config();
    if let Some((user, pass)) = h.auth {
        if !credentials_ok(&cfg, user, pass) {
            w.error(WRONGPASS);
            return ConnOutcome::Done;
        }
        cx.conn.authed = true;
    }
    if !authenticated(cx, &cfg) {
        w.error(
            b"NOAUTH HELLO must be called with the client already authenticated, otherwise the \
              HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and \
              select the RESP protocol version at the same time",
        );
        return ConnOutcome::Done;
    }
    if let Some(name) = h.setname {
        set_name(cx.conn, name);
    }
    if let Some(p) = h.proto {
        cx.conn.proto = p;
        w.proto = p;
    }
    let mode: &[u8] = if cfg.cluster_enabled {
        b"cluster"
    } else {
        b"standalone"
    };
    w.hello(
        HELLO_SERVER,
        HELLO_VERSION,
        cx.conn.id as i64,
        mode,
        b"master",
    );
    ConnOutcome::Done
}

/// Redis `getIntFromObjectOrReply`.
fn parse_c_int(arg: &[u8], w: &mut ReplyWriter<'_>) -> Option<i32> {
    match parse_i64(arg) {
        None => w.error(err::NOT_INTEGER),
        Some(v) => match i32::try_from(v) {
            Ok(v) => return Some(v),
            Err(_) => w.error(err::INT_RANGE),
        },
    }
    None
}

/// SELECT n.
pub fn select(cx: &mut ConnCtx<'_>, a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    let Some(id) = parse_c_int(a.arg(1), w) else {
        return ConnOutcome::Done;
    };
    let cfg = cx.shared.config();
    if cfg.cluster_enabled && id != 0 {
        w.error(b"ERR SELECT is not allowed in cluster mode");
    } else if id < 0 || id >= i32::from(cfg.databases) {
        w.error(err::DB_RANGE);
    } else {
        cx.conn.db = id as u8;
        w.ok();
    }
    ConnOutcome::Done
}

/// QUIT: `+OK`, then the connection closes once the reply is flushed.
pub fn quit(_cx: &mut ConnCtx<'_>, _a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    w.ok();
    ConnOutcome::Close
}

/// RESET (Redis `clearClientConnectionState`): database 0, RESP2, no name, no MULTI, no WATCH,
/// READONLY / NO-EVICT / NO-TOUCH cleared, de-authenticated when a password is configured.
pub fn reset(cx: &mut ConnCtx<'_>, _a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    let cfg = cx.shared.config();
    let c = &mut *cx.conn;
    c.db = 0;
    c.proto = Proto::Resp2;
    c.name = None;
    c.multi = None;
    c.watch.clear();
    c.readonly = false;
    c.flags &= !(flags::NO_EVICT | flags::NO_TOUCH | flags::WATCH_DIRTY);
    c.authed = cfg.requirepass.is_empty();
    w.proto = Proto::Resp2;
    w.simple(b"RESET");
    ConnOutcome::Done
}

// ---------------------------------------------------------------------------------------------
// CLIENT

/// `CLIENT` without a subcommand (normally answered by command resolution).
pub fn client_container(
    _cx: &mut ConnCtx<'_>,
    _a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
) -> ConnOutcome {
    super::write_arity_error("client", w);
    ConnOutcome::Done
}

pub fn client_id(cx: &mut ConnCtx<'_>, _a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    w.int(cx.conn.id as i64);
    ConnOutcome::Done
}

pub fn client_getname(cx: &mut ConnCtx<'_>, _a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    match &cx.conn.name {
        Some(n) => w.bulk(n),
        None => w.null(),
    }
    ConnOutcome::Done
}

/// Sets (non-empty) or clears (empty) the connection name.
fn set_name(c: &mut Conn, name: &[u8]) {
    c.name = (!name.is_empty()).then(|| name.into());
}

/// CLIENT SETNAME name: printable ASCII without spaces; empty clears the name.
pub fn client_setname(cx: &mut ConnCtx<'_>, a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    let name = a.arg(2);
    if !kv_resp::valid_client_name(name) {
        w.error(kv_resp::CLIENT_NAME_ERROR);
    } else {
        set_name(cx.conn, name);
        w.ok();
    }
    ConnOutcome::Done
}

/// Renders one CLIENT LIST / CLIENT INFO line (with its trailing `\n`) for `c`. `current` is
/// the command being executed on that connection (the caller's own CLIENT LIST / INFO);
/// otherwise the connection's last command is shown (`NULL` before the first one).
pub fn write_client_line(
    c: &Conn,
    current: Option<kv_cmd::CmdId>,
    now_boot_ns: u64,
    out: &mut Vec<u8>,
) {
    use std::io::Write as _;
    let addr = c.addr.map(|a| a.to_string()).unwrap_or_default();
    let laddr = c.laddr.map(|a| a.to_string()).unwrap_or_default();
    let cmd = current.or(c.last_cmd_id).map(|id| id.spec().name);
    let age = now_boot_ns.saturating_sub(c.created) / 1_000_000_000;
    let idle = now_boot_ns.saturating_sub(c.last_cmd) / 1_000_000_000;
    let mut fl = String::new();
    if c.multi.is_some() {
        fl.push('x');
    }
    if c.flags & flags::WATCH_DIRTY != 0 {
        fl.push('d');
    }
    if c.readonly {
        fl.push('r');
    }
    if c.flags & flags::NO_EVICT != 0 {
        fl.push('e');
    }
    if c.flags & flags::NO_TOUCH != 0 {
        fl.push('T');
    }
    if fl.is_empty() {
        fl.push('N');
    }
    let multi = c.multi.as_ref().map_or(-1, |m| m.queued.len() as i64);
    let qbuf = c.query_buffer_len();
    let qfree = c.read_buf.len() - qbuf;
    let omem = c.out.capacity();
    let tot = c.read_buf.capacity() + omem;
    let resp = match c.proto {
        Proto::Resp2 => 2,
        Proto::Resp3 => 3,
    };
    let _ = write!(
        out,
        "id={} addr={addr} laddr={laddr} fd={} name=",
        c.id, c.fd
    );
    out.extend_from_slice(c.name.as_deref().unwrap_or(b""));
    let _ = write!(
        out,
        " age={age} idle={idle} flags={fl} db={} sub=0 psub=0 ssub=0 multi={multi} qbuf={qbuf} qbuf-free={qfree} \
         argv-mem=0 multi-mem=0 rbs={} rbp={qbuf} obl=0 oll=0 omem={omem} tot-mem={tot} events=r cmd={} \
         user=default redir=-1 resp={resp} lib-name=",
        c.db,
        c.read_buf.capacity(),
        cmd.unwrap_or("NULL"),
    );
    out.extend_from_slice(c.lib_name.as_deref().unwrap_or(b""));
    out.extend_from_slice(b" lib-ver=");
    out.extend_from_slice(c.lib_ver.as_deref().unwrap_or(b""));
    out.push(b'\n');
}

/// CLIENT INFO: this connection's line (verbatim `txt` in RESP3).
pub fn client_info(cx: &mut ConnCtx<'_>, _a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    let mut line = Vec::with_capacity(320);
    write_client_line(
        cx.conn,
        Some(kv_cmd::CmdId::ClientInfo),
        kv_common::clock::boot_ns(),
        &mut line,
    );
    w.verbatim(b"txt", &line);
    ConnOutcome::Done
}

/// Client types of the TYPE filters. Every connection of a data node is `normal`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ClientType {
    Normal,
    Replica,
    Master,
    Pubsub,
}

fn client_type(name: &[u8]) -> Option<ClientType> {
    let n = super::c_str(name);
    if n.eq_ignore_ascii_case(b"normal") {
        Some(ClientType::Normal)
    } else if n.eq_ignore_ascii_case(b"slave") || n.eq_ignore_ascii_case(b"replica") {
        Some(ClientType::Replica)
    } else if n.eq_ignore_ascii_case(b"master") {
        Some(ClientType::Master)
    } else if n.eq_ignore_ascii_case(b"pubsub") {
        Some(ClientType::Pubsub)
    } else {
        None
    }
}

fn write_unknown_type(name: &[u8], w: &mut ReplyWriter<'_>) {
    let mut m = b"ERR Unknown client type '".to_vec();
    m.extend_from_slice(super::c_str(name));
    m.push(b'\'');
    w.error(&m);
}

/// The selection of CLIENT LIST.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct ClientFilter {
    pub kind: Option<ClientType>,
    /// `ID id ...`: only these connections, in this order.
    pub ids: Option<Vec<u64>>,
}

impl ClientFilter {
    /// Whether connection `id` is listed.
    pub fn matches(&self, id: u64) -> bool {
        self.kind.is_none_or(|k| k == ClientType::Normal)
            && self.ids.as_ref().is_none_or(|ids| ids.contains(&id))
    }
}

/// Why CLIENT LIST / KILL arguments were refused (the reply text).
pub(crate) type ArgError = Vec<u8>;

/// Redis 7.0 CLIENT LIST arguments: nothing, `TYPE t`, or `ID id [id ...]`.
pub(crate) fn parse_client_list(argv: &[&[u8]]) -> Result<ClientFilter, ArgError> {
    let mut f = ClientFilter::default();
    if argv.len() == 4 && is_opt(argv[2], b"type") {
        match client_type(argv[3]) {
            Some(t) => f.kind = Some(t),
            None => {
                let mut out = Vec::new();
                write_unknown_type(argv[3], &mut ReplyWriter::new(&mut out, Proto::Resp2));
                return Err(out);
            }
        }
    } else if argv.len() > 3 && is_opt(argv[2], b"id") {
        let mut ids = Vec::with_capacity(argv.len() - 3);
        for &a in &argv[3..] {
            match parse_i64(a) {
                Some(id) => ids.push(id as u64),
                None => return Err(b"-ERR Invalid client ID\r\n".to_vec()),
            }
        }
        f.ids = Some(ids);
    } else if argv.len() != 2 {
        return Err(b"-ERR syntax error\r\n".to_vec());
    }
    Ok(f)
}

/// CLIENT LIST [TYPE t | ID id ...]: node operation `NodeOp::ClientList`.
pub fn client_list(_cx: &mut ConnCtx<'_>, a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    match parse_client_list(&a.argv) {
        Ok(_) => ConnOutcome::Async,
        Err(reply) => {
            w.buf.extend_from_slice(&reply);
            ConnOutcome::Done
        }
    }
}

/// The selection of CLIENT KILL (new form: every given filter must match).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct KillFilter {
    pub id: Option<u64>,
    pub kind: Option<ClientType>,
    /// `ip:port` of the peer / local end.
    pub addr: Option<String>,
    pub laddr: Option<String>,
    /// Skip the calling connection (default in the new form).
    pub skipme: bool,
}

impl KillFilter {
    /// Whether connection `id` (peer `addr`, local `laddr`) is killed by a call from `caller`.
    pub fn matches(&self, id: u64, addr: &str, laddr: &str, caller: u64) -> bool {
        !(self.skipme && id == caller)
            && self.id.is_none_or(|x| x == id)
            && self.kind.is_none_or(|k| k == ClientType::Normal)
            && self.addr.as_deref().is_none_or(|a| a == addr)
            && self.laddr.as_deref().is_none_or(|a| a == laddr)
    }

    /// Whether connection `c` is killed by a call from `caller`.
    pub fn matches_conn(&self, c: &Conn, caller: u64) -> bool {
        let addr = c.addr.map(|a| a.to_string()).unwrap_or_default();
        let laddr = c.laddr.map(|a| a.to_string()).unwrap_or_default();
        self.matches(c.id, &addr, &laddr, caller)
    }
}

/// Redis 7.0 CLIENT KILL arguments: `addr:port` (old form, may kill the caller) or option
/// pairs `ID n | TYPE t | ADDR a | LADDR a | USER u | SKIPME yes|no`. Returns the filter and
/// whether the old form was used.
pub(crate) fn parse_client_kill(argv: &[&[u8]]) -> Result<(KillFilter, bool), ArgError> {
    let mut f = KillFilter {
        id: None,
        kind: None,
        addr: None,
        laddr: None,
        skipme: true,
    };
    let reply = |text: &[u8]| {
        let mut out = Vec::with_capacity(text.len() + 3);
        out.push(b'-');
        out.extend_from_slice(text);
        out.extend_from_slice(b"\r\n");
        out
    };
    match argv.len() {
        3 => {
            f.addr = Some(String::from_utf8_lossy(super::c_str(argv[2])).into_owned());
            f.skipme = false;
            return Ok((f, true));
        }
        n if n < 3 => return Err(reply(err::SYNTAX)),
        _ => {}
    }
    let mut i = 2;
    while i < argv.len() {
        let (opt, val) = (argv[i], argv.get(i + 1).copied());
        let Some(val) = val else {
            return Err(reply(err::SYNTAX));
        };
        if is_opt(opt, b"id") {
            match parse_i64(val) {
                Some(id) if id >= 1 => f.id = Some(id as u64),
                _ => return Err(reply(b"ERR client-id should be greater than 0")),
            }
        } else if is_opt(opt, b"type") {
            match client_type(val) {
                Some(t) => f.kind = Some(t),
                None => {
                    let mut out = Vec::new();
                    write_unknown_type(val, &mut ReplyWriter::new(&mut out, Proto::Resp2));
                    return Err(out);
                }
            }
        } else if is_opt(opt, b"addr") {
            f.addr = Some(String::from_utf8_lossy(super::c_str(val)).into_owned());
        } else if is_opt(opt, b"laddr") {
            f.laddr = Some(String::from_utf8_lossy(super::c_str(val)).into_owned());
        } else if is_opt(opt, b"user") {
            if super::c_str(val) != b"default" {
                let mut m = b"ERR No such user '".to_vec();
                m.extend_from_slice(super::c_str(val));
                m.push(b'\'');
                return Err(reply(&m));
            }
        } else if is_opt(opt, b"skipme") {
            if is_opt(val, b"yes") {
                f.skipme = true;
            } else if is_opt(val, b"no") {
                f.skipme = false;
            } else {
                return Err(reply(err::SYNTAX));
            }
        } else {
            return Err(reply(err::SYNTAX));
        }
        i += 2;
    }
    Ok((f, false))
}

/// CLIENT KILL ...: node operation `NodeOp::ClientKill`.
pub fn client_kill(_cx: &mut ConnCtx<'_>, a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    match parse_client_kill(&a.argv) {
        Ok(_) => ConnOutcome::Async,
        Err(reply) => {
            w.buf.extend_from_slice(&reply);
            ConnOutcome::Done
        }
    }
}

/// CLIENT SETINFO LIB-NAME|LIB-VER value (Redis 7.2): printable ASCII without spaces; empty
/// clears the attribute.
pub fn client_setinfo(cx: &mut ConnCtx<'_>, a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    let (attr, val) = (a.arg(2), a.arg(3));
    let lib_name = is_opt(attr, b"lib-name");
    if !lib_name && !is_opt(attr, b"lib-ver") {
        let mut m = b"ERR Unrecognized option '".to_vec();
        m.extend_from_slice(super::c_str(attr));
        m.push(b'\'');
        w.error(&m);
        return ConnOutcome::Done;
    }
    if !kv_resp::valid_client_name(val) {
        let mut m = b"ERR ".to_vec();
        m.extend_from_slice(super::c_str(attr));
        m.extend_from_slice(b" cannot contain spaces, newlines or special characters.");
        w.error(&m);
        return ConnOutcome::Done;
    }
    let v = (!val.is_empty()).then(|| val.into());
    if lib_name {
        cx.conn.lib_name = v;
    } else {
        cx.conn.lib_ver = v;
    }
    w.ok();
    ConnOutcome::Done
}

/// `on` / `off` of CLIENT NO-EVICT / NO-TOUCH into connection flag `bit`.
fn client_flag(
    cx: &mut ConnCtx<'_>,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    bit: u32,
) -> ConnOutcome {
    if is_opt(a.arg(2), b"on") {
        cx.conn.flags |= bit;
        w.ok();
    } else if is_opt(a.arg(2), b"off") {
        cx.conn.flags &= !bit;
        w.ok();
    } else {
        w.error(err::SYNTAX);
    }
    ConnOutcome::Done
}

/// CLIENT NO-EVICT on|off (accepted; nothing is ever evicted).
pub fn client_no_evict(cx: &mut ConnCtx<'_>, a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    client_flag(cx, a, w, flags::NO_EVICT)
}

/// CLIENT NO-TOUCH on|off (Redis 7.2; accepted, no access times are kept).
pub fn client_no_touch(cx: &mut ConnCtx<'_>, a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    client_flag(cx, a, w, flags::NO_TOUCH)
}

/// CLIENT REPLY ON. OFF and SKIP (suppressing replies) are not supported.
pub fn client_reply(_cx: &mut ConnCtx<'_>, a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    if is_opt(a.arg(2), b"on") {
        w.ok();
    } else if is_opt(a.arg(2), b"off") || is_opt(a.arg(2), b"skip") {
        w.error(b"ERR CLIENT REPLY OFF and SKIP are not supported");
    } else {
        w.error(err::SYNTAX);
    }
    ConnOutcome::Done
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::cmd::{ConnFn, NodeOp};
    use crate::shared::NodeShared;
    use std::sync::Arc;

    /// A connection and a node for running connection handlers.
    pub(crate) struct Fixture {
        pub shared: Arc<NodeShared>,
        pub conn: Conn,
    }

    pub(crate) fn new_conn(id: u64) -> Conn {
        let mut c = Conn::new(
            id,
            7,
            kv_common::clock::boot_ns(),
            kv_resp::Limits::default(),
        );
        c.authed = false;
        c
    }

    pub(crate) fn words_owned(line: &str) -> Vec<Vec<u8>> {
        line.split_ascii_whitespace()
            .map(|s| s.as_bytes().to_vec())
            .collect()
    }

    impl Fixture {
        pub fn new() -> Self {
            Self::with_config(Config::default())
        }

        pub fn with_config(cfg: Config) -> Self {
            Fixture {
                shared: Arc::new(NodeShared::new(cfg, 4, 1).expect("node state")),
                conn: new_conn(12),
            }
        }

        /// Runs `f` on `line`; returns the reply as text.
        pub fn run_conn(&mut self, f: ConnFn, line: &str) -> String {
            self.run_argv(f, &words_owned(line)).0
        }

        pub fn run_argv(&mut self, f: ConnFn, argv: &[Vec<u8>]) -> (String, ConnOutcome) {
            let args = Args {
                argv: argv.iter().map(Vec::as_slice).collect(),
            };
            let mut out = Vec::new();
            let mut w = ReplyWriter::new(&mut out, self.conn.proto);
            let mut cx = ConnCtx {
                conn: &mut self.conn,
                shared: &self.shared,
                worker: 0,
            };
            let o = f(&mut cx, &args, &mut w);
            (String::from_utf8_lossy(&out).into_owned(), o)
        }

        /// Runs the registered handler of `line`; on `Async` returns its node operation.
        pub fn async_op(&mut self, line: &str) -> Option<NodeOp> {
            let argv = words_owned(line);
            let refs: Vec<&[u8]> = argv.iter().map(Vec::as_slice).collect();
            let spec = kv_cmd::resolve(&refs).ok()?;
            let crate::cmd::Handler::Conn(f) = crate::cmd::handler(spec.id) else {
                return None;
            };
            let (reply, o) = self.run_argv(f, &argv);
            match o {
                ConnOutcome::Async => {
                    assert!(reply.is_empty(), "an Async handler writes nothing: {reply}");
                    crate::cmd::node_op(spec.id, &self.conn, &refs)
                }
                _ => None,
            }
        }
    }

    #[test]
    fn ping_echo() {
        let mut f = Fixture::new();
        assert_eq!(f.run_conn(ping, "PING"), "+PONG\r\n");
        assert_eq!(f.run_conn(ping, "PING a"), "$1\r\na\r\n");
        assert_eq!(
            f.run_conn(ping, "PING a b"),
            "-ERR wrong number of arguments for 'ping' command\r\n"
        );
        assert_eq!(f.run_conn(echo, "ECHO hi"), "$2\r\nhi\r\n");
        let (r, _) = f.run_argv(echo, &[b"ECHO".to_vec(), b"\r\n".to_vec()]);
        assert_eq!(r, "$2\r\n\r\n\r\n");
    }

    #[test]
    fn auth_without_password() {
        let mut f = Fixture::new();
        assert_eq!(f.run_conn(auth, "AUTH a b c"), "-ERR syntax error\r\n");
        assert_eq!(
            f.run_conn(auth, "AUTH x"),
            "-ERR AUTH <password> called without any password configured for the default user. \
             Are you sure your configuration is correct?\r\n"
        );
        assert_eq!(f.run_conn(auth, "AUTH default x"), "+OK\r\n");
        assert_eq!(
            f.run_conn(auth, "AUTH bob x"),
            "-WRONGPASS invalid username-password pair or user is disabled.\r\n"
        );
    }

    #[test]
    fn auth_with_password() {
        let mut f = Fixture::with_config(Config {
            requirepass: "s3cret".into(),
            ..Config::default()
        });
        assert_eq!(
            f.run_conn(auth, "AUTH wrong"),
            "-WRONGPASS invalid username-password pair or user is disabled.\r\n"
        );
        assert!(!f.conn.authed);
        assert!(
            f.run_conn(hello, "HELLO 3")
                .starts_with("-NOAUTH HELLO must be called")
        );
        assert_eq!(
            f.conn.proto,
            Proto::Resp2,
            "a refused HELLO switches nothing"
        );
        assert_eq!(
            f.run_conn(hello, "HELLO 3 AUTH default nope"),
            "-WRONGPASS invalid username-password pair or user is disabled.\r\n"
        );
        assert_eq!(f.run_conn(auth, "AUTH s3cret"), "+OK\r\n");
        assert!(f.conn.authed);
        assert_eq!(f.run_conn(reset, "RESET"), "+RESET\r\n");
        assert!(
            !f.conn.authed,
            "RESET de-authenticates when a password is set"
        );
        assert!(
            f.run_conn(hello, "HELLO 3 AUTH default s3cret SETNAME n")
                .starts_with('%')
        );
        assert!(f.conn.authed);
        assert_eq!(f.conn.name.as_deref(), Some(&b"n"[..]));
        assert_eq!(f.run_conn(auth, "AUTH default s3cret"), "+OK\r\n");
        assert!(!same_secret(b"s3cre", b"s3cret"));
    }

    #[test]
    fn hello_switches_protocol() {
        let mut f = Fixture::new();
        assert_eq!(
            f.run_conn(hello, "HELLO 4"),
            "-NOPROTO unsupported protocol version\r\n"
        );
        assert_eq!(
            f.run_conn(hello, "HELLO abc"),
            "-ERR Protocol version is not an integer or out of range\r\n"
        );
        assert_eq!(
            f.run_conn(hello, "HELLO 3 FOO"),
            "-ERR Syntax error in HELLO option 'FOO'\r\n"
        );
        assert_eq!(
            f.run_conn(hello, "HELLO 3 AUTH a"),
            "-ERR Syntax error in HELLO option 'AUTH'\r\n"
        );
        assert_eq!(
            f.run_conn(hello, "HELLO 3 AUTH bob pw"),
            "-WRONGPASS invalid username-password pair or user is disabled.\r\n"
        );
        let r = f.run_conn(hello, "HELLO 3");
        assert_eq!(
            r,
            "%7\r\n$6\r\nserver\r\n$3\r\nkvd\r\n$7\r\nversion\r\n$5\r\n7.2.0\r\n$5\r\nproto\r\n:3\r\n$2\r\nid\r\n:12\r\n\
             $4\r\nmode\r\n$10\r\nstandalone\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n"
        );
        assert_eq!(f.conn.proto, Proto::Resp3);
        // HELLO without a version replies in the current protocol
        assert!(f.run_conn(hello, "HELLO").starts_with("%7\r\n"));
        assert!(
            f.run_conn(hello, "HELLO 2 SETNAME x AUTH default p")
                .starts_with("*14\r\n")
        );
        assert_eq!(f.conn.proto, Proto::Resp2);
        assert_eq!(f.run_conn(client_getname, "CLIENT GETNAME"), "$1\r\nx\r\n");
    }

    #[test]
    fn select_rules() {
        let mut f = Fixture::new();
        assert_eq!(f.run_conn(select, "SELECT 15"), "+OK\r\n");
        assert_eq!(f.conn.db, 15);
        assert_eq!(
            f.run_conn(select, "SELECT 16"),
            "-ERR DB index is out of range\r\n"
        );
        assert_eq!(
            f.run_conn(select, "SELECT -1"),
            "-ERR DB index is out of range\r\n"
        );
        assert_eq!(
            f.run_conn(select, "SELECT 1.5"),
            "-ERR value is not an integer or out of range\r\n"
        );
        assert_eq!(
            f.run_conn(select, "SELECT 015"),
            "-ERR value is not an integer or out of range\r\n"
        );
        assert_eq!(
            f.run_conn(select, "SELECT 2147483648"),
            "-ERR value is out of range, value must between -2147483648 and 2147483647\r\n"
        );
        assert_eq!(f.conn.db, 15, "a failed SELECT keeps the database");
        let mut f = Fixture::with_config(Config {
            cluster_enabled: true,
            databases: 1,
            ..Config::default()
        });
        assert_eq!(
            f.run_conn(select, "SELECT 1"),
            "-ERR SELECT is not allowed in cluster mode\r\n"
        );
        assert_eq!(f.run_conn(select, "SELECT 0"), "+OK\r\n");
    }

    #[test]
    fn quit_and_reset() {
        let mut f = Fixture::new();
        let (r, o) = f.run_argv(quit, &words_owned("QUIT"));
        assert_eq!(r, "+OK\r\n");
        assert!(matches!(o, ConnOutcome::Close));
        f.conn.db = 3;
        f.conn.proto = Proto::Resp3;
        f.conn.name = Some(b"n".to_vec().into());
        f.conn.readonly = true;
        f.conn.flags |= flags::NO_EVICT;
        f.conn.multi = Some(crate::net::conn::MultiState {
            queued: Vec::new(),
            errored: false,
        });
        assert_eq!(f.run_conn(reset, "RESET"), "+RESET\r\n");
        assert_eq!(
            (f.conn.db, f.conn.proto, f.conn.readonly),
            (0, Proto::Resp2, false)
        );
        assert!(f.conn.name.is_none() && f.conn.multi.is_none());
        assert_eq!(f.conn.flags & flags::NO_EVICT, 0);
        assert!(f.conn.authed);
    }

    #[test]
    fn client_name_and_info() {
        let mut f = Fixture::new();
        assert_eq!(f.run_conn(client_id, "CLIENT ID"), ":12\r\n");
        assert_eq!(f.run_conn(client_getname, "CLIENT GETNAME"), "$-1\r\n");
        assert_eq!(
            f.run_conn(client_setname, "CLIENT SETNAME worker-1"),
            "+OK\r\n"
        );
        assert_eq!(
            f.run_conn(client_getname, "CLIENT GETNAME"),
            "$8\r\nworker-1\r\n"
        );
        let (r, _) = f.run_argv(
            client_setname,
            &[
                b"CLIENT".to_vec(),
                b"SETNAME".to_vec(),
                b"has space".to_vec(),
            ],
        );
        assert_eq!(
            r,
            "-ERR Client names cannot contain spaces, newlines or special characters.\r\n"
        );
        let (r, _) = f.run_argv(
            client_setname,
            &[b"CLIENT".to_vec(), b"SETNAME".to_vec(), Vec::new()],
        );
        assert_eq!(r, "+OK\r\n");
        assert_eq!(f.run_conn(client_getname, "CLIENT GETNAME"), "$-1\r\n");
        f.conn.name = Some(b"foo".to_vec().into());
        let info = f.run_conn(client_info, "CLIENT INFO");
        assert!(
            info.starts_with('$')
                && info.contains("id=12 addr= laddr= fd=7 name=foo age=0 idle=0 flags=N db=0 "),
            "{info}"
        );
        assert!(
            info.contains(" multi=-1 ")
                && info.contains(
                    " cmd=client|info user=default redir=-1 resp=2 lib-name= lib-ver=\n\r\n"
                )
        );
        f.conn.proto = Proto::Resp3;
        assert!(
            f.run_conn(client_info, "CLIENT INFO")
                .contains("\r\ntxt:id=12 ")
        );
    }

    #[test]
    fn client_setinfo_and_flags() {
        let mut f = Fixture::new();
        assert_eq!(
            f.run_conn(client_setinfo, "CLIENT SETINFO lib-name redis-py"),
            "+OK\r\n"
        );
        assert_eq!(
            f.run_conn(client_setinfo, "CLIENT SETINFO LIB-VER 5.2.1"),
            "+OK\r\n"
        );
        assert_eq!(f.conn.lib_name.as_deref(), Some(&b"redis-py"[..]));
        assert_eq!(
            f.run_conn(client_setinfo, "CLIENT SETINFO foo x"),
            "-ERR Unrecognized option 'foo'\r\n"
        );
        let (r, _) = f.run_argv(
            client_setinfo,
            &[
                b"CLIENT".to_vec(),
                b"SETINFO".to_vec(),
                b"Lib-Name".to_vec(),
                b"a b".to_vec(),
            ],
        );
        assert_eq!(
            r,
            "-ERR Lib-Name cannot contain spaces, newlines or special characters.\r\n"
        );
        assert_eq!(f.run_conn(client_no_evict, "CLIENT NO-EVICT on"), "+OK\r\n");
        assert_ne!(f.conn.flags & flags::NO_EVICT, 0);
        assert_eq!(
            f.run_conn(client_no_evict, "CLIENT NO-EVICT maybe"),
            "-ERR syntax error\r\n"
        );
        assert_eq!(
            f.run_conn(client_no_touch, "CLIENT NO-TOUCH OFF"),
            "+OK\r\n"
        );
        assert_eq!(f.run_conn(client_reply, "CLIENT REPLY ON"), "+OK\r\n");
        assert_eq!(
            f.run_conn(client_reply, "CLIENT REPLY foo"),
            "-ERR syntax error\r\n"
        );
    }

    #[test]
    fn client_list_and_kill_arguments() {
        let mut f = Fixture::new();
        assert_eq!(
            f.run_conn(client_list, "CLIENT LIST TYPE foo"),
            "-ERR Unknown client type 'foo'\r\n"
        );
        assert_eq!(
            f.run_conn(client_list, "CLIENT LIST ID abc"),
            "-ERR Invalid client ID\r\n"
        );
        assert_eq!(
            f.run_conn(client_list, "CLIENT LIST foo"),
            "-ERR syntax error\r\n"
        );
        assert_eq!(
            f.run_conn(client_list, "CLIENT LIST ID"),
            "-ERR syntax error\r\n"
        );
        assert_eq!(
            f.async_op("CLIENT LIST ID 1 12"),
            Some(NodeOp::ClientList(ClientFilter {
                kind: None,
                ids: Some(vec![1, 12])
            }))
        );
        let Some(NodeOp::ClientList(filter)) = f.async_op("CLIENT LIST TYPE master") else {
            panic!()
        };
        assert!(!filter.matches(12));
        let Some(NodeOp::ClientList(filter)) = f.async_op("CLIENT LIST") else {
            panic!()
        };
        assert!(filter.matches(12));

        assert_eq!(
            f.run_conn(client_kill, "CLIENT KILL ID abc"),
            "-ERR client-id should be greater than 0\r\n"
        );
        assert_eq!(
            f.run_conn(client_kill, "CLIENT KILL ID 0"),
            "-ERR client-id should be greater than 0\r\n"
        );
        assert_eq!(
            f.run_conn(client_kill, "CLIENT KILL foo bar"),
            "-ERR syntax error\r\n"
        );
        assert_eq!(
            f.run_conn(client_kill, "CLIENT KILL SKIPME maybe"),
            "-ERR syntax error\r\n"
        );
        assert_eq!(
            f.run_conn(client_kill, "CLIENT KILL ID 1 SKIPME"),
            "-ERR syntax error\r\n"
        );
        assert_eq!(
            f.run_conn(client_kill, "CLIENT KILL TYPE foo"),
            "-ERR Unknown client type 'foo'\r\n"
        );
        assert_eq!(
            f.run_conn(client_kill, "CLIENT KILL USER bob"),
            "-ERR No such user 'bob'\r\n"
        );
        let Some(NodeOp::ClientKill {
            filter,
            old_form: false,
            caller: 12,
        }) = f.async_op("CLIENT KILL USER default")
        else {
            panic!()
        };
        assert!(!filter.matches(12, "", "", 12), "SKIPME yes by default");
        assert!(filter.matches(13, "", "", 12));
        let Some(NodeOp::ClientKill {
            filter,
            old_form: true,
            ..
        }) = f.async_op("CLIENT KILL 1.2.3.4:5")
        else {
            panic!()
        };
        assert!(
            filter.matches(12, "1.2.3.4:5", "", 12),
            "the old form may kill the caller"
        );
        assert!(!filter.matches(13, "1.2.3.4:6", "", 12));
        let Some(NodeOp::ClientKill { filter, .. }) = f.async_op("CLIENT KILL ID 13 SKIPME no")
        else {
            panic!()
        };
        assert!(filter.matches(13, "x", "y", 13) && !filter.matches(12, "x", "y", 13));
    }

    #[test]
    fn client_list_reply_rendering() {
        let f = Fixture::new();
        let mut f = f;
        f.conn.addr = Some("127.0.0.1:5000".parse().unwrap());
        f.conn.laddr = Some("127.0.0.1:6379".parse().unwrap());
        let mut lines = Vec::new();
        write_client_line(&f.conn, None, f.conn.created, &mut lines);
        let s = String::from_utf8(lines.clone()).unwrap();
        assert!(s.starts_with(
            "id=12 addr=127.0.0.1:5000 laddr=127.0.0.1:6379 fd=7 name= age=0 idle=0 flags=N db=0"
        ));
        assert!(s.ends_with(" cmd=NULL user=default redir=-1 resp=2 lib-name= lib-ver=\n"));
        let mut out = Vec::new();
        let op = NodeOp::ClientList(ClientFilter::default());
        crate::cmd::server::write_node_op_reply(
            &op,
            crate::cmd::server::NodeOpDone::Clients(&lines),
            &mut ReplyWriter::new(&mut out, Proto::Resp3),
        );
        assert!(out.starts_with(b"=") && out.windows(4).any(|w| w == b"txt:"));
        let mut out = Vec::new();
        let op = NodeOp::ClientKill {
            filter: parse_client_kill(&[b"CLIENT", b"KILL", b"a:1"]).unwrap().0,
            old_form: true,
            caller: 1,
        };
        crate::cmd::server::write_node_op_reply(
            &op,
            crate::cmd::server::NodeOpDone::Killed(0),
            &mut ReplyWriter::new(&mut out, Proto::Resp2),
        );
        assert_eq!(out, b"-ERR No such client\r\n");
    }
}
