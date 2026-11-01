//! Client topology service on `client-port` (DESIGN §23.5).
//!
//! CLUSTER SLOTS/SHARDS/NODES/INFO rendered from the committed state (cached per version);
//! CLUSTER MYID / MYSHARDID / KEYSLOT; protocol-compatibility replies (HELLO with `mode:
//! cluster`, PING, ECHO, AUTH, SELECT 0, QUIT, RESET, READONLY, READWRITE, ASKING, COMMAND*,
//! CLIENT*, INFO with `server_kind:coordinator`); keyed commands answered with MOVED to the
//! slot's primary, CLUSTERDOWN or CROSSSLOT (bulk arguments are read and discarded); MULTI is
//! accepted and EXEC aborts. Followers serve topology from their committed state while they
//! heard from a leader within 2 s, else `-CLUSTERDOWN The cluster is down`.
//!
//! Keyless commands that clients send to "all masters" are not in this service's table with
//! coordinator line style R1. With style R2 they get the constant-literal replies below: no
//! keyspace abstraction and no stored state exist here (LC-38).
//!
//! The client password (`requirepass`, a cluster parameter) applies here as on the data nodes,
//! so clients configured with it bootstrap from coordinators unchanged.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use kv_cmd::{CmdFlags, CmdId, ResolveError};
use kv_ctrl::NodeId;
use kv_resp::{Limits, Parsed, Parser, Proto, ProtoError, ReplyWriter};
use kv_topo::ClusterState;
use kv_topo::redirect::{self, KeysRedirect};
use kv_topo::render::{self, CoordLineStyle, RenderedTopology};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::wsr::Wsr;
use crate::wsr::rpc::ct_eq;

/// R2 stub: DBSIZE
pub const STUB_DBSIZE: &[u8] = b":0\r\n";
/// R2 stub: KEYS
pub const STUB_KEYS: &[u8] = b"*0\r\n";
/// R2 stub: SCAN
pub const STUB_SCAN: &[u8] = b"*2\r\n$1\r\n0\r\n*0\r\n";
/// R2 stub: RANDOMKEY (RESP2)
pub const STUB_RANDOMKEY_RESP2: &[u8] = b"$-1\r\n";
/// R2 stub: RANDOMKEY (RESP3)
pub const STUB_RANDOMKEY_RESP3: &[u8] = b"_\r\n";
/// R2 stub: FLUSHALL / FLUSHDB (no-op)
pub const STUB_FLUSH: &[u8] = b"+OK\r\n";
/// R2 stub: WAIT
pub const STUB_WAIT: &[u8] = b":0\r\n";
/// CLUSTER COUNTKEYSINSLOT / GETKEYSINSLOT
pub const ERR_NO_KEYS: &[u8] = b"-ERR the coordinator stores no keys\r\n";
/// Follower without a recent leader.
pub const ERR_CLUSTER_DOWN: &[u8] = b"-CLUSTERDOWN The cluster is down\r\n";

/// Is `node` suspected right now (the leader's PFAIL view; followers suspect nobody)?
pub type PfailFn = Arc<dyn Fn(&NodeId) -> bool + Send + Sync>;

/// Options of the client service.
#[derive(Clone)]
pub struct ClientSvcCfg {
    pub style: CoordLineStyle,
    pub pfail: Option<PfailFn>,
    /// The client port, for INFO.
    pub port: u16,
}

impl Default for ClientSvcCfg {
    fn default() -> Self {
        ClientSvcCfg {
            style: CoordLineStyle::R1,
            pfail: None,
            port: 0,
        }
    }
}

/// State shared by every client connection.
struct Svc {
    wsr: Arc<Wsr>,
    cfg: ClientSvcCfg,
    /// Rendered topology of `(version, peer generation)`.
    cache: Mutex<Option<(u64, u64, Arc<RenderedTopology>)>>,
    next_id: AtomicU64,
    clients: AtomicUsize,
    started: Instant,
    run_id: String,
}

pub async fn serve_clients(l: tokio::net::TcpListener, wsr: Arc<Wsr>) -> std::io::Result<()> {
    serve_clients_with(l, wsr, ClientSvcCfg::default()).await
}

/// [`serve_clients`] with explicit options (line style, the leader's PFAIL view).
pub async fn serve_clients_with(
    l: tokio::net::TcpListener,
    wsr: Arc<Wsr>,
    cfg: ClientSvcCfg,
) -> std::io::Result<()> {
    let svc = Arc::new(Svc {
        wsr,
        cfg,
        cache: Mutex::new(None),
        next_id: AtomicU64::new(1),
        clients: AtomicUsize::new(0),
        started: Instant::now(),
        run_id: kv_ctrl::CoordId::random().hex40(),
    });
    loop {
        let (s, peer) = l.accept().await?;
        let svc = svc.clone();
        tokio::spawn(async move {
            let _ = s.set_nodelay(true);
            svc.clients.fetch_add(1, Ordering::Relaxed);
            if let Err(e) = svc.clone().conn(s, peer).await {
                tracing::debug!(%peer, "client connection closed: {e}");
            }
            svc.clients.fetch_sub(1, Ordering::Relaxed);
        });
    }
}

/// Per-connection state.
struct Conn {
    id: u64,
    peer: SocketAddr,
    proto: Proto,
    authed: bool,
    name: Option<Vec<u8>>,
    in_multi: bool,
    close: bool,
}

/// Bytes read per socket read.
const READ_CHUNK: usize = 64 * 1024;

impl Svc {
    fn password(&self, s: &ClusterState) -> Option<String> {
        s.params.client_password.clone()
    }

    fn topology(&self, s: &ClusterState) -> Arc<RenderedTopology> {
        let peers = self.wsr.peers();
        let mut c = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((v, g, t)) = c.as_ref() {
            if *v == s.version && *g == peers.r#gen {
                return t.clone();
            }
        }
        let t = Arc::new(RenderedTopology::build(
            s,
            &peers.endpoints(),
            &self.wsr.id(),
            self.cfg.style,
        ));
        *c = Some((s.version, peers.r#gen, t.clone()));
        t
    }

    async fn conn(self: Arc<Self>, mut sock: TcpStream, peer: SocketAddr) -> std::io::Result<()> {
        let mut c = Conn {
            id: self.next_id.fetch_add(1, Ordering::Relaxed),
            peer,
            proto: Proto::Resp2,
            authed: self.password(&self.wsr.committed()).is_none(),
            name: None,
            in_multi: false,
            close: false,
        };
        let mut parser = Parser::new(Limits::default());
        let mut buf: Vec<u8> = Vec::with_capacity(READ_CHUNK);
        let mut out: Vec<u8> = Vec::with_capacity(4096);
        let mut args: Vec<(u32, u32)> = Vec::new();
        loop {
            let start = buf.len();
            buf.resize(start + READ_CHUNK, 0);
            let n = sock.read(&mut buf[start..]).await?;
            buf.truncate(start + n);
            if n == 0 {
                return Ok(());
            }
            let mut pos = 0;
            loop {
                parser.set_auth_required(!c.authed);
                args.clear();
                match parser.parse(&buf[pos..], &mut args) {
                    Ok(Parsed::Incomplete) => break,
                    Ok(Parsed::Empty { consumed }) => pos += consumed,
                    Ok(Parsed::Command { consumed }) => {
                        let src = parser.arg_source(&buf[pos..]);
                        let argv: Vec<&[u8]> = args
                            .iter()
                            .map(|&(o, l)| &src[o as usize..(o + l) as usize])
                            .collect();
                        self.command(&mut c, &argv, &mut out);
                        pos += consumed;
                        if c.close {
                            break;
                        }
                    }
                    Err(e) => {
                        protocol_error(&e, &mut out);
                        c.close = true;
                        break;
                    }
                }
            }
            buf.drain(..pos);
            if !out.is_empty() {
                sock.write_all(&out).await?;
                out.clear();
            }
            if c.close {
                let _ = sock.shutdown().await;
                return Ok(());
            }
        }
    }

    fn command(&self, c: &mut Conn, argv: &[&[u8]], out: &mut Vec<u8>) {
        let s = self.wsr.committed();
        let mut w = ReplyWriter::new(out, c.proto);
        let spec = match kv_cmd::resolve(argv) {
            Ok(spec) => spec,
            Err(e) => {
                if !c.authed && !is_no_auth(argv) {
                    return w.error(b"NOAUTH Authentication required.");
                }
                return e.write(argv, &mut w);
            }
        };
        if !c.authed && !spec.flags.contains(CmdFlags::NO_AUTH) {
            return w.error(b"NOAUTH Authentication required.");
        }
        if c.in_multi && !matches!(spec.id, CmdId::Exec | CmdId::Discard | CmdId::Multi | CmdId::Quit | CmdId::Reset) {
            // Commands that would be answered here are queued; anything redirected or refused
            // answers the error at once (EXEC then aborts, as in Redis).
            let mut probe = Vec::new();
            self.dispatch(c, spec, argv, &s, &mut ReplyWriter::new(&mut probe, c.proto));
            if probe.first() == Some(&b'-') {
                out.extend_from_slice(&probe);
            } else {
                ReplyWriter::new(out, c.proto).simple(b"QUEUED");
            }
            return;
        }
        self.dispatch(c, spec, argv, &s, &mut w);
    }

    fn dispatch(
        &self,
        c: &mut Conn,
        spec: &'static kv_cmd::CommandSpec,
        argv: &[&[u8]],
        s: &ClusterState,
        w: &mut ReplyWriter<'_>,
    ) {
        if spec.flags.contains(CmdFlags::COORD_OK) {
            return self.local(c, spec, argv, s, w);
        }
        if spec.has_keys() {
            let pos = kv_cmd::key_positions(spec, argv);
            if !pos.is_empty() {
                if !self.wsr.serving_topology() {
                    return w.error(&ERR_CLUSTER_DOWN[1..ERR_CLUSTER_DOWN.len() - 2]);
                }
                let slots: Vec<u16> = pos.iter().map(|&p| kv_common::key_slot(argv[usize::from(p)])).collect();
                let pfail = |n: &NodeId| self.cfg.pfail.as_ref().is_some_and(|f| f(n));
                if let Some(r) = redirect::for_key_slots(s, &slots, &pfail) {
                    return match r {
                        KeysRedirect::CrossSlot => w.error(kv_cmd::CrossSlot::MESSAGE),
                        r => r.write(w),
                    };
                }
            }
        }
        if self.cfg.style == CoordLineStyle::R2 {
            let stub: Option<&[u8]> = match spec.id {
                CmdId::Dbsize => Some(STUB_DBSIZE),
                CmdId::Keys => Some(STUB_KEYS),
                CmdId::Scan => Some(STUB_SCAN),
                CmdId::Randomkey => Some(match c.proto {
                    Proto::Resp2 => STUB_RANDOMKEY_RESP2,
                    Proto::Resp3 => STUB_RANDOMKEY_RESP3,
                }),
                CmdId::Flushall | CmdId::Flushdb => Some(STUB_FLUSH),
                CmdId::Wait => Some(STUB_WAIT),
                _ => None,
            };
            if let Some(stub) = stub {
                return w.buf.extend_from_slice(stub);
            }
        }
        // Not served by a coordinator: as if it were not in the table.
        let e = if spec.name.contains('|') {
            ResolveError::UnknownSubcommand
        } else {
            ResolveError::UnknownCommand
        };
        e.write(argv, w);
    }

    fn local(
        &self,
        c: &mut Conn,
        spec: &'static kv_cmd::CommandSpec,
        argv: &[&[u8]],
        s: &ClusterState,
        w: &mut ReplyWriter<'_>,
    ) {
        match spec.id {
            CmdId::Ping => match argv.get(1) {
                None => w.simple(b"PONG"),
                Some(m) => w.bulk(m),
            },
            CmdId::Echo => w.bulk(argv[1]),
            CmdId::Hello => self.hello(c, argv, s, w),
            CmdId::Auth => {
                let (user, pass) = if argv.len() == 3 {
                    (argv[1], argv[2])
                } else {
                    (&b"default"[..], argv[1])
                };
                match self.password(s) {
                    None if argv.len() == 2 => w.error(
                        b"ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?",
                    ),
                    pw => {
                        let ok = user == b"default"
                            && pw.as_deref().is_none_or(|p| ct_eq(p.as_bytes(), pass));
                        if ok {
                            c.authed = true;
                            w.ok();
                        } else {
                            w.error(b"WRONGPASS invalid username-password pair or user is disabled.");
                        }
                    }
                }
            }
            CmdId::Select => match kv_common::num::string2ll(argv[1]) {
                Some(0) => w.ok(),
                Some(_) => w.error(b"ERR DB index is out of range"),
                None => w.error(b"ERR value is not an integer or out of range"),
            },
            CmdId::Quit => {
                w.ok();
                c.close = true;
            }
            CmdId::Reset => {
                c.in_multi = false;
                c.name = None;
                c.proto = Proto::Resp2;
                c.authed = self.password(s).is_none();
                ReplyWriter::new(&mut *w.buf, Proto::Resp2).simple(b"RESET");
            }
            CmdId::Readonly | CmdId::Readwrite | CmdId::Asking => w.ok(),
            CmdId::Multi => {
                if c.in_multi {
                    w.error(b"ERR MULTI calls can not be nested");
                } else {
                    c.in_multi = true;
                    w.ok();
                }
            }
            CmdId::Exec => {
                if !c.in_multi {
                    w.error(b"ERR EXEC without MULTI");
                } else {
                    c.in_multi = false;
                    w.error(b"EXECABORT Transaction discarded because of previous errors.");
                }
            }
            CmdId::Discard => {
                if !c.in_multi {
                    w.error(b"ERR DISCARD without MULTI");
                } else {
                    c.in_multi = false;
                    w.ok();
                }
            }
            CmdId::Info => self.info(argv, s, w),
            _ => self.container(c, spec, argv, s, w),
        }
    }

    fn container(
        &self,
        c: &mut Conn,
        spec: &'static kv_cmd::CommandSpec,
        argv: &[&[u8]],
        s: &ClusterState,
        w: &mut ReplyWriter<'_>,
    ) {
        let name = spec.name;
        match name {
            "command" => kv_cmd::write_command_all(w),
            "command|count" => w.int(kv_cmd::command_count() as i64),
            "command|info" => kv_cmd::write_command_info_reply(&argv[2..], w),
            "command|docs" => kv_cmd::write_command_docs_reply(&argv[2..], w),
            "command|list" => kv_cmd::write_command_list_reply(&argv[2..], w),
            "command|getkeys" => kv_cmd::write_command_getkeys(&argv[2..], w),
            "client|id" => w.int(i64::try_from(c.id).unwrap_or(i64::MAX)),
            "client|getname" => match &c.name {
                Some(n) => w.bulk(n),
                None => w.null(),
            },
            "client|setname" => {
                if !kv_resp::valid_client_name(argv[2]) {
                    w.error(kv_resp::CLIENT_NAME_ERROR);
                } else {
                    c.name = (!argv[2].is_empty()).then(|| argv[2].to_vec());
                    w.ok();
                }
            }
            "client|info" | "client|list" => {
                let mut line = format!("id={} addr={} laddr= fd=0 name=", c.id, c.peer).into_bytes();
                line.extend_from_slice(c.name.as_deref().unwrap_or(b""));
                line.extend_from_slice(
                    format!(
                        " age=0 idle=0 flags=N db=0 sub=0 psub=0 ssub=0 multi={} qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=0 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd={} user=default redir=-1 resp={}\n",
                        if c.in_multi { 0 } else { -1 },
                        name,
                        match c.proto { Proto::Resp2 => 2, Proto::Resp3 => 3 }
                    )
                    .as_bytes(),
                );
                if name == "client|info" {
                    render::write_text(w, &line);
                } else {
                    w.bulk(&line);
                }
            }
            "client|setinfo" | "client|no-evict" | "client|no-touch" | "client|reply" => w.ok(),
            "client|kill" => w.error(b"ERR No such client"),
            n if n.starts_with("cluster") => self.cluster(argv, s, w),
            _ => ResolveError::UnknownSubcommand.write(argv, w),
        }
    }

    fn cluster(&self, argv: &[&[u8]], s: &ClusterState, w: &mut ReplyWriter<'_>) {
        let sub = argv.get(1).map(|a| a.to_ascii_lowercase()).unwrap_or_default();
        match sub.as_slice() {
            b"myid" => return render::cluster_myid(&self.wsr.id(), w),
            b"myshardid" => return render::cluster_myshardid(w),
            b"keyslot" => return w.int(i64::from(kv_common::key_slot(argv[2]))),
            b"countkeysinslot" | b"getkeysinslot" => return w.buf.extend_from_slice(ERR_NO_KEYS),
            _ => {}
        }
        if !self.wsr.serving_topology() {
            return w.buf.extend_from_slice(ERR_CLUSTER_DOWN);
        }
        let t = self.topology(s);
        let p = w.proto;
        match sub.as_slice() {
            b"slots" => w.buf.extend_from_slice(t.slots(p)),
            b"shards" => w.buf.extend_from_slice(t.shards(p)),
            b"nodes" => w.buf.extend_from_slice(t.nodes(p)),
            b"info" => w.buf.extend_from_slice(t.info(p)),
            _ => ResolveError::UnknownSubcommand.write(argv, w),
        }
    }

    fn hello(&self, c: &mut Conn, argv: &[&[u8]], s: &ClusterState, w: &mut ReplyWriter<'_>) {
        let args = match kv_resp::parse_hello(argv) {
            Ok(a) => a,
            Err(e) => return e.write(argv, w),
        };
        if let Some((user, pass)) = args.auth {
            let ok = user == b"default"
                && self
                    .password(s)
                    .as_deref()
                    .is_none_or(|p| ct_eq(p.as_bytes(), pass));
            if !ok {
                return w.error(b"WRONGPASS invalid username-password pair or user is disabled.");
            }
            c.authed = true;
        }
        if !c.authed {
            return w.error(
                b"NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time",
            );
        }
        if let Some(n) = args.setname {
            c.name = (!n.is_empty()).then(|| n.to_vec());
        }
        if let Some(p) = args.proto {
            c.proto = p;
        }
        let mut w = ReplyWriter::new(&mut *w.buf, c.proto);
        render::coordinator_hello(&mut w, c.id, self.cfg.style);
    }

    fn info(&self, argv: &[&[u8]], s: &ClusterState, w: &mut ReplyWriter<'_>) {
        let wanted: Vec<Vec<u8>> = argv[1..].iter().map(|a| a.to_ascii_lowercase()).collect();
        let all = wanted.is_empty()
            || wanted
                .iter()
                .any(|a| matches!(a.as_slice(), b"all" | b"default" | b"everything"));
        let want = |sec: &[u8]| all || wanted.iter().any(|a| a.as_slice() == sec);
        let mut t = String::new();
        if want(b"server") {
            let up = self.started.elapsed().as_secs();
            t.push_str("# Server\r\n");
            t.push_str(&format!("redis_version:{}\r\n", render::COMPAT_VERSION));
            t.push_str("server_kind:coordinator\r\n");
            t.push_str("redis_mode:cluster\r\n");
            t.push_str(&format!("process_id:{}\r\n", std::process::id()));
            t.push_str(&format!("run_id:{}\r\n", self.run_id));
            t.push_str(&format!("tcp_port:{}\r\n", self.cfg.port));
            t.push_str(&format!("uptime_in_seconds:{up}\r\n"));
            t.push_str(&format!("uptime_in_days:{}\r\n", up / 86_400));
            t.push_str("\r\n");
        }
        if want(b"clients") {
            t.push_str("# Clients\r\n");
            t.push_str(&format!(
                "connected_clients:{}\r\n\r\n",
                self.clients.load(Ordering::Relaxed)
            ));
        }
        if want(b"cluster") {
            t.push_str("# Cluster\r\ncluster_enabled:1\r\n\r\n");
        }
        let _ = s;
        while t.ends_with("\r\n\r\n") {
            t.truncate(t.len() - 2);
        }
        render::write_text(w, t.as_bytes());
    }
}

/// AUTH, HELLO, QUIT and RESET are allowed before authentication (Redis `no_auth`).
fn is_no_auth(argv: &[&[u8]]) -> bool {
    argv.first().is_some_and(|n| {
        [&b"AUTH"[..], b"HELLO", b"QUIT", b"RESET"]
            .iter()
            .any(|x| n.eq_ignore_ascii_case(x))
    })
}

fn protocol_error(e: &ProtoError, out: &mut Vec<u8>) {
    e.write_reply(out);
}
