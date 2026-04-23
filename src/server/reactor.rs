//! Single-threaded io_uring reactor for the RESP server.
//!
//! Replaces the thread-per-connection model (`RedisServer::run`) with a
//! Valkey-shaped event loop: one kernel thread hosts an `io_uring` SQ,
//! accepts connections, drains recv buffers, parses RESP in place, runs
//! commands on the same thread, and queues sends.
//!
//! Single-threaded on purpose — it matches Redis/Valkey's original design,
//! sidesteps multi-threaded command execution, and keeps `RedisHandler`'s
//! `Cell`/`RefCell`-based per-connection state valid. A future change can
//! add IO worker threads for the parse/write split.

use std::cell::Cell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tracing::{debug, error, info};

use crate::cluster::router::ClusterRouter;
use crate::io::uring_net::{NetEvent, UringServer};
use crate::server::db_manager::{DatabaseManager, DbHandler};
use crate::server::redis::{PubSubManager, RedisHandler, RespParser, ServerTuning};
use crate::storage::wal::WriteAheadLog;

/// Per-connection state. RESP parser, its pending output buffer, and the
/// handler holding this client's SELECT / MULTI / WATCH state.
struct ConnState {
    parser: RespParser,
    handler: RedisHandler,
    /// Response bytes produced but not yet sent. May be pending durability
    /// (pending_up_to > 0) or ready to flush (pending_up_to == 0).
    pending_out: Vec<u8>,
    /// Max WAL position this connection is waiting on for durability.
    /// Set after a handle_command that did a put_nowait / delete_nowait;
    /// cleared once the reactor observes durable_position() >= it and
    /// flushes pending_out to the socket.
    pending_up_to: u64,
}

impl ConnState {
    fn new(
        db_manager: Arc<DatabaseManager>,
        router: Option<Arc<ClusterRouter>>,
        replica_read: bool,
        pubsub: Arc<PubSubManager>,
        tuning: ServerTuning,
    ) -> Self {
        let handler = if let Some(r) = router {
            RedisHandler::with_router_and_pubsub(db_manager, r, replica_read, pubsub)
        } else {
            RedisHandler::new_with_pubsub(db_manager, pubsub)
        };
        Self {
            parser: RespParser::new(tuning.read_buf_bytes),
            handler,
            pending_out: Vec::with_capacity(tuning.write_buf_bytes),
            pending_up_to: 0,
        }
    }
}

/// Reactor configuration mirrored from `RedisServer`.
pub struct ReactorServer {
    addr: SocketAddr,
    db_manager: Arc<DatabaseManager>,
    router: Option<Arc<ClusterRouter>>,
    replica_read: bool,
    pubsub: Arc<PubSubManager>,
    tuning: ServerTuning,
    live_conns: Arc<AtomicUsize>,
}

impl ReactorServer {
    pub fn new(
        addr: SocketAddr,
        db_manager: Arc<DatabaseManager>,
        tuning: ServerTuning,
    ) -> Self {
        Self {
            addr,
            db_manager,
            router: None,
            replica_read: false,
            pubsub: Arc::new(PubSubManager::new()),
            tuning,
            live_conns: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn with_router(
        addr: SocketAddr,
        db_manager: Arc<DatabaseManager>,
        router: Arc<ClusterRouter>,
        replica_read: bool,
        tuning: ServerTuning,
    ) -> Self {
        Self {
            addr,
            db_manager,
            router: Some(router),
            replica_read,
            pubsub: Arc::new(PubSubManager::new()),
            tuning,
            live_conns: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn run(&self) -> std::io::Result<()> {
        self.run_with_reuseport(false)
    }

    /// Variant of `run` that binds with SO_REUSEPORT so multiple reactor
    /// threads can share the same port. Intended for the multi-reactor
    /// startup path — the kernel load-balances new connections across
    /// reactors.
    pub fn run_with_reuseport(&self, reuse_port: bool) -> std::io::Result<()> {
        // Generous queue depth — 4096 SQEs covers many concurrent accepts,
        // recvs and sends in flight simultaneously.
        let queue_depth: u32 = 4096;
        let mut server = UringServer::new_with_options(
            self.addr,
            queue_depth,
            self.tuning.read_buf_bytes,
            reuse_port,
        )?;
        server.start_accept()?;

        info!(
            "Redis reactor listening on {}{}",
            self.addr,
            if reuse_port { " [SO_REUSEPORT]" } else { "" }
        );

        let mut connections: HashMap<RawFd, ConnState> = HashMap::new();

        let db_manager = Arc::clone(&self.db_manager);
        let router = self.router.clone();
        let replica_read = self.replica_read;
        let pubsub = Arc::clone(&self.pubsub);
        let tuning = self.tuning;
        let live_conns = Arc::clone(&self.live_conns);
        let max_conns = self.tuning.max_connections;

        // Snapshot the SSD handlers so the reactor can poll their WALs'
        // durable_position each iteration. Kept outside the closure to
        // avoid the db_manager borrow. Memory-only DBs have no WAL; we
        // ignore those indices.
        let wals: Vec<Option<Arc<WriteAheadLog>>> = (0..db_manager.num_dbs())
            .map(|i| {
                db_manager
                    .db(i)
                    .and_then(|d| d.as_ssd())
                    .and_then(|h| h.wal().cloned())
            })
            .collect();
        // Smallest durable position across all DBs. It's safe to flush a
        // pending response only when every WAL this connection may have
        // written to is durable — but since a single connection is pinned
        // to one current_db at a time, taking the min is the conservative
        // shortcut that sidesteps per-conn DB-index bookkeeping at the
        // cost of tiny extra wait when DBs advance unevenly.
        let current_min_durable = |wals: &[Option<Arc<WriteAheadLog>>]| -> u64 {
            wals.iter()
                .filter_map(|w| w.as_ref().map(|w| w.durable_position()))
                .min()
                .unwrap_or(u64::MAX)
        };

        // Wake cadence for the reactor. A plain `wait(1)` would deadlock
        // when every client's response is pending durability and no new
        // I/O is arriving: the WAL commit thread advances `durable_pos`
        // but the reactor sleeps forever on io_uring. Using a bounded
        // wait means we always wake within this budget to poll durability.
        let wake_budget = std::time::Duration::from_micros(200);

        // Per-iteration: block on at least one completion (or the budget),
        // then drain. The closure only captures &mut refs to things we
        // own via move above.
        loop {
            // Bounded wait; returns on any io_uring completion or when the
            // timeout fires.
            if let Err(e) = server.wait_timeout(wake_budget) {
                error!("reactor wait error: {}", e);
                continue;
            }

            let process_result = server.process_completions(|event| match event {
                NetEvent::Accept(fd) => {
                    // Enforce --max-connections at the reactor-level too.
                    let mut cur = live_conns.load(Ordering::Relaxed);
                    let accepted = loop {
                        if cur >= max_conns {
                            break false;
                        }
                        match live_conns.compare_exchange_weak(
                            cur,
                            cur + 1,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => break true,
                            Err(actual) => cur = actual,
                        }
                    };

                    if !accepted {
                        debug!(
                            "reactor: refusing connection (fd={}), at max-connections {}",
                            fd, max_conns
                        );
                        // Reply with a RESP error and let UringServer close
                        // via Data->Close flow. Simpler: return an error
                        // response; queue_send will dispatch then recv will
                        // return EOF shortly.
                        return Some(
                            b"-ERR max number of clients reached\r\n".to_vec(),
                        );
                    }

                    let state = ConnState::new(
                        Arc::clone(&db_manager),
                        router.clone(),
                        replica_read,
                        Arc::clone(&pubsub),
                        tuning,
                    );
                    connections.insert(fd, state);
                    None
                }
                NetEvent::Data(fd, data) => {
                    let state = match connections.get_mut(&fd) {
                        Some(s) => s,
                        None => return None,
                    };

                    if let Err(e) = state.parser.append_bytes(&data) {
                        error!("resp parse error on fd={}: {}", fd, e);
                        return None;
                    }

                    // Drain every complete value. After each command,
                    // capture any WAL position the handler reported via
                    // put_nowait / delete_nowait — that becomes this
                    // connection's pending durability watermark.
                    loop {
                        match state.parser.next_value() {
                            Ok(Some(value)) => {
                                state.handler.handle_command(value, &mut state.pending_out);
                                let pos = state.handler.take_wal_position();
                                if pos > state.pending_up_to {
                                    state.pending_up_to = pos;
                                }
                            }
                            Ok(None) => break, // partial, wait for more
                            Err(e) => {
                                error!("resp parse error on fd={}: {}", fd, e);
                                break;
                            }
                        }
                    }

                    // If nothing to send, nothing to do. If something is
                    // pending but needs durability, hold onto it —
                    // the outer loop flushes when the WAL catches up.
                    if state.pending_out.is_empty() || state.pending_up_to > 0 {
                        None
                    } else {
                        Some(std::mem::take(&mut state.pending_out))
                    }
                }
                NetEvent::Close(fd) => {
                    if connections.remove(&fd).is_some() {
                        live_conns.fetch_sub(1, Ordering::Relaxed);
                    }
                    None
                }
            });

            if let Err(e) = process_result {
                error!("reactor process_completions error: {}", e);
                // continue; a transient error shouldn't kill the server
            }

            // Durability flush: check each connection's pending_up_to
            // against the min durable position. Every WAL byte the
            // connection wrote is now on disk — send the buffered
            // response. This is what pipelines hundreds of writes per
            // fsync: the commit thread advances durable_pos for the
            // whole batch at once, and one reactor iteration flushes
            // every ready response.
            let min_durable = current_min_durable(&wals);
            if min_durable > 0 {
                for (&fd, state) in connections.iter_mut() {
                    if state.pending_up_to > 0
                        && state.pending_up_to <= min_durable
                        && !state.pending_out.is_empty()
                    {
                        let bytes = std::mem::take(&mut state.pending_out);
                        state.pending_up_to = 0;
                        if let Err(e) = server.queue_send(fd, bytes) {
                            error!("reactor queue_send error on fd={}: {}", fd, e);
                        }
                    } else if state.pending_up_to > 0
                        && state.pending_up_to <= min_durable
                    {
                        // pending_out is empty but we were waiting — no-op
                        // apart from clearing the watermark.
                        state.pending_up_to = 0;
                    }
                }
            }

            // Submit any queued SQEs (sends etc.)
            if let Err(e) = server.submit() {
                error!("reactor submit error: {}", e);
            }
        }
    }
}

/// Start the reactor on its own OS thread so main can keep control flow.
pub fn start_reactor_server(
    addr: SocketAddr,
    db_manager: Arc<DatabaseManager>,
    tuning: ServerTuning,
) -> std::thread::JoinHandle<()> {
    start_reactor_multi(addr, db_manager, None, false, tuning, 1)
        .into_iter()
        .next()
        .expect("at least one reactor thread")
}

pub fn start_reactor_server_clustered(
    addr: SocketAddr,
    db_manager: Arc<DatabaseManager>,
    router: Arc<ClusterRouter>,
    replica_read: bool,
    tuning: ServerTuning,
) -> std::thread::JoinHandle<()> {
    start_reactor_multi(addr, db_manager, Some(router), replica_read, tuning, 1)
        .into_iter()
        .next()
        .expect("at least one reactor thread")
}

/// Start `n` reactor threads, each binding the same port via SO_REUSEPORT
/// when n > 1. The kernel distributes incoming connections across them.
/// Returns the spawned JoinHandles (never joined — reactors run until
/// process exit).
pub fn start_reactor_multi(
    addr: SocketAddr,
    db_manager: Arc<DatabaseManager>,
    router: Option<Arc<ClusterRouter>>,
    replica_read: bool,
    tuning: ServerTuning,
    n: usize,
) -> Vec<std::thread::JoinHandle<()>> {
    let n = n.max(1);
    let reuse_port = n > 1;
    (0..n)
        .map(|i| {
            let db_manager = Arc::clone(&db_manager);
            let router = router.clone();
            std::thread::Builder::new()
                .name(format!("resp-reactor-{}", i))
                .spawn(move || {
                    let server = if let Some(r) = router {
                        ReactorServer::with_router(addr, db_manager, r, replica_read, tuning)
                    } else {
                        ReactorServer::new(addr, db_manager, tuning)
                    };
                    if let Err(e) = server.run_with_reuseport(reuse_port) {
                        error!("reactor {} fatal: {}", i, e);
                    }
                })
                .expect("failed to spawn reactor thread")
        })
        .collect()
}
