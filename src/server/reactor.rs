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
use crate::server::db_manager::DatabaseManager;
use crate::server::redis::{PubSubManager, RedisHandler, RespParser, ServerTuning};

/// Per-connection state. RESP parser, its pending output buffer, and the
/// handler holding this client's SELECT / MULTI / WATCH state.
struct ConnState {
    parser: RespParser,
    handler: RedisHandler,
    /// Responses we've produced but haven't yet handed to UringServer.
    /// process_completions() returns it per Data event.
    pending_out: Vec<u8>,
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
        // Generous queue depth — 4096 SQEs covers many concurrent accepts,
        // recvs and sends in flight simultaneously.
        let queue_depth: u32 = 4096;
        let mut server = UringServer::new(self.addr, queue_depth, self.tuning.read_buf_bytes)?;
        server.start_accept()?;

        info!("Redis reactor listening on {}", self.addr);

        let mut connections: HashMap<RawFd, ConnState> = HashMap::new();

        let db_manager = Arc::clone(&self.db_manager);
        let router = self.router.clone();
        let replica_read = self.replica_read;
        let pubsub = Arc::clone(&self.pubsub);
        let tuning = self.tuning;
        let live_conns = Arc::clone(&self.live_conns);
        let max_conns = self.tuning.max_connections;

        // Per-iteration: block on at least one completion, then drain. The
        // closure only captures &mut refs to things we own via move above.
        loop {
            // Block until something completes. On error, log and continue —
            // the event loop must never exit silently.
            if let Err(e) = server.wait(1) {
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

                    // Drain every complete value.
                    loop {
                        match state.parser.next_value() {
                            Ok(Some(value)) => {
                                state.handler.handle_command(value, &mut state.pending_out);
                            }
                            Ok(None) => break, // partial, wait for more
                            Err(e) => {
                                error!("resp parse error on fd={}: {}", fd, e);
                                break;
                            }
                        }
                    }

                    if state.pending_out.is_empty() {
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
    std::thread::Builder::new()
        .name("resp-reactor".to_string())
        .spawn(move || {
            let server = ReactorServer::new(addr, db_manager, tuning);
            if let Err(e) = server.run() {
                error!("reactor fatal: {}", e);
            }
        })
        .expect("failed to spawn reactor thread")
}

pub fn start_reactor_server_clustered(
    addr: SocketAddr,
    db_manager: Arc<DatabaseManager>,
    router: Arc<ClusterRouter>,
    replica_read: bool,
    tuning: ServerTuning,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("resp-reactor".to_string())
        .spawn(move || {
            let server =
                ReactorServer::with_router(addr, db_manager, router, replica_read, tuning);
            if let Err(e) = server.run() {
                error!("reactor fatal: {}", e);
            }
        })
        .expect("failed to spawn reactor thread")
}
