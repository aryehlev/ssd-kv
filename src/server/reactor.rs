//! Single-threaded io_uring reactor for the RESP server.
//!
//! Uses io_uring for async TCP accept/recv/send. Commands are executed
//! synchronously on the reactor thread; no WAL durability waiting is needed
//! because KvEngine writes are immediately persistent.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tracing::{debug, error, info};

use crate::io::uring_net::{NetEvent, UringServer};
use crate::server::db_manager::DatabaseManager;
use crate::server::redis::{RedisHandler, RespParser, ServerTuning};

struct ConnState {
    parser: RespParser,
    handler: RedisHandler,
}

impl ConnState {
    fn new(db_manager: Arc<DatabaseManager>, _tuning: ServerTuning) -> Self {
        ConnState {
            parser: RespParser::new(_tuning.read_buf_bytes),
            handler: RedisHandler::new(db_manager),
        }
    }
}

pub struct ReactorServer {
    addr: SocketAddr,
    db_manager: Arc<DatabaseManager>,
    tuning: ServerTuning,
    live_conns: Arc<AtomicUsize>,
    reactor_id: usize,
}

impl ReactorServer {
    pub fn new(
        addr: SocketAddr,
        db_manager: Arc<DatabaseManager>,
        tuning: ServerTuning,
    ) -> Self {
        ReactorServer {
            addr,
            db_manager,
            tuning,
            live_conns: Arc::new(AtomicUsize::new(0)),
            reactor_id: 0,
        }
    }

    pub fn new_with_reactor_id(
        addr: SocketAddr,
        db_manager: Arc<DatabaseManager>,
        tuning: ServerTuning,
        reactor_id: usize,
    ) -> Self {
        ReactorServer {
            addr,
            db_manager,
            tuning,
            live_conns: Arc::new(AtomicUsize::new(0)),
            reactor_id,
        }
    }

    pub fn run(&self) -> std::io::Result<()> {
        self.run_with_reuseport(false)
    }

    pub fn run_with_reuseport(&self, reuse_port: bool) -> std::io::Result<()> {
        let queue_depth: u32 = 4096;
        let mut server = UringServer::new_with_options(
            self.addr,
            queue_depth,
            self.tuning.read_buf_bytes,
            reuse_port,
        )?;
        server.start_accept()?;

        info!(
            "SIndex reactor [{}] listening on {}{}",
            self.reactor_id,
            self.addr,
            if reuse_port { " [SO_REUSEPORT]" } else { "" }
        );

        let mut connections: HashMap<RawFd, ConnState> = HashMap::new();
        let db_manager = Arc::clone(&self.db_manager);
        let tuning = self.tuning;
        let live_conns = Arc::clone(&self.live_conns);
        let max_conns = tuning.max_connections;
        let wake_budget = std::time::Duration::from_micros(500);

        loop {
            if let Err(e) = server.wait_timeout(wake_budget) {
                error!("reactor wait error: {}", e);
                continue;
            }

            let process_result = server.process_completions(|event| match event {
                NetEvent::Accept(fd) => {
                    let cur = live_conns.load(Ordering::Relaxed);
                    if cur >= max_conns {
                        debug!("connection limit reached fd={}", fd);
                        unsafe { libc::close(fd) };
                        return None;
                    }
                    live_conns.fetch_add(1, Ordering::Relaxed);
                    connections.insert(fd, ConnState::new(Arc::clone(&db_manager), tuning));
                    debug!("accepted fd={}", fd);
                    None
                }
                NetEvent::Data(fd, data) => {
                    let state = match connections.get_mut(&fd) {
                        Some(s) => s,
                        None => return None,
                    };
                    let mut out = Vec::new();
                    if let Err(e) = state.parser.append_bytes(&data) {
                        error!("parse error fd={}: {}", fd, e);
                        crate::server::redis::RespValue::err(&e.to_string())
                            .serialize_into(&mut out);
                        return if out.is_empty() { None } else { Some(out) };
                    }
                    loop {
                        match state.parser.next_value() {
                            Ok(Some(resp)) => {
                                let args = match resp {
                                    crate::server::redis::RespValue::Array(Some(a)) => a,
                                    other => vec![other],
                                };
                                state.handler.handle_command(&args, &mut out);
                            }
                            Ok(None) => break,
                            Err(e) => {
                                error!("RESP error fd={}: {}", fd, e);
                                crate::server::redis::RespValue::err(&e.to_string())
                                    .serialize_into(&mut out);
                                break;
                            }
                        }
                    }
                    if out.is_empty() { None } else { Some(out) }
                }
                NetEvent::Close(fd) => {
                    if connections.remove(&fd).is_some() {
                        live_conns.fetch_sub(1, Ordering::Relaxed);
                        debug!("closed fd={}", fd);
                    }
                    None
                }
            });

            if let Err(e) = process_result {
                error!("process_completions error: {}", e);
            }
        }
    }
}

/// Start a single reactor server in a new thread.
pub fn start_reactor_server(
    addr: SocketAddr,
    db_manager: Arc<DatabaseManager>,
    tuning: ServerTuning,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let reactor = ReactorServer::new(addr, db_manager, tuning);
        if let Err(e) = reactor.run() {
            error!("reactor error: {}", e);
        }
    })
}

/// Start multiple reactor threads sharing the same port via SO_REUSEPORT.
pub fn start_reactor_multi(
    addr: SocketAddr,
    db_manager: Arc<DatabaseManager>,
    tuning: ServerTuning,
    num_reactors: usize,
) -> Vec<std::thread::JoinHandle<()>> {
    (0..num_reactors)
        .map(|i| {
            let dm = Arc::clone(&db_manager);
            let a = addr;
            let t = tuning;
            std::thread::spawn(move || {
                let reactor = ReactorServer::new_with_reactor_id(a, dm, t, i);
                if let Err(e) = reactor.run_with_reuseport(true) {
                    error!("reactor {} error: {}", i, e);
                }
            })
        })
        .collect()
}
