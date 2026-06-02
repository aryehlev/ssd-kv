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
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tracing::{debug, error, info};

use crate::cluster::router::ClusterRouter;
use crate::engine::index_entry::RecordLocation;
use crate::io::uring_net::{NetEvent, UringServer};
use crate::server::db_manager::{DatabaseManager, DbHandler};
use crate::server::handler::GetResult;
use crate::server::redis::{PubSubManager, RespValue, RedisHandler, RespParser, ServerTuning};
use crate::storage::ipage::{IPage, LargePage, PAGE_SIZE};
use crate::storage::wal::WriteAheadLog;

/// An in-flight async disk read for a GET command.
struct PendingRead {
    req_id: u64,
    loc: RecordLocation,
    /// Byte offset into `pending_out` at the time the read was submitted.
    /// When the completion arrives the response is appended after this offset.
    output_offset: usize,
    started: std::time::Instant,
}

/// A disk read that needs to be submitted after `process_completions` returns.
/// We can't call `server.submit_disk_read` inside the closure (double &mut borrow),
/// so we collect them here and submit in a second pass.
struct PendingDiskSubmit {
    file_fd: RawFd,
    offset: u64,
    size: usize,
    conn_fd: RawFd,
    req_id: u64,
    loc: RecordLocation,
    out_offset: usize,
}

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
    /// The specific WAL shard this connection's pending write was sent
    /// to. We watch only this WAL's durable_position() rather than
    /// taking a global minimum across all DBs — idle DBs (never written
    /// to) have durable_position() == 0 forever and would stall all
    /// responses if included in a global min.
    pending_wal: Option<Arc<WriteAheadLog>>,
    /// Timestamp of when `pending_up_to` was set. Measured at the point
    /// the SET/DEL command completed its synchronous work and the
    /// response got buffered awaiting durability. Used to record total
    /// latency (including the fsync wait) into the SSD handler's
    /// `set_latency` histogram when the response finally flushes.
    pending_since: Option<std::time::Instant>,
    /// True when a disk read is in-flight for this connection. While set,
    /// recv is suppressed so pipelined commands are not processed out of
    /// order. Re-armed when the DiskRead completion arrives.
    disk_read_inflight: bool,
    /// Monotonically increasing per-connection request ID for disk reads.
    next_req_id: u64,
    /// Queue of pending disk reads (ordered by submission).
    pending_reads: VecDeque<PendingRead>,
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
            pending_wal: None,
            pending_since: None,
            disk_read_inflight: false,
            next_req_id: 0,
            pending_reads: VecDeque::new(),
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
    /// Which WAL shard this reactor's connections should write to.
    /// Each reactor owns one shard end-to-end: accept → shard_hint
    /// on ConnState → writes hit that shard's WAL → durability wait
    /// polls that shard's durable_pos only. Default 0 keeps
    /// single-reactor deployments unchanged.
    reactor_id: usize,
}

impl ReactorServer {
    pub fn new(
        addr: SocketAddr,
        db_manager: Arc<DatabaseManager>,
        tuning: ServerTuning,
    ) -> Self {
        Self::new_with_reactor_id(addr, db_manager, tuning, 0)
    }

    pub fn new_with_reactor_id(
        addr: SocketAddr,
        db_manager: Arc<DatabaseManager>,
        tuning: ServerTuning,
        reactor_id: usize,
    ) -> Self {
        Self {
            addr,
            db_manager,
            router: None,
            replica_read: false,
            pubsub: Arc::new(PubSubManager::new()),
            tuning,
            live_conns: Arc::new(AtomicUsize::new(0)),
            reactor_id,
        }
    }

    pub fn with_router(
        addr: SocketAddr,
        db_manager: Arc<DatabaseManager>,
        router: Arc<ClusterRouter>,
        replica_read: bool,
        tuning: ServerTuning,
    ) -> Self {
        Self::with_router_and_reactor_id(addr, db_manager, router, replica_read, tuning, 0)
    }

    pub fn with_router_and_reactor_id(
        addr: SocketAddr,
        db_manager: Arc<DatabaseManager>,
        router: Arc<ClusterRouter>,
        replica_read: bool,
        tuning: ServerTuning,
        reactor_id: usize,
    ) -> Self {
        Self {
            addr,
            db_manager,
            router: Some(router),
            replica_read,
            pubsub: Arc::new(PubSubManager::new()),
            tuning,
            live_conns: Arc::new(AtomicUsize::new(0)),
            reactor_id,
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

        // Snapshot each SSD DB's WAL SHARD belonging to this reactor so
        // we can poll its `durable_position()` for durability-pending
        // responses. Each reactor watches exactly one shard per DB; if
        // another reactor's shard advances, that's irrelevant to us.
        // This is the core of parallel WAL shards: N reactors, N
        // independent fsync pipelines, zero cross-reactor coordination.
        //
        // If a DB has fewer shards than reactors (extra reactor threads
        // vs configured shards), `wal_for` clamps to the last shard —
        // degraded but correct: those reactors share a shard.
        let my_shard = self.reactor_id;
        let wals: Vec<Option<Arc<WriteAheadLog>>> = (0..db_manager.num_dbs())
            .map(|i| {
                db_manager
                    .db(i)
                    .and_then(|d| d.as_ssd())
                    .and_then(|h| {
                        let shards = h.wal_shards();
                        if shards.is_empty() {
                            None
                        } else {
                            Some(Arc::clone(&shards[my_shard.min(shards.len() - 1)]))
                        }
                    })
            })
            .collect();

        // Snapshot DB 0's stats for recording per-write latency on the
        // reactor flush path. Multi-DB deployments record into DB 0
        // unconditionally — per-DB histograms would need connection-to-
        // DB tracking, and the dominant use case has 1 DB anyway.
        let latency_sink: Option<Arc<crate::server::handler::HandlerStats>> = db_manager
            .db(0)
            .and_then(|d| d.as_ssd())
            .map(|h| h.stats());

        // Create a wake eventfd and register it with every WAL. The WAL
        // commit thread writes to this fd every time durable_pos
        // advances; our io_uring has a persistent read on it so we wake
        // immediately and can flush any now-durable responses out. The
        // `wait_timeout` below remains as a safety net.
        //
        // On Linux-only — eventfd isn't available elsewhere, and the
        // reactor is already Linux-only via io_uring anyway.
        let wake_fd = unsafe {
            libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK)
        };
        if wake_fd >= 0 {
            for w in wals.iter().flatten() {
                w.register_wake_eventfd(wake_fd);
            }
            if let Err(e) = server.watch_eventfd(wake_fd) {
                tracing::warn!("Could not arm eventfd wake: {e}; falling back to poll");
                for w in wals.iter().flatten() {
                    w.unregister_wake_eventfd(wake_fd);
                }
                unsafe {
                    libc::close(wake_fd);
                }
            }
        }
        // Per-iteration: block on at least one completion (or the budget),
        // then drain. The closure only captures &mut refs to things we
        // own via move above.
        //
        // Adaptive wake budget: when connections are waiting for WAL
        // durability before we can send their response, we use a short
        // 200µs timeout so responses go out quickly after the fsync.
        // When no durability is pending (pure idle or GET-only traffic)
        // we use a much longer 5ms timeout — the eventfd from the WAL
        // commit thread will interrupt us early if needed anyway.
        let wake_budget_active  = std::time::Duration::from_micros(200);
        let wake_budget_idle    = std::time::Duration::from_millis(5);

        // Disk reads collected during process_completions that need to be
        // submitted after the closure releases its &mut server borrow.
        let mut pending_disk_submits: Vec<PendingDiskSubmit> = Vec::new();

        loop {
            let has_inflight = connections.values().any(|s| s.pending_up_to > 0 || s.disk_read_inflight);
            let wake_budget = if has_inflight { wake_budget_active } else { wake_budget_idle };
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
                    // Pin this connection to our reactor's WAL shard.
                    // Every write on this connection goes through the
                    // same shard, so writers never cross reactor
                    // boundaries and each shard's commit thread owns
                    // one reactor's worth of load.
                    state.handler.shard_hint.set(my_shard);
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
                    //
                    // GET intercept: if a parsed command is a simple GET on
                    // an SSD-backed DB, we attempt to serve from staging RAM
                    // (O(1) no-lock). On a staging miss we submit an async
                    // io_uring read — the response is delivered in the
                    // DiskRead completion arm below.  While a disk read is
                    // in flight we stop draining this connection's recv
                    // buffer so responses go out in order.
                    loop {
                        if state.disk_read_inflight {
                            break; // wait for DiskRead completion
                        }
                        match state.parser.next_value() {
                            Ok(Some(value)) => {
                                // Try async GET intercept for SSD-backed DBs.
                                let ssd_handler = state.handler.current_handler_ssd();
                                if let (Some(key), Some(h)) =
                                    (peek_disk_get(&value), ssd_handler)
                                {
                                    match h.try_get_async(key, &mut state.pending_out) {
                                        GetResult::Immediate => {
                                            // Served from staging; response already
                                            // appended to pending_out.
                                        }
                                        GetResult::Miss => {
                                            state.pending_out.extend_from_slice(b"$-1\r\n");
                                        }
                                        GetResult::NeedsDisk { fd: file_fd, offset, size, loc } => {
                                            // Cannot call server.submit_disk_read here
                                            // (double &mut borrow). Collect and submit
                                            // after process_completions returns.
                                            let req_id = state.next_req_id;
                                            state.next_req_id += 1;
                                            let out_offset = state.pending_out.len();
                                            state.disk_read_inflight = true;
                                            state.pending_reads.push_back(PendingRead {
                                                req_id,
                                                loc,
                                                output_offset: out_offset,
                                                started: std::time::Instant::now(),
                                            });
                                            pending_disk_submits.push(PendingDiskSubmit {
                                                file_fd, offset, size,
                                                conn_fd: fd, req_id, loc,
                                                out_offset,
                                            });
                                        }
                                    }
                                } else {
                                    state.handler.handle_command(value, &mut state.pending_out);
                                    let pos = state.handler.take_wal_position();
                                    if pos > state.pending_up_to {
                                        state.pending_up_to = pos;
                                        let db_idx = state.handler.current_db();
                                        state.pending_wal = wals
                                            .get(db_idx)
                                            .and_then(|w| w.as_ref())
                                            .map(Arc::clone);
                                        if state.pending_since.is_none() {
                                            state.pending_since = Some(std::time::Instant::now());
                                        }
                                    }
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
                    // If a disk read is in-flight, also hold — DiskRead
                    // completion will trigger the send.
                    if state.pending_out.is_empty()
                        || state.pending_up_to > 0
                        || state.disk_read_inflight
                    {
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
                NetEvent::DiskRead(dr) => {
                    let conn_fd = dr.conn_fd;
                    let state = match connections.get_mut(&conn_fd) {
                        Some(s) => s,
                        None => {
                            // Connection closed before the read completed;
                            // return buffer to pool via drop (pool is inside
                            // UringServer which handles cleanup).
                            return None;
                        }
                    };

                    // Pop the matching pending read (should be the front).
                    let pending = state.pending_reads.pop_front();
                    state.disk_read_inflight = false;

                    // Decode the buffer into a RESP response.
                    let resp_bytes = if dr.result > 0 {
                        let buf = &dr.buffer;
                        decode_disk_page(buf, &dr.loc)
                    } else {
                        b"$-1\r\n".to_vec()
                    };

                    // Record async GET latency.
                    if let Some(pr) = pending {
                        if let Some(sink) = latency_sink.as_ref() {
                            sink.get_latency.record(pr.started.elapsed().as_micros() as u64);
                            sink.gets.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            if dr.result > 0 {
                                sink.get_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            } else {
                                sink.get_misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                    }

                    // Append to pending_out (in order) and flush if ready.
                    state.pending_out.extend_from_slice(&resp_bytes);

                    // Now try to drain any further commands from the parser
                    // that were queued while the disk read was in-flight.
                    // (single-pass to keep latency bounded; the loop runs again
                    // next iteration via re-armed recv).

                    if state.pending_out.is_empty()
                        || state.pending_up_to > 0
                        || state.disk_read_inflight
                    {
                        None
                    } else {
                        Some(std::mem::take(&mut state.pending_out))
                    }
                }
            });

            if let Err(e) = process_result {
                error!("reactor process_completions error: {}", e);
                // continue; a transient error shouldn't kill the server
            }

            // Submit disk reads collected during process_completions.
            // This is a separate pass because we can't call server.submit_disk_read
            // inside the process_completions closure (double &mut borrow).
            for ds in pending_disk_submits.drain(..) {
                match server.submit_disk_read(ds.file_fd, ds.offset, ds.size, ds.conn_fd, ds.req_id, ds.loc) {
                    Ok(()) => {}
                    Err(e) => {
                        error!("disk read submit failed fd={}: {}", ds.conn_fd, e);
                        // Undo inflight state and emit $-1
                        if let Some(state) = connections.get_mut(&ds.conn_fd) {
                            state.disk_read_inflight = false;
                            state.pending_reads.pop_back();
                            state.pending_out.extend_from_slice(b"$-1\r\n");
                        }
                    }
                }
            }

            // Durability flush: check each connection's pending_up_to
            // against that connection's own WAL's durable_position().
            // Every WAL byte the connection wrote is now on disk — send
            // the buffered response. This is what pipelines hundreds of
            // writes per fsync: the commit thread advances durable_pos
            // for the whole batch at once, and one reactor iteration
            // flushes every ready response.
            //
            // We check per-connection WALs (not a global minimum) so
            // that idle DBs whose durable_position stays at 0 forever
            // do not block connections that only write to active DBs.
            for (&fd, state) in connections.iter_mut() {
                if state.pending_up_to == 0 {
                    continue;
                }
                let conn_durable = state
                    .pending_wal
                    .as_ref()
                    .map_or(u64::MAX, |w| w.durable_position());
                if state.pending_up_to <= conn_durable
                    && !state.pending_out.is_empty()
                {
                    let bytes = std::mem::take(&mut state.pending_out);
                    state.pending_up_to = 0;
                    state.pending_wal = None;
                    // Record the end-to-end durable-write latency for
                    // this batch. `set_latency` is the coarse
                    // aggregate across SET/DEL/writes since we don't
                    // split here — that's fine, the user can read
                    // per-op breakdown from the individual
                    // put_sync/delete_sync paths used by non-reactor
                    // callers.
                    if let (Some(start), Some(sink)) =
                        (state.pending_since.take(), latency_sink.as_ref())
                    {
                        let us = start.elapsed().as_micros() as u64;
                        sink.set_latency.record(us);
                    }
                    if let Err(e) = server.queue_send(fd, bytes) {
                        error!("reactor queue_send error on fd={}: {}", fd, e);
                    }
                } else if state.pending_up_to <= conn_durable {
                    // pending_out is empty but we were waiting — no-op
                    // apart from clearing the watermark.
                    state.pending_up_to = 0;
                    state.pending_wal = None;
                    state.pending_since = None;
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
                        ReactorServer::with_router_and_reactor_id(
                            addr, db_manager, r, replica_read, tuning, i,
                        )
                    } else {
                        ReactorServer::new_with_reactor_id(
                            addr, db_manager, tuning, i,
                        )
                    };
                    if let Err(e) = server.run_with_reuseport(reuse_port) {
                        error!("reactor {} fatal: {}", i, e);
                    }
                })
                .expect("failed to spawn reactor thread")
        })
        .collect()
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// If `value` is a `GET key` command, return the key bytes; otherwise `None`.
/// Used to intercept GET before `handle_command` so we can serve from staging
/// or submit an async disk read instead of blocking.
fn peek_disk_get(value: &RespValue) -> Option<&[u8]> {
    if let RespValue::Array(Some(args)) = value {
        if args.len() == 2 {
            if let RespValue::BulkString(Some(cmd)) = &args[0] {
                if cmd.eq_ignore_ascii_case(b"get") {
                    if let RespValue::BulkString(Some(key)) = &args[1] {
                        return Some(key.as_slice());
                    }
                }
            }
        }
    }
    None
}

/// Decode a raw 4 KB page buffer into a RESP bulk-string response.
fn decode_disk_page(buf: &[u8], loc: &RecordLocation) -> Vec<u8> {
    let bytes = if buf.len() >= PAGE_SIZE {
        &buf[..PAGE_SIZE * loc.span.max(1) as usize]
    } else {
        buf
    };
    if loc.is_large() {
        match LargePage::decode(bytes) {
            Ok(lp) => {
                let pe = lp.into_entry();
                if pe.is_deleted || pe.is_expired() {
                    return b"$-1\r\n".to_vec();
                }
                let vlen = pe.value.len();
                let mut out = Vec::with_capacity(16 + vlen);
                out.push(b'$');
                out.extend_from_slice(itoa::Buffer::new().format(vlen).as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(&pe.value);
                out.extend_from_slice(b"\r\n");
                out
            }
            Err(_) => b"$-1\r\n".to_vec(),
        }
    } else {
        match IPage::from_bytes(bytes) {
            Ok(ipage) => match ipage.read_entry(loc.slot_idx) {
                Some(pe) if !pe.is_deleted && !pe.is_expired() => {
                    let vlen = pe.value.len();
                    let mut out = Vec::with_capacity(16 + vlen);
                    out.push(b'$');
                    out.extend_from_slice(itoa::Buffer::new().format(vlen).as_bytes());
                    out.extend_from_slice(b"\r\n");
                    out.extend_from_slice(&pe.value);
                    out.extend_from_slice(b"\r\n");
                    out
                }
                _ => b"$-1\r\n".to_vec(),
            },
            Err(_) => b"$-1\r\n".to_vec(),
        }
    }
}
