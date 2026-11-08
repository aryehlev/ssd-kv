//! The worker tick loop (DESIGN §6.4).
//!
//! 1. Reap completions: accept, recv, send, disk completions (to the owning partition), flush
//!    completions (to the device's flush tracker), doorbell, timeout.
//! 2. For each connection with new bytes and not backpressured: parse complete commands (bounded
//!    by `max-pending-per-conn`; the large-value soft limit and txn credits are checked before a
//!    command is consumed, §6.7) and route each (local partition: submit inline; remote:
//!    `XMsg::Cmd`).
//! 3. Drain inbound queues, one FIFO turn per source.
//! 4. For each owned partition: execute ready requests to completion, then `tick` (page writer,
//!    foreground scrubs, background tasks under static rates).
//! 5. Flush trackers: at most one flush per device in flight.
//! 6. Release replies whose tickets / commits are satisfied, strictly in request order per
//!    connection; one send per connection with all ready bytes.
//! 7. Enter the ring (wait only when nothing is runnable).
//!
//! This file holds the loop, the connection table and the network side; command dispatch and
//! execution (jobs, tickets, transactions, node operations) are in [`super::exec`].

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use kv_common::Now;
use kv_resp::{Limits, ProtoError};
use kv_storage::partition::PartState;
use kv_storage::recovery::{RecoveryMode, RecoveryProgress};

use crate::config::Config;
use crate::dispatch::router::Router;
use crate::metrics::WorkerCounters;
use crate::net::conn::{Conn, Hold, flags};
use crate::net::listener::{Listener, MAX_CLIENTS_REPLY};
use crate::runtime::backend::Backend;
use crate::runtime::exec::ExecState;
use crate::runtime::ring::{Completion, Ring, advance_segments, sys};
use crate::runtime::xmsg::{ConnRef, XMsg};
use crate::shared::NodeShared;

/// Messages taken from one source queue per turn (§6.4 step 3: one FIFO turn per source).
const DRAIN_TURN: usize = 256;
/// Timer of the tick loop while background work is pending (§6.4 step 7).
const BUSY_TIMER_NS: u32 = 1_000_000;
/// Timer while idle: background sweeps, idle-client timeouts and counter publication.
const IDLE_TIMER_NS: u32 = 100_000_000;
/// Timer while replication streams are open and nothing else is pending.
const CLUSTER_TIMER_NS: u32 = 5_000_000;
/// Counter mirrors are published at most this often.
const PUBLISH_EVERY_NS: u64 = 1_000_000;
/// Upper bound of the shutdown drain.
const SHUTDOWN_DRAIN_NS: u64 = 10_000_000_000;

/// Everything a worker starts with.
pub struct WorkerInit<B> {
    pub id: u16,
    pub shared: Arc<NodeShared>,
    pub backend: B,
    pub listeners: Vec<Listener>,
    pub recovery: RecoveryMode,
}

pub struct Worker<B: Backend> {
    pub id: u16,
    pub(crate) ring: Ring,
    pub(crate) shared: Arc<NodeShared>,
    pub(crate) cfg: Arc<Config>,
    cfg_gen: u64,
    pub(crate) router: Router,
    pub(crate) backend: B,
    listeners: Vec<Listener>,
    pub(crate) conns: Vec<Option<Conn>>,
    pub(crate) gens: Vec<u32>,
    free_slots: Vec<u32>,
    pub(crate) parse_q: Vec<u32>,
    pub(crate) send_q: Vec<u32>,
    cq: Vec<Completion>,
    pub(crate) x: ExecState,
    pub(crate) counters: WorkerCounters,
    pub(crate) now: Now,
    last_publish: u64,
    /// Connections held on a node-wide limit (re-checked every millisecond).
    shared_held: Vec<u32>,
    /// The connection [`parse_conn`](Self::parse_conn) took out of the table.
    pub(crate) checked_out: Option<u32>,
    /// Results for the checked-out connection, delivered when it is back.
    pub(crate) deferred: Vec<(crate::runtime::exec::ReplyTo, crate::runtime::xmsg::ReplyBody)>,
    /// Counters changed since the last publication (rate-limited to one per millisecond).
    publish_due: bool,
    recovering: Vec<u16>,
    recovery: RecoveryMode,
    stopping: Option<u64>,
    /// The worker's staged-write memory reached its share of `max-dirty-bytes` (§6.7).
    dirty_full: bool,
    spare_out: Vec<Vec<u8>>,
    io_buf: Vec<(std::os::fd::RawFd, u8, kv_storage::io::IoReq)>,
    /// Cluster mode: replication streams, sync and migration endpoints of the partitions.
    pub(crate) cluster: Option<crate::cluster::repl_glue::ClusterWorker>,
}

/// Token of a connection's ring operations: slot and generation.
#[inline]
fn token(slot: u32, generation: u32) -> u64 {
    (u64::from(generation) << 32) | u64::from(slot)
}

impl<B: Backend> Worker<B> {
    pub fn new(init: WorkerInit<B>) -> std::io::Result<Self> {
        let ring = Ring::new()?;
        let cfg = init.shared.config();
        let cfg_gen = init.shared.cfg_gen.load(Ordering::Acquire);
        let router = Router {
            partitions: init.shared.partitions,
            workers: init.shared.workers.len() as u16,
            me: init.id,
            cluster: cfg.cluster_enabled,
        };
        let recovering = init.backend.owned().to_vec();
        let cluster = match (cfg.cluster_enabled, init.shared.cluster()) {
            (true, Some(cs)) => Some(crate::cluster::repl_glue::ClusterWorker::new(cs, init.backend.owned(), init.shared.partitions, &cfg)),
            _ => None,
        };
        Ok(Worker {
            id: init.id,
            ring,
            shared: init.shared,
            cfg,
            cfg_gen,
            router,
            backend: init.backend,
            listeners: init.listeners,
            conns: Vec::new(),
            gens: Vec::new(),
            free_slots: Vec::new(),
            parse_q: Vec::new(),
            send_q: Vec::new(),
            cq: Vec::new(),
            x: ExecState::new(router.workers),
            counters: WorkerCounters::default(),
            now: Now::read(),
            last_publish: 0,
            publish_due: false,
            checked_out: None,
            shared_held: Vec::new(),
            deferred: Vec::new(),
            recovering,
            recovery: init.recovery,
            stopping: None,
            dirty_full: false,
            spare_out: Vec::new(),
            io_buf: Vec::new(),
            cluster,
        })
    }

    /// Runs the tick loop until shutdown.
    pub fn run(&mut self) {
        if let Err(e) = self.start() {
            tracing::error!(worker = self.id, "worker start failed: {e}");
            self.shared.request_shutdown();
            return;
        }
        loop {
            self.now = Now::read();
            self.refresh_config();
            // 1. completions
            self.reap();
            if self.stopping.is_none() {
                // 2. parse and route
                self.parse_pending();
            }
            // 3. inbound messages
            self.drain_inbound();
            // recovery progress (§18.1)
            self.drive_recovery();
            // 4. execute ready requests, partition ticks
            self.execute_ready();
            self.backend.tick(self.now);
            // changes the ticks committed (expiries) reach the WATCH sessions first
            self.drain_all_touched();
            self.release_executed();
            // 5./6. tickets, replies, flushes, sends
            self.release_tickets();
            if self.cluster.is_some() {
                // streams, walks, acks, committed replies (§23.7, §23.8)
                self.cluster_tick();
            }
            self.flush_outbox();
            self.submit_io();
            self.flush_sends();
            self.publish(false);
            self.recheck_shared_holds();
            if let Some(since) = self.stopping {
                if self.now.mono_ns.saturating_sub(since) > SHUTDOWN_DRAIN_NS {
                    tracing::warn!(worker = self.id, "shutdown drain timed out: the next start takes the full scan");
                    break;
                }
                // drained, then (unless NOSAVE) every partition's snapshot section handed in
                // while the partitions keep ticking their remaining background work (§10.10)
                if self.drained() && (!self.shared.shutdown_snapshot.load(Ordering::Acquire) || self.backend.snapshot_sections(self.now)) {
                    break;
                }
            }
            // 7. enter the ring
            self.enter();
        }
        self.finish();
    }

    fn start(&mut self) -> std::io::Result<()> {
        let efd = self.shared.workers[usize::from(self.id)].eventfd;
        self.ring.arm_doorbell(efd)?;
        for (i, l) in self.listeners.iter().enumerate() {
            self.ring.accept(l.fd, i as u32)?;
        }
        for &p in self.backend.owned() {
            self.shared.set_partition_state(p, PartState::Loading);
        }
        Ok(())
    }

    fn refresh_config(&mut self) {
        let g = self.shared.cfg_gen.load(Ordering::Acquire);
        if g != self.cfg_gen {
            self.cfg_gen = g;
            self.cfg = self.shared.config();
            let limits = self.limits();
            for c in self.conns.iter_mut().flatten() {
                c.parser.set_limits(limits);
                c.parser.set_auth_required(!c.authed && !self.cfg.requirepass.is_empty());
            }
        }
    }

    /// Parser limits from the configuration (§7.2).
    pub(super) fn limits(&self) -> Limits {
        Limits {
            max_bulk: usize::try_from(self.cfg.proto_max_bulk_len).unwrap_or(usize::MAX),
            max_args: Limits::DEFAULT_MAX_ARGS,
            max_inline: Limits::DEFAULT_MAX_INLINE,
        }
    }

    // -----------------------------------------------------------------------------------------
    // 1. completions

    fn reap(&mut self) {
        self.ring.reap(&mut self.cq);
        let mut cq = std::mem::take(&mut self.cq);
        for c in cq.drain(..) {
            match c {
                Completion::Accept { listener, res, more } => self.on_accept(listener, res, more),
                Completion::Recv { token, res, buf } => self.on_recv(token, res, buf),
                Completion::Send { token, res, segs } => self.on_send(token, res, segs),
                Completion::Disk { kind, dev, done } => self.backend.io_done(self.now, kind, dev, done),
                Completion::Doorbell => {
                    let efd = self.shared.workers[usize::from(self.id)].eventfd;
                    if let Err(e) = self.ring.arm_doorbell(efd) {
                        tracing::error!(worker = self.id, "doorbell re-arm failed: {e}");
                    }
                }
                Completion::Timer => self.on_timer(),
            }
        }
        self.cq = cq;
    }

    fn on_accept(&mut self, listener: u32, res: i32, more: bool) {
        if !more
            && self.stopping.is_none()
            && let Some(l) = self.listeners.get(listener as usize)
            && let Err(e) = self.ring.accept(l.fd, listener)
        {
            tracing::error!(worker = self.id, "accept re-arm failed: {e}");
        }
        if res < 0 {
            if res != -libc::ECANCELED {
                tracing::warn!(worker = self.id, errno = -res, "accept failed");
            }
            return;
        }
        let fd = res;
        if self.stopping.is_some() {
            sys::close(fd);
            return;
        }
        // maxclients is enforced at accept (§6.7)
        let n = self.shared.clients.fetch_add(1, Ordering::AcqRel);
        if n >= u64::from(self.cfg.maxclients) {
            self.shared.clients.fetch_sub(1, Ordering::AcqRel);
            sys::send_now(fd, MAX_CLIENTS_REPLY);
            sys::close(fd);
            self.counters.rejected_connections += 1;
            return;
        }
        sys::tune_accepted(fd, self.cfg.tcp_keepalive);
        let id = self.shared.next_client_id();
        let mut conn = Conn::new(id, fd, self.now.boot_ns, self.limits());
        conn.addr = sys::peer_addr(fd);
        conn.laddr = sys::local_addr(fd);
        conn.authed = self.cfg.requirepass.is_empty();
        conn.parser.set_auth_required(!conn.authed);
        let slot = match self.free_slots.pop() {
            Some(s) => s,
            None => {
                self.conns.push(None);
                self.gens.push(0);
                (self.conns.len() - 1) as u32
            }
        };
        conn.generation = self.gens[slot as usize];
        self.conns[slot as usize] = Some(conn);
        self.counters.total_connections_received += 1;
        self.counters.connected_clients += 1;
        self.arm_recv(slot);
    }

    /// The connection in `slot` if `generation` matches.
    #[inline]
    pub(super) fn conn_mut(&mut self, slot: u32, generation: u32) -> Option<&mut Conn> {
        self.conns.get_mut(slot as usize).and_then(Option::as_mut).filter(|c| c.generation == generation)
    }

    /// The reference other workers use to reach `slot`.
    pub(super) fn conn_ref(&self, slot: u32) -> ConnRef {
        ConnRef { worker: self.id, slot, generation: self.gens[slot as usize] }
    }

    fn on_recv(&mut self, tok: u64, res: i32, buf: Vec<u8>) {
        let (slot, generation) = ((tok & 0xffff_ffff) as u32, (tok >> 32) as u32);
        let Some(conn) = self.conn_mut(slot, generation) else { return };
        conn.read_buf = buf;
        conn.recv_inflight = false;
        if res > 0 && !conn.closing() {
            conn.received(res as usize);
            self.counters.total_net_input_bytes += res as u64;
            self.queue_parse(slot);
        } else if res == 0 || (res < 0 && res != -libc::EAGAIN && res != -libc::EINTR) {
            // end of stream, reset or shutdown: nothing more will be read
            conn.flags |= flags::KILLED;
            self.maybe_close(slot);
        } else {
            self.arm_recv(slot);
        }
    }

    fn on_send(&mut self, tok: u64, res: i32, mut segs: Vec<crate::runtime::ring::OutSeg>) {
        let (slot, generation) = ((tok & 0xffff_ffff) as u32, (tok >> 32) as u32);
        let spare = &mut self.spare_out;
        let Some(conn) = self.conns.get_mut(slot as usize).and_then(Option::as_mut).filter(|c| c.generation == generation) else {
            return;
        };
        conn.send_inflight = false;
        if res < 0 {
            conn.flags |= flags::KILLED;
            self.maybe_close(slot);
            return;
        }
        let n = res as usize;
        advance_segments(&mut segs, n, &mut |mut v| {
            if spare.len() < 64 {
                v.clear();
                spare.push(v);
            }
        });
        conn.sent(n, segs);
        self.counters.total_net_output_bytes += n as u64;
        let was_held = conn.hold == Some(Hold::Output);
        let more = conn.has_output();
        if was_held {
            conn.hold = None;
        }
        if more {
            self.queue_send(slot);
        }
        if was_held {
            self.queue_parse(slot);
        }
        self.maybe_close(slot);
    }

    fn on_timer(&mut self) {
        // idle-client timeout (`timeout`, seconds; 0 = never)
        let timeout_ns = self.cfg.timeout.saturating_mul(1_000_000_000);
        if timeout_ns == 0 {
            return;
        }
        let now = self.now.boot_ns;
        let idle: Vec<u32> = self
            .conns
            .iter()
            .enumerate()
            .filter_map(|(i, c)| c.as_ref().map(|c| (i, c)))
            .filter(|(_, c)| c.pending() == 0 && c.multi.is_none() && now.saturating_sub(c.last_cmd) > timeout_ns)
            .map(|(i, _)| i as u32)
            .collect();
        for slot in idle {
            if let Some(c) = self.conns[slot as usize].as_mut() {
                c.flags |= flags::KILLED;
            }
            self.maybe_close(slot);
        }
    }

    // -----------------------------------------------------------------------------------------
    // connection lifecycle

    pub(super) fn queue_parse(&mut self, slot: u32) {
        if let Some(c) = self.conns[slot as usize].as_mut()
            && !c.queued_for_parse
        {
            c.queued_for_parse = true;
            self.parse_q.push(slot);
        }
    }

    pub(super) fn queue_send(&mut self, slot: u32) {
        if let Some(c) = self.conns[slot as usize].as_mut()
            && !c.queued_for_send
        {
            c.queued_for_send = true;
            self.send_q.push(slot);
        }
    }

    /// Re-arms the receive of `slot` unless reading is held, closing, or already armed.
    pub(super) fn arm_recv(&mut self, slot: u32) {
        let Some(c) = self.conns[slot as usize].as_mut() else { return };
        if c.recv_inflight || c.closing() || c.hold.is_some() {
            return;
        }
        let want = c.parser.in_progress(c.unparsed()).and_then(|p| p.bulk_len).map_or(0, |l| l + 2);
        c.prepare_read(want);
        let buf = std::mem::take(&mut c.read_buf);
        let at = c.filled();
        let (fd, tok) = (c.fd, token(slot, c.generation));
        c.recv_inflight = true;
        if let Err(e) = self.ring.recv(fd, tok, buf, at) {
            tracing::error!(worker = self.id, "recv submit failed: {e}");
            if let Some(c) = self.conns[slot as usize].as_mut() {
                c.recv_inflight = false;
                c.flags |= flags::KILLED;
            }
            self.maybe_close(slot);
        }
    }

    /// Closes `slot` once a close was requested, its output is flushed (or it was killed) and no
    /// ring operation references its buffers.
    pub(super) fn maybe_close(&mut self, slot: u32) {
        let Some(c) = self.conns[slot as usize].as_mut() else { return };
        let killed = c.flags & flags::KILLED != 0;
        let quitting = c.flags & flags::CLOSE_AFTER_REPLY != 0;
        if !killed && !(quitting && !c.has_output() && c.pending() == 0) {
            return;
        }
        if c.recv_inflight || c.send_inflight {
            // in-flight operations complete promptly once the socket is shut down
            sys::shutdown(c.fd);
            c.flags |= flags::KILLED;
            return;
        }
        sys::close(c.fd);
        let token = std::mem::take(&mut c.watch_token);
        self.conns[slot as usize] = None;
        self.release_watch(token);
        self.gens[slot as usize] = self.gens[slot as usize].wrapping_add(1);
        self.free_slots.push(slot);
        self.shared.clients.fetch_sub(1, Ordering::AcqRel);
        self.counters.connected_clients = self.counters.connected_clients.saturating_sub(1);
    }

    /// Lifts a hold when its limit no longer applies (the same threshold that set it, §6.7)
    /// and resumes parsing.
    pub(super) fn reconsider(&mut self, slot: u32) {
        let max_pending = self.cfg.max_pending_per_conn as usize;
        let max_out = self.cfg.output_limit();
        let queues_free = self.x.outbox.iter().all(VecDeque::is_empty);
        let Some(c) = self.conns[slot as usize].as_mut() else { return };
        let lift = match c.hold {
            None => false,
            Some(Hold::Pending) => c.pending() < c.reply_window(max_out, max_pending),
            Some(Hold::Output) => c.output_bytes() < max_out,
            Some(Hold::Queue) => queues_free,
            Some(Hold::LargeValue) => self.shared.budget.may_start(),
            // parsed again after `CREDIT_HOLD_NS` too: the pre-consume check refuses it then
            Some(Hold::TxnCredit) => {
                self.shared.txn_credits.load(Ordering::Acquire) > 0
                    || c.credit_hold_since.is_some_and(|t| self.now.boot_ns.saturating_sub(t) >= crate::runtime::exec::CREDIT_HOLD_NS)
            }
            // parsed again: the pre-consume check holds it again while the state lasts
            Some(Hold::Gate) => true,
            Some(Hold::Dirty) => !self.dirty_full,
            // lifted by the transaction when its locks are sent
            Some(Hold::Order) => false,
        };
        if lift {
            c.hold = None;
            self.queue_parse(slot);
        }
    }

    /// Connections held on a node-wide limit (large-value budget, txn credits) are released by
    /// other workers' progress, which sends them nothing: re-check them (at most once per
    /// millisecond while any is held; the worker keeps its timer armed for that).
    fn recheck_shared_holds(&mut self) {
        if self.shared_held.is_empty() {
            return;
        }
        let mut held = std::mem::take(&mut self.shared_held);
        for &slot in &held {
            self.reconsider(slot);
        }
        held.retain(|&s| self.conns.get(s as usize).and_then(Option::as_ref).is_some_and(|c| matches!(c.hold, Some(Hold::LargeValue | Hold::TxnCredit | Hold::Gate))));
        self.shared_held = held;
    }

    // -----------------------------------------------------------------------------------------
    // 2. parsing

    fn parse_pending(&mut self) {
        let mut q = std::mem::take(&mut self.parse_q);
        for slot in q.drain(..) {
            if let Some(c) = self.conns[slot as usize].as_mut() {
                c.queued_for_parse = false;
            } else {
                continue;
            }
            self.parse_conn(slot);
        }
        if self.parse_q.is_empty() {
            self.parse_q = q;
        } else {
            self.parse_q.append(&mut q);
        }
    }

    /// Parses and dispatches every complete command of `slot` that the single limits allow.
    fn parse_conn(&mut self, slot: u32) {
        let Some(mut conn) = self.conns[slot as usize].take() else { return };
        self.checked_out = Some(slot);
        let mut args: Vec<(u32, u32)> = Vec::with_capacity(8);
        let max_pending = self.cfg.max_pending_per_conn as usize;
        let max_out = self.cfg.output_limit();
        loop {
            if conn.closing() || conn.hold.is_some() {
                break;
            }
            if conn.pending() >= conn.reply_window(max_out, max_pending) {
                conn.hold = Some(Hold::Pending);
                break;
            }
            if conn.output_bytes() >= max_out {
                conn.hold = Some(Hold::Output);
                break;
            }
            if self.dirty_full {
                conn.hold = Some(Hold::Dirty);
                break;
            }
            let (start, end) = (conn.consumed, conn.filled());
            if start == end {
                break;
            }
            args.clear();
            let parsed = conn.parser.parse_in_place(&mut conn.read_buf[start..end], &mut args);
            match parsed {
                Ok(kv_resp::Parsed::Empty { consumed }) => conn.consumed += consumed,
                Ok(kv_resp::Parsed::Incomplete) => {
                    if !self.admit_incomplete(&mut conn) {
                        break;
                    }
                    break;
                }
                Ok(kv_resp::Parsed::Command { consumed }) => {
                    let base = start;
                    // pre-consume checks (§6.7): the command stays unconsumed if one fails
                    match self.x.pre_consume(&self.shared, &mut conn, &args, base, self.now.boot_ns) {
                        Ok(lv) => {
                            conn.consumed += consumed;
                            conn.lv_admitted = false;
                            conn.last_cmd = self.now.boot_ns;
                            self.counters.total_commands_processed += 1;
                            self.dispatch(slot, &mut conn, &args, base, lv);
                        }
                        Err(hold) => {
                            conn.hold = Some(hold);
                            break;
                        }
                    }
                }
                Err(e) => {
                    self.protocol_error(&mut conn, e);
                    break;
                }
            }
        }
        if conn.hold.is_some() {
            conn.flags |= flags::BACKPRESSURED;
            if matches!(conn.hold, Some(Hold::LargeValue | Hold::TxnCredit | Hold::Gate)) && !self.shared_held.contains(&slot) {
                self.shared_held.push(slot);
            }
        } else {
            conn.flags &= !flags::BACKPRESSURED;
        }
        self.conns[slot as usize] = Some(conn);
        self.checked_out = None;
        // results produced for this connection while it was out of the table
        while let Some((to, body)) = self.deferred.pop() {
            self.deliver(to, body);
        }
        if self.conns[slot as usize].as_ref().is_some_and(Conn::has_output) {
            self.queue_send(slot);
        }
        self.arm_recv(slot);
        self.maybe_close(slot);
    }

    /// A command still being received: the large-value check before anything is consumed
    /// (§6.9). Returns whether reading may go on.
    fn admit_incomplete(&mut self, conn: &mut Conn) -> bool {
        let Some(ip) = conn.parser.in_progress(conn.unparsed()) else { return true };
        if let Some(len) = ip.bulk_len
            && crate::net::budget::is_large(len)
            && !conn.lv_admitted
        {
            if self.shared.budget.may_start() {
                conn.lv_admitted = true;
            } else {
                conn.hold = Some(Hold::LargeValue);
                self.counters.large_value_backpressured_conns += 1;
                return false;
            }
        }
        true
    }

    fn protocol_error(&mut self, conn: &mut Conn, e: ProtoError) {
        let mut out = Vec::with_capacity(64);
        e.write_reply(&mut out);
        conn.reply_now(crate::runtime::xmsg::ReplyBody::Bytes(out));
        conn.flags |= flags::CLOSE_AFTER_REPLY;
        // nothing after the error is read
        conn.consumed = conn.filled();
        tracing::debug!(worker = self.id, conn = conn.id, "protocol error: {e}");
    }

    // -----------------------------------------------------------------------------------------
    // 3. inbound messages

    fn drain_inbound(&mut self) {
        // messages this worker addressed to itself, in order; the ones they produce wait for
        // the next drain (never handled re-entrantly)
        for _ in 0..self.x.local.len() {
            let Some(m) = self.x.local.pop_front() else { break };
            self.on_message(m);
        }
        let shared = self.shared.clone();
        let me = &shared.workers[usize::from(self.id)];
        while let Some(m) = me.side.pop() {
            self.on_message(m);
        }
        for src in 0..me.inbox.len() {
            for _ in 0..DRAIN_TURN {
                let Some(m) = me.inbox[src].pop() else { break };
                self.on_message(m);
            }
        }
    }

    fn on_message(&mut self, m: XMsg) {
        match m {
            XMsg::Shutdown => self.begin_stop(),
            other => self.handle_message(other),
        }
    }

    fn begin_stop(&mut self) {
        if self.stopping.is_some() {
            return;
        }
        self.stopping = Some(self.now.mono_ns);
        // stop accepting: listeners close; multishot accepts complete with errors
        self.listeners.clear();
        // cluster mode: streams end, replies waiting for the shard commit are answered
        if self.cluster.is_some() {
            self.cluster_stop();
        }
        // commands waiting on the routing gate (a handoff before this stop) are answered now:
        // redirected, or TRYAGAIN, rather than dropped with their connections
        self.x.gate_hold_off = true;
        for slot in 0..self.conns.len() as u32 {
            let held = self.conns[slot as usize].as_ref().is_some_and(|c| c.hold == Some(Hold::Gate));
            if held {
                if let Some(c) = self.conns[slot as usize].as_mut() {
                    c.hold = None;
                }
                self.parse_conn(slot);
            }
        }
        // drain: every partition writes what it executed (§8.2 SHUTDOWN)
        self.start_local_barrier_for_shutdown();
        tracing::info!(worker = self.id, "worker stopping");
    }

    fn drained(&self) -> bool {
        self.x.idle() && self.recovering.is_empty() && self.conns.iter().flatten().all(|c| !c.send_inflight || c.closing())
    }

    fn finish(&mut self) {
        for slot in 0..self.conns.len() {
            if let Some(c) = self.conns[slot].take() {
                sys::shutdown(c.fd);
                sys::close(c.fd);
                self.shared.clients.fetch_sub(1, Ordering::AcqRel);
            }
        }
        self.counters.connected_clients = 0;
        self.publish(true);
    }

    // -----------------------------------------------------------------------------------------
    // recovery

    fn drive_recovery(&mut self) {
        if self.recovering.is_empty() {
            return;
        }
        let fallback = self.shared.recovery_fallback.load(Ordering::Acquire);
        if fallback && self.recovery == RecoveryMode::Snapshot {
            // a snapshot section failed somewhere: every partition takes the full scan (§18.2)
            self.recovery = RecoveryMode::FullScan;
            self.recovering = self.backend.owned().to_vec();
        }
        let mut i = 0;
        while i < self.recovering.len() {
            let p = self.recovering[i];
            match self.backend.recover(p, self.now, self.recovery) {
                RecoveryProgress::Running => i += 1,
                RecoveryProgress::Done { bytes_scanned } => {
                    self.shared.recovery.bytes_scanned.fetch_add(bytes_scanned, Ordering::Relaxed);
                    self.recovering.swap_remove(i);
                    // counts first: DBSIZE is right as soon as the partition serves
                    self.shared.counters[usize::from(p)].publish(self.backend.counters(p));
                    let st = self.backend.state(p);
                    self.shared.set_partition_state(p, if st == PartState::Loading { PartState::Ready } else { st });
                    if self.shared.partitions_recovered.fetch_add(1, Ordering::AcqRel) + 1 == u32::from(self.shared.partitions) {
                        let ms = self.now.mono_ns.saturating_sub(self.shared.start.mono_ns) / 1_000_000;
                        self.shared.recovery.duration_ms.store(ms, Ordering::Relaxed);
                        tracing::info!(ms, "every partition is ready");
                    }
                }
                RecoveryProgress::SnapshotInvalid => {
                    tracing::warn!(partition = p, "snapshot section invalid: falling back to the full scan");
                    self.shared.recovery_fallback.store(true, Ordering::Release);
                    self.shared.recovery.scanned.store(true, Ordering::Relaxed);
                    self.recovery = RecoveryMode::FullScan;
                    self.recovering = self.backend.owned().to_vec();
                    return;
                }
            }
        }
    }

    // -----------------------------------------------------------------------------------------
    // 5./6. output

    fn submit_io(&mut self) {
        self.backend.take_io(&mut self.io_buf);
        for (fd, dev, req) in self.io_buf.drain(..) {
            if let Err(e) = self.ring.disk(fd, dev, req) {
                tracing::error!(worker = self.id, dev, "disk submit failed: {e}");
            }
        }
    }

    fn flush_sends(&mut self) {
        let mut q = std::mem::take(&mut self.send_q);
        for slot in q.drain(..) {
            let spare = self.spare_out.pop().unwrap_or_default();
            let Some(c) = self.conns[slot as usize].as_mut() else { continue };
            c.queued_for_send = false;
            if c.send_inflight || !c.has_output() || c.flags & flags::KILLED != 0 {
                self.spare_out.push(spare);
                continue;
            }
            let segs = c.take_output(spare);
            let (fd, tok) = (c.fd, token(slot, c.generation));
            c.send_inflight = true;
            if let Err(e) = self.ring.send(fd, tok, segs) {
                tracing::error!(worker = self.id, "send submit failed: {e}");
                if let Some(c) = self.conns[slot as usize].as_mut() {
                    c.send_inflight = false;
                    c.flags |= flags::KILLED;
                }
                self.maybe_close(slot);
            }
        }
        if self.send_q.is_empty() {
            self.send_q = q;
        }
    }

    fn publish(&mut self, force: bool) {
        if !force && self.now.mono_ns.saturating_sub(self.last_publish) < PUBLISH_EVERY_NS {
            // published by a later tick; a parking worker arms the timer for it
            self.publish_due = true;
            return;
        }
        self.publish_due = false;
        self.last_publish = self.now.mono_ns;
        let mut c = self.counters;
        c.backpressured_clients = self.conns.iter().flatten().filter(|c| c.hold.is_some()).count() as u64;
        self.shared.worker_counters[usize::from(self.id)].publish(&c);
        let dirty = self.backend.staged_bytes();
        for &p in self.backend.owned() {
            let pc = self.backend.counters(p);
            self.shared.counters[usize::from(p)].publish(pc);
            let st = self.backend.state(p);
            if st != PartState::Loading && self.shared.partition_state(p) != st {
                self.shared.set_partition_state(p, st);
            }
        }
        self.x.flush_latency(&self.shared, self.id);
        // the staged-write limit: one threshold to stop and to resume (§6.7)
        // (with nothing staged the limit never holds: at a limit below one write, e.g.
        // `max-dirty-bytes 0`, connections were held with nothing left to drain, for good)
        let share = self.cfg.max_dirty_bytes / u64::from(self.router.workers.max(1));
        let full = dirty > 0 && dirty >= share;
        if self.dirty_full && !full {
            let held: Vec<u32> = (0..self.conns.len() as u32)
                .filter(|&s| self.conns[s as usize].as_ref().is_some_and(|c| c.hold == Some(Hold::Dirty)))
                .collect();
            self.dirty_full = false;
            for slot in held {
                self.reconsider(slot);
            }
        }
        self.dirty_full = full;
    }

    // -----------------------------------------------------------------------------------------
    // 7. waiting

    fn runnable(&self) -> bool {
        !self.parse_q.is_empty() || !self.send_q.is_empty() || self.x.runnable() || !self.recovering.is_empty()
    }

    fn enter(&mut self) {
        let cluster_busy = self.cluster.as_ref().is_some_and(|c| c.busy());
        let busy = self.backend.busy()
            || cluster_busy
            || !self.recovering.is_empty()
            || self.stopping.is_some()
            || self.x.waiting_on_gate()
            || self.publish_due
            || !self.shared_held.is_empty();
        let timer = if busy {
            Some(BUSY_TIMER_NS)
        } else if self.cluster.as_ref().is_some_and(|c| c.active()) {
            // replication heartbeats, coalesced acks and position persistence
            Some(CLUSTER_TIMER_NS)
        } else if !self.conns.is_empty() && self.cfg.timeout > 0 {
            Some(IDLE_TIMER_NS)
        } else {
            None
        };
        if let Some(ns) = timer
            && let Err(e) = self.ring.arm_timer(ns)
        {
            tracing::error!(worker = self.id, "timer arm failed: {e}");
        }
        if self.runnable() {
            if let Err(e) = self.ring.submit_and_wait(0) {
                tracing::error!(worker = self.id, "io_uring_enter failed: {e}");
            }
            return;
        }
        let handle = &self.shared.workers[usize::from(self.id)];
        if handle.park() {
            let r = self.ring.submit_and_wait(1);
            handle.unpark();
            if let Err(e) = r {
                tracing::error!(worker = self.id, "io_uring_enter failed: {e}");
            }
        } else if let Err(e) = self.ring.submit_and_wait(0) {
            tracing::error!(worker = self.id, "io_uring_enter failed: {e}");
        }
    }

    /// Outbound messages that found their queue full, in FIFO order per destination.
    fn flush_outbox(&mut self) {
        let backlog = self.x.outbox.iter().any(|q| !q.is_empty());
        if !backlog {
            return;
        }
        for w in 0..self.x.outbox.len() {
            while let Some(m) = self.x.outbox[w].pop_front() {
                match self.shared.workers[w].try_send(self.id, m) {
                    Ok(()) => self.counters.xmsg_hops += 1,
                    Err(m) => {
                        self.x.outbox[w].push_front(m);
                        break;
                    }
                }
            }
        }
        // connections held because a destination queue was full resume once it drained (the
        // same threshold, §6.7)
        let held: Vec<u32> = (0..self.conns.len() as u32)
            .filter(|&s| self.conns[s as usize].as_ref().is_some_and(|c| c.hold == Some(Hold::Queue)))
            .collect();
        for slot in held {
            let Some(c) = self.conns[slot as usize].as_ref() else { continue };
            let _ = c;
            if self.x.outbox.iter().all(VecDeque::is_empty) {
                if let Some(c) = self.conns[slot as usize].as_mut() {
                    c.hold = None;
                }
                self.queue_parse(slot);
            }
        }
    }
}

/// An empty per-destination queue set.
pub(super) fn outboxes(workers: u16) -> Vec<VecDeque<XMsg>> {
    (0..workers).map(|_| VecDeque::new()).collect()
}
