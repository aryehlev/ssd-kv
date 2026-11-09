//! Node startup and shutdown (DESIGN §6.1, §18.1, §20.1, §22).
//!
//! Startup order (§18.1): resolve the data devices (formatting `{data-dir}/kvd.data` on first
//! start when `auto-format yes`), read the superblock to learn P, the format mode and the
//! restart path, open the engine (substrate checks, metadata, startup superblock), start the
//! meta thread, bind every worker's listeners (so a bad address exits 1 before anything
//! listens), then start the workers at once: each recovers its partitions while already
//! serving connections (`-LOADING` until a partition is ready). A service thread handles
//! SIGTERM/SIGINT, samples `instantaneous_ops_per_sec` and serves `/metrics`.
//!
//! Shutdown (SHUTDOWN, SIGTERM, SIGINT): every worker stops accepting, drains its partitions
//! (a barrier per partition) and exits; then the engine shuts down (snapshot when asked and
//! possible, CLEAN).

use std::os::fd::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;

use kv_storage::engine::{EngineCfg, PageStore, StorageEngine};
use kv_storage::partition::{BgRates, PartitionStore};
use kv_storage::recovery::RecoveryMode;
use kv_storage::superblock::{SB_CLEAN, SB_CLUSTER_MODE, SB_SNAPSHOT_VALID, Superblock};

use crate::config::Config;
use crate::net::listener::{Listener, bind_all};
use crate::runtime::backend::{Backend, PageBackend, partitions_of};
use crate::runtime::worker::{Worker, WorkerInit};
use crate::runtime::xmsg::XMsg;
use crate::shared::NodeShared;

/// Delivers the meta thread's replies to one worker.
type MetaHook = Box<dyn Fn(u16, u64, kv_storage::metastore::MetaDone) + Send>;
/// One worker's partitions, by id.
type OwnedParts = Vec<(u16, PartitionStore)>;

/// A startup failure: one line for the operator, exit code 1 (§22).
#[derive(Debug)]
pub struct StartError(pub String);

impl std::fmt::Display for StartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

fn fail(what: &str, e: impl std::fmt::Display) -> StartError {
    StartError(format!("{what}: {e}"))
}

/// A running node.
pub struct NodeHandle {
    pub shared: Arc<NodeShared>,
    workers: Vec<JoinHandle<()>>,
    service: Option<JoinHandle<()>>,
    engine: Option<PageStore>,
    /// Device files kept open for the rings.
    _devices: Vec<std::fs::File>,
    meta: Option<JoinHandle<()>>,
}

impl NodeHandle {
    /// Waits until the node stopped; returns the process exit code.
    pub fn wait(mut self) -> i32 {
        for w in self.workers.drain(..) {
            let _ = w.join();
        }
        self.shared.workers_exited.store(true, Ordering::Release);
        // cluster mode: the agent's meta client goes too, so the meta thread can end
        if let Some(cs) = self.shared.cluster() {
            cs.release_meta();
        }
        // the service thread notices the shutdown within its sampling period
        self.shared.request_shutdown();
        if let Some(s) = self.service.take() {
            let _ = s.join();
        }
        // the meta thread ends once every partition (its last client) is gone; its queued
        // metadata writes land before the final superblock
        if let Some(m) = self.meta.take() {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while !m.is_finished() && std::time::Instant::now() < deadline {
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            if m.is_finished() {
                let _ = m.join();
            } else {
                tracing::warn!("the meta thread did not stop in time");
            }
        }
        let mut code = 0;
        if let Some(mut e) = self.engine.take() {
            let clean = self.shared.shutdown_snapshot.load(Ordering::Acquire);
            if let Err(err) = e.shutdown(clean) {
                tracing::error!("storage shutdown failed: {err}");
                code = 1;
            }
            drop(e);
        }
        tracing::info!("kvd stopped");
        code
    }
}

/// The restart facts read from device 0's superblocks before the engine opens.
struct Format {
    partitions: u16,
    cluster: bool,
    clean: bool,
    snapshot: bool,
}

/// Reads the newest valid superblock of `path` (both A/B copies).
pub fn read_superblock(path: &Path) -> std::io::Result<Option<Superblock>> {
    use std::os::unix::fs::FileExt;
    let f = std::fs::File::open(path)?;
    let mut a = vec![0u8; kv_storage::layout::PAGE];
    let mut b = vec![0u8; kv_storage::layout::PAGE];
    let pa = kv_storage::layout::SB_PAGE[0] * kv_storage::layout::PAGE as u64;
    let pb = kv_storage::layout::SB_PAGE[1] * kv_storage::layout::PAGE as u64;
    if f.read_exact_at(&mut a, pa).is_err() || f.read_exact_at(&mut b, pb).is_err() {
        return Ok(None);
    }
    Ok(Superblock::pick(&a, &b).map(|(_, _, sb)| sb))
}

fn read_format(path: &Path) -> Result<Format, StartError> {
    let sb = read_superblock(path)
        .map_err(|e| fail(&format!("cannot read {}", path.display()), e))?
        .ok_or_else(|| StartError(format!("{} holds no kvssd format (run `kvd format`)", path.display())))?;
    Ok(Format {
        partitions: sb.partitions,
        cluster: sb.flags & SB_CLUSTER_MODE != 0,
        clean: sb.flags & SB_CLEAN != 0,
        snapshot: sb.flags & SB_CLEAN != 0 && sb.flags & SB_SNAPSHOT_VALID != 0,
    })
}

/// The data devices, formatting `{data-dir}/kvd.data` on first start (§20.1).
fn prepare_devices(cfg: &Config) -> Result<Vec<PathBuf>, StartError> {
    let (devs, auto_format) = cfg.data_devices();
    if auto_format {
        let (path, size) = &devs[0];
        if !path.exists() {
            if let Some(dir) = path.parent() {
                std::fs::create_dir_all(dir).map_err(|e| fail(&format!("cannot create {}", dir.display()), e))?;
            }
            let partitions = format_partitions_for(cfg, size.unwrap_or(0))?;
            tracing::info!(path = %path.display(), bytes = size.unwrap_or(0), partitions, "formatting a new data file");
            let opts = kv_storage::format::FormatOpts {
                devices: devs.clone(),
                partitions,
                cluster: cfg.cluster_enabled,
                snapshot_region: cfg.snapshot_region,
            };
            kv_storage::format::format(&opts).map_err(|e| fail("format", e))?;
        }
    }
    Ok(devs.into_iter().map(|(p, _)| p).collect())
}

/// The partition count of a new data file of `bytes`: `format-partitions`, or the largest
/// smaller count whose partitions all get their first two allocation groups under
/// `max-storage-fill` (`kv_storage::engine::check_capacity`). With too many partitions for the
/// file, writes got `-OOM` with the file almost empty (64 partitions on 1 GB: about 20 MB
/// stored). A file too small even for 16 partitions is refused.
fn format_partitions_for(cfg: &Config, bytes: u64) -> Result<u16, StartError> {
    let pages = bytes / kv_storage::layout::PAGE as u64;
    let Ok(layout) = kv_storage::layout::DeviceLayout::compute(pages, true, 0) else {
        // the format reports a size it cannot lay out
        return Ok(cfg.format_partitions);
    };
    let fits = |p: u16| kv_storage::engine::check_capacity(std::slice::from_ref(&layout), p, cfg.max_storage_fill);
    if fits(cfg.format_partitions).is_ok() {
        return Ok(cfg.format_partitions);
    }
    match [64u16, 32, 16].into_iter().filter(|&p| p < cfg.format_partitions).find(|&p| fits(p).is_ok()) {
        Some(p) => {
            tracing::warn!(requested = cfg.format_partitions, partitions = p, ags = layout.n_ags, "the data file is too small for format-partitions: formatting with fewer partitions");
            Ok(p)
        }
        None => Err(fail("format", fits(16.min(cfg.format_partitions)).expect_err("does not fit"))),
    }
}

/// Refuses data devices whose allocation groups cannot give each of the `partitions` its
/// first SMALL and EXTENT AG under `max-storage-fill` (formatted elsewhere, or with a lower
/// fill limit): the node would answer `-OOM` with the devices almost empty.
fn check_capacity(paths: &[PathBuf], partitions: u16, max_fill: f32) -> Result<(), StartError> {
    let mut layouts = Vec::with_capacity(paths.len());
    for p in paths {
        let sb = read_superblock(p).map_err(|e| fail(&format!("cannot read {}", p.display()), e))?.ok_or_else(|| StartError(format!("{} holds no kvssd format", p.display())))?;
        layouts.push(sb.layout());
    }
    kv_storage::engine::check_capacity(&layouts, partitions, max_fill).map_err(|e| fail("storage", e))
}

/// `cache-size auto`: 60% of `MemAvailable` minus an index estimate (12.5 B per 300 B record
/// of device space), never more than the devices hold, at least 64 MiB (§6.9).
fn cache_bytes(cfg: &Config, device_bytes: u64) -> usize {
    if let Some(c) = cfg.cache_size {
        return usize::try_from(c).unwrap_or(usize::MAX);
    }
    let avail = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("MemAvailable:")).and_then(|l| l.split_whitespace().nth(1)?.parse::<u64>().ok()))
        .map_or(1 << 30, |kb| kb * 1024);
    let index_estimate = device_bytes / 300 * 25 / 2;
    let want = (avail / 10 * 6).saturating_sub(index_estimate).min(device_bytes).max(64 << 20);
    usize::try_from(want).unwrap_or(usize::MAX)
}

/// Node-total background rates shared out per partition (§19).
fn rates(cfg: &Config, partitions: u16) -> BgRates {
    let per = |v: u64| v.div_ceil(u64::from(partitions.max(1)));
    BgRates {
        scrub_iops: per(cfg.scrub_iops),
        warm_iops: per(cfg.warm_iops),
        expire_reads: per(cfg.active_expire_reads_per_sec),
        reclaim_iops: per(cfg.reclaim_iops),
        purge_iops: per(cfg.purge_iops),
        sync_mbps: cfg.sync_mbps,
        sync_iops: cfg.sync_iops,
    }
}

/// Binds every worker's listeners (`SO_REUSEPORT`, one set per worker, §7.1).
pub fn bind_listeners(cfg: &Config, workers: u16) -> Result<Vec<Vec<Listener>>, StartError> {
    if cfg.port == 0 {
        return Err(StartError("unixsocket listeners are not available yet (M1.3): set a TCP port".to_owned()));
    }
    let all: Vec<Vec<Listener>> =
        (0..workers).map(|_| bind_all(&cfg.bind, cfg.port, cfg.tcp_backlog).map_err(|e| fail("listen", e))).collect::<Result<_, _>>()?;
    for l in all.first().into_iter().flatten() {
        tracing::info!(addr = %l.addr, workers, "listening");
    }
    Ok(all)
}

/// Starts the workers: `make(w)` builds worker `w`'s backend on its own thread (so its memory
/// is first touched there), pinned to a distinct CPU when the system reports enough of them.
pub fn spawn_workers<B, F>(shared: &Arc<NodeShared>, listeners: Vec<Vec<Listener>>, make: F, recovery: RecoveryMode) -> Result<Vec<JoinHandle<()>>, StartError>
where
    B: Backend + 'static,
    F: Fn(u16) -> B + Send + Sync + 'static,
{
    let make = Arc::new(make);
    let cores = core_affinity::get_core_ids().unwrap_or_default();
    let mut handles = Vec::new();
    for (w, ls) in listeners.into_iter().enumerate() {
        let w = w as u16;
        let shared = shared.clone();
        let make = make.clone();
        let core = (cores.len() >= shared.workers.len()).then(|| cores[usize::from(w)]);
        let h = std::thread::Builder::new()
            .name(format!("kvd-w{w}"))
            .spawn(move || {
                if let Some(c) = core {
                    core_affinity::set_for_current(c);
                }
                let backend = make(w);
                match Worker::new(WorkerInit { id: w, shared: shared.clone(), backend, listeners: ls, recovery }) {
                    Ok(mut worker) => worker.run(),
                    Err(e) => {
                        tracing::error!(worker = w, "cannot create the io_uring ring: {e}");
                        shared.request_shutdown();
                    }
                }
            })
            .map_err(|e| fail("cannot start a worker thread", e))?;
        handles.push(h);
    }
    Ok(handles)
}

/// The service thread: signals, `instantaneous_ops_per_sec`, `/metrics` (§6.1, §21).
pub fn spawn_service(shared: &Arc<NodeShared>) -> Result<JoinHandle<()>, StartError> {
    let shared = shared.clone();
    std::thread::Builder::new()
        .name("kvd-service".to_owned())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => {
                    tracing::error!("service runtime: {e}");
                    return;
                }
            };
            rt.block_on(service(shared));
        })
        .map_err(|e| fail("cannot start the service thread", e))
}

async fn service(shared: Arc<NodeShared>) {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).ok();
    let mut int = signal(SignalKind::interrupt()).ok();
    let cfg = shared.config();
    let metrics = if cfg.metrics_port != 0 {
        let host = cfg.bind.first().map_or("127.0.0.1", |b| b.trim_start_matches('-')).to_owned();
        match tokio::net::TcpListener::bind((host.as_str(), cfg.metrics_port)).await {
            Ok(l) => Some(l),
            Err(e) => {
                tracing::error!(port = cfg.metrics_port, "metrics listener: {e}");
                None
            }
        }
    } else {
        None
    };
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(100));
    let mut samples: std::collections::VecDeque<u64> = std::collections::VecDeque::with_capacity(16);
    loop {
        tokio::select! {
            _ = async { match term.as_mut() { Some(s) => { s.recv().await; } None => std::future::pending().await } } => {
                // a shard primary hands its shard off first (§23.12); a second signal stops now
                if shared.cluster().is_some_and(|cs| cs.begin_shutdown_handoff()) {
                    tracing::info!("SIGTERM: handing the shard off before shutting down");
                } else {
                    tracing::info!("SIGTERM: shutting down");
                    shared.request_shutdown();
                }
            }
            _ = async { match int.as_mut() { Some(s) => { s.recv().await; } None => std::future::pending().await } } => {
                if shared.cluster().is_some_and(|cs| cs.begin_shutdown_handoff()) {
                    tracing::info!("SIGINT: handing the shard off before shutting down");
                } else {
                    tracing::info!("SIGINT: shutting down");
                    shared.request_shutdown();
                }
            }
            conn = async { match metrics.as_ref() { Some(l) => l.accept().await, None => std::future::pending().await } } => {
                if let Ok((sock, _)) = conn {
                    let shared = shared.clone();
                    tokio::spawn(serve_metrics(sock, shared));
                }
            }
            _ = tick.tick() => {
                let total: u64 = shared.worker_counters.iter().map(|w| w.total_commands_processed.load(Ordering::Relaxed)).sum();
                samples.push_back(total);
                if samples.len() > 11 {
                    samples.pop_front();
                }
                if let (Some(first), Some(last)) = (samples.front(), samples.back()) {
                    let secs = (samples.len().saturating_sub(1)) as u64;
                    let ops = ((last - first) * 10).checked_div(secs).unwrap_or(0);
                    shared.ops_per_sec.store(ops, Ordering::Relaxed);
                }
                // stop once the workers drained their queues, or ended (nothing reads a message
                // that arrives in a side queue after that, e.g. a late replication event)
                if shared.shutdown_requested()
                    && (shared.workers_exited.load(Ordering::Acquire) || shared.workers.iter().all(|w| w.side.is_empty()))
                {
                    break;
                }
            }
        }
    }
}

async fn serve_metrics(mut sock: tokio::net::TcpStream, shared: Arc<NodeShared>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut req = [0u8; 1024];
    let _ = sock.read(&mut req).await;
    let mut body = Vec::new();
    crate::metrics::render_prometheus(&shared, &mut body);
    let head = format!("HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
    let _ = sock.write_all(head.as_bytes()).await;
    let _ = sock.write_all(&body).await;
}

/// Cluster mode: the node-wide cluster state, the unassigned gate and the replication
/// transport (its listener is bound here, so a bad address fails startup).
fn start_cluster(shared: &Arc<NodeShared>, cfg: &Config, boot: crate::cluster::BootMeta, meta: kv_storage::metastore::MetaClient) -> Result<(), StartError> {
    use crate::cluster::ownership::{GateFlags, Ownership};
    let partitions = shared.partitions;
    let cs = Arc::new(crate::cluster::ClusterShared::new(boot, meta, cfg.cluster_auth_token.as_bytes().to_vec(), partitions));
    let flags = GateFlags { syncing: cs.syncing.clone(), handoff: cs.handoff.clone() };
    *shared.ownership.write() = Arc::new(Ownership::unassigned(partitions, Some(flags)));
    shared.set_cluster(cs.clone());
    let addr = std::net::SocketAddr::new(crate::cluster::bind_ip(cfg), crate::cluster::repl_port(cfg));
    let net = crate::cluster::net::ReplNet::start(shared, addr).map_err(|e| fail(&format!("replication port {addr}"), e))?;
    tracing::info!(addr = %net.listen_addr, "replication port listening");
    let _ = cs.net.set(net);
    Ok(())
}

/// Opens the storage and starts a node serving `cfg` (the `kvd` path).
pub fn start(cfg: Config) -> Result<NodeHandle, StartError> {
    let paths = prepare_devices(&cfg)?;
    let fmt = read_format(&paths[0])?;
    if fmt.cluster != cfg.cluster_enabled {
        return Err(StartError(format!(
            "format mismatch: the devices were formatted for {} mode",
            if fmt.cluster { "cluster" } else { "standalone" }
        )));
    }
    let partitions = fmt.partitions;
    check_capacity(&paths, partitions, cfg.max_storage_fill)?;
    let workers = cfg.worker_count(partitions);
    // cluster mode: the node's own metadata as persisted, read before the engine opens
    let boot_meta = if cfg.cluster_enabled {
        let sb = read_superblock(&paths[0])
            .map_err(|e| fail(&format!("cannot read {}", paths[0].display()), e))?
            .ok_or_else(|| StartError(format!("{} holds no kvssd format", paths[0].display())))?;
        Some(crate::cluster::read_boot_meta(&paths[0], &sb).map_err(|e| fail("node meta", e))?)
    } else {
        None
    };
    let mut device_files = Vec::with_capacity(paths.len());
    let mut device_bytes = 0u64;
    let mut write_through = Vec::with_capacity(paths.len());
    let mut lbas = Vec::with_capacity(paths.len());
    for p in &paths {
        let probe = kv_storage::devprobe::probe(p).map_err(|r| fail(&format!("refused {}", p.display()), r))?;
        lbas.push(probe.lba);
        // the engine's rule (`flush-method none` or a write-through cache: no flushes, §13.8)
        write_through.push(
            cfg.flush_method == kv_storage::engine::FlushMethod::None
                || match cfg.device_cache {
                    kv_storage::engine::DeviceCachePolicy::WriteThrough => true,
                    kv_storage::engine::DeviceCachePolicy::WriteBack => false,
                    kv_storage::engine::DeviceCachePolicy::Auto => !probe.write_cache,
                },
        );
        use std::os::unix::fs::OpenOptionsExt;
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT | libc::O_CLOEXEC)
            .open(p)
            .map_err(|e| fail(&format!("cannot open {}", p.display()), e))?;
        device_bytes += kv_storage::io::device_size(&f).unwrap_or(0);
        device_files.push(f);
    }
    let cache = cache_bytes(&cfg, device_bytes);
    let ecfg = EngineCfg {
        devices: paths.clone(),
        workers,
        cluster: cfg.cluster_enabled,
        durability: cfg.effective_durability(),
        page_update: cfg.page_update,
        device_cache: cfg.device_cache,
        flush_method: cfg.flush_method,
        write_granularity: cfg.write_granularity,
        page_slack_cap: cfg.page_slack_cap,
        page_slack_atomic: cfg.page_slack_atomic,
        max_storage_fill: cfg.max_storage_fill,
        cache_bytes: cache,
        recovery_qd: cfg.recovery_qd,
        rates: rates(&cfg, partitions),
    };
    let (engine, parts) = PageStore::open(&ecfg).map_err(|e| fail("storage", e))?;
    if parts.len() != usize::from(partitions) {
        return Err(StartError(format!("storage opened {} partitions, the superblock names {partitions}", parts.len())));
    }
    let shared = Arc::new(NodeShared::new(cfg.clone(), partitions, workers).map_err(|e| fail("node state", e))?);
    shared.recovery.last_shutdown_clean.store(fmt.clean, Ordering::Relaxed);
    shared.shutdown_snapshot.store(cfg.snapshot_on_shutdown, Ordering::Release);
    shared.recovery.scanned.store(!fmt.snapshot, Ordering::Relaxed);

    // the meta thread owns all fixed-location metadata I/O (§6.1)
    let mut meta_devs: Vec<Box<dyn kv_storage::io::BlockDevice + Send>> = Vec::new();
    for (i, p) in paths.iter().enumerate() {
        let bytes = kv_storage::io::device_size(&device_files[i]).map_err(|e| fail("stat", e))?;
        let mut info = kv_storage::io::DevInfo::new(bytes / kv_storage::layout::PAGE as u64, lbas[i]);
        info.write_through = write_through[i];
        let opts = kv_storage::io::SyncOpen { direct: true, exclusive: false, read_only: false };
        let d = kv_storage::SyncFileDevice::open_with(p, info, opts).map_err(|e| fail("meta device", e))?;
        meta_devs.push(Box::new(d));
    }
    let hooks: Vec<MetaHook> = (0..workers)
        .map(|w| {
            let shared = shared.clone();
            let f: MetaHook = Box::new(move |partition, token, done| shared.workers[usize::from(w)].send_side(XMsg::Meta { partition, token, done }));
            f
        })
        .collect();
    let (meta_client, meta_thread) = kv_storage::metastore::MetaStore::start_with_fill(meta_devs, hooks, cfg.max_storage_fill);
    if let Some(boot) = boot_meta {
        start_cluster(&shared, &cfg, boot, meta_client.clone())?;
    }

    let listeners = bind_listeners(&cfg, workers)?;
    // partitions go to their owner worker (p % W)
    let mut by_worker: Vec<OwnedParts> = (0..workers).map(|_| Vec::new()).collect();
    for (p, part) in parts.into_iter().enumerate() {
        by_worker[p % usize::from(workers)].push((p as u16, part));
    }
    // each worker thread takes its partitions once (so their memory is first touched there)
    let slots: Vec<parking_lot::Mutex<Option<OwnedParts>>> = by_worker.into_iter().map(|v| parking_lot::Mutex::new(Some(v))).collect();
    let fds: Vec<RawFd> = device_files.iter().map(AsRawFd::as_raw_fd).collect();
    let per_worker_cache = cache / usize::from(workers);
    let recovery = if fmt.snapshot { RecoveryMode::Snapshot } else { RecoveryMode::FullScan };
    let txn_ids = meta_client.txn_ids();
    let snapshot = engine.snapshot_writer();
    let make = move |w: u16| {
        let parts = slots[usize::from(w)].lock().take().unwrap_or_default();
        debug_assert!(parts.iter().map(|(p, _)| *p).eq(partitions_of(w, workers, partitions)));
        let flush = write_through.iter().enumerate().map(|(d, &wt)| kv_storage::flush::FlushTracker::new(d as u8, wt)).collect();
        let cache = kv_storage::cache::PageCache::new(per_worker_cache, true);
        PageBackend::new(parts, partitions, cache, flush, meta_client.clone(), fds.clone(), Some(txn_ids.clone()), Some(snapshot.clone()))
    };
    let worker_threads = spawn_workers(&shared, listeners, make, recovery)?;
    let service = spawn_service(&shared)?;
    tracing::info!(port = cfg.port, workers, partitions, "kvd ready to accept connections");
    Ok(NodeHandle {
        shared,
        workers: worker_threads,
        service: Some(service),
        engine: Some(engine),
        _devices: device_files,
        meta: Some(meta_thread),
    })
}

/// Starts a node over caller-made backends (tests, simulators): no devices are opened.
#[cfg(test)]
pub fn start_with<B, F>(cfg: Config, partitions: u16, make: F) -> Result<NodeHandle, StartError>
where
    B: Backend + 'static,
    F: Fn(u16, &Arc<NodeShared>) -> B + Send + Sync + 'static,
{
    let workers = cfg.worker_count(partitions);
    let shared = Arc::new(NodeShared::new(cfg.clone(), partitions, workers).map_err(|e| fail("node state", e))?);
    let listeners = bind_listeners(&cfg, workers)?;
    let s2 = shared.clone();
    let worker_threads = spawn_workers(&shared, listeners, move |w| make(w, &s2), RecoveryMode::FullScan)?;
    let service = spawn_service(&shared)?;
    Ok(NodeHandle { shared, workers: worker_threads, service: Some(service), engine: None, _devices: Vec::new(), meta: None })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn background_rates_are_shared_per_partition() {
        let cfg = Config::default();
        let r = rates(&cfg, 64);
        assert_eq!(r.scrub_iops, 4000_u64.div_ceil(64));
        assert_eq!(r.expire_reads, 20000_u64.div_ceil(64));
        assert_eq!(r.sync_mbps, 200, "stream rates are per stream");
    }

    #[test]
    fn auto_cache_is_bounded_by_the_devices() {
        let cfg = Config::default();
        assert_eq!(cache_bytes(&cfg, 128 << 20), 128 << 20);
        let c = Config { cache_size: Some(1 << 20), ..Config::default() };
        assert_eq!(cache_bytes(&c, 1 << 40), 1 << 20);
        assert!(cache_bytes(&cfg, 1 << 50) >= 64 << 20);
    }

    #[test]
    fn auto_format_creates_a_formatted_data_file() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config {
            data_dir: dir.path().join("sub"),
            // the smallest file giving 16 partitions their first two allocation groups
            data_size: 600 << 20,
            format_partitions: 16,
            snapshot_region: kv_storage::format::SnapshotRegionSize::Percent(0.0),
            ..Config::default()
        };
        let devs = prepare_devices(&cfg).unwrap();
        assert_eq!(devs, vec![dir.path().join("sub/kvd.data")]);
        let f = read_format(&devs[0]).unwrap();
        assert_eq!((f.partitions, f.cluster, f.clean, f.snapshot), (16, false, true, false));
        // a second start keeps the existing format
        let again = prepare_devices(&cfg).unwrap();
        assert_eq!(again, devs);
        assert!(read_format(&dir.path().join("missing")).is_err());
        check_capacity(&devs, 16, 0.95).expect("16 partitions fit");
        assert!(check_capacity(&devs, 64, 0.95).is_err(), "64 partitions get no AG each");
    }

    /// A new data file too small for `format-partitions` is formatted with fewer partitions
    /// (64 partitions on 1 GB got `-OOM` after about 20 MB); one too small even for 16 is
    /// refused before anything is written.
    #[test]
    fn format_partitions_fit_the_data_file() {
        let cfg = |p: u16| Config { format_partitions: p, max_storage_fill: 0.95, ..Config::default() };
        assert_eq!(format_partitions_for(&cfg(64), 4 << 30).unwrap(), 64, "the default file holds the default count");
        assert_eq!(format_partitions_for(&cfg(128), 4 << 30).unwrap(), 64);
        assert_eq!(format_partitions_for(&cfg(64), 1 << 30).unwrap(), 16);
        assert_eq!(format_partitions_for(&cfg(64), 2 << 30).unwrap(), 32);
        let e = format_partitions_for(&cfg(16), 64 << 20).unwrap_err();
        assert!(e.0.contains("too small for 16 partitions"), "{}", e.0);
    }
}
