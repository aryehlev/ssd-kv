//! SSD-KV: High-Performance Key-Value Store

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use parking_lot::RwLock;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

mod cluster;
mod config;
mod engine;
mod io;
mod perf;
mod server;
mod storage;

use cluster::node::NodeInfo;
use cluster::peer_pool::PeerConnectionPool;
use cluster::replication::{PeerServer, ReplicationManager};
use cluster::router::ClusterRouter;
use cluster::topology::ClusterTopology;
use cluster::health::{HealthChecker, HealthConfig};
use config::Config;
use engine::{recover_index, recover_with_wal, Index};
use perf::PerfTuning;
use server::{Handler, DatabaseManager, DbHandler, start_redis_server, start_redis_server_clustered, ServerTuning};
use storage::compaction::{start_compaction_thread, CompactionConfig};
use storage::eviction::{start_eviction_thread, EvictionConfig, EvictionPolicy};
use storage::file_manager::FileManager;
use storage::memory_store::MemoryStore;
use storage::wal::{WalConfig, WriteAheadLog};
use storage::wblock_cache::WblockCache;
use storage::write_buffer::WriteBuffer;

/// Use mimalloc as the global allocator for better performance.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command line arguments
    let config = Config::parse();

    // Validate configuration
    config.validate()?;

    // Initialize logging
    let log_level = if config.verbose {
        "debug"
    } else {
        &config.log_level
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(log_level)),
        )
        .init();

    info!("SSD-KV starting...");
    info!("Data directory: {:?}", config.data_dir);
    info!("Bind address: {}", config.bind);
    if config.cluster_mode {
        info!(
            "Cluster mode: node_id={}, total_nodes={}, replication_factor={}",
            config.node_id.unwrap(),
            config.total_nodes.unwrap(),
            config.replication_factor
        );
    }

    // Create data directory
    std::fs::create_dir_all(&config.data_dir)?;

    // Auto-tune performance settings
    let tuning = PerfTuning::auto_tune();
    info!("Performance tuning: {:?}", tuning);

    // CPU pinning is delegated to the kubelet's static CPU manager when
    // running under the operator (Guaranteed QoS + integer CPU). For non-K8s
    // deployments use taskset(1) or cgroups; pinning only the main thread
    // here had no real effect since it spends its life parked on a signal.

    let eviction_policy = EvictionPolicy::from_str(&config.eviction_policy);

    // Optional wblock read cache, shared across SSD-backed DBs. 0 disables it;
    // without the cache every GET does a 1 MiB SSD pread.
    let wblock_cache = if config.wblock_cache_mb > 0 {
        let c = WblockCache::new(config.wblock_cache_mb);
        info!("Wblock cache enabled: {} MiB", config.wblock_cache_mb);
        Some(c)
    } else {
        None
    };

    // Create all databases
    info!("Initializing {} databases...", config.num_dbs);
    let mut db_handlers = Vec::with_capacity(config.num_dbs as usize);
    let mut compaction_stops: Vec<Arc<AtomicBool>> = Vec::new();
    let mut eviction_stops: Vec<Arc<AtomicBool>> = Vec::new();

    for db_id in 0..config.num_dbs {
        if config.is_memory_db(db_id) {
            // Memory-only DB
            let mut store = MemoryStore::new();
            store.set_eviction_config(eviction_policy, config.max_entries, config.max_data_mb);
            db_handlers.push(DbHandler::Memory(Arc::new(store)));
            info!("DB {}: memory-only", db_id);
        } else {
            // SSD-backed DB
            let db_data_dir = if config.num_dbs == 1 {
                config.data_dir.clone() // backward compat: use data_dir directly
            } else {
                config.data_dir.join(format!("db{}", db_id))
            };
            std::fs::create_dir_all(&db_data_dir)?;

            let fm = Arc::new(FileManager::new(&db_data_dir)?);
            let idx = Arc::new(Index::new());
            let wb = Arc::new(WriteBuffer::new(fm.file_count() as u32, config.wblocks_per_file));

            // Group-commit WAL for this DB. Every durable write goes here
            // before we ack to the client; recovery replays it on startup.
            let wal_dir = db_data_dir.join("wal");
            let wal = Arc::new(WriteAheadLog::new(WalConfig {
                dir: wal_dir,
                fsync_interval: std::time::Duration::from_micros(config.fsync_interval_us),
                fsync_batch: config.fsync_batch,
                ..Default::default()
            })?);

            if fm.file_count() == 0 {
                fm.create_file()?;
            }

            // Start compaction thread for this DB
            if !config.no_compaction {
                let compaction_config = CompactionConfig {
                    utilization_threshold: config.compaction_threshold,
                    check_interval_secs: config.compaction_interval,
                    ..Default::default()
                };
                let (_compactor, stop) = start_compaction_thread(
                    compaction_config,
                    Arc::clone(&idx),
                    Arc::clone(&fm),
                    Arc::clone(&wb),
                );
                compaction_stops.push(stop);
            }

            let mut handler_inner = Handler::new(
                Arc::clone(&idx),
                Arc::clone(&fm),
                Arc::clone(&wb),
            );
            handler_inner.set_eviction_config(eviction_policy, config.max_entries, config.max_data_mb);
            if let Some(cache) = &wblock_cache {
                handler_inner.set_wblock_cache(Arc::clone(cache));
            }

            // Recover: scan data files, then replay WAL for records that were
            // ack'd but not yet flushed. Done BEFORE wiring the WAL into the
            // handler so replayed records don't re-append to the log.
            let recovery_stats = recover_with_wal(&handler_inner, &fm, &wal)?;
            info!(
                "DB {}: SSD, recovered {} records ({} expired, {} deleted, {} from WAL, max gen {})",
                db_id,
                recovery_stats.records_indexed,
                recovery_stats.records_expired,
                recovery_stats.records_deleted,
                recovery_stats.wal_entries_replayed,
                recovery_stats.max_generation,
            );

            // From here on, every put_sync/delete_sync is durable.
            handler_inner.set_wal(Arc::clone(&wal));

            let handler = Arc::new(handler_inner);

            // Start eviction thread for this SSD DB
            if config.eviction_policy != "noeviction"
                || config.max_entries > 0
                || config.max_data_mb > 0
            {
                let eviction_config = EvictionConfig {
                    policy: eviction_policy,
                    max_entries: config.max_entries,
                    max_data_mb: config.max_data_mb,
                    check_interval_secs: config.eviction_interval,
                    sample_size: 16,
                };
                let (_evictor, stop) = start_eviction_thread(
                    eviction_config,
                    Arc::clone(&handler),
                    Arc::clone(&idx),
                );
                eviction_stops.push(stop);
            }

            db_handlers.push(DbHandler::Ssd(handler));
        }
    }

    let db_manager = Arc::new(DatabaseManager::new(db_handlers));
    info!("All databases initialized");

    let tuning = ServerTuning {
        read_buf_bytes: config.read_buffer_size(),
        write_buf_bytes: config.write_buffer_size(),
        max_connections: config.max_connections,
    };
    info!(
        "Server tuning: read_buf={}KB write_buf={}KB max_conns={}",
        tuning.read_buf_bytes / 1024,
        tuning.write_buf_bytes / 1024,
        tuning.max_connections,
    );

    // Get DB 0 handler for cluster components that need Arc<Handler>
    let primary_handler = db_manager.db(0).unwrap().as_ssd().cloned();

    // Initialize cluster components or standalone
    let _health_stop = if config.cluster_mode {
        let node_id = config.node_id.unwrap();
        let total_nodes = config.total_nodes.unwrap();
        let bind_ip = config.bind.ip();

        // Build node list
        let mut nodes = Vec::new();
        let peer_addrs: Vec<String> = config
            .cluster_peers
            .as_deref()
            .unwrap_or("")
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();

        for i in 0..total_nodes {
            let redis_port = config.bind.port();
            let cluster_port = config.cluster_port;

            let (redis_addr, cluster_addr) = if i as usize >= peer_addrs.len() {
                // No peer address provided, use localhost with offset
                let addr = format!("{}:{}", bind_ip, redis_port);
                let caddr = format!("{}:{}", bind_ip, cluster_port);
                (addr.parse().unwrap(), caddr.parse().unwrap())
            } else {
                // Parse peer address (host:cluster_port)
                let peer = &peer_addrs[i as usize];
                let cluster_addr: std::net::SocketAddr = peer.parse().unwrap_or_else(|_| {
                    format!("{}:{}", bind_ip, cluster_port + i as u16)
                        .parse()
                        .unwrap()
                });
                let redis_addr = format!("{}:{}", cluster_addr.ip(), redis_port)
                    .parse()
                    .unwrap();
                (redis_addr, cluster_addr)
            };

            nodes.push(NodeInfo::new(i, redis_addr, cluster_addr));
        }

        // Create topology
        let topology = Arc::new(RwLock::new(ClusterTopology::new(
            node_id,
            nodes.clone(),
            config.replication_factor,
        )));

        // Create peer connection pool
        let peers = Arc::new(PeerConnectionPool::new());
        for node in &nodes {
            if node.id != node_id {
                peers.add_peer(node.id, node.cluster_addr);
            }
        }

        // In cluster mode, DB 0 must be SSD-backed
        let cluster_handler = primary_handler.clone()
            .expect("DB 0 must be SSD-backed in cluster mode");

        // Create router
        let router = Arc::new(ClusterRouter::new(
            Arc::clone(&topology),
            Arc::clone(&cluster_handler),
            Arc::clone(&peers),
        ));

        // Create replication manager
        let replication = Arc::new(ReplicationManager::new(
            Arc::clone(&topology),
            Arc::clone(&cluster_handler),
            Arc::clone(&peers),
        ));

        // Start peer server
        let peer_addr = format!("0.0.0.0:{}", config.cluster_port).parse().unwrap();
        let peer_server = Arc::new(PeerServer::new(
            Arc::clone(&cluster_handler),
            Arc::clone(&replication),
            Arc::clone(&topology),
        ));
        tokio::spawn(async move {
            if let Err(e) = peer_server.run(peer_addr).await {
                error!("Peer server error: {}", e);
            }
        });
        info!("Cluster peer server on 0.0.0.0:{}", config.cluster_port);

        // Start health checker
        let health_config = HealthConfig {
            check_interval: Duration::from_millis(config.health_check_interval_ms),
            suspect_threshold: config.health_check_threshold.saturating_sub(1).max(1),
            dead_threshold: config.health_check_threshold,
        };
        let health_checker = Arc::new(HealthChecker::new(
            Arc::clone(&topology),
            Arc::clone(&peers),
            health_config,
        ));
        let health_stop = health_checker.stop_handle();
        let hc = Arc::clone(&health_checker);
        tokio::spawn(async move {
            hc.run().await;
        });
        info!("Health checker started");

        // Start Redis server with cluster routing
        let _redis_handle = start_redis_server_clustered(
            config.bind,
            Arc::clone(&db_manager),
            router,
            config.replica_read,
            tuning,
        );
        info!("Redis-compatible server (clustered) on {}", config.bind);

        // Log shard ownership
        let topo = topology.read();
        let local_shards = topo.shards_for_node(node_id);
        info!(
            "Node {} owns {} shards as primary (topology v{})",
            node_id,
            local_shards.len(),
            topo.current_version()
        );

        Some(health_stop)
    } else {
        // Standalone mode
        let _redis_handle = start_redis_server(config.bind, Arc::clone(&db_manager), tuning);
        info!("Redis-compatible server on {}", config.bind);
        None
    };

    let shutdown_signal = Arc::new(AtomicBool::new(false));

    // Handle shutdown signals
    let db_manager_clone = Arc::clone(&db_manager);
    let shutdown_clone = Arc::clone(&shutdown_signal);
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install CTRL+C handler");
        info!("Shutdown signal received");

        // Flush pending writes
        if let Err(e) = db_manager_clone.flush_all().await {
            error!("Error flushing on shutdown: {}", e);
        }

        shutdown_clone.store(true, Ordering::SeqCst);
    });

    // Wait for shutdown signal
    while !shutdown_signal.load(Ordering::SeqCst) {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // Stop health checker
    if let Some(stop) = _health_stop {
        stop.store(true, Ordering::SeqCst);
    }

    // Stop eviction threads
    for stop in &eviction_stops {
        stop.store(true, Ordering::SeqCst);
    }

    // Stop compaction threads
    for stop in &compaction_stops {
        stop.store(true, Ordering::SeqCst);
    }
    if !compaction_stops.is_empty() {
        info!("Waiting for compaction threads to stop...");
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // Final flush
    info!("Flushing pending writes...");
    db_manager.flush_all().await?;

    info!("SSD-KV shutdown complete");
    Ok(())
}
