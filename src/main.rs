//! SSD-KV: High-Performance Key-Value Store
//!
//! An Aerospike-inspired KV store with:
//! - Index in RAM for fast lookups
//! - Data on SSD with O_DIRECT for consistent latency
//! - io_uring for async I/O
//! - Sharded hashmap index with 256 shards

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use clap::Parser;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

mod config;
mod engine;
mod io;
mod perf;
mod server;
mod storage;

use config::Config;
use engine::{recover_index, Index};
use perf::{HotCache, PerfTuning, pin_to_cpu};
use server::{Handler, Server, ServerConfig, start_redis_server};
use storage::compaction::{start_compaction_thread, CompactionConfig};
use storage::file_manager::FileManager;
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

    // Create data directory
    std::fs::create_dir_all(&config.data_dir)?;

    // Initialize file manager
    let file_manager = Arc::new(FileManager::new(&config.data_dir)?);
    info!("File manager initialized with {} existing files", file_manager.file_count());

    // Initialize index
    let index = Arc::new(Index::new());

    // Initialize write buffer
    let write_buffer = Arc::new(WriteBuffer::new(
        file_manager.file_count() as u32,
        config.wblocks_per_file,
    ));

    // Recover index from existing data files
    info!("Recovering index from data files...");
    let recovery_stats = recover_index(&index, &file_manager)?;
    info!(
        "Recovery complete: {} records indexed, {} expired, {} deleted",
        recovery_stats.records_indexed,
        recovery_stats.records_expired,
        recovery_stats.records_deleted
    );

    // Create initial file if none exist
    if file_manager.file_count() == 0 {
        file_manager.create_file()?;
        info!("Created initial data file");
    }

    // Start compaction thread if enabled
    let compaction_stop = if !config.no_compaction {
        let compaction_config = CompactionConfig {
            utilization_threshold: config.compaction_threshold,
            check_interval_secs: config.compaction_interval,
            ..Default::default()
        };

        let (compactor, stop) = start_compaction_thread(
            compaction_config,
            Arc::clone(&index),
            Arc::clone(&file_manager),
            Arc::clone(&write_buffer),
        );
        info!("Compaction thread started");
        Some(stop)
    } else {
        info!("Compaction disabled");
        None
    };

    // Auto-tune performance settings
    let tuning = PerfTuning::auto_tune();
    info!("Performance tuning: {:?}", tuning);

    // Pin main thread to CPU 0
    let _ = pin_to_cpu(0);

    // Create hot cache
    let hot_cache = Arc::new(HotCache::new());

    // Create request handler with hot cache
    let handler = Arc::new(Handler::with_hot_cache(
        Arc::clone(&index),
        Arc::clone(&file_manager),
        Arc::clone(&write_buffer),
        hot_cache,
    ));

    // Configure server with optimized settings
    let server_config = ServerConfig {
        bind_addr: config.bind,
        max_connections: config.max_connections,
        read_buffer_size: config.read_buffer_size(),
        write_buffer_size: config.write_buffer_size(),
        num_workers: config.num_workers(),
    };

    // Create and start TCP server
    let server = Arc::new(Server::new(server_config, Arc::clone(&handler)));

    // Start Redis-compatible server on port + 1
    let redis_addr: std::net::SocketAddr = format!(
        "{}:{}",
        config.bind.ip(),
        config.bind.port() + 1
    ).parse().unwrap();
    let _redis_handle = start_redis_server(redis_addr, Arc::clone(&handler));
    info!("Redis-compatible server on {}", redis_addr);

    // Handle shutdown signals
    let server_clone = Arc::clone(&server);
    let handler_clone = Arc::clone(&handler);
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install CTRL+C handler");
        info!("Shutdown signal received");

        // Flush pending writes
        if let Err(e) = handler_clone.flush().await {
            error!("Error flushing on shutdown: {}", e);
        }

        server_clone.shutdown();
    });

    // Run server
    info!("Server starting on {}", config.bind);
    if let Err(e) = server.run().await {
        error!("Server error: {}", e);
    }

    // Stop compaction thread
    if let Some(stop) = compaction_stop {
        stop.store(true, Ordering::SeqCst);
        info!("Waiting for compaction thread to stop...");
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // Final flush
    info!("Flushing pending writes...");
    handler.flush().await?;

    info!("SSD-KV shutdown complete");
    Ok(())
}
