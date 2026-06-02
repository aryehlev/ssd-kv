//! SIndex KV server entry point.

use std::sync::Arc;

use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

mod config;
mod engine;
mod io;
mod server;

use config::Config;
use engine::KvEngine;
use server::reactor::{start_reactor_multi, start_reactor_server};
use server::{DatabaseManager, DbHandler, Handler, ServerTuning};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::parse();
    config.validate()?;

    let log_level = if config.verbose { "debug" } else { &config.log_level };
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(log_level)),
        )
        .init();

    info!(
        "SIndex KV starting — bind={} data_dir={:?} num_dbs={} reactor_threads={}",
        config.bind, config.data_dir, config.num_dbs, config.reactor_threads
    );
    info!(
        "Paper: 'The Design of Trillion-scale SSD-based Indexing with \
         Deterministic Latency for Cloud Block Storage', ACM TOS 2024"
    );

    std::fs::create_dir_all(&config.data_dir)?;

    // Create one KvEngine per database (SELECT 0..num_dbs-1).
    let mut dbs: Vec<DbHandler> = Vec::with_capacity(config.num_dbs as usize);
    for db_idx in 0..config.num_dbs {
        let db_dir = config.data_dir.join(format!("db{}", db_idx));
        let engine = KvEngine::open(&db_dir)?;
        let handler = Arc::new(Handler::new(engine));
        dbs.push(DbHandler::new(handler));
    }
    let db_manager = Arc::new(DatabaseManager::new(dbs));

    let tuning = ServerTuning {
        read_buf_bytes: config.read_buffer_bytes(),
        write_buf_bytes: config.write_buffer_bytes(),
        max_connections: config.max_connections,
    };

    if config.reactor_threads <= 1 {
        info!("starting single reactor on {}", config.bind);
        let handle = start_reactor_server(config.bind, Arc::clone(&db_manager), tuning);
        handle.join().ok();
    } else {
        info!(
            "starting {} reactor threads on {} (SO_REUSEPORT)",
            config.reactor_threads, config.bind
        );
        let handles = start_reactor_multi(
            config.bind,
            Arc::clone(&db_manager),
            tuning,
            config.reactor_threads,
        );
        for h in handles {
            h.join().ok();
        }
    }

    Ok(())
}
