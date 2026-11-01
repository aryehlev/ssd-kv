//! `kvc`: the kvssd coordinator binary (DESIGN §23, §24.12).
#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(unsafe_code)]

use std::net::SocketAddr;
use std::process::ExitCode;

use clap::Parser;
use kv_coord::config::{Cli, Command, InitArgs, RecoverArgs, ServeConfig};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) => {
            use clap::error::ErrorKind;
            if matches!(e.kind(), ErrorKind::DisplayHelp | ErrorKind::DisplayVersion) {
                e.exit();
            }
            // Bad configuration: exit code 1 with a one-line reason (DESIGN §22).
            let _ = e.print();
            return ExitCode::from(1);
        }
    };
    let r = match cli.command {
        Some(Command::Init(a)) => init(&a),
        Some(Command::Recover(a)) => recover(&a),
        None => serve(&cli.serve),
    };
    match r {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("kvc: {e}");
            ExitCode::from(1)
        }
    }
}

fn init(a: &InitArgs) -> Result<(), String> {
    let r = kv_coord::init_dir(
        &a.dir,
        &a.id,
        &a.peers,
        (a.client_port, a.admin_port),
        a.join,
        a.half_bytes,
    )?;
    println!("{r}");
    Ok(())
}

fn recover(a: &RecoverArgs) -> Result<(), String> {
    let r = kv_coord::recover_dir(&a.dir, &a.from, a.id.as_deref(), a.peers.as_deref())?;
    println!("{r}");
    Ok(())
}

fn serve(args: &kv_coord::config::ServeArgs) -> Result<(), String> {
    let cfg = ServeConfig::load(args)?;
    let logfile = (!cfg.base.logfile.is_empty()).then(|| std::path::PathBuf::from(&cfg.base.logfile));
    kv_coord::log::init(&cfg.base.loglevel, logfile.as_deref()).map_err(|e| e.to_string())?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("kvc")
        .build()
        .map_err(|e| format!("runtime: {e}"))?;
    rt.block_on(async move {
        let node = kv_coord::start(&cfg).await.map_err(|e| e.to_string())?;
        let addrs: [(&str, SocketAddr); 3] = [
            ("client", node.client_addr),
            ("peer", node.peer_addr),
            ("admin", node.admin_addr),
        ];
        for (what, a) in addrs {
            tracing::info!(%a, "{what} port listening");
        }
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .map_err(|e| e.to_string())?;
        let failed = node.wait_failed();
        tokio::select! {
            _ = tokio::signal::ctrl_c() => tracing::info!("interrupted, stopping"),
            _ = term.recv() => tracing::info!("terminated, stopping"),
            _ = failed => {
                node.shutdown().await;
                return Err("configuration state could not be persisted".to_owned());
            }
        }
        node.shutdown().await;
        Ok(())
    })
}
