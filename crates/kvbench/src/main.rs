//! `kvbench`: the kvssd benchmark client (DESIGN §25.8, §27).
//!
//! Closed or open loop RESP load generator: `--load` inserts the dataset (`key:%016d` keys),
//! `--ratio R:W` read/update mixes, `--dist uniform|zipf:0.99`, `--conns`, `--pipeline`,
//! `--rate` (open loop, ops/s), HDR latency output with p50/p90/p99/p99.9/p99.99.
#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(unsafe_code)]
#![allow(dead_code, unused_variables)]

use std::path::PathBuf;

use clap::Parser;

#[derive(Parser)]
#[command(name = "kvbench", version, about = "kvssd benchmark client")]
struct Cli {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value_t = 6379)]
    port: u16,
    /// Insert the whole keyspace once, then exit.
    #[arg(long)]
    load: bool,
    /// Number of keys.
    #[arg(long, default_value_t = 20_000_000)]
    keys: u64,
    /// Value size in bytes.
    #[arg(long, default_value_t = 300)]
    value_size: usize,
    /// Read:write ratio, e.g. 90:10.
    #[arg(long, default_value = "90:10")]
    ratio: String,
    /// Key distribution: uniform or zipf:<theta>.
    #[arg(long, default_value = "uniform")]
    dist: String,
    #[arg(long, default_value_t = 64)]
    conns: u32,
    #[arg(long, default_value_t = 1)]
    pipeline: u32,
    /// Open-loop target rate in ops/s (closed loop when absent).
    #[arg(long)]
    rate: Option<u64>,
    /// Measured duration in seconds.
    #[arg(long, default_value_t = 300)]
    duration: u64,
    /// Warm-up seconds before measuring.
    #[arg(long, default_value_t = 60)]
    warmup: u64,
    /// Write the merged latency histograms to this file.
    #[arg(long, value_name = "PATH")]
    hdr_out: Option<PathBuf>,
}

fn main() {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) => {
            use clap::error::ErrorKind;
            if matches!(e.kind(), ErrorKind::DisplayHelp | ErrorKind::DisplayVersion) {
                e.exit();
            }
            let _ = e.print();
            std::process::exit(1);
        }
    };
    let _ = cli;
    eprintln!("kvbench: not yet implemented");
    std::process::exit(2);
}
