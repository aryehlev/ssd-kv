//! `kvd`: the kvssd data node (DESIGN §4.2, §6, §18.1, §20, §24.11).
//!
//! Threads (§6.1): `threads` pinned io_uring workers, each owning its client connections, the
//! partitions `{p : p % W == w}`, a page cache and one flush tracker per device; one meta thread
//! for fixed-location metadata; one agent thread (tokio current-thread) for the coordinator
//! control channel, leases and `/metrics`. Startup (§18.1): probe and open devices, start workers
//! and listeners at once (commands needing a partition reply `-LOADING` until it is Ready),
//! recover each partition (snapshot or full scan), write the startup superblock before any data
//! write.
//!
//! `unsafe` is allowed only in `runtime/ring.rs` (§24).
#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(unsafe_code)]
#![allow(dead_code, unused_variables)]

mod cluster;
mod cmd;
mod config;
mod dispatch;
mod log;
mod metrics;
mod net;
mod runtime;
mod shared;

use clap::Parser;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    let cli = match config::Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) => {
            use clap::error::ErrorKind;
            if matches!(e.kind(), ErrorKind::DisplayHelp | ErrorKind::DisplayVersion) {
                e.exit();
            }
            // Bad configuration: exit code 1 with a one-line reason before listening (§22).
            let _ = e.print();
            std::process::exit(1);
        }
    };
    let _ = cli;
    eprintln!("kvd: not yet implemented");
    std::process::exit(2);
}
