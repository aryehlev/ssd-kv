//! `kvc`: the kvssd coordinator binary (DESIGN §23, §24.12).
#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(unsafe_code)]
#![allow(dead_code, unused_variables)]

use clap::Parser;
use kv_coord::config::Cli;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) => {
            use clap::error::ErrorKind;
            if matches!(e.kind(), ErrorKind::DisplayHelp | ErrorKind::DisplayVersion) {
                e.exit();
            }
            // Bad configuration: exit code 1 with a one-line reason (DESIGN §22).
            let _ = e.print();
            std::process::exit(1);
        }
    };
    let _ = cli;
    eprintln!("kvc: not yet implemented");
    std::process::exit(2);
}
