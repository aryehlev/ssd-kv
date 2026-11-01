//! `faultdev`: a RAM-only NBD block device with `SimDevice` semantics for power-cut tests
//! (DESIGN §17.1, §25.4, LC-33). Test-only, never shipped.
//!
//! It holds the durable image and the volatile write-back set in RAM, honours FLUSH/FUA and
//! (optionally) atomic write units, and serves them as `/dev/nbdN`. On a power-cut request it
//! drops unflushed writes and tears in-flight ones at byte or sector granularity; the resulting
//! image is then served as a fresh device. It records no per-write history: the only artifacts
//! of a failure are the seed and the final image, written on request by `write_final_image`
//! (the only file this program ever creates).
#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(unsafe_code)]
#![allow(dead_code, unused_variables)]

use std::path::{Path, PathBuf};

use clap::Parser;

/// The device model served over NBD.
type Device = kv_storage::sim::SimDevice;

#[derive(Parser)]
#[command(name = "faultdev", version, about = "RAM-only fault-injection NBD device for power-cut tests")]
struct Cli {
    /// Device size, e.g. 4gb.
    #[arg(long)]
    size: String,
    /// Logical block size (512 or 4096).
    #[arg(long, default_value_t = 4096)]
    lba: u32,
    /// Verified atomic write unit in bytes (atomic page-update mode).
    #[arg(long)]
    atomic_unit: Option<u32>,
    /// Seed for tearing decisions.
    #[arg(long, default_value_t = 0)]
    seed: u64,
    /// NBD device to attach, e.g. /dev/nbd0.
    #[arg(long, value_name = "DEV")]
    nbd: PathBuf,
    /// Tear granularity of in-flight writes at a power cut: bytes, sectors or prefix.
    #[arg(long, default_value = "bytes")]
    tear: String,
    /// Where to write the final image when asked (failure artifact).
    #[arg(long, value_name = "PATH")]
    image_out: Option<PathBuf>,
}

/// Writes the final device image (the only artifact besides the seed, LC-33).
fn write_final_image(path: &Path, image: &[u8]) -> std::io::Result<()> {
    todo!("DESIGN §25.4: final image artifact")
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
    eprintln!("faultdev: not yet implemented");
    std::process::exit(2);
}
