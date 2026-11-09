//! `kv-sim`: command line of the deterministic simulators (DESIGN §25.3); test-only.
//!
//! * `kv-sim storage --seeds N [--start S] [--mode atomic|two-image|both] [--cache wt|wb|both]
//!   [--lba 512|4096|both] [--jobs J] [--scale X] [--out DIR]` runs the storage simulation over
//!   a seed range; `kv-sim storage --replay FILE` runs one case file (see
//!   [`kv_sim::storage_sim::cli`]).
//! * `kv-sim cluster --seeds N [--start S] [--sim-ms MS] [--hostile] [--quiet-net]` runs the
//!   cluster simulation (3 coordinators + 6 nodes over the simulated network) over a seed range;
//!   `--hostile` adds message drops, duplicates and long delays, `--quiet-net` turns network
//!   faults off (see [`kv_sim::cluster_sim::cli`]).
#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(unsafe_code)]

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let code = if args.first().map(String::as_str) == Some("cluster") {
        kv_sim::cluster_sim::cli::main(args[1..].to_vec())
    } else {
        kv_sim::storage_sim::cli::main(args)
    };
    std::process::exit(code);
}
