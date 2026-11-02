//! `kv-sim`: command line of the deterministic simulators (DESIGN §25.3); test-only.
//!
//! `kv-sim storage --seeds N [--start S] [--mode atomic|two-image|both] [--cache wt|wb|both]
//! [--lba 512|4096|both]` runs the storage simulation over a seed range (see
//! [`kv_sim::storage_sim::cli`]).
#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(unsafe_code)]

fn main() {
    std::process::exit(kv_sim::storage_sim::cli::main(std::env::args().skip(1).collect()));
}
