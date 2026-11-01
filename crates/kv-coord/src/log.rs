//! Coordinator diagnostics (DESIGN §20.2 `loglevel`/`logfile`, LC-02, LC-32).
//!
//! Text diagnostics through `tracing`, to stderr or to `logfile`. This module and
//! `kv-node/src/log.rs` are the only places allowed to open a file for appending (named
//! exemption, LC-02). Diagnostics never carry user bytes.

/// Initializes the diagnostics sink.
pub fn init(loglevel: &str, logfile: Option<&std::path::Path>) -> std::io::Result<()> {
    todo!("DESIGN §20.2: diagnostics log")
}
