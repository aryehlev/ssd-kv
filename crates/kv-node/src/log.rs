//! Diagnostics log (DESIGN §20.2 `loglevel`/`logfile`, §22, LC-02, LC-32).
//!
//! Text diagnostics through `tracing`, to stderr (`logfile ""`) or to `logfile`. This module and
//! `kv-coord/src/log.rs` are the only places allowed to open a file for appending (named
//! exemption, LC-02). Diagnostics at every level carry key length, db, slot, partition and at
//! most a 32-bit tag through `kv_common::Redacted`, never key, field or value bytes (LC-32).
//! Core dumps are disabled at startup unless `core-dumps yes`.

use crate::config::LogLevel;

/// Initializes the diagnostics sink.
pub fn init(level: LogLevel, logfile: &str) -> std::io::Result<()> {
    todo!("DESIGN §20.2: diagnostics log")
}
