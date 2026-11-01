//! Diagnostics log (DESIGN §20.2 `loglevel`/`logfile`, §22, LC-02, LC-32).
//!
//! Text diagnostics through `tracing`, to stderr (`logfile ""`) or to `logfile`. This module and
//! `kv-coord/src/log.rs` are the only places allowed to open a file for appending (named
//! exemption, LC-02). Diagnostics at every level carry key length, db, slot, partition and at
//! most a 32-bit tag through `kv_common::Redacted`, never key, field or value bytes (LC-32).
//! Core dumps are disabled at startup unless `core-dumps yes`.
//!
//! Level mapping: `debug` -> TRACE, `verbose` -> DEBUG, `notice` -> INFO, `warning` -> WARN.
//! The level can change at run time (CONFIG SET loglevel) through [`set_level`].

use std::sync::OnceLock;

use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::reload;
use tracing_subscriber::util::SubscriberInitExt as _;

use crate::config::LogLevel;

type Registry = tracing_subscriber::Registry;

/// Handle to change the level after [`init`].
static RELOAD: OnceLock<reload::Handle<LevelFilter, Registry>> = OnceLock::new();

/// The `tracing` filter of a Redis log level.
pub fn level_filter(level: LogLevel) -> LevelFilter {
    match level {
        LogLevel::Debug => LevelFilter::TRACE,
        LogLevel::Verbose => LevelFilter::DEBUG,
        LogLevel::Notice => LevelFilter::INFO,
        LogLevel::Warning => LevelFilter::WARN,
    }
}

/// Initializes the diagnostics sink.
///
/// Called once per process; a second call (or a process whose global subscriber was installed
/// by someone else, e.g. a test harness) only adjusts the level.
pub fn init(level: LogLevel, logfile: &str) -> std::io::Result<()> {
    if RELOAD.get().is_some() {
        set_level(level);
        return Ok(());
    }
    let (filter, handle) = reload::Layer::new(level_filter(level));
    let fmt = tracing_subscriber::fmt::layer().with_target(false).with_thread_names(true);
    let installed = if logfile.is_empty() {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt.with_writer(std::io::stderr).with_ansi(false))
            .try_init()
    } else {
        // The one diagnostic file opened for appending (LC-02 named exemption).
        let file = std::fs::OpenOptions::new().create(true).append(true).open(logfile)?;
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt.with_writer(std::sync::Mutex::new(file)).with_ansi(false))
            .try_init()
    };
    if installed.is_ok() {
        let _ = RELOAD.set(handle);
    }
    Ok(())
}

/// Changes the level (CONFIG SET loglevel). No-op before [`init`].
pub fn set_level(level: LogLevel) {
    if let Some(h) = RELOAD.get() {
        let _ = h.modify(|f| *f = level_filter(level));
    }
}

/// Applies `core-dumps`: `no` makes the process non-dumpable (`PR_SET_DUMPABLE 0`, LC-32).
pub fn set_core_dumps(enabled: bool) -> std::io::Result<()> {
    crate::runtime::ring::sys::set_dumpable(enabled)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn levels_map_to_tracing() {
        assert_eq!(level_filter(LogLevel::Debug), LevelFilter::TRACE);
        assert_eq!(level_filter(LogLevel::Verbose), LevelFilter::DEBUG);
        assert_eq!(level_filter(LogLevel::Notice), LevelFilter::INFO);
        assert_eq!(level_filter(LogLevel::Warning), LevelFilter::WARN);
    }

    #[test]
    fn init_is_idempotent() {
        init(LogLevel::Warning, "").unwrap();
        init(LogLevel::Notice, "").unwrap();
        set_level(LogLevel::Debug);
    }
}
