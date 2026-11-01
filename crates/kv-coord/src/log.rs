//! Coordinator diagnostics (DESIGN §20.2 `loglevel`/`logfile`, LC-02, LC-32).
//!
//! Text diagnostics through `tracing`, to stderr or to `logfile`. This module and
//! `kv-node/src/log.rs` are the only places allowed to open a file for appending (named
//! exemption, LC-02). Diagnostics never carry user bytes (the coordinator holds none; tokens and
//! the client password are never formatted).
//!
//! Levels follow Redis's names: `debug`, `verbose` (= info with more detail), `notice` (info),
//! `warning`, `nothing`. `RUST_LOG` overrides them when set.

use std::fs::OpenOptions;
use std::sync::Mutex;

use tracing_subscriber::EnvFilter;

fn filter_for(loglevel: &str) -> &'static str {
    match loglevel {
        "debug" => "debug",
        "verbose" => "info,kv_coord=debug",
        "warning" => "warn",
        "nothing" => "off",
        _ => "info",
    }
}

/// Initializes the diagnostics sink.
pub fn init(loglevel: &str, logfile: Option<&std::path::Path>) -> std::io::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter_for(loglevel)));
    let builder = tracing_subscriber::fmt().with_env_filter(filter).with_target(false);
    let r = match logfile {
        Some(path) if !path.as_os_str().is_empty() => {
            // The one appended file of this process (LC-02 exemption).
            let f = OpenOptions::new().create(true).append(true).open(path)?;
            builder.with_ansi(false).with_writer(Mutex::new(f)).try_init()
        }
        _ => builder.with_writer(std::io::stderr).try_init(),
    };
    r.map_err(|e| std::io::Error::other(format!("diagnostics already initialized: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn levels() {
        assert_eq!(filter_for("notice"), "info");
        assert_eq!(filter_for("warning"), "warn");
        assert_eq!(filter_for("nothing"), "off");
        assert_eq!(filter_for("debug"), "debug");
    }
}
