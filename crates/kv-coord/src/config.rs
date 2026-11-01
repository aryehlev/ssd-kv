//! `kvc` command line and configuration (DESIGN §23.1, §23.3, §23.12).
//!
//! ```text
//! kvc [CONFIG_FILE] [--<name> <value>...]                                  serve
//! kvc init --dir DIR --id ID --peers id=host:peer,... --client-port N --admin-port N
//! kvc recover --dir DIR --from FILE        single-member group at version + 1, coord_term + 2^32
//! ```
//! Ports: `client-port` (RESP topology service, default 7000), `peer-port` (coordinator peers),
//! `admin-port` (kvadm). One `cluster-auth-token` is shared with the data nodes; `admin-token`
//! authenticates kvadm.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "kvc",
    version,
    about = "kvssd coordinator: topology service and control plane (stores no user data)",
    args_conflicts_with_subcommands = true
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,
    #[command(flatten)]
    pub serve: ServeArgs,
}

#[derive(Subcommand)]
pub enum Command {
    /// Create this coordinator's fixed-size configuration-state files (once per coordinator).
    Init(InitArgs),
    /// Disaster recovery: start a new single-member group from a state file or export.
    Recover(RecoverArgs),
}

/// Serving options (each also settable in CONFIG_FILE as `name value`).
#[derive(Args, Default)]
pub struct ServeArgs {
    /// Configuration file (`name value` lines).
    #[arg(value_name = "CONFIG_FILE")]
    pub config_file: Option<PathBuf>,
    /// Directory holding coord.meta and coord.state.
    #[arg(long)]
    pub dir: Option<PathBuf>,
    #[arg(long, value_name = "ADDR")]
    pub bind: Option<String>,
    /// RESP topology service port (default 7000).
    #[arg(long, value_name = "PORT")]
    pub client_port: Option<u16>,
    /// Coordinator peer port.
    #[arg(long, value_name = "PORT")]
    pub peer_port: Option<u16>,
    /// Operator API port.
    #[arg(long, value_name = "PORT")]
    pub admin_port: Option<u16>,
    #[arg(long, value_name = "TOKEN")]
    pub cluster_auth_token: Option<String>,
    #[arg(long, value_name = "TOKEN")]
    pub admin_token: Option<String>,
    #[arg(long, value_name = "LEVEL")]
    pub loglevel: Option<String>,
    #[arg(long, value_name = "PATH")]
    pub logfile: Option<String>,
    #[arg(long, value_name = "yes|no")]
    pub core_dumps: Option<String>,
}

#[derive(Args, Debug)]
pub struct InitArgs {
    #[arg(long)]
    pub dir: PathBuf,
    /// This coordinator's id (must appear in --peers).
    #[arg(long)]
    pub id: String,
    /// Initial peer list: `id=host:peer_port,...`.
    #[arg(long, value_name = "LIST")]
    pub peers: String,
    #[arg(long, value_name = "PORT")]
    pub client_port: u16,
    #[arg(long, value_name = "PORT")]
    pub admin_port: u16,
}

#[derive(Args, Debug)]
pub struct RecoverArgs {
    #[arg(long)]
    pub dir: PathBuf,
    /// A coord.state file or a `kvadm coord export` file.
    #[arg(long, value_name = "FILE")]
    pub from: PathBuf,
}

/// Effective coordinator configuration.
#[derive(Clone, Debug)]
pub struct CoordConfig {
    pub dir: PathBuf,
    pub bind: String,
    pub client_port: u16,
    pub peer_port: u16,
    pub admin_port: u16,
    pub loglevel: String,
    pub logfile: String,
    pub core_dumps: bool,
}

impl Default for CoordConfig {
    fn default() -> Self {
        CoordConfig {
            dir: PathBuf::from("."),
            bind: "127.0.0.1".to_owned(),
            client_port: 7000,
            peer_port: 7100,
            admin_port: 7200,
            loglevel: "notice".to_owned(),
            logfile: String::new(),
            core_dumps: false,
        }
    }
}

impl CoordConfig {
    /// Applies the config file, then the command-line overrides.
    pub fn load(args: &ServeArgs) -> Result<Self, String> {
        todo!("DESIGN §23.1: coordinator configuration")
    }
}
