//! `kvc` command line and configuration (DESIGN §23.1, §23.3, §23.12).
//!
//! ```text
//! kvc [CONFIG_FILE] [--<name> <value>...]                                  serve
//! kvc init --dir DIR --id ID --peers id=host:peer,... --client-port N --admin-port N
//! kvc init --dir DIR --id ID --peers id=host:peer/client/admin --join     (a coordinator to add)
//! kvc recover --dir DIR --from FILE        single-member group at version + 1, coord_term + 2^32
//! ```
//! Ports: `client-port` (RESP topology service, default 7000), `peer-port` (coordinator peers),
//! `admin-port` (kvadm). One `cluster-auth-token` is shared with the data nodes; `admin-token`
//! authenticates kvadm.
//!
//! The configuration file has `name value` lines, `#` comments and `include path` lines (as the
//! data node's, §20.2); a command-line `--name value` overrides the file. A coordinator's own
//! ports come from its record in its peer list (`coord.meta`); `bind` only changes the address
//! the listeners bind to (e.g. `0.0.0.0`).

use std::path::{Path, PathBuf};

use clap::{Args, Parser, Subcommand};
use kv_topo::render::CoordLineStyle;

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
    /// RESP topology service port (default: from this coordinator's peer-list record).
    #[arg(long, value_name = "PORT")]
    pub client_port: Option<u16>,
    /// Coordinator peer port (default: from this coordinator's peer-list record).
    #[arg(long, value_name = "PORT")]
    pub peer_port: Option<u16>,
    /// Operator API port (default: from this coordinator's peer-list record).
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
    /// How coordinators appear in CLUSTER NODES: r1 (slotless replica lines) or r2 (slotless
    /// master lines with constant replies for fan-out commands).
    #[arg(long, value_name = "r1|r2")]
    pub coord_line_style: Option<String>,
}

#[derive(Args, Debug)]
pub struct InitArgs {
    #[arg(long)]
    pub dir: PathBuf,
    /// This coordinator's id (must appear in --peers).
    #[arg(long)]
    pub id: String,
    /// Initial peer list: `id=host:peer_port[/client_port[/admin_port]],...`.
    #[arg(long, value_name = "LIST")]
    pub peers: String,
    #[arg(long, value_name = "PORT", default_value_t = 7000)]
    pub client_port: u16,
    #[arg(long, value_name = "PORT", default_value_t = 7200)]
    pub admin_port: u16,
    /// Create a coordinator that joins an existing group: --peers names only this coordinator;
    /// it takes part once the operator adds it (`kvadm coord add`).
    #[arg(long)]
    pub join: bool,
    /// Bytes of one state half (tests use small halves; default 16 MiB).
    #[arg(long, value_name = "BYTES", hide = true)]
    pub half_bytes: Option<usize>,
}

#[derive(Args, Debug)]
pub struct RecoverArgs {
    #[arg(long)]
    pub dir: PathBuf,
    /// A coord.state file or a `kvadm coord export` file.
    #[arg(long, value_name = "FILE")]
    pub from: PathBuf,
    /// This coordinator's id, when DIR holds no coordinator files yet.
    #[arg(long)]
    pub id: Option<String>,
    /// This coordinator's own entry (`id=host:peer/client/admin`), when DIR holds no
    /// coordinator files yet.
    #[arg(long, value_name = "ENTRY")]
    pub peers: Option<String>,
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

/// Everything `kvc` serves with: the file/CLI settings plus the secrets and options that are
/// not part of [`CoordConfig`]'s public fields. No `Debug` (it holds the tokens).
#[derive(Clone)]
pub struct ServeConfig {
    pub base: CoordConfig,
    /// `bind` was given explicitly (else each listener binds its peer-list address).
    pub bind_set: bool,
    /// Ports given explicitly (else from the peer-list record).
    pub client_port_set: bool,
    pub peer_port_set: bool,
    pub admin_port_set: bool,
    pub cluster_auth_token: String,
    pub admin_token: String,
    pub style: CoordLineStyle,
}

/// Raw `name value` settings in file order.
fn read_file(path: &Path, depth: usize, out: &mut Vec<(String, String)>) -> Result<(), String> {
    if depth > 8 {
        return Err(format!("{}: include nesting too deep", path.display()));
    }
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (name, value) = match line.split_once(char::is_whitespace) {
            Some((n, v)) => (n, v.trim()),
            None => (line, ""),
        };
        let value = value.trim_matches('"');
        let name = name.to_ascii_lowercase();
        if name == "include" {
            let p = Path::new(value);
            let p = if p.is_relative() {
                path.parent().unwrap_or(Path::new(".")).join(p)
            } else {
                p.to_path_buf()
            };
            read_file(&p, depth + 1, out)?;
        } else {
            out.push((name, value.to_owned()));
        }
    }
    Ok(())
}

fn yes_no(name: &str, v: &str) -> Result<bool, String> {
    match v.to_ascii_lowercase().as_str() {
        "yes" | "true" | "1" => Ok(true),
        "no" | "false" | "0" => Ok(false),
        _ => Err(format!("{name}: expected yes or no, got '{v}'")),
    }
}

fn port(name: &str, v: &str) -> Result<u16, String> {
    v.parse::<u16>()
        .ok()
        .filter(|&p| p != 0)
        .ok_or_else(|| format!("{name}: invalid port '{v}'"))
}

fn style(v: &str) -> Result<CoordLineStyle, String> {
    match v.to_ascii_lowercase().as_str() {
        "r1" => Ok(CoordLineStyle::R1),
        "r2" => Ok(CoordLineStyle::R2),
        _ => Err(format!("coord-line-style: expected r1 or r2, got '{v}'")),
    }
}

impl CoordConfig {
    /// Applies the config file, then the command-line overrides.
    pub fn load(args: &ServeArgs) -> Result<Self, String> {
        ServeConfig::load(args).map(|c| c.base)
    }
}

impl ServeConfig {
    /// Applies the config file, then the command-line overrides.
    pub fn load(args: &ServeArgs) -> Result<Self, String> {
        let mut c = ServeConfig {
            base: CoordConfig::default(),
            bind_set: false,
            client_port_set: false,
            peer_port_set: false,
            admin_port_set: false,
            cluster_auth_token: String::new(),
            admin_token: String::new(),
            style: CoordLineStyle::R1,
        };
        let mut settings = Vec::new();
        if let Some(f) = &args.config_file {
            read_file(f, 0, &mut settings)?;
        }
        let cli: [(&str, Option<String>); 11] = [
            ("dir", args.dir.as_ref().map(|d| d.display().to_string())),
            ("bind", args.bind.clone()),
            ("client-port", args.client_port.map(|p| p.to_string())),
            ("peer-port", args.peer_port.map(|p| p.to_string())),
            ("admin-port", args.admin_port.map(|p| p.to_string())),
            ("cluster-auth-token", args.cluster_auth_token.clone()),
            ("admin-token", args.admin_token.clone()),
            ("loglevel", args.loglevel.clone()),
            ("logfile", args.logfile.clone()),
            ("core-dumps", args.core_dumps.clone()),
            ("coord-line-style", args.coord_line_style.clone()),
        ];
        settings.extend(
            cli.into_iter()
                .filter_map(|(n, v)| v.map(|v| (n.to_owned(), v))),
        );
        for (name, v) in settings {
            match name.replace('_', "-").as_str() {
                "dir" => c.base.dir = PathBuf::from(v),
                "bind" => {
                    c.base.bind = v;
                    c.bind_set = true;
                }
                "client-port" => {
                    c.base.client_port = port(&name, &v)?;
                    c.client_port_set = true;
                }
                "peer-port" => {
                    c.base.peer_port = port(&name, &v)?;
                    c.peer_port_set = true;
                }
                "admin-port" => {
                    c.base.admin_port = port(&name, &v)?;
                    c.admin_port_set = true;
                }
                "cluster-auth-token" => c.cluster_auth_token = v,
                "admin-token" => c.admin_token = v,
                "loglevel" => {
                    if !matches!(v.as_str(), "debug" | "verbose" | "notice" | "warning" | "nothing") {
                        return Err(format!("loglevel: unknown level '{v}'"));
                    }
                    c.base.loglevel = v;
                }
                "logfile" => c.base.logfile = v,
                "core-dumps" => c.base.core_dumps = yes_no(&name, &v)?,
                "coord-line-style" => c.style = style(&v)?,
                other => return Err(format!("unknown configuration parameter '{other}'")),
            }
        }
        if c.cluster_auth_token.is_empty() {
            return Err("cluster-auth-token is required".into());
        }
        if c.admin_token.is_empty() {
            return Err("admin-token is required".into());
        }
        Ok(c)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_then_cli() {
        let dir = std::env::temp_dir().join(format!("kvc-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("inc.conf"), "admin-token secret2\n").unwrap();
        std::fs::write(
            dir.join("kvc.conf"),
            "# coordinator\ndir /var/lib/kvc\nclient-port 7001\ncluster-auth-token \"tok\"\ninclude inc.conf\ncoord-line-style r2\n",
        )
        .unwrap();
        let args = ServeArgs {
            config_file: Some(dir.join("kvc.conf")),
            client_port: Some(7005),
            ..ServeArgs::default()
        };
        let c = ServeConfig::load(&args).unwrap();
        assert_eq!(c.base.dir, PathBuf::from("/var/lib/kvc"));
        assert_eq!(c.base.client_port, 7005);
        assert!(c.client_port_set && !c.peer_port_set);
        assert_eq!(c.cluster_auth_token, "tok");
        assert_eq!(c.admin_token, "secret2");
        assert_eq!(c.style, CoordLineStyle::R2);
        assert_eq!(CoordConfig::load(&args).unwrap().client_port, 7005);
        std::fs::write(dir.join("bad.conf"), "no-such-thing 1\n").unwrap();
        let bad = ServeArgs {
            config_file: Some(dir.join("bad.conf")),
            ..ServeArgs::default()
        };
        assert!(ServeConfig::load(&bad).is_err());
        let missing = ServeArgs::default();
        assert!(ServeConfig::load(&missing).err().is_some_and(|e| e.contains("cluster-auth-token")));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
