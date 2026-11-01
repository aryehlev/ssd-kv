//! `kv-coord`: the coordinator service `kvc` (DESIGN §23.1-§23.5, §23.9-§23.13, §24.12).
//!
//! A topology service and control plane: it stores no user data and has no keyspace or storage
//! layer (LC-38). Coordinators replicate the whole [`kv_topo::ClusterState`] among themselves
//! (WSR, [`wsr`]): Raft-style randomized election, depth-one proposals of the whole next state,
//! fixed A/B persistence halves, and an operator-set peer list kept outside the voted value. The
//! leader dials every registered node ([`push`]), grants leases ([`lease`]), detects failures
//! from missed renewals ([`detector`]), fails over by the static rule ([`failover`]), commits ISR
//! changes ([`isr`]), orchestrates operator migrations with position barriers ([`migration`]),
//! serves client topology and redirects ([`clientsvc`]) and the operator API ([`admin`],
//! [`rebalance`]). Side effects are produced only from committed state (§23.3 rule 6).
//! Runtime: tokio multi-thread.
//!
//! This file also assembles a running coordinator ([`start`], [`Coordinator`]) and implements
//! `kvc init` ([`init_dir`]) and `kvc recover` ([`recover_dir`]), so the binary and the
//! in-process tests run the same code.
#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(unsafe_code)]

pub mod admin;
pub mod clientsvc;
pub mod config;
pub mod detector;
pub mod failover;
pub mod isr;
pub mod lease;
pub mod log;
pub mod migration;
pub mod push;
pub mod rebalance;
pub mod wsr;

use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

pub use admin::{Controllers, serve_admin};
pub use clientsvc::serve_clients;
pub use failover::{Candidate, choose_primary, run_failover};
pub use lease::LeaseTable;
pub use push::Pusher;
pub use rebalance::rebalance_plan;
pub use wsr::Wsr;

use kv_topo::{ClusterParams, ClusterState};

use crate::admin::CtlCfg;
use crate::clientsvc::ClientSvcCfg;
use crate::config::ServeConfig;
use crate::push::PushCfg;
use crate::wsr::WsrCfg;
use crate::wsr::peers::{CoordRecord, PeerList, coord_name, parse_coord_id};
use crate::wsr::persist::{Export, STATE_HALF_BYTES, StateFiles, Versioned};

/// `kvc init`: creates `dir`'s fixed-size `coord.meta` (peer list generation 1, or an empty
/// joining list with `join`) and `coord.state` (version 1 with default parameters).
pub fn init_dir(
    dir: &Path,
    id: &str,
    peers: &str,
    default_ports: (u16, u16),
    join: bool,
    half_bytes: Option<usize>,
) -> Result<String, String> {
    let me_id = parse_coord_id(id).ok_or_else(|| format!("invalid coordinator id '{id}'"))?;
    let list = PeerList::parse(1, peers, Some(default_ports))?;
    let me = list
        .record(&me_id)
        .cloned()
        .ok_or_else(|| format!("--id {id} must appear in --peers"))?;
    let peers = if join {
        if list.peers.len() != 1 {
            return Err("with --join, --peers names only this coordinator".into());
        }
        PeerList::joining()
    } else {
        list
    };
    let half = half_bytes.unwrap_or(STATE_HALF_BYTES);
    StateFiles::create_with_identity(dir, half, Some(&me), &peers).map_err(|e| format!("{}: {e}", dir.display()))?;
    let (mut files, _) = StateFiles::open(dir).map_err(|e| e.to_string())?;
    let state = ClusterState::initial(ClusterParams::default(), rand::random::<[u8; 16]>());
    files
        .write_state(
            &Versioned {
                version: 1,
                term: 0,
                bytes: state.encode(),
            },
            None,
        )
        .map_err(|e| e.to_string())?;
    Ok(if join {
        format!(
            "initialized {} as coordinator {} (joining: add it with kvadm coord add)",
            dir.display(),
            coord_name(&me.id)
        )
    } else {
        format!(
            "initialized {} as coordinator {} (peer generation 1: {})",
            dir.display(),
            coord_name(&me.id),
            peers
        )
    })
}

/// `kvc recover --from FILE`: a new single-member group from an export or a `coord.state`
/// file, at `version + 1`, peer generation + 1 and coordinator term `export term + 2^32`
/// (DESIGN §23.3), so nodes accept its assignments however stale the export is. Nothing is
/// asked of any data node.
pub fn recover_dir(dir: &Path, from: &Path, id: Option<&str>, entry: Option<&str>) -> Result<String, String> {
    let x = Export::read_any(from).map_err(|e| format!("{}: {e}", from.display()))?;
    let mut state = ClusterState::decode(&x.bytes).map_err(|e| format!("the exported state does not decode: {e}"))?;
    if !dir.join(wsr::persist::META_FILE).exists() {
        let (Some(id), Some(entry)) = (id, entry) else {
            return Err("DIR holds no coordinator files: give --id and --peers (this coordinator's entry)".into());
        };
        let me_id = parse_coord_id(id).ok_or_else(|| format!("invalid coordinator id '{id}'"))?;
        let list = PeerList::parse(1, entry, None)?;
        let me = list
            .record(&me_id)
            .cloned()
            .ok_or_else(|| format!("--id {id} must appear in --peers"))?;
        StateFiles::create_with_identity(dir, STATE_HALF_BYTES, Some(&me), &PeerList::joining())
            .map_err(|e| e.to_string())?;
    }
    let (mut files, p) = StateFiles::open(dir).map_err(|e| e.to_string())?;
    let me: CoordRecord = p
        .me
        .ok_or("coord.meta records no identity for this coordinator")?;
    let term = x
        .coord_term
        .max(p.term)
        .checked_add(1 << 32)
        .ok_or("coordinator term overflow")?;
    let peer_gen = x.peer_gen.max(p.peers.r#gen) + 1;
    let peers = PeerList {
        r#gen: peer_gen,
        peers: vec![me.clone()],
    };
    state.version = x.version + 1;
    let bytes = state.encode();
    files.write_meta(term, None, &peers).map_err(|e| e.to_string())?;
    files
        .write_state(
            &Versioned {
                version: state.version,
                term,
                bytes,
            },
            None,
        )
        .map_err(|e| e.to_string())?;
    Ok(format!(
        "recovered version {} as a single-member group: coordinator {}, term {term}, peer generation {peer_gen}",
        state.version,
        coord_name(&me.id)
    ))
}

/// A running coordinator: WSR member, node pusher, controllers, client and admin services.
pub struct Coordinator {
    pub wsr: Arc<Wsr>,
    pub ctl: Arc<Controllers>,
    pub client_addr: SocketAddr,
    pub peer_addr: SocketAddr,
    pub admin_addr: SocketAddr,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

/// Starts a coordinator from its state directory and serving configuration.
pub async fn start(cfg: &ServeConfig) -> std::io::Result<Coordinator> {
    start_with(cfg, WsrCfg::default()).await
}

/// [`start`] with explicit WSR timers (tests use shorter election timeouts); `wsr.id`,
/// `peer_listen` and `auth` are filled in from the state files and `cfg`.
pub async fn start_with(cfg: &ServeConfig, mut wsr_cfg: WsrCfg) -> std::io::Result<Coordinator> {
    let dir = cfg.base.dir.clone();
    let (files, p) = tokio::task::spawn_blocking(move || StateFiles::open(&dir))
        .await
        .map_err(std::io::Error::other)??;
    let me = p.me.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "coord.meta records no identity for this coordinator (run kvc init)",
        )
    })?;
    let ip = |rec: SocketAddr| -> std::io::Result<IpAddr> {
        if cfg.bind_set {
            cfg.base
                .bind
                .parse()
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "bind: not an IP address"))
        } else {
            Ok(rec.ip())
        }
    };
    let pick = |rec: SocketAddr, set: bool, port: u16| -> std::io::Result<SocketAddr> {
        Ok(SocketAddr::new(ip(rec)?, if set { port } else { rec.port() }))
    };
    let peer_addr = pick(me.peer, cfg.peer_port_set, cfg.base.peer_port)?;
    let client_addr = pick(me.client, cfg.client_port_set, cfg.base.client_port)?;
    let admin_addr = pick(me.admin, cfg.admin_port_set, cfg.base.admin_port)?;
    let peer_l = tokio::net::TcpListener::bind(peer_addr).await?;
    let client_l = tokio::net::TcpListener::bind(client_addr).await?;
    let admin_l = tokio::net::TcpListener::bind(admin_addr).await?;
    let (peer_addr, client_addr, admin_addr) = (peer_l.local_addr()?, client_l.local_addr()?, admin_l.local_addr()?);
    let auth = cfg.cluster_auth_token.as_bytes().to_vec();
    wsr_cfg.id = me.id;
    wsr_cfg.peer_listen = peer_addr;
    wsr_cfg.auth = auth.clone();

    let wsr = Wsr::start_with_listener(wsr_cfg, files, peer_l).await?;
    let (pusher, events) = Pusher::new(wsr.clone(), PushCfg::new(me.id, auth.clone()));
    let ctl = Controllers::new(
        wsr.clone(),
        pusher.clone(),
        CtlCfg {
            admin_token: cfg.admin_token.as_bytes().to_vec(),
            auth,
        },
    );
    let mut tasks = Vec::new();
    tasks.push(tokio::spawn(pusher.clone().run()));
    tasks.push(tokio::spawn(ctl.clone().run(events)));
    let view = ctl.clone();
    let svc = ClientSvcCfg {
        style: cfg.style,
        pfail: Some(Arc::new(move |n: &kv_ctrl::NodeId| view.suspected(n))),
        port: client_addr.port(),
    };
    let w = wsr.clone();
    tasks.push(tokio::spawn(async move {
        if let Err(e) = clientsvc::serve_clients_with(client_l, w, svc).await {
            tracing::error!("client service stopped: {e}");
        }
    }));
    let (w, c) = (wsr.clone(), ctl.clone());
    tasks.push(tokio::spawn(async move {
        if let Err(e) = admin::serve_admin(admin_l, w, c).await {
            tracing::error!("admin service stopped: {e}");
        }
    }));
    Ok(Coordinator {
        wsr,
        ctl,
        client_addr,
        peer_addr,
        admin_addr,
        tasks,
    })
}

impl Coordinator {
    /// Resolves once the WSR member stopped after a persistence failure.
    pub fn wait_failed(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let wsr = self.wsr.clone();
        async move {
            while !wsr.is_failed() {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }

    /// Stops every service and the WSR member (the state files stay as last written).
    pub async fn shutdown(mut self) {
        for t in self.tasks.drain(..) {
            t.abort();
            let _ = t.await;
        }
        self.wsr.shutdown().await;
    }
}
