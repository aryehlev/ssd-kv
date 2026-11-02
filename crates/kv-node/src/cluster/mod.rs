//! Cluster mode of a data node (DESIGN §23.4, §23.6-§23.13).
//!
//! * [`agent`]: the agent thread: coordinator control connection (the coordinator dials in),
//!   assignment application in `(coord_term, version)` order, lease renewals, fences, handoffs.
//! * [`ownership`]: the routing gate built from the node's own assignment (no full slot map).
//! * [`lease`]: the lease expiry cell re-checked before each reply is released.
//! * [`net`]: the replication transport: the repl-port listener, dials to primaries and
//!   migration sources, framed byte pumping. It moves bytes only; every stream is served by the
//!   worker owning its partition.
//! * [`repl_glue`]: per-partition replication streams wired to partitions and sockets.
//! * [`migrate_glue`]: migration source/destination endpoints wired to partitions.
//! * [`track`]: position trackers (contiguous durable prefixes, commit markers).
//!
//! This module holds [`ClusterShared`], the node-wide state the agent, the transport and the
//! workers share, and the startup read of the node's own fixed-location metadata.

pub mod agent;
pub mod lease;
pub mod migrate_glue;
pub mod net;
pub mod ownership;
pub mod repl_glue;
pub mod track;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use kv_common::abblock::AbCopy;
use kv_ctrl::{NodeAssignment, NodeId};
use kv_storage::layout::{DeviceLayout, PAGE, Seq};
use kv_storage::metastore::{MetaClient, MetaDone};
use kv_storage::pmeta::{NodeMeta, PartitionMeta};
use kv_storage::superblock::Superblock;

/// Meta-request tokens of the agent carry this bit (partition tokens never do; worker tokens
/// carry bit 63).
pub const AGENT_TOKEN_BIT: u64 = 1 << 62;
/// Meta-request tokens of a worker's replication glue (partition meta writes) carry this bit.
pub const REPL_TOKEN_BIT: u64 = 1 << 61;

/// The node's persisted cluster metadata as found at startup.
pub struct BootMeta {
    pub node: NodeMeta,
    pub parts: Vec<PartitionMeta>,
    pub set_uuid: [u8; 16],
    /// The last shutdown was clean (`Recovered::Clean`), else the partitions were scanned.
    pub clean: bool,
}

impl BootMeta {
    /// The metadata of a node that never joined (tests, fresh devices).
    pub fn empty(partitions: u16) -> Self {
        BootMeta {
            node: NodeMeta::initial(),
            parts: (0..partitions).map(PartitionMeta::initial).collect(),
            set_uuid: [0; 16],
            clean: true,
        }
    }
}

/// Reads node meta and every partition meta from device 0 (both A/B copies, newest valid),
/// before the engine opens (the meta thread owns these blocks afterwards).
pub fn read_boot_meta(path: &Path, sb: &Superblock) -> std::io::Result<BootMeta> {
    use std::os::unix::fs::FileExt;
    let bad = |what: &str| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{what} has no valid copy"),
        )
    };
    let f = std::fs::File::open(path)?;
    let layout =
        DeviceLayout::compute(sb.device_pages, true, sb.snap_region_pages).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("device layout: {e}"),
            )
        })?;
    let read = |page: u64| -> std::io::Result<Vec<u8>> {
        let mut b = vec![0u8; PAGE];
        f.read_exact_at(&mut b, page * PAGE as u64)?;
        Ok(b)
    };
    let a = read(layout.nmeta_copy_page(AbCopy::A))?;
    let b = read(layout.nmeta_copy_page(AbCopy::B))?;
    let node = NodeMeta::pick(&a, &b)
        .map(|(_, _, m)| m)
        .ok_or_else(|| bad("node meta"))?;
    let mut parts = Vec::with_capacity(usize::from(sb.partitions));
    for p in 0..sb.partitions {
        let a = read(layout.pmeta_copy_page(p, AbCopy::A))?;
        let b = read(layout.pmeta_copy_page(p, AbCopy::B))?;
        parts.push(
            PartitionMeta::pick(p, sb.partitions, &a, &b)
                .map(|(_, _, m)| m)
                .ok_or_else(|| bad("partition meta"))?,
        );
    }
    Ok(BootMeta {
        node,
        parts,
        set_uuid: sb.set_uuid,
        clean: sb.flags & kv_storage::superblock::SB_CLEAN != 0,
    })
}

/// Something a worker (or the meta thread's reply path) tells the agent.
pub enum AgentMsg {
    /// A meta request of the agent completed.
    Meta { token: u64, done: MetaDone },
    /// A worker's partitions answered a fence: `(partition, applied)`.
    FenceApplied { req: u64, applied: Vec<(u16, u64)> },
    /// A worker's primary partitions reached their handoff barrier: `(partition, last)`.
    HandoffReached { req: u64, last: Vec<(u16, u64)> },
    /// Primary: on `partition`, `member` is caught up and counted (or no longer).
    Expand {
        shard: u16,
        term: u32,
        member: NodeId,
        partition: u16,
        counted: bool,
    },
    /// Primary: `member` has unacknowledged groups older than `repl-ack-timeout-ms`.
    Shrink {
        shard: u16,
        term: u32,
        member: NodeId,
    },
    /// Member: `partition` streams the current term after any repair or full sync (`ready`),
    /// or started one (`!ready`).
    PartReady {
        shard: u16,
        term: u32,
        partition: u16,
        ready: bool,
    },
    /// Migration source: `partition` reached its last barrier.
    MigReady { id: u64, partition: u16 },
    /// Migration destination: `partition` committed everything up to the seal.
    Sealed { id: u64, partition: u16 },
    /// Primary: `member`'s handshake was refused with NeedOperatorResync (it cannot catch up
    /// by Continue or Repair).
    NeedsResync { shard: u16, term: u32, member: NodeId },
}

/// Node-wide cluster state (one per process, cluster mode only).
pub struct ClusterShared {
    /// Random per process start (`HelloAck`, `Renew`).
    pub incarnation: u64,
    pub boot: BootMeta,
    /// The meta thread (node meta writes); released at shutdown so that the meta thread can
    /// end once the partitions are gone.
    meta: parking_lot::Mutex<Option<MetaClient>>,
    /// The cluster token (constant-time compared in every handshake).
    pub auth: Vec<u8>,
    to_agent: tokio::sync::mpsc::UnboundedSender<AgentMsg>,
    agent_rx: parking_lot::Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<AgentMsg>>>,
    /// The node's identity once it joined (CLUSTER MYID, replication handshakes).
    pub identity: parking_lot::RwLock<Identity>,
    /// The last applied assignment.
    pub assignment: parking_lot::RwLock<Option<Arc<NodeAssignment>>>,
    /// Per partition: a merge full sync runs (replica reads refused, §23.6 rule 4).
    pub syncing: Arc<[AtomicBool]>,
    /// A graceful handoff runs: new writes answer TRYAGAIN (§23.12).
    pub handoff: Arc<AtomicBool>,
    /// The replication transport (started once the node is up).
    pub net: std::sync::OnceLock<net::ReplNet>,
    next_token: AtomicU64,
    /// After an unclean restart: the highest term this node may have served as a primary
    /// before it (its persisted fence term). A primary role of a term at or below it is never
    /// served again: the recovered partitions may lack groups the previous process produced
    /// and shipped, so its next seqs could repeat ones a member already holds. Only a newer
    /// term (a promotion through the normal failover path) makes it a primary again.
    pub restart_term: Option<u32>,
    /// The current assignment names this node primary of a term it must not serve (see
    /// `restart_term`), or, before the first assignment, it was a primary when it stopped:
    /// renewals report `health.failed`, so the coordinator flags it and fails the shard over
    /// (a new coordinator leader cannot tell a restart from a first renewal otherwise).
    pub refusing_primary: AtomicBool,
    /// SIGTERM on a primary: since when the node waits for the coordinator to hand its shard
    /// off before it shuts down (§23.12); bounded by the agent.
    shutdown_handoff: parking_lot::Mutex<Option<std::time::Instant>>,
    /// Per partition, refreshed by the owning worker's tick: a primary partition's members with
    /// an attached stream (bit = member index, bits 0..62); a member partition sets bit 63
    /// while its stream to the primary has been accepted. INFO replication reads these
    /// (`connected_slaves`, `master_link_status`, LC-24: no ids or positions).
    links: Box<[AtomicU64]>,
    /// Per partition: keys stored in slots this node does not serve (an inert migration copy,
    /// slots being purged), refreshed every 50 ms; DBSIZE in cluster mode leaves them out.
    unserved_keys: Box<[AtomicU64]>,
}

/// `links` bit of a member partition whose stream to the primary is up.
pub const LINK_UP: u64 = 1 << 63;

/// Who this node is in the cluster.
#[derive(Clone, Copy)]
pub struct Identity {
    pub cluster_id: Option<[u8; 16]>,
    pub node_id: Option<NodeId>,
    pub fence_term: u32,
    pub version: u64,
}

impl ClusterShared {
    pub fn new(boot: BootMeta, meta: MetaClient, auth: Vec<u8>, partitions: u16) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let joined = boot.node.node_id != [0u8; 20];
        let restart_term = (joined && !boot.clean).then_some(boot.node.fence_term);
        let was_primary = boot.node.role == kv_storage::pmeta::ROLE_PRIMARY;
        let identity = Identity {
            cluster_id: joined.then_some(boot.node.cluster_id),
            node_id: joined.then_some(NodeId(boot.node.node_id)),
            fence_term: boot.node.fence_term,
            version: boot.node.assignment_version,
        };
        ClusterShared {
            incarnation: random_u64(),
            boot,
            meta: parking_lot::Mutex::new(Some(meta)),
            auth,
            to_agent: tx,
            agent_rx: parking_lot::Mutex::new(Some(rx)),
            identity: parking_lot::RwLock::new(identity),
            assignment: parking_lot::RwLock::new(None),
            syncing: (0..partitions)
                .map(|_| AtomicBool::new(false))
                .collect::<Vec<_>>()
                .into(),
            handoff: Arc::new(AtomicBool::new(false)),
            net: std::sync::OnceLock::new(),
            next_token: AtomicU64::new(1),
            restart_term,
            refusing_primary: AtomicBool::new(restart_term.is_some() && was_primary),
            shutdown_handoff: parking_lot::Mutex::new(None),
            links: (0..partitions).map(|_| AtomicU64::new(0)).collect(),
            unserved_keys: (0..partitions).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    /// Whether `a` names this node primary of a term it must not serve after an unclean
    /// restart (see [`ClusterShared::restart_term`]).
    pub fn refuses_primary(&self, a: &NodeAssignment) -> bool {
        match (self.restart_term, a.role.as_ref()) {
            (Some(rt), Some(r)) => r.is_primary && r.term <= rt,
            _ => false,
        }
    }

    /// SIGTERM: when this node is a shard primary, starts the handoff-before-shutdown (the
    /// agent asks the coordinator for a graceful handoff and shuts the node down once it is
    /// no longer the primary, or after a bound) and returns true; false means shut down now
    /// (not a primary, or a second signal).
    pub fn begin_shutdown_handoff(&self) -> bool {
        // only a primary with an in-sync ISR member to hand off to (else nobody could take
        // the shard and the node stops at once; its shard fails over or waits for it)
        let primary = self.assignment().and_then(|a| {
            a.role.as_ref().map(|r| {
                r.is_primary
                    && r.members
                        .iter()
                        .any(|(_, st, in_isr)| *in_isr && *st == kv_ctrl::MemberState::InSync)
            })
        });
        let mut h = self.shutdown_handoff.lock();
        if primary != Some(true) || h.is_some() {
            return false;
        }
        *h = Some(std::time::Instant::now());
        true
    }

    /// Since when a handoff-before-shutdown runs (see [`ClusterShared::begin_shutdown_handoff`]).
    pub fn shutdown_handoff_since(&self) -> Option<std::time::Instant> {
        *self.shutdown_handoff.lock()
    }

    /// Records partition `p`'s replication links (see `links`).
    pub fn set_links(&self, p: u16, v: u64) {
        if let Some(a) = self.links.get(usize::from(p)) {
            a.store(v, Ordering::Relaxed);
        }
    }

    /// Members with an attached stream on every partition (INFO `connected_slaves`).
    pub fn connected_members(&self) -> u32 {
        if self.links.is_empty() {
            return 0;
        }
        self.links.iter().fold(!LINK_UP, |acc, a| acc & a.load(Ordering::Relaxed)).count_ones()
    }

    /// Every partition's stream to the primary is up (INFO `master_link_status:up`).
    pub fn member_links_up(&self) -> bool {
        !self.links.is_empty() && self.links.iter().all(|a| a.load(Ordering::Relaxed) & LINK_UP != 0)
    }

    /// Records the keys of partition `p` in slots this node does not serve.
    pub fn set_unserved_keys(&self, p: u16, n: u64) {
        if let Some(a) = self.unserved_keys.get(usize::from(p)) {
            a.store(n, Ordering::Relaxed);
        }
    }

    /// Keys stored in slots this node does not serve, over all partitions.
    pub fn unserved_keys(&self) -> u64 {
        self.unserved_keys.iter().map(|a| a.load(Ordering::Relaxed)).sum()
    }

    /// A node-meta request; `false` once the node is shutting down.
    pub fn meta_request(
        &self,
        partition: u16,
        token: u64,
        r: kv_storage::metastore::MetaReq,
    ) -> bool {
        match self.meta.lock().as_ref() {
            Some(m) => {
                m.request(partition, token, r);
                true
            }
            None => false,
        }
    }

    /// Shutdown: drops this state's meta client.
    pub fn release_meta(&self) {
        self.meta.lock().take();
    }

    /// Delivers a message to the agent (never blocks).
    pub fn tell_agent(&self, m: AgentMsg) {
        let _ = self.to_agent.send(m);
    }

    /// The agent's receiving end (taken once).
    pub fn take_agent_rx(&self) -> Option<tokio::sync::mpsc::UnboundedReceiver<AgentMsg>> {
        self.agent_rx.lock().take()
    }

    /// A fresh meta token with `bit` set.
    pub fn token(&self, bit: u64) -> u64 {
        bit | (self.next_token.fetch_add(1, Ordering::Relaxed) & (REPL_TOKEN_BIT - 1))
    }

    pub fn identity(&self) -> Identity {
        *self.identity.read()
    }

    /// The node's fence term (frames of lower terms are refused).
    pub fn fence_term(&self) -> u32 {
        self.identity.read().fence_term
    }

    pub fn assignment(&self) -> Option<Arc<NodeAssignment>> {
        self.assignment.read().clone()
    }

    /// The persisted replication position of partition `p` at startup (Hello `applied`).
    pub fn boot_part(&self, p: u16) -> Option<&PartitionMeta> {
        self.boot.parts.get(usize::from(p))
    }
}

/// Constant-time byte comparison (handshake tokens).
pub fn same_token(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// 64 random bits from the kernel (falls back to the clocks).
pub fn random_u64() -> u64 {
    use std::io::Read;
    let mut b = [0u8; 8];
    if std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut b))
        .is_ok()
    {
        return u64::from_le_bytes(b);
    }
    let n = kv_common::Now::read();
    n.mono_ns.rotate_left(17) ^ n.boot_ns ^ u64::from(std::process::id())
}

/// The cluster control port (`cluster-control-port`, default `port + 10000`).
pub fn control_port(cfg: &crate::config::Config) -> u16 {
    cfg.cluster_control_port
        .unwrap_or_else(|| (u32::from(cfg.port) + 10_000).min(65_535) as u16)
}

/// The replication port (`cluster-repl-port`, default `port + 20000`).
pub fn repl_port(cfg: &crate::config::Config) -> u16 {
    cfg.cluster_repl_port
        .unwrap_or_else(|| (u32::from(cfg.port) + 20_000).min(65_535) as u16)
}

/// The address the node's cluster listeners bind (the first `bind` address).
pub fn bind_ip(cfg: &crate::config::Config) -> std::net::IpAddr {
    cfg.bind
        .first()
        .and_then(|b| b.trim_start_matches('-').parse().ok())
        .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED))
}

/// The seq before `s` (the last position a stream starting at `s` already holds).
pub fn before(s: Seq) -> Seq {
    Seq(s.0.saturating_sub(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_compare_in_constant_time_shape() {
        assert!(same_token(b"abc", b"abc"));
        assert!(!same_token(b"abc", b"abd"));
        assert!(!same_token(b"abc", b"ab"));
        assert!(same_token(b"", b""));
    }

    fn assignment(primary: bool, term: u32) -> NodeAssignment {
        NodeAssignment {
            version: 9,
            coord_term: 1,
            node_id: NodeId([4; 20]),
            coordinators: Vec::new(),
            role: Some(kv_ctrl::ShardRole {
                shard: kv_ctrl::ShardId(1),
                term,
                is_primary: primary,
                owned_slots: kv_common::SlotSet::new(),
                primary_repl: None,
                members: Vec::new(),
                min_isr: 1,
                history: Vec::new(),
                migrations: Vec::new(),
            }),
            client_password: None,
            params: kv_ctrl::NodeParams::default(),
        }
    }

    fn shared_after(clean: bool, role: u8, fence_term: u32) -> ClusterShared {
        let mut boot = BootMeta::empty(4);
        boot.clean = clean;
        boot.node.node_id = [4; 20];
        boot.node.role = role;
        boot.node.fence_term = fence_term;
        ClusterShared::new(boot, MetaClient::disconnected(), b"t".to_vec(), 4)
    }

    /// After an unclean restart a primary role of the term served before is refused (its next
    /// seqs could repeat ones a member holds) and renewals report failed until a newer term
    /// is assigned; a clean restart resumes, and a replica never refuses its member role.
    #[test]
    fn a_restarted_primary_never_resumes_its_term() {
        use kv_storage::pmeta::{ROLE_PRIMARY, ROLE_REPLICA};
        let cs = shared_after(false, ROLE_PRIMARY, 5);
        assert_eq!(cs.restart_term, Some(5));
        assert!(cs.refusing_primary.load(Ordering::Acquire), "failed until an assignment");
        assert!(cs.refuses_primary(&assignment(true, 5)));
        assert!(cs.refuses_primary(&assignment(true, 4)));
        assert!(!cs.refuses_primary(&assignment(true, 6)), "a promotion to a newer term");
        assert!(!cs.refuses_primary(&assignment(false, 5)), "member of the same term");
        let clean = shared_after(true, ROLE_PRIMARY, 5);
        assert_eq!(clean.restart_term, None);
        assert!(!clean.refusing_primary.load(Ordering::Acquire));
        assert!(!clean.refuses_primary(&assignment(true, 5)));
        let replica = shared_after(false, ROLE_REPLICA, 5);
        assert!(!replica.refusing_primary.load(Ordering::Acquire));
        assert!(replica.refuses_primary(&assignment(true, 5)), "fenced for 5 before the crash");
    }

    #[test]
    fn replication_links_for_info() {
        let cs = shared_after(true, 0, 0);
        assert_eq!(cs.connected_members(), 0);
        assert!(!cs.member_links_up());
        for p in 0..4 {
            cs.set_links(p, 0b101);
        }
        assert_eq!(cs.connected_members(), 2);
        cs.set_links(2, 0b001);
        assert_eq!(cs.connected_members(), 1, "attached on every partition");
        for p in 0..4 {
            cs.set_links(p, LINK_UP);
        }
        assert!(cs.member_links_up());
        assert_eq!(cs.connected_members(), 0);
        cs.set_links(3, 0);
        assert!(!cs.member_links_up());
        cs.set_unserved_keys(1, 7);
        cs.set_unserved_keys(3, 5);
        assert_eq!(cs.unserved_keys(), 12);
    }

    #[test]
    fn token_bits_do_not_collide() {
        assert_eq!(AGENT_TOKEN_BIT & REPL_TOKEN_BIT, 0);
        assert_eq!(AGENT_TOKEN_BIT & crate::runtime::exec::WORKER_TOKEN_BIT, 0);
        assert_eq!(REPL_TOKEN_BIT & crate::runtime::exec::WORKER_TOKEN_BIT, 0);
        assert_eq!(before(Seq(5)), Seq(4));
        assert_eq!(before(Seq(0)), Seq(0));
    }
}
