//! The slot map object (DESIGN §23.2).
//!
//! `version` is +1 per committed batch of transitions (the only map version); `cluster_id` is
//! drawn once at `kvc init`. Collections are `Vec`s in operator order; the canonical encoding is
//! one format byte followed by postcard of the struct with Vecs in stored order, the slot array
//! run-length encoded. Clients never receive terms or seqs (LC-28). A batch whose resulting
//! encoded state is larger than 8 MiB - 64 KiB is rejected (`StateTooLarge`, §23.3).
//!
//! The state holds no coordinator list: each coordinator's peer list lives in its own
//! `coord.meta`, outside the voted value (LC-38).
//!
//! Failover helpers ([`ClusterState::promotion_order`], [`ClusterState::choose_primary`],
//! [`ClusterState::failover_promote`], [`static_choice`]) implement the static rule of §23.9:
//! eligibility comes from the committed ISR, the coordinator's own lease table and the
//! operator's maintenance flags; the choice is highest operator priority, then earliest
//! position in the shard's operator-ordered member list. Node ids, replication positions and
//! reply timing never take part (LC-25, LC-27).

use core::fmt;
use std::net::SocketAddr;

use kv_common::{SLOTS, SlotSet};
use kv_ctrl::{
    CoordEndpoint, MigrationTask, NodeAssignment, NodeId, NodeParams, PeerAddr, ShardId, ShardRole,
};
pub use kv_ctrl::{MemberState, MigPhase, SlotRanges, TermEnd};
use serde::{Deserialize, Serialize};

use crate::transition::{Reject, Transition, apply_one, is_secret_key, normalize_key};

/// `slots[s]` value of an unassigned slot.
pub const SLOT_UNASSIGNED: u16 = 0xFFFF;

/// Largest encoded state a batch may produce (8 MiB state half minus 64 KiB of header room).
pub const MAX_STATE_BYTES: usize = 8 * 1024 * 1024 - 64 * 1024;

/// Format byte that starts the canonical encoding.
pub const STATE_FORMAT_VERSION: u8 = 1;

/// Past terms kept per shard (`history`, last 4).
pub const MAX_HISTORY: usize = 4;

/// Finished migrations kept for `kvadm migrations`; the oldest are dropped when an operator
/// starts a new migration.
pub const MAX_DONE_MIGRATIONS: usize = 64;

/// Longest zone label.
pub const MAX_ZONE_LEN: usize = 64;

/// Longest client password.
pub const MAX_PASSWORD_LEN: usize = 512;

/// Largest `min_isr` (it counts the primary).
pub const MAX_MIN_ISR: u8 = 16;

/// Largest `--barriers` value of a migration.
pub const MAX_BARRIERS: u8 = 16;

/// Partition counts a cluster may use (P, §5).
pub const PARTITION_CHOICES: [u16; 4] = [16, 32, 64, 128];

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterState {
    /// +1 per committed batch of transitions; the only map version
    pub version: u64,
    /// random once at `kvc init`; never per transition
    pub cluster_id: [u8; 16],
    pub params: ClusterParams,
    /// operator order
    pub nodes: Vec<NodeRecord>,
    pub shards: Vec<ShardRecord>,
    /// ShardId or 0xFFFF; run-length encoded on the wire
    #[serde(with = "slots_rle")]
    pub slots: Box<[u16; 16384]>,
    pub migrations: Vec<Migration>,
}

/// Cluster parameters (no coordinator list: peer lists live in each coord.meta, §23.3).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterParams {
    /// P, cluster-wide constant
    pub partitions: u16,
    pub lease_ms: u32,
    pub renew_ms: u32,
    pub fail_grace_ms: u32,
    pub repl_ack_timeout_ms: u32,
    /// Pushed to nodes as `requirepass`; never shown by `Debug`.
    pub client_password: Option<String>,
}

impl Default for ClusterParams {
    fn default() -> Self {
        ClusterParams {
            partitions: 64,
            lease_ms: 1500,
            renew_ms: 100,
            fail_grace_ms: 200,
            repl_ack_timeout_ms: 250,
            client_password: None,
        }
    }
}

impl fmt::Debug for ClusterParams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusterParams")
            .field("partitions", &self.partitions)
            .field("lease_ms", &self.lease_ms)
            .field("renew_ms", &self.renew_ms)
            .field("fail_grace_ms", &self.fail_grace_ms)
            .field("repl_ack_timeout_ms", &self.repl_ack_timeout_ms)
            .field(
                "client_password",
                &self
                    .client_password
                    .as_ref()
                    .map(|_| format_args!("<redacted>")),
            )
            .finish()
    }
}

fn parse_u32(v: &str) -> Result<u32, Reject> {
    let v = v.trim();
    if v.is_empty() || !v.bytes().all(|c| c.is_ascii_digit()) {
        return Err(Reject::Precondition(
            "parameter value must be a non-negative integer".into(),
        ));
    }
    v.parse()
        .map_err(|_| Reject::Precondition("parameter value out of range".into()))
}

impl ClusterParams {
    /// The parameters a data node receives in its assignment.
    pub fn node_params(&self) -> NodeParams {
        NodeParams {
            partitions: self.partitions,
            lease_ms: self.lease_ms,
            renew_ms: self.renew_ms,
            repl_ack_timeout_ms: self.repl_ack_timeout_ms,
        }
    }

    /// Range checks of every parameter (`kvc init` and `ParamSet`).
    pub fn validate(&self) -> Result<(), Reject> {
        let bad = |s: &str| Err(Reject::Precondition(s.into()));
        if !PARTITION_CHOICES.contains(&self.partitions) {
            return bad("partitions must be 16, 32, 64 or 128");
        }
        if !(10..=10_000).contains(&self.renew_ms) {
            return bad("renew_ms must be between 10 and 10000");
        }
        if !(100..=600_000).contains(&self.lease_ms)
            || u64::from(self.lease_ms) < 3 * u64::from(self.renew_ms)
        {
            return bad("lease_ms must be between 100 and 600000 and at least 3 x renew_ms");
        }
        if self.fail_grace_ms > 600_000 {
            return bad("fail_grace_ms must be at most 600000");
        }
        if !(1..=600_000).contains(&self.repl_ack_timeout_ms) {
            return bad("repl_ack_timeout_ms must be between 1 and 600000");
        }
        if self
            .client_password
            .as_ref()
            .is_some_and(|p| p.is_empty() || p.len() > MAX_PASSWORD_LEN)
        {
            return bad("client_password must be 1 to 512 bytes");
        }
        Ok(())
    }

    /// `kvadm cluster params set <key> <value>`. Keys: `partitions` (only while no node is
    /// registered), `lease_ms`, `renew_ms`, `fail_grace_ms`, `repl_ack_timeout_ms`,
    /// `client_password` (alias `requirepass`; an empty value clears it); `-` and `_` are
    /// interchangeable. Nothing changes unless the resulting parameter set is valid.
    pub fn set(&mut self, key: &str, value: &str, partitions_settable: bool) -> Result<(), Reject> {
        let mut next = self.clone();
        match normalize_key(key).as_str() {
            "partitions" => {
                if !partitions_settable {
                    return Err(Reject::Precondition(
                        "partitions is fixed once a node is registered (nodes are formatted with it)".into(),
                    ));
                }
                let v = parse_u32(value)?;
                next.partitions = u16::try_from(v).map_err(|_| {
                    Reject::Precondition("partitions must be 16, 32, 64 or 128".into())
                })?;
            }
            "lease_ms" => next.lease_ms = parse_u32(value)?,
            "renew_ms" => next.renew_ms = parse_u32(value)?,
            "fail_grace_ms" => next.fail_grace_ms = parse_u32(value)?,
            "repl_ack_timeout_ms" => next.repl_ack_timeout_ms = parse_u32(value)?,
            k if is_secret_key(k) => {
                next.client_password = if value.is_empty() {
                    None
                } else {
                    Some(value.to_owned())
                };
            }
            _ => return Err(Reject::Precondition("unknown cluster parameter".into())),
        }
        next.validate()?;
        *self = next;
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
pub struct NodeFlags {
    pub fail: bool,
    pub maint: bool,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct NodeRecord {
    pub id: NodeId,
    pub ctrl: SocketAddr,
    pub client: SocketAddr,
    pub repl: SocketAddr,
    /// Operator-assigned availability zone label.
    pub zone: String,
    /// Static operator priority, used only by the coordinator's static failover choice.
    pub priority: u8,
    pub flags: NodeFlags,
    pub partitions: u16,
    pub added_version: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum ShardStatus {
    Serving,
    NoPrimary,
    Promoting,
}

impl ShardStatus {
    /// Lowercase name used by `kvadm status`.
    pub fn as_str(self) -> &'static str {
        match self {
            ShardStatus::Serving => "serving",
            ShardStatus::NoPrimary => "no-primary",
            ShardStatus::Promoting => "promoting",
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ShardRecord {
    pub id: ShardId,
    pub primary: Option<NodeId>,
    /// operator order
    pub members: Vec<Member>,
    pub term: u32,
    pub min_isr: u8,
    /// last 4
    pub history: Vec<TermEnd>,
    pub status: ShardStatus,
}

impl ShardRecord {
    pub fn member(&self, node: &NodeId) -> Option<&Member> {
        self.members.iter().find(|m| m.node == *node)
    }

    /// Committed ISR members (the primary included), in member order.
    pub fn isr(&self) -> impl Iterator<Item = &Member> + '_ {
        self.members.iter().filter(|m| m.in_isr)
    }

    pub fn is_primary(&self, node: &NodeId) -> bool {
        self.primary == Some(*node)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Member {
    pub node: NodeId,
    pub state: MemberState,
    pub in_isr: bool,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Migration {
    pub id: u64,
    pub slots: SlotRanges,
    pub src: ShardId,
    pub dst: ShardId,
    pub phase: MigPhase,
    /// a static token-bucket rate, never a priority
    pub rate_mbps: Option<u32>,
    /// Further position barriers after the copy (`--barriers`, default 2).
    pub barriers: u8,
}

/// Static operator data of one failover candidate: its priority and its position in the
/// shard's operator-ordered member list. No id, position, score or reply field (LC-25, LC-27).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct StaticRank {
    pub priority: u8,
    pub order: usize,
}

/// The static rule of §23.9 over `eligible`, skipping the indexes in `unusable`: highest
/// priority, then earliest member order (then earliest index, for malformed input with equal
/// orders). Returns an index into `eligible`.
pub fn static_choice(eligible: &[StaticRank], unusable: &[usize]) -> Option<usize> {
    let mut best: Option<usize> = None;
    for (i, c) in eligible.iter().enumerate() {
        if unusable.contains(&i) {
            continue;
        }
        let better = match best {
            None => true,
            Some(b) => {
                let bc = eligible[b];
                c.priority > bc.priority || (c.priority == bc.priority && c.order < bc.order)
            }
        };
        if better {
            best = Some(i);
        }
    }
    best
}

impl ClusterState {
    /// Version 1, no nodes, no shards, every slot unassigned (`kvc init`, §23.12).
    pub fn initial(params: ClusterParams, cluster_id: [u8; 16]) -> Self {
        ClusterState {
            version: 1,
            cluster_id,
            params,
            nodes: Vec::new(),
            shards: Vec::new(),
            slots: Box::new([SLOT_UNASSIGNED; SLOTS]),
            migrations: Vec::new(),
        }
    }

    /// pure; version + 1
    ///
    /// Applies the transitions in order to a copy; the whole batch is refused if any one is.
    /// An empty batch commits a new version with no other change.
    pub fn apply(&self, batch: &[Transition]) -> Result<ClusterState, Reject> {
        self.apply_with_encoding(batch).map(|(s, _)| s)
    }

    /// [`ClusterState::apply`] plus the canonical encoding of the result (computed anyway for
    /// the size limit), so a caller that persists the state does not encode it twice.
    pub fn apply_with_encoding(
        &self,
        batch: &[Transition],
    ) -> Result<(ClusterState, Vec<u8>), Reject> {
        let version = self.version.checked_add(1).ok_or(Reject::Malformed)?;
        let mut next = self.clone();
        next.version = version;
        for t in batch {
            apply_one(&mut next, t, version)?;
        }
        debug_assert_eq!(next.check_invariants(), Ok(()));
        let bytes = next.encode();
        if bytes.len() > MAX_STATE_BYTES {
            return Err(Reject::StateTooLarge);
        }
        Ok((next, bytes))
    }

    /// Canonical encoding: [`STATE_FORMAT_VERSION`] followed by postcard of the struct.
    pub fn encode(&self) -> Vec<u8> {
        // Serializing plain data into a Vec cannot fail (every sequence has a known length).
        postcard::to_extend(self, vec![STATE_FORMAT_VERSION])
            .expect("cluster state always serializes")
    }

    /// Inverse of [`ClusterState::encode`]; also checks the structural invariants.
    pub fn decode(b: &[u8]) -> Result<Self, Reject> {
        let (&format, rest) = b.split_first().ok_or(Reject::Malformed)?;
        if format != STATE_FORMAT_VERSION {
            return Err(Reject::Malformed);
        }
        let (s, tail) =
            postcard::take_from_bytes::<ClusterState>(rest).map_err(|_| Reject::Malformed)?;
        if !tail.is_empty() {
            return Err(Reject::Malformed);
        }
        s.check_invariants().map_err(|_| Reject::Malformed)?;
        Ok(s)
    }

    /// Structural invariants every committed state satisfies: unique node ids and addresses,
    /// unique shard ids, members are registered nodes in at most one shard, the primary is an
    /// ISR member, slots name existing shards, unfinished migrations reference existing shards
    /// with disjoint slots owned by their source (destination during cutover).
    pub fn check_invariants(&self) -> Result<(), &'static str> {
        for (i, n) in self.nodes.iter().enumerate() {
            for o in &self.nodes[..i] {
                if o.id == n.id {
                    return Err("duplicate node id");
                }
                if o.client == n.client || o.ctrl == n.ctrl || o.repl == n.repl {
                    return Err("duplicate node address");
                }
            }
        }
        for (i, sh) in self.shards.iter().enumerate() {
            if sh.id.0 == SLOT_UNASSIGNED {
                return Err("reserved shard id");
            }
            if self.shards[..i].iter().any(|o| o.id == sh.id) {
                return Err("duplicate shard id");
            }
            if sh.term == 0 || sh.min_isr == 0 {
                return Err("shard term and min_isr start at 1");
            }
            if sh.history.len() > MAX_HISTORY
                || sh.history.windows(2).any(|w| w[0].term >= w[1].term)
                || sh.history.last().is_some_and(|h| h.term >= sh.term)
            {
                return Err("shard history must hold past terms in increasing order");
            }
            for (j, m) in sh.members.iter().enumerate() {
                if self.node(&m.node).is_none() {
                    return Err("member is not a registered node");
                }
                if sh.members[..j].iter().any(|o| o.node == m.node) {
                    return Err("duplicate member");
                }
                if self.shards[..i].iter().any(|o| o.member(&m.node).is_some()) {
                    return Err("node is a member of two shards");
                }
            }
            match sh.primary {
                Some(p) => match sh.member(&p) {
                    Some(m) if m.in_isr => {}
                    _ => return Err("primary must be an ISR member"),
                },
                None if sh.status == ShardStatus::Serving => {
                    return Err("serving shard without primary");
                }
                None => {}
            }
        }
        for (_, _, id) in self.slot_runs() {
            if self.shard(id).is_none() {
                return Err("slot assigned to an unknown shard");
            }
        }
        for (i, m) in self.migrations.iter().enumerate() {
            if self.migrations[..i].iter().any(|o| o.id == m.id) {
                return Err("duplicate migration id");
            }
            if m.src == m.dst || self.shard(m.src).is_none() || self.shard(m.dst).is_none() {
                return Err("migration must reference two existing shards");
            }
            if m.slots.is_empty() || m.slots.normalized().as_ref() != Some(&m.slots) {
                return Err("migration slot ranges must be normalized and non-empty");
            }
            if m.phase == MigPhase::Done {
                continue;
            }
            let owner = if m.phase == MigPhase::Cutover {
                m.dst
            } else {
                m.src
            };
            if m.slots
                .iter_slots()
                .any(|s| self.slots[usize::from(s)] != owner.0)
            {
                return Err(
                    "unfinished migration slots must be owned by its source (destination in cutover)",
                );
            }
            for o in &self.migrations[..i] {
                if o.phase != MigPhase::Done && m.slots.iter_slots().any(|s| o.slots.contains(s)) {
                    return Err("unfinished migrations overlap");
                }
            }
        }
        Ok(())
    }

    pub fn node(&self, id: &NodeId) -> Option<&NodeRecord> {
        self.nodes.iter().find(|n| n.id == *id)
    }

    pub fn shard(&self, id: ShardId) -> Option<&ShardRecord> {
        self.shards.iter().find(|s| s.id == id)
    }

    /// The shard the node is a member of (a node has at most one role).
    pub fn shard_of_node(&self, id: &NodeId) -> Option<&ShardRecord> {
        self.shards.iter().find(|s| s.member(id).is_some())
    }

    /// The shard id assigned to `slot`, if any.
    pub fn slot_owner(&self, slot: u16) -> Option<ShardId> {
        let v = *self.slots.get(usize::from(slot))?;
        (v != SLOT_UNASSIGNED).then_some(ShardId(v))
    }

    pub fn shard_of_slot(&self, slot: u16) -> Option<&ShardRecord> {
        self.slot_owner(slot).and_then(|id| self.shard(id))
    }

    /// The slots assigned to `shard`.
    pub fn slot_set_of(&self, shard: ShardId) -> SlotSet {
        let mut set = SlotSet::new();
        for (s, &v) in self.slots.iter().enumerate() {
            if v == shard.0 {
                // s < 16384
                set.insert(s as u16);
            }
        }
        set
    }

    /// The slots assigned to `shard` as ascending maximal ranges.
    pub fn slot_ranges_of(&self, shard: ShardId) -> SlotRanges {
        let mut out = Vec::new();
        for (a, b, id) in self.slot_runs() {
            if id == shard {
                out.push((a, b));
            }
        }
        SlotRanges(out)
    }

    /// Maximal runs `(first, last, shard)` of assigned slots, ascending.
    pub fn slot_runs(&self) -> Vec<(u16, u16, ShardId)> {
        let mut runs = Vec::new();
        let mut i = 0usize;
        while i < SLOTS {
            let v = self.slots[i];
            let start = i;
            while i < SLOTS && self.slots[i] == v {
                i += 1;
            }
            if v != SLOT_UNASSIGNED {
                // start, i - 1 < 16384
                runs.push((start as u16, (i - 1) as u16, ShardId(v)));
            }
        }
        runs
    }

    /// Number of assigned slots.
    pub fn assigned_slots(&self) -> usize {
        self.slots.iter().filter(|&&v| v != SLOT_UNASSIGNED).count()
    }

    /// True when the shard can take writes as far as committed state says: serving status and
    /// a primary without a FAIL flag.
    pub fn shard_serving(&self, sh: &ShardRecord) -> bool {
        sh.status == ShardStatus::Serving
            && sh
                .primary
                .and_then(|p| self.node(&p))
                .is_some_and(|n| !n.flags.fail)
    }

    /// The node's projection of this state (§23.4): its own role in its own shard only.
    ///
    /// * A primary gets `is_primary` only while its shard is `Serving`; it receives its peers
    ///   (`(node, state, in_isr)`, no addresses) and its migration tasks (`Out`, or `In` with the
    ///   source primary's replication address).
    /// * A replica gets the primary's replication address while the shard is `Serving`, no
    ///   member list and no migration tasks.
    /// * `owned_slots` are the slots assigned to the shard; a node with no role gets `None`.
    pub fn assignment_for(
        &self,
        node: &NodeId,
        coord_term: u64,
        coordinators: &[CoordEndpoint], /* static peer list order */
    ) -> NodeAssignment {
        let role = self.shard_of_node(node).map(|sh| {
            let serving = sh.status == ShardStatus::Serving;
            let is_primary = serving && sh.is_primary(node);
            let primary_repl = if serving && !is_primary {
                sh.primary
                    .and_then(|p| self.node(&p))
                    .map(|n| PeerAddr(n.repl))
            } else {
                None
            };
            let (members, migrations) = if is_primary {
                let members = sh
                    .members
                    .iter()
                    .filter(|m| m.node != *node)
                    .map(|m| (m.node, m.state, m.in_isr))
                    .collect();
                (members, self.migration_tasks(sh.id))
            } else {
                (Vec::new(), Vec::new())
            };
            ShardRole {
                shard: sh.id,
                term: sh.term,
                is_primary,
                owned_slots: self.slot_set_of(sh.id),
                primary_repl,
                members,
                min_isr: sh.min_isr,
                history: sh.history.clone(),
                migrations,
            }
        });
        NodeAssignment {
            version: self.version,
            coord_term,
            node_id: *node,
            coordinators: coordinators.to_vec(),
            role,
            client_password: self.params.client_password.clone(),
            params: self.params.node_params(),
        }
    }

    fn migration_tasks(&self, shard: ShardId) -> Vec<MigrationTask> {
        let mut out = Vec::new();
        for m in &self.migrations {
            if m.src == shard {
                out.push(MigrationTask::Out {
                    id: m.id,
                    slots: m.slots.clone(),
                    phase: m.phase,
                });
            } else if m.dst == shard {
                let src_repl = self
                    .shard(m.src)
                    .and_then(|s| s.primary)
                    .and_then(|p| self.node(&p))
                    .map(|n| n.repl);
                if let Some(a) = src_repl {
                    out.push(MigrationTask::In {
                        id: m.id,
                        slots: m.slots.clone(),
                        src_repl: PeerAddr(a),
                        phase: m.phase,
                    });
                }
            }
        }
        out
    }

    /// Candidates of a failover of `shard`, in the order the coordinator fences them (§23.9):
    /// committed ISR minus the current primary, restricted to members whose lease the
    /// coordinator holds (`lease_held`, its own lease table) and that are not flagged FAIL;
    /// members not in maintenance first, each group by the static rule (highest priority, then
    /// earliest member position). Empty when the shard is unknown or nobody is eligible.
    pub fn promotion_order(
        &self,
        shard: ShardId,
        lease_held: &dyn Fn(&NodeId) -> bool,
    ) -> Vec<NodeId> {
        self.static_order(shard, lease_held, false)
    }

    /// Candidates of a graceful handoff of `shard` without `--to` (§23.12): the same rule over
    /// InSync ISR members with a held lease.
    pub fn handoff_order(
        &self,
        shard: ShardId,
        lease_held: &dyn Fn(&NodeId) -> bool,
    ) -> Vec<NodeId> {
        self.static_order(shard, lease_held, true)
    }

    fn static_order(
        &self,
        shard: ShardId,
        lease_held: &dyn Fn(&NodeId) -> bool,
        in_sync_only: bool,
    ) -> Vec<NodeId> {
        let Some(sh) = self.shard(shard) else {
            return Vec::new();
        };
        let mut groups: [Vec<(StaticRank, NodeId)>; 2] = [Vec::new(), Vec::new()];
        for (order, m) in sh.members.iter().enumerate() {
            if !m.in_isr
                || sh.is_primary(&m.node)
                || (in_sync_only && m.state != MemberState::InSync)
            {
                continue;
            }
            let Some(rec) = self.node(&m.node) else {
                continue;
            };
            if rec.flags.fail || !lease_held(&m.node) {
                continue;
            }
            groups[usize::from(rec.flags.maint)].push((
                StaticRank {
                    priority: rec.priority,
                    order,
                },
                m.node,
            ));
        }
        let mut out = Vec::new();
        for mut g in groups {
            while !g.is_empty() {
                let ranks: Vec<StaticRank> = g.iter().map(|(r, _)| *r).collect();
                let Some(i) = static_choice(&ranks, &[]) else {
                    break;
                };
                out.push(g.remove(i).1);
            }
        }
        out
    }

    /// The first node of [`ClusterState::promotion_order`] not in `unusable` (nodes that did
    /// not answer their fence within the timeout, §23.9 step 3).
    pub fn choose_primary(
        &self,
        shard: ShardId,
        lease_held: &dyn Fn(&NodeId) -> bool,
        unusable: &[NodeId],
    ) -> Option<NodeId> {
        self.promotion_order(shard, lease_held)
            .into_iter()
            .find(|n| !unusable.contains(n))
    }

    /// The `Promote` the coordinator commits after fencing `primary` (§23.9 step 5):
    /// `isr = committed ISR - old primary - nodes the lease detector flagged (flagged)`, the new
    /// primary always included; `term = term + 1`; `term_end = {term, takeover}` where
    /// `takeover` is the fenced node's `FenceAck.applied`. The ISR never depends on who answered
    /// anything: `takeover` only fills `term_end`.
    pub fn failover_promote(
        &self,
        shard: ShardId,
        primary: NodeId,
        flagged: &dyn Fn(&NodeId) -> bool,
        takeover: Vec<u64>,
    ) -> Result<Transition, Reject> {
        let sh = self.shard(shard).ok_or(Reject::UnknownShard)?;
        let old = sh.primary;
        let isr = sh
            .isr()
            .map(|m| m.node)
            .filter(|n| *n == primary || (Some(*n) != old && !flagged(n)))
            .collect();
        self.promote(sh, primary, isr, takeover)
    }

    /// The `Promote` that completes a graceful handoff (§23.12): the committed ISR unchanged
    /// (the old primary stays an InSync member), `term_end = {term, last}` from `HandoffDone`.
    pub fn handoff_promote(
        &self,
        shard: ShardId,
        to: NodeId,
        last: Vec<u64>,
    ) -> Result<Transition, Reject> {
        let sh = self.shard(shard).ok_or(Reject::UnknownShard)?;
        let isr = sh.isr().map(|m| m.node).collect();
        self.promote(sh, to, isr, last)
    }

    fn promote(
        &self,
        sh: &ShardRecord,
        primary: NodeId,
        isr: Vec<NodeId>,
        takeover: Vec<u64>,
    ) -> Result<Transition, Reject> {
        if !sh.member(&primary).is_some_and(|m| m.in_isr) {
            return Err(Reject::Precondition(
                "the new primary is not in the committed ISR".into(),
            ));
        }
        let term = sh.term.checked_add(1).ok_or(Reject::Malformed)?;
        Ok(Transition::Promote {
            shard: sh.id,
            primary,
            term,
            isr,
            term_end: TermEnd {
                term: sh.term,
                takeover,
            },
        })
    }

    /// `kvadm replica resync <node>` precondition (§23.12): the member is NeedsFullSync, or it
    /// reports corrupt pages in its `Renew` health (known only to the coordinator's lease table,
    /// hence the argument). `apply` itself refuses primaries, non-members and members already
    /// syncing.
    pub fn resync_allowed(&self, node: &NodeId, reports_corrupt_pages: bool) -> Result<(), Reject> {
        self.node(node).ok_or(Reject::UnknownNode)?;
        let sh = self
            .shard_of_node(node)
            .ok_or_else(|| Reject::Precondition("node has no shard role".into()))?;
        if sh.is_primary(node) {
            return Err(Reject::Precondition("cannot resync a shard primary".into()));
        }
        match sh.member(node).map(|m| m.state) {
            Some(MemberState::NeedsFullSync) => Ok(()),
            Some(MemberState::Syncing) => {
                Err(Reject::Precondition("member is already syncing".into()))
            }
            Some(_) if reports_corrupt_pages => Ok(()),
            _ => Err(Reject::Precondition(
                "resync needs a NeedsFullSync member or reported corrupt pages".into(),
            )),
        }
    }
}

impl fmt::Debug for ClusterState {
    /// A summary: slot ownership is shown as ranges per shard, the password redacted.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct Slots<'a>(&'a ClusterState);
        impl fmt::Debug for Slots<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let mut m = f.debug_map();
                for sh in &self.0.shards {
                    m.entry(&sh.id.0, &format_args!("{}", self.0.slot_ranges_of(sh.id)));
                }
                m.finish()
            }
        }
        f.debug_struct("ClusterState")
            .field("version", &self.version)
            .field("cluster_id", &format_args!("{:02x?}", self.cluster_id))
            .field("params", &self.params)
            .field("nodes", &self.nodes)
            .field("shards", &self.shards)
            .field("slots", &Slots(self))
            .field("migrations", &self.migrations)
            .finish()
    }
}

/// The slot array as `(first_slot, run_len - 1, value)` runs.
mod slots_rle {
    use serde::de::Error;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(slots: &[u16; 16384], ser: S) -> Result<S::Ok, S::Error> {
        let mut runs: Vec<(u16, u16, u16)> = Vec::new();
        let mut i = 0usize;
        while i < slots.len() {
            let v = slots[i];
            let start = i;
            while i < slots.len() && slots[i] == v {
                i += 1;
            }
            // start and i - start - 1 are below 16384
            runs.push((start as u16, (i - start - 1) as u16, v));
        }
        runs.serialize(ser)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<Box<[u16; 16384]>, D::Error> {
        let runs = Vec::<(u16, u16, u16)>::deserialize(de)?;
        let mut slots = Box::new([0u16; 16384]);
        let mut next = 0usize;
        for (start, len_minus_1, v) in runs {
            let (start, end) = (
                usize::from(start),
                usize::from(start) + usize::from(len_minus_1) + 1,
            );
            if start != next || end > slots.len() {
                return Err(D::Error::custom("slot runs must tile 0..16384"));
            }
            slots[start..end].fill(v);
            next = end;
        }
        if next != slots.len() {
            return Err(D::Error::custom("slot runs must tile 0..16384"));
        }
        Ok(slots)
    }
}
