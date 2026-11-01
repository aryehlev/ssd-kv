//! Cluster state transitions (DESIGN §23.2, §23.9, §23.12, §23.13, LC-25).
//!
//! `Admin(AdminOp)` is the only kind that changes `nodes`, member lists, the assignment of
//! unassigned slots, migration start/resume/abort and params; it comes only from operator
//! commands. The others record health flags, promotions decided by the static failover rule, ISR
//! changes, member states and migration phases. Nothing re-replicates, rebalances or migrates on
//! its own.
//!
//! Non-admin transitions may move slots only between the source and destination shard of a
//! migration the operator started (cutover, and the revert to the source when the migration is
//! paused by a failure). They never add or remove nodes or members, never assign an unassigned
//! slot, never start a full sync (no non-admin path sets a member to `Syncing`) and never bring
//! a `NeedsFullSync` member back without the operator's `replica resync`.
//!
//! Every function here is deterministic: no clock, no randomness, no hash-map iteration order.

use core::fmt;
use std::net::SocketAddr;

use kv_ctrl::{MemberState, MigPhase, NodeId, ShardId, SlotRanges, TermEnd};
use serde::{Deserialize, Serialize};

use crate::state::{
    ClusterState, MAX_BARRIERS, MAX_DONE_MIGRATIONS, MAX_HISTORY, MAX_MIN_ISR, MAX_ZONE_LEN,
    Member, Migration, NodeFlags, NodeRecord, SLOT_UNASSIGNED, ShardRecord, ShardStatus,
};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Transition {
    Admin(AdminOp),
    Health {
        node: NodeId,
        fail: bool,
    },
    Promote {
        shard: ShardId,
        primary: NodeId,
        term: u32,
        isr: Vec<NodeId>,
        term_end: TermEnd,
    },
    Isr {
        shard: ShardId,
        term: u32,
        node: NodeId,
        add: bool,
    },
    MemberState {
        shard: ShardId,
        node: NodeId,
        state: MemberState,
    },
    Migration {
        id: u64,
        phase: MigPhase,
        slots_to_dst: bool,
    },
}

impl Transition {
    /// True for operator commands, the only transitions that change membership or assign slots
    /// (LC-25).
    pub fn is_admin(&self) -> bool {
        matches!(self, Transition::Admin(_))
    }
}

/// Operator commands (§23.12).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdminOp {
    ParamSet {
        key: String,
        value: String,
    },
    NodeAdd {
        id: NodeId,
        ctrl: SocketAddr,
        client: SocketAddr,
        repl: SocketAddr,
        zone: String,
        priority: u8,
        partitions: u16,
    },
    NodeRemove {
        id: NodeId,
        force: bool,
    },
    NodeMaint {
        id: NodeId,
        on: bool,
    },
    ShardCreate {
        id: ShardId,
        primary: NodeId,
        min_isr: u8,
    },
    ShardDelete {
        id: ShardId,
    },
    SlotsAssign {
        slots: SlotRanges,
        shard: ShardId,
    },
    ReplicaAdd {
        shard: ShardId,
        node: NodeId,
    },
    ReplicaRemove {
        shard: ShardId,
        node: NodeId,
    },
    ReplicaResync {
        node: NodeId,
    },
    MigrateStart {
        id: u64,
        slots: SlotRanges,
        dst: ShardId,
        rate_mbps: Option<u32>,
        barriers: u8,
    },
    MigratePause {
        id: u64,
    },
    MigrateResume {
        id: u64,
    },
    MigrateAbort {
        id: u64,
    },
}

/// Why a batch was refused.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Reject {
    UnknownNode,
    UnknownShard,
    UnknownMigration,
    /// A §23.12 precondition does not hold.
    Precondition(String),
    StaleTerm,
    StateTooLarge,
    Malformed,
}

impl fmt::Display for Reject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Reject::UnknownNode => f.write_str("unknown node"),
            Reject::UnknownShard => f.write_str("unknown shard"),
            Reject::UnknownMigration => f.write_str("unknown migration"),
            Reject::Precondition(s) => write!(f, "precondition failed: {s}"),
            Reject::StaleTerm => f.write_str("stale term"),
            Reject::StateTooLarge => f.write_str("cluster state too large"),
            Reject::Malformed => f.write_str("malformed cluster state or transition"),
        }
    }
}

impl std::error::Error for Reject {}

/// A NeedsFullSync member leaves that state only through `replica resync` (or removal).
const NEEDS_RESYNC: &str = "member needs an operator resync (kvadm replica resync)";

fn pre(s: &str) -> Reject {
    Reject::Precondition(s.to_owned())
}

/// True for the parameter key that holds the client password (never shown in diagnostics).
pub(crate) fn is_secret_key(key: &str) -> bool {
    matches!(
        normalize_key(key).as_str(),
        "client_password" | "requirepass"
    )
}

/// Parameter keys are accepted in `snake_case` or `kebab-case`, any ASCII case.
pub(crate) fn normalize_key(key: &str) -> String {
    key.trim().to_ascii_lowercase().replace('-', "_")
}

impl fmt::Debug for AdminOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AdminOp::ParamSet { key, value } => {
                let mut d = f.debug_struct("ParamSet");
                d.field("key", key);
                if is_secret_key(key) {
                    d.field("value", &format_args!("<redacted>"));
                } else {
                    d.field("value", value);
                }
                d.finish()
            }
            AdminOp::NodeAdd {
                id,
                ctrl,
                client,
                repl,
                zone,
                priority,
                partitions,
            } => f
                .debug_struct("NodeAdd")
                .field("id", id)
                .field("ctrl", ctrl)
                .field("client", client)
                .field("repl", repl)
                .field("zone", zone)
                .field("priority", priority)
                .field("partitions", partitions)
                .finish(),
            AdminOp::NodeRemove { id, force } => f
                .debug_struct("NodeRemove")
                .field("id", id)
                .field("force", force)
                .finish(),
            AdminOp::NodeMaint { id, on } => f
                .debug_struct("NodeMaint")
                .field("id", id)
                .field("on", on)
                .finish(),
            AdminOp::ShardCreate {
                id,
                primary,
                min_isr,
            } => f
                .debug_struct("ShardCreate")
                .field("id", id)
                .field("primary", primary)
                .field("min_isr", min_isr)
                .finish(),
            AdminOp::ShardDelete { id } => f.debug_struct("ShardDelete").field("id", id).finish(),
            AdminOp::SlotsAssign { slots, shard } => f
                .debug_struct("SlotsAssign")
                .field("slots", &format_args!("{slots}"))
                .field("shard", shard)
                .finish(),
            AdminOp::ReplicaAdd { shard, node } => f
                .debug_struct("ReplicaAdd")
                .field("shard", shard)
                .field("node", node)
                .finish(),
            AdminOp::ReplicaRemove { shard, node } => f
                .debug_struct("ReplicaRemove")
                .field("shard", shard)
                .field("node", node)
                .finish(),
            AdminOp::ReplicaResync { node } => {
                f.debug_struct("ReplicaResync").field("node", node).finish()
            }
            AdminOp::MigrateStart {
                id,
                slots,
                dst,
                rate_mbps,
                barriers,
            } => f
                .debug_struct("MigrateStart")
                .field("id", id)
                .field("slots", &format_args!("{slots}"))
                .field("dst", dst)
                .field("rate_mbps", rate_mbps)
                .field("barriers", barriers)
                .finish(),
            AdminOp::MigratePause { id } => f.debug_struct("MigratePause").field("id", id).finish(),
            AdminOp::MigrateResume { id } => {
                f.debug_struct("MigrateResume").field("id", id).finish()
            }
            AdminOp::MigrateAbort { id } => f.debug_struct("MigrateAbort").field("id", id).finish(),
        }
    }
}

impl fmt::Debug for Transition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Transition::Admin(op) => f.debug_tuple("Admin").field(op).finish(),
            Transition::Health { node, fail } => f
                .debug_struct("Health")
                .field("node", node)
                .field("fail", fail)
                .finish(),
            Transition::Promote {
                shard,
                primary,
                term,
                isr,
                term_end,
            } => f
                .debug_struct("Promote")
                .field("shard", shard)
                .field("primary", primary)
                .field("term", term)
                .field("isr", isr)
                .field("term_end", term_end)
                .finish(),
            Transition::Isr {
                shard,
                term,
                node,
                add,
            } => f
                .debug_struct("Isr")
                .field("shard", shard)
                .field("term", term)
                .field("node", node)
                .field("add", add)
                .finish(),
            Transition::MemberState { shard, node, state } => f
                .debug_struct("MemberState")
                .field("shard", shard)
                .field("node", node)
                .field("state", state)
                .finish(),
            Transition::Migration {
                id,
                phase,
                slots_to_dst,
            } => f
                .debug_struct("Migration")
                .field("id", id)
                .field("phase", phase)
                .field("slots_to_dst", slots_to_dst)
                .finish(),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Application of one transition to a (scratch) state. `version` is the version the batch will
// commit as; it stamps `NodeRecord::added_version`.

pub(crate) fn apply_one(st: &mut ClusterState, t: &Transition, version: u64) -> Result<(), Reject> {
    match t {
        Transition::Admin(op) => apply_admin(st, op, version)?,
        Transition::Health { node, fail } => apply_health(st, node, *fail)?,
        Transition::Promote {
            shard,
            primary,
            term,
            isr,
            term_end,
        } => apply_promote(st, *shard, primary, *term, isr, term_end)?,
        Transition::Isr {
            shard,
            term,
            node,
            add,
        } => apply_isr(st, *shard, *term, node, *add)?,
        Transition::MemberState { shard, node, state } => {
            apply_member_state(st, *shard, node, *state)?
        }
        Transition::Migration {
            id,
            phase,
            slots_to_dst,
        } => apply_migration(st, *id, *phase, *slots_to_dst)?,
    }
    refresh_statuses(st);
    Ok(())
}

fn node_index(st: &ClusterState, id: &NodeId) -> Result<usize, Reject> {
    st.nodes
        .iter()
        .position(|n| n.id == *id)
        .ok_or(Reject::UnknownNode)
}

fn shard_index(st: &ClusterState, id: ShardId) -> Result<usize, Reject> {
    st.shards
        .iter()
        .position(|s| s.id == id)
        .ok_or(Reject::UnknownShard)
}

fn migration_index(st: &ClusterState, id: u64) -> Result<usize, Reject> {
    st.migrations
        .iter()
        .position(|m| m.id == id)
        .ok_or(Reject::UnknownMigration)
}

fn member_index(sh: &ShardRecord, node: &NodeId) -> Option<usize> {
    sh.members.iter().position(|m| m.node == *node)
}

fn has_role(st: &ClusterState, node: &NodeId) -> bool {
    st.shards.iter().any(|sh| member_index(sh, node).is_some())
}

fn node_failed(st: &ClusterState, node: &NodeId) -> bool {
    st.nodes
        .iter()
        .find(|n| n.id == *node)
        .is_none_or(|n| n.flags.fail)
}

/// Shard status follows from committed state: `Serving` is set only by `ShardCreate` and
/// `Promote`; a shard whose primary is flagged FAIL (or that is already waiting for a
/// promotion) is `Promoting` while another ISR member without a FAIL flag exists, else
/// `NoPrimary` (§23.9 step 1).
fn refresh_statuses(st: &mut ClusterState) {
    for i in 0..st.shards.len() {
        let sh = &st.shards[i];
        let primary_failed = sh.primary.is_none_or(|p| node_failed(st, &p));
        if sh.status == ShardStatus::Serving && !primary_failed {
            continue;
        }
        let candidate = sh
            .members
            .iter()
            .any(|m| Some(m.node) != sh.primary && m.in_isr && !node_failed(st, &m.node));
        st.shards[i].status = if candidate {
            ShardStatus::Promoting
        } else {
            ShardStatus::NoPrimary
        };
    }
}

/// Pauses every running migration from or to `shard`, routing its slots back to the source
/// (§23.13 step 5: a failure before Done reverts routing only; no data moves).
fn pause_migrations_of(st: &mut ClusterState, shard: ShardId) {
    for i in 0..st.migrations.len() {
        let m = &st.migrations[i];
        if m.phase.is_active() && (m.src == shard || m.dst == shard) {
            if m.phase == MigPhase::Cutover {
                let (slots, src) = (m.slots.clone(), m.src);
                route(st, &slots, src);
            }
            st.migrations[i].phase = MigPhase::Paused;
        }
    }
}

fn route(st: &mut ClusterState, slots: &SlotRanges, to: ShardId) {
    for s in slots.iter_slots() {
        st.slots[usize::from(s)] = to.0;
    }
}

fn owned_by(st: &ClusterState, slots: &SlotRanges, shard: ShardId) -> bool {
    slots
        .iter_slots()
        .all(|s| st.slots[usize::from(s)] == shard.0)
}

// ---------------------------------------------------------------------------------------------

fn apply_health(st: &mut ClusterState, node: &NodeId, fail: bool) -> Result<(), Reject> {
    let ni = node_index(st, node)?;
    st.nodes[ni].flags.fail = fail;
    if fail {
        // A failed primary ends the migrations of its shard (reverted, paused).
        let primaries: Vec<ShardId> = st
            .shards
            .iter()
            .filter(|sh| sh.primary == Some(*node))
            .map(|sh| sh.id)
            .collect();
        for sid in primaries {
            pause_migrations_of(st, sid);
        }
    }
    Ok(())
}

fn apply_promote(
    st: &mut ClusterState,
    shard: ShardId,
    primary: &NodeId,
    term: u32,
    isr: &[NodeId],
    term_end: &TermEnd,
) -> Result<(), Reject> {
    let si = shard_index(st, shard)?;
    let ni = node_index(st, primary)?;
    let partitions = usize::from(st.params.partitions);
    let sh = &st.shards[si];
    if term <= sh.term {
        return Err(Reject::StaleTerm);
    }
    if Some(term) != sh.term.checked_add(1) {
        return Err(pre("a promotion term must be the shard term + 1"));
    }
    if term_end.term != sh.term {
        return Err(pre("term_end must close the shard's current term"));
    }
    if term_end.takeover.len() != partitions {
        return Err(pre("term_end needs one takeover seq per partition"));
    }
    if st.nodes[ni].flags.fail {
        return Err(pre("the new primary is flagged failed"));
    }
    let Some(pi) = member_index(sh, primary) else {
        return Err(pre("the new primary is not a member of the shard"));
    };
    if !sh.members[pi].in_isr {
        return Err(pre("the new primary is not in the committed ISR"));
    }
    if !isr.contains(primary) {
        return Err(pre("the new ISR must contain the new primary"));
    }
    for (i, n) in isr.iter().enumerate() {
        if isr[..i].contains(n) {
            return Err(pre("duplicate node in the new ISR"));
        }
        match member_index(sh, n) {
            Some(mi) if sh.members[mi].in_isr => {}
            _ => return Err(pre("the new ISR must be a subset of the committed ISR")),
        }
    }
    let old = sh.primary;
    // Graceful handoff: the old primary stays in the ISR and nothing diverged (§23.12).
    let handoff = old.is_some_and(|o| o != *primary && isr.contains(&o));
    let sh = &mut st.shards[si];
    for m in &mut sh.members {
        if m.node == *primary {
            m.state = MemberState::InSync;
            m.in_isr = true;
        } else if isr.contains(&m.node) {
            m.in_isr = true;
            if !handoff {
                // Repaired against the new primary before it is advertised again (§23.9 step 6).
                m.state = MemberState::Reconciling;
            }
        } else if m.in_isr {
            // In the committed ISR but left out: the old primary or a node already flagged.
            m.state = MemberState::Failed;
            m.in_isr = false;
        } else if matches!(m.state, MemberState::InSync | MemberState::Reconciling) {
            m.state = MemberState::CatchingUp;
        }
    }
    sh.primary = Some(*primary);
    sh.term = term;
    sh.history.push(term_end.clone());
    if sh.history.len() > MAX_HISTORY {
        let extra = sh.history.len() - MAX_HISTORY;
        sh.history.drain(..extra);
    }
    sh.status = ShardStatus::Serving;
    pause_migrations_of(st, shard);
    Ok(())
}

fn apply_isr(
    st: &mut ClusterState,
    shard: ShardId,
    term: u32,
    node: &NodeId,
    add: bool,
) -> Result<(), Reject> {
    let si = shard_index(st, shard)?;
    let sh = &mut st.shards[si];
    if term < sh.term {
        return Err(Reject::StaleTerm);
    }
    if term != sh.term {
        return Err(pre("ISR change for a term the shard has not reached"));
    }
    if sh.primary == Some(*node) {
        return Err(pre("the primary is always in its ISR"));
    }
    let Some(mi) = member_index(sh, node) else {
        return Err(pre("not a member of the shard"));
    };
    let m = &mut sh.members[mi];
    if add {
        if m.state == MemberState::NeedsFullSync {
            return Err(pre(NEEDS_RESYNC));
        }
        m.in_isr = true;
        m.state = MemberState::InSync;
    } else {
        m.in_isr = false;
        if matches!(m.state, MemberState::InSync | MemberState::Reconciling) {
            m.state = MemberState::CatchingUp;
        }
    }
    Ok(())
}

fn apply_member_state(
    st: &mut ClusterState,
    shard: ShardId,
    node: &NodeId,
    state: MemberState,
) -> Result<(), Reject> {
    if state == MemberState::Syncing {
        // Full sync starts only from `replica add` / `replica resync` (LC-25).
        return Err(pre("only replica add or replica resync start a full sync"));
    }
    let si = shard_index(st, shard)?;
    let sh = &mut st.shards[si];
    let Some(mi) = member_index(sh, node) else {
        return Err(pre("not a member of the shard"));
    };
    if sh.primary == Some(*node) && state != MemberState::InSync {
        return Err(pre("the primary's member state is in-sync"));
    }
    let m = &mut sh.members[mi];
    if m.state == MemberState::NeedsFullSync
        && !matches!(state, MemberState::NeedsFullSync | MemberState::Failed)
    {
        return Err(pre(NEEDS_RESYNC));
    }
    m.state = state;
    if !matches!(state, MemberState::InSync | MemberState::Reconciling) {
        m.in_isr = false;
    }
    Ok(())
}

fn apply_migration(
    st: &mut ClusterState,
    id: u64,
    phase: MigPhase,
    slots_to_dst: bool,
) -> Result<(), Reject> {
    let mi = migration_index(st, id)?;
    if slots_to_dst != matches!(phase, MigPhase::Cutover | MigPhase::Done) {
        return Err(pre("slots_to_dst must be set exactly for cutover and done"));
    }
    let m = st.migrations[mi].clone();
    if m.phase == phase && phase != MigPhase::Copy {
        return Ok(()); // repeated report of the phase already committed
    }
    match (m.phase, phase) {
        (MigPhase::Copy, MigPhase::Ready) | (MigPhase::Cutover, MigPhase::Done) => {}
        (MigPhase::Copy | MigPhase::Ready, MigPhase::Cutover) => {
            if !owned_by(st, &m.slots, m.src) {
                return Err(pre(
                    "the migrating slots are no longer owned by the source shard",
                ));
            }
            let dst = shard_index(st, m.dst)?;
            if st.shards[dst].status != ShardStatus::Serving {
                return Err(pre("the destination shard has no serving primary"));
            }
            route(st, &m.slots, m.dst);
        }
        (MigPhase::Copy | MigPhase::Ready | MigPhase::Cutover, MigPhase::Paused) => {
            if m.phase == MigPhase::Cutover {
                route(st, &m.slots, m.src);
            }
        }
        _ => return Err(pre("invalid migration phase change")),
    }
    st.migrations[mi].phase = phase;
    Ok(())
}

// ---------------------------------------------------------------------------------------------

fn apply_admin(st: &mut ClusterState, op: &AdminOp, version: u64) -> Result<(), Reject> {
    match op {
        AdminOp::ParamSet { key, value } => {
            let no_nodes = st.nodes.is_empty();
            st.params.set(key, value, no_nodes)
        }
        AdminOp::NodeAdd {
            id,
            ctrl,
            client,
            repl,
            zone,
            priority,
            partitions,
        } => {
            if st.nodes.iter().any(|n| n.id == *id) {
                return Err(pre("node id already registered"));
            }
            if *partitions != st.params.partitions {
                return Err(pre("node is formatted with a different partition count"));
            }
            if zone.len() > MAX_ZONE_LEN {
                return Err(pre("zone label too long"));
            }
            for n in &st.nodes {
                if n.ctrl == *ctrl || n.client == *client || n.repl == *repl {
                    return Err(pre("address already used by a registered node"));
                }
            }
            if ctrl == client || ctrl == repl || client == repl {
                return Err(pre("control, client and replication addresses must differ"));
            }
            st.nodes.push(NodeRecord {
                id: *id,
                ctrl: *ctrl,
                client: *client,
                repl: *repl,
                zone: zone.clone(),
                priority: *priority,
                flags: NodeFlags::default(),
                partitions: *partitions,
                added_version: version,
            });
            Ok(())
        }
        AdminOp::NodeRemove { id, force } => {
            let ni = node_index(st, id)?;
            if has_role(st, id) {
                if !*force {
                    return Err(pre(
                        "node has a shard role (use --force once it failed and was replaced)",
                    ));
                }
                if !st.nodes[ni].flags.fail {
                    return Err(pre("--force needs the node to be flagged failed"));
                }
                if st.shards.iter().any(|sh| sh.primary == Some(*id)) {
                    return Err(pre("node is still a shard primary"));
                }
                for sh in &mut st.shards {
                    sh.members.retain(|m| m.node != *id);
                }
            }
            st.nodes.remove(ni);
            Ok(())
        }
        AdminOp::NodeMaint { id, on } => {
            let ni = node_index(st, id)?;
            st.nodes[ni].flags.maint = *on;
            Ok(())
        }
        AdminOp::ShardCreate {
            id,
            primary,
            min_isr,
        } => {
            if id.0 == SLOT_UNASSIGNED {
                return Err(pre("shard id 65535 is reserved"));
            }
            if st.shards.iter().any(|s| s.id == *id) {
                return Err(pre("shard already exists"));
            }
            let ni = node_index(st, primary)?;
            if has_role(st, primary) {
                return Err(pre("node already has a shard role"));
            }
            if st.nodes[ni].flags.fail {
                return Err(pre("node is flagged failed"));
            }
            if *min_isr == 0 || *min_isr > MAX_MIN_ISR {
                return Err(pre(
                    "min-isr must be between 1 and 16 (it counts the primary)",
                ));
            }
            st.shards.push(ShardRecord {
                id: *id,
                primary: Some(*primary),
                members: vec![Member {
                    node: *primary,
                    state: MemberState::InSync,
                    in_isr: true,
                }],
                term: 1,
                min_isr: *min_isr,
                history: Vec::new(),
                status: ShardStatus::Serving,
            });
            Ok(())
        }
        AdminOp::ShardDelete { id } => {
            let si = shard_index(st, *id)?;
            if st.slots.contains(&id.0) {
                return Err(pre("shard owns slots"));
            }
            if st
                .migrations
                .iter()
                .any(|m| m.phase != MigPhase::Done && (m.src == *id || m.dst == *id))
            {
                return Err(pre("shard is part of an unfinished migration"));
            }
            st.migrations.retain(|m| m.src != *id && m.dst != *id);
            st.shards.remove(si);
            Ok(())
        }
        AdminOp::SlotsAssign { slots, shard } => {
            let slots = slots
                .normalized()
                .filter(|r| !r.is_empty())
                .ok_or_else(|| pre("invalid slot ranges"))?;
            shard_index(st, *shard)?;
            if slots
                .iter_slots()
                .any(|s| st.slots[usize::from(s)] != SLOT_UNASSIGNED)
            {
                return Err(pre("slots already assigned"));
            }
            route(st, &slots, *shard);
            Ok(())
        }
        AdminOp::ReplicaAdd { shard, node } => {
            let si = shard_index(st, *shard)?;
            node_index(st, node)?;
            if has_role(st, node) {
                return Err(pre("node already has a shard role"));
            }
            // A learner: full sync (insert) starts from this operator command only (LC-25).
            st.shards[si].members.push(Member {
                node: *node,
                state: MemberState::Syncing,
                in_isr: false,
            });
            Ok(())
        }
        AdminOp::ReplicaRemove { shard, node } => {
            let si = shard_index(st, *shard)?;
            node_index(st, node)?;
            let sh = &mut st.shards[si];
            let Some(mi) = member_index(sh, node) else {
                return Err(pre("not a member of the shard"));
            };
            if sh.primary == Some(*node) {
                return Err(pre("cannot remove the shard primary (fail over first)"));
            }
            sh.members.remove(mi);
            Ok(())
        }
        AdminOp::ReplicaResync { node } => {
            node_index(st, node)?;
            let sh = st
                .shards
                .iter_mut()
                .find(|sh| member_index(sh, node).is_some())
                .ok_or_else(|| pre("node has no shard role"))?;
            if sh.primary == Some(*node) {
                return Err(pre("cannot resync a shard primary"));
            }
            let Some(mi) = member_index(sh, node) else {
                return Err(pre("node has no shard role"));
            };
            let m = &mut sh.members[mi];
            if m.state == MemberState::Syncing {
                return Err(pre("member is already syncing"));
            }
            m.state = MemberState::Syncing;
            m.in_isr = false;
            Ok(())
        }
        AdminOp::MigrateStart {
            id,
            slots,
            dst,
            rate_mbps,
            barriers,
        } => {
            if st.migrations.iter().any(|m| m.id == *id) {
                return Err(pre("migration id already used"));
            }
            let slots = slots
                .normalized()
                .filter(|r| !r.is_empty())
                .ok_or_else(|| pre("invalid slot ranges"))?;
            shard_index(st, *dst)?;
            let first = slots
                .iter_slots()
                .next()
                .map(|s| st.slots[usize::from(s)])
                .unwrap_or(SLOT_UNASSIGNED);
            if first == SLOT_UNASSIGNED || !owned_by(st, &slots, ShardId(first)) {
                return Err(pre("slots must all be owned by one source shard"));
            }
            let src = ShardId(first);
            if src == *dst {
                return Err(pre("slots are already owned by the destination shard"));
            }
            for m in &st.migrations {
                if m.phase != MigPhase::Done && slots.iter_slots().any(|s| m.slots.contains(s)) {
                    return Err(pre("slots are part of another unfinished migration"));
                }
            }
            if *rate_mbps == Some(0) {
                return Err(pre("rate must be positive"));
            }
            if *barriers > MAX_BARRIERS {
                return Err(pre("too many barriers"));
            }
            // Keep the newest finished migrations for `kvadm migrations`; drop the oldest.
            let done = st
                .migrations
                .iter()
                .filter(|m| m.phase == MigPhase::Done)
                .count();
            let mut excess = (done + 1).saturating_sub(MAX_DONE_MIGRATIONS);
            st.migrations.retain(|m| {
                if excess > 0 && m.phase == MigPhase::Done {
                    excess -= 1;
                    false
                } else {
                    true
                }
            });
            st.migrations.push(Migration {
                id: *id,
                slots,
                src,
                dst: *dst,
                phase: MigPhase::Copy,
                rate_mbps: *rate_mbps,
                barriers: *barriers,
            });
            Ok(())
        }
        AdminOp::MigratePause { id } => {
            let mi = migration_index(st, *id)?;
            let m = st.migrations[mi].clone();
            match m.phase {
                MigPhase::Paused => Ok(()),
                MigPhase::Done => Err(pre("migration is done")),
                MigPhase::Copy | MigPhase::Ready | MigPhase::Cutover => {
                    if m.phase == MigPhase::Cutover {
                        route(st, &m.slots, m.src);
                    }
                    st.migrations[mi].phase = MigPhase::Paused;
                    Ok(())
                }
            }
        }
        AdminOp::MigrateResume { id } => {
            let mi = migration_index(st, *id)?;
            let m = &st.migrations[mi];
            if m.phase != MigPhase::Paused {
                return Err(pre("migration is not paused"));
            }
            if !owned_by(st, &m.slots, m.src) {
                return Err(pre(
                    "the migrating slots are no longer owned by the source shard",
                ));
            }
            // Copy again from the start, as a merge (§23.13 step 5).
            st.migrations[mi].phase = MigPhase::Copy;
            Ok(())
        }
        AdminOp::MigrateAbort { id } => {
            let mi = migration_index(st, *id)?;
            let m = st.migrations[mi].clone();
            if m.phase == MigPhase::Done {
                return Err(pre("migration is done"));
            }
            if m.phase == MigPhase::Cutover {
                route(st, &m.slots, m.src);
            }
            // The destination's next assignment no longer lists the task: it deletes its inert
            // partial copy with paced deletes (§23.13 step 5).
            st.migrations.remove(mi);
            Ok(())
        }
    }
}
