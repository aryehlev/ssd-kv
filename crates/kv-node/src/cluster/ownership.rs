//! The data-node routing gate (DESIGN §23.6, LC-24).
//!
//! For every command in cluster mode, before execution (and again at queue time inside MULTI):
//! 1. keyless: served locally on the node's own data; keyless writes (FLUSHDB/FLUSHALL) need
//!    role primary with a valid lease (replica: READONLY error);
//! 2. keys in more than one slot -> CROSSSLOT;
//! 3. primary, slot owned, lease valid, slot not sealed out by a migration cutover -> serve;
//!    writes with a committed ISR below `min_isr` -> NOREPLICAS;
//! 4. replica, READONLY connection, read-only command, slot in the shard, lease valid, partition
//!    not in full sync -> serve locally (no replication-state gate, LC-28);
//! 5. slot pending seal on a migration destination -> TRYAGAIN;
//! 6. otherwise -> `-MOVED <slot> <coord_addrs[conn_id % n]>` from the pushed static coordinator
//!    list: on one connection identical for every slot the node does not serve, apart from the
//!    echoed slot number. A data node never names another data node.
//!
//! fields are private to cluster/ownership.rs; kv-node/src/cmd/* may only call gate() and
//! slot_filter() (lint, LC-24)
//!
//! Rule 5 is checked before rule 3: after a cutover the destination already owns the slots it
//! must not serve until the seal is committed. A primary whose lease expired answers MOVED to a
//! coordinator (§23.10). During a graceful handoff the primary answers new writes with TRYAGAIN
//! (§23.12).

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use kv_cmd::CmdFlags;
use kv_common::SlotSet;
use kv_ctrl::{MigPhase, MigrationTask, NodeAssignment};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Mode {
    Standalone,
    Cluster,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role {
    Primary,
    Replica,
}

pub struct Ownership {
    mode: Mode,
    role: Option<Role>,
    owned: SlotSet,
    shard_slots: SlotSet,
    sealed_out: SlotSet,
    pending_seal: SlotSet,
    min_isr: u8,
    isr_len: u8,
    /// static list order
    coord_addrs: Vec<SocketAddr>,
    term: u32,
    /// P (partition of a slot = slot % P).
    partitions: u16,
    /// Per partition: a merge full sync runs (shared with the workers).
    syncing: Option<Arc<[AtomicBool]>>,
    /// A graceful handoff runs (shared with the agent).
    handoff: Option<Arc<AtomicBool>>,
    /// Primary: a migration from or to this shard is running (Copy, Ready or Cutover): keyless
    /// writes (FLUSHDB, FLUSHALL) are refused until it is Done, paused or aborted, since a flush
    /// on either side would make the destination's copy disagree with what the source
    /// acknowledged.
    migrating: bool,
}

/// Decision of the gate.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Gate {
    Serve,
    Moved(u16, SocketAddr),
    CrossSlot,
    NoReplicas,
    TryAgain,
    ReadOnlyReplica,
    ClusterDown,
    /// A keyless write (FLUSHDB/FLUSHALL) while a migration of this shard runs.
    MigrationRunning,
}

/// opaque predicate: does this node serve slot s for keyless walks
pub struct SlotFilter {
    serves: Option<SlotSet>,
}

impl SlotFilter {
    pub fn serves(&self, slot: u16) -> bool {
        self.serves.as_ref().is_none_or(|s| s.contains(slot))
    }
}

/// Shared flags an ownership projection reads (owned by the node's cluster state).
pub struct GateFlags {
    pub syncing: Arc<[AtomicBool]>,
    pub handoff: Arc<AtomicBool>,
}

impl Ownership {
    /// Standalone mode: every slot is served locally.
    pub fn standalone() -> Self {
        Ownership {
            mode: Mode::Standalone,
            role: None,
            owned: SlotSet::new(),
            shard_slots: SlotSet::new(),
            sealed_out: SlotSet::new(),
            pending_seal: SlotSet::new(),
            min_isr: 1,
            isr_len: 1,
            coord_addrs: Vec::new(),
            term: 0,
            partitions: 1,
            syncing: None,
            handoff: None,
            migrating: false,
        }
    }

    /// Cluster mode before any assignment: nothing is served (MOVED to a coordinator once the
    /// list is known, CLUSTERDOWN before).
    pub fn unassigned(partitions: u16, flags: Option<GateFlags>) -> Self {
        let mut o = Self::standalone();
        o.mode = Mode::Cluster;
        o.partitions = partitions.max(1);
        if let Some(f) = flags {
            o.syncing = Some(f.syncing);
            o.handoff = Some(f.handoff);
        }
        o
    }

    /// Builds the gate from an applied assignment.
    pub fn from_assignment(a: &NodeAssignment) -> Self {
        Self::from_assignment_with(a, None)
    }

    /// [`Ownership::from_assignment`] reading the node's shared sync and handoff flags.
    pub fn from_assignment_with(a: &NodeAssignment, flags: Option<GateFlags>) -> Self {
        let mut o = Self::unassigned(a.params.partitions, flags);
        o.coord_addrs = a.coordinators.iter().map(|c| c.client).collect();
        let Some(r) = &a.role else { return o };
        o.term = r.term;
        o.min_isr = r.min_isr.max(1);
        o.shard_slots = r.owned_slots.clone();
        if r.is_primary {
            o.role = Some(Role::Primary);
            o.owned = r.owned_slots.clone();
            o.isr_len =
                (1 + r.members.iter().filter(|(_, _, in_isr)| *in_isr).count()).min(255) as u8;
            o.migrating = r.migrations.iter().any(|t| t.phase().is_active());
            for t in &r.migrations {
                match t {
                    // the cutover moved these slots to the destination: not served here while
                    // the task lasts, unless this shard owns them (again): a later migration
                    // that moved them back makes them servable, whatever older task remains
                    MigrationTask::Out {
                        slots,
                        phase: MigPhase::Cutover | MigPhase::Done,
                        ..
                    } => {
                        for s in slots.iter_slots() {
                            if !r.owned_slots.contains(s) {
                                o.sealed_out.insert(s);
                            }
                        }
                    }
                    // owned since the cutover, served only after the seal is committed
                    MigrationTask::In {
                        slots,
                        phase: MigPhase::Cutover,
                        ..
                    } => {
                        for s in slots.iter_slots() {
                            o.pending_seal.insert(s);
                        }
                    }
                    _ => {}
                }
            }
        } else {
            o.role = Some(Role::Replica);
            o.isr_len = 1;
        }
        o
    }

    fn moved(&self, slot: u16, conn_id: u64) -> Gate {
        match self.coord_addrs.len() {
            0 => Gate::ClusterDown,
            n => Gate::Moved(slot, self.coord_addrs[(conn_id % n as u64) as usize]),
        }
    }

    fn syncing(&self, slot: u16) -> bool {
        let p = usize::from(slot % self.partitions.max(1));
        self.syncing
            .as_ref()
            .and_then(|s| s.get(p))
            .is_some_and(|f| f.load(Ordering::Acquire))
    }

    fn in_handoff(&self) -> bool {
        self.handoff
            .as_ref()
            .is_some_and(|h| h.load(Ordering::Acquire))
    }

    /// Moved target = coord_addrs[conn_id % n], independent of slot state
    pub fn gate(
        &self,
        slot: Option<u16>,
        flags: kv_cmd::CmdFlags,
        readonly_conn: bool,
        lease_ok: bool,
        conn_id: u64,
    ) -> Gate {
        if self.mode == Mode::Standalone {
            return Gate::Serve;
        }
        let write = flags.contains(CmdFlags::WRITE);
        let Some(slot) = slot else {
            // 1. keyless: local data; keyless writes need a primary with a valid lease
            if !write {
                return Gate::Serve;
            }
            return match self.role {
                Some(Role::Primary) if lease_ok && self.in_handoff() => Gate::TryAgain,
                Some(Role::Primary) if lease_ok && self.migrating => Gate::MigrationRunning,
                Some(Role::Primary) if lease_ok && self.isr_len < self.min_isr => Gate::NoReplicas,
                Some(Role::Primary) if lease_ok => Gate::Serve,
                Some(Role::Replica) => Gate::ReadOnlyReplica,
                _ => Gate::ClusterDown,
            };
        };
        // 5. destination after cutover, before the committed seal
        if self.pending_seal.contains(slot) {
            return Gate::TryAgain;
        }
        // 3. primary
        if self.role == Some(Role::Primary)
            && self.owned.contains(slot)
            && !self.sealed_out.contains(slot)
            && lease_ok
        {
            if write && self.in_handoff() {
                return Gate::TryAgain;
            }
            if write && self.isr_len < self.min_isr {
                return Gate::NoReplicas;
            }
            return Gate::Serve;
        }
        // 4. replica reads on READONLY connections (no replication-state gate, LC-28)
        if self.role == Some(Role::Replica)
            && readonly_conn
            && !write
            && flags.contains(CmdFlags::READONLY)
            && self.shard_slots.contains(slot)
            && lease_ok
            && !self.syncing(slot)
        {
            return Gate::Serve;
        }
        // 6. constant redirect to a coordinator
        self.moved(slot, conn_id)
    }

    pub fn slot_filter(&self) -> SlotFilter {
        match (self.mode, self.role) {
            (Mode::Standalone, _) => SlotFilter { serves: None },
            (Mode::Cluster, Some(Role::Primary)) => {
                let mut s = self.owned.clone();
                for x in self
                    .sealed_out
                    .ranges()
                    .into_iter()
                    .flat_map(|(a, b)| a..=b)
                {
                    s.remove(x);
                }
                SlotFilter { serves: Some(s) }
            }
            (Mode::Cluster, Some(Role::Replica)) => SlotFilter {
                serves: Some(self.shard_slots.clone()),
            },
            (Mode::Cluster, None) => SlotFilter {
                serves: Some(SlotSet::new()),
            },
        }
    }

    /// Whether this node is the shard primary (replies of writes wait for the shard commit).
    pub fn is_primary(&self) -> bool {
        self.role == Some(Role::Primary)
    }

    /// Whether this node is a replica of a shard (INFO replication `role:slave`).
    pub fn is_replica(&self) -> bool {
        self.mode == Mode::Cluster && self.role == Some(Role::Replica)
    }

    /// Writes committed now would reach fewer ISR members than `min_isr` (their replies are
    /// `-NOREPLICAS`, §23.8 step 4).
    pub fn below_min_isr(&self) -> bool {
        self.mode == Mode::Cluster && self.isr_len < self.min_isr
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kv_ctrl::{
        CoordEndpoint, CoordId, MemberState, NodeId, NodeParams, ShardId, ShardRole, SlotRanges,
    };

    fn assignment(
        primary: bool,
        slots: &str,
        migrations: Vec<MigrationTask>,
        isr: usize,
        min_isr: u8,
    ) -> NodeAssignment {
        let coords = (0..3)
            .map(|i| CoordEndpoint {
                id: CoordId([i; 20]),
                client: format!("10.0.0.{}:7000", i + 1).parse().unwrap(),
            })
            .collect();
        let members = (0..2)
            .map(|i| (NodeId([i + 5; 20]), MemberState::InSync, (i as usize) < isr))
            .collect();
        NodeAssignment {
            version: 9,
            coord_term: 2,
            node_id: NodeId([1; 20]),
            coordinators: coords,
            role: Some(ShardRole {
                shard: ShardId(1),
                term: 3,
                is_primary: primary,
                owned_slots: SlotRanges::parse(slots).unwrap().to_slot_set(),
                primary_repl: None,
                members: if primary { members } else { Vec::new() },
                min_isr,
                history: Vec::new(),
                migrations,
            }),
            client_password: None,
            params: NodeParams {
                partitions: 16,
                ..NodeParams::default()
            },
        }
    }

    const W: CmdFlags = CmdFlags::WRITE;
    const R: CmdFlags = CmdFlags::READONLY;

    #[test]
    fn primary_serves_owned_slots_and_redirects_the_rest_to_coordinators() {
        let o = Ownership::from_assignment(&assignment(true, "0-99", Vec::new(), 2, 1));
        assert_eq!(o.gate(Some(5), W, false, true, 7), Gate::Serve);
        assert_eq!(o.gate(Some(5), R, false, true, 7), Gate::Serve);
        let a = "10.0.0.2:7000".parse().unwrap();
        assert_eq!(
            o.gate(Some(100), R, false, true, 7),
            Gate::Moved(100, a),
            "7 % 3 = 1"
        );
        // lease expired: MOVED to a coordinator, never served
        assert_eq!(o.gate(Some(5), R, false, false, 7), Gate::Moved(5, a));
        assert_eq!(o.gate(None, R, false, true, 7), Gate::Serve);
        assert_eq!(o.gate(None, W, false, true, 7), Gate::Serve);
        assert_eq!(o.gate(None, W, false, false, 7), Gate::ClusterDown);
        let f = o.slot_filter();
        assert!(f.serves(99) && !f.serves(100));
    }

    /// LC-24 golden: on one connection every reply about a slot the node does not serve is
    /// identical apart from the echoed slot number, across ownership states.
    #[test]
    fn unowned_slot_replies_identical() {
        let states = [
            Ownership::from_assignment(&assignment(true, "0-99", Vec::new(), 2, 1)),
            Ownership::from_assignment(&assignment(false, "0-99", Vec::new(), 0, 1)),
            Ownership::from_assignment(&assignment(true, "200-300", Vec::new(), 0, 2)),
            Ownership::from_assignment(&NodeAssignment {
                role: None,
                ..assignment(true, "0", Vec::new(), 0, 1)
            }),
        ];
        for conn in [0u64, 1, 2, 3, 11] {
            let mut target = None;
            for o in &states {
                for slot in [1000u16, 5000, 16383] {
                    for flags in [W, R] {
                        let g = o.gate(Some(slot), flags, false, true, conn);
                        let Gate::Moved(s, addr) = g else {
                            panic!("expected MOVED, got {g:?}")
                        };
                        assert_eq!(s, slot);
                        assert_eq!(
                            *target.get_or_insert(addr),
                            addr,
                            "one target per connection"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn replica_reads_need_readonly_a_lease_and_no_full_sync() {
        let syncing: Arc<[AtomicBool]> = (0..16)
            .map(|_| AtomicBool::new(false))
            .collect::<Vec<_>>()
            .into();
        let flags = GateFlags {
            syncing: syncing.clone(),
            handoff: Arc::new(AtomicBool::new(false)),
        };
        let o = Ownership::from_assignment_with(
            &assignment(false, "0-99", Vec::new(), 0, 1),
            Some(flags),
        );
        assert_eq!(o.gate(Some(5), R, true, true, 0), Gate::Serve);
        assert!(
            matches!(o.gate(Some(5), R, false, true, 0), Gate::Moved(..)),
            "no READONLY"
        );
        assert!(
            matches!(o.gate(Some(5), W, true, true, 0), Gate::Moved(..)),
            "writes"
        );
        assert!(
            matches!(o.gate(Some(5), R, true, false, 0), Gate::Moved(..)),
            "no lease"
        );
        syncing[5].store(true, Ordering::Release);
        assert!(
            matches!(o.gate(Some(5), R, true, true, 0), Gate::Moved(..)),
            "full sync"
        );
        assert_eq!(
            o.gate(Some(6), R, true, true, 0),
            Gate::Serve,
            "other partitions"
        );
        assert_eq!(o.gate(None, W, true, true, 0), Gate::ReadOnlyReplica);
        assert!(!o.is_primary());
    }

    #[test]
    fn min_isr_handoff_and_migration_windows() {
        let o = Ownership::from_assignment(&assignment(true, "0-99", Vec::new(), 0, 2));
        assert_eq!(o.gate(Some(5), W, false, true, 0), Gate::NoReplicas);
        assert_eq!(
            o.gate(Some(5), R, false, true, 0),
            Gate::Serve,
            "reads are never refused for replication state"
        );
        assert!(o.below_min_isr());
        let handoff = Arc::new(AtomicBool::new(true));
        let flags = GateFlags {
            syncing: Arc::new([]),
            handoff,
        };
        let o = Ownership::from_assignment_with(
            &assignment(true, "0-99", Vec::new(), 2, 1),
            Some(flags),
        );
        assert_eq!(o.gate(Some(5), W, false, true, 0), Gate::TryAgain);
        assert_eq!(o.gate(Some(5), R, false, true, 0), Gate::Serve);
        let src = MigrationTask::Out {
            id: 1,
            slots: SlotRanges::parse("0-9").unwrap(),
            phase: MigPhase::Cutover,
            barriers: 2,
        };
        // the cutover commit moved 0-9 to the destination
        let o = Ownership::from_assignment(&assignment(true, "10-99", vec![src.clone()], 2, 1));
        assert!(
            matches!(o.gate(Some(3), R, false, true, 0), Gate::Moved(..)),
            "sealed out at the source"
        );
        assert_eq!(
            o.gate(None, W, false, true, 0),
            Gate::MigrationRunning,
            "no FLUSHDB/FLUSHALL while the migration runs"
        );
        assert_eq!(
            o.gate(None, R, false, true, 0),
            Gate::Serve,
            "keyless reads are served"
        );
        assert!(!o.slot_filter().serves(3) && o.slot_filter().serves(10));
        let dst = MigrationTask::In {
            id: 1,
            slots: SlotRanges::parse("0-9").unwrap(),
            src_repl: kv_ctrl::PeerAddr("10.0.1.1:9000".parse().unwrap()),
            phase: MigPhase::Cutover,
        };
        let o = Ownership::from_assignment(&assignment(true, "0-99", vec![dst], 2, 1));
        assert_eq!(
            o.gate(Some(3), W, false, true, 0),
            Gate::TryAgain,
            "destination before the seal"
        );
        assert_eq!(o.gate(Some(30), W, false, true, 0), Gate::Serve);
        assert!(Ownership::standalone().slot_filter().serves(16383));
    }

    /// F2 (fixed): slots migrated away and later migrated back are served again, whatever
    /// task of the first migration an assignment still carries (a finished `Out` task used to
    /// seal them forever: every node answered MOVED for the whole range).
    #[test]
    fn slots_migrated_back_are_served_again() {
        let done_out = MigrationTask::Out {
            id: 18,
            slots: SlotRanges::parse("0-9").unwrap(),
            phase: MigPhase::Done,
            barriers: 2,
        };
        let back_in = MigrationTask::In {
            id: 31,
            slots: SlotRanges::parse("0-9").unwrap(),
            src_repl: kv_ctrl::PeerAddr("10.0.1.1:9000".parse().unwrap()),
            phase: MigPhase::Done,
        };
        let o = Ownership::from_assignment(&assignment(
            true,
            "0-99",
            vec![done_out.clone(), back_in],
            2,
            1,
        ));
        assert_eq!(o.gate(Some(3), W, false, true, 0), Gate::Serve);
        assert_eq!(o.gate(Some(3), R, false, true, 0), Gate::Serve);
        assert!(o.slot_filter().serves(3));
        // while the slots are elsewhere the finished task still redirects them
        let o = Ownership::from_assignment(&assignment(true, "10-99", vec![done_out], 2, 1));
        assert!(matches!(o.gate(Some(3), R, false, true, 0), Gate::Moved(..)));
        assert_eq!(
            Ownership::unassigned(16, None).gate(Some(1), R, false, true, 0),
            Gate::ClusterDown
        );
    }
}
