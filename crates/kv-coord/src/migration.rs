//! Operator-initiated slot migration orchestration (DESIGN §23.13).
//!
//! Copy: commit the migration, push `MigrateOut` to the source primary and `MigrateIn` (with the
//! source's replication address) to the destination primary; destination partitions dial the
//! source. Ready: the source reports `MigrationReady` after its position barriers complete.
//! Cutover: commit `slots := dst` (pushed to the source first); the source seals; the
//! destination reports `SealDone`; commit Done; the source purges. Any failover of either shard
//! before Done reverts routing to the source and pauses the migration; the destination's partial
//! copy stays inert until the operator aborts or resumes. Nothing resumes or re-plans by itself.
//!
//! The migration tasks and their phases reach the two primaries in their assignments
//! (`ShardRole::migrations`, pushed by [`crate::push`] with the source first at cutover). The
//! functions here turn the nodes' reports into transitions: each report is accepted only from
//! the primary that owns that step and only in the phase it closes, so a late or repeated report
//! changes nothing.

use kv_ctrl::{MigPhase, NodeId, ShardId, SlotRanges};
use kv_topo::{ClusterState, Migration, Transition};

/// Leader-side progress of one migration.
pub struct MigrationRun {
    pub id: u64,
    pub phase: MigPhase,
    pub ready_reported: bool,
    pub seal_done: bool,
}

impl MigrationRun {
    /// The progress of migration `id` as committed state shows it.
    pub fn from_state(s: &ClusterState, id: u64) -> Option<Self> {
        let m = s.migrations.iter().find(|m| m.id == id)?;
        Some(MigrationRun {
            id,
            phase: m.phase,
            ready_reported: matches!(m.phase, MigPhase::Ready | MigPhase::Cutover | MigPhase::Done),
            seal_done: m.phase == MigPhase::Done,
        })
    }
}

fn migration(s: &ClusterState, id: u64) -> Option<&Migration> {
    s.migrations.iter().find(|m| m.id == id)
}

fn primary_of(s: &ClusterState, shard: ShardId) -> Option<NodeId> {
    s.shard(shard).and_then(|sh| sh.primary)
}

/// `MigrationReady { id }` from `from`: accepted from the source primary while the migration
/// copies; commits Ready and the cutover (`slots := dst`) in one batch.
pub fn on_ready(s: &ClusterState, from: &NodeId, id: u64) -> Option<Vec<Transition>> {
    let m = migration(s, id)?;
    if primary_of(s, m.src) != Some(*from) {
        return None;
    }
    let cutover = Transition::Migration {
        id,
        phase: MigPhase::Cutover,
        slots_to_dst: true,
    };
    match m.phase {
        MigPhase::Copy => Some(vec![
            Transition::Migration {
                id,
                phase: MigPhase::Ready,
                slots_to_dst: false,
            },
            cutover,
        ]),
        MigPhase::Ready => Some(vec![cutover]),
        _ => None,
    }
}

/// `SealDone { id }` from `from`: accepted from the destination primary during the cutover;
/// commits Done.
pub fn on_seal_done(s: &ClusterState, from: &NodeId, id: u64) -> Option<Transition> {
    let m = migration(s, id)?;
    if m.phase != MigPhase::Cutover || primary_of(s, m.dst) != Some(*from) {
        return None;
    }
    Some(Transition::Migration {
        id,
        phase: MigPhase::Done,
        slots_to_dst: true,
    })
}

/// A fresh migration id: above every id in the state and above the state's version (so ids
/// stay unique after finished migrations are pruned), and above `last_issued`.
pub fn next_id(s: &ClusterState, last_issued: u64) -> u64 {
    let max = s.migrations.iter().map(|m| m.id).fold(s.version, u64::max);
    max.max(last_issued) + 1
}

/// One line of `kvadm migrations`.
pub fn describe(s: &ClusterState, m: &Migration) -> String {
    let rate = m
        .rate_mbps
        .map_or_else(|| "default".to_owned(), |r| format!("{r}mbps"));
    let routed = if m.slots.iter_slots().all(|x| s.slot_owner(x) == Some(m.dst)) {
        "dst"
    } else {
        "src"
    };
    format!(
        "{} slots={} src={} dst={} phase={} routed={} rate={} barriers={}",
        m.id,
        m.slots,
        m.src.0,
        m.dst.0,
        m.phase.as_str(),
        routed,
        rate,
        m.barriers
    )
}

/// The slot ranges a shard owns, for status output.
pub fn owned(s: &ClusterState, shard: ShardId) -> SlotRanges {
    s.slot_ranges_of(shard)
}

#[cfg(test)]
mod tests {
    use kv_topo::{AdminOp, ClusterParams};

    use super::*;

    fn nid(i: u8) -> NodeId {
        NodeId([i; 20])
    }

    fn state() -> ClusterState {
        let s = ClusterState::initial(ClusterParams::default(), [1; 16]);
        let mut b = Vec::new();
        for i in 1..=2u8 {
            let base = 9000 + u16::from(i) * 10;
            b.push(crate::failover::test_support::node_add(nid(i), base, s.params.partitions));
            b.push(Transition::Admin(AdminOp::ShardCreate {
                id: ShardId(u16::from(i)),
                primary: nid(i),
                min_isr: 1,
            }));
        }
        b.push(Transition::Admin(AdminOp::SlotsAssign {
            slots: SlotRanges::parse("0-16383").unwrap(),
            shard: ShardId(1),
        }));
        let s = s.apply(&b).unwrap();
        let id = next_id(&s, 0);
        s.apply(&[Transition::Admin(AdminOp::MigrateStart {
            id,
            slots: SlotRanges::parse("0-99").unwrap(),
            dst: ShardId(2),
            rate_mbps: None,
            barriers: 2,
        })])
        .unwrap()
    }

    #[test]
    fn reports_drive_the_phases() {
        let s = state();
        let id = s.migrations[0].id;
        assert!(id > 1);
        // only the source primary reports readiness, only the destination primary the seal
        assert!(on_ready(&s, &nid(2), id).is_none());
        assert!(on_seal_done(&s, &nid(2), id).is_none(), "not in cutover yet");
        let b = on_ready(&s, &nid(1), id).unwrap();
        let s = s.apply(&b).unwrap();
        assert_eq!(s.migrations[0].phase, MigPhase::Cutover);
        assert_eq!(s.slot_owner(0), Some(ShardId(2)));
        assert_eq!(s.slot_owner(100), Some(ShardId(1)));
        assert!(on_ready(&s, &nid(1), id).is_none(), "a repeated report changes nothing");
        assert!(on_seal_done(&s, &nid(1), id).is_none());
        let t = on_seal_done(&s, &nid(2), id).unwrap();
        let s = s.apply(&[t]).unwrap();
        assert_eq!(s.migrations[0].phase, MigPhase::Done);
        assert!(MigrationRun::from_state(&s, id).unwrap().seal_done);
        assert!(describe(&s, &s.migrations[0]).contains("phase=done routed=dst"));
        assert!(next_id(&s, 0) > id);
        assert_eq!(next_id(&s, 1000), 1001);
    }
}
