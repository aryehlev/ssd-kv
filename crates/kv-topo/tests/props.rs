//! Property tests of DESIGN §25.2 for kv-topo:
//!
//! * `apply_is_deterministic`: the same transition sequence yields byte-identical states, also
//!   when applied to a decoded copy, and batching equals one-by-one application.
//! * `non_admin_transitions_keep_membership` (LC-25): health, promotion, ISR, member-state and
//!   migration-phase transitions never add or remove nodes or members, never change params,
//!   never assign or unassign a slot, never start, resume or abort a migration and never start
//!   a full sync; slots move only between the source and destination of a migration the
//!   operator started.
//! * `epochs_strictly_increase` (LC-27): every committed batch is version + 1, shard terms never
//!   decrease and change only by a promotion of that shard, by exactly 1; a refused batch
//!   changes nothing.
//! * `choose_primary_invariant_under_id_permutation` (LC-27): renaming nodes, or registering
//!   them in another order, never changes which member the static rule picks.
//! * `promote_isr_independent_of_fence_replies` (LC-25): the committed promotion names the same
//!   primary and ISR whatever the fenced nodes answered.
//!
//! Sequences are generated as abstract operations resolved against the current state (indexes
//! into its nodes, shards, members and migrations), so most of them are valid and drive the
//! state through failovers, ISR changes and migrations; invalid ones exercise rejection.

mod common;

use std::net::SocketAddr;

use common::*;
use kv_ctrl::{MemberState, MigPhase, NodeId, ShardId, SlotRanges};
use kv_topo::state::ShardStatus;
use kv_topo::{AdminOp, ClusterParams, ClusterState, Reject, Transition};
use proptest::prelude::*;

#[derive(Clone, Debug)]
enum Op {
    NodeAdd { prio: u8 },
    NodeRemove { n: u8, force: bool },
    NodeMaint { n: u8, on: bool },
    ShardCreate { n: u8, min_isr: u8 },
    ShardDelete { s: u8 },
    SlotsAssign { s: u8, a: u16, len: u16 },
    ReplicaAdd { s: u8, n: u8 },
    ReplicaRemove { s: u8, m: u8 },
    ReplicaResync { n: u8 },
    ParamSet { k: u8, v: u32 },
    MigrateStart { slot: u16, len: u16, dst: u8, barriers: u8 },
    MigrateCtl { m: u8, what: u8 },
    Health { n: u8, fail: bool },
    Promote { s: u8, pick: u8, flagged: u8, handoff: bool, skew: i8 },
    Isr { s: u8, m: u8, add: bool, skew: i8 },
    SetState { s: u8, m: u8, state: u8 },
    Phase { m: u8, phase: u8, flag_ok: bool },
    /// The migration's natural next phase (Copy -> Ready -> Cutover -> Done).
    Advance { m: u8 },
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => any::<u8>().prop_map(|prio| Op::NodeAdd { prio }),
        1 => (any::<u8>(), any::<bool>()).prop_map(|(n, force)| Op::NodeRemove { n, force }),
        1 => (any::<u8>(), any::<bool>()).prop_map(|(n, on)| Op::NodeMaint { n, on }),
        2 => (any::<u8>(), 0u8..4).prop_map(|(n, min_isr)| Op::ShardCreate { n, min_isr }),
        1 => any::<u8>().prop_map(|s| Op::ShardDelete { s }),
        2 => (any::<u8>(), 0u16..16384, 0u16..4000).prop_map(|(s, a, len)| Op::SlotsAssign { s, a, len }),
        3 => (any::<u8>(), any::<u8>()).prop_map(|(s, n)| Op::ReplicaAdd { s, n }),
        1 => (any::<u8>(), any::<u8>()).prop_map(|(s, m)| Op::ReplicaRemove { s, m }),
        1 => any::<u8>().prop_map(|n| Op::ReplicaResync { n }),
        1 => (0u8..8, 0u32..5000).prop_map(|(k, v)| Op::ParamSet { k, v }),
        2 => (0u16..16384, 0u16..300, any::<u8>(), 0u8..4).prop_map(|(slot, len, dst, barriers)| Op::MigrateStart { slot, len, dst, barriers }),
        2 => (any::<u8>(), 0u8..3).prop_map(|(m, what)| Op::MigrateCtl { m, what }),
        4 => (any::<u8>(), any::<bool>()).prop_map(|(n, fail)| Op::Health { n, fail }),
        4 => (any::<u8>(), any::<u8>(), any::<u8>(), any::<bool>(), prop_oneof![8 => Just(0i8), 1 => Just(-1i8), 1 => Just(1i8)])
            .prop_map(|(s, pick, flagged, handoff, skew)| Op::Promote { s, pick, flagged, handoff, skew }),
        4 => (any::<u8>(), any::<u8>(), any::<bool>(), prop_oneof![8 => Just(0i8), 1 => Just(-1i8), 1 => Just(1i8)])
            .prop_map(|(s, m, add, skew)| Op::Isr { s, m, add, skew }),
        3 => (any::<u8>(), any::<u8>(), 0u8..6).prop_map(|(s, m, state)| Op::SetState { s, m, state }),
        4 => (any::<u8>(), 0u8..5, prop_oneof![9 => Just(true), 1 => Just(false)])
            .prop_map(|(m, phase, flag_ok)| Op::Phase { m, phase, flag_ok }),
        4 => any::<u8>().prop_map(|m| Op::Advance { m }),
    ]
}

const STATES: [MemberState; 6] = [
    MemberState::InSync,
    MemberState::Reconciling,
    MemberState::Syncing,
    MemberState::CatchingUp,
    MemberState::Failed,
    MemberState::NeedsFullSync,
];

const PHASES: [MigPhase; 5] = [MigPhase::Copy, MigPhase::Ready, MigPhase::Cutover, MigPhase::Done, MigPhase::Paused];

fn unknown_node() -> NodeId {
    NodeId([0xEE; 20])
}

fn pick_node(s: &ClusterState, n: u8) -> NodeId {
    if s.nodes.is_empty() { unknown_node() } else { s.nodes[usize::from(n) % s.nodes.len()].id }
}

fn pick_shard(s: &ClusterState, i: u8) -> ShardId {
    if s.shards.is_empty() { ShardId(4000) } else { s.shards[usize::from(i) % s.shards.len()].id }
}

fn pick_member(s: &ClusterState, sid: ShardId, m: u8) -> NodeId {
    match s.shard(sid) {
        Some(sh) if !sh.members.is_empty() => sh.members[usize::from(m) % sh.members.len()].node,
        _ => unknown_node(),
    }
}

fn pick_migration(s: &ClusterState, m: u8) -> u64 {
    if s.migrations.is_empty() { 999_999 } else { s.migrations[usize::from(m) % s.migrations.len()].id }
}

/// Resolves an abstract op against the current state. `serial` numbers new nodes, shards and
/// migrations (deterministic).
fn resolve(s: &ClusterState, op: &Op, serial: &mut u32) -> Transition {
    *serial += 1;
    let k = *serial;
    match *op {
        Op::NodeAdd { prio } => {
            let mut b = [0u8; 20];
            b[0] = 0x5A;
            b[16..].copy_from_slice(&k.to_be_bytes());
            let ip = std::net::Ipv4Addr::from(0x0B00_0000 + k);
            node_add_at(NodeId(b), SocketAddr::new(ip.into(), 6379), prio % 4)
        }
        Op::NodeRemove { n, force } => admin(AdminOp::NodeRemove { id: pick_node(s, n), force }),
        Op::NodeMaint { n, on } => admin(AdminOp::NodeMaint { id: pick_node(s, n), on }),
        Op::ShardCreate { n, min_isr } => {
            admin(AdminOp::ShardCreate { id: ShardId(100 + (k % 50) as u16), primary: pick_node(s, n), min_isr })
        }
        Op::ShardDelete { s: i } => admin(AdminOp::ShardDelete { id: pick_shard(s, i) }),
        Op::SlotsAssign { s: i, a, len } => {
            let b = a.saturating_add(len).min(16383);
            admin(AdminOp::SlotsAssign { slots: SlotRanges(vec![(a, b)]), shard: pick_shard(s, i) })
        }
        Op::ReplicaAdd { s: i, n } => admin(AdminOp::ReplicaAdd { shard: pick_shard(s, i), node: pick_node(s, n) }),
        Op::ReplicaRemove { s: i, m } => {
            let sid = pick_shard(s, i);
            admin(AdminOp::ReplicaRemove { shard: sid, node: pick_member(s, sid, m) })
        }
        Op::ReplicaResync { n } => admin(AdminOp::ReplicaResync { node: pick_node(s, n) }),
        Op::ParamSet { k: key, v } => {
            let keys = ["lease_ms", "renew_ms", "fail_grace_ms", "repl_ack_timeout_ms", "requirepass", "partitions", "bogus", "lease-ms"];
            admin(AdminOp::ParamSet { key: keys[usize::from(key)].into(), value: v.to_string() })
        }
        Op::MigrateStart { slot, len, dst, barriers } => admin(AdminOp::MigrateStart {
            id: u64::from(k),
            slots: SlotRanges(vec![(slot, slot.saturating_add(len).min(16383))]),
            dst: pick_shard(s, dst),
            rate_mbps: Some(50),
            barriers,
        }),
        Op::MigrateCtl { m, what } => {
            let id = pick_migration(s, m);
            admin(match what {
                0 => AdminOp::MigratePause { id },
                1 => AdminOp::MigrateResume { id },
                _ => AdminOp::MigrateAbort { id },
            })
        }
        Op::Health { n, fail } => Transition::Health { node: pick_node(s, n), fail },
        Op::Promote { s: i, pick, flagged, handoff, skew } => {
            let sid = pick_shard(s, i);
            let Some(sh) = s.shard(sid) else {
                return Transition::Health { node: unknown_node(), fail: true };
            };
            let isr: Vec<NodeId> = sh.isr().map(|m| m.node).collect();
            let to = if isr.is_empty() { unknown_node() } else { isr[usize::from(pick) % isr.len()] };
            let flagged_set: Vec<NodeId> =
                sh.members.iter().enumerate().filter(|(j, _)| flagged & (1 << (j % 8)) != 0).map(|(_, m)| m.node).collect();
            let take = vec![u64::from(k); usize::from(s.params.partitions)];
            let t = if handoff {
                s.handoff_promote(sid, to, take)
            } else {
                s.failover_promote(sid, to, &|n: &NodeId| flagged_set.contains(n), take)
            };
            match t {
                Ok(Transition::Promote { shard, primary, term, isr, term_end }) => Transition::Promote {
                    shard,
                    primary,
                    term: term.saturating_add_signed(i32::from(skew)),
                    isr,
                    term_end,
                },
                _ => Transition::Promote {
                    shard: sid,
                    primary: to,
                    term: sh.term + 1,
                    isr: vec![to],
                    term_end: kv_ctrl::TermEnd { term: sh.term, takeover: vec![] },
                },
            }
        }
        Op::Isr { s: i, m, add, skew } => {
            let sid = pick_shard(s, i);
            let term = s.shard(sid).map_or(1, |sh| sh.term).saturating_add_signed(i32::from(skew));
            Transition::Isr { shard: sid, term, node: pick_member(s, sid, m), add }
        }
        Op::SetState { s: i, m, state } => {
            let sid = pick_shard(s, i);
            Transition::MemberState { shard: sid, node: pick_member(s, sid, m), state: STATES[usize::from(state)] }
        }
        Op::Phase { m, phase, flag_ok } => {
            let phase = PHASES[usize::from(phase)];
            let flag = matches!(phase, MigPhase::Cutover | MigPhase::Done);
            Transition::Migration { id: pick_migration(s, m), phase, slots_to_dst: if flag_ok { flag } else { !flag } }
        }
        Op::Advance { m } => {
            let id = pick_migration(s, m);
            let next = match s.migrations.iter().find(|x| x.id == id).map(|x| x.phase) {
                Some(MigPhase::Copy) => MigPhase::Ready,
                Some(MigPhase::Ready) => MigPhase::Cutover,
                _ => MigPhase::Done,
            };
            Transition::Migration { id, phase: next, slots_to_dst: matches!(next, MigPhase::Cutover | MigPhase::Done) }
        }
    }
}

fn base(rich: bool) -> ClusterState {
    if rich { three_by_two() } else { ClusterState::initial(ClusterParams::default(), [5; 16]) }
}

/// Runs a sequence one transition per batch: (transition, result) per step.
fn run(start: &ClusterState, ops: &[Op]) -> Vec<(Transition, Result<ClusterState, Reject>, ClusterState)> {
    let mut s = start.clone();
    let mut serial = 0;
    let mut out = Vec::new();
    for o in ops {
        let t = resolve(&s, o, &mut serial);
        let r = s.apply(std::slice::from_ref(&t));
        let before = s.clone();
        if let Ok(n) = &r {
            s = n.clone();
        }
        out.push((t, r, before));
    }
    out
}

/// Nodes as the operator registered them (everything but the FAIL flag, which health updates).
fn registry(s: &ClusterState) -> Vec<(NodeId, SocketAddr, SocketAddr, SocketAddr, String, u8, u16, u64, bool)> {
    s.nodes
        .iter()
        .map(|n| (n.id, n.ctrl, n.client, n.repl, n.zone.clone(), n.priority, n.partitions, n.added_version, n.flags.maint))
        .collect()
}

fn member_lists(s: &ClusterState) -> Vec<(ShardId, Vec<NodeId>, u8)> {
    s.shards.iter().map(|sh| (sh.id, sh.members.iter().map(|m| m.node).collect(), sh.min_isr)).collect()
}

fn migration_records(s: &ClusterState) -> Vec<(u64, SlotRanges, ShardId, ShardId, Option<u32>, u8)> {
    s.migrations.iter().map(|m| (m.id, m.slots.clone(), m.src, m.dst, m.rate_mbps, m.barriers)).collect()
}

fn normalized_for_batch(s: &ClusterState) -> ClusterState {
    let mut s = s.clone();
    s.version = 0;
    for n in &mut s.nodes {
        n.added_version = 0;
    }
    s
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 192, ..ProptestConfig::default() })]

    #[test]
    fn apply_is_deterministic(rich in any::<bool>(), ops in prop::collection::vec(op(), 1..60), chunk in 2usize..6) {
        let start = base(rich);
        let a = run(&start, &ops);
        let b = run(&start, &ops);
        prop_assert_eq!(a.len(), b.len());
        for ((ta, ra, _), (tb, rb, _)) in a.iter().zip(&b) {
            prop_assert_eq!(ta, tb);
            match (ra, rb) {
                (Ok(x), Ok(y)) => prop_assert_eq!(x.encode(), y.encode()),
                (Err(x), Err(y)) => prop_assert_eq!(x, y),
                _ => prop_assert!(false, "different outcomes for {:?}", ta),
            }
        }
        // the same transition on a decoded copy gives the same result
        for (t, r, before) in &a {
            let copy = ClusterState::decode(&before.encode()).unwrap();
            prop_assert!(copy == *before);
            let r2 = copy.apply(std::slice::from_ref(t));
            prop_assert_eq!(r.as_ref().map(ClusterState::encode), r2.as_ref().map(ClusterState::encode));
        }
        // batching accepted steps equals applying them one by one (versions aside)
        let accepted: Vec<Transition> = a.iter().filter(|(_, r, _)| r.is_ok()).map(|(t, _, _)| t.clone()).collect();
        let mut seq = start.clone();
        for t in &accepted {
            seq = seq.apply(std::slice::from_ref(t)).unwrap();
        }
        let mut batched = start.clone();
        for c in accepted.chunks(chunk) {
            batched = batched.apply(c).unwrap();
        }
        prop_assert_eq!(batched.version, start.version + accepted.chunks(chunk).count() as u64);
        prop_assert!(normalized_for_batch(&batched) == normalized_for_batch(&seq));
    }

    #[test]
    fn non_admin_transitions_keep_membership(rich in any::<bool>(), ops in prop::collection::vec(op(), 1..80)) {
        for (t, r, before) in run(&base(rich), &ops) {
            let Ok(after) = r else { continue };
            prop_assert_eq!(after.check_invariants(), Ok(()));
            if t.is_admin() {
                continue;
            }
            prop_assert_eq!(registry(&after), registry(&before), "nodes changed by {:?}", t);
            prop_assert_eq!(member_lists(&after), member_lists(&before), "member lists changed by {:?}", t);
            prop_assert!(after.params == before.params);
            prop_assert_eq!(after.cluster_id, before.cluster_id);
            prop_assert_eq!(migration_records(&after), migration_records(&before), "migrations started/ended by {:?}", t);
            prop_assert_eq!(after.assigned_slots(), before.assigned_slots());
            for slot in 0..16384usize {
                let (o, n) = (before.slots[slot], after.slots[slot]);
                if o != n {
                    let ok = before.migrations.iter().any(|m| {
                        m.slots.contains(slot as u16)
                            && ((o == m.src.0 && n == m.dst.0) || (o == m.dst.0 && n == m.src.0))
                    });
                    prop_assert!(ok, "slot {} moved {} -> {} by {:?}", slot, o, n, t);
                }
            }
            // no non-admin path starts a full sync
            for (sb, sa) in before.shards.iter().zip(&after.shards) {
                for (mb, ma) in sb.members.iter().zip(&sa.members) {
                    if ma.state == MemberState::Syncing {
                        prop_assert_eq!(mb.state, MemberState::Syncing, "{:?} started a full sync", t);
                    }
                }
            }
        }
    }

    #[test]
    fn epochs_strictly_increase(rich in any::<bool>(), ops in prop::collection::vec(op(), 1..80)) {
        let start = base(rich);
        let mut last = start.version;
        for (t, r, before) in run(&start, &ops) {
            match r {
                Ok(after) => {
                    prop_assert_eq!(after.version, before.version + 1);
                    prop_assert!(after.version > last);
                    last = after.version;
                    for sa in &after.shards {
                        let Some(sb) = before.shard(sa.id) else { continue };
                        prop_assert!(sa.term >= sb.term, "term decreased by {:?}", t);
                        if sa.term != sb.term {
                            let promoted = matches!(&t, Transition::Promote { shard, .. } if *shard == sa.id);
                            prop_assert!(promoted, "term changed without a promotion: {:?}", t);
                            prop_assert_eq!(sa.term, sb.term + 1);
                            prop_assert_eq!(sa.history.last().map(|h| h.term), Some(sb.term));
                            prop_assert_eq!(sa.status, ShardStatus::Serving);
                        }
                    }
                }
                Err(_) => {
                    // a stale or skipped promotion term is never accepted
                    if let Transition::Promote { shard, term, .. } = &t
                        && let Some(sh) = before.shard(*shard)
                    {
                        prop_assert!(*term != sh.term + 1 || before.apply(std::slice::from_ref(&t)).is_err());
                    }
                }
            }
        }
    }
}

/// A shard of `k` members (member 0 is the primary) with the given static data, registered in
/// `reg_order`, with node ids `ids[j]` for member j.
#[derive(Clone, Debug)]
struct ShardSpec {
    prio: Vec<u8>,
    maint: Vec<bool>,
    in_isr: Vec<bool>,
    fail: Vec<bool>,
    lease: Vec<bool>,
}

fn shard_spec() -> impl Strategy<Value = ShardSpec> {
    (2usize..7).prop_flat_map(|k| {
        (
            prop::collection::vec(0u8..4, k),
            prop::collection::vec(prop::bool::weighted(0.2), k),
            prop::collection::vec(prop::bool::weighted(0.8), k),
            prop::collection::vec(prop::bool::weighted(0.15), k),
            prop::collection::vec(prop::bool::weighted(0.8), k),
        )
            .prop_map(|(prio, maint, in_isr, fail, lease)| ShardSpec { prio, maint, in_isr, fail, lease })
    })
}

fn build_shard(spec: &ShardSpec, ids: &[NodeId], reg_order: &[usize]) -> ClusterState {
    let k = spec.prio.len();
    let mut s = ClusterState::initial(ClusterParams::default(), [1; 16]);
    let adds: Vec<Transition> = reg_order
        .iter()
        .map(|&j| node_add_at(ids[j], client_addr(j as u8 + 1), spec.prio[j]))
        .collect();
    s = apply_ok(&s, &adds);
    let sid = ShardId(1);
    s = apply_ok(&s, &[admin(AdminOp::ShardCreate { id: sid, primary: ids[0], min_isr: 1 })]);
    for j in 1..k {
        s = apply_ok(&s, &[admin(AdminOp::ReplicaAdd { shard: sid, node: ids[j] })]);
        if spec.in_isr[j] {
            s = apply_ok(&s, &[Transition::Isr { shard: sid, term: 1, node: ids[j], add: true }]);
        }
    }
    let mut b = Vec::new();
    for j in 0..k {
        if spec.maint[j] {
            b.push(admin(AdminOp::NodeMaint { id: ids[j], on: true }));
        }
        if spec.fail[j] {
            b.push(Transition::Health { node: ids[j], fail: true });
        }
    }
    apply_ok(&s, &b)
}

fn distinct_ids(raw: &[[u8; 20]]) -> Option<Vec<NodeId>> {
    let ids: Vec<NodeId> = raw.iter().copied().map(NodeId).collect();
    let distinct = ids.iter().enumerate().all(|(i, a)| ids[..i].iter().all(|b| a != b));
    distinct.then_some(ids)
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    #[test]
    fn choose_primary_invariant_under_id_permutation(
        spec in shard_spec(),
        raw_a in prop::collection::vec(any::<[u8; 20]>(), 7),
        raw_b in prop::collection::vec(any::<[u8; 20]>(), 7),
        perm in Just((0usize..7).collect::<Vec<_>>()).prop_shuffle(),
        unusable_mask in any::<u8>(),
    ) {
        let k = spec.prio.len();
        let (Some(ids_a), Some(ids_b)) = (distinct_ids(&raw_a), distinct_ids(&raw_b)) else { return Ok(()) };
        // b: other ids, and the nodes registered in a permuted order
        let reg_a: Vec<usize> = (0..k).collect();
        let reg_b: Vec<usize> = perm.iter().copied().filter(|&j| j < k).collect();
        let sa = build_shard(&spec, &ids_a, &reg_a);
        let sb = build_shard(&spec, &ids_b, &reg_b);
        let member_idx = |ids: &[NodeId], n: NodeId| ids.iter().position(|x| *x == n).unwrap();
        let lease_a = |n: &NodeId| spec.lease[member_idx(&ids_a, *n)];
        let lease_b = |n: &NodeId| spec.lease[member_idx(&ids_b, *n)];
        let order_a: Vec<usize> = sa.promotion_order(ShardId(1), &lease_a).into_iter().map(|n| member_idx(&ids_a, n)).collect();
        let order_b: Vec<usize> = sb.promotion_order(ShardId(1), &lease_b).into_iter().map(|n| member_idx(&ids_b, n)).collect();
        prop_assert_eq!(&order_a, &order_b);
        let unusable_a: Vec<NodeId> = (0..k).filter(|j| unusable_mask & (1 << j) != 0).map(|j| ids_a[j]).collect();
        let unusable_b: Vec<NodeId> = (0..k).filter(|j| unusable_mask & (1 << j) != 0).map(|j| ids_b[j]).collect();
        let ca = sa.choose_primary(ShardId(1), &lease_a, &unusable_a).map(|n| member_idx(&ids_a, n));
        let cb = sb.choose_primary(ShardId(1), &lease_b, &unusable_b).map(|n| member_idx(&ids_b, n));
        prop_assert_eq!(ca, cb);
        // the rule itself: eligible = ISR - primary, lease held, no FAIL; non-maintenance first,
        // then highest priority, then earliest member position
        let mut expect: Vec<usize> = (1..k).filter(|&j| spec.in_isr[j] && spec.lease[j] && !spec.fail[j]).collect();
        expect.sort_by_key(|&j| (spec.maint[j], std::cmp::Reverse(spec.prio[j]), j));
        prop_assert_eq!(order_a, expect);
    }

    #[test]
    fn promote_isr_independent_of_fence_replies(
        spec in shard_spec(),
        flagged_mask in any::<u8>(),
        answered_mask in any::<u8>(),
        applied in prop::collection::vec(any::<u64>(), 64),
        applied2 in prop::collection::vec(any::<u64>(), 64),
    ) {
        let k = spec.prio.len();
        let ids: Vec<NodeId> = (0..k).map(|j| nid(j as u8 + 1)).collect();
        let s = build_shard(&spec, &ids, &(0..k).collect::<Vec<_>>());
        let s = apply_ok(&s, &[Transition::Health { node: ids[0], fail: true }]);
        let lease = |n: &NodeId| spec.lease[ids.iter().position(|x| x == n).unwrap()];
        let flagged = |n: &NodeId| flagged_mask & (1 << ids.iter().position(|x| x == n).unwrap()) != 0;
        // sequential fencing: the candidates that did not answer become unusable, in order
        let order = s.promotion_order(ShardId(1), &lease);
        let unusable: Vec<NodeId> = order
            .iter()
            .copied()
            .take_while(|n| answered_mask & (1 << ids.iter().position(|x| x == n).unwrap()) == 0)
            .collect();
        let Some(chosen) = s.choose_primary(ShardId(1), &lease, &unusable) else {
            prop_assert_eq!(unusable.len(), order.len());
            return Ok(());
        };
        prop_assert_eq!(Some(&chosen), order.get(unusable.len()));
        let t1 = s.failover_promote(ShardId(1), chosen, &flagged, applied).unwrap();
        let t2 = s.failover_promote(ShardId(1), chosen, &flagged, applied2).unwrap();
        let (Transition::Promote { primary: p1, isr: i1, term: m1, .. }, Transition::Promote { primary: p2, isr: i2, term: m2, .. }) = (&t1, &t2) else {
            panic!("not a promotion");
        };
        prop_assert_eq!(p1, p2);
        prop_assert_eq!(i1, i2);
        prop_assert_eq!(m1, m2);
        // the ISR is committed ISR - old primary - flagged, from committed state only
        let committed: Vec<NodeId> = s.shard(ShardId(1)).unwrap().isr().map(|m| m.node).collect();
        let expect: Vec<NodeId> = committed.into_iter().filter(|n| *n == chosen || (*n != ids[0] && !flagged(n))).collect();
        prop_assert_eq!(i1, &expect);
        let after = s.apply(&[t1]).unwrap();
        let sh = after.shard(ShardId(1)).unwrap();
        prop_assert_eq!(sh.primary, Some(chosen));
        prop_assert_eq!(sh.isr().map(|m| m.node).collect::<Vec<_>>(), expect);
    }
}

/// The generated sequences reach the interesting transitions (so the properties above are not
/// vacuous): accepted promotions, ISR changes, cutovers, finished and paused migrations.
#[test]
fn generator_reaches_failovers_and_migrations() {
    use proptest::strategy::ValueTree;
    use proptest::test_runner::{Config, TestRunner};
    let mut runner = TestRunner::new_with_rng(Config::default(), proptest::test_runner::TestRng::deterministic_rng(
        proptest::test_runner::RngAlgorithm::ChaCha,
    ));
    let strat = (any::<bool>(), prop::collection::vec(op(), 40..80));
    let mut counts = std::collections::BTreeMap::<&'static str, usize>::new();
    for _ in 0..200 {
        let (rich, ops) = strat.new_tree(&mut runner).unwrap().current();
        for (t, r, _) in run(&base(rich), &ops) {
            let key = match (&t, r.is_ok()) {
                (Transition::Promote { .. }, true) => "promote",
                (Transition::Isr { add: true, .. }, true) => "isr-expand",
                (Transition::Isr { add: false, .. }, true) => "isr-shrink",
                (Transition::Migration { phase: MigPhase::Cutover, .. }, true) => "cutover",
                (Transition::Migration { phase: MigPhase::Done, .. }, true) => "done",
                (Transition::Admin(AdminOp::MigrateStart { .. }), true) => "migrate-start",
                (Transition::Health { fail: true, .. }, true) => "health-fail",
                (_, false) => "rejected",
                _ => "other",
            };
            *counts.entry(key).or_default() += 1;
        }
    }
    for k in ["promote", "isr-expand", "isr-shrink", "cutover", "done", "migrate-start", "health-fail", "rejected"] {
        assert!(counts.get(k).copied().unwrap_or(0) >= 20, "{k}: {counts:?}");
    }
}
