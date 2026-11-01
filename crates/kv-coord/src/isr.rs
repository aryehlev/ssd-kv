//! ISR changes (DESIGN §23.8).
//!
//! `IsrShrink { shard, term, member }` from the shard's primary is committed even when the ISR
//! falls below `min_isr`; the primary stops waiting for that member only after the committed
//! assignment arrives. `IsrExpand` is sent by the primary once it counts a caught-up member's
//! acks; after the commit the member is promotable and advertised. Requests of a stale term are
//! ignored. Membership itself never changes here (LC-25).
//!
//! `MemberReady` closes a member's catch-up: a repaired ISR member (Reconciling) becomes InSync
//! and is advertised again; a member that finished an operator-started full sync (Syncing)
//! becomes CatchingUp until its primary's `IsrExpand`.

use kv_ctrl::{MemberState, NodeId, ShardId};
use kv_topo::{ClusterState, Transition};

/// The transition for an ISR request, or `None` when it is stale or already applied.
pub fn isr_transition(s: &ClusterState, shard: ShardId, term: u32, member: NodeId, add: bool) -> Option<Transition> {
    let sh = s.shard(shard)?;
    if sh.term != term || sh.primary == Some(member) {
        return None;
    }
    let m = sh.member(&member)?;
    if add {
        if m.state == MemberState::NeedsFullSync || (m.in_isr && m.state == MemberState::InSync) {
            return None;
        }
    } else if !m.in_isr {
        return None;
    }
    Some(Transition::Isr {
        shard,
        term,
        node: member,
        add,
    })
}

/// An ISR request is accepted only from the shard's current primary.
pub fn from_primary(s: &ClusterState, shard: ShardId, from: &NodeId) -> bool {
    s.shard(shard).is_some_and(|sh| sh.primary == Some(*from))
}

/// The transition for `MemberReady { shard, term, member }` sent by `from` (the member itself
/// or its primary), or `None` when stale or nothing changes.
pub fn member_ready_transition(
    s: &ClusterState,
    from: &NodeId,
    shard: ShardId,
    term: u32,
    member: NodeId,
) -> Option<Transition> {
    let sh = s.shard(shard)?;
    if sh.term != term || (*from != member && sh.primary != Some(*from)) || sh.primary == Some(member) {
        return None;
    }
    let m = sh.member(&member)?;
    let state = match m.state {
        MemberState::Reconciling if m.in_isr => MemberState::InSync,
        MemberState::Syncing => MemberState::CatchingUp,
        _ => return None,
    };
    // `Syncing -> CatchingUp` ends a full sync the operator started; nothing here starts one.
    Some(Transition::MemberState {
        shard,
        node: member,
        state,
    })
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
        for i in 1..=3u8 {
            let base = 9000 + u16::from(i) * 10;
            b.push(crate::failover::test_support::node_add(nid(i), base, s.params.partitions));
        }
        b.push(Transition::Admin(AdminOp::ShardCreate {
            id: ShardId(1),
            primary: nid(1),
            min_isr: 1,
        }));
        b.push(Transition::Admin(AdminOp::ReplicaAdd {
            shard: ShardId(1),
            node: nid(2),
        }));
        s.apply(&b).unwrap()
    }

    #[test]
    fn sync_ready_expand_shrink() {
        let s = state();
        let sh = ShardId(1);
        assert!(isr_transition(&s, sh, 1, nid(2), false).is_none(), "not in the ISR yet");
        assert!(isr_transition(&s, sh, 1, nid(1), true).is_none(), "the primary");
        assert!(isr_transition(&s, sh, 2, nid(2), true).is_none(), "wrong term");
        assert!(isr_transition(&s, sh, 1, nid(3), true).is_none(), "not a member");
        let t = member_ready_transition(&s, &nid(2), sh, 1, nid(2)).unwrap();
        let s = s.apply(&[t]).unwrap();
        assert_eq!(s.shard(sh).unwrap().member(&nid(2)).unwrap().state, MemberState::CatchingUp);
        assert!(member_ready_transition(&s, &nid(3), sh, 1, nid(2)).is_none(), "a stranger");
        assert!(from_primary(&s, sh, &nid(1)) && !from_primary(&s, sh, &nid(2)));
        let t = isr_transition(&s, sh, 1, nid(2), true).unwrap();
        let s = s.apply(&[t]).unwrap();
        let m = s.shard(sh).unwrap().member(&nid(2)).unwrap();
        assert!(m.in_isr && m.state == MemberState::InSync);
        assert!(isr_transition(&s, sh, 1, nid(2), true).is_none(), "already applied");
        let t = isr_transition(&s, sh, 1, nid(2), false).unwrap();
        let s = s.apply(&[t]).unwrap();
        assert!(!s.shard(sh).unwrap().member(&nid(2)).unwrap().in_isr);
        // membership is unchanged by ISR changes
        assert_eq!(s.shard(sh).unwrap().members.len(), 2);
        assert_eq!(s.nodes.len(), 3);
    }
}
