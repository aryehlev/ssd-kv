//! Failover by promotion: a unilateral static choice (DESIGN §23.9, LC-25, LC-27).
//!
//! Eligible = committed ISR minus the failed primary P, restricted to members whose lease the
//! leader holds and that are not in maintenance (maintenance nodes only if no other). The new
//! primary is chosen by [`choose_primary`]: highest static operator priority, then earliest
//! position in the shard's operator-ordered member list; replication positions, node ids and
//! reply timing never take part. Only the chosen node is fenced (`Fence { new_term: t + 1 }`);
//! if it does not answer within the fence timeout it is marked unusable for this failover and
//! the next node by the same rule is fenced, sequentially, never several at once. After P's
//! lease has expired the leader commits `Promote { isr: committed ISR - P - nodes already
//! flagged }`, independent of any reply, and pushes the assignment.
//!
//! When no other member is eligible the shard is `NoPrimary` (CLUSTERDOWN for its slots); once
//! P itself renews again it is re-promoted with term t + 1 (the RF1 path), fenced first like
//! any other choice.
//!
//! A graceful handoff ([`run_handoff`], `kvadm failover`, maintenance) asks the primary to stop
//! taking writes and reach its own position barrier; its `HandoffDone` positions become the
//! closing term's takeover point and the old primary stays an InSync member.

use std::sync::Arc;
use std::time::Duration;

use kv_ctrl::{CoordToNode, MemberState, NodeId, NodeToCoord, ShardId};
use kv_topo::state::{ShardStatus, StaticRank, static_choice};

use crate::push::Pusher;
use crate::wsr::{ProposeError, Wsr};

/// static operator data only: no position, score or reply field (LC-25)
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Candidate {
    pub priority: u8,
    pub order: usize,
}

/// no NodeId argument by construction (LC-27)
///
/// The index (into `eligible`) of the candidate with the highest priority, then the earliest
/// member order, skipping the indexes in `unusable`.
pub fn choose_primary(eligible: &[Candidate], unusable: &[usize]) -> Option<usize> {
    let ranks: Vec<StaticRank> = eligible
        .iter()
        .map(|c| StaticRank {
            priority: c.priority,
            order: c.order,
        })
        .collect();
    static_choice(&ranks, unusable)
}

/// How long the chosen node has to answer its fence (§23.9 step 3).
pub const FENCE_TIMEOUT: Duration = Duration::from_millis(500);
/// How long a primary has to finish a graceful handoff.
pub const HANDOFF_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub enum FailoverError {
    NoEligibleMember,
    NotLeader,
    Propose(crate::wsr::ProposeError),
    UnknownShard,
    /// The shard no longer needs this failover (already promoted, or its primary changed).
    Superseded,
    /// Handoff: the target is not an InSync ISR member with a held lease.
    BadTarget,
    /// Handoff: the primary did not complete it in time.
    HandoffTimeout,
}

impl std::fmt::Display for FailoverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FailoverError::NoEligibleMember => f.write_str("no eligible member to promote"),
            FailoverError::NotLeader => f.write_str("this coordinator is not the leader"),
            FailoverError::Propose(e) => write!(f, "{e}"),
            FailoverError::UnknownShard => f.write_str("unknown shard"),
            FailoverError::Superseded => f.write_str("the shard changed meanwhile"),
            FailoverError::BadTarget => {
                f.write_str("the target is not an in-sync ISR member holding a lease")
            }
            FailoverError::HandoffTimeout => {
                f.write_str("the primary did not complete the handoff in time")
            }
        }
    }
}

impl std::error::Error for FailoverError {}

/// Waits until a new leader may promote: `lease_ms + 1000 ms + fail_grace_ms` after it won
/// (§23.4), so every lease a previous leader granted has run out.
async fn new_leader_wait(wsr: &Wsr, push: &Pusher) -> Result<(), FailoverError> {
    loop {
        if !wsr.is_leader() {
            return Err(FailoverError::NotLeader);
        }
        let p = &wsr.committed().params;
        let wait_ns = (u64::from(p.lease_ms) + 1000 + u64::from(p.fail_grace_ms)) * 1_000_000;
        let ready_at = push.leader_since_ns().saturating_add(wait_ns);
        let now = kv_common::clock::boot_ns();
        if now >= ready_at {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_nanos((ready_at - now).min(200_000_000))).await;
    }
}

/// Waits until `n`'s lease has certainly expired by the leader's rule (§23.9 step 4).
async fn lease_gone(wsr: &Wsr, push: &Pusher, n: &NodeId) -> Result<(), FailoverError> {
    loop {
        if !wsr.is_leader() {
            return Err(FailoverError::NotLeader);
        }
        let now = kv_common::clock::boot_ns();
        match push.lease_expiry(n) {
            Some(exp) if now < exp => {
                tokio::time::sleep(Duration::from_nanos((exp - now).min(200_000_000))).await;
            }
            _ => return Ok(()),
        }
    }
}

/// While a failover fences a node other than the failed primary, that primary gets no lease
/// (so the step 4 wait for its lease ends); released when the failover run ends, however it
/// ends.
struct LeaseHoldOff {
    push: Arc<Pusher>,
    node: NodeId,
    on: bool,
}

impl LeaseHoldOff {
    fn new(push: &Arc<Pusher>, node: NodeId) -> Self {
        LeaseHoldOff { push: push.clone(), node, on: false }
    }

    fn set(&mut self) {
        if !self.on {
            self.on = true;
            let n = self.node;
            self.push.with_lease(|ls| ls.detector.hold_off(n));
        }
    }
}

impl Drop for LeaseHoldOff {
    fn drop(&mut self) {
        if self.on {
            let n = self.node;
            self.push.with_lease(|ls| ls.detector.release_hold(&n));
        }
    }
}

pub async fn run_failover(wsr: Arc<Wsr>, push: Arc<Pusher>, shard: ShardId, failed: NodeId) -> Result<(), FailoverError> {
    new_leader_wait(&wsr, &push).await?;
    let mut unusable: Vec<NodeId> = Vec::new();
    let mut hold = LeaseHoldOff::new(&push, failed);
    loop {
        if !wsr.is_leader() {
            return Err(FailoverError::NotLeader);
        }
        let s = wsr.committed();
        let sh = s.shard(shard).ok_or(FailoverError::UnknownShard)?;
        if sh.status == ShardStatus::Serving || sh.primary != Some(failed) {
            return Err(FailoverError::Superseded);
        }
        let now = kv_common::clock::boot_ns();
        let held = |n: &NodeId| push.lease_held(n, now);
        let mut chosen = s.choose_primary(shard, &held, &unusable);
        if chosen.is_none() && sh.status == ShardStatus::NoPrimary {
            // RF1 path: the failed primary itself is back (flag cleared, lease held).
            let back = s.node(&failed).is_some_and(|r| !r.flags.fail)
                && sh.member(&failed).is_some_and(|m| m.in_isr)
                && held(&failed)
                && !unusable.contains(&failed);
            if back {
                chosen = Some(failed);
            }
        }
        let Some(n) = chosen else {
            return Err(FailoverError::NoEligibleMember);
        };
        let term = sh.term;
        let new_term = term.checked_add(1).ok_or(FailoverError::Superseded)?;
        let partitions = usize::from(s.params.partitions);
        let req = push.next_req();
        if n != failed {
            // From the fence on, the failed primary's lease only runs out: the leader grants
            // it no new one (its renewals may be healthy again), so the wait below ends.
            hold.set();
        }
        // One fence, to the chosen node only; no other node is asked anything.
        let takeover = match push
            .request(&n, CoordToNode::Fence { req, shard, new_term }, FENCE_TIMEOUT)
            .await
        {
            Ok(NodeToCoord::FenceAck { req: r, applied }) if r == req && applied.len() == partitions => {
                applied
            }
            other => {
                tracing::warn!(shard = shard.0, node = %n.hex40(), "fence not acknowledged ({}); next by the static rule",
                    match other { Ok(m) => m.kind(), Err(e) => e.kind() });
                unusable.push(n);
                continue;
            }
        };
        if n != failed {
            lease_gone(&wsr, &push, &failed).await?;
        }
        let s = wsr.committed();
        let sh = s.shard(shard).ok_or(FailoverError::UnknownShard)?;
        if sh.term != term || sh.primary != Some(failed) || sh.status == ShardStatus::Serving {
            return Err(FailoverError::Superseded);
        }
        // The new ISR: committed ISR - P - nodes the detector or a committed flag marks. Fence
        // replies never shape it (LC-25).
        let flagged = |x: &NodeId| push.suspected(x) || s.node(x).is_none_or(|r| r.flags.fail);
        let t = s
            .failover_promote(shard, n, &flagged, takeover)
            .map_err(|e| FailoverError::Propose(ProposeError::Rejected(e)))?;
        wsr.propose(vec![t]).await.map_err(FailoverError::Propose)?;
        tracing::info!(shard = shard.0, primary = %n.hex40(), term = new_term, "promoted by the static rule");
        return Ok(());
    }
}

/// Graceful handoff of `shard` to `to` (or, without it, to the first node of the static rule
/// over InSync ISR members with a held lease). Returns the new primary.
pub async fn run_handoff(
    wsr: Arc<Wsr>,
    push: Arc<Pusher>,
    shard: ShardId,
    to: Option<NodeId>,
) -> Result<NodeId, FailoverError> {
    if !wsr.is_leader() {
        return Err(FailoverError::NotLeader);
    }
    let s = wsr.committed();
    let sh = s.shard(shard).ok_or(FailoverError::UnknownShard)?;
    if sh.status != ShardStatus::Serving {
        return Err(FailoverError::Superseded);
    }
    let Some(p) = sh.primary else {
        return Err(FailoverError::Superseded);
    };
    let now = kv_common::clock::boot_ns();
    let held = |n: &NodeId| push.lease_held(n, now);
    let target = match to {
        Some(n) => {
            let ok = n != p
                && sh
                    .member(&n)
                    .is_some_and(|m| m.in_isr && m.state == MemberState::InSync)
                && s.node(&n).is_some_and(|r| !r.flags.fail)
                && held(&n);
            if !ok {
                return Err(FailoverError::BadTarget);
            }
            n
        }
        None => s
            .handoff_order(shard, &held)
            .into_iter()
            .next()
            .ok_or(FailoverError::NoEligibleMember)?,
    };
    let term = sh.term;
    let req = push.next_req();
    let last = match push
        .request(&p, CoordToNode::Handoff { req, shard, to: target }, HANDOFF_TIMEOUT)
        .await
    {
        Ok(NodeToCoord::HandoffDone { req: r, last }) if r == req => last,
        _ => return Err(FailoverError::HandoffTimeout),
    };
    let s = wsr.committed();
    let sh = s.shard(shard).ok_or(FailoverError::UnknownShard)?;
    if sh.term != term || sh.primary != Some(p) {
        return Err(FailoverError::Superseded);
    }
    let t = s
        .handoff_promote(shard, target, last)
        .map_err(|e| FailoverError::Propose(ProposeError::Rejected(e)))?;
    wsr.propose(vec![t]).await.map_err(FailoverError::Propose)?;
    tracing::info!(shard = shard.0, primary = %target.hex40(), term = term + 1, "handoff committed");
    Ok(target)
}

/// Raises `shard`'s term from `term` to `new_term` by a graceful handoff of its primary to
/// itself (§23.12): a member brought records of a term above the shard's from another shard
/// (terms count per shard, §23.2), which every seq of the shard's current term would sort
/// below. The primary answers new writes with TRYAGAIN, reaches its position barrier and
/// keeps its lease; the coordinator commits `Promote { same primary, new_term, term_end = its
/// barrier positions }` with the ISR and member states unchanged.
pub async fn run_term_raise(
    wsr: Arc<Wsr>,
    push: Arc<Pusher>,
    shard: ShardId,
    term: u32,
    new_term: u32,
) -> Result<(), FailoverError> {
    if !wsr.is_leader() {
        return Err(FailoverError::NotLeader);
    }
    let s = wsr.committed();
    let sh = s.shard(shard).ok_or(FailoverError::UnknownShard)?;
    let Some(p) = sh.primary.filter(|_| sh.status == ShardStatus::Serving && sh.term == term)
    else {
        return Err(FailoverError::Superseded);
    };
    if new_term <= term {
        return Err(FailoverError::Superseded);
    }
    let req = push.next_req();
    let last = match push
        .request(&p, CoordToNode::Handoff { req, shard, to: p }, HANDOFF_TIMEOUT)
        .await
    {
        Ok(NodeToCoord::HandoffDone { req: r, last }) if r == req => last,
        _ => return Err(FailoverError::HandoffTimeout),
    };
    let s = wsr.committed();
    let sh = s.shard(shard).ok_or(FailoverError::UnknownShard)?;
    if sh.term != term || sh.primary != Some(p) || sh.status != ShardStatus::Serving {
        return Err(FailoverError::Superseded);
    }
    let t = s
        .term_raise_promote(shard, new_term, last)
        .map_err(|e| FailoverError::Propose(ProposeError::Rejected(e)))?;
    wsr.propose(vec![t]).await.map_err(FailoverError::Propose)?;
    tracing::info!(shard = shard.0, term = new_term, "term raise committed");
    Ok(())
}

/// Test fixtures shared by the module tests (the operator's static priority field lives here
/// and in admin.rs only, LC-14).
#[cfg(test)]
pub(crate) mod test_support {
    use std::net::SocketAddr;

    use kv_ctrl::NodeId;
    use kv_topo::{AdminOp, Transition};

    /// `NodeAdd` of `id` with control/client/replication ports `base`, `base + 1`, `base + 2`
    /// on 127.0.0.1, priority 0.
    pub fn node_add(id: NodeId, base: u16, partitions: u16) -> Transition {
        let a = |p: u16| SocketAddr::from(([127, 0, 0, 1], p));
        Transition::Admin(AdminOp::NodeAdd {
            id,
            ctrl: a(base),
            client: a(base + 1),
            repl: a(base + 2),
            zone: String::new(),
            priority: 0,
            partitions,
        })
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn static_rule() {
        let c = |priority, order| Candidate { priority, order };
        assert_eq!(choose_primary(&[], &[]), None);
        assert_eq!(choose_primary(&[c(0, 0), c(0, 1)], &[]), Some(0));
        assert_eq!(choose_primary(&[c(0, 0), c(5, 1)], &[]), Some(1));
        assert_eq!(choose_primary(&[c(5, 2), c(5, 1)], &[]), Some(1));
        assert_eq!(choose_primary(&[c(5, 2), c(5, 1)], &[1]), Some(0));
        assert_eq!(choose_primary(&[c(5, 2), c(5, 1)], &[0, 1]), None);
    }

    proptest! {
        /// The choice depends on the static data only: shuffling the candidates (as a renaming
        /// of their ids would) picks the same candidate.
        #[test]
        fn choice_invariant_under_permutation(
            v in proptest::collection::vec((0u8..4, 0usize..1000), 1..8),
            rot in 0usize..8,
        ) {
            // distinct member orders, as in a real member list
            let mut seen = std::collections::BTreeSet::new();
            let cands: Vec<Candidate> = v.into_iter()
                .filter(|(_, o)| seen.insert(*o))
                .map(|(priority, order)| Candidate { priority, order })
                .collect();
            let a = choose_primary(&cands, &[]).map(|i| cands[i]);
            let mut rotated = cands.clone();
            let k = rot % rotated.len();
            rotated.rotate_left(k);
            rotated.reverse();
            let b = choose_primary(&rotated, &[]).map(|i| rotated[i]);
            prop_assert_eq!(a, b);
        }
    }
}
