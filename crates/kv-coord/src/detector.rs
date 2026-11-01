//! Failure detection (DESIGN §23.10).
//!
//! Only the leader detects failures, from lease renewals: three missed renewals mark a node
//! PFAIL (leader RAM only; redirects for its slots become CLUSTERDOWN); after
//! `t_last_renew + lease_ms * 1.001 + 10 ms + fail_grace_ms`, or on a new incarnation, the leader
//! commits `Health { node, fail: true }`. FAIL is a flag: the node stays a member until the
//! operator removes it. A primary's FAIL starts the static failover (§23.9). Data nodes never
//! heartbeat each other.
//!
//! A FAIL flag is cleared (`Health { fail: false }`, a health change, never a membership change)
//! once the node renews again with a healthy report, unless it is still the recorded primary of
//! a shard whose promotion is pending (the failover decides first; §23.9 "returning members").
//!
//! [`Detector::tick`] is pure over its inputs (committed state, lease table, the time and the
//! leader's start time) so it is tested without a network.

use kv_ctrl::NodeId;
use kv_topo::state::ShardStatus;
use kv_topo::{ClusterState, Transition};

use crate::lease::{LeaseTable, leader_lease_ns};

pub struct Detector {
    /// Nodes currently suspected (leader RAM only).
    pfail: hashbrown::HashSet<NodeId, foldhash::fast::RandomState>,
    /// Nodes that restarted (new incarnation) or reported `health.failed` and whose FAIL flag is
    /// not committed yet: no lease is granted to them meanwhile.
    fail_pending: hashbrown::HashSet<NodeId, foldhash::fast::RandomState>,
}

impl Default for Detector {
    fn default() -> Self {
        Self::new()
    }
}

impl Detector {
    pub fn new() -> Self {
        Detector {
            pfail: hashbrown::HashSet::default(),
            fail_pending: hashbrown::HashSet::default(),
        }
    }

    /// Is `n` currently suspected (PFAIL or FAIL)?
    pub fn suspected(&self, n: &NodeId) -> bool {
        self.pfail.contains(n) || self.fail_pending.contains(n)
    }

    /// The nodes currently suspected.
    pub fn suspects(&self) -> Vec<NodeId> {
        self.pfail.iter().chain(self.fail_pending.iter()).copied().collect()
    }

    /// A renewal from `n` arrived from a new process (§23.10).
    pub fn on_new_incarnation(&mut self, n: NodeId) {
        self.fail_pending.insert(n);
    }

    /// `n` reported `health.failed` in a renewal.
    pub fn on_reported_failed(&mut self, n: NodeId) {
        self.fail_pending.insert(n);
    }

    /// May the leader grant `n` a lease? Not while a FAIL flag is committed or pending.
    pub fn may_grant(&self, s: &ClusterState, n: &NodeId) -> bool {
        !self.fail_pending.contains(n) && s.node(n).is_some_and(|r| !r.flags.fail)
    }

    /// Forgets all suspicion (the coordinator stopped leading).
    pub fn clear(&mut self) {
        self.pfail.clear();
        self.fail_pending.clear();
    }

    /// One detection pass at `now_ns` (CLOCK_BOOTTIME). `leader_since_ns` is when this
    /// coordinator became leader: a node it has not heard from yet counts as last renewed then.
    /// Returns the health transitions to commit (each one alone, in node order).
    pub fn tick(
        &mut self,
        s: &ClusterState,
        leases: &LeaseTable,
        now_ns: u64,
        leader_since_ns: u64,
    ) -> Vec<Transition> {
        let renew_ns = u64::from(s.params.renew_ms) * 1_000_000;
        let fail_after = leader_lease_ns(s.params.lease_ms)
            + u64::from(s.params.fail_grace_ms) * 1_000_000;
        self.pfail.retain(|n| s.node(n).is_some());
        self.fail_pending.retain(|n| s.node(n).is_some());
        let mut out = Vec::new();
        for rec in &s.nodes {
            let n = rec.id;
            let last = leases
                .last_renew(&n)
                .unwrap_or(leader_since_ns)
                .max(leader_since_ns);
            let silent = now_ns.saturating_sub(last);
            if silent > 3 * renew_ns {
                self.pfail.insert(n);
            } else {
                self.pfail.remove(&n);
            }
            if rec.flags.fail {
                // Committed: nothing pending any more.
                self.fail_pending.remove(&n);
                let healthy = leases.health(&n).is_some_and(|h| !h.failed);
                if !self.pfail.contains(&n) && healthy && leases.last_renew(&n).is_some()
                    && !awaiting_promotion(s, &n) {
                        out.push(Transition::Health { node: n, fail: false });
                    }
            } else if silent >= fail_after || self.fail_pending.contains(&n) {
                out.push(Transition::Health { node: n, fail: true });
            }
        }
        out
    }
}

/// `n` is the recorded primary of a shard waiting for a promotion of another member.
fn awaiting_promotion(s: &ClusterState, n: &NodeId) -> bool {
    s.shards
        .iter()
        .any(|sh| sh.primary == Some(*n) && sh.status == ShardStatus::Promoting)
}

#[cfg(test)]
mod tests {
    use kv_ctrl::{Health, ShardId};
    use kv_topo::{AdminOp, ClusterParams};

    use super::*;

    const MS: u64 = 1_000_000;

    fn nid(i: u8) -> NodeId {
        NodeId([i; 20])
    }

    fn state() -> ClusterState {
        let s = ClusterState::initial(ClusterParams::default(), [9; 16]);
        let mut batch = Vec::new();
        for i in 1..=2u8 {
            let base = 9000 + u16::from(i) * 10;
            batch.push(crate::failover::test_support::node_add(nid(i), base, s.params.partitions));
        }
        batch.push(Transition::Admin(AdminOp::ShardCreate {
            id: ShardId(1),
            primary: nid(1),
            min_isr: 1,
        }));
        s.apply(&batch).unwrap()
    }

    #[test]
    fn pfail_then_fail_then_clear() {
        let s = state();
        let mut l = LeaseTable::new(s.params.lease_ms);
        let mut d = Detector::new();
        let t0 = 1_000 * MS;
        l.on_renew(nid(1), 1, t0);
        l.on_renew(nid(2), 1, t0);
        assert!(d.tick(&s, &l, t0 + 250 * MS, t0).is_empty());
        assert!(!d.suspected(&nid(1)));
        // three renewals missed: PFAIL, nothing committed yet
        assert!(d.tick(&s, &l, t0 + 301 * MS, t0).is_empty());
        assert!(d.suspected(&nid(1)) && d.suspected(&nid(2)));
        // lease * 1.001 + 10 ms + grace = 1711.5 ms
        assert!(d.tick(&s, &l, t0 + 1711 * MS, t0).is_empty());
        let out = d.tick(&s, &l, t0 + 1712 * MS, t0);
        assert_eq!(out.len(), 2);
        assert!(matches!(out[0], Transition::Health { fail: true, .. }));
        let s2 = s.apply(&out[1..]).unwrap();
        // node 2 comes back healthy: its flag is cleared
        let t1 = t0 + 2000 * MS;
        l.on_renew(nid(2), 1, t1);
        l.set_health(&nid(2), Health { ready: true, ..Health::default() });
        let out = d.tick(&s2, &l, t1 + MS, t0);
        assert!(out.iter().any(|t| matches!(t, Transition::Health { node, fail: false } if *node == nid(2))));
        assert!(!d.may_grant(&s2, &nid(2)));
    }

    #[test]
    fn new_incarnation_commits_fail_and_blocks_grants() {
        let s = state();
        let mut l = LeaseTable::new(s.params.lease_ms);
        let mut d = Detector::new();
        l.on_renew(nid(2), 1, 0);
        assert!(d.may_grant(&s, &nid(2)));
        l.on_renew(nid(2), 2, 50 * MS);
        d.on_new_incarnation(nid(2));
        assert!(!d.may_grant(&s, &nid(2)));
        let out = d.tick(&s, &l, 60 * MS, 0);
        assert!(out.iter().any(|t| matches!(t, Transition::Health { node, fail: true } if *node == nid(2))));
    }

    #[test]
    fn leader_start_is_the_baseline_for_silent_nodes() {
        let s = state();
        let l = LeaseTable::new(s.params.lease_ms);
        let mut d = Detector::new();
        let since = 5_000 * MS;
        assert!(d.tick(&s, &l, since + 1000 * MS, since).is_empty());
        assert_eq!(d.tick(&s, &l, since + 1712 * MS, since).len(), 2);
    }
}
