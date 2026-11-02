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
    /// Recorded primaries of shards whose running failover has fenced (or is fencing) another
    /// node: no lease is granted to them until that failover ends, so its wait for their lease
    /// (§23.9 step 4) always ends. Without this, a primary that renews healthy again while its
    /// shard waits for a promotion keeps its lease forever and the shard never serves again.
    held_off: hashbrown::HashSet<NodeId, foldhash::fast::RandomState>,
    /// When this leadership's detector first saw each node in the committed state: a node
    /// added after the leader started is silent since its addition, not since the leader's
    /// start (otherwise a freshly added node is flagged FAIL before it could renew once).
    first_seen: hashbrown::HashMap<NodeId, u64, foldhash::fast::RandomState>,
    /// `first_seen` holds the nodes registered when this leadership started.
    primed: bool,
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
            held_off: hashbrown::HashSet::default(),
            first_seen: hashbrown::HashMap::default(),
            primed: false,
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

    /// May the leader grant `n` a lease? Not while a FAIL flag is committed or pending, and not
    /// while a failover that fenced another node waits for `n`'s lease to end.
    pub fn may_grant(&self, s: &ClusterState, n: &NodeId) -> bool {
        !self.fail_pending.contains(n) && !self.held_off.contains(n) && s.node(n).is_some_and(|r| !r.flags.fail)
    }

    /// A failover of the shard whose recorded primary is `n` fences another node: `n` gets no
    /// lease until [`Detector::release_hold`].
    pub fn hold_off(&mut self, n: NodeId) {
        self.held_off.insert(n);
    }

    /// The failover that held `n`'s leases off ended.
    pub fn release_hold(&mut self, n: &NodeId) {
        self.held_off.remove(n);
    }

    /// Whether `n`'s leases are held off by a running failover.
    pub fn held_off(&self, n: &NodeId) -> bool {
        self.held_off.contains(n)
    }

    /// Forgets all suspicion (the coordinator stopped leading).
    pub fn clear(&mut self) {
        self.pfail.clear();
        self.fail_pending.clear();
        self.held_off.clear();
        self.first_seen.clear();
        self.primed = false;
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
        self.held_off.retain(|n| s.node(n).is_some());
        self.first_seen.retain(|n, _| s.node(n).is_some());
        if !self.primed {
            // the nodes registered when the leadership started are silent since its start
            self.primed = true;
            for rec in &s.nodes {
                self.first_seen.insert(rec.id, leader_since_ns);
            }
        }
        let mut out = Vec::new();
        for rec in &s.nodes {
            let n = rec.id;
            let seen = *self.first_seen.entry(n).or_insert(now_ns);
            let renewed = leases.last_renew(&n);
            // never renewed: silent since the leader started or since the node was added,
            // whichever is later
            let last = renewed.unwrap_or(seen).max(leader_since_ns);
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
            } else if self.fail_pending.contains(&n) || (silent >= fail_after && (renewed.is_some() || has_role(s, &n))) {
                // a node that never renewed to this leader and has no shard role is not
                // flagged: it is still joining (`node add`) and its flag would change nothing
                out.push(Transition::Health { node: n, fail: true });
            }
        }
        out
    }
}

/// `n` is a member of some shard.
fn has_role(s: &ClusterState, n: &NodeId) -> bool {
    s.shards.iter().any(|sh| sh.member(n).is_some())
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

    /// A recorded primary whose shard's failover fenced another node gets no lease while that
    /// failover runs, even when it renews healthy (otherwise the wait for its lease never ends
    /// and the shard stays down).
    #[test]
    fn a_held_off_primary_gets_no_lease_until_released() {
        let s = state();
        let mut d = Detector::new();
        assert!(d.may_grant(&s, &nid(1)));
        d.hold_off(nid(1));
        assert!(d.held_off(&nid(1)));
        assert!(!d.may_grant(&s, &nid(1)));
        assert!(d.may_grant(&s, &nid(2)), "only that node");
        d.release_hold(&nid(1));
        assert!(d.may_grant(&s, &nid(1)));
        d.hold_off(nid(1));
        d.clear();
        assert!(d.may_grant(&s, &nid(1)), "a new leadership starts without holds");
    }

    /// A node added after the leader started is silent since its addition: it gets the full
    /// grace before PFAIL/FAIL, and without a shard role it is never flagged before its first
    /// renewal.
    #[test]
    fn a_freshly_added_node_is_not_flagged_before_it_renews() {
        let s = state();
        let mut l = LeaseTable::new(s.params.lease_ms);
        let mut d = Detector::new();
        let since = 1_000 * MS;
        l.on_renew(nid(1), 1, since);
        l.on_renew(nid(2), 1, since);
        assert!(d.tick(&s, &l, since + 10 * MS, since).is_empty());
        // node 3 is added an hour into the leadership
        let t_add = since + 3_600_000 * MS;
        l.on_renew(nid(1), 1, t_add);
        l.on_renew(nid(2), 1, t_add);
        let s3 = s.apply(&[crate::failover::test_support::node_add(nid(3), 9500, s.params.partitions)]).unwrap();
        assert!(d.tick(&s3, &l, t_add + MS, since).is_empty(), "not at once");
        assert!(!d.suspected(&nid(3)));
        l.on_renew(nid(1), 1, t_add + 5_000 * MS);
        l.on_renew(nid(2), 1, t_add + 5_000 * MS);
        let out = d.tick(&s3, &l, t_add + 5_000 * MS, since);
        assert!(out.is_empty(), "no role and never renewed: never flagged");
        assert!(d.suspected(&nid(3)), "PFAIL only (leader RAM)");
        // once it renewed, the normal rule applies
        l.on_renew(nid(3), 7, t_add + 5_000 * MS);
        assert!(d.tick(&s3, &l, t_add + 5_100 * MS, since).is_empty());
        let out = d.tick(&s3, &l, t_add + 7_000 * MS, since);
        assert!(out.iter().any(|t| matches!(t, Transition::Health { node, fail: true } if *node == nid(3))));
    }

    #[test]
    fn leader_start_is_the_baseline_for_silent_nodes() {
        let s = state();
        let l = LeaseTable::new(s.params.lease_ms);
        let mut d = Detector::new();
        let since = 5_000 * MS;
        assert!(d.tick(&s, &l, since + 1000 * MS, since).is_empty());
        let out = d.tick(&s, &l, since + 1712 * MS, since);
        // node 1 (a shard primary) is flagged; node 2 has no role and never renewed: PFAIL only
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], Transition::Health { node, fail: true } if node == nid(1)));
        assert!(d.suspected(&nid(2)));
    }
}
