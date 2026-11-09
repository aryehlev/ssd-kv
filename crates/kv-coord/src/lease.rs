//! Node leases (DESIGN §23.4, §23.10).
//!
//! Nodes send `Renew { seq, incarnation, .. }` every `renew_ms`; the leader answers
//! `LeaseGrant { seq, lease_ms }` only while its own check-quorum lease is valid and records
//! the grant time. The leader considers a lease held until `t_grant + lease_ms * 1.001 + 10 ms`
//! (the node uses the conservative `t_send + lease_ms * 0.95 - 10 ms`). A new incarnation
//! (process restart) is reported to the failure detector, and the grant of the previous process
//! is void at once (that process is gone, so the lease wait of a failover is skipped). A newly
//! elected leader renews leases at once but promotes nobody until `lease_ms + 1000 ms +
//! fail_grace_ms` after winning.
//!
//! All times are CLOCK_BOOTTIME nanoseconds (`kv_common::clock::boot_ns`), which count VM
//! suspension (§23.10).

use kv_ctrl::{Health, NodeId};

/// Result of one renewal.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RenewOutcome {
    /// Answer with `LeaseGrant`.
    Granted,
    /// Same node, new process: answer, and tell the detector (lease wait can be skipped).
    NewIncarnation,
    /// Stale or out-of-order renewal.
    Ignored,
}

/// What the leader knows about one node's lease.
#[derive(Clone, Copy, Debug)]
struct LeaseEntry {
    incarnation: u64,
    /// Receive time of the newest renewal.
    last_renew_ns: u64,
    /// Time of the newest grant to the current incarnation.
    granted_ns: Option<u64>,
    /// Health carried by the newest renewal.
    health: Health,
}

/// Lease margin of the leader's rule: `lease_ms * 1.001 + 10 ms`, in nanoseconds.
pub fn leader_lease_ns(lease_ms: u32) -> u64 {
    let ms = u64::from(lease_ms);
    ms * 1_000_000 + ms * 1_000 + 10_000_000
}

pub struct LeaseTable {
    lease_ms: u32,
    held: hashbrown::HashMap<NodeId, LeaseEntry, foldhash::fast::RandomState>,
}

impl LeaseTable {
    pub fn new(lease_ms: u32) -> Self {
        LeaseTable {
            lease_ms,
            held: hashbrown::HashMap::default(),
        }
    }

    pub fn lease_ms(&self) -> u32 {
        self.lease_ms
    }

    /// A committed `lease_ms` change applies to grants from now on; the expiry of earlier
    /// grants is computed with the larger of the two values (never shortened).
    pub fn set_lease_ms(&mut self, lease_ms: u32) {
        self.lease_ms = self.lease_ms.max(lease_ms);
        if lease_ms < self.lease_ms && self.held.values().all(|e| e.granted_ns.is_none()) {
            self.lease_ms = lease_ms;
        }
    }

    /// Records a renewal received at `t_recv_ns` (CLOCK_BOOTTIME). The caller grants (and
    /// calls [`LeaseTable::grant`]) on `Granted` or `NewIncarnation` when it may.
    pub fn on_renew(&mut self, n: NodeId, incarnation: u64, t_recv_ns: u64) -> RenewOutcome {
        match self.held.get_mut(&n) {
            None => {
                self.held.insert(
                    n,
                    LeaseEntry {
                        incarnation,
                        last_renew_ns: t_recv_ns,
                        granted_ns: None,
                        health: Health::default(),
                    },
                );
                RenewOutcome::Granted
            }
            Some(e) => {
                if t_recv_ns < e.last_renew_ns {
                    return RenewOutcome::Ignored;
                }
                e.last_renew_ns = t_recv_ns;
                if e.incarnation != incarnation {
                    // The previous process is gone: its lease ended with it.
                    e.incarnation = incarnation;
                    e.granted_ns = None;
                    RenewOutcome::NewIncarnation
                } else {
                    RenewOutcome::Granted
                }
            }
        }
    }

    /// Records the health a renewal carried.
    pub fn set_health(&mut self, n: &NodeId, h: Health) {
        if let Some(e) = self.held.get_mut(n) {
            e.health = h;
        }
    }

    /// The health of the newest renewal from `n`.
    pub fn health(&self, n: &NodeId) -> Option<Health> {
        self.held.get(n).map(|e| e.health)
    }

    /// Records a `LeaseGrant` sent to `n` at `t_grant_ns`.
    pub fn grant(&mut self, n: &NodeId, t_grant_ns: u64) {
        if let Some(e) = self.held.get_mut(n) {
            e.granted_ns = Some(e.granted_ns.map_or(t_grant_ns, |g| g.max(t_grant_ns)));
        }
    }

    /// The leader's view: `n`'s lease is (possibly) valid at `now_ns`.
    pub fn held(&self, n: &NodeId, now_ns: u64) -> bool {
        self.held
            .get(n)
            .and_then(|e| e.granted_ns)
            .is_some_and(|g| now_ns < g.saturating_add(leader_lease_ns(self.lease_ms)))
    }

    /// When `n`'s lease certainly ends by the leader's rule (`None`: no grant to the current
    /// process, so nothing to wait for).
    pub fn expiry_ns(&self, n: &NodeId) -> Option<u64> {
        self.held
            .get(n)
            .and_then(|e| e.granted_ns)
            .map(|g| g.saturating_add(leader_lease_ns(self.lease_ms)))
    }

    /// Nodes that were granted a lease that has run out by `now_ns`.
    pub fn expired(&self, now_ns: u64) -> Vec<NodeId> {
        self.held
            .iter()
            .filter(|(_, e)| {
                e.granted_ns
                    .is_some_and(|g| now_ns >= g.saturating_add(leader_lease_ns(self.lease_ms)))
            })
            .map(|(n, _)| *n)
            .collect()
    }

    /// Receive time of the newest renewal from `n`.
    pub fn last_renew(&self, n: &NodeId) -> Option<u64> {
        self.held.get(n).map(|e| e.last_renew_ns)
    }

    /// The incarnation of the newest renewal from `n`.
    pub fn incarnation(&self, n: &NodeId) -> Option<u64> {
        self.held.get(n).map(|e| e.incarnation)
    }

    /// Withdraws `n`'s grant (the node is flagged failed, or the leader stops granting).
    pub fn revoke(&mut self, n: &NodeId) {
        if let Some(e) = self.held.get_mut(n) {
            e.granted_ns = None;
        }
    }

    /// Forgets `n` (removed from the cluster).
    pub fn forget(&mut self, n: &NodeId) {
        self.held.remove(n);
    }

    /// Forgets every node not in `keep`.
    pub fn retain(&mut self, keep: &dyn Fn(&NodeId) -> bool) {
        self.held.retain(|n, _| keep(n));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: u64 = 1_000_000;

    #[test]
    fn grant_hold_expire() {
        let n = NodeId([1; 20]);
        let mut t = LeaseTable::new(1500);
        assert!(!t.held(&n, 0));
        assert_eq!(t.on_renew(n, 7, 100 * MS), RenewOutcome::Granted);
        // no grant recorded yet: nothing is held
        assert!(!t.held(&n, 100 * MS));
        t.grant(&n, 100 * MS);
        // held until t_grant + 1500 * 1.001 + 10 ms = 100 + 1511.5 ms
        assert!(t.held(&n, 1611 * MS));
        assert!(!t.held(&n, 1612 * MS));
        assert_eq!(t.expiry_ns(&n), Some(1_611_500_000));
        assert!(t.expired(1611 * MS).is_empty());
        assert_eq!(t.expired(1612 * MS), vec![n]);
        // out-of-order renewal
        assert_eq!(t.on_renew(n, 7, 50 * MS), RenewOutcome::Ignored);
        assert_eq!(t.last_renew(&n), Some(100 * MS));
    }

    #[test]
    fn new_incarnation_voids_the_old_grant() {
        let n = NodeId([2; 20]);
        let mut t = LeaseTable::new(1500);
        t.on_renew(n, 1, 0);
        t.grant(&n, 0);
        assert!(t.held(&n, 10 * MS));
        assert_eq!(t.on_renew(n, 2, 20 * MS), RenewOutcome::NewIncarnation);
        assert!(!t.held(&n, 20 * MS));
        assert_eq!(t.expiry_ns(&n), None);
        assert_eq!(t.incarnation(&n), Some(2));
        assert_eq!(t.on_renew(n, 2, 120 * MS), RenewOutcome::Granted);
    }

    #[test]
    fn lease_ms_never_shortens_outstanding_grants() {
        let n = NodeId([3; 20]);
        let mut t = LeaseTable::new(1500);
        t.on_renew(n, 1, 0);
        t.grant(&n, 0);
        t.set_lease_ms(500);
        assert!(t.held(&n, 1000 * MS));
        t.revoke(&n);
        t.set_lease_ms(500);
        assert_eq!(t.lease_ms(), 500);
        t.forget(&n);
        assert_eq!(t.last_renew(&n), None);
    }
}
