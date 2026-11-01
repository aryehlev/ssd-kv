//! Failure detection (DESIGN §23.10).
//!
//! Only the leader detects failures, from lease renewals: three missed renewals mark a node
//! PFAIL (leader RAM only; redirects for its slots become CLUSTERDOWN); after
//! `t_last_renew + lease_ms * 1.001 + 10 ms + fail_grace_ms`, or on a new incarnation, the leader
//! commits `Health { node, fail: true }`. FAIL is a flag: the node stays a member until the
//! operator removes it. A primary's FAIL starts the static failover (§23.9). Data nodes never
//! heartbeat each other.

use kv_ctrl::NodeId;

pub struct Detector {
    /// Nodes currently suspected (leader RAM only).
    pfail: hashbrown::HashSet<NodeId, foldhash::fast::RandomState>,
}

impl Detector {
    /// Is `n` currently suspected (PFAIL or FAIL)?
    pub fn suspected(&self, n: &NodeId) -> bool {
        self.pfail.contains(n)
    }
}
