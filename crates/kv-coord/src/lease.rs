//! Node leases (DESIGN §23.4, §23.10).
//!
//! Nodes send `Renew { seq, incarnation, .. }` every `renew_ms`; the leader answers
//! `LeaseGrant { seq, lease_ms }` only while its own check-quorum lease is valid and records
//! the grant time. The leader considers a lease held until `t_grant + lease_ms * 1.001 + 10 ms`
//! (the node uses the conservative `t_send + lease_ms * 0.95 - 10 ms`). A new incarnation
//! (process restart) is reported to the failure detector. A newly elected leader renews leases
//! at once but promotes nobody until `lease_ms + 1000 ms + fail_grace_ms` after winning.

use kv_ctrl::NodeId;

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

pub struct LeaseTable {
    lease_ms: u32,
    /// `(node, incarnation, last grant boot-time ns)`
    held: hashbrown::HashMap<NodeId, (u64, u64), foldhash::fast::RandomState>,
}

impl LeaseTable {
    pub fn on_renew(&mut self, n: NodeId, incarnation: u64, t_recv_ns: u64) -> RenewOutcome {
        todo!("DESIGN §23.4: renewal")
    }

    pub fn held(&self, n: &NodeId, now_ns: u64) -> bool {
        todo!("DESIGN §23.4: lease held")
    }

    pub fn expired(&self, now_ns: u64) -> Vec<NodeId> {
        todo!("DESIGN §23.10: expired leases")
    }
}
