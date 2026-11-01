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

use std::sync::Arc;

use kv_ctrl::{NodeId, ShardId};

use crate::push::Pusher;
use crate::wsr::Wsr;

/// static operator data only: no position, score or reply field (LC-25)
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Candidate {
    pub priority: u8,
    pub order: usize,
}

/// no NodeId argument by construction (LC-27)
pub fn choose_primary(eligible: &[Candidate], unusable: &[usize]) -> Option<usize> {
    todo!("DESIGN §23.9: static choice")
}

#[derive(Debug)]
pub enum FailoverError {
    NoEligibleMember,
    NotLeader,
    Propose(crate::wsr::ProposeError),
}

pub async fn run_failover(wsr: Arc<Wsr>, push: Arc<Pusher>, shard: ShardId, failed: NodeId) -> Result<(), FailoverError> {
    todo!("DESIGN §23.9: failover")
}
