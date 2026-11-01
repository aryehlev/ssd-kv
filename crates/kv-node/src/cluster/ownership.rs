//! The data-node routing gate (DESIGN §23.6, LC-24).
//!
//! For every command in cluster mode, before execution (and again at queue time inside MULTI):
//! 1. keyless: served locally on the node's own data; keyless writes (FLUSHDB/FLUSHALL) need
//!    role primary with a valid lease (replica: READONLY error);
//! 2. keys in more than one slot -> CROSSSLOT;
//! 3. primary, slot owned, lease valid, slot not sealed out by a migration cutover -> serve;
//!    writes with a committed ISR below `min_isr` -> NOREPLICAS;
//! 4. replica, READONLY connection, read-only command, slot in the shard, lease valid, partition
//!    not in full sync -> serve locally (no replication-state gate, LC-28);
//! 5. slot pending seal on a migration destination -> TRYAGAIN;
//! 6. otherwise -> `-MOVED <slot> <coord_addrs[conn_id % n]>` from the pushed static coordinator
//!    list: on one connection identical for every slot the node does not serve, apart from the
//!    echoed slot number. A data node never names another data node.
//!
//! fields are private to cluster/ownership.rs; kv-node/src/cmd/* may only call gate() and
//! slot_filter() (lint, LC-24)

use std::net::SocketAddr;

use kv_common::SlotSet;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Mode {
    Standalone,
    Cluster,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role {
    Primary,
    Replica,
}

pub struct Ownership {
    mode: Mode,
    role: Option<Role>,
    owned: SlotSet,
    shard_slots: SlotSet,
    sealed_out: SlotSet,
    pending_seal: SlotSet,
    min_isr: u8,
    isr_len: u8,
    /// static list order
    coord_addrs: Vec<SocketAddr>,
    term: u32,
}

/// Decision of the gate.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Gate {
    Serve,
    Moved(u16, SocketAddr),
    CrossSlot,
    NoReplicas,
    TryAgain,
    ReadOnlyReplica,
    ClusterDown,
}

/// opaque predicate: does this node serve slot s for keyless walks
pub struct SlotFilter {
    serves: Option<SlotSet>,
}

impl SlotFilter {
    pub fn serves(&self, slot: u16) -> bool {
        todo!("DESIGN §23.6: slot filter")
    }
}

impl Ownership {
    /// Standalone mode: every slot is served locally.
    pub fn standalone() -> Self {
        Ownership {
            mode: Mode::Standalone,
            role: None,
            owned: SlotSet::new(),
            shard_slots: SlotSet::new(),
            sealed_out: SlotSet::new(),
            pending_seal: SlotSet::new(),
            min_isr: 1,
            isr_len: 1,
            coord_addrs: Vec::new(),
            term: 0,
        }
    }

    /// Builds the gate from an applied assignment.
    pub fn from_assignment(a: &kv_ctrl::NodeAssignment) -> Self {
        todo!("DESIGN §23.6: ownership from assignment")
    }

    /// Moved target = coord_addrs[conn_id % n], independent of slot state
    pub fn gate(&self, slot: Option<u16>, flags: kv_cmd::CmdFlags, readonly_conn: bool, lease_ok: bool, conn_id: u64) -> Gate {
        todo!("DESIGN §23.6: routing gate")
    }

    pub fn slot_filter(&self) -> SlotFilter {
        todo!("DESIGN §23.6: slot filter")
    }
}
