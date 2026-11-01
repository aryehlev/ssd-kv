//! Partition meta and node meta codecs (DESIGN §10.5, §10.6; AbBlock magics `KVPMETA1`,
//! `KVNMETA1`; device 0).
//!
//! Partition meta (copy A at `pmeta_page + 2p`, B at `+ 2p + 1`): `seq_floor` (standalone:
//! `next_seq` after restart; reservations of `next + 2^24`, E12), `term`, per-db
//! `flush_cutoff` (records of db i with `seq < flush_cutoff[i]` are dead, E5), replica
//! positions `repl_durable` / `repl_commit` (lower bounds) and the takeover points of past terms
//! (`history`, newest first). Flag b0 SYNCING marks a running merge full sync (§23.7).
//!
//! Node meta (`nmeta_page` A/B): cluster id, node id, shard (0xFFFF none), role (0 none,
//! 1 primary, 2 replica), the highest fence term accepted, and the applied assignment version.

use crate::layout::{PAGE, Seq};

/// `KVPMETA1`
pub const PMETA_MAGIC: u64 = kv_common::abblock::magic(b"KVPMETA1");
/// `KVNMETA1`
pub const NMETA_MAGIC: u64 = kv_common::abblock::magic(b"KVNMETA1");
/// PartitionMeta.flags b0
pub const PMETA_SYNCING: u32 = 1;
/// NodeMeta.shard when the node has no shard.
pub const SHARD_NONE: u16 = 0xFFFF;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PartitionMeta {
    pub partition: u16,
    /// b0 SYNCING
    pub flags: u32,
    pub seq_floor: Seq,
    pub term: u32,
    pub flush_cutoff: [u64; 16],
    pub repl_durable: Seq,
    pub repl_commit: Seq,
    /// `(term, end_seq)` takeover points of past terms, newest first.
    pub history: [(u32, u64); 4],
}

impl PartitionMeta {
    pub fn encode(&self, partitions: u16, seq: u64, out: &mut [u8; PAGE]) {
        todo!("DESIGN §10.5: partition meta encode")
    }

    pub fn decode(b: &[u8]) -> Option<(u64, Self)> {
        todo!("DESIGN §10.5: partition meta decode")
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct NodeMeta {
    pub cluster_id: [u8; 16],
    pub node_id: [u8; 20],
    pub shard: u16,
    pub role: u8,
    pub fence_term: u32,
    pub assignment_version: u64,
}

impl NodeMeta {
    pub fn encode(&self, seq: u64, out: &mut [u8; PAGE]) {
        todo!("DESIGN §10.6: node meta encode")
    }

    pub fn decode(b: &[u8]) -> Option<(u64, Self)> {
        todo!("DESIGN §10.6: node meta decode")
    }
}
