//! Partition meta and node meta codecs (DESIGN §10.5, §10.6; AbBlock magics `KVPMETA1`,
//! `KVNMETA1`; device 0).
//!
//! Partition meta (copy A at `pmeta_page + 2p`, B at `+ 2p + 1`): `seq_floor` (standalone:
//! `next_seq` after restart; reservations of `next + 2^24`, E12), `term`, per-db
//! `flush_cutoff` (records of db i with `seq < flush_cutoff[i]` are dead, E5), replica
//! positions `repl_durable` / `repl_commit` (lower bounds) and the takeover points of past terms
//! (`history`, newest first). Flag b0 SYNCING marks a running merge full sync (§23.7).
//!
//! ```text
//! 0x018 u16 partition   0x01A u16 partitions   0x01C u32 flags (b0 SYNCING)
//! 0x020 u64 seq_floor   0x028 u32 term         0x02C u32 zero
//! 0x030 u64 flush_cutoff[16]
//! 0x0B0 u64 repl_durable_seq   0x0B8 u64 repl_commit_seq
//! 0x0C0 history[4] x { u32 term, u32 zero, u64 end_seq }
//! ```
//!
//! Node meta (`nmeta_page` A/B): cluster id, node id, shard (0xFFFF none), role (0 none,
//! 1 primary, 2 replica), the highest fence term accepted, and the applied assignment version.
//!
//! ```text
//! 0x018 [16] cluster_id   0x028 [20] node_id   0x03C u16 shard_id   0x03E u8 role   0x03F u8 zero
//! 0x040 u32 fence_term    0x044 u32 zero       0x048 u64 assignment_version
//! ```

use kv_common::abblock::AbCopy;

use crate::layout::{MAX_PARTITIONS, PAGE, Seq, ab_build, ab_parse, ab_select, le};

/// `KVPMETA1`
pub const PMETA_MAGIC: u64 = kv_common::abblock::magic(b"KVPMETA1");
/// `KVNMETA1`
pub const NMETA_MAGIC: u64 = kv_common::abblock::magic(b"KVNMETA1");
/// PartitionMeta.flags b0
pub const PMETA_SYNCING: u32 = 1;
/// NodeMeta.shard when the node has no shard.
pub const SHARD_NONE: u16 = 0xFFFF;
/// NodeMeta.role values.
pub const ROLE_NONE: u8 = 0;
pub const ROLE_PRIMARY: u8 = 1;
pub const ROLE_REPLICA: u8 = 2;
/// Databases with a flush cutoff.
pub const CUTOFF_DBS: usize = 16;

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
    /// The block written at format: no cutoffs, no replication state, `seq_floor = (0, 1)` so
    /// that `Seq::ZERO` is never assigned to a record.
    pub fn initial(partition: u16) -> Self {
        PartitionMeta {
            partition,
            flags: 0,
            seq_floor: Seq::new(0, 1),
            term: 0,
            flush_cutoff: [0; CUTOFF_DBS],
            repl_durable: Seq::ZERO,
            repl_commit: Seq::ZERO,
            history: [(0, 0); 4],
        }
    }

    pub fn syncing(&self) -> bool {
        self.flags & PMETA_SYNCING != 0
    }

    /// True when a record of `db` with version `seq` is dead by a flush cutoff (E5).
    pub fn flushed(&self, db: u8, seq: Seq) -> bool {
        self.flush_cutoff.get(usize::from(db)).is_some_and(|&c| seq.0 < c)
    }

    pub fn encode(&self, partitions: u16, seq: u64, out: &mut [u8; PAGE]) {
        ab_build(PMETA_MAGIC, seq, out, |b| {
            le::put_u16(b, 0x018, self.partition);
            le::put_u16(b, 0x01A, partitions);
            le::put_u32(b, 0x01C, self.flags);
            le::put_u64(b, 0x020, self.seq_floor.0);
            le::put_u32(b, 0x028, self.term);
            for (i, &c) in self.flush_cutoff.iter().enumerate() {
                le::put_u64(b, 0x030 + 8 * i, c);
            }
            le::put_u64(b, 0x0B0, self.repl_durable.0);
            le::put_u64(b, 0x0B8, self.repl_commit.0);
            for (i, &(term, end)) in self.history.iter().enumerate() {
                le::put_u32(b, 0x0C0 + 16 * i, term);
                le::put_u64(b, 0x0C8 + 16 * i, end);
            }
        });
    }

    pub fn decode(b: &[u8]) -> Option<(u64, Self)> {
        Self::decode_with_partitions(b).map(|(seq, _, m)| (seq, m))
    }

    /// Decodes and also returns the stored partition count.
    pub fn decode_with_partitions(b: &[u8]) -> Option<(u64, u16, Self)> {
        ab_parse(PMETA_MAGIC, b, |b| {
            let partitions = le::u16_at(b, 0x01A);
            let partition = le::u16_at(b, 0x018);
            if partitions == 0 || partitions > MAX_PARTITIONS || partition >= partitions {
                return None;
            }
            let mut flush_cutoff = [0u64; CUTOFF_DBS];
            for (i, c) in flush_cutoff.iter_mut().enumerate() {
                *c = le::u64_at(b, 0x030 + 8 * i);
            }
            let mut history = [(0u32, 0u64); 4];
            for (i, h) in history.iter_mut().enumerate() {
                *h = (le::u32_at(b, 0x0C0 + 16 * i), le::u64_at(b, 0x0C8 + 16 * i));
            }
            let m = PartitionMeta {
                partition,
                flags: le::u32_at(b, 0x01C),
                seq_floor: Seq(le::u64_at(b, 0x020)),
                term: le::u32_at(b, 0x028),
                flush_cutoff,
                repl_durable: Seq(le::u64_at(b, 0x0B0)),
                repl_commit: Seq(le::u64_at(b, 0x0B8)),
                history,
            };
            Some((partitions, m))
        })
        .map(|(seq, (partitions, m))| (seq, partitions, m))
    }

    /// Reads both copies of partition `partition`'s block (of a node with `partitions`
    /// partitions) and returns the valid one with the higher `block_seq`; copies naming another
    /// partition or partition count are ignored.
    pub fn pick(partition: u16, partitions: u16, a: &[u8], b: &[u8]) -> Option<(AbCopy, u64, Self)> {
        let ok = |d: Option<(u64, u16, Self)>| {
            d.filter(|(_, n, m)| *n == partitions && m.partition == partition).map(|(s, _, m)| (s, m))
        };
        ab_select(ok(Self::decode_with_partitions(a)), ok(Self::decode_with_partitions(b)))
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
    /// The block written at format: no cluster, no shard, no role.
    pub fn initial() -> Self {
        NodeMeta {
            cluster_id: [0; 16],
            node_id: [0; 20],
            shard: SHARD_NONE,
            role: ROLE_NONE,
            fence_term: 0,
            assignment_version: 0,
        }
    }

    pub fn encode(&self, seq: u64, out: &mut [u8; PAGE]) {
        ab_build(NMETA_MAGIC, seq, out, |b| {
            b[0x018..0x028].copy_from_slice(&self.cluster_id);
            b[0x028..0x03C].copy_from_slice(&self.node_id);
            le::put_u16(b, 0x03C, self.shard);
            b[0x03E] = self.role;
            le::put_u32(b, 0x040, self.fence_term);
            le::put_u64(b, 0x048, self.assignment_version);
        });
    }

    /// Rejects unknown roles.
    pub fn decode(b: &[u8]) -> Option<(u64, Self)> {
        ab_parse(NMETA_MAGIC, b, |b| {
            let role = b[0x03E];
            if role > ROLE_REPLICA {
                return None;
            }
            Some(NodeMeta {
                cluster_id: le::bytes(b, 0x018),
                node_id: le::bytes(b, 0x028),
                shard: le::u16_at(b, 0x03C),
                role,
                fence_term: le::u32_at(b, 0x040),
                assignment_version: le::u64_at(b, 0x048),
            })
        })
    }

    /// Reads both copies and returns the valid one with the higher `block_seq`.
    pub fn pick(a: &[u8], b: &[u8]) -> Option<(AbCopy, u64, Self)> {
        ab_select(Self::decode(a), Self::decode(b))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig { cases: 256, failure_persistence: None, ..ProptestConfig::default() })]

        #[test]
        fn prop_pmeta_round_trip(
            pp in (0u32..4).prop_flat_map(|e| { let n = 16u16 << e; (Just(n), 0..n) }),
            flags in any::<u32>(),
            seqs in any::<(u64, u64, u64)>(),
            term in any::<u32>(),
            cut in any::<[u64; 16]>(),
            hist in any::<[(u32, u64); 4]>(),
            seq in any::<u64>(),
        ) {
            let (partitions, partition) = pp;
            let m = PartitionMeta {
                partition,
                flags,
                seq_floor: Seq(seqs.0),
                term,
                flush_cutoff: cut,
                repl_durable: Seq(seqs.1),
                repl_commit: Seq(seqs.2),
                history: hist,
            };
            let mut b = [0u8; PAGE];
            m.encode(partitions, seq, &mut b);
            prop_assert_eq!(PartitionMeta::decode_with_partitions(&b), Some((seq, partitions, m)));
        }

        #[test]
        fn prop_nmeta_round_trip(
            ids in any::<([u8; 16], [u8; 20])>(),
            shard in any::<u16>(),
            role in 0u8..=2,
            fence in any::<u32>(),
            ver in any::<u64>(),
            seq in any::<u64>(),
        ) {
            let m = NodeMeta { cluster_id: ids.0, node_id: ids.1, shard, role, fence_term: fence, assignment_version: ver };
            let mut b = [0u8; PAGE];
            m.encode(seq, &mut b);
            prop_assert_eq!(NodeMeta::decode(&b), Some((seq, m)));
        }
    }

    pub(crate) fn sample_pmeta() -> PartitionMeta {
        let mut m = PartitionMeta::initial(5);
        m.flags = PMETA_SYNCING;
        m.seq_floor = Seq::new(3, 1 << 24);
        m.term = 3;
        m.flush_cutoff[0] = 17;
        m.flush_cutoff[15] = 0xFFFF_0000_1234;
        m.repl_durable = Seq::new(3, 100);
        m.repl_commit = Seq::new(3, 99);
        m.history = [(3, 0), (2, Seq::new(2, 500).0), (1, Seq::new(1, 9).0), (0, 0)];
        m
    }

    pub(crate) fn sample_nmeta() -> NodeMeta {
        NodeMeta {
            cluster_id: *b"cluster-id-01234",
            node_id: *b"node-id-0123456789ab",
            shard: 7,
            role: ROLE_REPLICA,
            fence_term: 12,
            assignment_version: 0x0102_0304_0506_0708,
        }
    }

    #[test]
    fn pmeta_round_trip() {
        let m = sample_pmeta();
        let mut b = [0u8; PAGE];
        m.encode(64, 3, &mut b);
        assert_eq!(&b[..8], b"KVPMETA1");
        assert_eq!(le::u16_at(&b, 0x18), 5);
        assert_eq!(le::u16_at(&b, 0x1A), 64);
        assert_eq!(le::u32_at(&b, 0x1C), 1);
        assert_eq!(le::u64_at(&b, 0x20), Seq::new(3, 1 << 24).0);
        assert_eq!(le::u64_at(&b, 0x30), 17);
        assert_eq!(le::u64_at(&b, 0xA8), 0xFFFF_0000_1234);
        assert_eq!(le::u64_at(&b, 0xB0), Seq::new(3, 100).0);
        assert_eq!(le::u32_at(&b, 0xD0), 2);
        assert_eq!(le::u64_at(&b, 0xD8), Seq::new(2, 500).0);
        assert!(le::zero(&b, 0x2C..0x30));
        assert!(le::zero(&b, 0x100..PAGE));
        assert_eq!(PartitionMeta::decode(&b), Some((3, m.clone())));
        assert_eq!(PartitionMeta::decode_with_partitions(&b).map(|(_, p, _)| p), Some(64));
        assert!(m.syncing());
        assert!(m.flushed(0, Seq(16)));
        assert!(!m.flushed(0, Seq(17)));
        assert!(!m.flushed(1, Seq(0)));
        assert!(!m.flushed(16, Seq(0)));
    }

    #[test]
    fn pmeta_rejects_bad_partition() {
        let mut b = [0u8; PAGE];
        PartitionMeta::initial(64).encode(64, 1, &mut b);
        assert!(PartitionMeta::decode(&b).is_none());
        PartitionMeta::initial(1).encode(200, 1, &mut b);
        assert!(PartitionMeta::decode(&b).is_none());
        PartitionMeta::initial(1).encode(16, 1, &mut b);
        assert!(PartitionMeta::decode(&b).is_some());
        assert!(PartitionMeta::pick(1, 32, &b, &b).is_none());
        assert!(PartitionMeta::pick(2, 16, &b, &b).is_none());
        assert_eq!(PartitionMeta::pick(1, 16, &[0u8; PAGE], &b).map(|(c, s, _)| (c, s)), Some((AbCopy::B, 1)));
    }

    #[test]
    fn nmeta_round_trip() {
        let m = sample_nmeta();
        let mut b = [0u8; PAGE];
        m.encode(8, &mut b);
        assert_eq!(&b[..8], b"KVNMETA1");
        assert_eq!(&b[0x18..0x28], b"cluster-id-01234");
        assert_eq!(&b[0x28..0x3C], b"node-id-0123456789ab");
        assert_eq!(le::u16_at(&b, 0x3C), 7);
        assert_eq!(b[0x3E], 2);
        assert_eq!(b[0x3F], 0);
        assert_eq!(le::u32_at(&b, 0x40), 12);
        assert!(le::zero(&b, 0x44..0x48));
        assert_eq!(le::u64_at(&b, 0x48), 0x0102_0304_0506_0708);
        assert!(le::zero(&b, 0x50..PAGE));
        assert_eq!(NodeMeta::decode(&b), Some((8, m)));
        let mut bad = sample_nmeta();
        bad.role = 3;
        bad.encode(8, &mut b);
        assert!(NodeMeta::decode(&b).is_none());
        NodeMeta::initial().encode(1, &mut b);
        assert_eq!(NodeMeta::decode(&b).map(|(_, m)| m.shard), Some(SHARD_NONE));
    }
}
