//! Standalone multi-partition reads and deletes (DESIGN §6.8, LC-37).
//!
//! MGET, EXISTS, DEL, UNLINK and TOUCH whose keys map to several partitions are split by
//! `FanOutSplit` (a fixed per-command routine) into exactly one sub-request per distinct
//! partition, each carrying key bytes only, and gathered in the connection's reply slot. Not
//! atomic across partitions (documented). Cluster mode never fans out: keys must share one slot.

use kv_cmd::CmdId;
use smallvec::SmallVec;

use crate::cmd::FanOutSplit;

/// Splits the key arguments at `pos` by partition.
pub fn split(argv: &[&[u8]], pos: &[u16], partitions: u16) -> FanOutSplit {
    let mut parts: SmallVec<[(u16, SmallVec<[u16; 4]>); 4]> = SmallVec::new();
    for &i in pos {
        let p = kv_common::key_slot(argv[usize::from(i)]) % partitions;
        match parts.iter_mut().find(|(q, _)| *q == p) {
            Some((_, ix)) => ix.push(i),
            None => parts.push((p, smallvec::smallvec![i])),
        }
    }
    FanOutSplit { parts }
}

/// Commands that may be split across partitions (reads and deletes whose per-key results are
/// independent: integer sums or per-key array elements).
pub fn is_fanout_command(id: CmdId) -> bool {
    matches!(id, CmdId::Mget | CmdId::Exists | CmdId::Del | CmdId::Unlink | CmdId::Touch)
}

/// The argv of sub-request `part`: the command name, then that partition's keys in their
/// original order.
pub fn sub_argv<'a>(split: &FanOutSplit, part: usize, argv: &[&'a [u8]]) -> SmallVec<[&'a [u8]; 8]> {
    let mut out = SmallVec::with_capacity(split.parts[part].1.len() + 1);
    out.push(argv[0]);
    out.extend(split.parts[part].1.iter().map(|&i| argv[usize::from(i)]));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn one_part_per_distinct_partition_in_key_order() {
        let argv: [&[u8]; 6] = [b"MGET", b"{a}1", b"{b}1", b"{a}2", b"{b}2", b"{a}1"];
        let s = split(&argv, &[1, 2, 3, 4, 5], 64);
        assert_eq!(s.parts.len(), 2);
        let pa = kv_common::key_slot(b"a") % 64;
        assert_eq!(s.parts[0].0, pa);
        assert_eq!(s.parts[0].1.as_slice(), &[1, 3, 5]);
        assert_eq!(s.parts[1].1.as_slice(), &[2, 4]);
        assert_eq!(sub_argv(&s, 1, &argv).as_slice(), &[&b"MGET"[..], b"{b}1", b"{b}2"]);
        assert!(is_fanout_command(CmdId::Unlink) && !is_fanout_command(CmdId::Mset));
    }

    proptest! {
        /// LC-37 `fanout_width_equals_partitions`: the split has exactly one sub-request per
        /// distinct partition of the keys, every key lands in the part of its own partition, and
        /// the order of keys inside a part is their argument order.
        #[test]
        fn fanout_width_equals_partitions(keys in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..12), 1..40),
                                          p in prop_oneof![Just(16u16), Just(32), Just(64), Just(128)]) {
            let mut argv: Vec<&[u8]> = vec![b"DEL"];
            argv.extend(keys.iter().map(|k| k.as_slice()));
            let pos: Vec<u16> = (1..argv.len() as u16).collect();
            let s = split(&argv, &pos, p);
            let mut distinct: Vec<u16> = keys.iter().map(|k| kv_common::key_slot(k) % p).collect();
            distinct.sort_unstable();
            distinct.dedup();
            prop_assert_eq!(s.parts.len(), distinct.len());
            let mut seen = 0;
            for (part, ix) in &s.parts {
                prop_assert!(ix.windows(2).all(|w| w[0] < w[1]));
                for &i in ix {
                    prop_assert_eq!(kv_common::key_slot(argv[usize::from(i)]) % p, *part);
                    seen += 1;
                }
            }
            prop_assert_eq!(seen, keys.len());
        }
    }
}
