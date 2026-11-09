//! `kvadm rebalance plan` (DESIGN §23.12).
//!
//! `--by slots` only: a deterministic greedy list of migrate commands, computed from the
//! committed `ClusterState` alone, that evens slot counts per shard. `rebalance apply` runs them
//! one at a time, each started only when the previous one is Done, and stops at the first
//! failover or health change without re-planning.
//!
//! The plan: with `n` shards and `A` assigned slots, the first `A % n` shards (operator order)
//! should own `A / n + 1` slots and the others `A / n`. Receivers (below target) are filled in
//! operator order from donors (above target) in operator order; a donor gives its highest slots
//! first. Slots of unfinished migrations stay where they are. Nothing is measured on the nodes.

use core::fmt;

use kv_ctrl::{MigPhase, ShardId, SlotRanges};
use kv_topo::ClusterState;

/// One planned `kvadm migrate <ranges> --to <shard>`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MigrateCmd {
    pub slots: SlotRanges,
    pub to: ShardId,
}

impl fmt::Display for MigrateCmd {
    /// `migrate <ranges> --to <shard>` (the line format `rebalance apply` reads back).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "migrate {} --to {}", self.slots, self.to.0)
    }
}

impl MigrateCmd {
    /// Parses one plan line (`migrate <ranges> --to <shard>`).
    pub fn parse(line: &str) -> Result<Self, String> {
        let w: Vec<&str> = line.split_whitespace().collect();
        match w.as_slice() {
            ["migrate", ranges, "--to", sid] => {
                let slots = SlotRanges::parse(ranges)
                    .and_then(|r| r.normalized())
                    .filter(|r| !r.is_empty())
                    .ok_or_else(|| format!("invalid slot ranges '{ranges}'"))?;
                let to = sid
                    .parse::<u16>()
                    .map(ShardId)
                    .map_err(|_| format!("invalid shard id '{sid}'"))?;
                Ok(MigrateCmd { slots, to })
            }
            _ => Err(format!("plan line must be 'migrate <ranges> --to <shard>': '{line}'")),
        }
    }

    /// Parses a whole plan (one command per line; blank lines and `#` comments skipped).
    pub fn parse_plan(text: &str) -> Result<Vec<Self>, String> {
        text.lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .map(Self::parse)
            .collect()
    }
}

/// --by slots only; deterministic greedy over ClusterState
pub fn rebalance_plan(s: &ClusterState) -> Vec<MigrateCmd> {
    let n = s.shards.len();
    if n < 2 {
        return Vec::new();
    }
    let busy = |slot: u16| {
        s.migrations
            .iter()
            .any(|m| m.phase != MigPhase::Done && m.slots.contains(slot))
    };
    let counts: Vec<usize> = s
        .shards
        .iter()
        .map(|sh| s.slot_ranges_of(sh.id).slot_count())
        .collect();
    let total: usize = counts.iter().sum();
    let (base, extra) = (total / n, total % n);
    let target = |i: usize| base + usize::from(i < extra);
    // What each donor gives: its highest movable slots beyond its target, in ascending order
    // (so a donor keeps its lowest slots and receivers get contiguous runs).
    let mut give: Vec<std::collections::VecDeque<u16>> = s
        .shards
        .iter()
        .enumerate()
        .map(|(i, sh)| {
            let movable: Vec<u16> = s
                .slot_ranges_of(sh.id)
                .iter_slots()
                .filter(|&x| !busy(x))
                .collect();
            let excess = counts[i].saturating_sub(target(i)).min(movable.len());
            movable[movable.len() - excess..].iter().copied().collect()
        })
        .collect();
    let mut have = counts.clone();
    let mut plan = Vec::new();
    for r in 0..n {
        for d in 0..n {
            if have[r] >= target(r) {
                break;
            }
            if d == r || have[d] <= target(d) {
                continue;
            }
            let k = (target(r) - have[r])
                .min(have[d] - target(d))
                .min(give[d].len());
            if k == 0 {
                continue;
            }
            let moved: Vec<u16> = give[d].drain(..k).collect();
            have[d] -= k;
            have[r] += k;
            plan.push(MigrateCmd {
                slots: ranges_of(&moved),
                to: s.shards[r].id,
            });
        }
    }
    plan
}

fn ranges_of(sorted: &[u16]) -> SlotRanges {
    let mut v: Vec<(u16, u16)> = Vec::new();
    for &x in sorted {
        match v.last_mut() {
            Some((_, hi)) if *hi + 1 == x => *hi = x,
            _ => v.push((x, x)),
        }
    }
    SlotRanges(v)
}

#[cfg(test)]
mod tests {
    use kv_ctrl::NodeId;
    use kv_topo::{AdminOp, ClusterParams, Transition};

    use super::*;

    fn cluster(shards: u8, assign: &[(&str, u16)]) -> ClusterState {
        let s = ClusterState::initial(ClusterParams::default(), [1; 16]);
        let mut b = Vec::new();
        for i in 1..=shards {
            let base = 9000 + u16::from(i) * 10;
            b.push(crate::failover::test_support::node_add(NodeId([i; 20]), base, s.params.partitions));
            b.push(Transition::Admin(AdminOp::ShardCreate {
                id: ShardId(u16::from(i)),
                primary: NodeId([i; 20]),
                min_isr: 1,
            }));
        }
        for (r, sh) in assign {
            b.push(Transition::Admin(AdminOp::SlotsAssign {
                slots: SlotRanges::parse(r).unwrap(),
                shard: ShardId(*sh),
            }));
        }
        s.apply(&b).unwrap()
    }

    /// Applies a plan as committed migrations would (instant cutover).
    fn applied(s: &ClusterState, plan: &[MigrateCmd]) -> Vec<usize> {
        let mut slots = s.slots.clone();
        for c in plan {
            for x in c.slots.iter_slots() {
                slots[usize::from(x)] = c.to.0;
            }
        }
        s.shards
            .iter()
            .map(|sh| slots.iter().filter(|&&o| o == sh.id.0).count())
            .collect()
    }

    #[test]
    fn evens_slot_counts_deterministically() {
        let s = cluster(3, &[("0-16383", 1)]);
        let plan = rebalance_plan(&s);
        assert_eq!(plan, rebalance_plan(&s));
        assert_eq!(applied(&s, &plan), vec![5462, 5461, 5461]);
        assert_eq!(plan.len(), 2);
        assert_eq!(plan[0].to_string(), "migrate 5462-10922 --to 2");
        assert_eq!(plan[1].to_string(), "migrate 10923-16383 --to 3");
        assert!(applied(&s, &plan) == vec![5462, 5461, 5461]);
        // every command moves slots of one source shard
        for c in &plan {
            let src = s.slot_owner(c.slots.iter_slots().next().unwrap());
            assert!(c.slots.iter_slots().all(|x| s.slot_owner(x) == src));
        }
        // round trip of the plan text
        let text: String = plan.iter().map(|c| format!("{c}\n")).collect();
        assert_eq!(MigrateCmd::parse_plan(&text).unwrap(), plan);
        // balanced already: nothing to do
        let s2 = cluster(2, &[("0-8191", 1), ("8192-16383", 2)]);
        assert!(rebalance_plan(&s2).is_empty());
        assert!(rebalance_plan(&cluster(1, &[("0-16383", 1)])).is_empty());
    }

    #[test]
    fn uneven_and_partial_assignments() {
        let s = cluster(4, &[("0-9999", 1), ("10000-10099", 2), ("10100-16383", 3)]);
        let plan = rebalance_plan(&s);
        let after = applied(&s, &plan);
        let max = *after.iter().max().unwrap();
        let min = *after.iter().min().unwrap();
        assert!(max - min <= 1, "{after:?}");
        assert_eq!(after.iter().sum::<usize>(), 16384);
        assert!(MigrateCmd::parse("migrate x --to 1").is_err());
        assert!(MigrateCmd::parse("move 1-2 --to 1").is_err());
    }
}
