//! Lazy and active expiry (DESIGN §9.5, §16.3, §16.4).
//!
//! Lazy: any `exec` that finds `expire_at <= now_ms` treats the key as absent and expires it
//! without a write: the index entry and counters are removed, and the record is dead once
//! `expire_at <= clock_floor_durable` (cluster primaries replicate `Op::Expired`). A key that
//! still has older copies on media is removed like a delete instead, its older copies first
//! (an expired newest copy keeps shadowing them, §16.2 v1.2).
//!
//! Active: each partition sweeps its tables round-robin in RAM comparing hint planes with `now`;
//! candidates are verified by reading their record, paced by the `active-expire-reads-per-sec /
//! P` bucket, and either expired without write or re-hinted. The sweep is paced by time, not by
//! ticks: a full pass over every TTL entry and every table of the dbs holding TTL keys is spread
//! over `SWEEP_PASS_NS` (hints have 1 s resolution, so sweeping faster finds nothing new), with
//! at most 64 Ki positions per tick. An idle partition ticked every millisecond therefore visits
//! a handful of positions per tick instead of whole tables, and one with no TTL key none.
//! It never scans the drive (LC-12). Replicas never expire keys on their own: they hide
//! logically expired keys on reads and remove them when the primary's op arrives. The hint
//! base is rebased hourly (`SlotTable::rehint`).
//!
//! The sweep itself lives in `partition.rs` (it needs the page cache); this module holds its
//! resume point and the time rules.

use kv_index::Pos;

/// Seconds the hint base trails the current time (§9.5): hints of keys expiring in the last
/// minute before the base still encode as candidates.
pub(crate) const HINT_BASE_LAG_S: u64 = 60;

/// One full pass of the RAM sweep over a partition's TTL entries takes about this long.
pub(crate) const SWEEP_PASS_NS: u64 = 1_000_000_000;

/// Resume point of the active expiry RAM sweep of one partition.
pub(crate) struct ExpirySweep {
    pub(crate) db: u8,
    pub(crate) slot_local: u16,
    pub(crate) pos: Pos,
    /// Unix seconds that hints are encoded against (`now_s - 60` at startup and every hour).
    pub(crate) hint_base_s: u64,
    /// Monotonic time of the last sweep step.
    pub(crate) last_ns: u64,
    /// Sweep work (positions and table visits) earned by elapsed time and not spent yet.
    pub(crate) credit: u64,
}

impl ExpirySweep {
    /// A sweep starting at the first table, with hints based on `now_ms`.
    pub(crate) fn new(now_ms: u64) -> Self {
        ExpirySweep { db: 0, slot_local: 0, pos: Pos::START, hint_base_s: (now_ms / 1000).saturating_sub(HINT_BASE_LAG_S), last_ns: 0, credit: 0 }
    }

    /// Earns sweep work for the time since the last step: one pass costs `pass_cost` (TTL
    /// entries plus table visits) per [`SWEEP_PASS_NS`]; the balance is capped at `cap`.
    /// Returns the work this step may do.
    pub(crate) fn earn(&mut self, now_ns: u64, pass_cost: u64, cap: u64) -> u64 {
        let elapsed = now_ns.saturating_sub(self.last_ns).min(SWEEP_PASS_NS);
        self.last_ns = self.last_ns.max(now_ns);
        let earned = (u128::from(pass_cost) * u128::from(elapsed) / u128::from(SWEEP_PASS_NS)) as u64;
        self.credit = self.credit.saturating_add(earned).min(cap);
        self.credit
    }
}

/// The time records are judged against at recovery: `max(now, clock_floor)` (§16.4, E13). A
/// record with `expire_at <= dead_before` is expired.
pub(crate) fn recovery_now(now_ms: u64, clock_floor_ms: u64) -> u64 {
    now_ms.max(clock_floor_ms)
}

/// True when a record expiring at `expire_at` (unix ms, `None` = no TTL) is expired at `now_ms`.
#[inline]
pub(crate) fn is_expired(expire_at: Option<u64>, now_ms: u64) -> bool {
    expire_at.is_some_and(|t| t <= now_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sweep_work_follows_elapsed_time() {
        let mut s = ExpirySweep::new(0);
        s.last_ns = 1_000;
        // 10 000 entries per pass: 1 ms earns 10, nothing elapsed earns nothing
        assert_eq!(s.earn(1_000 + 1_000_000, 10_000, 65_536), 10);
        s.credit = 0;
        assert_eq!(s.earn(1_000 + 1_000_000, 10_000, 65_536), 0);
        // a long pause earns at most one pass, capped
        assert_eq!(s.earn(1_000 + 9_000_000_000, 10_000, 65_536), 10_000);
        s.credit = 0;
        assert_eq!(s.earn(1_000 + 20_000_000_000, 1_000_000, 65_536), 65_536);
    }

    #[test]
    fn base_trails_now_and_recovery_uses_the_floor() {
        let s = ExpirySweep::new(1_790_000_000_500);
        assert_eq!(s.hint_base_s, 1_790_000_000 - HINT_BASE_LAG_S);
        assert_eq!(recovery_now(5, 9), 9);
        assert_eq!(recovery_now(10, 9), 10);
        assert!(is_expired(Some(10), 10));
        assert!(!is_expired(Some(11), 10));
        assert!(!is_expired(None, u64::MAX));
    }
}
