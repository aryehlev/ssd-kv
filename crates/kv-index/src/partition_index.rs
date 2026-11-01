//! Per-partition index (DESIGN §9.1, §16.6).
//!
//! `tables` is indexed by `db * (16384 / P) + slot / P` (a slot belongs to partition `p` iff
//! `slot % P == p`). Tables are created on first insert and freed when empty. Per-db key and
//! expiry counters back DBSIZE and INFO keyspace; `nonempty` lists (with a position map) give
//! RANDOMKEY a uniformly drawn non-empty table. `databases` is 16 standalone, 1 in cluster mode.
//!
//! Mutations go through the `&mut SlotTable` that [`PartitionIndex::table_mut`] lends out. The
//! index remembers which table it lent and that table's key and TTL counts at that moment; the
//! next call that lends a different table, drops tables or otherwise needs settled state folds
//! the difference into the per-db counters, updates the `nonempty` list and frees the table if
//! it became empty. Read accessors (`keys`, `expires`, `random_table`) take the lent table's
//! current counts into account, so every answer is exact at all times and stays O(1).

use kv_common::SLOTS;

use crate::table::SlotTable;

/// Position-map value of a table that is not in its db's `nonempty` list.
const ABSENT: u16 = u16::MAX;

/// The table last lent by `table_mut`, with its counts when it was lent.
#[derive(Clone, Copy, Debug)]
struct Lent {
    idx: usize,
    len: usize,
    ttl: usize,
}

pub struct PartitionIndex {
    partition: u16,
    partitions: u16,
    databases: u8,
    /// `slot / P == slot >> shift` (P is a power of two).
    shift: u32,
    /// Tables per db: `16384 / P`.
    local: u16,
    tables: Vec<Option<Box<SlotTable>>>,
    /// Per db: slot-local indexes of non-empty tables.
    nonempty: Vec<Vec<u16>>,
    /// Per table: its position in its db's `nonempty` list, or [`ABSENT`].
    nonempty_pos: Vec<u16>,
    keys: Vec<u64>,
    expires: Vec<u64>,
    lent: Option<Lent>,
}

impl core::fmt::Debug for PartitionIndex {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PartitionIndex")
            .field("partition", &self.partition)
            .field("partitions", &self.partitions)
            .field("databases", &self.databases)
            .finish_non_exhaustive()
    }
}

/// `floor(r * n / 2^64)`: a uniform draw in `0..n`.
#[inline]
fn pick(r: u64, n: usize) -> usize {
    ((u128::from(r) * n as u128) >> 64) as usize
}

impl PartitionIndex {
    /// Panics unless `partitions` is a power of two dividing 16384, `partition < partitions`
    /// and `databases >= 1` (configuration invariants, §5).
    pub fn new(partition: u16, partitions: u16, databases: u8) -> Self {
        assert!(
            partitions.is_power_of_two() && usize::from(partitions) <= SLOTS,
            "partition count must be a power of two <= 16384"
        );
        assert!(partition < partitions, "partition id out of range");
        assert!(databases >= 1, "at least one database");
        let local = (SLOTS / usize::from(partitions)) as u16;
        let n = usize::from(databases) * usize::from(local);
        let mut tables = Vec::with_capacity(n);
        tables.resize_with(n, || None);
        PartitionIndex {
            partition,
            partitions,
            databases,
            shift: partitions.trailing_zeros(),
            local,
            tables,
            nonempty: vec![Vec::new(); usize::from(databases)],
            nonempty_pos: vec![ABSENT; n],
            keys: vec![0; usize::from(databases)],
            expires: vec![0; usize::from(databases)],
            lent: None,
        }
    }

    pub fn partition(&self) -> u16 {
        self.partition
    }

    pub fn partitions(&self) -> u16 {
        self.partitions
    }

    pub fn databases(&self) -> u8 {
        self.databases
    }

    /// Number of slots of this partition, `16384 / P` (slot-local indexes `0..local_slots()`).
    pub fn local_slots(&self) -> u16 {
        self.local
    }

    /// The slot of slot-local index `local`: `local * P + partition`.
    pub fn slot_of(&self, local: u16) -> u16 {
        (local << self.shift) | self.partition
    }

    #[inline]
    fn idx(&self, db: u8, slot: u16) -> usize {
        debug_assert!(db < self.databases, "db out of range");
        debug_assert!(self.owns(slot), "slot not owned by this partition");
        usize::from(db) * usize::from(self.local) + usize::from(slot >> self.shift)
    }

    #[inline]
    fn owns(&self, slot: u16) -> bool {
        usize::from(slot) < SLOTS && slot & (self.partitions - 1) == self.partition
    }

    /// The table of `(db, slot)` if it holds at least one key.
    #[inline]
    pub fn table(&self, db: u8, slot: u16) -> Option<&SlotTable> {
        self.tables[self.idx(db, slot)]
            .as_deref()
            .filter(|t| !t.is_empty())
    }

    /// creates on demand
    ///
    /// Panics if `db` is out of range or `slot` does not belong to this partition.
    #[inline]
    pub fn table_mut(&mut self, db: u8, slot: u16) -> &mut SlotTable {
        assert!(
            db < self.databases && self.owns(slot),
            "table_mut: (db, slot) not in this partition"
        );
        let idx = self.idx(db, slot);
        match self.lent {
            Some(l) if l.idx == idx => {}
            Some(_) => {
                self.settle();
                self.lend(idx);
            }
            None => self.lend(idx),
        }
        self.tables[idx].get_or_insert_with(Box::default)
    }

    fn lend(&mut self, idx: usize) {
        let t = self.tables[idx].get_or_insert_with(Box::default);
        self.lent = Some(Lent {
            idx,
            len: t.len(),
            ttl: t.ttl_len(),
        });
    }

    /// Folds the lent table's changes into the counters and lists; frees it if empty.
    fn settle(&mut self) {
        let Some(l) = self.lent.take() else { return };
        let db = l.idx / usize::from(self.local);
        let (len, ttl) = self.tables[l.idx]
            .as_deref()
            .map_or((0, 0), |t| (t.len(), t.ttl_len()));
        self.keys[db] = self.keys[db]
            .wrapping_add(len as u64)
            .wrapping_sub(l.len as u64);
        self.expires[db] = self.expires[db]
            .wrapping_add(ttl as u64)
            .wrapping_sub(l.ttl as u64);
        match (l.len > 0, len > 0) {
            (false, true) => self.nonempty_add(l.idx),
            (true, false) => self.nonempty_remove(l.idx),
            _ => {}
        }
        if len == 0 {
            self.tables[l.idx] = None;
        }
    }

    fn nonempty_add(&mut self, idx: usize) {
        let local = usize::from(self.local);
        let list = &mut self.nonempty[idx / local];
        self.nonempty_pos[idx] = list.len() as u16;
        list.push((idx % local) as u16);
    }

    fn nonempty_remove(&mut self, idx: usize) {
        let local = usize::from(self.local);
        let db = idx / local;
        let pos = usize::from(self.nonempty_pos[idx]);
        let list = &mut self.nonempty[db];
        list.swap_remove(pos);
        if let Some(&moved) = list.get(pos) {
            self.nonempty_pos[db * local + usize::from(moved)] = pos as u16;
        }
        self.nonempty_pos[idx] = ABSENT;
    }

    /// The lent table, if it belongs to `db`, with its current `(len, ttl_len)`.
    #[inline]
    fn lent_in(&self, db: u8) -> Option<(Lent, usize, usize)> {
        let l = self.lent?;
        if l.idx / usize::from(self.local) != usize::from(db) {
            return None;
        }
        let (len, ttl) = self.tables[l.idx]
            .as_deref()
            .map_or((0, 0), |t| (t.len(), t.ttl_len()));
        Some((l, len, ttl))
    }

    /// Drops every table of `db` (FLUSHDB / FLUSHALL, §16.5).
    pub fn drop_db(&mut self, db: u8) {
        self.settle();
        let local = usize::from(self.local);
        let range = usize::from(db) * local..(usize::from(db) + 1) * local;
        for t in &mut self.tables[range.clone()] {
            *t = None;
        }
        self.nonempty_pos[range].fill(ABSENT);
        self.nonempty[usize::from(db)].clear();
        self.keys[usize::from(db)] = 0;
        self.expires[usize::from(db)] = 0;
    }

    /// Drops the table of `(db, slot)` (slot migrated away, §23.13).
    pub fn drop_slot(&mut self, db: u8, slot: u16) {
        self.settle();
        let idx = self.idx(db, slot);
        if let Some(t) = self.tables[idx].take() {
            let d = usize::from(db);
            self.keys[d] -= t.len() as u64;
            self.expires[d] -= t.ttl_len() as u64;
            if self.nonempty_pos[idx] != ABSENT {
                self.nonempty_remove(idx);
            }
        }
    }

    pub fn keys(&self, db: u8) -> u64 {
        let base = self.keys[usize::from(db)];
        match self.lent_in(db) {
            Some((l, len, _)) => base.wrapping_add(len as u64).wrapping_sub(l.len as u64),
            None => base,
        }
    }

    pub fn expires(&self, db: u8) -> u64 {
        let base = self.expires[usize::from(db)];
        match self.lent_in(db) {
            Some((l, _, ttl)) => base.wrapping_add(ttl as u64).wrapping_sub(l.ttl as u64),
            None => base,
        }
    }

    /// A uniformly drawn non-empty table of `db` (returns its slot).
    pub fn random_table(&self, db: u8, r: u64) -> Option<u16> {
        let list = &self.nonempty[usize::from(db)];
        // the lent table may have changed between empty and non-empty since it was lent
        let mut extra = None;
        let mut missing = None;
        if let Some((l, len, _)) = self.lent_in(db) {
            match (l.len > 0, len > 0) {
                (false, true) => extra = Some((l.idx % usize::from(self.local)) as u16),
                (true, false) => missing = Some(usize::from(self.nonempty_pos[l.idx])),
                _ => {}
            }
        }
        let total = list.len() + usize::from(extra.is_some()) - usize::from(missing.is_some());
        if total == 0 {
            return None;
        }
        let mut k = pick(r, total);
        if missing.is_some_and(|p| k >= p) {
            k += 1;
        }
        let local = match list.get(k) {
            Some(&l) => l,
            None => extra?,
        };
        Some(self.slot_of(local))
    }

    /// Index memory of the partition: every table's [`SlotTable::memory_bytes`] plus the
    /// per-partition table array, lists and counters.
    pub fn memory_bytes(&self) -> usize {
        core::mem::size_of::<Self>()
            + self.tables.capacity() * core::mem::size_of::<Option<Box<SlotTable>>>()
            + self
                .tables
                .iter()
                .flatten()
                .map(|t| t.memory_bytes())
                .sum::<usize>()
            + self
                .nonempty
                .iter()
                .map(|l| l.capacity() * 2 + core::mem::size_of::<Vec<u16>>())
                .sum::<usize>()
            + self.nonempty_pos.capacity() * 2
            + (self.keys.capacity() + self.expires.capacity()) * 8
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::{Entry, Kind, VType};
    use hashbrown::HashMap;
    use kv_common::KeyHash;
    use proptest::prelude::*;

    fn small(page36: u64) -> Entry {
        Entry {
            fp32: 0,
            x8: 0,
            page36,
            kind: Kind::Small,
            vtype: VType::String,
            sync_mark: false,
        }
    }

    fn h(k: u64) -> KeyHash {
        KeyHash::of(7, &k.to_le_bytes())
    }

    /// Removes the (only) entry with this key hash's fingerprint from the table.
    fn remove_key(t: &mut SlotTable, k: u64) {
        let mut c = smallvec::SmallVec::new();
        t.candidates(h(k), &mut c);
        let cand = c
            .iter()
            .find(|c| c.entry.page36 == k + 1)
            .expect("key present");
        t.remove(cand.pos);
    }

    #[test]
    fn layout_and_slots() {
        let p = PartitionIndex::new(5, 64, 16);
        assert_eq!(p.local_slots(), 256);
        assert_eq!(p.slot_of(0), 5);
        assert_eq!(p.slot_of(3), 3 * 64 + 5);
        assert_eq!(p.tables.len(), 16 * 256);
        assert!(p.table(0, 5).is_none());
        assert_eq!(p.keys(15), 0);
        assert_eq!(p.random_table(0, 123), None);
        let one = PartitionIndex::new(0, 1, 1);
        assert_eq!(one.local_slots(), 16384);
    }

    #[test]
    #[should_panic(expected = "not in this partition")]
    fn foreign_slot_panics() {
        let mut p = PartitionIndex::new(1, 16, 1);
        p.table_mut(0, 2);
    }

    #[test]
    fn counters_follow_lent_table() {
        let mut p = PartitionIndex::new(3, 16, 2);
        let s1 = p.slot_of(1);
        let s2 = p.slot_of(2);
        for k in 0..10u64 {
            p.table_mut(1, s1)
                .insert(h(k), small(k + 1), if k < 4 { 9 } else { 0 });
            assert_eq!(p.keys(1), k + 1);
        }
        assert_eq!(p.expires(1), 4);
        assert_eq!(p.keys(0), 0);
        assert_eq!(p.random_table(1, 0), Some(s1));
        assert_eq!(p.random_table(0, 0), None);
        // lending another table settles the first
        p.table_mut(1, s2).insert(h(100), small(101), 0);
        assert_eq!(p.keys(1), 11);
        assert_eq!(p.nonempty[1].len(), 1);
        assert_eq!(p.table(1, s1).map(SlotTable::len), Some(10));
        // empty the first table: it disappears from random_table at once, and is freed on settle
        for k in 0..10u64 {
            remove_key(p.table_mut(1, s1), k);
        }
        assert_eq!(p.keys(1), 1);
        assert_eq!(p.expires(1), 0);
        assert!(p.table(1, s1).is_none());
        for r in [0, u64::MAX / 2, u64::MAX] {
            assert_eq!(p.random_table(1, r), Some(s2));
        }
        p.table_mut(0, p.slot_of(0));
        assert!(
            p.tables[usize::from(p.local) + 1].is_none(),
            "empty table freed"
        );
        assert!(p.tables[0].is_some(), "lent table exists until settled");
        p.drop_slot(1, s2);
        assert_eq!(p.keys(1), 0);
        assert!(p.tables[0].is_none(), "settled on drop_slot");
        assert_eq!(p.random_table(1, 5), None);
    }

    #[test]
    fn drop_db_resets() {
        let mut p = PartitionIndex::new(0, 16, 3);
        for db in 0..3u8 {
            for l in 0..5u16 {
                let s = p.slot_of(l);
                p.table_mut(db, s)
                    .insert(h(u64::from(l)), small(u64::from(l) + 1), 1);
            }
        }
        assert_eq!(p.keys(1), 5);
        assert_eq!(p.expires(2), 5);
        p.drop_db(1);
        assert_eq!(p.keys(1), 0);
        assert_eq!(p.expires(1), 0);
        assert_eq!(p.random_table(1, 1), None);
        assert_eq!(p.keys(0), 5);
        assert_eq!(p.keys(2), 5);
        assert!((0..5).all(|l| p.table(1, p.slot_of(l)).is_none()));
        let before = p.memory_bytes();
        p.drop_db(0);
        p.drop_db(2);
        assert!(p.memory_bytes() < before);
    }

    #[test]
    fn random_table_uniform() {
        let mut p = PartitionIndex::new(2, 32, 1);
        let picks = [0u16, 7, 9, 100, 511];
        for &l in &picks {
            p.table_mut(0, p.slot_of(l))
                .insert(h(u64::from(l)), small(1), 0);
        }
        let mut hits: HashMap<u16, u32> = HashMap::new();
        let n = 5000u64;
        for i in 0..n {
            let r = i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            *hits.entry(p.random_table(0, r).unwrap()).or_default() += 1;
        }
        assert_eq!(hits.len(), picks.len());
        for &l in &picks {
            let c = hits[&p.slot_of(l)];
            assert!((800..1200).contains(&c), "table {l}: {c}");
        }
    }

    #[derive(Clone, Debug)]
    enum Op {
        Insert { db: u8, local: u16, ttl: bool },
        Remove { db: u8, local: u16 },
        DropSlot { db: u8, local: u16 },
        DropDb { db: u8 },
        Probe { db: u8, r: u64 },
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            6 => (0u8..2, 0u16..4, any::<bool>()).prop_map(|(db, local, ttl)| Op::Insert { db, local, ttl }),
            4 => (0u8..2, 0u16..4).prop_map(|(db, local)| Op::Remove { db, local }),
            1 => (0u8..2, 0u16..4).prop_map(|(db, local)| Op::DropSlot { db, local }),
            1 => (0u8..2).prop_map(|db| Op::DropDb { db }),
            3 => (0u8..2, any::<u64>()).prop_map(|(db, r)| Op::Probe { db, r }),
        ]
    }

    proptest! {
        /// Counters, nonempty lists and table lifetimes against a model, after every op.
        #[test]
        fn matches_model(ops in proptest::collection::vec(op(), 1..200)) {
            let mut p = PartitionIndex::new(1, 4096, 2);
            // model: (db, local) -> keys (id, ttl)
            let mut m: HashMap<(u8, u16), Vec<(u64, bool)>> = HashMap::new();
            let mut next = 0u64;
            for op in ops {
                match op {
                    Op::Insert { db, local, ttl } => {
                        let s = p.slot_of(local);
                        p.table_mut(db, s).insert(h(next), small(next + 1), u16::from(ttl));
                        m.entry((db, local)).or_default().push((next, ttl));
                        next += 1;
                    }
                    Op::Remove { db, local } => {
                        if let Some((k, _)) = m.get_mut(&(db, local)).and_then(Vec::pop) {
                            let s = p.slot_of(local);
                            remove_key(p.table_mut(db, s), k);
                        }
                    }
                    Op::DropSlot { db, local } => {
                        p.drop_slot(db, p.slot_of(local));
                        m.remove(&(db, local));
                    }
                    Op::DropDb { db } => {
                        p.drop_db(db);
                        m.retain(|&(d, _), _| d != db);
                    }
                    Op::Probe { db, r } => {
                        let got = p.random_table(db, r);
                        let nonempty = m.iter().any(|(&(d, _), v)| d == db && !v.is_empty());
                        prop_assert_eq!(got.is_some(), nonempty);
                        if let Some(s) = got {
                            let local = s >> 12;
                            prop_assert!(m.get(&(db, local)).is_some_and(|v| !v.is_empty()));
                            prop_assert!(p.table(db, s).is_some());
                        }
                    }
                }
                for db in 0..2u8 {
                    let keys: usize = m.iter().filter(|(k, _)| k.0 == db).map(|(_, v)| v.len()).sum();
                    let ttl: usize = m.iter().filter(|(k, _)| k.0 == db).map(|(_, v)| v.iter().filter(|x| x.1).count()).sum();
                    prop_assert_eq!(p.keys(db), keys as u64);
                    prop_assert_eq!(p.expires(db), ttl as u64);
                    for local in 0..4u16 {
                        let n = m.get(&(db, local)).map_or(0, Vec::len);
                        prop_assert_eq!(p.table(db, p.slot_of(local)).map_or(0, SlotTable::len), n);
                    }
                }
                // settled tables are never empty; the lent one may be
                let lent = p.lent.map(|l| l.idx);
                for (i, t) in p.tables.iter().enumerate() {
                    if let Some(t) = t {
                        prop_assert!(!t.is_empty() || lent == Some(i));
                    }
                }
            }
        }
    }
}
