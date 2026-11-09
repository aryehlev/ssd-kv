//! Unit and property tests of `SlotTable` (DESIGN §25.2).
//!
//! The model is a `hashbrown` map from a key id to its entry and hint. Every live key has a
//! unique `page36`, so a key is found in the table by its fingerprint plus its page. Tests
//! drive the crate-internal fingerprint API so that fingerprint distributions (uniform,
//! narrow ranges with many equal fp32 values, skewed top bits) can be chosen freely.

use super::*;
use crate::hint;
use crate::segment::{SEG_MAX, cap_sequence, seg_bytes};
use hashbrown::{HashMap, HashSet};
use proptest::prelude::*;

fn entry(fp32: u32, x8: u8, page36: u64, kind: Kind, vtype: VType) -> Entry {
    Entry {
        fp32,
        x8,
        page36,
        kind,
        vtype,
        sync_mark: false,
    }
}

fn small(fp32: u32, x8: u8, page36: u64) -> Entry {
    entry(fp32, x8, page36, Kind::Small, VType::String)
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        splitmix(&mut self.0)
    }
    fn below(&mut self, n: usize) -> usize {
        pick(self.next(), n)
    }
}

/// How fingerprints are drawn.
#[derive(Clone, Copy, Debug)]
enum Dist {
    Uniform,
    /// `0..n`: many equal fp32 values and long runs, deep splits.
    Narrow(u32),
    /// Top `k` bits zero: splits whose upper child is empty.
    TopZero(u32),
    /// A few clusters of nearby values (each spread over 2^18 fp values, so they separate
    /// within the directory depth limit).
    Clustered,
}

impl Dist {
    fn draw(self, rng: &mut Rng) -> u32 {
        let r = rng.next();
        match self {
            Dist::Uniform => r as u32,
            Dist::Narrow(n) => (r % u64::from(n)) as u32,
            Dist::TopZero(k) => (r as u32) >> k,
            Dist::Clustered => {
                let centre =
                    [0x1000_0000u32, 0x7FFF_0000, 0xC000_0000, 0xFFFF_FF00][(r >> 60) as usize & 3];
                centre.wrapping_add((r as u32) & 0x3_FFFF)
            }
        }
    }
}

/// A table plus its model.
struct Harness {
    t: SlotTable,
    m: HashMap<u64, (Entry, u16)>,
    ids: Vec<u64>,
    next_id: u64,
    next_page: u64,
    /// Model keys with a non-zero hint.
    ttl: usize,
}

impl Harness {
    fn new() -> Self {
        Harness {
            t: SlotTable::new(),
            m: HashMap::new(),
            ids: Vec::new(),
            next_id: 0,
            next_page: 1,
            ttl: 0,
        }
    }

    fn add(&mut self, e: Entry, hint: u16) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.m.insert(id, (e, hint));
        self.ids.push(id);
        self.ttl += usize::from(hint != 0);
        id
    }

    fn fresh_page(&mut self) -> u64 {
        let p = self.next_page;
        self.next_page += 1;
        p
    }

    /// Position of key `id` in the table.
    fn find(&self, id: u64) -> Option<Pos> {
        let (e, _) = self.m.get(&id)?;
        self.t
            .positions_of_fp(e.fp32)
            .into_iter()
            .find(|&p| self.t.get(p).is_some_and(|(x, _)| x.page36 == e.page36))
    }

    fn insert(&mut self, rng: &mut Rng, dist: Dist) -> u64 {
        let fp = dist.draw(rng);
        let r = rng.next();
        let kind = [Kind::Small, Kind::Small, Kind::Extent, Kind::ChunkMap][(r & 3) as usize];
        let vtype = if r & 4 != 0 {
            VType::Hash
        } else {
            VType::String
        };
        let hint = if r & 8 != 0 {
            ((r >> 16) as u16).max(1)
        } else {
            0
        };
        let e = entry(fp, (r >> 32) as u8, self.fresh_page(), kind, vtype);
        self.t.insert_entry(e, hint).unwrap();
        self.add(e, hint)
    }

    fn remove(&mut self, k: usize) -> Option<(u64, Pos)> {
        if self.ids.is_empty() {
            return None;
        }
        let k = k % self.ids.len();
        let id = self.ids.swap_remove(k);
        let pos = self.find(id).expect("model key in table");
        self.t.remove(pos);
        let (_, hint) = self.m.remove(&id).unwrap();
        self.ttl -= usize::from(hint != 0);
        Some((id, pos))
    }

    fn update(&mut self, k: usize, r: u64) -> Option<u64> {
        if self.ids.is_empty() {
            return None;
        }
        let id = self.ids[k % self.ids.len()];
        let pos = self.find(id).expect("model key in table");
        let (old, old_hint) = self.m[&id];
        let kind = [Kind::Small, Kind::Extent, Kind::ChunkMap, Kind::Small][(r & 3) as usize];
        let x8 = if kind == Kind::Small && old.kind == Kind::Small {
            old.x8
        } else {
            (r >> 8) as u8
        };
        let vtype = if r & 4 != 0 {
            VType::Hash
        } else {
            VType::String
        };
        let hint = if r & 8 != 0 {
            ((r >> 24) as u16).max(1)
        } else {
            0
        };
        let mut e = entry(old.fp32, x8, self.fresh_page(), kind, vtype);
        self.t.update(pos, e, hint);
        // update keeps a set merge-sync mark
        e.sync_mark = old.sync_mark;
        self.m.insert(id, (e, hint));
        self.ttl = self.ttl + usize::from(hint != 0) - usize::from(old_hint != 0);
        Some(id)
    }

    /// The touched key is where the model says, with the model's entry and hint.
    fn check_key(&self, id: u64) -> Result<(), String> {
        let (e, hint) = self.m[&id];
        let pos = self.find(id).ok_or("key not found")?;
        let (got, got_hint) = self.t.get(pos).ok_or("empty position")?;
        if got != e || got_hint != hint {
            return Err(format!(
                "entry mismatch: {got:?}/{got_hint} vs {e:?}/{hint}"
            ));
        }
        // the public candidate path finds it too
        let mut c = SmallVec::new();
        self.t.candidates_fp(e.fp32, e.x8, &mut c);
        if !c.iter().any(|c| c.entry == e && c.hint == hint) {
            return Err("candidates miss the key".into());
        }
        if c.iter()
            .any(|c| c.entry.fp32 != e.fp32 || (c.entry.kind == Kind::Small && c.entry.x8 != e.x8))
        {
            return Err("candidates include a non-matching entry".into());
        }
        Ok(())
    }

    fn check_counts(&self) -> Result<(), String> {
        let ttl = self.ttl;
        if self.t.len() != self.m.len() || self.t.ttl_len() != ttl {
            return Err(format!(
                "counts {} / {} vs model {} / {ttl}",
                self.t.len(),
                self.t.ttl_len(),
                self.m.len()
            ));
        }
        Ok(())
    }

    /// Full equivalence: every model key present (counts equal, pages unique => exact match).
    fn check_all(&self) -> Result<(), String> {
        self.t.check()?;
        self.check_counts()?;
        for &id in &self.ids {
            self.check_key(id)?;
        }
        Ok(())
    }
}

// ------------------------------------------------------------------------------------------
// basic behaviour

#[test]
fn empty_table() {
    let t = SlotTable::new();
    assert!(t.is_empty());
    assert_eq!(t.segments(), 0);
    let mut c = SmallVec::new();
    t.candidates_fp(5, 0, &mut c);
    assert!(c.is_empty());
    let mut out = Vec::new();
    let s = t.scan_pages(0, 10, ScanFilter::ALL, &mut out);
    assert!(out.is_empty());
    assert_eq!((s.fp_lo, s.fp_hi_excl, s.next), (0, 1 << 32, None));
    assert!(t.random_entry(1).is_none());
    assert_eq!(SlotTable::snapshot_iter(&t).count(), 0);
    assert_eq!(
        t.expiry_candidates(Pos::START, 100, u16::MAX, &mut Vec::new()),
        Pos::END
    );
    assert!(t.memory_bytes() <= core::mem::size_of::<SlotTable>());
    t.check().unwrap();
}

#[test]
fn public_api_with_key_hashes() {
    let mut t = SlotTable::new();
    let keys: Vec<KeyHash> = (0..500u32)
        .map(|k| KeyHash::of(42, &k.to_le_bytes()))
        .collect();
    for (k, h) in keys.iter().enumerate() {
        // fp32/x8 placeholders are replaced from the hash
        t.insert(*h, small(0, 0, k as u64 + 1), 0);
    }
    t.check().unwrap();
    let mut c = SmallVec::new();
    for (k, h) in keys.iter().enumerate() {
        t.candidates(*h, &mut c);
        let hit = c
            .iter()
            .find(|c| c.entry.page36 == k as u64 + 1)
            .expect("found");
        assert_eq!(hit.entry.fp32, h.fp32());
        assert_eq!(hit.entry.x8, h.x8());
    }
    // remove half through candidate positions
    for (k, h) in keys.iter().enumerate().step_by(2) {
        t.candidates(*h, &mut c);
        let pos = c
            .iter()
            .find(|c| c.entry.page36 == k as u64 + 1)
            .unwrap()
            .pos;
        t.remove(pos);
        t.check().unwrap();
    }
    assert_eq!(t.len(), 250);
    for (k, h) in keys.iter().enumerate() {
        t.candidates(*h, &mut c);
        assert_eq!(c.iter().any(|c| c.entry.page36 == k as u64 + 1), k % 2 == 1);
    }
}

#[test]
fn fingerprint_collisions() {
    let mut t = SlotTable::new();
    // same fp32: small entries differ in x8, extents carry a size class in x8
    t.insert_entry(small(77, 1, 10), 0).unwrap();
    t.insert_entry(small(77, 2, 11), 0).unwrap();
    t.insert_entry(small(77, 1, 12), 0).unwrap();
    t.insert_entry(entry(77, 9, 13, Kind::Extent, VType::String), 0)
        .unwrap();
    t.insert_entry(entry(77, 3, 14, Kind::ChunkMap, VType::Hash), 0)
        .unwrap();
    t.insert_entry(small(76, 1, 15), 0).unwrap();
    t.insert_entry(small(78, 1, 16), 0).unwrap();
    t.check().unwrap();
    let pages = |x8: u8| -> Vec<u64> {
        let mut c = SmallVec::new();
        t.candidates_fp(77, x8, &mut c);
        let mut v: Vec<u64> = c.iter().map(|c| c.entry.page36).collect();
        v.sort_unstable();
        v
    };
    assert_eq!(pages(1), vec![10, 12, 13, 14]);
    assert_eq!(pages(2), vec![11, 13, 14]);
    assert_eq!(pages(200), vec![13, 14]);
}

#[test]
fn update_in_place() {
    let mut t = SlotTable::new();
    t.insert_entry(small(1000, 5, 1), 0).unwrap();
    let pos = t.positions_of_fp(1000)[0];
    t.update(pos, entry(1000, 17, 99, Kind::Extent, VType::Hash), 33);
    assert_eq!(t.ttl_len(), 1);
    assert_eq!(
        t.get(pos),
        Some((entry(1000, 17, 99, Kind::Extent, VType::Hash), 33))
    );
    t.update(pos, small(1000, 5, 98), 0);
    assert_eq!(t.ttl_len(), 0);
    assert_eq!(t.get(pos), Some((small(1000, 5, 98), 0)));
    t.check().unwrap();
}

#[test]
fn directory_sizes_lc09() {
    // LC-09: the directory allocation is never a power of two; D = 0 is inline (no allocation)
    assert_eq!(dir_bytes(0), 0);
    for d in 1..=MAX_DIR_DEPTH {
        let b = dir_bytes(d);
        assert_eq!(b, ((1 << d) + 1) * 4);
        assert!(!b.is_power_of_two(), "D = {d}");
    }
    for cap in cap_sequence() {
        assert_ne!(seg_bytes(cap) % 64, 0);
    }
}

#[test]
fn capacity_exhaustion_is_reported() {
    // 8 328 positions at SEG_MAX: identical fingerprints fill one depth-19 segment exactly
    let mut t = SlotTable::new();
    let total = (SEG_MAX + crate::segment::tail(SEG_MAX)) as usize;
    for k in 0..total {
        t.insert_entry(small(0, 0, k as u64 + 1), 0).unwrap();
    }
    assert_eq!(t.depth(), MAX_DIR_DEPTH);
    assert_eq!(
        t.try_insert_fp_for_test(small(0, 0, 1_000_000)),
        Err(IndexFull)
    );
    assert_eq!(t.len(), total);
    t.check().unwrap();
    // other fingerprints still go to their own segments
    t.insert_entry(small(u32::MAX, 0, 2_000_000), 0).unwrap();
    t.check().unwrap();
}

impl SlotTable {
    fn try_insert_fp_for_test(&mut self, e: Entry) -> Result<(), IndexFull> {
        self.insert_entry(e, 0)
    }
}

#[test]
fn shrink_and_release() {
    let mut h = Harness::new();
    let mut rng = Rng(9);
    for _ in 0..3000 {
        h.insert(&mut rng, Dist::Uniform);
    }
    let peak = h.t.memory_bytes();
    while h.ids.len() > 10 {
        let k = rng.below(h.ids.len());
        h.remove(k);
    }
    h.check_all().unwrap();
    assert!(h.t.memory_bytes() < peak / 10, "segments shrank");
    while !h.ids.is_empty() {
        h.remove(0);
    }
    assert_eq!(h.t.segments(), 0);
    assert_eq!(h.t.memory_bytes(), core::mem::size_of::<SlotTable>());
}

// ------------------------------------------------------------------------------------------
// model property tests

#[derive(Clone, Debug)]
enum Op {
    Insert,
    InsertMany(u16),
    Remove(usize),
    RemoveMany(u16),
    Update(usize, u64),
    Mark(usize, bool),
    ClearMarks,
    Rehint,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        30 => Just(Op::Insert),
        3 => (1u16..400).prop_map(Op::InsertMany),
        20 => any::<usize>().prop_map(Op::Remove),
        2 => (1u16..400).prop_map(Op::RemoveMany),
        15 => (any::<usize>(), any::<u64>()).prop_map(|(k, r)| Op::Update(k, r)),
        4 => (any::<usize>(), any::<bool>()).prop_map(|(k, on)| Op::Mark(k, on)),
        1 => Just(Op::ClearMarks),
        1 => Just(Op::Rehint),
    ]
}

fn dist() -> impl Strategy<Value = Dist> {
    prop_oneof![
        Just(Dist::Uniform),
        (1u32..3000).prop_map(Dist::Narrow),
        (1u32..28).prop_map(Dist::TopZero),
        Just(Dist::Clustered),
    ]
}

/// Applies `op`; returns the key ids to check afterwards and the position touched.
fn apply(
    h: &mut Harness,
    rng: &mut Rng,
    d: Dist,
    op: &Op,
    marks: &mut HashSet<u64>,
) -> (Vec<u64>, Option<Pos>) {
    match *op {
        Op::Insert => {
            let id = h.insert(rng, d);
            let pos = h.find(id);
            (vec![id], pos)
        }
        Op::InsertMany(n) => ((0..n).map(|_| h.insert(rng, d)).collect(), None),
        Op::Remove(k) => {
            let r = h.remove(k);
            if let Some((id, _)) = r {
                marks.remove(&id);
            }
            (vec![], r.map(|x| x.1))
        }
        Op::RemoveMany(n) => {
            for _ in 0..n {
                let k = rng.below(usize::MAX);
                if let Some((id, _)) = h.remove(k) {
                    marks.remove(&id);
                }
            }
            (vec![], None)
        }
        Op::Update(k, r) => {
            let id = h.update(k, r);
            let pos = id.and_then(|id| h.find(id));
            (id.into_iter().collect(), pos)
        }
        Op::Mark(k, on) => {
            if h.ids.is_empty() {
                return (vec![], None);
            }
            let id = h.ids[k % h.ids.len()];
            let pos = h.find(id).unwrap();
            h.t.set_sync_mark(pos, on);
            if on {
                marks.insert(id);
            } else {
                marks.remove(&id);
            }
            let (e, _) = h.m.get_mut(&id).unwrap();
            e.sync_mark = on;
            (vec![id], Some(pos))
        }
        Op::ClearMarks => {
            h.t.clear_sync_marks();
            marks.clear();
            for (e, _) in h.m.values_mut() {
                e.sync_mark = false;
            }
            (vec![], None)
        }
        Op::Rehint => {
            // nonzero hints rotate; every 4th value is cleared
            let f = |x: u16| {
                if x.is_multiple_of(4) {
                    0
                } else {
                    x.rotate_left(3).max(1)
                }
            };
            h.t.rehint(&mut |x| f(x));
            for (_, hint) in h.m.values_mut() {
                if *hint != 0 {
                    *hint = f(*hint);
                }
            }
            h.ttl = h.m.values().filter(|(_, x)| *x != 0).count();
            (vec![], None)
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 48, ..ProptestConfig::default() })]

    /// SlotTable == hashbrown model under random insert/remove/update (including growth,
    /// shrinking and, with the narrow and skewed distributions, deep splits); I-a, I-b, I-c and
    /// the directory invariants after every op.
    #[test]
    fn model_small(seed in any::<u64>(), d in dist(), ops in proptest::collection::vec(op(), 1..160)) {
        let mut rng = Rng(seed);
        let mut h = Harness::new();
        let mut marks = HashSet::new();
        for op in &ops {
            let (touched, _) = apply(&mut h, &mut rng, d, op, &mut marks);
            h.t.check().map_err(TestCaseError::fail)?;
            h.check_counts().map_err(TestCaseError::fail)?;
            for id in touched {
                h.check_key(id).map_err(TestCaseError::fail)?;
            }
        }
        h.check_all().map_err(TestCaseError::fail)?;
        // marks: the unmarked-only walk sees exactly the unmarked keys' pages
        let mut out = Vec::new();
        let mut fp = 0u32;
        let mut pages = HashSet::new();
        loop {
            out.clear();
            let s = h.t.scan_pages(fp, 64, ScanFilter { vtype: None, unmarked_only: true }, &mut out);
            pages.extend(out.iter().map(|p| p.page36));
            match s.next { Some(n) => fp = n, None => break }
        }
        let expect: HashSet<u64> = h.m.iter().filter(|(id, _)| !marks.contains(*id)).map(|(_, (e, _))| e.page36).collect();
        prop_assert_eq!(pages, expect);
        // snapshot iteration: fp order, every entry once, marks reported as 0
        let snap: Vec<(Entry, u16)> = SlotTable::snapshot_iter(&h.t).collect();
        prop_assert_eq!(snap.len(), h.m.len());
        prop_assert!(snap.windows(2).all(|w| w[0].0.fp32 <= w[1].0.fp32));
        prop_assert!(snap.iter().all(|(e, _)| !e.sync_mark));
        // bulk build from the snapshot reproduces the content
        let b = SlotTable::bulk_build(snap.clone());
        b.check().map_err(TestCaseError::fail)?;
        let snap2: Vec<(Entry, u16)> = SlotTable::snapshot_iter(&b).collect();
        let canon = |mut v: Vec<(Entry, u16)>| { v.sort_by_key(|(e, h)| (e.fp32, e.page36, *h)); v };
        prop_assert_eq!(canon(snap), canon(snap2));
    }
}

/// Large tables: thousands of keys per segment so that growth through the 9/8 capacities,
/// splits at SEG_MAX with directory doubling, and shrinking all happen. Every op checks the
/// invariants on the cluster it touched and the touched key; every restructuring (and every
/// 2048 ops) checks the whole table, and every 2048 ops the whole model.
fn run_large(seed: u64, d: Dist, rounds: usize, grow_to: usize) {
    let mut rng = Rng(seed);
    let mut h = Harness::new();
    let mut marks = HashSet::new();
    let mut shape = h.t.shape();
    let mut ops = 0usize;
    let mut saw = (false, false, false); // grow, split, shrink
    for round in 0..rounds {
        let target_up = round % 2 == 0;
        let goal = if target_up { grow_to } else { grow_to / 8 };
        while if target_up {
            h.ids.len() < goal
        } else {
            h.ids.len() > goal
        } {
            let r = rng.next();
            let op = match (target_up, r % 10) {
                (true, 0..=6) | (false, 0..=1) => Op::Insert,
                (_, 7..=8) => Op::Update(rng.below(usize::MAX), rng.next()),
                _ => Op::Remove(rng.below(usize::MAX)),
            };
            let (touched, pos) = apply(&mut h, &mut rng, d, &op, &mut marks);
            ops += 1;
            if let Some(p) = pos {
                h.t.check_near(p).unwrap();
            }
            for id in touched {
                h.check_key(id).unwrap();
            }
            h.check_counts().unwrap();
            let now = h.t.shape();
            if now != shape {
                if now.0 > shape.0 {
                    saw.1 = true;
                } else if now.1 > shape.1 {
                    saw.0 = true;
                } else if now.1 < shape.1 {
                    saw.2 = true;
                }
                shape = now;
                h.t.check().unwrap();
            }
            if ops.is_multiple_of(2048) {
                h.check_all().unwrap();
            }
        }
        h.check_all().unwrap();
    }
    assert!(
        saw.0 && saw.1 && saw.2,
        "grow/split/shrink exercised: {saw:?}"
    );
}

#[test]
fn model_large_uniform() {
    run_large(1, Dist::Uniform, 3, 24_000);
}

#[test]
fn model_large_skewed() {
    run_large(2, Dist::TopZero(6), 2, 18_000);
}

#[test]
fn model_large_clustered() {
    // long runs (each cluster covers part of one segment's home range): O(run) operations
    run_large(3, Dist::Clustered, 2, 9_000);
}

// ------------------------------------------------------------------------------------------
// SCAN (scan_pages)

/// Keys for the SCAN tests share pages (small records) or own one (extents).
#[derive(Clone, Copy, Debug)]
struct ScanKey {
    fp: u32,
    page: u64,
    kind: Kind,
    vtype: VType,
}

/// SCAN model: keys by id, plus an id list for O(1) random picks.
struct ScanHarness {
    t: SlotTable,
    keys: HashMap<u64, ScanKey>,
    ids: Vec<u64>,
    id_at: HashMap<u64, usize>,
    next_id: u64,
    pages: u64,
}

impl ScanHarness {
    fn new(pages: u64) -> Self {
        ScanHarness {
            t: SlotTable::new(),
            keys: HashMap::new(),
            ids: Vec::new(),
            id_at: HashMap::new(),
            next_id: 0,
            pages,
        }
    }

    fn pos_of(&self, k: &ScanKey) -> Pos {
        self.t
            .positions_of_fp(k.fp)
            .into_iter()
            .find(|&p| {
                self.t.get(p).is_some_and(|(e, _)| {
                    e.page36 == k.page && e.kind == k.kind && e.vtype == k.vtype
                })
            })
            .expect("key in table")
    }

    fn insert(&mut self, rng: &mut Rng, d: Dist) {
        let r = rng.next();
        let kind = if r.is_multiple_of(5) {
            Kind::Extent
        } else {
            Kind::Small
        };
        let page = if kind == Kind::Small {
            1 + (r >> 8) % self.pages
        } else {
            1_000_000 + self.next_id
        };
        let vtype = if r & 0x10 != 0 {
            VType::Hash
        } else {
            VType::String
        };
        let k = ScanKey {
            fp: d.draw(rng),
            page,
            kind,
            vtype,
        };
        let class = if kind == Kind::Small {
            (r >> 40) as u8
        } else {
            (r % 7) as u8
        };
        self.t
            .insert_entry(entry(k.fp, class, k.page, k.kind, k.vtype), 0)
            .unwrap();
        let id = self.next_id;
        self.next_id += 1;
        self.keys.insert(id, k);
        self.id_at.insert(id, self.ids.len());
        self.ids.push(id);
    }

    fn random_id(&self, rng: &mut Rng) -> Option<u64> {
        (!self.ids.is_empty()).then(|| self.ids[rng.below(self.ids.len())])
    }

    fn remove(&mut self, id: u64) {
        let k = self.keys.remove(&id).unwrap();
        let p = self.pos_of(&k);
        self.t.remove(p);
        let i = self.id_at.remove(&id).unwrap();
        self.ids.swap_remove(i);
        if let Some(&moved) = self.ids.get(i) {
            self.id_at.insert(moved, i);
        }
    }

    fn move_page(&mut self, id: u64, rng: &mut Rng) {
        let mut k = self.keys[&id];
        let p = self.pos_of(&k);
        let (mut e, _) = self.t.get(p).unwrap();
        if k.kind == Kind::Small {
            k.page = 1 + rng.next() % self.pages;
            e.page36 = k.page;
            self.t.update(p, e, 0);
            self.keys.insert(id, k);
        }
    }

    /// `(fp32, id)` of the keys whose fp32 lies in the span, sorted.
    fn in_span(&self, s: &ScanSpan) -> Vec<(u32, u64)> {
        let mut v: Vec<(u32, u64)> = self
            .keys
            .iter()
            .filter(|(_, k)| {
                u64::from(k.fp) >= u64::from(s.fp_lo) && u64::from(k.fp) < s.fp_hi_excl
            })
            .map(|(id, k)| (k.fp, *id))
            .collect();
        v.sort_unstable();
        v
    }

    fn passes(k: &ScanKey, f: ScanFilter) -> bool {
        f.vtype.is_none_or(|v| v == k.vtype)
    }
}

/// One full SCAN iteration with mutations between steps; checks completeness, uniqueness and
/// the exact page set of every step.
fn scan_iteration(
    seed: u64,
    d: Dist,
    initial: usize,
    mut_per_step: usize,
    f: ScanFilter,
    count: usize,
) -> Result<(), String> {
    let mut rng = Rng(seed);
    let mut h = ScanHarness::new((initial as u64 / 4).max(1));
    for _ in 0..initial {
        h.insert(&mut rng, d);
    }
    let at_start: HashSet<u64> = h.keys.keys().copied().collect();
    let mut removed: HashSet<u64> = HashSet::new();
    let mut returned: HashMap<u64, u32> = HashMap::new();
    let mut cursor = 0u32;
    let mut out = Vec::new();
    let mut steps = 0usize;
    loop {
        steps += 1;
        if steps > 1_000_000 {
            return Err("no progress".into());
        }
        out.clear();
        let step_count = 1 + rng.below(count);
        let s = h.t.scan_pages(cursor, step_count, f, &mut out);
        // span shape
        if s.fp_lo != cursor || s.fp_hi_excl < u64::from(s.fp_lo) {
            return Err(format!("span bounds {s:?} from cursor {cursor}"));
        }
        match s.next {
            Some(n) if u64::from(n) != s.fp_hi_excl || n <= cursor => {
                return Err(format!("next {s:?}"));
            }
            None if s.fp_hi_excl != 1 << 32 => return Err(format!("end {s:?}")),
            _ => {}
        }
        // de-duplicated, bounded, and exactly the pages of the admitted entries in the span
        let set: HashSet<PageRef> = out.iter().copied().collect();
        if set.len() != out.len() {
            return Err("duplicate page refs".into());
        }
        if out.iter().any(|p| p.kind == Kind::Small && p.class != 0) {
            return Err("small page refs carry fingerprint bits".into());
        }
        let span = h.in_span(&s);
        let expect: HashSet<(u64, Kind)> = span
            .iter()
            .map(|(_, id)| &h.keys[id])
            .filter(|k| ScanHarness::passes(k, f))
            .map(|k| (k.page, k.kind))
            .collect();
        let got: HashSet<(u64, Kind)> = out.iter().map(|p| (p.page36, p.kind)).collect();
        if got != expect {
            return Err(format!(
                "page set differs: {} vs {}",
                got.len(),
                expect.len()
            ));
        }
        let mut max_group = 0usize;
        for g in span.chunk_by(|a, b| a.0 == b.0) {
            max_group = max_group.max(g.len());
        }
        if span.len() > step_count + max_group.saturating_sub(1) {
            return Err(format!("step visited {} > count {step_count}", span.len()));
        }
        if out.len() > SCAN_STEP_MAX_PAGES && max_group <= SCAN_STEP_MAX_PAGES {
            return Err(format!("{} pages in one step", out.len()));
        }
        // step 3 emulation: records on the referenced pages, inside the span
        for (_, id) in &span {
            let k = &h.keys[id];
            if ScanHarness::passes(k, f)
                && out.iter().any(|p| p.page36 == k.page && p.kind == k.kind)
            {
                *returned.entry(*id).or_default() += 1;
            }
        }
        let Some(next) = s.next else { break };
        cursor = next;
        // concurrent mutation between steps: inserts, removes, page moves, growth and
        // shrinking; balanced, so the population stays around its initial size
        for _ in 0..mut_per_step {
            match rng.next() % 8 {
                0..=1 => h.insert(&mut rng, d),
                2..=3 => {
                    if let Some(id) = h.random_id(&mut rng) {
                        h.remove(id);
                        removed.insert(id);
                    }
                }
                4..=6 => {
                    if let Some(id) = h.random_id(&mut rng) {
                        h.move_page(id, &mut rng);
                    }
                }
                _ => {
                    // bursts force rebuilds, splits and shrinks mid-iteration
                    if rng.next().is_multiple_of(2) {
                        for _ in 0..150 {
                            h.insert(&mut rng, d);
                        }
                    } else {
                        for _ in 0..150 {
                            if let Some(id) = h.random_id(&mut rng) {
                                h.remove(id);
                                removed.insert(id);
                            }
                        }
                    }
                }
            }
        }
    }
    h.t.check()?;
    for (id, n) in &returned {
        if *n > 1 {
            return Err(format!("key {id} returned {n} times"));
        }
    }
    for id in at_start.difference(&removed) {
        let k = &h.keys[id];
        if ScanHarness::passes(k, f) && !returned.contains_key(id) {
            return Err(format!("key {id} present throughout was not returned"));
        }
    }
    Ok(())
}

fn filter() -> impl Strategy<Value = ScanFilter> {
    prop_oneof![
        3 => Just(ScanFilter::ALL),
        1 => Just(ScanFilter { vtype: Some(VType::Hash), unmarked_only: false }),
        1 => Just(ScanFilter { vtype: Some(VType::String), unmarked_only: false }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 40, ..ProptestConfig::default() })]

    /// SCAN completeness under concurrent mutation: every key present throughout is returned,
    /// none twice; each step emits only de-duplicated page refs, exactly those of the admitted
    /// entries of its span, and at most SCAN_STEP_MAX_PAGES of them.
    #[test]
    fn scan_complete(seed in any::<u64>(), d in dist(), initial in 0usize..3000, muts in 0usize..6, f in filter(), count in 1usize..120) {
        scan_iteration(seed, d, initial, muts, f, count).map_err(TestCaseError::fail)?;
    }
}

#[test]
fn scan_complete_across_splits() {
    // large enough that the iteration crosses splits and directory doubling
    scan_iteration(11, Dist::Uniform, 20_000, 8, ScanFilter::ALL, 400).unwrap();
    scan_iteration(
        12,
        Dist::TopZero(5),
        12_000,
        8,
        ScanFilter {
            vtype: Some(VType::Hash),
            unmarked_only: false,
        },
        64,
    )
    .unwrap();
}

#[test]
fn scan_page_bound_and_shared_out() {
    let mut t = SlotTable::new();
    // 100 keys on 100 distinct pages, distinct fps
    for k in 0..100u32 {
        t.insert_entry(small(k * 1000, 0, u64::from(k) + 1), 0)
            .unwrap();
    }
    let mut out = Vec::new();
    let s = t.scan_pages(0, 1000, ScanFilter::ALL, &mut out);
    assert_eq!(out.len(), SCAN_STEP_MAX_PAGES);
    assert_eq!(s.next, Some(32 * 1000));
    // a second table walked into the same `out`: the bound applies to the total
    let mut t2 = SlotTable::new();
    t2.insert_entry(small(5, 0, 1), 0).unwrap(); // page 1 already in `out`
    t2.insert_entry(small(6, 0, 500), 0).unwrap();
    let s2 = t2.scan_pages(0, 1000, ScanFilter::ALL, &mut out);
    assert_eq!(out.len(), SCAN_STEP_MAX_PAGES);
    assert_eq!(
        (s2.fp_hi_excl, s2.next),
        (6, Some(6)),
        "the shared page is consumed, the new one is not"
    );
    // a step that starts with an empty `out` always consumes its first group
    let mut t3 = SlotTable::new();
    for k in 0..40u64 {
        t3.insert_entry(small(9, 0, k + 1), 0).unwrap();
    }
    out.clear();
    let s3 = t3.scan_pages(0, 1, ScanFilter::ALL, &mut out);
    assert_eq!(out.len(), 40);
    assert_eq!(s3.next, None);
    // TYPE filter applied before emitting; extents carry their size class
    let mut t4 = SlotTable::new();
    t4.insert_entry(entry(1, 7, 10, Kind::Extent, VType::Hash), 0)
        .unwrap();
    t4.insert_entry(entry(2, 3, 11, Kind::Small, VType::String), 0)
        .unwrap();
    out.clear();
    t4.scan_pages(
        0,
        10,
        ScanFilter {
            vtype: Some(VType::Hash),
            unmarked_only: false,
        },
        &mut out,
    );
    assert_eq!(
        out,
        vec![PageRef {
            page36: 10,
            kind: Kind::Extent,
            class: 7
        }]
    );
}

// ------------------------------------------------------------------------------------------
// expiry sweep, rehint, random entries, bulk build, memory

#[test]
fn expiry_sweep_matches_model() {
    let base = 1_790_000_000u64;
    let mut rng = Rng(5);
    let mut h = Harness::new();
    for _ in 0..20_000 {
        let fp = rng.next() as u32;
        let ttl = rng.next().is_multiple_of(3);
        let hint = if ttl {
            hint::encode(Some((base + rng.next() % 100_000) * 1000), base)
        } else {
            0
        };
        let e = small(fp, 0, h.fresh_page());
        h.t.insert_entry(e, hint).unwrap();
        h.add(e, hint);
    }
    let now_ms = (base + 50_000) * 1000;
    let code = hint::now_code(now_ms, base);
    let expect: HashSet<u64> =
        h.m.values()
            .filter(|(_, hh)| hint::decode_lb_s(*hh, base).is_some_and(|lb| lb * 1000 <= now_ms))
            .map(|(e, _)| e.page36)
            .collect();
    assert!(!expect.is_empty());
    for budget in [1usize, 7, 1000, 1 << 20] {
        let mut pos = Pos::START;
        let mut got = Vec::new();
        let mut calls = 0;
        loop {
            pos = h.t.expiry_candidates(pos, budget, code, &mut got);
            calls += 1;
            if pos.is_end() {
                break;
            }
            assert!(calls < 1_000_000);
        }
        let pages: HashSet<u64> = got.iter().map(|&p| h.t.get(p).unwrap().0.page36).collect();
        assert_eq!(pages.len(), got.len(), "budget {budget}: duplicates");
        assert_eq!(pages, expect, "budget {budget}");
    }
    // now_lb 0: nothing qualifies
    let mut got = Vec::new();
    let mut pos = Pos::START;
    while !pos.is_end() {
        pos = h.t.expiry_candidates(pos, 4096, 0, &mut got);
    }
    assert!(got.is_empty());
    // a stale position is clamped, never panics
    assert!(
        h.t.expiry_candidates(
            Pos {
                seg: 3,
                idx: u32::MAX - 1
            },
            10,
            code,
            &mut got
        )
        .seg >= 3
    );
    assert!(
        h.t.expiry_candidates(
            Pos {
                seg: 1 << 30,
                idx: 0
            },
            10,
            code,
            &mut got
        )
        .is_end()
    );
}

#[test]
fn rehint_rebases() {
    let base = 1_790_000_000u64;
    let mut t = SlotTable::new();
    let mut rng = Rng(3);
    let mut exps = Vec::new();
    for k in 0..3000u64 {
        let exp = (base + 10_000 + rng.next() % 1_000_000) * 1000;
        let hh = if k % 2 == 0 {
            hint::encode(Some(exp), base)
        } else {
            0
        };
        t.insert_entry(small(rng.next() as u32, 0, k + 1), hh)
            .unwrap();
        exps.push(exp);
    }
    assert_eq!(t.ttl_len(), 1500);
    let nb = base + 3600;
    t.rehint(&mut |hh| hint::rebase(hh, base, nb));
    assert_eq!(t.ttl_len(), 1500);
    for (e, hh) in SlotTable::snapshot_iter(&t) {
        let exp = exps[(e.page36 - 1) as usize];
        if (e.page36 - 1) % 2 == 0 {
            assert!(hint::decode_lb_s(hh, nb).unwrap() * 1000 <= exp);
        } else {
            assert_eq!(hh, 0);
        }
    }
    t.rehint(&mut |_| 0);
    assert_eq!(t.ttl_len(), 0);
    t.check().unwrap();
}

#[test]
fn random_entry_uniform() {
    let mut h = Harness::new();
    let mut rng = Rng(77);
    for _ in 0..30_000 {
        h.insert(&mut rng, Dist::Uniform);
    }
    // thin out most segments heavily (sparse segments exercise the counting fallback)
    while h.ids.len() > 600 {
        let k = rng.below(h.ids.len());
        h.remove(k);
    }
    h.check_all().unwrap();
    let n = h.ids.len();
    let mut hits: HashMap<u64, u32> = HashMap::new();
    let draws = n * 200;
    for i in 0..draws {
        let (pos, e) = h.t.random_entry(i as u64 ^ 0xABCD_EF01).unwrap();
        assert_eq!(h.t.get(pos).map(|x| x.0), Some(e));
        *hits.entry(e.page36).or_default() += 1;
    }
    assert_eq!(hits.len(), n, "every entry drawn");
    // chi-square with n - 1 degrees of freedom: mean n - 1, sd sqrt(2(n - 1))
    let expect = draws as f64 / n as f64;
    let chi: f64 = hits
        .values()
        .map(|&c| (f64::from(c) - expect).powi(2) / expect)
        .sum();
    let dof = (n - 1) as f64;
    assert!(
        chi < dof + 6.0 * (2.0 * dof).sqrt(),
        "chi-square {chi} for {dof} dof"
    );
}

#[test]
fn bulk_build_shapes() {
    let mut rng = Rng(21);
    for n in [0usize, 1, 100, 6963, 6964, 50_000, 120_000] {
        let mut v: Vec<(Entry, u16)> = (0..n)
            .map(|k| (small(rng.next() as u32, 1, k as u64 + 1), (k % 3) as u16))
            .collect();
        if n == 50_000 {
            // unsorted input and EMPTY entries are tolerated
            v.push((small(5, 0, 0), 0));
        } else {
            v.sort_by_key(|(e, _)| e.fp32);
        }
        let t = SlotTable::bulk_build(v);
        t.check().unwrap();
        assert_eq!(t.len(), n);
        assert_eq!(t.ttl_len(), (0..n).filter(|k| k % 3 != 0).count());
        if n > 6963 {
            assert!(t.depth() >= 1);
        }
        if n >= 50_000 {
            let bpk = t.memory_bytes() as f64 / n as f64;
            // segments + hint planes (2.03 B per position) at load 0.75..0.85 (the capacity
            // sequence's 12.5 % steps below the 0.85 target)
            assert!(bpk < (10.16 + 2.04) / 0.75, "bulk {n}: {bpk} B/key");
        }
    }
    // pathological input: more entries with one fp32 than any table holds
    let v: Vec<(Entry, u16)> = (0..9000u64).map(|k| (small(0, 0, k + 1), 0)).collect();
    assert_eq!(SlotTable::try_bulk_build(v).err(), Some(IndexFull));
}

/// Memory per key at steady state (§9.6, §25.2): within 9.5 % of 10.16 / 0.85 B/key, without
/// TTL hints, for tables of growing size under random insert/remove churn.
#[test]
fn memory_per_key_steady_state() {
    let target = 10.16 / 0.85;
    let (lo, hi) = (target * (1.0 - 0.095), target * (1.0 + 0.095));
    let mut rng = Rng(123);
    let mut t = SlotTable::new();
    let mut live: Vec<u32> = Vec::new();
    let mut page = 1u64;
    let mut samples = Vec::new();
    for n in (20_000..=60_000).step_by(2_500) {
        while live.len() < n {
            let fp = rng.next() as u32;
            t.insert_entry(small(fp, 0, page), 0).unwrap();
            page += 1;
            live.push(fp);
        }
        // churn at constant population
        for _ in 0..n / 4 {
            let k = rng.below(live.len());
            let fp = live.swap_remove(k);
            let pos = t.positions_of_fp(fp)[0];
            t.remove(pos);
            let nfp = rng.next() as u32;
            t.insert_entry(small(nfp, 0, page), 0).unwrap();
            page += 1;
            live.push(nfp);
        }
        let bpk = t.memory_bytes() as f64 / t.len() as f64;
        samples.push(bpk);
        assert!(
            (lo..=hi).contains(&bpk),
            "n = {n}: {bpk:.3} B/key outside [{lo:.2}, {hi:.2}]"
        );
    }
    t.check().unwrap();
    let mean = samples.iter().sum::<f64>() / samples.len() as f64;
    assert!(
        (mean - target).abs() < target * 0.05,
        "mean {mean:.3} B/key"
    );
}

#[test]
fn memory_with_ttl_hints() {
    // every key with a TTL: segments + hint planes, ~14.4 B/key (§9.6), <= 16 B/key target (§1.3)
    let mut rng = Rng(8);
    let mut t = SlotTable::new();
    for k in 0..50_000u64 {
        t.insert_entry(small(rng.next() as u32, 0, k + 1), 7)
            .unwrap();
    }
    let bpk = t.memory_bytes() as f64 / t.len() as f64;
    assert!(bpk < 16.0, "{bpk} B/key");
}
