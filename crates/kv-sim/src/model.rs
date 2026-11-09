//! Reference model for the storage simulator (DESIGN §25.3).
//!
//! Per key: the bytes of the newest executed state (what a read sees now) and the history of
//! executed states since the last point a crash could roll back to. Every history entry names
//! the operation that produced it and how far that operation was acknowledged (`Written`:
//! survives kill -9; `Durable`: survives power loss). After recovery every key must be in
//! {last acked state, a state produced by a later unacked op} for the loss that happened (I1);
//! acked deletes and FLUSH never resurrect and expired keys never return (I2, by normalising
//! every state against the clock); durability per ticket state (I3); multi-key writes are
//! all-or-nothing (I4, [`RefModel::check_txns`]).
//!
//! States are compared by length, a 64-bit fingerprint of the bytes and the expiry deadline:
//! every value the simulator writes embeds its operation id, so distinct writes never share a
//! fingerprint. The model holds the bytes of the current state only.

use std::collections::BTreeMap;

/// A key of the model: `(db, key index)`.
pub type KeyRef = (u8, u32);

/// A stored state: length, fingerprint and expiry deadline (ms since the epoch).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct State {
    pub len: u64,
    pub fp: u64,
    pub at: Option<u64>,
}

impl State {
    pub fn of(bytes: &[u8], at: Option<u64>) -> Self {
        State { len: bytes.len() as u64, fp: fingerprint(bytes), at }
    }

    /// The state as a reader at `now` sees it (an expired key is absent).
    pub fn live(self, now: u64) -> Option<State> {
        match self.at {
            Some(t) if t <= now => None,
            _ => Some(self),
        }
    }
}

/// `state` as seen at `now`.
pub fn live(state: Option<State>, now: u64) -> Option<State> {
    state.and_then(|s| s.live(now))
}

/// 64-bit fingerprint of a value.
pub fn fingerprint(bytes: &[u8]) -> u64 {
    kv_common::KeyHash::of(0x5EED_F1A9, bytes).raw()
}

/// How far an operation was acknowledged.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Ack {
    None,
    /// The ticket reached `Written` (survives kill -9).
    Written,
    /// The ticket reached `Durable` (survives power loss).
    Durable,
}

/// What a crash lost.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Loss {
    /// kill -9: completed writes survive in the device (cache).
    Process,
    /// Power cut: only what was made durable survives.
    Power,
    /// Clean shutdown: nothing may be lost.
    Nothing,
}

/// One state in a key's history.
#[derive(Clone, Copy, Debug)]
pub struct Entry {
    /// The operation (command part) that produced it; 0 for a rebased entry.
    pub op: u64,
    pub state: Option<State>,
    pub ack: Ack,
    /// Produced by a multi-key write (I4 applies).
    pub txn: bool,
}

/// Acknowledged and in-flight state of one key.
pub struct KeyModel {
    /// Bytes and deadline of the newest executed state (`None` = absent).
    pub cur: Option<(Vec<u8>, Option<u64>)>,
    /// `hist[0]` is the base (durable); later entries in execution order.
    pub hist: Vec<Entry>,
}

impl KeyModel {
    fn new() -> Self {
        KeyModel { cur: None, hist: vec![Entry { op: 0, state: None, ack: Ack::Durable, txn: false }] }
    }

    /// Index of the oldest state a crash of kind `loss` may roll back to.
    pub fn floor(&self, loss: Loss) -> usize {
        let need = match loss {
            Loss::Process => Ack::Written,
            Loss::Power => Ack::Durable,
            Loss::Nothing => return self.hist.len() - 1,
        };
        self.hist.iter().rposition(|e| e.ack >= need).unwrap_or(0)
    }
}

/// The indices of a key's allowed history that match an observed state.
#[derive(Clone, Copy, Debug)]
pub struct Match {
    pub min: usize,
    pub max: usize,
}

/// The model of one simulated node (ordered maps: deterministic iteration under a seed).
#[derive(Default)]
pub struct RefModel {
    keys: BTreeMap<KeyRef, KeyModel>,
    /// Keys touched by each operation whose ticket is not final yet.
    open_ops: BTreeMap<u64, Vec<KeyRef>>,
}

impl RefModel {
    pub fn new() -> Self {
        RefModel::default()
    }

    pub fn keys(&self) -> impl Iterator<Item = (&KeyRef, &KeyModel)> {
        self.keys.iter()
    }

    pub fn key(&self, k: KeyRef) -> Option<&KeyModel> {
        self.keys.get(&k)
    }

    /// The newest executed state of `k` as a reader at `now` sees it.
    pub fn cur_live(&self, k: KeyRef, now: u64) -> Option<(&[u8], Option<u64>)> {
        let km = self.keys.get(&k)?;
        let (v, at) = km.cur.as_ref()?;
        match at {
            Some(t) if *t <= now => None,
            _ => Some((v.as_slice(), *at)),
        }
    }

    /// Operation `op` executed and changed `k` to `new` (`None` = deleted).
    pub fn write(&mut self, k: KeyRef, op: u64, txn: bool, new: Option<(Vec<u8>, Option<u64>)>) {
        let km = self.keys.entry(k).or_insert_with(KeyModel::new);
        let state = new.as_ref().map(|(v, at)| State::of(v, *at));
        km.cur = new;
        km.hist.push(Entry { op, state, ack: Ack::None, txn });
        let ks = self.open_ops.entry(op).or_default();
        if !ks.contains(&k) {
            ks.push(k);
        }
    }

    /// Operation `op`'s ticket reached `level`.
    pub fn ack(&mut self, op: u64, level: Ack) {
        let Some(ks) = self.open_ops.get(&op) else { return };
        for k in ks {
            if let Some(km) = self.keys.get_mut(k) {
                for e in km.hist.iter_mut().rev().filter(|e| e.op == op) {
                    if e.ack < level {
                        e.ack = level;
                    }
                }
            }
        }
        if level == Ack::Durable {
            self.open_ops.remove(&op);
        }
    }

    /// Operation `op` failed after it executed (its ticket failed): its effects may or may
    /// not be on media, whatever its ticket reported before.
    pub fn unack(&mut self, op: u64) {
        let Some(ks) = self.open_ops.remove(&op) else { return };
        for k in ks {
            if let Some(km) = self.keys.get_mut(&k) {
                for e in km.hist.iter_mut().filter(|e| e.op == op) {
                    e.ack = Ack::None;
                }
            }
        }
    }

    /// Operations whose effects were executed but are not durable yet.
    pub fn open_ops(&self) -> impl Iterator<Item = u64> + '_ {
        self.open_ops.keys().copied()
    }

    /// Checks the state observed for `k` after a crash of kind `loss` (I1, I2).
    pub fn check(&self, k: KeyRef, observed: Option<State>, loss: Loss, now: u64) -> Result<Match, String> {
        let observed = live(observed, now);
        let Some(km) = self.keys.get(&k) else {
            return match observed {
                None => Ok(Match { min: 0, max: 0 }),
                Some(s) => Err(format!("key never written holds a value of {} bytes", s.len)),
            };
        };
        let floor = km.floor(loss);
        let hits: Vec<usize> = (floor..km.hist.len()).filter(|&i| live(km.hist[i].state, now) == observed).collect();
        match (hits.first(), hits.last()) {
            (Some(&min), Some(&max)) => Ok(Match { min, max }),
            _ => {
                let older = (0..floor).rev().find(|&i| live(km.hist[i].state, now) == observed);
                let what = match older {
                    Some(i) => format!("an older state (history index {i} of {}, below the {:?} floor {floor}): an acknowledged effect was lost", km.hist.len(), loss),
                    None if observed.is_none() => "absent: an acknowledged write was lost".to_string(),
                    None => "a state no operation produced since the floor (resurrected or corrupt)".to_string(),
                };
                Err(format!("observed {} is {what}; allowed: {}", describe(observed), allowed_text(km, floor, now)))
            }
        }
    }

    /// I4: for every multi-key write still in an allowed history, its effects are visible on all
    /// of its keys or on none (`matches` holds the match of every checked key).
    pub fn check_txns(&self, matches: &BTreeMap<KeyRef, Match>, loss: Loss) -> Result<(), String> {
        // op -> (keys where it is certainly visible, keys where it is certainly not)
        let mut seen: BTreeMap<u64, (Vec<KeyRef>, Vec<KeyRef>)> = BTreeMap::new();
        for (k, m) in matches {
            let Some(km) = self.keys.get(k) else { continue };
            let floor = km.floor(loss);
            for (i, e) in km.hist.iter().enumerate().skip(floor.max(1)) {
                if !e.txn {
                    continue;
                }
                let s = seen.entry(e.op).or_default();
                if m.min == i && m.max == i {
                    s.0.push(*k);
                } else if m.max < i {
                    s.1.push(*k);
                }
            }
        }
        for (op, (vis, not)) in seen {
            if !vis.is_empty() && !not.is_empty() {
                return Err(format!(
                    "multi-key write op {op} applied partly: visible on {:?}, absent on {:?}",
                    vis, not
                ));
            }
        }
        Ok(())
    }

    /// After a verified recovery: the observed state becomes the base the next crash is
    /// checked against; states a power cut could still roll back to are kept.
    pub fn rebase(&mut self, k: KeyRef, m: Match, loss: Loss, observed: Option<(Vec<u8>, Option<u64>)>) {
        let Some(km) = self.keys.get_mut(&k) else {
            if observed.is_some() {
                // a key the model does not know: the check already failed
                return;
            }
            return;
        };
        let state = observed.as_ref().map(|(v, at)| State::of(v, *at));
        let at_m = km.hist[m.max];
        let mut hist = match loss {
            Loss::Process => {
                let pf = km.floor(Loss::Power).min(m.max);
                km.hist[pf..m.max].to_vec()
            }
            Loss::Power | Loss::Nothing => Vec::new(),
        };
        let ack = match loss {
            Loss::Process => at_m.ack.max(Ack::Written),
            Loss::Power | Loss::Nothing => Ack::Durable,
        };
        if hist.is_empty() {
            hist.push(Entry { op: 0, state, ack: Ack::Durable, txn: false });
        } else {
            hist.push(Entry { op: at_m.op, state, ack, txn: at_m.txn });
        }
        km.hist = hist;
        km.cur = observed;
    }

    /// A read without a crash saw `observed` (runs with failed writes): it becomes the current
    /// state commands build on; the history and its acknowledgements stay as they are.
    pub fn observed(&mut self, k: KeyRef, observed: Option<(Vec<u8>, Option<u64>)>) {
        if let Some(km) = self.keys.get_mut(&k) {
            km.cur = observed;
        }
    }

    /// Forgets the operations whose tickets were open at a crash (their connections are gone).
    pub fn crashed(&mut self) {
        self.open_ops.clear();
    }

    /// Every key whose newest executed state is live at `now`.
    pub fn live_keys(&self, now: u64) -> Vec<KeyRef> {
        self.keys.keys().copied().filter(|k| self.cur_live(*k, now).is_some()).collect()
    }
}

fn describe(s: Option<State>) -> String {
    match s {
        None => "absent".to_string(),
        Some(s) => format!("({} bytes, fp {:016x}, deadline {:?})", s.len, s.fp, s.at),
    }
}

fn allowed_text(km: &KeyModel, floor: usize, now: u64) -> String {
    let mut out = String::new();
    for (i, e) in km.hist.iter().enumerate().skip(floor) {
        out.push_str(&format!("[{i}: op {} {:?} {}] ", e.op, e.ack, describe(live(e.state, now))));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(b: u8) -> Option<(Vec<u8>, Option<u64>)> {
        Some((vec![b; 10], None))
    }

    #[test]
    fn floors_follow_acks() {
        let mut m = RefModel::new();
        let k = (0, 1);
        m.write(k, 1, false, v(1));
        m.ack(1, Ack::Durable);
        m.write(k, 2, false, v(2));
        m.ack(2, Ack::Written);
        m.write(k, 3, false, v(3));
        let s = |b: u8| Some(State::of(&[b; 10], None));
        // kill -9: op 2 survives
        assert!(m.check(k, s(1), Loss::Process, 0).is_err());
        assert!(m.check(k, s(2), Loss::Process, 0).is_ok());
        assert!(m.check(k, s(3), Loss::Process, 0).is_ok());
        // power cut: op 1 survives
        assert!(m.check(k, s(1), Loss::Power, 0).is_ok());
        assert!(m.check(k, None, Loss::Power, 0).is_err());
    }

    #[test]
    fn expired_states_read_as_absent() {
        let mut m = RefModel::new();
        let k = (0, 1);
        m.write(k, 1, false, Some((vec![7; 3], Some(100))));
        m.ack(1, Ack::Durable);
        assert!(m.check(k, None, Loss::Power, 100).is_ok());
        assert!(m.check(k, Some(State::of(&[7; 3], Some(100))), Loss::Power, 100).is_ok(), "an expired copy reads as absent");
        assert!(m.check(k, None, Loss::Power, 99).is_err());
    }

    #[test]
    fn partial_txn_is_detected() {
        let mut m = RefModel::new();
        let (a, b) = ((0, 1), (0, 2));
        m.write(a, 1, false, v(1));
        m.write(b, 1, false, v(1));
        m.ack(1, Ack::Durable);
        m.write(a, 2, true, v(2));
        m.write(b, 2, true, v(3));
        let ma = m.check(a, Some(State::of(&[2; 10], None)), Loss::Power, 0).unwrap();
        let mb = m.check(b, Some(State::of(&[1; 10], None)), Loss::Power, 0).unwrap();
        let mut all = BTreeMap::new();
        all.insert(a, ma);
        all.insert(b, mb);
        assert!(m.check_txns(&all, Loss::Power).is_err());
        let mb = m.check(b, Some(State::of(&[3; 10], None)), Loss::Power, 0).unwrap();
        all.insert(b, mb);
        assert!(m.check_txns(&all, Loss::Power).is_ok());
    }

    #[test]
    fn rebase_after_kill_keeps_power_candidates() {
        let mut m = RefModel::new();
        let k = (0, 1);
        m.write(k, 1, false, v(1));
        m.ack(1, Ack::Durable);
        m.write(k, 2, false, v(2));
        m.write(k, 3, false, v(3));
        let s2 = Some(State::of(&[2; 10], None));
        let mt = m.check(k, s2, Loss::Process, 0).unwrap();
        m.crashed();
        m.rebase(k, mt, Loss::Process, v(2));
        // a later power cut may return the durable op 1 or op 2, never op 3
        assert!(m.check(k, Some(State::of(&[1; 10], None)), Loss::Power, 0).is_ok());
        assert!(m.check(k, s2, Loss::Power, 0).is_ok());
        assert!(m.check(k, Some(State::of(&[3; 10], None)), Loss::Power, 0).is_err());
        // and a later kill -9 only op 2
        assert!(m.check(k, Some(State::of(&[1; 10], None)), Loss::Process, 0).is_err());
    }
}
