//! A DEL of a key whose SET is still on its way to the device (staged, or its page or extent
//! write in flight), followed by a crash at every completion boundary: once the DEL's ticket is
//! complete (absent, `Written` for kill -9, `Durable` for a power cut) the key never comes back
//! after recovery, nor after a second crash once recovery's own work is done (I1, I2).
//!
//! What a DEL's ticket guarantees when its key had a write in flight: a DEL that found the key
//! always gets a ticket (only a DEL that found nothing, reply 0, has none: nothing has to
//! reach the device). The ticket is `Written` once the delete is on the device in a form that
//! hides every older copy of the key, the one the in-flight SET may still leave behind
//! included (a newer image of the same page, written after the one in flight, or the zeroing
//! of the SET's extent once its write completed), and `Durable` once that is flushed. A crash at any point
//! after the ticket's state never brings the key back; a crash before it leaves the key either
//! deleted or at one of its versions that were not acknowledged as deleted.

#![cfg(feature = "sim")]

mod common;

use common::*;
use kv_storage::engine::{LocalEngine, format_devices};
use kv_storage::io::BlockDevice;
use kv_storage::sim::{SimDevice, TearMode};
use kv_storage::ticket::TicketState;

const T0: u64 = 1_800_000_000_000;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Crash {
    /// kill -9: submitted writes complete; completed writes stay in the device cache.
    Kill,
    /// kill -9: each submitted write completes, is dropped or is torn.
    KillTorn,
    /// Power cut: only durable data survives.
    Power,
    /// Power cut with torn in-flight and volatile sectors.
    PowerTorn,
}

impl Crash {
    fn apply(self, d: &mut SimDevice) {
        match self {
            Crash::Kill => d.crash(TearMode::None),
            Crash::KillTorn => d.crash(TearMode::Sectors),
            Crash::Power => d.power_cut(TearMode::None),
            Crash::PowerTorn => d.power_cut(TearMode::Sectors),
        }
    }

    /// The DEL is acknowledged at the crash.
    fn acked(self, t: Option<TicketState>) -> bool {
        match t {
            None => true,
            Some(TicketState::Durable) => true,
            Some(TicketState::Written) => matches!(self, Crash::Kill | Crash::KillTorn),
            _ => false,
        }
    }
}

struct Case {
    old: Option<usize>,
    new: usize,
    /// Pumps between the SET and the DEL (0: the SET is still staged).
    gap: usize,
    write_through: bool,
    lba: u32,
    atomic: Option<u32>,
}

fn open(dev: SimDevice) -> Db<SimDevice> {
    let mut eng = LocalEngine::open(vec![dev], &cfg()).unwrap();
    eng.recover().unwrap();
    Db::new(eng, T0)
}

/// Runs `c` up to the DEL and returns the engine with the key's partition and the DEL's ticket.
fn start(c: &Case, seed: u64) -> (Db<SimDevice>, u16, Option<kv_storage::ticket::TicketId>) {
    let mut dev = SimDevice::new(96 * 4096, c.lba, c.atomic, seed).with_write_through(c.write_through);
    dev.set_max_completions(1);
    format_devices(&mut [&mut dev as &mut dyn BlockDevice], 16, false, 0, seed).unwrap();
    let mut db = open(dev);
    let k: &[u8] = b"f4-key";
    if let Some(len) = c.old {
        let t = db.set(0, k, &value(seed, len), None).unwrap();
        db.wait(k, t, TicketState::Durable);
        db.quiesce();
    }
    let _t_set = db.set(0, k, &value(seed + 1, c.new), None).unwrap();
    for _ in 0..c.gap {
        db.eng.settle(1);
    }
    let (found, t_del) = db.del(0, k).unwrap();
    assert!(found, "the SET's version is visible to the DEL");
    assert!(t_del.is_some(), "a DEL that removed a key has a ticket");
    let p = db.eng.partition_of(k);
    (db, p, t_del)
}

/// Driver iterations after the DEL until the engine is quiescent with nothing on the device.
fn boundaries(c: &Case, seed: u64) -> usize {
    let (mut db, _, _) = start(c, seed);
    for n in 1..10_000 {
        if db.eng.settle(1) && db.eng.device(0).pending() == 0 {
            return n;
        }
    }
    panic!("not quiescent");
}

/// Runs `c` up to the DEL, drives `n` more iterations and crashes. Returns whether the DEL was
/// acknowledged and the value found after recovery and after a second crash.
fn run(c: &Case, crash: Crash, n: usize, seed: u64) -> (bool, Option<Vec<u8>>, Option<Vec<u8>>) {
    let k: &[u8] = b"f4-key";
    let (mut db, p, t_del) = start(c, seed);
    for _ in 0..n {
        db.eng.settle(1);
    }
    let st = t_del.map(|t| db.eng.ticket_state(p, t).unwrap_or(TicketState::Executed));
    let acked = crash.acked(st);
    let mut devs = db.eng.into_devices();
    let mut dev = devs.pop().unwrap();
    crash.apply(&mut dev);
    let mut db = open(dev);
    let after = db.get(0, k).unwrap().map(|x| x.1);
    // recovery's scrubs and zeroing run to the end, then a second crash
    db.quiesce();
    let mut devs = db.eng.into_devices();
    let mut dev = devs.pop().unwrap();
    crash.apply(&mut dev);
    let mut db = open(dev);
    let again = db.get(0, k).unwrap().map(|x| x.1);
    db.quiesce();
    (acked, after, again)
}

#[test]
fn deleted_key_with_a_write_in_flight_never_comes_back() {
    // KVSSD_F4_FULL=1: every device shape with every crash model (about ten times longer); a
    // debug build runs one write-back shape per page-update mode
    let full = std::env::var("KVSSD_F4_FULL").is_ok();
    let devices: &[(bool, u32, Option<u32>)] = if full {
        &[(false, 512, None), (true, 4096, None), (false, 4096, Some(4096)), (true, 512, Some(4096))]
    } else if cfg!(debug_assertions) {
        &[(false, 512, None)]
    } else {
        &[(false, 512, None), (false, 4096, Some(4096))]
    };
    let crashes: &[Crash] = if full { &[Crash::Kill, Crash::KillTorn, Crash::Power, Crash::PowerTorn] } else { &[Crash::KillTorn, Crash::Power] };
    let mut cases = Vec::new();
    let olds: &[Option<usize>] = if cfg!(debug_assertions) && !full { &[None, Some(20_000)] } else { &[None, Some(100), Some(20_000)] };
    for &old in olds {
        for new in [100usize, 20_000] {
            for gap in 0..3 {
                for &(write_through, lba, atomic) in devices {
                    cases.push(Case { old, new, gap, write_through, lba, atomic });
                }
            }
        }
    }
    let mut bad = Vec::new();
    let (mut count, mut acked_points) = (0usize, 0usize);
    for (i, c) in cases.iter().enumerate() {
        for &crash in crashes {
            let seed = 1000 + i as u64 * 7;
            let last = boundaries(c, seed);
            for n in 0..=last {
                count += 1;
                let (acked, after, again) = run(c, crash, n, seed);
                acked_points += usize::from(acked);
                let allowed = |v: &Option<Vec<u8>>| match v {
                    None => true,
                    Some(v) => !acked && (Some(v.clone()) == c.old.map(|l| value(seed, l)) || *v == value(seed + 1, c.new)),
                };
                if !allowed(&after) || !allowed(&again) || (after.is_none() && again.is_some()) {
                    bad.push(format!(
                        "old={:?} new={} gap={} wt={} lba={} atomic={:?} {crash:?} after {n} pumps: acked={acked} after={:?} again={:?}",
                        c.old,
                        c.new,
                        c.gap,
                        c.write_through,
                        c.lba,
                        c.atomic,
                        after.as_ref().map(Vec::len),
                        again.as_ref().map(Vec::len)
                    ));
                }
            }
        }
    }
    eprintln!("{count} crash points, the DEL acknowledged at {acked_points}");
    assert!(acked_points > 0 && acked_points < count);
    assert!(bad.is_empty(), "{} of {} crash points brought a deleted key back or lost it inconsistently:\n{}", bad.len(), count, bad.join("\n"));
}
