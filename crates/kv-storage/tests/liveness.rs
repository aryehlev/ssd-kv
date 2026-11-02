//! Liveness regressions: every accepted request reaches a terminal state in bounded time
//! (DESIGN §6.5, §14, §16; "no hangs"). Each test names the stuck state it once produced.

mod common;

use common::*;
use kv_storage::engine::{LocalEngine, format_devices};
use kv_storage::io::BlockDevice;
use kv_storage::ticket::TicketState;

const T0: u64 = 1_800_000_000_000;

fn fresh(pages: u64, seed: u64) -> Db<RamDev> {
    let mut d = RamDev::new(pages, 512, seed);
    format_devices(&mut [&mut d as &mut dyn BlockDevice], 16, false, 0, seed).unwrap();
    let mut eng = LocalEngine::open(vec![d], &cfg()).unwrap();
    eng.recover().unwrap();
    Db::new(eng, T0)
}

/// Two writes of one key executed in the same tick, the first staging a new location (an
/// extent, or a small record on another page) that the second replaces before it is written:
/// the first command's ticket is handed to the second's effect and must complete with it.
/// It used to be counted twice (once for the cancelled write, again for the new effect) and
/// stayed `Executed` forever.
#[test]
fn ticket_handed_from_a_cancelled_staged_write_completes() {
    let shapes: [(usize, usize); 6] = [(6000, 10), (10, 6000), (6000, 9000), (3_000_000, 10), (10, 3_000_000), (1500, 1800)];
    for (i, &(a, b)) in shapes.iter().enumerate() {
        let mut db = fresh(64 * 4096, 7 + i as u64);
        let key = format!("k{i}");
        let k = key.as_bytes();
        // an existing small version first, so that the second write may move the key
        let t0 = db.set(0, k, b"seed", None).unwrap();
        assert_eq!(db.wait(k, t0, TicketState::Written), TicketState::Durable);
        // `run` returns right after exec: the second write executes before any tick
        let t1 = db.set(0, k, &value(1, a), None).unwrap();
        let t2 = db.set(0, k, &value(2, b), None).unwrap();
        assert_eq!(db.wait(k, t2, TicketState::Durable), TicketState::Durable, "shape {a}->{b}: second write");
        assert_eq!(db.wait(k, t1, TicketState::Durable), TicketState::Durable, "shape {a}->{b}: first write's ticket");
        let (_, v) = db.get(0, k).unwrap().expect("present");
        assert_eq!(v, value(2, b));
        assert!(db.eng.settle(100_000), "shape {a}->{b}: engine settles");
    }
}
