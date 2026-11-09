//! Per-key history checker for the cluster simulation (DESIGN §23.11, §25.3): acknowledged
//! writes are visible to every read that starts after the acknowledgement until overwritten; no
//! read returns a never-written value; no acknowledged delete resurrects. Reads of unacknowledged
//! writes are permitted (G4).
//!
//! The model is a register per key whose writes and deletes are the operations of every client
//! on it. A write or delete that was never acknowledged may take effect at any time, or never
//! (G6: possibly later, after a repair), so it is a valid source for any read that does not end
//! before it started. An acknowledged operation `c` is a valid source for a read `r` unless some
//! other acknowledged operation `a` superseded it: `c` was acknowledged before `a` started and `a`
//! was acknowledged before `r` started (then `r` must see `a` or something newer). The initial
//! absent state is an acknowledged delete at time 0. Read-read order is not checked: with
//! read-uncommitted visibility (G4) a read may see a write that a later failover loses.

/// One observed operation on a key.
pub enum Event {
    Write {
        id: u64,
        start_ns: u64,
        ack_ns: Option<u64>,
    },
    Delete {
        id: u64,
        start_ns: u64,
        ack_ns: Option<u64>,
    },
    Read {
        start_ns: u64,
        end_ns: u64,
        observed: Option<u64>,
    },
}

/// A write or delete (`value: None`).
#[derive(Clone, Copy)]
struct Mutation {
    value: Option<u64>,
    start: u64,
    ack: Option<u64>,
}

/// `a` was acknowledged before `b` started (real-time order of two acknowledged operations).
fn before(a: &Mutation, b: &Mutation) -> bool {
    a.ack.is_some_and(|t| t < b.start)
}

/// Checks one key's history.
pub fn check_key(history: &[Event]) -> Result<(), String> {
    let mut muts = vec![Mutation {
        value: None,
        start: 0,
        ack: Some(0),
    }];
    let mut ids = Vec::new();
    for e in history {
        match *e {
            Event::Write {
                id,
                start_ns,
                ack_ns,
            } => {
                if ids.contains(&id) {
                    return Err(format!("write id {id} used twice"));
                }
                ids.push(id);
                muts.push(Mutation {
                    value: Some(id),
                    start: start_ns,
                    ack: ack_ns,
                });
            }
            Event::Delete {
                start_ns, ack_ns, ..
            } => muts.push(Mutation {
                value: None,
                start: start_ns,
                ack: ack_ns,
            }),
            Event::Read { .. } => {}
        }
        if let Event::Write {
            start_ns,
            ack_ns: Some(a),
            ..
        }
        | Event::Delete {
            start_ns,
            ack_ns: Some(a),
            ..
        } = *e
            && a < start_ns
        {
            return Err(format!(
                "operation acknowledged at {a} before it started at {start_ns}"
            ));
        }
    }
    for e in history {
        let Event::Read {
            start_ns,
            end_ns,
            observed,
        } = *e
        else {
            continue;
        };
        if let Some(id) = observed
            && !ids.contains(&id)
        {
            return Err(format!(
                "read at {start_ns} returned value {id}, which was never written"
            ));
        }
        let valid = muts.iter().any(|c| {
            c.value == observed
                && c.start < end_ns
                && !muts.iter().any(|a| {
                    !std::ptr::eq(a, c) && a.ack.is_some_and(|t| t < start_ns) && before(c, a)
                })
        });
        if !valid {
            let what = match observed {
                Some(id) => format!("value {id}"),
                None => "no value".to_string(),
            };
            // name the acknowledged operation that should have been visible
            let newest = muts
                .iter()
                .filter(|a| a.ack.is_some_and(|t| t < start_ns))
                .max_by_key(|a| a.ack)
                .map(|a| match a.value {
                    Some(v) => format!("write {v} acknowledged at {}", a.ack.unwrap_or(0)),
                    None => format!("delete acknowledged at {}", a.ack.unwrap_or(0)),
                })
                .unwrap_or_default();
            return Err(format!(
                "read [{start_ns}, {end_ns}] returned {what}: superseded ({newest})"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn w(id: u64, s: u64, a: Option<u64>) -> Event {
        Event::Write {
            id,
            start_ns: s,
            ack_ns: a,
        }
    }
    fn d(id: u64, s: u64, a: Option<u64>) -> Event {
        Event::Delete {
            id,
            start_ns: s,
            ack_ns: a,
        }
    }
    fn r(s: u64, e: u64, o: Option<u64>) -> Event {
        Event::Read {
            start_ns: s,
            end_ns: e,
            observed: o,
        }
    }

    #[test]
    fn acked_writes_visible_until_overwritten() {
        // empty key reads nothing
        assert!(check_key(&[r(0, 5, None)]).is_ok());
        // a read after the ack sees the write
        assert!(check_key(&[w(1, 10, Some(20)), r(30, 40, Some(1))]).is_ok());
        assert!(
            check_key(&[w(1, 10, Some(20)), r(30, 40, None)]).is_err(),
            "acked write lost"
        );
        // a read overlapping the write may see either
        assert!(check_key(&[w(1, 10, Some(20)), r(15, 25, None)]).is_ok());
        assert!(check_key(&[w(1, 10, Some(20)), r(15, 25, Some(1))]).is_ok());
        // overwrite: the newer acked value must be seen
        let h = [w(1, 10, Some(20)), w(2, 30, Some(40)), r(50, 60, Some(1))];
        assert!(check_key(&h).is_err(), "stale read of an overwritten value");
        let h = [w(1, 10, Some(20)), w(2, 30, Some(40)), r(50, 60, Some(2))];
        assert!(check_key(&h).is_ok());
        // concurrent writes: either order
        let h = [w(1, 10, Some(40)), w(2, 15, Some(35)), r(50, 60, Some(1))];
        assert!(check_key(&h).is_ok());
        let h = [w(1, 10, Some(40)), w(2, 15, Some(35)), r(50, 60, Some(2))];
        assert!(check_key(&h).is_ok());
    }

    #[test]
    fn unacked_writes_may_or_may_not_take_effect() {
        let h = [w(1, 10, Some(20)), w(2, 30, None), r(50, 60, Some(1))];
        assert!(check_key(&h).is_ok());
        let h = [w(1, 10, Some(20)), w(2, 30, None), r(50, 60, Some(2))];
        assert!(check_key(&h).is_ok(), "read-uncommitted visibility (G4)");
        // ... but never before they were issued
        let h = [w(1, 10, Some(20)), w(2, 70, None), r(50, 60, Some(2))];
        assert!(check_key(&h).is_err());
        // an unacked write may take effect after a later acked one (G6)
        let h = [w(1, 10, None), w(2, 30, Some(40)), r(50, 60, Some(1))];
        assert!(check_key(&h).is_ok());
    }

    #[test]
    fn never_written_values_and_resurrection() {
        assert!(check_key(&[w(1, 10, Some(20)), r(30, 40, Some(9))]).is_err());
        // an acked delete never resurrects the value it removed
        let h = [w(1, 10, Some(20)), d(2, 30, Some(40)), r(50, 60, Some(1))];
        assert!(check_key(&h).is_err(), "resurrection");
        let h = [w(1, 10, Some(20)), d(2, 30, Some(40)), r(50, 60, None)];
        assert!(check_key(&h).is_ok());
        // an unacked delete may explain an absent key
        let h = [w(1, 10, Some(20)), d(2, 30, None), r(50, 60, None)];
        assert!(check_key(&h).is_ok());
        // a delete that finished before the write started cannot
        let h = [d(2, 1, Some(5)), w(1, 10, Some(20)), r(50, 60, None)];
        assert!(check_key(&h).is_err());
        // write after delete is visible again
        let h = [
            w(1, 10, Some(20)),
            d(2, 30, Some(40)),
            w(3, 45, Some(48)),
            r(50, 60, Some(3)),
        ];
        assert!(check_key(&h).is_ok());
        assert!(check_key(&[w(1, 10, Some(5))]).is_err(), "ack before start");
        assert!(
            check_key(&[w(1, 1, None), w(1, 2, None)]).is_err(),
            "duplicate id"
        );
    }
}
