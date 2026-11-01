//! Per-key history checker for the cluster simulation (DESIGN §23.11, §25.3): acknowledged
//! writes are visible to every read that starts after the acknowledgement until overwritten; no
//! read returns a never-written value; no acknowledged delete resurrects. Reads of unacknowledged
//! writes are permitted (G4).

/// One observed operation on a key.
pub enum Event {
    Write { id: u64, start_ns: u64, ack_ns: Option<u64> },
    Delete { id: u64, start_ns: u64, ack_ns: Option<u64> },
    Read { start_ns: u64, end_ns: u64, observed: Option<u64> },
}

/// Checks one key's history.
pub fn check_key(history: &[Event]) -> Result<(), String> {
    todo!("DESIGN §25.3: per-key history check")
}
