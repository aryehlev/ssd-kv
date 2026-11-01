//! Reference model for the storage simulator (DESIGN §25.3).
//!
//! Per key: the last acknowledged state and the set of in-flight (unacknowledged) operations.
//! After recovery every key must be in {last acked state, a state produced by an unacked
//! in-flight op} (I1); acked deletes and FLUSH never resurrect and expired keys never return
//! (I2); durability per mode (I3); multi-key transactions are all-or-nothing (I4).

/// Acknowledged and in-flight state of one key.
pub struct KeyModel {
    pub acked: Option<Vec<u8>>,
    pub in_flight: Vec<Option<Vec<u8>>>,
}

/// The model of one simulated node (ordered map: deterministic iteration under a seed).
pub struct RefModel {
    keys: std::collections::BTreeMap<(u8, Vec<u8>), KeyModel>,
}
