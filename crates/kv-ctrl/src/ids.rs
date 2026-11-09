//! Identifiers (DESIGN §23.2, LC-27).
//!
//! `NodeId` and `CoordId` are 20 random bytes shown as 40 hex digits. They implement
//! `Eq + Hash + Clone + Copy + Debug + serde` and deliberately not `Ord`/`PartialOrd`: no
//! decision anywhere is taken by id order (coordinator election is randomized, failover uses
//! static operator priority and member order). Checked at compile time below. `ShardId` is the
//! operator-chosen shard number; shards are kept in operator order.
//!
//! Ids are drawn once (a node id at `kvadm node add`, a coordinator id at `kvc init`) and never
//! per transition.

use core::fmt;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub [u8; 20]);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CoordId(pub [u8; 20]);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct ShardId(pub u16);

// LC-27: no leader, primary or tie-break is ever chosen by comparing ids.
static_assertions::assert_not_impl_any!(NodeId: PartialOrd, Ord);
static_assertions::assert_not_impl_any!(CoordId: PartialOrd, Ord);
static_assertions::assert_impl_all!(NodeId: Copy, Eq, core::hash::Hash, fmt::Debug, Send, Sync);
static_assertions::assert_impl_all!(CoordId: Copy, Eq, core::hash::Hash, fmt::Debug, Send, Sync);
static_assertions::assert_impl_all!(ShardId: Copy, Eq, core::hash::Hash, fmt::Debug, Send, Sync);

const HEX: &[u8; 16] = b"0123456789abcdef";

fn write_hex40(b: &[u8; 20], f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let mut s = [0u8; 40];
    for (i, &x) in b.iter().enumerate() {
        s[2 * i] = HEX[usize::from(x >> 4)];
        s[2 * i + 1] = HEX[usize::from(x & 15)];
    }
    // The buffer holds ASCII hex digits only.
    f.write_str(core::str::from_utf8(&s).map_err(|_| fmt::Error)?)
}

fn hex40(b: &[u8; 20]) -> String {
    let mut s = String::with_capacity(40);
    for &x in b {
        s.push(char::from(HEX[usize::from(x >> 4)]));
        s.push(char::from(HEX[usize::from(x & 15)]));
    }
    s
}

fn parse_hex40(s: &str) -> Option<[u8; 20]> {
    let s = s.as_bytes();
    if s.len() != 40 {
        return None;
    }
    let nib = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    };
    let mut out = [0u8; 20];
    for (i, o) in out.iter_mut().enumerate() {
        *o = (nib(s[2 * i])? << 4) | nib(s[2 * i + 1])?;
    }
    Some(out)
}

impl NodeId {
    /// A fresh random id (drawn once, at `kvadm node add`).
    pub fn random() -> Self {
        NodeId(rand::random())
    }

    /// The 40 lowercase hex digits shown in CLUSTER SLOTS/SHARDS/NODES and MYID.
    pub fn hex40(&self) -> String {
        hex40(&self.0)
    }

    /// Parses 40 hex digits (either case).
    pub fn parse_hex40(s: &str) -> Option<Self> {
        parse_hex40(s).map(NodeId)
    }
}

impl CoordId {
    /// A fresh random id (drawn once, at `kvc init`).
    pub fn random() -> Self {
        CoordId(rand::random())
    }

    /// The 40 lowercase hex digits shown in CLUSTER NODES and MYID.
    pub fn hex40(&self) -> String {
        hex40(&self.0)
    }

    /// Parses 40 hex digits (either case).
    pub fn parse_hex40(s: &str) -> Option<Self> {
        parse_hex40(s).map(CoordId)
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex40(&self.0, f)
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NodeId(")?;
        write_hex40(&self.0, f)?;
        f.write_str(")")
    }
}

impl fmt::Display for CoordId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex40(&self.0, f)
    }
}

impl fmt::Debug for CoordId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CoordId(")?;
        write_hex40(&self.0, f)?;
        f.write_str(")")
    }
}

impl fmt::Display for ShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex40_round_trip() {
        let id = NodeId::random();
        let s = id.hex40();
        assert_eq!(s.len(), 40);
        assert_eq!(NodeId::parse_hex40(&s), Some(id));
        assert_eq!(NodeId::parse_hex40(&s.to_uppercase()), Some(id));
        assert_eq!(NodeId::parse_hex40("xyz"), None);
        assert_eq!(NodeId::parse_hex40(&"g".repeat(40)), None);
        assert_eq!(id.to_string(), s);
        assert_eq!(format!("{id:?}"), format!("NodeId({s})"));

        let c = CoordId::random();
        assert_eq!(CoordId::parse_hex40(&c.hex40()), Some(c));
        assert_eq!(c.to_string(), c.hex40());
    }

    #[test]
    fn hex40_known_vector() {
        let mut b = [0u8; 20];
        b[0] = 0x9b;
        b[19] = 0x0f;
        let id = NodeId(b);
        assert_eq!(id.hex40(), "9b0000000000000000000000000000000000000f");
        assert_eq!(ShardId(7).to_string(), "7");
    }

    #[test]
    fn random_ids_differ() {
        assert_ne!(NodeId::random(), NodeId::random());
        assert_ne!(CoordId::random(), CoordId::random());
    }
}
