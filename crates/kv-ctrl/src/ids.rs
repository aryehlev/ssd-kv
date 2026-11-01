//! Identifiers (DESIGN §23.2, LC-27).
//!
//! `NodeId` and `CoordId` are 20 random bytes shown as 40 hex digits. They implement
//! `Eq + Hash + Clone + Copy + Debug + serde` and deliberately not `Ord`/`PartialOrd`: no
//! decision anywhere is taken by id order (coordinator election is randomized, failover uses
//! static operator priority and member order). Checked at compile time below.

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct NodeId(pub [u8; 20]);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct CoordId(pub [u8; 20]);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct ShardId(pub u16);

static_assertions::assert_not_impl_any!(NodeId: PartialOrd, Ord);
static_assertions::assert_not_impl_any!(CoordId: PartialOrd, Ord);

fn hex40(b: &[u8; 20]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
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
    pub fn random() -> Self {
        NodeId(rand::random())
    }

    pub fn hex40(&self) -> String {
        hex40(&self.0)
    }

    pub fn parse_hex40(s: &str) -> Option<Self> {
        parse_hex40(s).map(NodeId)
    }
}

impl CoordId {
    pub fn random() -> Self {
        CoordId(rand::random())
    }

    pub fn hex40(&self) -> String {
        hex40(&self.0)
    }

    pub fn parse_hex40(s: &str) -> Option<Self> {
        parse_hex40(s).map(CoordId)
    }
}

#[cfg(test)]
mod tests {
    use super::NodeId;

    #[test]
    fn hex40_round_trip() {
        let id = NodeId::random();
        let s = id.hex40();
        assert_eq!(s.len(), 40);
        assert_eq!(NodeId::parse_hex40(&s), Some(id));
        assert_eq!(NodeId::parse_hex40("xyz"), None);
    }
}
