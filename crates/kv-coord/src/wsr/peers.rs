//! the operator-set voter list, never part of ClusterState (DESIGN §23.3, LC-38)
//!
//! Each coordinator keeps `{peer_gen, peers}` in its own `coord.meta`, written only by the
//! operator call `KVADM COORD SETPEERS <gen> <list>`, which kvadm sends to each coordinator in
//! turn. A change adds or removes exactly one coordinator relative to generation `gen - 1`;
//! kvadm refuses the next change until every coordinator of the new list reports the new
//! generation. No list is ever derived from who answered.
//!
//! List syntax (operator input, `kvc init --peers`, `KVADM COORD SETPEERS`): comma-separated
//! entries `ID=HOST:PEERPORT[/CLIENTPORT[/ADMINPORT]]`. `ID` is 40 hex digits or a name of 1 to
//! 20 characters `[A-Za-z0-9._-]` (stored zero padded in the 20 id bytes). A missing client or
//! admin port takes the default given by the caller (`kvc init --client-port/--admin-port`);
//! the client and admin addresses use the peer address's host. The canonical form printed by
//! [`PeerList`]'s `Display` always carries all three ports.

use core::fmt;
use std::net::{SocketAddr, ToSocketAddrs};

use kv_ctrl::{CoordEndpoint, CoordId};
use serde::{Deserialize, Serialize};

/// Most coordinators in a peer list.
pub const MAX_PEERS: usize = 7;

/// Longest coordinator name accepted in the list syntax (the id has 20 bytes).
pub const MAX_NAME_LEN: usize = 20;

/// One coordinator of the peer list.
#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct CoordRecord {
    pub id: CoordId,
    /// Id shown in CLUSTER NODES coordinator lines (40 hex digits when rendered).
    pub id40: [u8; 20],
    pub peer: SocketAddr,
    pub client: SocketAddr,
    pub admin: SocketAddr,
}

impl CoordRecord {
    /// A record whose display id equals its protocol id.
    pub fn new(id: CoordId, peer: SocketAddr, client: SocketAddr, admin: SocketAddr) -> Self {
        CoordRecord {
            id,
            id40: id.0,
            peer,
            client,
            admin,
        }
    }

    /// The coordinator as data nodes and CLUSTER NODES see it.
    pub fn endpoint(&self) -> CoordEndpoint {
        CoordEndpoint {
            id: CoordId(self.id40),
            client: self.client,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct PeerList {
    /// Generation (`gen` is reserved in edition 2024, hence the raw identifier). Generation 0
    /// with an empty list is a coordinator created to join an existing group (`kvc init
    /// --join`): it takes no part in elections until the operator gives it a list.
    pub r#gen: u64,
    pub peers: Vec<CoordRecord>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PeerChangeError {
    /// `new.gen != cur.gen + 1`.
    Generation,
    /// Not exactly one coordinator added or removed.
    NotSingleServer,
    TooMany,
    Empty,
    /// The same id or address appears twice in the new list.
    Duplicate,
    /// A coordinator present in both lists changed its addresses (that is a remove and an add:
    /// two changes).
    AddressChanged,
}

impl fmt::Display for PeerChangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            PeerChangeError::Generation => "the new generation must be the current one + 1",
            PeerChangeError::NotSingleServer => {
                "a peer list change adds or removes exactly one coordinator"
            }
            PeerChangeError::TooMany => "a peer list holds at most 7 coordinators",
            PeerChangeError::Empty => "a peer list cannot be empty",
            PeerChangeError::Duplicate => "duplicate coordinator id or address in the peer list",
            PeerChangeError::AddressChanged => {
                "a coordinator kept in the list must keep its addresses"
            }
        })
    }
}

impl std::error::Error for PeerChangeError {}

impl PeerList {
    /// The list of a coordinator created to join a group (`kvc init --join`).
    pub fn joining() -> Self {
        PeerList {
            r#gen: 0,
            peers: Vec::new(),
        }
    }

    /// Generation 0 with an empty list: not yet given a list by the operator.
    pub fn is_joining(&self) -> bool {
        self.r#gen == 0 && self.peers.is_empty()
    }

    /// Position of `id` in the operator-ordered list.
    pub fn index_of(&self, id: &CoordId) -> Option<usize> {
        self.peers.iter().position(|p| p.id == *id)
    }

    pub fn contains(&self, id: &CoordId) -> bool {
        self.index_of(id).is_some()
    }

    pub fn record(&self, id: &CoordId) -> Option<&CoordRecord> {
        self.peers.iter().find(|p| p.id == *id)
    }

    /// Client endpoints in list order: the coordinator list of node assignments (MOVED
    /// targets) and the coordinator lines of CLUSTER NODES.
    pub fn endpoints(&self) -> Vec<CoordEndpoint> {
        self.peers.iter().map(CoordRecord::endpoint).collect()
    }

    /// Structural checks of one list: non-empty, at most [`MAX_PEERS`], no duplicate id or
    /// address.
    pub fn validate(&self) -> Result<(), PeerChangeError> {
        if self.peers.is_empty() {
            return Err(PeerChangeError::Empty);
        }
        if self.peers.len() > MAX_PEERS {
            return Err(PeerChangeError::TooMany);
        }
        for (i, p) in self.peers.iter().enumerate() {
            let addrs = [p.peer, p.client, p.admin];
            if addrs[0] == addrs[1] || addrs[0] == addrs[2] || addrs[1] == addrs[2] {
                return Err(PeerChangeError::Duplicate);
            }
            for o in &self.peers[..i] {
                let other = [o.peer, o.client, o.admin];
                if o.id == p.id || o.id40 == p.id40 || addrs.iter().any(|a| other.contains(a)) {
                    return Err(PeerChangeError::Duplicate);
                }
            }
        }
        Ok(())
    }

    /// Parses the list syntax of the module documentation. Host names are resolved once
    /// (first address); `default_ports` gives the client and admin ports of entries that omit
    /// them (`None`: every entry must name all three ports).
    pub fn parse(r#gen: u64, s: &str, default_ports: Option<(u16, u16)>) -> Result<Self, String> {
        let mut peers = Vec::new();
        for item in s.split(',') {
            let item = item.trim();
            if item.is_empty() {
                return Err("empty entry in the peer list".into());
            }
            peers.push(parse_entry(item, default_ports)?);
        }
        let list = PeerList { r#gen, peers };
        list.validate().map_err(|e| e.to_string())?;
        Ok(list)
    }
}

impl fmt::Display for PeerList {
    /// The canonical list syntax (all three ports on every entry).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, p) in self.peers.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            write!(
                f,
                "{}={}/{}/{}",
                coord_name(&p.id),
                p.peer,
                p.client.port(),
                p.admin.port()
            )?;
        }
        Ok(())
    }
}

/// Parses a coordinator id: 40 hex digits, or a name of 1 to 20 characters `[A-Za-z0-9._-]`
/// stored zero padded.
pub fn parse_coord_id(s: &str) -> Option<CoordId> {
    if s.len() == 40
        && let Some(id) = CoordId::parse_hex40(s) {
            return Some(id);
        }
    let ok = |c: u8| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b'-');
    if s.is_empty() || s.len() > MAX_NAME_LEN || !s.bytes().all(ok) {
        return None;
    }
    let mut b = [0u8; 20];
    b[..s.len()].copy_from_slice(s.as_bytes());
    Some(CoordId(b))
}

/// The operator-facing form of an id: its name when the bytes are a name followed by zero
/// padding, else 40 hex digits ([`parse_coord_id`] reads both back to the same bytes).
pub fn coord_name(id: &CoordId) -> String {
    let n = id.0.iter().position(|&b| b == 0).unwrap_or(20);
    let ok = |c: u8| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b'-');
    if n > 0 && id.0[..n].iter().all(|&c| ok(c)) && id.0[n..].iter().all(|&b| b == 0) {
        // ASCII checked above
        String::from_utf8_lossy(&id.0[..n]).into_owned()
    } else {
        id.hex40()
    }
}

fn resolve(hostport: &str) -> Result<SocketAddr, String> {
    if let Ok(a) = hostport.parse::<SocketAddr>() {
        return Ok(a);
    }
    hostport
        .to_socket_addrs()
        .map_err(|e| format!("cannot resolve '{hostport}': {e}"))?
        .next()
        .ok_or_else(|| format!("'{hostport}' resolves to no address"))
}

fn parse_port(s: &str) -> Result<u16, String> {
    match s.parse::<u16>() {
        Ok(p) if p != 0 => Ok(p),
        _ => Err(format!("invalid port '{s}'")),
    }
}

fn parse_entry(item: &str, default_ports: Option<(u16, u16)>) -> Result<CoordRecord, String> {
    let (id, addr) = item
        .split_once('=')
        .ok_or_else(|| format!("peer entry '{item}' must be ID=HOST:PORT[/CLIENT[/ADMIN]]"))?;
    let id = parse_coord_id(id.trim())
        .ok_or_else(|| format!("invalid coordinator id '{}'", id.trim()))?;
    let mut parts = addr.trim().split('/');
    let peer = resolve(parts.next().unwrap_or(""))?;
    if peer.port() == 0 {
        return Err(format!("peer entry '{item}' needs a non-zero peer port"));
    }
    let client = parts.next().map(parse_port).transpose()?;
    let admin = parts.next().map(parse_port).transpose()?;
    if parts.next().is_some() {
        return Err(format!("peer entry '{item}' has too many ports"));
    }
    let (client, admin) = match (client, admin, default_ports) {
        (Some(c), Some(a), _) => (c, a),
        (Some(c), None, Some((_, a))) => (c, a),
        (None, None, Some((c, a))) => (c, a),
        _ => {
            return Err(format!(
                "peer entry '{item}' needs its client and admin ports (HOST:PEER/CLIENT/ADMIN)"
            ));
        }
    };
    Ok(CoordRecord::new(
        id,
        peer,
        SocketAddr::new(peer.ip(), client),
        SocketAddr::new(peer.ip(), admin),
    ))
}

/// gen = cur.gen + 1, exactly one server added or removed
///
/// A coordinator still joining (generation 0, empty list) accepts any well-formed list of a
/// generation >= 1: it is the server being added.
pub fn set_peers(cur: &PeerList, new: &PeerList) -> Result<(), PeerChangeError> {
    new.validate()?;
    if cur.is_joining() {
        return if new.r#gen >= 1 {
            Ok(())
        } else {
            Err(PeerChangeError::Generation)
        };
    }
    if Some(new.r#gen) != cur.r#gen.checked_add(1) {
        return Err(PeerChangeError::Generation);
    }
    let removed = cur.peers.iter().filter(|p| !new.contains(&p.id)).count();
    let added = new.peers.iter().filter(|p| !cur.contains(&p.id)).count();
    if removed + added != 1 {
        return Err(PeerChangeError::NotSingleServer);
    }
    for p in &new.peers {
        if let Some(old) = cur.record(&p.id)
            && old != p {
                return Err(PeerChangeError::AddressChanged);
            }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(name: &str, port: u16) -> CoordRecord {
        let ip = "127.0.0.1".parse().unwrap();
        CoordRecord::new(
            parse_coord_id(name).unwrap(),
            SocketAddr::new(ip, port),
            SocketAddr::new(ip, port + 1000),
            SocketAddr::new(ip, port + 2000),
        )
    }

    /// Addresses depend on the name only, so a coordinator keeps them across lists.
    fn list(r#gen: u64, names: &[&str]) -> PeerList {
        PeerList {
            r#gen,
            peers: names
                .iter()
                .map(|n| rec(n, 7100 + u16::from(n.as_bytes()[0])))
                .collect(),
        }
    }

    #[test]
    fn parse_and_print_round_trip() {
        let l = PeerList::parse(
            1,
            "c1=127.0.0.1:7100, c2=127.0.0.1:7101/7001/7201,c3=[::1]:7102",
            Some((7000, 7200)),
        )
        .unwrap();
        assert_eq!(l.peers.len(), 3);
        assert_eq!(l.peers[0].client, "127.0.0.1:7000".parse().unwrap());
        assert_eq!(l.peers[1].admin, "127.0.0.1:7201".parse().unwrap());
        assert_eq!(l.peers[2].client, "[::1]:7000".parse().unwrap());
        let s = l.to_string();
        assert_eq!(
            s,
            "c1=127.0.0.1:7100/7000/7200,c2=127.0.0.1:7101/7001/7201,c3=[::1]:7102/7000/7200"
        );
        assert_eq!(PeerList::parse(1, &s, None).unwrap(), l);
        // without defaults every entry needs its ports
        assert!(PeerList::parse(1, "c1=127.0.0.1:7100", None).is_err());
        for bad in [
            "",
            "c1",
            "=127.0.0.1:1",
            "c1=127.0.0.1:0",
            "c1=127.0.0.1:1/2/3/4",
            "c1=127.0.0.1:7100,c1=127.0.0.1:7101",
            "c1=127.0.0.1:7100,c2=127.0.0.1:7100",
            "this-name-is-way-too-long-for-an-id=127.0.0.1:1",
        ] {
            assert!(PeerList::parse(1, bad, Some((7000, 7200))).is_err(), "{bad}");
        }
    }

    #[test]
    fn ids_and_names() {
        let id = parse_coord_id("c1").unwrap();
        assert_eq!(coord_name(&id), "c1");
        let hex = CoordId::random();
        assert_eq!(parse_coord_id(&coord_name(&hex)), Some(hex));
        assert_eq!(parse_coord_id(&hex.hex40()), Some(hex));
        assert!(parse_coord_id("a b").is_none());
        assert!(parse_coord_id("").is_none());
    }

    #[test]
    fn single_server_deltas_only() {
        let g1 = list(1, &["a", "b", "c"]);
        assert_eq!(set_peers(&g1, &list(2, &["a", "b", "c", "d"])), Ok(()));
        assert_eq!(set_peers(&g1, &list(2, &["a", "b"])), Ok(()));
        assert_eq!(set_peers(&g1, &list(2, &["b", "a"])), Ok(()));
        assert_eq!(
            set_peers(&g1, &list(3, &["a", "b", "c", "d"])),
            Err(PeerChangeError::Generation)
        );
        assert_eq!(
            set_peers(&g1, &list(1, &["a", "b", "c", "d"])),
            Err(PeerChangeError::Generation)
        );
        assert_eq!(
            set_peers(&g1, &list(2, &["a", "b", "c"])),
            Err(PeerChangeError::NotSingleServer)
        );
        assert_eq!(
            set_peers(&g1, &list(2, &["a", "b", "d"])),
            Err(PeerChangeError::NotSingleServer)
        );
        assert_eq!(
            set_peers(&g1, &list(2, &["a", "b", "c", "d", "e"])),
            Err(PeerChangeError::NotSingleServer)
        );
        assert_eq!(
            set_peers(&list(1, &["a"]), &list(2, &[])),
            Err(PeerChangeError::Empty)
        );
        let mut moved = list(2, &["a", "b", "c", "d"]);
        moved.peers[0].client = "127.0.0.1:1".parse().unwrap();
        assert_eq!(set_peers(&g1, &moved), Err(PeerChangeError::AddressChanged));
        let big = list(1, &["a", "b", "c", "d", "e", "f", "g"]);
        assert_eq!(
            set_peers(&big, &list(2, &["a", "b", "c", "d", "e", "f", "g", "h"])),
            Err(PeerChangeError::TooMany)
        );
        // a joining coordinator accepts the list it is added by
        assert_eq!(
            set_peers(&PeerList::joining(), &list(5, &["a", "b", "c", "d"])),
            Ok(())
        );
        assert_eq!(
            set_peers(&PeerList::joining(), &list(0, &["a"])),
            Err(PeerChangeError::Generation)
        );
    }

    /// §25.2: `set_peers` accepts exactly the single-server deltas with gen + 1 (LC-38),
    /// exhaustively over every pair of lists drawn from five coordinators.
    #[test]
    fn set_peers_accepts_only_single_server_deltas() {
        let names = ["a", "b", "c", "d", "e"];
        let subsets: Vec<Vec<&str>> = (1u32..32)
            .map(|m| {
                names
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| m & (1 << i) != 0)
                    .map(|(_, n)| *n)
                    .collect()
            })
            .collect();
        for a in &subsets {
            for b in &subsets {
                let cur = list(4, a);
                for g in [3u64, 4, 5, 6] {
                    let new = list(g, b);
                    let delta = a.iter().filter(|n| !b.contains(n)).count()
                        + b.iter().filter(|n| !a.contains(n)).count();
                    let ok = g == 5 && delta == 1;
                    assert_eq!(set_peers(&cur, &new).is_ok(), ok, "{a:?} -> {b:?} gen {g}");
                }
            }
        }
    }
}
