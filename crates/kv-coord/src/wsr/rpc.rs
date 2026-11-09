//! WSR peer messages and transport (DESIGN §23.3).
//!
//! Frames are `u32 len || postcard((version, msg))` (the kv-ctrl frame codec, at most 16 MiB)
//! over the coordinators' peer port. Each coordinator dials every other coordinator of its own
//! list once and sends all its messages to that peer on that connection; it receives on the
//! connections the others dial in. A connection starts with [`PeerHello`], which carries the
//! sender's id and the cluster token (constant-time compared); afterwards every message is
//! attributed to that sender, so messages carry no sender or leader field. Every message carries
//! the sender's `peer_gen`; a beat carries no state value.
//!
//! Delivery is best effort: a message to a peer that is down or whose queue is full is dropped,
//! and the protocol retries (beats re-send unacknowledged proposals, elections time out).

use core::fmt;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kv_ctrl::CoordId;
use kv_ctrl::codec::{HEADER_LEN, decode_body, frame_len, try_encode};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use super::peers::PeerList;

/// A replicated state value: the canonical encoding of a `ClusterState`, shared between the
/// core, the transport and the state files without copying.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Blob(pub Arc<[u8]>);

impl Blob {
    pub fn new(bytes: Vec<u8>) -> Self {
        Blob(bytes.into())
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for Blob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Blob({} bytes)", self.0.len())
    }
}

impl Serialize for Blob {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for Blob {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = Blob;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a byte string")
            }
            fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<Blob, E> {
                Ok(Blob(v.into()))
            }
            fn visit_byte_buf<E: serde::de::Error>(self, v: Vec<u8>) -> Result<Blob, E> {
                Ok(Blob::new(v))
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut a: A) -> Result<Blob, A::Error> {
                let mut v = Vec::with_capacity(a.size_hint().unwrap_or(0).min(1 << 20));
                while let Some(b) = a.next_element::<u8>()? {
                    v.push(b);
                }
                Ok(Blob::new(v))
            }
        }
        d.deserialize_bytes(V)
    }
}

/// Peer messages. `V` is the state value: [`Blob`] on the wire, a small stand-in in the model
/// checker. `term` is always the sender's current term.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum WsrMsg<V> {
    /// Would the receiver vote for the sender at `term` (= the sender's term + 1)? Changes no
    /// state at the receiver.
    PreVote { peer_gen: u64, term: u64, last_term: u64, last_version: u64 },
    PreVoteReply { peer_gen: u64, term: u64, granted: bool },
    RequestVote { peer_gen: u64, term: u64, last_term: u64, last_version: u64 },
    VoteReply { peer_gen: u64, term: u64, granted: bool },
    /// The whole next state at `version`, proposed in `term`.
    Proposal { peer_gen: u64, term: u64, version: u64, value: V },
    /// `ok`: the receiver persisted the proposal `(term, version)`; `!ok` only answers a
    /// proposal of an older term (then `term` is the receiver's newer term).
    Accepted { peer_gen: u64, term: u64, version: u64, ok: bool },
    /// The proposal `(term, version)` is committed.
    Commit { peer_gen: u64, term: u64, version: u64 },
    /// Leader beat (no value): its committed `(version, term of that value)`; `round` is echoed.
    Beat { peer_gen: u64, term: u64, round: u64, committed_version: u64, committed_term: u64 },
    /// `need`: the follower holds no value for the leader's committed version.
    BeatReply { peer_gen: u64, term: u64, round: u64, need: bool },
    /// The leader's committed value for a follower that lacks it.
    Install { peer_gen: u64, term: u64, version: u64, value_term: u64, value: V },
}

impl<V> WsrMsg<V> {
    pub fn peer_gen(&self) -> u64 {
        match self {
            WsrMsg::PreVote { peer_gen, .. }
            | WsrMsg::PreVoteReply { peer_gen, .. }
            | WsrMsg::RequestVote { peer_gen, .. }
            | WsrMsg::VoteReply { peer_gen, .. }
            | WsrMsg::Proposal { peer_gen, .. }
            | WsrMsg::Accepted { peer_gen, .. }
            | WsrMsg::Commit { peer_gen, .. }
            | WsrMsg::Beat { peer_gen, .. }
            | WsrMsg::BeatReply { peer_gen, .. }
            | WsrMsg::Install { peer_gen, .. } => *peer_gen,
        }
    }

    pub fn term(&self) -> u64 {
        match self {
            WsrMsg::PreVote { term, .. }
            | WsrMsg::PreVoteReply { term, .. }
            | WsrMsg::RequestVote { term, .. }
            | WsrMsg::VoteReply { term, .. }
            | WsrMsg::Proposal { term, .. }
            | WsrMsg::Accepted { term, .. }
            | WsrMsg::Commit { term, .. }
            | WsrMsg::Beat { term, .. }
            | WsrMsg::BeatReply { term, .. }
            | WsrMsg::Install { term, .. } => *term,
        }
    }

    /// Short name for diagnostics.
    pub fn kind(&self) -> &'static str {
        match self {
            WsrMsg::PreVote { .. } => "pre-vote",
            WsrMsg::PreVoteReply { .. } => "pre-vote-reply",
            WsrMsg::RequestVote { .. } => "request-vote",
            WsrMsg::VoteReply { .. } => "vote-reply",
            WsrMsg::Proposal { .. } => "proposal",
            WsrMsg::Accepted { .. } => "accepted",
            WsrMsg::Commit { .. } => "commit",
            WsrMsg::Beat { .. } => "beat",
            WsrMsg::BeatReply { .. } => "beat-reply",
            WsrMsg::Install { .. } => "install",
        }
    }
}

impl<V: fmt::Debug> fmt::Debug for WsrMsg<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WsrMsg::PreVote { peer_gen, term, last_term, last_version } => {
                write!(f, "PreVote(g{peer_gen} t{term} last={last_term}/{last_version})")
            }
            WsrMsg::PreVoteReply { peer_gen, term, granted } => {
                write!(f, "PreVoteReply(g{peer_gen} t{term} {granted})")
            }
            WsrMsg::RequestVote { peer_gen, term, last_term, last_version } => {
                write!(f, "RequestVote(g{peer_gen} t{term} last={last_term}/{last_version})")
            }
            WsrMsg::VoteReply { peer_gen, term, granted } => {
                write!(f, "VoteReply(g{peer_gen} t{term} {granted})")
            }
            WsrMsg::Proposal { peer_gen, term, version, value } => {
                write!(f, "Proposal(g{peer_gen} t{term} v{version} {value:?})")
            }
            WsrMsg::Accepted { peer_gen, term, version, ok } => {
                write!(f, "Accepted(g{peer_gen} t{term} v{version} {ok})")
            }
            WsrMsg::Commit { peer_gen, term, version } => {
                write!(f, "Commit(g{peer_gen} t{term} v{version})")
            }
            WsrMsg::Beat { peer_gen, term, round, committed_version, committed_term } => write!(
                f,
                "Beat(g{peer_gen} t{term} r{round} c={committed_term}/{committed_version})"
            ),
            WsrMsg::BeatReply { peer_gen, term, round, need } => {
                write!(f, "BeatReply(g{peer_gen} t{term} r{round} need={need})")
            }
            WsrMsg::Install { peer_gen, term, version, value_term, value } => write!(
                f,
                "Install(g{peer_gen} t{term} v{version}@{value_term} {value:?})"
            ),
        }
    }
}

/// First frame on a peer connection: who dials, authenticated by the cluster token. No `Debug`
/// (it carries the token).
#[derive(Serialize, Deserialize)]
pub struct PeerHello {
    pub from: CoordId,
    pub auth: Vec<u8>,
}

/// Constant-time equality of two secrets (the comparison time depends only on the longer
/// length).
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    let n = a.len().max(b.len());
    let mut diff = u8::from(a.len() != b.len());
    for i in 0..n {
        diff |= a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(0);
    }
    diff == 0
}

/// Reads one frame body (after its 4-byte header) from `s`.
pub(crate) async fn read_frame<R: AsyncReadExt + Unpin>(s: &mut R) -> std::io::Result<Vec<u8>> {
    let mut head = [0u8; HEADER_LEN];
    s.read_exact(&mut head).await?;
    let len = frame_len(head).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let mut body = vec![0u8; len];
    s.read_exact(&mut body).await?;
    Ok(body)
}

pub(crate) fn encode_frame<T: Serialize>(m: &T) -> std::io::Result<Vec<u8>> {
    let mut out = Vec::new();
    try_encode(m, &mut out).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(out)
}

/// Messages received from peers, attributed to the authenticated sender.
pub type Inbound = mpsc::Sender<(CoordId, WsrMsg<Blob>)>;

/// Frames queued per peer before further messages are dropped.
const PEER_QUEUE: usize = 64;
/// Connect and hello timeout.
const CONNECT_TIMEOUT: Duration = Duration::from_millis(1000);
/// Pause between reconnection attempts.
const RECONNECT_PAUSE: Duration = Duration::from_millis(100);

struct Link {
    addr: SocketAddr,
    tx: mpsc::Sender<Arc<Vec<u8>>>,
    task: tokio::task::JoinHandle<()>,
}

/// Outbound side of the peer transport: one dialing task per coordinator of the local list.
pub struct PeerNet {
    me: CoordId,
    auth: Arc<Vec<u8>>,
    links: Mutex<hashbrown::HashMap<CoordId, Link, foldhash::fast::RandomState>>,
}

impl PeerNet {
    pub fn new(me: CoordId, auth: Vec<u8>) -> Arc<Self> {
        Arc::new(PeerNet {
            me,
            auth: Arc::new(auth),
            links: Mutex::new(hashbrown::HashMap::default()),
        })
    }

    /// Makes the set of dialed peers equal to `peers` (minus this coordinator).
    pub fn set_peers(&self, peers: &PeerList) {
        let mut links = self.links.lock().unwrap_or_else(|e| e.into_inner());
        links.retain(|id, l| {
            let keep = peers.record(id).is_some_and(|r| r.peer == l.addr);
            if !keep {
                l.task.abort();
            }
            keep
        });
        for r in &peers.peers {
            if r.id == self.me || links.contains_key(&r.id) {
                continue;
            }
            let (tx, rx) = mpsc::channel(PEER_QUEUE);
            let task = tokio::spawn(dial_loop(r.peer, self.me, self.auth.clone(), rx));
            links.insert(
                r.id,
                Link {
                    addr: r.peer,
                    tx,
                    task,
                },
            );
        }
    }

    /// Queues `msg` for `to`; dropped when `to` is unknown or its queue is full.
    pub fn send(&self, to: &CoordId, msg: &WsrMsg<Blob>) {
        let frame = match encode_frame(msg) {
            Ok(f) => Arc::new(f),
            Err(e) => {
                tracing::warn!(kind = msg.kind(), "peer message not encodable: {e}");
                return;
            }
        };
        let links = self.links.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(l) = links.get(to) {
            let _ = l.tx.try_send(frame);
        }
    }

    /// Stops every dialing task.
    pub fn shutdown(&self) {
        let mut links = self.links.lock().unwrap_or_else(|e| e.into_inner());
        for (_, l) in links.drain() {
            l.task.abort();
        }
    }
}

impl Drop for PeerNet {
    fn drop(&mut self) {
        self.shutdown();
    }
}

async fn dial_loop(addr: SocketAddr, me: CoordId, auth: Arc<Vec<u8>>, mut rx: mpsc::Receiver<Arc<Vec<u8>>>) {
    let hello = match encode_frame(&PeerHello {
        from: me,
        auth: auth.to_vec(),
    }) {
        Ok(h) => h,
        Err(_) => return,
    };
    loop {
        let conn = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr)).await;
        let mut s = match conn {
            Ok(Ok(s)) => s,
            _ => {
                // Messages queued while the peer is unreachable are stale by the next attempt.
                while rx.try_recv().is_ok() {}
                tokio::time::sleep(RECONNECT_PAUSE).await;
                continue;
            }
        };
        let _ = s.set_nodelay(true);
        if s.write_all(&hello).await.is_err() {
            continue;
        }
        loop {
            let Some(frame) = rx.recv().await else {
                return;
            };
            if s.write_all(&frame).await.is_err() {
                break;
            }
        }
        tokio::time::sleep(RECONNECT_PAUSE).await;
    }
}

/// Accepts peer connections on the peer port and feeds messages to the core.
pub async fn serve_peers(l: TcpListener, auth: Vec<u8>, sink: Inbound) -> std::io::Result<()> {
    let auth = Arc::new(auth);
    loop {
        let (s, from_addr) = l.accept().await?;
        let auth = auth.clone();
        let sink = sink.clone();
        tokio::spawn(async move {
            if let Err(e) = serve_peer_conn(s, &auth, sink).await {
                tracing::debug!(%from_addr, "peer connection closed: {e}");
            }
        });
    }
}

async fn serve_peer_conn(mut s: TcpStream, auth: &[u8], sink: Inbound) -> std::io::Result<()> {
    let _ = s.set_nodelay(true);
    let body = tokio::time::timeout(CONNECT_TIMEOUT, read_frame(&mut s))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "no peer hello"))??;
    let hello: PeerHello = decode_body(&body).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    if !ct_eq(&hello.auth, auth) {
        tracing::warn!("peer connection with a bad cluster token refused");
        return Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, "bad token"));
    }
    let from = hello.from;
    loop {
        let body = read_frame(&mut s).await?;
        let msg: WsrMsg<Blob> = decode_body(&body).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        if sink.send((from, msg)).await.is_err() {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_round_trip_and_constant_time_compare() {
        let m: WsrMsg<Blob> = WsrMsg::Proposal {
            peer_gen: 2,
            term: 7,
            version: 9,
            value: Blob::new(vec![1, 2, 3, 200]),
        };
        let f = encode_frame(&m).unwrap();
        let (back, n): (WsrMsg<Blob>, usize) = kv_ctrl::codec::decode(&f).unwrap().unwrap();
        assert_eq!(n, f.len());
        assert!(back == m);
        assert_eq!(format!("{back:?}"), "Proposal(g2 t7 v9 Blob(4 bytes))");
        assert!(ct_eq(b"token", b"token"));
        assert!(!ct_eq(b"token", b"tokem"));
        assert!(!ct_eq(b"token", b"token1"));
        assert!(!ct_eq(b"", b"x"));
        assert!(ct_eq(b"", b""));
    }
}
