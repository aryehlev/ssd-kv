//! Redirects for keyed commands sent to a coordinator (DESIGN §23.5).
//!
//! `-MOVED <slot> <primary ip:port>` from the committed map; an unassigned slot, or a shard that
//! is `NoPrimary`/`Promoting`, whose primary carries a committed FAIL flag, or whose primary the
//! leader currently suspects (PFAIL, leader RAM only, §23.10) gives
//! `-CLUSTERDOWN Hash slot not served`. Keys in different slots give `-CROSSSLOT`.
//!
//! ASK is never emitted: a migration is a copy, one ownership flip and a short TRYAGAIN window
//! on the destination (§23.13), so no builder for it exists. [`for_slot`] never returns
//! [`Redirect::TryAgain`] either (the coordinator redirects to the destination from the cutover
//! commit on); the variant and its error text serve callers that answer TRYAGAIN themselves.
//!
//! The error texts are byte-identical to redis-server 7.0.15.

use std::fmt::Write as _;
use std::net::SocketAddr;

use kv_ctrl::NodeId;

use crate::state::ClusterState;

/// `-CLUSTERDOWN` text for a slot without a serving primary.
pub const CLUSTERDOWN_SLOT: &[u8] = b"CLUSTERDOWN Hash slot not served";
/// `-CLUSTERDOWN` text of a coordinator that has not heard from a leader recently (§23.3).
pub const CLUSTERDOWN_STATE: &[u8] = b"CLUSTERDOWN The cluster is down";
/// `-TRYAGAIN` text (Redis wording).
pub const TRYAGAIN: &[u8] = b"TRYAGAIN Multiple keys request during rehashing of slot";
/// `-CROSSSLOT` text.
pub const CROSSSLOT: &[u8] = b"CROSSSLOT Keys in request don't hash to the same slot";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Redirect {
    Moved(u16, SocketAddr),
    ClusterDown,
    TryAgain,
}

/// Outcome for a keyed command with one or more key slots.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum KeysRedirect {
    Redirect(Redirect),
    CrossSlot,
}

/// Where a coordinator sends a keyed command for `slot` (§23.5): MOVED to the slot's primary
/// client address while the shard is serving and its primary is neither FAIL-flagged nor
/// suspected by the leader (`now_pfail`), else CLUSTERDOWN. Out-of-range slots and unassigned
/// slots are CLUSTERDOWN.
pub fn for_slot(s: &ClusterState, slot: u16, now_pfail: &dyn Fn(&NodeId) -> bool) -> Redirect {
    let Some(sh) = s.shard_of_slot(slot) else {
        return Redirect::ClusterDown;
    };
    if !s.shard_serving(sh) {
        return Redirect::ClusterDown;
    }
    match sh.primary.and_then(|p| s.node(&p)) {
        Some(n) if !now_pfail(&n.id) => Redirect::Moved(slot, n.client),
        _ => Redirect::ClusterDown,
    }
}

/// [`for_slot`] for the key slots of one command: `None` for a keyless command, `CrossSlot`
/// when the keys hash to different slots.
pub fn for_key_slots(
    s: &ClusterState,
    slots: &[u16],
    now_pfail: &dyn Fn(&NodeId) -> bool,
) -> Option<KeysRedirect> {
    let (&first, rest) = slots.split_first()?;
    if rest.iter().any(|&x| x != first) {
        return Some(KeysRedirect::CrossSlot);
    }
    Some(KeysRedirect::Redirect(for_slot(s, first, now_pfail)))
}

/// `MOVED <slot> <ip>:<port>` (no brackets around IPv6 addresses, as Redis prints them).
pub fn moved_text(slot: u16, to: SocketAddr) -> Vec<u8> {
    let mut s = String::with_capacity(40);
    // Writing to a String cannot fail.
    let _ = write!(s, "MOVED {slot} {}:{}", to.ip(), to.port());
    s.into_bytes()
}

impl Redirect {
    /// The error text without the leading `-` and the trailing CRLF.
    pub fn error_text(&self) -> Vec<u8> {
        match *self {
            Redirect::Moved(slot, to) => moved_text(slot, to),
            Redirect::ClusterDown => CLUSTERDOWN_SLOT.to_vec(),
            Redirect::TryAgain => TRYAGAIN.to_vec(),
        }
    }

    /// Writes the error reply.
    pub fn write(&self, w: &mut kv_resp::ReplyWriter<'_>) {
        w.error(&self.error_text());
    }
}

impl KeysRedirect {
    pub fn error_text(&self) -> Vec<u8> {
        match self {
            KeysRedirect::Redirect(r) => r.error_text(),
            KeysRedirect::CrossSlot => CROSSSLOT.to_vec(),
        }
    }

    pub fn write(&self, w: &mut kv_resp::ReplyWriter<'_>) {
        w.error(&self.error_text());
    }
}
