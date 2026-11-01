//! `kv-ctrl`: the coordinator <-> data node control channel (DESIGN §23.4, §24.9).
//!
//! The coordinator leader dials every registered node's control port; frames are
//! `u32 len || postcard((version, msg))`, at most 16 MiB ([`codec`]). Messages ([`msg`]): the
//! coordinator pushes assignments, lease grants, fences (to one node at a time), handoffs and
//! migration phases; nodes renew leases and report ISR, readiness and migration events. Replies
//! to coordinator requests carry identity, acknowledgements and positions only, never role,
//! shard, slot, key-count or term fields (LC-26). [`assignment`] is a node's projection of the
//! cluster state; no full slot map type exists here. Ids ([`ids`]) are not ordered (LC-27).
#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(unsafe_code)]

pub mod assignment;
pub mod codec;
pub mod ids;
pub mod msg;

pub use assignment::{
    CoordEndpoint, MemberState, MigPhase, MigrationTask, NodeAssignment, NodeParams, PeerAddr,
    ShardRole, SlotRanges, TermEnd,
};
pub use codec::{
    CodecError, MAX_FRAME, PROTO_VERSION, decode, decode_body, encode, frame_len, try_encode,
};
pub use ids::{CoordId, NodeId, ShardId};
pub use msg::{CoordToNode, Health, NodeError, NodeToCoord};
