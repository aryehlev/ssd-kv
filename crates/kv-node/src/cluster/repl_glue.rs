//! Replication streams of the node's partitions (DESIGN §23.7, §23.8).
//!
//! Primary: every executed command's group is encoded into the partition's `ReplSender` and
//! shipped to connected members in parallel with the page writes; replies wait for the shard
//! commit (`commit_seq >= seq`) and a valid lease. Member: each partition dials the primary's
//! replication port with `Hello`; the accepting worker hands the socket to the worker owning the
//! partition (`XMsg::ReplSocket`); frames become partition requests with `Origin::Replicated`,
//! `Image` or `Snapshot`; acks report the durable prefix. Replication positions are persisted to
//! partition meta every 50 ms. Streams read their sockets under the same pre-decode checks as
//! clients (§6.7).

use kv_repl::member::MemberStream;
use kv_repl::primary::ReplSender;

/// The replication endpoint of one partition on this node.
pub enum PartitionStream {
    Primary(ReplSender),
    Member(MemberStream),
}
