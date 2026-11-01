//! Cluster mode of a data node (DESIGN §23.4, §23.6-§23.13).
//!
//! * [`agent`]: the agent thread: coordinator control connection (the coordinator dials in),
//!   assignment application in `(coord_term, version)` order, lease renewals, `/metrics`.
//! * [`ownership`]: the routing gate built from the node's own assignment (no full slot map).
//! * [`lease`]: the lease expiry cell re-checked before each reply is released.
//! * [`repl_glue`]: per-partition replication streams wired to partitions and sockets.
//! * [`migrate_glue`]: migration source/destination endpoints wired to partitions.

pub mod agent;
pub mod lease;
pub mod migrate_glue;
pub mod ownership;
pub mod repl_glue;
