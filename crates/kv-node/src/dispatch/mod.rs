//! Command dispatch (DESIGN §6.2, §6.5, §6.8, §23.6).
//!
//! * [`router`]: command lookup, arity, auth, MULTI queueing, the cluster routing gate and
//!   `slot -> partition -> worker` routing (local submit or `XMsg::Cmd`).
//! * [`multikey`]: standalone fan-out of MGET/EXISTS/DEL/UNLINK/TOUCH across partitions
//!   (exactly one sub-request per distinct partition, LC-37) and the gather of their replies.
//! * [`xtxn`]: standalone cross-partition transactions (MSET, MSETNX, RENAME, RENAMENX, COPY,
//!   EXEC spanning partitions) under the node-global conflict gate.
//!
//! Dispatch never parks a parsed command because of load and uses no clocks (LC-14).

pub mod multikey;
pub mod router;
pub mod xtxn;
