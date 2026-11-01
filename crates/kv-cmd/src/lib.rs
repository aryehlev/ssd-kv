//! `kv-cmd`: the command table shared by `kvd` and `kvc` (DESIGN §8, §24.5).
//!
//! * [`spec`]: [`CommandSpec`], [`CmdId`], [`CmdFlags`], [`KeySpec`], [`Access`].
//! * [`table`]: the static table (one array, looked up through a 512-entry open-addressed
//!   index over uppercase names built at startup), [`lookup`], [`lookup_sub`], [`all`].
//! * [`keys`]: key positions, arity checks and the same-slot rule ([`common_slot`]), used by the
//!   data node's router and by the coordinator's MOVED computation.
//! * [`docs`]: COMMAND INFO / COMMAND DOCS replies (DOCS returns `summary`, `since`, `group`).
//!
//! The table contains no scripting, function, sort or module command (LC-17).
#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(unsafe_code)]
#![allow(dead_code, unused_variables)]

pub mod docs;
pub mod keys;
pub mod spec;
pub mod table;

pub use docs::{write_command_docs, write_command_info};
pub use keys::{CrossSlot, check_arity, common_slot, key_positions};
pub use spec::{Access, CmdFlags, CmdId, CommandSpec, KeySpec};
pub use table::{all, lookup, lookup_sub};
