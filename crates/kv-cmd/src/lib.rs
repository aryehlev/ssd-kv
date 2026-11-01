//! `kv-cmd`: the command table shared by `kvd` and `kvc` (DESIGN §8, §24.5).
//!
//! * [`spec`]: [`CommandSpec`], [`CmdId`], [`CmdFlags`], [`KeySpec`], [`Access`].
//! * [`table`]: the static table (one array, looked up through a 512-entry open-addressed
//!   index over uppercase names built at startup), [`lookup`], [`lookup_sub`], [`all`].
//! * [`keys`]: command resolution with Redis's error replies ([`resolve`]), key positions and
//!   per-key access modes, arity checks and the same-slot rule ([`common_slot`]), used by the
//!   data node's router and by the coordinator's MOVED computation; COMMAND GETKEYS.
//! * [`docs`]: COMMAND, COMMAND INFO / DOCS / LIST / COUNT replies (DOCS returns `summary`,
//!   `since`, `group`).
//!
//! The table contains no scripting, function, sort or module command (LC-17).
#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(unsafe_code)]

pub mod docs;
pub mod keys;
pub mod spec;
pub mod table;

pub use docs::{
    command_count, write_command_all, write_command_docs, write_command_docs_reply, write_command_info,
    write_command_info_reply, write_command_list_reply,
};
pub use keys::{
    CrossSlot, ResolveError, check_arity, common_slot, key_access, key_positions, resolve, write_arity_message,
    write_command_getkeys,
};
pub use spec::{Access, CmdFlags, CmdId, CommandSpec, KeySpec};
pub use table::{all, all_with_subcommands, by_id, lookup, lookup_fullname, lookup_sub};
