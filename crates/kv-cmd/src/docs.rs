//! COMMAND INFO and COMMAND DOCS replies (DESIGN §8.2).
//!
//! COMMAND INFO uses the Redis 7 reply shape (name, arity, flags, first key, last key, step,
//! ACL categories, tips, key specs, subcommands); COMMAND DOCS returns only `summary`, `since`
//! and `group` for each command, from the table.

use crate::spec::CommandSpec;

pub fn write_command_info(spec: &CommandSpec, w: &mut kv_resp::ReplyWriter<'_>) {
    todo!("DESIGN §8.2: COMMAND INFO")
}

pub fn write_command_docs(spec: &CommandSpec, w: &mut kv_resp::ReplyWriter<'_>) {
    todo!("DESIGN §8.2: COMMAND DOCS")
}
