//! The static command table (DESIGN §8.1-§8.7, §24.5).
//!
//! One static array of [`CommandSpec`]s; a 512-entry open-addressed index over the uppercase
//! names is built once at startup (no perfect-hash crate). Lookups are ASCII case-insensitive.
//! Commands that kvssd does not implement are simply absent and get Redis's
//! `-ERR unknown command` reply; in particular the table never contains scripting, function,
//! sort or module commands (LC-17).

use crate::spec::CommandSpec;

/// Looks up a top-level command by name (ASCII case-insensitive).
pub fn lookup(name: &[u8]) -> Option<&'static CommandSpec> {
    todo!("DESIGN §8.1: command lookup")
}

/// Looks up a subcommand of a container command (CLIENT, COMMAND, CONFIG, CLUSTER).
pub fn lookup_sub(spec: &'static CommandSpec, sub: &[u8]) -> Option<&'static CommandSpec> {
    todo!("DESIGN §8.1: subcommand lookup")
}

/// Every top-level command, in table order.
pub fn all() -> &'static [CommandSpec] {
    todo!("DESIGN §8: the command table")
}
