//! Client networking (DESIGN §6.7, §7.1, §7.5, §7.6).
//!
//! * [`listener`]: per-worker `SO_REUSEPORT` listeners, optional unix socket, `maxclients`.
//! * [`conn`]: connection state, read/write buffers, the reply reorder buffer.
//! * [`budget`]: the node-global large-value soft limit checked before a command is consumed.

pub mod budget;
pub mod conn;
pub mod listener;
