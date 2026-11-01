//! Connection state (DESIGN §7.1, §7.4-§7.6, §6.7).
//!
//! `read_buf` starts at 16 KiB, grows to fit the current command and shrinks back when idle;
//! `out` plus shared iovec bodies for values >= 64 KiB, one sendmsg per tick. Every parsed
//! command gets a `req_seq`; its reply slot is `Ready(bytes)`, `Waiting{partition, ticket,
//! commit_seq}`, `Remote` or `Gather{n_parts, parts}`, and slots are released strictly in
//! `req_seq` order. A connection stops re-arming recv while any single limit of §6.7 holds and
//! resumes when it no longer holds. With `requirepass` set, commands other than AUTH, HELLO,
//! QUIT and RESET reply `-NOAUTH Authentication required.` until authenticated.

use kv_resp::Proto;

use crate::runtime::xmsg::OwnedCmd;

/// MULTI state: queued commands and whether a queue-time error occurred (EXECABORT).
pub struct MultiState {
    pub queued: Vec<OwnedCmd>,
    pub errored: bool,
}

/// A WATCHed key (exact bytes; no `Debug`, LC-32).
pub struct WatchKey {
    pub db: u8,
    pub key: Box<[u8]>,
}

/// Connection flags.
pub mod flags {
    pub const CLOSE_AFTER_REPLY: u32 = 1;
    pub const BACKPRESSURED: u32 = 2;
    pub const NO_EVICT: u32 = 4;
    pub const NO_TOUCH: u32 = 8;
    pub const WATCH_DIRTY: u32 = 16;
}

pub struct Conn {
    pub id: u64,
    pub fd: i32,
    pub proto: Proto,
    pub db: u8,
    pub name: Option<Box<[u8]>>,
    pub lib_name: Option<Box<[u8]>>,
    pub lib_ver: Option<Box<[u8]>>,
    pub authed: bool,
    pub readonly: bool,
    pub multi: Option<MultiState>,
    pub watch: Vec<WatchKey>,
    /// Boot-clock ns of creation / of the last command.
    pub created: u64,
    pub last_cmd: u64,
    pub flags: u32,
    pub read_buf: Vec<u8>,
    pub out: Vec<u8>,
    pub next_req_seq: u64,
}
