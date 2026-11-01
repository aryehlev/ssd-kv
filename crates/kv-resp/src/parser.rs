//! Incremental server-side RESP parser (DESIGN §7.2, §6.7).
//!
//! Zero-copy: for one complete command it pushes argument `(offset, len)` pairs into the
//! caller's vector, pointing into the connection's `read_buf`, or reports `Incomplete`.
//! Multibulk (`*N\r\n$L\r\n...`) and inline commands (space separated, quotes per Redis
//! `sdssplitargs`, see [`crate::inline`]). `*0`, `*-1` and empty inline lines are skipped
//! (`Parsed::Empty`). Limits: `max_bulk` = `proto-max-bulk-len` (512 MiB), `max_args` = 1048576,
//! `max_inline` = 64 KiB. CRLF is located with `memchr`; integers use a hand-written fast path.
//!
//! [`Parser::in_progress`] describes a command that is still being received (its name once
//! parsed, its argument count and the length of the bulk being received) so the pre-consume
//! checks of §6.7 (large-value soft limit, txn credits) run before any byte is consumed.

use crate::error::ProtoError;

/// Parser limits (from `proto-max-bulk-len` and the fixed Redis limits).
#[derive(Clone, Copy)]
pub struct Limits {
    pub max_bulk: usize,
    pub max_args: usize,
    pub max_inline: usize,
}

/// Outcome of one [`Parser::parse`] call.
pub enum Parsed {
    /// One complete command; `consumed` bytes of `buf` belong to it.
    Command { consumed: usize },
    /// More bytes are needed; nothing was consumed.
    Incomplete,
    /// An empty command (`*0`, `*-1`, empty inline line) was skipped.
    Empty { consumed: usize },
}

/// State of a command still being received.
pub struct InProgress {
    /// `(offset, len)` of the command name once it has been received.
    pub name: Option<(u32, u32)>,
    pub argc: u32,
    /// Declared length of the bulk currently being received.
    pub bulk_len: Option<usize>,
}

/// Incremental parser state for one connection.
pub struct Parser {
    limits: Limits,
    /// Bytes of the current command already validated (resume point).
    scanned: usize,
    /// Declared argument count of the current multibulk command.
    argc: Option<u32>,
}

impl Parser {
    pub fn new(limits: Limits) -> Self {
        Parser { limits, scanned: 0, argc: None }
    }

    /// Parses at most one command from `buf`; argument (offset, len) pairs are pushed to `args`.
    pub fn parse(&mut self, buf: &[u8], args: &mut Vec<(u32, u32)>) -> Result<Parsed, ProtoError> {
        todo!("DESIGN §7.2: incremental RESP parse")
    }

    /// State of the command still being received (nothing consumed yet): used by the
    /// pre-consume checks of §6.7.
    pub fn in_progress(&self, buf: &[u8]) -> Option<InProgress> {
        todo!("DESIGN §7.2: in-progress command view")
    }
}
