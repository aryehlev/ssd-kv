//! Incremental server-side RESP parser (DESIGN §7.2, §6.7).
//!
//! Zero-copy: for one complete command it pushes argument `(offset, len)` pairs into the
//! caller's vector, pointing into the connection's `read_buf`, or reports `Incomplete`.
//! Multibulk (`*N\r\n$L\r\n...`) and inline commands (space separated, quotes per Redis
//! `sdssplitargs`, see [`crate::inline`]). `*0`, `*-1` (any count <= 0) and empty inline lines
//! are skipped (`Parsed::Empty`). Limits: `max_bulk` = `proto-max-bulk-len` (512 MiB),
//! `max_args` = the multibulk count limit, `max_inline` = 64 KiB (inline requests and multibulk /
//! bulk header lines). CRLF is located with `memchr`; integers use Redis `string2ll`.
//!
//! The rules are Redis 7.0.15's `processMultibulkBuffer` / `processInlineBuffer`, observable
//! quirks included:
//! * a header line ends at the first `\r` (the byte after it is skipped unchecked), and the two
//!   bytes after a bulk's data are skipped unchecked;
//! * a header or inline line is searched with C `strchr`, so a NUL byte before its terminator
//!   makes the line "not yet complete" until more than 64 KiB are buffered, then a too-big
//!   error is returned;
//! * the too-big errors (`too big inline request`, `too big mbulk count string`, `too big bulk
//!   count string`) are returned when more than `max_inline` bytes are buffered from the start of
//!   a line that has no terminator yet, so (as in Redis) a long line that arrives at once is
//!   accepted;
//! * `-ERR Protocol error: expected '$', got 'X'` is reported only once the offending header
//!   line is complete;
//! * with [`Parser::set_auth_required`], a multibulk count above 10 or a bulk longer than 16384
//!   bytes is refused (`unauthenticated ... length`).
//!
//! Incremental contract: after `Incomplete`, nothing is consumed; the next call must pass a
//! buffer starting with the same bytes (more may be appended). The parser remembers how far it
//! got, so a large command arriving in many reads is scanned once. Arguments are returned only
//! with `Parsed::Command`.
//!
//! Inline arguments containing quotes or escapes have no contiguous image in the buffer: they
//! are unescaped into a side buffer and [`Parser::arg_source`] names the bytes the pairs refer
//! to. [`Parser::parse_in_place`] instead writes the unescaped arguments back over the line, so
//! the pairs always refer to the caller's buffer.
//!
//! [`Parser::in_progress`] describes a command that is still being received (its name once
//! parsed, its argument count and the length of the bulk being received) so the pre-consume
//! checks of §6.7 (large-value soft limit, txn credits) run before any byte is consumed.

use kv_common::num::string2ll;

use crate::error::ProtoError;
use crate::inline::split_inline;

/// Parser limits (from `proto-max-bulk-len` and the fixed Redis limits).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Limits {
    pub max_bulk: usize,
    pub max_args: usize,
    pub max_inline: usize,
}

impl Limits {
    /// `proto-max-bulk-len`'s default.
    pub const DEFAULT_MAX_BULK: usize = 512 * 1024 * 1024;
    /// The multibulk count limit of DESIGN §7.2 (Redis 7.0.15 itself accepts up to `i32::MAX`).
    pub const DEFAULT_MAX_ARGS: usize = 1024 * 1024;
    /// Redis `PROTO_INLINE_MAX_SIZE`.
    pub const DEFAULT_MAX_INLINE: usize = 64 * 1024;
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_bulk: Self::DEFAULT_MAX_BULK,
            max_args: Self::DEFAULT_MAX_ARGS,
            max_inline: Self::DEFAULT_MAX_INLINE,
        }
    }
}

/// Outcome of one [`Parser::parse`] call.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Parsed {
    /// One complete command; `consumed` bytes of `buf` belong to it.
    Command { consumed: usize },
    /// More bytes are needed; nothing was consumed.
    Incomplete,
    /// An empty command (`*0`, `*-1`, empty inline line) was skipped.
    Empty { consumed: usize },
}

/// State of a command still being received.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct InProgress {
    /// `(offset, len)` of the command name once it has been received.
    pub name: Option<(u32, u32)>,
    /// Declared argument count (0 while the multibulk count line, or an inline line, is still
    /// being received).
    pub argc: u32,
    /// Declared length of the bulk currently being received.
    pub bulk_len: Option<usize>,
}

/// Unauthenticated limits of Redis 7.0 (`authRequired`).
const UNAUTH_MAX_ARGS: i64 = 10;
const UNAUTH_MAX_BULK: i64 = 16384;

/// Progress through a multibulk command.
#[derive(Clone, Copy, Debug)]
struct Mb {
    /// Declared argument count (> 0).
    argc: u32,
    /// Arguments fully received.
    done: u32,
    /// Next unparsed byte: a bulk header, or the data of `bulk`.
    pos: usize,
    /// Declared length of the bulk whose header has been parsed (its data starts at `pos`).
    bulk: Option<usize>,
}

#[derive(Clone, Copy, Debug)]
enum State {
    Idle,
    Multibulk(Mb),
    /// An inline line without terminator: `scanned` bytes hold no `\n`; `nul` when a NUL byte
    /// was met (the line can then never complete, as with C `strchr`).
    Inline {
        scanned: usize,
        nul: bool,
    },
}

/// Incremental parser state for one connection.
pub struct Parser {
    limits: Limits,
    auth_required: bool,
    state: State,
    /// Arguments of the multibulk command in progress (moved out of the caller's vector on
    /// `Incomplete`, moved back when the command completes).
    pending: Vec<(u32, u32)>,
    /// Unescaped arguments of the last inline command that needed it.
    side: Vec<u8>,
    /// The pairs of the last `Command` refer to `side`.
    args_in_side: bool,
}

/// C `strchr(buf + from, b)` over a NUL-terminated buffer: the index of the first `b` at or after
/// `from`, or `None` if a NUL byte or the end comes first.
#[inline]
fn find_terminated(buf: &[u8], from: usize, b: u8) -> Option<usize> {
    let rest = buf.get(from..)?;
    // header lines are short: scan the first bytes inline before calling into memchr
    for (i, &c) in rest.iter().take(16).enumerate() {
        if c == b {
            return Some(from + i);
        }
        if c == 0 {
            return None;
        }
    }
    if rest.len() <= 16 {
        return None;
    }
    match memchr::memchr2(b, 0, &rest[16..]) {
        Some(i) if rest[16 + i] == b => Some(from + 16 + i),
        _ => None,
    }
}

/// Advances `mb` over complete bulks of `buf`, calling `sink(offset, len)` for each argument.
/// `Ok(true)` when the command is complete (`mb.pos` = bytes consumed), `Ok(false)` when more
/// bytes are needed.
#[inline]
fn scan_bulks(
    lim: &Limits,
    auth_required: bool,
    mb: &mut Mb,
    buf: &[u8],
    sink: &mut impl FnMut(u32, u32),
) -> Result<bool, ProtoError> {
    while mb.done < mb.argc {
        let len = match mb.bulk {
            Some(len) => len,
            None => {
                let pos = mb.pos;
                if pos >= buf.len() {
                    return Ok(false);
                }
                let Some(cr) = find_terminated(buf, pos, b'\r') else {
                    if buf.len() - pos > lim.max_inline {
                        return Err(ProtoError::TooBigBulkCount);
                    }
                    return Ok(false);
                };
                if cr + 1 >= buf.len() {
                    return Ok(false); // the byte after `\r` has not arrived
                }
                if buf[pos] != b'$' {
                    return Err(ProtoError::ExpectedDollar(buf[pos]));
                }
                let n = match string2ll(&buf[pos + 1..cr]) {
                    Some(n) if n >= 0 && (n as u64) <= lim.max_bulk as u64 => n,
                    _ => return Err(ProtoError::InvalidBulkLen),
                };
                if auth_required && n > UNAUTH_MAX_BULK {
                    return Err(ProtoError::UnauthenticatedBulkLen);
                }
                let data = cr + 2;
                // argument pairs are u32: a command may not reach past 4 GiB
                if data as u64 + n as u64 > u64::from(u32::MAX) {
                    return Err(ProtoError::InvalidBulkLen);
                }
                mb.pos = data;
                mb.bulk = Some(n as usize);
                n as usize
            }
        };
        if buf.len() - mb.pos < len + 2 {
            return Ok(false);
        }
        sink(mb.pos as u32, len as u32);
        mb.pos += len + 2;
        mb.done += 1;
        mb.bulk = None;
    }
    Ok(true)
}

/// Header of a multibulk command at `buf[0] == b'*'`.
enum Header {
    Incomplete,
    /// Count <= 0: an empty command of `consumed` bytes.
    Empty(usize),
    Start(Mb),
}

#[inline]
fn parse_header(lim: &Limits, auth_required: bool, buf: &[u8]) -> Result<Header, ProtoError> {
    let Some(cr) = find_terminated(buf, 1, b'\r') else {
        if buf.len() > lim.max_inline {
            return Err(ProtoError::TooBigMultibulkCount);
        }
        return Ok(Header::Incomplete);
    };
    if cr + 1 >= buf.len() {
        return Ok(Header::Incomplete);
    }
    let max = (lim.max_args as u64).min(i32::MAX as u64);
    let n = match string2ll(&buf[1..cr]) {
        Some(n) if n <= 0 || (n as u64) <= max => n,
        _ => return Err(ProtoError::InvalidMultibulkLen),
    };
    if auth_required && n > UNAUTH_MAX_ARGS {
        return Err(ProtoError::UnauthenticatedMultibulkLen);
    }
    if n <= 0 {
        return Ok(Header::Empty(cr + 2));
    }
    Ok(Header::Start(Mb {
        argc: n as u32,
        done: 0,
        pos: cr + 2,
        bulk: None,
    }))
}

impl Parser {
    pub fn new(limits: Limits) -> Self {
        Parser {
            limits,
            auth_required: false,
            state: State::Idle,
            pending: Vec::new(),
            side: Vec::new(),
            args_in_side: false,
        }
    }

    /// The limits in force.
    pub fn limits(&self) -> Limits {
        self.limits
    }

    /// Replaces the limits (CONFIG SET proto-max-bulk-len); applies from the next header parsed.
    pub fn set_limits(&mut self, limits: Limits) {
        self.limits = limits;
    }

    /// Applies Redis's unauthenticated request limits (multibulk count <= 10, bulk length <=
    /// 16384) while a password is required and the connection has not authenticated.
    pub fn set_auth_required(&mut self, required: bool) {
        self.auth_required = required;
    }

    /// Forgets any partially received command (connection reset or buffer discarded).
    pub fn reset(&mut self) {
        self.state = State::Idle;
        self.pending.clear();
        self.args_in_side = false;
    }

    /// Parses at most one command from `buf`; argument (offset, len) pairs are pushed to `args`.
    ///
    /// The pairs of an inline command whose arguments needed unescaping refer to the parser's
    /// side buffer, not to `buf`: read arguments through [`Parser::arg_source`], or use
    /// [`Parser::parse_in_place`].
    pub fn parse(&mut self, buf: &[u8], args: &mut Vec<(u32, u32)>) -> Result<Parsed, ProtoError> {
        self.args_in_side = false;
        let base = args.len();
        let r = self.parse_inner(buf, args);
        if r.is_err() {
            // arguments are only ever returned with a complete command
            args.truncate(base);
            self.reset();
        }
        r
    }

    /// As [`Parser::parse`], but the arguments of an inline command with quotes or escapes are
    /// written back over the start of its line in `buf` (unescaping never lengthens a line), so
    /// the pairs always refer to `buf`.
    pub fn parse_in_place(
        &mut self,
        buf: &mut [u8],
        args: &mut Vec<(u32, u32)>,
    ) -> Result<Parsed, ProtoError> {
        let r = self.parse(buf, args)?;
        if self.args_in_side {
            buf[..self.side.len()].copy_from_slice(&self.side);
            self.args_in_side = false;
        }
        Ok(r)
    }

    /// The bytes the argument pairs of the last `Parsed::Command` refer to: `buf` itself, or the
    /// parser's side buffer for an inline command whose arguments needed unescaping. Valid
    /// until the next call to `parse`.
    #[inline]
    pub fn arg_source<'a>(&'a self, buf: &'a [u8]) -> &'a [u8] {
        if self.args_in_side { &self.side } else { buf }
    }

    fn parse_inner(
        &mut self,
        buf: &[u8],
        args: &mut Vec<(u32, u32)>,
    ) -> Result<Parsed, ProtoError> {
        let mut mb = match self.state {
            State::Idle => {
                let Some(&first) = buf.first() else {
                    return Ok(Parsed::Incomplete);
                };
                if first != b'*' {
                    return self.parse_inline(buf, 0, false, args);
                }
                match parse_header(&self.limits, self.auth_required, buf)? {
                    Header::Incomplete => return Ok(Parsed::Incomplete),
                    Header::Empty(consumed) => return Ok(Parsed::Empty { consumed }),
                    Header::Start(mb) => mb,
                }
            }
            State::Inline { scanned, nul } => return self.parse_inline(buf, scanned, nul, args),
            State::Multibulk(mb) => {
                if mb.pos > buf.len() {
                    // contract violation: the buffer no longer holds the command's start
                    debug_assert!(false, "parser resumed with a shorter buffer");
                    self.reset();
                    return self.parse_inner(buf, args);
                }
                args.append(&mut self.pending);
                mb
            }
        };
        let base = args.len() - mb.done as usize;
        if scan_bulks(
            &self.limits,
            self.auth_required,
            &mut mb,
            buf,
            &mut |o, l| args.push((o, l)),
        )? {
            self.state = State::Idle;
            Ok(Parsed::Command { consumed: mb.pos })
        } else {
            self.pending.extend(args.drain(base..));
            self.state = State::Multibulk(mb);
            Ok(Parsed::Incomplete)
        }
    }

    fn parse_inline(
        &mut self,
        buf: &[u8],
        scanned: usize,
        nul: bool,
        args: &mut Vec<(u32, u32)>,
    ) -> Result<Parsed, ProtoError> {
        let found = if nul {
            None
        } else {
            memchr::memchr2(b'\n', 0, &buf[scanned.min(buf.len())..]).map(|i| scanned + i)
        };
        let nl = match found {
            Some(i) if buf[i] == b'\n' => i,
            other => {
                if buf.len() > self.limits.max_inline {
                    self.state = State::Idle;
                    return Err(ProtoError::TooBigInline);
                }
                self.state = match other {
                    Some(z) => State::Inline {
                        scanned: z,
                        nul: true,
                    },
                    None => State::Inline {
                        scanned: buf.len(),
                        nul,
                    },
                };
                return Ok(Parsed::Incomplete);
            }
        };
        self.state = State::Idle;
        let line_end = if nl > 0 && buf[nl - 1] == b'\r' {
            nl - 1
        } else {
            nl
        };
        let consumed = nl + 1;
        if consumed > u32::MAX as usize {
            return Err(ProtoError::TooBigInline);
        }
        let base = args.len();
        self.args_in_side = split_inline(&buf[..line_end], &mut self.side, args)?;
        if args.len() == base {
            self.args_in_side = false;
            Ok(Parsed::Empty { consumed })
        } else {
            Ok(Parsed::Command { consumed })
        }
    }

    /// State of the command still being received (nothing consumed yet): used by the
    /// pre-consume checks of §6.7. Call it after `parse` returned `Incomplete`; it also accounts
    /// for bytes appended since. `None` when `buf` is empty, holds a complete (or empty) command,
    /// or starts with a protocol error (which `parse` reports).
    pub fn in_progress(&self, buf: &[u8]) -> Option<InProgress> {
        let incomplete = InProgress {
            name: None,
            argc: 0,
            bulk_len: None,
        };
        let (mut mb, mut name) = match self.state {
            State::Idle => {
                let &first = buf.first()?;
                if first != b'*' {
                    return match memchr::memchr2(b'\n', 0, buf) {
                        Some(i) if buf[i] == b'\n' => None,
                        _ => Some(incomplete),
                    };
                }
                match parse_header(&self.limits, self.auth_required, buf).ok()? {
                    Header::Incomplete => return Some(incomplete),
                    Header::Empty(_) => return None,
                    Header::Start(mb) => (mb, None),
                }
            }
            State::Inline { .. } => return Some(incomplete),
            State::Multibulk(mb) => (mb, self.pending.first().copied()),
        };
        if mb.pos > buf.len() {
            return None;
        }
        let complete = scan_bulks(
            &self.limits,
            self.auth_required,
            &mut mb,
            buf,
            &mut |o, l| {
                if name.is_none() {
                    name = Some((o, l));
                }
            },
        )
        .ok()?;
        if complete {
            return None;
        }
        Some(InProgress {
            name,
            argc: mb.argc,
            bulk_len: mb.bulk,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn lim() -> Limits {
        Limits::default()
    }

    /// Parses every command of `buf` at once: (args, consumed) per command, or the error.
    fn parse_all(buf: &[u8]) -> Result<Vec<Vec<Vec<u8>>>, ProtoError> {
        let mut p = Parser::new(lim());
        let mut off = 0;
        let mut out = Vec::new();
        loop {
            let mut args = Vec::new();
            match p.parse(&buf[off..], &mut args)? {
                Parsed::Command { consumed } => {
                    let src = p.arg_source(&buf[off..]);
                    out.push(
                        args.iter()
                            .map(|&(o, l)| src[o as usize..(o + l) as usize].to_vec())
                            .collect(),
                    );
                    off += consumed;
                }
                Parsed::Empty { consumed } => off += consumed,
                Parsed::Incomplete => return Ok(out),
            }
        }
    }

    fn one(buf: &[u8]) -> Result<Parsed, ProtoError> {
        Parser::new(lim()).parse(buf, &mut Vec::new())
    }

    fn cmd(words: &[&[u8]]) -> Vec<Vec<u8>> {
        words.iter().map(|w| w.to_vec()).collect()
    }

    #[test]
    fn multibulk_basic() {
        let b = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nvalue\r\n*1\r\n$4\r\nPING\r\n";
        assert_eq!(
            parse_all(b).unwrap(),
            vec![cmd(&[b"SET", b"k", b"value"]), cmd(&[b"PING"])]
        );
        let mut args = Vec::new();
        assert_eq!(
            Parser::new(lim()).parse(b, &mut args).unwrap(),
            Parsed::Command { consumed: 31 }
        );
        assert_eq!(args, vec![(8, 3), (17, 1), (24, 5)]);
        // binary-safe bulks
        assert_eq!(
            parse_all(b"*2\r\n$4\r\nECHO\r\n$4\r\n\r\n\x00\n\r\n").unwrap(),
            vec![cmd(&[b"ECHO", b"\r\n\x00\n"])]
        );
        assert_eq!(
            parse_all(b"*2\r\n$4\r\nECHO\r\n$0\r\n\r\n").unwrap(),
            vec![cmd(&[b"ECHO", b""])]
        );
    }

    #[test]
    fn redis_quirks() {
        // the byte after a header's `\r`, and the two bytes after a bulk, are not checked
        assert_eq!(
            parse_all(b"*1\rX$4\r\nPING\r\n").unwrap(),
            vec![cmd(&[b"PING"])]
        );
        assert_eq!(
            parse_all(b"*1\r\n$4\r\nPINGxx").unwrap(),
            vec![cmd(&[b"PING"])]
        );
        // counts <= 0 are empty commands
        assert_eq!(one(b"*0\r\n"), Ok(Parsed::Empty { consumed: 4 }));
        assert_eq!(one(b"*-1\r\n"), Ok(Parsed::Empty { consumed: 5 }));
        assert_eq!(
            parse_all(b"*-5\r\n*0\r\nPING\r\n").unwrap(),
            vec![cmd(&[b"PING"])]
        );
        // an inline command may follow a multibulk one and vice versa
        assert_eq!(
            parse_all(b"PING\r\n*1\r\n$4\r\nPING\r\n").unwrap(),
            vec![cmd(&[b"PING"]), cmd(&[b"PING"])]
        );
        // `$` is checked only once the header line is complete
        assert_eq!(one(b"*1\r\nPING"), Ok(Parsed::Incomplete));
        assert_eq!(one(b"*1\r\nPING\r"), Ok(Parsed::Incomplete));
        assert_eq!(
            one(b"*1\r\nPING\r\n"),
            Err(ProtoError::ExpectedDollar(b'P'))
        );
        assert_eq!(one(b"*1\r\n\r\n"), Err(ProtoError::ExpectedDollar(b'\r')));
        // NUL before a terminator: never complete, until too big
        assert_eq!(one(b"*1\x00\r\n$4\r\nPING\r\n"), Ok(Parsed::Incomplete));
        assert_eq!(one(b"PING a\x00b\r\nPING\r\n"), Ok(Parsed::Incomplete));
        let mut big = b"PING \x00".to_vec();
        big.resize(70_000, b'\n');
        assert_eq!(one(&big), Err(ProtoError::TooBigInline));
    }

    #[test]
    fn protocol_errors() {
        assert_eq!(one(b"*abc\r\n"), Err(ProtoError::InvalidMultibulkLen));
        assert_eq!(one(b"*\r\n"), Err(ProtoError::InvalidMultibulkLen));
        assert_eq!(one(b"*+1\r\n"), Err(ProtoError::InvalidMultibulkLen));
        assert_eq!(one(b"*01\r\n"), Err(ProtoError::InvalidMultibulkLen));
        assert_eq!(one(b"* 1\r\n"), Err(ProtoError::InvalidMultibulkLen));
        assert_eq!(one(b"*-0\r\n"), Err(ProtoError::InvalidMultibulkLen));
        assert_eq!(one(b"*1048577\r\n"), Err(ProtoError::InvalidMultibulkLen));
        assert_eq!(one(b"*1048576\r\n"), Ok(Parsed::Incomplete));
        let redis = Limits {
            max_args: usize::MAX,
            ..lim()
        };
        let p = |b: &[u8]| Parser::new(redis).parse(b, &mut Vec::new());
        assert_eq!(p(b"*2147483647\r\n"), Ok(Parsed::Incomplete));
        assert_eq!(p(b"*2147483648\r\n"), Err(ProtoError::InvalidMultibulkLen));
        assert_eq!(one(b"*1\r\n$-1\r\n"), Err(ProtoError::InvalidBulkLen));
        assert_eq!(one(b"*1\r\n$-0\r\n"), Err(ProtoError::InvalidBulkLen));
        assert_eq!(one(b"*1\r\n$abc\r\n"), Err(ProtoError::InvalidBulkLen));
        assert_eq!(one(b"*1\r\n$+4\r\n"), Err(ProtoError::InvalidBulkLen));
        assert_eq!(one(b"*1\r\n$ 4\r\n"), Err(ProtoError::InvalidBulkLen));
        assert_eq!(
            one(b"*1\r\n$536870913\r\n"),
            Err(ProtoError::InvalidBulkLen)
        );
        assert_eq!(one(b"*1\r\n$536870912\r\n"), Ok(Parsed::Incomplete));
        assert_eq!(one(b"PING \"abc\r\n"), Err(ProtoError::UnbalancedQuotes));
        // too-big header lines: more than 64 KiB buffered without a terminator
        let mut b = b"*".to_vec();
        b.resize(65_536, b'1');
        assert_eq!(one(&b), Ok(Parsed::Incomplete));
        b.push(b'1');
        assert_eq!(one(&b), Err(ProtoError::TooBigMultibulkCount));
        b.extend_from_slice(b"\r\n");
        assert_eq!(
            one(&b),
            Err(ProtoError::InvalidMultibulkLen),
            "a complete line is parsed as Redis does"
        );
        let mut b = b"*1\r\n$".to_vec();
        b.resize(70_000, b'1');
        assert_eq!(one(&b), Err(ProtoError::TooBigBulkCount));
        let mut b = vec![b'A'; 65_536];
        assert_eq!(one(&b), Ok(Parsed::Incomplete));
        b.push(b'A');
        assert_eq!(one(&b), Err(ProtoError::TooBigInline));
        // a long inline line that arrives at once is accepted (Redis does the same)
        let mut b = b"ECHO ".to_vec();
        b.resize(70_005, b'A');
        b.extend_from_slice(b"\r\n");
        assert_eq!(parse_all(&b).unwrap()[0][1].len(), 70_000);
    }

    #[test]
    fn unauthenticated_limits() {
        let mut p = Parser::new(lim());
        p.set_auth_required(true);
        assert_eq!(p.parse(b"*10\r\n", &mut Vec::new()), Ok(Parsed::Incomplete));
        p.reset();
        assert_eq!(
            p.parse(b"*11\r\n", &mut Vec::new()),
            Err(ProtoError::UnauthenticatedMultibulkLen)
        );
        assert_eq!(
            p.parse(b"*1\r\n$16384\r\n", &mut Vec::new()),
            Ok(Parsed::Incomplete)
        );
        p.reset();
        assert_eq!(
            p.parse(b"*1\r\n$16385\r\n", &mut Vec::new()),
            Err(ProtoError::UnauthenticatedBulkLen)
        );
        let mut big = b"GET ".to_vec();
        big.resize(20_004, b'x');
        big.extend_from_slice(b"\r\n");
        assert!(
            matches!(p.parse(&big, &mut Vec::new()), Ok(Parsed::Command { .. })),
            "inline is not limited"
        );
        p.set_auth_required(false);
        assert_eq!(p.parse(b"*11\r\n", &mut Vec::new()), Ok(Parsed::Incomplete));
    }

    #[test]
    fn inline_commands() {
        assert_eq!(parse_all(b"PING\r\n").unwrap(), vec![cmd(&[b"PING"])]);
        assert_eq!(
            parse_all(b"PING\nPING\n").unwrap(),
            vec![cmd(&[b"PING"]), cmd(&[b"PING"])]
        );
        assert_eq!(
            parse_all(b"SET inline value\r\nGET inline\r\n").unwrap(),
            vec![
                cmd(&[b"SET", b"inline", b"value"]),
                cmd(&[b"GET", b"inline"])
            ]
        );
        assert_eq!(
            parse_all(b"\r\n\n  \r\nPING\r\n").unwrap(),
            vec![cmd(&[b"PING"])]
        );
        assert_eq!(one(b"\r\n"), Ok(Parsed::Empty { consumed: 2 }));
        assert_eq!(one(b"   \n"), Ok(Parsed::Empty { consumed: 4 }));
        assert_eq!(one(b"PING"), Ok(Parsed::Incomplete));
        assert_eq!(
            parse_all(b"$4\r\n").unwrap(),
            vec![cmd(&[b"$4"])],
            "only `*` starts a multibulk"
        );
        assert_eq!(
            parse_all(b"ECHO \"a b\" 'c'\r\n").unwrap(),
            vec![cmd(&[b"ECHO", b"a b", b"c"])]
        );
        assert_eq!(
            parse_all(b"ECHO \"a\\x41\\n\"\r\n").unwrap(),
            vec![cmd(&[b"ECHO", b"aA\n"])]
        );
    }

    #[test]
    fn in_place_unescaping() {
        let mut buf = b"ECHO \"a\\x41\\n\" x\r\nPING\r\n".to_vec();
        let mut p = Parser::new(lim());
        let mut args = Vec::new();
        let Parsed::Command { consumed } = p.parse_in_place(&mut buf, &mut args).unwrap() else {
            panic!()
        };
        assert_eq!(consumed, 18);
        let words: Vec<&[u8]> = args
            .iter()
            .map(|&(o, l)| &buf[o as usize..(o + l) as usize])
            .collect();
        assert_eq!(words, vec![&b"ECHO"[..], b"aA\n", b"x"]);
        assert_eq!(p.arg_source(&buf).as_ptr(), buf.as_ptr());
        // the next command is untouched
        assert_eq!(&buf[consumed..], b"PING\r\n");
    }

    #[test]
    fn resumes_without_rescanning() {
        let full = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$10\r\n0123456789\r\n";
        let mut p = Parser::new(lim());
        let mut args = vec![(99, 99)]; // the caller's earlier entries are kept
        for cut in [3, 9, 12, 20, 30] {
            assert_eq!(
                p.parse(&full[..cut], &mut args).unwrap(),
                Parsed::Incomplete,
                "cut {cut}"
            );
            assert_eq!(args, vec![(99, 99)], "no partial arguments are returned");
        }
        assert_eq!(
            p.parse(full, &mut args).unwrap(),
            Parsed::Command {
                consumed: full.len()
            }
        );
        assert_eq!(args, vec![(99, 99), (8, 3), (17, 1), (25, 10)]);
    }

    #[test]
    fn in_progress_view() {
        let full = b"*3\r\n$4\r\nMSET\r\n$1\r\nk\r\n$5000\r\n";
        let mut p = Parser::new(lim());
        assert_eq!(p.in_progress(b""), None);
        assert_eq!(
            p.in_progress(b"*3"),
            Some(InProgress {
                name: None,
                argc: 0,
                bulk_len: None
            })
        );
        assert_eq!(
            p.in_progress(b"*3\r\n$4\r\nMS"),
            Some(InProgress {
                name: None,
                argc: 3,
                bulk_len: Some(4)
            })
        );
        assert_eq!(p.parse(full, &mut Vec::new()).unwrap(), Parsed::Incomplete);
        assert_eq!(
            p.in_progress(full),
            Some(InProgress {
                name: Some((8, 4)),
                argc: 3,
                bulk_len: Some(5000)
            })
        );
        // bytes appended after the last parse are taken into account
        let mut more = full.to_vec();
        more.extend_from_slice(&[b'v'; 5000]);
        more.extend_from_slice(b"\r\n");
        assert_eq!(p.in_progress(&more), None, "complete");
        assert_eq!(
            p.in_progress(&more[..full.len() + 10]).unwrap().bulk_len,
            Some(5000)
        );
        let p = Parser::new(lim());
        assert_eq!(
            p.in_progress(b"GET k"),
            Some(InProgress {
                name: None,
                argc: 0,
                bulk_len: None
            })
        );
        assert_eq!(p.in_progress(b"GET k\r\n"), None);
        assert_eq!(p.in_progress(b"*0\r\n"), None);
        assert_eq!(p.in_progress(b"*x\r\n"), None);
    }

    fn arb_arg() -> impl Strategy<Value = Vec<u8>> {
        prop_oneof![
            proptest::collection::vec(any::<u8>(), 0..40),
            Just(b"\r\n".to_vec()),
            Just(Vec::new()),
            proptest::collection::vec(any::<u8>(), 200..600),
        ]
    }

    fn arb_stream() -> impl Strategy<Value = Vec<u8>> {
        let multibulk = proptest::collection::vec(arb_arg(), 1..6).prop_map(|args| {
            let mut out = Vec::new();
            let refs: Vec<&[u8]> = args.iter().map(|a| &a[..]).collect();
            crate::encode_command(&refs, &mut out);
            out
        });
        let inline =
            proptest::collection::vec("[a-z0-9]{1,6}|\"[a-z \\\\]{0,6}\"|'[a-z ]{0,4}'", 0..4)
                .prop_map(|words| format!("{}\r\n", words.join(" ")).into_bytes());
        let empty = prop_oneof![
            Just(b"*0\r\n".to_vec()),
            Just(b"*-1\r\n".to_vec()),
            Just(b"\n".to_vec())
        ];
        let garbage = prop_oneof![
            Just(b"*x\r\n".to_vec()),
            Just(b"*1\r\nPING\r\n".to_vec()),
            Just(b"*1\r\n$-3\r\n".to_vec()),
            Just(b"ECHO \"abc\r\n".to_vec()),
        ];
        proptest::collection::vec(
            prop_oneof![8 => multibulk, 3 => inline, 1 => empty, 1 => garbage],
            1..8,
        )
        .prop_map(|parts| parts.concat())
    }

    /// Feeds `buf` in pieces ending at `cuts`, as a connection would, collecting every command.
    fn parse_split(buf: &[u8], cuts: &[usize]) -> Result<Vec<Vec<Vec<u8>>>, ProtoError> {
        let mut p = Parser::new(lim());
        let mut out = Vec::new();
        let mut start = 0; // start of the unconsumed bytes
        let mut ends: Vec<usize> = cuts.iter().map(|&c| c.min(buf.len())).collect();
        ends.sort_unstable();
        ends.push(buf.len());
        for end in ends {
            loop {
                let window = &buf[start..end];
                let mut args = Vec::new();
                match p.parse(window, &mut args)? {
                    Parsed::Command { consumed } => {
                        let src = p.arg_source(window);
                        out.push(
                            args.iter()
                                .map(|&(o, l)| src[o as usize..(o + l) as usize].to_vec())
                                .collect(),
                        );
                        start += consumed;
                    }
                    Parsed::Empty { consumed } => start += consumed,
                    Parsed::Incomplete => break,
                }
            }
        }
        Ok(out)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2_000))]

        /// §25.1: incremental parse == whole-buffer parse for any split of the stream.
        #[test]
        fn incremental_equals_whole(buf in arb_stream(), cuts in proptest::collection::vec(any::<usize>(), 0..12)) {
            let cuts: Vec<usize> = cuts.iter().map(|c| c % (buf.len() + 1)).collect();
            let whole = parse_split(&buf, &[]);
            let split = parse_split(&buf, &cuts);
            prop_assert_eq!(&split, &whole);
            // byte-at-a-time arrival as well
            let every: Vec<usize> = (1..buf.len()).collect();
            prop_assert_eq!(parse_split(&buf, &every), whole);
        }

        #[test]
        fn never_panics_on_garbage(buf in proptest::collection::vec(any::<u8>(), 0..300)) {
            let _ = parse_split(&buf, &[]);
            let _ = Parser::new(lim()).in_progress(&buf);
        }

        #[test]
        fn encoded_commands_round_trip(args in proptest::collection::vec(arb_arg(), 1..8)) {
            let refs: Vec<&[u8]> = args.iter().map(|a| &a[..]).collect();
            let mut buf = Vec::new();
            crate::encode_command(&refs, &mut buf);
            prop_assert_eq!(parse_all(&buf).unwrap(), vec![args.clone()]);
        }
    }
}
