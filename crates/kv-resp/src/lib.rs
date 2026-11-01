//! `kv-resp`: the RESP2/RESP3 wire layer (DESIGN §7, §24.4).
//!
//! * [`parser`]: incremental, zero-copy server-side parser for multibulk and inline commands,
//!   with Redis limits and error texts, plus the pre-consume view used by admission control
//!   (§6.7, §7.2).
//! * [`inline`]: inline command splitting with Redis `sdssplitargs` quoting rules.
//! * [`writer`]: [`ReplyWriter`], protocol-aware reply encoding (§7.3).
//! * [`error`]: [`ProtoError`] and its Redis error texts.
//! * client side: [`Value`], [`decode_value`], [`encode_command`] for kvadm, kvbench, tests and
//!   the coordinator's internal tools.
#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(unsafe_code)]
#![allow(dead_code, unused_variables)]

pub mod error;
pub mod inline;
pub mod parser;
pub mod writer;

pub use error::ProtoError;
pub use parser::{InProgress, Limits, Parsed, Parser};
pub use writer::ReplyWriter;

/// RESP protocol version of a connection (§7.4).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Proto {
    Resp2,
    Resp3,
}

/// A decoded RESP value (client side).
pub enum Value {
    Simple(Vec<u8>),
    Error(Vec<u8>),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Value>>),
    Map(Vec<(Value, Value)>),
    Double(f64),
    Null,
    Bool(bool),
}

/// Decodes one complete value from `buf`; `Ok(None)` when more bytes are needed. On success
/// returns the value and the number of bytes consumed.
pub fn decode_value(buf: &[u8]) -> Result<Option<(Value, usize)>, ProtoError> {
    todo!("DESIGN §24.4: client-side RESP decoder")
}

/// Appends `args` encoded as a RESP multibulk command.
pub fn encode_command(args: &[&[u8]], out: &mut Vec<u8>) {
    let mut n = itoa::Buffer::new();
    out.push(b'*');
    out.extend_from_slice(n.format(args.len()).as_bytes());
    out.extend_from_slice(b"\r\n");
    for a in args {
        out.push(b'$');
        out.extend_from_slice(n.format(a.len()).as_bytes());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
}
