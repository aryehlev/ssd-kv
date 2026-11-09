//! Protocol errors (DESIGN §7.2, §22).
//!
//! A protocol error is answered with the Redis 7.0 error text, the output is flushed and the
//! connection is closed. Texts (after `-ERR `):
//! `Protocol error: invalid multibulk length`, `Protocol error: invalid bulk length`,
//! `Protocol error: expected '$', got 'X'`, `Protocol error: unbalanced quotes in request`,
//! `Protocol error: too big inline request`, plus Redis 7.0's `too big mbulk count string`,
//! `too big bulk count string` (a header line longer than 64 KiB) and the
//! `unauthenticated multibulk length` / `unauthenticated bulk length` limits applied while a
//! password is required and the connection has not authenticated (more than 10 arguments, a bulk
//! longer than 16384 bytes).

use core::fmt;

/// A request the server cannot parse (or, for [`ProtoError::InvalidReply`], a reply the client
/// side decoder cannot parse). Carries no user bytes except the single offending byte of
/// [`ProtoError::ExpectedDollar`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ProtoError {
    /// Multibulk count not a valid integer, or above the argument limit.
    InvalidMultibulkLen,
    /// Bulk length not a valid integer, negative, or above `proto-max-bulk-len`.
    InvalidBulkLen,
    /// The byte found where `$` was expected.
    ExpectedDollar(u8),
    UnbalancedQuotes,
    /// An inline request longer than 64 KiB.
    TooBigInline,
    /// A multibulk count line longer than 64 KiB.
    TooBigMultibulkCount,
    /// A bulk length line longer than 64 KiB.
    TooBigBulkCount,
    /// More than 10 arguments before authentication.
    UnauthenticatedMultibulkLen,
    /// A bulk longer than 16384 bytes before authentication.
    UnauthenticatedBulkLen,
    /// Client side only: a malformed reply ([`crate::decode_value`]).
    InvalidReply,
}

impl ProtoError {
    /// The Redis error message for this error, including the `ERR` code, without the leading
    /// `-` and the CRLF (the argument of `ReplyWriter::error`). For
    /// [`ProtoError::ExpectedDollar`] the caller appends the offending byte and the closing
    /// quote; [`ProtoError::write_message`] does that.
    pub fn redis_message(&self) -> &'static str {
        match self {
            ProtoError::InvalidMultibulkLen => "ERR Protocol error: invalid multibulk length",
            ProtoError::InvalidBulkLen => "ERR Protocol error: invalid bulk length",
            ProtoError::ExpectedDollar(_) => "ERR Protocol error: expected '$', got '",
            ProtoError::UnbalancedQuotes => "ERR Protocol error: unbalanced quotes in request",
            ProtoError::TooBigInline => "ERR Protocol error: too big inline request",
            ProtoError::TooBigMultibulkCount => "ERR Protocol error: too big mbulk count string",
            ProtoError::TooBigBulkCount => "ERR Protocol error: too big bulk count string",
            ProtoError::UnauthenticatedMultibulkLen => {
                "ERR Protocol error: unauthenticated multibulk length"
            }
            ProtoError::UnauthenticatedBulkLen => "ERR Protocol error: unauthenticated bulk length",
            ProtoError::InvalidReply => "ERR Protocol error: invalid reply",
        }
    }

    /// Appends the complete message (as [`ProtoError::redis_message`], with the offending byte
    /// and closing quote of `ExpectedDollar`; a CR or LF byte is shown as a space, as Redis
    /// does).
    pub fn write_message(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.redis_message().as_bytes());
        if let ProtoError::ExpectedDollar(b) = *self {
            out.push(if b == b'\r' || b == b'\n' { b' ' } else { b });
            out.push(b'\'');
        }
    }

    /// Appends the whole error reply: `-<message>\r\n` (identical in RESP2 and RESP3).
    pub fn write_reply(&self, out: &mut Vec<u8>) {
        out.push(b'-');
        self.write_message(out);
        out.extend_from_slice(b"\r\n");
    }
}

impl fmt::Display for ProtoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut m = Vec::with_capacity(64);
        self.write_message(&mut m);
        f.write_str(&String::from_utf8_lossy(&m))
    }
}

impl std::error::Error for ProtoError {}

#[cfg(test)]
mod tests {
    use super::ProtoError;

    #[test]
    fn redis_texts() {
        let mut out = Vec::new();
        ProtoError::ExpectedDollar(b'P').write_reply(&mut out);
        assert_eq!(out, b"-ERR Protocol error: expected '$', got 'P'\r\n");
        out.clear();
        ProtoError::ExpectedDollar(b'\r').write_reply(&mut out);
        assert_eq!(out, b"-ERR Protocol error: expected '$', got ' '\r\n");
        out.clear();
        ProtoError::TooBigMultibulkCount.write_reply(&mut out);
        assert_eq!(out, b"-ERR Protocol error: too big mbulk count string\r\n");
        assert_eq!(
            ProtoError::InvalidBulkLen.to_string(),
            "ERR Protocol error: invalid bulk length"
        );
        assert_eq!(
            ProtoError::UnbalancedQuotes.redis_message(),
            "ERR Protocol error: unbalanced quotes in request"
        );
    }
}
