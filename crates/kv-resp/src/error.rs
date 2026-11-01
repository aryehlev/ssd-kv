//! Protocol errors (DESIGN §7.2, §22).
//!
//! A protocol error is answered with the Redis 7.0 error text, the output is flushed and the
//! connection is closed. Texts (after `-ERR `):
//! `Protocol error: invalid multibulk length`, `Protocol error: invalid bulk length`,
//! `Protocol error: expected '$', got 'X'`, `Protocol error: unbalanced quotes in request`,
//! `Protocol error: too big inline request`.

#[derive(Debug)]
pub enum ProtoError {
    InvalidMultibulkLen,
    InvalidBulkLen,
    /// The byte found where `$` was expected.
    ExpectedDollar(u8),
    UnbalancedQuotes,
    TooBigInline,
}

impl ProtoError {
    /// The Redis error message for this error. For [`ProtoError::ExpectedDollar`] the caller
    /// appends the offending byte and the closing quote.
    pub fn redis_message(&self) -> &'static str {
        todo!("DESIGN §7.2: Redis protocol error texts")
    }
}
