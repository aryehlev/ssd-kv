//! Inline commands (DESIGN §7.2).
//!
//! Splits one inline request line into arguments with Redis `sdssplitargs` rules: whitespace
//! separated words, double quotes with `\n \r \t \b \a \\ \"` and `\xHH` escapes, single quotes
//! with `\'`, a closing quote must be followed by whitespace or the end of the line, otherwise
//! `ProtoError::UnbalancedQuotes`. Lines longer than `max_inline` give
//! `ProtoError::TooBigInline`. Ported from Redis <= 7.2 (BSD-3-Clause, see NOTICE).
//!
//! Open point for the implementer: an argument containing escapes has no contiguous image in
//! `read_buf`; the parser's `(offset, len)` contract needs the unescaped bytes written back into
//! the line (unescaping never lengthens an argument) or a side buffer.

use crate::error::ProtoError;

/// Splits `line` (without its line terminator) into arguments, pushing `(offset, len)` pairs
/// relative to `line`.
pub(crate) fn split_inline(line: &[u8], max_inline: usize, args: &mut Vec<(u32, u32)>) -> Result<(), ProtoError> {
    todo!("DESIGN §7.2: sdssplitargs")
}
