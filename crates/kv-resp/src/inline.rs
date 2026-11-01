//! Inline commands (DESIGN §7.2).
//!
//! Splits one inline request line into arguments with Redis `sdssplitargs` rules: whitespace
//! separated words, double quotes with `\n \r \t \b \a \\ \"` and `\xHH` escapes, single quotes
//! with `\'`, a closing quote must be followed by whitespace or the end of the line, otherwise
//! `ProtoError::UnbalancedQuotes`. Ported from Redis <= 7.2 (BSD-3-Clause, see NOTICE).
//!
//! Details that matter for byte-exact compatibility: leading blanks are skipped with C
//! `isspace` (space, `\t`, `\n`, `\v`, `\f`, `\r`), but an unquoted word ends only at space,
//! `\t`, `\n` or `\r` (so `\v` and `\f` inside a word belong to it); a quote may start in the
//! middle of a word (`foo"bar baz"` is `foobar baz`); an unknown escape `\c` yields `c`; a
//! malformed `\x` escape yields `x`.
//!
//! Arguments of a line without quote characters are contiguous in the line and are returned as
//! `(offset, len)` pairs into it (zero copy). When the line contains a quote character every
//! argument is unescaped into a side buffer and the pairs refer to that buffer; unescaping never
//! lengthens the line, so the side buffer can also be copied back over the line
//! ([`crate::Parser::parse_in_place`]). The 64 KiB inline limit is enforced by the parser
//! (`ProtoError::TooBigInline`) before a line is split.

use crate::error::ProtoError;

/// C locale `isspace`.
#[inline]
fn is_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

/// Bytes that end an unquoted word (`sdssplitargs`; the C string terminator is the line end).
#[inline]
fn ends_word(b: u8) -> bool {
    matches!(b, b' ' | b'\n' | b'\r' | b'\t')
}

#[inline]
fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Splits `line` (without its line terminator) into arguments. Returns `Ok(false)` when the
/// `(offset, len)` pairs pushed to `args` refer to `line`, `Ok(true)` when they refer to `side`
/// (cleared first). A NUL byte ends the line, as the C string terminator does.
pub(crate) fn split_inline(
    line: &[u8],
    side: &mut Vec<u8>,
    args: &mut Vec<(u32, u32)>,
) -> Result<bool, ProtoError> {
    let line = match memchr::memchr(0, line) {
        Some(n) => &line[..n],
        None => line,
    };
    if memchr::memchr2(b'"', b'\'', line).is_none() {
        split_plain(line, args);
        Ok(false)
    } else {
        side.clear();
        let base = args.len();
        split_quoted(line, side, args).inspect_err(|_| args.truncate(base))?;
        Ok(true)
    }
}

/// Fast path: no quote characters, every word is a contiguous run of the line.
fn split_plain(line: &[u8], args: &mut Vec<(u32, u32)>) {
    let n = line.len();
    let mut i = 0;
    loop {
        while i < n && is_space(line[i]) {
            i += 1;
        }
        if i == n {
            return;
        }
        let start = i;
        while i < n && !ends_word(line[i]) {
            i += 1;
        }
        args.push((start as u32, (i - start) as u32));
    }
}

/// General path: a transcription of `sdssplitargs`, writing unescaped words into `side`.
fn split_quoted(
    line: &[u8],
    side: &mut Vec<u8>,
    args: &mut Vec<(u32, u32)>,
) -> Result<(), ProtoError> {
    // the byte at `i`, 0 at and past the end (the C terminator)
    let at = |i: usize| line.get(i).copied().unwrap_or(0);
    let mut p = 0usize;
    loop {
        while at(p) != 0 && is_space(at(p)) {
            p += 1;
        }
        if at(p) == 0 {
            return Ok(());
        }
        let start = side.len();
        let (mut inq, mut insq) = (false, false);
        loop {
            let c = at(p);
            if inq {
                if c == b'\\'
                    && at(p + 1) == b'x'
                    && hex_val(at(p + 2)).is_some()
                    && hex_val(at(p + 3)).is_some()
                {
                    let hi = hex_val(at(p + 2)).unwrap_or(0);
                    let lo = hex_val(at(p + 3)).unwrap_or(0);
                    side.push(hi * 16 + lo);
                    p += 3;
                } else if c == b'\\' && at(p + 1) != 0 {
                    p += 1;
                    side.push(match at(p) {
                        b'n' => b'\n',
                        b'r' => b'\r',
                        b't' => b'\t',
                        b'b' => 0x08,
                        b'a' => 0x07,
                        other => other,
                    });
                } else if c == b'"' {
                    // the closing quote must be followed by a space or the end of the line
                    if at(p + 1) != 0 && !is_space(at(p + 1)) {
                        return Err(ProtoError::UnbalancedQuotes);
                    }
                    p += 1;
                    break;
                } else if c == 0 {
                    return Err(ProtoError::UnbalancedQuotes);
                } else {
                    side.push(c);
                }
            } else if insq {
                if c == b'\\' && at(p + 1) == b'\'' {
                    p += 1;
                    side.push(b'\'');
                } else if c == b'\'' {
                    if at(p + 1) != 0 && !is_space(at(p + 1)) {
                        return Err(ProtoError::UnbalancedQuotes);
                    }
                    p += 1;
                    break;
                } else if c == 0 {
                    return Err(ProtoError::UnbalancedQuotes);
                } else {
                    side.push(c);
                }
            } else {
                match c {
                    b' ' | b'\n' | b'\r' | b'\t' | 0 => {
                        if c != 0 {
                            p += 1;
                        }
                        break;
                    }
                    b'"' => inq = true,
                    b'\'' => insq = true,
                    other => side.push(other),
                }
            }
            if at(p) != 0 {
                p += 1;
            }
        }
        args.push((start as u32, (side.len() - start) as u32));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn split(line: &[u8]) -> Result<Vec<Vec<u8>>, ProtoError> {
        let mut side = Vec::new();
        let mut args = Vec::new();
        let in_side = split_inline(line, &mut side, &mut args)?;
        let src = if in_side { &side[..] } else { line };
        Ok(args
            .iter()
            .map(|&(o, l)| src[o as usize..(o + l) as usize].to_vec())
            .collect())
    }

    fn v(words: &[&[u8]]) -> Vec<Vec<u8>> {
        words.iter().map(|w| w.to_vec()).collect()
    }

    #[test]
    fn plain_words() {
        assert_eq!(split(b"SET k v").unwrap(), v(&[b"SET", b"k", b"v"]));
        assert_eq!(split(b"  PING   ").unwrap(), v(&[b"PING"]));
        assert_eq!(split(b"").unwrap(), v(&[]));
        assert_eq!(split(b" \t\x0b\x0c ").unwrap(), v(&[]));
        assert_eq!(
            split(b"ECHO a\x0bb").unwrap(),
            v(&[b"ECHO", b"a\x0bb"]),
            "\\v does not end a word"
        );
        assert_eq!(
            split(b"\x0bECHO ab").unwrap(),
            v(&[b"ECHO", b"ab"]),
            "but is skipped as a blank"
        );
        assert_eq!(split(b"ECHO a\rb").unwrap(), v(&[b"ECHO", b"a", b"b"]));
        assert_eq!(
            split(b"a\\nb").unwrap(),
            v(&[b"a\\nb"]),
            "no escapes outside quotes"
        );
        assert_eq!(split(b"a\x00b c").unwrap(), v(&[b"a"]), "NUL ends the line");
    }

    #[test]
    fn quoting_matches_redis() {
        // Replies of Redis 7.0.15 to `ECHO <arg>` sent inline.
        assert_eq!(
            split(b"ECHO \"a\\x41\\n\\tb\"").unwrap(),
            v(&[b"ECHO", b"aA\n\tb"])
        );
        assert_eq!(split(b"ECHO 'it\\'s'").unwrap(), v(&[b"ECHO", b"it's"]));
        assert_eq!(
            split(b"ECHO foo\"bar baz\"").unwrap(),
            v(&[b"ECHO", b"foobar baz"])
        );
        assert_eq!(split(b"ECHO \"\\xZZ\"").unwrap(), v(&[b"ECHO", b"xZZ"]));
        assert_eq!(
            split(b"ECHO \"\\b\\a\\\\\\\"\\q\"").unwrap(),
            v(&[b"ECHO", b"\x08\x07\\\"q"])
        );
        assert_eq!(
            split(b"ECHO 'a\\nb'").unwrap(),
            v(&[b"ECHO", b"a\\nb"]),
            "single quotes keep backslashes"
        );
        assert_eq!(split(b"a \"\" b").unwrap(), v(&[b"a", b"", b"b"]));
        assert_eq!(
            split(b"\"x\"\x0b").unwrap(),
            v(&[b"x"]),
            "closing quote followed by \\v"
        );
        assert_eq!(split(b"\"\\x4a\\x4B\"").unwrap(), v(&[b"JK"]));
        assert_eq!(split(b"x \"y\"").unwrap(), v(&[b"x", b"y"]));
    }

    #[test]
    fn unbalanced() {
        for line in [
            &b"PING \"abc"[..],
            b"ECHO \"abc\"x",
            b"ECHO \"abc\\\"",
            b"'abc",
            b"'abc'x",
            b"\"\\",
        ] {
            assert_eq!(split(line), Err(ProtoError::UnbalancedQuotes), "{line:?}");
        }
    }

    #[test]
    fn side_buffer_never_longer_than_line() {
        for line in [
            &b"ECHO \"a\\x41\\n\\tb\""[..],
            b"'it\\'s' \"x\" y",
            b"foo\"bar baz\"",
        ] {
            let mut side = Vec::new();
            let mut args = Vec::new();
            assert!(split_inline(line, &mut side, &mut args).unwrap());
            assert!(side.len() <= line.len());
        }
    }
}
