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

use core::fmt;

use kv_common::Redacted;

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
///
/// RESP3 types without a variant of their own are folded: sets (`~`) and pushes (`>`) decode as
/// `Array`, verbatim strings (`=`) as `Bulk` of the text after the `fmt:` prefix, big numbers
/// (`(`) as `Bulk` of their digits, blob errors (`!`) as `Error`; attributes (`|`) are skipped
/// and the value they annotate is returned. `Debug` output redacts every byte payload (LC-32).
#[derive(Clone, PartialEq)]
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

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Simple(s) => write!(f, "Simple({})", Redacted(s)),
            Value::Error(s) => write!(f, "Error({})", Redacted(s)),
            Value::Int(v) => write!(f, "Int({v})"),
            Value::Bulk(Some(b)) => write!(f, "Bulk({})", Redacted(b)),
            Value::Bulk(None) => f.write_str("Bulk(nil)"),
            Value::Array(Some(a)) => f.debug_tuple("Array").field(a).finish(),
            Value::Array(None) => f.write_str("Array(nil)"),
            Value::Map(m) => f.debug_tuple("Map").field(m).finish(),
            Value::Double(v) => write!(f, "Double({v})"),
            Value::Null => f.write_str("Null"),
            Value::Bool(b) => write!(f, "Bool({b})"),
        }
    }
}

/// Deepest aggregate nesting accepted by [`decode_value`] (guards the decoder's recursion).
pub const MAX_REPLY_DEPTH: usize = 128;

/// Decodes one complete value from `buf`; `Ok(None)` when more bytes are needed. On success
/// returns the value and the number of bytes consumed. Malformed input gives
/// `ProtoError::InvalidReply`.
pub fn decode_value(buf: &[u8]) -> Result<Option<(Value, usize)>, ProtoError> {
    let mut pos = 0;
    Ok(decode_at(buf, &mut pos, 0)?.map(|v| (v, pos)))
}

/// The line starting at `*pos` up to its CRLF (excluded); advances `*pos` past the CRLF.
fn line<'a>(buf: &'a [u8], pos: &mut usize) -> Result<Option<&'a [u8]>, ProtoError> {
    let start = *pos;
    let Some(i) = memchr::memchr(b'\r', &buf[start..]) else {
        return Ok(None);
    };
    let cr = start + i;
    match buf.get(cr + 1) {
        None => Ok(None),
        Some(b'\n') => {
            *pos = cr + 2;
            Ok(Some(&buf[start..cr]))
        }
        Some(_) => Err(ProtoError::InvalidReply),
    }
}

fn int_of(s: &[u8]) -> Result<i64, ProtoError> {
    let (neg, digits) = match s.split_first() {
        Some((b'-', rest)) => (true, rest),
        Some((b'+', rest)) => (false, rest),
        _ => (false, s),
    };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return Err(ProtoError::InvalidReply);
    }
    let mut v: i64 = 0;
    for &d in digits {
        let d = i64::from(d - b'0');
        v = v.checked_mul(10).and_then(|v| if neg { v.checked_sub(d) } else { v.checked_add(d) }).ok_or(ProtoError::InvalidReply)?;
    }
    Ok(v)
}

/// A length header: `-1` (null) or a non-negative count.
fn len_of(s: &[u8]) -> Result<Option<usize>, ProtoError> {
    match int_of(s)? {
        -1 => Ok(None),
        n if n >= 0 => usize::try_from(n).map(Some).map_err(|_| ProtoError::InvalidReply),
        _ => Err(ProtoError::InvalidReply),
    }
}

/// The payload of a bulk-like value of `n` bytes at `*pos`, followed by CRLF.
fn payload<'a>(buf: &'a [u8], pos: &mut usize, n: usize) -> Result<Option<&'a [u8]>, ProtoError> {
    let start = *pos;
    let end = start.checked_add(n).ok_or(ProtoError::InvalidReply)?;
    if buf.len() < end.saturating_add(2) {
        return Ok(None);
    }
    if &buf[end..end + 2] != b"\r\n" {
        return Err(ProtoError::InvalidReply);
    }
    *pos = end + 2;
    Ok(Some(&buf[start..end]))
}

fn decode_at(buf: &[u8], pos: &mut usize, depth: usize) -> Result<Option<Value>, ProtoError> {
    if depth > MAX_REPLY_DEPTH {
        return Err(ProtoError::InvalidReply);
    }
    let Some(&tag) = buf.get(*pos) else {
        return Ok(None);
    };
    let mut p = *pos + 1;
    let Some(head) = line(buf, &mut p)? else {
        return Ok(None);
    };
    let value = match tag {
        b'+' => Value::Simple(head.to_vec()),
        b'-' => Value::Error(head.to_vec()),
        b':' => Value::Int(int_of(head)?),
        b'_' if head.is_empty() => Value::Null,
        b'#' => match head {
            b"t" => Value::Bool(true),
            b"f" => Value::Bool(false),
            _ => return Err(ProtoError::InvalidReply),
        },
        b',' => Value::Double(match head {
            b"inf" | b"+inf" => f64::INFINITY,
            b"-inf" => f64::NEG_INFINITY,
            b"nan" | b"-nan" => f64::NAN,
            _ => core::str::from_utf8(head).ok().and_then(|s| s.parse().ok()).ok_or(ProtoError::InvalidReply)?,
        }),
        b'(' => {
            let digits = head.strip_prefix(b"-").unwrap_or(head);
            if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
                return Err(ProtoError::InvalidReply);
            }
            Value::Bulk(Some(head.to_vec()))
        }
        b'$' | b'=' | b'!' => match len_of(head)? {
            None if tag == b'$' => Value::Bulk(None),
            None => return Err(ProtoError::InvalidReply),
            Some(n) => {
                let Some(data) = payload(buf, &mut p, n)? else {
                    return Ok(None);
                };
                match tag {
                    b'$' => Value::Bulk(Some(data.to_vec())),
                    b'!' => Value::Error(data.to_vec()),
                    _ => {
                        if data.len() < 4 || data[3] != b':' {
                            return Err(ProtoError::InvalidReply);
                        }
                        Value::Bulk(Some(data[4..].to_vec()))
                    }
                }
            }
        },
        b'*' | b'~' | b'>' => match len_of(head)? {
            None if tag == b'*' => Value::Array(None),
            None => return Err(ProtoError::InvalidReply),
            Some(n) => {
                // never trust a count for preallocation: each element takes at least 3 bytes
                let mut items = Vec::with_capacity(n.min((buf.len() - p) / 3));
                for _ in 0..n {
                    match decode_at(buf, &mut p, depth + 1)? {
                        Some(v) => items.push(v),
                        None => return Ok(None),
                    }
                }
                Value::Array(Some(items))
            }
        },
        b'%' | b'|' => {
            let n = len_of(head)?.ok_or(ProtoError::InvalidReply)?;
            let mut pairs = Vec::with_capacity(n.min((buf.len() - p) / 6));
            for _ in 0..n {
                let Some(k) = decode_at(buf, &mut p, depth + 1)? else {
                    return Ok(None);
                };
                let Some(v) = decode_at(buf, &mut p, depth + 1)? else {
                    return Ok(None);
                };
                pairs.push((k, v));
            }
            if tag == b'|' {
                // an attribute annotates the value that follows it
                let annotated = decode_at(buf, &mut p, depth + 1)?;
                *pos = p;
                return Ok(annotated);
            }
            Value::Map(pairs)
        }
        _ => return Err(ProtoError::InvalidReply),
    };
    *pos = p;
    Ok(Some(value))
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

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn dec(b: &[u8]) -> Value {
        let (v, n) = decode_value(b).unwrap().expect("complete");
        assert_eq!(n, b.len(), "consumed everything");
        v
    }

    fn bulk(b: &[u8]) -> Value {
        Value::Bulk(Some(b.to_vec()))
    }

    #[test]
    fn decodes_every_type() {
        assert_eq!(dec(b"+OK\r\n"), Value::Simple(b"OK".to_vec()));
        assert_eq!(dec(b"-ERR x\r\n"), Value::Error(b"ERR x".to_vec()));
        assert_eq!(dec(b":-12\r\n"), Value::Int(-12));
        assert_eq!(dec(b"$3\r\na\r\n\r\n"), bulk(b"a\r\n"));
        assert_eq!(dec(b"$-1\r\n"), Value::Bulk(None));
        assert_eq!(dec(b"*-1\r\n"), Value::Array(None));
        assert_eq!(dec(b"*2\r\n:1\r\n$1\r\nx\r\n"), Value::Array(Some(vec![Value::Int(1), bulk(b"x")])));
        assert_eq!(dec(b"~1\r\n+a\r\n"), Value::Array(Some(vec![Value::Simple(b"a".to_vec())])));
        assert_eq!(dec(b">1\r\n+a\r\n"), Value::Array(Some(vec![Value::Simple(b"a".to_vec())])));
        assert_eq!(dec(b"%1\r\n+k\r\n:2\r\n"), Value::Map(vec![(Value::Simple(b"k".to_vec()), Value::Int(2))]));
        assert_eq!(dec(b"_\r\n"), Value::Null);
        assert_eq!(dec(b"#t\r\n"), Value::Bool(true));
        assert_eq!(dec(b",1.5\r\n"), Value::Double(1.5));
        assert_eq!(dec(b",-inf\r\n"), Value::Double(f64::NEG_INFINITY));
        assert!(matches!(dec(b",nan\r\n"), Value::Double(v) if v.is_nan()));
        assert_eq!(dec(b"=8\r\ntxt:abcd\r\n"), bulk(b"abcd"));
        assert_eq!(dec(b"(-123\r\n"), bulk(b"-123"));
        assert_eq!(dec(b"!5\r\nERR x\r\n"), Value::Error(b"ERR x".to_vec()));
        assert_eq!(dec(b"|1\r\n+ttl\r\n:3\r\n+value\r\n"), Value::Simple(b"value".to_vec()));
    }

    #[test]
    fn incomplete_and_malformed() {
        for b in [&b""[..], b"+OK", b"+OK\r", b"$3\r\nab", b"$3\r\nabc\r", b"*2\r\n:1\r\n", b"%1\r\n+k\r\n", b"|1\r\n+a\r\n:1\r\n"] {
            assert_eq!(decode_value(b), Ok(None), "{b:?}");
        }
        for b in [&b"?x\r\n"[..], b":1x\r\n", b"$-2\r\n", b"$3\r\nabcXY", b"#x\r\n", b"+a\rb", b"=2\r\nab\r\n", b"(1a\r\n", b"%-1\r\n"] {
            assert_eq!(decode_value(b), Err(ProtoError::InvalidReply), "{b:?}");
        }
        let deep = b"*1\r\n".repeat(MAX_REPLY_DEPTH + 2);
        assert_eq!(decode_value(&deep), Err(ProtoError::InvalidReply));
        // a huge declared count does not preallocate
        assert_eq!(decode_value(b"*4000000000\r\n:1\r\n"), Ok(None));
        // pipelined values decode one at a time
        let (v, n) = decode_value(b"+A\r\n+B\r\n").unwrap().unwrap();
        assert_eq!((v, n), (Value::Simple(b"A".to_vec()), 4));
    }

    #[test]
    fn debug_redacts_payloads() {
        let s = format!("{:?}", Value::Array(Some(vec![bulk(b"secret"), Value::Int(3)])));
        assert!(!s.contains("secret"), "{s}");
        assert!(s.contains("Int(3)"));
    }

    fn arb_value() -> impl Strategy<Value = Value> {
        let leaf = prop_oneof![
            proptest::collection::vec(any::<u8>(), 0..20).prop_map(|b| Value::Bulk(Some(b))),
            Just(Value::Bulk(None)),
            any::<i64>().prop_map(Value::Int),
            "[a-zA-Z ]{0,10}".prop_map(|s| Value::Simple(s.into_bytes())),
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            (-1e300f64..1e300).prop_map(Value::Double),
        ];
        leaf.prop_recursive(3, 24, 4, |inner| {
            prop_oneof![
                proptest::collection::vec(inner.clone(), 0..4).prop_map(|v| Value::Array(Some(v))),
                proptest::collection::vec((inner.clone(), inner), 0..3).prop_map(Value::Map),
            ]
        })
    }

    fn write(w: &mut ReplyWriter<'_>, v: &Value) {
        match v {
            Value::Simple(s) => w.simple(s),
            Value::Error(s) => w.error(s),
            Value::Int(i) => w.int(*i),
            Value::Bulk(Some(b)) => w.bulk(b),
            Value::Bulk(None) => w.null(),
            Value::Array(Some(a)) => {
                w.array(a.len());
                a.iter().for_each(|x| write(w, x));
            }
            Value::Array(None) => w.null_array(),
            Value::Map(m) => {
                w.map(m.len());
                for (k, x) in m {
                    write(w, k);
                    write(w, x);
                }
            }
            Value::Double(d) => w.double(*d),
            Value::Null => w.null(),
            Value::Bool(b) => w.boolean(*b),
        }
    }

    proptest! {
        /// RESP3 writer output decodes back to the same value, at every split point.
        #[test]
        fn writer_decoder_round_trip(v in arb_value()) {
            let mut buf = Vec::new();
            write(&mut ReplyWriter::new(&mut buf, Proto::Resp3), &v);
            let expect = match &v { Value::Bulk(None) => Value::Null, other => other.clone() };
            let (got, n) = decode_value(&buf).unwrap().unwrap();
            prop_assert_eq!(n, buf.len());
            // RESP3 encodes a null bulk as `_`
            fn norm(v: Value) -> Value {
                match v {
                    Value::Bulk(None) => Value::Null,
                    Value::Array(Some(a)) => Value::Array(Some(a.into_iter().map(norm).collect())),
                    Value::Map(m) => Value::Map(m.into_iter().map(|(k, v)| (norm(k), norm(v))).collect()),
                    other => other,
                }
            }
            prop_assert_eq!(norm(got), norm(expect));
            for cut in 0..buf.len() {
                prop_assert_eq!(decode_value(&buf[..cut]), Ok(None));
            }
        }
    }
}
