//! Reply encoding (DESIGN §7.3, §7.4).
//!
//! [`ReplyWriter`] encodes for the connection's protocol: `null` is `$-1` (RESP2) / `_` (RESP3),
//! `null_array` is `*-1` / `_`, `map(n)` is an array of `2n` in RESP2, `set(n)` an array in
//! RESP2, `double` is `,` in RESP3 and a bulk in RESP2 (both `%.17g`, Redis 7.0), `boolean` is
//! `#t`/`#f` in RESP3 and `:1`/`:0` in RESP2, `verbatim` is `=` in RESP3 and a bulk in RESP2.
//! Integers are formatted with `itoa`. Push frames are never emitted in M1-M3.

use crate::Proto;

pub struct ReplyWriter<'a> {
    pub buf: &'a mut Vec<u8>,
    pub proto: Proto,
}

impl<'a> ReplyWriter<'a> {
    pub fn new(buf: &'a mut Vec<u8>, proto: Proto) -> Self {
        ReplyWriter { buf, proto }
    }

    fn header(&mut self, tag: u8, n: usize) {
        self.buf.push(tag);
        self.buf.extend_from_slice(itoa::Buffer::new().format(n).as_bytes());
        self.buf.extend_from_slice(b"\r\n");
    }

    pub fn ok(&mut self) {
        self.buf.extend_from_slice(b"+OK\r\n");
    }

    /// `+s`; `s` must not contain CR or LF.
    pub fn simple(&mut self, s: &[u8]) {
        self.buf.push(b'+');
        self.buf.extend_from_slice(s);
        self.buf.extend_from_slice(b"\r\n");
    }

    /// `-s`; `s` includes the error code (e.g. `ERR ...`) and must not contain CR or LF.
    pub fn error(&mut self, s: &[u8]) {
        self.buf.push(b'-');
        self.buf.extend_from_slice(s);
        self.buf.extend_from_slice(b"\r\n");
    }

    pub fn int(&mut self, v: i64) {
        self.buf.push(b':');
        self.buf.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        self.buf.extend_from_slice(b"\r\n");
    }

    pub fn bulk(&mut self, b: &[u8]) {
        self.header(b'$', b.len());
        self.buf.extend_from_slice(b);
        self.buf.extend_from_slice(b"\r\n");
    }

    /// Bulk header only; the caller appends `len` bytes (or sends them as an iovec) and `crlf()`.
    pub fn bulk_len(&mut self, len: usize) {
        self.header(b'$', len);
    }

    pub fn crlf(&mut self) {
        self.buf.extend_from_slice(b"\r\n");
    }

    pub fn null(&mut self) {
        match self.proto {
            Proto::Resp2 => self.buf.extend_from_slice(b"$-1\r\n"),
            Proto::Resp3 => self.buf.extend_from_slice(b"_\r\n"),
        }
    }

    pub fn null_array(&mut self) {
        match self.proto {
            Proto::Resp2 => self.buf.extend_from_slice(b"*-1\r\n"),
            Proto::Resp3 => self.buf.extend_from_slice(b"_\r\n"),
        }
    }

    pub fn array(&mut self, n: usize) {
        self.header(b'*', n);
    }

    pub fn map(&mut self, n: usize) {
        match self.proto {
            Proto::Resp2 => self.header(b'*', 2 * n),
            Proto::Resp3 => self.header(b'%', n),
        }
    }

    pub fn set(&mut self, n: usize) {
        match self.proto {
            Proto::Resp2 => self.header(b'*', n),
            Proto::Resp3 => self.header(b'~', n),
        }
    }

    pub fn double(&mut self, v: f64) {
        let mut s = Vec::with_capacity(32);
        kv_common::num::fmt_double_17g(v, &mut s);
        match self.proto {
            Proto::Resp2 => self.bulk(&s),
            Proto::Resp3 => {
                self.buf.push(b',');
                self.buf.extend_from_slice(&s);
                self.buf.extend_from_slice(b"\r\n");
            }
        }
    }

    pub fn boolean(&mut self, b: bool) {
        match self.proto {
            Proto::Resp2 => self.int(i64::from(b)),
            Proto::Resp3 => self.buf.extend_from_slice(if b { b"#t\r\n" } else { b"#f\r\n" }),
        }
    }

    /// RESP3 verbatim string `=len\r\nfmt:s\r\n`; a plain bulk of `s` in RESP2.
    pub fn verbatim(&mut self, fmt: &[u8; 3], s: &[u8]) {
        match self.proto {
            Proto::Resp2 => self.bulk(s),
            Proto::Resp3 => {
                self.header(b'=', s.len() + 4);
                self.buf.extend_from_slice(fmt);
                self.buf.push(b':');
                self.buf.extend_from_slice(s);
                self.buf.extend_from_slice(b"\r\n");
            }
        }
    }
}
