//! Reply encoding (DESIGN §7.3, §7.4).
//!
//! [`ReplyWriter`] encodes for the connection's protocol: `null` is `$-1` (RESP2) / `_` (RESP3),
//! `null_array` is `*-1` / `_`, `map(n)` is an array of `2n` in RESP2, `set(n)` an array in
//! RESP2, `double` is `,` in RESP3 and a bulk in RESP2 (both `%.17g`, Redis 7.0, with
//! `inf`/`-inf`), `boolean` is `#t`/`#f` in RESP3 and `:1`/`:0` in RESP2, `verbatim` is `=` in
//! RESP3 and a bulk in RESP2, `big_number` is `(` in RESP3 and a bulk in RESP2, `push(n)` is `>`
//! in RESP3 and an array in RESP2. Integers are formatted with `itoa`. Push frames are never
//! emitted in M1-M3.
//!
//! Simple strings and errors cannot carry CR or LF: like Redis's error formatting, any CR or LF
//! byte is written as a space, so a reply can never break the framing.

use crate::Proto;

pub struct ReplyWriter<'a> {
    pub buf: &'a mut Vec<u8>,
    pub proto: Proto,
}

impl<'a> ReplyWriter<'a> {
    pub fn new(buf: &'a mut Vec<u8>, proto: Proto) -> Self {
        ReplyWriter { buf, proto }
    }

    #[inline]
    fn header(&mut self, tag: u8, n: usize) {
        self.buf.push(tag);
        self.buf
            .extend_from_slice(itoa::Buffer::new().format(n).as_bytes());
        self.buf.extend_from_slice(b"\r\n");
    }

    /// Appends `s` with CR and LF replaced by spaces.
    #[inline]
    fn line(&mut self, s: &[u8]) {
        let start = self.buf.len();
        self.buf.extend_from_slice(s);
        for b in &mut self.buf[start..] {
            if *b == b'\r' || *b == b'\n' {
                *b = b' ';
            }
        }
        self.buf.extend_from_slice(b"\r\n");
    }

    #[inline]
    pub fn ok(&mut self) {
        self.buf.extend_from_slice(b"+OK\r\n");
    }

    /// `+s`; CR and LF in `s` are written as spaces.
    #[inline]
    pub fn simple(&mut self, s: &[u8]) {
        self.buf.push(b'+');
        self.line(s);
    }

    /// `-s`; `s` includes the error code (e.g. `ERR ...`); CR and LF in `s` are written as
    /// spaces.
    #[inline]
    pub fn error(&mut self, s: &[u8]) {
        self.buf.push(b'-');
        self.line(s);
    }

    #[inline]
    pub fn int(&mut self, v: i64) {
        self.buf.push(b':');
        self.buf
            .extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        self.buf.extend_from_slice(b"\r\n");
    }

    #[inline]
    pub fn bulk(&mut self, b: &[u8]) {
        self.header(b'$', b.len());
        self.buf.extend_from_slice(b);
        self.buf.extend_from_slice(b"\r\n");
    }

    /// A bulk holding the decimal rendering of `v` (Redis `addReplyBulkLongLong`).
    #[inline]
    pub fn bulk_i64(&mut self, v: i64) {
        let mut b = itoa::Buffer::new();
        let s = b.format(v);
        self.bulk(s.as_bytes());
    }

    /// Bulk header only; the caller appends `len` bytes (or sends them as an iovec) and `crlf()`.
    #[inline]
    pub fn bulk_len(&mut self, len: usize) {
        self.header(b'$', len);
    }

    #[inline]
    pub fn crlf(&mut self) {
        self.buf.extend_from_slice(b"\r\n");
    }

    /// Null bulk: `$-1` (RESP2) / `_` (RESP3).
    #[inline]
    pub fn null(&mut self) {
        match self.proto {
            Proto::Resp2 => self.buf.extend_from_slice(b"$-1\r\n"),
            Proto::Resp3 => self.buf.extend_from_slice(b"_\r\n"),
        }
    }

    /// Null array: `*-1` (RESP2) / `_` (RESP3).
    #[inline]
    pub fn null_array(&mut self) {
        match self.proto {
            Proto::Resp2 => self.buf.extend_from_slice(b"*-1\r\n"),
            Proto::Resp3 => self.buf.extend_from_slice(b"_\r\n"),
        }
    }

    #[inline]
    pub fn array(&mut self, n: usize) {
        self.header(b'*', n);
    }

    /// Map of `n` key/value pairs (`%n`); an array of `2n` elements in RESP2.
    #[inline]
    pub fn map(&mut self, n: usize) {
        match self.proto {
            Proto::Resp2 => self.header(b'*', 2 * n),
            Proto::Resp3 => self.header(b'%', n),
        }
    }

    /// Set of `n` elements (`~n`); an array in RESP2.
    #[inline]
    pub fn set(&mut self, n: usize) {
        match self.proto {
            Proto::Resp2 => self.header(b'*', n),
            Proto::Resp3 => self.header(b'~', n),
        }
    }

    /// Push frame of `n` elements (`>n`); an array in RESP2. Never emitted in M1-M3.
    #[inline]
    pub fn push(&mut self, n: usize) {
        match self.proto {
            Proto::Resp2 => self.header(b'*', n),
            Proto::Resp3 => self.header(b'>', n),
        }
    }

    /// Redis `addReplyDouble`: `,<%.17g>` in RESP3, a bulk of the same text in RESP2;
    /// infinities are `inf` / `-inf`.
    pub fn double(&mut self, v: f64) {
        let mut s = [0u8; kv_common::num::DOUBLE_17G_MAX];
        let n = kv_common::num::fmt_double_17g_buf(v, &mut s);
        match self.proto {
            Proto::Resp2 => self.bulk(&s[..n]),
            Proto::Resp3 => {
                self.buf.push(b',');
                self.buf.extend_from_slice(&s[..n]);
                self.buf.extend_from_slice(b"\r\n");
            }
        }
    }

    /// `#t`/`#f` in RESP3, `:1`/`:0` in RESP2.
    #[inline]
    pub fn boolean(&mut self, b: bool) {
        match self.proto {
            Proto::Resp2 => self.int(i64::from(b)),
            Proto::Resp3 => self
                .buf
                .extend_from_slice(if b { b"#t\r\n" } else { b"#f\r\n" }),
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

    /// Big number (`(digits` in RESP3, Redis `addReplyBigNum`); a bulk of the digits in RESP2.
    /// `digits` is an optional `-` followed by decimal digits.
    pub fn big_number(&mut self, digits: &[u8]) {
        match self.proto {
            Proto::Resp2 => self.bulk(digits),
            Proto::Resp3 => {
                self.buf.push(b'(');
                self.line(digits);
            }
        }
    }

    /// An empty array (`*0`, in both protocols).
    #[inline]
    pub fn empty_array(&mut self) {
        self.buf.extend_from_slice(b"*0\r\n");
    }

    /// The HELLO reply (§7.4) in the writer's protocol, which the caller has already switched to
    /// the requested version: a map of `server`, `version`, `proto`, `id`, `mode`, `role` and an
    /// empty `modules` array, in Redis 7's order and types.
    pub fn hello(&mut self, server: &[u8], version: &[u8], id: i64, mode: &[u8], role: &[u8]) {
        self.map(7);
        self.bulk(b"server");
        self.bulk(server);
        self.bulk(b"version");
        self.bulk(version);
        self.bulk(b"proto");
        self.int(match self.proto {
            Proto::Resp2 => 2,
            Proto::Resp3 => 3,
        });
        self.bulk(b"id");
        self.int(id);
        self.bulk(b"mode");
        self.bulk(mode);
        self.bulk(b"role");
        self.bulk(role);
        self.bulk(b"modules");
        self.empty_array();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(proto: Proto, f: impl FnOnce(&mut ReplyWriter<'_>)) -> Vec<u8> {
        let mut buf = Vec::new();
        f(&mut ReplyWriter::new(&mut buf, proto));
        buf
    }

    fn both(f: impl Fn(&mut ReplyWriter<'_>)) -> (Vec<u8>, Vec<u8>) {
        (enc(Proto::Resp2, &f), enc(Proto::Resp3, &f))
    }

    #[test]
    fn scalar_encodings() {
        assert_eq!(both(|w| w.ok()), (b"+OK\r\n".to_vec(), b"+OK\r\n".to_vec()));
        assert_eq!(both(|w| w.simple(b"PONG")).0, b"+PONG\r\n");
        assert_eq!(
            both(|w| w.error(b"ERR syntax error")).1,
            b"-ERR syntax error\r\n"
        );
        assert_eq!(
            both(|w| w.error(b"ERR a\r\nb")).0,
            b"-ERR a  b\r\n",
            "CR/LF never break framing"
        );
        assert_eq!(both(|w| w.simple(b"x\ny")).0, b"+x y\r\n");
        assert_eq!(both(|w| w.int(-42)).0, b":-42\r\n");
        assert_eq!(both(|w| w.int(i64::MIN)).1, b":-9223372036854775808\r\n");
        assert_eq!(both(|w| w.bulk(b"")).0, b"$0\r\n\r\n");
        assert_eq!(both(|w| w.bulk(b"a\r\nb")).1, b"$4\r\na\r\nb\r\n");
        assert_eq!(both(|w| w.bulk_i64(-7)).0, b"$2\r\n-7\r\n");
        assert_eq!(
            both(|w| {
                w.bulk_len(3);
                w.buf.extend_from_slice(b"abc");
                w.crlf();
            })
            .0,
            b"$3\r\nabc\r\n"
        );
        assert_eq!(
            both(|w| w.empty_array()),
            (b"*0\r\n".to_vec(), b"*0\r\n".to_vec())
        );
    }

    #[test]
    fn null_encodings() {
        assert_eq!(both(|w| w.null()), (b"$-1\r\n".to_vec(), b"_\r\n".to_vec()));
        assert_eq!(
            both(|w| w.null_array()),
            (b"*-1\r\n".to_vec(), b"_\r\n".to_vec())
        );
    }

    #[test]
    fn aggregate_encodings() {
        assert_eq!(
            both(|w| w.array(3)),
            (b"*3\r\n".to_vec(), b"*3\r\n".to_vec())
        );
        assert_eq!(both(|w| w.map(2)), (b"*4\r\n".to_vec(), b"%2\r\n".to_vec()));
        assert_eq!(both(|w| w.set(5)), (b"*5\r\n".to_vec(), b"~5\r\n".to_vec()));
        assert_eq!(
            both(|w| w.push(2)),
            (b"*2\r\n".to_vec(), b">2\r\n".to_vec())
        );
        // HELLO-style map: same element sequence in both protocols
        let (r2, r3) = both(|w| {
            w.map(1);
            w.bulk(b"proto");
            w.int(3);
        });
        assert_eq!(r2, b"*2\r\n$5\r\nproto\r\n:3\r\n");
        assert_eq!(r3, b"%1\r\n$5\r\nproto\r\n:3\r\n");
    }

    #[test]
    fn hello_reply_matches_redis() {
        // Redis 7.0.15's reply to HELLO 3 / HELLO 2 (server and version differ by design)
        let (r2, r3) = both(|w| w.hello(b"redis", b"7.0.15", 3374, b"standalone", b"master"));
        assert_eq!(
            r3,
            &b"%7\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$6\r\n7.0.15\r\n$5\r\nproto\r\n:3\r\n$2\r\nid\r\n:3374\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n"[..]
        );
        assert!(r2.starts_with(b"*14\r\n$6\r\nserver\r\n"));
        assert!(r2.windows(11).any(|w| w == b"proto\r\n:2\r\n"));
    }

    #[test]
    fn resp3_only_types() {
        assert_eq!(
            both(|w| w.double(1.5)),
            (b"$3\r\n1.5\r\n".to_vec(), b",1.5\r\n".to_vec())
        );
        assert_eq!(both(|w| w.double(0.1)).1, b",0.10000000000000001\r\n");
        assert_eq!(
            both(|w| w.double(f64::INFINITY)),
            (b"$3\r\ninf\r\n".to_vec(), b",inf\r\n".to_vec())
        );
        assert_eq!(both(|w| w.double(f64::NEG_INFINITY)).1, b",-inf\r\n");
        assert_eq!(both(|w| w.double(1e17)).0, b"$5\r\n1e+17\r\n");
        assert_eq!(
            both(|w| w.boolean(true)),
            (b":1\r\n".to_vec(), b"#t\r\n".to_vec())
        );
        assert_eq!(
            both(|w| w.boolean(false)),
            (b":0\r\n".to_vec(), b"#f\r\n".to_vec())
        );
        assert_eq!(
            both(|w| w.verbatim(b"txt", b"# Server\r\n")),
            (
                b"$10\r\n# Server\r\n\r\n".to_vec(),
                b"=14\r\ntxt:# Server\r\n\r\n".to_vec()
            )
        );
        assert_eq!(
            both(|w| w.big_number(b"-3492890328409238509324850943850943825024385")),
            (
                b"$44\r\n-3492890328409238509324850943850943825024385\r\n".to_vec(),
                b"(-3492890328409238509324850943850943825024385\r\n".to_vec()
            )
        );
    }
}
