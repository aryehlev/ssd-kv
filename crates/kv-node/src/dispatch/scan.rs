//! SCAN, KEYS and RANDOMKEY driving (DESIGN §16.6, LC-17, LC-18).
//!
//! These commands are registered as `Handler::Scan`: the runtime runs them. The connection's
//! home worker parses the arguments ([`parse_scan`], [`parse_keys`]) and sends one step at a
//! time ([`ScanReq`]) to the worker owning the cursor's partition, which runs it with
//! `PartitionStore::scan_keys` on that one thread and answers with the keys and the next cursor
//! ([`ScanOutcome`]). SCAN is one step; KEYS runs steps of COUNT 1000 over all partitions
//! strictly one after another, at most one step in flight per call (test `keys_sequential`);
//! RANDOMKEY is one step on one partition. No SCAN-family request is fanned out across workers,
//! and only key names travel (MATCH and TYPE are fixed built-in filters).

use kv_index::VType;
use kv_storage::keyspace::StoreError;

/// COUNT of each KEYS step.
pub const KEYS_STEP_COUNT: u32 = 1000;

/// Default SCAN COUNT (Redis).
pub const SCAN_DEFAULT_COUNT: u32 = 10;

/// What a step is for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StepKind {
    Scan,
    Keys,
    /// RANDOMKEY with its random draw.
    RandomKey { r: u64 },
}

/// One step on one partition (key bytes only; holds a user pattern: no `Debug`, LC-32).
pub struct ScanReq {
    pub kind: StepKind,
    pub partition: u16,
    pub db: u8,
    /// Global cursor (`partition << 48 | slot_local << 32 | fp32`).
    pub cursor: u64,
    pub count: u32,
    pub vtype: Option<VType>,
    /// MATCH pattern; `None` = every key (also for the pattern `*`).
    pub pattern: Option<Box<[u8]>>,
    pub proto: kv_resp::Proto,
}

/// The answer to a step: the next global cursor and the keys found.
pub struct ScanOutcome {
    pub res: Result<(u64, Vec<Box<[u8]>>), StoreError>,
}

/// Partition of a global cursor.
#[inline]
pub fn cursor_partition(cursor: u64) -> u16 {
    (cursor >> 48) as u16
}

/// Parsed `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]`.
pub struct ScanArgs<'a> {
    pub cursor: u64,
    pub count: u32,
    pub pattern: Option<&'a [u8]>,
    /// `Some(None)`: a TYPE that no key can have (Redis 7.0 returns no keys, no error).
    pub vtype: Option<Option<VType>>,
}

/// Redis 7.0.15 `scanGenericCommand` argument rules; `Err` carries the error text.
pub fn parse_scan<'a>(argv: &[&'a [u8]]) -> Result<ScanArgs<'a>, &'static [u8]> {
    let cursor = kv_common::num::parse_scan_cursor(argv[1]).ok_or(&b"ERR invalid cursor"[..])?;
    let mut a = ScanArgs { cursor, count: SCAN_DEFAULT_COUNT, pattern: None, vtype: None };
    let mut j = 2;
    while j < argv.len() {
        let more = argv.len() - j - 1;
        let opt = argv[j];
        if opt.eq_ignore_ascii_case(b"count") && more >= 1 {
            let n = kv_common::num::string2l(argv[j + 1]).ok_or(&b"ERR value is not an integer or out of range"[..])?;
            if n < 1 {
                return Err(b"ERR syntax error");
            }
            a.count = u32::try_from(n).unwrap_or(u32::MAX);
            j += 2;
        } else if opt.eq_ignore_ascii_case(b"match") && more >= 1 {
            let p = argv[j + 1];
            a.pattern = if kv_common::glob::is_match_all(p) { None } else { Some(p) };
            j += 2;
        } else if opt.eq_ignore_ascii_case(b"type") && more >= 1 {
            let t = argv[j + 1];
            a.vtype = Some(if t.eq_ignore_ascii_case(b"string") {
                Some(VType::String)
            } else if t.eq_ignore_ascii_case(b"hash") {
                Some(VType::Hash)
            } else {
                None
            });
            j += 2;
        } else {
            return Err(b"ERR syntax error");
        }
    }
    Ok(a)
}

/// The KEYS pattern (`None` = every key).
pub fn parse_keys<'a>(argv: &[&'a [u8]]) -> Option<&'a [u8]> {
    let p = argv[1];
    if kv_common::glob::is_match_all(p) { None } else { Some(p) }
}

/// The key filter of a step: MATCH (Redis glob) or everything.
pub fn matcher(pattern: Option<&[u8]>) -> impl Fn(&[u8]) -> bool + '_ {
    move |k: &[u8]| pattern.is_none_or(|p| kv_common::glob::glob_match(p, k, false))
}

/// Writes a SCAN reply: `[cursor as bulk, [keys...]]`.
pub fn write_scan_reply(next: u64, keys: &[Box<[u8]>], w: &mut kv_resp::ReplyWriter<'_>) {
    w.array(2);
    let mut b = Vec::with_capacity(20);
    kv_common::num::ull2buf(next, &mut b);
    w.bulk(&b);
    w.array(keys.len());
    for k in keys {
        w.bulk(k);
    }
}

/// Writes a KEYS reply.
pub fn write_keys_reply(keys: &[Box<[u8]>], w: &mut kv_resp::ReplyWriter<'_>) {
    w.array(keys.len());
    for k in keys {
        w.bulk(k);
    }
}

/// KEYS state on the home worker: the accumulated keys and the next cursor.
pub struct KeysState {
    pub db: u8,
    pub pattern: Option<Box<[u8]>>,
    pub keys: Vec<Box<[u8]>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err(argv: &[&[u8]]) -> &'static [u8] {
        match parse_scan(argv) {
            Err(e) => e,
            Ok(_) => panic!("expected an error"),
        }
    }

    #[test]
    fn scan_arguments_follow_redis() {
        let a = parse_scan(&[b"SCAN", b"0"]).unwrap();
        assert_eq!((a.cursor, a.count, a.pattern, a.vtype), (0, 10, None, None));
        let a = parse_scan(&[b"scan", b"5", b"MATCH", b"a*", b"count", b"100", b"TYPE", b"string"]).unwrap();
        assert_eq!((a.cursor, a.count, a.pattern, a.vtype), (5, 100, Some(&b"a*"[..]), Some(Some(VType::String))));
        assert_eq!(parse_scan(&[b"SCAN", b"0", b"MATCH", b"*"]).unwrap().pattern, None);
        assert_eq!(parse_scan(&[b"SCAN", b"0", b"TYPE", b"list"]).unwrap().vtype, Some(None));
        assert_eq!(err(&[b"SCAN", b"x"]), b"ERR invalid cursor");
        assert_eq!(err(&[b"SCAN", b"0", b"COUNT", b"0"]), b"ERR syntax error");
        assert_eq!(err(&[b"SCAN", b"0", b"COUNT", b"x"]), b"ERR value is not an integer or out of range");
        assert_eq!(err(&[b"SCAN", b"0", b"COUNT"]), b"ERR syntax error");
        assert_eq!(err(&[b"SCAN", b"0", b"FOO", b"1"]), b"ERR syntax error");
    }

    #[test]
    fn replies_and_matchers() {
        let mut out = Vec::new();
        let keys: Vec<Box<[u8]>> = vec![b"a".to_vec().into(), b"bc".to_vec().into()];
        write_scan_reply(1 << 48, &keys, &mut kv_resp::ReplyWriter::new(&mut out, kv_resp::Proto::Resp2));
        assert_eq!(out, b"*2\r\n$15\r\n281474976710656\r\n*2\r\n$1\r\na\r\n$2\r\nbc\r\n");
        assert_eq!(cursor_partition(1 << 48 | 77), 1);
        let m = matcher(Some(b"a*"));
        assert!(m(b"abc") && !m(b"b"));
        let all = matcher(None);
        assert!(all(b""));
        assert_eq!(parse_keys(&[b"KEYS", b"*"]), None);
        assert_eq!(parse_keys(&[b"KEYS", b"h?llo"]), Some(&b"h?llo"[..]));
    }
}
