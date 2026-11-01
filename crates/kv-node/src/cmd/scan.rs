//! SCAN and KEYS (DESIGN §16.6, LC-17, LC-18).
//!
//! The cursor is one scalar `partition:16 | slot_local:16 | fp32`. A SCAN call runs entirely on
//! the worker owning the cursor partition: page-reference walk of the slot tables (filters
//! applied inside the walk), prefetch of at most `SCAN_STEP_MAX_PAGES` pages, then key hashes
//! recomputed from the bytes read and compared with the span bounds; only key names are emitted
//! (MATCH glob and TYPE are fixed built-in filters). KEYS runs SCAN steps with COUNT 1000 over
//! all partitions strictly one at a time, at most one step in flight per call; no SCAN-family
//! request is fanned out across workers. Cluster mode walks only slots `slot_filter()` serves.
//!
//! This module holds the command side of that machinery: argument parsing with Redis 7.0's
//! errors ([`parse_scan`]), the partition request of one step ([`scan_request`]), the MATCH
//! predicate ([`key_matcher`]), the KEYS walk state ([`KeysWalk`]: one step at a time), the
//! RANDOMKEY partition choice ([`random_partition`]) and the replies. The page-reference walk
//! itself is `PartitionStore::scan_keys`.

use kv_common::glob;
use kv_index::{VType, cursor};
use kv_resp::ReplyWriter;
use kv_storage::partition::{Origin, ReqKind, Request};
use smallvec::SmallVec;

use super::{err, is_opt, parse_i64};
use crate::shared::NodeShared;

/// The TYPE filter of SCAN.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TypeFilter {
    /// No TYPE option.
    Any,
    /// Only keys of this type (applied inside the page-reference walk).
    Only(VType),
    /// A type no key here can have (`list`, `zset`, unknown names, ...): the iteration is
    /// empty, as Redis's filter would drop every key.
    Nothing,
}

/// Parsed SCAN arguments (holds the MATCH pattern: no `Debug`, LC-32).
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ScanArgs<'a> {
    pub cursor: u64,
    /// MATCH pattern (`None` or `*`: every key).
    pub pattern: Option<&'a [u8]>,
    /// COUNT: entries walked per step (bounds work, not results).
    pub count: u32,
    pub type_filter: TypeFilter,
}

/// Default COUNT (Redis).
pub const DEFAULT_COUNT: u32 = 10;
/// COUNT of each KEYS step (§16.6).
pub const KEYS_STEP_COUNT: u32 = 1000;

/// Parses `SCAN cursor [MATCH p] [COUNT n] [TYPE t]` exactly as Redis 7.0: the cursor first
/// (`-ERR invalid cursor`), then the options left to right (COUNT must be an integer >= 1, an
/// option without its argument or an unknown option is a syntax error). Writes the error and
/// returns `None` on failure.
pub fn parse_scan<'a>(argv: &[&'a [u8]], w: &mut ReplyWriter<'_>) -> Option<ScanArgs<'a>> {
    let Some(cursor) = kv_common::num::parse_scan_cursor(argv[1]) else {
        w.error(err::INVALID_CURSOR);
        return None;
    };
    let mut a = ScanArgs {
        cursor,
        pattern: None,
        count: DEFAULT_COUNT,
        type_filter: TypeFilter::Any,
    };
    let mut i = 2;
    while i < argv.len() {
        let has_arg = argv.len() - i >= 2;
        if is_opt(argv[i], b"count") && has_arg {
            match parse_i64(argv[i + 1]) {
                None => {
                    w.error(err::NOT_INTEGER);
                    return None;
                }
                Some(n) if n < 1 => {
                    w.error(err::SYNTAX);
                    return None;
                }
                Some(n) => a.count = u32::try_from(n).unwrap_or(u32::MAX),
            }
        } else if is_opt(argv[i], b"match") && has_arg {
            a.pattern = Some(argv[i + 1]);
        } else if is_opt(argv[i], b"type") && has_arg {
            let t = super::c_str(argv[i + 1]);
            a.type_filter = if t.eq_ignore_ascii_case(b"string") {
                TypeFilter::Only(VType::String)
            } else if t.eq_ignore_ascii_case(b"hash") {
                TypeFilter::Only(VType::Hash)
            } else {
                TypeFilter::Nothing
            };
        } else {
            w.error(err::SYNTAX);
            return None;
        }
        i += 2;
    }
    Some(a)
}

/// The MATCH predicate: Redis `stringmatchlen` (case-sensitive), with `*` matching every key
/// (including the empty key) without running the matcher.
pub fn key_matcher(pattern: Option<&[u8]>) -> impl Fn(&[u8]) -> bool + '_ {
    let all = pattern.is_none_or(glob::is_match_all);
    move |key: &[u8]| all || glob::glob_match(pattern.unwrap_or(b"*"), key, false)
}

/// The partition request of one SCAN step at `cursor` over `db`, and the partition it goes to;
/// `None` when the cursor is past the last partition (the iteration is over).
pub fn scan_request(
    db: u8,
    cursor: u64,
    count: u32,
    vtype: Option<VType>,
    partitions: u16,
) -> Option<(u16, Request)> {
    let (partition, _, _) = cursor::decode(cursor);
    (partition < partitions).then(|| {
        (
            partition,
            Request {
                db,
                keys: SmallVec::new(),
                kind: ReqKind::ScanStep {
                    db,
                    cursor,
                    count,
                    vtype,
                },
                origin: Origin::Client,
            },
        )
    })
}

/// The cursor returned to the client after a step: the partition's next position, or 0 once
/// it points past the last partition.
pub fn client_cursor(next: u64, partitions: u16) -> u64 {
    let (partition, _, _) = cursor::decode(next);
    if partition >= partitions { 0 } else { next }
}

/// What a SCAN call does after its arguments were parsed.
pub enum ScanPlan<'a> {
    /// Reply at once (the iteration is over or cannot return anything).
    Reply { next: u64 },
    /// Run one step on `partition`, filter keys with [`key_matcher`]`(pattern)`, then reply
    /// with [`write_scan_reply`] and [`client_cursor`] of the step's next cursor.
    Step {
        partition: u16,
        request: Request,
        pattern: Option<&'a [u8]>,
    },
}

/// Plans one SCAN call (`db` is the connection's database).
pub fn plan_scan<'a>(db: u8, a: &ScanArgs<'a>, partitions: u16) -> ScanPlan<'a> {
    let vtype = match a.type_filter {
        TypeFilter::Any => None,
        TypeFilter::Only(t) => Some(t),
        TypeFilter::Nothing => return ScanPlan::Reply { next: 0 },
    };
    match scan_request(db, a.cursor, a.count, vtype, partitions) {
        None => ScanPlan::Reply { next: 0 },
        Some((partition, request)) => ScanPlan::Step {
            partition,
            request,
            pattern: a.pattern,
        },
    }
}

/// `[cursor, [key, ...]]` (the cursor as a decimal bulk, as Redis replies it).
pub fn write_scan_reply(next: u64, keys: &[Box<[u8]>], w: &mut ReplyWriter<'_>) {
    w.array(2);
    w.bulk(super::Decimal::u64(next).as_bytes());
    w.array(keys.len());
    for k in keys {
        w.bulk(k);
    }
}

/// The state of one KEYS call: SCAN steps with COUNT 1000 over the partitions strictly one
/// after another; the caller issues the next step only after the previous one completed
/// (`keys_sequential`, LC-18).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct KeysWalk {
    pub db: u8,
    partitions: u16,
    cursor: u64,
    done: bool,
}

impl KeysWalk {
    pub fn new(db: u8, partitions: u16) -> Self {
        KeysWalk {
            db,
            partitions,
            cursor: 0,
            done: partitions == 0,
        }
    }

    /// The next step, or `None` when the walk is complete.
    pub fn next_step(&self) -> Option<(u16, Request)> {
        if self.done {
            return None;
        }
        scan_request(self.db, self.cursor, KEYS_STEP_COUNT, None, self.partitions)
    }

    /// Records the next cursor returned by the step just completed.
    pub fn on_step_done(&mut self, next: u64) {
        self.cursor = client_cursor(next, self.partitions);
        self.done = self.cursor == 0;
    }

    pub fn is_done(&self) -> bool {
        self.done
    }
}

/// The KEYS reply.
pub fn write_keys_reply(keys: &[Box<[u8]>], w: &mut ReplyWriter<'_>) {
    w.array(keys.len());
    for k in keys {
        w.bulk(k);
    }
}

/// RANDOMKEY's partition: drawn with probability proportional to its published key count in
/// `db` (so every key is about equally likely); `None` when the database is empty.
pub fn random_partition(shared: &NodeShared, db: u8, r: u64) -> Option<u16> {
    let total = shared.dbsize(db);
    if total == 0 {
        return None;
    }
    let mut pick = r % total;
    for (p, c) in shared.counters.iter().enumerate() {
        let n = c.keys(db);
        if pick < n {
            return u16::try_from(p).ok();
        }
        pick -= n;
    }
    None
}

/// The partition request of one RANDOMKEY attempt (`r` picks the table and entry).
pub fn randomkey_request(db: u8, r: u64) -> Request {
    Request {
        db,
        keys: SmallVec::new(),
        kind: ReqKind::RandomKey { db, r },
        origin: Origin::Client,
    }
}

/// Attempts of RANDOMKEY before replying nil (expired entries and fingerprint false positives
/// are retried, §16.6).
pub const RANDOMKEY_ATTEMPTS: u32 = 16;

/// The RANDOMKEY reply.
pub fn write_randomkey_reply(key: Option<&[u8]>, w: &mut ReplyWriter<'_>) {
    match key {
        Some(k) => w.bulk(k),
        None => w.null(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kv_resp::Proto;

    fn parse(line: &str) -> Result<ScanArgs<'static>, String> {
        let argv: Vec<&'static [u8]> = line
            .split_ascii_whitespace()
            .map(|s| &*Box::leak(s.as_bytes().to_vec().into_boxed_slice()))
            .collect();
        let mut out = Vec::new();
        let mut w = ReplyWriter::new(&mut out, Proto::Resp2);
        parse_scan(&argv, &mut w).ok_or_else(|| String::from_utf8(out).unwrap())
    }

    #[test]
    fn scan_argument_errors() {
        assert_eq!(parse("SCAN abc").err().unwrap(), "-ERR invalid cursor\r\n");
        assert_eq!(
            parse("SCAN abc COUNT 0").err().unwrap(),
            "-ERR invalid cursor\r\n"
        );
        assert_eq!(
            parse("SCAN 0 COUNT 0").err().unwrap(),
            "-ERR syntax error\r\n"
        );
        assert_eq!(
            parse("SCAN 0 COUNT -5").err().unwrap(),
            "-ERR syntax error\r\n"
        );
        assert_eq!(
            parse("SCAN 0 COUNT abc").err().unwrap(),
            "-ERR value is not an integer or out of range\r\n"
        );
        assert_eq!(
            parse("SCAN 0 COUNT").err().unwrap(),
            "-ERR syntax error\r\n"
        );
        assert_eq!(
            parse("SCAN 0 MATCH").err().unwrap(),
            "-ERR syntax error\r\n"
        );
        assert_eq!(
            parse("SCAN 0 foo bar").err().unwrap(),
            "-ERR syntax error\r\n"
        );
        assert_eq!(
            parse("SCAN 0 COUNT abc MATCH").err().unwrap(),
            "-ERR value is not an integer or out of range\r\n"
        );
    }

    #[test]
    fn scan_arguments() {
        let a = parse("SCAN 18446744073709551615 match a* COUNT 9223372036854775807 type STRING")
            .unwrap();
        assert_eq!(a.cursor, u64::MAX);
        assert_eq!(a.pattern, Some(&b"a*"[..]));
        assert_eq!(a.count, u32::MAX);
        assert_eq!(a.type_filter, TypeFilter::Only(VType::String));
        assert_eq!(
            parse("SCAN 0 TYPE list").unwrap().type_filter,
            TypeFilter::Nothing
        );
        assert_eq!(
            parse("SCAN 0 TYPE hash").unwrap().type_filter,
            TypeFilter::Only(VType::Hash)
        );
        assert_eq!(
            parse("SCAN 0 COUNT 5 COUNT 7").unwrap().count,
            7,
            "the last option wins"
        );
        assert_eq!(
            parse("SCAN -1").unwrap().cursor,
            u64::MAX,
            "strtoul negation"
        );
        assert_eq!(parse("SCAN 0").unwrap().count, DEFAULT_COUNT);
    }

    #[test]
    fn matcher_rules() {
        let all = key_matcher(Some(b"*"));
        assert!(all(b"") && all(b"anything"));
        let none = key_matcher(None);
        assert!(none(b""));
        let m = key_matcher(Some(b"h[ae]llo"));
        assert!(m(b"hello") && m(b"hallo") && !m(b"hillo") && !m(b"HELLO"));
        let lit = key_matcher(Some(b"a*b"));
        assert!(lit(b"a*b") && lit(b"axxb") && !lit(b""));
    }

    #[test]
    fn plans_and_cursors() {
        let a = parse("SCAN 0 TYPE zset").unwrap();
        assert!(matches!(plan_scan(0, &a, 64), ScanPlan::Reply { next: 0 }));
        let a = parse("SCAN 0 MATCH k* COUNT 20 TYPE hash").unwrap();
        let ScanPlan::Step {
            partition,
            request,
            pattern,
        } = plan_scan(3, &a, 64)
        else {
            panic!()
        };
        assert_eq!(partition, 0);
        assert_eq!(pattern, Some(&b"k*"[..]));
        assert_eq!(
            request.kind,
            ReqKind::ScanStep {
                db: 3,
                cursor: 0,
                count: 20,
                vtype: Some(VType::Hash)
            }
        );
        let c = cursor::encode(5, 2, 77);
        let a = parse(&format!("SCAN {c}")).unwrap();
        let ScanPlan::Step { partition, .. } = plan_scan(0, &a, 64) else {
            panic!()
        };
        assert_eq!(partition, 5);
        let past = cursor::encode(64, 0, 0);
        assert!(matches!(
            plan_scan(0, &parse(&format!("SCAN {past}")).unwrap(), 64),
            ScanPlan::Reply { next: 0 }
        ));
        assert_eq!(client_cursor(past, 64), 0);
        assert_eq!(client_cursor(c, 64), c);

        let mut out = Vec::new();
        write_scan_reply(
            c,
            &[b"a".to_vec().into_boxed_slice()],
            &mut ReplyWriter::new(&mut out, Proto::Resp3),
        );
        assert_eq!(
            out,
            format!("*2\r\n${}\r\n{c}\r\n*1\r\n$1\r\na\r\n", c.to_string().len()).into_bytes()
        );
    }

    #[test]
    fn keys_walk_is_sequential() {
        let mut k = KeysWalk::new(2, 3);
        let mut seen = Vec::new();
        while let Some((p, req)) = k.next_step() {
            seen.push(p);
            let ReqKind::ScanStep {
                db,
                cursor,
                count,
                vtype,
            } = req.kind
            else {
                panic!()
            };
            assert_eq!((db, count, vtype), (2, KEYS_STEP_COUNT, None));
            // a partition takes two steps: one mid-partition, then the next partition's start
            let (cp, _, fp) = cursor::decode(cursor);
            let next = if fp == 0 {
                cursor::encode(cp, 0, 99)
            } else {
                cursor::encode(cp + 1, 0, 0)
            };
            k.on_step_done(next);
        }
        assert_eq!(seen, [0, 0, 1, 1, 2, 2]);
        assert!(k.is_done());
        assert!(KeysWalk::new(0, 0).next_step().is_none());
    }

    #[test]
    fn random_partition_is_weighted_by_keys() {
        let s = NodeShared::new(crate::config::Config::default(), 4, 1).unwrap();
        assert_eq!(random_partition(&s, 0, 7), None);
        s.counters[1].keys[0].store(2, std::sync::atomic::Ordering::Relaxed);
        s.counters[3].keys[0].store(1, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(random_partition(&s, 0, 0), Some(1));
        assert_eq!(random_partition(&s, 0, 1), Some(1));
        assert_eq!(random_partition(&s, 0, 2), Some(3));
        assert_eq!(random_partition(&s, 0, 5), Some(3));
        assert_eq!(random_partition(&s, 1, 5), None);
        assert_eq!(
            randomkey_request(1, 9).kind,
            ReqKind::RandomKey { db: 1, r: 9 }
        );
        let mut out = Vec::new();
        write_randomkey_reply(None, &mut ReplyWriter::new(&mut out, Proto::Resp2));
        assert_eq!(out, b"$-1\r\n");
    }
}
