//! Property tests across the keyed handlers (DESIGN §8, §16.3, §25.2), run against the
//! reference model of `testkit` (the `Keyspace` contract):
//!
//! * `errors_stage_nothing`: a command that answers an error has staged no effect, and the
//!   read-only commands never stage one (the "validate before the first mutating call" rule of
//!   `cmd/mod.rs`, which makes a failed command safe to drop and re-run);
//! * `ttl_shorten` (LC-10): any write may set, shorten, lengthen or remove a TTL, and the
//!   NX/XX/GT/LT conditions of the EXPIRE family follow Redis's rules ("no TTL" is infinite);
//! * `incr_matches_wide_arithmetic`: INCR/DECR/INCRBY/DECRBY agree with 128-bit arithmetic
//!   and the i64 range (overflow is an error that changes nothing);
//! * `string_ops_match_byte_model`: APPEND/SETRANGE/GETRANGE/STRLEN/GET agree with a plain byte
//!   vector, for contiguous and chunked values;
//! * `gather_matches_single_partition`: a fan-out split of MGET/EXISTS/DEL/TOUCH gathered with
//!   `cmd::gather` replies exactly what the unsplit command replies (§6.8).

use kv_cmd::CmdId;
use kv_resp::{Proto, ReplyWriter};
use proptest::prelude::*;
use smallvec::SmallVec;

use super::testkit::{Model, run, run_argv, show};
use super::{FanOutSplit, gather};

/// A whole-second wall clock, so that second-based expiry arguments are exact.
const NOW: u64 = 1_700_000_000_000;

/// One live record: `((db, key), value, expiry)`.
type LiveRec = ((u8, Vec<u8>), Vec<u8>, Option<u64>);

/// The live records of the model (expired ones are absent), sorted.
fn live(m: &Model) -> Vec<LiveRec> {
    let mut v: Vec<_> = m
        .data
        .iter()
        .filter(|(_, r)| r.expire.is_none_or(|e| e > m.now))
        .map(|(k, r)| (k.clone(), r.value.clone(), r.expire))
        .collect();
    v.sort();
    v
}

/// The keyed commands of the M1 table that the random streams use.
const KEYED: &[&str] = &[
    "GET",
    "SET",
    "SETNX",
    "SETEX",
    "PSETEX",
    "GETSET",
    "GETDEL",
    "GETEX",
    "MGET",
    "MSET",
    "MSETNX",
    "APPEND",
    "STRLEN",
    "GETRANGE",
    "SUBSTR",
    "SETRANGE",
    "INCR",
    "DECR",
    "INCRBY",
    "DECRBY",
    "INCRBYFLOAT",
    "DEL",
    "UNLINK",
    "EXISTS",
    "TYPE",
    "RENAME",
    "RENAMENX",
    "COPY",
    "EXPIRE",
    "PEXPIRE",
    "EXPIREAT",
    "PEXPIREAT",
    "TTL",
    "PTTL",
    "EXPIRETIME",
    "PEXPIRETIME",
    "PERSIST",
    "TOUCH",
    "HSET",
    "HSETNX",
    "HGET",
    "HMGET",
    "HMSET",
    "HDEL",
    "HGETALL",
    "HKEYS",
    "HVALS",
    "HLEN",
    "HEXISTS",
    "HINCRBY",
    "HINCRBYFLOAT",
    "HSTRLEN",
];

/// Commands that never stage an effect.
const READ_ONLY: &[&str] = &[
    "GET",
    "MGET",
    "STRLEN",
    "GETRANGE",
    "SUBSTR",
    "EXISTS",
    "TYPE",
    "TTL",
    "PTTL",
    "EXPIRETIME",
    "PEXPIRETIME",
    "TOUCH",
    "HGET",
    "HMGET",
    "HGETALL",
    "HKEYS",
    "HVALS",
    "HLEN",
    "HEXISTS",
    "HSTRLEN",
];

/// Keys: a string holding an integer, a string with a TTL, a hash, a missing key.
const KEYS: &[&str] = &["k", "s", "h", "n"];

/// Argument tokens: keys of every kind, values, integers at the edges, floats, options.
const TOKENS: &[&str] = &[
    "k",
    "s",
    "h",
    "n",
    "k",
    "s",
    "h",
    "",
    "v",
    "10",
    "-3",
    "0",
    "1",
    "1.5",
    "abc",
    "+1",
    "01",
    "9223372036854775807",
    "-9223372036854775808",
    "9223372036854775808",
    "1e400",
    "inf",
    "NX",
    "xx",
    "GT",
    "lt",
    "GET",
    "KEEPTTL",
    "EX",
    "px",
    "EXAT",
    "PXAT",
    "PERSIST",
    "DB",
    "REPLACE",
    "1700000000",
    "1700000100000",
    "100",
    "-100",
    "2147483648",
    "536870912",
    "f",
];

fn preloaded() -> Model {
    let mut m = Model::new(NOW);
    m.set_str(b"k", b"5", None);
    m.set_str(b"s", b"hello", Some(NOW + 100_000));
    let _ = run(&mut m, "HSET h f 1 g x");
    m
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2048))]

    #[test]
    fn errors_stage_nothing(
        cmd in 0..KEYED.len(),
        key in 0..KEYS.len(),
        args in proptest::collection::vec(0..TOKENS.len(), 0..8),
        extra in 0usize..3,
        chunk in 0usize..3,
    ) {
        let mut m = preloaded();
        m.chunk = chunk;
        // mostly the command's own argument count (and one more or two for variadic ones), so
        // that the handlers rather than the arity check answer
        let spec = kv_cmd::lookup(KEYED[cmd].as_bytes()).expect("command in the table");
        let base = usize::from(spec.arity.unsigned_abs()).saturating_sub(2);
        let n = if spec.arity < 0 { base + extra } else { base };
        let mut argv: Vec<&[u8]> = vec![KEYED[cmd].as_bytes(), KEYS[key].as_bytes()];
        argv.extend(args.iter().cycle().take(n.min(args.len())).map(|&i| TOKENS[i].as_bytes()));
        let (before, writes) = (live(&m), m.writes);
        let reply = run_argv(&mut m, &argv, Proto::Resp2);
        if reply.first() == Some(&b'-') || READ_ONLY.contains(&KEYED[cmd]) {
            prop_assert_eq!(m.writes, writes, "{:?} -> {}", argv, show(&reply));
            prop_assert!(live(&m) == before, "{:?} -> {} changed the data", argv, show(&reply));
        }
    }

    /// `ttl_shorten` (LC-10).
    #[test]
    fn ttl_shorten(t0 in 1u64..1_000_000, t1 in 1u64..1_000_000, form in 0usize..11, cond in 0usize..5) {
        let ms = t1 * 1000;
        let at = NOW / 1000 + t1;
        let setters = [
            format!("EXPIRE k {t1}"),
            format!("PEXPIRE k {ms}"),
            format!("EXPIREAT k {at}"),
            format!("PEXPIREAT k {}", NOW + ms),
            format!("SET k 7 EX {t1}"),
            format!("SET k 7 PX {ms}"),
            format!("SET k 7 XX EXAT {at}"),
            format!("SETEX k {t1} 7"),
            format!("PSETEX k {ms} 7"),
            format!("GETEX k EX {t1}"),
            format!("GETEX k PXAT {}", NOW + ms),
        ];
        let mut m = Model::new(NOW);
        m.set_str(b"k", b"3", Some(NOW + t0 * 1000));
        // any write may set, shorten or lengthen the TTL
        let _ = run(&mut m, &setters[form]);
        prop_assert_eq!(show(&run(&mut m, "PTTL k")), format!(":{ms}\\r\\n"));

        // the EXPIRE-family conditions against the current TTL t1 (and against no TTL)
        let t2 = t0;
        let conds = ["", " NX", " XX", " GT", " LT"];
        let line = format!("EXPIRE k {t2}{}", conds[cond]);
        let allowed = match cond {
            0 => true,
            1 => false,
            2 => true,
            3 => t2 > t1,
            _ => t2 < t1,
        };
        prop_assert_eq!(show(&run(&mut m, &line)), format!(":{}\\r\\n", u8::from(allowed)));
        let want = if allowed { t2 } else { t1 };
        prop_assert_eq!(show(&run(&mut m, "TTL k")), format!(":{want}\\r\\n"));
        let _ = run(&mut m, "PERSIST k");
        let allowed = matches!(cond, 0 | 1 | 4);
        prop_assert_eq!(show(&run(&mut m, &line)), format!(":{}\\r\\n", u8::from(allowed)));

        // writes that keep the TTL, then writes that remove it
        let mut m = Model::new(NOW);
        m.set_str(b"k", b"3", Some(NOW + ms));
        for keep in ["APPEND k 1", "SETRANGE k 0 4", "INCR k", "INCRBYFLOAT k 1", "SET k 9 KEEPTTL", "GETEX k"] {
            let _ = run(&mut m, keep);
            prop_assert_eq!(show(&run(&mut m, "PTTL k")), format!(":{ms}\\r\\n"), "after {}", keep);
        }
        let removers = ["SET k 1", "GETSET k 1", "PERSIST k", "GETEX k PERSIST", "MSET k 1"];
        let _ = run(&mut m, removers[form % removers.len()]);
        prop_assert_eq!(show(&run(&mut m, "TTL k")), ":-1\\r\\n");
    }

    #[test]
    fn incr_matches_wide_arithmetic(start in any::<i64>(), n in any::<i64>(), op in 0usize..4, edge in 0usize..4) {
        // bias the start towards the ends of the range
        let start = match edge {
            0 => start,
            1 => i64::MAX - (start & 0xff),
            2 => i64::MIN + (start & 0xff),
            _ => start >> 40,
        };
        let mut m = Model::new(NOW);
        m.set_str(b"k", start.to_string().as_bytes(), None);
        let (line, delta) = match op {
            0 => ("INCR k".to_string(), Some(1i128)),
            1 => ("DECR k".to_string(), Some(-1)),
            2 => (format!("INCRBY k {n}"), Some(i128::from(n))),
            _ => (format!("DECRBY k {n}"), (n != i64::MIN).then(|| -i128::from(n))),
        };
        let got = show(&run(&mut m, &line));
        match delta.map(|d| i128::from(start) + d) {
            None => prop_assert_eq!(got, "-ERR decrement would overflow\\r\\n"),
            Some(r) if r > i128::from(i64::MAX) || r < i128::from(i64::MIN) => {
                prop_assert_eq!(got, "-ERR increment or decrement would overflow\\r\\n");
                prop_assert_eq!(show(&run(&mut m, "GET k")), show(format!("${}\r\n{start}\r\n", start.to_string().len()).as_bytes()));
            }
            Some(r) => {
                prop_assert_eq!(got, format!(":{r}\\r\\n"));
                prop_assert_eq!(show(&run(&mut m, "GET k")), show(format!("${}\r\n{r}\r\n", r.to_string().len()).as_bytes()));
            }
        }
    }

    #[test]
    fn string_ops_match_byte_model(
        ops in proptest::collection::vec((0usize..5, -24i64..24, -24i64..24, "[a-c]{0,6}"), 1..40),
        chunk in 0usize..4,
    ) {
        let mut m = Model::new(NOW);
        m.chunk = chunk;
        let mut model: Option<Vec<u8>> = None;
        for (op, a, b, data) in ops {
            match op {
                0 => {
                    let v = model.get_or_insert_with(Vec::new);
                    v.extend_from_slice(data.as_bytes());
                    let want = format!(":{}\r\n", v.len());
                    let got = run_argv(&mut m, &[b"APPEND", b"k", data.as_bytes()], Proto::Resp2);
                    prop_assert_eq!(show(&got), show(want.as_bytes()));
                }
                1 => {
                    let off = a.unsigned_abs() as usize;
                    let want = match &mut model {
                        None if data.is_empty() => 0,
                        v => {
                            let v = v.get_or_insert_with(Vec::new);
                            if !data.is_empty() {
                                if v.len() < off + data.len() {
                                    v.resize(off + data.len(), 0);
                                }
                                v[off..off + data.len()].copy_from_slice(data.as_bytes());
                            }
                            v.len()
                        }
                    };
                    let off_s = off.to_string();
                    let got = run_argv(&mut m, &[b"SETRANGE", b"k", off_s.as_bytes(), data.as_bytes()], Proto::Resp2);
                    prop_assert_eq!(show(&got), format!(":{want}\\r\\n"));
                }
                2 => {
                    // Redis 7.0 getrangeCommand, in wide arithmetic
                    let v = model.as_deref().unwrap_or(b"");
                    let len = v.len() as i128;
                    let (mut s, mut e) = (i128::from(a), i128::from(b));
                    let empty = s < 0 && e < 0 && s > e;
                    if s < 0 { s += len; }
                    if e < 0 { e += len; }
                    s = s.max(0);
                    e = e.max(0).min(len - 1);
                    let want: &[u8] = if empty || s > e || len == 0 { b"" } else { &v[s as usize..=e as usize] };
                    let (a_s, b_s) = (a.to_string(), b.to_string());
                    let got = run_argv(&mut m, &[b"GETRANGE", b"k", a_s.as_bytes(), b_s.as_bytes()], Proto::Resp2);
                    let mut exp = format!("${}\r\n", want.len()).into_bytes();
                    exp.extend_from_slice(want);
                    exp.extend_from_slice(b"\r\n");
                    prop_assert_eq!(show(&got), show(&exp), "GETRANGE k {} {}", a, b);
                }
                3 => {
                    let n = model.as_ref().map_or(0, Vec::len);
                    prop_assert_eq!(show(&run(&mut m, "STRLEN k")), format!(":{n}\\r\\n"));
                }
                _ => {
                    let got = run(&mut m, "GET k");
                    let exp = match &model {
                        None => b"$-1\r\n".to_vec(),
                        Some(v) => {
                            let mut e = format!("${}\r\n", v.len()).into_bytes();
                            e.extend_from_slice(v);
                            e.extend_from_slice(b"\r\n");
                            e
                        }
                    };
                    prop_assert_eq!(show(&got), show(&exp));
                }
            }
        }
    }

    #[test]
    fn gather_matches_single_partition(
        keys in proptest::collection::vec(0u8..8, 1..12),
        mult in 1u16..97,
        parts in 1u16..6,
        cmd in 0usize..4,
        resp3 in any::<bool>(),
    ) {
        let proto = if resp3 { Proto::Resp3 } else { Proto::Resp2 };
        let mut m = Model::new(NOW);
        for k in [0u8, 2, 3, 5] {
            m.set_str(format!("key{k}").as_bytes(), format!("v{k}").as_bytes(), None);
        }
        let _ = run(&mut m, "HSET key6 f 1");
        let (name, id): (&[u8], CmdId) = [
            (&b"MGET"[..], CmdId::Mget),
            (b"EXISTS", CmdId::Exists),
            (b"DEL", CmdId::Del),
            (b"TOUCH", CmdId::Touch),
        ][cmd];
        let names: Vec<Vec<u8>> = keys.iter().map(|k| format!("key{k}").into_bytes()).collect();
        let mut argv: Vec<&[u8]> = vec![name];
        argv.extend(names.iter().map(Vec::as_slice));

        // the split of §6.8: one part per distinct partition, in order of first appearance
        let mut split = FanOutSplit { parts: SmallVec::new() };
        for (i, &k) in keys.iter().enumerate() {
            let p = (u16::from(k) * mult) % parts;
            let ix = u16::try_from(i + 1).unwrap();
            match split.parts.iter_mut().find(|(q, _)| *q == p) {
                Some((_, v)) => v.push(ix),
                None => split.parts.push((p, SmallVec::from_slice(&[ix]))),
            }
        }
        let width = split.parts.len();
        let mut distinct: Vec<u16> = keys.iter().map(|&k| (u16::from(k) * mult) % parts).collect();
        distinct.sort_unstable();
        distinct.dedup();
        prop_assert_eq!(width, distinct.len(), "fan-out width = distinct partitions (LC-37)");

        let mut whole = m.clone();
        let want = run_argv(&mut whole, &argv, proto);
        let replies: Vec<Vec<u8>> = (0..width)
            .map(|i| run_argv(&mut m, &split.sub_argv(i, &argv), proto))
            .collect();
        let refs: Vec<&[u8]> = replies.iter().map(Vec::as_slice).collect();
        let mut got = Vec::new();
        gather(id, &split, argv.len(), &refs, &mut ReplyWriter::new(&mut got, proto));
        prop_assert_eq!(show(&got), show(&want));
        prop_assert!(live(&m) == live(&whole), "the parts did what the whole command does");
    }
}
