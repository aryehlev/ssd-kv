//! Key commands (DESIGN §8.4, §16.1, §16.2): DEL / UNLINK (reply after the durable delete),
//! EXISTS (duplicates counted each time), TYPE, RENAME / RENAMENX (TTL carried, crash-atomic,
//! §14.6), COPY (M1.3) and RANDOMKEY (a random non-empty table of a random partition, one
//! partition per call).
//!
//! The handlers here run when all of a command's keys live in one partition (always in cluster
//! mode, where they share a slot). Standalone commands whose keys span partitions are split by
//! the dispatcher (DEL/UNLINK/EXISTS: one sub-request per partition, gathered by
//! `cmd::gather`) or run as a cross-partition transaction (RENAME, RENAMENX, COPY). RANDOMKEY is
//! a partition walk (`scan.rs`). DEL and UNLINK are the same command: freeing is a page write.

use kv_index::VType;
use kv_resp::ReplyWriter;
use kv_storage::keyspace::StoreError;

use super::store::{self, Store};
use super::{Args, err, is_opt, parse_i64};
use crate::runtime::xmsg::ExecEnv;

/// DEL / UNLINK k..: the number of keys that existed (a repeated key counts once).
pub fn del<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    let mut n = 0i64;
    for &key in &a.argv[1..] {
        n += i64::from(ks.delete(key)?);
    }
    w.int(n);
    Ok(())
}

/// EXISTS k..: the number of existing keys, a repeated key counted each time.
pub fn exists<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    let mut n = 0i64;
    for &key in &a.argv[1..] {
        n += i64::from(ks.meta(key)?.is_some());
    }
    w.int(n);
    Ok(())
}

/// The Redis type name of a value type.
pub(crate) fn type_name(t: VType) -> &'static [u8] {
    match t {
        VType::String => b"string",
        VType::Hash => b"hash",
    }
}

/// TYPE k: `+string`, `+hash` or `+none`.
pub fn type_<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    match ks.meta(a.arg(1))? {
        Some(m) => w.simple(type_name(m.vtype)),
        None => w.simple(b"none"),
    }
    Ok(())
}

/// Redis 7.0 `renameGenericCommand`: a missing source is `-ERR no such key` (also when source
/// and destination are equal); equal names are a no-op; the destination takes the source's
/// value, type and TTL, replacing whatever it held (RENAMENX: `:0` if it exists).
fn rename_generic<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    nx: bool,
) -> Result<(), StoreError> {
    let (src, dst) = (a.arg(1), a.arg(2));
    let (meta, value) = match ks.get(src)? {
        None => {
            w.error(err::NO_SUCH_KEY);
            return Ok(());
        }
        Some(_) if src == dst => {
            if nx {
                w.int(0);
            } else {
                w.ok();
            }
            return Ok(());
        }
        Some((m, v)) => (m, store::to_vec(&v)),
    };
    if nx && ks.meta(dst)?.is_some() {
        w.int(0);
        return Ok(());
    }
    ks.put(dst, &value, meta.vtype, meta.expire_at_ms)?;
    ks.delete(src)?;
    if nx {
        w.int(1);
    } else {
        w.ok();
    }
    Ok(())
}

/// RENAME src dst.
pub fn rename<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    rename_generic(ks, a, w, false)
}

/// RENAMENX src dst.
pub fn renamenx<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    rename_generic(ks, a, w, true)
}

/// Options of COPY src dst [DB n] [REPLACE].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct CopyOpts {
    /// Destination database (`None` = the source's).
    pub db: Option<i64>,
    pub replace: bool,
}

/// Why COPY's options were refused (Redis 7.0 `copyCommand`, in its order).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum CopyOptsError {
    Syntax,
    NotInteger,
    DbRange,
}

/// Parses `argv[3..]` of COPY. `databases` bounds the DB option (`0..databases`).
pub(crate) fn parse_copy_opts(argv: &[&[u8]], databases: u8) -> Result<CopyOpts, CopyOptsError> {
    let mut o = CopyOpts {
        db: None,
        replace: false,
    };
    let mut j = 3;
    while j < argv.len() {
        let more = argv.len() - j - 1;
        if is_opt(argv[j], b"replace") {
            o.replace = true;
        } else if is_opt(argv[j], b"db") && more >= 1 {
            // getIntFromObjectOrReply: a 32-bit int
            let db = parse_i64(argv[j + 1])
                .filter(|v| i32::try_from(*v).is_ok())
                .ok_or(CopyOptsError::NotInteger)?;
            if db < 0 || db >= i64::from(databases) {
                return Err(CopyOptsError::DbRange);
            }
            o.db = Some(db);
            j += 1;
        } else {
            return Err(CopyOptsError::Syntax);
        }
        j += 1;
    }
    Ok(o)
}

impl CopyOptsError {
    fn write(self, w: &mut ReplyWriter<'_>) {
        w.error(match self {
            CopyOptsError::Syntax => err::SYNTAX,
            CopyOptsError::NotInteger => err::NOT_INTEGER,
            CopyOptsError::DbRange => err::DB_RANGE,
        });
    }
}

/// The databases a partition request can address (`PartitionCounters` keeps 16).
const MAX_DATABASES: u8 = 16;

/// COPY's option errors, answered before routing (needs the configured database count).
pub(crate) fn precheck_copy(
    argv: &[&[u8]],
    databases: u8,
    cluster: bool,
    w: &mut ReplyWriter<'_>,
) -> bool {
    match parse_copy_opts(argv, databases) {
        Err(e) => {
            e.write(w);
            false
        }
        // in cluster mode only database 0 exists (and the connection is always on it)
        Ok(CopyOpts { db: Some(db), .. }) if cluster && db != 0 => {
            w.error(b"ERR Copying to another database is not allowed in cluster mode");
            false
        }
        Ok(_) => true,
    }
}

/// COPY src dst [DB n] [REPLACE] (Redis 7.0 `copyCommand`): `:1` when copied with the source's
/// type and TTL, `:0` when the source is missing or the destination exists without REPLACE.
pub fn copy<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    let o = match parse_copy_opts(&a.argv, MAX_DATABASES) {
        Ok(o) => o,
        Err(e) => {
            e.write(w);
            return Ok(());
        }
    };
    let src_db = ks.db();
    let dst_db = o.db.map_or(src_db, |d| d as u8);
    let (src, dst) = (a.arg(1), a.arg(2));
    if src_db == dst_db && src == dst {
        w.error(b"ERR source and destination objects are the same");
        return Ok(());
    }
    let (meta, value) = match ks.get(src)? {
        None => {
            w.int(0);
            return Ok(());
        }
        Some((m, v)) => (m, store::to_vec(&v)),
    };
    ks.select(dst_db);
    let result = (|| {
        if ks.meta(dst)?.is_some() && !o.replace {
            return Ok(false);
        }
        ks.put(dst, &value, meta.vtype, meta.expire_at_ms)?;
        Ok(true)
    })();
    ks.select(src_db);
    w.int(i64::from(result?));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::testkit::{Model, Rec, check, run};

    const NOW: u64 = 1_700_000_000_000;

    #[test]
    fn del_exists_type() {
        let mut m = Model::new(NOW);
        check(&mut m, "MSET a 1 b 2", "+OK\r\n");
        check(&mut m, "DEL a a b c", ":2\r\n");
        check(&mut m, "MSET a 1 b 2", "+OK\r\n");
        check(&mut m, "UNLINK a c", ":1\r\n");
        check(&mut m, "EXISTS a b b c", ":2\r\n");
        check(&mut m, "TYPE b", "+string\r\n");
        check(&mut m, "TYPE c", "+none\r\n");
        m.data.insert(
            (0, b"h".to_vec()),
            Rec {
                vtype: VType::Hash,
                value: vec![0; 4],
                expire: None,
            },
        );
        check(&mut m, "TYPE h", "+hash\r\n");
        m.set_str(b"x", b"v", Some(NOW));
        check(&mut m, "EXISTS x", ":0\r\n");
        check(&mut m, "TYPE x", "+none\r\n");
        m.set_str(b"x", b"v", Some(NOW));
        check(&mut m, "DEL x", ":0\r\n");
    }

    #[test]
    fn rename_family() {
        let mut m = Model::new(NOW);
        check(&mut m, "RENAME c d", "-ERR no such key\r\n");
        check(&mut m, "RENAMENX c d", "-ERR no such key\r\n");
        check(&mut m, "RENAME c c", "-ERR no such key\r\n");
        m.set_str(b"a", b"1", Some(NOW + 100_000));
        check(&mut m, "RENAME a a", "+OK\r\n");
        check(&mut m, "RENAMENX a a", ":0\r\n");
        check(&mut m, "RENAME a z", "+OK\r\n");
        check(&mut m, "TTL z", ":100\r\n");
        assert!(m.rec(0, b"a").is_none());
        m.set_str(b"b", b"2", None);
        check(&mut m, "RENAMENX z b", ":0\r\n");
        check(&mut m, "RENAMENX z y", ":1\r\n");
        check(&mut m, "GET y", "$1\r\n1\r\n");
        // RENAME replaces a destination of another type, and moves hashes
        m.data.insert(
            (0, b"h".to_vec()),
            Rec {
                vtype: VType::Hash,
                value: vec![1, 2],
                expire: None,
            },
        );
        check(&mut m, "RENAME y h", "+OK\r\n");
        check(&mut m, "TYPE h", "+string\r\n");
        m.data.insert(
            (0, b"h2".to_vec()),
            Rec {
                vtype: VType::Hash,
                value: vec![1, 2],
                expire: None,
            },
        );
        check(&mut m, "RENAME h2 q", "+OK\r\n");
        assert_eq!(m.rec(0, b"q").map(|r| r.vtype), Some(VType::Hash));
    }

    #[test]
    fn copy_options() {
        let p = |line: &str| parse_copy_opts(&crate::cmd::testkit::words(line), 16);
        assert_eq!(
            p("COPY a b"),
            Ok(CopyOpts {
                db: None,
                replace: false
            })
        );
        assert_eq!(
            p("COPY a b REPLACE DB 3"),
            Ok(CopyOpts {
                db: Some(3),
                replace: true
            })
        );
        assert_eq!(p("COPY a b DB 16"), Err(CopyOptsError::DbRange));
        assert_eq!(p("COPY a b DB -1"), Err(CopyOptsError::DbRange));
        assert_eq!(p("COPY a b DB 4294967296"), Err(CopyOptsError::NotInteger));
        assert_eq!(p("COPY a b DB abc"), Err(CopyOptsError::NotInteger));
        assert_eq!(p("COPY a b REPLACE DB"), Err(CopyOptsError::Syntax));
        assert_eq!(p("COPY a b foo"), Err(CopyOptsError::Syntax));
        assert_eq!(
            p("COPY a b DB abc foo"),
            Err(CopyOptsError::NotInteger),
            "left to right"
        );
        assert_eq!(
            parse_copy_opts(&crate::cmd::testkit::words("COPY a b DB 3"), 2),
            Err(CopyOptsError::DbRange)
        );
    }

    #[test]
    fn copy_semantics() {
        let mut m = Model::new(NOW);
        m.set_str(b"y", b"val", Some(NOW + 100_000));
        check(
            &mut m,
            "COPY y y",
            "-ERR source and destination objects are the same\r\n",
        );
        check(&mut m, "COPY y y DB 1", ":1\r\n");
        assert_eq!(
            m.rec(1, b"y").map(|r| r.value.clone()),
            Some(b"val".to_vec())
        );
        assert_eq!(m.db, 0, "COPY returns to the source database");
        check(
            &mut m,
            "COPY y x DB 16",
            "-ERR DB index is out of range\r\n",
        );
        check(
            &mut m,
            "COPY y x DB abc",
            "-ERR value is not an integer or out of range\r\n",
        );
        check(&mut m, "COPY y x REPLACE DB", "-ERR syntax error\r\n");
        check(&mut m, "COPY missing x", ":0\r\n");
        m.set_str(b"b", b"old", None);
        check(&mut m, "COPY y b", ":0\r\n");
        check(&mut m, "GET b", "$3\r\nold\r\n");
        check(&mut m, "COPY y b REPLACE", ":1\r\n");
        check(&mut m, "GET b", "$3\r\nval\r\n");
        check(&mut m, "TTL b", ":100\r\n");
        check(&mut m, "COPY y y DB 1", ":0\r\n");
        check(&mut m, "COPY y y DB 1 REPLACE", ":1\r\n");
        let _ = run(&mut m, "GET y");
    }

    #[test]
    fn copy_precheck() {
        let mut out = Vec::new();
        let mut w = ReplyWriter::new(&mut out, kv_resp::Proto::Resp2);
        assert!(!precheck_copy(
            &crate::cmd::testkit::words("COPY a b DB 1"),
            1,
            true,
            &mut w
        ));
        assert_eq!(out, b"-ERR DB index is out of range\r\n");
        let mut out = Vec::new();
        let mut w = ReplyWriter::new(&mut out, kv_resp::Proto::Resp2);
        assert!(!precheck_copy(
            &crate::cmd::testkit::words("COPY a b DB 1"),
            16,
            true,
            &mut w
        ));
        assert_eq!(
            out,
            b"-ERR Copying to another database is not allowed in cluster mode\r\n"
        );
        let mut out = Vec::new();
        let mut w = ReplyWriter::new(&mut out, kv_resp::Proto::Resp2);
        assert!(precheck_copy(
            &crate::cmd::testkit::words("COPY a b DB 0 REPLACE"),
            1,
            true,
            &mut w
        ));
        assert!(out.is_empty());
    }
}
