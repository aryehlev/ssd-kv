//! Hash commands (DESIGN §8.6, M1.4 stretch): HSET, HSETNX, HGET, HMGET, HMSET, HDEL, HGETALL,
//! HKEYS, HVALS, HLEN, HEXISTS, HINCRBY, HINCRBYFLOAT, HSTRLEN. Value encoding: `u32 n_fields`
//! then `(varint flen, field, varint vlen, value)` in insertion order; whole-record
//! read-modify-write up to `hash-max-bytes`; replicated as a `Put` of the whole encoded hash.
//!
//! The count is little endian and the lengths are unsigned LEB128 varints. Replies list fields
//! in insertion order (Redis's listpack/hashtable order is not part of its contract; this is a
//! documented difference). A write that would leave the hash empty deletes the key, as in
//! Redis. Every handler validates its arguments, the key's type and the stored encoding before
//! it stages a write. Reads keep the TTL untouched; writes carry it over unchanged.

use kv_common::ldouble::LdError;
use kv_index::VType;
use kv_resp::ReplyWriter;
use kv_storage::keyspace::{KeyMeta, StoreError};
use smallvec::SmallVec;

use super::store::{self, Store};
use super::{Args, err, parse_i64, write_arity_error};
use crate::runtime::xmsg::ExecEnv;

/// `hash-max-bytes` default (§20.2): the largest encoded hash a write may produce (the current
/// value is `cmd::hash_limit`).
pub(crate) const HASH_MAX_BYTES: usize = 16 * 1024 * 1024;

/// Field/value pairs of one decoded hash, borrowing the record bytes.
type Fields<'a> = SmallVec<[(&'a [u8], &'a [u8]); 8]>;

/// The stored encoding is not a valid hash (never written by this module): a storage defect.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct BadEncoding;

fn read_varint(b: &[u8], pos: &mut usize) -> Option<usize> {
    let mut v: u64 = 0;
    for shift in (0..64).step_by(7) {
        let byte = *b.get(*pos)?;
        *pos += 1;
        v |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return usize::try_from(v).ok();
        }
    }
    None
}

fn write_varint(mut v: usize, out: &mut Vec<u8>) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Decodes an encoded hash.
pub(crate) fn decode(b: &[u8]) -> Result<Fields<'_>, BadEncoding> {
    let n = u32::from_le_bytes(
        b.get(..4)
            .ok_or(BadEncoding)?
            .try_into()
            .map_err(|_| BadEncoding)?,
    ) as usize;
    let mut pos = 4;
    let mut out = Fields::with_capacity(n.min(1024));
    let take = |pos: &mut usize| -> Option<&[u8]> {
        let len = read_varint(b, pos)?;
        let s = b.get(*pos..pos.checked_add(len)?)?;
        *pos += len;
        Some(s)
    };
    for _ in 0..n {
        let f = take(&mut pos).ok_or(BadEncoding)?;
        let v = take(&mut pos).ok_or(BadEncoding)?;
        out.push((f, v));
    }
    if pos != b.len() {
        return Err(BadEncoding);
    }
    Ok(out)
}

/// Encodes `fields` (in order).
pub(crate) fn encode<'a>(
    fields: impl ExactSizeIterator<Item = (&'a [u8], &'a [u8])> + Clone,
) -> Vec<u8> {
    let size: usize = 4 + fields
        .clone()
        .map(|(f, v)| f.len() + v.len() + 20)
        .sum::<usize>();
    let mut out = Vec::with_capacity(size);
    out.extend_from_slice(&(fields.len() as u32).to_le_bytes());
    for (f, v) in fields {
        write_varint(f.len(), &mut out);
        out.extend_from_slice(f);
        write_varint(v.len(), &mut out);
        out.extend_from_slice(v);
    }
    out
}

fn find(fields: &Fields<'_>, field: &[u8]) -> Option<usize> {
    fields.iter().position(|(f, _)| *f == field)
}

/// The hash at `key`: `Ok(None)` when missing, `Err(())` after a WRONGTYPE or corruption reply.
/// The record bytes are copied only when the value is chunked.
enum Loaded<'v> {
    Missing,
    Refused,
    Found(KeyMeta, std::borrow::Cow<'v, [u8]>),
}

fn load<'v, S: Store + ?Sized>(
    ks: &'v mut S,
    key: &[u8],
    w: &mut ReplyWriter<'_>,
) -> Result<Loaded<'v>, StoreError> {
    Ok(match ks.get(key)? {
        None => Loaded::Missing,
        Some((m, _)) if m.vtype != VType::Hash => {
            w.error(err::WRONGTYPE);
            Loaded::Refused
        }
        Some((m, v)) => match v {
            kv_storage::keyspace::Value::Contig(b) => {
                Loaded::Found(m, std::borrow::Cow::Borrowed(b))
            }
            ref chunks => Loaded::Found(m, std::borrow::Cow::Owned(store::to_vec(chunks))),
        },
    })
}

fn write_corrupt(w: &mut ReplyWriter<'_>) {
    w.error(b"ERR storage corruption detected");
}

/// Writes the new hash (or deletes the key when it became empty), keeping the TTL.
fn store_hash<S: Store + ?Sized>(
    ks: &mut S,
    key: &[u8],
    encoded: &[u8],
    n_fields: usize,
    expire_at_ms: Option<u64>,
    w: &mut ReplyWriter<'_>,
) -> Result<bool, StoreError> {
    if n_fields == 0 {
        ks.delete(key)?;
        return Ok(true);
    }
    if encoded.len() as u64 > super::hash_limit() {
        w.error(b"ERR hash exceeds maximum allowed size (hash-max-bytes)");
        return Ok(false);
    }
    ks.put(key, encoded, VType::Hash, expire_at_ms)?;
    Ok(true)
}

/// Applies `pairs` (field, value) to the hash at `key`; returns the number of new fields, or
/// `None` after an error reply. With `nx`, existing fields are left alone.
fn set_fields<S: Store + ?Sized>(
    ks: &mut S,
    key: &[u8],
    pairs: &[&[u8]],
    nx: bool,
    w: &mut ReplyWriter<'_>,
) -> Result<Option<i64>, StoreError> {
    let (meta, encoded, n, added) = {
        let (meta, bytes) = match load(ks, key, w)? {
            Loaded::Refused => return Ok(None),
            Loaded::Missing => (None, std::borrow::Cow::Borrowed(&[][..])),
            Loaded::Found(m, b) => (Some(m), b),
        };
        let mut fields = if meta.is_some() {
            match decode(&bytes) {
                Ok(f) => f,
                Err(BadEncoding) => {
                    write_corrupt(w);
                    return Ok(None);
                }
            }
        } else {
            Fields::new()
        };
        let mut added = 0i64;
        // an index keeps many-field updates of large hashes linear
        let mut index: Option<hashbrown::HashMap<&[u8], usize, foldhash::fast::RandomState>> =
            (fields.len() * (pairs.len() / 2) > 256).then(|| {
                fields
                    .iter()
                    .enumerate()
                    .map(|(i, (f, _))| (*f, i))
                    .collect()
            });
        for pair in pairs.chunks_exact(2) {
            let (f, v) = (pair[0], pair[1]);
            let at = match &index {
                Some(ix) => ix.get(f).copied(),
                None => find(&fields, f),
            };
            match at {
                Some(_) if nx => {}
                Some(i) => fields[i].1 = v,
                None => {
                    if let Some(ix) = index.as_mut() {
                        ix.insert(f, fields.len());
                    }
                    fields.push((f, v));
                    added += 1;
                }
            }
        }
        if nx && added == 0 {
            return Ok(Some(0));
        }
        (meta, encode(fields.iter().copied()), fields.len(), added)
    };
    if store_hash(ks, key, &encoded, n, meta.and_then(|m| m.expire_at_ms), w)? {
        Ok(Some(added))
    } else {
        Ok(None)
    }
}

/// HSET key field value [field value ...]: the number of fields added.
pub fn hset<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    if a.len() % 2 == 1 {
        write_arity_error("hset", w);
        return Ok(());
    }
    if let Some(n) = set_fields(ks, a.arg(1), &a.argv[2..], false, w)? {
        w.int(n);
    }
    Ok(())
}

/// HMSET key field value [field value ...]: `+OK`.
pub fn hmset<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    if a.len() % 2 == 1 {
        write_arity_error("hmset", w);
        return Ok(());
    }
    if set_fields(ks, a.arg(1), &a.argv[2..], false, w)?.is_some() {
        w.ok();
    }
    Ok(())
}

/// HSETNX key field value: `:1` when the field was added.
pub fn hsetnx<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    if let Some(n) = set_fields(ks, a.arg(1), &a.argv[2..4], true, w)? {
        w.int(n);
    }
    Ok(())
}

/// Runs `f` over the decoded hash at `key` (`None` when missing). Answers WRONGTYPE and
/// corruption itself.
fn read_hash<S: Store + ?Sized>(
    ks: &mut S,
    key: &[u8],
    w: &mut ReplyWriter<'_>,
    f: impl FnOnce(Option<&Fields<'_>>, &mut ReplyWriter<'_>),
) -> Result<(), StoreError> {
    match load(ks, key, w)? {
        Loaded::Refused => {}
        Loaded::Missing => f(None, w),
        Loaded::Found(_, bytes) => match decode(&bytes) {
            Ok(fields) => f(Some(&fields), w),
            Err(BadEncoding) => write_corrupt(w),
        },
    }
    Ok(())
}

/// HGET key field.
pub fn hget<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    let field = a.arg(2);
    read_hash(ks, a.arg(1), w, |h, w| {
        match h.and_then(|h| find(h, field).map(|i| h[i].1)) {
            Some(v) => w.bulk(v),
            None => w.null(),
        }
    })
}

/// HMGET key field [field ...]: one element per field.
pub fn hmget<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    let fields = &a.argv[2..];
    read_hash(ks, a.arg(1), w, |h, w| {
        w.array(fields.len());
        for f in fields {
            match h.and_then(|h| find(h, f).map(|i| h[i].1)) {
                Some(v) => w.bulk(v),
                None => w.null(),
            }
        }
    })
}

/// HGETALL key: a map (RESP2: flat array) of every field and value.
pub fn hgetall<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    read_hash(ks, a.arg(1), w, |h, w| {
        let h = h.map_or(&[][..], |h| h.as_slice());
        w.map(h.len());
        for (f, v) in h {
            w.bulk(f);
            w.bulk(v);
        }
    })
}

/// HKEYS key.
pub fn hkeys<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    read_hash(ks, a.arg(1), w, |h, w| {
        let h = h.map_or(&[][..], |h| h.as_slice());
        w.array(h.len());
        h.iter().for_each(|(f, _)| w.bulk(f));
    })
}

/// HVALS key.
pub fn hvals<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    read_hash(ks, a.arg(1), w, |h, w| {
        let h = h.map_or(&[][..], |h| h.as_slice());
        w.array(h.len());
        h.iter().for_each(|(_, v)| w.bulk(v));
    })
}

/// HLEN key.
pub fn hlen<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    read_hash(ks, a.arg(1), w, |h, w| {
        w.int(h.map_or(0, |h| h.len() as i64))
    })
}

/// HEXISTS key field.
pub fn hexists<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    let field = a.arg(2);
    read_hash(ks, a.arg(1), w, |h, w| {
        w.int(i64::from(h.is_some_and(|h| find(h, field).is_some())))
    })
}

/// HSTRLEN key field: the value's length, 0 when missing.
pub fn hstrlen<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    let field = a.arg(2);
    read_hash(ks, a.arg(1), w, |h, w| {
        w.int(
            h.and_then(|h| find(h, field))
                .map_or(0, |i| h.unwrap()[i].1.len() as i64),
        )
    })
}

/// HDEL key field [field ...]: the number of fields removed; an emptied hash is deleted.
pub fn hdel<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    let key = a.arg(1);
    let (meta, encoded, n, removed) = {
        let (meta, bytes) = match load(ks, key, w)? {
            Loaded::Refused => return Ok(()),
            Loaded::Missing => {
                w.int(0);
                return Ok(());
            }
            Loaded::Found(m, b) => (m, b),
        };
        let Ok(mut fields) = decode(&bytes) else {
            write_corrupt(w);
            return Ok(());
        };
        let before = fields.len();
        for f in &a.argv[2..] {
            if let Some(i) = find(&fields, f) {
                fields.remove(i);
            }
        }
        let removed = (before - fields.len()) as i64;
        if removed == 0 {
            w.int(0);
            return Ok(());
        }
        (meta, encode(fields.iter().copied()), fields.len(), removed)
    };
    if store_hash(ks, key, &encoded, n, meta.expire_at_ms, w)? {
        w.int(removed);
    }
    Ok(())
}

/// The new value of one field computed from its current value (`None` = missing field), or
/// the error reply.
type FieldUpdate = fn(Option<&[u8]>, &[u8]) -> Result<Vec<u8>, &'static [u8]>;

/// Read-modify-write of one field (HINCRBY, HINCRBYFLOAT); replies the new value through
/// `reply`.
fn update_field<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    update: FieldUpdate,
    reply: fn(&[u8], &mut ReplyWriter<'_>),
) -> Result<(), StoreError> {
    let (key, field, arg) = (a.arg(1), a.arg(2), a.arg(3));
    let (meta, encoded, n, new) = {
        let (meta, bytes) = match load(ks, key, w)? {
            Loaded::Refused => return Ok(()),
            Loaded::Missing => (None, std::borrow::Cow::Borrowed(&[][..])),
            Loaded::Found(m, b) => (Some(m), b),
        };
        // declared before `fields`, which borrows it (drop order)
        #[allow(clippy::needless_late_init)]
        let new: Vec<u8>;
        let mut fields = if meta.is_some() {
            match decode(&bytes) {
                Ok(f) => f,
                Err(BadEncoding) => {
                    write_corrupt(w);
                    return Ok(());
                }
            }
        } else {
            Fields::new()
        };
        let at = find(&fields, field);
        new = match update(at.map(|i| fields[i].1), arg) {
            Ok(v) => v,
            Err(e) => {
                w.error(e);
                return Ok(());
            }
        };
        match at {
            Some(i) => fields[i].1 = &new,
            None => fields.push((field, &new)),
        }
        let encoded = encode(fields.iter().copied());
        let n = fields.len();
        drop(fields);
        (meta, encoded, n, new)
    };
    if store_hash(ks, key, &encoded, n, meta.and_then(|m| m.expire_at_ms), w)? {
        reply(&new, w);
    }
    Ok(())
}

fn incr_int(cur: Option<&[u8]>, incr: &[u8]) -> Result<Vec<u8>, &'static [u8]> {
    let incr = parse_i64(incr).ok_or(err::NOT_INTEGER)?;
    let old = match cur {
        None => 0,
        Some(v) => parse_i64(v).ok_or(&b"ERR hash value is not an integer"[..])?,
    };
    let new = old.checked_add(incr).ok_or(err::OVERFLOW)?;
    Ok(super::Decimal::i64(new).as_bytes().to_vec())
}

fn incr_float(cur: Option<&[u8]>, incr: &[u8]) -> Result<Vec<u8>, &'static [u8]> {
    // the increment alone first: not a float, then NaN or infinite
    match super::strings::ld_add(None, incr) {
        Err(LdError::NotFloat) => return Err(err::NOT_FLOAT),
        Err(LdError::NanOrInf) => return Err(b"ERR value is NaN or Infinity"),
        Ok(v) if cur.is_none() => return Ok(v),
        Ok(_) => {}
    }
    super::strings::ld_add(cur, incr).map_err(|e| match e {
        LdError::NotFloat => &b"ERR hash value is not a float"[..],
        LdError::NanOrInf => err::NAN_OR_INF,
    })
}

/// HINCRBY key field n (the increment is checked before the key).
pub fn hincrby<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    if parse_i64(a.arg(3)).is_none() {
        w.error(err::NOT_INTEGER);
        return Ok(());
    }
    update_field(ks, a, w, incr_int, |v, w| w.int(parse_i64(v).unwrap_or(0)))
}

/// HINCRBYFLOAT key field f (the increment is checked before the key).
pub fn hincrbyfloat<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    if let Err(e) = incr_float(None, a.arg(3)) {
        w.error(e);
        return Ok(());
    }
    update_field(ks, a, w, incr_float, |v, w| w.bulk(v))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::testkit::{Model, check, check3};
    use proptest::prelude::*;

    const NOW: u64 = 1_700_000_000_000;

    #[test]
    fn hash_replies() {
        let mut m = Model::new(NOW);
        check(
            &mut m,
            "HSET h a",
            "-ERR wrong number of arguments for 'hset' command\r\n",
        );
        check(
            &mut m,
            "HSET h a 1 b",
            "-ERR wrong number of arguments for 'hset' command\r\n",
        );
        check(&mut m, "HSET h a 1 b 2 a 3", ":2\r\n");
        check(
            &mut m,
            "HGETALL h",
            "*4\r\n$1\r\na\r\n$1\r\n3\r\n$1\r\nb\r\n$1\r\n2\r\n",
        );
        check(&mut m, "HSETNX h a 9", ":0\r\n");
        check(&mut m, "HSETNX h c 9", ":1\r\n");
        check(&mut m, "HMGET h a x", "*2\r\n$1\r\n3\r\n$-1\r\n");
        check(&mut m, "HMSET h d 4", "+OK\r\n");
        check(&mut m, "HDEL h a x a", ":1\r\n");
        check(&mut m, "HLEN h", ":3\r\n");
        check(&mut m, "HEXISTS h b", ":1\r\n");
        check(&mut m, "HEXISTS h a", ":0\r\n");
        check(&mut m, "HSTRLEN h b", ":1\r\n");
        check(&mut m, "HSTRLEN h zz", ":0\r\n");
        check(
            &mut m,
            "HINCRBY h b x",
            "-ERR value is not an integer or out of range\r\n",
        );
        check(&mut m, "HINCRBY h b 5", ":7\r\n");
        check(
            &mut m,
            "HINCRBY h c 9223372036854775807",
            "-ERR increment or decrement would overflow\r\n",
        );
        check(&mut m, "HINCRBY h new -3", ":-3\r\n");
        check(&mut m, "HSET h s abc", ":1\r\n");
        check(
            &mut m,
            "HINCRBY h s 1",
            "-ERR hash value is not an integer\r\n",
        );
        check(
            &mut m,
            "HINCRBYFLOAT h s 1",
            "-ERR hash value is not a float\r\n",
        );
        check(
            &mut m,
            "HINCRBYFLOAT h b inf",
            "-ERR value is NaN or Infinity\r\n",
        );
        check(
            &mut m,
            "HINCRBYFLOAT h b nan",
            "-ERR value is not a valid float\r\n",
        );
        check(
            &mut m,
            "HINCRBYFLOAT h b abc",
            "-ERR value is not a valid float\r\n",
        );
        check(&mut m, "HINCRBYFLOAT h b 1.5", "$3\r\n8.5\r\n");
        check(&mut m, "HINCRBYFLOAT h f 0.25", "$4\r\n0.25\r\n");
        let argv: [&[u8]; 4] = [b"HSET", b"h", b"z", b"2\0"];
        assert_eq!(
            crate::cmd::testkit::run_argv(&mut m, &argv, kv_resp::Proto::Resp2),
            b":1\r\n"
        );
        check(
            &mut m,
            "HINCRBYFLOAT h z 1",
            "-ERR hash value is not a float\r\n",
        );
        check(&mut m, "HDEL h z", ":1\r\n");
        check(
            &mut m,
            "HKEYS h",
            "*6\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$3\r\nnew\r\n$1\r\ns\r\n$1\r\nf\r\n",
        );
        check(
            &mut m,
            "HVALS h",
            "*6\r\n$3\r\n8.5\r\n$1\r\n9\r\n$1\r\n4\r\n$2\r\n-3\r\n$3\r\nabc\r\n$4\r\n0.25\r\n",
        );
        check(&mut m, "HKEYS nokey", "*0\r\n");
        check(&mut m, "HVALS nokey", "*0\r\n");
        check(&mut m, "HGETALL nokey", "*0\r\n");
        check(&mut m, "HLEN nokey", ":0\r\n");
        check(&mut m, "HGET nokey a", "$-1\r\n");
        check(&mut m, "HMGET nokey a b", "*2\r\n$-1\r\n$-1\r\n");
        check(&mut m, "HDEL nokey a", ":0\r\n");
        check(&mut m, "HDEL h b c d new s f", ":6\r\n");
        assert!(m.rec(0, b"h").is_none(), "an emptied hash is deleted");
        check(&mut m, "TYPE h", "+none\r\n");
    }

    #[test]
    fn resp3_and_ttl() {
        let mut m = Model::new(NOW);
        check(&mut m, "HSET h3 a 1", ":1\r\n");
        check3(&mut m, "HGETALL h3", "%1\r\n$1\r\na\r\n$1\r\n1\r\n");
        check3(&mut m, "HGETALL nokey", "%0\r\n");
        check3(&mut m, "HKEYS nokey", "*0\r\n");
        check3(&mut m, "HGET h3 zz", "_\r\n");
        check(&mut m, "EXPIRE h3 100", ":1\r\n");
        check(&mut m, "HSET h3 b 2", ":1\r\n");
        check(&mut m, "HINCRBY h3 a 1", ":2\r\n");
        check(&mut m, "TTL h3", ":100\r\n");
        check(&mut m, "TYPE h3", "+hash\r\n");
    }

    #[test]
    fn wrongtype_and_order_of_checks() {
        let mut m = Model::new(NOW);
        m.set_str(b"str", b"v", None);
        let wt = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        for c in [
            "HGET str a",
            "HGETALL str",
            "HSET str a 1",
            "HSETNX str a 1",
            "HDEL str a",
            "HLEN str",
            "HKEYS str",
            "HVALS str",
            "HEXISTS str a",
            "HSTRLEN str a",
            "HMGET str a",
            "HINCRBY str a 1",
            "HINCRBYFLOAT str a 1",
        ] {
            check(&mut m, c, wt);
        }
        check(
            &mut m,
            "HINCRBYFLOAT str a x",
            "-ERR value is not a valid float\r\n",
        );
        check(
            &mut m,
            "HINCRBY str a x",
            "-ERR value is not an integer or out of range\r\n",
        );
        assert_eq!(m.writes, 0);
        // GET on a hash is WRONGTYPE; SET replaces it
        check(&mut m, "HSET h a 1", ":1\r\n");
        check(&mut m, "GET h", wt);
        check(&mut m, "SET h v", "+OK\r\n");
        check(&mut m, "TYPE h", "+string\r\n");
    }

    #[test]
    fn chunked_hash_values() {
        let mut m = Model::new(NOW);
        check(&mut m, "HSET h alpha 1 beta 22 gamma 333", ":3\r\n");
        m.chunk = 3;
        check(&mut m, "HGET h gamma", "$3\r\n333\r\n");
        check(&mut m, "HSET h delta 4", ":1\r\n");
        check(&mut m, "HLEN h", ":4\r\n");
    }

    #[test]
    fn corrupt_encoding_is_reported() {
        let mut m = Model::new(NOW);
        m.data.insert(
            (0, b"h".to_vec()),
            crate::cmd::testkit::Rec {
                vtype: VType::Hash,
                value: vec![9, 0, 0, 0, 1],
                expire: None,
            },
        );
        check(&mut m, "HGET h a", "-ERR storage corruption detected\r\n");
        check(&mut m, "HSET h a 1", "-ERR storage corruption detected\r\n");
        assert_eq!(m.writes, 0);
    }

    #[test]
    fn varints() {
        for v in [
            0usize,
            1,
            127,
            128,
            300,
            16383,
            16384,
            1 << 31,
            usize::MAX >> 1,
        ] {
            let mut b = Vec::new();
            write_varint(v, &mut b);
            let mut pos = 0;
            assert_eq!(read_varint(&b, &mut pos), Some(v));
            assert_eq!(pos, b.len());
        }
        let mut pos = 0;
        assert_eq!(read_varint(&[0x80, 0x80], &mut pos), None);
    }

    proptest! {
        #[test]
        fn encoding_round_trips(fields in proptest::collection::vec((proptest::collection::vec(any::<u8>(), 0..40), proptest::collection::vec(any::<u8>(), 0..300)), 0..40)) {
            let pairs: Vec<(&[u8], &[u8])> = fields.iter().map(|(f, v)| (f.as_slice(), v.as_slice())).collect();
            let enc = encode(pairs.iter().copied());
            let dec = decode(&enc).unwrap();
            prop_assert_eq!(dec.as_slice(), pairs.as_slice());
        }

        #[test]
        fn decode_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..200)) {
            let _ = decode(&bytes);
        }

        #[test]
        fn hset_matches_a_map_model(ops in proptest::collection::vec((0u8..3, 0u8..6, 0u8..4), 1..60)) {
            // HSET / HDEL / HINCRBY on a few fields against an ordered map model
            let mut m = Model::new(NOW);
            let mut model: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
            for (op, f, v) in ops {
                let field = format!("f{f}");
                match op {
                    0 => {
                        let val = format!("{v}");
                        let new = !model.iter().any(|(k, _)| k == field.as_bytes());
                        let got = crate::cmd::testkit::run(&mut m, &format!("HSET h {field} {val}"));
                        prop_assert_eq!(got, format!(":{}\r\n", u8::from(new)).into_bytes());
                        match model.iter_mut().find(|(k, _)| k == field.as_bytes()) {
                            Some(e) => e.1 = val.into_bytes(),
                            None => model.push((field.into_bytes(), val.into_bytes())),
                        }
                    }
                    1 => {
                        let had = model.iter().position(|(k, _)| k == field.as_bytes());
                        let got = crate::cmd::testkit::run(&mut m, &format!("HDEL h {field}"));
                        prop_assert_eq!(got, format!(":{}\r\n", u8::from(had.is_some())).into_bytes());
                        if let Some(i) = had { model.remove(i); }
                    }
                    _ => {
                        let cur = model.iter().find(|(k, _)| k == field.as_bytes()).map(|(_, v)| String::from_utf8(v.clone()).unwrap());
                        let new = cur.as_deref().map_or(0, |c| c.parse::<i64>().unwrap()) + i64::from(v);
                        let got = crate::cmd::testkit::run(&mut m, &format!("HINCRBY h {field} {v}"));
                        prop_assert_eq!(got, format!(":{new}\r\n").into_bytes());
                        match model.iter_mut().find(|(k, _)| k == field.as_bytes()) {
                            Some(e) => e.1 = new.to_string().into_bytes(),
                            None => model.push((field.into_bytes(), new.to_string().into_bytes())),
                        }
                    }
                }
                let stored = m.rec(0, b"h").map(|r| decode(&r.value).unwrap().iter().map(|(f, v)| (f.to_vec(), v.to_vec())).collect::<Vec<_>>());
                prop_assert_eq!(stored.unwrap_or_default(), model.clone());
            }
        }
    }
}
