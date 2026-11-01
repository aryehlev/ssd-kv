//! String commands (DESIGN §8.3, Redis 7.0.15 semantics): GET, SET (NX/XX/GET/EX/PX/EXAT/PXAT/
//! KEEPTTL), SETNX, SETEX, PSETEX, GETSET, GETDEL, GETEX, MGET, MSET, MSETNX, APPEND, STRLEN,
//! GETRANGE/SUBSTR, SETRANGE, INCR/DECR/INCRBY/DECRBY (`string2ll`, overflow errors) and
//! INCRBYFLOAT (long double through `kv_common::ldouble`, stored and replicated as the formatted
//! result).
//!
//! Every handler validates its arguments and the key's type before its first mutating call, so
//! an error reply never leaves a staged effect behind. A SET without NX/XX/GET/KEEPTTL is a
//! blind write (no read of the old record, §8.1); the TTL rules of §16.3 apply: SET, GETSET and
//! MSET clear the TTL, KEEPTTL keeps it, APPEND/SETRANGE/INCR* keep it.

use kv_cmd::CmdId;
use kv_common::ldouble::{self, LdError};
use kv_index::VType;
use kv_resp::ReplyWriter;
use kv_storage::keyspace::{StoreError, Value};

use super::expire::{self, Unit};
use super::store::{self, Store};
use super::{Args, MAX_STRING_LEN, err, parse_i64, write_arity_error, write_value};
use crate::runtime::xmsg::ExecEnv;

/// GET: the value, nil when missing, WRONGTYPE on a hash.
pub fn get<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    match ks.get(a.arg(1))? {
        None => w.null(),
        Some((m, _)) if m.vtype != VType::String => w.error(err::WRONGTYPE),
        Some((_, v)) => write_value(w, &v),
    }
    Ok(())
}

/// Options of SET and GETEX (Redis 7.0 `parseExtendedStringArgumentsOrReply`). Holds an
/// argument slice: no `Debug` (LC-32).
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SetOpts<'a> {
    pub nx: bool,
    pub xx: bool,
    pub get: bool,
    pub keepttl: bool,
    pub persist: bool,
    /// The expiry form and its argument.
    pub expire: Option<(ExpireForm, &'a [u8])>,
}

/// `EX` / `PX` / `EXAT` / `PXAT`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum ExpireForm {
    Ex,
    Px,
    Exat,
    Pxat,
}

impl ExpireForm {
    fn unit(self) -> Unit {
        match self {
            ExpireForm::Ex | ExpireForm::Exat => Unit::Seconds,
            ExpireForm::Px | ExpireForm::Pxat => Unit::Millis,
        }
    }

    fn relative(self) -> bool {
        matches!(self, ExpireForm::Ex | ExpireForm::Px)
    }
}

/// Which command's options are parsed.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum OptsFor {
    Set,
    Getex,
}

/// Parses the options of SET (from `argv[3]`) or GETEX (from `argv[2]`) exactly as Redis 7.0:
/// option names are C strings matched case-insensitively; NX/XX exclude each other; the expiry
/// forms, KEEPTTL and PERSIST exclude each other except that one form may repeat (the last
/// argument wins); an expiry form needs a following argument. Anything else is a syntax error.
pub(crate) fn parse_set_opts<'a>(argv: &[&'a [u8]], cmd: OptsFor) -> Option<SetOpts<'a>> {
    /// The option names of both commands.
    enum Opt {
        Nx,
        Xx,
        Get,
        Keepttl,
        Persist,
        Form(ExpireForm),
    }
    const NAMES: [(&[u8], Opt); 8] = [
        (b"nx", Opt::Nx),
        (b"xx", Opt::Xx),
        (b"get", Opt::Get),
        (b"keepttl", Opt::Keepttl),
        (b"persist", Opt::Persist),
        (b"ex", Opt::Form(ExpireForm::Ex)),
        (b"px", Opt::Form(ExpireForm::Px)),
        (b"exat", Opt::Form(ExpireForm::Exat)),
    ];
    let mut o = SetOpts::default();
    let set = cmd == OptsFor::Set;
    let mut j = if set { 3 } else { 2 };
    while j < argv.len() {
        let name = super::c_str(argv[j]);
        let opt = if name.eq_ignore_ascii_case(b"pxat") {
            &Opt::Form(ExpireForm::Pxat)
        } else {
            &NAMES.iter().find(|(n, _)| name.eq_ignore_ascii_case(n))?.1
        };
        let any_expire = o.expire.is_some();
        match *opt {
            Opt::Nx if set && !o.xx => o.nx = true,
            Opt::Xx if set && !o.nx => o.xx = true,
            Opt::Get if set => o.get = true,
            Opt::Keepttl if set && !o.persist && !any_expire => o.keepttl = true,
            Opt::Persist if !set && !any_expire && !o.keepttl => o.persist = true,
            Opt::Form(f) => {
                // one form may repeat (the last argument wins); forms exclude each other
                let arg = argv.get(j + 1).copied()?;
                let other_form = o.expire.is_some_and(|(g, _)| g != f);
                if o.keepttl || o.persist || other_form {
                    return None;
                }
                o.expire = Some((f, arg));
                j += 1;
            }
            _ => return None,
        }
        j += 1;
    }
    Some(o)
}

/// SET k v [NX|XX] [GET] [EX s|PX ms|EXAT s|PXAT ms|KEEPTTL] (Redis 7.0 `setGenericCommand`).
pub fn set<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    let Some(o) = parse_set_opts(&a.argv, OptsFor::Set) else {
        w.error(err::SYNTAX);
        return Ok(());
    };
    let expire_at = match o.expire {
        None => None,
        Some((f, arg)) => match expire::expire_arg_ms(arg, f.unit(), f.relative(), ks.now_ms()) {
            Ok(ms) => Some(ms),
            Err(e) => {
                e.write("set", w);
                return Ok(());
            }
        },
    };
    let (key, value) = (a.arg(1), a.arg(2));
    if !(o.nx || o.xx || o.get || o.keepttl) {
        // blind write: no read of the previous record
        ks.put(key, value, VType::String, expire_at)?;
        w.ok();
        return Ok(());
    }
    // GET replies with the old value (WRONGTYPE on a hash, nothing set) before the NX/XX test
    let old = if o.get {
        match ks.get(key)? {
            None => {
                w.null();
                None
            }
            Some((m, _)) if m.vtype != VType::String => {
                w.error(err::WRONGTYPE);
                return Ok(());
            }
            Some((m, v)) => {
                write_value(w, &v);
                Some(m)
            }
        }
    } else {
        ks.meta(key)?
    };
    let found = old.is_some();
    if (o.nx && found) || (o.xx && !found) {
        if !o.get {
            w.null();
        }
        return Ok(());
    }
    let expire_at = if o.keepttl {
        old.and_then(|m| m.expire_at_ms)
    } else {
        expire_at
    };
    ks.put(key, value, VType::String, expire_at)?;
    if !o.get {
        w.ok();
    }
    Ok(())
}

/// SETNX k v: `:1` when set, `:0` when the key exists (of any type).
pub fn setnx<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    if ks.meta(a.arg(1))?.is_some() {
        w.int(0);
    } else {
        ks.put(a.arg(1), a.arg(2), VType::String, None)?;
        w.int(1);
    }
    Ok(())
}

fn setex_generic<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    unit: Unit,
    command: &str,
) -> Result<(), StoreError> {
    match expire::expire_arg_ms(a.arg(2), unit, true, ks.now_ms()) {
        Ok(at) => {
            ks.put(a.arg(1), a.arg(3), VType::String, Some(at))?;
            w.ok();
        }
        Err(e) => e.write(command, w),
    }
    Ok(())
}

/// SETEX k seconds v.
pub fn setex<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    setex_generic(ks, a, w, Unit::Seconds, "setex")
}

/// PSETEX k milliseconds v.
pub fn psetex<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    setex_generic(ks, a, w, Unit::Millis, "psetex")
}

/// Redis `getGenericCommand`: writes the old value (or nil) and returns whether the key holds
/// a string or is missing (`false` after a WRONGTYPE reply).
fn reply_old_string<S: Store + ?Sized>(
    ks: &mut S,
    key: &[u8],
    w: &mut ReplyWriter<'_>,
) -> Result<Option<bool>, StoreError> {
    Ok(match ks.get(key)? {
        None => {
            w.null();
            Some(false)
        }
        Some((m, _)) if m.vtype != VType::String => {
            w.error(err::WRONGTYPE);
            None
        }
        Some((_, v)) => {
            write_value(w, &v);
            Some(true)
        }
    })
}

/// GETSET k v: the old value; the new value has no TTL.
pub fn getset<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    if reply_old_string(ks, a.arg(1), w)?.is_some() {
        ks.put(a.arg(1), a.arg(2), VType::String, None)?;
    }
    Ok(())
}

/// GETDEL k: the value, then a delete under the durable-delete rule (§16.2).
pub fn getdel<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    if reply_old_string(ks, a.arg(1), w)? == Some(true) {
        ks.delete(a.arg(1))?;
    }
    Ok(())
}

/// GETEX k [EX s|PX ms|EXAT s|PXAT ms|PERSIST] (Redis 7.0 `getexCommand`): a missing key is nil
/// before the expiry is validated; a past EXAT/PXAT deletes the key after replying its value.
pub fn getex<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    let Some(o) = parse_set_opts(&a.argv, OptsFor::Getex) else {
        w.error(err::SYNTAX);
        return Ok(());
    };
    let key = a.arg(1);
    let now = ks.now_ms();
    /// What GETEX does after replying the value.
    enum Then {
        Nothing,
        Delete,
        Expire(u64),
        Persist,
    }
    let then = match ks.get(key)? {
        None => {
            w.null();
            return Ok(());
        }
        Some((m, _)) if m.vtype != VType::String => {
            w.error(err::WRONGTYPE);
            return Ok(());
        }
        Some((m, v)) => {
            let then = match o.expire {
                None if o.persist && m.expire_at_ms.is_some() => Then::Persist,
                None => Then::Nothing,
                Some((f, arg)) => match expire::expire_arg_ms(arg, f.unit(), f.relative(), now) {
                    Ok(at) if !f.relative() && expire::already_expired(at as i64, now) => {
                        Then::Delete
                    }
                    Ok(at) => Then::Expire(at),
                    Err(e) => {
                        e.write("getex", w);
                        return Ok(());
                    }
                },
            };
            write_value(w, &v);
            then
        }
    };
    match then {
        Then::Nothing => {}
        Then::Delete => {
            ks.delete(key)?;
        }
        Then::Expire(at) => {
            ks.set_expire(key, Some(at))?;
        }
        Then::Persist => {
            ks.set_expire(key, None)?;
        }
    }
    Ok(())
}

/// MGET k..: one element per key, nil for missing keys and keys of another type.
pub fn mget<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    w.array(a.len() - 1);
    for &key in &a.argv[1..] {
        match ks.get(key)? {
            Some((m, v)) if m.vtype == VType::String => write_value(w, &v),
            _ => w.null(),
        }
    }
    Ok(())
}

/// MSET/MSETNX need an odd argument count (pairs after the name); Redis checks this in the
/// command, before any key is looked at.
pub(crate) fn precheck_mset(id: CmdId, argv: &[&[u8]], w: &mut ReplyWriter<'_>) -> bool {
    if argv.len().is_multiple_of(2) {
        write_arity_error(if id == CmdId::Mset { "mset" } else { "msetnx" }, w);
        return false;
    }
    true
}

/// MSET k v [k v ..]: every key set without TTL (the last pair wins for repeated keys).
pub fn mset<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    if !precheck_mset(CmdId::Mset, &a.argv, w) {
        return Ok(());
    }
    for pair in a.argv[1..].chunks_exact(2) {
        ks.put(pair[0], pair[1], VType::String, None)?;
    }
    w.ok();
    Ok(())
}

/// MSETNX k v [k v ..]: all-or-nothing; `:0` if any key exists.
pub fn msetnx<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    if !precheck_mset(CmdId::Msetnx, &a.argv, w) {
        return Ok(());
    }
    for pair in a.argv[1..].chunks_exact(2) {
        if ks.meta(pair[0])?.is_some() {
            w.int(0);
            return Ok(());
        }
    }
    for pair in a.argv[1..].chunks_exact(2) {
        ks.put(pair[0], pair[1], VType::String, None)?;
    }
    w.int(1);
    Ok(())
}

/// Redis `checkStringLength`: `size + append` must not exceed the maximum string length.
#[inline]
fn fits(size: u64, append: u64) -> bool {
    size.checked_add(append)
        .is_some_and(|t| t <= MAX_STRING_LEN)
}

/// APPEND k v: the new length; creates the key without TTL, keeps an existing TTL.
pub fn append<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    let (key, data) = (a.arg(1), a.arg(2));
    match ks.meta(key)? {
        None => {
            ks.put(key, data, VType::String, None)?;
            w.int(data.len() as i64);
        }
        Some(m) if m.vtype != VType::String => w.error(err::WRONGTYPE),
        Some(m) if !fits(m.len, data.len() as u64) => w.error(err::STRING_TOO_LONG),
        Some(m) if data.is_empty() => w.int(m.len as i64),
        Some(_) => {
            let n = ks.append(key, data)?;
            w.int(n as i64);
        }
    }
    Ok(())
}

/// STRLEN k: the length, 0 when missing.
pub fn strlen<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    match ks.meta(a.arg(1))? {
        None => w.int(0),
        Some(m) if m.vtype != VType::String => w.error(err::WRONGTYPE),
        Some(m) => w.int(m.len as i64),
    }
    Ok(())
}

/// The byte range `[start, end]` (inclusive) GETRANGE returns for a string of `len` bytes, or
/// `None` for the empty reply (Redis 7.0 `getrangeCommand`, including its `0 -100` quirk: a
/// negative end clamps to 0, so the first byte is returned).
pub(crate) fn getrange_bounds(mut start: i64, mut end: i64, len: usize) -> Option<(usize, usize)> {
    if start < 0 && end < 0 && start > end {
        return None;
    }
    let n = len as i64;
    if start < 0 {
        start = n.wrapping_add(start);
    }
    if end < 0 {
        end = n.wrapping_add(end);
    }
    start = start.max(0);
    end = end.max(0);
    if end >= n {
        end = n - 1;
    }
    if start > end || len == 0 {
        return None;
    }
    Some((start as usize, end as usize))
}

/// GETRANGE / SUBSTR k start end: an empty bulk for a missing key or an empty range.
pub fn getrange<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    let (Some(start), Some(end)) = (parse_i64(a.arg(2)), parse_i64(a.arg(3))) else {
        w.error(err::NOT_INTEGER);
        return Ok(());
    };
    match ks.get(a.arg(1))? {
        None => w.bulk(b""),
        Some((m, _)) if m.vtype != VType::String => w.error(err::WRONGTYPE),
        Some((_, v)) => match getrange_bounds(start, end, store::value_len(&v)) {
            None => w.bulk(b""),
            Some((s, e)) => write_range(w, &v, s, e + 1),
        },
    }
    Ok(())
}

/// One bulk of bytes `[start, end)` of a value.
fn write_range(w: &mut ReplyWriter<'_>, v: &Value<'_>, start: usize, end: usize) {
    if let Some(b) = store::contig(v) {
        return w.bulk(&b[start..end]);
    }
    w.bulk_len(end - start);
    store::copy_range(v, start, end, w.buf);
    w.crlf();
}

/// SETRANGE k offset v (Redis 7.0 `setrangeCommand`): zero-pads; an empty value on a missing
/// key returns 0 and creates nothing; `offset + len` beyond the maximum string length is an
/// error. Keeps an existing TTL.
pub fn setrange<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    let Some(offset) = parse_i64(a.arg(2)) else {
        w.error(err::NOT_INTEGER);
        return Ok(());
    };
    if offset < 0 {
        w.error(b"ERR offset is out of range");
        return Ok(());
    }
    let (key, data) = (a.arg(1), a.arg(3));
    let offset = offset as u64;
    match ks.meta(key)? {
        None => {
            if data.is_empty() {
                w.int(0);
                return Ok(());
            }
            if !fits(offset, data.len() as u64) {
                w.error(err::STRING_TOO_LONG);
                return Ok(());
            }
            let mut v = vec![0u8; offset as usize + data.len()];
            v[offset as usize..].copy_from_slice(data);
            ks.put(key, &v, VType::String, None)?;
            w.int(v.len() as i64);
        }
        Some(m) if m.vtype != VType::String => w.error(err::WRONGTYPE),
        Some(m) if data.is_empty() => w.int(m.len as i64),
        Some(_) if !fits(offset, data.len() as u64) => w.error(err::STRING_TOO_LONG),
        Some(_) => {
            let n = ks.set_range(key, offset, data)?;
            w.int(n as i64);
        }
    }
    Ok(())
}

/// Redis 7.0 `incrDecrCommand`: the stored value must be a `string2ll` integer; the result must
/// stay in i64. The TTL is kept.
fn incr_by<S: Store + ?Sized>(
    ks: &mut S,
    key: &[u8],
    incr: i64,
    w: &mut ReplyWriter<'_>,
) -> Result<(), StoreError> {
    let (old, expire_at) = match ks.get(key)? {
        None => (0, None),
        Some((m, _)) if m.vtype != VType::String => {
            w.error(err::WRONGTYPE);
            return Ok(());
        }
        Some((m, v)) => match store::contig(&v).and_then(parse_i64) {
            Some(n) => (n, m.expire_at_ms),
            None => {
                w.error(err::NOT_INTEGER);
                return Ok(());
            }
        },
    };
    let Some(new) = old.checked_add(incr) else {
        w.error(err::OVERFLOW);
        return Ok(());
    };
    ks.put(
        key,
        super::Decimal::i64(new).as_bytes(),
        VType::String,
        expire_at,
    )?;
    w.int(new);
    Ok(())
}

pub fn incr<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    incr_by(ks, a.arg(1), 1, w)
}

pub fn decr<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    incr_by(ks, a.arg(1), -1, w)
}

/// INCRBY k n: the increment is parsed before the key is looked at.
pub fn incrby<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    match parse_i64(a.arg(2)) {
        Some(n) => incr_by(ks, a.arg(1), n, w),
        None => {
            w.error(err::NOT_INTEGER);
            Ok(())
        }
    }
}

/// DECRBY k n: `i64::MIN` cannot be negated (`-ERR decrement would overflow`).
pub fn decrby<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    match parse_i64(a.arg(2)) {
        Some(i64::MIN) => {
            w.error(b"ERR decrement would overflow");
            Ok(())
        }
        Some(n) => incr_by(ks, a.arg(1), -n, w),
        None => {
            w.error(err::NOT_INTEGER);
            Ok(())
        }
    }
}

/// `current + incr` in long double (Redis `string2ld` on both, then `ld2string`). Redis also
/// requires `strtold` to consume the whole argument, so a NUL byte anywhere makes an argument
/// not a float; that rule is applied here before the shim is called.
pub(crate) fn ld_add(current: Option<&[u8]>, incr: &[u8]) -> Result<Vec<u8>, LdError> {
    let has_nul = |s: &[u8]| s.contains(&0);
    if current.is_some_and(has_nul) || has_nul(incr) {
        return Err(LdError::NotFloat);
    }
    ldouble::incrbyfloat(current, incr)
}

/// INCRBYFLOAT k f: long double arithmetic through the C shim; the formatted result is stored
/// (and replicated as a Put of it) and returned as a bulk. The TTL is kept.
pub fn incrbyfloat<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    let key = a.arg(1);
    let (result, expire_at) = match ks.get(key)? {
        None => (ld_add(None, a.arg(2)), None),
        Some((m, _)) if m.vtype != VType::String => {
            w.error(err::WRONGTYPE);
            return Ok(());
        }
        Some((m, v)) => match store::contig(&v) {
            Some(cur) => (ld_add(Some(cur), a.arg(2)), m.expire_at_ms),
            // a chunked value is far longer than any float literal
            None => (Err(LdError::NotFloat), None),
        },
    };
    match result {
        Ok(s) => {
            ks.put(key, &s, VType::String, expire_at)?;
            w.bulk(&s);
        }
        Err(LdError::NotFloat) => w.error(err::NOT_FLOAT),
        Err(LdError::NanOrInf) => w.error(err::NAN_OR_INF),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::testkit::{Model, check, check3, run, run_argv};
    use kv_resp::Proto;

    const NOW: u64 = 1_700_000_000_000;

    fn model() -> Model {
        Model::new(NOW)
    }

    #[test]
    fn set_option_grammar() {
        fn p(line: &'static str) -> Option<SetOpts<'static>> {
            parse_set_opts(&crate::cmd::testkit::words(line), OptsFor::Set)
        }
        fn g(line: &'static str) -> Option<SetOpts<'static>> {
            parse_set_opts(&crate::cmd::testkit::words(line), OptsFor::Getex)
        }
        assert!(p("SET k v NX XX").is_none());
        assert!(p("SET k v XX NX").is_none());
        assert_eq!(p("SET k v NX NX").map(|o| o.nx), Some(true));
        assert_eq!(
            p("SET k v EX 10 EX 20").and_then(|o| o.expire),
            Some((ExpireForm::Ex, &b"20"[..]))
        );
        assert!(p("SET k v EX 10 PX 20").is_none());
        assert!(p("SET k v KEEPTTL EX 5").is_none());
        assert!(p("SET k v EX 5 KEEPTTL").is_none());
        assert!(p("SET k v EX").is_none());
        assert!(p("SET k v PERSIST").is_none());
        assert!(p("SET k v get Get NX").is_some_and(|o| o.get && o.nx));
        assert!(p("SET k v exat 5 exat 6").is_some());
        assert!(p("SET k v pxat 5 exat 6").is_none());
        assert!(g("GETEX k NX").is_none());
        assert!(g("GETEX k GET").is_none());
        assert!(g("GETEX k KEEPTTL").is_none());
        assert!(g("GETEX k EX 10 PERSIST").is_none());
        assert!(g("GETEX k PERSIST EX 10").is_none());
        assert!(g("GETEX k PERSIST PERSIST").is_some_and(|o| o.persist));
        assert!(
            g("GETEX k PXAT 1").is_some_and(|o| o.expire == Some((ExpireForm::Pxat, &b"1"[..])))
        );
        // option names are C strings
        let argv: [&[u8]; 4] = [b"SET", b"k", b"v", b"nx\0zz"];
        assert!(parse_set_opts(&argv, OptsFor::Set).is_some_and(|o| o.nx));
    }

    #[test]
    fn set_family() {
        let mut m = model();
        check(&mut m, "SET k v EX 10 EX 20", "+OK\r\n");
        check(&mut m, "TTL k", ":20\r\n");
        check(
            &mut m,
            "SET k v PX 9223372036854775807",
            "-ERR invalid expire time in 'set' command\r\n",
        );
        check(
            &mut m,
            "SET k v EX 9223372036854775",
            "-ERR invalid expire time in 'set' command\r\n",
        );
        check(
            &mut m,
            "SET k v EX 0",
            "-ERR invalid expire time in 'set' command\r\n",
        );
        check(
            &mut m,
            "SET k v EXAT 0",
            "-ERR invalid expire time in 'set' command\r\n",
        );
        check(
            &mut m,
            "SET k v EX abc",
            "-ERR value is not an integer or out of range\r\n",
        );
        check(&mut m, "SET k v EX", "-ERR syntax error\r\n");
        check(&mut m, "SET k v NX XX", "-ERR syntax error\r\n");
        check(&mut m, "TTL k", ":20\r\n");
        // a past PXAT stores a logically expired key (counted until touched)
        check(&mut m, "SET k v PXAT 1", "+OK\r\n");
        assert_eq!(m.dbsize(0), 1);
        check(&mut m, "GET k", "$-1\r\n");
        assert_eq!(m.dbsize(0), 0);
        check(&mut m, "SET k v NX GET", "$-1\r\n");
        check(&mut m, "SET k w XX GET", "$1\r\nv\r\n");
        check(&mut m, "GET k", "$1\r\nw\r\n");
        check(&mut m, "SET k x NX GET", "$1\r\nw\r\n");
        check(&mut m, "GET k", "$1\r\nw\r\n");
        check(&mut m, "SET k2 v NX GET", "$-1\r\n");
        check(&mut m, "SET k3 v XX", "$-1\r\n");
        check(&mut m, "SET k3 v NX", "+OK\r\n");
        check(&mut m, "SET k3 v NX", "$-1\r\n");
        check3(&mut m, "SET k3 v NX", "_\r\n");
        check(&mut m, "SET k v GET GET", "$1\r\nw\r\n");
    }

    #[test]
    fn set_ttl_rules() {
        let mut m = model();
        check(&mut m, "SET k v EX 100", "+OK\r\n");
        check(&mut m, "SET k v2 KEEPTTL", "+OK\r\n");
        check(&mut m, "TTL k", ":100\r\n");
        check(&mut m, "SET k v3 XX KEEPTTL GET", "$2\r\nv2\r\n");
        check(&mut m, "TTL k", ":100\r\n");
        check(&mut m, "SET k v4", "+OK\r\n");
        check(&mut m, "TTL k", ":-1\r\n");
        check(&mut m, "SET k v PXAT 1700000005000", "+OK\r\n");
        check(&mut m, "PTTL k", ":5000\r\n");
        check(&mut m, "SET k v EXAT 1700000010", "+OK\r\n");
        check(&mut m, "PTTL k", ":10000\r\n");
        check(&mut m, "GETSET k z", "$1\r\nv\r\n");
        check(&mut m, "TTL k", ":-1\r\n");
    }

    #[test]
    fn wrongtype_on_hash() {
        let mut m = model();
        m.data.insert(
            (0, b"h".to_vec()),
            crate::cmd::testkit::Rec {
                vtype: VType::Hash,
                value: vec![0; 4],
                expire: None,
            },
        );
        let wt = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        for c in [
            "GET h",
            "SET h v GET",
            "GETSET h v",
            "GETDEL h",
            "GETEX h",
            "APPEND h x",
            "STRLEN h",
            "GETRANGE h 0 1",
            "SETRANGE h 0 x",
            "INCR h",
            "DECRBY h 2",
            "INCRBYFLOAT h 1",
        ] {
            check(&mut m, c, wt);
        }
        assert_eq!(m.writes, 0, "WRONGTYPE replies stage nothing");
        check(&mut m, "SETNX h v", ":0\r\n");
        check(&mut m, "MGET h", "*1\r\n$-1\r\n");
        check(&mut m, "SET h v", "+OK\r\n");
        check(&mut m, "GET h", "$1\r\nv\r\n");
    }

    #[test]
    fn setex_psetex_setnx() {
        let mut m = model();
        check(
            &mut m,
            "SETEX k 0 v",
            "-ERR invalid expire time in 'setex' command\r\n",
        );
        check(
            &mut m,
            "PSETEX k -1 v",
            "-ERR invalid expire time in 'psetex' command\r\n",
        );
        check(
            &mut m,
            "SETEX k abc v",
            "-ERR value is not an integer or out of range\r\n",
        );
        assert_eq!(m.dbsize(0), 0, "failed SETs create no keys");
        check(&mut m, "SETEX k 10 v", "+OK\r\n");
        check(&mut m, "PTTL k", ":10000\r\n");
        check(&mut m, "PSETEX k 10 v", "+OK\r\n");
        check(&mut m, "PTTL k", ":10\r\n");
        check(&mut m, "SETNX k v", ":0\r\n");
        check(&mut m, "SETNX n v", ":1\r\n");
        check(&mut m, "TTL n", ":-1\r\n");
    }

    #[test]
    fn getex_semantics() {
        let mut m = model();
        check(&mut m, "GETEX missing EX -1", "$-1\r\n");
        check(&mut m, "GETEX missing EX abc", "$-1\r\n");
        check(&mut m, "GETEX missing NX", "-ERR syntax error\r\n");
        check(&mut m, "SET k v", "+OK\r\n");
        check(
            &mut m,
            "GETEX k EX -1",
            "-ERR invalid expire time in 'getex' command\r\n",
        );
        check(&mut m, "GETEX k EX 10 PERSIST", "-ERR syntax error\r\n");
        check(&mut m, "GETEX k", "$1\r\nv\r\n");
        check(&mut m, "GETEX k EX 10", "$1\r\nv\r\n");
        check(&mut m, "TTL k", ":10\r\n");
        check(&mut m, "GETEX k PERSIST", "$1\r\nv\r\n");
        check(&mut m, "TTL k", ":-1\r\n");
        let writes = m.writes;
        check(&mut m, "GETEX k PERSIST", "$1\r\nv\r\n");
        assert_eq!(m.writes, writes, "PERSIST without a TTL writes nothing");
        check(&mut m, "GETEX k PX 1500", "$1\r\nv\r\n");
        check(&mut m, "PTTL k", ":1500\r\n");
        check(&mut m, "GETEX k EXAT 1700000100", "$1\r\nv\r\n");
        check(&mut m, "TTL k", ":100\r\n");
        check(&mut m, "GETEX k PXAT 1", "$1\r\nv\r\n");
        assert!(m.rec(0, b"k").is_none(), "a past PXAT deletes");
        check(&mut m, "SET k v", "+OK\r\n");
        check(&mut m, "GETEX k EXAT 1700000000", "$1\r\nv\r\n");
        assert!(m.rec(0, b"k").is_none(), "EXAT at now deletes");
    }

    #[test]
    fn getset_getdel_mget_mset() {
        let mut m = model();
        check(&mut m, "GETSET q 1", "$-1\r\n");
        check(&mut m, "GETSET q 5", "$1\r\n1\r\n");
        check(&mut m, "GETDEL q", "$1\r\n5\r\n");
        check(&mut m, "GETDEL q", "$-1\r\n");
        check(
            &mut m,
            "MSET a",
            "-ERR wrong number of arguments for 'mset' command\r\n",
        );
        check(
            &mut m,
            "MSET a b c",
            "-ERR wrong number of arguments for 'mset' command\r\n",
        );
        check(
            &mut m,
            "MSETNX a 1 b",
            "-ERR wrong number of arguments for 'msetnx' command\r\n",
        );
        check(&mut m, "MSET dup 1 dup 2", "+OK\r\n");
        check(&mut m, "GET dup", "$1\r\n2\r\n");
        check(&mut m, "MSETNX q 1 a 2", ":1\r\n");
        check(&mut m, "MSETNX q 1 r 2", ":0\r\n");
        assert!(m.rec(0, b"r").is_none(), "MSETNX is all-or-nothing");
        check(
            &mut m,
            "MGET a q missing",
            "*3\r\n$1\r\n2\r\n$1\r\n1\r\n$-1\r\n",
        );
        check3(&mut m, "MGET a missing", "*2\r\n$1\r\n2\r\n_\r\n");
        check(&mut m, "SET t v EX 100", "+OK\r\n");
        check(&mut m, "MSET t w", "+OK\r\n");
        check(&mut m, "TTL t", ":-1\r\n");
    }

    #[test]
    fn append_strlen() {
        let mut m = model();
        check(&mut m, "APPEND k ab", ":2\r\n");
        check(&mut m, "EXPIRE k 100", ":1\r\n");
        check(&mut m, "APPEND k cde", ":5\r\n");
        check(&mut m, "TTL k", ":100\r\n");
        check(&mut m, "GET k", "$5\r\nabcde\r\n");
        check(&mut m, "STRLEN k", ":5\r\n");
        check(&mut m, "STRLEN missing", ":0\r\n");
        let argv: [&[u8]; 3] = [b"APPEND", b"k", b""];
        assert_eq!(run_argv(&mut m, &argv, Proto::Resp2), b":5\r\n");
        // the length limit applies to existing keys only (the argument is already bounded)
        m.set_str(b"big", &[], None);
        m.data.get_mut(&(0, b"big".to_vec())).unwrap().value = vec![b'x'; 16];
        assert!(fits(MAX_STRING_LEN - 1, 1));
        assert!(!fits(MAX_STRING_LEN, 1));
        assert!(!fits(u64::MAX, 2));
    }

    #[test]
    fn getrange_math() {
        // (start, end, len) -> bounds, Redis 7.0 rules
        assert_eq!(
            getrange_bounds(0, -100, 5),
            Some((0, 0)),
            "7.0 quirk: the first byte"
        );
        assert_eq!(getrange_bounds(0, -1, 5), Some((0, 4)));
        assert_eq!(getrange_bounds(-1, -5, 5), None);
        assert_eq!(getrange_bounds(3, 2, 5), None);
        assert_eq!(getrange_bounds(-3, -1, 5), Some((2, 4)));
        assert_eq!(getrange_bounds(-100, 100, 5), Some((0, 4)));
        assert_eq!(getrange_bounds(5, 10, 5), None);
        assert_eq!(getrange_bounds(0, 0, 0), None);
        assert_eq!(getrange_bounds(i64::MIN, i64::MAX, 5), Some((0, 4)));
        assert_eq!(getrange_bounds(i64::MIN, -1, 5), Some((0, 4)));
        assert_eq!(
            getrange_bounds(-2, i64::MIN, 5),
            None,
            "both negative and start > end"
        );
        assert_eq!(
            getrange_bounds(4, -3, 5),
            None,
            "start > end after conversion"
        );
    }

    #[test]
    fn setrange_getrange() {
        let mut m = model();
        check(&mut m, "SETRANGE k -1 x", "-ERR offset is out of range\r\n");
        check(
            &mut m,
            "SETRANGE k abc x",
            "-ERR value is not an integer or out of range\r\n",
        );
        check(
            &mut m,
            "SETRANGE k 536870911 xx",
            "-ERR string exceeds maximum allowed size (proto-max-bulk-len)\r\n",
        );
        let argv: [&[u8]; 4] = [b"SETRANGE", b"k", b"0", b""];
        assert_eq!(run_argv(&mut m, &argv, Proto::Resp2), b":0\r\n");
        assert!(m.rec(0, b"k").is_none(), "an empty value creates nothing");
        check(&mut m, "SETRANGE k 5 ab", ":7\r\n");
        check(&mut m, "GET k", "$7\r\n\0\0\0\0\0ab\r\n");
        let argv: [&[u8]; 4] = [b"SETRANGE", b"k", b"1", b""];
        assert_eq!(run_argv(&mut m, &argv, Proto::Resp2), b":7\r\n");
        check(&mut m, "EXPIRE k 100", ":1\r\n");
        check(&mut m, "SETRANGE k 1 zz", ":7\r\n");
        check(&mut m, "TTL k", ":100\r\n");
        check(
            &mut m,
            "SETRANGE k 536870911 xx",
            "-ERR string exceeds maximum allowed size (proto-max-bulk-len)\r\n",
        );
        check(&mut m, "GETRANGE k 0 -100", "$1\r\n\0\r\n");
        check(&mut m, "GETRANGE k 0 2", "$3\r\n\0zz\r\n");
        check(&mut m, "SUBSTR k -2 -1", "$2\r\nab\r\n");
        check(&mut m, "GETRANGE missing 0 -1", "$0\r\n\r\n");
        check(&mut m, "GETRANGE k -1 -5", "$0\r\n\r\n");
        check(
            &mut m,
            "GETRANGE k a 1",
            "-ERR value is not an integer or out of range\r\n",
        );
        check(
            &mut m,
            "GETRANGE missing a 1",
            "-ERR value is not an integer or out of range\r\n",
        );
    }

    #[test]
    fn chunked_values() {
        let mut m = model();
        m.chunk = 3;
        m.set_str(b"k", b"0123456789", None);
        check(&mut m, "GET k", "$10\r\n0123456789\r\n");
        check(&mut m, "GETRANGE k 2 7", "$6\r\n234567\r\n");
        check(&mut m, "GETRANGE k 3 5", "$3\r\n345\r\n");
        check(&mut m, "GETRANGE k 9 100", "$1\r\n9\r\n");
        check(&mut m, "MGET k", "*1\r\n$10\r\n0123456789\r\n");
        m.set_str(b"n", b"12345", None);
        check(
            &mut m,
            "INCR n",
            "-ERR value is not an integer or out of range\r\n",
        );
    }

    #[test]
    fn incr_family() {
        let mut m = model();
        check(&mut m, "INCR n", ":1\r\n");
        check(&mut m, "INCRBY n 41", ":42\r\n");
        check(&mut m, "DECR n", ":41\r\n");
        check(&mut m, "DECRBY n 50", ":-9\r\n");
        check(&mut m, "GET n", "$2\r\n-9\r\n");
        check(&mut m, "SET n 9223372036854775807", "+OK\r\n");
        check(
            &mut m,
            "INCR n",
            "-ERR increment or decrement would overflow\r\n",
        );
        check(
            &mut m,
            "DECRBY n -1",
            "-ERR increment or decrement would overflow\r\n",
        );
        check(
            &mut m,
            "DECRBY n -9223372036854775808",
            "-ERR decrement would overflow\r\n",
        );
        check(
            &mut m,
            "INCRBY n abc",
            "-ERR value is not an integer or out of range\r\n",
        );
        check(
            &mut m,
            "INCRBY missing abc",
            "-ERR value is not an integer or out of range\r\n",
        );
        assert!(m.rec(0, b"missing").is_none());
        check(&mut m, "SET n -9223372036854775808", "+OK\r\n");
        check(
            &mut m,
            "DECR n",
            "-ERR increment or decrement would overflow\r\n",
        );
        check(
            &mut m,
            "INCRBY n -1",
            "-ERR increment or decrement would overflow\r\n",
        );
        check(&mut m, "INCRBY n 9223372036854775807", ":-1\r\n");
        for bad in ["abc", "01", "-0", "+1", "1.5", "9223372036854775808"] {
            m.set_str(b"z", bad.as_bytes(), None);
            check(
                &mut m,
                "INCR z",
                "-ERR value is not an integer or out of range\r\n",
            );
        }
        let argv: [&[u8]; 3] = [b"SET", b"z", b" 1"];
        assert_eq!(run_argv(&mut m, &argv, Proto::Resp2), b"+OK\r\n");
        check(
            &mut m,
            "INCR z",
            "-ERR value is not an integer or out of range\r\n",
        );
        check(&mut m, "SET z 1", "+OK\r\n");
        check(&mut m, "EXPIRE z 100", ":1\r\n");
        check(&mut m, "INCR z", ":2\r\n");
        check(&mut m, "TTL z", ":100\r\n");
    }

    #[test]
    fn incrbyfloat_family() {
        let mut m = model();
        check(&mut m, "INCRBYFLOAT f 2.5", "$3\r\n2.5\r\n");
        check(&mut m, "INCRBYFLOAT f 0.5", "$1\r\n3\r\n");
        check(&mut m, "EXPIRE f 100", ":1\r\n");
        check(&mut m, "INCRBYFLOAT f 1.5", "$3\r\n4.5\r\n");
        check(&mut m, "TTL f", ":100\r\n");
        check(
            &mut m,
            "INCRBYFLOAT f abc",
            "-ERR value is not a valid float\r\n",
        );
        check(
            &mut m,
            "INCRBYFLOAT f inf",
            "-ERR increment would produce NaN or Infinity\r\n",
        );
        check(&mut m, "SET s abc", "+OK\r\n");
        check(
            &mut m,
            "INCRBYFLOAT s 1",
            "-ERR value is not a valid float\r\n",
        );
        check(&mut m, "INCRBYFLOAT g 0x10", "$2\r\n16\r\n");
        // a NUL byte anywhere makes an argument not a float (strtold must consume it all)
        let argv: [&[u8]; 3] = [b"INCRBYFLOAT", b"g", b"1\0"];
        assert_eq!(
            run_argv(&mut m, &argv, Proto::Resp2),
            b"-ERR value is not a valid float\r\n"
        );
        let mut zeros = vec![0u8; 54];
        zeros.extend_from_slice(b"nil");
        m.set_str(b"z", &zeros, None);
        check(
            &mut m,
            "INCRBYFLOAT z 1",
            "-ERR value is not a valid float\r\n",
        );
        check(&mut m, "GET f", "$3\r\n4.5\r\n");
        let _ = run(&mut m, "GET f");
    }
}
