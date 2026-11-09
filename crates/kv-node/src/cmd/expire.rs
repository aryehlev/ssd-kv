//! Expiry commands (DESIGN §8.4, §16.3, LC-10): EXPIRE / PEXPIRE / EXPIREAT / PEXPIREAT with
//! NX / XX / GT / LT (GT/LT treat "no TTL" as infinite; a non-positive relative time or a past
//! absolute time deletes the key), TTL / PTTL / EXPIRETIME / PEXPIRETIME (rounding
//! `(ms + 500) / 1000`), PERSIST. Every comparison of two expiry values lives in this module.
//!
//! The expiry-argument rules of SET, SETEX, PSETEX and GETEX ([`expire_arg_ms`]) and the
//! "already expired" test ([`already_expired`]) are here too, so that every decision that
//! compares an expiry with a time or with another expiry is made in one place. Times are Unix
//! milliseconds from the request (`Store::now_ms`).

use kv_resp::ReplyWriter;
use kv_storage::keyspace::StoreError;

use super::store::Store;
use super::{Args, err, is_opt, parse_i64};
use crate::runtime::xmsg::ExecEnv;

/// Unit of an expiry argument.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Unit {
    Seconds,
    Millis,
}

/// Why an expiry argument was refused.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum ExpireArgError {
    /// `-ERR value is not an integer or out of range`
    NotInteger,
    /// `-ERR invalid expire time in '<command>' command`
    Invalid,
}

impl ExpireArgError {
    pub(crate) fn write(self, command: &str, w: &mut ReplyWriter<'_>) {
        match self {
            ExpireArgError::NotInteger => w.error(err::NOT_INTEGER),
            ExpireArgError::Invalid => write_invalid_expire(command, w),
        }
    }
}

/// `-ERR invalid expire time in '<command>' command`.
pub(crate) fn write_invalid_expire(command: &str, w: &mut ReplyWriter<'_>) {
    let mut m = Vec::with_capacity(48 + command.len());
    m.extend_from_slice(b"ERR invalid expire time in '");
    m.extend_from_slice(command.as_bytes());
    m.extend_from_slice(b"' command");
    w.error(&m);
}

/// The expiry argument of SET EX/PX/EXAT/PXAT, SETEX, PSETEX and GETEX as an absolute Unix ms
/// time (Redis 7.0 `getExpireMillisecondsOrReply`): the value must be a positive integer, a
/// seconds value must not overflow when scaled, and a relative value is added to `now_ms`
/// (wrapping like the reference's signed addition, then refused if not positive).
pub(crate) fn expire_arg_ms(
    arg: &[u8],
    unit: Unit,
    relative: bool,
    now_ms: u64,
) -> Result<u64, ExpireArgError> {
    let v = parse_i64(arg).ok_or(ExpireArgError::NotInteger)?;
    if v <= 0 || (unit == Unit::Seconds && v > i64::MAX / 1000) {
        return Err(ExpireArgError::Invalid);
    }
    let mut ms = if unit == Unit::Seconds { v * 1000 } else { v };
    if relative {
        ms = ms.wrapping_add(now_i64(now_ms));
    }
    if ms <= 0 {
        return Err(ExpireArgError::Invalid);
    }
    Ok(ms as u64)
}

#[inline]
fn now_i64(now_ms: u64) -> i64 {
    i64::try_from(now_ms).unwrap_or(i64::MAX)
}

/// Redis `checkAlreadyExpired`: an absolute expiry at or before now deletes the key instead of
/// setting a TTL.
#[inline]
pub(crate) fn already_expired(when_ms: i64, now_ms: u64) -> bool {
    when_ms <= now_i64(now_ms)
}

/// The condition flags of the EXPIRE family.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
struct Cond {
    nx: bool,
    xx: bool,
    gt: bool,
    lt: bool,
}

/// Redis 7.0 `parseExtendedExpireArgumentsOrReply` over `argv[3..]`.
fn parse_cond(a: &Args<'_>, w: &mut ReplyWriter<'_>) -> Option<Cond> {
    let mut c = Cond::default();
    for &opt in &a.argv[3..] {
        if is_opt(opt, b"nx") {
            c.nx = true;
        } else if is_opt(opt, b"xx") {
            c.xx = true;
        } else if is_opt(opt, b"gt") {
            c.gt = true;
        } else if is_opt(opt, b"lt") {
            c.lt = true;
        } else {
            let mut m = b"ERR Unsupported option ".to_vec();
            m.extend_from_slice(super::c_str(opt));
            w.error(&m);
            return None;
        }
    }
    if c.nx && (c.xx || c.gt || c.lt) {
        w.error(b"ERR NX and XX, GT or LT options at the same time are not compatible");
        return None;
    }
    if c.gt && c.lt {
        w.error(b"ERR GT and LT options at the same time are not compatible");
        return None;
    }
    Some(c)
}

/// Whether the NX/XX/GT/LT conditions allow setting `when` over the current expiry
/// (`None` = no TTL, treated as infinite by GT and LT; LC-10).
fn cond_allows(c: Cond, current: Option<u64>, when: i64) -> bool {
    let cur = current.map(|e| i64::try_from(e).unwrap_or(i64::MAX));
    if c.nx && cur.is_some() {
        return false;
    }
    if c.xx && cur.is_none() {
        return false;
    }
    if c.gt {
        match cur {
            None => return false,
            Some(cur_expire) if when <= cur_expire => return false,
            Some(_) => {}
        }
    }
    !(c.lt && cur.is_some_and(|cur_expire| when >= cur_expire))
}

/// Base time of an EXPIRE-family command.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Base {
    Now,
    Epoch,
}

/// Redis 7.0 `expireGenericCommand`.
fn expire_generic<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    command: &str,
    base: Base,
    unit: Unit,
) -> Result<(), StoreError> {
    let Some(cond) = parse_cond(a, w) else {
        return Ok(());
    };
    let Some(mut when) = parse_i64(a.arg(2)) else {
        w.error(err::NOT_INTEGER);
        return Ok(());
    };
    if unit == Unit::Seconds {
        if !(i64::MIN / 1000..=i64::MAX / 1000).contains(&when) {
            write_invalid_expire(command, w);
            return Ok(());
        }
        when *= 1000;
    }
    let base_ms = match base {
        Base::Now => now_i64(ks.now_ms()),
        Base::Epoch => 0,
    };
    if when > i64::MAX - base_ms {
        write_invalid_expire(command, w);
        return Ok(());
    }
    when += base_ms;

    let key = a.arg(1);
    let Some(meta) = ks.meta(key)? else {
        w.int(0);
        return Ok(());
    };
    if !cond_allows(cond, meta.expire_at_ms, when) {
        w.int(0);
        return Ok(());
    }
    if already_expired(when, ks.now_ms()) {
        // a normal delete under the durable-delete rule (§16.2, §16.3)
        ks.delete(key)?;
    } else {
        ks.set_expire(key, Some(when as u64))?;
    }
    w.int(1);
    Ok(())
}

pub fn expire<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    expire_generic(ks, a, w, "expire", Base::Now, Unit::Seconds)
}

pub fn pexpire<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    expire_generic(ks, a, w, "pexpire", Base::Now, Unit::Millis)
}

pub fn expireat<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    expire_generic(ks, a, w, "expireat", Base::Epoch, Unit::Seconds)
}

pub fn pexpireat<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    expire_generic(ks, a, w, "pexpireat", Base::Epoch, Unit::Millis)
}

/// What a TTL-family command reports.
#[derive(Clone, Copy, PartialEq, Eq)]
enum TtlOut {
    Relative(Unit),
    Absolute(Unit),
}

/// Redis 7.0 `ttlGenericCommand`: -2 missing, -1 no TTL, otherwise the remaining (or absolute)
/// time clamped at 0; seconds round to nearest as `(ms + 500) / 1000`.
fn ttl_generic<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    out: TtlOut,
) -> Result<(), StoreError> {
    let Some(meta) = ks.meta(a.arg(1))? else {
        w.int(-2);
        return Ok(());
    };
    let Some(expire_at) = meta.expire_at_ms else {
        w.int(-1);
        return Ok(());
    };
    let (ms, unit) = match out {
        TtlOut::Relative(u) => (expire_at.saturating_sub(ks.now_ms()), u),
        TtlOut::Absolute(u) => (expire_at, u),
    };
    let ms = ms.min(i64::MAX as u64);
    let v = match unit {
        Unit::Millis => ms,
        Unit::Seconds => (ms + 500) / 1000,
    };
    w.int(v as i64);
    Ok(())
}

pub fn ttl<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    ttl_generic(ks, a, w, TtlOut::Relative(Unit::Seconds))
}

pub fn pttl<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    ttl_generic(ks, a, w, TtlOut::Relative(Unit::Millis))
}

pub fn expiretime<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    ttl_generic(ks, a, w, TtlOut::Absolute(Unit::Seconds))
}

pub fn pexpiretime<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    ttl_generic(ks, a, w, TtlOut::Absolute(Unit::Millis))
}

/// PERSIST: `:1` when a TTL was removed; no write when there was none.
pub fn persist<S: Store + ?Sized>(
    ks: &mut S,
    a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
    _e: &ExecEnv,
) -> Result<(), StoreError> {
    let key = a.arg(1);
    match ks.meta(key)? {
        Some(m) if m.expire_at_ms.is_some() => {
            ks.set_expire(key, None)?;
            w.int(1);
        }
        _ => w.int(0),
    }
    Ok(())
}

/// Current Unix time in microseconds (TIME). Wall-clock reads of the command layer live in
/// this module (LC-28).
pub(crate) fn unix_time_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::testkit::{Model, check, run};

    const NOW: u64 = 1_700_000_000_000;

    #[test]
    fn expire_arg_rules() {
        assert_eq!(
            expire_arg_ms(b"10", Unit::Seconds, true, NOW),
            Ok(NOW + 10_000)
        );
        assert_eq!(expire_arg_ms(b"10", Unit::Millis, true, NOW), Ok(NOW + 10));
        assert_eq!(expire_arg_ms(b"5", Unit::Seconds, false, NOW), Ok(5000));
        assert_eq!(
            expire_arg_ms(b"0", Unit::Seconds, true, NOW),
            Err(ExpireArgError::Invalid)
        );
        assert_eq!(
            expire_arg_ms(b"-1", Unit::Millis, false, NOW),
            Err(ExpireArgError::Invalid)
        );
        assert_eq!(
            expire_arg_ms(b"abc", Unit::Millis, false, NOW),
            Err(ExpireArgError::NotInteger)
        );
        assert_eq!(
            expire_arg_ms(b"+1", Unit::Millis, false, NOW),
            Err(ExpireArgError::NotInteger)
        );
        // scaling overflow, and relative additions wrapping past i64::MAX
        assert_eq!(
            expire_arg_ms(b"9223372036854776", Unit::Seconds, false, NOW),
            Err(ExpireArgError::Invalid)
        );
        assert_eq!(
            expire_arg_ms(b"9223372036854775", Unit::Seconds, true, NOW),
            Err(ExpireArgError::Invalid)
        );
        assert_eq!(
            expire_arg_ms(b"9223372036854775807", Unit::Millis, true, NOW),
            Err(ExpireArgError::Invalid)
        );
        assert_eq!(
            expire_arg_ms(b"9223372036854775807", Unit::Millis, false, NOW),
            Ok(i64::MAX as u64)
        );
        assert_eq!(
            expire_arg_ms(b"9223372036854775", Unit::Seconds, false, NOW),
            Ok(9_223_372_036_854_775_000)
        );
    }

    #[test]
    fn conditions() {
        let c = |nx, xx, gt, lt| Cond { nx, xx, gt, lt };
        // NX: only without TTL
        assert!(cond_allows(c(true, false, false, false), None, 5));
        assert!(!cond_allows(c(true, false, false, false), Some(9), 5));
        // XX: only with TTL
        assert!(!cond_allows(c(false, true, false, false), None, 5));
        assert!(cond_allows(c(false, true, false, false), Some(9), 5));
        // GT: no TTL is infinite, so GT always fails without one; strictly greater otherwise
        assert!(!cond_allows(c(false, false, true, false), None, i64::MAX));
        assert!(!cond_allows(c(false, false, true, false), Some(9), 9));
        assert!(cond_allows(c(false, false, true, false), Some(9), 10));
        // LT: no TTL is infinite, so LT always passes without one; strictly lower otherwise
        assert!(cond_allows(c(false, false, false, true), None, i64::MAX));
        assert!(!cond_allows(c(false, false, false, true), Some(9), 9));
        assert!(cond_allows(c(false, false, false, true), Some(9), -5));
        // XX + GT
        assert!(!cond_allows(c(false, true, true, false), None, 5));
        assert!(cond_allows(c(false, true, true, false), Some(4), 5));
    }

    #[test]
    fn expire_family_replies() {
        let mut m = Model::new(NOW);
        m.set_str(b"k", b"v", None);
        check(
            &mut m,
            "EXPIRE k 100 NX XX",
            "-ERR NX and XX, GT or LT options at the same time are not compatible\r\n",
        );
        check(
            &mut m,
            "EXPIRE k 100 GT LT",
            "-ERR GT and LT options at the same time are not compatible\r\n",
        );
        check(
            &mut m,
            "EXPIRE k 100 NX GT",
            "-ERR NX and XX, GT or LT options at the same time are not compatible\r\n",
        );
        check(&mut m, "EXPIRE k 100 XX GT", ":0\r\n");
        check(&mut m, "EXPIRE k 100 LT", ":1\r\n");
        check(&mut m, "TTL k", ":100\r\n");
        check(&mut m, "EXPIRE k 50 GT", ":0\r\n");
        check(&mut m, "EXPIRE k 150 gt", ":1\r\n");
        check(&mut m, "PTTL k", ":150000\r\n");
        check(
            &mut m,
            "EXPIRE k abc",
            "-ERR value is not an integer or out of range\r\n",
        );
        check(
            &mut m,
            "EXPIRE missing abc",
            "-ERR value is not an integer or out of range\r\n",
        );
        check(
            &mut m,
            "EXPIRE missing 10 foo",
            "-ERR Unsupported option foo\r\n",
        );
        check(&mut m, "EXPIRE missing 10", ":0\r\n");
        check(
            &mut m,
            "EXPIRE k 9223372036854776",
            "-ERR invalid expire time in 'expire' command\r\n",
        );
        check(
            &mut m,
            "EXPIRE k -9223372036854776",
            "-ERR invalid expire time in 'expire' command\r\n",
        );
        check(
            &mut m,
            "PEXPIRE k 9223372036854775807",
            "-ERR invalid expire time in 'pexpire' command\r\n",
        );
        check(
            &mut m,
            "EXPIREAT k 9223372036854775807",
            "-ERR invalid expire time in 'expireat' command\r\n",
        );
        check(&mut m, "PEXPIREAT k 9223372036854775807", ":1\r\n");
        check(&mut m, "PTTL k", &format!(":{}\r\n", i64::MAX as u64 - NOW));
        check(&mut m, "EXPIRETIME k", ":9223372036854776\r\n");
        check(&mut m, "PEXPIRETIME k", ":9223372036854775807\r\n");
        check(&mut m, "PERSIST k", ":1\r\n");
        let writes = m.writes;
        check(&mut m, "PERSIST k", ":0\r\n");
        assert_eq!(m.writes, writes, "PERSIST without a TTL writes nothing");
        check(&mut m, "TTL k", ":-1\r\n");
        check(&mut m, "EXPIRETIME k", ":-1\r\n");
        check(&mut m, "EXPIRE k 0", ":1\r\n");
        assert!(
            m.rec(0, b"k").is_none(),
            "a non-positive relative expiry deletes"
        );
        check(&mut m, "EXPIRE k 100", ":0\r\n");
        for c in [
            "TTL missing",
            "PTTL missing",
            "EXPIRETIME missing",
            "PEXPIRETIME missing",
        ] {
            check(&mut m, c, ":-2\r\n");
        }
    }

    #[test]
    fn past_absolute_times_delete() {
        let mut m = Model::new(NOW);
        m.set_str(b"k", b"v", None);
        check(&mut m, &format!("EXPIREAT k {}", NOW / 1000 - 1), ":1\r\n");
        assert!(m.rec(0, b"k").is_none());
        m.set_str(b"k", b"v", None);
        check(&mut m, &format!("PEXPIREAT k {NOW}"), ":1\r\n");
        assert!(
            m.rec(0, b"k").is_none(),
            "an expiry equal to now is already expired"
        );
        m.set_str(b"k", b"v", None);
        check(&mut m, "PEXPIRE k -5 LT", ":1\r\n");
        assert!(m.rec(0, b"k").is_none());
        // GT with a past time on a key without TTL fails before the delete
        m.set_str(b"k", b"v", None);
        check(&mut m, "PEXPIRE k -5 GT", ":0\r\n");
        assert!(m.rec(0, b"k").is_some());
    }

    #[test]
    fn ttl_rounding() {
        let mut m = Model::new(NOW);
        m.set_str(b"a", b"v", Some(NOW + 1499));
        m.set_str(b"b", b"v", Some(NOW + 1500));
        m.set_str(b"c", b"v", Some(NOW + 1));
        check(&mut m, "TTL a", ":1\r\n");
        check(&mut m, "TTL b", ":2\r\n");
        check(&mut m, "TTL c", ":0\r\n");
        check(&mut m, "PTTL c", ":1\r\n");
        m.set_str(b"d", b"v", Some(500_300));
        m.set_str(b"e", b"v", Some(500_700));
        // absolute times in the past are still reported for keys not yet touched by expiry
        m.now = 0;
        check(&mut m, "EXPIRETIME d", ":500\r\n");
        check(&mut m, "EXPIRETIME e", ":501\r\n");
        check(&mut m, "PEXPIRETIME e", ":500700\r\n");
    }

    #[test]
    fn expired_keys_are_absent() {
        let mut m = Model::new(NOW);
        m.set_str(b"k", b"v", Some(NOW));
        check(&mut m, "TTL k", ":-2\r\n");
        check(&mut m, "EXPIRE k 10", ":0\r\n");
        check(&mut m, "PERSIST k", ":0\r\n");
    }

    #[test]
    fn options_are_c_strings() {
        let mut m = Model::new(NOW);
        m.set_str(b"k", b"v", Some(NOW + 5000));
        let argv: [&[u8]; 4] = [b"EXPIRE", b"k", b"100", b"nx\0a"];
        let got = crate::cmd::testkit::run_argv(&mut m, &argv, kv_resp::Proto::Resp2);
        assert_eq!(got, b":0\r\n");
        let argv: [&[u8]; 4] = [b"EXPIRE", b"k", b"100", b"f\0oo"];
        let got = crate::cmd::testkit::run_argv(&mut m, &argv, kv_resp::Proto::Resp2);
        assert_eq!(got, b"-ERR Unsupported option f\r\n");
        let _ = run(&mut m, "TTL k");
    }
}
