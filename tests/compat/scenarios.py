"""Deterministic hand-written compatibility scenarios (pytest), target only.

    TARGET=127.0.0.1:7777 pytest scenarios.py            # default target
    TARGET=127.0.0.1:7777 pytest scenarios.py -m "not impl_specific"

Expected values were taken from real Redis 7.0.15 (x86-64).  Tests marked
``impl_specific`` check behaviour that is legitimately implementation-specific
(active expiry timing, INFO field names, DEBUG, x87 long-double float
formatting, inline protocol, protocol-error handling, COMMAND metadata).
Real Redis passes them; a from-scratch server may choose to differ.

WARNING: every test starts by flushing all 16 databases of the target.
"""

from __future__ import annotations

import os
import socket
import threading
import time

import pytest
import redis

from compat_common import RawConn, encode_command, make_typed_client, parse_hostport

HOST, PORT = parse_hostport(os.environ.get("TARGET", "127.0.0.1:7777"))
NUM_DBS = 16
OK = ("+", b"OK")
NIL2 = ("$", None)  # RESP2 null bulk
NIL3 = ("_", None)  # RESP3 null

impl_specific = pytest.mark.impl_specific


# --------------------------------------------------------------------------
# fixtures
# --------------------------------------------------------------------------
def _client(**kw) -> redis.Redis:
    kw.setdefault("protocol", 2)
    return redis.Redis(host=HOST, port=PORT, decode_responses=False, socket_timeout=60,
                       lib_name=None, lib_version=None, **kw)


def _flush_all_dbs():
    c = make_typed_client(HOST, PORT)
    try:
        for db in range(NUM_DBS):
            assert c.execute_command("SELECT", db) == OK
            assert c.execute_command("FLUSHDB") == OK
    finally:
        c.close()


@pytest.fixture(scope="session", autouse=True)
def _target_reachable():
    try:
        socket.create_connection((HOST, PORT), timeout=3).close()
    except OSError as e:
        pytest.exit(f"target {HOST}:{PORT} not reachable ({e}); set TARGET=host:port", returncode=2)


@pytest.fixture(autouse=True)
def clean():
    _flush_all_dbs()
    yield


@pytest.fixture
def r():
    c = _client()
    yield c
    c.close()


@pytest.fixture
def typed():
    """Typed raw replies: ('+', b'OK'), ('$', b'..'), ('_', None), ..."""
    c = make_typed_client(HOST, PORT)
    yield c
    c.close()


@pytest.fixture
def typed3():
    c = make_typed_client(HOST, PORT)
    hello = c.execute_command("HELLO", "3")
    assert hello[0] == "%", f"HELLO 3 did not return a RESP3 map: {hello!r}"
    yield c
    c.close()


@pytest.fixture
def raw():
    c = RawConn(HOST, PORT)
    yield c
    c.close()


def err_prefix(reply) -> bytes:
    assert reply[0] in ("-", "!"), f"expected an error reply, got {reply!r}"
    return reply[1].split(b" ", 1)[0]


# --------------------------------------------------------------------------
# basics
# --------------------------------------------------------------------------
def test_ping_echo(typed):
    assert typed.execute_command("PING") == ("+", b"PONG")
    assert typed.execute_command("PING", b"hi\x00there") == ("$", b"hi\x00there")
    assert typed.execute_command("ECHO", b"") == ("$", b"")
    assert typed.execute_command("ECHO", b"\r\n") == ("$", b"\r\n")


def test_errors_keep_connection_usable(typed):
    assert err_prefix(typed.execute_command("GET")) == b"ERR"
    assert err_prefix(typed.execute_command("NOSUCHCOMMAND", "x")) == b"ERR"
    assert err_prefix(typed.execute_command("SET", "k", "v", "NX", "XX")) == b"ERR"
    assert typed.execute_command("SET", "k", "v") == OK
    assert err_prefix(typed.execute_command("INCR", "k")) == b"ERR"
    assert typed.execute_command("GET", "k") == ("$", b"v")


@impl_specific
def test_error_messages_match_redis(typed):
    """Exact Redis 7.0 error texts (clients rarely depend on them)."""
    assert typed.execute_command("GET") == ("-", b"ERR wrong number of arguments for 'get' command")
    assert typed.execute_command("SET", "k", "v", "NX", "XX") == ("-", b"ERR syntax error")
    typed.execute_command("SET", "k", "v")
    assert typed.execute_command("INCR", "k") == ("-", b"ERR value is not an integer or out of range")
    assert typed.execute_command("SELECT", "16") == ("-", b"ERR DB index is out of range")


# --------------------------------------------------------------------------
# pipelining
# --------------------------------------------------------------------------
def test_pipelining_10k_sets_then_gets(r):
    n = 10_000
    p = r.pipeline(transaction=False)
    for i in range(n):
        p.set(f"pl:{i}", f"value-{i}")
    assert p.execute() == [True] * n
    p = r.pipeline(transaction=False)
    for i in range(n):
        p.get(f"pl:{i}")
    got = p.execute()
    assert got == [f"value-{i}".encode() for i in range(n)]
    assert r.dbsize() == n


def test_pipelining_single_write_mixed_replies(raw):
    """20k commands written in ONE send; replies must come back in order,
    errors inline, no reply lost or merged."""
    n = 10_000
    buf = bytearray()
    for i in range(n):
        buf += encode_command("SET", f"raw:{i}", f"v{i}")
    for i in range(n):
        buf += encode_command("GET", f"raw:{i}") if i % 100 else encode_command("INCR", f"raw:{i}")
    raw.send_raw(bytes(buf))
    for i in range(n):
        assert raw.read() == OK
    for i in range(n):
        rep = raw.read()
        if i % 100:
            assert rep == ("$", f"v{i}".encode())
        else:
            assert rep[0] == "-"


def test_pipelining_fragmented_writes(raw):
    """Commands split at every possible byte boundary still parse."""
    payload = encode_command("SET", "frag", "x" * 50) + encode_command("GET", "frag")
    for i in range(len(payload)):
        raw.send_raw(payload[i:i + 1])
        if i % 7 == 0:
            time.sleep(0.001)
    assert raw.read() == OK
    assert raw.read() == ("$", b"x" * 50)
    # header split between "*2\r" and "\n"
    raw.send_raw(b"*2\r")
    time.sleep(0.05)
    raw.send_raw(b"\n$3\r\nGET\r\n$4\r\nfr")
    time.sleep(0.05)
    raw.send_raw(b"ag\r\n")
    assert raw.read() == ("$", b"x" * 50)


# --------------------------------------------------------------------------
# large values
# --------------------------------------------------------------------------
@pytest.mark.parametrize("size", [1 << 20, 16 << 20], ids=["1MiB", "16MiB"])
def test_large_values(r, size):
    val = os.urandom(1024) * (size // 1024)
    assert r.set("big", val) is True
    assert r.strlen("big") == size
    assert r.get("big") == val
    assert r.getrange("big", -10, -1) == val[-10:]
    assert r.getrange("big", size // 2, size // 2 + 99) == val[size // 2: size // 2 + 100]
    assert r.append("big", b"!") == size + 1
    assert r.get("big") == val + b"!"
    # shrink, then grow again (size changes on overwrite)
    assert r.set("big", b"small") is True
    assert r.get("big") == b"small"
    assert r.set("big", val) is True
    assert r.get("big") == val
    assert r.delete("big") == 1


def test_large_value_setrange_padding(r):
    assert r.setrange("pad", 1 << 20, b"end") == (1 << 20) + 3
    v = r.get("pad")
    assert len(v) == (1 << 20) + 3 and v[:-3] == b"\x00" * (1 << 20) and v[-3:] == b"end"


def test_many_large_values_pipelined(r):
    vals = {f"lv:{i}": os.urandom(256 * 1024) for i in range(32)}  # 8 MiB in one pipeline
    p = r.pipeline(transaction=False)
    for k, v in vals.items():
        p.set(k, v)
    p.execute()
    assert r.mget(list(vals)) == list(vals.values())


# --------------------------------------------------------------------------
# binary safety
# --------------------------------------------------------------------------
def test_binary_safe_keys_and_values(r):
    all_bytes = bytes(range(256))
    pairs = {
        b"": b"empty-key",
        b"\x00": all_bytes,
        b"\r\n": b"\r\n\r\n",
        all_bytes: b"",
        "ключ🔑".encode(): "значение".encode(),
        b"*1\r\n$4\r\nPING\r\n": b"$-1\r\n",
        b"\xff\xfe\xfd": b"\x00" * 1000,
    }
    for k, v in pairs.items():
        assert r.set(k, v) is True
    for k, v in pairs.items():
        assert r.get(k) == v
        assert r.strlen(k) == len(v)
        assert r.exists(k) == 1
    assert r.mget(list(pairs)) == list(pairs.values())
    assert set(r.keys("*")) == set(pairs)
    assert r.getrange(b"\x00", 250, 255) == bytes(range(250, 256))
    assert r.append(b"", b"\x00") == len(b"empty-key") + 1
    assert r.get(b"") == b"empty-key\x00"
    assert r.delete(*pairs) == len(pairs)
    assert r.dbsize() == 0


def test_empty_value_vs_missing(typed):
    assert typed.execute_command("SET", "e", b"") == OK
    assert typed.execute_command("GET", "e") == ("$", b"")
    assert typed.execute_command("EXISTS", "e") == (":", b"1")
    assert typed.execute_command("STRLEN", "e") == (":", b"0")
    assert typed.execute_command("GET", "missing") == NIL2
    assert typed.execute_command("TYPE", "e") == ("+", b"string")
    assert typed.execute_command("TYPE", "missing") == ("+", b"none")


# --------------------------------------------------------------------------
# expiry
# --------------------------------------------------------------------------
def test_expiry_removes_key(r):
    r.set("keep", "1")
    assert r.set("short", "v", px=200) is True
    assert 0 < r.pttl("short") <= 200
    assert r.dbsize() == 2
    time.sleep(0.4)
    assert r.get("short") is None
    assert r.exists("short") == 0
    assert r.ttl("short") == -2
    assert r.pttl("short") == -2
    assert r.dbsize() == 1, "DBSIZE must exclude the expired key"
    assert r.keys("*") == [b"keep"]


def test_expired_keys_hidden_without_prior_access(r):
    """KEYS / SCAN / EXISTS / MGET must never return logically expired keys,
    even if the server only expires lazily."""
    for i in range(20):
        r.set(f"exp:{i}", "v", px=150)
    r.set("stay", "v")
    time.sleep(0.35)
    assert r.keys("exp:*") == []
    assert sorted(r.scan_iter(match="*", count=5)) == [b"stay"]
    assert r.mget([f"exp:{i}" for i in range(20)]) == [None] * 20
    assert r.exists(*[f"exp:{i}" for i in range(20)]) == 0


@impl_specific
def test_active_expiry_reclaims_untouched_keys(r):
    """Redis also expires keys nobody touches (active expiry), so DBSIZE drops
    on its own.  A lazy-only server would legitimately fail this."""
    for i in range(50):
        r.set(f"act:{i}", "v", px=100)
    deadline = time.time() + 3
    while time.time() < deadline and r.dbsize():
        time.sleep(0.1)
    assert r.dbsize() == 0


def test_expiry_variants_really_expire(r):
    now_ms = int(time.time() * 1000)
    r.psetex("v1", 200, "x")
    r.set("v2", "x")
    r.pexpire("v2", 200)
    r.set("v3", "x", pxat=now_ms + 200)
    r.set("v4", "x")
    r.getex("v4", px=200)
    r.set("v5", "x", px=200)
    r.rename("v5", "v5renamed")  # RENAME carries the TTL
    r.set("v6", "x", px=200)
    r.set("v6", "y", keepttl=True)
    r.set("v7", "x", px=200)
    r.set("v7", "y")  # plain SET clears the TTL
    r.set("v8", "x", px=200)
    r.persist("v8")
    r.set("v9", "x", px=200)
    r.append("v9", "more")  # APPEND keeps the TTL
    time.sleep(0.45)
    for k in ("v1", "v2", "v3", "v4", "v5renamed", "v6", "v9"):
        assert r.get(k) is None, k
    assert r.get("v7") == b"y"
    assert r.get("v8") == b"x"
    assert r.dbsize() == 2


def test_ttl_commands(r):
    assert r.ttl("nokey") == -2 and r.pttl("nokey") == -2
    assert r.expiretime("nokey") == -2 and r.pexpiretime("nokey") == -2
    r.set("k", "v")
    assert r.ttl("k") == -1 and r.pttl("k") == -1
    assert r.expiretime("k") == -1 and r.pexpiretime("k") == -1
    r.set("k", "v", ex=100)
    assert 99 <= r.ttl("k") <= 100
    assert 99_000 <= r.pttl("k") <= 100_000
    base_s = int(time.time()) + 500
    for frac, rounded_s in ((300, base_s), (700, base_s + 1)):  # EXPIRETIME rounds to nearest second
        at_ms = base_s * 1000 + frac
        assert r.pexpireat("k", at_ms) is True
        assert r.pexpiretime("k") == at_ms
        assert r.expiretime("k") == rounded_s
    at_s = int(time.time()) + 1000
    assert r.expireat("k", at_s) is True
    assert r.expiretime("k") == at_s
    assert r.pexpiretime("k") == at_s * 1000
    assert r.persist("k") is True
    assert r.persist("k") is False
    assert r.ttl("k") == -1
    assert r.expire("nokey", 100) is False
    # GETEX
    r.set("g", "v")
    assert r.getex("g", ex=50) == b"v"
    assert 49 <= r.ttl("g") <= 50
    assert r.getex("g", persist=True) == b"v"
    assert r.ttl("g") == -1
    assert r.getex("nokey", ex=5) is None


def test_expire_in_past_deletes_immediately(r):
    for k in ("a", "b", "c", "d", "e"):
        r.set(k, "v")
    assert r.expire("a", -1) is True
    assert r.expire("b", 0) is True
    assert r.expireat("c", 1) is True
    assert r.pexpireat("d", 1) is True
    assert r.execute_command("GETEX", "e", "EXAT", 1) == b"v"
    assert r.dbsize() == 0


def test_expire_flags(typed):
    ex = lambda *a: typed.execute_command("EXPIRE", *a)  # noqa: E731
    typed.execute_command("SET", "k", "v")
    assert ex("k", 100, "XX") == (":", b"0")  # no TTL yet
    assert ex("k", 100, "GT") == (":", b"0")  # no TTL counts as infinite
    assert ex("k", 100, "NX") == (":", b"1")
    assert ex("k", 200, "NX") == (":", b"0")
    assert ex("k", 50, "GT") == (":", b"0")
    assert ex("k", 500, "GT") == (":", b"1")
    assert ex("k", 1000, "LT") == (":", b"0")
    assert ex("k", 300, "LT") == (":", b"1")
    assert ex("k", 400, "XX") == (":", b"1")
    typed.execute_command("PERSIST", "k")
    assert ex("k", 100, "LT") == (":", b"1")  # no TTL counts as infinite
    assert err_prefix(ex("k", 100, "NX", "XX")) == b"ERR"
    assert err_prefix(ex("k", 100, "GT", "LT")) == b"ERR"
    assert err_prefix(ex("k", 100, "NX", "GT")) == b"ERR"
    assert err_prefix(ex("k", 100, "BOGUS")) == b"ERR"
    assert err_prefix(ex("k", "abc")) == b"ERR"
    assert err_prefix(ex("k", 9223372036854775807)) == b"ERR"


def test_set_expiry_option_validation(typed):
    s = lambda *a: typed.execute_command("SET", "k", "v", *a)  # noqa: E731
    for bad in (("EX", 0), ("EX", -1), ("PX", 0), ("EXAT", 0), ("EX", "abc"), ("EX", 9223372036854775807),
                ("EX", 10, "PX", 10), ("EX", 10, "KEEPTTL"), ("NX", "XX"), ("EX",), ("BOGUS",)):
        assert err_prefix(s(*bad)) == b"ERR", bad
    assert err_prefix(typed.execute_command("SETEX", "k", 0, "v")) == b"ERR"
    assert err_prefix(typed.execute_command("PSETEX", "k", -5, "v")) == b"ERR"
    assert err_prefix(typed.execute_command("GETEX", "k", "EX", 10, "PERSIST")) == b"ERR"
    assert typed.execute_command("DBSIZE") == (":", b"0"), "failed SETs must not create keys"


# --------------------------------------------------------------------------
# SET family semantics
# --------------------------------------------------------------------------
def test_set_nx_xx_get(typed):
    s = typed.execute_command
    assert s("SET", "k", "v1", "XX") == NIL2
    assert s("SET", "k", "v1", "NX") == OK
    assert s("SET", "k", "v2", "NX") == NIL2
    assert s("SET", "k", "v2", "XX") == OK
    assert s("SET", "k", "v3", "GET") == ("$", b"v2")
    assert s("SET", "new", "x", "GET") == NIL2
    assert s("SET", "k", "v4", "NX", "GET") == ("$", b"v3")  # NX+GET allowed since 7.0
    assert s("GET", "k") == ("$", b"v3")
    assert s("SET", "k", "v5", "XX", "GET") == ("$", b"v3")
    assert s("SETNX", "k", "x") == (":", b"0")
    assert s("SETNX", "k2", "x") == (":", b"1")
    assert s("GETSET", "k", "v6") == ("$", b"v5")
    assert s("GETSET", "k3", "v") == NIL2
    assert s("GETDEL", "k") == ("$", b"v6")
    assert s("GETDEL", "k") == NIL2
    assert s("SET", "ttl", "v", "EX", 100) == OK
    assert s("SET", "ttl", "w", "KEEPTTL") == OK
    assert s("TTL", "ttl")[1] in (b"99", b"100")
    assert s("SETEX", "se", 100, "v") == OK and s("TTL", "se")[1] in (b"99", b"100")
    assert s("PSETEX", "pse", 100000, "v") == OK and s("TTL", "pse")[1] in (b"99", b"100")


def test_mset_mget_msetnx(r):
    assert r.mset({"a": 1, "b": 2, "c": 3}) is True
    assert r.mget("a", "nokey", "c", "a") == [b"1", None, b"3", b"1"]
    assert r.msetnx({"c": 9, "d": 4}) is False  # atomic: nothing set
    assert r.get("d") is None and r.get("c") == b"3"
    assert r.msetnx({"d": 4, "e": 5}) is True
    assert r.execute_command("MSET", "dup", "1", "dup", "2") is True
    assert r.get("dup") == b"2"
    big = {f"m:{i}": f"v{i}" for i in range(10_000)}
    assert r.mset(big) is True
    assert r.mget(list(big)) == [v.encode() for v in big.values()]


def test_append_strlen_getrange_setrange(r):
    assert r.append("s", "Hello") == 5
    assert r.append("s", " World") == 11
    assert r.strlen("s") == 11 and r.strlen("nokey") == 0
    assert r.getrange("s", 0, 4) == b"Hello"
    assert r.getrange("s", -5, -1) == b"World"
    assert r.getrange("s", 5, 2) == b""
    assert r.getrange("s", -100, 100) == b"Hello World"
    # Redis 7.0 quirk: a negative end clamped to 0 still yields the first byte
    # (7.2 changed this to an empty string); the reference is 7.0.15.
    assert r.getrange("s", 0, -100) == b"H"
    assert r.getrange("s", -100, -100) == b"H"
    assert r.getrange("nokey", 0, -1) == b""
    assert r.setrange("s", 6, "Redis") == 11
    assert r.get("s") == b"Hello Redis"
    assert r.setrange("pad", 5, "x") == 6
    assert r.get("pad") == b"\x00" * 5 + b"x"
    assert r.setrange("nokey2", 0, "") == 0
    assert r.exists("nokey2") == 0, "SETRANGE with empty value must not create the key"
    with pytest.raises(redis.ResponseError):
        r.setrange("s", -1, "x")
    with pytest.raises(redis.ResponseError):
        r.setrange("s", 536870912, "x")  # beyond 512 MiB


def test_incr_decr_edges(typed):
    s = typed.execute_command
    assert s("INCR", "n") == (":", b"1")
    assert s("INCRBY", "n", 41) == (":", b"42")
    assert s("DECR", "n") == (":", b"41")
    assert s("DECRBY", "n", -9) == (":", b"50")
    assert s("SET", "max", "9223372036854775807") == OK
    assert err_prefix(s("INCR", "max")) == b"ERR"
    assert s("GET", "max") == ("$", b"9223372036854775807")
    assert s("SET", "min", "-9223372036854775808") == OK
    assert err_prefix(s("DECR", "min")) == b"ERR"
    assert s("INCRBY", "min", "9223372036854775807") == (":", b"-1")
    for bad in (b"abc", b" 1", b"1 ", b"+1", b"01", b"1.0", b"", b"9223372036854775808", b"-0"):
        s("SET", "bad", bad)
        assert err_prefix(s("INCR", "bad")) == b"ERR", bad
        assert s("GET", "bad") == ("$", bad)
    assert err_prefix(s("INCRBY", "n", "1.5")) == b"ERR"
    assert err_prefix(s("INCRBY", "n", "9223372036854775808")) == b"ERR"
    assert err_prefix(s("DECRBY", "n", "-9223372036854775808")) == b"ERR"
    s("SET", "big", "x" * 100)
    assert err_prefix(s("INCR", "big")) == b"ERR"


def test_incrbyfloat_portable(typed):
    """Results exactly representable in binary floating point: same output
    under double and long double."""
    s = typed.execute_command
    cases = [(None, "2.5", b"2.5"), ("5", "1e3", b"1005"), ("3", "-3", b"0"), ("-0.0", "0", b"0"),
             (None, "5.0e3", b"5000"), ("2.5", "2.5", b"5"), ("-5", "2.5", b"-2.5"), (None, ".5", b"0.5"),
             (None, "5.", b"5"), ("7", "0x10", b"23"), (None, "1E2", b"100"), ("10", "0.25", b"10.25"),
             ("9007199254740993", "1", b"9007199254740994"), (None, "1e-20", b"0")]
    for init, inc, want in cases:
        s("DEL", "f")
        if init is not None:
            s("SET", "f", init)
        assert s("INCRBYFLOAT", "f", inc) == ("$", want), (init, inc)
        assert s("GET", "f") == ("$", want)
    for init, inc in (("abc", "1"), ("1", "abc"), ("1", "inf"), ("1", "nan"), (" 1", "1"), ("1", "1 "),
                      (None, ""), ("1", "-inf")):
        s("DEL", "f")
        if init is not None:
            s("SET", "f", init)
        assert err_prefix(s("INCRBYFLOAT", "f", inc)) == b"ERR", (init, inc)


@impl_specific
def test_incrbyfloat_long_double_formatting(typed):
    """Redis on x86-64 computes in 80-bit long double and prints %.17Lf with
    trailing zeros trimmed; differs from f64 + shortest-repr formatting (and
    even from Redis built for aarch64/Apple Silicon)."""
    s = typed.execute_command
    cases = [(None, "0.1", b"0.1"), ("0.1", "0.2", b"0.3"), ("1.1", "2.2", b"3.3"), ("10.50", "0.1", b"10.6"),
             (None, "1.5e-5", b"0.000015"), ("100", "-0.001", b"99.999"), (None, "-1e-10", b"-0.0000000001"),
             ("1", "1e17", b"100000000000000001"), (None, "3.0000000000000000001", b"3"),
             (None, "0.30000000000000004", b"0.30000000000000004"),
             (None, "123456789012345678901234567890", b"123456789012345678899921813504")]
    for init, inc, want in cases:
        s("DEL", "f")
        if init is not None:
            s("SET", "f", init)
        assert s("INCRBYFLOAT", "f", inc) == ("$", want), (init, inc)


# --------------------------------------------------------------------------
# keyspace commands
# --------------------------------------------------------------------------
def test_del_unlink_exists_counts(r):
    r.mset({"a": 1, "b": 2, "c": 3, "d": 4})
    assert r.exists("a", "a", "nokey", "b") == 3
    assert r.delete("a", "b", "nokey") == 2
    assert r.delete("a") == 0
    assert r.delete("c", "c") == 1
    assert r.unlink("d", "nokey", "d") == 1
    assert r.unlink("nokey") == 0
    assert r.dbsize() == 0
    for i in range(100):
        r.set(f"many:{i}", i)
    assert r.delete(*[f"many:{i}" for i in range(0, 100, 2)]) == 50
    assert r.unlink(*[f"many:{i}" for i in range(100)]) == 50


def test_rename(typed):
    s = typed.execute_command
    assert err_prefix(s("RENAME", "nokey", "x")) == b"ERR"
    assert err_prefix(s("RENAMENX", "nokey", "x")) == b"ERR"
    s("SET", "src", "v")
    s("PEXPIRE", "src", 100000)
    assert s("RENAME", "src", "dst") == OK
    assert s("EXISTS", "src") == (":", b"0")
    assert s("GET", "dst") == ("$", b"v")
    assert 0 < int(s("PTTL", "dst")[1]) <= 100000, "RENAME must carry the TTL"
    s("SET", "other", "o")
    assert s("RENAMENX", "dst", "other") == (":", b"0")
    assert s("RENAMENX", "dst", "fresh") == (":", b"1")
    assert s("RENAME", "fresh", "fresh") == OK
    assert s("RENAMENX", "fresh", "fresh") == (":", b"0")
    assert s("RENAME", "fresh", "other") == OK  # overwrites
    assert s("GET", "other") == ("$", b"v")
    assert s("DBSIZE") == (":", b"1")


def test_keys_glob_patterns(r):
    keys = [b"hello", b"hallo", b"hxllo", b"hllo", b"heello", b"a*b", b"axb", b"a?c", b"abc", b"key:1",
            b"key:2", b"key:10", b"back\\slash", b"[x]", b"x", b"", b"\x00bin", b"h^llo", b"h-llo",
            "键1".encode()]
    for k in keys:
        r.set(k, b"v")
    expect = {
        b"*": sorted(keys),
        b"h?llo": [b"h-llo", b"h^llo", b"hallo", b"hello", b"hxllo"],
        b"h*llo": [b"h-llo", b"h^llo", b"hallo", b"heello", b"hello", b"hllo", b"hxllo"],
        b"h[ae]llo": [b"hallo", b"hello"],
        b"h[^e]llo": [b"h-llo", b"h^llo", b"hallo", b"hxllo"],
        b"h[a-e]llo": [b"hallo", b"hello"],
        b"h[e-a]llo": [b"hallo", b"hello"],
        b"a\\*b": [b"a*b"],
        b"a*b": [b"a*b", b"axb"],
        b"a\\?c": [b"a?c"],
        b"a?c": [b"a?c", b"abc"],
        b"key:?": [b"key:1", b"key:2"],
        b"key:*": [b"key:1", b"key:10", b"key:2"],
        b"*\\\\*": [b"back\\slash"],
        b"\\[x\\]": [b"[x]"],
        b"?": [b"x"],  # ? is one BYTE, so the 4-byte "键1" does not match
        b"": [b""],
        b"\x00*": [b"\x00bin"],
        "键*".encode(): ["键1".encode()],
        b"h[\\^]llo": [b"h^llo"],
        b"nomatch": [],
        b"[abk]*": [b"a*b", b"a?c", b"abc", b"axb", b"back\\slash", b"key:1", b"key:10", b"key:2"],
        b"*[0-9]": [b"key:1", b"key:10", b"key:2", "键1".encode()],
    }
    for pat, want in expect.items():
        assert sorted(r.keys(pat)) == sorted(want), pat


def test_scan_full_iteration(typed):
    s = typed.execute_command
    names = {f"scan:{i}".encode() for i in range(1000)} | {b"other:1", b"other:2", b""}
    for k in names:
        s("SET", k, "v")

    def full(*opts):
        cursor, seen, steps = b"0", set(), 0
        while True:
            rep = s("SCAN", cursor, *opts)
            assert rep[0] == "*" and len(rep[1]) == 2, rep
            (ct, cursor), (at, items) = rep[1]
            assert ct == "$" and at == "*", f"SCAN reply shape must be [bulk, array]: {rep!r:.200}"
            for t, k in items:
                assert t == "$"
                seen.add(k)
            steps += 1
            assert steps < 100_000
            if cursor == b"0":
                return seen, steps

    assert full()[0] == names
    seen, steps = full("COUNT", 10)
    assert seen == names and steps > 1
    assert full("MATCH", "other:*")[0] == {b"other:1", b"other:2"}
    assert full("MATCH", "scan:99?", "COUNT", 50)[0] == {f"scan:{i}".encode() for i in range(990, 1000)}
    assert full("TYPE", "string")[0] == names
    assert full("TYPE", "list")[0] == set()
    assert err_prefix(s("SCAN", "abc")) == b"ERR"
    assert err_prefix(s("SCAN", "0", "COUNT", "0")) == b"ERR"
    assert err_prefix(s("SCAN", "0", "MATCH")) == b"ERR"


def test_select_isolation(typed):
    c0 = typed
    c1 = make_typed_client(HOST, PORT)
    try:
        assert c1.execute_command("SELECT", 1) == OK
        assert c1.execute_command("SET", "k", "db1") == OK
        assert c0.execute_command("GET", "k") == NIL2, "SELECT must be per connection"
        assert c0.execute_command("SET", "k", "db0") == OK
        assert c1.execute_command("GET", "k") == ("$", b"db1")
        assert c0.execute_command("SELECT", 15) == OK
        assert c0.execute_command("DBSIZE") == (":", b"0")
        assert c0.execute_command("KEYS", "*") == ("*", ())
        assert c0.execute_command("SELECT", 0) == OK
        assert c0.execute_command("GET", "k") == ("$", b"db0")
        assert c1.execute_command("RENAME", "k", "k2") == OK  # only in db1
        assert c0.execute_command("GET", "k") == ("$", b"db0")
        assert c1.execute_command("FLUSHDB") == OK
        assert c1.execute_command("DBSIZE") == (":", b"0")
        assert c0.execute_command("DBSIZE") == (":", b"1"), "FLUSHDB must only clear the selected DB"
        for bad in ("16", "-1", "abc", "1.0"):
            assert err_prefix(c0.execute_command("SELECT", bad)) == b"ERR", bad
        assert c0.execute_command("GET", "k") == ("$", b"db0"), "failed SELECT must not change DB"
    finally:
        c1.close()


def test_flushdb(r):
    for i in range(500):
        r.set(f"f:{i}", i)
    r.set("ttl", "v", ex=1000)
    assert r.dbsize() == 501
    assert r.flushdb() is True
    assert r.dbsize() == 0 and r.keys("*") == [] and r.get("ttl") is None
    r.set("a", 1)
    assert r.execute_command("FLUSHDB", "ASYNC") is True
    assert r.dbsize() == 0
    r.set("a", 1)
    assert r.execute_command("FLUSHDB", "SYNC") is True
    assert r.dbsize() == 0
    with pytest.raises(redis.ResponseError):
        r.execute_command("FLUSHDB", "BOGUS")


# --------------------------------------------------------------------------
# RESP3 / HELLO
# --------------------------------------------------------------------------
def _map(reply) -> dict:
    assert reply[0] == "%", f"expected RESP3 map, got {reply!r}"
    return {k[1]: v for k, v in reply[1]}


def test_hello3_map_and_resp3_types(raw):
    hello = raw.call("HELLO", "3")
    m = _map(hello)
    for field in (b"server", b"version", b"proto", b"id", b"mode", b"role", b"modules"):
        assert field in m, f"HELLO 3 map lacks {field!r}: {hello!r}"
    assert m[b"proto"] == (":", b"3")
    assert m[b"id"][0] == ":"
    assert m[b"server"][0] in ("$", "+") and m[b"version"][0] in ("$", "+")
    assert m[b"modules"][0] == "*"
    # RESP3 null, and nulls nested in arrays
    assert raw.call("GET", "nokey") == NIL3
    raw.call("SET", "a", "1")
    assert raw.call("MGET", "a", "nokey") == ("*", (("$", b"1"), NIL3))
    assert raw.call("SET", "a", "2", "NX") == NIL3
    assert raw.call("SET", "a", "3", "GET") == ("$", b"1")
    assert raw.call("PING") == ("+", b"PONG")
    assert raw.call("TTL", "a") == (":", b"-1")
    # HELLO with no version reports the current protocol
    assert _map(raw.call("HELLO"))[b"proto"] == (":", b"3")
    # back to RESP2: flat array and $-1 nulls
    h2 = raw.call("HELLO", "2")
    assert h2[0] == "*" and len(h2[1]) % 2 == 0
    fields = dict(zip((x[1] for x in h2[1][0::2]), h2[1][1::2]))
    assert fields[b"proto"] == (":", b"2")
    assert raw.call("GET", "nokey") == NIL2
    assert raw.call("MGET", "nokey") == ("*", (NIL2,))
    # unsupported version
    assert err_prefix(raw.call("HELLO", "4")) == b"NOPROTO"
    assert err_prefix(raw.call("HELLO", "abc")) == b"ERR"
    assert raw.call("PING") == ("+", b"PONG")


def test_resp3_no_unsolicited_attributes_or_pushes(raw):
    """Plain redis-py cannot parse attribute ('|') frames, so the target must
    not send any (or pushes) for ordinary commands."""
    raw.call("HELLO", "3")
    cmds = [("SET", "k", "v"), ("GET", "k"), ("MGET", "k", "x"), ("KEYS", "*"), ("SCAN", "0"),
            ("TTL", "k"), ("TYPE", "k"), ("DBSIZE",), ("INCR", "n"), ("INCRBYFLOAT", "f", "1.5"),
            ("EXISTS", "k"), ("DEL", "k"), ("GET", "k"), ("PING",), ("ECHO", "x"), ("CLIENT", "GETNAME")]

    def walk(rep):
        assert rep[0] not in ("|", ">"), f"unsolicited {rep[0]!r} frame: {rep!r}"
        if rep[0] in "*~" and rep[1]:
            for x in rep[1]:
                walk(x)
        if rep[0] == "%":
            for a, b in rep[1]:
                walk(a)
                walk(b)
    for c in cmds:
        walk(raw.call(*c))


def test_redis_py_resp3_client_end_to_end():
    """Stock redis-py in RESP3 mode (HELLO 3 handshake, default CLIENT SETINFO
    whose error, if any, redis-py ignores)."""
    c = redis.Redis(host=HOST, port=PORT, protocol=3, decode_responses=True)
    try:
        assert c.ping() is True
        assert c.set("k", "v") is True
        assert c.get("k") == "v"
        assert c.get("nokey") is None
        assert c.mget("k", "nokey") == ["v", None]
        assert c.incrbyfloat("f", 2.5) == 2.5
        assert c.ttl("k") == -1
        assert sorted(c.scan_iter()) == ["f", "k"]
    finally:
        c.close()


@impl_specific
def test_resp3_attribute_reply_via_debug(raw):
    """Real attribute frames only come from DEBUG PROTOCOL attrib (Redis
    needs enable-debug-command); skipped when unavailable."""
    raw.call("HELLO", "3")
    rep = raw.call("DEBUG", "PROTOCOL", "attrib")
    if rep[0] == "-":
        pytest.skip(f"DEBUG PROTOCOL not available: {rep[1][:80]!r}")
    assert rep[0] == "|"
    attrs, inner = rep[1]
    assert attrs and inner[0] in ("$", "+")
    assert raw.call("PING") == ("+", b"PONG")


# --------------------------------------------------------------------------
# connection-level commands
# --------------------------------------------------------------------------
def test_client_setname_getname(typed):
    s = typed.execute_command
    assert s("CLIENT", "GETNAME") == NIL2
    assert s("CLIENT", "SETNAME", "worker-1") == OK
    assert s("CLIENT", "GETNAME") == ("$", b"worker-1")
    assert err_prefix(s("CLIENT", "SETNAME", "has space")) == b"ERR"
    assert s("CLIENT", "GETNAME") == ("$", b"worker-1")
    other = make_typed_client(HOST, PORT)
    try:
        assert other.execute_command("CLIENT", "GETNAME") == NIL2, "names are per connection"
    finally:
        other.close()
    assert s("CLIENT", "SETNAME", "") == OK
    assert s("CLIENT", "GETNAME") == NIL2
    hello = s("HELLO", "3", "SETNAME", "via-hello")
    assert hello[0] == "%"
    assert s("CLIENT", "GETNAME") == ("$", b"via-hello")


def test_redis_py_client_name_handshake():
    c = redis.Redis(host=HOST, port=PORT, client_name="named-conn", lib_name=None, lib_version=None)
    try:
        assert c.execute_command("CLIENT", "GETNAME") in (b"named-conn", "named-conn")
    finally:
        c.close()


def test_command_count(typed):
    rep = typed.execute_command("COMMAND", "COUNT")
    assert rep[0] == ":", f"COMMAND COUNT must return an integer, got {rep!r}"
    assert int(rep[1]) > 0


@impl_specific
def test_command_metadata(typed):
    """Full COMMAND / COMMAND DOCS (used by redis-cli hints and some cluster
    clients); Redis returns a non-empty array / map."""
    rep = typed.execute_command("COMMAND")
    assert rep[0] == "*" and len(rep[1]) > 0
    rep = typed.execute_command("COMMAND", "DOCS", "GET")
    assert rep[0] in ("*", "%")


def test_info(typed, typed3):
    rep = typed.execute_command("INFO")
    assert rep[0] == "$", f"INFO must be a bulk string in RESP2, got {rep[0]!r}"
    assert b"# Server" in rep[1]
    assert b"\r\n" in rep[1]
    rep = typed.execute_command("INFO", "server")
    assert rep[0] == "$" and b"# Server" in rep[1]
    rep3 = typed3.execute_command("INFO")
    # Redis 7 sends a verbatim string (=txt:...) in RESP3; plain bulk is also fine
    assert rep3[0] in ("$", "="), rep3[0]
    assert b"# Server" in rep3[1]


def test_info_parses_with_redis_py(r):
    info = r.info()
    assert isinstance(info, dict) and info


@impl_specific
def test_info_has_redis_fields(r):
    """Some clients/tools read redis_version / connected_clients from INFO."""
    info = r.info()
    assert "redis_version" in info


# --------------------------------------------------------------------------
# concurrency
# --------------------------------------------------------------------------
def test_concurrent_clients_incr_and_set():
    threads, per = 16, 500
    errors = []

    def work(tid):
        try:
            c = _client()
            for i in range(per):
                c.incr("shared")
                c.set(f"t:{tid}:{i}", f"{tid}-{i}")
            for i in range(0, per, 50):
                assert c.get(f"t:{tid}:{i}") == f"{tid}-{i}".encode()
            c.close()
        except Exception as e:  # pragma: no cover
            errors.append(e)

    ts = [threading.Thread(target=work, args=(t,)) for t in range(threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert not errors, errors
    c = _client()
    assert c.get("shared") == str(threads * per).encode()
    assert c.dbsize() == threads * per + 1
    c.close()


def test_many_connections():
    conns = [RawConn(HOST, PORT) for _ in range(200)]
    try:
        for c in conns:
            c.send("PING")
        for c in conns:
            assert c.read() == ("+", b"PONG")
    finally:
        for c in conns:
            c.close()


def test_client_disconnect_mid_command_does_not_break_server():
    s = socket.create_connection((HOST, PORT))
    s.sendall(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$100\r\npartial")
    s.close()
    with RawConn(HOST, PORT) as c:
        assert c.call("PING") == ("+", b"PONG")
        assert c.call("EXISTS", "k") == (":", b"0")


# --------------------------------------------------------------------------
# protocol corner cases (optional parts of the RESP spec)
# --------------------------------------------------------------------------
@impl_specific
def test_inline_commands(raw):
    raw.send_raw(b"PING\r\n")
    assert raw.read() == ("+", b"PONG")
    raw.send_raw(b"SET inline value\r\nGET inline\r\n")
    assert raw.read() == OK
    assert raw.read() == ("$", b"value")


@impl_specific
def test_protocol_error_reply(raw):
    raw.send_raw(b"*abc\r\n")
    rep = raw.read()
    assert rep[0] == "-" and b"Protocol error" in rep[1]
