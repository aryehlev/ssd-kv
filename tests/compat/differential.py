#!/usr/bin/env python3
"""Differential (reference vs. target) RESP compatibility fuzzer.

Starts a reference ``redis-server`` (7.0.15), connects to the server under
test, replays the same seeded random command stream against both over several
RESP2 and RESP3 connections and compares every reply.  On the first mismatch
it prints the offending command, both replies, and a shrunk reproducer
(the command-sequence prefix, minimised with delta debugging), saves the
reproducer as JSON (replayable with ``--replay``) and exits with status 1.

Replies are read with redis-py ``execute_command`` (``decode_responses=False``,
response callbacks cleared) through ``compat_common.TypedRESPParser``, which
keeps the RESP wire type, so ``+OK`` vs ``$2\\r\\nOK``, ``$-1`` vs ``_`` and the
exact integer/double text are all compared.

Normalisation rules (everything not listed here must match byte-for-byte and
type-for-type):

* TTL / PTTL / EXPIRETIME / PEXPIRETIME: both integers; if either is negative
  they must be equal (-1 no TTL, -2 missing); otherwise both >= 0 and within
  2 s (TTL, EXPIRETIME) or 2000 ms (PTTL, PEXPIRETIME).
* KEYS: compared as sorted lists (order-insensitive, duplicates still count).
* SCAN: the harness runs a *full* iteration (from cursor 0 until the server
  returns 0) on each server and compares the union of returned keys as a set;
  cursors, batch sizes and order are ignored.  Every SCAN step reply must
  still have the shape [bulk cursor, array of bulk keys].
* Errors: compared by prefix (first word: ERR, WRONGTYPE, NOPROTO, ...).
  ``--strict-errors`` compares the full message.
* HELLO: the values of ``id``, ``server`` and ``version`` are ignored (their
  RESP types are still compared); map pairs are compared order-insensitively.
* RESP3 maps and sets are compared order-insensitively everywhere.
* ``--loose-types`` additionally treats simple and bulk strings as equal, all
  null encodings ($-1, *-1, _) as equal, verbatim strings as bulk, big
  numbers as integers and drops attributes.  Default is strict.
* INCRBYFLOAT (and everything else) is compared exactly: Redis' long-double
  "%.17Lf + trim trailing zeros" formatting is part of the contract.

Timing: relative expiries in the random stream are >= --min-ttl (default
60 s) and absolute ones are at least that far in the future, so nothing
expires mid-run.  Past absolute times are only generated for commands whose
Redis semantics delete the key immediately (EXPIREAT/PEXPIREAT/GETEX
EXAT|PXAT, EXPIRE <= 0).  Real expiry is exercised by rare *expiry blocks*:
set a 300 ms TTL, sleep 650 ms, then touch every short-lived key (EXISTS/GET)
so lazy-expiry and active-expiry implementations converge before anything
else (DBSIZE, KEYS, ...) looks at the keyspace.
"""

from __future__ import annotations

import argparse
import base64
import bisect
import collections
import json
import os
import random
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

from redis.exceptions import RedisError

from compat_common import (
    ServerProcess,
    fill_template,
    fmt_reply,
    free_port,
    is_error,
    make_typed_client,
    parse_hostport,
    quote_arg,
    start_reference_redis,
)

I64_MAX = 2**63 - 1
I64_MIN = -(2**63)
NUM_DBS = 16
# connection layout: index -> protocol the prelude switches it to
CONN_PROTOCOLS = (2, 3, 2, 3)
CONN_WEIGHTS = (4, 4, 1, 1)


class HarnessError(Exception):
    """Something is wrong with the setup, not with the target's replies."""


# --------------------------------------------------------------------------
# Key space and value pools
# --------------------------------------------------------------------------
def _b(s) -> bytes:
    return s if isinstance(s, bytes) else s.encode("utf-8")


KEYSPACE: list[bytes] = [_b(k) for k in [
    # hot keys (picked ~50% of the time)
    "a", "b", "c", "counter", "{user1}:a", "{user1}:b", b"\x00", b"",
    "ключ", "h[ae]llo", "float", "big",
    # plain
    "foo", "bar", "key:1", "key:2", "key:10", "user:1000", "x", "y",
    # hashtags
    "{user1}:c", "{user2}:a", "{}x", "{a}{b}",
    # glob-special characters inside names
    "a*b", "a?c", "hello", "hallo", "hxllo", "back\\slash", "^caret", "brack]et",
    # binary / whitespace
    b"\x00\x01\x02", b"\xff\xfe", b"bin\x00key", b"\r\n", b"line\nbreak", b"tab\tkey", b" ",
    b"  lead",
    # unicode
    "键", "🔑", "café", "é",
    # numeric-looking
    "0", "-1", "trail ",
    # long names
    "L" * 300, "long:" + "x" * 1000, "z",
]]
assert len(KEYSPACE) == 50, len(KEYSPACE)
HOT_KEYS = KEYSPACE[:12]

WORDS = [b"hello", b"world", b"redis", b"value", b"foo", b"bar", b"OK", b"nil", b"PONG",
         b"string", b"none", b"0", b"-", b"*", b"?", b"[a]"]
INT_EDGE = [str(x).encode() for x in (
    I64_MAX, I64_MIN, I64_MAX - 1, I64_MIN + 1, I64_MAX + 1, I64_MIN - 1, 2**64 - 1,
    2**31 - 1, -(2**31), 2**32, 2**53 + 1, 0, 1, -1, 10, 100)] + [
    b"-0", b"+1", b"007", b"00", b" 1", b"1 ", b"1e3", b"0x10", b"1.0", b"12345678901234567890"]
FLOAT_POOL = [
    b"3.14", b"-2.5", b"0.1", b"0.2", b"0.3", b"1e10", b"1.5e-5", b".5", b"5.", b"-0.0", b"0",
    b"1e308", b"1.7976931348623157e308", b"2.2250738585072014e-308", b"4.9e-324", b"inf",
    b"-inf", b"+inf", b"Infinity", b"nan", b"1e400", b"0x1p-3", b"10.50", b"1_0", b"",
    b" 1.0", b"1.0 ", b"3.0000000000000000001", b"123456789012345678901234567890", b"-1e-10",
    b"0.30000000000000004", b"5e-1", b"1E2", b"9007199254740993", b"-9223372036854775808",
]
SPECIAL_VALUES = [b"\r\n", b"\x00", b"$-1\r\n", b"*1\r\n$4\r\nPING\r\n", b"-ERR fake\r\n",
                  b"+OK\r\n", b"\x00" * 17, b"\xff" * 8, b" ", b"\t\n"]
UNICODE_VALUES = ["значение", "值", "🙂🙃", "naïve café", "​", "a\u0000b"]

GLOB_SIMPLE = [
    b"*", b"*", b"a*", b"*a*", b"key:*", b"key:?", b"key:??", b"h?llo", b"h[ae]llo", b"h[^e]llo",
    b"h[a-e]llo", b"h[e-a]llo", b"{user1}:*", b"*}:*", b"a\\*b", b"a\\?c", b"*\\\\*", b"h\\[ae]llo",
    b"?", b"??", b"???*", b"", b"nomatch*", b"[abc]", b"[abc]*", b"*[0-9]", b"\x00*", b"*\x00*",
    "键*".encode(), "*é*".encode(), b"L*", b"*:*", b"[\\^]caret", b"brack]et", b"b*",
]
GLOB_EDGE = [b"h[ae", b"a\\", b"[", b"[]", b"[^]", b"h[a-]llo", b"h[-a]llo", b"\\", b"*[", b"[a-"]


# --------------------------------------------------------------------------
# Command model
# --------------------------------------------------------------------------
@dataclass(frozen=True)
class AbsTime:
    """An absolute unix time relative to *execution* time (resolved once per
    command and sent identically to both servers)."""
    offset_ms: int
    unit: str  # 's' or 'ms'

    def resolve(self, now_ms: int) -> bytes:
        t = now_ms + self.offset_ms
        return str(t // 1000 if self.unit == "s" else t).encode()


@dataclass
class Cmd:
    conn: int
    args: list  # bytes | AbsTime
    kind: str = "cmd"  # 'cmd' | 'scan' (full iteration) | 'sleep'
    sleep_ms: int = 0
    resolved: Optional[list] = None

    @property
    def name(self) -> str:
        if self.kind == "sleep":
            return "__SLEEP__"
        if self.kind == "scan":
            return "SCAN"
        return self.args[0].decode("utf-8", "replace").upper()

    def resolve(self) -> list:
        now_ms = int(time.time() * 1000)
        self.resolved = [a.resolve(now_ms) if isinstance(a, AbsTime) else a for a in self.args]
        return self.resolved

    def display(self) -> str:
        if self.kind == "sleep":
            return f"[--] SLEEP {self.sleep_ms}ms   # harness-side pause, no command sent"
        args = self.resolved or [a.resolve(int(time.time() * 1000)) if isinstance(a, AbsTime) else a
                                 for a in self.args]
        text = f"[c{self.conn}] " + " ".join(quote_arg(a) for a in args)
        notes = []
        for a in self.args:
            if isinstance(a, AbsTime):
                notes.append(f"abs time = now{a.offset_ms:+d}ms ({a.unit})")
        if self.kind == "scan":
            notes.append("full SCAN iteration, keys compared as a set")
        return text + (("   # " + "; ".join(notes)) if notes else "")

    # -- JSON (for --replay) ------------------------------------------
    def to_json(self) -> dict:
        def enc(a):
            if isinstance(a, AbsTime):
                return {"abs_offset_ms": a.offset_ms, "unit": a.unit}
            if len(a) <= 4096:
                try:
                    return a.decode("utf-8")
                except UnicodeDecodeError:
                    pass
            return {"b64": base64.b64encode(a).decode()}
        d = {"conn": self.conn, "kind": self.kind}
        if self.kind == "sleep":
            d["sleep_ms"] = self.sleep_ms
        else:
            d["args"] = [enc(a) for a in self.args]
        d["display"] = self.display()
        return d

    @staticmethod
    def from_json(d: dict) -> "Cmd":
        def dec(a):
            if isinstance(a, str):
                return a.encode("utf-8")
            if "b64" in a:
                return base64.b64decode(a["b64"])
            return AbsTime(int(a["abs_offset_ms"]), a["unit"])
        if d["kind"] == "sleep":
            return Cmd(conn=-1, args=[], kind="sleep", sleep_ms=int(d["sleep_ms"]))
        return Cmd(conn=int(d["conn"]), args=[dec(a) for a in d["args"]], kind=d["kind"])


def prelude() -> list[Cmd]:
    """Switch the RESP3 connections with HELLO 3 (compared like any command)."""
    return [Cmd(i, [b"HELLO", b"3"]) for i, p in enumerate(CONN_PROTOCOLS) if p == 3]


# --------------------------------------------------------------------------
# Random command generator
# --------------------------------------------------------------------------
class Generator:
    def __init__(self, rng: random.Random, opts):
        self.rng = rng
        self.min_ttl = opts.min_ttl
        self.large_prob = opts.large_prob
        self.expiry_prob = opts.expiry_scenario_prob
        self.globs = GLOB_SIMPLE + (GLOB_EDGE if opts.edge_globs else [])
        builders = [
            (10, self.g_get), (12, self.g_set), (2, self.g_setnx), (2, self.g_setex),
            (2, self.g_psetex), (2, self.g_getset), (2, self.g_getdel), (3, self.g_getex),
            (3, self.g_mget), (3, self.g_mset), (2, self.g_msetnx), (4, self.g_append),
            (3, self.g_strlen), (3, self.g_getrange), (3, self.g_setrange), (3, self.g_incr),
            (2, self.g_decr), (3, self.g_incrby), (2, self.g_decrby), (4, self.g_incrbyfloat),
            (3, self.g_del), (2, self.g_unlink), (3, self.g_exists), (2, self.g_type),
            (3, self.g_expire), (2, self.g_pexpire), (2, self.g_expireat), (2, self.g_pexpireat),
            (3, self.g_ttl), (2, self.g_pttl), (2, self.g_expiretime), (2, self.g_pexpiretime),
            (2, self.g_persist), (2, self.g_rename), (2, self.g_renamenx), (2, self.g_keys),
            (2, self.g_scan), (1, self.g_ping), (1, self.g_echo), (2, self.g_select),
            (2, self.g_dbsize), (0.15, self.g_flushdb), (0.4, self.g_hello), (2.5, self.g_arity),
            (0.4, self.g_unknown),
        ]
        self.funcs = [f for _, f in builders]
        self.cum = []
        acc = 0.0
        for w, _ in builders:
            acc += w
            self.cum.append(acc)
        self.conn_cum = []
        acc = 0
        for w in CONN_WEIGHTS:
            acc += w
            self.conn_cum.append(acc)

    # -- primitives ---------------------------------------------------
    def step(self) -> list[Cmd]:
        if self.rng.random() < self.expiry_prob:
            return self.expiry_block()
        i = bisect.bisect_right(self.cum, self.rng.random() * self.cum[-1])
        return self.funcs[min(i, len(self.funcs) - 1)]()

    def conn(self) -> int:
        return bisect.bisect_right(self.conn_cum, self.rng.randrange(self.conn_cum[-1]))

    def kw(self, s: str) -> bytes:
        r = self.rng.random()
        if r < 0.9:
            return s.upper().encode()
        if r < 0.97:
            return s.lower().encode()
        return "".join(ch.upper() if self.rng.random() < 0.5 else ch.lower() for ch in s).encode()

    def c(self, *args, conn: Optional[int] = None, kind: str = "cmd") -> Cmd:
        out = []
        for a in args:
            if isinstance(a, (bytes, AbsTime)):
                out.append(a)
            else:
                out.append(str(a).encode())
        return Cmd(self.conn() if conn is None else conn, out, kind=kind)

    def key(self) -> bytes:
        if self.rng.random() < 0.5:
            return self.rng.choice(HOT_KEYS)
        return self.rng.choice(KEYSPACE)

    def keys(self, lo: int = 1, hi: int = 5) -> list[bytes]:
        return [self.key() for _ in range(self.rng.randint(lo, hi))]

    def value(self) -> bytes:
        r = self.rng.random()
        rng = self.rng
        if r < self.large_prob:
            return rng.randbytes(rng.randint(256 * 1024, 1024 * 1024))
        r = rng.random()
        if r < 0.08:
            return b""
        if r < 0.28:
            w = rng.choice(WORDS)
            return w + (str(rng.randint(0, 99)).encode() if rng.random() < 0.3 else b"")
        if r < 0.43:
            return str(rng.randint(-1000, 1000)).encode()
        if r < 0.51:
            return rng.choice(INT_EDGE)
        if r < 0.59:
            return rng.choice(FLOAT_POOL)
        if r < 0.71:
            return rng.randbytes(rng.randint(1, 64))
        if r < 0.76:
            return rng.choice(UNICODE_VALUES).encode()
        if r < 0.80:
            return rng.choice(SPECIAL_VALUES)
        if r < 0.97:
            return rng.randbytes(rng.randint(65, 4096))
        return rng.randbytes(rng.randint(4097, 65536))

    def int_arg(self, small: tuple[int, int] = (-100, 100)) -> bytes:
        r = self.rng.random()
        if r < 0.75:
            return str(self.rng.randint(*small)).encode()
        if r < 0.9:
            return self.rng.choice(INT_EDGE)
        return self.rng.choice([b"abc", b"1.5", b"", b" 1", b"+1", b"01", b"-", b"1e2",
                                b"9223372036854775808", b"-9223372036854775809"])

    def bad_number(self) -> bytes:
        return self.rng.choice([b"abc", b"1.5", b"", b"+100", b"0100", b" 100", b"100 ",
                                b"9223372036854775807", b"9223372036854775",
                                b"9223372036854775808", b"-9223372036854775808"])

    def dur_s(self) -> bytes:
        r = self.rng.random()
        if r < 0.70:
            return str(self.rng.randint(self.min_ttl, 10**6)).encode()
        if r < 0.85:
            return str(self.rng.randint(self.min_ttl, max(self.min_ttl, 3600))).encode()
        if r < 0.89:
            return b"0"
        if r < 0.93:
            return str(-self.rng.randint(1, 1000)).encode()
        return self.bad_number()

    def dur_ms(self) -> bytes:
        r = self.rng.random()
        if r < 0.70:
            return str(self.rng.randint(self.min_ttl * 1000, 10**9)).encode()
        if r < 0.85:
            return str(self.rng.randint(self.min_ttl * 1000, max(self.min_ttl, 3600) * 1000)).encode()
        if r < 0.89:
            return b"0"
        if r < 0.93:
            return str(-self.rng.randint(1, 10**6)).encode()
        return self.bad_number()

    def abs_t(self, unit: str, allow_past: bool):
        r = self.rng.random()
        if r < 0.80:
            return AbsTime(self.rng.randint(self.min_ttl * 1000, 10**9), unit)
        if allow_past and r < 0.90:
            return AbsTime(-self.rng.randint(1000, 10**9), unit)
        if allow_past and r < 0.93:
            return self.rng.choice([b"1", b"0", b"-1"])
        if not allow_past:
            return self.rng.choice([b"0", b"-1", b"abc", b"9223372036854775807", b"1.5"])
        return self.bad_number()

    def pattern(self) -> bytes:
        return self.rng.choice(self.globs)

    def weighted_count(self, weights: list[int]) -> int:
        return self.rng.choices(range(len(weights)), weights=weights)[0]

    # -- builders -----------------------------------------------------
    def g_get(self):
        return [self.c(self.kw("GET"), self.key())]

    def g_set(self):
        args = [self.kw("SET"), self.key(), self.value()]
        for _ in range(self.weighted_count([40, 40, 15, 5])):
            o = self.rng.choice(["NX", "XX", "GET", "KEEPTTL", "EX", "PX", "EXAT", "PXAT", "EX", "PX", "GET"])
            args.append(self.kw(o))
            if o == "EX":
                args.append(self.dur_s())
            elif o == "PX":
                args.append(self.dur_ms())
            elif o == "EXAT":
                args.append(self.abs_t("s", allow_past=False))
            elif o == "PXAT":
                args.append(self.abs_t("ms", allow_past=False))
        r = self.rng.random()
        if r < 0.02:  # option missing its value -> syntax error
            args.append(self.kw(self.rng.choice(["EX", "PX", "EXAT", "PXAT"])))
        elif r < 0.03:
            args.append(b"BOGUS")
        return [self.c(*args)]

    def g_setnx(self):
        return [self.c(self.kw("SETNX"), self.key(), self.value())]

    def g_setex(self):
        return [self.c(self.kw("SETEX"), self.key(), self.dur_s(), self.value())]

    def g_psetex(self):
        return [self.c(self.kw("PSETEX"), self.key(), self.dur_ms(), self.value())]

    def g_getset(self):
        return [self.c(self.kw("GETSET"), self.key(), self.value())]

    def g_getdel(self):
        return [self.c(self.kw("GETDEL"), self.key())]

    def g_getex(self):
        args = [self.kw("GETEX"), self.key()]
        for _ in range(self.weighted_count([30, 55, 15])):
            o = self.rng.choice(["EX", "PX", "EXAT", "PXAT", "PERSIST"])
            args.append(self.kw(o))
            if o == "EX":
                args.append(self.dur_s())
            elif o == "PX":
                args.append(self.dur_ms())
            elif o == "EXAT":
                args.append(self.abs_t("s", allow_past=True))
            elif o == "PXAT":
                args.append(self.abs_t("ms", allow_past=True))
        if self.rng.random() < 0.02:
            args.append(self.kw(self.rng.choice(["EX", "PX", "FOO"])))
        return [self.c(*args)]

    def g_mget(self):
        return [self.c(self.kw("MGET"), *self.keys(1, 6))]

    def _pairs(self):
        out = []
        for _ in range(self.rng.randint(1, 4)):
            out += [self.key(), self.value()]
        return out

    def g_mset(self):
        return [self.c(self.kw("MSET"), *self._pairs())]

    def g_msetnx(self):
        return [self.c(self.kw("MSETNX"), *self._pairs())]

    def g_append(self):
        return [self.c(self.kw("APPEND"), self.key(), self.value())]

    def g_strlen(self):
        return [self.c(self.kw("STRLEN"), self.key())]

    def _index(self) -> bytes:
        r = self.rng.random()
        if r < 0.6:
            return str(self.rng.randint(-20, 20)).encode()
        if r < 0.75:
            return self.rng.choice([b"0", b"-1", b"-2"])
        if r < 0.85:
            return str(self.rng.randint(-(2**20), 2**20)).encode()
        if r < 0.95:
            return self.rng.choice([str(I64_MAX).encode(), str(I64_MIN).encode(), b"2147483648",
                                    b"-2147483649", b"4294967296"])
        return self.rng.choice([b"a", b"1.5", b"", b"9223372036854775808"])

    def g_getrange(self):
        return [self.c(self.kw("GETRANGE"), self.key(), self._index(), self._index())]

    def g_setrange(self):
        r = self.rng.random()
        val = self.value() if self.rng.random() < 0.9 else b""
        if r < 0.7:
            off = str(self.rng.randint(0, 100)).encode()
        elif r < 0.8:
            off = str(self.rng.randint(0, 2**20)).encode()
        elif r < 0.85:
            off = str(-self.rng.randint(1, 10)).encode()
        elif r < 0.9:  # > 512 MiB limit -> error, never allocates
            off, val = b"536870912", (val or b"x")
        elif r < 0.93:  # at the limit with an empty value -> no allocation
            off, val = self.rng.choice([b"536870911", b"536870912"]), b""
        else:
            off = self.bad_number()
        return [self.c(self.kw("SETRANGE"), self.key(), off, val)]

    def g_incr(self):
        return [self.c(self.kw("INCR"), self.key())]

    def g_decr(self):
        return [self.c(self.kw("DECR"), self.key())]

    def g_incrby(self):
        return [self.c(self.kw("INCRBY"), self.key(), self.int_arg())]

    def g_decrby(self):
        return [self.c(self.kw("DECRBY"), self.key(), self.int_arg())]

    def g_incrbyfloat(self):
        r = self.rng.random()
        if r < 0.45:
            inc = self.rng.choice(FLOAT_POOL)
        elif r < 0.65:
            inc = str(self.rng.randint(-1000, 1000)).encode()
        elif r < 0.85:
            inc = f"{self.rng.uniform(-1000, 1000):.{self.rng.randint(0, 8)}f}".encode()
        else:
            inc = f"{self.rng.uniform(-10, 10):.3e}".encode()
        key = self.rng.choice([b"float", b"counter", self.key()])
        return [self.c(self.kw("INCRBYFLOAT"), key, inc)]

    def g_del(self):
        return [self.c(self.kw("DEL"), *self.keys())]

    def g_unlink(self):
        return [self.c(self.kw("UNLINK"), *self.keys())]

    def g_exists(self):
        return [self.c(self.kw("EXISTS"), *self.keys())]

    def g_type(self):
        return [self.c(self.kw("TYPE"), self.key())]

    def _expire_flags(self) -> list[bytes]:
        n = self.weighted_count([50, 40, 10])
        flags = [self.kw(self.rng.choice(["NX", "XX", "GT", "LT"])) for _ in range(n)]
        if self.rng.random() < 0.01:
            flags.append(b"FOO")
        return flags

    def g_expire(self):
        return [self.c(self.kw("EXPIRE"), self.key(), self.dur_s(), *self._expire_flags())]

    def g_pexpire(self):
        return [self.c(self.kw("PEXPIRE"), self.key(), self.dur_ms(), *self._expire_flags())]

    def g_expireat(self):
        return [self.c(self.kw("EXPIREAT"), self.key(), self.abs_t("s", True), *self._expire_flags())]

    def g_pexpireat(self):
        return [self.c(self.kw("PEXPIREAT"), self.key(), self.abs_t("ms", True), *self._expire_flags())]

    def g_ttl(self):
        return [self.c(self.kw("TTL"), self.key())]

    def g_pttl(self):
        return [self.c(self.kw("PTTL"), self.key())]

    def g_expiretime(self):
        return [self.c(self.kw("EXPIRETIME"), self.key())]

    def g_pexpiretime(self):
        return [self.c(self.kw("PEXPIRETIME"), self.key())]

    def g_persist(self):
        return [self.c(self.kw("PERSIST"), self.key())]

    def g_rename(self):
        src = self.key()
        dst = src if self.rng.random() < 0.1 else self.key()
        return [self.c(self.kw("RENAME"), src, dst)]

    def g_renamenx(self):
        src = self.key()
        dst = src if self.rng.random() < 0.1 else self.key()
        return [self.c(self.kw("RENAMENX"), src, dst)]

    def g_keys(self):
        return [self.c(self.kw("KEYS"), self.pattern())]

    def g_scan(self):
        r = self.rng.random()
        if r < 0.06:
            cursor = self.rng.choice([b"abc", b"1.5", b" 0", b"18446744073709551616", b""])
        else:
            cursor = b"0"
        args = [self.kw("SCAN"), cursor]
        opts = ["MATCH", "COUNT", "TYPE"]
        self.rng.shuffle(opts)
        for o in opts[: self.weighted_count([25, 35, 25, 15])]:
            args.append(self.kw(o))
            if o == "MATCH":
                args.append(self.pattern())
            elif o == "COUNT":
                args.append(self.rng.choice([b"1", b"2", b"10", b"100", b"1000", b"5"]) if self.rng.random() < 0.9
                            else self.rng.choice([b"0", b"-1", b"abc"]))
            else:
                args.append(self.rng.choice([b"string", b"STRING", b"String", b"list", b"hash", b"none",
                                             b"zset", b"nosuchtype"]))
        if self.rng.random() < 0.02:
            args.append(self.kw(self.rng.choice(["MATCH", "COUNT", "TYPE", "BOGUS"])))
        return [self.c(*args, kind="scan")]

    def g_ping(self):
        if self.rng.random() < 0.5:
            return [self.c(self.kw("PING"))]
        return [self.c(self.kw("PING"), self.value())]

    def g_echo(self):
        return [self.c(self.kw("ECHO"), self.value())]

    def g_select(self):
        r = self.rng.random()
        if r < 0.40:
            db = b"0"
        elif r < 0.65:
            db = b"1"
        elif r < 0.75:
            db = b"2"
        elif r < 0.90:
            db = str(self.rng.randint(3, 15)).encode()
        else:
            db = self.rng.choice([b"16", b"-1", b"abc", b"1.0", b"", b"99999999999999999999"])
        return [self.c(self.kw("SELECT"), db)]

    def g_dbsize(self):
        return [self.c(self.kw("DBSIZE"))]

    def g_flushdb(self):
        r = self.rng.random()
        extra = [] if r < 0.6 else [self.kw(self.rng.choice(["ASYNC", "SYNC"]))] if r < 0.9 else [b"BOGUS"]
        return [self.c(self.kw("FLUSHDB"), *extra)]

    def g_hello(self):
        variant = self.rng.choice([[], [b"3"], [b"2"], [b"3"], [b"2"], [b"4"], [b"1"], [b"abc"]])
        return [self.c(self.kw("HELLO"), *variant)]

    ARITY_ERRORS = [
        ["GET"], ["GET", "a", "b"], ["SET", "a"], ["SET"], ["SETNX", "a"], ["SETEX", "a", "10"],
        ["PSETEX", "a", "100"], ["GETSET", "a"], ["GETDEL"], ["GETDEL", "a", "b"], ["GETEX"],
        ["MGET"], ["MSET"], ["MSET", "a"], ["MSET", "a", "1", "b"], ["MSETNX", "a"], ["MSETNX"],
        ["APPEND", "a"], ["STRLEN"], ["STRLEN", "a", "b"], ["GETRANGE", "a", "0"],
        ["GETRANGE", "a", "0", "1", "2"], ["SETRANGE", "a", "0"], ["INCR"], ["INCR", "a", "b"],
        ["DECR"], ["INCRBY", "a"], ["DECRBY", "a", "1", "2"], ["INCRBYFLOAT", "a"], ["DEL"],
        ["UNLINK"], ["EXISTS"], ["TYPE"], ["TYPE", "a", "b"], ["EXPIRE", "a"], ["PEXPIRE"],
        ["EXPIREAT", "a"], ["PEXPIREAT", "a"], ["TTL"], ["TTL", "a", "b"], ["PTTL"],
        ["EXPIRETIME"], ["PEXPIRETIME", "a", "b"], ["PERSIST"], ["PERSIST", "a", "b"],
        ["RENAME", "a"], ["RENAMENX", "a"], ["RENAME", "a", "b", "c"], ["KEYS"],
        ["KEYS", "a", "b"], ["SCAN"], ["PING", "a", "b"], ["ECHO"], ["ECHO", "a", "b"],
        ["SELECT"], ["SELECT", "0", "1"], ["DBSIZE", "x"], ["FLUSHDB", "ASYNC", "SYNC"],
    ]

    def g_arity(self):
        args = list(self.rng.choice(self.ARITY_ERRORS))
        args = [self.kw(args[0])] + [self.key() if a in ("a", "b", "c") else a.encode() for a in args[1:]]
        return [self.c(*args)]

    def g_unknown(self):
        name = self.rng.choice([b"NOSUCHCOMMAND", b"GETT", b"SETT", b"FLUSHDBB", b"", b"get\x00",
                                b"\xff\xfe", b"MGETX"])
        return [self.c(name, *self.keys(0, 3))]

    # -- dedicated expiry scenario -------------------------------------
    def expiry_block(self) -> list[Cmd]:
        """Short real TTLs, an explicit sleep, then every short-lived key is
        touched so lazy and active expiry converge."""
        conn = self.conn()
        k = self.key()
        k2 = self.key()
        v = self.rng.choice(WORDS)
        v2 = self.rng.choice(WORDS) + b"2"
        c = lambda *a: self.c(*a, conn=conn)  # noqa: E731
        ms = b"300"
        variant = self.rng.randrange(13)
        short = [k]
        cmds: list[Cmd] = []
        if variant == 0:
            cmds.append(c(b"SET", k, v, b"PX", ms))
        elif variant == 1:
            cmds.append(c(b"PSETEX", k, ms, v))
        elif variant == 2:
            cmds += [c(b"SET", k, v), c(b"PEXPIRE", k, ms)]
        elif variant == 3:
            cmds.append(c(b"SET", k, v, b"PXAT", AbsTime(300, "ms")))
        elif variant == 4:
            cmds += [c(b"SET", k, v), c(b"GETEX", k, b"PX", ms)]
        elif variant == 5:
            cmds += [c(b"SET", k, v, b"PX", ms), c(b"RENAME", k, k2)]
            short.append(k2)
        elif variant == 6:
            cmds += [c(b"SET", k, v, b"PX", ms), c(b"SET", k, v2, b"KEEPTTL")]
        elif variant == 7:  # plain SET clears the TTL -> key survives
            cmds += [c(b"SET", k, v, b"PX", ms), c(b"SET", k, v2)]
        elif variant == 8:  # PERSIST -> key survives
            cmds += [c(b"SET", k, v, b"PX", ms), c(b"PERSIST", k)]
        elif variant == 9:  # SET with an absolute time in the past
            unit = self.rng.choice(["s", "ms"])
            cmds += [c(b"SET", k, v, b"EXAT" if unit == "s" else b"PXAT", AbsTime(-5000, unit)),
                     c(b"EXISTS", k), c(b"GET", k), c(b"DBSIZE")]
            return cmds
        elif variant == 10:  # APPEND / SETRANGE keep the TTL
            cmds += [c(b"SET", k, v, b"PX", ms), c(b"APPEND", k, b"x"), c(b"SETRANGE", k, b"0", b"Z")]
        elif variant == 11:
            cmds += [c(b"MSET", k, v), c(b"PEXPIREAT", k, AbsTime(300, "ms"))]
        else:  # GT/LT against a short TTL
            cmds += [c(b"SET", k, v, b"PX", ms), c(b"PEXPIRE", k, b"200", b"LT"),
                     c(b"PEXPIRE", k, b"100000", b"LT")]
        cmds += [c(b"PTTL", k), c(b"EXISTS", k)]
        cmds.append(Cmd(conn=-1, args=[], kind="sleep", sleep_ms=650))
        cmds += [c(b"EXISTS", *short), c(b"GET", k), c(b"TTL", k), c(b"DBSIZE")]
        return cmds


# --------------------------------------------------------------------------
# Comparison
# --------------------------------------------------------------------------
TTL_TOLERANCE = {"TTL": 2, "PTTL": 2000, "EXPIRETIME": 2, "PEXPIRETIME": 2000}
HELLO_IGNORED = {b"id", b"server", b"version"}


def loose(r):
    t, v = r
    if t in ("EXC", "SCANSET", "SCANERR"):
        return r
    if v is None or t == "_":
        return ("_", None)
    if t == "+":
        return ("$", v)
    if t == "=":
        return ("$", v[4:])
    if t == "(":
        return (":", v)
    if t in "*~>":
        return ("*", tuple(loose(x) for x in v))
    if t == "%":
        return ("%", tuple((loose(a), loose(b)) for a, b in v))
    if t == "|":
        return loose(v[1])
    return r


def canon(r):
    """Order-insensitive view of maps and sets (recursively)."""
    t, v = r
    if v is None or not isinstance(v, tuple) or t in ("SCANSET",):
        return r
    if t == "%":
        return ("%", tuple(sorted(((canon(a), canon(b)) for a, b in v), key=repr)))
    if t == "~":
        return ("~", tuple(sorted((canon(x) for x in v), key=repr)))
    if t in "*>":
        return (t, tuple(canon(x) for x in v))
    if t == "|":
        attrs, inner = v
        return ("|", (tuple(sorted(((canon(a), canon(b)) for a, b in attrs), key=repr)), canon(inner)))
    return r


def normalize_hello(r):
    t, v = r
    if v is None:
        return r
    if t == "%":
        pairs = list(v)
    elif t == "*" and len(v) % 2 == 0:
        pairs = list(zip(v[0::2], v[1::2]))
    else:
        return r
    out = []
    for k, val in pairs:
        if isinstance(k[1], bytes) and k[1].lower() in HELLO_IGNORED:
            val = (val[0], b"<ignored>")
        out.append((k, val))
    return ("HELLO-" + t, tuple(sorted(out, key=repr)))


def _int_of(r) -> Optional[int]:
    if r[0] == ":" and isinstance(r[1], bytes):
        return int(r[1])
    return None


def compare(name: str, ref, tgt, opts) -> Optional[str]:
    """Return None when the replies are equivalent, else a reason."""
    if ref[0] == "EXC":
        return "reference raised an exception (harness/reference problem)"
    if tgt[0] == "EXC":
        return "target raised an exception (connection closed / timeout / protocol violation)"
    ref_err, tgt_err = is_error(ref), is_error(tgt)
    if ref_err or tgt_err:
        if ref_err != tgt_err:
            return "only one side returned an error"
        if ref[0] != tgt[0] and not opts.loose_types:
            return f"error type differs ({ref[0]!r} vs {tgt[0]!r})"
        if opts.strict_errors:
            return None if ref[1] == tgt[1] else "error message differs (--strict-errors)"
        rp = ref[1].split(b" ", 1)[0]
        tp = tgt[1].split(b" ", 1)[0]
        return None if rp == tp else f"error prefix differs ({rp!r} vs {tp!r})"
    if opts.loose_types:
        ref, tgt = loose(ref), loose(tgt)
    if name in TTL_TOLERANCE:
        a, b = _int_of(ref), _int_of(tgt)
        if a is None or b is None:
            return None if ref == tgt else "non-integer TTL-family reply differs"
        if a < 0 or b < 0:
            return None if a == b else "TTL state differs (-1 = no ttl, -2 = missing)"
        tol = TTL_TOLERANCE[name]
        return None if abs(a - b) <= tol else f"TTL differs by {abs(a - b)} (> tolerance {tol})"
    if name == "KEYS":
        if ref[0] == "*" and tgt[0] == "*" and ref[1] is not None and tgt[1] is not None:
            if sorted(ref[1], key=repr) == sorted(tgt[1], key=repr):
                return None
            return "KEYS result differs (compared as sorted lists)"
        return None if ref == tgt else "KEYS reply differs"
    if name == "SCAN":
        if ref[0] == "SCANSET" and tgt[0] == "SCANSET":
            return None if ref[1] == tgt[1] else "SCAN full-iteration key set differs"
        if tgt[0] == "SCANERR":
            return f"target SCAN iteration failed: {tgt[1]}"
        if ref[0] == "SCANERR":
            return f"reference SCAN iteration failed: {ref[1]}"
        return None if canon(ref) == canon(tgt) else "SCAN reply differs"
    if name == "HELLO":
        return None if normalize_hello(ref) == normalize_hello(tgt) else \
            "HELLO reply differs (id/server/version values ignored)"
    return None if canon(ref) == canon(tgt) else "replies differ"


def explain_diff(name: str, ref, tgt) -> str:
    def keyset(r):
        if r[0] == "SCANSET":
            return set(r[1])
        if r[0] == "*" and r[1] is not None:
            return {x[1] for x in r[1]}
        return None
    if name in ("KEYS", "SCAN"):
        a, b = keyset(ref), keyset(tgt)
        if a is not None and b is not None:
            miss = sorted(a - b)
            extra = sorted(b - a)
            out = []
            if miss:
                out.append("  keys missing on target: " + ", ".join(quote_arg(k, 40) for k in miss[:20]))
            if extra:
                out.append("  unexpected keys on target: " + ", ".join(quote_arg(k, 40) for k in extra[:20]))
            if out:
                return "\n".join(out)
    return ""


def fmt_any(r) -> str:
    if r[0] == "SCANSET":
        ks = sorted(r[1])
        body = ", ".join(quote_arg(k, 40) for k in ks[:30])
        return f"(scan key set, {len(ks)} keys) {{{body}{', ...' if len(ks) > 30 else ''}}}"
    if r[0] in ("SCANERR", "EXC"):
        return f"({r[0]}) {r[1].decode('utf-8', 'replace') if isinstance(r[1], bytes) else r[1]}"
    return fmt_reply(r)


# --------------------------------------------------------------------------
# Execution
# --------------------------------------------------------------------------
class Side:
    def __init__(self, label: str, host: str, port: int, nconns: int):
        self.label, self.host, self.port, self.nconns = label, host, port, nconns
        self.clients: list = []

    def connect(self):
        self.close()
        self.clients = [make_typed_client(self.host, self.port) for _ in range(self.nconns)]

    def close(self):
        for c in self.clients:
            try:
                c.close()
            except Exception:
                pass
        self.clients = []

    def flush_all(self):
        """SELECT i + FLUSHDB for every DB (FLUSHALL is not in the target's set)."""
        c = make_typed_client(self.host, self.port, timeout=60)
        try:
            for db in range(NUM_DBS):
                for cmd in ((b"SELECT", str(db).encode()), (b"FLUSHDB",)):
                    r = c.execute_command(*cmd)
                    if r != ("+", b"OK"):
                        raise HarnessError(f"{self.label}: cannot reset state, {cmd!r} replied {r!r}")
        finally:
            c.close()

    def run(self, cmd: Cmd, args: list):
        client = self.clients[cmd.conn]
        try:
            if cmd.kind == "scan":
                return self._scan(client, args)
            return client.execute_command(*args)
        except (RedisError, OSError) as e:
            return ("EXC", f"{type(e).__name__}: {e}".encode())

    @staticmethod
    def _scan(client, args):
        cursor = args[1]
        keys = set()
        for step in range(100_000):
            r = client.execute_command(args[0], cursor, *args[2:])
            if is_error(r):
                if step == 0:
                    return r
                return ("SCANERR", f"error mid-iteration: {r!r}")
            t, v = r
            if t != "*" or v is None or len(v) != 2 or v[0][0] != "$" or v[1][0] != "*" or v[1][1] is None:
                return ("SCANERR", f"malformed SCAN reply (want [bulk cursor, array]): {r!r:.300}")
            for item in v[1][1]:
                if item[0] != "$" or item[1] is None:
                    return ("SCANERR", f"SCAN element is not a bulk string: {item!r:.200}")
                keys.add(item[1])
            cursor = v[0][1]
            if cursor == b"0":
                return ("SCANSET", frozenset(keys))
        return ("SCANERR", "iteration did not terminate after 100000 calls")


class Runner:
    def __init__(self, ref: Side, tgt: Side, opts, ensure_target: Optional[Callable[[], None]] = None):
        self.ref, self.tgt, self.opts = ref, tgt, opts
        self.ensure_target = ensure_target
        self.stats = collections.Counter()
        self.ref_errors = collections.Counter()

    def reset(self):
        if self.ensure_target:
            self.ensure_target()
        for side in (self.ref, self.tgt):
            side.close()
            side.flush_all()
            side.connect()

    def exec_one(self, cmd: Cmd):
        if cmd.kind == "sleep":
            time.sleep(cmd.sleep_ms / 1000.0)
            return None, None, None
        args = cmd.resolve()
        r = self.ref.run(cmd, args)
        t = self.tgt.run(cmd, args)
        return r, t, compare(cmd.name, r, t, self.opts)

    def reproduces(self, cmds: list[Cmd]) -> bool:
        """True iff replaying ``cmds`` from a clean state mismatches exactly at
        the last command (and nowhere before)."""
        try:
            self.reset()
        except Exception:
            return False
        for i, cmd in enumerate(cmds):
            _, _, diff = self.exec_one(cmd)
            if diff:
                return i == len(cmds) - 1
        return False


def ddmin(prefix: list[Cmd], last: Cmd, test: Callable[[list[Cmd]], bool], deadline: float,
          log: Callable[[str], None]) -> list[Cmd]:
    """Delta debugging (complement-only variant) over ``prefix``."""
    items = list(prefix)
    n = 2
    while len(items) >= 1 and time.monotonic() < deadline:
        chunk = max(1, -(-len(items) // n))
        reduced = False
        for start in range(0, len(items), chunk):
            if time.monotonic() >= deadline:
                break
            cand = items[:start] + items[start + chunk:]
            if test(cand + [last]):
                items = cand
                n = max(n - 1, 2)
                reduced = True
                log(f"  shrink: {len(items) + 1} commands")
                break
        if not reduced:
            if chunk == 1:
                break
            n = min(len(items), n * 2)
    return items


def keys_of(cmd: Cmd) -> set[bytes]:
    """Best-effort set of key arguments (for the slicing heuristic)."""
    if cmd.kind != "cmd" or not cmd.args:
        return set()
    name = cmd.name
    a = [x for x in cmd.args[1:] if isinstance(x, bytes)]
    if name in ("MGET", "DEL", "UNLINK", "EXISTS"):
        return set(a)
    if name in ("MSET", "MSETNX"):
        return set(a[0::2])
    if name in ("RENAME", "RENAMENX"):
        return set(a[:2])
    if name in ("PING", "ECHO", "SELECT", "DBSIZE", "FLUSHDB", "HELLO", "KEYS"):
        return set()
    return set(a[:1])


KNOWN_COMMANDS = {
    "PING", "ECHO", "SELECT", "DBSIZE", "FLUSHDB", "GET", "SET", "SETNX", "SETEX", "PSETEX", "GETSET",
    "GETDEL", "GETEX", "MGET", "MSET", "MSETNX", "APPEND", "STRLEN", "GETRANGE", "SETRANGE", "INCR",
    "DECR", "INCRBY", "DECRBY", "INCRBYFLOAT", "DEL", "UNLINK", "EXISTS", "TYPE", "EXPIRE", "PEXPIRE",
    "EXPIREAT", "PEXPIREAT", "TTL", "PTTL", "EXPIRETIME", "PEXPIRETIME", "PERSIST", "RENAME",
    "RENAMENX", "KEYS", "SCAN", "HELLO",
}
GLOBAL_CMDS = {"SELECT", "FLUSHDB", "HELLO", "__SLEEP__"}


def shrink(runner: Runner, history: list[Cmd], budget: float, log) -> list[Cmd]:
    deadline = time.monotonic() + budget
    last = history[-1]
    prefix = history[:-1]
    if not runner.reproduces(history):
        log("  mismatch did NOT reproduce when replaying the full prefix from a clean state "
            "(timing-dependent?) -- reporting the unshrunk prefix")
        return history
    focus = keys_of(last)
    if focus and last.name not in ("DBSIZE", "KEYS", "SCAN"):
        sliced = [c for c in prefix if c.name in GLOBAL_CMDS or (keys_of(c) & focus)
                  or c.name in ("RENAME", "RENAMENX", "FLUSHDB")]
        if len(sliced) < len(prefix) and runner.reproduces(sliced + [last]):
            log(f"  key slice: {len(prefix) + 1} -> {len(sliced) + 1} commands")
            prefix = sliced
    prefix = ddmin(prefix, last, runner.reproduces, deadline, log)
    if time.monotonic() >= deadline:
        log("  shrink budget exhausted; reproducer may not be 1-minimal")
    return prefix + [last]


# --------------------------------------------------------------------------
# Environment (reference + target lifecycle)
# --------------------------------------------------------------------------
class Environment:
    def __init__(self, opts):
        self.opts = opts
        self.tmp = tempfile.mkdtemp(prefix="ssdkv-differential-")
        self.ref_proc: Optional[ServerProcess] = None
        self.tgt_proc: Optional[ServerProcess] = None
        self.ref_addr = None
        self.tgt_addr = None
        self.tgt_cmd = None

    def __enter__(self):
        o = self.opts
        if o.ref:
            self.ref_addr = parse_hostport(o.ref)
        else:
            refdir = os.path.join(self.tmp, "reference")
            os.makedirs(refdir)
            self.ref_proc, port = start_reference_redis(o.redis_server, refdir)
            self.ref_addr = ("127.0.0.1", port)
        if o.target:
            self.tgt_addr = parse_hostport(o.target)
        else:
            port = o.target_port or free_port()
            tdir = os.path.join(self.tmp, "target-data")
            os.makedirs(tdir)
            self.tgt_cmd = fill_template(o.target_cmd, port=port, dir=tdir)
            self.tgt_addr = (o.target_host, port)
            self.start_target()
        return self

    def start_target(self):
        log = os.path.join(self.tmp, "target.log")
        self.tgt_proc = ServerProcess(self.tgt_cmd, cwd=self.tmp, log_path=log, name="target")
        self.tgt_proc.wait_ready(*self.tgt_addr, timeout=self.opts.target_startup_timeout)

    def ensure_target(self):
        if self.tgt_proc is not None and not self.tgt_proc.alive():
            print(f"  target process died (exit {self.tgt_proc.proc.returncode}); restarting it",
                  file=sys.stderr)
            self.tgt_proc.kill()
            self.start_target()

    def __exit__(self, *exc):
        for p in (self.tgt_proc, self.ref_proc):
            if p is not None:
                p.kill()
        if not self.opts.keep_tmp:
            shutil.rmtree(self.tmp, ignore_errors=True)
        else:
            print(f"temp dir kept: {self.tmp}")


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def save_repro(path: str, cmds: list[Cmd], meta: dict):
    with open(path, "w") as f:
        json.dump({"format": "ssdkv-differential-v1", **meta,
                   "connections": [{"conn": i, "initial_protocol": 2} for i in range(len(CONN_PROTOCOLS))],
                   "commands": [c.to_json() for c in cmds]}, f, indent=1)


def report_mismatch(runner: Runner, env: Environment, history: list[Cmd], ref, tgt, reason: str,
                    meta: dict) -> None:
    opts = runner.opts
    cmd = history[-1]
    print("\n" + "=" * 78)
    print(f"MISMATCH at command #{len(history)} ({meta.get('where', '')})")
    print(f"  command:   {cmd.display()}")
    print(f"  reason:    {reason}")
    print("  reference:\n" + fmt_any(ref).replace("\n", "\n    ").join(["    ", ""]))
    print("  target:\n" + fmt_any(tgt).replace("\n", "\n    ").join(["    ", ""]))
    extra = explain_diff(cmd.name, ref, tgt)
    if extra:
        print(extra)
    out_dir = opts.out_dir or tempfile.mkdtemp(prefix="ssdkv-diff-failure-")
    os.makedirs(out_dir, exist_ok=True)
    tag = meta.get("tag", "replay")
    full_path = os.path.join(out_dir, f"repro-{tag}-full.json")
    save_repro(full_path, history, {**meta, "reason": reason})
    repro = history
    if not opts.no_shrink and len(history) > 1:
        print(f"\nShrinking {len(history)}-command prefix (budget {opts.shrink_budget:.0f}s) ...")
        repro = shrink(runner, history, opts.shrink_budget, lambda s: print(s, flush=True))
    min_path = os.path.join(out_dir, f"repro-{tag}-min.json")
    save_repro(min_path, repro, {**meta, "reason": reason})
    print(f"\nMinimal reproducer ({len(repro)} commands"
          f"{'' if repro is history else f', shrunk from {len(history)}'}); "
          f"start from empty DBs, connections c0..c{len(CONN_PROTOCOLS) - 1} all fresh RESP2, db 0:")
    for c in repro:
        print("  " + c.display())
    print(f"\nSaved full prefix:  {full_path}")
    print(f"Saved reproducer:   {min_path}")
    tgt_flag = f"--target {opts.target}" if opts.target else f"--target-cmd {opts.target_cmd!r}"
    print(f"Replay with:        {sys.argv[0]} {tgt_flag} --replay {min_path}")


def run_seed(runner: Runner, seed: int, iterations: int, verbose: bool, history_out: list):
    rng = random.Random(seed)
    gen = Generator(rng, runner.opts)
    runner.reset()
    history_out.clear()
    executed = 0
    queue = prelude()
    while executed < iterations or queue:
        if not queue:
            queue = gen.step()
        cmd = queue.pop(0)
        r, t, diff = runner.exec_one(cmd)
        history_out.append(cmd)
        if cmd.kind == "sleep":
            continue
        executed += 1
        stat = cmd.name if cmd.name in KNOWN_COMMANDS else "(unknown cmd)"
        runner.stats[stat] += 1
        if r is not None and is_error(r):
            runner.ref_errors[stat] += 1
        if verbose:
            print(f"  {executed:6d} {cmd.display()[:160]}  ->  {fmt_any(r).splitlines()[0][:80]}")
        if diff:
            return r, t, diff
    return None


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    g = p.add_argument_group("target (server under test)")
    g.add_argument("--target", help="host:port of an already running server under test")
    g.add_argument("--target-cmd", help="shell command that launches the server under test; "
                   "{port} and {dir} placeholders are substituted")
    g.add_argument("--target-port", type=int, help="port the --target-cmd server listens on "
                   "(default: a free port substituted into {port})")
    g.add_argument("--target-host", default="127.0.0.1")
    g.add_argument("--target-startup-timeout", type=float, default=30.0)
    g = p.add_argument_group("reference")
    g.add_argument("--redis-server", default="/usr/bin/redis-server")
    g.add_argument("--ref", help="host:port of an already running reference (skip launching one)")
    g = p.add_argument_group("workload")
    g.add_argument("--seed", type=int, default=1, help="first seed (default 1)")
    g.add_argument("--seeds", type=int, default=5, help="number of consecutive seeds (default 5)")
    g.add_argument("--iterations", type=int, default=2000, help="commands per seed (default 2000)")
    g.add_argument("--min-ttl", type=int, default=60,
                   help="minimum relative TTL in seconds for the random stream (default 60; must exceed "
                        "the per-seed run time)")
    g.add_argument("--large-prob", type=float, default=0.005,
                   help="probability that a generated value is 256 KiB..1 MiB (default 0.005)")
    g.add_argument("--expiry-scenario-prob", type=float, default=0.0015,
                   help="per-step probability of a real-expiry block with a 650 ms sleep (default 0.0015)")
    g.add_argument("--edge-globs", action="store_true",
                   help="also use malformed glob patterns (unterminated [, trailing backslash, ...)")
    g = p.add_argument_group("comparison")
    g.add_argument("--strict-errors", action="store_true", help="compare full error messages")
    g.add_argument("--loose-types", action="store_true",
                   help="treat simple==bulk strings and all null encodings as equal")
    g = p.add_argument_group("reporting")
    g.add_argument("--replay", help="replay a saved reproducer JSON instead of random seeds")
    g.add_argument("--no-shrink", action="store_true")
    g.add_argument("--shrink-budget", type=float, default=120.0, help="seconds (default 120)")
    g.add_argument("--out-dir", help="where reproducer JSON files go (default: a new temp dir)")
    g.add_argument("--keep-tmp", action="store_true", help="keep the reference/target temp dir")
    g.add_argument("-v", "--verbose", action="store_true", help="print every command")
    o = p.parse_args(argv)
    if bool(o.target) == bool(o.target_cmd):
        p.error("give exactly one of --target or --target-cmd")
    if o.target_cmd and "{port}" not in o.target_cmd and not o.target_port:
        p.error("--target-cmd without a {port} placeholder needs --target-port")
    return o


def main(argv=None) -> int:
    opts = parse_args(argv)
    try:
        with Environment(opts) as env:
            ref = Side("reference", *env.ref_addr, len(CONN_PROTOCOLS))
            tgt = Side("target", *env.tgt_addr, len(CONN_PROTOCOLS))
            runner = Runner(ref, tgt, opts, ensure_target=env.ensure_target if env.tgt_proc else None)
            probe = make_typed_client(*env.ref_addr)
            hello = probe.execute_command(b"HELLO")
            probe.close()
            ref_version = "?"
            if hello[0] == "*" and hello[1]:
                fields = dict(zip((x[1] for x in hello[1][0::2]), (x[1] for x in hello[1][1::2])))
                ref_version = (fields.get(b"version") or b"?").decode()
            print(f"reference: redis {ref_version} at {env.ref_addr[0]}:{env.ref_addr[1]}   "
                  f"target: {env.tgt_addr[0]}:{env.tgt_addr[1]}")
            if ref_version != "7.0.15":
                print(f"WARNING: reference is redis {ref_version}, the harness was calibrated "
                      f"against 7.0.15", file=sys.stderr)
            t0 = time.monotonic()
            if opts.replay:
                with open(opts.replay) as f:
                    data = json.load(f)
                cmds = [Cmd.from_json(d) for d in data["commands"]]
                runner.reset()
                for i, cmd in enumerate(cmds):
                    r, t, diff = runner.exec_one(cmd)
                    if diff:
                        report_mismatch(runner, env, cmds[: i + 1], r, t, diff,
                                        {"where": f"replay of {opts.replay}", "tag": "replay"})
                        return 1
                print(f"replay of {len(cmds)} commands: no mismatch")
                return 0
            history: list[Cmd] = []
            total = 0
            for seed in range(opts.seed, opts.seed + opts.seeds):
                ts = time.monotonic()
                res = run_seed(runner, seed, opts.iterations, opts.verbose, history)
                n = sum(1 for c in history if c.kind != "sleep")
                total += n
                if res:
                    r, t, diff = res
                    report_mismatch(runner, env, list(history), r, t, diff,
                                    {"where": f"seed {seed}, command #{n}", "tag": f"seed{seed}",
                                     "seed": seed})
                    return 1
                dt = time.monotonic() - ts
                print(f"seed {seed}: {n} commands, no mismatch ({dt:.1f}s)", flush=True)
                if dt > opts.min_ttl * 0.8:
                    print(f"  WARNING: seed took {dt:.0f}s, close to --min-ttl {opts.min_ttl}s; keys may "
                          f"expire mid-run and cause timing flakiness. Raise --min-ttl.", file=sys.stderr)
            print(f"\nOK: {opts.seeds} seeds, {total} commands, zero mismatches "
                  f"({time.monotonic() - t0:.1f}s)")
            cov = ", ".join(f"{k}={v}" + (f"(err {runner.ref_errors[k]})" if runner.ref_errors[k] else "")
                            for k, v in sorted(runner.stats.items()))
            print(f"coverage (reference error replies in parentheses): {cov}")
            return 0
    except HarnessError as e:
        print(f"HARNESS ERROR: {e}", file=sys.stderr)
        return 2
    except RuntimeError as e:
        print(f"SETUP ERROR: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
