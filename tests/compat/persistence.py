#!/usr/bin/env python3
"""Crash-durability checker (SIGKILL + restart) for a RESP key-value server.

The server is launched from a command template; ``{port}`` and ``{dir}`` are
substituted (the data directory is reused across restarts)::

    persistence.py --target-cmd './target/release/ssd-kv --port {port} --data-dir {dir}' --sync

Each cycle: write a random but fully modelled stream of operations (SET with
and without TTLs, overwrites with size changes, APPEND, INCRBY, DEL, UNLINK,
EXPIRE/PEXPIRE/PEXPIREAT, PERSIST, MSET, RENAME, SELECT, FLUSHDB on a scratch
DB) over one pipelined connection, SIGKILL the server's process group (either
after everything was acknowledged, or while writes are in flight), wait until
every short TTL has passed, restart it on the same data dir and verify every
key the model knows about (GET + PEXPIRETIME), plus SCAN every DB for keys
nobody wrote.

Durability contract (what must survive the crash):
  --sync              every acknowledged write (server runs in fsync-per-write
                      mode).  With --sync-cmd as well, the sync command is sent
                      after every pipeline batch and a write counts once a
                      later sync command was acknowledged.
  --sync-cmd 'CMD..'  (without --sync) the command is sent every --sync-every
                      ops; writes acknowledged before an acknowledged sync must
                      survive.
  neither             best effort: lost writes are only reported (use
                      --fail-on-loss to fail on them).

Always failures, in every mode: CORRUPT (value nobody wrote), RESURRECTED (a
durably deleted key is back), EXPIRED_RESURRECTED (a key whose TTL passed is
readable), REGRESSED (state older than what a previous restart recovered),
TTL_LOST / TTL_WRONG, UNEXPECTED_KEY, WRONG_REPLY (pre-crash reply differs
from the model).  LOST (a durable write is missing or stale) is a failure in
--sync / --sync-cmd mode.  Writes that were in flight at the kill may or may
not be applied (per key, any later in-flight state is accepted).  A global
"prefix consistency" check (is there one cut point in the op stream that
explains every key?) is reported, and enforced with --require-prefix.
"""

from __future__ import annotations

import argparse
import collections
import os
import random
import re
import shlex
import shutil
import signal
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

from compat_common import (
    RawConn,
    ServerProcess,
    encode_command,
    fill_template,
    free_port,
    is_error,
    port_is_free,
    quote_arg,
)

OK = ("+", b"OK")
_INT_RE = re.compile(rb"^-?[1-9][0-9]{0,17}$|^0$")


# --------------------------------------------------------------------------
# model
# --------------------------------------------------------------------------
@dataclass
class Expiry:
    lo: int  # earliest possible absolute expiry (ms since epoch)
    hi: int  # latest possible
    rel_ms: Optional[int] = None  # relative TTL (hi is patched at ack / kill time)


@dataclass(frozen=True)
class KState:
    value: bytes
    exp: Optional[Expiry] = None


@dataclass
class Op:
    idx: int
    db: int
    args: list
    effects: list = field(default_factory=list)  # [((db, key), KState|None)]
    expect: Optional[set] = None  # acceptable replies (None = don't check)
    new_expiries: list = field(default_factory=list)
    kind: str = "write"  # write | sync | select
    acked: bool = False

    def display(self) -> str:
        return " ".join(quote_arg(a if isinstance(a, bytes) else str(a).encode(), 40) for a in self.args)


def int_reply(n: int):
    return (":", str(n).encode())


class Model:
    def __init__(self, rng: random.Random, opts):
        self.rng = rng
        self.opts = opts
        self.main_dbs = opts.dbs
        self.scratch_db = opts.scratch_db
        self.state: dict = {}  # (db, key) -> KState  (absent keys are missing)
        self.hist: dict = collections.defaultdict(list)
        self.ever: dict = collections.defaultdict(set)  # values seen in any cycle
        self.universe: dict = collections.defaultdict(set)  # db -> keys ever touched
        self.names = []
        for i in range(opts.keys):
            if i % 40 == 7:
                self.names.append(b"bin:\x00\xff\r\n:%d" % i)
            elif i % 40 == 23:
                self.names.append(("ключ:%d" % i).encode())
            else:
                self.names.append(b"k:%d" % i)
        self.idx = 0
        self.cur_db = 0
        self.cycle = 0

    # -- cycle bookkeeping ---------------------------------------------
    def start_cycle(self, cycle: int):
        self.cycle = cycle
        self.hist = collections.defaultdict(list)
        for k, st in self.state.items():
            self.hist[k].append((-1, st))

    def hist_of(self, k):
        h = self.hist.get(k)
        if not h:
            h = self.hist[k] = [(-1, self.state.get(k))]
        return h

    # -- helpers -------------------------------------------------------
    def now_ms(self) -> int:
        return int(time.time() * 1000)

    def live(self, k) -> tuple[Optional[KState], str]:
        """Model state as the *running* server sees it now:
        'present' | 'absent' | 'volatile' (TTL about to fire / just fired)."""
        st = self.state.get(k)
        if st is None:
            return None, "absent"
        if st.exp is None:
            return st, "present"
        now = self.now_ms()
        margin = 400
        if st.exp.hi + margin < now:
            return None, "absent"
        if st.exp.lo - margin <= now:
            return st, "volatile"
        return st, "present"

    def value(self) -> bytes:
        r = self.rng.random()
        tag = b"c%d:o%d:" % (self.cycle, self.idx)
        if r < self.opts.large_prob:
            n = self.rng.randint(64 * 1024, self.opts.max_value)
        elif r < 0.10:
            n = self.rng.randint(1024, 16 * 1024)
        elif r < 0.15:
            return b""  # empty value (not unique, deliberately)
        else:
            n = self.rng.randint(0, 100)
        return tag + self.rng.randbytes(n)

    def _set(self, k, st: Optional[KState], op: Op):
        h = self.hist_of(k)  # must be created from the state *before* this op
        op.effects.append((k, st))
        if st is None:
            self.state.pop(k, None)
        else:
            self.state[k] = st
            self.ever[k].add(st.value)
        self.universe[k[0]].add(k[1])
        h.append((op.idx, st))

    def rel_exp(self, ms: int, op: Op) -> Expiry:
        now = self.now_ms()
        e = Expiry(lo=now + ms, hi=now + ms + 600_000, rel_ms=ms)
        op.new_expiries.append(e)
        return e

    def pick_key(self, db: Optional[int] = None):
        db = self.cur_db if db is None else db
        return (db, self.rng.choice(self.names))

    # -- generation ----------------------------------------------------
    def next_ops(self) -> list[Op]:
        """One logical step (usually one op)."""
        r = self.rng.random()
        if r < 0.002:
            return self.flush_scratch()
        if r < 0.03 and len(self.main_dbs) > 1:
            db = self.rng.choice(self.main_dbs)
            return [self.mk_select(db)]
        return [self.mk_write()]

    def new_op(self, db, args, kind="write") -> Op:
        op = Op(idx=self.idx, db=db, args=args, kind=kind)
        self.idx += 1
        return op

    def mk_select(self, db) -> Op:
        op = self.new_op(db, [b"SELECT", str(db).encode()], kind="select")
        op.expect = {OK}
        self.cur_db = db
        return op

    def mk_sync(self, args) -> Op:
        return self.new_op(self.cur_db, list(args), kind="sync")

    def flush_scratch(self) -> list[Op]:
        back = self.cur_db
        ops = [self.mk_select(self.scratch_db)]
        pairs = []
        keys = [(self.scratch_db, b"scratch:%d" % i) for i in range(self.rng.randint(5, 30))]
        op = self.new_op(self.scratch_db, [b"MSET"])
        for k in keys:
            v = self.value()
            op.args += [k[1], v]
            pairs.append((k, v))
        for k, v in pairs:
            self._set(k, KState(v), op)
        op.expect = {OK}
        ops.append(op)
        if self.rng.random() < 0.5:
            k = keys[0]
            op = self.new_op(self.scratch_db, [b"SET", k[1], b"ttl-val", b"EX", b"100000"])
            self._set(k, KState(b"ttl-val", self.rel_exp(100_000_000, op)), op)
            op.expect = {OK}
            ops.append(op)
        if self.rng.random() < 0.8:
            op = self.new_op(self.scratch_db, [b"FLUSHDB"])
            for key in sorted(self.universe[self.scratch_db]):
                self._set((self.scratch_db, key), None, op)
            op.expect = {OK}
            ops.append(op)
        ops.append(self.mk_select(back))
        return ops

    def mk_write(self) -> Op:
        rng = self.rng
        db = self.cur_db
        k = self.pick_key()
        st, live = self.live(k)
        kind = rng.choices(
            ["set", "set_ex", "set_px_short", "set_keepttl", "append", "incrby", "del", "unlink",
             "expire", "pexpire_short", "pexpireat", "persist", "mset", "rename"],
            weights=[30, 5, 4, 2, 5, 5, 8, 3, 3, 2, 2, 2, 4, 2])[0]
        if live == "volatile" and kind not in ("set", "set_ex", "set_px_short", "mset"):
            kind = "set"  # outcome of the other ops would depend on expiry timing
        if kind in ("append", "set_keepttl") and st is not None and len(st.value) > 256 * 1024:
            kind = "set"
        if kind == "incrby" and st is not None and not _INT_RE.match(st.value):
            kind = "set_int"
        if kind in ("expire", "pexpire_short", "pexpireat", "persist", "rename") and st is None:
            kind = "set"

        if kind == "set":
            v = self.value()
            op = self.new_op(db, [b"SET", k[1], v])
            self._set(k, KState(v), op)
            op.expect = {OK}
        elif kind == "set_int":
            v = str(rng.randint(-10**6, 10**6)).encode()
            op = self.new_op(db, [b"SET", k[1], v])
            self._set(k, KState(v), op)
            op.expect = {OK}
        elif kind == "set_ex":
            v = self.value()
            ttl = rng.randint(3600, 86400)
            op = self.new_op(db, [b"SET", k[1], v, b"EX", str(ttl).encode()])
            self._set(k, KState(v, self.rel_exp(ttl * 1000, op)), op)
            op.expect = {OK}
        elif kind == "set_px_short":
            v = self.value()
            ms = self.opts.short_ttl_ms + rng.randint(0, 500)
            op = self.new_op(db, [b"SET", k[1], v, b"PX", str(ms).encode()])
            self._set(k, KState(v, self.rel_exp(ms, op)), op)
            op.expect = {OK}
        elif kind == "set_keepttl":
            v = self.value()
            op = self.new_op(db, [b"SET", k[1], v, b"KEEPTTL"])
            self._set(k, KState(v, st.exp if st else None), op)
            op.expect = {OK}
        elif kind == "append":
            v = self.value()
            op = self.new_op(db, [b"APPEND", k[1], v])
            nv = (st.value if st else b"") + v
            self._set(k, KState(nv, st.exp if st else None), op)
            op.expect = {int_reply(len(nv))}
        elif kind == "incrby":
            n = rng.randint(-1000, 1000)
            cur = int(st.value) if st else 0
            op = self.new_op(db, [b"INCRBY", k[1], str(n).encode()])
            nv = str(cur + n).encode()
            self._set(k, KState(nv, st.exp if st else None), op)
            op.expect = {int_reply(cur + n)}
        elif kind in ("del", "unlink"):
            keys = {k} | {self.pick_key() for _ in range(rng.randint(0, 2))}
            keys = sorted(keys)
            op = self.new_op(db, [kind.upper().encode()] + [x[1] for x in keys])
            sure = maybe = 0
            for x in keys:
                s, lv = self.live(x)
                if lv == "present":
                    sure += 1
                elif lv == "volatile":
                    maybe += 1
                self._set(x, None, op)
            op.expect = {int_reply(n) for n in range(sure, sure + maybe + 1)}
        elif kind == "expire":
            ttl = rng.randint(3600, 86400)
            op = self.new_op(db, [b"EXPIRE", k[1], str(ttl).encode()])
            self._set(k, KState(st.value, self.rel_exp(ttl * 1000, op)), op)
            op.expect = {int_reply(1)}
        elif kind == "pexpire_short":
            ms = self.opts.short_ttl_ms + rng.randint(0, 500)
            op = self.new_op(db, [b"PEXPIRE", k[1], str(ms).encode()])
            self._set(k, KState(st.value, self.rel_exp(ms, op)), op)
            op.expect = {int_reply(1)}
        elif kind == "pexpireat":
            at = self.now_ms() + rng.randint(3600_000, 86400_000)
            op = self.new_op(db, [b"PEXPIREAT", k[1], str(at).encode()])
            self._set(k, KState(st.value, Expiry(at, at)), op)
            op.expect = {int_reply(1)}
        elif kind == "persist":
            op = self.new_op(db, [b"PERSIST", k[1]])
            self._set(k, KState(st.value, None), op)
            op.expect = {int_reply(1 if st.exp else 0)}
        elif kind == "mset":
            keys = sorted({k} | {self.pick_key() for _ in range(rng.randint(1, 3))})
            op = self.new_op(db, [b"MSET"])
            for x in keys:
                v = self.value()
                op.args += [x[1], v]
                self._set(x, KState(v), op)
            op.expect = {OK}
        elif kind == "rename":
            dst = self.pick_key()
            dst_st, dst_live = self.live(dst)
            if dst == k or dst_live == "volatile":
                return self.mk_write()
            op = self.new_op(db, [b"RENAME", k[1], dst[1]])
            self._set(k, None, op)
            self._set(dst, st, op)
            op.expect = {OK}
        else:  # pragma: no cover
            raise AssertionError(kind)
        return op


# --------------------------------------------------------------------------
# verification
# --------------------------------------------------------------------------
def effective(st: Optional[KState], now: int, tol: int) -> str:
    if st is None:
        return "absent"
    if st.exp is None:
        return "present"
    if st.exp.hi + tol < now:
        return "absent"
    if st.exp.lo - tol > now:
        return "present"
    return "ambiguous"


def matches(obs, st: Optional[KState], now: int, tol: int) -> bool:
    eff = effective(st, now, tol)
    if obs is None:
        return eff in ("absent", "ambiguous")
    if eff == "absent":
        return False
    value, pet = obs
    if st.value != value:
        return False
    if st.exp is None:
        return pet == -1
    return st.exp.lo - tol <= pet <= st.exp.hi + tol


def intersect(a: list, b: list) -> list:
    out, i, j = [], 0, 0
    while i < len(a) and j < len(b):
        lo, hi = max(a[i][0], b[j][0]), min(a[i][1], b[j][1])
        if lo <= hi:
            out.append((lo, hi))
        if a[i][1] < b[j][1]:
            i += 1
        else:
            j += 1
    return out


def read_all(host, port, dbs_keys: dict, timeout: float = 120.0) -> dict:
    """{(db, key): None | (value, pexpiretime)} via pipelined GET + PEXPIRETIME."""
    out = {}
    with RawConn(host, port, timeout=timeout) as c:
        for db, keys in sorted(dbs_keys.items()):
            keys = sorted(keys)
            assert c.call("SELECT", db) == OK
            for start in range(0, len(keys), 500):
                chunk = keys[start:start + 500]
                c.send_raw(b"".join(encode_command(b"GET", k) + encode_command(b"PEXPIRETIME", k)
                                    for k in chunk))
                for k in chunk:
                    g = c.read()
                    p = c.read()
                    if is_error(g) or is_error(p):
                        raise RuntimeError(f"verification read failed for {k!r}: {g!r} {p!r}")
                    if g[1] is None:
                        out[(db, k)] = None
                    else:
                        out[(db, k)] = (g[1], int(p[1]))
    return out


def scan_keys(host, port, db) -> set:
    keys = set()
    with RawConn(host, port) as c:
        assert c.call("SELECT", db) == OK
        cursor = b"0"
        while True:
            rep = c.call("SCAN", cursor, "COUNT", 1000)
            if is_error(rep):
                raise RuntimeError(f"SCAN failed: {rep!r}")
            cursor = rep[1][0][1]
            keys.update(x[1] for x in rep[1][1][1])
            if cursor == b"0":
                return keys


# --------------------------------------------------------------------------
# checker
# --------------------------------------------------------------------------
class Checker:
    def __init__(self, opts):
        self.opts = opts
        self.rng = random.Random(opts.seed)
        self.model = Model(self.rng, opts)
        self.own_dir = opts.dir is None
        self.dir = opts.dir or tempfile.mkdtemp(prefix="ssdkv-persistence-")
        os.makedirs(self.dir, exist_ok=True)
        self.port = opts.port or free_port()
        self.host = opts.host
        self.logdir = tempfile.mkdtemp(prefix="ssdkv-persistence-logs-")
        self.proc: Optional[ServerProcess] = None
        self.failures = collections.Counter()
        self.examples = collections.defaultdict(list)
        self.info = collections.Counter()
        self.restarts = 0

    # -- process control -------------------------------------------------
    def start(self, template: str):
        cmd = fill_template(template, port=self.port, dir=self.dir)
        log = os.path.join(self.logdir, f"server-{self.restarts}.log")
        self.restarts += 1
        t0 = time.monotonic()
        self.proc = ServerProcess(cmd, cwd=self.dir, log_path=log, name="target")
        self.proc.wait_ready(self.host, self.port, timeout=self.opts.startup_timeout)
        return time.monotonic() - t0

    def kill(self) -> float:
        t = time.time()
        sig = getattr(signal, "SIG" + self.opts.kill_signal.upper().removeprefix("SIG"))
        self.proc.kill(sig)
        deadline = time.monotonic() + 30
        while not port_is_free(self.port, self.host) and time.monotonic() < deadline:
            time.sleep(0.05)
        return t

    def fail(self, cat: str, msg: str):
        self.failures[cat] += 1
        if len(self.examples[cat]) < self.opts.max_examples:
            self.examples[cat].append(msg)

    # -- one cycle -----------------------------------------------------
    def write_phase(self, cycle: int):
        o = self.opts
        m = self.model
        m.start_cycle(cycle)
        cycle_first_idx = m.idx
        conn = RawConn(self.host, self.port, timeout=60)
        assert conn.call("SELECT", m.cur_db) == OK
        sync_args = shlex.split(o.sync_cmd) if o.sync_cmd else None
        sync_every = o.sync_every or o.pipeline
        stats = dict(sent=0, acked=0, last_acked=-1, last_sent=-1, barrier=-1, kill_time=None)
        stop = threading.Event()
        killing = threading.Event()
        kill_timer = None
        died_early = False
        if o.kill_mode == "inflight":
            delay = self.rng.uniform(o.kill_after_min, o.kill_after_max)

            def do_kill():
                killing.set()
                stats["kill_time"] = self.kill()
                stop.set()
            kill_timer = threading.Timer(delay, do_kill)
            kill_timer.start()
        since_sync = 0
        all_ops: list[Op] = []
        broken = False
        while stats["sent"] < o.ops and not stop.is_set() and not broken:
            batch: list[Op] = []
            while len(batch) < o.pipeline and stats["sent"] + len(batch) < o.ops:
                for op in m.next_ops():
                    batch.append(op)
                    since_sync += op.kind == "write"
                if sync_args and since_sync >= sync_every:
                    batch.append(m.mk_sync([a.encode() for a in sync_args]))
                    since_sync = 0
            all_ops += batch
            stats["sent"] += sum(op.kind == "write" for op in batch)
            stats["last_sent"] = batch[-1].idx
            try:
                conn.send_raw(b"".join(encode_command(*op.args) for op in batch))
            except OSError:
                broken, died_early = True, not killing.is_set()
                break
            for op in batch:
                try:
                    rep = conn.read()
                except Exception:
                    broken, died_early = True, not killing.is_set()
                    break
                t_ack = int(time.time() * 1000)
                op.acked = True
                stats["last_acked"] = op.idx
                if op.kind == "write":
                    stats["acked"] += 1
                for e in op.new_expiries:
                    e.hi = t_ack + e.rel_ms
                if op.kind == "sync":
                    if is_error(rep):
                        self.fail("SYNC_ERROR", f"{op.display()} -> {rep!r}")
                    else:
                        stats["barrier"] = op.idx
                elif op.expect is not None and rep not in op.expect:
                    self.fail("WRONG_REPLY", f"op #{op.idx} db{op.db} {op.display()} -> {rep!r}, "
                                             f"expected one of {sorted(op.expect)!r}")
        conn.close()
        if kill_timer is not None:
            kill_timer.join()  # writer done (budget or connection lost) -> kill fires on schedule
        else:
            stats["kill_time"] = self.kill()
        if stats["kill_time"] is None:
            stats["kill_time"] = self.kill()
        if died_early:
            self.fail("SERVER_DIED", f"connection lost before the planned kill in cycle {cycle} "
                                     f"(exit code {self.proc.proc.returncode}):\n{self.proc.log_tail()}")
        kill_ms = int(stats["kill_time"] * 1000)
        for op in all_ops:  # unacknowledged ops: they may still execute right up to the kill
            if not op.acked:
                for e in op.new_expiries:
                    e.hi = kill_ms + e.rel_ms
        stats["first_idx"] = cycle_first_idx
        return stats

    def durable_upto(self, stats) -> int:
        o = self.opts
        if o.sync_cmd:
            return stats["barrier"]
        if o.sync:
            return stats["last_acked"]
        return -1

    def verify_phase(self, cycle: int, stats) -> dict:
        o = self.opts
        m = self.model
        tol = o.ttl_tolerance_ms
        durable = self.durable_upto(stats)
        acked = stats["last_acked"]
        sent_upto = stats["last_sent"]
        universe = {db: set(keys) for db, keys in m.universe.items()}
        obs = read_all(self.host, self.port, universe)
        now = int(time.time() * 1000)
        counts = collections.Counter()
        cut = [(-1, max(sent_upto, -1))]
        new_state = {}
        for k, o_k in obs.items():
            h = m.hist_of(k)
            # index of the latest state the contract requires
            j_min = 0
            j_acked = 0
            for j, (idx, _) in enumerate(h):
                if idx <= durable:
                    j_min = j
                if idx <= acked:
                    j_acked = j
            ok_js = [j for j in range(j_min, len(h)) if matches(o_k, h[j][1], now, tol)]
            # prefix-consistency intervals for this key (any j)
            ivs = []
            for j, (idx, st) in enumerate(h):
                if matches(o_k, st, now, tol):
                    end = (h[j + 1][0] - 1) if j + 1 < len(h) else sent_upto
                    ivs.append((max(idx, -1), max(end, max(idx, -1))))
            cut = intersect(cut, ivs) if cut else cut
            if ok_js:
                counts["ok"] += 1
                if max(ok_js) < j_acked:
                    counts["lost_allowed"] += 1
                    self.info["acknowledged writes lost (allowed by the contract)"] += 1
                if min(ok_js) > j_acked:
                    counts["inflight_applied"] += 1
            else:
                cat, why = self.classify(k, o_k, h, j_min, now, tol)
                counts[cat] += 1
                self.fail(cat, f"cycle {cycle} db{k[0]} key {quote_arg(k[1], 40)}: {why}")
            if o_k is None:
                new_state[k] = None
            else:
                v, pet = o_k
                new_state[k] = KState(v, None if pet == -1 else Expiry(pet, pet))
        # unexpected keys
        for db in range(16):
            present = scan_keys(self.host, self.port, db)
            extra = present - universe.get(db, set())
            for key in sorted(extra)[: o.max_examples]:
                self.fail("UNEXPECTED_KEY", f"cycle {cycle} db{db}: key {quote_arg(key, 40)} was never written")
            if len(extra) > o.max_examples:
                self.failures["UNEXPECTED_KEY"] += len(extra) - o.max_examples
            counts["unexpected"] += len(extra)
        # model := what the server actually recovered
        m.state = {k: st for k, st in new_state.items() if st is not None}
        prefix_ok = bool(cut)
        if not prefix_ok:
            self.info["cycles without a consistent prefix cut"] += 1
            if o.require_prefix:
                self.fail("NOT_PREFIX_CONSISTENT", f"cycle {cycle}: no single cut point in the op stream "
                                                   f"explains the recovered state")
        counts["keys"] = len(obs)
        return {"counts": counts, "prefix": cut, "durable": durable}

    def classify(self, k, o_k, h, j_min, now, tol) -> tuple[str, str]:
        req = h[j_min][1]
        req_desc = describe(req, now, tol)
        # required state = what the previous restart recovered -> regression, not a loss
        lost = "REGRESSED" if h[j_min][0] < 0 else ("LOST" if self.strict_loss() else "LOST_BEST_EFFORT")
        if o_k is None:
            return lost, f"key missing; required state: {req_desc}"
        value, pet = o_k
        got = f"got {quote_arg(value, 40)} (len {len(value)}, pexpiretime {pet})"
        for j in range(j_min, len(h)):
            st = h[j][1]
            if st is not None and st.value == value:
                eff = effective(st, now, tol)
                if eff == "absent":
                    return "EXPIRED_RESURRECTED", f"{got}; that value's TTL had passed ({describe(st, now, tol)})"
                if st.exp is not None and pet == -1:
                    return "TTL_LOST", f"{got}; expected TTL ({describe(st, now, tol)})"
                return "TTL_WRONG", f"{got}; expected {describe(st, now, tol)}"
        for j in range(j_min - 1, -1, -1):
            st = h[j][1]
            if st is not None and st.value == value:
                deleted_later = any(h[x][1] is None for x in range(j + 1, j_min + 1))
                if deleted_later:
                    return "RESURRECTED", f"{got}: an older value came back after a durable delete; required: {req_desc}"
                return lost, f"{got}: stale value from op #{h[j][0]}; required: {req_desc}"
        if value in self.model.ever[k]:
            return "REGRESSED", f"{got}: value from a previous cycle, older than the recovered state"
        return "CORRUPT", f"{got}: no write ever produced this value; required: {req_desc}"

    def strict_loss(self) -> bool:
        return bool(self.opts.sync or self.opts.sync_cmd)

    def wait_short_ttls(self):
        latest = 0
        for st in self.model.state.values():
            if st.exp is not None and st.exp.rel_ms is not None and st.exp.rel_ms < 3600_000:
                latest = max(latest, st.exp.hi)
        for k, h in self.model.hist.items():
            for _, st in h:
                if st is not None and st.exp is not None and st.exp.rel_ms is not None \
                        and st.exp.rel_ms < 3600_000:
                    latest = max(latest, st.exp.hi)
        wait = (latest + self.opts.ttl_tolerance_ms + 200) / 1000 - time.time()
        if wait > 0:
            time.sleep(wait)
        return max(wait, 0)

    # -- driver --------------------------------------------------------
    def run(self) -> int:
        o = self.opts
        mode = ("sync (every acknowledged write must survive)" if o.sync and not o.sync_cmd else
                f"barrier via {o.sync_cmd!r}" if o.sync_cmd else "best effort (losses reported only)")
        print(f"data dir: {self.dir}   port: {self.port}   logs: {self.logdir}")
        print(f"durability contract: {mode}; kill mode: {o.kill_mode}; signal: {o.kill_signal}")
        try:
            self.start(o.target_cmd)
            with RawConn(self.host, self.port) as c:
                for db in range(16):
                    assert c.call("SELECT", db) == OK
                    assert c.call("FLUSHDB") == OK
            for cycle in range(1, o.cycles + 1):
                t0 = time.monotonic()
                stats = self.write_phase(cycle)
                waited = self.wait_short_ttls()
                startup = self.start(o.restart_cmd or o.target_cmd)
                res = self.verify_phase(cycle, stats)
                c = res["counts"]
                inflight = stats["last_sent"] - stats["last_acked"]
                print(f"cycle {cycle}/{o.cycles}: {stats['sent']} writes sent, {stats['acked']} acked, "
                      f"{inflight} ops in flight at kill, durable up to op #{res['durable']} "
                      f"(ops #{stats['first_idx']}..#{stats['last_sent']})")
                print(f"  waited {waited:.1f}s for short TTLs, restart took {startup:.2f}s, "
                      f"cycle {time.monotonic() - t0:.1f}s")
                bad = {k: v for k, v in c.items() if k not in ("ok", "keys", "lost_allowed",
                                                               "inflight_applied", "unexpected")}
                print(f"  verified {c['keys']} keys: ok {c['ok']}, in-flight writes applied "
                      f"{c['inflight_applied']}, lost-but-allowed {c['lost_allowed']}, "
                      f"unexpected keys {c['unexpected']}" + (f", PROBLEMS {dict(bad)}" if bad else ""))
                cut = res["prefix"]
                print("  prefix-consistent: " + (f"yes (cut point in ops {cut[0][0]}..{cut[0][1]})" if cut
                                                 else "NO (no single cut point explains the state)"))
        except RuntimeError as e:
            print(f"SETUP/RUNTIME ERROR: {e}", file=sys.stderr)
            self.cleanup(keep=True)
            return 2
        self.cleanup(keep=bool(self.failures) or o.keep_dir)
        return self.report()

    def report(self) -> int:
        o = self.opts
        hard = {k: v for k, v in self.failures.items() if k != "LOST_BEST_EFFORT"}
        if o.fail_on_loss and self.failures.get("LOST_BEST_EFFORT"):
            hard["LOST_BEST_EFFORT"] = self.failures["LOST_BEST_EFFORT"]
        print()
        for k, v in self.info.items():
            print(f"info: {k}: {v}")
        if self.failures:
            for cat, n in sorted(self.failures.items()):
                print(f"{cat}: {n}")
                for ex in self.examples[cat]:
                    print(f"    {ex}")
        if hard:
            print(f"\nFAIL: {sum(hard.values())} durability/correctness problems "
                  f"({', '.join(f'{k}={v}' for k, v in sorted(hard.items()))})")
            return 1
        print(f"\nOK: {o.cycles} crash/restart cycles, no durability violations")
        return 0

    def cleanup(self, keep: bool):
        if self.proc is not None:
            self.proc.kill()
        if keep:
            print(f"kept data dir {self.dir} and logs {self.logdir}")
            return
        if self.own_dir:
            shutil.rmtree(self.dir, ignore_errors=True)
        shutil.rmtree(self.logdir, ignore_errors=True)


def describe(st: Optional[KState], now: int, tol: int) -> str:
    if st is None:
        return "absent"
    d = f"value {quote_arg(st.value, 32)} (len {len(st.value)})"
    if st.exp is None:
        return d + ", no TTL"
    return d + f", expiry in [{st.exp.lo - now:+d}ms, {st.exp.hi - now:+d}ms] from now"


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    p.add_argument("--target-cmd", required=True,
                   help="shell command template launching the server; {port} and {dir} are substituted")
    p.add_argument("--restart-cmd", help="template used for restarts (default: --target-cmd)")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, help="port (default: a free one)")
    p.add_argument("--dir", help="data directory (default: a new temp dir, removed on success)")
    p.add_argument("--sync", action="store_true", help="every acknowledged write must survive")
    p.add_argument("--sync-cmd", help="durability barrier command, e.g. 'WAIT 0 0' or a custom FSYNC")
    p.add_argument("--sync-every", type=int, help="writes between sync commands (default: one per batch)")
    p.add_argument("--fail-on-loss", action="store_true", help="fail on losses even in best-effort mode")
    p.add_argument("--require-prefix", action="store_true",
                   help="fail if the recovered state is not a prefix of the op stream")
    p.add_argument("--cycles", type=int, default=3)
    p.add_argument("--ops", type=int, default=50000,
                   help="max writes per cycle (default 50000; in inflight mode the kill usually comes first)")
    p.add_argument("--keys", type=int, default=2000, help="key names per DB (default 2000)")
    p.add_argument("--dbs", type=lambda s: [int(x) for x in s.split(",")], default=[0, 1],
                   help="comma-separated main DBs (default 0,1)")
    p.add_argument("--scratch-db", type=int, default=2, help="DB used for FLUSHDB checks (default 2)")
    p.add_argument("--pipeline", type=int, default=64)
    p.add_argument("--kill-mode", choices=["quiesced", "inflight"], default="inflight",
                   help="kill after all writes were acknowledged, or in the middle of the stream")
    p.add_argument("--kill-after-min", type=float, default=0.3)
    p.add_argument("--kill-after-max", type=float, default=2.0)
    p.add_argument("--kill-signal", default="KILL")
    p.add_argument("--short-ttl-ms", type=int, default=1500, help="TTL used for keys that must expire")
    p.add_argument("--ttl-tolerance-ms", type=int, default=1000)
    p.add_argument("--max-value", type=int, default=1 << 20)
    p.add_argument("--large-prob", type=float, default=0.002)
    p.add_argument("--startup-timeout", type=float, default=120.0)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--keep-dir", action="store_true")
    p.add_argument("--max-examples", type=int, default=10)
    o = p.parse_args(argv)
    if "{port}" not in o.target_cmd and not o.port:
        p.error("--target-cmd has no {port} placeholder; pass --port")
    return o


def main(argv=None) -> int:
    return Checker(parse_args(argv)).run()


if __name__ == "__main__":
    sys.exit(main())
