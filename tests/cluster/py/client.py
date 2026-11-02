#!/usr/bin/env python3
"""redis-py verifier client of the cluster client matrix (DESIGN §25.6).

Usage: client.py --name NAME --ctrl DIR --seeds h:p,h:p,h:p [--proto 2|3]

1. Bootstraps a ``RedisCluster`` with the documented settings (all coordinators as startup
   nodes, ``dynamic_startup_nodes=False``), writes static keys, then checks a cluster
   pipeline, MULTI/EXEC on one slot (hashtag keys) and the server's CROSSSLOT error; writes
   ``NAME.static`` when they pass.
2. Writes (and sometimes overwrites) keys until ``DIR/stop`` appears, remembering every
   acknowledged write and every attempt whose outcome is unknown, and the time windows in
   which its writes failed (client-visible outages).
3. Reads the static keys through replicas (``RANDOM_REPLICA`` routing sends READONLY).
4. Verifies that every key holds its last acknowledged value or a value of a later or
   unacknowledged attempt (G6): zero acknowledged writes lost. Writes ``NAME.result`` (JSON).

Every wait is bounded (socket timeouts, deadlines); the script never blocks forever.
"""

import argparse
import json
import os
import random
import sys
import time

from redis.cluster import ClusterNode, RedisCluster
from redis.exceptions import RedisError, ResponseError

try:
    from redis.cluster import LoadBalancingStrategy
except ImportError:  # redis-py < 5.3
    LoadBalancingStrategy = None


# A gap between two successful writes of one slot range longer than this is an outage.
GAP_S = 0.25


class History:
    """Per-key attempt history of one sequential writer, and its client-visible outages per
    slot range (``slot >> 12``: four ranges aligned with every topology boundary of the
    matrix), so that writes to healthy shards never mask an outage of another shard."""

    def __init__(self, slot_of):
        self.keys = {}  # key -> list of (value, acked)
        self.acked = 0
        self.unacked = 0
        self.outages = []  # [start, end, range]
        self._slot_of = slot_of
        self._fail_since = {}
        self._last_ok = {}

    def record(self, key, value, ok, now, started):
        """``started``: when the call began (a call that blocks counts as unavailability)."""
        self.keys.setdefault(key, []).append((value, ok))
        b = self._slot_of(key) >> 12
        if ok:
            self.acked += 1
            last = self._last_ok.get(b, started)
            if now - last > GAP_S or b in self._fail_since:
                self.outages.append([last, now, b])
            self._fail_since.pop(b, None)
            self._last_ok[b] = now
        else:
            self.unacked += 1
            self._fail_since.setdefault(b, self._last_ok.get(b, started))

    def finish(self, now):
        for b, t in self._fail_since.items():
            self.outages.append([t, now, b])
        self._fail_since = {}

    def allowed(self, key):
        """(must_exist, allowed values): the last acked value or any unacknowledged one."""
        h = self.keys[key]
        last = max((i for i, (_, ok) in enumerate(h) if ok), default=None)
        if last is None:
            return None
        vals = {h[last][0]} | {v for v, ok in h if not ok}
        return vals


def log(name, msg):
    print(f"[{name}] {msg}", flush=True)


def retry(fn, deadline, what):
    last = None
    while time.time() < deadline:
        try:
            return fn()
        except RedisError as e:  # transient: topology changes, failover windows
            last = e
            time.sleep(0.1)
    raise RuntimeError(f"{what}: {last}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True)
    ap.add_argument("--ctrl", required=True)
    ap.add_argument("--seeds", required=True)
    ap.add_argument("--proto", type=int, default=2)
    ap.add_argument("--pause-ms", type=float, default=float(os.environ.get("KV_MATRIX_WRITER_PAUSE_MS", "4")))
    a = ap.parse_args()
    name, ctrl = a.name, a.ctrl
    seeds = [ClusterNode(h, int(p)) for h, p in (s.rsplit(":", 1) for s in a.seeds.split(","))]
    common = dict(
        startup_nodes=seeds,
        dynamic_startup_nodes=False,
        protocol=a.proto,
        socket_timeout=3,
        socket_connect_timeout=2,
        decode_responses=True,
    )
    result = {"name": name, "lib": "redis-py", "ok": False, "checks": {}}
    deadline = time.time() + 60
    rc = retry(lambda: RedisCluster(**common), deadline, "bootstrap")
    result["checks"]["bootstrap"] = "ok"

    # static keys (later read back through replicas)
    static = {f"{name}:s:{i}": f"s{i}-{name}" for i in range(200)}
    for k, v in static.items():
        assert retry(lambda: rc.set(k, v), deadline, f"SET {k}") is True
    for k, v in static.items():
        got = retry(lambda: rc.get(k), deadline, f"GET {k}")
        assert got == v, f"GET {k}: {got!r}"

    # pipeline (redis-py groups the commands by node)
    pipe = rc.pipeline()
    pk = [f"{name}:p:{i}" for i in range(100)]
    for k in pk:
        pipe.set(k, k)
    for k in pk:
        pipe.get(k)
    out = retry(pipe.execute, deadline, "pipeline")
    assert out[:100] == [True] * 100 and out[100:] == pk, "pipeline replies"
    result["checks"]["pipeline"] = "ok (100 SET + 100 GET)"

    # MULTI/EXEC on one slot
    tag = "{" + name + "-tx}"
    try:
        tx = rc.pipeline(transaction=True)
        tx.set(f"{tag}:a", "1")
        tx.incr(f"{tag}:a")
        tx.set(f"{tag}:b", "x")
        tx.get(f"{tag}:a")
        out = retry(tx.execute, deadline, "MULTI/EXEC")
        mode = "cluster pipeline transaction"
    except (NotImplementedError, AttributeError, TypeError) as e:
        node = rc.get_node_from_key(f"{tag}:a")
        tx = node.redis_connection.pipeline(transaction=True)
        tx.set(f"{tag}:a", "1")
        tx.incr(f"{tag}:a")
        tx.set(f"{tag}:b", "x")
        tx.get(f"{tag}:a")
        out = tx.execute()
        mode = f"node connection transaction ({type(e).__name__})"
    norm = ["OK" if x is True else x for x in out]
    assert norm == ["OK", 2, "OK", "2"], f"MULTI/EXEC replies {out!r}"
    assert rc.get(f"{tag}:b") == "x"
    result["checks"]["multi"] = f"ok ({mode})"

    # CROSSSLOT: two keys of different slots in one MSET, sent to the owner of the first key
    k1, k2 = f"{name}:x:a", f"{name}:x:b"
    while rc.keyslot(k1) == rc.keyslot(k2):
        k2 += "b"
    node = rc.get_node_from_key(k1)
    try:
        node.redis_connection.mset({k1: "1", k2: "2"})
        raise AssertionError("cross-slot MSET was accepted")
    except ResponseError as e:
        # redis-py maps the CROSSSLOT error code to ClusterCrossSlotError and strips the code
        assert type(e).__name__ == "ClusterCrossSlotError" or str(e).startswith("CROSSSLOT"), f"cross-slot MSET: {type(e).__name__}: {e}"
        assert "hash to the same slot" in str(e), f"cross-slot MSET message: {e}"
    assert rc.get(k1) is None and rc.get(k2) is None, "nothing written by the refused MSET"
    result["checks"]["crossslot"] = "ok (server CROSSSLOT)"
    open(os.path.join(ctrl, f"{name}.static"), "w").write(json.dumps(result["checks"]))
    log(name, "static checks passed")

    # writer under disruptions
    hist = History(rc.keyslot)
    rnd = random.Random(hash(name) & 0xFFFF)
    i = 0
    stop = os.path.join(ctrl, "stop")
    hard_end = time.time() + float(os.environ.get("KV_MATRIX_MAX_WRITE_S", "900"))
    while not os.path.exists(stop) and time.time() < hard_end:
        i += 1
        if i > 10 and i % 5 == 0:
            k = f"{name}:w:{rnd.randrange(1, i // 5 * 4)}"
        else:
            k = f"{name}:w:{i}"
        v = f"{name}-{i}"
        t0 = time.time()
        try:
            ok = rc.set(k, v) is True
        except RedisError:
            ok = False
        hist.record(k, v, ok, time.time(), t0)
        if a.pause_ms > 0:
            time.sleep(a.pause_ms / 1000.0)
    hist.finish(time.time())
    result["acked"], result["unacked"], result["outages"] = hist.acked, hist.unacked, hist.outages
    log(name, f"writer done: {hist.acked} acked, {hist.unacked} not acknowledged")

    # READONLY replica reads of the static keys
    deadline = time.time() + 60
    if LoadBalancingStrategy is not None:
        ro = retry(lambda: RedisCluster(load_balancing_strategy=LoadBalancingStrategy.RANDOM_REPLICA, **common), deadline, "replica client")
    else:
        ro = retry(lambda: RedisCluster(read_from_replicas=True, **common), deadline, "replica client")
    stale = 0
    for k, v in static.items():
        end = time.time() + 10
        while True:
            got = retry(lambda: ro.get(k), deadline, f"replica GET {k}")
            if got == v:
                break
            if time.time() > end:
                raise AssertionError(f"replica read of {k}: {got!r}")
            stale += 1
            time.sleep(0.05)
    result["checks"]["readonly"] = f"ok ({len(static)} replica reads, {stale} stale retries)"

    # zero lost acknowledged writes
    lost = []
    deadline = time.time() + 300
    for k in hist.keys:
        allowed = hist.allowed(k)
        if allowed is None:
            continue
        got = retry(lambda: rc.get(k), deadline, f"verify GET {k}")
        if got not in allowed:
            lost.append(f"{k}={got!r}")
    result["lost"] = len(lost)
    result["lost_examples"] = lost[:5]
    result["ok"] = not lost
    open(os.path.join(ctrl, f"{name}.result"), "w").write(json.dumps(result))
    log(name, f"verify: {len(lost)} lost of {hist.acked} acknowledged ({'OK' if not lost else 'FAILED'})")
    return 0 if not lost else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:  # noqa: BLE001 - every failure ends the client with a message
        print(f"FAILED: {type(e).__name__}: {e}", flush=True)
        sys.exit(1)
