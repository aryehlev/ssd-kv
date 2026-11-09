# Black-box Redis compatibility harness

Tests the new RESP2/RESP3 server as a black box at `host:port`. It never
imports or builds anything from `src/`.

| file | what it does | needs |
|---|---|---|
| `differential.py` | Seeded random command streams sent to a reference `redis-server` 7.0.15 **and** the target over RESP2 and RESP3 connections, with replies compared type-exact. Stops at the first mismatch, shrinks it to a minimal reproducer, saves it as JSON and exits 1 | `/usr/bin/redis-server` |
| `scenarios.py` | Hand-written pytest scenarios, run against the target only (pipelining, 1/16 MiB values, binary safety, expiry, SELECT, HELLO 3, CLIENT, INFO, ...) | `TARGET` env var |
| `persistence.py` | Crash-durability checker: modelled writes, SIGKILL, restart, verify | a launch command template |
| `compat_common.py` | Shared code: type-preserving RESP parser for redis-py, process helpers | |

**All three tools flush every database (0-15) on the server they test.**
Never point them at a server whose data you need.

## Setup

```bash
cd tests/compat
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt   # redis==5.2.1, pytest
```

## differential.py

```bash
# against a running target
.venv/bin/python differential.py --target 127.0.0.1:7777

# or let the harness launch it ({port} and {dir} are substituted; process group killed at exit)
.venv/bin/python differential.py \
    --target-cmd '../../target/release/ssd-kv --port {port} --data-dir {dir}'
# a fixed port and no {port} placeholder also works: --target-cmd '...' --target-port 7777

# more / different seeds, longer streams
.venv/bin/python differential.py --target 127.0.0.1:7777 --seed 1000 --seeds 50 --iterations 5000
```

The harness starts its own reference with
`redis-server --port <free> --save '' --appendonly no --daemonize no` in a temp
dir and kills it at the end. You can pass `--ref host:port` to use one that is
already running.

Useful flags:

| flag | meaning |
|---|---|
| `--seed N --seeds K` | run seeds N..N+K-1 (default 1, 5) |
| `--iterations N` | commands per seed (default 2000) |
| `--strict-errors` | compare full error messages, not only the first word |
| `--loose-types` | treat simple and bulk strings as the same, and treat every null encoding as the same (see below) |
| `--edge-globs` | also use malformed glob patterns (`h[ae`, trailing `\`, `[]`, ...) |
| `--min-ttl S` | smallest relative TTL in the random stream (default 60 s) |
| `--large-prob P` | probability that a value is 256 KiB to 1 MiB (default 0.005) |
| `--expiry-scenario-prob P` | probability of a real-expiry block per step, each with a 650 ms sleep (default 0.0015) |
| `--replay FILE` | replay a saved reproducer instead of random seeds |
| `--no-shrink`, `--shrink-budget S` | turn off shrinking, or set its time budget (default 120 s) |
| `--out-dir DIR` | where the reproducer JSON files go (default: a new temp dir) |
| `-v` | print every command and its reference reply |

### What gets generated

* **Connections:** four connections per server. c0 and c2 stay on RESP2. c1 and c3 send
  `HELLO 3` first, as part of the compared stream. Random `HELLO [2|3|4|abc]`
  switches happen later. `SELECT` is per connection, so the connections
  usually sit in different DBs.
* **Commands:** PING, ECHO, SELECT, DBSIZE, FLUSHDB [ASYNC|SYNC], GET, SET with
  random combinations of EX/PX/EXAT/PXAT/NX/XX/KEEPTTL/GET (conflicting
  combinations included), SETNX, SETEX, PSETEX, GETSET, GETDEL, GETEX
  EX/PX/EXAT/PXAT/PERSIST, MGET, MSET, MSETNX, APPEND, STRLEN, GETRANGE,
  SETRANGE, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, DEL, UNLINK, EXISTS, TYPE,
  EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT with NX/XX/GT/LT, TTL, PTTL, EXPIRETIME,
  PEXPIRETIME, PERSIST, RENAME, RENAMENX, KEYS, SCAN MATCH/COUNT/TYPE, HELLO.
  Command names and options appear in random case.
* **Error cases:** wrong arity for every command, unknown commands, malformed
  integers (`+1`, `01`, ` 1`, `1.5`, `9223372036854775808`), INCR overflow,
  INCR on non-integers, invalid floats, inf/nan, SELECT out of range, SETRANGE
  past 512 MiB (this never allocates), invalid SCAN cursors and options, and
  conflicting SET, GETEX and EXPIRE flags. `WRONGTYPE` cannot happen, because the
  command set only creates strings.
* **Keys:** 50 names, half the picks going to 12 hot keys: the empty key,
  `\x00`, binary keys, CR/LF, tab, unicode (incl. a combining character),
  hashtags `{user1}:a`, glob characters in names (`a*b`, `h[ae]llo`), 300 B and
  1 KB names.
* **Values:** empty, words, small ints, i64 edges (±2^63, ±2^63±1, 2^64-1),
  near-integers (`-0`, `007`, `1e3`, `0x10`), floats (`1e308`, `4.9e-324`,
  `1e400`, `0x1p-3`, `inf`, `nan`, ...), random binary, RESP look-alikes
  (`*1\r\n$4\r\nPING\r\n`), 64 B to 64 KiB blobs, and sometimes 256 KiB to 1 MiB blobs.

### Normalization rules

Everything not listed here must match **byte-for-byte and RESP-type-for-type**.
Replies are read through redis-py `execute_command` with
`decode_responses=False` and all response callbacks cleared, using
`compat_common.TypedRESPParser`. That parser keeps the wire type, so the harness
can tell apart `+OK` vs `$2 OK`, `$-1` (RESP2 null) vs `_` (RESP3 null), and the
exact integer text.

| reply to | rule |
|---|---|
| TTL, EXPIRETIME | If either side is negative, both must be equal (-1 means no TTL, -2 means missing). Otherwise both must be >= 0 and within **2 s** of each other. |
| PTTL, PEXPIRETIME | Same as above, within **2000 ms**. |
| KEYS | Compared as sorted lists, so order is ignored. Duplicates still count. |
| SCAN | The harness runs a **full iteration** (cursor 0 until 0) on each server and compares the **set** of keys. Cursors, batch sizes and order are ignored. Every step must still have the shape `[bulk cursor, array of bulk]`. If the first call is an error, the errors are compared. |
| errors | Compared by **prefix**, the first word (`ERR`, `NOPROTO`, ...). `--strict-errors` compares the full message. A reply that is an error on only one side is always a mismatch. |
| HELLO | The values of `id`, `server` and `version` are ignored; their RESP types are still compared. Pairs are compared order-insensitively. |
| RESP3 map / set | Compared order-insensitively everywhere. |
| INCRBYFLOAT | **Exact**, with no float tolerance. See the caveats below. |
| `--loose-types` | Simple string = bulk string, every null encoding (`$-1`, `*-1`, `_`) is equal, verbatim = bulk, big number = integer, attributes dropped. |

#### Avoiding timing flakiness

* No TTL in the random stream is shorter than `--min-ttl` (60 s), and absolute
  times are at least that far in the future. So nothing expires during a seed;
  the harness warns if a seed runs longer than 80 % of `--min-ttl`. Durations
  are drawn from wide ranges, which makes GT/LT ties between two expiry times
  a few ms apart practically impossible.
* Times in the past are only used where Redis deletes the key immediately and
  deterministically: EXPIREAT, PEXPIREAT, GETEX EXAT/PXAT, and EXPIRE with a
  value <= 0. `SET ... EXAT <past>` leaves the key counted by DBSIZE until it is
  next accessed, so it only appears inside expiry blocks, followed directly by
  EXISTS.
* Absolute times are stored as *offsets from now*. They are resolved once per
  command, the same value is sent to both servers, and replays and shrinking
  resolve them again.
* **Expiry blocks** exercise real expiry. Each one sets a 300 ms TTL using one
  of 13 forms: SET PX, PSETEX, PEXPIRE, SET PXAT, GETEX PX, RENAME carrying the
  TTL, KEEPTTL, a plain SET that clears the TTL, PERSIST, APPEND/SETRANGE, MSET
  plus PEXPIREAT, or LT. Then the harness sleeps 650 ms and runs `EXISTS` on
  every short-lived key, followed by GET, TTL and DBSIZE. Touching the keys
  first lets a lazy-expiry target and Redis's active expiry converge before
  anything counts keys.

### On a mismatch

The harness prints the command, both replies (type-annotated) and the reason.
It then shrinks the prefix: first it keeps only commands that touch the
failing command's keys, plus SELECT, HELLO and FLUSHDB. Then it runs delta
debugging, replaying every candidate from empty DBs on fresh RESP2
connections. If you launched the target with `--target-cmd` and it crashed,
it is restarted for each replay. The minimal sequence is printed in
redis-cli-pasteable quoting (`[cN]` is the connection). Both the full and the
shrunk prefix are saved as JSON:

```bash
.venv/bin/python differential.py --target 127.0.0.1:7777 --replay /tmp/ssdkv-diff-failure-xxx/repro-seed3-min.json
```

Exit codes: 0 means no mismatch, 1 means a mismatch, 2 means a setup or harness
error (for example, the target cannot answer `SELECT` or `FLUSHDB`, which the
harness uses to reset state).

## scenarios.py

```bash
TARGET=127.0.0.1:7777 .venv/bin/python -m pytest scenarios.py            # default TARGET is 127.0.0.1:7777
TARGET=127.0.0.1:7777 .venv/bin/python -m pytest scenarios.py -m "not impl_specific"
```

Expected values were captured from real Redis 7.0.15 on x86-64. This file
covers:

* pipelining: 10k SETs then 10k GETs, 20k commands in a single write, byte-by-byte fragmented writes
* 1 MiB and 16 MiB values, including shrinking and growing on overwrite, plus 8 MiB in one pipeline
* binary-safe keys and values: every byte value, the empty key, RESP look-alikes
* expiry actually removing keys: `SET PX 200`, sleep 400 ms, then GET is nil, DBSIZE excludes the key, KEYS and SCAN hide it; nine expiry variants
* TTL, EXPIRETIME, GT/LT/NX/XX, and past times deleting immediately
* DEL and UNLINK return counts, including duplicates
* SELECT isolation per connection, and FLUSHDB scope and options
* HELLO 3 map fields, RESP3 nulls, HELLO 2 fallback, NOPROTO, no unsolicited attribute or push frames
* stock redis-py in RESP3 mode
* CLIENT SETNAME/GETNAME, including HELLO SETNAME
* COMMAND COUNT, and INFO as a bulk string containing `# Server` (RESP3 accepts verbatim `=` or bulk)
* SET/GET family semantics, INCR edges, portable INCRBYFLOAT, KEYS glob semantics, a full SCAN iteration
* concurrency: 16 threads × 500 INCR/SET, and 200 connections

Tests marked **`impl_specific`** cover behavior that is legitimately
implementation-specific. Real Redis passes them, but the target may differ by
design:

| test | why it is implementation-specific |
|---|---|
| `test_error_messages_match_redis` | exact error text |
| `test_active_expiry_reclaims_untouched_keys` | DBSIZE dropping without any access needs active expiry |
| `test_incrbyfloat_long_double_formatting` | x87 80-bit long double results (`0.1+0.2` gives `0.3`, `1+1e17` gives `100000000000000001`); Redis built for aarch64 or Apple Silicon differs too |
| `test_command_metadata` | full `COMMAND` / `COMMAND DOCS` |
| `test_info_has_redis_fields` | `redis_version` in INFO |
| `test_resp3_attribute_reply_via_debug` | `DEBUG PROTOCOL attrib`. Skipped when DEBUG is unavailable, which is the Redis default (`enable-debug-command no`) |
| `test_inline_commands` | the telnet-style inline protocol |
| `test_protocol_error_reply` | the `-ERR Protocol error` reply to garbage input |

## persistence.py

```bash
# every acknowledged write must survive SIGKILL
.venv/bin/python persistence.py --sync \
    --target-cmd '../../target/release/ssd-kv --port {port} --data-dir {dir}'

# the server only guarantees durability after an explicit command
.venv/bin/python persistence.py --sync-cmd 'FSYNC' --sync-every 500 --target-cmd '...'

# best effort: losses are reported, but corruption, resurrection etc. still fail
.venv/bin/python persistence.py --target-cmd '...'
```

`{port}` and `{dir}` are substituted into `--target-cmd`. The data dir is kept
across restarts. `--restart-cmd` sets a different template for the restarts,
and `--dir` / `--port` fix the values.

Each cycle (`--cycles`, default 3) works as follows:

1. Pipeline up to `--ops` writes (default 50000) on one connection. The writes
   are SET (plain, EX, short PX, KEEPTTL), APPEND, INCRBY, DEL, UNLINK, EXPIRE,
   short PEXPIRE, PEXPIREAT, PERSIST, MSET, RENAME, SELECT between DBs 0 and 1,
   and MSET followed by FLUSHDB in scratch DB 2. They go to 2000 key names per
   DB with lots of overwrites and size changes; `--large-prob` gives values up
   to `--max-value`. Every write is modelled, and each reply is checked against
   the model (`WRONG_REPLY`).
2. `SIGKILL` the server's process group (`--kill-signal` changes the signal).
   In the default `--kill-mode inflight` the kill comes at a random point
   0.3 to 2 s into the stream, while writes are in flight. In `quiesced` mode
   it comes after everything was acknowledged.
3. Wait until every short TTL (`--short-ttl-ms`, default 1500) has passed, so
   those keys expire while the server is down. Then restart the server and wait
   for `PING` (`-LOADING` replies keep it waiting).
4. Verify every key the model ever touched with GET + PEXPIRETIME (±1000 ms,
   `--ttl-tolerance-ms`), and SCAN all 16 DBs for keys nobody wrote. The
   recovered state becomes the starting state of the next cycle.

Durability contract:

* **`--sync`:** every acknowledged write survives.
* **`--sync-cmd`:** writes acknowledged before an acknowledged sync command survive.
* **neither (best effort):** losses are only counted. `--fail-on-loss` turns them into failures.

A write that was in flight at the kill may or may not be applied; for each key,
any later in-flight state is accepted.

Failure categories:

| category | when it fails |
|---|---|
| `LOST` | A durable write is missing or stale. Only in `--sync` or `--sync-cmd` mode; best effort reports it as `LOST_BEST_EFFORT`. |
| `RESURRECTED` | A durably deleted key is back. Always a failure. |
| `EXPIRED_RESURRECTED` | A key whose TTL had passed is readable. Always a failure. |
| `TTL_LOST` / `TTL_WRONG` | The value is right, but the expiry is gone or wrong. A TTL that was stored relative and re-applied after restart shows up here. Always a failure. |
| `REGRESSED` | The state is older than what the previous restart recovered. Always a failure. |
| `CORRUPT` | A value nobody wrote. Always a failure. |
| `UNEXPECTED_KEY` | A key that was never written. Always a failure. |
| `WRONG_REPLY` / `SYNC_ERROR` / `SERVER_DIED` | Problems before the crash. Always a failure. |
| prefix consistency | Checks whether one cut point in the op stream explains all keys. It is reported every cycle, and is a failure with `--require-prefix`. |

Exit codes: 0 means OK, 1 means a violation, 2 means a setup error. On failure,
the data dir and server logs are kept.

## Validating the harness itself (against real Redis)

```bash
redis-server --port 16380 --save '' --appendonly no --daemonize yes
.venv/bin/python differential.py --target 127.0.0.1:16380 --seeds 40 --strict-errors   # expect 0 mismatches
TARGET=127.0.0.1:16380 .venv/bin/python -m pytest scenarios.py                         # expect all pass (1 skip)
.venv/bin/python persistence.py --sync \
    --target-cmd "exec redis-server --port {port} --dir {dir} --save '' --appendonly yes --appendfsync always"  # expect OK
.venv/bin/python persistence.py --sync \
    --target-cmd "exec redis-server --port {port} --dir {dir} --save '' --appendonly no"  # expect LOST failures
```

## Caveats

* **Redis 7.0.15 quirks are the contract,** because the reference is 7.0.15.
  * `GETRANGE k 0 -100` on a non-empty string returns its first byte (7.2 returns "").
  * `EXPIRETIME` rounds to the nearest second (`(ms+500)/1000`).
  * `SET NX GET` is allowed.
  * Repeated options such as `SET k v EX 10 EX 10` and `XX XX` are accepted.
  * GT/LT treat "no TTL" as infinite.
  * `SCAN TYPE <unknown>` returns nothing instead of an error.
  * INCRBYFLOAT goes through `strtold`, so it accepts hex (`0x10`) and values like `1e400`, and prints `%.17Lf` with trailing zeros trimmed.
* **INCRBYFLOAT** is compared exactly, so a target using f64 will mismatch on
  most fractional increments: Redis returns `0.1` where f64 `%.17f` gives
  `0.10000000000000001`, and `0.3` for `0.1+0.2`. This is intended. Matching
  needs long-double-compatible arithmetic and formatting.
* DBSIZE counts logically-expired-but-unreclaimed keys in Redis. The random
  stream only checks DBSIZE after short-lived keys have been touched, and the
  pure active-expiry check lives in an `impl_specific` scenario.
* TTL tolerances assume the client clock and both servers are on one host.
* The harness reads at most 1 MiB for a header or simple-string line;
  anything longer is reported as a protocol error.
