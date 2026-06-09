# ssd-kv

A from-scratch Rust implementation of **SIndex** — *"SIndex: An SSD-based
Large-scale Indexing with Deterministic Latency for Cloud Block Storage"*
(ICPP '24, [DOI 10.1145/3673038.3673041](https://doi.org/10.1145/3673038.3673041);
extended as ACM TOS, [DOI 10.1145/3789205](https://doi.org/10.1145/3789205)) —
generalised from fixed-size block mappings to arbitrary keys and values, and
fronted by the Redis RESP protocol so any Redis client can talk to it.

```
client ──RESP──▶ reactor (io_uring, SO_REUSEPORT × N threads)
                    │
                    ▼
                KvEngine
                ├── Segment table (MLI high level)
                │     DashMap: partition_id → Partition
                │     partition_id = top 16 bits of xxh3(key) → 65 536 partitions
                │
                ├── Partition (MLI low level)          × 65 536, lazy
                │   ├── B+ tree of 4 KB ipages, height ≤ 3, per-partition RwLock
                │   └── SegmentFile  (ipage store on SSD, pread/pwrite)
                │
                ├── WSBCache — write-staging buffer cache (shared)
                │     16 clock lists, AccCount eviction, dirty pages pinned
                │     until synced; flushed pages stay cached (TSS stage 2)
                │
                └── ValueLog — append-only (key,value) store ⊕ redo journal
                      put  = append ALIVE entry   (this IS the WAL)
                      del  = append DELETED tombstone
```

## How a write works (the paper's write-staging + two-stage sync)

A `SET` appends to the value log (journal-first, no fsync) and updates the
B-Tree **in memory only** — modified ipages are staged dirty in the
WSBCache. The request path never waits for an fsync. A background TSS
thread every 50 ms:

1. takes a brief exclusive *epoch* guard and snapshots the dirty page set
   plus the value-log high-water mark `P` (every entry below `P` is fully
   indexed at that instant);
2. fsyncs the value log, writes + fsyncs the snapshot ipages (CRC-sealed)
   to their segment files, then atomically advances the on-disk
   checkpoint to `P`. Flushed pages **stay cached** so read-after-write
   is served from memory — the paper's "buffered" ipage state.

**Crash recovery** replays the value log from the checkpoint: ALIVE
entries are re-inserted, tombstones re-applied, a torn tail (magic/CRC
validated) is truncated. Replay is idempotent. Crash-consistency window =
one sync interval; data acked before a crash is recovered from the log.

## How a read works (deterministic latency)

`GET` = one shared-lock B-Tree descent — at most `BTREE_MAX_HEIGHT` (3)
ipage loads, each a WSBCache hit or a single `pread` — plus one value-log
`pread`. That is a **fixed upper bound of 4 storage round-trips**
independent of data volume; with the upper tree levels resident in the
WSBCache the common cold case is 2 preads. Value-log reads are lockless
(`pread` on a dedicated descriptor); readers of one partition proceed in
parallel and never block on the sync cycle.

## Paper fidelity map

| Paper mechanism (§) | Status here |
| --- | --- |
| 4 KB ipages, CRC, AccCount/Dirty state (§4.2.1) | ✅ per-ipage CRC sealed on write, verified on read |
| MLI: hash table → per-vplane B-Tree, height ≤ 3, per-tree lock (§4.2.3) | ✅ DashMap → per-partition B+ tree, height ≤ 3, per-partition RwLock |
| WSBCache, 16 clock lists, clock eviction (§4.3) | ✅ 16 hash-sharded clock lists (shard by page key rather than shortest-list, eviction inline rather than 16 threads) |
| Two-stage sync; flushed pages stay buffered for read-after-write (§4.3) | ✅ TSS thread; dirty pages never evicted; clean pages remain cached |
| WAL before ipage overwrite + replay recovery (§4.5) | ✅ value log doubles as the journal; checkpoint + idempotent replay + torn-tail truncation |
| Pointer swizzling (§4.2.2) | ✖ simplified to a sharded hash-map page table |
| Inter-SSD scheduling: RO/WO separation, epoch transition, EGD, DRS (§4.4) | ✖ requires ≥ 3 physical SSDs — out of scope on a single volume |

## Measured performance

Single VM (4 cores, virtio-blk + ext4 — *slower* than the bare NVMe in
the paper and in vendor benchmarks), engine micro-benchmarks via
`cargo bench`, server numbers via `redis-benchmark` / raw RESP sockets.

### Engine (in-process API)

| Operation | Latency | Notes |
| --- | --- | --- |
| `put` | **2.2 µs** | value-log append + staged B-Tree update; ~440 K ops/s sustained single-thread |
| `get` warm | **0.85 µs** | WSBCache hit |
| `get` cold | **123 µs** | OS page cache dropped before *every* op; ≈ leaf pread + value pread |
| `delete` | 4.0 µs | tombstone + staged remove |

### Server, RESP over loopback (1 reactor thread)

| Metric | ssd-kv (durable, SSD) | Redis 7 (no persistence, RAM) |
| --- | --- | --- |
| GET p50 / p99 | **48 µs / 98 µs** | 67 µs / 111 µs |
| SET throughput, c=50 | 79 K ops/s | 95 K ops/s |
| GET throughput, c=50 | 88 K ops/s | 90 K ops/s |
| GET p99.9 under 4 concurrent writers | 717 µs | 571 µs |
| GET p50, cold (caches dropped per op) | **198 µs** | n/a (RAM only) |

ssd-kv serves point reads *faster than in-memory Redis* once the working
set is staged, while being durable and bounded by SSD capacity instead of
RAM.

### vs Aerospike

A live head-to-head wasn't possible in this sandbox (package repo and
Docker registry blocked), so the comparison uses Aerospike's published
SSD numbers: their ACT certification targets **95 % of reads < 1 ms** on
NVMe, with typical SSD-namespace read medians around 0.5–1 ms under load.
ssd-kv on a slower virtualised disk measures **p50 198 µs / p95 415 µs
cold**, p99 < 1 ms warm under mixed load — comfortably inside Aerospike's
published envelope, with the caveat that this is a single-node loopback
measurement, not a clustered production benchmark.

Where the comparison is structural rather than measured: like Aerospike,
ssd-kv keeps a memory-resident index and values on SSD with a bounded
number of device reads per op. Unlike Aerospike's DRAM primary index
(64 B per record in RAM), the SIndex design keeps the *index itself* on
SSD behind the staging cache — RAM holds only the 65 536-entry segment
table plus the WSBCache, so index RAM is O(hot set), not O(total keys).

## Build & run

```bash
cargo build --release

# Default: 127.0.0.1:6379, data in ./data
./target/release/ssd-kv --bind 127.0.0.1:7379 --data-dir /var/lib/ssd-kv

redis-cli -p 7379 SET hello world
redis-cli -p 7379 GET hello
```

### Server flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `--data-dir <path>` | `./data` | Segment files, value log, checkpoint |
| `--bind <addr>` | `127.0.0.1:6379` | RESP listen address |
| `--reactor-threads <n>` | `1` | io_uring reactors sharing the port via SO_REUSEPORT |
| `--max-connections <n>` | `10000` | Concurrent client cap |
| `--num-dbs <1..16>` | `16` | Logical DBs (`SELECT 0..N-1`) |
| `--read-buffer-kb` / `--write-buffer-kb` | `64` | Per-connection buffer sizes |
| `--log-level <lvl>` | `info` | `trace` … `error` |

## Supported commands

Strings/generic: `GET`, `SET` (`EX/PX/EXAT/PXAT/NX/XX/KEEPTTL`), `SETNX`,
`SETEX`, `PSETEX`, `GETSET`, `APPEND`, `STRLEN`, `INCR/DECR/INCRBY/DECRBY`,
`MGET`, `MSET`, `DEL`, `EXISTS`, `TYPE`, `RENAME`, `RENAMENX`, `RANDOMKEY`,
`KEYS`, `SCAN` (`MATCH`/`COUNT`), TTL family (`EXPIRE`, `PEXPIRE`, `TTL`,
`PTTL`, `PERSIST`, …), transactions (`MULTI`/`EXEC`/`DISCARD`/`WATCH`),
pub/sub (in-process), and server commands (`PING`, `INFO`, `DBSIZE`,
`SELECT`, `FLUSHDB`, `FLUSHALL`, `CONFIG GET`, `WAIT`, …).

## Repository layout

```
src/
  engine/
    ipage.rs      # 4 KB index-page layout (leaf + internal), per-page CRC
    btree.rs      # per-partition B+ tree over ipages, height ≤ 3
    segment.rs    # per-partition segment file (pread/pwrite, header CRC)
    value_log.rs  # append-only value store + redo journal + replay scan
    wsbcache.rs   # write-staging buffer cache: 16 clock shards, TSS support
    kv_engine.rs  # MLI assembly, epoch guard, TSS thread, recovery
  server/         # RESP reactor (io_uring), command dispatch, multi-DB
  io/             # io_uring network helpers, aligned buffers
  config.rs       # CLI flags
benches/          # criterion benchmarks (warm/cold split, drop_caches)
tests/            # integration tests
```

## Known limitations

- **Torn multi-page flush window.** A crash *mid sync-cycle* can leave a
  segment with a partially-applied page set; recovery replays the value
  log from the checkpoint over that state, which converges for entries
  in the replay window but does not shadow-page the tree itself. CoW
  ipage allocation is the planned fix.
- The value log is never compacted; overwritten/deleted values occupy
  space until a (future) GC pass.
- `KEYS`/`SCAN` materialise matching keys in memory.
- Single-node: the paper's multi-SSD scheduling layer (and any
  clustering) is not part of this branch.
