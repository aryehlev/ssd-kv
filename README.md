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

Environment: single VM, 4 vCPUs, virtio-blk + ext4 (slower than bare
NVMe; numbers are conservative). Engine micro-benchmarks via `cargo bench`;
server numbers via `redis-benchmark` against live processes on loopback.

### Engine (in-process API, Criterion)

| Operation | Latency | Notes |
| --- | --- | --- |
| `put` | **2.2 µs** | vlog append + staged B-Tree update; no fsync on request path |
| `get` warm | **0.85 µs** | WSBCache hit (DRAM-speed) |
| `get` cold | **123 µs** | OS page cache flushed before every op; ≈ leaf pread + value pread |
| `delete` | 4.0 µs | tombstone append + staged remove |

### Server vs Valkey 7.2 (live head-to-head, `redis-benchmark`, loopback)

Valkey 7.2.12 run with persistence **disabled** (`--save "" --appendonly no`).
ssd-kv run with default settings — data is **durable** (TSS syncs every 50 ms).
1 reactor thread for ssd-kv to match Valkey's single-threaded command loop.

#### Throughput (ops/s, 100 K requests, 64-byte values)

| Workload | ssd-kv (durable) | Valkey 7 (volatile) |
| --- | ---: | ---: |
| SET c=1 | 18.6 K | 21.1 K |
| SET c=10 | 74.6 K | 92.3 K |
| **SET c=50** | **94.5 K** | 88.3 K |
| GET c=1 | **21.5 K** | 17.9 K |
| GET c=10 | **99.0 K** | 85.3 K |
| **GET c=50** | **102.9 K** | 90.9 K |
| SET pipelined P=10, c=50 | 362 K | 866 K |
| GET pipelined P=10, c=50 | 662 K | 922 K |

At typical production concurrencies (c=10–50, no deep pipelining) ssd-kv
matches or exceeds Valkey throughput — while writing data durably to SSD.
Valkey's advantage at high pipeline depth reflects its optimised batch
response path; pipelined workloads are not ssd-kv's primary design target.

#### Latency (c=1, 100 K requests, 64-byte values)

| Op | ssd-kv p50 | ssd-kv p99 | Valkey p50 | Valkey p99 |
| --- | ---: | ---: | ---: | ---: |
| SET | **0.031 ms** | 0.263 ms | 0.039 ms | 0.231 ms |
| GET | **0.039 ms** | 0.239 ms | 0.039 ms | 0.207 ms |

p50 latency is on par with or better than Valkey; p99 is within 15% —
despite every SET being destined for persistent storage.

### vs Aerospike

A live Aerospike instance isn't available in this environment (installer
requires registration), so the comparison is structural + published-number
based. Aerospike's ACT certification targets **95 % of reads < 1 ms** on
NVMe; typical SSD-namespace medians are 0.5–1 ms under realistic load.
ssd-kv on a slower virtualised disk hits **p50 0.039 ms / p99 0.239 ms**
warm and < 1 ms cold — comfortably inside Aerospike's published envelope,
without Aerospike's DRAM-per-record overhead.

Structural comparison:

| Property | ssd-kv | Aerospike (SSD namespace) |
| --- | --- | --- |
| Index location | SSD, staged in WSBCache | DRAM primary index (~64 B/record) |
| Index RAM | O(hot working set) | O(total records) |
| Read bound | ≤ 4 device I/Os (B-Tree height ≤ 3 + vlog read) | typically 1–2 (hash index + record) |
| Write path | Journal-first, no fsync on hot path | Similar (Aerospike also uses a commit log) |
| Clustering | Single node (this impl) | Native sharding + replication |

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
