# ssd-kv

A string key-value store written in Rust that speaks the Redis RESP protocol,
so any Redis client can talk to it. The hot index lives in RAM, values live
on SSD. Ships with a Go-based Kubernetes operator for clustered deployments.

```
                                  ┌─▶ WAL files (durability log: fsync
                                  │    per batch, trimmed once records
                                  │    are in a data file)
client ──RESP──▶ ssd-kv ──┬─▶ in-memory index (RAM)
                          │
                          ├─▶ WriteBuffer: 1 MiB WBlock staging in RAM
                          │    (rotates to a pending queue when full)
                          │
                          └─▶ data files: 1023 × 1 MiB WBlocks per file
                               (≈ 1 GiB, O_DIRECT aligned sequential
                               append; unlinked when fully reclaimed)
```

Writes go to the WAL first for durability, then accumulate in the in-RAM
WriteBuffer, then flush to a data file as one 4 KiB-aligned `pwrite`.
Reads take one index lookup → one positioned read from a data file (or
from the optional read-side cache, or directly out of the WriteBuffer if
the record hasn't flushed yet).

---

## Build & run

```bash
# Build
cargo build --release

# Run standalone (default: 127.0.0.1:7777, data in ./data,
# --wal-mode odirect with fsync per batch)
./target/release/ssd-kv

# Then talk to it with any Redis client
redis-cli -p 7777 SET hello world
redis-cli -p 7777 GET hello
```

Docker:

```bash
docker compose up ssd-kv
```

Kubernetes (Go operator):

```bash
kubectl apply -f operator/deploy/crd.yaml
kubectl apply -f operator/deploy/rbac.yaml
kubectl apply -f operator/deploy/operator.yaml
kubectl apply -f operator/deploy/sample-cluster.yaml
```

The operator forces every `SsdkvCluster` pod into the **Guaranteed** QoS class
(requests == limits) and rejects fractional CPU. Combined with a kubelet
started with `--cpu-manager-policy=static` and a non-zero `--reserved-cpus`,
each pod gets exclusive whole cores from the static CPU manager, so worker
threads aren't preempted by neighbours. Without static CPU manager the cluster
still runs — it just doesn't get pinning.

`spec.resources` therefore takes a single `cpu` (whole cores) and `memory`,
not the usual `requests`/`limits` pair:

```yaml
spec:
  resources:
    cpu: "2"
    memory: 2Gi
```

---

## Server flags

### Storage & durability
| Flag                              | Default          | Meaning                                                       |
| --------------------------------- | ---------------- | ------------------------------------------------------------- |
| `--data-dir <path>`               | `./data`         | Where data files live (repeat flag for multi-device striping) |
| `--wal-mode <mode>`               | `odirect`        | `buffered`, `odirect`, or `odirect-trust-device`. `odirect` bypasses the page cache + fsyncs per batch. `odirect-trust-device` drops the fsync entirely — only safe on PLP-class NVMe. |
| `--reactor-threads <n>`           | CPU-derived      | Number of network threads; also drives WAL shard count (one shard per reactor for independent fsync pipelines). |
| `--wblock-cache-mb <n>`           | `0`              | RAM budget for the read-side WBlock cache (0 = off)           |
| `--wal-trim-interval-secs <s>`    | `30`             | How often old WAL files get unlinked once their records are in a data file |
| `--io-workers <n>`                | `2`              | io_uring workers serving cache-miss reads                     |
| `--wblocks-per-file <1..1023>`    | `1023`           | 1 MB blocks per data file (1023 → ~1 GB files)                |

### Compaction & eviction
| Flag                          | Default           | Meaning                                                       |
| ----------------------------- | ----------------- | ------------------------------------------------------------- |
| `--no-compaction`             | off               | Disable background compaction                                 |
| `--compaction-threshold <f>`  | `0.5`             | Compact blocks below this live-data ratio                     |
| `--compaction-interval <s>`   | `60`              | Compaction check interval                                     |
| `--eviction-policy <p>`       | `noeviction`      | `noeviction`, `allkeys-lru`, `volatile-lru`, `allkeys-random`, `volatile-random`, `volatile-ttl` |
| `--max-entries <n>`           | `0`               | Index size cap (0 = unlimited)                                |
| `--max-data-mb <n>`           | `0`               | Data size cap (0 = unlimited)                                 |
| `--eviction-interval <s>`     | `1`               | Eviction check interval                                       |

### Server & cluster
| Flag                          | Default           | Meaning                                                       |
| ----------------------------- | ----------------- | ------------------------------------------------------------- |
| `--bind <addr>`               | `127.0.0.1:7777`  | Listen address                                                |
| `--num-dbs <1..16>`           | `16`              | Number of logical DBs (`SELECT 0..N-1`)                       |
| `--memory-dbs <list>`         | —                 | DB indices that are memory-only (e.g. `--memory-dbs 1,2`)     |
| `--cluster-mode`              | off               | Run as a cluster member                                       |
| `--node-id <n>`               | —                 | This node's ordinal (required in cluster mode)                |
| `--total-nodes <n>`           | —                 | Cluster size                                                  |
| `--replication-factor <n>`    | `2`               | Copies per key (including primary)                            |
| `--cluster-port <p>`          | `7780`            | Inter-node port                                               |
| `--cluster-peers <list>`      | —                 | `host:port,host:port,...` of peers                            |
| `--health-check-interval-ms`  | `1000`            | Heartbeat interval                                            |
| `--health-check-threshold`    | `3`               | Missed heartbeats before a node is marked dead                |
| `--replica-read`              | off               | Allow `READONLY` reads from replicas                          |
| `--log-level <lvl>`           | `info`            | `trace`, `debug`, `info`, `warn`, `error`                     |
| `--verbose`                   | off               | Shortcut for `--log-level debug`                              |

---

## Supported commands

All commands speak RESP-2; clients pipeline freely.

**Strings / generic**
`GET`, `SET` (`EX`, `PX`, `EXAT`, `PXAT`, `NX`, `XX`, `KEEPTTL`, `GET`),
`SETNX`, `SETEX`, `PSETEX`, `GETDEL`, `GETEX`, `GETRANGE`, `SETRANGE`,
`APPEND`, `STRLEN`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `INCRBYFLOAT`,
`MGET`, `MSET`, `DEL`, `UNLINK` (alias of `DEL`), `EXISTS`, `TYPE`,
`RENAME`, `RENAMENX`, `COPY`, `RANDOMKEY`, `KEYS`, `SCAN`,
`OBJECT ENCODING`.

**TTL**
`TTL`, `PTTL`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `PERSIST`.

**Connection / server**
`PING`, `ECHO`, `TIME`, `DBSIZE`, `SELECT`, `READONLY`, `READWRITE`,
`INFO`, `CONFIG GET`, `CONFIG RESETSTAT`, `WAIT`.

`INFO` exposes `# Server`, `# Memory` (`used_memory`, peak, `maxmemory`,
eviction policy, wblock cache hit ratio), `# Replication` (per-replica
`sent`/`acked`/`failed`/`lag`/`diverged` counters), `# Latencystats`
(p50/p99/p999 per SET/GET/DEL), and `# Wal` (per-shard entries, bytes,
syncs, flushes, current file size, durable position).

`CONFIG GET` returns a read-only view of `maxmemory`, `maxmemory-policy`,
`max-entries`, `appendonly`, `appendfsync`, `save`. `CONFIG SET` returns
an error — every knob is a startup-only CLI flag.

**Transactions**
`MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`. `WATCH` snapshots the
per-key generation counter; `EXEC` aborts and returns nil if any watched
key's generation changed.

**Pub/sub**
`SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBLISH`.
In-process only — no cross-node fan-out in cluster mode.

**Cluster**
`CLUSTER INFO`, `CLUSTER MYID`, `CLUSTER NODES`, `CLUSTER SLOTS`,
`CLUSTER KEYSLOT`. All return live topology data.

---

## Storage model

### Write path (every SET is 2× to SSD, on purpose)

```
client SET k v
   │
   ▼
 ① append to WAL shard      ─ fsync per batch → ACK to client
   │                          (skip fsync in odirect-trust-device)
   ▼
 ② append to WriteBuffer    ─ 1 MiB staging WBlock in RAM
   │        (when full…)
   ▼
 ③ flush to data file       ─ one aligned pwrite
   │
   ▼
 ④ once every record in a   ─ WAL trim thread unlinks the WAL file
   WAL file is durable in     (default: check every 30 s)
   a data file
```

Yes, every byte hits SSD twice: once as a WAL record, once as a
data-file record. Same pattern as Postgres WAL + heap, RocksDB WAL +
SSTable, Redis AOF. The WAL is ephemeral — it's trimmed as soon as its
records are in a data file — so the **steady-state disk footprint is
~1× the data, not 2×.** Peak footprint is ~1× data + a few WAL files
worth of not-yet-trimmed log.

Why not skip the WAL and ACK only after the WriteBuffer flushes? Then
small writes have to wait for ~1 MiB of other writes to accumulate
before they're safe, or we fsync on every record (brutal write-amp).
The WAL lets us ACK after a tiny append-and-fsync while the WriteBuffer
keeps batching in the background — same tradeoff every durable KV
makes.

### Components

- **Index in RAM.** 256 shards, each a `HashMap<u64, Vec<IndexEntry>>`
  keyed by `xxh3(key)` behind its own `parking_lot::RwLock`. Each entry
  holds the key (inline if ≤ 23 bytes, else heap), the on-disk location
  (file id, block, offset), value length, generation, and a flag byte.
  TTL lives in the on-disk record header, not in the in-memory entry.
- **Write-ahead log.** Every write first goes to a WAL shard (one per
  reactor thread) with an xxh3-framed record. `odirect` mode fsyncs per
  batch; `odirect-trust-device` drops the fsync for PLP hardware. Once
  all records in a WAL file are durable in a data file, the WAL file is
  unlinked by the trim thread.
- **WriteBuffer (staging).** After WAL commit, records append into a
  1 MiB in-RAM WBlock. When it fills, it rotates into a pending queue
  (bounded — returns `OOM command not allowed` under sustained overload
  so clients can back off rather than the server getting OOM-killed)
  and a background flush writes it to a data file as one aligned
  `pwrite`.
- **Data files.** Each file is 1023 × 1 MiB WBlocks ≈ 1 GiB. Writes are
  sequential-append; reads are a single positioned read. Each WBlock
  carries an xxh3 integrity footer checked on recovery — sufficient to
  distinguish clean shutdown from torn flush on top of the per-record
  CRC.
- **Read path.** Index → WriteBuffer (unflushed records) → optional
  WBlock cache (`--wblock-cache-mb`) → positioned read from the data
  file. Cache misses can be served by io_uring workers.
- **Recovery.** Scan data files to rebuild the index, then replay any
  WAL entries past the highest durable generation. Records that fail
  per-record CRC are skipped; each WBlock's footer match/miss/corrupt
  is surfaced in recovery stats.
- **Compaction.** A background thread copies live records out of blocks
  whose live-data ratio falls below `--compaction-threshold`, marks the
  source blocks reclaimed, and **unlinks the whole 1 GiB data file**
  once every block in it is reclaimed and the file is sealed. Read-side
  cache entries are invalidated before unlink.
- **Eviction.** Optional LRU / TTL / random sampler runs in the
  background once any of `--max-entries`, `--max-data-mb`, or a
  non-`noeviction` policy is set.
- **Cluster.** Keys are mapped to one of 16,384 slots, slots are split
  across `--total-nodes` nodes, and `--replication-factor` copies per
  key are stored. Replication is async — the primary doesn't block on
  replica ack — but per-replica `sent`/`acked`/`failed`/`lag` counters
  surface divergence in `INFO`. Heartbeats fire every
  `--health-check-interval-ms`.

---

## Resource usage

The design point is **index in RAM, values on SSD**, so RAM scales with
*number of keys* and SSD scales with *total value bytes*. That is the
trade-off vs an all-in-memory store like Redis (which keeps values in
RAM too).

### Benchmarks

Single-node, `-c 16 -P <pipeline>`, one VM, Redis 7 with `--save ''`,
three durability tiers compared apples-to-apples:

**Tier 3: strong durability (every ACK fsynced to disk).**

| Workload        | Redis `appendfsync=always` | **ssd-kv `odirect`** (default) | ssd-kv `odirect-trust-device` (PLP) |
| --------------- | -------------------------- | ------------------------------ | ----------------------------------- |
| d=100 P=1       | 16.1k                      | 14.9k                          | **24.5k**                           |
| d=100 P=4       | 42.6k                      | **52.1k**                      | **96.2k**                           |
| d=100 P=16      | 132.5k                     | **192.3k** (+45%)              | **344.8k** (+160%)                  |
| d=4096 P=1      | 8.6k                       | **10.3k**                      | **18.5k**                           |
| d=4096 P=4      | 19.7k                      | **30.3k** (+54%)               | **44.1k**                           |
| d=4096 P=16     | 22.8k                      | **52.7k** (+131%)              | **45.6k**                           |

**Tier 2: weak durability (Redis `appendfsync=everysec`, ≤ 1 s loss
window on crash).** This is the common "production Redis" config and
sits above both of our per-ACK-durable modes because it doesn't fsync
on the critical path: 82k (P=1) → 476k (P=16) at 100 B, 55k → 114k at
4 KiB.

**Tier 1: no durability (Redis `appendonly no`, pure in-RAM).** The
theoretical ceiling for anything disk-backed: 909k at d=100 P=16.

All ssd-kv numbers are from the post-fix build: per-WBlock xxh3
integrity footer on the flush path, per-replica divergence counters
on the replication path, compaction actually unlinks reclaimed files.

---

## Durability tradeoffs vs similar systems

Disks have a volatile write cache. A `write()` returning successfully
doesn't mean the bytes are on flash — they may still be in the drive's
DRAM, which vanishes on power-cut. `fsync` sends a FLUSH CACHE command
that forces the drive to drain that DRAM. Every durable KV picks where
on the safety/throughput curve to sit:

|                                          | fsync on hot path | Requires PLP hardware | Assumes replication | Safe on a laptop / consumer SSD | Single-node durable on power-cut |
| ---------------------------------------- | :---------------: | :-------------------: | :-----------------: | :-----------------------------: | :------------------------------: |
| **ssd-kv `odirect`** (default)           | ✅                | ❌                    | ❌                  | ✅                              | ✅                               |
| ssd-kv `odirect-trust-device`            | ❌                | ✅                    | ❌                  | ❌                              | ❌                               |
| Redis `appendfsync=always`               | ✅                | ❌                    | ❌                  | ✅                              | ✅                               |
| Redis `appendfsync=everysec`             | every 1 s         | ❌                    | ❌                  | ✅ (≤ 1 s loss)                 | ⚠️ (≤ 1 s loss)                  |
| Aerospike `commit-to-device=false` (def) | ❌                | ✅                    | ✅ (RF≥2)           | ❌                              | ❌                               |
| Aerospike `commit-to-device=true`        | ✅                | ❌                    | ❌                  | ✅                              | ✅                               |

Two observations:

- **Aerospike's default is ssd-kv's opt-in (`odirect-trust-device`).**
  Both skip fsync and rely on PLP hardware. The difference is
  positioning: Aerospike is sold as an enterprise product, ships as
  "no fsync" out of the box, and loudly requires PLP drives + RF ≥ 2.
  ssd-kv is open-source, so we assume people will run it on whatever,
  and default to fsync-on. If you know you have PLP NVMe, flip
  `--wal-mode odirect-trust-device` and take the win (same as setting
  Aerospike defaults).
- **ssd-kv `odirect` and Redis `appendfsync=always` are in the same
  class.** Same fsync-per-batch story, same single-node durability, no
  hardware prerequisites. The benchmark table above compares them
  directly; `odirect` runs 45-131% ahead at pipeline ≥ 4.

If you want Redis-`everysec` semantics (accept ≤ 1 s data loss for
throughput), the analogous ssd-kv mode is currently **not** exposed —
our `buffered` mode still fsyncs per batch, it just goes through the
page cache. A periodic-fsync mode would be a reasonable addition.

---

## Why it's fast

- **O_DIRECT WAL with batched group commit.** The WAL path bypasses the
  kernel page cache and fsyncs per *batch* rather than per *write*, so
  many concurrent clients amortise a single sync. An opt-in
  `odirect-trust-device` mode skips the fsync entirely — safe only on
  PLP-class NVMe, but lands another 40-290% throughput in exchange.
- **Parallel WAL shards.** One WAL shard per reactor thread → N
  independent fsync pipelines running concurrently instead of serialising
  through a single log. Each shard keeps its own durable-position cursor;
  there's no cross-shard ordering dependency.
- **Sequential writes, random reads.** Writes hit the WriteBuffer, then
  go to disk as one big aligned append. SSDs love this pattern. Reads
  are one positioned read per hit.
- **One hash lookup per GET.** The index is 256 sharded `HashMap`s keyed
  by `xxh3`. No B-tree, no LSM read amplification.
- **No copies on the hot path.** The RESP parser reuses a per-connection
  buffer; responses are built into a contiguous output buffer and flushed
  once per pipeline batch.
- **Pipelining.** Up to 128 commands per connection are processed before
  the socket is flushed, amortising syscalls.
- **io_uring async reader** for cache-miss reads so the reactor doesn't
  block on slow SSD reads.
- **mimalloc.** Replaces the system allocator; cheaper small allocations.
- **CPU pinning delegated to Kubernetes.** Pinning is done by the
  kubelet's static CPU manager (Guaranteed QoS + integer cores, enforced
  by the operator), instead of `sched_setaffinity` calls that would
  silently no-op inside a restricted cpuset.
- **Append-only log + real background compaction.** Deletes and
  overwrites are free at write time; reclamation happens off the hot
  path, and fully-reclaimed 1 GiB files are actually unlinked so disk
  usage doesn't grow forever.
- **Tight RESP fast path.** Common commands (`GET`/`SET`/`PING`/`DEL`)
  match early in the dispatch and skip most argument validation.

---

## Repository layout

```
src/
  server/        # RESP server, command dispatch, multi-DB
  engine/        # In-memory index, recovery
  storage/       # WAL, write buffer, file manager, compaction, eviction,
                 # wblock cache
  cluster/       # Topology, replication, routing, health
  io/            # io_uring helpers, aligned buffers
  perf/          # Tuning helpers (CPU pinning, NUMA, prefetch, ...)
  config.rs      # CLI flags
  main.rs

operator/        # Go controller-runtime operator (SsdkvCluster CRD)
  deploy/        # CRD + RBAC + operator + sample CR

benches/         # Criterion benchmarks
benchmark/       # Comparison scripts
tests/           # Integration tests
```
