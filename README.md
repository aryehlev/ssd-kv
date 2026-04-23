# ssd-kv

A Redis-compatible key-value store written in Rust. The hot index lives in RAM,
values live on SSD. Speaks the standard RESP protocol, so any Redis client
works. Ships with a Go-based Kubernetes operator for clustered deployments.

```
client ──RESP──▶ ssd-kv ──┬─▶ in-memory index (RAM)
                          └─▶ append-only log files (SSD)
```

---

## Build & run

```bash
# Build
cargo build --release

# Run standalone (default: 127.0.0.1:7777, data in ./data)
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

| Flag                          | Default           | Meaning                                                       |
| ----------------------------- | ----------------- | ------------------------------------------------------------- |
| `--data-dir <path>`           | `./data`          | Where log files live                                          |
| `--bind <addr>`               | `127.0.0.1:7777`  | Listen address                                                |
| `--workers <n>`               | `0` (auto)        | Worker threads (0 = `available_parallelism`)                  |
| `--max-connections <n>`       | `10000`           | Max concurrent client connections                             |
| `--num-dbs <1..16>`           | `16`              | Number of logical DBs (`SELECT 0..N-1`)                       |
| `--memory-dbs <list>`         | —                 | DB indices that are memory-only (e.g. `--memory-dbs 1,2`)     |
| `--no-compaction`             | off               | Disable background compaction                                 |
| `--compaction-threshold <f>`  | `0.5`             | Compact files below this live-data ratio                      |
| `--compaction-interval <s>`   | `60`              | Compaction check interval                                     |
| `--read-buffer-kb <n>`        | `64`              | Per-connection read buffer                                    |
| `--write-buffer-kb <n>`       | `64`              | Per-connection write buffer                                   |
| `--wblocks-per-file <1..1023>`| `1023`            | 1 MB blocks per data file (1023 → ~1 GB files)                |
| `--eviction-policy <p>`       | `noeviction`      | `noeviction`, `allkeys-lru`, `volatile-lru`, `allkeys-random`, `volatile-random`, `volatile-ttl` |
| `--max-entries <n>`           | `0`               | Index size cap (0 = unlimited)                                |
| `--max-data-mb <n>`           | `0`               | Data size cap (0 = unlimited)                                 |
| `--eviction-interval <s>`     | `1`               | Eviction check interval                                       |
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
`MGET`, `MSET`, `DEL`, `UNLINK`, `EXISTS`, `TYPE`, `RENAME`, `RENAMENX`,
`COPY`, `RANDOMKEY`, `OBJECT`, `KEYS`, `SCAN`.

**TTL**
`TTL`, `PTTL`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `PERSIST`.

**Server / connection**
`PING`, `ECHO`, `TIME`, `INFO`, `COMMAND`, `DBSIZE`, `FLUSHDB`, `FLUSHALL`,
`SELECT`, `READONLY`, `READWRITE`, `WAIT`.

**Transactions**
`MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`.

**Pub/Sub**
`SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBLISH`.

**Cluster**
`CLUSTER INFO`, `CLUSTER MYID`, `CLUSTER NODES`, `CLUSTER SLOTS`,
`CLUSTER KEYSLOT`.

Not supported: `DUMP`, `RESTORE`, `DEBUG`, Lua scripting, streams, sorted
sets, hashes, lists, geo, bitmap, HyperLogLog. The store is a string KV; it
does not implement Redis's collection types.

---

## Storage model

- **Index in RAM.** `DashMap` from `xxh3(key)` to a fixed-size entry
  (file id, offset, length, TTL, flags). One index entry is small (tens of
  bytes), so 100M keys fit in a few GB.
- **Values on SSD.** Values are appended to ~1 GB log files in the data
  directory. Writes are sequential; reads are a single positioned read.
- **WAL + recovery.** Records are framed and CRC'd. On startup the index is
  rebuilt by scanning the log; expired and tombstoned records are skipped.
- **Compaction.** A background thread copies live records out of files whose
  live ratio falls below `--compaction-threshold` and frees the source file.
- **Eviction.** Optional LRU/TTL/random sampler runs in the background once
  any of `--max-entries`, `--max-data-mb`, or a non-`noeviction` policy is set.
- **Cluster.** 16,384 slots are split across `--total-nodes` nodes;
  `--replication-factor` copies per key. Heartbeats every
  `--health-check-interval-ms`.

---

## Resource usage

These are the design targets and what falls out of the architecture; treat
them as approximate.

| Workload                        | ssd-kv                          | Redis                           | Aerospike (CE)                   |
| ------------------------------- | ------------------------------- | ------------------------------- | -------------------------------- |
| RAM per 1 M small keys          | ~50–100 MB (index only)         | ~1–2 GB (key + value + meta)    | ~64 MB (index only)              |
| Where values live               | SSD (log-structured)            | RAM (or RDB/AOF on disk)        | SSD (block-aligned)              |
| Disk write pattern              | Sequential append               | Snapshot / append-only file     | Sequential append                |
| Cold-start                      | Scan log → rebuild index        | Load RDB / replay AOF           | Scan device → rebuild index      |
| Working set bound by            | RAM (index) + SSD (values)      | RAM (everything)                | RAM (index) + SSD (values)       |
| Idle CPU                        | Low (event-driven RESP loop)    | Low                             | Higher (background subsystems)   |

Typical sizing rule for ssd-kv: **plan for index in RAM, values on SSD**.
For 100 M keys with 1 KB values you need a few GB of RAM and ~100 GB of SSD,
versus ~120 GB of RAM for an all-in-memory store.

---

## Why it's fast

- **Sequential writes, random reads.** Writes hit a write buffer, then go to
  disk as one big append. SSDs love this pattern.
- **One hash lookup per GET.** The index is a sharded `DashMap` keyed by
  `xxh3`. No B-tree, no LSM read amplification.
- **No copies on the hot path.** The RESP parser reuses a per-connection
  buffer; responses are built into a contiguous output buffer and flushed
  once per pipeline batch.
- **Pipelining.** Up to 128 commands per connection are processed before the
  socket is flushed, amortizing syscalls.
- **mimalloc.** Replaces the system allocator; cheaper small allocations.
- **Worker auto-tune + cgroup-honest CPU pinning.** Worker count defaults to
  `available_parallelism`. Pinning is delegated to the kubelet's static CPU
  manager (Guaranteed QoS + integer cores, enforced by the operator), instead
  of `sched_setaffinity` calls that would silently no-op inside a restricted
  cpuset.
- **Append-only log + background compaction.** Deletes and overwrites are
  free at write time; reclamation happens off the hot path.
- **Tight RESP fast path.** Common commands (`GET`/`SET`/`PING`/`DEL`) match
  early in the dispatch and skip most argument validation.

---

## Repository layout

```
src/
  server/        # RESP server, command dispatch, multi-DB
  engine/        # In-memory index, recovery
  storage/       # WAL, write buffer, file manager, compaction, eviction
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
