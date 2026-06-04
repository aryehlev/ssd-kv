# ssd-kv

A string key-value store written in Rust that speaks the Redis RESP protocol,
so any Redis client can talk to it. The hot index lives in RAM; values live
on SSD. Ships with a Go-based Kubernetes operator for clustered deployments.

Implements the SIndex architecture from:
> "The Design of Trillion-scale SSD-based Indexing with Deterministic Latency
> for Cloud Block Storage", ACM TOS 2024 (DOI 10.1145/3789205)

```
client ──RESP──▶ ssd-kv ──▶ PartitionTable (65,536 partitions, top-16 bits of xxh3)
                                │  Each partition:
                                │    BTree (4 KB ipages, height ≤ 4) ──▶ SegmentFile (SSD)
                                │
                                └──▶ ValueLog (shared append-only, raw key+value bytes)
```

Writes append `(key, value)` to the shared ValueLog, then insert a leaf entry
`{key_hash, value_ptr, key_len, value_len}` into the partition's B-Tree and
flush dirty ipages to the segment file on SSD. Reads hash the key with xxh3,
route to one of 65,536 partitions, traverse the B-Tree (at most 4 ipage reads),
verify the key against the ValueLog, and fetch the value — a fixed upper bound
of I/Os regardless of dataset size.

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

### Storage & server
| Flag                        | Default            | Meaning                                                  |
| --------------------------- | ------------------ | -------------------------------------------------------- |
| `--data-dir <path>`         | `./data`           | Directory for segment files and value log                |
| `--bind <addr>`             | `127.0.0.1:7777`   | Listen address (Redis RESP protocol)                     |
| `--max-connections <n>`     | `10000`            | Maximum concurrent client connections                    |
| `--reactor-threads <n>`     | `1`                | RESP reactor threads; each shares the port via SO_REUSEPORT |
| `--read-buffer-kb <n>`      | `64`               | Per-connection read buffer size in KB                    |
| `--write-buffer-kb <n>`     | `64`               | Per-connection write buffer size in KB                   |
| `--num-dbs <1..16>`         | `16`               | Number of logical databases (`SELECT 0..N-1`)            |
| `--log-level <lvl>`         | `info`             | `trace`, `debug`, `info`, `warn`, `error`                |
| `--verbose`                 | off                | Shortcut for `--log-level debug`                         |

### Cluster (requires `--cluster-mode`)
| Flag                          | Default  | Meaning                                                          |
| ----------------------------- | -------- | ---------------------------------------------------------------- |
| `--cluster-mode`              | off      | Run as a cluster member; enables the ready protocol              |
| `--node-id <n>`               | —        | This node's ordinal (required in cluster mode)                   |
| `--total-nodes <n>`           | —        | Cluster size (required in cluster mode)                          |
| `--cluster-port <p>`          | `7780`   | Inter-node port for the ready-protocol handshake                 |
| `--cluster-peers <list>`      | —        | `host:port,host:port,...` of peer cluster-port addresses         |
| `--replication-factor <n>`    | `2`      | Copies per key including primary (plumbing only — replication not yet implemented) |
| `--health-check-interval-ms`  | `1000`   | Heartbeat interval in ms (plumbing only)                         |
| `--health-check-threshold`    | `3`      | Missed heartbeats before a node is marked dead (plumbing only)   |
| `--replica-read`              | off      | Allow reads from replica nodes (plumbing only)                   |

---

## Supported commands

All commands speak RESP-2; clients pipeline freely.

**Strings / generic**
`GET`, `SET` (`EX`, `PX`, `EXAT`, `PXAT`, `NX`, `XX`, `KEEPTTL`, `GET`),
`SETNX`, `SETEX`, `PSETEX`, `GETSET`, `APPEND`, `STRLEN`,
`INCR`, `DECR`, `INCRBY`, `DECRBY`,
`MGET`, `MSET`, `DEL`, `EXISTS`, `TYPE`,
`RENAME`, `RENAMENX`, `RANDOMKEY`, `KEYS`, `SCAN`.

**TTL (stubs)**
`TTL`, `PTTL` always return `-1` (TTL not implemented).
`EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PERSIST` always return `0`.

**Connection / server**
`PING`, `QUIT`, `RESET`, `DBSIZE`, `SELECT`,
`INFO`, `CONFIG GET`, `CONFIG SET`, `CONFIG RESETSTAT`,
`BGSAVE`, `BGREWRITEAOF`, `SAVE`, `LASTSAVE`,
`COMMAND`, `CLIENT`, `OBJECT`, `WAIT`, `REPLICAOF`, `SLAVEOF`, `LOLWUT`.

**Not yet implemented (return an error)**
`CLUSTER *`, `SUBSCRIBE` / `UNSUBSCRIBE` / `PSUBSCRIBE` / `PUNSUBSCRIBE` / `PUBLISH`,
`MULTI` / `EXEC` / `DISCARD` / `WATCH` / `UNWATCH`.

---

## Storage model

- **Index in RAM.** 65,536 partitions, keyed by the top 16 bits of `xxh3(key)`.
  Each partition holds one B+ tree whose nodes are 4 KB ipages stored in a
  segment file (`.seg`) on SSD. Each leaf entry carries `{key_hash, value_ptr,
  key_len, value_len, flags}`.

- **ValueLog.** A single shared append-only file stores the raw `(key, value)`
  bytes for all partitions. Each record has a 16-byte header (magic, key_len,
  value_len, flags, CRC32). The B-Tree leaf stores only the byte offset into
  this file, keeping ipages small.

- **Write path.** `xxh3(key)` selects a partition → append `(key, value)` to
  the ValueLog (returns `value_ptr`) → insert or update `LeafEntry` in the
  B-Tree → flush dirty ipages to the segment file.

- **Read path.** `xxh3(key)` → partition lookup → B-Tree traversal (≤ 4 ipage
  reads from SSD) → verify key by reading the key bytes from the ValueLog →
  read value bytes from the ValueLog. At most 5 SSD reads for any key,
  regardless of dataset size (the B-Tree height is capped at 4).

- **Persistence.** Segment files and the ValueLog are written synchronously.
  On restart the server reopens all existing `.seg` files and rebuilds the
  in-memory index.

- **Cluster.** When `--cluster-mode` is set, nodes perform the
  **ready protocol**: each node listens on `--cluster-port`, dials every
  peer, and exchanges a `READY <node_id> <total_nodes>` handshake. The server
  starts accepting client connections immediately; `wait_for_quorum` logs
  "cluster quorum reached" once a majority of peers have completed the
  handshake. Full slot-based routing, replication, and health-checking are
  planned.

---

## Resource usage

The design point is **index in RAM, values on SSD**, so RAM scales with
*number of keys* and SSD scales with *total value bytes*. That is the
trade-off vs an all-in-memory store like Redis (which keeps values in RAM too).

### Benchmarks

#### Read latency: cold SSD vs warm page cache vs in-memory

Measurements on a single VM (ext4/virtio-blk), 1 client, 1 reactor thread.
"Cold" = OS page cache dropped via `/proc/sys/vm/drop_caches` before **each**
individual GET so every B-Tree traversal and ValueLog read must reach the SSD.
"Warm" = page cache already populated (DRAM-speed reads).

| System | Condition | p50 | p95 | p99 |
|---|---|---|---|---|
| **ssd-kv** | cold (SSD read) | **353 µs** | 631 µs | 635 µs |
| **ssd-kv** | warm (OS page cache) | 61 µs | 138 µs | 221 µs |
| Redis 7.0 | in-memory, no persistence | 54 µs | 90 µs | 148 µs |
| Aerospike CE ¹ | SSD (NVMe, published) | ~500 µs | ~1.5 ms | ~2 ms |

¹ Aerospike Community Edition numbers are from Aerospike's published
benchmark reports on a single NVMe SSD node; actual results vary by
hardware and record size.

Key takeaways:
- **Cold ssd-kv (353 µs p50) is competitive with Aerospike on NVMe SSD.**
  This VM uses a virtualised block device, so bare-metal NVMe would be faster.
- **Warm ssd-kv ≈ Redis** once the OS page cache is hot, because both end up
  serving reads from DRAM. The difference is that ssd-kv's working set
  is not bounded by available RAM.

#### Criterion micro-benchmarks (engine API, no network)

Run with `cargo bench`:

| Benchmark | Result |
|---|---|
| `latency/warm_get` | 3.2 µs |
| `latency/cold_get` | 281 µs |
| `latency/put` | 743 µs |
| `latency/delete` | 14 µs |
| `get_warm/1000` | ~268 K ops/s |
| `get_cold/1000` | ~15 K ops/s |

The `get_cold` benchmarks use `iter_batched(PerIteration)` to drop the OS
page cache before every single timed iteration, so the numbers reflect
genuine SSD reads, not cached ones.

#### Write throughput

SET throughput is currently bounded by synchronous segment-file writes
(one `pwrite` per partition per key). Batched/async flushing across concurrent
clients is a planned improvement.

| System | SET ops/s | avg latency |
|---|---|---|
| Redis 7.0 (`appendonly no`) | ~87 K | 0.31 ms |
| **ssd-kv** (1 reactor thread) | ~1.3 K | 7.4 ms |
| Redis 7.0 (`appendfsync=always`) | ~17 K | ~3 ms |

---

## Why it's fast

- **Deterministic latency.** The B-Tree height is capped at 4, so every GET
  touches at most 4 ipages on SSD and one ValueLog read — a hard upper bound
  independent of dataset size.
- **65,536-way partitioning.** `xxh3(key)` routes each operation to one of
  65,536 independent partitions. Concurrent requests on different key prefixes
  never contend on the same lock.
- **No copies on the hot path.** The RESP parser reuses a per-connection
  buffer; responses are built into a contiguous output buffer and flushed
  once per pipeline batch.
- **Pipelining.** Up to 128 commands per connection are processed before the
  socket is flushed, amortising syscalls.
- **io_uring async I/O.** The reactor uses io_uring for TCP accept/recv/send;
  the registered-buffer path avoids extra copies for large responses.
- **mimalloc.** Replaces the system allocator; cheaper small allocations on
  the hot path.
- **CPU pinning delegated to Kubernetes.** Pinning is done by the kubelet's
  static CPU manager (Guaranteed QoS + integer cores, enforced by the
  operator), instead of `sched_setaffinity` calls that would silently no-op
  inside a restricted cpuset.

---

## Repository layout

```
src/
  server/    # RESP server (io_uring reactor), command dispatch, multi-DB
  engine/    # SIndex B-Tree (btree, ipage, segment, value_log, kv_engine)
  io/        # io_uring helpers, aligned buffers
  cluster/   # Ready protocol (cluster mode)
  config.rs  # CLI flags
  main.rs    # Entry point

operator/    # Go controller-runtime operator (SsdkvCluster CRD)
  deploy/    # CRD + RBAC + operator + sample CR

benches/     # Criterion benchmarks
benchmark/   # Comparison scripts
```

---

## Planned features

- TTL / expiry (`EXPIRE`, `PERSIST`, `TTL` currently return stub values)
- Pub/sub (`SUBSCRIBE` / `PUBLISH` return "not supported")
- Transactions (`MULTI` / `EXEC` return "not supported")
- Slot-based key routing and cross-node replication in cluster mode
- ValueLog compaction and space reclamation
- Batched / async segment flushes for higher write throughput
