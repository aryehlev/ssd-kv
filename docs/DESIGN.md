# KVSSD: implementation specification (single source of truth)

Status: APPROVED FOR IMPLEMENTATION (architecture freeze v1.3, 2026-11-01; v1.3 = coordinator peer-change sync step in §23.3; v1.2 = expired-newest-copy recovery fix in §13.3, §16.2, §18.3; v1.1 = design-review
revision: placement order, in-place/bounded-copy updates, fixed snapshot regions, substrate checks, page-reference SCAN,
position barriers, unilateral failover, coordinator peer lists outside the voted state, merge sync).
Audience: the implementation team (parallel workstreams), test/QA, and operations.
Scope: data node (`kvd`), coordinator service (`kvc`), operator CLI (`kvadm`), shared crates. Milestones M1-M3.

Normative words: MUST / MUST NOT / SHOULD / MAY. Anything marked **(future)** is out of scope for M1-M3 and MUST NOT be
implemented (not even feature-flagged) unless this document is amended. Section numbers are referenced by code comments
(`// DESIGN §13.4`) and by CI checks (`LC-xx` ids, §3).

Reference command semantics: Redis 7.0.15 as captured by the black-box harness in `tests/compat/` (differential, scenarios,
persistence). Where this spec says "Redis semantics", the harness is the tie-breaker. Product wording: "Redis-protocol
compatible"; never "Redis" as a product name.

---------------------------------------------------------------------------------------------------------------------

## 0. Reading guide

| If you implement | Read first |
|---|---|
| anything | §1, §2, §3, §5, §24 (crate layout + interfaces), §25 (tests) |
| `kv-resp`, `kv-cmd`, command handlers | §7, §8 |
| `kv-index` | §9 |
| `kv-storage` | §10-§18 |
| `kv-node` runtime / net / dispatch | §6, §7, §19-§22 |
| `kv-repl`, node cluster agent | §23.6-§23.13 |
| `kv-coord`, `kv-topo`, `kvadm` | §23.1-§23.5, §23.11-§23.13 |
| benchmarks | §27 |

---------------------------------------------------------------------------------------------------------------------

## 1. Goals, non-goals, targets

### 1.1 Goals
1. A sharded, replicated key-value database speaking RESP2 and RESP3 that Redis-Cluster client libraries (redis-py, Jedis,
   Lettuce, go-redis, ioredis) can use unmodified (with the documented settings, §23.5).
2. Index in RAM, keys and values on SSD/NVMe (raw block devices or pre-allocated, zero-filled files on XFS/ext4, always
   `O_DIRECT`). Every other storage substrate is refused at open (LC-34).
3. Beat Aerospike per node on: index RAM per key, usable SSD fraction, restart time after clean shutdown, durability of
   acknowledged writes and deletes, tail latency under mixed load, license.
4. Every engineering invariant of §3 (LC-01..LC-39) holds and is checked in CI. The storage format is not log-based,
   anywhere, ever.
5. Correct under kill -9 and under power loss (with the documented durability mode), proven by a deterministic simulator
   with byte-granular torn-write injection and by real-process crash loops.

### 1.2 Non-goals for M1-M3
Lists, sets, sorted sets, streams, pub/sub across nodes, Lua/functions, ACL beyond `requirepass`/one cluster token,
TLS/mTLS, RDB/AOF files, `MIGRATE` command, `SWAPDB`, `MOVE`, `OBJECT`, client-side caching (`CLIENT TRACKING`),
cross-datacenter replication, secondary indexes, queries, aggregation, GPU anything, automatic rebalancing, incremental
(change-bitmap) crash restart (designed in Appendix B, **future**), on-line record consolidation (**future**; when added
it runs on every member while that member keeps serving reads, with no member-state, health or routing change, LC-30).

### 1.3 Quantitative targets (acceptance criteria; measured per §27)

| Metric | Target | Aerospike reference |
|---|---|---|
| Index RAM (segments, directories, hint planes), no TTL | <= 12.5 B/key (design: 11.7-12.1) | 64 B/record/replica |
| Index RAM, all keys with TTL | <= 16 B/key (design: 14.0-14.4) | 64 B |
| Allocator state (`page_free`, extent bitmaps), reported separately (`alloc_state_bytes`) | <= 0.2 B/key (design: ~0.1 at 300 B values) | n/a |
| Device reads per GET (values <= 8 MiB) | exactly 1 on cache miss, 0 on a hit; fingerprint false-positive extra read < 1e-6 typical | 1 |
| SSD-bound throughput, 32 vCPU, 1x gen4 NVMe, 90/10, 300 B values, RF1 | >= 300K ops/s at p99.99 <= 2 ms | ~250K at RF2 |
| Cached GET, non-pipelined | >= 130K ops/s per vCPU | ~110-130K |
| Usable SSD (user bytes / raw) at 300 B values | ~70% (design: 69.7% two-image mode, 77% atomic page-update mode, both with the default 2 x 3% fixed snapshot regions and `max-storage-fill 0.95`; +4.5 points with snapshot regions disabled) | ~42% (<= 50% fill) |
| Space-management write amplification at 75% fill | <= 1.1x (no cleaner exists) | 4x |
| Byte write amplification (host bytes/user bytes), values >= 16 KiB | <= 1.2x (2-16 KiB: <= 2.3x) | 2-4x + full-record rewrite |
| Clean restart, 1B keys | <= 15 s | CE: full cold scan |
| Crash restart, 1B keys x 300 B, PCIe4 6.5 GB/s | <= 60 s (full scan, §18.3) | CE: full cold scan of a 2x larger footprint |
| Durable deletes, no resurrection after kill -9 | always | EE-only tombstones |
| fsync-before-ack | `durability flushed` | `commit-to-device` (EE) |
| Max value | 512 MiB | 8 MiB |
| License | Apache-2.0 OR MIT, no caps | AGPL CE / commercial EE |

Honest limits, accepted and documented: byte write amplification for small-value in-place updates is
`4096 / record_bytes` on 4Kn devices (12x at 330 B) and about `1024 / record_bytes` with sector-granular writes on
512-byte-LBA devices (§13.6). Crash restart is a full scan of allocated AGs (54 s for 352 GB at 6.5 GB/s). Clean restart
uses the snapshot only when it fits its fixed region (§10.10); otherwise it takes the crash path.

---------------------------------------------------------------------------------------------------------------------

## 2. Architect's decision record

### 2.1 Proposal scores (1-10)

| Proposal | Perf | Cost | Correct | Implementable | Verdict |
|---|---|---|---|---|---|
| P1 "Anvil" (thread-per-core slot store, CBM restart) | 9 | 8 | 6 | 5 | Fastest; but default-mode in-place overwrites on non-atomic devices can destroy the last durable version on power loss, fsync-mode shadow relocation moves nearly every update to another page on 512 B-AWU devices (the index changes on every write), EXISTS/TYPE answered from RAM without key verification, and the fuzzy-checkpoint + soft-dirty revalidation recovery is the riskiest code in any proposal. |
| P2 "Tessera" cost-first (Elias-Fano shelves) | 7 | 10 | 6 | 5 | Best RAM (6.3 B/key) but depends on RWF_ATOMIC/AWUPF (absent on the test box and on most files); its shadow-page fallback writes every update out of place; multi-group RENAME/MULTI not crash-atomic; bit-packed shelves + deep refresh are hard to get right. |
| P3 "Tessera" correctness-first (two-image pages) | 7 | 7 | 9 | 7 | Torn-write-proof on any device with no atomic-write assumption, deterministic simulator, crash-atomic multi-key commits. Weaknesses: 20 B/key with TTL, delete markers, cross-shard order-point acks, offsets in the index. |
| Cluster "Keystone-VP" (WSR coordinator, write-all) | 8 | 8 | 8 | 7 | Best coordinator and migration design (no command holding, SEAL). Its `noaddr` self-line breaks Lettuce MOVED-to-coordinator (verified in Lettuce source, §23.5); commit-wait read gate conflicts with LC-28 (no replication-state read gates); catch-up ships the old primary's frames. |
| Cluster "CMSI" (Raft-1, ISR, PSYNC-like) | 8 | 8 | 8 | 7 | Best client-compat analysis (coordinators as slotless CLUSTER NODES entries) and ISR mechanics. Pending-set read gate conflicts with LC-28; exposes shard_epoch / replication offsets to clients (LC-28); handoff holds commands (conflicts with LC-14/LC-15). |

### 2.2 Choice
* **Storage base: P3** (two-image page rule, tiling parser with keyed location-bound checksums, ordered linear-probing
  index segments with stored fingerprints, deterministic simulator, intent table for crash-atomic multi-key writes).
* **Grafted from P1:** 16-bit TTL hint in RAM (as an optional per-segment plane), per-partition FLUSHDB seq cutoffs,
  extents with size classes so values up to 8 MiB are read in one I/O, sector-granular writeback on 512-byte-LBA devices,
  SIEVE page cache, thread-per-core io_uring tick loop with backpressure applied before parsing, `-OOM` at a static fill
  limit, change-bitmap fast restart (Appendix B, future).
* **Grafted from P2:** page-level index addressing (the index stores the page, the record is found inside the page by key
  hash + key compare), so in-page updates never touch the index; FLUSHALL fast reset; page FIFO wait lists; CI greps.
* **New in this spec:** per-(db, slot) index tables (fingerprint collision rate ~1e-7, per-slot iteration for migration
  and replica sync), marker-free deletes by older-copy ordering (§16.2), per-partition replication streams,
  cross-term catch-up by key images instead of forwarding another primary's frames (LC-29), no replication-state read gates
  (LC-28), keyed pseudo-random placement (§11), atomic in-place page updates where the device guarantees them (§13),
  fixed snapshot regions (§10.10), page-reference SCAN (§16.6), merge sync (§23.7).
* **Cluster base: Keystone-VP** coordinator (whole-state replication, A/B persistence, leases, fencing, failover by
  promotion, copy/seal migration) **+ CMSI** client service (coordinators listed as slotless entries in CLUSTER NODES,
  data nodes answer CLUSTER INFO with constant fields), ISR shrink/expand through the coordinator, PSYNC-like handshake
  with REPAIR. Failover is a unilateral static choice (§23.9); coordinator peer lists are operator-set and outside the
  replicated state (§23.3).

---------------------------------------------------------------------------------------------------------------------

## 3. Engineering invariants (normative)

These invariants are normative for the product and for everything this project runs (tests, CI, benchmarks, tools).
Every row has a check id (`LC-xx`). The ids are stable: code comments, CI output and other sections of this document
reference them, and they are grouped by area rather than numbered contiguously. `ci/invariant_lint.sh` (grep/AST
rules), `ci/deny.toml` (cargo-deny), `clippy.toml` (disallowed types/methods), and named tests implement the checks. A
PR that fails any LC check MUST NOT merge. Changing or removing an invariant requires amending this section. Open
engineering questions are collected in §28.

### 3.1 Storage format, index layout and on-disk hygiene
| Id | Invariant | Design choice | Code-verifiable check | Engineering rationale |
|---|---|---|---|---|
| LC-01 | Records live at fixed, allocator-chosen locations and are updated in place; placement order is independent of time and device address | Records live in 4 KiB slotted pages (small) or allocator-chosen extents (large) at fixed addresses. Where a page write is power-fail atomic (atomic mode, §13.1) a non-txn update whose rounded length is unchanged or smaller is rewritten **at its existing offset**; elsewhere (two-image mode) it stays in its page and every superseded copy is zeroed by the page's next build, so at most two copies of a key exist on media (I11). Extent updates prefer the key's twin run (A/B shadow, at most two extents per key, I12, §14.4). Free space anywhere is reused by unrelated keys (best fit). Fresh pages, free AGs, tie-breaks and carve offsets are chosen by keyed pseudo-random draws, and one page build places its records first-fit-decreasing by length, so neither the identifiers nor the resulting device image order records by time or address. There is no append offset, write frontier, head/tail pointer, circular buffer, segment, cleaner or time-ordered record sequence. | `prop_update_keeps_page` (two-image mode: same-size and shrinking updates keep `Entry.page`); `prop_update_keeps_offset` (atomic mode: they keep the record offset); `prop_bounded_copies` (I11, I12); `prop_holes_reused` (pre-filled device, random deletes: insert addresses are not monotonic and >= 90% land in pre-existing holes); `prop_placement_uncorrelated` (bulk load on an EMPTY device, and uniform-TTL churn over >= 3 device wraps: \|Spearman(seq, device byte address)\| < 0.05 over live small records and \|Spearman(seq, first page)\| < 0.05 over extent allocations); `prop_ag_acquire_aperiodic` (same runs: the AG-acquisition sequence has no increasing run longer than 8 and no period p with `seq[i] == seq[i+p]` for >= 10% of i); lint: identifiers `append_offset\|write_head\|frontier\|tail_ptr\|segment_cleaner\|log_head\|wal\|journal\|aof\|next_free\|cursor` banned in `kv-storage/src/{alloc,metastore,image,writer}.rs`, and `append_offset\|write_head\|frontier\|tail_ptr\|segment_cleaner\|log_head\|wal\|journal\|aof` in `crates/kv-storage`, `crates/kv-repl`, `crates/kv-coord`; in `alloc.rs`/`metastore.rs` free pages, AGs, tie-breaks and carve offsets are selected only through `alloc::keyed_pick` (`first_one`, `iter_ones().next()`, `.min()`/`.max()` over page or AG ids banned there). | Bounded copies per key (at most two on media, I11/I12) and no cleaner or compaction: space use is predictable (no free-space headroom reserved for a cleaner, `max-storage-fill 0.95`, §11.6), write amplification has no space-management term (Appendix A) and no foreground write ever waits for reclamation. Same-size and shrinking small-record updates never touch the index. Keyed placement spreads writes and wear over the device and keeps the layout (and therefore fragmentation and restart cost) independent of workload history; no cursor or frontier state has to be persisted or recovered. |
| LC-02 | No second on-disk copy of values or record metadata: no WAL, redo/undo journal, AOF, doublewrite area or append-mode file | No WAL, redo/undo journal, AOF, doublewrite area, hint/manifest files, ZNS zone append, NVMe FDP/streams/directives or write-lifetime hints. Small state (superblock, AG table, partition meta, intent slots, coordinator state, node fence) is rewritten whole into fixed A/B locations; the index snapshot overwrites one of two fixed regions reserved at format (§10.10). The intent table holds only committed txn ids, no values, no record headers. The only appended files are text diagnostics: `kv-node/src/log.rs` and `kv-coord/src/log.rs` may each open one file with `O_APPEND` (named exemption; they never carry user bytes, LC-32). | lint: no `O_APPEND`, `OpenOptions::append`, `RWF_APPEND`, `IORING_OP_*APPEND` outside the two `log.rs` files; no `File::set_len` growth after format; identifiers and constants `zone\|zns\|BLKREPORTZONE\|BLKRESETZONE\|BLKOPENZONE\|BLKCLOSEZONE\|BLKFINISHZONE\|NVME_IOCTL\|nvme_admin\|DIRECTIVE\|dtype\|fdp\|placement_id\|F_SET_RW_HINT\|F_SET_FILE_RW_HINT\|RWH_WRITE_LIFE` banned anywhere (except refusal strings in `devprobe.rs`); test `ab_files_constant_size` (coordinator state, node files unchanged in length after 10^5 commits); test `fd_inventory` (under load `/proc/self/fd` shows only data devices/files, sockets, eventfds, io_uring fds, the coordinator `.ab` files and the one diagnostic log file); lint: no `%`/`wrapping_add`/`rem_euclid` on a slot index in `kv-storage/src/intent.rs` (lowest-index slot selection, no rotation, §10.7); snapshot writes target only `snap_region_page[..]` ranges (unit test over `snapshot.rs`). | Crash consistency comes from the page rules (§13) and fixed A/B blocks, so every user byte is written once by the host (no double write), restart never replays anything, files never grow after format and space accounting is exact. Host-directed placement features (zones, FDP, streams, write hints) would tie the format to device-specific behaviour that the crash model (§17) does not cover. |
| LC-03 | Exactly one storage engine is shipped; no alternate engine, not even disabled | `StorageEngine` is a trait with exactly one implementation (`PageStore`). | lint: exactly one `impl StorageEngine for`; no cargo feature names containing `log`/`lsm`/`wal`. | One on-disk format to test, simulate, recover and support; no dormant code paths or feature-flag combinations in release builds. |
| LC-04 | The index is a hash table, never a tree | Ordered linear probing in segments under an extendible directory (§9). | lint: `BTreeMap\|BTreeSet\|rbtree\|red_black\|RBTree` banned in `crates/kv-index` and in index-holding modules of `kv-storage`. | O(1) expected lookups with ~5.5-entry probes (1-2 cache lines, §9.4), 10 B per entry with no child pointers, and fingerprint-ordered iteration for SCAN and per-slot walks (§16.6) without a tree. |
| LC-05 | No arena, bump or region allocator for the index | Each segment, directory and hint plane is an individual allocation through `seg_alloc()`: `mmap` for >= 64 KiB, else `std::alloc::System` (glibc malloc; glibc calls its per-thread malloc heaps "arenas", which are per-object-free heaps, not region allocation). No pooling, no bump or region allocation. | lint: `bumpalo\|typed_arena\|slab\|arena\|region_alloc` banned in `crates/kv-index`; index allocations go through `seg_alloc()`/`seg_free()` only. | Every segment, directory and hint plane is freed individually, so memory returns to the OS when tables shrink or are dropped (FLUSHDB, slot purge), `memory_bytes` accounting is exact, and no long-lived object can pin a region. |
| LC-06 | Non-cryptographic hashing only for key identity | xxh3-64 (seeded) for fingerprints; CRC16-XMODEM for slots; CRC32C for integrity. std `HashMap` (SipHash) banned; hashbrown+foldhash. | cargo-deny bans `sha1 sha2 sha3 md-5 blake2 blake3 ripemd siphasher`; clippy `disallowed-types = ["std::collections::HashMap","std::collections::HashSet"]`. | Key identity is established by full-key verification (LC-07), so collision resistance is not needed; seeded xxh3, CRC16 and hardware CRC32C cost far less per key than cryptographic digests or SipHash on the hot path; CRC16 is required for Redis Cluster slot compatibility. |
| LC-07 | Full key verified against SSD bytes | Every lookup compares `key_len` + key bytes read from the page/extent before acting; EXISTS/TYPE/TTL also verify. | test `fp_collision_forced` (test-only hook forces equal fingerprints; all commands stay correct). | The index stores no key bytes; verification keeps every command correct under fingerprint collisions (§9.7) and makes EXISTS/TYPE/TTL as exact as GET. |
| LC-08 | One device read of one location per GET; no striping | A record is inside one AG on one device; values <= 8 MiB are one contiguous read. Values > 8 MiB (chunkmap) read chunkmap + chunks sequentially, same device (documented exception). Device chosen by the allocator, never from hash bits. | test `record_single_device`; lint: `PageId::new(dev, ..)` only called in `alloc.rs`/`format.rs`/`recovery.rs`. | One I/O per read gives predictable latency (§1.3: exactly one read on a miss), keeps a record inside one failure domain, and lets the allocator balance fill across devices instead of tying placement to key hashes. |
| LC-09 | Index entries are packed and unaligned; index allocations are not padded to cache-line or page multiples | 10-byte packed entries, unaligned, in segments whose byte length `10 x (cap + 8 + cap/64)` is never a multiple of 64 (true for every cap of the §9.3 sequence); the directory has `2^D + 1` entries (sentinel); allocator state (`page_free`) is one variable-length `Vec<u8>` per partition and is not part of the index. | lint: no `repr(align` in `crates/kv-index`; no `[u8; 4096]` field type in `kv-storage/src/alloc.rs`; unit tests assert `seg_bytes % 64 != 0` for every cap of the sequence and that `dir_bytes(D)` is not a power of two for any D. | Packing is what makes ~12 B/key possible (§9.6); padding entries to 16 B or segments to 64 B or page multiples would cost RAM on every key. The size assertions keep padding from being reintroduced unnoticed. Allocator state is kept outside the index so `index_bytes_per_key` measures the index only. |
| LC-32 | No user bytes in diagnostics or anything else written to disk | Logs (every level), metrics, panic/assert messages, `Debug` output, error replies written to logs and core dumps never contain key, field or value bytes. Diagnostics carry key length, db, slot, partition and at most a 32-bit xxh3 tag through `kv_common::redact::Redacted`; types holding user bytes implement `Debug` through it. Core dumps are off by default (`prctl(PR_SET_DUMPABLE, 0)`, unit `LimitCORE=0`); memory holding user bytes is `MADV_DONTDUMP` (§12, §23.7). `kvd inspect` prints records redacted unless `--show-data` is given. | lint: `tracing`/`log`/`panic!`/`assert*!`/`expect`/`unreachable!` invocations in kv-node, kv-storage, kv-repl, kv-coord with an argument of type `&[u8]`, `Box<[u8]>`, `Vec<u8>` or named `key\|field\|value\|val\|data` not wrapped in `Redacted` fail; `#[derive(Debug)]` banned on types with such fields; test `logs_have_no_user_bytes` (compat scenarios at `loglevel debug` with random marker bytes in every key and value; grep the log file, stderr and `/metrics` output for the markers). | Logs, metrics, panic messages, core dumps and inspect output leave the data path (support bundles, monitoring systems, tickets). Redaction keeps user data out of them, so access control on the data devices remains the only control that matters. |
| LC-33 | Test and CI tooling never record write-by-write traces | Power-cut tests use `tests/crash/faultdev`, a RAM-only NBD server with `SimDevice` semantics (§25.4). No test, CI job, simulator or benchmark persists a per-write record (sector, length, flags, order, payload). Failure artifacts are seeds and final device images only. `dm-log-writes`, `blktrace` captures and per-write sim traces with payloads are not used. | `ci/invariant_lint.sh` fails on `log-writes\|blktrace\|btrace\|dm-log` in `tests/` and `ci/` (the lint script's own pattern line excepted); the `kv-sim` artifact writer takes only `(seed, image)`; lint: no `File::create`/`OpenOptions` in `tests/crash/faultdev` except writing the final image on request. | Failures are reproduced from (seed, final image), which is small and deterministic (§25.3). Per-write traces with payloads are large, slow to replay, add a second write path to the system under test, and can carry production-like data into CI artifacts. |
| LC-34 | Allowed storage substrates only | At open, `kvd` (data devices) and `kvc` (state files) refuse with exit code 1: a file whose `fstatfs` `f_type` is not XFS or ext4; a file on ext4 mounted `data=journal` (`/proc/self/mountinfo`) or carrying the per-file data-journaling flag (`FS_IOC_GETFLAGS`); a block device, or a file system's backing device, that is zoned (`queue/zoned != none`), stacked on a device-mapper target other than `linear` or `crypt` (`/sys/block/dm-*/dm`, table via `DM_TABLE_STATUS`; in particular `log-writes`, `zoned`, `vdo`, `writecache`, `cache`, `era`), on md other than raid0/raid1/raid10 without a journal device, or a loop device whose backing file fails these rules. Data files are zero-filled at format regardless of size (no unwritten-extent conversions while serving). All probing lives in `kv-storage/src/devprobe.rs` (format, inspect and open only). Allowed substrates are listed in the deployment guide and in every benchmark config. | unit tests in `devprobe.rs` over captured `mountinfo`/sysfs trees (dm `linear`/`crypt` accepted; `log-writes`, `zoned`, `vdo`, `writecache`, zoned `nvme`, btrfs/f2fs/zfs/bcachefs/nilfs2/tmpfs, ext4 `data=journal`, `+j` files refused); integration test `substrate_refused` (btrfs loop image and zoned `null_blk` when the CI kernel provides them); lint: `fallocate` only in `format.rs` and always followed by the zero-fill pass. | The crash model (§17) assumes that a completed, flushed write is stored at its own address and nowhere else. Copy-on-write and data-journaling file systems, zoned devices and caching or deduplicating dm targets break that assumption, add hidden write amplification and double writes, and make `O_DIRECT` atomicity and fill accounting unpredictable. Zero-filling at format removes unwritten-extent conversions (file-system metadata updates) from the serving write path. |
| LC-35 | Project vocabulary for product structures (style rule) | Docs, CLI help and code comments never describe a product structure as a log: no "log-structured", "append-only", "write-ahead", "journal", "group commit" outside §3 and §28 of this document and `devprobe.rs` (which names refused substrates). RAM-side additions are "staged", flush coalescing is "flush batching", the large-value index object is a CHUNKMAP. | `ci/invariant_lint.sh` case-insensitive regex `log-structured\|append-only\|write-ahead\|journal\|group.commit` over `docs/`, Rust comments and CLI help strings, with the listed exclusions. | Style rule. |

### 3.2 Deletes and TTL
| Id | Invariant | Design choice | Code-verifiable check | Engineering rationale |
|---|---|---|---|---|
| LC-10 | Exact Redis TTL semantics, shortening allowed | Any write may set, shorten, lengthen or remove TTL; GT/LT/NX/XX evaluated only inside EXPIRE-family handlers. | test `ttl_shorten`; lint: comparisons of two expiry values only in `cmd/expire.rs`. | Redis compatibility (§8.4, the `tests/compat` harness): any write may set, shorten, lengthen or clear a TTL. Keeping expiry comparisons in one module keeps the GT/LT/NX/XX semantics in one place. |
| LC-11 | Deletes zero record bytes in place; no tombstone record with an expiry field | Deletes zero the record bytes in place. The only marker that exists is the txn-scoped delete marker (kind 4, §10.8), which has no expiry field and lives only until its txn's phase 2 completes. | codec test: marker layout has no expiry field; sim invariant: no marker older than `marker-max-age-ms` (2000). | Delete durability comes from older-copy ordering (§16.2), so nothing accumulates on media and no tombstone garbage collection exists; the only marker is scoped to one multi-key commit and is retired within milliseconds. |
| LC-12 | No background job scans the drive; only recovery reads whole AGs | Active expiry scans RAM hint planes and verifies single records. FLUSHDB is an O(1) seq cutoff. Recovery is the only full-device reader. | lint: only `recovery.rs` may issue reads of whole AGs. | Background I/O stays proportional to actual work (expiry candidates come from RAM hint planes, §16.4), so housekeeping never competes with foreground reads for full-device bandwidth. |
| LC-13 | No namespace/db default TTL; one version per key (no multi-version chains) | No such config keys. One index entry per key; versions (`seq`) exist only to resolve crash/replication races. The extent `twin_page_id` (§10.9) names a free run to prefer for the next version, never a readable older version. | config schema test; `Entry` has no version/next fields; no code path reads a record through `twin_page_id`. | One version per key keeps index entries at 10 B and reads single-location; Redis has no default TTL; `twin_page_id` is an allocation hint only, so no read path depends on freed space. |

### 3.3 Admission control, scheduling and pacing
| Id | Invariant | Design choice | Code-verifiable check | Engineering rationale |
|---|---|---|---|---|
| LC-14 | A received command is never delayed for load: admission control is stop-reading or an immediate error, with single thresholds and no priorities | No priorities exist. Admission control = stop re-arming `recv` on the socket (single limits, no high/low pairs) or an immediate error (`-OOM`, `-NOREPLICAS`, `-TRYAGAIN`, `-BUSY`). Once parsed, a command is never parked because of load. | lint: no identifiers `priority\|high_water\|low_water\|watermark` in `kv-node/src/{net,dispatch,runtime,cluster}`, `crates/kv-repl`, `crates/kv-coord` (the operator's static `priority` field of `NodeRecord`/`Candidate` is the named exemption, used only by `choose_primary`); no timers in dispatch. | Predictable tail latency and simple flow control: TCP backpressure before parsing bounds memory, an immediate error gives clients a fast signal, and a parsed command never sits in a delay queue; single thresholds need no hysteresis tuning. |
| LC-15 | Run to completion; one FIFO per connection/partition; a consumed command never waits for a budget or a capacity count | A command executes synchronously once its pages are present (§6.5). It may wait for its own I/O (including writes the foreground page writer builds for it, §16.2) or for an earlier command on the same key, never for load, a memory budget or a slot count. The large-value soft limit and txn credits are taken before a command is consumed (§6.7); scrubs on a command's completion path are built by the foreground page writer in the tick the command executes and never paced. | sim invariant: per-key execution order == arrival order; sim invariant I13: no consumed command's completion depends on a `TokenBucket`, `LatencyController`, `large-value-memory` or a txn-credit count; lint: the foreground scrub path in `kv-storage/src/stale.rs` and `writer.rs` does not reference `TokenBucket`/`LatencyController`; no `VecDeque`/waiter-list type in `kv-storage/src/intent.rs` or in the large-value accounting module (`kv-node/src/net/budget.rs`). | A command's latency depends only on its own I/O and on earlier commands on its keys, never on shared budgets or on background pacing; no head-of-line blocking across unrelated keys. Budgets are enforced where checking them costs nothing: before a command is consumed. |
| LC-16 | Background pacing only by static token buckets or a continuous device-latency controller | `kv_common::pace::{TokenBucket, LatencyController}` are the only pacing APIs; their constructors take rates/targets only; the controller scales background rates only. | lint: pacing modules may not import queue-depth or connection metrics. | Background work can never starve foreground traffic, and device read latency is the signal that actually measures interference; coupling pacing to client queues would create feedback loops between load and housekeeping. |
| LC-36 | Job progress by fixed-position barriers only; no load-vs-constant comparisons | Migration Ready and SEAL, full sync, Repair, handoff and ISR expand advance on `Seq >= Seq` tests against positions recorded in the past (§23.13); `rebalance apply` starts the next migration only when the previous one completes. Lag, backlog and in-flight sizes are metrics only. `--rate`/`rate_mbps` are static token-bucket rates, never priorities. | lint: in kv-repl, kv-coord and `kv-node/src/cluster`, no comparison whose operand identifier matches `lag\|backlog\|outstanding\|inflight\|in_flight\|pending_groups\|unacked_count` against a literal or a config/param field (exempt: ring capacity arithmetic in `kv-repl/src/backlog.rs`, frame size limits in `codec.rs`/`frame.rs`; time-outs such as the age of a member's oldest unacknowledged group vs `repl-ack-timeout-ms`, §23.8, are failure detection, not load tests, and use `*_age_ns` names); test `migration_ready_is_position_barrier` (a member held artificially 10^5 groups behind still gets `MigrationReady` exactly when it commits `copy_end`). | A barrier recorded in the past is always reached, so jobs terminate deterministically and are easy to test in simulation; there are no thresholds to tune and no livelock when a destination lags (the lag is visible in `kvadm migrations`, and the operator may pause or abort). |

### 3.4 Iteration, queries and fan-out
SCAN and KEYS follow Redis semantics: fixed built-in, key-only filters over the primary index, with no user-supplied
functions.

| Id | Invariant | Design choice | Code-verifiable check | Engineering rationale |
|---|---|---|---|---|
| LC-17 | No secondary indexes, query planner, server-side map/reduce or aggregation; no client code on index paths | Not implemented. SCAN/KEYS filters are fixed built-ins (MATCH = Redis `stringmatchlen` glob, TYPE = vtype equality) and return key names only, never values, computed fields or per-key derived data. No path that reads through the index accepts client-supplied code or expressions. Rebalance statistics are not computed by passes over records (§23.12). | lint: no `secondary\|planner\|aggregate\|map_reduce` modules; the `kv-cmd` table contains none of `EVAL EVALSHA EVAL_RO EVALSHA_RO FCALL FCALL_RO FUNCTION SCRIPT SORT SORT_RO` nor any name starting `FT.`/`WASM`; SCAN/KEYS handlers write only key bulks (golden tests). | Scope (§1.2): a key-value store with Redis command semantics. No server-side scripting or user code keeps per-command cost bounded and the attack surface small; key-only results keep SCAN cheap and predictable. |
| LC-18 | SCAN-family steps run on one thread over page references; intermediate lists carry page references, then real keys | `SlotTable::scan_pages` emits de-duplicated `PageRef{page36, kind, class}`s (the TYPE filter applied inside the walk, before anything is emitted) plus one scalar `ScanSpan`; step 3 recomputes each record's key hash from the bytes read from SSD and compares it with the span bounds, so no list of digests exists at any point (§16.6). One SCAN/KEYS step executes on exactly one worker thread; KEYS runs its per-partition steps strictly one at a time; no SCAN-family request is fanned out across workers. The per-step read bound `SCAN_STEP_MAX_PAGES = 32` is a compile-time constant tied to the COUNT batch, not configurable and never derived from device count, queue count, `nr_requests` or measured device concurrency. | tests `scan_single_thread` (thread id asserted in debug builds) and `keys_sequential` (at most one step in flight per KEYS call); lint: `Vec<Entry>`, `Vec<(Entry, _)>`, `Vec<KeyHash>` and `Vec<u32>`/`Vec<u64>` bindings named `*fp*\|*hash*\|*digest*` banned in `cmd/scan.rs`, `cmd/cluster_cmds.rs` and `crates/kv-repl`; `SlotTable::snapshot_iter` called only from `kv-storage/src/snapshot.rs`; `/sys/block` paths and NVMe identify data read only in `devprobe.rs`, whose `DevProbe` struct has no queue-count, `nr_requests` or queue-depth field. | A step's work and memory are bounded by constants (COUNT, 32 pages). Emitting page references instead of entry lists keeps the step's RAM small and de-duplicates reads; recomputing each key hash from SSD bytes makes the span test exact. One thread per step keeps SCAN from competing with foreground work on other workers, and a compile-time bound behaves identically on every machine. |
| LC-37 | Fan-out width and read concurrency never derived from devices | A request fans out to exactly the distinct partitions its keys map to (`FanOutSplit`, a fixed per-command routine). `XMsg::Cmd` and `XTxn::Lock` carry key bytes only; the owner partition computes `KeyHash`. `threads` defaults from the CPU count only. No per-request, per-stream or per-device read-concurrency value is derived from device attributes. Each per-partition sync or migration stream is started by its own peer `Hello` and served only by the owning worker; the coordinator and kvadm never issue storage I/O or fan out reads; stream pacing is rate-based only (no streams-per-device or concurrency knob). | lint: no `KeyHash` field in any `XMsg`/`XTxnMsg` type; `threads` default computed only from `std::thread::available_parallelism`; config schema has no key matching `*-per-device\|*-concurrency\|*-streams` and no `*-qd` key other than `recovery-qd`; test `fanout_width_equals_partitions`; `ci/check_graph.sh`: kv-coord and kvadm link no `kv-storage`. | The hash seed stays inside the owner partition and cross-worker messages stay small; fan-out cost equals the partitions actually touched; behaviour does not change with device probing results, which keeps deployments reproducible and benchmarks comparable. |

### 3.5 Index memory model and key placement
| Id | Invariant | Design choice | Code-verifiable check | Engineering rationale |
|---|---|---|---|---|
| LC-19 | No shared-memory index | Private process memory only. | lint: `shmget\|shm_open\|memfd_create\|MAP_SHARED` banned except `MAP_SHARED` on io_uring ring mmaps inside the `io-uring` crate. | Private memory cannot be corrupted by another process, and restart always rebuilds from the snapshot region or the full scan (§18) instead of trusting memory a crashed process left behind. |
| LC-20 | No pointers between index entries | Pure open addressing. | `Entry` is `Copy`, contains no pointer/index fields. | Open addressing: no pointer chasing, cache-friendly probes, trivially copyable entries (rebuild, split and snapshot are linear copies). |
| LC-21 | No partition, device or lock selector is carved out of fingerprint bits; the device is never derived from the key hash | Slot = CRC16 % 16384; partition = slot % P; device chosen by allocator. | lint as LC-08. | Slots follow Redis Cluster; every fingerprint bit goes to collision resistance; the allocator, not the key hash, balances fill across devices. |
| LC-22 | No GPU code; no RIPEMD-160 anywhere | none | cargo-deny bans `cuda*`, `cudarc`, `wgpu`, `vulkano`, `opencl*`, `ocl`, `ripemd`. | Scope: CPU-only server; dependency hygiene (see LC-06). |

### 3.6 Cluster
| Id | Invariant | Design choice | Code-verifiable check | Engineering rationale |
|---|---|---|---|---|
| LC-23 | Replication backlogs are RAM-only and never reach any disk, cluster-wide | Replication backlogs and members' received-backlogs are RAM rings in `MAP_HUGETLB` (not swappable) or `mlock`ed memory, marked `MADV_DONTDUMP`, and are never persisted in any form (no file, no swap, no core dump); Repair coverage after a crash comes from `maybe_divergent` (§18.4) or an operator resync. The shipped systemd unit sets `LimitMEMLOCK=infinity`, `MemorySwapMax=0`, `LimitCORE=0`. The coordinator persists whole state in fixed A/B halves of one pre-allocated file; node fence/assignment state is in fixed A/B blocks. | LC-02 checks run on all crates; test `backlog_has_no_io` (`Backlog` has no `File`/fd fields; compile-time `static_assertions`); test `backlog_never_reaches_disk` (`/proc/self/smaps`: every backlog VMA shows `dd` and `lo` or `ht` in `VmFlags`); unit file golden test. | The backlog only serves reconnects and failover Repair while processes run; persisting it would add a second write path for every write. Locked, non-dumpable memory keeps replication free of swap stalls and keeps user data out of swap and core files (LC-32). |
| LC-24 | Coordinator is sole owner of the slot map; data nodes never answer any request with topology, ownership or data-location content | Data nodes answer CLUSTER SLOTS/SHARDS/NODES/REPLICAS/SLAVES/LINKS/GETKEYSINSLOT/COUNTKEYSINSLOT/MYSHARDID with `-ERR cluster topology is served by the coordinator`; CLUSTER INFO carries `cluster_state` and constant 0 for every slot, epoch and node-count field; INFO replication carries only `role`, `connected_slaves`, `master_link_status`; control replies carry no role, shard, term or slot (§23.4). MOVED from a data node is `-MOVED <slot> <coord_addrs[conn_id % n]>` from the static coordinator list, identical for every slot the node does not serve. | golden test `unowned_slot_replies_identical` (on one connection every reply about a slot the node does not serve is byte-identical apart from the echoed slot number, across ownership states); `kv-node` does not depend on `kv-topo` (`cargo tree` check in CI); lint: `kv-node/src/cmd/*` never reads `Ownership.{owned, shard_slots, members, term, isr_len}` (only `cluster/ownership.rs` may; handlers call `gate()` and `slot_filter()`). | One source of truth for topology: clients converge on one versioned map served by the coordinators, a data node can never hand out stale ownership, and a state-independent MOVED needs no node-side topology cache or invalidation. |
| LC-25 | Membership changes only by operator command; failover is a unilateral static choice; no automatic rebalancing, re-replication or migration | Only `AdminOp` transitions mutate `nodes`/`slots`/replica lists. "Rebalancing" means any change of the slot -> shard assignment or the creation of a new replica copy; only `AdminOp` triggers it. Failover picks the new primary by static operator priority, then operator member order, among lease-holding ISR members; it fences only that node (sequentially, never a gather) and commits `isr = previous ISR - P - lease-flagged nodes`; no set of replies shapes the result (§23.9). Full sync starts only from `replica add`/`replica resync`; no automatic path exists, not even disabled. Repair/Continue cover only keys of the member's own shard whose state differs from the primary's. A migration failure reverts routing only; B's partial copy stays inert until `migrate abort`/`resume`; `rebalance apply` stops at the first failover or health change. | property tests `non_admin_transitions_keep_membership`, `promote_isr_independent_of_fence_replies`; sim: killing nodes produces zero snapshot/migration bytes; lint: no identifier `auto_full\|auto_resync\|auto_rebalance\|auto_migrat` anywhere; `Candidate` has no applied/position/score field. | Failures never trigger data movement (no rebalancing storms during incidents); the promotion result is a deterministic function of committed state, reproducible in simulation and independent of message timing; operators stay in control of capacity changes. |
| LC-26 | One versioned map object, single response to clients, no polling of nodes, operator registers nodes, coordinator pushes, single-owner execution, no proxying, no 2PC | §23.3-§23.4. Replies to coordinator requests (`HelloAck`, `AssignAck`, `FenceAck`, `HandoffDone`, `Error`) carry no role, shard, slot or key-count fields; `rebalance plan` uses only `ClusterState` (`--by slots`); DR never queries nodes for their assignment. | `NodeAssignment` has no full-map field; `kv-node` links no RESP client; control/replication message enums contain no prepare/abort/vote types; lint over the `kv-ctrl` reply variants: no field named `role\|shard\|slot\|slots\|owned\|keys\|key_count\|term`. | Push-based assignment and one map object keep the control plane simple: node state derives only from committed coordinator state, there is no polling load, no proxy hop and no distributed commit protocol to make crash-safe. |
| LC-27 | No leader by id order; no automatic eviction; no all-to-all heartbeats; monotonic integer epochs | Coordinator election is randomized (Raft-style). `NodeId`, `CoordId` do not implement `Ord`/`PartialOrd`. FAIL is a flag. Data nodes talk only to the coordinator (leases) and to their own shard peers (replication). `version` and `term` are u64/u32 counters. | compile-time: `static_assertions::assert_not_impl_any!(NodeId: PartialOrd)`; property test `epochs_strictly_increase`. | Randomized elections avoid id-biased leadership; heartbeats are O(n) to the coordinator instead of O(n^2) between nodes; FAIL as a flag avoids membership churn on flapping nodes; integer epochs are deterministic. |
| LC-28 | One consistency regime; no hybrid clock; no per-partition regime numbers to clients; no replication-state read gates | No mode switch. Versions are `(term, counter)` integers; wall clock only in TTL. Clients never receive terms or seqs: CLUSTER NODES config-epoch = 0, CLUSTER SHARDS replication-offset = 0, data-node INFO has no term/seq fields. Reads are never held or refused because a write's replication is pending. | lint: `SystemTime`/`now_unix_ms` only in `ttl`/`expire`/`clock`/`metrics`/`log` modules; golden tests; sim test `no_read_waits_on_replication`. | One consistency regime to document, test and operate (§23.11); integer versions do not depend on clock synchronization; reads never stall on replication, so read latency is independent of replica health. |
| LC-29 | Full payloads; no named pipes; no trigger queues; in-process; each primary ships only its own writes | `ReplOp` variants all carry values; no digest variant. Frames are shipped only by the primary of the term that produced them; cross-term catch-up ships key images from the new primary's own state (§23.9). | fuzz round-trip; lint bans `mkfifo`, `std::process::Command` in `kv-node`; test `catchup_never_forwards_foreign_term`. | Full-payload ops make members self-sufficient (no read-back from the primary); in-process streams need no extra daemons or IPC; shipping only frames of the current term keeps catch-up correct after failover (§23.9). |
| LC-30 | Maintenance never makes replicas unreadable or changes routing | Background storage tasks have no dependency on routing/health modules. Future on-line consolidation, if added, runs on every member while it keeps serving reads with no `MemberState`, CLUSTER SLOTS/SHARDS health or routing change. | `cargo tree`/module-graph check: `kv-storage` has no dependency on `kv-ctrl`/`kv-topo`; `HealthReport` has no storage-maintenance field. | Storage housekeeping is local to each member and invisible to routing, so maintenance never reduces availability or read capacity. |
| LC-31 | No RIPEMD/SHA as key identity; no GPU | as LC-06/LC-22 | as LC-06/LC-22 | As LC-06/LC-22. |
| LC-38 | Coordinators are a topology service, not database nodes; the voters' own membership is not in the voted value | `ClusterState` has no coordinator list. Each coordinator keeps its peer list in its own fixed A/B `coord.meta`, written only by the operator call `KVADM COORD SETPEERS` one server at a time (§23.3). Coordinator stub replies to keyless data commands are constant literals in `clientsvc.rs` (no keyspace abstraction, no stored state; FLUSH* are no-ops). Docs, CLI, INFO and code call it "coordinator", never node, master or database server; its persistence is "configuration state". | lint: no `CoordRecord`/`Vec<CoordRecord>` field in `ClusterState`; `ci/check_graph.sh` forbids kv-coord -> kv-index/kv-storage/kv-repl/kv-node and any crate that contains `impl StorageEngine`; stub replies in `clientsvc.rs` are `const &[u8]` items; Stateright model of SETPEERS (§23.3); vocabulary lint: kv-coord help/INFO text never says "master node" or "database node". | Separating the control plane from the data plane keeps the coordinator small and free of storage dependencies. Keeping peer lists outside the replicated state and changing them one server at a time (§23.3) makes reconfiguration safe by the single-server-change argument. Consistent naming keeps coordinators and data nodes distinct in docs and tooling. |
| LC-39 | Replica sync is an in-place merge: it never resets, bulk-releases, defragments or relocates storage; resync is documented only as a consistency repair | Full sync is an in-place merge (§23.7): identical records are confirmed without a write, differing ones go through the normal write path, unconfirmed ones are deleted by ordinary deletes at the end. Nothing is reset, bulk-released, cut off or moved on behalf of replication. `replica resync` is accepted only for NeedsFullSync members or members reporting corrupt pages; learners use `replica add`. CLI help, docs, INFO fields and runbooks never describe resync, failover, maintenance mode or reformat + `replica add` as compaction, defragmentation or space reclamation. | test `full_sync_keeps_layout` (prefilled member, 1% divergent keys: >= 98% of index entries keep their page id, the member's AG set is unchanged, page writes within 1.5x the number of divergent keys); lint: kv-repl calls none of `reset_allocator\|release_all_ags\|set_flush_cutoff\|move_record\|bulk_release`; vocabulary lint: `compact\|defrag\|reclaim` absent from kvadm help and runbook text for resync/failover/maintenance. | A merge writes only the keys that differ (a post-crash resync writes ~1-2% of a full reload), keeps the member's allocator state and layout, and needs no empty-device window. Documenting resync only as a repair keeps operators from using an expensive operation as routine maintenance. |

### 3.7 Other design rules
* No timer-driven partial-page flushes. Page writes are event driven (end of event-loop iteration, previous write
  durable); there is no coalescing timeout anywhere (§13.7). Write latency is bounded by the tick, and there is no
  timeout to tune.
* No cuckoo hashing with truncated tags; index entries hold only SSD page ids, never RAM pointers (LC-20); no
  epoch-protection framework and no hash chains through records; no on-flash bucket chains. Ordered linear probing
  gives fingerprint-ordered iteration and in-place entry updates, and page-id entries stay valid across restarts
  (the snapshot stores them unchanged, §10.10).

---------------------------------------------------------------------------------------------------------------------

## 4. Architecture overview

### 4.1 Cluster

```
             operator
                |  kvadm  (RESP over admin port, token auth)
                v
   +------------------------------------------------+
   | Coordinator group kvc x3/5 (no user data)      |      clients (redis-py, Jedis, Lettuce, go-redis, ioredis)
   |  WSR consensus (whole ClusterState, A/B file)  |<---- seeds = coordinator client addrs
   |  leases, failure detector, failover planner,   |      CLUSTER SLOTS/SHARDS/NODES, MOVED <slot> <owner>
   |  migration orchestrator, topology service      |
   +------------------------------------------------+
        |  control TCP (coordinator dials node):            ^
        |  Assign, LeaseGrant, Fence, Promote, Follow,      |  -MOVED <slot> <coordinator>   (data node, slot not served)
        |  Handoff, Migrate*;  node -> Renew, FenceAck,..   |
        v                                                   |
   +---------------------+   repl (per partition)   +---------------------+
   | kvd  PRIMARY S1     |=========================>| kvd  REPLICA S1     |
   | slots 0-5460        |<== acks ==================| (ISR member)        |
   +---------------------+                          +---------------------+
   +---------------------+                          +---------------------+
   | kvd  PRIMARY S2 ... |=========================>| kvd  REPLICA S2 ... |
   +---------------------+                          +---------------------+
       ^  data commands directly from clients (single owner executes every command)
```

### 4.2 Data node (`kvd`)

```
 TCP clients (SO_REUSEPORT, one listener per worker)          coordinator (control)      replication peers
        |                                                          |                         |
 +------v-------------------- worker thread w (pinned) -----------|-------------------------v---------+
 | io_uring ring: accept/recv/send, disk read/write/fsync, eventfd doorbell, timeouts               |
 | Conn table --> RESP parser --> Dispatcher --(slot%P == mine)--> Partition p (single-threaded)      |
 |      ^  reorder buffer           |            (else XMsg to owner worker)   |  Index: (db,slot)->|
 |      |                           v                                          |    SlotTable       |
 |      +----- replies <----- XMsg inbox (SPSC per source) <---- other workers  |  PageStore: alloc, |
 | Page cache (SIEVE, 4 KiB frames)   FlushTracker (flush batching per device)  |  page writer,      |
 | ReplSender / ReplApplier per owned partition                                  |  extents, expiry   |
 +------------------------------------------------------------------------------+--------------------+
 +-- meta thread: superblock, AG table, partition meta, intent table (A/B blocks, batched fdatasync) --+
 +-- agent thread (tokio current-thread): control channel, leases, metrics HTTP ------------------------+
 Devices: raw NVMe or zero-filled files on XFS/ext4, O_DIRECT. Layout: SB A/B | AG table A/B | meta | snapshot regions A, B | AGs (16 MiB)
```

---------------------------------------------------------------------------------------------------------------------

## 5. Terminology

| Term | Meaning |
|---|---|
| node | one `kvd` process with its own devices, one client port, one control port, one replication port |
| shard | replication group in the cluster map: one primary node + RF-1 replica nodes, owns a set of hash slots |
| slot | Redis Cluster hash slot, `CRC16-XMODEM(tag or key) % 16384` |
| partition | storage/execution unit inside a node: `partition = slot % P`. P (16/32/64/128, default 64) is fixed at format and cluster-wide. One partition = one single-threaded state machine with its own index tables, allocator state, seq counter, replication stream |
| worker | pinned OS thread running an io_uring event loop; owns partitions `{p : p % W == w}` |
| AG | allocation group, 4096 pages = 16 MiB, kind SMALL / EXTENT, owned by one partition |
| page | 4096 B; `PageId = dev<<32 \| page_in_dev` |
| small record | record <= 2048 B stored in a SMALL page |
| extent | contiguous run of 1..2048 pages in an EXTENT AG holding one record (VALUE), a CHUNKMAP (the list of a large value's chunks), or a CHUNK |
| twin run | the run a key's previous extent version occupied (recorded as `twin_page_id` in the current header); preferred, never reserved, for the key's next version (§14.4) |
| snapshot region | one of two fixed regions A/B reserved at format on every device; a clean-shutdown index snapshot overwrites one of them whole (§10.10) |
| atomic mode / two-image mode | page-update mode of a device: atomic when a 4 KiB page write is verified power-fail atomic (§13.1), else two-image |
| seq | 64-bit record version: `term << 40 \| counter`, per partition, strictly increasing |
| M / N | last durable image of a page / the image being written |
| staged | a change recorded in RAM (`Pending`) and not yet in any image |
| foreground write | a page or extent write built by the page writer in the tick of the command that needs it; never paced (§16.2) |
| txn credit | one commit position in the intent table, reserved per worker before a multi-key write is consumed (§14.6) |
| durable | on media per the device cache model: write completed AND (device write-through OR a covering flush completed) |
| ticket | handle tracking when a command's storage effects are written/durable |
| ISR | in-sync replica set of a shard, committed by the coordinator |
| term | per-shard counter, +1 on every promotion; high 24 bits of seq |
| version | ClusterState version, +1 per committed coordinator change |
| coordinator | a `kvc` process: topology service and control plane; stores no user data, serves no data, is never a database node |
| rebalancing | any change of the slot -> shard assignment or creation of a new replica copy; only operator `AdminOp`s trigger it (LC-25) |
| position barrier | a test `x >= p` where p is a seq recorded in the past; the only way background jobs advance (LC-36) |

---------------------------------------------------------------------------------------------------------------------

## 6. Threading and concurrency model (data node)

### 6.1 Threads
| Thread | Count | Owns | Never does |
|---|---|---|---|
| worker | `threads` (default = online CPUs, clamped to [1, P]) | one io_uring ring; client sockets it accepted; partitions `{p : p % W == w}`; its page cache; one `FlushTracker` per device; replication sockets of its partitions | block on a syscall other than `io_uring_enter` |
| meta | 1 | all fixed-location A/B metadata I/O (§10.2-§10.7): superblock, AG table, partition meta, intent table; AG pool | touch index or pages |
| agent | 1 (tokio current-thread runtime) | coordinator control connection, lease state, `/metrics` HTTP (Prometheus text), signal handling | touch partitions directly (talks to workers by `XMsg`) |

Workers are pinned with `sched_setaffinity` (crate `core_affinity`) to distinct CPUs, NUMA-local to the NIC/NVMe when
`/sys` reports it. Ring flags: `IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN | IORING_SETUP_COOP_TASKRUN`,
SQ size 4096, CQ size 16384. Linux >= 6.1 required, 6.18 targeted. Raw NVMe MAY use an extra `IORING_SETUP_IOPOLL` ring
**(future)**.

### 6.2 Key -> slot -> partition -> worker
* `slot = key_slot(key)` (CRC16-XMODEM, Redis hashtag rule, `kv_common::slot`).
* `partition = slot % P`. `worker = partition % W`.
* `KeyHash = xxh3_64_with_seed(key, node_seed)` (seed: superblock, secret, per node). Used only inside the partition
  (index fingerprint, record `h32`). Computed by the **owner partition** from the key bytes, for every command; `XMsg`
  and `XTxn` messages carry key bytes only (xxh3 of a ~20 B key costs ~3-5 ns, under 5% of the ~100 ns XMsg copy;
  LC-37).

### 6.3 Cross-worker messaging (`kv-node/src/runtime/xmsg.rs`)
* One bounded queue per ordered worker pair `(src, dst)`: `crossbeam_queue::ArrayQueue<XMsg>` capacity 4096, used as
  SPSC. Messages are owned values (keys/values copied into one `Box<[u8]>` per command; ~100 ns for 300 B).
* Doorbell: each worker has one `eventfd` with a re-armed `IORING_OP_READ` in its ring and an `AtomicBool parked`.
  Sender: `push` then `if dst.parked.load(Acquire) { write(eventfd, 1) }`. Receiver: before a waiting
  `io_uring_enter`, `parked.store(true, SeqCst)`, re-check all inbound queues, then wait; on wake `parked = false`.
* If a destination queue is full the sender keeps the command in the connection's local pending list and stops
  parsing that connection until space frees (backpressure, §6.7). Messages are never dropped.

### 6.4 Tick loop (one per worker)
1. Reap CQEs: accept, recv, send, disk completions -> `partition.on_io_done`, flush completions -> `FlushTracker`,
   eventfd, timeout.
2. For each connection with new bytes and not backpressured: parse complete commands (bounded by
   `max-pending-per-conn`; large-value budget and txn credits are taken before a command is consumed, §6.7), route each
   (local partition: `submit` inline; remote: enqueue `XMsg::Cmd`).
3. Drain inbound queues round-robin (one FIFO turn per source): commands -> `submit`; replies -> connection reorder
   buffers; control/meta messages -> handlers.
4. For each owned partition: `take_ready` -> `exec` each ready command (synchronous, run-to-completion) -> reply into
   the connection's slot or `XMsg::Reply`; `tick` (page writer builds/submits writes, including the foreground scrubs
   of commands executed this tick; then background tasks under token buckets).
5. `FlushTracker::tick` (submit at most one flush per device if writes completed since the last flush was submitted).
6. Release replies whose tickets/commits are satisfied; one `SEND`/`SENDMSG` per connection with all in-order ready
   bytes.
7. `io_uring_enter(submit, wait = nothing_runnable ? 1 : 0)` with a 1 ms `IORING_OP_TIMEOUT` armed only while
   background work is pending (timers drive background pacing only, never page flushes).

### 6.5 Command execution pipeline (inside a partition)
```
submit(CmdKeys) -> [key lock queue] -> prefetch pages/extents -> READY -> exec(f) (pure RAM, synchronous)
      -> effects applied to logical page states + index + ReplSender
      -> ExecOut{ticket, seq} -> reply slot waits for ticket (durability mode) [+ ISR commit in cluster mode]
```
* `CmdKeys` lists `(db, key, AccessMode)` for every key the command touches (from `kv-cmd` key specs plus
  command-specific access modes, §8.1).
* Key locks: per partition `busy: HashMap<(db, KeyHash), VecDeque<ReqId>>` (hashbrown/foldhash). A request becomes
  READY only when it is at the head of every queue for its keys and all needed pages are cached. Commands on different
  keys overlap their I/O; commands on the same key execute in arrival order (LC-15).
* `exec` runs to completion with no I/O. Anything it needs was prefetched. If `exec` discovers it needs a page that
  was not prefetched (e.g. a fingerprint false positive led to a second candidate), it returns `NeedMore(pages)`; the
  partition loads them and re-runs `exec` from the start (commands are written to be re-runnable: they mutate only at
  the end through `Keyspace` methods that are staged and committed atomically on success).
* Keyless partition ops (SCAN step, RANDOMKEY, DBSIZE part, FLUSHDB part, expiry sweep) use the same queue with a
  partition-wide lock bit only for FLUSHDB/FLUSHALL.

### 6.6 Ordering guarantees
1. Replies on a connection are delivered in request order (per-connection reorder buffer keyed by request seq).
2. Commands of one connection that hit the same partition execute in request order (FIFO per (src worker, dst
   worker) queue + per-key queues).
3. Commands of one connection that hit different partitions may take effect in a different order relative to each
   other (as with Redis Cluster pipelines across nodes). Non-pipelined clients observe strict order. Documented.
4. Per key, all operations from all connections are totally ordered by partition arrival order.

### 6.7 Admission control and backpressure (single limits; LC-14, LC-15)
A connection stops re-arming `recv` when any of these holds, and resumes when it no longer holds (same threshold):
* unreplied commands >= `max-pending-per-conn` (default 4096);
* output bytes queued >= `client-output-buffer-limit` (default 64 MiB);
* a destination `XMsg` queue is full;
* the worker's staged-write memory (pending encoded records) >= `max-dirty-bytes / W` (default 256 MiB total);
* the command being received has a bulk longer than `SMALL_REC_MAX` (2048 B), holds no large-value reservation yet,
  and the node-global large-value counter is >= the soft limit (`large-value-memory / 2`); the bytes stay unconsumed
  in `read_buf` (§6.9);
* the command being received is a multi-key write (MSET/MSETNX with >= 2 keys, RENAME/RENAMENX, COPY, EXEC with >= 2
  written keys) and the worker's txn-credit pool is empty (§14.6); the bytes stay unconsumed.
Each item is one limit checked before the command is consumed; nothing is checked again after it is consumed, so a
consumed command waits only for its own I/O and for earlier commands on its keys (LC-15). Replication, sync and
migration streams use the same two pre-decode checks and stop reading their socket.
Immediate errors: `-OOM` when the device fill limit is reached (§11.6), `-NOREPLICAS` when ISR < `min_isr`,
`-TRYAGAIN` in the migration pre-seal window, `-LOADING` during recovery, `-BUSY` never used for load. `maxclients`
is enforced at accept: the new connection receives `-ERR max number of clients reached` and is closed.

### 6.8 Multi-key commands in standalone mode (`cluster-enabled no`)
* MGET, EXISTS, DEL, UNLINK, TOUCH with keys in several partitions: split per partition by `FanOutSplit` (a fixed
  per-command routine; the fan-out width is exactly the number of distinct partitions the keys map to, LC-37), each
  sub-request carrying key bytes only, gathered in the reply slot. Not atomic across partitions (documented, as in
  Dragonfly's default).
* MSET, MSETNX, RENAME, RENAMENX, COPY, and EXEC whose keys span partitions: **cross-partition transaction (XTxn)**,
  `kv-node/src/dispatch/xtxn.rs`:
  1. Take the node-global `XTxnGate` (a `parking_lot::Mutex<()>` held by the coordinating worker, i.e. the worker
     that received the command; one XTxn at a time). The txn credit was reserved when the command was parsed (§6.7).
  2. Send `XTxn::Lock{txn, keys}` (key bytes only) to each involved partition. On arrival a partition stops admitting
     new requests (queues them FIFO), lets already-READY requests finish, prefetches the txn keys, then replies
     `Locked`.
  3. Read phase: `XTxn::Read` returns values/metas needed by the command (MSETNX existence, RENAME source value).
  4. Write phase: the coordinating worker takes a txn id (§14.6) and sends `XTxn::Apply{txn_id, ops}`; partitions
     apply with TXN-flagged records and return tickets.
  5. When all parts are written durable the coordinating worker requests the commit (§14.6), sends `XTxn::Commit`,
     partitions unlock (their queued requests resume in FIFO order), reply released.
  The gate is a conflict lock, not load control. Cluster mode never uses XTxn (cross-slot -> `-CROSSSLOT`).
* Same-partition multi-key commands run as one partition request (one `exec`), atomic, and crash-atomic via the intent
  table when they touch more than one key (§14.6).

### 6.9 Memory budgets (all configurable)
`cache-size` (page frames, default 60% of `MemAvailable` minus index estimate), `max-dirty-bytes` (256 MiB),
`large-value-memory` (2 GiB maximum; accounting of buffers for extents/chunkmaps being received, read or written),
`repl-backlog-size` per partition (default 32 MiB, i.e. 2 GiB per node at P=64; locked, never swapped or dumped,
§23.7), `client-output-buffer-limit`.
* **Large-value memory is a soft limit enforced only before commands are consumed**, with one node-global atomic
  counter. When the parser reports a bulk header longer than `SMALL_REC_MAX` for the command being received and the
  command holds no reservation yet, `net/budget.rs` checks `counter < soft limit` (soft limit = `large-value-memory /
  2`). If the check fails the command stays unconsumed in `read_buf` and the connection stops re-arming `recv` (§6.7).
  If it passes, every large bulk of that command reserves its rounded length unconditionally and MAY be received
  straight into the reserved 4 KiB-aligned extent buffer (saves one memcpy). Reservations are released when the
  command's buffers are freed.
* Reads never consult the limit: the extent buffer of a GET is allocated unconditionally at exec (counted, never
  waited for). A CHUNKMAP value is streamed through one reusable chunk window (<= 8 MiB) per command, advanced only by
  read completion and socket drain (`client-output-buffer-limit`); the budget is never re-checked between chunks.
* Worst-case overshoot = the large commands already consumed when the counter crossed the soft limit (each <=
  `proto-max-bulk-len`); with the soft limit at half the maximum the total stays under `large-value-memory` in
  practice. Nothing ever waits FIFO for budget. Values <= 2 KiB never touch the counter.

---------------------------------------------------------------------------------------------------------------------

## 7. Network and RESP layer

### 7.1 Sockets (`kv-node/src/net/listener.rs`, `conn.rs`)
* Each worker binds its own listener with `SO_REUSEPORT` (all `bind` addresses, `port`), `TCP_NODELAY` on accepted
  sockets, `SO_KEEPALIVE` with `tcp-keepalive` seconds. Multishot `IORING_OP_ACCEPT`.
* `unixsocket` path optional (one listener on worker 0) (M1.3).
* Receive: single-shot `IORING_OP_RECV` into the connection's `read_buf` (initial 16 KiB, grows to fit the current
  command, shrinks back to 16 KiB when idle and > 1 MiB). Re-armed after each completion unless backpressured.
  Provided-buffer rings are a (future) optimization.
* Send: `out: Vec<u8>` plus `out_iov: Vec<SharedBuf>` for value bodies >= 64 KiB (no copy). One `SENDMSG` per tick per
  connection. Partial sends keep the remainder. `SEND_ZC` is (future).
* Close: on EOF, error, `QUIT` (after flushing `+OK`), protocol error (after flushing the error), `CLIENT KILL`.

### 7.2 Parser (`kv-resp/src/parser.rs`)
* Incremental, zero-copy: returns argument `(offset, len)` pairs into `read_buf` for one complete command, or
  `Incomplete`. Multibulk (`*N\r\n$L\r\n...`) and inline commands (space-separated, quotes per Redis `sdssplitargs`).
* Limits (Redis wording for errors, then close): `proto-max-bulk-len` (512 MiB) ->
  `-ERR Protocol error: invalid bulk length`; multibulk count > 1048576 -> `-ERR Protocol error: invalid multibulk length`;
  inline line > 64 KiB -> `-ERR Protocol error: too big inline request`; unbalanced quotes ->
  `-ERR Protocol error: unbalanced quotes in request`; `$` expected -> `-ERR Protocol error: expected '$', got 'X'`.
* `*0`/`*-1` and empty inline lines are skipped (Redis behavior).
* `Parser::in_progress()` reports, for a command not yet complete, its name (once parsed), argument count and the
  length of the bulk being received, so that the pre-consume checks of §6.7 (large-value soft limit, txn credits) run
  before any byte of the command is consumed.
* Integers parsed with a hand-written fast path; CRLF located with `memchr`.

### 7.3 Writer (`kv-resp/src/writer.rs`)
`ReplyWriter` encodes for the connection's protocol (`Proto::Resp2 | Resp3`):
`simple, error, int, bulk, null (RESP2 $-1 / RESP3 _), null_array (RESP2 *-1 / RESP3 _), array(n), map(n) (RESP2:
array 2n), set(n) (RESP2: array), double (RESP3 ,; RESP2 bulk), bool (RESP3 #; RESP2 int 0/1), verbatim(fmt, s) (RESP3 =;
RESP2 bulk), big_number, push(n)` (never emitted in M1-M3). Integers via `itoa`, doubles via `ryu` with Redis `%.17g`
compatible formatting helper in `kv_common::num`.

### 7.4 HELLO and protocol switching
`HELLO [protover [AUTH user pass] [SETNAME name]]`: protover 2 or 3, else `-NOPROTO unsupported protocol version`.
AUTH inside HELLO is checked first. Reply (map, encoded in the *new* protocol): `server`=`"kvd"`, `version`=`"7.2.0"`
(compatibility version string clients parse), `proto`, `id`, `mode` (`"standalone"`/`"cluster"`), `role`
(`"master"`/`"replica"`), `modules` (empty array). `HELLO` with no args replies in the current protocol.

### 7.5 Pipelining and reply ordering
Every parsed command gets `req_seq` (per connection). Its reply slot is one of `Ready(bytes)`,
`Waiting{partition, ticket, commit_seq}`, `Remote` (owner worker will send `XMsg::Reply`), `Gather{n_parts, parts}`.
Slots are released strictly in `req_seq` order. Inside MULTI, commands reply `+QUEUED` immediately; EXEC's reply is one
slot. Unknown command: `-ERR unknown command 'x', with args beginning with: 'a' 'b' ` (Redis 7.0 format, args truncated
to 128 chars each as Redis does). Arity error: `-ERR wrong number of arguments for 'get' command`.

### 7.6 Authentication and connection state
`Conn { proto, db, name, lib_name, lib_ver, authed, readonly, multi: Option<MultiState>, watch: Vec<WatchKey>,
created, last_cmd, flags }`. With `requirepass` set, commands other than AUTH/HELLO/QUIT/RESET reply
`-NOAUTH Authentication required.` until authenticated.

---------------------------------------------------------------------------------------------------------------------

## 8. Command table (M1 unless marked)

### 8.1 Conventions
* Arity/flags/key specs live in `kv-cmd/src/table.rs` (one static array, `phf`-free: a 512-entry open-addressed table
  built at startup over uppercase names). The same table is used by `kvd` and `kvc` (for MOVED slot computation).
* Access modes per key: `M` = meta only (header: existence, type, TTL, length), `R` = full value read, `W` = write
  (implies prefetch of the existing record if a fingerprint candidate exists; blind writes of new keys need no read),
  `RW` = read-modify-write.
* "Module" is the file under `crates/kv-node/src/cmd/` that implements the handler.
* Error texts are Redis 7.0.15's. Common ones: `-WRONGTYPE Operation against a key holding the wrong kind of value`,
  `-ERR value is not an integer or out of range`, `-ERR syntax error`, `-ERR invalid expire time in 'X' command`,
  `-ERR increment or decrement would overflow`, `-ERR value is not a valid float`,
  `-ERR increment would produce NaN or Infinity`, `-ERR string exceeds maximum allowed size (proto-max-bulk-len)`,
  `-ERR no such key`, `-ERR DB index is out of range`, `-ERR invalid cursor`.
* Integer parsing follows Redis `string2ll` exactly (no `+`, no leading zeros, no spaces, `-0` invalid, i64 range).
  Float parsing for INCRBYFLOAT follows `strtold` (hex, `1e400`, `inf` rejected for result), via the C shim (§24.3).
* Redis <= 7.2 and Valkey are BSD-3-Clause; their source MAY be read and ported with attribution in `NOTICE`.
  Redis >= 7.4 source MUST NOT be read.

### 8.2 Connection and server (`conn_cmds.rs`, `server.rs`, `info.rs`, `config_cmd.rs`)
| Command | Keys | Semantics |
|---|---|---|
| PING [msg] | - | `+PONG` or bulk msg. In a RESP2 connection inside MULTI, queued. |
| ECHO msg | - | bulk msg |
| HELLO ... | - | §7.4 |
| AUTH [user] pass | - | user must be `default`; `-WRONGPASS invalid username-password pair or user is disabled.`; no requirepass -> `-ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?` |
| SELECT n | - | standalone 0..`databases-1` (16); cluster: n != 0 -> `-ERR SELECT is not allowed in cluster mode` |
| QUIT | - | `+OK`, close after flush |
| RESET | - | `+RESET`; db 0, RESP2, clear name/MULTI/WATCH/READONLY, de-auth if requirepass |
| CLIENT ID / GETNAME / SETNAME / INFO / LIST [TYPE normal] / SETINFO LIB-NAME\|LIB-VER / NO-EVICT on\|off / NO-TOUCH on\|off / KILL ID n / REPLY ON | - | Redis formats; LIST/INFO line: `id= addr= laddr= fd= name= age= idle= flags=N db= sub=0 psub=0 ssub=0 multi=-1 qbuf= qbuf-free= argv-mem= multi-mem=0 rbs= rbp= obl=0 oll=0 omem= tot-mem= events=r cmd= user=default redir=-1 resp= lib-name= lib-ver=` |
| COMMAND / COUNT / INFO [n..] / DOCS [n..] / LIST / GETKEYS cmd args.. | - | from `kv-cmd` table; DOCS returns `summary`, `since`, `group` only |
| INFO [section..] | - | §21; bulk (RESP3: verbatim `txt`) |
| CONFIG GET pat.. / SET k v [k v ..] / RESETSTAT | - | §20; CONFIG REWRITE -> `-ERR CONFIG REWRITE not supported` |
| DBSIZE | - | sum of per-(partition, db) entry counters (atomics); counts logically-expired-but-unreclaimed keys like Redis |
| FLUSHDB [ASYNC\|SYNC], FLUSHALL [ASYNC\|SYNC] | - | §16.5; both modes synchronous; reply after meta durable |
| TIME | - | `[unix_s, usec]` |
| SAVE / BGSAVE [SCHEDULE] / LASTSAVE | - | SAVE = durability barrier: all executed effects written and flushed, then `+OK`. BGSAVE: start barrier, reply `+Background saving started`. LASTSAVE = unix seconds of last completed barrier (startup time initially). No RDB file exists. |
| SHUTDOWN [NOSAVE\|SAVE] [NOW] [FORCE] | - | stop accepting, drain writes; remaining stale-copy scrubs are built by the foreground page writer (unpaced, §16.2); a snapshot is written only when `stale_copies` is empty and it fits its fixed region (§10.10); write the index snapshot (unless NOSAVE), set CLEAN, exit 0; cluster primaries hand off first (§23.12) |
| WAIT n timeout | - | standalone: `:0`; cluster: number of ISR replicas (writes are already replicated when acked) |
| WAITAOF numlocal numreplicas timeout | - | waits for the connection's last write to be durable locally if numlocal >= 1, replies `[1, ISR replicas]` (cluster) / `[1, 0]` |
| TOUCH key.. | M | count of existing keys |
| SWAPDB, MOVE, OBJECT, MEMORY, MONITOR, DEBUG, SLAVEOF/REPLICAOF, SYNC/PSYNC, MIGRATE, SORT/SORT_RO, EVAL*/FCALL*/FUNCTION/SCRIPT, FT.*, pubsub cmds | - | `-ERR unknown command ...` (not in table; LC-17) except REPLICAOF/SLAVEOF -> `-ERR replication is managed by kvadm` |

### 8.3 Strings (`strings.rs`)
| Command | Keys | Semantics (Redis 7.0.15) |
|---|---|---|
| GET k | R | bulk or null; WRONGTYPE on hash |
| SET k v [NX\|XX] [GET] [EX s\|PX ms\|EXAT s\|PXAT ms\|KEEPTTL] | W (RW with GET/KEEPTTL/XX/NX) | NX/XX exclusive; one expiry form; repeated identical options accepted; `SET NX GET` allowed; EX/PX <= 0 or overflow -> invalid expire; past EXAT/PXAT stores a logically expired key (counted by DBSIZE until touched); GET returns old value (WRONGTYPE if hash, nothing set); without KEEPTTL a SET clears the TTL |
| SETNX k v | W | `:1`/`:0` |
| SETEX k s v / PSETEX k ms v | W | s/ms > 0 else invalid expire |
| GETSET k v | RW | old value; clears TTL |
| GETDEL k | RW | value then delete (durable delete rule §16.2) |
| GETEX k [EX\|PX\|EXAT\|PXAT\|PERSIST] | RW | past EXAT/PXAT deletes; PERSIST removes TTL |
| MGET k.. | R each | nulls for missing/wrong type |
| MSET k v [k v ..] | W each | atomic; crash-atomic via intent table when > 1 key (§14.6); cluster: same slot or CROSSSLOT |
| MSETNX k v .. | RW each | all-or-nothing, `:1`/`:0` |
| APPEND k v | RW | new length; > 512 MiB -> string exceeds maximum allowed size |
| STRLEN k | M | length or 0 |
| GETRANGE k s e / SUBSTR | R (range read for chunkmaps) | 7.0 semantics incl. `0 -100` quirk |
| SETRANGE k off v | RW | zero-pads; off+len > 512 MiB -> error; empty v on missing key returns 0 and creates nothing |
| INCR/DECR/INCRBY/DECRBY k [n] | RW | i64, overflow error, string2ll |
| INCRBYFLOAT k f | RW | long double via shim; result stored as the formatted string; replicated as Put of the result |

### 8.4 Keys (`keys.rs`), expiry (`expire.rs`), iteration (`scan.rs`)
| Command | Keys | Semantics |
|---|---|---|
| DEL k.. / UNLINK k.. | M each | count deleted (duplicates counted once per existing key); reply after durable delete (§16.2) |
| EXISTS k.. | M each | count, duplicates counted each time |
| TYPE k | M | `+string`, `+hash`, `+none` |
| RENAME src dst / RENAMENX | RW src, W dst | TTL carried; src==dst returns OK (or 0 for NX) if exists; missing -> `-ERR no such key`; crash-atomic (§14.6) |
| COPY src dst [DB n] [REPLACE] (M1.3) | R src, W dst | standalone only for DB option |
| RANDOMKEY | - | random non-empty slot table of a random partition (single partition per call), random entry, read key; nil if empty |
| KEYS pattern | - | SCAN steps over all partitions strictly one at a time (each on its owner worker, at most one step in flight per KEYS call, LC-18); glob per Redis `stringmatchlen`; O(N) device reads, documented |
| SCAN cursor [MATCH p] [COUNT n] [TYPE t] | - | §16.6 |
| EXPIRE/PEXPIRE k t [NX\|XX\|GT\|LT] | RW (meta) | t <= 0 or absolute past -> delete, `:1`; GT/LT treat no-TTL as infinite; NX+XX/GT+LT/NX+GT conflicts -> `-ERR NX and XX, GT or LT options at the same time are not compatible` (Redis text) |
| EXPIREAT/PEXPIREAT k ts [..] | RW (meta) | as above with absolute times |
| TTL / PTTL k | M | -2 missing, -1 no TTL; TTL rounds `(ms+500)/1000` |
| EXPIRETIME / PEXPIRETIME k | M | -2/-1; EXPIRETIME rounds `(ms+500)/1000` |
| PERSIST k | RW (meta) | `:1` if a TTL was removed |

### 8.5 Transactions (`txn.rs`)
| Command | Semantics |
|---|---|
| MULTI / EXEC / DISCARD | Commands queued per connection; EXEC runs them as one partition request (or one XTxn in standalone when partitions differ). Errors at queue time (arity, unknown) -> EXECABORT. Runtime errors are per-command replies inside the EXEC array. Crash-atomic via intent table. In cluster mode all keys must share one slot (`-CROSSSLOT` at queue time). |
| WATCH k.. / UNWATCH (M1.4) | each connection has one WATCH session (`kv-node/src/runtime/watch.rs`, node-wide table); each watched key is registered with its partition on the partition's own worker (`PartitionStore::watch_key`) and the `+OK` waits until every owner registered; every worker drains its partitions' changed watched keys (`take_touched`: writes, deletes, lazy/active/replicated expiry, FLUSHDB/FLUSHALL) right after the request or tick that committed them, before its replies, and marks the sessions dirty; EXEC checks the session where its body runs, right before it runs, and returns a null array when dirty; EXEC, DISCARD, UNWATCH, RESET and close end the session. In cluster mode WATCH keys must be served by the node and share one slot (MOVED / CROSSSLOT). |

### 8.6 Hashes (`hash.rs`, M1.4 stretch)
HSET, HSETNX, HGET, HMGET, HMSET, HDEL, HGETALL, HKEYS, HVALS, HLEN, HEXISTS, HINCRBY, HINCRBYFLOAT, HSTRLEN.
Value encoding: `u32 n_fields` then n x `(varint flen, field, varint vlen, value)` in insertion order (HGETALL order is
insertion order, documented difference from Redis listpack/hashtable order). Whole-record read-modify-write; hash values
up to `hash-max-bytes` (16 MiB). Replicated as `Put` of the whole encoded hash. RESP3 HGETALL is a map.

### 8.7 CLUSTER and related on data nodes (`cluster_cmds.rs`)
| Mode | Command | Reply |
|---|---|---|
| standalone | any `CLUSTER ...`, READONLY, READWRITE, ASKING | `-ERR This instance has cluster support disabled` |
| cluster | CLUSTER KEYSLOT k | slot |
| cluster | CLUSTER MYID | own 40-hex node id (identity only) |
| cluster | CLUSTER INFO | constant fields only: `cluster_enabled:1`, `cluster_state:ok\|fail` (ok iff the lease is valid), and 0 for every slot, epoch and node-count field (`cluster_slots_assigned:0`, `cluster_slots_ok:0`, `cluster_slots_pfail:0`, `cluster_slots_fail:0`, `cluster_known_nodes:0`, `cluster_size:0`, `cluster_current_epoch:0`, `cluster_my_epoch:0`, `cluster_stats_messages_*:0`). ioredis `readyCheck` reads only `cluster_state`. |
| cluster | CLUSTER SLOTS, SHARDS, NODES, REPLICAS, SLAVES, LINKS, MYSHARDID, COUNTKEYSINSLOT, GETKEYSINSLOT | `-ERR cluster topology is served by the coordinator` (LC-24; no supported client library uses the last three; `redis-cli --cluster` is unsupported) |
| cluster | CLUSTER MEET, ADDSLOTS[RANGE], DELSLOTS[RANGE], SETSLOT, FAILOVER, FORGET, REPLICATE, RESET, FLUSHSLOTS, BUMPEPOCH, SAVECONFIG, SET-CONFIG-EPOCH, COUNT-FAILURE-REPORTS | `-ERR cluster membership and slots are managed by kvadm` |
| cluster | ASKING | `+OK` (no-op; data nodes never emit ASK) |
| cluster | READONLY / READWRITE | `+OK`; sets connection flag (§23.8) |

---------------------------------------------------------------------------------------------------------------------

## 9. RAM index (`crates/kv-index`)

### 9.1 Organization
* Per partition: `PartitionIndex { tables: Vec<Option<Box<SlotTable>>> }` indexed by
  `db * (16384 / P) + slot / P` (slot belongs to the partition because `slot % P == p`). `databases` = 16 standalone,
  1 in cluster mode. Tables are created on first insert and freed when empty.
* A `SlotTable` is an extendible-hashing directory (Fagin 1979) of ordered linear-probing segments (Amble & Knuth 1974).
  Keys of one slot of one db live in exactly one table, which gives per-slot iteration (migration, replica sync) and
  makes the slot's 14 bits an implicit part of the key identity.

### 9.2 Entry (10 bytes, little endian, unaligned, `Copy`)
```
bytes 0..4   fp32   = KeyHash bits 63..32                       (sort key and position source)
byte  4      x8     = kind Small:   KeyHash bits 31..24          (extra fingerprint bits)
                      kind Extent/ChunkMap: extent size class (0..=255, §10.9)
bytes 5..10  w (40-bit little-endian integer):
             w[0..36]  page36 = dev(4 bits) << 32 | page_in_dev(32 bits)   (first page for extents)
             w[36..38] kind   0 = Small (record inside SMALL page), 1 = Extent (VALUE), 2 = ChunkMap, 3 = reserved
             w[38]     vtype  0 = string, 1 = hash
             w[39]     sync_mark: RAM-only "confirmed" bit of a running merge sync of this slot (§23.7); 0 otherwise,
                       always written as 0 to snapshots
EMPTY  <=>  page36 == 0          (page 0 of device 0 is superblock A, never data)
```
The entry holds no pointer, no seq, no expiry, no key bytes, no digest. The record is located inside the page by
`h32` + key compare (§15). Effective fingerprint: 40 bits (Small) or 32 bits (extents) inside a 14-bit slot.

### 9.3 Segment
```rust
struct Segment {
    cap: u32,              // home positions; total slots = cap + tail(cap), tail(cap) = 8 + cap/64 (no wraparound)
    len: u32, ttl_count: u32,
    depth: u8,             // local depth d (number of top fp bits fixed for this segment)
    ents: SysBuf,          // 10 * (cap + tail) bytes, from seg_alloc(), never pooled (LC-05)
    hints: Option<SysBuf>, // 2 * (cap + tail) bytes, lazily allocated when the first TTL key arrives
}
home(fp) = (((fp << d) as u32) as u64 * cap as u64) >> 32      // monotonic in fp
```
Invariants: (I-a) entries are sorted by `fp32` (non-decreasing) over the whole array; (I-b) every entry sits at a
position >= `home(fp)`; (I-c) no EMPTY lies between an entry's home and its position. Capacity sequence: 16, 32, 64,
..., 1024, then `ceil(cap * 9 / 8)` up to `SEG_MAX = 8192`. Load factor target: grow when `len > 0.90 * cap`, shrink
(rebuild at a smaller cap) when `len < 0.25 * cap` and `cap > 16`.
`ents` byte length `10 x (cap + 8 + cap/64)` is never a multiple of 64 for any cap of this sequence (checked by a unit
test over the whole sequence; LC-09). Allocation: every segment array, hint plane and directory is one individual
allocation through `seg_alloc(bytes)` / `seg_free()`: anonymous `mmap` for >= 64 KiB (about 3% page rounding on those
only), else `std::alloc::System` (glibc `malloc`). No region, bump or arena allocation and no pooling is used (LC-05);
the "arenas" in glibc's documentation (`M_ARENA_MAX`) are its per-thread malloc heaps with per-object free.

### 9.4 Operations (all O(run length), no global pauses)
* **Directory:** `dir` of segment ids with `2^D + 1` u32 entries (the last is a sentinel equal to `u32::MAX`, so the
  directory's byte size is never a power of two); `seg = dir[fp >> (32 - D)]` (D = 0 -> `dir[0]`).
* **Candidates(h):** `i = home(fp)`; scan forward: stop at EMPTY or `e.fp32 > fp`; collect `e` if `e.fp32 == fp` and
  (`e.kind != Small` or `e.x8 == h.x8()`). Expected probe length at load 0.9: about 5.5 entries (55 B, 1-2 cache lines).
* **Insert:** from `home(fp)` skip entries with `fp32 <= fp`; insert at the first EMPTY or first larger entry by
  shifting the run up to the next EMPTY right by one (memmove of `ents` and `hints`). If no EMPTY exists before the end
  of the tail, or the grow threshold is crossed: rebuild (below), then insert.
* **Remove(pos):** backward shift: move `pos+1..` left while the next entry is non-EMPTY and displaced
  (`position > home`); the last vacated slot becomes EMPTY. No RAM tombstones.
* **Update(pos):** in-place rewrite of `page36/kind/vtype/x8` and hint (fp never changes).
* **Rebuild (grow/shrink):** allocate the new arrays, single linear pass placing each entry at
  `max(prev_pos + 1, home'(fp))`. 8192 entries: ~20 us. Runs inline on the partition thread.
* **Split** (when `cap == SEG_MAX` and growth is needed): entries with fp bit `31 - d` == 0 go left, 1 right (each half
  stays sorted); both children get `depth d+1` and `cap = max(16, next_cap(len_child / 0.75))`; directory doubles when
  `d + 1 > D` (copy of `2^D + 1` u32s; D <= 19).
* Positions (`Pos { seg: u32, idx: u32 }`) are valid only until the next mutation of that table; the partition always
  looks up and mutates within one synchronous `exec`.

### 9.5 TTL hint plane
* `hint: u16`: 0 = no TTL; else `scale = hint >> 14`, `v = hint & 0x3FFF`; lower bound of `expire_at` in unix seconds:
  scale 0: `base + v - 1` (1 s units, ~4.5 h), scale 1: `base + v*16` (~73 h), scale 2: `base + v*256` (~48 d),
  scale 3: `base + v*4096` (~776 d; `0x3FFF` at scale 3 = "far", still a lower bound).
* `base` = per-partition `hint_base_s`, set to `now_s - 60` at startup and every hour; rebase re-encodes every hint
  (RAM pass, ~2 ns/slot, sliced 64 Ki slots per tick). Encoding always floors, so `decode(hint) <= expire_at`.
* Active expiry candidate: `decode(hint) * 1000 <= now_ms` (§16.4).

### 9.6 Bytes per key
Index memory (segments + directories + hint planes): segment memory = `10 * (cap + 8 + cap/64)` ~ `10.16 * cap`;
load oscillates 0.80-0.90 (mean ~0.85) -> **11.95 B/key**; + directory/segment headers < 0.01 B -> **~11.96 B/key
without TTL**. Segments that hold TTL keys add `2.03 / 0.85 = 2.4 B` -> **~14.4 B/key** when every key has a TTL.
Per-node fixed overhead: <= 16384 x 16 DBs table headers (48 B) when every table exists. 1B keys: ~12 GB (Aerospike
59.6 GiB). 10B keys: ~120 GB.
Allocator state is not part of the index and is reported separately (`INFO memory alloc_state_bytes`): `page_free`
(1 B per owned SMALL page, ~0.09 B/key at ~11 keys/page) and extent bitmaps (§11.2).

### 9.7 Fingerprint collisions
Two different keys with equal `fp32` (and `x8` for Small) in one slot table: probability per lookup
`n_slot / 2^40` (Small) or `n_slot / 2^32` (extents): ~6e-8 / ~1.4e-5 at 61K keys/slot (1B keys on one node). On a
collision the command's `exec` returns `NeedMore` and the next candidate page is read (§6.5). Equal entries (same fp,
same page, same kind) of two different keys are interchangeable: any one can be updated when one of the keys moves.

### 9.8 Bulk build and snapshot iteration
`SlotTable::bulk_build(sorted: Vec<(Entry, u16)>)`: segments = next power of two >= `n / (0.85 * SEG_MAX)`, D = log2,
linear placement. `SlotTable::snapshot_iter()` yields entries in fp order and is used **only** by the snapshot writer
(`kv-storage/src/snapshot.rs`, LC-18). SCAN, KEYS, replica sync and migration use
`SlotTable::scan_pages()` (§16.6), which emits de-duplicated page references and a scalar fp span, never entries or
fingerprints.

---------------------------------------------------------------------------------------------------------------------

## 10. On-SSD format (byte exact; all integers little endian)

### 10.1 Geometry and device layout
* Page = 4096 B. AG = 4096 pages = 16 MiB. `lba` = the device's direct-I/O offset alignment
  (`statx(STATX_DIOALIGN).dio_offset_align`, or `BLKSSZGET` for block devices), 512 or 4096, stored at format.
* `PageId(u64) = dev << 32 | page_in_dev`; byte offset on device `dev` = `page_in_dev * 4096`.
* Files: pre-allocated at format and always zero-filled, whatever their size (about 0.5 h per 4 TB at 2.5 GB/s), so no
  unwritten-extent conversion happens while serving; never extended afterwards. Files and devices must pass the
  substrate checks at format and at every open (LC-34, `devprobe.rs`). Raw devices: optional `BLKDISCARD` at format;
  garbage content is tolerated (keyed checksums).

```
Device d (every device):
  page 0             superblock copy A            (AbBlock, magic KVSSDSB1)
  page 1             superblock copy B
  pages 2..3         zero
  pages 4..4+2T-1    AG table: block i copy A at page 4+2i, copy B at 4+2i+1, T = ceil(n_ags / 2032)
Device 0 additionally (start = SB.pmeta_page, first page after the AG table rounded up to 64):
  2 x 128 pages      partition meta: partition p copy A at pmeta_page+2p, copy B at +2p+1   (128 = max P)
  2 pages            node meta A/B (nmeta_page)
  2 pages            intent table header A/B (intent_page)
  128 pages          intent slots (intent_slot_page): slot s copy c at byte (2s+c)*lba, 64 slots
Every device again:
pad to a 16 MiB boundary
  snapshot region A  pages [snap_region_page[0], + snap_region_pages)   fixed at format, whole 16 MiB units
  snapshot region B  pages [snap_region_page[1], + snap_region_pages)   (snap_region_pages = 0: no regions)
  AG area: AG k = pages [ag_area_page + 4096k, ag_area_page + 4096(k+1))
```

### 10.2 AbBlock (common header of every fixed-location metadata block, 4096 B)
```
0x000 u64 magic
0x008 u32 crc32c over [0x00C, 0x1000)
0x00C u32 version (1)
0x010 u64 block_seq      // copy with the higher block_seq and valid crc wins; writer always writes the other copy
0x018 ... payload (4072 B)
```
A torn write damages only the copy being written; the other copy is the previous valid state. Writers never write
both copies of one block in the same batch.

### 10.3 Superblock (AbBlock magic `KVSSDSB1`)
```
0x018 [16] set_uuid              0x028 [16] device_uuid
0x038 u16 device_index           0x03A u16 device_count       0x03C u16 partitions (P)
0x03E u16 flags: b0 CLEAN, b1 CLUSTER_MODE, b2 SNAPSHOT_VALID
0x040 u32 page_size = 4096       0x044 u32 ag_pages = 4096    0x048 u32 lba     0x04C u32 n_ags
0x050 u64 device_pages           0x058 u64 ag_table_page = 4  0x060 u32 ag_table_blocks   0x064 u32 zero
0x068 u64 ag_area_page           0x070 u64 pmeta_page         0x078 u64 nmeta_page
0x080 u64 intent_page            0x088 u64 intent_slot_page   (device 0 only; 0 elsewhere)
0x090 u64 hash_seed              0x098 u32 record_crc_salt    0x09C u32 boot_gen
0x0A0 u64 clock_floor_unix_ms    0x0A8 u64 created_unix_ms
0x0B0 u64 snap_seq               0x0B8 u32 snap_header_crc    0x0BC u8 snap_region (0 = A, 1 = B: region of the
                                                              last snapshot written)   0x0BD [3] zero
0x0C0 u64 snap_bytes             0x0C8 u16 databases (16 standalone, 1 cluster) 0x0CA [6] zero
0x0D0 u64 snap_region_page[2]    // this device's regions A and B (fixed at format)
0x0E0 u64 snap_region_pages      // pages per region on this device (multiple of 4096; 0 = no regions)
0x0E8 [24] zero
0x100 [16 x 16] member device uuids by device_index (device 0 only)
rest zero
```
Device 0's copy is authoritative for set-wide fields (seed, salt, boot_gen, clock floor, flags, snapshot). Device 0's
superblock is rewritten: at startup (`boot_gen += 1`, CLEAN cleared, before any data write: edge E7), every
`clock-floor-interval` (1 s) with `clock_floor = now`, at clean shutdown (snapshot fields, CLEAN set).

### 10.4 AG table block (AbBlock magic `KVAGTBL1`)
```
0x018 u32 block_index   0x01C u16 n_entries (<= 2032)   0x01E u16 zero
0x020 entries[2032] x { u8 kind (0 FREE, 1 SMALL, 2 EXTENT, 3 reserved), u8 owner_partition (0xFF none) }
```
Rewritten (A/B, only the changed block) when an AG is acquired, released, or changes kind. An AG changes kind or
owner only when it holds no live record and no tracked stale copy.

### 10.5 Partition meta (AbBlock magic `KVPMETA1`, device 0)
```
0x018 u16 partition       0x01A u16 partitions       0x01C u32 flags: b0 SYNCING (a merge full sync is running, §23.7)
0x020 u64 seq_floor       // standalone: next_seq after restart = seq_floor; reservation: floor = next + 2^24
0x028 u32 term            // cluster: current stream term of this node for this partition; standalone 0
0x02C u32 zero
0x030 u64 flush_cutoff[16]          // records of db i with seq < flush_cutoff[i] are dead (0 = none)
0x0B0 u64 repl_durable_seq          // replica: every stream group <= this is durable here (lower bound)
0x0B8 u64 repl_commit_seq           // highest commit_seq seen from the primary (lower bound)
0x0C0 history[4] x { u32 term, u32 zero, u64 end_seq }   // takeover points of past terms (newest first)
rest zero
```

### 10.6 Node meta (AbBlock magic `KVNMETA1`, device 0)
```
0x018 [16] cluster_id    0x028 [20] node_id    0x03C u16 shard_id (0xFFFF none)   0x03E u8 role (0 none, 1 primary, 2 replica)
0x03F u8 zero            0x040 u32 fence_term  // highest term this node accepted a Fence/Promote/Follow for
0x044 u32 zero           0x048 u64 assignment_version
rest zero
```

### 10.7 Intent table (device 0)
* Header (AbBlock magic `KVINTNT1`, pages intent_page / +1): `0x018 u64 txn_floor` (txn ids handed out are
  `< txn_floor`; reserve +2^20 at a time), `0x020 u64 resolved_w` (every txn id <= W is resolved: its `COMMITTED`
  listing was durable, or it aborted and its records were zeroed; phase 2 of committed txns is **not** required),
  `0x028 u32 n_slots = 64`, `0x02C u32 ids_per_copy = 60`.
* Slot copy (the first 512 B of one `lba`-sized sector each, so a torn write damages one copy of one slot only):
  `0x00 u32 magic "KVIS"`, `0x04 u32 crc32c([0x08, 0x200))`, `0x08 u64 copy_seq`, `0x10 u16 n` (0..=60), `0x12 [6]
  zero`, `0x18 u64 txn_ids[60]` (the committed set of this copy; unused entries zero), rest of sector zero. The writer
  overwrites the copy with the lower `copy_seq`.
* A slot's *open ids* are the ids of its newest valid copy that are `> resolved_w`. The meta thread writes each commit
  batch into the **lowest-index slot whose open ids plus the batch fit in 60** (a larger batch is split over several
  such slots in the same flush); the slot's new copy lists exactly its open ids plus the batch, so resolved ids are
  dropped by the next rewrite and a slot with no open ids is free for immediate reuse. There is no cursor or rotating
  index (no modular increment of a slot index exists; lint in `intent.rs`). Ids are dropped from a copy only after a
  `resolved_w` covering them is durable (E10).
* Validity of a TXN-flagged record with id `t`: valid iff `t <= resolved_w` **or** `t` is listed in some slot's newest
  valid copy (`COMMITTED(t)`). Recovery reads the slots only to test this membership; nothing is ever ordered by slot
  index or by `copy_seq` across slots. The table never holds values, keys or record headers.

### 10.8 SMALL page and small records
```
SMALL page header (32 B; identical in every image while the page is SMALL for this partition):
0x00 u32 magic "KVP1"   0x04 u8 kind = 1   0x05 u8 partition   0x06 u16 format = 1
0x08 u64 page_id        0x10 u64 zero      0x18 u32 zero       0x1C u32 crc32c([0x00, 0x1C))
records: 16-byte aligned tiles in [0x20, 0x1000)

Small record (header 32 B, +8 when TXN), total length rounded up to 16, <= 2048 B:
0x00 u8  magic 0xA5
0x01 u8  kind: 1 STRING, 2 HASH, 4 TXN_DELETE_MARKER
0x02 u8  db
0x03 u8  flags: b0 HAS_EXPIRE, b1 TXN (b2..b7 zero)
0x04 u32 crc = crc32c_seeded(salt; page_id u64 || off u16 || rec[0x00..0x04] || rec[0x08..len_unpadded])
0x08 u64 seq
0x10 u64 expire_at_unix_ms (0 unless HAS_EXPIRE)
0x18 u16 key_len (>= 0)   0x1A u16 val_len   0x1C u32 h32 (KeyHash bits 31..0)
0x20 [u64 txn_id]          only if TXN
then key bytes, value bytes, zero padding to a 16-byte boundary
TXN_DELETE_MARKER: flags must have TXN and not HAS_EXPIRE; val_len = 0; means "key deleted by txn txn_id at seq".
```
Tiling parse (`page.rs`): at `off = 0x20`, step: if `rec[0] == 0xA5`, kind/flags valid, `len` fits before 0x1000, and
crc matches -> record, `off += round16(len)`; else `off += 16`. Bytes inside an accepted record are never examined as
headers. The checksum is keyed (secret `record_crc_salt`, never sent off-node) and location-bound, so a record image
copied elsewhere or embedded by a client inside a value can never validate.

### 10.9 Extents (EXTENT AGs)
```
Extent header (first 80 B of its first page; EXT_HDR = 80):
0x00 u32 magic "KVX1"    0x04 u8 kind (1 VALUE, 2 CHUNKMAP, 3 CHUNK)   0x05 u8 vtype (1 string, 2 hash; CHUNK 0)
0x06 u8 db               0x07 u8 flags (b0 HAS_EXPIRE, b1 TXN)
0x08 u64 page_id (first page)      0x10 u64 seq       0x18 u64 expire_at_unix_ms
0x20 u32 n_pages                   0x24 u32 key_len   0x28 u64 val_len (VALUE: value bytes; CHUNKMAP: total
                                                                         logical value length; CHUNK: payload bytes)
0x30 u64 txn_id (0 unless TXN)     0x38 u32 h32       0x3C u32 zero
0x40 u64 twin_page_id              // allocation hint only: first page of the run this key's (or chunk's) previous
                                   // version occupied, 0 = none. Never dereferenced for data (that run is free).
0x48 u32 zero                      0x4C u32 crc = crc32c_seeded(salt; page_id || hdr[0x00..0x4C] || body)
0x50 body:  VALUE    = key || value
            CHUNKMAP = key || u32 n_chunks || u32 zero || n_chunks x ChunkRef{u64 page_id, u32 n_pages, u32 len,
                       u32 chunk_crc, u32 zero}
            CHUNK    = payload            (key_len = 0)
zero padding to n_pages * 4096
```
* Size classes (entry `x8`): `CLASS_PAGES[0..32] = 1..=32`; then `p[i] = max(p[i-1] + 1, ceil(p[i-1] * 1.025))`
  until 2048 (last class = 2048 pages = 8 MiB). Waste <= 2.5% above 128 KiB. A VALUE extent is used when the small
  record would exceed 2048 B and `80 + key + value <= 8 MiB`; reading the class size gives the whole record in one I/O.
* Values larger than one extent: a CHUNKMAP extent (the list of the value's chunks) + CHUNK extents (each <= 2048
  pages), all on the chunkmap's device. 512 MiB = 64 chunks. APPEND/SETRANGE rewrite only the chunks of changed ranges
  and the chunkmap; unchanged chunks are shared by old and new chunkmaps. A chunk is free when no live chunkmap
  references it.
* An extent cannot hold two images, so an update writes the new version into another run: the key's **twin run**
  (named by the current header's `twin_page_id`) when it is free and of the same class, else a best-fit run (§11.4); in
  atomic mode an extent whose byte size is within the verified atomic write unit is rewritten in place (§14.4). The old
  extent's first sector is zeroed after the new one is durable and its pages are freed when that is durable. At most two
  extents of a key (plus chunks shared by live chunkmaps) exist on media (I12).

### 10.10 Snapshot regions (clean shutdown only)
* Two **fixed** regions A and B on every device, reserved at format (`kvd format --snapshot-region SIZE`, default 3% of
  the device's capacity rounded up to 16 MiB, per region; `0` disables clean-restart snapshots). Their page offsets are
  in each device's superblock (`snap_region_page[2]`, `snap_region_pages`). Snapshot region X is the concatenation of
  every device's region X in device-index order. The regions are never AGs, never come from the AG pool and never
  change size or place after format.
* Logical page 0 of a region = header (AbBlock magic `KVSNAPH1`): `0x018 u64 snap_seq, 0x020 u32 boot_gen,
  0x024 u16 partitions, 0x026 u8 region (0 A, 1 B), 0x027 u8 zero, 0x028 u64 total_bytes, 0x030 [16] zero,
  0x040 sections[P] x {u64 offset, u64 len, u32 crc32c, u32 zero}`. Sections follow contiguously from logical page 1.
* Section (per partition): `u32 "SNP1", u16 partition, u16 databases, u64 next_seq, u64 hint_base_s, u32 n_tables,
  u32 zero`, then per table `{u8 db, u8 flags (b0 hints), u16 slot, u32 n, n x 10 B entries in fp order (bit 39 = 0),
  [n x u16 hints]}`, then `u32 n_small_ags, {u32 ag, [u8; 4096] page_free}`, then `u32 n_extent_ags, {u32 ag,
  [u8; 512] alloc_bits, [u8; 512] start_bits}`, then `u32 crc32c` of the section.
* A snapshot overwrites, whole and from its first byte, the region the superblock does **not** name (`1 -
  snap_region`); then the superblock flips to name it (E8). A/B alternation of two fixed places, never appended. If the
  encoded snapshot is larger than one region, no snapshot is written: `SNAPSHOT_VALID` stays unset (CLEAN may still be
  set), the next start takes the full-scan path, and INFO reports `snapshot_skipped_too_large:1`; the operator may
  re-format with a larger `--snapshot-region`. No snapshot ever takes space from anywhere else.
* Sizing: a snapshot is ~10.1 B per key (12.1 B with TTL) plus ~1 B per SMALL page; at 300 B values and 95% fill that
  is ~2.3% (TTL: ~2.8%) of the AG area, which the 3% default covers. Smaller values (more keys per page) need a larger
  region or accept full-scan restarts.

### 10.11 Versions and identifiers
| Field | Width | Generation | Monotonic across restarts by |
|---|---|---|---|
| record `seq` | u64 = term:24 << 40 \| counter:40 | per partition; primary only | standalone: `seq_floor` reservation; cluster: a new term per promotion (counter restarts at 1) |
| `block_seq` | u64 | per A/B block | read both copies at startup, continue from max |
| `txn_id` | u64 | node-global | `txn_floor` reservation |
| `boot_gen` | u32 | per start | superblock |
| ClusterState `version` | u64 | coordinator | A/B state file |
| shard `term` | u32 (24 used) | coordinator | ClusterState |
| `clock_floor_unix_ms` | u64 | node | superblock, 1 s cadence |

---------------------------------------------------------------------------------------------------------------------

## 11. Free-space management and allocator (`kv-storage/src/alloc.rs`, AG pool in `metastore.rs`)

### 11.1 AG pool (node-global, meta thread)
* Free-AG bitmap per device, built from the AG tables at startup.
* `acquire(kind, partition)`: device with the most free AGs (tie: lower outstanding I/O count reported by workers,
  then a `keyed_pick` draw); on that device a **uniformly random free AG** drawn by `keyed_pick` (below), never the
  lowest index. The AG table block is written durably (A/B) before the grant is delivered. The device is never derived
  from key hash bits (LC-08/LC-21).
* `keyed_pick(domain, candidates)`: `r = xxh3_64_with_seed(domain || boot_gen || draw_counter, hash_seed)`, select the
  `(r mod n)`-th set bit of the candidate bitmap (rank/select over 64-bit popcounts). `draw_counter` is an in-RAM count
  of draws (it is not a position: it selects nothing by itself). Every draw is independent, so the acquisition sequence
  is neither monotonic nor periodic even under first-in-first-out release (a static permutation with "lowest rank
  first" would become periodic under uniform-TTL churn, which is why a draw is used). Deterministic under the simulator
  seed.
* `release(ag)`: AG table entry FREE (durable), then discard if `discard yes` and the device is raw
  (`BLKDISCARD`, token bucket `discard-mbps`); files are not hole-punched (would recreate unwritten extents).
* Each partition keeps one spare SMALL and one spare EXTENT AG pre-acquired (requested asynchronously when the spare is
  consumed), so the hot path never waits for the meta thread unless the device is nearly full.

### 11.2 Per-partition state
```rust
struct SmallAg { ag: AgId, base: u32 /* offset of this AG's 4096 bytes in Alloc.page_free */, fresh: u16 /* pages with page_free == 255 */ }
// page_free[base + i]: free bytes / 16, 255 = completely free and holds no tracked stale copy (such a page is "fresh":
// never parsed, its next write is a full-page write). Exact for cached/written pages, a hint otherwise.
struct ExtentAg { ag: AgId, alloc: BitBox<4096>, start: BitBox<4096>, runs: Vec<(u16, u16)> /* free runs, sorted by (len, start) */ }
struct Alloc { page_free: Vec<u8> /* one variable-length allocation per partition, indexed by owned SMALL page */,
               small: Vec<SmallAg>, extent: Vec<ExtentAg>, cached_space: [IndexSet<PageId>; 32] /* cached pages by 128-B free class */, warm: WarmQueue, ... }
```
`page_free` is allocator state, not index (reported as `alloc_state_bytes`, LC-09). Releasing a SMALL AG moves the last
AG's 4096-byte range into the hole (swap-remove, O(4 KiB)).

### 11.3 Placing a small record of `need` bytes (insert, or a record that no longer fits its page)
Two-image mode: `slack(page, need) = min(max(need, largest_record(page)), page-slack-cap)` (default cap 1024 B).
Atomic mode (§13.1): same-size and shrinking updates need no gap, so `slack = min(need, page-slack-atomic)` (default
128 B, room for typical growth). A page qualifies if `free_nonprotected(page) - need >= slack(page, need)`, so every
record in a page can normally be rewritten without leaving it.
1. Best fit among **cached** qualifying pages (smallest sufficient free class; among the pages of that class a
   `keyed_pick` draw, never "lowest page id" or insertion order).
2. Else a **fresh** page: the owned SMALL AG with the fewest fresh pages left (> 0; tie: `keyed_pick`), and within it a
   `keyed_pick` draw among its fresh pages (`page_free == 255`), never the lowest fresh page. Fresh pages are not read;
   their first write is a full-page write (§13.6).
3. Else best fit among **uncached** pages whose hint qualifies (tie: `keyed_pick`; the command waits for the page read).
4. Else take the spare SMALL AG (fresh pages, drawn as in 2), request a new spare.
5. Else (fill limit, §11.6) `-OOM`.
A paced warming task (token bucket `warm-iops`, default 500/s per partition) reads uncached pages with the most free
bytes into the cache so step 1 usually finds holes; holes are therefore reused before fresh pages in steady state.
There is no cursor, frontier or rotating pointer (LC-01), and because every choice among equals is a keyed draw, the
device image produced by a bulk load, a FLUSHALL reset, a migration ingest or uniform-TTL churn has no correlation
between record seq and device address (`prop_placement_uncorrelated`).

### 11.4 Extents
Request `n = CLASS_PAGES[class]` pages, in this order:
1. **Twin run:** if the update's previous version names a `twin_page_id` (§10.9) whose `n` pages are all free in the
   partition's EXTENT AGs, take exactly that run (an exact-fit hole).
2. **Best-fit hole:** the smallest free run >= n over the partition's EXTENT AGs (tie: `keyed_pick`). If the run is
   exactly n, take it.
3. **Carving** (the chosen run is longer than n, including the remaining space of a spare or freshly acquired AG): start
   at a `keyed_pick` among the n-aligned offsets `run.start + k*n` (`0 <= k < floor(len/n)`) of the run, never at its
   first page. The remaining pieces are multiples of n plus one remainder, reusable by same-class requests.
4. Else use the spare EXTENT AG (step 3 applies to it), request a new spare.
Set the `alloc` bits and the `start` bit. Extents never cross AG boundaries (max 2048 pages <= 4096). Random carving
costs some external fragmentation with mixed classes, bounded by holes-first best fit; W7 measures it (target: < 2
points of usable fill lost). `prop_placement_uncorrelated` also asserts \|Spearman(seq, first page)\| < 0.05 over
extent allocations on empty AGs.

### 11.5 Freeing
* Small-record bytes become free when the image that drops (or overwrites) them is durable.
* Extent pages become free when the zeroing of the extent's first `lba` sector is durable (`alloc`/`start` cleared).
* Freed space is immediately reusable by any key of the partition; an empty AG is released (11.7) and reusable by any
  partition. Live records are never moved to reclaim space (no compaction, no cleaner).

### 11.6 Fill limit
`max-storage-fill` (default 0.95): fraction of the AG area's AGs that may be SMALL/EXTENT (the fixed snapshot regions
are outside the AG area). With no cleaner the free-AG headroom only serves spare AGs and allocation slack, so 5% is
enough. Writes that need space when none fits and no AG may be acquired get an immediate
`-OOM storage full: command not allowed when device fill > 'max-storage-fill'` (immediate error, LC-14). Deletes,
overwrites that fit, and reads always proceed.

### 11.7 AG release
A SMALL AG with every `page_free == 255`, no cached dirty page and no tracked stale copy, or an EXTENT AG with no `alloc`
bit set, is released (token bucket `ag-release-per-sec`, default 20). Releasing first requires that any page of the AG
known to hold a droppable record of a key that is live elsewhere has been zeroed (tracked stale copies, §16.2); dead
records (flush cutoff, expired, aborted txn) may remain. A released AG is later re-acquired only through a `keyed_pick`
draw (§11.1).

### 11.8 Space utilization (SMALL pages, `page-slack-cap 1024`, `page-slack-atomic 128`)
Last two columns: x `max-storage-fill` 0.95 x 0.94 (AG area after the default 2 x 3% snapshot regions) = x 0.893.
| key + value | record | records/page two-image (atomic) | user bytes / page two-image (atomic) | of raw device two-image (atomic) |
|---|---|---|---|---|
| 20 + 100 B | 160 B | 24 (24) | 70% (70%) | 63% (63%) |
| 20 + 300 B | 352 B | 10 (11) | 78% (86%) | 70% (77%) |
| 20 + 600 B | 656 B | 5 (6) | 76% (91%) | 68% (81%) |
| 20 + 1000 B | 1056 B | 2 (3) (cap 512: 3, updates then move) | 50% (75%) | 45% (67%) |
| 20 + 2000 B | extent 1 page | 1 | 49% | 44% |
| 20 + 3990 B | extent 1 page | 1 | 98% | 87% |
| >= 64 KiB | extent | - | >= 97% | >= 87% |
Aerospike: record + 39 B header, 16-B rounding, 50% fill: 300 B values -> ~42%. Mid-size values (0.8-2.5 KiB) are our
weak spot in two-image mode (documented); `page-slack-cap 512` trades in-place updates for 25 points of space in that
band. Atomic mode (4Kn NVMe or verified `RWF_ATOMIC`, §13.1) needs no same-size gap and recovers most of it.

---------------------------------------------------------------------------------------------------------------------

## 12. Page cache (`kv-storage/src/cache.rs`)
* One `PageCache` per worker, shared by the worker's partitions (a page belongs to exactly one partition).
* Frames: 4096-B aligned, carved from 2 MiB `MAP_HUGETLB` mappings (fallback: `MAP_ANONYMOUS` + `MADV_HUGEPAGE`),
  `cache-size / W` bytes per worker, marked `MADV_DONTDUMP` (LC-32); the shipped unit's `MemorySwapMax=0` keeps the
  fallback mapping out of swap. Optionally registered with `io_uring_register_buffers` (`READ_FIXED`/`WRITE_FIXED`)
  when `uring-fixed-buffers yes` (default yes).
* Map `hashbrown::HashMap<PageId, FrameIdx, foldhash>`. Frame meta: `{page, pin: u16, visited: bool, state:
  Free | Loading{waiters} | Ready, page_state: Option<Box<PageState>>}`.
* Eviction: SIEVE (Zhang et al., NSDI'24): a hand walks frames; visited -> clear and skip; evictable only if
  `pin == 0`, no pending changes, no write in flight, no blocked drop and no tracked stale copy in the page.
* Concurrent loads of one page share the `Loading` frame's waiter list (FIFO).
* Extents are read into `ExtentBuf`s (aligned buffers, `MADV_DONTDUMP`, counted in `large-value-memory`; a read
  never waits for the budget, §6.9) and released after the command; caching extents <= 64 KiB in frame runs is
  optional (M1.3, `extent-cache-max 64k`).
* Write buffers: a write copies the built image N into a separate frame (or `ExtentBuf`), so the M frame and the
  logical state can keep changing while N is in flight; on durability the N frame becomes the page's M frame and the
  old M frame is freed.

---------------------------------------------------------------------------------------------------------------------

## 13. The page writer: atomic and two-image modes (`kv-storage/src/image.rs`, `writer.rs`)

### 13.1 Page-update modes and rules (crash consistency without logs)
Each device runs in one page-update mode, chosen at open (`page-update auto|two-image|atomic-overwrite`, default auto):
* **Atomic mode** (auto picks it wherever it is verified; M1.3): `devprobe.rs` reports that a 4096-B write at a
  4096-aligned offset is power-fail atomic: `statx(STATX_WRITE_ATOMIC)` on the opened fd (files on XFS/ext4 and block
  devices) or `queue/atomic_write_unit_max_bytes` (block devices) gives `unit_min <= 4096 <= unit_max`. On a 4Kn NVMe
  namespace the kernel reports >= 4096 because a single-LBA write is power-fail atomic (AWUPF is 0's-based). Every
  page-image write is then a single write issued with `RWF_ATOMIC`; after a crash the page holds M or N, never a mix.
  `atomic-overwrite` refuses to start on a device that does not verify; auto falls back to two-image.
* **Two-image mode** (every other device, e.g. the virtio test disk): no atomicity is assumed; a torn write leaves
  every byte equal to M's or N's byte at that position (no stronger assumption).

For a SMALL page let M be its last durable image and N the image being written.
* **R1** (two-image) Every *protected* record of M appears in N byte-identical at the same offset.
* **R1a** (atomic) A changed record whose rounded length is unchanged or smaller is rewritten **at its existing
  offset** (a true in-place update), unless the new version is TXN-flagged: a txn record never overwrites the version
  it supersedes, which stays Protected until the commit is durable (13.3 c, E6) and is placed by R2. A grown record is
  placed by R2 and its old copy is zeroed in the same image. Same-page protection (13.3 a and the same-page part of b)
  does not exist in atomic mode because N replaces M whole; txn protection (c) and cross-location ordering (d, E2,
  E9) still apply.
* **R2** Records that need free bytes are placed **first-fit-decreasing**: the build sorts the page's staged Puts by
  encoded length (descending; ties by `h32`, then key bytes) and places each at the lowest 16-aligned offset >= 0x20
  that overlaps no Live/Protected record of M and no record already placed in N. Placement depends on sizes, never on
  execution order or time; there is no pointer.
* **R3** A record of M is dropped (zeroed or overwritten) only if it is droppable (13.3). In two-image mode every
  *superseded* copy (an older copy of a key whose newer version is durable) is zeroed by the first build of the page
  after that newer version became durable (mandatory, not opportunistic; a page evicted meanwhile keeps the copy
  recognizable, because Live is derived from the index and the higher seq when the page is loaded). A page therefore
  never durably holds more than two copies of a key, the second only while the newer one is not yet durable or until
  that next build zeroes it (sim invariant I11). No extra write is scheduled for it.
* **R4** Inside the written range every byte not belonging to a kept or placed record is zero.
* **R5** At most one image write per page is in flight; N is issued only after M is durable (edge E1), in both modes.

### 13.2 In-RAM page state
```rust
pub struct PageState {
    pub page: PageId,
    pub m_frame: FrameIdx,                 // M bytes (fresh page: treated as all-garbage, never parsed)
    pub recs: SmallVec<[RecDesc; 16]>,     // valid records of M (from tiling parse or from the last build)
    pub pending: SmallVec<[Pending; 4]>,   // staged changes not in any image yet, in execution order (read overlay);
                                           // placement order is R2, not this order
    pub inflight: Option<Inflight>,        // N frame, its recs, sub-I/O count, state Writing|AwaitFlush
    pub fresh: bool,
    pub gen: u32,                          // +1 per durable build (tickets reference (page, gen))
}
pub struct RecDesc { pub off: u16, pub len: u16, pub h32: u32, pub db: u8, pub kind: u8, pub flags: u8,
                     pub seq: Seq, pub txn: u64, pub status: RecStatus }
pub enum RecStatus { Live, Protected, Superseded, Droppable }
pub enum Pending { Put(EncodedRecord /* boxed bytes + RecDesc template */), Remove { db: u8, h32: u32, key: Box<[u8]> } }
```
Reads consult `pending` (newest first) before `recs`. Commands never touch frames directly; they stage changes through
`Keyspace` (§24.7), which records `Pending` entries and updates the index.

### 13.3 Classification of each record r of M at build time
* **Live**: r is the version the logical state says is current for its key (no pending change for that key).
* **Protected** (kept byte-identical) if any:
  (a) (two-image) r is the newest durable copy of its key and a staged/in-flight newer version of that key in this
      page is not durable yet;
  (b) r's key is being removed and another durable copy of the key exists on another page/extent (`stale_copies`,
      §16.2) or, in two-image mode, in this page;
  (c) a TXN record or TXN marker that supersedes r belongs to a txn that is not committed durably (E6);
  (d) r is the target of an outgoing move whose new copy is not durable (E2).
* **Superseded** (two-image): an older copy of a key whose newer version is durable in this page or elsewhere; zeroed
  by this build (R3). In atomic mode an older same-page copy cannot exist.
* **Droppable** otherwise, in particular: the key is absent and r is its only durable copy; r is dead (seq < flush
  cutoff of its db, TXN id > W and not committed, or belongs to a slot purged after migration); or r is expired
  (`expire_at <= clock_floor_durable`) AND r is the only member of `Copies(K)` (no older copy of its key exists on this
  page or in `stale_copies`). An expired record that still has older copies is **Protected** (rule (b) applies as for a
  delete) until their zeroing is durable (§16.2, v1.2 fix).
* When a page is loaded, Live is derived from the index: a record is Live iff its key's index entry points to this page
  (same kind/vtype) and it has the highest seq among the page's records of that key; lower-seq records of that key are
  Superseded. Defensive rules (both conditions are impossible by construction; counted in `kvd_unexpected_orphans`,
  simulator invariant): a non-dead record whose key is indexed to another page is registered in `stale_copies`; a
  non-dead record whose key is not indexed at all is zeroed by the next build of the page.

### 13.4 Build (called by `PartitionStore::tick` for every dirty page with no write in flight and M durable)
1. `N := copy(M)` (fresh page: `N := zeroed page` + page header).
2. Classify `recs`. Coalesce `pending` per key: only the newest staged effect of each key is materialized (a staged
   Remove after a Put cancels it; the waiting tickets of superseded staged effects move to the surviving one, D2).
3. Zero every Superseded record (R3). Other Droppable records are zeroed when (i) they lie inside the write span
   anyway, (ii) they are tracked stale copies blocking a delete, an AG release or a third-copy write (§14.3), or (iii)
   the page is being written for the scrub task; otherwise their bytes are simply free space for step 5.
4. `Remove`s: if the key's M copy is its only durable copy (16.2) zero it; else keep it Protected, zero the older copies
   that are in this page, make sure the older copies on other pages are being zeroed by foreground writes built in this
   tick (16.2), and keep the `Remove` staged.
5. `Put`s: atomic mode first rewrites every updated non-TXN record whose rounded length is unchanged or smaller at its
   offset (R1a). All remaining Puts (new keys, grown records, every update in two-image mode) are placed by R2; each gets its
   crc for `(page, off)`. In two-image mode the previous version of the key in M becomes Protected for this build (R1).
   If no gap exists, the record **moves** (14.3).
6. Compute changed byte ranges; choose the I/O shape (13.6); submit; `inflight := N`.
7. On completion: if the device is write-through or the write used `RWF_DSYNC`: durable now; else wait for the next
   flush that is submitted after this completion (13.8). On durable: `M := N`, `recs := N.recs` with statuses reset
   (versions superseded by N become Superseded), `gen += 1`, resolve tickets and E2/E3/E6/E9 dependencies, re-mark
   dirty if `pending` is non-empty.

### 13.5 Why updates stay in place
* **Atomic mode:** a same-size or shrinking update is rewritten at its offset; the record's address and the index
  entry never change (LC-01 `prop_update_keeps_offset`).
* **Two-image mode:** the slack rule (11.3) keeps a free gap at least as large as the page's largest record (up to the
  cap). An update's new version is placed by R2 in the page's free bytes, which normally include the bytes of the
  superseded copy that R3 zeroes in the same build; at most two copies of the key are on media (I11), the older one
  only until the new one is durable and the page is next built. The index entry (which stores only the page) never changes (LC-01 `prop_update_keeps_page`).
* Both disciplines are classical: fixed-location page rewrite (PostgreSQL and Berkeley DB heap files), shadow and
  ping-pong pages (Lorie 1977; System R; Gray & Reuter 1993), heap-only tuple updates inside a page (PostgreSQL 2008).

### 13.6 I/O shape and write granularity (`write-granularity auto|page|lba`, default auto = lba if `lba == 512`)
* First write of a fresh page: one full 4 KiB write (both modes).
* **Atomic mode:** exactly one write per image: the smallest naturally aligned power-of-two span covering every
  changed byte, at least `max(lba, unit_min)` and at most 4096 B, issued with `RWF_ATOMIC` (a 352 B in-place update:
  typically 512-1024 B on 512-B-LBA devices that verify, 4096 B on 4Kn).
* **Two-image mode:** changed span > 2048 B, or > 2 disjoint changed spans: one full 4 KiB write. Otherwise up to 2
  SQEs, each the smallest `lba`-aligned span covering one changed range; the image counts as written when both complete
  (they cover disjoint bytes of the same N).
* Host bytes per 300 B update: 512-1024 B on 512-B LBA devices (byte WA ~1.6-3.2x, ~2.4x mean); 4096 B on 4Kn
  (12.8x). NAND-level programs on 4 KiB-mapping FTLs are still 4 KiB (endurance caveat, §27.5). R3 zeroing adds no
  write on 4Kn (the full page is written anyway) and at most an occasional extra sector on 512-B devices, because the
  next version normally lands on the superseded copy's bytes.

### 13.7 Coalescing (event driven, no timers)
All changes to a page that accumulate while its previous image is in flight or awaiting flush are materialized into
the single next image (placed by R2, not in arrival order). Inserts executed in one tick into the same fill page produce
one write. Nothing waits for a timeout to "fill" a buffer.

### 13.8 Flush tracking (`FlushTracker`, one per worker per device)
* `device-cache auto|write-through|write-back`: auto uses `devprobe.rs`, which reads `queue/write_cache` of the device
  (files: of the file system's backing device). Write-through: durable at completion; no flushes issued.
  "Flush batching": one flush covers every write completed before it was submitted.
* Write-back: each write completion records `flush_epoch_needed = tracker.submitted_epoch + 1`. When completed-but-
  unflushed writes exist and no flush is in flight, submit `IORING_OP_FSYNC` (`IORING_FSYNC_DATASYNC`) on the device fd
  and increment `submitted_epoch`; on its completion every write with `flush_epoch_needed <= completed_epoch` is durable.
* `flush-method dsync`: writes carry `RWF_DSYNC` (FUA on NVMe) instead; durable at completion.
* E1 makes flushes mandatory before re-writing a page on write-back devices in every durability mode (otherwise power
  loss could mix three images).

### 13.9 Tickets
`Ticket { deps: SmallVec<[(PageId, gen); 2]>, extent_deps, txn: Option<TxnId>, flags: HAS_DELETE | HAS_META }`
with state `Executed -> Written -> Durable` (or `Failed(io error)`). `Written` = every dependency's image write
completed; `Durable` = durable per 13.8 (and txn committed durably, meta writes durable).

---------------------------------------------------------------------------------------------------------------------

## 14. Write path

### 14.1 SET k v (small record), on the owner partition
1. `submit`: `KeyHash` from the key bytes; index candidates for `(db, slot, KeyHash)`. No candidate -> no prefetch.
   Candidate(s) -> prefetch the first candidate page (cache hit or one 4 KiB read; the request joins the page's waiter
   list).
2. READY -> `exec`: locate the key's current version (pending overlay, then M records) by `h32`, db, key compare.
   Apply SET semantics (NX/XX/GET/KEEPTTL/expiry). Assign `seq = next_seq++` (one seq per command; every record the
   command writes carries it).
3. Encode the record. Existing key in page P: stage `Pending::Put` on P (in place, in P's free bytes, or a move is
   decided at build time, 13.4). New key: choose a page by 11.3, stage `Pending::Put`, insert the index entry
   (`kind Small`, page). The hint plane is updated with the new TTL (RAM only).
4. Mark P dirty; the reply slot waits for the ticket (14.7). The page writer builds and submits at the end of the tick.

### 14.2 Read-modify-write commands
GETSET, GETDEL, GETEX, INCR*, APPEND, SETRANGE, SET GET/KEEPTTL/NX/XX, EXPIRE*, PERSIST: same as 14.1 with access mode
RW (the page is prefetched). EXPIRE/PERSIST rewrite the record with the new `expire_at` (new seq). Large values:
EXPIRE/PERSIST rewrite only the chunkmap (or the VALUE extent, §14.4).

### 14.3 Moves (small record grows beyond its page's free gap, or crosses the 2048 B small/extent boundary)
1. Place the new version in another page Q (11.3) or a new extent; update the index entry (page, kind, class).
2. **Third-copy rule:** if K still has a tracked stale copy elsewhere (an earlier move or extent version whose zeroing
   is not durable), any write that would create a further copy of K (this move, an extent update, or a two-image
   in-page update of K at its new location) is issued only once that zeroing is durable (edge E14); the zeroing is
   built by the foreground page writer in the same tick. The command waits only for its own I/O; at most two copies of
   K ever exist on media (I11, I12). This is rare: it needs a second write of K before the paced cleanup of its
   previous page.
3. The old version in P stays Protected (13.3 d) until the new copy is durable (E2); then it is Superseded and every
   record of that key in P is registered in `stale_copies` until P's next durable image zeroes them (P is marked dirty
   with a scrub reason; that scrub is paced, `scrub-iops`, because no command waits for it).
4. The command's ticket waits for the new copy only. A DEL (or any command whose completion needs the old copy gone,
   §16.2) that arrives before the scrub completes moves that scrub onto the foreground path in that tick and waits only
   for those writes.

### 14.4 Extent (VALUE) writes: in place where atomic, else the twin run
1. **Atomic in place:** in atomic mode, when the new version has the same class, `n_pages * 4096 <= unit_max` (on
   4Kn NVMe: 1-page extents, i.e. values of 2-4 KiB) and the write is not TXN-flagged, the extent is rewritten at its
   location with one `RWF_ATOMIC` write; the index entry and `twin_page_id` are unchanged; no stale copy exists.
2. **Twin run otherwise:** allocate by 11.4, whose first choice is the run named by the old header's `twin_page_id`
   (the run this key's previous-previous version occupied, freed when the previous version became durable). The new
   header's `twin_page_id` is the old extent's first page. If that twin run's zeroing is still in flight, the write is
   issued when it is durable (E4/E14). A key whose class does not change therefore alternates between two fixed runs
   (A/B shadow); runs are preferred, never reserved, so other keys may take them in the meantime. Build header + key +
   value into an `ExtentBuf`; one write (full pages). The index entry is updated in RAM at exec time (reads of the key
   are served from the buffer until the write completes).
3. When the new extent is durable, the writer immediately (event driven, unpaced) zeroes the first `lba` sector of the
   old extent (or drops the old small record) and frees its pages when that is durable. Until then the old extent is a
   tracked stale copy.
4. Bytes written per update: value + header rounded to pages, + one sector (none in atomic place). Byte WA 1.0-1.1x for
   values >= 16 KiB.

### 14.5 Large values (CHUNKMAP + CHUNK extents, value > one 8 MiB extent, <= 512 MiB)
* SET: the value (received under the pre-consume soft limit, §6.9) is split into chunks of <= 2048 pages; chunk
  extents are written (in parallel, on the chunkmap's device: the allocator picks the device once per value; runs by
  11.4); after all chunk writes are durable (E3) the new chunkmap is written into the **previous chunkmap's twin run**
  (its header is read anyway for key verification), never "the next free run after its chunks"; then the old value is
  freed as in 14.4.
* APPEND: when the trailing chunk is < 1 MiB it is rewritten together with the appended bytes copy-on-write into **that
  chunk's own twin run** (its header is read for the merge); otherwise new chunk(s) are allocated by 11.4; then the new
  chunkmap goes into the chunkmap's twin run; the old chunkmap and unreferenced chunks are freed.
* SETRANGE: rewrite only the chunks overlapping the range, each into its own twin run (their headers are read for the
  read-modify-write), then the new chunkmap into its twin run.
* GET / GETRANGE: chunkmap read, then the needed chunk reads in order, one outstanding at a time per command
  (sequential, same device), through one reusable chunk window (<= 8 MiB) advanced only by read completion and socket
  drain (§6.9). Replies stream chunk buffers as iovecs.
* Socket-to-extent streaming of large bulk arguments is (future); M1 buffers the value in RAM (it MAY be received
  straight into the aligned extent buffer, §6.9).

### 14.6 Multi-key atomic writes (intent table, §10.7)
Used by every command (or EXEC) that writes >= 2 keys, and by XTxn. Single-key commands never use it.
0. **Txn credits.** The table holds 64 slots x 60 ids = 3840 commit positions, divided statically among workers at
   startup (`3840 / W` each; replication, sync and migration streams draw from their worker's pool). A worker takes
   one credit **before it consumes** a multi-key write from a client or stream; with no credit left it leaves the
   command unconsumed and stops reading that connection (§6.7, single limit). A consumed txn therefore always has a
   commit position, and nothing ever waits for a free slot.
1. `txn_id` from the node-global id allocator (an `AtomicU64` inside the reserved `txn_floor` range; the meta thread
   extends the floor by 2^20 asynchronously at half use; were the range ever exhausted, the next multi-key write would
   stay unconsumed exactly like a missing credit). An id taken by a command that fails before staging anything is
   reported as aborted at once, so `resolved_w` never stalls on it.
2. `exec` stages records with flag TXN and `txn_id`; deletes inside the txn stage a `TXN_DELETE_MARKER` record for the
   key instead of a `Remove`; all superseded/deleted versions stay Protected (13.3 c).
3. **Commit batching:** when every page/extent write of the txn is durable, the partition sends `CommitTxn{txn}` to the
   meta thread. The meta thread takes every commit request that arrived since its previous slot write, writes them
   (<= 60 per slot copy, §10.7 placement rule) into the older copy of the chosen slot(s), flushes, and replies
   `Committed` for each id (E6). One sector write commits every txn that is ready. Replies (per 14.7) and replication
   commit wait for this.
4. Phase 2 (holds no slot and no credit): superseded versions become Superseded; each marker becomes Droppable once
   all older copies of its key are durably gone (E9). Phase-2 writes are built by the page writer as soon as the commit
   is durable (event driven, unpaced), so markers live for milliseconds; `marker-max-age-ms` (2000) is a simulator
   invariant.
5. Resolution: when every txn id <= t has a durable `COMMITTED` listing or is aborted-and-zeroed, the meta thread
   advances `resolved_w` to t (header A/B, durable). The credits of those txns return to their workers and their ids
   are dropped from slot copies by the next rewrite (E10). TXN-flagged records with id <= W stay valid forever via W,
   so slot lifetime is about two metadata writes and never depends on phase 2 or on paced work.
6. Abort (error before commit, e.g. `-OOM` on the second key): staged TXN records are dropped as normal droppable
   records; no commit is written; the command replies the error; the txn resolves when the drops are durable.

### 14.7 Durability modes and reply release (`durability async|written|flushed`)
| mode | ordinary writes reply after | deletes, FLUSH*, txn commits reply after | guarantee |
|---|---|---|---|
| `async` (standalone only) | exec (RAM) | Written | kill -9 loses at most writes executed but not yet completed (one tick + device latency); acked deletes never resurrect after kill -9 |
| `written` (standalone default) | Written | Written | kill -9: every acked write/delete survives. Power loss: each key old-or-new, recent acked effects not yet flushed may be lost (documented) |
| `flushed` (cluster default and minimum for cluster mode: `written` or `flushed`) | Durable | Durable | power loss: every acked effect survives |
In cluster mode the reply additionally waits for the shard commit (§23.8). `durability async` is a config error in
cluster mode. The async loss window is bounded without any timer: a command's page image is built and submitted at the
end of the tick that executed it, and on write-back devices the batched flush is submitted as soon as the write
completes; worst case = one tick + device write latency + flush latency (typically < 2 ms on NVMe).

---------------------------------------------------------------------------------------------------------------------

## 15. Read path
1. Receiving worker: parse, `slot`, `partition`; local partition -> `submit`, else `XMsg::Cmd` (key bytes only; the
   owner partition computes `KeyHash`, §6.2).
2. `submit` (access R or M): candidates. None -> READY immediately (exec replies nil; zero I/O).
3. Candidate page cached -> READY. Else allocate a frame (SIEVE), `IORING_OP_READ` 4 KiB (Small) or
   `CLASS_PAGES[class] * 4096` (Extent; M access reads only `ceil((80 + key_len) / 4096)` pages) or the chunkmap
   (ChunkMap; R access then reads chunks). On completion: header check, tiling parse with crc verification for SMALL
   pages; whole-extent crc for VALUE extents (R access) or header+key crc check for M access (the header crc covers the
   body, so M access validates `magic/page_id/kind/len` and defers full crc to the first R access).
4. `exec`: find the key (pending overlay first, then `recs` with matching `h32`, db, key bytes). Not found -> next
   candidate (`NeedMore`) or nil. Expired (`expire_at <= now_ms`) -> treated as absent; lazy expiry (16.4).
5. Reply: value copied into the connection buffer (local) or into `XMsg::Reply`; values >= 64 KiB are passed as shared
   buffers and sent as iovecs.
* **One device read of one location per GET** for every value <= 8 MiB; zero reads for misses (no candidate) and cache
  hits. Fingerprint false positives add a read with probability ~1e-7 (Small) / ~1e-5 (extents).
* Corruption (crc mismatch, page id mismatch): the page/extent is marked corrupt; commands on keys whose candidate is
  that page reply `-ERR storage corruption detected` (metric `storage_corrupt_pages`); in cluster mode the node reports
  `health.corrupt_pages` to the coordinator (operator-driven `replica resync`, a merge sync that rewrites only what
  differs, §23.7; no automatic repair in M1-M3). Corruption reports name the page id only, never record bytes (LC-32).

---------------------------------------------------------------------------------------------------------------------

## 16. Deletes, TTL, expiry, FLUSH, SCAN

### 16.1 DEL / UNLINK / GETDEL
`exec` removes the index entry (and hint), decrements counters, stages `Pending::Remove` on the key's page (or
schedules the extent's first-sector zeroing), and registers the command's ticket on that pending effect. UNLINK is
identical to DEL (no lazy-free distinction needed: freeing is a page write). The reply waits for `Written` (async and
written modes) or `Durable` (flushed mode). There is no tombstone of any kind (LC-11).

### 16.2 No-resurrection rule (marker-free deletes)
Let `Copies(K)` be the valid, not-dead records of key K on media (dead = seq below its db's flush cutoff, or
uncommitted TXN id > W). **Expired records are NOT dead for this rule** (v1.2 fix): an expired newest copy still
shadows every older copy of K, so it stays in `Copies(K)` and acts as "absent" until all older copies are durably
zeroed; only then may its bytes be dropped/reused. Otherwise a crash could resurrect an older, unexpired copy after the
newest one expired. TTL semantics are unchanged (any write may shorten or remove a TTL).
* **Rule D1:** the newest durable copy of K may be dropped only when it is the only member of `Copies(K)` (E9).
  Older copies are tracked: same-page copies are visible in the page's `recs`; copies on other pages/extents are in
  the partition's `stale_copies: HashMap<(u8 db, Box<[u8]> key), SmallVec<[StaleRef; 1]>>` (exact keys, never
  hashes), populated by moves (14.3), extent updates (14.4), recovery (18.3 d) and cleared when the copy's
  zeroing is durable. By I11/I12 there is at most one such older copy per key.
* A delete whose key has older copies has their zeroing **written by the foreground page writer as part of the
  command**: the builds (or extent first-sector zeroings) are made and submitted in the tick the delete executes, keeps
  the newest copy Protected, and drops it in the next build after they are durable. Only the E1/E9 ordering edges
  constrain these writes; they never draw from `scrub-iops` and are never scaled by the latency controller (LC-15).
  Typical cost: one write; worst case (key updated in the previous image of the same page): two writes.
* **Foreground scrub rule (all commands):** any zeroing on the completion path of a received command is a foreground
  write built in the tick the command executes: DEL/UNLINK/GETDEL, EXPIRE/PEXPIRE/GETEX-to-delete, RENAME/RENAMENX
  source removal, MSET/COPY overwrite removals, the third-copy zeroing of §14.3, and SHUTDOWN's drain. When such a
  command arrives for a key whose stale copies are queued for a paced scrub (including after recovery, §18.5), that
  key's scrubs move to the foreground path at that moment. Only zeroings that no command waits for (post-move
  cleanup of P, post-recovery stale copies of untouched keys) stay paced by `scrub-iops`.
* **Rule D2:** a delete's ticket completes when the key's newest copy is durably gone **or** durably superseded by a
  later write of the same key (the later effect inherits the waiting tickets of the superseded pending effect).
* Records that are dead never block anything and may stay on media indefinitely; they can never become live again
  because cutoffs are durable before FLUSHDB replies (E5), the clock floor never decreases (E13), and txn ids are never
  reused (E10).

### 16.3 TTL storage and semantics
* Authoritative `expire_at_unix_ms` lives in the record header; the RAM hint (9.5) is a lower bound used only to find
  candidates. Any write may set, shorten, lengthen or clear it (LC-10). SET without KEEPTTL clears; KEEPTTL copies the
  old value; RENAME carries it; APPEND/SETRANGE/INCR keep it.
* EXPIRE family: `t <= 0` (relative) or absolute time in the past deletes the key (normal delete, durable rule) and
  returns 1. GT/LT treat "no TTL" as infinite. All comparisons of two expiry values live in `cmd/expire.rs`.
* TTL/PTTL/EXPIRETIME/PEXPIRETIME read the header (access M).

### 16.4 Lazy and active expiry
* **Lazy:** any `exec` that finds `expire_at <= now_ms` treats the key as absent and performs an *expire without
  write*: the index entry and counters are removed; the record becomes dead once `expire_at <= clock_floor_durable`
  (at most `clock-floor-interval` later) and is droppable earlier anyway (absence is the correct state). No page write
  is issued. In cluster mode the primary assigns a seq and replicates `ReplOp::Expired{db, key}`.
* **Active:** each partition sweeps its tables round-robin, RAM only: SIMD compare of hint planes against `now`
  (64 Ki slots per tick budget). Candidates are verified by reading the record (token bucket
  `active-expire-reads-per-sec / P` per partition) and either expired without write or have their hint re-encoded.
  This scans individual keys in RAM; it never scans the drive (LC-12).
* Replicas never expire keys on their own; they hide logically expired keys on reads and remove them when the
  primary's `Expired`/`Del` op arrives.
* Recovery treats `expire_at <= max(now_unix_ms, clock_floor)` as dead.

### 16.5 FLUSHDB / FLUSHALL
1. The receiving worker broadcasts `XMsg::Flush{db | all, req}` to every partition (standalone) or to every owned
   partition (cluster; replicated as `ReplOp::FlushDb` in each partition stream).
2. Each partition, at its FIFO position: `cutoff = next_seq` (every existing record has a smaller seq); asks the meta
   thread to write `flush_cutoff[db] = cutoff` (all dbs for FLUSHALL) durably; drops the db's tables from RAM; zeroes
   per-db counters; marks cached records of that db with `seq < cutoff` Droppable; notifies WATCHers.
3. The reply is sent when every partition's meta write is durable (E5).
4. Space: FLUSHALL resets the partition allocator: every SMALL/EXTENT AG it owns is released (contents dead by cutoff)
   once no write is in flight to it; refilling re-acquires AGs and fresh pages only by keyed draws (§11.1, §11.3), so
   the refilled partition is not laid down in address order. This is the client's FLUSHALL (also when a replica
   applies it from the stream), never a step of replica sync (LC-39). FLUSHDB of one db (others non-empty) starts a paced read-only *reclaim walk*
   (token bucket `reclaim-iops`) that loads the partition's pages/extent headers to recompute free space; until walked,
   that space is not reusable (INFO `flush_reclaim_pending_pages`).
5. Replicas apply the stream's `FlushDb` at its seq with `cutoff = op.seq`.

### 16.6 SCAN (`cmd/scan.rs`, partition `scan_step`)
* Cursor (u64): `partition:16 << 48 | slot_local:16 << 32 | fp32`, a single scalar position. `slot = slot_local * P +
  partition`. 0 = start/end.
* A call is routed to the worker owning `partition` and runs entirely on that one thread (LC-18):
  1. Walk slot tables `slot_local, slot_local+1, ...` of `(db, partition)` (cluster: slots that the gate's
     `slot_filter()` says this node serves; at most 4096 empty tables per call) with
     `SlotTable::scan_pages(fp_from, max_entries = COUNT, vtype_filter, &mut refs) -> ScanSpan{fp_lo, fp_hi_excl,
     next}`. The walk consumes whole equal-fp32 groups of entries in fp order, applies the TYPE filter to each entry
     before anything is emitted, and pushes only **de-duplicated page references** `PageRef{page36, kind, class}`; it
     stops after `COUNT` entries or `SCAN_STEP_MAX_PAGES = 32` distinct pages (compile-time constant). Nothing
     carrying a per-element fingerprint, hash or entry leaves the walk; the output is the page list plus two scalar
     bounds.
  2. Prefetch the referenced pages/extent headers (at most 32, the constant above).
  3. `exec`: for each page, compute `KeyHash` of each record's key from the bytes read from SSD and keep the record iff
     its slot is the table's slot, `fp_lo <= fp32 < fp_hi_excl`, it is the key's Live version (or its staged Put in
     the pending overlay), it is not expired, its vtype passes TYPE, and its key matches MATCH (Redis glob); emit the
     real key bytes.
  4. Next cursor = `fp_hi_excl` of the span (or the next table); at the end of the partition -> `(partition+1, 0, 0)`;
     after the last partition -> 0.
* Guarantees: every key present for the whole iteration is returned; no key is returned twice (each key's fp32 lies in
  exactly one span, and fingerprint order is stable under growth, split and shrink). COUNT bounds work, not results.
  Invalid cursor text -> `-ERR invalid cursor`.
* KEYS: SCAN steps with COUNT 1000 over all partitions strictly one after another (at most one step in flight per KEYS
  call, test `keys_sequential`), each on its owner, results accumulated in the requesting worker; O(N) reads. No
  SCAN-family request is ever fanned out across workers.
* MATCH and TYPE are fixed built-in key-only filters; SCAN/KEYS never return values or derived data (LC-17).
* RANDOMKEY: random partition, random non-empty table (`nonempty: Vec<u16>` per partition/db with position map),
  one random entry, read; up to 16 retries on expired/false-positive.

---------------------------------------------------------------------------------------------------------------------

## 17. Crash consistency: model, edges, proof sketch

### 17.1 Failure model
* **kill -9 / panic / OOM-kill:** I/O already submitted to the kernel completes or fails; unsubmitted SQEs are lost;
  completed writes are in the device (possibly in its volatile cache).
* **Power loss / kernel crash:** writes not covered by a completed flush (or FUA) may be lost; a write in flight may be
  torn at **byte** granularity (each byte old or new; stronger than the sector model; real devices tear at sector or
  atomic-unit granularity). Exception: in atomic mode (§13.1) a page-image write issued with `RWF_ATOMIC` on a
  device that verified atomic 4 KiB writes is all-or-nothing. Devices honour FLUSH/FUA; completed-and-flushed data is
  not silently lost; misdirected writes are detected (page id + location-bound crc) but not tolerated.
* The model is exercised by `SimDevice` in the simulator and by the RAM-only `faultdev` block device in real-process
  tests (§25.4); no tool records the write stream (LC-33).

### 17.2 Durability edges (A must be durable before B is issued); asserted in debug builds and by the simulator
| # | A | B |
|---|---|---|
| E1 | page image W_t(P) | W_t+1(P) |
| E2 | new copy of K (page Q or extent) | dropping K's old copy |
| E3 | all CHUNK extents of a value | its CHUNKMAP write |
| E4 | image that replaces/deletes an extent or chunkmap, and no GET pin on it | reuse of the old extent's pages |
| E5 | partition meta with new `flush_cutoff` (all partitions involved) | FLUSH reply; reuse of the flushed space |
| E6 | every TXN record and marker of txn t | `COMMITTED(t)`; then reply, replication commit, phase 2 |
| E7 | superblock with CLEAN=0, boot_gen+1 | first data-page write after startup |
| E8 | snapshot sections and header | superblock with CLEAN=1 naming it |
| E9 | every other copy of K durably gone | dropping K's newest copy (delete) or retiring K's txn marker |
| E10 | aborted-txn records zeroed; committed txns' `COMMITTED` listing durable (phase 2 not required) | `resolved_w` passing them; dropping their ids from slot copies and returning their credits only after that `resolved_w` is durable |
| E11 | AG table entry (acquire) | first write into the AG; AG FREE entry -> reuse by another partition |
| E12 | `seq_floor` reservation | assigning seqs >= the previous floor |
| E13 | `clock_floor` value | treating records with `expire_at <= clock_floor` as dead |
| E14 | zeroing of K's older tracked copy (stale page copy, previous extent run) | issuing a write that would create a further copy of K (move, extent update into that run or elsewhere) |

### 17.3 Page theorem
**Atomic mode:** the page after a crash is M or N whole; by E1 and the build rules each is a state the page had (before
or after N), and cross-location edges E2/E9/E14 are unchanged, so every key resolves to its state before or after N.
**Two-image mode:**
For a SMALL page with durable image M and in-flight image N built per 13.4, every byte mix X of M and N resolves every
key K (maximum seq over K's valid records on media, markers and dead rules applied) to K's state before N or after N.
1. Bytes of Protected records are equal in M and N (R1), so they survive in X. A record of M is dropped only when
   (13.3) a newer copy of its key is already durable (Superseded: K's resolved state is unaffected), or K's N-state is
   "absent" and the record is K's only copy (E9) (then X yields present-old or absent-new), or it is dead (no
   effect).
2. A record that validates in X consists entirely of M bytes or entirely of N bytes: a mixed image fails the keyed,
   location-bound crc (probability of a false accept 2^-32 per torn record), and the tiling parser never interprets
   bytes inside an accepted record.
3. Records present in N and not in M are exactly the newly placed versions (seq greater than every earlier version of
   their key) and txn markers; therefore the maximum over X is K's M-maximum or K's N-version.
4. Keys of one page resolve independently; multi-key atomicity is provided by the intent table (17.5).

### 17.4 Cross-location ordering and no-resurrection
* **Acked writes survive:** a reply is released after the image (or extent) holding the write is Written (kill -9
  model) or Durable (power model); a durable record is dropped only after a newer copy of its key is durable or the
  key's deletion became the latest state (13.3).
* **Acked deletes never resurrect:** the reply waits until the newest copy is durably gone or superseded (D2); by E9
  no older copy existed at that time; copies that remain are dead permanently (E5, E13, E10).
* **Moves/extents:** E2 means at every instant at least one durable copy of the newest acked version exists; old copies
  have lower seq and lose the maximum, until zeroed. R3 and E14 keep the number of copies of a key on media at two or
  fewer (I11, I12), so recovery resolves a key among at most two copies.
* **Expired keys never return:** recovery uses `max(now, clock_floor)`; E13.
* **No regression across restarts:** seqs are monotonic (E12, terms), so post-restart writes always beat older records left on media.

### 17.5 Multi-key atomicity
A txn's records are invalid unless `COMMITTED(t)` is durable (or `t <= W`); old versions remain Protected until then
(E6). Crash before the commit: all of the txn's records are dead, every old version is intact. Crash after: all records
valid; markers delete their keys; old versions lose by seq. The commit slot write is a single-sector A/B write: torn ->
the other copy is the previous state (the slot's earlier open ids, all still committed), i.e. the new batch is "not
committed". A slot copy drops ids only after a durable `resolved_w` covers them (E10), so no committed txn ever loses
its proof of commit.

### 17.6 Fixed-location metadata
Every block is an AbBlock with two copies; the writer alternates; a torn write invalidates only the copy being written,
whose previous content was superseded by the other copy anyway. Readers take the valid copy with the higher
`block_seq`. Multi-block orderings use E5, E7, E8, E10-E14. The snapshot regions are not AbBlocks: a region is
rewritten whole and is trusted only when the superblock names it with SNAPSHOT_VALID and the header and section crcs
match (E8).

### 17.7 Torn-write outcomes
| Object | Outcome after power loss mid-write |
|---|---|
| SMALL page image, atomic mode | M or N whole (17.3) |
| SMALL page image, two-image mode | per key old or new (17.3) |
| fresh SMALL page first write | new records lost (were not durable, not acked in written/flushed modes); page holds no protected record |
| VALUE/CHUNKMAP/CHUNK extent being written into another run | crc fails, ignored; old version intact (E2/E3) |
| extent rewritten in place (atomic mode, <= atomic unit) | old or new whole |
| extent first-sector zeroing | the old extent may still validate: for an update it has a lower seq than the durable new copy (E2); for a delete it was the only copy (E9) and the delete was not yet acknowledged |
| superblock / AG table / meta / intent header | other copy valid |
| intent slot copy | other copy (previous state) valid -> txn not committed |
| snapshot region | SNAPSHOT_VALID not set (superblock still names the other region, whose snapshot was already invalidated at startup by E7) -> full scan |

---------------------------------------------------------------------------------------------------------------------

## 18. Recovery and restart

### 18.1 Startup sequence (`kvd` main + `kv-storage/src/recovery.rs`)
1. Probe and open devices (`devprobe.rs`: substrate rules of LC-34, exit 1 on any violation; page-update mode,
   §13.1; cache mode, §13.8). `O_DIRECT`, files with `flock(LOCK_EX|LOCK_NB)`, block devices `O_EXCL`. Read
   superblocks A/B; choose the newest valid; check `set_uuid`, device indexes, geometry. Read AG tables (per block
   A/B), partition metas, node meta, intent header and slots.
2. Start workers and listeners at once. Until a partition is Ready, commands needing it reply
   `-LOADING Redis is loading the dataset in memory`; INFO, CONFIG GET, CLIENT, HELLO, AUTH, SELECT, QUIT, RESET,
   COMMAND are served.
3. If CLEAN and SNAPSHOT_VALID and the snapshot header crc matches: clean path (18.2); otherwise full scan (18.3).
4. Write superblock `boot_gen+1, CLEAN=0, SNAPSHOT_VALID=0` (E7) before any data write.
5. Partitions become Ready individually; post-recovery tasks start (18.4).

### 18.2 Clean path
Each worker reads its partitions' sections from the snapshot region the superblock names (1 MiB reads, QD 8),
verifies section crc, bulk-builds slot tables and
hint planes, restores allocator state, sets `next_seq` from the section and `seq_floor`. Any section error -> the whole
node falls back to the full scan. 1B keys: ~10 GB entries + 0.1 GB allocator at 3.5-6 GB/s: ~3-5 s, build ~2 s on
32 workers.

### 18.3 Full scan (crash path; correct by construction)
Per partition p, on its worker, over every AG the AG table assigns to p (FREE AGs are skipped; the snapshot regions
lie outside the AG area):
1. Read the AG in 1 MiB reads (`recovery-qd`, default 16 per worker).
2. SMALL pages: header check (magic, page id, partition == p; else the page is foreign/garbage and holds only dead
   data). Tiling parse with crc. For each record: `slot = key_slot(key)` (must satisfy `slot % P == p`, else dead),
   `KeyHash`; dead filter (cutoff, TXN id > W not committed) — **expiry is NOT part of the per-record dead filter**.
   Same-key records inside the page (at most two, R3; a third can only appear in a page torn by power loss): keep the
   higher seq (a committed TXN marker wins as "absent"; an expired record still competes on seq); the loser is
   Superseded and is zeroed by the page's first build. Insert the survivor into its slot table, flagged `expired` if
   `expire_at <= max(now, clock_floor)`; if an entry with the same `fp32`/`x8` already exists, record `(db, slot, fp)` in
   `conflicts` (both entries kept). Accumulate per-page occupied bytes.
3. EXTENT AGs: page by page: a valid header (magic, page id, kind, `n_pages` within the AG) -> read the rest of the
   extent if needed, verify crc; VALUE/CHUNKMAP become entries (class from `n_pages`), CHUNK goes to a chunk table;
   skip `n_pages`. Invalid -> next page.
4. Conflicts: re-read the pages/extent headers of every entry in each conflict group, group records by actual key,
   keep the max seq per key (marker = absent; expired records compete on seq like any other), remove the losers'
   entries, add losers to `stale_copies`.
4a. Expired survivors (v1.2 fix): after steps 2-4, every entry flagged `expired` is the newest copy of its key, so the
   key is absent: remove the entry from the index. If the key has stale copies (from step 4 or same-page losers), keep
   the expired record's bytes reserved and register it as the key's newest copy in `stale_copies` handling so that it
   is dropped only after the older copies' zeroing is durable (§16.2 rule D1); otherwise its bytes are free. Never let
   an older unexpired copy win because the newest copy has expired.
5. Chunkmaps: every referenced chunk must exist with the referenced crc, else the chunkmap is corrupt (reported).
   Unreferenced chunks are free.
6. Allocator: `page_free` from live + stale-copy bytes (dead bytes are free); extent `alloc/start` bits from live and
   stale extents. Hint planes from `expire_at`. Counters.
7. `next_seq = max(seq_floor, max_seq_seen + 1)` (standalone). Cluster: see 18.4.
8. Peak RAM = final index + conflict/stale lists (no per-record vectors).

### 18.4 Cluster additions
* Records with `seq > repl_commit_seq[p]` that win their key are collected (with key bytes) into `maybe_divergent[p]`
  (bounded by roughly 50 ms of writes). The node rejoins via the handshake (§23.9), learns the takeover points of past
  terms, and requests REPAIR images for those keys whose seq lies beyond the takeover point of its term.
* A node whose shutdown was clean has no divergence (graceful shutdown hands off primaries and drains, §23.12).
* After the coordinator pushes the assignment, keys of slots the node no longer owns are purged (paced deletes).

### 18.5 Post-recovery tasks
Scrub `stale_copies` (paced by `scrub-iops` because no command waits for them; a command on such a key whose
completion needs the copy gone moves that key's scrub to the foreground path, §16.2); retire txn markers found on
media together with the older copies of their keys (event driven, unpaced, bounded by the 3840 txn credits); zero
records of aborted txns then advance `resolved_w`; start the hint-rebase and expiry sweeps; (cluster) purge non-owned
slots.

### 18.6 Restart time (1B keys, 20 B keys, 300 B values, 10 records/page, ~100M pages, ~410 GB allocated)
| Case | Work | Gen4 1x NVMe 6.5 GB/s | 2x NVMe | 2.5 GB/s device |
|---|---|---|---|---|
| clean | read ~10 GB snapshot + build | ~4-6 s | ~3-4 s | ~6-8 s |
| crash | read ~410 GB (at 90% fill: ~360 GB) + parse (~150 ns/record / 32 workers ~5 s) | ~55-63 s | ~28-32 s | ~145-165 s |
Aerospike CE cold start scans its whole used footprint (records at 50% fill plus stale copies), i.e. about twice the
bytes for the same data. Incremental crash restart via change bitmaps is designed in Appendix B (future).

---------------------------------------------------------------------------------------------------------------------

## 19. Background tasks and pacing (all per partition unless noted; LC-15, LC-16)
| Task | Pacing | Trigger |
|---|---|---|
| page writer (builds + submits images, including every foreground scrub a command waits for, §16.2, and phase-2 txn writes) | none: it is the foreground write path, event driven | dirty page, previous image durable |
| extent old-copy zeroing after an update (§14.4) | none, event driven | new extent durable |
| flush (batched) | none, event driven, one in flight per device per worker | completed unflushed writes |
| stale-copy scrub of copies no command waits for (post-move page cleanup, post-recovery stale copies) | `scrub-iops` (node total, default 4000/s) | `stale_copies` non-empty |
| hole warming | `warm-iops` (node total 8000/s) | allocator lacks cached pages with space |
| active expiry verification reads | `active-expire-reads-per-sec` (node total 20000/s) | hint <= now |
| hint rebase, expiry RAM sweep | 64 Ki slots per tick | hourly / continuous |
| FLUSHDB reclaim walk | `reclaim-iops` (node 4000/s) | single-db FLUSHDB |
| AG release / discard (meta) | `ag-release-per-sec` 20, `discard-mbps` 200 | empty AG |
| clock floor persist (meta) | every `clock-floor-interval-ms` (1000) | always |
| replication watermark persist (meta) | every 50 ms when changed | cluster |
| txn commit batching and resolution (meta) | event | commit requests; COMMITTED listing durable |
| full sync / migration snapshot reads, sync-done deletes of unconfirmed keys | static token buckets `sync-mbps` (200), `sync-iops` (5000) per stream (migration: `rate_mbps` if the operator set one) | operator command |
| slot purge after migration / reassignment | `purge-iops` (node 4000/s) | migration done |
Optional `latency-controller yes`: every 100 ms, `scale = clamp(EWMA(target_p99_read_us / measured_p99_read_us),
0.1, 1.0)` multiplies every background bucket rate above (continuous proportional control). Inputs: device read
latency only; never queue depths, lag or client state (LC-16). Nothing on the completion path of a received command is
paced or scaled (LC-15).

---------------------------------------------------------------------------------------------------------------------

## 20. Configuration and CLI (`kv-node/src/config.rs`)

### 20.1 CLI
```
kvd [CONFIG_FILE] [--<name> <value>...]           # Redis-style: file first, then overrides; unknown name -> exit 1
kvd format --device PATH[:SIZE] [--device ...] [--partitions 64] [--cluster] [--snapshot-region SIZE|PCT] [--force]
kvd inspect --device PATH [--ag N] [--page ID] [--show-data]   # dump superblock, AG table, metas, a page (read-only;
                                                               # record keys/values redacted unless --show-data)
kvd check --device PATH...                         # offline full scan + invariant check, exit 0/1
```
`kvd --port {port} --data-dir {dir}` (the `tests/compat` launch template) uses `{dir}/kvd.data` of `data-size` bytes,
formatting it on first start (`auto-format yes` applies only to `data-dir` files, never to `device` entries).
`format` zero-fills files whatever their size and, like every open, refuses substrates outside LC-34 (raw block
devices, dm `linear`/`crypt`, md raid0/1/10 without extra cache members (exact rules: LC-34), or files on XFS or ext4 in its default ordered
data mode). The deployment guide (`docs/DEPLOY.md`) and every benchmark config carry this list.
Shipped systemd units (`dist/kvd.service`, `dist/kvc.service`): `LimitMEMLOCK=infinity`, `MemorySwapMax=0`,
`LimitCORE=0`, `LimitNOFILE=1048576` (LC-23, LC-32).

### 20.2 Parameters (file syntax `name value ...`, `#` comments, `include path`; R = settable by CONFIG SET)
| Name | Default | R | Notes |
|---|---|---|---|
| bind | 127.0.0.1 | - | list |
| port | 6379 | - | |
| unixsocket | "" | - | M1.3 |
| tcp-backlog / tcp-keepalive / timeout | 511 / 300 / 0 | R(timeout) | timeout = idle client seconds |
| maxclients | 10000 | R | |
| requirepass | "" | R | |
| databases | 16 | - | forced 1 in cluster mode |
| threads | auto | - | <= P |
| data-dir / data-size / auto-format | . / 4gb / yes | - | test/dev convenience |
| device | none | - | `device PATH [SIZE]`, repeatable, set at format |
| format-partitions | 64 | - | P: 16/32/64/128, format time; cluster-wide constant |
| cache-size | auto | R | |
| durability | written (standalone), flushed (cluster) | R | async/written/flushed (§14.7) |
| flush-method | auto | - | auto/fdatasync/dsync/none |
| device-cache | auto | - | auto/write-through/write-back |
| write-granularity | auto | - | auto/page/lba |
| page-update | auto | - | auto/two-image/atomic-overwrite (§13.1); atomic-overwrite refuses to start unless verified |
| page-slack-cap / page-slack-atomic | 1024 / 128 | R | bytes, 0-2048 (two-image / atomic mode) |
| max-storage-fill | 0.95 | R | fraction of the AG area |
| snapshot-on-shutdown | yes | R | |
| snapshot-region | 3% | - | per region, per device, set at format (`kvd format --snapshot-region`); 0 = none |
| max-pending-per-conn / client-output-buffer-limit | 4096 / 64mb | R | single thresholds |
| proto-max-bulk-len | 512mb | R | |
| max-dirty-bytes / large-value-memory | 256mb / 2gb | R | large-value-memory: maximum; its half is the soft limit checked only before a large command is consumed (§6.9) |
| active-expire-reads-per-sec, scrub-iops, warm-iops, reclaim-iops, purge-iops | 20000, 4000, 8000, 4000, 4000 | R | node totals |
| discard / discard-mbps / ag-release-per-sec | yes / 200 / 20 | R | |
| latency-controller / latency-target-p99-us | no / 1000 | R | |
| recovery-qd / clock-floor-interval-ms | 16 / 1000 | - | |
| hash-max-bytes / extent-cache-max | 16mb / 0 | R | |
| loglevel / logfile | notice / "" | R(loglevel) | tracing; text diagnostics only, never user bytes (LC-32); `logfile` is the one `O_APPEND` file (LC-02 exemption, `kv-node/src/log.rs`); "" = stderr |
| core-dumps | no | - | no: `PR_SET_DUMPABLE 0`; yes: dumps allowed, user-data regions still `MADV_DONTDUMP` |
| metrics-port | 0 (off) | - | Prometheus text on agent thread |
| cluster-enabled | no | - | |
| cluster-control-port / cluster-repl-port | port+10000 / port+20000 | - | |
| cluster-announce-ip / cluster-announce-port | "" / 0 | - | |
| cluster-auth-token | "" | - | required in cluster mode |
| repl-backlog-size | 32mb | - | per partition (RAM ring; hugetlb or mlocked, `MADV_DONTDUMP`; never persisted) |
| repl-ack-timeout-ms / repl-heartbeat-ms | 500 / 10 | R | |
| sync-mbps / sync-iops | 200 / 5000 | R | static token-bucket rates for full sync and migration streams |
CONFIG GET also answers compatibility keys clients query: `save ""`, `appendonly no`, `maxmemory 0`,
`maxmemory-policy noeviction`, `cluster-enabled`, `databases`; unknown patterns -> empty array (Redis behavior).

---------------------------------------------------------------------------------------------------------------------

## 21. INFO and metrics (`kv-node/src/cmd/info.rs`, `metrics.rs`)
Per-worker counters are plain `u64` in a `#[repr(C)]` struct published through `AtomicU64` mirrors every tick;
latencies in per-worker `hdrhistogram::Histogram<u32>` (1 us - 60 s, 3 significant digits), merged on demand.

| Section | Fields |
|---|---|
| server | `redis_version:7.2.0`, `redis_mode:standalone\|cluster`, `kvd_version`, `os`, `arch_bits`, `process_id`, `run_id`, `tcp_port`, `uptime_in_seconds`, `uptime_in_days`, `executable`, `config_file`, `io_threads_active:0` |
| clients | `connected_clients`, `maxclients`, `blocked_clients:0`, `backpressured_clients` |
| memory | `used_memory` (RSS), `used_memory_human`, `maxmemory:0`, `index_bytes`, `index_bytes_per_key` (segments, directories, hint planes only), `alloc_state_bytes` (reported separately), `cache_bytes`, `cache_used_bytes`, `dirty_bytes`, `large_value_bytes`, `repl_backlog_bytes` |
| persistence | `loading`, `recovery_mode:snapshot\|scan`, `recovery_duration_ms`, `recovery_bytes_scanned`, `last_shutdown_clean`, `rdb_changes_since_last_save`, `rdb_last_save_time`, `rdb_last_bgsave_status:ok`, `aof_enabled:0`, `durability`, `flushes_total`, `flush_latency_p99_us` |
| stats | `total_connections_received`, `total_commands_processed`, `instantaneous_ops_per_sec`, `keyspace_hits`, `keyspace_misses`, `expired_keys`, `evicted_keys:0`, `total_net_input_bytes`, `total_net_output_bytes`, `xmsg_hops`, `rejected_connections` |
| replication | `role:master\|slave`, `connected_slaves:<n>`, `master_link_status:up\|down` (replicas) only; no shard id, term, seq, lag position or peer identity (LC-24, LC-28). Operators get shard, ISR and lag details from `kvadm status`. |
| keyspace | `dbN:keys=K,expires=E,avg_ttl=0` |
| cluster | `cluster_enabled:0\|1` |
| commandstats / latencystats | `cmdstat_<name>:calls=,usec=,usec_per_call=,rejected_calls=,failed_calls=`; `latency_percentiles_usec_<name>:p50=,p99=,p99.9=,p99.99=` |
| kvd | `devices`, `device_bytes`, `page_update_mode:atomic\|two-image`, `ags_small/extent/free`, `snapshot_region_bytes`, `snapshot_skipped_too_large`, `fill_pct`, `pages_cached`, `cache_hit_ratio`, `page_reads`, `page_writes_{update,insert,delete,move,scrub,txn}`, `extent_writes`, `host_bytes_written`, `user_bytes_written`, `write_amp`, `sector_writes`, `full_page_writes`, `moves`, `fp_false_positive_reads`, `stale_copies`, `foreground_scrub_writes`, `txn_markers`, `txn_credits_in_use`, `large_value_backpressured_conns`, `flush_reclaim_pending_pages`, `corrupt_pages`, `device_read_p99_us`, `device_write_p99_us`, `bg_scale` |
Prometheus names: `kvd_<section>_<field>` on `metrics-port`. Labels are limited to `worker`, `partition`, `device` and `command`; no metric or label carries node ids, shard ids, terms, slot sets or user bytes (LC-24, LC-32).

---------------------------------------------------------------------------------------------------------------------

## 22. Error handling
| Condition | Behavior |
|---|---|
| Protocol error | Redis error text, flush, close (§7.2) |
| Read I/O error | retry once; then `-ERR storage I/O error`; counter; >= 16 errors/min on a device -> device degraded |
| Write I/O error | the page's tickets fail with `-ERR storage I/O error`; the partition enters ReadOnly (writes fail fast, reads served); cluster: `health.failed = true` in renewals -> coordinator treats a primary as failed (§23.10) |
| Corruption | §15; never served; operator `kvadm replica resync` (cluster) or restore |
| Device full | `-OOM ...` (§11.6) |
| Memory exhaustion | allocation failure aborts (Rust default, `panic = abort` in release); acknowledged data is durable by construction |
| Panic | abort; supervisor restarts; recovery path. No core file by default (`core-dumps no`, unit `LimitCORE=0`); panic messages carry no user bytes (LC-32) |
| Bad config / format mismatch | exit code 1 with a one-line reason before listening |
| Clock jumps | TTL uses wall clock; leases/timeouts use `CLOCK_MONOTONIC`/`CLOCK_BOOTTIME`; recovery uses `max(now, clock_floor)` |
| Peer protocol violation (repl/control) | close the connection, log, counter; never panic |

---------------------------------------------------------------------------------------------------------------------

## 23. Cluster design (M2, M3)

### 23.1 Components and deployment
| Component | Role | User data | Serves topology |
|---|---|---|---|
| `kvc` coordinator (3 or 5 processes, one per zone) | owns the versioned `ClusterState`; whole-state replication (WSR) among coordinators; leases; failure detection; failover and ISR decisions; migration orchestration; pushes assignments; client topology service; admin API. A topology service and control plane, not a database node: no keyspace, no user data, no data-storage layer (LC-38) | no | yes (only it) |
| `kvd` data node | one role in one shard (primary or replica) or standby; RESP server; page store; replication; migration endpoints | yes | no |
| `kvadm` | operator CLI (RESP to the coordinator admin port) | no | n/a |
* Ports. Coordinator: `client-port` (RESP, default 7000), `peer-port` (WSR), `admin-port`. Node: `port` (RESP),
  `cluster-control-port` (coordinator dials it), `cluster-repl-port` (replicas and migration destinations dial it).
* A host may run several `kvd` instances on separate devices so that primaries and replicas interleave across hosts.
* Authentication: one `cluster-auth-token` shared by coordinators and nodes (constant-time compare in every
  handshake); `admin-token` for kvadm; `requirepass` (client password) is a cluster parameter pushed to nodes. TLS is
  (future).

### 23.2 The slot map object, versions and terms (`crates/kv-topo/src/state.rs`)
```rust
pub struct ClusterState {
    pub version: u64,                       // +1 per committed batch of transitions; the only map version
    pub cluster_id: [u8; 16],               // random once at `kvc init`; never per transition
    pub params: ClusterParams,              // partitions P, lease_ms 1500, renew_ms 100, fail_grace_ms 200,
                                            // repl_ack_timeout_ms 500, client_password
                                            // (no coordinator list: peer lists live in each coord.meta, §23.3)
    pub nodes: Vec<NodeRecord>,             // operator order. {id: NodeId, ctrl, client, repl, zone, priority: u8,
                                            //  flags: {fail, maint}, partitions: u16, added_version}
    pub shards: Vec<ShardRecord>,           // {id: ShardId, primary: Option<NodeId>, members: Vec<Member>,
                                            //  term: u32, min_isr: u8, history: Vec<TermEnd> (last 4),
                                            //  status: Serving|NoPrimary|Promoting}
    pub slots: Box<[u16; 16384]>,           // ShardId or 0xFFFF; run-length encoded on the wire
    pub migrations: Vec<Migration>,         // {id: u64, slots: SlotRanges, src, dst, phase, rate_mbps: Option<u32>
                                            //  (a static token-bucket rate, never a priority)}
}
pub struct Member { pub node: NodeId, pub state: MemberState /* InSync|Reconciling|Syncing|CatchingUp|Failed|NeedsFullSync */,
                    pub in_isr: bool }
pub struct TermEnd { pub term: u32, pub takeover: Vec<u64> /* per partition: last seq of `term` kept by the new primary */ }
```
* Integer counters only (LC-27): `version` (map), `term` per shard (+1 per promotion; high 24 bits of every seq
  written in that term). No random value is generated per transition.
* Clients never receive terms or seqs: CLUSTER NODES config-epoch column = 0; CLUSTER SHARDS `replication-offset` = 0;
  CLUSTER INFO `cluster_current_epoch` = `version` (one global map counter), `cluster_my_epoch` = 0 (LC-28).
* `NodeId([u8; 20])` (shown as 40 hex), `CoordId`, `ShardId(u16)` implement `Eq + Hash` and **not** `Ord`/`PartialOrd`
  (NodeId, CoordId). Collections are `Vec`s in operator order or hashbrown maps; canonical encoding = postcard of the
  struct with Vecs in stored order.
* Transitions (`transition.rs`): `Admin(AdminOp)` (the only kind that changes `nodes`, member lists, `slots`
  assignment of unassigned slots, migrations start/resume/abort, params), `Health{node, fail}`,
  `Promote{shard, primary, term, isr, term_end}`, `Isr{shard, term, node, add}`, `MemberState{shard, node, state}`,
  `Migration{id, phase, slots_to_dst: bool}`. `ClusterState::apply(&self, &[Transition]) -> Result<Self, Reject>` is
  pure and deterministic.

### 23.3 Coordinator HA and configuration state without a log (`crates/kv-coord/src/wsr/`)
**Whole-state replication (WSR):** Raft-style election with single-proposal (depth-one) replication of the whole next
state; one accepted proposal is held in the inactive half of two fixed state halves.
* Files (pre-allocated by `kvc init`, never extended; O_DIRECT `pwrite` + `fdatasync`; substrate rules LC-34; LC-02):
  `coord.meta` = 2 x 4 KiB AbBlocks `{term u64, voted_for: Option<CoordId>, peer_gen u64, peers: Vec<CoordRecord>
  (<= 7; {id: CoordId, id40: [u8; 20], peer, client, admin})}`;
  `coord.state` = 2 x 8 MiB halves `{magic, half_seq u64, committed {version, term, len, bytes}, accepted:
  Option<{version, term, len, bytes}>, crc32c}`. Load picks the valid half with the higher `half_seq`.
  Encoded state > 8 MiB - 64 KiB -> transition rejected (`StateTooLarge`).
* **Peer list (who votes) is not part of the voted value.** `ClusterState` holds no coordinator list. Each coordinator
  keeps `{peer_gen, peers}` in its own `coord.meta`, written only by the operator admin call
  `KVADM COORD SETPEERS <gen> <list>`, which kvadm sends to each coordinator in turn. A change adds or removes exactly
  one coordinator relative to generation `gen - 1`, and kvadm refuses the next change until every coordinator in the
  new list reports the new `gen` (`KVADM COORD PEERS`) **and** (v1.3, found by the model checker) a value has been
  committed under the new generation and every coordinator staying in the list has committed it: kvadm first asks the
  leader to commit a no-op value under the current generation (`KVADM COORD SYNC`) and then polls `KVADM COORD STATE`
  until each remaining coordinator reports that version. Without this step a 3 -> 4 -> 3 sequence can leave the
  majorities of generations 1 and 3 disjoint and elect two leaders in one term (regression test
  `generation_only_rule_is_unsafe`). WSR messages carry `peer_gen`; a coordinator grants votes to and
  accepts proposals from peers of its own `peer_gen` only, and counts majorities over its own list. With single-server
  deltas and at most two generations coexisting, the two generations cannot both assemble a majority (an added server
  is always in the newer generation; a quorum of each needs more servers than exist), so safety follows the
  single-server-change argument. Elections and commits read only the local list; no list is ever derived from who
  answered.
* Rules:
  1. Persist `coord.meta` before granting a vote or adopting a higher term.
  2. A follower persists `accepted` before acknowledging `Proposal{term, leader, peer_gen, value: (version, state)}`.
  3. Depth one: the leader proposes version v+1 only after v is committed; all queued transitions are batched into the
     next proposal (typical 1-3 ms per commit on NVMe: >= 300 commits/s).
  4. Vote granted only if the candidate's `(last_term, last_version)` >= the voter's (last = accepted if present,
     else committed), first come first served within a term; pre-vote and check-quorum enabled.
  5. A new leader first re-proposes its own `accepted` value (if any) under its new term (Paxos phase 2 with the
     highest accepted value), then proposes new batches.
  6. Side effects (assignment pushes, lease grants, redirects, admin replies) are produced only from committed state.
* Timers: election timeout uniform 500-1000 ms; leader beat (`Beat{term, leader, peer_gen, committed_version}`, no
  value) every 100 ms, among coordinators only. The leader is whoever first gathers a majority; ids are never compared
  (LC-27). Scheduling delays and slow state writes never cost the group its leader on their own: a leader keeps beating
  while its state files are written (only the beat step and current-term beat replies are stepped meanwhile, only
  beats are sent, every other message waits for the write in arrival order; never while its term or vote is being
  written); a follower likewise keeps answering the beats of its leader's term while it writes a state value (never
  its term or vote, and only while every message waiting for the write is one of that term from that leader; the beat
  itself is stepped after the write, ahead of later messages), so followers persisting every proposal on a slow disk
  never let the leader's check-quorum lease lapse; check-quorum steps a leader down only when no majority answered
  during a window of twice the minimum
  election timeout; a check-quorum or election timer that fires more than 100 ms late (the member did not run) is
  postponed once by one beat, so the replies and beats that queued up meanwhile are handled first.
* Followers serve client topology from their committed state if they heard from a leader within 2 s, else reply
  `-CLUSTERDOWN The cluster is down`. Admin commands on a follower are relayed to the leader: the follower forwards
  only the admin command and returns only the leader's reply text to kvadm.
* Coordinator membership changes: operator-only, one server at a time (`kvadm coord add/remove` are wrappers that run
  the SETPEERS sequence above). Replacing a coordinator is an online, sequenced kvadm procedure.
* Disaster recovery (majority lost): `kvadm coord export <file>` writes the committed state over one operator-named
  file (or over a fixed A/B pair `<file>.a`/`<file>.b`, alternating); it never accumulates a series of files.
  `kvc recover --from <state file|export>` boots a new single-member group (`peer_gen + 1`, peers = itself) at
  `version + 1` and **`coord_term = export coord_term + 2^32`** (documented), so every node accepts its assignments by
  `(coord_term, version)` order (§23.4) even when the export is stale. The operator reconciles roles with
  `kvadm status`; recovery never queries or polls data nodes for what they own (LC-26).
* Verification: Stateright model (`kv-sim/src/wsr_model.rs`) of rules 1-6 and of SETPEERS sequences (single-server
  deltas applied in any order, crash/restart, message loss): at most one value committed per version, committed state
  never lost, at most one leader per term across generations; plus deterministic simulation.

### 23.4 Control channel, assignment push, leases (`kv-ctrl`, `kv-coord/src/{push,lease}.rs`, `kv-node/src/cluster/agent.rs`)
* The coordinator **leader dials** every registered node's control port (nodes never dial or poll anyone for
  topology). Frames: `u32 len || postcard(msg)`, max 16 MiB.
* Handshake: `CoordToNode::Hello{cluster_id, coord_id, coord_term, auth}` -> `NodeToCoord::HelloAck{node_id?,
  incarnation: u64 (random per process start), set_uuid, partitions, ready: bool}` (exactly the §24.9 struct: identity
  and readiness only, no role, shard, term or slot field). A node that has never joined adopts `cluster_id` and the
  `node_id` from its first `Assign` and persists them in node meta. The coordinator learns ownership only from its
  own committed state, never from node replies.
* `Assign(NodeAssignment)`: pushed on every committed change of the node's projection. The node applies assignments
  in `(coord_term, version)` order, persists role/shard/fence/version in node meta (A/B, durable) before
  `AssignAck{version}`, and answers an older assignment with `Error(StaleAssignment)`, which has no contents.
* Replies to coordinator requests (`HelloAck`, `AssignAck`, `FenceAck`, `HandoffDone`, `Error`) never carry role,
  shard, slot or key-count fields (LC-26). Requests carry a `req` id; replies echo only it.
```rust
pub struct NodeAssignment {
    pub version: u64, pub coord_term: u64, pub node_id: NodeId,
    pub coordinators: Vec<CoordEndpoint>,          // client addrs for MOVED, rendered from the coordinators'
                                                   // static operator-set peer list (§23.3), in list order
    pub role: Option<ShardRole>, pub client_password: Option<String>, pub params: NodeParams,
}
pub struct ShardRole {
    pub shard: ShardId, pub term: u32, pub is_primary: bool,
    pub owned_slots: SlotSet,                      // 2 KiB bitmap
    pub primary_repl: Option<PeerAddr>,            // replicas: whom to dial
    pub members: Vec<(NodeId, MemberState, bool /*in_isr*/)>, // primary: its peers (no addresses needed: they dial in)
    pub min_isr: u8, pub history: Vec<TermEnd>,
    pub migrations: Vec<MigrationTask>,            // Out{id, slots, phase} | In{id, slots, src_repl, phase}
                                                   // (the destination dials the source, §23.13)
}
```
  No full slot map type exists in `kv-node` (LC-24/LC-26).
* Leases: the node sends `Renew{seq, incarnation, version_seen, health}` every `renew_ms` (100 ms) on the control
  connection, recording `t_send` (CLOCK_BOOTTIME). The leader answers `LeaseGrant{seq, lease_ms}` only while its own
  check-quorum lease is valid. Node: `lease_expiry = t_send + lease_ms * 0.95 - 10 ms`. Leader: the lease is held until
  `t_grant + lease_ms * 1.001 + 10 ms`. A primary (or readable replica) serves only while `now < lease_expiry`,
  re-checked right before each reply is released.
* A newly elected leader renews leases immediately but makes no promotion until `lease_ms + 1000 ms + fail_grace_ms`
  after winning.

### 23.5 Client bootstrap and redirects (`kv-coord/src/clientsvc.rs`, renderers in `kv-topo/src/render.rs`)
Seeds given to client libraries = coordinator client addresses (or one DNS name resolving to all of them).

| Command at a coordinator | Reply |
|---|---|
| CLUSTER SLOTS | Redis 7 format `[start, end, [ip, port, id40, {}], replicas...]` per contiguous range of one shard; primary first, then members with state InSync (Reconciling/Syncing members are not advertised). Coordinators are not listed. |
| CLUSTER SHARDS | per shard: `slots`, `nodes: [{id, port, ip, endpoint, role, replication-offset: 0, health: online\|loading\|failed}]` (health from replication state only, LC-30) |
| CLUSTER NODES | data nodes: `<id40> <ip>:<port>@0 <master\|slave>[,fail] <master-id\|-> 0 0 0 <connected\|disconnected> <slot ranges>`; plus one line per coordinator, rendered from the static peer list (§23.3), never from `ClusterState`, with no slots (`myself` only on the answering coordinator). Coordinator line format **R1 (preferred):** `<id40> <ip>:<port>@0 slave[,myself] - 0 0 0 connected`, a slotless replica entry whose master-id `-` matches no master, so Lettuce never sends master fan-out commands (DBSIZE/KEYS/SCAN/FLUSHALL) or replica reads to it. **R2 (fallback):** `master[,myself]` with no slots. The §25.6 matrix decides at M2.1 whether Lettuce follows MOVED to an R1 entry; the decision is recorded in §28. Never `noaddr`: Lettuce drops NOADDR nodes from its partitions (`DefaultClusterTopologyRefresh.validNode`) and then rejects MOVED to them (`validateClusterNodeMembership`, default true) - verified in Lettuce source. |
| CLUSTER INFO | `cluster_enabled:1`, `cluster_state:ok\|fail` (fail if any slot unassigned or a shard has no serving primary), `cluster_slots_assigned/ok/pfail/fail`, `cluster_known_nodes`, `cluster_size`, `cluster_current_epoch:<version>`, `cluster_my_epoch:0` |
| CLUSTER MYID / MYSHARDID / KEYSLOT | coordinator id40 / `0`x40 / slot |
| CLUSTER COUNTKEYSINSLOT / GETKEYSINSLOT | `-ERR the coordinator stores no keys` |
| HELLO, PING, ECHO, AUTH, SELECT 0, QUIT, RESET, READONLY, READWRITE, ASKING, COMMAND*, CLIENT*, INFO (server, clients, cluster) | protocol-compatibility replies of the topology service (HELLO `mode: cluster`, `role: replica` with R1 / `master` with R2; INFO server carries `server_kind:coordinator`) |
| keyed command (key specs from `kv-cmd`) | `-MOVED <slot> <primary ip:port>` from the committed map; slot unassigned or shard `NoPrimary`/`Promoting` -> `-CLUSTERDOWN Hash slot not served`; keys in different slots -> `-CROSSSLOT Keys in request don't hash to the same slot`. Bulk arguments are read and discarded. |
| MULTI / EXEC / DISCARD | MULTI `+OK`; queued keyed commands get MOVED; EXEC -> `-EXECABORT Transaction discarded because of previous errors.` |
| keyless commands clients fan out to "all masters" | R1: not in the coordinator's table (`-ERR unknown command`), since no client fans out to replica entries. R2 only: constant-literal compatibility stubs in `clientsvc.rs` (`const &[u8]` replies; no keyspace abstraction, no stored state): DBSIZE `:0`, KEYS `[]`, SCAN `["0", []]`, RANDOMKEY nil, FLUSHALL/FLUSHDB `+OK` (no-op), WAIT `:0`; others `-ERR unknown command` |
Rendered SLOTS/SHARDS/NODES replies are cached per `version`.

**Per-library behavior (verified against current client sources in the research scratchpad):**
| Library (minimum) | Bootstrap / refresh | Redirect handling | Settings |
|---|---|---|---|
| redis-py >= 5.0 `RedisCluster` | CLUSTER SLOTS against shuffled `startup_nodes`; ResponseError -> next node. Default `dynamic_startup_nodes=True` replaces seeds with nodes from SLOTS, after which refreshes reach only data nodes | `move_slot()` patches the slot to the MOVED target (coordinator, then owner); full re-init every `reinitialize_steps` (5) MOVEDs; TRYAGAIN retried; CLUSTERDOWN: sleep 0.25 s, re-init | **Required:** `startup_nodes=[all coordinators]`, `dynamic_startup_nodes=False`; before 7.2 the seeds by host name (below) |
| Jedis >= 4.0 `JedisCluster` | shuffled `startNodes` CLUSTER SLOTS | `renewClusterSlots` tries the MOVED connection (data node: error caught), then `startNodes`; `maxAttempts` 5 | **Required for <= 3 s failover:** 1 s connection and socket timeouts and `maxTotalRetriesDuration` 1 s (below); TRYAGAIN/CLUSTERDOWN surface as exceptions in ms-scale windows. Jedis 3.x unsupported |
| Lettuce >= 6.1 `RedisClusterClient` | CLUSTER NODES + INFO from seeds; needs a `myself` line | follows MOVED (maxRedirects 5) only to nodes in its partitions; coordinators are listed (R1 or R2), so MOVED-to-coordinator works | **Recommended:** `ClusterTopologyRefreshOptions.builder().dynamicRefreshSources(false).enableAllAdaptiveRefreshTriggers().enablePeriodicRefresh(Duration.ofSeconds(30))`; **required for <= 3 s failover:** `.adaptiveRefreshTriggersTimeout(Duration.ofMillis(500)).refreshTriggersReconnectAttempts(1)` (below) |
| go-redis v9 `ClusterClient` | CLUSTER SHARDS then SLOTS on shuffled addrs; all-fail -> back to seeds | MOVED -> LazyReload + retry (MaxRedirects 3; we need <= 2); TRYAGAIN/CLUSTERDOWN retried | **Recommended:** `ClusterSlots` func that queries the coordinators; add an `OnConnect` hook sending READONLY if replica reads are wanted; **required for <= 3 s failover:** `ReloadState` after a failed command (below) |
| ioredis >= 5 `Cluster` | CLUSTER SLOTS; `pool.reset()` drops coordinators; `readyCheck` sends CLUSTER INFO to data nodes (answered locally, §8.7) | MOVED: patch slot, `findOrCreate(target)`, refresh (reaches the coordinator) | none (expect harmless node-error events when a refresh first hits a data node) |
| redis-cli `-c` | n/a | follows MOVED | `--cluster` subcommands unsupported (use kvadm) |
The MOVED-to-coordinator rule costs one extra hop only on topology changes. ASK is never emitted (migration is copy +
one ownership flip + a ms-scale TRYAGAIN window, §23.13); `ASKING` is accepted as a no-op. All 16384 slots must be
assigned before clients bootstrap (redis-py `require_full_coverage=True`, Jedis). CI runs the matrix (§25.6).

**Client settings for a failover of at most 3 s as clients see it.** A primary that dies sends no MOVED: its
connections fail or time out, and a client sees the new primary only after it refreshes its slot map from a
coordinator. The server side takes about 1.75 s (§23.9 step 7); the settings below bound the client side so that a
write is retried against the new primary within the remaining time. They are the settings the matrix (§25.6) runs
with, and its `kill-primary` scenario asserts the 3 s bound with them.
* **Lettuce:** `ClusterTopologyRefreshOptions.builder().dynamicRefreshSources(false)
  .enableAllAdaptiveRefreshTriggers().enablePeriodicRefresh(Duration.ofSeconds(30))
  .adaptiveRefreshTriggersTimeout(Duration.ofMillis(500)).refreshTriggersReconnectAttempts(1)`. With the defaults
  (adaptive refreshes rate-limited to one per 30 s, a refresh triggered only after 5 reconnect attempts) a refresh taken
  just before the promotion is committed blocks the next one for 30 s. A 1 s command timeout
  (`TimeoutOptions.enabled(Duration.ofSeconds(1))`) keeps a write to the dead primary from waiting longer than that.
* **go-redis:** call `ClusterClient.ReloadState(ctx)` after a command that failed (it does not block). go-redis reloads
  its slot map only on MOVED/ASK or when the map is 10 s old, and a dead primary answers neither.
* **Jedis:** connection and socket timeouts of 1 s (`DefaultJedisClientConfig.builder().connectionTimeoutMillis(1000)
  .socketTimeoutMillis(1000)`) and `maxTotalRetriesDuration` of 1 s (the `JedisCluster` constructor argument after
  `maxAttempts`). With the default bound (socket timeout x `maxAttempts`) one SET was seen blocked for 13 s across a
  failover: the backoff sleeps scale with the time left and the slot map is renewed late.
* **redis-py:** the required settings of the table (`startup_nodes` = every coordinator, `dynamic_startup_nodes=False`)
  and a socket timeout of at most 3 s; CLUSTERDOWN and connection errors re-initialize from the coordinators. Before
  redis-py 7.2 a connection error to a command's target removes the startup node of that name for good (7.2 and later
  move it to the end of the list instead). With every coordinator down for longer than a lease, the data nodes
  redirect to coordinators (`-MOVED <slot> <coordinator ip:port>`), the client's command to that coordinator fails,
  and after one such failure per coordinator the client has no seed left and never reconnects. With redis-py < 7.2,
  give the seeds by host name (DNS names of the coordinators, or one name resolving to all of them): MOVED replies
  carry the IP addresses of the coordinator list, so a removed name never matches a seed. Treat
  `RedisClusterException` ("cannot be connected", raised while no coordinator answers) as retryable like
  `ConnectionError`.
* **ioredis:** `retryDelayOnClusterDown` and `retryDelayOnTryAgain` of at most 100 ms, `slotsRefreshTimeout` of at most
  2 s and a `commandTimeout` of at most 3 s.
Every library: retry a write that failed with a connection error, a timeout, TRYAGAIN or CLUSTERDOWN (it may or may not
have taken effect, G6); never treat such an error as a lost acknowledged write.

### 23.6 Data-node routing gate (`kv-node/src/cluster/ownership.rs`, applied in `dispatch/router.rs`)
For every command in cluster mode, before execution (and again at queue time inside MULTI):
1. Keyless: served locally on the node's own data (DBSIZE, SCAN, KEYS, RANDOMKEY, INFO, ...). Keyless writes
   (FLUSHDB/FLUSHALL) need role primary with a valid lease (replica: `-READONLY You can't write against a read only
   replica.`).
2. Keys in more than one slot -> `-CROSSSLOT Keys in request don't hash to the same slot`.
3. Primary, slot owned, lease valid, slot not sealed-out by a migration cutover -> execute. Writes when the committed
   ISR (including the primary) has fewer than `min_isr` members -> `-NOREPLICAS Not enough good replicas to write.`
4. Replica, connection READONLY, read-only command, slot in the shard, lease valid, partition not in full sync, and
   the replica's own committed member state (its projection carries its own member entry) neither Syncing (a learner,
   which may still hold another shard's copy of these slots) nor NeedsFullSync -> execute locally (no
   replication-state gate, LC-28).
5. Slot in `incoming_pending_seal` (migration destination after cutover, before SEAL is committed) ->
   `-TRYAGAIN Multiple keys request during rehashing of slot`.
6. Otherwise -> `-MOVED <slot> <coord_addrs[conn_id % n]>` from the pushed static coordinator list, never "the
   leader" or any other tracked state: on one connection the reply is identical for every slot the node does not
   serve, apart from the echoed slot number (golden test `unowned_slot_replies_identical`). A data node never names
   another data node (LC-24).
Command handlers never read `Ownership` fields; they call `gate()` and, for keyless walks (SCAN, KEYS, DBSIZE,
RANDOMKEY), `slot_filter()`, which return decisions, not slot sets (lint, LC-24).

### 23.7 Replication protocol (`crates/kv-repl`)
* **Unit:** one stream per (shard, partition): primary partition p -> partition p of every member (same P everywhere).
  Each member dials the primary's repl port once per partition (64 connections per member at P=64); the accepting
  worker hands the socket to the worker owning p.
* **Handshake:** member -> `Hello{cluster_id, auth, node_id, shard, partition, fence_term, last_term, applied: Seq
  (= persisted repl_durable_seq, a lower bound), recovered: Clean|Scanned|SyncInterrupted, learner: bool}`; primary ->
  `Accept{term, mode}` with `mode = Continue{from: Seq} | Repair{at: Seq} | Full{at: Seq}`, or
  `Reject{StaleTerm | NotMember | NotPrimary | NeedOperatorResync}`. Both sides reject peers whose term is below their
  fence.
* **Frame header (24 B):** `u16 magic 0x524B ("KR"), u8 version, u8 type, u16 flags, u16 partition, u32 term,
  u32 payload_len (<= 8 MiB), u32 crc32c(payload), u32 zero`.
* **Frame types:** `GROUPS` (sequence of groups), `COMMIT{commit_seq}`, `HEARTBEAT{commit_seq}` (every
  `repl-heartbeat-ms` when idle), `ACK{durable_seq, applied_seq}` (member -> primary, coalesced, <= 1 per ms),
  `SNAP_BEGIN{slot}`, `SNAP_REC{db, vtype, expire_at, key, value}`, `SNAP_END{slot, count}`, `SYNC_DONE{at}`,
  `REPAIR_KEYS{[(db, key)]}` (member -> primary), `IMAGE{db, key, Option<(vtype, expire_at, value)>}`,
  `SEAL{mig_id, last_src_seq}`, `PUT_PART{...}` (values > 4 MiB split into fragments).
* **Group** (one client command or one EXEC): `{seq: u64, n_ops: u16, ops}`. Ops (all carry full payloads; no digest
  variant exists, LC-29): `Put{db, vtype, expire_at, key, value}`, `Del{db, key}`, `Expired{db, key}`,
  `Expire{db, key, expire_at /*0 = persist*/}`, `Append{db, key, data}`, `SetRange{db, key, offset, data}`,
  `FlushDb{db | 0xFF}`, `FlushSlot{slot}`, `Noop`. Non-deterministic or float commands are shipped as effects
  (INCRBYFLOAT -> Put of the result; EXPIRE -> Expire with absolute time; SET EX -> Put with absolute time).
* **Backlog:** per partition RAM ring of encoded groups (`repl-backlog-size`, default 32 MiB) with a sparse
  seq -> offset index every 64 KiB; oldest overwritten. Members keep a backlog of received groups (used for repair key
  enumeration after a failover). Both live in `MAP_HUGETLB` mappings (not swappable) or `mlock`ed memory (the unit sets
  `LimitMEMLOCK=infinity`) and are `madvise(MADV_DONTDUMP)`. **Backlogs are never persisted in any form**: no file, no
  fd, no swap, no core dump (LC-23). Repair coverage after a crash comes from `maybe_divergent` (§18.4) or, if that is
  not enough, from an operator resync; the backlog is never made durable to cover restarts.
* **Member apply:** groups are applied in seq order as partition requests with `Origin::Replicated{seq}`; the records
  carry the primary's seq. Ops are idempotent under ordered replay from a lower bound (an op on key k is skipped if the
  stored version of k has seq >= op seq). The member acks its contiguous durable prefix per its `durability` mode and
  persists `repl_durable_seq`/`repl_commit_seq` (partition meta) every 50 ms.
* **Sync decision (primary, per partition):**
  1. `last_term == term` and `applied + 1 >= backlog start` -> `Continue{from: applied + 1}` (frames of the current
     term only, i.e. produced by this primary).
  2. `last_term < term`, the member is not a learner, and the needed key sets are known (below) -> `Repair{at}`.
  3. Learner (`replica add`) or `replica resync` -> `Full{at}`.
  4. Otherwise (including `recovered: SyncInterrupted`) -> `Reject{NeedOperatorResync}` (member state
     NeedsFullSync). Only `kvadm replica resync` or `replica add` can start a Full sync; no automatic path exists
     (LC-25).
  Terms count per shard (§23.2). A node's fence term belongs to the shard of its role: an assignment in another shard
  (a replica removed from one shard and added to another) starts the fence at that shard's term. Such a node's
  records may carry a term above its new shard's (`last_term > term` while `fence_term <= term`); every seq of the
  shard would sort below them, so the primary refuses the handshake (`StaleTerm`) and sends `TermRaise{shard, term,
  above: last_term}`; the coordinator raises the shard's term to `above + 1` by a handoff of the primary to itself
  (§23.12), after which the member syncs as above. "The previous term" of Repair is the history's last term below the
  current one (a raise skips terms no group carries).
* **Repair (cross-term catch-up, LC-29):** the primary never forwards groups produced in an earlier term by another
  primary. Key set = (a) keys of the member's own groups with seq in `(term_end[last_term][p], member max]` (from the
  member's backlog if it did not restart, else from its full-scan `maybe_divergent` set), sent as `REPAIR_KEYS`;
  (b) keys of groups in `(applied, term_end[last_term][p]]` from the primary's received-backlog of that term; (c) keys
  of whole intermediate terms from the primary's backlog; (d) keys of the current term's groups up to `at` (the
  primary's own backlog). If any needed range is not covered -> case 4. Repair and Continue only ever cover keys of the
  member's own shard whose state may differ from the primary's; they are never a full copy. The primary then sends one
  `IMAGE` per key with
  its current state, stamped `at` = its last executed seq, then continues with current-term groups from `at + 1`.
  Images carry current-term seqs, so they beat every divergent older-term record; together (a)-(d) cover every key
  whose state on the member can differ from the primary's state at `at`.
* **Full sync (per partition): an in-place merge, never a wipe-and-reload (LC-39).** Started only by `replica add`
  (learner) or `replica resync`. `at` = the primary's last executed seq. The member keeps its AGs, allocator state and
  index; it sets the partition-meta flag `SYNCING` (durable) before applying the first record. The primary walks the
  partition's slot tables slot by slot with `scan_pages` (page references, §16.6), paced by the static
  `sync-mbps`/`sync-iops` buckets (optionally scaled by the latency controller), and emits a `SNAP_REC` for every Live
  record read from the page store. The member applies each through the normal write path:
  1. the key is present with identical `(vtype, expire_at, value)`: set its entry's `sync_mark` (§9.2), write nothing;
  2. otherwise: write the record with seq `at` (`Origin::Snapshot`) like any update, in place in its page under the
     page-update rules (§13) or by ordinary §11.3/§11.4 placement, and set `sync_mark`. No other record is moved and
     nothing is packed into fresh AGs.
  Live groups during the walk: for slots already finished or keys whose `(slot, fp)` position is below the walk
  position -> forwarded and applied normally (they set `sync_mark`; a `Del` removes the key); for keys not yet passed
  -> dropped (the later snapshot read captures them; all on the partition thread, so "passed" is decided
  consistently, `MigrationSource`-style single scalar position). At `SYNC_DONE{at}` the member deletes every entry of
  the synced slots whose `sync_mark` is unset with ordinary D1/D2 deletes (§16.2), found with `scan_pages(unmarked)`
  and paced by `sync-iops`; then it clears the marks and the `SYNCING` flag and reports `MemberReady` (-> IsrExpand).
  A learner with an empty partition simply inserts. The member stays Syncing (never readable, §23.6 rule 4) until the
  final deletes are done; this refusal exists for correctness and overlaps no reclamation, defragmentation or
  relocation step. A member that restarts with `SYNCING` set reports `SyncInterrupted` and waits for the operator.
  Cost: about one page read per key the member already holds (same order as the primary's snapshot reads, same
  bucket); writes only for keys that differ (a post-crash resync of a mostly in-sync member writes ~1-2% of what a
  reload would); `sync_mark` uses a spare entry bit, so no extra RAM.

### 23.8 Write/ack path, durability, ISR
1. The primary partition executes the command, assigns `seq`, encodes the group into the backlog and sends it to every
   connected member (ISR and catching-up), batched per tick, in parallel with its own page writes.
2. `commit_seq(p) = min(local_durable(p), min over ISR members m != primary of acked_durable(m, p))`, where
   `local_durable` follows the node's `durability` (`written` or `flushed`; cluster default `flushed`). A group whose
   local write failed is never counted durable: `local_durable` (and the persisted `repl_durable` a restart announces)
   stays before it, the partition turns read-only (§22), and the node reports `health.failed` whenever another ISR
   member can take over (a primary alone in its ISR keeps serving reads and its other partitions), so the coordinator
   promotes a member, which holds the group.
3. The reply is released when the command's ticket satisfies the mode and `commit_seq(p) >= seq`, and the lease is
   valid. COMMIT/HEARTBEAT frames tell members the commit point.
4. **ISR shrink:** a member whose oldest unacknowledged group was covered by the primary's own durability more than
   `repl-ack-timeout-ms` (500) ago -> primary sends `IsrShrink{shard, term, member}`. The age test judges the member,
   never the primary: a member that acknowledged everything the primary has made durable is never reported, and the
   age runs on a stall clock (`kv_repl::primary::StallClock`) that counts at most 50 ms for a gap between two ticks and
   nothing while the node's replication transport has not run in the last 50 ms, so a primary that was stopped or
   descheduled (SIGSTOP, a VM pause, CPU starvation) never reports its healthy members when it resumes (their acks are
   still unread in its sockets). The coordinator commits a shrink (even if the ISR falls below `min_isr`) only while
   it holds the primary's lease and does not suspect it (a primary that missed its renewals cannot acknowledge writes,
   and its view of who lags is the stale one); only after the committed assignment arrives does the primary stop
   waiting for that member. Writes that cannot commit because the ISR is below `min_isr` reply `-NOREPLICAS` (they may
   still have taken effect: unacked). The 500 ms default leaves room for the sub-second stalls a busy or shared host
   gives a healthy member (a shrink there costs `-NOREPLICAS` under `min_isr` 2, and a shard with no promotable member
   if its primary then fails) while a write waiting on a dead member is still answered well within 1 s (timeout +
   one coordinator commit + push). The node decides which lagging members to report as one choice over all its
   partitions (`kv_repl::primary::shrink_choice`, each member's oldest age over the partitions): members the committed
   ISR can lose while it keeps `min_isr` members are reported at the timeout, oldest first; the youngest members that
   hold the ISR at `min_isr` are reported only once they pass `1.75 x repl-ack-timeout-ms` (875 ms by default), since
   dropping them would fail every write with `-NOREPLICAS`. A sub-second stall of every member together (a host disk
   or scheduling hiccup) therefore costs latency, not errors; a dead member is still dropped within a second.
5. **ISR expand:** a caught-up member (acked_durable >= commit_seq) -> the primary starts counting its acks, then
   sends `IsrExpand{shard, term, member}`; the coordinator commits; only then is the member promotable/advertised.
6. `min_isr` counts the primary; defaults: RF1 1, RF2 1, RF3 2. `WAIT n t` replies the number of ISR members other
   than the primary; `WAITAOF` replies `[1, that number]` after local durability.

### 23.9 Failover by promotion (no data copied): a unilateral static choice
Triggered when the coordinator leader declares the primary P of shard S (term t) FAIL (23.10) or P's node reports
`health.failed`. The decision is taken by the coordinator alone from committed state and its own lease table; there
is no round that gathers responses and no list derived from who responded (LC-25).
1. **Eligible** = the committed ISR(S) minus P, restricted to members whose lease the coordinator's `LeaseTable` holds
   and that are not in maintenance (maintenance nodes only if no other). None -> `S.status = NoPrimary` (CLUSTERDOWN
   for its slots), alert; when P returns it is re-promoted with term t+1 (RF1 path).
2. **Choose** by the static rule `choose_primary`: highest operator `priority`, then earliest position in the shard's
   operator-ordered member list. Replication positions, node ids and reply timing never take part (any ISR member holds
   every acknowledged write, I1-I3; LC-27).
3. **Fence only the chosen node N:** one request `Fence{req, shard, new_term: t+1}`; N persists `fence_term = t+1`
   (node meta, durable), closes its term-t streams and replies `FenceAck{req, applied: Vec<Seq> /*per partition*/}`
   (its takeover point). If N does not answer within the fence timeout (500 ms), the coordinator marks N unusable for
   this failover and takes the next node by the same static rule, sequentially; it never sends to several nodes at
   once and never waits for a set of replies.
4. Wait until P's lease has certainly expired by the coordinator's rule (usually already true).
5. **Commit** `Promote{shard, primary: N, term: t+1, isr: committed ISR - P - nodes the lease detector has already
   flagged PFAIL/FAIL, term_end: {t, takeover = N.applied}}`; P stays a member with state Failed. The new ISR never
   depends on who answered anything; stragglers leave it only through the existing primary-initiated `IsrShrink` path
   (23.8).
6. **Push** the committed assignment to every member of S. N persists role/term/history and starts term t+1 at seq
   `(t+1, 1)` once its lease is granted. Every other member persists the new fence term when the pushed `Assign`
   arrives (§23.4) and reconnects (Hello with `last_term t`): equal position -> Continue; behind or ahead -> Repair
   (23.7). They remain ISR members (they hold every acknowledged write) and are advertised after repair (MemberState
   InSync).
7. Server-side duration ~ lease (1.5 s) + grace (0.2 s) + one fence RTT + one coordinator commit + push ~ 1.75 s (no
   500 ms gather window); with a new incarnation (process restart detected) the lease wait is skipped (~0.1 s). The
   new primary may be slightly behind another ISR member; the writes it lacks were never acknowledged (they needed N's
   ack) and are repaired away (G6).
Property test `promote_isr_independent_of_fence_replies`: for every fence-reply pattern (any subset answering, any
order, any `applied` values) the committed `Promote` names the same primary (or the same next node after a timeout)
and the same ISR.

**No acknowledged write is lost.** (I1) a write is acknowledged only when durable on every ISR member and while the
primary's lease is valid; (I2) shrinking the ISR requires a coordinator commit first; (I3) the new primary is an ISR
member, so it holds every acknowledged write of term t; (I4) after N is fenced no term-t write can commit (a term-t
commit needs the ack of every ISR member, N included, and N rejects term-t frames) and P's lease has expired, so P
releases no further replies. By induction over terms every
acknowledged write is on the new primary and on every ISR member.

**Returning members** (old primary or crashed replica): an unclean restart uses the full-scan recovery (18.4) to get
`maybe_divergent`; the node renews, the coordinator clears its FAIL health flag (a health change, not a membership
change) and pushes `Follow` for its existing role; it catches up by Continue/Repair. If Repair is impossible it
becomes NeedsFullSync and waits for `kvadm replica resync` (LC-25).

### 23.10 Failure detection, fencing, split-brain prevention
* Only the coordinator leader detects failures, from node lease renewals. Three missed renewals -> PFAIL (leader RAM
  only; redirects for affected slots become CLUSTERDOWN). `t_last_renew + lease_ms * 1.001 + 10 ms + fail_grace_ms` or a
  new incarnation -> commit `Health{node, fail: true}`. FAIL is a flag; the node remains a member until `kvadm node
  remove` (LC-25/LC-27). Data nodes never heartbeat each other; replication heartbeats are primary -> own members only.
* Split brain is prevented by: one coordinator majority (WSR); lease disjointness (a new primary's lease starts only
  after the old one has provably expired or was relinquished); fencing terms in every replication frame and every
  control message (members persist `fence_term` before acknowledging); the server-side reply gate (lease re-checked
  before each reply; CLOCK_BOOTTIME counts VM suspension).
* A primary cut off from the coordinator stops serving at its lease expiry and answers MOVED to the coordinator.
* Coordinator quorum loss: leases are not renewed; every primary stops serving after the lease (CP choice; run 5
  coordinators across 3 zones).

### 23.11 The single consistency regime: exact guarantees
G1. An acknowledged write is durable (per `durability`, default `flushed` = power-loss durable) on the primary and on
every ISR member at the time of the acknowledgement.
G2. No failover performed by the coordinator rolls back an acknowledged write; acknowledged writes survive any failure
set that leaves at least one ISR member (as of the acknowledgement) recoverable.
G3. Single-key commands and same-slot multi-key commands (MSET, RENAME, MULTI/EXEC) execute atomically on the single
owning primary, totally ordered per key; same-slot multi-key writes are also crash-atomic (intent table).
G4. Reads on the primary return the latest executed state, which may include a write whose acknowledgement is still
pending; if the primary fails before that write commits, the write may be lost although a read observed it
(read-uncommitted visibility, as in Redis). Reads are never delayed or refused because of replication state.
G5. READONLY replica reads return the replica's applied state: possibly stale, monotonic per connection while it stays on
one replica.
G6. Unacknowledged writes may or may not take effect, possibly later (repair after failover).
G7. Nothing is atomic across slots. Writes require a coordinator majority, a primary with a valid lease and ISR >=
`min_isr`; a minority side is unavailable. There is no AP mode, no availability switch, no unclean or forced promotion.
G8. TTL uses the primary's wall clock and is replicated as absolute times; expiry is replicated as `Expired`/`Del`.
Versions are integers `(term, counter)`; there is no hybrid clock.

### 23.12 Operator commands (`kvadm` -> coordinator admin port: RESP `KVADM <verb> ...`, `AUTH <admin-token>`)
| Command | Transition / effect | Preconditions |
|---|---|---|
| `kvc init --id ID --peers id=host:peer,... --client-port --admin-port` (each coordinator once) | creates `coord.*` files (peer list gen 1 in `coord.meta`), version 1 state with params | empty dir |
| `kvadm cluster params set k v` | Admin ParamSet | |
| `kvadm node add <ctrl_addr> --client ip:port --repl ip:port --zone z [--priority n]` | NodeAdd (NodeId generated); leader dials it | node formatted with cluster P |
| `kvadm node remove <id> [--force]` | NodeRemove | no shard role (`--force`: node FAIL and its roles already replaced) |
| `kvadm node maintenance <id> on\|off` | NodeMaint; graceful handoff of its primaries | |
| `kvadm node clear-fail <id>` | Health | |
| `kvadm shard create <sid> --primary <node> [--min-isr n]` / `shard delete <sid>` | ShardCreate / ShardDelete | delete: no slots |
| `kvadm slots assign <ranges> <sid>` | SlotsAssign | slots unassigned |
| `kvadm replica add <sid> <node>` / `replica remove <sid> <node>` | member Syncing -> full sync (learner inserts) -> IsrExpand / member removed | add: node has no role |
| `kvadm replica resync <node>` | member -> Syncing, merge full sync (§23.7) | member is NeedsFullSync or reports corrupt pages; otherwise refused |
| `kvadm failover <sid> [--to <node>]` | graceful handoff (below) | target InSync; without `--to` the static `choose_primary` rule picks it |
| `kvadm migrate <ranges> --to <sid> [--rate mbps] [--barriers k]` / `migrate pause\|resume\|abort <id>` / `migrations` | §23.13 (`--rate`: a static token-bucket rate, never a priority) | slots owned by one source shard |
| `kvadm rebalance plan [--by slots]` / `rebalance apply <plan>` | `plan` computes from `ClusterState` alone a deterministic greedy list of `migrate` commands that evens slot counts per shard; `apply` runs them one at a time, each started only when the previous one is Done, and **stops** (does not pause) at the first failover or health change, never re-planning; the operator re-runs plan and apply. `--by keys\|bytes` is **(future)**: if ever added it may use only per-slot aggregates that nodes volunteer in their periodic `Renew` (node-initiated, never requested, never polled), with a fixed sum/sort and no pass over records | |
| `kvadm coord setpeers <gen> <list>` (and the wrappers `coord add\|remove`), `coord peers`, `coord export <file>`, `kvc recover --from <file>` | coordinator peer lists (one server per change, §23.3) / DR | previous gen reported by every listed coordinator |
| `kvadm status [--json]` | read-only summary | |
**Graceful handoff:** the target N is the operator's `--to` node or, without it, the static `choose_primary` rule
(§23.9) over InSync ISR members with a held lease. Coordinator -> P `Handoff{req, shard, to: N}`; P answers new writes
of the shard with `-TRYAGAIN` (immediate error), waits until `commit_seq >= last executed seq` on every partition (a
position barrier), relinquishes its lease and replies `HandoffDone{req, last: Vec<Seq>}`; coordinator commits
`Promote{N, term+1, term_end = last}`, P becomes an InSync member and continues from N with no divergence. SIGTERM on a
primary node triggers a handoff request first (same target rule). **Term raise:** on a primary's `TermRaise` (§23.7)
the coordinator sends `Handoff{to: P}` to P itself; P reaches the same barrier but keeps its lease (writes answer
`-TRYAGAIN` until the new assignment arrives, at most 2 s, then P serves its current term again) and the coordinator
commits `Promote{P, new_term, term_end = last}` with the ISR and member states unchanged (the only promotion whose term
may skip past `term + 1`).

### 23.13 Operator-initiated slot migration (`kv-repl/src/migrate.rs`, `kv-coord/src/migration.rs`)
1. **Copy:** commit `Migration{id, slots, src A, dst B, phase Copy, rate_mbps}`; push `MigrateOut` to A's primary and
   `MigrateIn` (with A's repl address) to B's primary. For each partition p that holds migrating slots, B's partition
   p dials A's repl port with `Hello{mig: id, partition: p}`: one stream per partition, started by the destination's
   own Hello and served only by A's owning worker (LC-37). A's partition streams `SNAP_BEGIN/SNAP_REC.../SNAP_END` for
   its migrating slots in slot order, read through `scan_pages` (page references, §16.6) and paced by the static
   `sync-mbps`/`sync-iops` buckets (or the migration's `rate_mbps` bucket), interleaved with A's live groups for those
   slots (forwarded for keys whose `(slot, fp)` is below the single scalar walk position, dropped otherwise, as in
   full sync). B applies them as its own writes (new B seqs, replicated to B's ISR) and acks A's seqs once committed on
   B's ISR. B's applies advance by position barriers on its own commit point: a burst (one fixed step, at most 64
   records or 256 KiB) is submitted only once the writes of the burst before it are committed on B's ISR; frames that
   arrive meanwhile wait, with their stream bytes, so the transport's admission stops reading A. The copy therefore
   never runs ahead of B's members and never makes them lag past `repl-ack-timeout-ms` (§23.8 step 4). Clients keep using A (B answers MOVED to the coordinator, which says A). Multi-key commands keep working.
2. **Ready (position barriers only, LC-36):** when A's partition p finishes the snapshot walk of all its migrating
   slots it records `copy_end[p]` = its last executed seq. A reports `MigrationReady` once B has acknowledged (as
   committed on B's ISR) every `copy_end[p]`: a `Seq >= Seq` test against positions fixed in the past; no count of
   outstanding groups or bytes is computed or compared. To shorten the cutover window A first runs a fixed number
   `k` (`--barriers`, default 2) of further barriers, `copy_end_i[p]` = A's last executed seq when barrier i-1
   completed; never "repeat until lag < N". In steady state B applies forwarded groups unpaced and is milliseconds
   behind, so the barriers complete at once; if B lags persistently the window grows by the lag accrued over k
   barriers, `kvadm migrations` shows it, and the operator may pause or abort.
3. **Cutover:** commit `slots := B` for the migration's slots (phase Cutover); push to A first, then B. A stops
   serving those slots (MOVED to coordinator); commands already executing finish and their groups flow to B; A then
   sends `SEAL{id, last_src_seq}` after all earlier frames (a single position barrier). B answers `-TRYAGAIN` for those
   slots until (a) everything up to `last_src_seq` is committed on B's ISR, (b) B reported `SealDone` and (c) the
   coordinator committed phase Done and pushed it to B; only then does B serve. The coordinator redirects to B from the
   Cutover commit on. No command is ever held (LC-14); the TRYAGAIN window is SEAL commit + one coordinator commit
   (~2-5 ms).
4. **Done:** after the Done commit, A purges the slots (paced deletes; `FlushSlot` replicated to A's members; each
   listing step of the purge waits until A's commit point covers the deletes of the step before it, so the purge never
   makes A's members lag). A
   finished migration leaves the primaries' projections (`ShardRole.migrations` carries running and paused tasks
   only; the record stays in `ClusterState` for `kvadm migrations`), so slots migrated back later are served again
   and purged by the shard that gave them back.
5. **Failures:** any failover of A or B before phase Done is committed -> the coordinator commits routing back to A (no
   data moves: A's ISR holds every write A acknowledged, A never purged, and B never acknowledged a write for those
   slots) and phase Paused. B keeps its partial copy **inert** (not owned, never served, excluded from SCAN/DBSIZE by
   `slot_filter()`) until the operator runs `migrate abort` (B deletes the copy with paced deletes, `FlushSlot`
   replicated to B's members) or `migrate resume` (copy again from the start as a merge: identical records confirmed,
   others written, unconfirmed ones deleted at the end, as in §23.7). After Done, failures are ordinary shard
   failovers. Nothing resumes, re-plans or re-copies automatically (LC-25).

### 23.14 Multi-key commands and hashtags
`slot = CRC16-XMODEM(tag or key) % 16384`, tag = bytes between the first `{` and the next `}` if non-empty (test
vector `CRC16("123456789") = 0x31C3`). MGET, MSET, MSETNX, DEL/EXISTS/UNLINK/TOUCH with several keys, RENAME, COPY,
MULTI/EXEC require one slot, else `-CROSSSLOT` (clients split as for Redis Cluster). On a primary they execute
atomically in one partition and replicate as one group. SCAN, KEYS, DBSIZE, RANDOMKEY, FLUSHALL act on the node's own
slots (clients fan out). SELECT 0 only. Pub/sub, keyspace notifications and blocking commands are not supported.

---------------------------------------------------------------------------------------------------------------------

## 24. Rust workspace: crates, exact module paths, public interfaces, dependencies

The existing `src/` is deleted. The repository root becomes a Cargo workspace. Rust 1.97 stable, edition 2024.
Signatures below are normative: modules MUST expose at least these items with these shapes so that workstreams can
compile against each other from week 1 (stubs with `todo!()` are acceptable until implemented). Private helpers are
free. All crates: `#![forbid(unsafe_op_in_unsafe_fn)]`; `unsafe` only in `kv-index/src/sysbuf.rs`,
`kv-storage/src/io.rs`, `kv-storage/src/cache.rs`, `kv-node/src/runtime/ring.rs`, `kv-common/src/ldouble.rs`, each
block with a `// SAFETY:` comment.

### 24.1 Tree
```
Cargo.toml                    [workspace] members = ["crates/*"], [workspace.dependencies] (24.13), profile.release: lto="thin", codegen-units=1, panic="abort"
rust-toolchain.toml           channel = "1.97"
clippy.toml                   disallowed-types/methods (LC-02, LC-06)
ci/deny.toml  ci/invariant_lint.sh  ci/check_graph.sh
LICENSE-APACHE  LICENSE-MIT  NOTICE (BSD attributions for anything ported from Redis <= 7.2 / Valkey)
crates/kv-common/   build.rs  c/ldouble.c  src/{lib,slot,hash,crc,clock,pace,abblock,glob,num,ldouble,metrics,shared_buf,redact}.rs
crates/kv-resp/     src/{lib,parser,inline,writer,error}.rs
crates/kv-cmd/      src/{lib,spec,table,keys,docs}.rs
crates/kv-index/    src/{lib,entry,sysbuf,segment,table,hint,partition_index,cursor}.rs
crates/kv-storage/  src/{lib,layout,superblock,agtable,pmeta,intent,record,page,extent,io,sim,cache,image,flush,
                         alloc,stale,ticket,effect,keyspace,partition,expire,recovery,snapshot,metastore,format,engine,
                         devprobe}.rs
crates/kv-repl/     src/{lib,codec,frame,backlog,primary,member,sync,migrate}.rs
crates/kv-ctrl/     src/{lib,ids,msg,assignment,codec}.rs
crates/kv-topo/     src/{lib,state,transition,render,redirect}.rs
crates/kv-node/     src/main.rs src/{config,shared,metrics,log}.rs
                    src/runtime/{mod,ring,worker,xmsg}.rs  src/net/{mod,listener,conn,budget}.rs
                    src/dispatch/{mod,router,multikey,xtxn}.rs
                    src/cmd/{mod,conn_cmds,server,info,config_cmd,strings,keys,expire,scan,txn,hash,cluster_cmds}.rs
                    src/cluster/{mod,agent,ownership,lease,repl_glue,migrate_glue}.rs
crates/kv-coord/    src/main.rs src/{lib,config,lease,detector,failover,isr,migration,push,clientsvc,admin,rebalance,log}.rs
                    src/wsr/{mod,persist,rpc,core,peers}.rs
crates/kvadm/       src/main.rs
crates/kv-sim/      src/{lib,simnet,storage_sim,model,cluster_sim,wsr_model,linearize}.rs
crates/kvbench/     src/main.rs                      (benchmark client, §27)
tests/compat/       existing black-box harness (kept)
tests/crash/        run_persistence.sh (wraps tests/compat/persistence.py), powercut.sh (RAM-only fault device),
                    faultdev/ (test-only Rust NBD server with SimDevice semantics; never shipped, LC-33)
tests/cluster/      matrix.sh, py/, java/ (Maven: Jedis + Lettuce), go/, node/
bench/              run_kvd.sh, run_aerospike.sh, aerospike.conf, kvd-bench.conf, report.py (configs list the allowed
                    substrates, LC-34)
dist/               kvd.service, kvc.service (LimitMEMLOCK=infinity, MemorySwapMax=0, LimitCORE=0)
docs/               DESIGN.md (this file), DEPLOY.md (deployment guide incl. allowed substrates, LC-34)
```

### 24.2 Crate dependency graph (enforced by `ci/check_graph.sh` via `cargo tree`)
```
kv-common  <- kv-resp <- kv-cmd
kv-common  <- kv-index
kv-common, kv-index <- kv-storage            (kv-storage MUST NOT depend on kv-ctrl/kv-topo/kv-node: LC-30)
kv-storage <- kv-repl                         (uses effect::{Op, Group}, layout::Seq)
kv-common  <- kv-ctrl <- kv-topo
kv-node  <- kv-common, kv-resp, kv-cmd, kv-index, kv-storage, kv-repl, kv-ctrl     (NOT kv-topo, NOT kv-coord: LC-24)
kv-coord <- kv-common, kv-resp, kv-cmd, kv-ctrl, kv-topo     (NOT kv-index/kv-storage/kv-repl/kv-node, nor any crate
                                                              containing `impl StorageEngine`: LC-38)
kvadm    <- kv-resp, kv-ctrl                                  (no storage crate: LC-37)
kv-sim   <- kv-storage (feature "sim"), kv-index, kv-repl, kv-topo, kv-coord (lib)
kvbench  <- kv-resp
faultdev (tests/crash) <- kv-storage (feature "sim")          (test-only, never in release artifacts)
```

### 24.3 `kv-common` (~1.3K LOC)
```rust
// slot.rs
pub const SLOTS: usize = 16384;
pub fn crc16_xmodem(data: &[u8]) -> u16;
pub fn hash_tag(key: &[u8]) -> &[u8];                 // Redis rule: first '{' .. next '}' non-empty, else whole key
pub fn key_slot(key: &[u8]) -> u16;                   // crc16(hash_tag(key)) & 16383
#[derive(Clone, PartialEq, Eq)] pub struct SlotSet(Box<[u64; 256]>);
impl SlotSet { pub fn new() -> Self; pub fn insert(&mut self, s: u16); pub fn remove(&mut self, s: u16);
    pub fn contains(&self, s: u16) -> bool; pub fn ranges(&self) -> Vec<(u16, u16)>; pub fn len(&self) -> usize; }
// hash.rs
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)] pub struct KeyHash(u64);
impl KeyHash { pub fn of(seed: u64, key: &[u8]) -> Self;  // xxh3_64_with_seed
    pub fn fp32(self) -> u32; pub fn x8(self) -> u8; pub fn h32(self) -> u32; pub fn raw(self) -> u64; }
// crc.rs
pub fn crc32c(data: &[u8]) -> u32;
pub fn crc32c_append(crc: u32, data: &[u8]) -> u32;
#[derive(Clone, Copy)] pub struct RecordCrc { salt: u32 }
impl RecordCrc { pub fn new(salt: u32) -> Self; pub fn compute(&self, page_id: u64, off: u16, parts: &[&[u8]]) -> u32; }
// clock.rs
#[derive(Clone, Copy, Debug)] pub struct Now { pub mono_ns: u64, pub boot_ns: u64, pub unix_ms: u64 }
impl Now { pub fn read() -> Self; }
// pace.rs
pub struct TokenBucket { /* rate, burst, tokens, last_ns */ }
impl TokenBucket { pub fn new(rate_per_s: u64, burst: u64) -> Self; pub fn set_rate(&mut self, rate_per_s: u64);
    pub fn try_take(&mut self, now_ns: u64, n: u64) -> bool; pub fn scaled(&mut self, scale: f32); }
#[derive(Clone, Copy)] pub struct DeviceLatencySample { pub p99_read_us: u32 }
pub struct LatencyController { /* target, ewma */ }
impl LatencyController { pub fn new(target_p99_us: u32, min_scale: f32) -> Self;
    pub fn update(&mut self, s: DeviceLatencySample) -> f32; }       // returns scale in [min_scale, 1.0]
// abblock.rs
pub const AB_BLOCK: usize = 4096;
pub fn ab_encode(magic: u64, block_seq: u64, payload: &[u8], out: &mut [u8; AB_BLOCK]);
pub fn ab_decode<'a>(magic: u64, block: &'a [u8]) -> Option<(u64, &'a [u8])>;   // (block_seq, payload)
pub fn ab_pick<'a>(a: Option<(u64, &'a [u8])>, b: Option<(u64, &'a [u8])>) -> Option<(AbCopy, u64, &'a [u8])>;
#[derive(Clone, Copy, PartialEq, Eq)] pub enum AbCopy { A, B }
// glob.rs
pub fn glob_match(pattern: &[u8], s: &[u8], nocase: bool) -> bool;    // Redis stringmatchlen semantics
// num.rs
pub fn string2ll(s: &[u8]) -> Option<i64>;  pub fn ll2buf(v: i64, out: &mut Vec<u8>);
pub fn parse_f64_strict(s: &[u8]) -> Option<f64>;  pub fn fmt_double_17g(v: f64, out: &mut Vec<u8>);
// ldouble.rs  (C shim c/ldouble.c compiled by build.rs with `cc`; uses `long double`, strtold, snprintf("%.17Lf"))
pub enum LdError { NotFloat, NanOrInf }
pub fn incrbyfloat(current: Option<&[u8]>, incr: &[u8]) -> Result<Vec<u8>, LdError>;
// metrics.rs
pub struct Hist(hdrhistogram::Histogram<u32>); impl Hist { pub fn new() -> Self; pub fn record_us(&mut self, us: u64);
    pub fn percentile(&self, p: f64) -> u64; pub fn merge(&mut self, o: &Hist); }
// shared_buf.rs
#[derive(Clone)] pub struct SharedBuf(std::sync::Arc<[u8]>);       // large reply bodies passed between threads
// redact.rs (LC-32): the only way user bytes may appear in a diagnostic
pub struct Redacted<'a>(pub &'a [u8]);      // Display/Debug: "<len=N tag=xxxxxxxx>" (32-bit xxh3 tag, non-cryptographic)
impl core::fmt::Display for Redacted<'_> { /* ... */ }  impl core::fmt::Debug for Redacted<'_> { /* same */ }
```

### 24.4 `kv-resp` (~1.1K LOC)
```rust
#[derive(Clone, Copy, PartialEq, Eq, Debug)] pub enum Proto { Resp2, Resp3 }
#[derive(Clone, Copy)] pub struct Limits { pub max_bulk: usize, pub max_args: usize, pub max_inline: usize }
pub enum Parsed { Command { consumed: usize }, Incomplete, Empty { consumed: usize } }
#[derive(Debug)] pub enum ProtoError { InvalidMultibulkLen, InvalidBulkLen, ExpectedDollar(u8), UnbalancedQuotes, TooBigInline }
impl ProtoError { pub fn redis_message(&self) -> &'static str; }
pub struct Parser { /* incremental state */ }
impl Parser {
    pub fn new(limits: Limits) -> Self;
    /// Parses at most one command from `buf`; argument (offset, len) pairs are pushed to `args`.
    pub fn parse(&mut self, buf: &[u8], args: &mut Vec<(u32, u32)>) -> Result<Parsed, ProtoError>;
    /// State of the command still being received (nothing consumed yet): used by the pre-consume checks of §6.7.
    pub fn in_progress(&self, buf: &[u8]) -> Option<InProgress>;
}
pub struct InProgress { pub name: Option<(u32, u32)>, pub argc: u32, pub bulk_len: Option<usize> }
pub struct ReplyWriter<'a> { pub buf: &'a mut Vec<u8>, pub proto: Proto }
impl<'a> ReplyWriter<'a> {
    pub fn new(buf: &'a mut Vec<u8>, proto: Proto) -> Self;
    pub fn ok(&mut self); pub fn simple(&mut self, s: &[u8]); pub fn error(&mut self, s: &[u8]); pub fn int(&mut self, v: i64);
    pub fn bulk(&mut self, b: &[u8]); pub fn bulk_len(&mut self, len: usize); pub fn crlf(&mut self);
    pub fn null(&mut self); pub fn null_array(&mut self); pub fn array(&mut self, n: usize); pub fn map(&mut self, n: usize);
    pub fn set(&mut self, n: usize); pub fn double(&mut self, v: f64); pub fn boolean(&mut self, b: bool);
    pub fn verbatim(&mut self, fmt: &[u8; 3], s: &[u8]);
}
/// Client side decoder used by kvadm, kvbench, tests and the coordinator's internal tools.
pub enum Value { Simple(Vec<u8>), Error(Vec<u8>), Int(i64), Bulk(Option<Vec<u8>>), Array(Option<Vec<Value>>), Map(Vec<(Value, Value)>), Double(f64), Null, Bool(bool) }
pub fn decode_value(buf: &[u8]) -> Result<Option<(Value, usize)>, ProtoError>;
pub fn encode_command(args: &[&[u8]], out: &mut Vec<u8>);
```

### 24.5 `kv-cmd` (~0.9K LOC)
```rust
bitflags::bitflags! { pub struct CmdFlags: u32 { const WRITE=1; const READONLY=2; const ADMIN=4; const FAST=8;
    const LOADING_OK=16; const NO_MULTI=32; const MULTI_KEY=64; const KEYLESS=128; const DELETE=256; const CONN=512;
    const CLUSTER_ONLY=1024; const COORD_OK=2048; const NO_AUTH=4096; } }
#[derive(Clone, Copy)] pub enum KeySpec { None, Range { first: u16, last: i16, step: u16 }, Custom(fn(&[&[u8]]) -> smallvec::SmallVec<[u16; 4]>) }
#[derive(Clone, Copy)] pub enum Access { Meta, Read, Write, ReadWrite }
pub struct CommandSpec { pub id: CmdId, pub name: &'static str, pub arity: i16, pub flags: CmdFlags, pub keys: KeySpec,
    pub access: Access, pub group: &'static str, pub since: &'static str, pub summary: &'static str,
    pub subcommands: &'static [CommandSpec] }
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)] #[repr(u16)] pub enum CmdId { /* one variant per command */ }
pub fn lookup(name: &[u8]) -> Option<&'static CommandSpec>;                  // ASCII case-insensitive
pub fn lookup_sub(spec: &'static CommandSpec, sub: &[u8]) -> Option<&'static CommandSpec>;
pub fn all() -> &'static [CommandSpec];
pub fn key_positions(spec: &CommandSpec, argv: &[&[u8]]) -> smallvec::SmallVec<[u16; 4]>;
pub fn check_arity(spec: &CommandSpec, argc: usize) -> bool;
pub fn common_slot(argv: &[&[u8]], pos: &[u16]) -> Result<Option<u16>, CrossSlot>;     // used by kvd and kvc
pub struct CrossSlot;
pub fn write_command_info(spec: &CommandSpec, w: &mut kv_resp::ReplyWriter<'_>);
pub fn write_command_docs(spec: &CommandSpec, w: &mut kv_resp::ReplyWriter<'_>);
```

### 24.6 `kv-index` (~1.4K LOC)
```rust
#[derive(Clone, Copy, PartialEq, Eq, Debug)] pub enum Kind { Small = 0, Extent = 1, ChunkMap = 2 }
#[derive(Clone, Copy, PartialEq, Eq, Debug)] pub enum VType { String = 0, Hash = 1 }
#[derive(Clone, Copy, PartialEq, Eq, Debug)] pub struct Entry { pub fp32: u32, pub x8: u8, pub page36: u64, pub kind: Kind, pub vtype: VType,
    pub sync_mark: bool /* bit 39, RAM-only merge-sync mark, §9.2 */ }
impl Entry { pub const BYTES: usize = 10; pub fn encode(&self, out: &mut [u8; 10]); pub fn decode(b: &[u8; 10]) -> Self; pub fn is_empty_raw(b: &[u8; 10]) -> bool; }
#[derive(Clone, Copy, Debug)] pub struct Pos { pub seg: u32, pub idx: u32 }
#[derive(Clone, Copy, Debug)] pub struct Cand { pub pos: Pos, pub entry: Entry, pub hint: u16 }
pub struct SlotTable { /* dir: SysBuf of 2^D + 1 u32 (sentinel), segs: Vec<Segment>, len, ttl_len */ }
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)] pub struct PageRef { pub page36: u64, pub kind: Kind, pub class: u8 }
#[derive(Clone, Copy, Debug)] pub struct ScanSpan { pub fp_lo: u32, pub fp_hi_excl: u64 /* 2^32 = end of table */, pub next: Option<u32> }
#[derive(Clone, Copy, Debug)] pub struct ScanFilter { pub vtype: Option<VType>, pub unmarked_only: bool }
pub const SCAN_STEP_MAX_PAGES: usize = 32;           // compile-time constant, never configurable (LC-18)
impl SlotTable {
    pub fn new() -> Self; pub fn len(&self) -> usize; pub fn ttl_len(&self) -> usize; pub fn memory_bytes(&self) -> usize;
    pub fn candidates(&self, h: KeyHash, out: &mut smallvec::SmallVec<[Cand; 2]>);
    pub fn insert(&mut self, h: KeyHash, e: Entry, hint: u16);
    pub fn update(&mut self, pos: Pos, e: Entry, hint: u16);        // fp32 must not change
    pub fn remove(&mut self, pos: Pos);
    /// SCAN/KEYS/sync/migration walk: consumes whole equal-fp groups from `fp_from` until `max_entries` entries or
    /// SCAN_STEP_MAX_PAGES distinct pages; filter applied before emitting; emits de-duplicated page refs only.
    pub fn scan_pages(&self, fp_from: u32, max_entries: usize, f: ScanFilter, out: &mut Vec<PageRef>) -> ScanSpan;
    /// Snapshot writer only (kv-storage/src/snapshot.rs; lint LC-18).
    pub fn snapshot_iter(&self) -> impl Iterator<Item = (Entry, u16)> + '_;
    pub fn set_sync_mark(&mut self, pos: Pos, on: bool); pub fn clear_sync_marks(&mut self);
    pub fn bulk_build(sorted: Vec<(Entry, u16)>) -> Self;
    pub fn expiry_candidates(&self, start: Pos, budget: usize, now_lb: u16, out: &mut Vec<Pos>) -> Pos; // resume pos
    pub fn rehint(&mut self, f: &mut dyn FnMut(u16) -> u16);         // rebase pass (sliced by caller)
    pub fn random_entry(&self, r: u64) -> Option<(Pos, Entry)>;
}
pub struct PartitionIndex { /* tables: Vec<Option<Box<SlotTable>>>, nonempty lists, counters */ }
impl PartitionIndex {
    pub fn new(partition: u16, partitions: u16, databases: u8) -> Self;
    pub fn table(&self, db: u8, slot: u16) -> Option<&SlotTable>;
    pub fn table_mut(&mut self, db: u8, slot: u16) -> &mut SlotTable;   // creates on demand
    pub fn drop_db(&mut self, db: u8); pub fn drop_slot(&mut self, db: u8, slot: u16);
    pub fn keys(&self, db: u8) -> u64; pub fn expires(&self, db: u8) -> u64;
    pub fn random_table(&self, db: u8, r: u64) -> Option<u16>;
}
pub mod hint { pub fn encode(expire_at_ms: Option<u64>, base_s: u64) -> u16; pub fn decode_lb_s(h: u16, base_s: u64) -> Option<u64>; }
pub mod cursor { pub fn encode(partition: u16, slot_local: u16, fp: u32) -> u64; pub fn decode(c: u64) -> (u16, u16, u32); }
```

### 24.7 `kv-storage` (~7K LOC)
```rust
// layout.rs
pub const PAGE: usize = 4096; pub const AG_PAGES: u32 = 4096; pub const SMALL_HDR: usize = 32;
pub const SMALL_REC_MAX: usize = 2048; pub const EXT_HDR: usize = 80; pub const EXT_MAX_PAGES: u32 = 2048;
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)] pub struct PageId(pub u64);
impl PageId { pub fn new(dev: u8, page: u32) -> Self; pub fn dev(self) -> u8; pub fn page(self) -> u32;
    pub fn to36(self) -> u64; pub fn from36(v: u64) -> Self; pub fn byte_offset(self) -> u64; }
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)] pub struct AgId { pub dev: u8, pub idx: u32 }
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)] pub struct Seq(pub u64);
impl Seq { pub fn new(term: u32, counter: u64) -> Self; pub fn term(self) -> u32; pub fn counter(self) -> u64; }
pub fn class_pages(class: u8) -> u32; pub fn class_for(pages: u32) -> Option<u8>;
// superblock.rs, agtable.rs, pmeta.rs, intent.rs: byte codecs of §10 with encode/decode + unit tests
pub struct Superblock { /* every field of 10.3 */ } impl Superblock { pub fn encode(&self, seq: u64, out: &mut [u8; PAGE]); pub fn decode(b: &[u8]) -> Option<(u64, Self)>; }
pub struct AgTableBlock { pub index: u32, pub entries: Vec<(AgKind, u8)> }  pub enum AgKind { Free, Small, Extent }
pub struct PartitionMeta { pub partition: u16, pub flags: u32 /* b0 SYNCING */, pub seq_floor: Seq, pub term: u32, pub flush_cutoff: [u64; 16],
    pub repl_durable: Seq, pub repl_commit: Seq, pub history: [(u32, u64); 4] }
pub struct NodeMeta { pub cluster_id: [u8; 16], pub node_id: [u8; 20], pub shard: u16, pub role: u8, pub fence_term: u32, pub assignment_version: u64 }
pub struct IntentHeader { pub txn_floor: u64, pub resolved_w: u64 }  pub struct IntentSlot { pub copy_seq: u64, pub txn_ids: smallvec::SmallVec<[u64; 60]> }
pub const INTENT_SLOTS: usize = 64; pub const IDS_PER_COPY: usize = 60;   // 3840 txn credits
pub struct TxnCredits { /* per-worker pool, 3840 / W; take() before consuming a multi-key write; returned on Resolved */ }
impl TxnCredits { pub fn try_take(&mut self) -> bool; pub fn give_back(&mut self, n: u32); pub fn in_use(&self) -> u32; }
pub struct TxnIds { /* AtomicU64 next + floor; never waits while the floor is extended at half use */ }
impl TxnIds { pub fn next(&self) -> u64; }
// record.rs / page.rs / extent.rs
pub struct RecHeader { pub kind: u8, pub db: u8, pub flags: u8, pub seq: Seq, pub expire_at_ms: u64, pub key_len: u16, pub val_len: u16, pub h32: u32, pub txn: Option<u64> }
pub fn small_len(key: usize, val: usize, txn: bool) -> usize;        // header + key + val rounded to 16
pub fn encode_small(h: &RecHeader, key: &[u8], val: &[u8], page: PageId, off: u16, crc: RecordCrc, out: &mut [u8]);
pub struct SmallView<'a> { pub off: u16, pub len: u16, pub hdr: RecHeader, pub key: &'a [u8], pub val: &'a [u8] }
pub fn write_small_header(buf: &mut [u8; PAGE], page: PageId, partition: u16);
pub fn parse_small_page<'a>(buf: &'a [u8; PAGE], page: PageId, partition: u16, crc: RecordCrc, out: &mut Vec<SmallView<'a>>) -> PageParse; // Ok | Foreign | BadHeader
pub struct ExtHeader { pub kind: u8, pub vtype: u8, pub db: u8, pub flags: u8, pub page: PageId, pub seq: Seq, pub expire_at_ms: u64, pub n_pages: u32, pub key_len: u32, pub val_len: u64, pub txn: u64, pub h32: u32,
    pub twin: Option<PageId> /* allocation hint only, §10.9 */ }
pub struct ChunkRef { pub page: PageId, pub n_pages: u32, pub len: u32, pub crc: u32 }
pub fn encode_extent(h: &ExtHeader, key: &[u8], body: &[&[u8]], crc: RecordCrc, out: &mut [u8]);
pub fn parse_extent_header(first_page: &[u8], page: PageId) -> Option<ExtHeader>;
pub fn verify_extent(buf: &[u8], h: &ExtHeader, crc: RecordCrc) -> bool;
pub fn decode_chunkmap(body: &[u8]) -> Option<Vec<ChunkRef>>;
// io.rs
pub struct AlignedBuf { /* ptr, len, cap; 4096-aligned; Send */ }
impl AlignedBuf { pub fn zeroed(len: usize) -> Self; pub fn as_slice(&self) -> &[u8]; pub fn as_mut_slice(&mut self) -> &mut [u8]; pub fn len(&self) -> usize; }
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)] pub struct IoTag { pub partition: u16, pub id: u64 }
pub enum IoOp { Read { buf: AlignedBuf }, Write { buf: AlignedBuf, buf_off: u32, dsync: bool, atomic: bool /* RWF_ATOMIC */ }, Flush, Discard }
pub struct IoReq { pub tag: IoTag, pub dev: u8, pub offset: u64, pub len: u32, pub op: IoOp }
pub struct IoDone { pub tag: IoTag, pub res: Result<u32, i32>, pub buf: Option<AlignedBuf> }
#[derive(Clone, Copy)] pub struct DevInfo { pub pages: u64, pub lba: u32, pub write_through: bool, pub discard: bool,
    pub page_update: PageUpdateMode, pub atomic_unit: Option<(u32, u32)> /* (min, max) verified */ }
#[derive(Clone, Copy, PartialEq, Eq, Debug)] pub enum PageUpdateMode { Atomic, TwoImage }
// devprobe.rs (the only module that reads /sys/block, mountinfo, statfs/statx atomic limits, DM tables; LC-18, LC-34)
pub struct DevProbe { pub substrate: Substrate, pub lba: u32, pub write_cache: bool, pub discard: bool,
    pub atomic_unit: Option<(u32, u32)> }   // deliberately no queue-count, nr_requests or queue-depth field
pub enum Substrate { RawBlock, DmLinear, DmCrypt, MdRaid, Loop, XfsFile, Ext4File }
pub enum SubstrateRefusal { FileSystem(&'static str), Ext4DataMode, Ext4PerFileDataFlag, Zoned, DmTarget(String), MdLevel(String), LoopBacking(Box<SubstrateRefusal>) }
pub fn probe(path: &std::path::Path) -> Result<DevProbe, SubstrateRefusal>;
pub trait BlockDevice { fn info(&self) -> DevInfo; fn submit(&mut self, r: IoReq); fn poll(&mut self, out: &mut Vec<IoDone>, wait: bool); }
pub struct SyncFileDevice;  // pread/pwrite/fdatasync; tools, tests, recovery tooling
// sim.rs (feature "sim")
pub struct SimDevice; impl SimDevice { pub fn new(pages: u64, lba: u32, atomic_unit: Option<u32>, seed: u64) -> Self;
    pub fn crash(&mut self, mode: TearMode); pub fn power_cut(&mut self, mode: TearMode); pub fn durable_image(&self) -> &[u8]; }
pub enum TearMode { None, Bytes, Sectors, Prefix }   // RWF_ATOMIC writes <= atomic_unit are never torn
// SimDevice keeps only the durable image and the volatile write-back set in RAM; it records no per-write history (LC-33)
// cache.rs
pub struct PageCache; impl PageCache { pub fn new(bytes: usize, huge: bool) -> Self; pub fn get(&mut self, p: PageId) -> Option<FrameIdx>;
    pub fn alloc(&mut self, p: PageId) -> Option<FrameIdx>; pub fn frame(&self, f: FrameIdx) -> &[u8; PAGE]; pub fn frame_mut(&mut self, f: FrameIdx) -> &mut [u8; PAGE];
    pub fn pin(&mut self, f: FrameIdx); pub fn unpin(&mut self, f: FrameIdx); pub fn evict_one(&mut self, can: &dyn Fn(PageId) -> bool) -> Option<PageId>; pub fn used(&self) -> usize; }
#[derive(Clone, Copy, PartialEq, Eq, Debug)] pub struct FrameIdx(pub u32);
// flush.rs
pub struct FlushTracker; impl FlushTracker { pub fn new(dev: u8, write_through: bool) -> Self; pub fn on_write_done(&mut self, w: WriteId) -> Option<WriteId /*durable now*/>;
    pub fn poll(&mut self, io: &mut Vec<IoReq>); pub fn on_flush_done(&mut self) -> Vec<WriteId>; }
// alloc.rs  (every choice among free pages, AGs, tie-breaks and carve offsets goes through keyed_pick: LC-01)
pub struct KeyedPick { seed: u64, boot_gen: u32, draws: u64 }
impl KeyedPick { pub fn new(hash_seed: u64, boot_gen: u32) -> Self;
    /// index of the (r mod n)-th set bit, r = xxh3(domain || boot_gen || draws, seed); None if no bit is set
    pub fn pick(&mut self, domain: u64, candidates: &[u64]) -> Option<usize>;
    pub fn pick_index(&mut self, domain: u64, n: usize) -> Option<usize>; }   // among n equal candidates
// effect.rs  (replication-visible effects; kv-repl encodes them)
pub enum Op { Put { db: u8, vtype: u8, expire_at: u64, key: Box<[u8]>, value: Vec<u8> }, Del { db: u8, key: Box<[u8]> },
    Expired { db: u8, key: Box<[u8]> }, Expire { db: u8, key: Box<[u8]>, expire_at: u64 },
    Append { db: u8, key: Box<[u8]>, data: Vec<u8> }, SetRange { db: u8, key: Box<[u8]>, offset: u64, data: Vec<u8> },
    FlushDb { db: Option<u8> }, FlushSlot { slot: u16 }, Noop }
pub struct Group { pub seq: Seq, pub ops: Vec<Op> }   // Debug for Op/Group/Pending/SnapRec/ImageRec goes through Redacted (LC-32)
// keyspace.rs (command-facing API; every method is staged and committed only if exec returns Ok)
pub enum StoreError { NeedMore, OutOfSpace, TooLarge, WrongType, Corrupt(PageId), Io(i32), ReadOnly, Loading }
pub struct KeyMeta { pub vtype: VType, pub expire_at_ms: Option<u64>, pub len: u64 }
pub enum Value<'a> { Contig(&'a [u8]), Chunks(smallvec::SmallVec<[&'a [u8]; 4]>) }
impl<'a> Value<'a> { pub fn len(&self) -> u64; pub fn to_vec(&self) -> Vec<u8>; pub fn slice(&self, start: u64, end: u64) -> Vec<u8>; }
pub struct Keyspace<'p> { /* &mut partition internals, staged ops, db, now */ }
impl<'p> Keyspace<'p> {
    pub fn db(&self) -> u8; pub fn select(&mut self, db: u8); pub fn now_ms(&self) -> u64;
    pub fn meta(&mut self, key: &[u8]) -> Result<Option<KeyMeta>, StoreError>;
    pub fn get(&mut self, key: &[u8]) -> Result<Option<(KeyMeta, Value<'_>)>, StoreError>;
    pub fn put(&mut self, key: &[u8], value: &[u8], vtype: VType, expire_at_ms: Option<u64>) -> Result<(), StoreError>;
    pub fn append(&mut self, key: &[u8], data: &[u8]) -> Result<u64, StoreError>;
    pub fn set_range(&mut self, key: &[u8], off: u64, data: &[u8]) -> Result<u64, StoreError>;
    pub fn set_expire(&mut self, key: &[u8], expire_at_ms: Option<u64>) -> Result<bool, StoreError>;
    pub fn delete(&mut self, key: &[u8]) -> Result<bool, StoreError>;
    pub fn apply_replicated(&mut self, op: &Op, seq: Seq, mode: ApplyMode) -> Result<ApplyOutcome, StoreError>;   // member / image / snapshot
}
pub enum ApplyMode { Ordered /* skip if stored seq >= op seq */, Image, SnapshotMerge /* §23.7: identical -> mark only */ }
pub enum ApplyOutcome { Applied, Skipped, Confirmed /* identical record kept, sync_mark set, nothing written */ }
impl<'p> Keyspace<'p> {
}
// ticket.rs
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)] pub struct TicketId(pub u64);
#[derive(Clone, Copy, PartialEq, Eq, Debug)] pub enum TicketState { Executed, Written, Durable, Failed(i32) }
// partition.rs
#[derive(Clone, Copy, PartialEq, Eq, Debug)] pub enum Durability { Async, Written, Flushed }
pub struct PartitionCfg { pub id: u16, pub partitions: u16, pub databases: u8, pub durability: Durability,
    pub granularity: WriteGranularity, pub page_update: PageUpdateMode, pub page_slack_cap: u16, pub page_slack_atomic: u16,
    pub hash_seed: u64, pub boot_gen: u32, pub crc: RecordCrc, pub lba: u32,
    pub produce_groups: bool, pub cluster: bool, pub rates: BgRates }
pub enum WriteGranularity { Page, Lba }
pub struct BgRates { pub scrub_iops: u64, pub warm_iops: u64, pub expire_reads: u64, pub reclaim_iops: u64, pub purge_iops: u64, pub sync_mbps: u64, pub sync_iops: u64 }
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)] pub struct ReqId(pub u32);
pub enum Origin { Client, Replicated(Seq), Image(Seq), Snapshot(Seq), Migration { src_seq: Seq } }
pub enum ReqKind { Keyed, Txn { txn_id: u64 }, ScanStep { db: u8, cursor: u64, count: u32, vtype: Option<VType> },
    RandomKey { db: u8, r: u64 }, FlushDb { db: Option<u8> },
    SlotIter { slot: u16, fp_from: u32, max_entries: u32 },   // sync/migration source walk via scan_pages (page refs)
    SyncPrune { slot: u16, fp_from: u32 },                    // merge-sync final deletes of unmarked entries
    Barrier }
pub struct Request { pub db: u8, pub keys: smallvec::SmallVec<[(Box<[u8]>, kv_cmd::Access); 2]>, pub kind: ReqKind, pub origin: Origin }
pub struct ExecOut { pub ticket: Option<TicketId>, pub group: Option<Group>, pub seq: Option<Seq> }
pub enum ExecStatus<R> { Done(R, ExecOut), Requeued, Failed(StoreError) }
pub struct StorageCtx<'a> { pub cache: &'a mut PageCache, pub io: &'a mut Vec<IoReq>, pub flush: &'a mut [FlushTracker],
    pub meta: &'a MetaClient, pub now: Now }
pub enum PartState { Loading, Ready, ReadOnly }
pub struct PartitionStore { /* index: PartitionIndex, alloc, pages, stale, tickets, queues, seq, rates, counters */ }
impl PartitionStore {
    pub fn new(cfg: PartitionCfg) -> Self;
    pub fn state(&self) -> PartState;
    pub fn submit(&mut self, cx: &mut StorageCtx<'_>, req: Request) -> ReqId;
    pub fn take_ready(&mut self, out: &mut Vec<ReqId>);
    pub fn exec<R>(&mut self, cx: &mut StorageCtx<'_>, id: ReqId,
                   f: &mut dyn FnMut(&mut Keyspace<'_>) -> Result<R, StoreError>) -> ExecStatus<R>;
    pub fn scan_keys(&mut self, cx: &mut StorageCtx<'_>, id: ReqId, matcher: &dyn Fn(&[u8]) -> bool, out: &mut Vec<Box<[u8]>>) -> ExecStatus<u64 /*next cursor*/>;
    pub fn on_io_done(&mut self, cx: &mut StorageCtx<'_>, d: IoDone);
    pub fn on_durable(&mut self, writes: &[WriteId]);
    pub fn on_meta_done(&mut self, cx: &mut StorageCtx<'_>, m: MetaDone);
    pub fn tick(&mut self, cx: &mut StorageCtx<'_>);                     // build/submit images, background tasks
    pub fn take_tickets(&mut self, out: &mut Vec<(TicketId, TicketState)>);
    pub fn next_seq(&self) -> Seq; pub fn set_term(&mut self, term: u32);
    pub fn recover(&mut self, cx: &mut StorageCtx<'_>, mode: RecoveryMode) -> RecoveryProgress;   // driven to completion by the worker
    pub fn snapshot_section(&mut self, cx: &mut StorageCtx<'_>, w: &mut SnapshotWriter) -> bool;   // true when done
    pub fn counters(&self) -> &PartitionCounters;
}
// metastore.rs (meta thread)
pub enum MetaReq { AcquireAg { partition: u16, kind: AgKind }, ReleaseAg { ag: AgId }, PartitionMeta { m: PartitionMeta },
    NodeMeta { m: NodeMeta }, ReserveSeq { partition: u16, floor: Seq }, CommitTxn { txn: u64 } /* batched, §14.6 */,
    TxnAborted { txn: u64 } /* records zeroed */, Superblock(SbUpdate), Barrier }
pub enum MetaDone { Ag(Option<AgId>), Ok, Committed { txn: u64 }, Resolved { credits: u32 } /* W advanced */, Failed(i32) }
pub struct MetaClient { /* crossbeam_queue / std mpsc sender + per-worker reply hook */ }
impl MetaClient { pub fn request(&self, partition: u16, token: u64, r: MetaReq); }
pub struct MetaStore; impl MetaStore { pub fn start(devs: Vec<Box<dyn BlockDevice + Send>>, reply: Vec<Box<dyn Fn(u16, u64, MetaDone) + Send>>) -> (MetaClient, std::thread::JoinHandle<()>); }
// format.rs / engine.rs
pub struct FormatOpts { pub devices: Vec<(std::path::PathBuf, Option<u64>)>, pub partitions: u16, pub cluster: bool,
    pub snapshot_region: SnapshotRegionSize /* Bytes(u64) | Percent(f32), default 3% */ }   // files are always zero-filled
pub fn format(opts: &FormatOpts) -> std::io::Result<()>;
pub trait StorageEngine: Sized { type Partition; fn open(cfg: &EngineCfg) -> std::io::Result<(Self, Vec<Self::Partition>)>;
    fn shutdown(&mut self, clean: bool) -> std::io::Result<()>; }
pub struct PageStore; impl StorageEngine for PageStore { type Partition = PartitionStore; /* ... */ }   // the only impl (LC-03)
```

### 24.8 `kv-repl` (~2.2K LOC)
```rust
pub use kv_storage::effect::{Group, Op}; pub use kv_storage::layout::Seq;
pub mod codec { pub fn encode_group(g: &Group, out: &mut Vec<u8>); pub fn decode_group(b: &[u8]) -> Result<(Group, usize), DecodeError>;
    pub fn group_keys(b: &[u8]) -> Result<Vec<(u8, Box<[u8]>)>, DecodeError>; pub struct DecodeError; }
pub mod frame {
    pub struct Hello { pub cluster_id: [u8; 16], pub auth: Vec<u8>, pub node_id: [u8; 20], pub shard: u16, pub partition: u16,
        pub fence_term: u32, pub last_term: u32, pub applied: Seq, pub recovered: Recovered, pub learner: bool,
        pub migration: Option<u64> /* set when a migration destination partition dials the source, §23.13 */ }
    pub enum Recovered { Clean, Scanned, SyncInterrupted }
    pub enum AcceptMode { Continue { from: Seq }, Repair { at: Seq }, Full { at: Seq } }
    pub enum RejectReason { StaleTerm, NotMember, NotPrimary, NeedOperatorResync, BadAuth }
    pub struct SnapRec { pub db: u8, pub vtype: u8, pub expire_at: u64, pub key: Box<[u8]>, pub value: Vec<u8> }
    pub struct ImageRec { pub db: u8, pub key: Box<[u8]>, pub image: Option<(u8, u64, Vec<u8>)> }
    pub enum Frame { Hello(Hello), Accept { term: u32, mode: AcceptMode }, Reject(RejectReason), Groups(Vec<u8>),
        Commit(Seq), Heartbeat(Seq), Ack { durable: Seq, applied: Seq }, SnapBegin { slot: u16 }, SnapRec(SnapRec),
        SnapEnd { slot: u16, count: u64 }, SyncDone { at: Seq }, RepairKeys(Vec<(u8, Box<[u8]>)>), Image(ImageRec),
        Seal { mig: u64, last: Seq }, PutPart { seq: Seq, db: u8, key: Box<[u8]>, offset: u64, total: u64, data: Vec<u8> } }
    pub fn encode(f: &Frame, partition: u16, term: u32, out: &mut Vec<u8>);
    pub struct FrameDecoder; impl FrameDecoder { pub fn new() -> Self; pub fn feed(&mut self, buf: &[u8]) -> Result<Option<(u16, u32, Frame, usize)>, FrameError>; }
    pub enum FrameError { BadMagic, BadCrc, TooLarge, Malformed }
}
pub mod backlog { // RAM ring in MAP_HUGETLB or mlock'ed memory, MADV_DONTDUMP; no File/fd field (LC-23)
    pub struct Backlog; impl Backlog { pub fn new(cap: usize) -> Self; pub fn push(&mut self, seq: Seq, encoded: &[u8]);
    pub fn first(&self) -> Option<Seq>; pub fn last(&self) -> Option<Seq>; pub fn covers_after(&self, s: Seq) -> bool;
    pub fn read_after(&self, s: Seq, max_bytes: usize, out: &mut Vec<u8>) -> Option<Seq>;
    pub fn keys_between(&self, after: Seq, upto: Seq) -> Option<Vec<(u8, Box<[u8]>)>>; } }
pub mod primary { pub type MemberId = u16;
    pub struct ReplSender; impl ReplSender {
        pub fn new(partition: u16, term: u32, backlog_bytes: usize) -> Self;
        pub fn on_group(&mut self, encoded: &[u8], seq: Seq);
        pub fn attach(&mut self, m: MemberId, mode: &crate::frame::AcceptMode);
        pub fn detach(&mut self, m: MemberId);
        pub fn set_isr(&mut self, counted: &[MemberId]);
        pub fn on_ack(&mut self, m: MemberId, durable: Seq, now_ns: u64);
        pub fn set_local_durable(&mut self, s: Seq);
        pub fn commit_seq(&self) -> Seq;
        pub fn fill_send(&mut self, m: MemberId, out: &mut Vec<u8>, max: usize) -> bool;   // frames due for m
        pub fn lagging(&self, now_ns: u64, timeout_ns: u64) -> smallvec::SmallVec<[MemberId; 2]>; } }
pub mod member { pub enum MemberAction { Apply(Group), Image(frame::ImageRec, Seq), Snap(frame::SnapRec, Seq) /* ApplyMode::SnapshotMerge */,
        SyncBegin(Seq) /* set SYNCING; nothing is reset */, Commit(Seq), SyncDone(Seq) /* prune unmarked entries */, SendRepairKeys, Nothing }
    pub struct MemberStream; impl MemberStream { pub fn new(partition: u16, backlog_bytes: usize) -> Self;
        pub fn on_frame(&mut self, f: frame::Frame) -> Result<MemberAction, frame::FrameError>;
        pub fn note_durable(&mut self, s: Seq); pub fn ack_due(&mut self, now_ns: u64) -> Option<frame::Frame>;
        pub fn received(&self) -> &backlog::Backlog; } }
pub mod sync { pub struct PrimaryView<'a> { pub term: u32, pub history: &'a [(u32, Vec<u64>)], pub backlog: &'a backlog::Backlog,
        pub received: &'a backlog::Backlog, pub is_member: bool, pub operator_full_sync: bool /* replica add/resync only */,
        pub last_executed: Seq }
    pub enum SyncDecision { Continue { from: Seq }, Repair { at: Seq, primary_keys: Vec<(u8, Box<[u8]>)>, want_member_keys: bool }, Full { at: Seq }, Reject(frame::RejectReason) }
    pub fn decide(h: &frame::Hello, v: &PrimaryView<'_>) -> SyncDecision; }
pub mod walk { // single scalar walk position shared by full sync and migration sources (LC-18, LC-37)
    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)] pub struct WalkPos(pub u64);   // slot_ordinal << 32 | fp32
    pub fn passed(cursor: WalkPos, slot_ordinal: u16, fp: u32) -> bool; }   // one scalar comparison
pub mod migrate { pub struct MigrationSource; pub struct MigrationDest;   // walk position, barriers, seal tracking
    impl MigrationSource { pub fn new(id: u64, slots: &[u16], barriers: u8) -> Self; pub fn pos(&self) -> walk::WalkPos;
        pub fn advance(&mut self, to: walk::WalkPos); pub fn walk_done(&mut self, last_executed: Seq);
        /// position barriers only: copy_end_i fixed when recorded; true once acked >= the last barrier (LC-36)
        pub fn on_dest_committed(&mut self, acked: Seq, last_executed: Seq) -> bool /* ready */; }
    impl MigrationDest { pub fn new(id: u64) -> Self; pub fn on_src_seq_committed(&mut self, s: Seq); pub fn sealed(&self) -> bool; } }
```

### 24.9 `kv-ctrl` (~0.5K LOC)
```rust
// ids.rs  (Eq + Hash + Clone + Copy + Debug + serde; NodeId/CoordId deliberately NOT Ord/PartialOrd: LC-27)
pub struct NodeId(pub [u8; 20]); pub struct CoordId(pub [u8; 20]); pub struct ShardId(pub u16);
impl NodeId { pub fn random() -> Self; pub fn hex40(&self) -> String; pub fn parse_hex40(s: &str) -> Option<Self>; }
// assignment.rs: NodeAssignment, ShardRole, MemberState, TermEnd, MigrationTask, CoordEndpoint, PeerAddr, NodeParams (§23.4)
// msg.rs
pub enum CoordToNode { Hello { cluster_id: [u8; 16], coord: CoordId, coord_term: u64, auth: Vec<u8> }, Assign(NodeAssignment),
    LeaseGrant { seq: u64, lease_ms: u32 }, Fence { req: u64, shard: ShardId, new_term: u32 } /* to one node only, §23.9 */,
    Handoff { req: u64, shard: ShardId, to: NodeId }, MigrationPhase { id: u64, phase: MigPhase }, Ping }
pub enum NodeToCoord {
    // replies to coordinator requests: identity, acks and positions only; no role/shard/slot/key-count/term field (LC-26)
    HelloAck { node: Option<NodeId>, incarnation: u64, set_uuid: [u8; 16], partitions: u16, ready: bool },
    AssignAck { version: u64 }, FenceAck { req: u64, applied: Vec<u64> }, HandoffDone { req: u64, last: Vec<u64> },
    Error(NodeError),
    // node-initiated messages
    Renew { seq: u64, incarnation: u64, version_seen: u64, health: Health },
    IsrShrink { shard: ShardId, term: u32, member: NodeId }, IsrExpand { shard: ShardId, term: u32, member: NodeId },
    MemberReady { shard: ShardId, term: u32, member: NodeId }, MigrationReady { id: u64 }, SealDone { id: u64 } }
pub enum NodeError { StaleAssignment, BadAuth, Malformed }   // no contents
pub struct Health { pub ready: bool, pub failed: bool, pub read_only_partitions: u16, pub corrupt_pages: u32 }
// codec.rs
pub fn encode<T: serde::Serialize>(m: &T, out: &mut Vec<u8>);          // u32 len || postcard
pub fn decode<T: serde::de::DeserializeOwned>(buf: &[u8]) -> Result<Option<(T, usize)>, CodecError>;
```

### 24.10 `kv-topo` (~1.3K LOC)
```rust
pub use state::{ClusterState, ClusterParams, NodeRecord, ShardRecord, Member, Migration, MigPhase};   // no CoordRecord in the state (LC-38)
pub use transition::{Transition, AdminOp, Reject};
impl ClusterState {
    pub fn initial(params: ClusterParams, cluster_id: [u8; 16]) -> Self;
    pub fn apply(&self, batch: &[Transition]) -> Result<ClusterState, Reject>;     // pure; version + 1
    pub fn encode(&self) -> Vec<u8>; pub fn decode(b: &[u8]) -> Result<Self, Reject>;
    pub fn assignment_for(&self, node: &NodeId, coord_term: u64, coordinators: &[CoordEndpoint] /* static peer list order */) -> NodeAssignment;
    pub fn shard_of_slot(&self, slot: u16) -> Option<&ShardRecord>;
}
pub mod render { pub fn cluster_slots(s: &ClusterState, w: &mut kv_resp::ReplyWriter<'_>);
    pub fn cluster_shards(s: &ClusterState, w: &mut kv_resp::ReplyWriter<'_>);
    pub fn cluster_nodes(s: &ClusterState, peers: &[CoordEndpoint] /* static list */, myself: &CoordId, style: CoordLineStyle /* R1 | R2 */) -> Vec<u8>;
    pub fn cluster_info(s: &ClusterState) -> Vec<u8>; }
pub mod redirect { pub enum Redirect { Moved(u16, std::net::SocketAddr), ClusterDown, TryAgain }
    pub fn for_slot(s: &ClusterState, slot: u16, now_pfail: &dyn Fn(&NodeId) -> bool) -> Redirect; }
```

### 24.11 `kv-node` (binary `kvd`, ~6K LOC) internal contracts
```rust
// shared.rs
pub struct NodeShared { pub cfg: parking_lot::RwLock<Arc<Config>>, pub counters: Vec<PartitionCountersAtomic>,
    pub workers: Vec<WorkerHandle>, pub ownership: parking_lot::RwLock<Arc<Ownership>>, pub lease: LeaseCell, pub start: Now }
pub struct WorkerHandle { pub id: u16, pub inbox: Vec<Arc<crossbeam_queue::ArrayQueue<XMsg>>>, pub eventfd: i32, pub parked: Arc<AtomicBool> }
// runtime/xmsg.rs
pub enum XMsg { Cmd { from: u16, conn: ConnRef, req_seq: u64, partition: u16, cmd: OwnedCmd, env: ExecEnv },
    Reply { conn: ConnRef, req_seq: u64, reply: ReplyBody }, Part { conn: ConnRef, req_seq: u64, part: u16, reply: ReplyBody },
    Flush { db: Option<u8>, gather: GatherRef }, XTxn(XTxnMsg), Meta { partition: u16, token: u64, done: MetaDone },
    Ctrl(CtrlEvent), ReplSocket { fd: i32, partition: u16, hello: Box<kv_repl::frame::Hello> }, Shutdown }
pub struct OwnedCmd { pub id: kv_cmd::CmdId, pub buf: Box<[u8]>, pub args: smallvec::SmallVec<[(u32, u32); 8]> }
pub enum ReplyBody { Bytes(Vec<u8>), Shared(Vec<u8>, kv_common::shared_buf::SharedBuf) }
#[derive(Clone, Copy)] pub struct ExecEnv { pub proto: kv_resp::Proto, pub db: u8, pub now_ms: u64, pub readonly_conn: bool, pub conn_id: u64 }
// cmd/mod.rs : handler registry
pub type KeyedFn = fn(&mut kv_storage::keyspace::Keyspace<'_>, &Args<'_>, &mut kv_resp::ReplyWriter<'_>, &ExecEnv) -> Result<(), kv_storage::keyspace::StoreError>;
pub type ConnFn  = fn(&mut ConnCtx<'_>, &Args<'_>, &mut kv_resp::ReplyWriter<'_>) -> ConnOutcome;  // keyless/connection commands on the home worker
pub struct Args<'a> { pub argv: smallvec::SmallVec<[&'a [u8]; 8]> }
pub enum ConnOutcome { Done, Close, FanOut(FanOutSplit), Async }
pub struct FanOutSplit { /* fixed per-command split: one sub-request per distinct partition of the keys, key bytes only (LC-37) */ }
pub fn handler(id: kv_cmd::CmdId) -> Handler;  pub enum Handler { Keyed(KeyedFn), Conn(ConnFn), Scan, Txn, Cluster(ConnFn) }
// cluster/ownership.rs
// fields are private to cluster/ownership.rs; kv-node/src/cmd/* may only call gate() and slot_filter() (lint, LC-24)
pub struct Ownership { mode: Mode, role: Option<Role>, owned: SlotSet, shard_slots: SlotSet, sealed_out: SlotSet,
    pending_seal: SlotSet, min_isr: u8, isr_len: u8, coord_addrs: Vec<std::net::SocketAddr> /* static list order */, term: u32 }
pub enum Gate { Serve, Moved(u16, std::net::SocketAddr), CrossSlot, NoReplicas, TryAgain, ReadOnlyReplica, ClusterDown }
pub struct SlotFilter { /* opaque predicate: does this node serve slot s for keyless walks */ }
impl SlotFilter { pub fn serves(&self, slot: u16) -> bool; }
impl Ownership { pub fn gate(&self, slot: Option<u16>, flags: kv_cmd::CmdFlags, readonly_conn: bool, lease_ok: bool, conn_id: u64) -> Gate;
    // Moved target = coord_addrs[conn_id % n], independent of slot state
    pub fn slot_filter(&self) -> SlotFilter; }
// cluster/lease.rs
pub struct LeaseCell(AtomicU64 /* expiry boot_ns */); impl LeaseCell { pub fn valid(&self, boot_ns: u64) -> bool; pub fn set(&self, expiry_boot_ns: u64); }
```

### 24.12 `kv-coord` (binary `kvc`, ~3K LOC; tokio multi-thread) and `kvadm` (~0.5K LOC)
```rust
// wsr/mod.rs
pub struct Wsr { /* core state machine + persistence + peers */ }
impl Wsr { pub async fn start(cfg: WsrCfg, persist: persist::StateFiles) -> std::io::Result<Arc<Self>>;
    pub async fn propose(&self, batch: Vec<Transition>) -> Result<u64 /*committed version*/, ProposeError>;
    pub fn committed(&self) -> Arc<ClusterState>; pub fn is_leader(&self) -> bool; pub fn term(&self) -> u64;
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<Arc<ClusterState>>; }
// wsr/persist.rs
pub struct StateFiles; impl StateFiles { pub fn create(dir: &Path, half_bytes: usize, peers: &PeerList) -> io::Result<()>; pub fn open(dir: &Path) -> io::Result<(Self, Persisted)>;
    pub fn write_meta(&mut self, term: u64, voted_for: Option<CoordId>, peers: &PeerList) -> io::Result<()>;
    pub fn write_state(&mut self, committed: &Versioned, accepted: Option<&Versioned>) -> io::Result<()>; }
// wsr/peers.rs: the operator-set voter list, never part of ClusterState (LC-38)
pub struct PeerList { pub gen: u64, pub peers: Vec<CoordRecord> }   // CoordRecord {id, id40, peer, client, admin}
pub fn set_peers(cur: &PeerList, new: &PeerList) -> Result<(), PeerChangeError>;   // gen = cur.gen + 1, exactly one server added or removed
// lease.rs / detector.rs / failover.rs / isr.rs / migration.rs / push.rs / clientsvc.rs / admin.rs / rebalance.rs
pub struct LeaseTable; impl LeaseTable { pub fn on_renew(&mut self, n: NodeId, incarnation: u64, t_recv_ns: u64) -> RenewOutcome;
    pub fn held(&self, n: &NodeId, now_ns: u64) -> bool; pub fn expired(&self, now_ns: u64) -> Vec<NodeId>; }
pub struct Candidate { pub priority: u8, pub order: usize }      // static operator data only: no position, score or reply field (LC-25)
pub fn choose_primary(eligible: &[Candidate], unusable: &[usize]) -> Option<usize>;   // no NodeId argument by construction (LC-27)
pub async fn run_failover(wsr: Arc<Wsr>, push: Arc<push::Pusher>, shard: ShardId, failed: NodeId) -> Result<(), FailoverError>;
pub struct Pusher; impl Pusher { pub async fn send(&self, n: &NodeId, m: CoordToNode) -> Result<(), PushError>;
    pub async fn request(&self, n: &NodeId, m: CoordToNode, timeout: Duration) -> Result<NodeToCoord, PushError>; }
pub async fn serve_clients(l: tokio::net::TcpListener, wsr: Arc<Wsr>) -> io::Result<()>;
pub async fn serve_admin(l: tokio::net::TcpListener, wsr: Arc<Wsr>, ctl: Arc<Controllers>) -> io::Result<()>;
pub fn rebalance_plan(s: &ClusterState) -> Vec<MigrateCmd>;      // --by slots only; deterministic greedy over ClusterState
```

### 24.13 Dependencies (`[workspace.dependencies]`; versions are minimums already released >= 2 weeks before 2026-11-01)
| Crate | Version | Used by | Purpose |
|---|---|---|---|
| io-uring | 0.7.13 | kv-node | ring |
| libc | 0.2.180 | kv-node, kv-storage | syscalls, statx, ioctls |
| xxhash-rust (feature xxh3) | 0.8.15 | kv-common | key fingerprints |
| crc32c | 0.6.8 | kv-common | integrity (hardware) |
| memchr | 2.7 | kv-resp | CRLF scan |
| smallvec (features union, const_generics) | 1.13 | all | small vectors |
| bitflags | 2.6 | kv-cmd | flags |
| hashbrown | 0.16 | kv-storage, kv-node, kv-coord | maps (foldhash default hasher) |
| foldhash | 0.2 | same | hasher |
| crossbeam-queue / crossbeam-utils | 0.3.11 / 0.8.21 | kv-node | ArrayQueue, CachePadded |
| parking_lot | 0.12.3 | kv-node, kv-storage | mutexes |
| core_affinity | 0.8.1 | kv-node | pinning |
| itoa / ryu | 1.0 / 1.0 | kv-resp, kv-common | formatting |
| serde (derive) / postcard (use-std) | 1.0.219 / 1.1 | kv-ctrl, kv-topo, kv-coord | control encoding, coordinator state |
| tokio (rt, rt-multi-thread, net, time, sync, macros, io-util, signal) | 1.47 | kv-coord, kv-node agent | async control plane |
| clap (derive) | 4.5 | binaries | CLI |
| tracing / tracing-subscriber (env-filter) | 0.1.41 / 0.3.19 | all binaries | logs |
| thiserror | 2.0 | all | errors |
| hdrhistogram | 7.5 | kv-common | latency |
| static_assertions | 1.1 | kv-ctrl, kv-repl | compile-time invariant checks |
| rand | 0.9 | kv-coord, kv-ctrl, kv-sim | election timeouts, ids |
| mimalloc (default-features false) | 0.1.48 | kv-node, kv-coord | global allocator (index segments use `std::alloc::System`, LC-05) |
| cc (build) | 1.2 | kv-common | long double shim |
| dev: proptest 1.6, tempfile 3.20, redis 1.7, stateright 0.31, criterion 0.5 | | tests | |
Banned by `ci/deny.toml`: `sha1 sha2 sha3 md-5 md5 blake2 blake3 ripemd siphasher` (LC-06/LC-22), `cudarc cuda-sys
cust wgpu vulkano ocl opencl3` (GPU), `bumpalo typed-arena` (LC-05), licenses `AGPL-3.0* SSPL-1.0 BUSL-1.1 GPL-*
LGPL-*`. Allowed licenses: MIT, Apache-2.0, BSD-2/3-Clause, ISC, Zlib, Unicode-3.0, CC0-1.0.

### 24.14 Size budget (production code, excluding tests/sim/bench)
kv-common 1.4K, kv-resp 1.1K, kv-cmd 0.9K, kv-index 1.4K, kv-storage 7.6K (incl. devprobe 0.4K, atomic mode and twin
runs 0.2K), kv-repl 2.3K, kv-ctrl 0.5K, kv-topo 1.3K, kv-node 6.1K, kv-coord 3.2K (incl. peer lists), kvadm 0.5K:
**~26K upper bound, ~22-23K expected**. Tests, simulator, `faultdev` (~0.5K) and bench tools add ~8.5K. If kv-storage
trends above budget, cut in this order: extent cache, sector-granular writes, hashes (never the LC checks).

---------------------------------------------------------------------------------------------------------------------

## 25. Testing strategy

### 25.1 Unit tests (in each module, `cargo test`)
* Byte codecs of §10 (golden byte vectors checked into `crates/kv-storage/tests/golden/`), AbBlock A/B selection
  including torn copies, CRC16 vectors (`"123456789" -> 0x31C3`), hashtag rule, `string2ll`, glob (ported Redis
  `stringmatchlen` test table), INCRBYFLOAT shim (known Redis outputs), RESP parser incremental == whole-buffer,
  writer RESP2/RESP3 encodings, command table arity/key-spec vectors, hint encode lower-bound property, Seq packing,
  size-class table, TokenBucket/LatencyController.

### 25.2 Property tests (proptest)
| Property | Crate |
|---|---|
| SlotTable == `hashbrown` model under random insert/remove/update/grow/split/shrink; invariants I-a, I-b, I-c after every op; `memory_bytes / len` within 9.5 % of 10.16/0.85 at steady state | kv-index |
| SCAN completeness: random mutations interleaved with a full cursor iteration return every key present throughout, none twice; `scan_pages` output contains only de-duplicated `PageRef`s and its span bounds | kv-index |
| Page theorem 17.3: random page histories; two-image mode: for every build enumerate all sector mixes (exhaustive for pages with <= 12 changed sectors, random byte mixes otherwise); atomic mode: M or N whole; resolution is old-or-new per key | kv-storage |
| Tiling parser never panics, never accepts a record not written at that location (random bytes, records embedded in values) | kv-storage |
| `prop_update_keeps_page`, `prop_update_keeps_offset` (atomic mode), `prop_bounded_copies` (I11/I12), `prop_holes_reused`, `prop_placement_uncorrelated` (empty device and >= 3 uniform-TTL wraps, small records and extents: \|Spearman(seq, address)\| < 0.05), `prop_ag_acquire_aperiodic` (all LC-01); `record_single_device` (LC-08); `ttl_shorten` (LC-10); `full_sync_keeps_layout` (LC-39); W7-style fragmentation check of random extent carving (< 2 points of fill lost vs lowest-start carving) | kv-storage |
| Segment byte lengths never a multiple of 64 and directory sizes never a power of two across the capacity sequence (LC-09) | kv-index |
| `migration_ready_is_position_barrier` (LC-36), `fanout_width_equals_partitions` (LC-37), `keys_sequential`, `scan_single_thread` (LC-18) | kv-repl, kv-node |
| Repl codec round trip; every `Op` encodes its full value bytes (LC-29) | kv-repl |
| `ClusterState::apply` determinism; non-admin transitions never change membership/slots (LC-25); versions/terms strictly increase (LC-27); `choose_primary` invariant under id permutation; `promote_isr_independent_of_fence_replies` (LC-25); `set_peers` accepts only single-server deltas with gen + 1 (LC-38) | kv-topo, kv-coord |

### 25.3 Deterministic simulation (`crates/kv-sim`)
* **storage_sim:** N partitions on `SimDevice` (per-byte durable layer + volatile layer, FLUSH semantics, lost
  unflushed writes, byte/sector/prefix tearing; atomic-unit mode where `RWF_ATOMIC` writes are never torn), both
  page-update modes, single-threaded driver with a seeded RNG choosing command mixes (SET
  variants, DEL, EXPIRE*, PERSIST, RENAME, MSET, MSETNX, APPEND/SETRANGE on all size classes incl. chunkmaps, FLUSHDB,
  FLUSHALL), crash points (any I/O completion boundary), crash type (kill -9 / power cut), durability mode, lba 512/4096.
  After recovery check against the reference model (acked state + set of in-flight ops per key):
  I1 every key is in {last acked state, a state produced by an unacked in-flight op}; I2 acked deletes and FLUSH never
  resurrect, expired keys never return; I3 `flushed` mode: every acked effect survives power cuts; `written`: survives
  kill -9; I4 multi-key txns all-or-nothing; I5 recovery is deterministic (same image -> identical index bytes);
  I6 allocator never double-allocates (shadow bitmap); I7 edges E1-E14 hold at issue time; I8 clean-snapshot restart
  and full-scan restart yield identical indexes; I9 no txn marker older than `marker-max-age-ms`; I10 `kvd_unexpected_orphans` stays 0;
  I11 (bounded copies) in every durable state at most two valid copies of a key exist on media, the second only while
  the newer one is not yet durable or while it is a superseded/stale copy awaiting its zeroing (a third may appear only in
  a page torn by power loss and is zeroed by that page's first post-recovery build); I12 at most two VALUE/CHUNKMAP
  extents per key and two versions per chunk position, plus chunks shared by live chunkmaps; I13 no consumed command's
  completion depends on a token bucket, the latency controller, the large-value counter or a txn-credit count; I14
  live-record seq vs device address stays uncorrelated (LC-01 bound) on every run that loads an empty device.
  Budget: 10^4 seeds per PR, >= 10^6 seeds nightly. Failing seeds are minimized and committed as regression tests.
  **Artifacts are seeds and final device images only**; the simulator never writes a per-write trace with payloads
  (LC-33).
* **cluster_sim:** 3 coordinators + 6 nodes (in-process, simulated network with drop/delay/duplicate/partition, clock
  rate drift <= 1e-3, crashes and restarts of any process, slow disks). Checks: no acknowledged write lost; at most one
  serving primary per shard per instant (lease disjointness); writes of an old term never commit after a fence;
  membership/slot map changed only by admin ops; zero full-sync/migration bytes caused by failures alone; repaired
  members converge to the primary (state equality after quiescence); per-key checker: acked writes visible to every
  read that starts after the ack until overwritten, no read returns a never-written value, no acked delete
  resurrects (G4 permits reads of unacked writes); failover with every fence-reply pattern (timeouts of the chosen
  node included) picks by the static rule only; a merge resync of a member with injected divergence converges and
  rewrites only the divergent keys; migrations with B held far behind reach Ready exactly at the recorded barriers.
* **wsr_model:** Stateright model of §23.3 (3 and 5 members, message loss/reorder, crash/restart with A/B files, and
  SETPEERS single-server deltas applied to members in any order): at most one value committed per version; committed
  state never lost; leader completeness; at most one leader per term across peer generations.

### 25.4 Real-process crash and power-cut tests (`tests/crash/`)
* `tests/compat/persistence.py --sync --target-cmd 'target/release/kvd --port {port} --data-dir {dir}'`: >= 20 cycles,
  `--kill-mode inflight` and `quiesced`, for `durability written` and `flushed`; best-effort run for `async`
  (losses reported, no RESURRECTED/EXPIRED_RESURRECTED/CORRUPT/REGRESSED allowed). Also `--require-prefix` is reported.
* Power cut (`tests/crash/powercut.sh`, root CI runner): `tests/crash/faultdev` is a RAM-only NBD server owned by the
  test suite with `SimDevice` semantics. It holds the durable image and the volatile write-back set in RAM, honours
  FLUSH/FUA and (optionally) atomic units, and attaches as `/dev/nbdN` (a whole-disk block device that passes LC-34).
  `kvd` runs a workload on it; at a random point the harness triggers a power cut: unflushed writes are dropped and
  in-flight ones are torn at byte or sector granularity; the resulting image is then served as a fresh device to a
  fresh `kvd`, which is verified against the model (flushed mode). Repeated >= 200 cuts per nightly run. No component
  persists a write-by-write record; the only artifacts of a failure are the seed and the final image (LC-33).
  `ci/invariant_lint.sh` fails on `log-writes` in `tests/` and `ci/`.

### 25.5 Compatibility (`tests/compat/`, standalone)
* `differential.py`: 0 mismatches over 40 seeds x 2000 commands (RESP2/RESP3 connections) against redis-server 7.0.15;
  `--strict-errors` tracked as a goal.
* `scenarios.py`: all tests pass, including the `impl_specific` ones for inline commands, protocol errors, INCRBYFLOAT
  long-double formatting and active expiry.
* redis-benchmark 7: every supported test (`-t ping,set,get,incr,mset`) with `-P 1/16/64`, `-c 50/200`, `-d 3/300/4096`.
* `tests/compat/redis_cli_smoke.sh`: scripted redis-cli session (RESP2 and `-3`), `redis-cli -c` against a cluster in M2.
* Rust integration tests (`redis` crate) for each command module.

### 25.6 Cluster client matrix (`tests/cluster/matrix.sh`, M2)
3 `kvc` + 6 `kvd` on localhost (separate data files). For redis-py (pip), Jedis + Lettuce (Maven, OpenJDK 21),
go-redis and ioredis (if toolchains are present; CI image installs them): bootstrap with documented settings, pipelines,
MULTI on one slot, CROSSSLOT error, kill a primary (failover <= 3 s client-visible), kill the coordinator leader,
migrate a slot range under load, READONLY replica reads, `node add` + `replica add`, coordinator replacement by
`coord setpeers` under load. Each scenario asserts zero lost acknowledged writes (verifier client).
**Coordinator line style (decided at M2.1, recorded in §28):** run the Lettuce scenarios with coordinator lines in
style R1 (slotless `slave` entries with master-id `-`, §23.5) and assert that Lettuce bootstraps, follows
MOVED-to-coordinator and never sends DBSIZE/KEYS/SCAN/FLUSHALL or replica reads to a coordinator. If it passes, R1 ships
and the coordinator's keyless stubs are dropped; if not, R2 ships with constant-literal stubs.

### 25.7 Invariant lint and graph checks (CI, every PR)
`ci/invariant_lint.sh` (LC-01..LC-39 grep/AST rules, including the `docs/` vocabulary scan of LC-35 and the `tests/`/`ci/`
trace-tool scan of LC-33), `cargo deny check` (`ci/deny.toml`), `cargo clippy -- -D warnings` (`clippy.toml`
disallowed types/methods), `ci/check_graph.sh` (crate graph of 24.2, including the kv-coord storage-crate ban of
LC-38), the named LC tests, and the golden tests of the systemd units and of data-node cluster replies (LC-23, LC-24).

### 25.8 Performance regression
criterion micro-benchmarks (index probe/insert, RESP parse, page build); `kvbench` smoke run (60 s) on the CI box with
a stored baseline (fail on > 10 % regression in cached GET ops/s or p99).

---------------------------------------------------------------------------------------------------------------------

## 26. Milestones (P5) and workstreams

| Milestone | Scope | Exit criteria |
|---|---|---|
| **M1.1** foundations (wk 1-3) | kv-common (incl. `redact`), kv-resp, kv-cmd, kv-index (`scan_pages`), kv-storage codecs, `devprobe`, SimDevice, page image builder (13, two-image mode with mandatory R3 zeroing and FFD placement), keyed-draw allocator, partition engine driven by `SyncFileDevice`, full-scan recovery, metastore; storage_sim I1-I7, I11, I14 | property tests green incl. LC-01/LC-09 tests; 10^5 sim seeds clean |
| **M1.2** server (wk 3-6) | runtime (io_uring), net (pre-consume checks), dispatch, XMsg (key bytes only), conn/server/strings/keys/expire commands, standalone multi-key + XTxn, durability modes, FlushTracker, foreground scrubs, INFO/CONFIG, diagnostics log (LC-32) | `differential.py` 0 mismatches; `scenarios.py` pass; `persistence.py --sync` 20 cycles; `logs_have_no_user_bytes` |
| **M1.3** completeness (wk 6-8) | SCAN/KEYS/RANDOMKEY, active expiry, extents + chunkmaps (512 MiB) with twin runs and random carving, sector-granular writes, **atomic page-update mode** (`page-update auto`, default where verified), clean-shutdown snapshot into fixed regions, hole warming, metrics HTTP, unixsocket, COPY, `faultdev` power-cut tests | restart targets measured (§18.6); benchmark baseline vs Aerospike CE on the test box (§27); `faultdev` nightly clean |
| **M1.4** atomicity (wk 8-9) | intent table (commit batching, txn credits), crash-atomic MSET/MSETNX/RENAME/COPY/EXEC, MULTI/EXEC/WATCH, hashes (stretch) | storage_sim I4, I12, I13; 10^6 nightly seeds clean |
| **M2.1** control plane (wk 9-11) | kv-ctrl, kv-topo, kv-coord WSR + A/B persistence + operator-set peer lists + Stateright model, client service (R1/R2 coordinator lines), admin (init, node add, shard create, slots assign), node agent (assignment, leases, gate, constant MOVED) | wsr_model clean incl. SETPEERS; redis-py/Jedis/Lettuce bootstrap against a 1-shard cluster; R1/R2 decision recorded |
| **M2.2** replication (wk 11-13) | per-partition streams, locked non-dumpable backlog, Continue, merge full sync (learner insert for `replica add`), ISR shrink/expand, commit path, `flushed` durability | cluster_sim: no acked loss with replica crashes; `backlog_never_reaches_disk`, `full_sync_keeps_layout` |
| **M2.3** failover (wk 13-15) | detection, static choice, single-node fence, promote, repair, returning members, graceful handoff | cluster_sim 10^5 seeds clean; `promote_isr_independent_of_fence_replies`; client matrix passes kill-primary and kill-leader; server-side failover p99 <= 3 s |
| **M3** operator (wk 15-18) | node remove/maintenance/clear-fail, replica remove/resync (merge), slot migration (copy/position-barrier ready/cutover/seal/done/failure reverts with inert copy, abort/resume), rebalance plan (`--by slots`)/apply (stops on failover), coord setpeers/add/remove/export/recover, kvadm status | migration under load: zero acked loss, TRYAGAIN windows <= 10 ms, `migration_ready_is_position_barrier`, client matrix migration and coordinator-replacement scenarios pass |

Parallel workstreams (week 1 interfaces frozen per §24): (A) kv-index; (B) kv-storage codecs + sim; (C) page writer +
allocator + partition; (D) kv-resp + kv-cmd + command handlers; (E) kv-node runtime/net/dispatch; (F) kv-topo +
kv-coord; (G) kv-repl; (H) tests/compat integration, crash tests, bench.

**Future (not in M1-M3; MUST NOT be implemented without amending this spec):** change-bitmap incremental crash
restart (Appendix B); on-line consolidation of sparse pages (must run concurrently on every member while it keeps
serving reads, with no `MemberState`, CLUSTER SLOTS/SHARDS health or routing change: LC-30); socket-to-extent
streaming and `SEND_ZC`; IOPOLL rings and provided-buffer receive; TLS/mTLS; ACL users; lists/sets/zsets/streams;
pub/sub; Lua/functions; CLIENT TRACKING; `rebalance plan --by keys|bytes` (only from per-slot aggregates volunteered in
`Renew`, §23.12); repair of corrupt pages from replicas; per-tenant quotas; cross-datacenter replication.

---------------------------------------------------------------------------------------------------------------------

## 27. Benchmark plan vs Aerospike CE 8.2 (this box first, production-class later)

### 27.1 Test-box setup (4 vCPU, 15 GiB, virtio ~20K QD1 IOPS, ext4)
* Server pinned to CPUs 0-1 (`taskset -c 0,1`), load generator to CPUs 2-3. Same 16 GiB pre-allocated file on ext4 for
  both systems (default ordered data mode; allowed substrates per LC-34 are listed in `bench/kvd-bench.conf`; the
  kvd file is zero-filled at format), direct I/O for both; `echo 3 > /proc/sys/vm/drop_caches` before each run; 60 s warm-up; 3 runs, median;
  record kernel, CPU model, file system, `fio --direct=1 --rw=randread --bs=4k --iodepth=1/32` baselines (install fio).
* Aerospike CE 8.2.0.1 unpacked to `/opt/as-ce`: `bench.conf` namespace `test`,
  `replication-factor 1`, `storage-engine device { file /data/as/test.dat filesize 16G flush-size 128K
  direct-files true defrag-lwm-pct 50 }`, `service-threads 10`, variant with `post-write-cache 8M`.
* kvd: `kvd bench/kvd-bench.conf` with `device /data/kvd/test.dat 16gb`, `threads 2`, `cache-size 512mb` (dataset
  much larger than the cache so reads hit the device), `durability written` (and `flushed` / `async` variants),
  `write-granularity auto`, `page-update auto` (two-image on the virtio disk; atomic on production 4Kn NVMe).
* Dataset: 20M keys (`key:%016d`, 20 B) x 300 B values. Aerospike ~384 B/record -> ~7.7 GB (48% of the file, inside
  its 50% guidance); kvd ~7.0 GB of pages.
* Load generators: Aerospike `asbench` (tools 13.1.0): `-h 127.0.0.1 -p 3000 -n test -s testset -k 20000000 -o B300
  --send-key`; kvd: `kvbench` (crates/kvbench: closed or open loop, `--ratio R:W`, `--dist uniform|zipf:0.99`,
  `--conns`, `--pipeline`, `--rate`, HDR output with p50/p90/p99/p99.9/p99.99), plus redis-benchmark and
  memtier_benchmark (if installed) as cross-checks.

### 27.2 Workloads
| Id | Workload | asbench | kvbench |
|---|---|---|---|
| W0 | load 20M (insert) | `-w I -z 32` | `--load --conns 64` |
| W1 | 100% read, uniform | `-w RU,100 -t 300 -z 64` | `--ratio 100:0 --conns 64` |
| W2 | 90/10 read/update, uniform | `-w RU,90 -t 300 -z 64` | `--ratio 90:10` |
| W3 | 50/50, uniform | `-w RU,50` | `--ratio 50:50` |
| W4 | 90/10, zipf 0.99 | (asbench key distribution option if available, else skip) | `--dist zipf:0.99` |
| W5 | latency at fixed rates: 25/50/75/90% of each system's W2 max, open loop | `-g <tps>` | `--rate <ops>` |
| W6 | crash restart: kill -9 under W2, time to first served GET; clean restart time | | |
| W7 | RAM per key (RSS delta / keys; Aerospike `index_used_bytes`; kvd `index_bytes` and `alloc_state_bytes` reported separately), disk bytes per key, usable fill (including the snapshot regions), extent fragmentation from random carving (mixed 4 KiB-1 MiB values; target < 2 points of fill lost) | `asinfo -v namespace/test` | INFO memory/kvd |
| W8 | write amplification: `/proc/diskstats` sectors written / user bytes during W2 and W3 | | |
| W9 | delete durability: delete 1M keys, kill -9, restart, count resurrected | | |
| W10 | pipelined/cached: 1M keys, cache fits, `--pipeline 16` and 1 | | |
| W11 | placement audit (kvd only): after W0 and after 3 device wraps of uniform-TTL churn, `kvd check --placement-report` gives \|Spearman(seq, device address)\| over live records and the AG-acquisition sequence statistics (LC-01 bounds) | | `kvd check` |

### 27.3 Report (`bench/report.py` -> markdown)
Throughput (ops/s), p50/p99/p99.9/p99.99 for reads and writes per workload, CPU per op (`/proc/<pid>/stat`), RAM/key,
disk/key, fill, WA, restart times, resurrection counts. Every number states durability semantics side by side:
Aerospike CE acknowledges from its write buffer (device write later, O_DSYNC per flush); compare against kvd `async`
and `written`; `flushed` is reported separately (CE has no commit-to-device).

### 27.4 Production-class plan (after M1.3)
32 vCPU / 256 GiB / 1x gen4 NVMe (i4i.8xlarge-class) and r6gd.8xlarge; 500M-1B keys; W1-W5, W6 restarts, W8 WA, the
§1.3 targets; RF2 runs for both after M2 (Aerospike EE eval for RF2 parity, subject to its EULA).

### 27.5 Expected outcome and honest caveats
On this box both systems are device-bound (~20K QD1 IOPS); the comparison is relative. kvd should win W1 (one read per
GET and a page cache), W7 (12 vs 64 B/key; ~70% vs ~42% usable), W6 clean restart, W9 (CE has no durable deletes), and
tie or win W2 with 512-byte-LBA sector writes. kvd is expected to lose W3 (update-heavy small values): every update
is a random write at the record's fixed page (plus a read when uncached), while Aerospike coalesces writes into large
blocks. W0 bulk load issues random rather than sequential 4 KiB writes (keyed placement, LC-01): throughput-neutral
on NVMe, at most a small device-internal effect while loading. NAND endurance for small uniform updates is worse than
with large-block writes (4 KiB FTL programs); deployment guidance recommends >= 3 DWPD drives for update-heavy
small-value tenants.

---------------------------------------------------------------------------------------------------------------------

## 28. Open engineering questions

Decide by the end of M1.2 unless noted, and record each decision here.

1. Default `page-slack-cap` (1024 vs 512) after measuring the 1 KiB-value band (§11.3, §11.8).
2. Whether `write-granularity lba` is safe on every 512e device: measure the tear granularity (the page theorem,
   §17.3, does not depend on it).
3. `repl-backlog-size` default: sized for replica reconnects and failover Repair while running; it never covers
   restarts (closed decision below).
4. Default `snapshot-region` (3%) vs measured snapshot sizes for small values (§10.10).
5. Whether to cache extents <= 64 KiB (`extent-cache-max`, §12).
6. Coordinator line style R1 vs R2 in CLUSTER NODES (§23.5), decided at M2.1 by the client matrix (§25.6).
   Decision: pending.

Closed decision (normative): replication backlogs are never persisted in any form; Repair coverage after a crash comes
from `maybe_divergent` or an operator resync (LC-23).

---------------------------------------------------------------------------------------------------------------------

## Appendix A. Cost and amplification arithmetic

**Index RAM.** Segment bytes per home slot = `10 * (1 + 1/64) + 80/cap` ~ 10.16; mean load 0.85 -> 11.95 B/key;
+ directories/headers < 0.01 B -> ~11.96 B/key. TTL keys: + `2 * 1.016 / 0.85` = 2.39 -> 14.4 B/key.
1B keys -> 12.0 GB (14.4 GB with TTLs). Aerospike: 64 B x 1B = 59.6 GiB per copy. Allocator state, reported
separately: `page_free` 1 B per SMALL page (~0.09 B/key at ~11 keys/page) plus extent bitmaps (~0.03 B per extent page).
Merge-sync marks use a spare entry bit (0 B).

**SSD footprint (1B keys, 20 B keys, 300 B values).** Two-image mode: 352 B records, 10 per page -> 100M pages = 410 GB
(user bytes 320 GB; 78% of page bytes). At `max-storage-fill 0.95` an AG area of 432 GB, plus the 2 x 3% fixed snapshot
regions: a 459 GB device (~0.5 TB; usable 69.7%). Atomic mode: 11 per page -> 91M pages = 373 GB, a 418 GB device
(usable 76.6%). Aerospike: 384 B x 1B / 0.5 = 768 GB.

**Write amplification (host bytes / user bytes, 300 B values).**
| Operation | 512-B LBA (sector writes) | 4Kn |
|---|---|---|
| in-place update | 512-1024 B -> 1.6-3.2x (mean ~2.4x) | 4096 B -> 12.8x |
| insert (coalesced into fill pages, ~4 per write under load) | ~1.3x | ~3.2x |
| delete | 512 B per delete (1.6x of the record) | 4096 B |
| update of 4 KiB value (2-page extent written into its twin run + 1 sector zeroing) | ~2.1x | ~2.1x |
| update of 2-4 KiB value, atomic mode (1-page extent rewritten in place) | n/a | ~1.3x (3 KiB value) |
| update of >= 64 KiB value | ~1.0-1.07x | ~1.0-1.07x |
| space management (cleaner) | 0 (none exists) | 0 |
Aerospike: full record rewrite per update plus defrag 2x at 50% fill, 4x at 75%, 10x at 90%.

**Endurance example.** 30K updates/s of 300 B on 4Kn: 123 MB/s = 10.6 TB/day (2.8 DWPD on 3.84 TB); with 512-B
sectors ~6.3 TB/day host (FTL still programs 4 KiB units). Guidance: mixed-use drives for update-heavy tiny values.

**Throughput model (90/10, 300 B, uniform, cache hit ratio h).** Device ops per client op =
`0.9 (1-h) + 0.1 ((1-h) + w)` with `w` ~ 1-3 read-equivalents per random small write. A 700K-IOPS device kept <= 65%
busy for p99.99: h = 0 -> ~350K ops/s; h = 0.3 -> ~455K ops/s. CPU: ~4-6 us per non-pipelined SSD-path op, ~3 us per
cached op (kernel TCP dominated) -> CPU is not the bound at 300K ops/s on 32 vCPU. The RAM saved by the index
(~50 GB per 1B keys vs Aerospike) is page cache, which raises h.

---------------------------------------------------------------------------------------------------------------------

## Appendix B. (future) Incremental crash restart with change bitmaps
Not part of M1-M3. Recorded so the format leaves room for it.
* A fixed region of per-page change bits (one bit per data page, 4 KiB blocks with A/B copies and a `base_gen`),
  overwritten in place (Linux md write-intent bitmap style, 2005). A bit must be durable before any write that changes
  the *set of keys* in a page (insert, delete, move, TTL-hint shortening); in-page updates of existing keys set no bit,
  because the index stores only the page.
* An online fuzzy index snapshot alternating between the two fixed snapshot regions (§10.10), started at generation g; commits only
  after every identity-changing write issued before its start is durable; pages with pending stale copies at start are
  carried into generation g's bits.
* Crash recovery = load the committed snapshot, drop its entries that point into changed pages, rescan only changed
  pages, resolve by max seq (fingerprint collisions by key reads), as in 18.3.
* Expected crash restart for 1B keys: snapshot read (~10 GB) + changed pages bounded by `restart-scan-budget` -> ~10-20 s.
* Open issue that blocks it for cluster members: divergence detection after a crash still needs enumeration of records
  newer than the commit watermark, which only a full scan provides (18.4).
