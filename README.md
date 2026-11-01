# kvssd

A Redis-protocol compatible key-value database (RESP2 and RESP3). The index lives in RAM; keys
and values live on SSD/NVMe in fixed-location 4 KiB pages and extents that are updated in place,
with crash consistency from the page-update rules rather than from a second copy of the data.
A small coordinator service owns the cluster map, routes cluster-aware clients, grants leases
and drives replication, failover and operator-initiated slot migration.

> **Status: under construction.** The project is being rebuilt from scratch as a Cargo
> workspace. The crates currently contain their frozen public interfaces with stub bodies
> (`todo!()`); nothing serves traffic yet. The black-box compatibility harness in
> `tests/compat/` is complete and is the acceptance test for the server.

## Workspace

| Crate | Binary | Purpose |
|---|---|---|
| `crates/kv-common` | | slots, key hashes, checksums, clocks, pacing, A/B blocks, number formats, INCRBYFLOAT arithmetic |
| `crates/kv-resp` | | RESP2/RESP3 parser, reply writer, client-side decoder |
| `crates/kv-cmd` | | command table: arity, flags, key specs, COMMAND INFO/DOCS |
| `crates/kv-index` | | RAM index: per-slot hash tables of ordered linear-probing segments |
| `crates/kv-storage` | | in-place SSD page store: formats, allocator, page writer, recovery |
| `crates/kv-repl` | | per-partition replication streams and migration endpoints |
| `crates/kv-ctrl` | | coordinator <-> data node control messages |
| `crates/kv-topo` | | versioned cluster state, topology replies, redirects |
| `crates/kv-node` | `kvd` | data node |
| `crates/kv-coord` | `kvc` | coordinator |
| `crates/kvadm` | `kvadm` | operator CLI |
| `crates/kv-sim` | | deterministic simulators (test only) |
| `crates/kvbench` | `kvbench` | benchmark client |
| `tests/crash/faultdev` | `faultdev` | RAM-only fault-injection block device (test only) |

Workstream ownership of each file is in [`crates/OWNERSHIP.md`](crates/OWNERSHIP.md).

## Build

Linux with io_uring (kernel 6.1 or newer), Rust 1.97 (`rust-toolchain.toml`) and a C compiler.

```bash
cargo build --workspace
cargo test --workspace
bash ci/check_graph.sh      # crate dependency graph (docs/DESIGN.md §24.2)
bash ci/invariant_lint.sh   # grep-able engineering invariants (docs/DESIGN.md §3, §25.7)
```

## Run (target interface)

```bash
# single node: formats {dir}/kvd.data on first start
target/release/kvd --port 6379 --data-dir /var/lib/kvssd
redis-cli -p 6379 PING

# dedicated devices
target/release/kvd format --device /dev/nvme0n1 --partitions 64
target/release/kvd /etc/kvssd/kvd.conf

# compatibility harness against a running build
python3 tests/compat/differential.py --help
```

Cluster mode adds `kvc` coordinators (`kvc init`, then `kvc`) and is operated with `kvadm`;
see docs/DESIGN.md §23. Systemd units are in `dist/`. `kvd --version` works today; every other
command currently exits with "not yet implemented".

## Documentation

* [`docs/DESIGN.md`](docs/DESIGN.md): the implementation specification (architecture, on-media
  format, crate interfaces, test strategy, milestones).
* [`tests/compat/README.md`](tests/compat/README.md): the compatibility harness.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. Third-party attributions are in [NOTICE](NOTICE).
