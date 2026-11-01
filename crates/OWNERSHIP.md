# File ownership for parallel implementation

Each workstream owns the files listed for it and may change their private items freely.

**Frozen:** the root `Cargo.toml` (members, `[workspace.dependencies]`, profiles), `Cargo.lock`
policy, every crate's `Cargo.toml`, and every crate's public interface (the items of
docs/DESIGN.md §24.3-§24.12 and the public types they reference) are frozen. Adding, removing
or changing a public item, a dependency or a crate edge goes through the integrator, who also
re-runs `ci/check_graph.sh` and `ci/invariant_lint.sh`. Workstreams may add private helpers,
private modules and tests inside the files they own.

| Workstream | Scope | Files |
|---|---|---|
| W-A | kv-common, kv-resp, kv-cmd | `crates/kv-common/**` (incl. `build.rs`, `c/ldouble.c`), `crates/kv-resp/**`, `crates/kv-cmd/**` |
| W-B | kv-index | `crates/kv-index/**` |
| W-S1 | kv-storage formats, I/O, simulator device, probing, format | `crates/kv-storage/src/{layout,superblock,agtable,pmeta,intent,record,page,extent,io,sim,devprobe,format}.rs` |
| W-S2 | kv-storage engine | `crates/kv-storage/src/{cache,flush,alloc,stale,ticket,effect,image,keyspace,partition,expire,recovery,snapshot,metastore,engine,lib}.rs` |
| W-E1 | kv-node runtime and plumbing | `crates/kv-node/src/main.rs`, `crates/kv-node/src/{config,shared,metrics,log}.rs`, `crates/kv-node/src/runtime/*`, `crates/kv-node/src/net/*`, `crates/kv-node/src/dispatch/*` |
| W-E2 | kv-node command handlers | `crates/kv-node/src/cmd/*` |
| W-R | kv-repl | `crates/kv-repl/**` |
| W-C | control plane | `crates/kv-ctrl/**`, `crates/kv-topo/**`, `crates/kv-coord/**`, `crates/kvadm/**` |
| W-T | simulators, benchmark, crash tests | `crates/kv-sim/**`, `crates/kvbench/**`, `tests/crash/**` (incl. `faultdev`) |
| integrator | workspace-wide files | `Cargo.toml`, `Cargo.lock`, `crates/*/Cargo.toml`, `rust-toolchain.toml`, `clippy.toml`, `ci/*`, `dist/*`, `LICENSE-*`, `NOTICE`, `README.md`, this file |

Not assigned by the plan: `crates/kv-node/src/cluster/*` (node cluster agent, ownership gate,
lease cell, replication and migration glue). Proposed owner: W-R (docs/DESIGN.md §0 groups the
node cluster agent with kv-repl); to be confirmed by the integrator. `tests/compat/` is the
existing black-box harness and is not modified by any workstream.

Ports of BSD-licensed Redis (<= 7.2) or Valkey code need an entry in `NOTICE`; send it to the
integrator with the change.

## Open points found while scaffolding

These are places where the specification leaves a choice to the implementer or where two parts
of it pull in different directions. The scaffold's resolution is noted; owners may revisit them
through the integrator.

1. **`kv_cmd::Access` lives in kv-common.** `kv_storage::partition::Request` carries an access
   mode, but kv-storage may depend only on kv-common and kv-index (§24.2). The enum is defined
   as `kv_common::Access` and re-exported as `kv_cmd::Access` (same type). (W-A, W-S2)
2. **kv-topo -> kv-resp.** The `render` functions of §24.10 take `kv_resp::ReplyWriter`, so
   kv-topo depends on kv-resp; `ci/check_graph.sh` lists this edge as a documented addition.
   (W-C)
3. **`BlockDevice` and `SyncFileDevice` are in `io.rs`**, next to the I/O types and in a file
   allowed to contain `unsafe` (`pread`/`pwrite`); both are re-exported at the crate root. (W-S1)
4. **No `writer.rs` in kv-storage.** §13 and some lint rules mention `writer.rs`, but the §24.1
   tree has none; the page writer lives in `image.rs`. The lint rules that name `writer.rs`
   apply to it if it is ever added. (W-S2)
5. **Reserved keyword `gen`.** Edition 2024 reserves `gen`; `PeerList.gen` (§24.12) and
   `PageState.gen` (§13.2) are declared as `r#gen` (the same field name). The kvadm
   `coord setpeers <gen>` argument is `r#gen` too. (W-C, W-S2)
6. **Directory size at D = 0 (LC-09).** `(2^D + 1) * 4` bytes is 8 bytes, a power of two, when
   D = 0. Start tables at D = 1 or keep the D = 0 directory inline (no separate allocation).
   (W-B)
7. **Inline commands with escapes.** The parser returns `(offset, len)` pairs into `read_buf`,
   but an inline argument with `\xHH` or quote escapes has no contiguous image there; unescape
   in place (never longer) or use a side buffer. (W-A)
8. **`ProtoError::redis_message` for `ExpectedDollar(b)`.** The Redis text embeds the byte; the
   `&'static str` return value cannot. The documented contract: the caller appends the byte and
   the closing quote. (W-A)
9. **CLOCK_BOOTTIME in `Now::read`.** `std` has no boottime clock and the only kv-common file
   allowed to contain `unsafe` is `ldouble.rs`; add a small clock function to the C shim and
   declare it there. (W-A)
10. **Locked backlog memory.** kv-repl contains no `unsafe`, so the huge-page / `mlock` /
    `MADV_DONTDUMP` mapping for backlogs needs a safe wrapper in an allowed file (kv-storage
    `io.rs` is the natural place). (W-R, W-S1)
11. **Substrate checks for coordinator state files.** LC-34 applies to `kvc` state files, but
    the probing lives in kv-storage `devprobe.rs`, which kv-coord must not link (LC-38). Needs a
    home both can use (for example a small probing module in kv-common). (W-C, W-S1)
12. **Keyed extent checksum.** `RecordCrc::compute(page_id, off, parts)` matches the small
    record crc; the extent crc (§10.9) has no offset. Use `off = 0` or add a method through the
    integrator. (W-A, W-S1)
13. **`xxhash-rust` license.** It is BSL-1.0, which is not in the §24.13 license allow list;
    `ci/deny.toml` allows BSL-1.0 for that crate only.
14. **Lint script name.** The public docs/DESIGN.md names the lint `ci/invariant_lint.sh`;
    `ci/legal_lint.sh` is kept as a one-line alias for existing tooling.
