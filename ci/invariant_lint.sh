#!/usr/bin/env bash
# Engineering-invariant lint (DESIGN §25.7): the grep-able LC-xx rules, plus the §24 rule that
# `unsafe` appears only in the five allowed files. Every check is named by its LC id; a failing
# check prints the offending lines. Checks that need types or runtime behaviour are covered by
# the named tests of §25 and are listed at the end.
#
# Scope: files tracked by git plus untracked files that are not ignored (so build output,
# local virtualenvs and ignored documents are never scanned). "code" checks ignore comment
# lines (`//`, `//!`, `///`); vocabulary checks (LC-35, LC-38, LC-39) read every line.
#
# Requires: bash >= 4, git, GNU grep with -P (PCRE), awk.
set -uo pipefail
cd "$(dirname "$0")/.."

fail=0
n_checks=0

report() { # id, description, offending lines (empty = pass)
  n_checks=$((n_checks + 1))
  if [ -z "$3" ]; then
    printf 'ok    %-7s %s\n' "$1" "$2"
  else
    printf 'FAIL  %-7s %s\n' "$1" "$2"
    printf '%s\n' "$3" | sed 's/^/        /'
    fail=1
  fi
}

# files PATHSPEC... : existing tracked + untracked-not-ignored files
files() { git ls-files -co --exclude-standard -- "$@" | sort -u | while read -r f; do [ -f "$f" ] && printf '%s\n' "$f"; done; }
# without EXCLUDE_REGEX < list
without() { grep -vE "$1" || true; }
# g PCRE [grep flags] < file list : file:line:text matches
g() { local re=$1; shift; local list; list=$(cat); [ -z "$list" ] && return 0; printf '%s\n' "$list" | xargs -d '\n' grep -nHP "$@" -- "$re" 2>/dev/null || true; }
# code < matches : drop comment lines
code() { grep -vP '^[^:]+:\d+:\s*//' || true; }

# tok WORD... : PCRE matching the snake_case words as identifier components in snake, Camel or
# UPPER case (so `wal` matches `wal_buf`, `WalBuf`, `WAL_SIZE` but not `walk` or `WalkPos`)
tok() {
  local lo=() ca=() up=() w
  for w in "$@"; do
    lo+=("$w")
    ca+=("$(printf '%s' "$w" | sed -E 's/(^|_)([a-z])/\U\2/g')")
    up+=("$(printf '%s' "$w" | tr '[:lower:]' '[:upper:]')")
  done
  local IFS='|'
  printf '(?<![A-Za-z0-9])(?:%s)(?![a-z0-9])|(?<![A-Z0-9])(?:%s)(?![a-z0-9])|(?<![A-Za-z0-9])(?:%s)(?![A-Z0-9])' \
    "${lo[*]}" "${ca[*]}" "${up[*]}"
}

# block FILE PREFIX : lines of the brace-delimited item whose first line starts with the literal
# PREFIX (e.g. 'pub struct Entry {'), as file:line:text
block() {
  [ -f "$1" ] || return 0
  awk -v p="$2" -v f="$1" '
    !on && index($0, p) == 1 { on = 1; depth = 0 }
    on {
      print f ":" NR ":" $0
      n = gsub(/\{/, "{"); m = gsub(/\}/, "}"); depth += n - m
      if ((n > 0 || m > 0) && depth <= 0) exit
    }' "$1"
}

# outside_fn FILE FN : file:line:text of the lines outside `fn FN` (brace counting)
outside_fn() {
  [ -f "$1" ] || return 0
  awk -v fn="$2" -v f="$1" '
    !on && $0 ~ ("fn " fn "[(<]") { on = 1; depth = 0; started = 0 }
    on {
      n = gsub(/\{/, "{"); m = gsub(/\}/, "}"); depth += n - m; if (n > 0) started = 1
      if (started && depth <= 0) on = 0
      next
    }
    { print f ":" NR ":" $0 }' "$1"
}

RS=$(files 'crates/*.rs' 'tests/crash/*.rs')
STORAGE=crates/kv-storage/src

# ---------------------------------------------------------------------------------------------
# LC-01: no log-based data format; placement only through keyed draws
out=$(files "$STORAGE/alloc.rs" "$STORAGE/metastore.rs" "$STORAGE/image.rs" "$STORAGE/writer.rs" |
  g "$(tok append_offset write_head frontier tail_ptr segment_cleaner log_head wal journal aof next_free cursor)" | code)
report LC-01 "no write-frontier/cursor identifiers in alloc, metastore, image, writer" "$out"

out=$(printf '%s\n' "$RS" | grep -E '^crates/(kv-storage|kv-repl|kv-coord)/' | without "^$STORAGE/devprobe\.rs$" |
  g "$(tok append_offset write_head frontier tail_ptr segment_cleaner log_head wal journal aof)" | code)
report LC-01 "no log-structure identifiers in kv-storage, kv-repl, kv-coord (devprobe.rs names refused substrates)" "$out"

out=$(files "$STORAGE/alloc.rs" "$STORAGE/metastore.rs" |
  g '\bfirst_one\b|iter_ones\(\)\s*\.next\(\)|\.(min|max)\(\s*\)' | code)
report LC-01 "alloc/metastore pick pages and AGs only through keyed draws (no first_one, .min(), .max())" "$out"

# ---------------------------------------------------------------------------------------------
# LC-02: no appended files except the two diagnostic logs; no growth; no zone/stream features
out=$(printf '%s\n' "$RS" | without '^crates/(kv-node|kv-coord)/src/log\.rs$' |
  g 'O_APPEND|OpenOptions::append|\.append\(\s*true\s*\)|RWF_APPEND|IORING_OP_\w*APPEND' | code)
report LC-02 "O_APPEND / append opens only in kv-node/src/log.rs and kv-coord/src/log.rs" "$out"

out=$(printf '%s\n' "$RS" | without "^($STORAGE/format\.rs|crates/kv-coord/src/wsr/persist\.rs)$" |
  g '\.set_len\(|\bftruncate\b' | code)
report LC-02 "files are sized only at format / state-file creation (set_len, ftruncate)" "$out"

out=$(printf '%s\n' "$RS" | without "^$STORAGE/devprobe\.rs$" |
  g '(?<![A-Za-z0-9])(zns|ZNS|BLKREPORTZONE|BLKRESETZONE|BLKOPENZONE|BLKCLOSEZONE|BLKFINISHZONE|NVME_IOCTL\w*|nvme_admin\w*|DIRECTIVE\w*|dtype|fdp|FDP|placement_id|F_SET_RW_HINT|F_SET_FILE_RW_HINT|RWH_WRITE_LIFE\w*)(?![A-Za-z0-9])' | code)
report LC-02 "no zone, stream, directive, placement-id or write-lifetime hint identifiers" "$out"

out=$(printf '%s\n' "$RS" | grep -E '^(crates/(kv-common|kv-index|kv-storage|kv-repl|kv-node|kv-sim|kvbench)|tests/crash)/' |
  without "^$STORAGE/devprobe\.rs$" | g '(?<![A-Za-z0-9])zone[ds]?(?![a-z0-9])' | code)
report LC-02 "no zoned-storage identifiers on storage paths (the operator zone label lives in the control plane)" "$out"

out=$(files "$STORAGE/intent.rs" | g '%|wrapping_add|rem_euclid' | code)
report LC-02 "intent.rs: no modular slot arithmetic (lowest-index slot selection)" "$out"

# ---------------------------------------------------------------------------------------------
# LC-03: exactly one storage engine; no log/lsm/wal cargo features
impls=$(printf '%s\n' "$RS" | g 'impl\s+StorageEngine\s+for\b' | code)
cnt=$(printf '%s' "$impls" | grep -c . || true)
if [ "$cnt" -eq 1 ]; then out=""; else out="found $cnt implementations:
$impls"; fi
report LC-03 "exactly one impl StorageEngine" "$out"

out=$(files 'Cargo.toml' 'crates/*/Cargo.toml' 'tests/crash/*/Cargo.toml' | while read -r f; do
  awk -v f="$f" '/^\[/ { on = ($0 ~ /^\[features\]/) ; next } on && /^[A-Za-z0-9_-]+[[:space:]]*=/ { split($0, a, "="); k = a[1]; gsub(/[[:space:]]/, "", k); if (tolower(k) ~ /log|lsm|wal/) print f ":" NR ":" $0 }' "$f"
done)
report LC-03 "no cargo feature names containing log/lsm/wal" "$out"

# ---------------------------------------------------------------------------------------------
# LC-04 / LC-05 / LC-09 / LC-13 / LC-20: the index
out=$(printf '%s\n' "$RS" | grep -E '^crates/(kv-index|kv-storage)/' | g 'BTreeMap|BTreeSet|rbtree|red_black|RBTree' | code)
report LC-04 "no tree maps in kv-index and kv-storage" "$out"

out=$(files 'crates/kv-index' | g 'bumpalo|typed_arena|typed-arena|slab|arena|region_alloc' -i)
report LC-05 "no pooled or grouped allocators in kv-index" "$out"

out=$(files 'crates/kv-index' | g 'repr\s*\(\s*align' | code)
report LC-09 "no repr(align) in kv-index" "$out"

out=$(files "$STORAGE/alloc.rs" | g '\[u8;\s*(4096|PAGE)\]' | code)
report LC-09 "no page-sized array fields in alloc.rs" "$out"

out=$(block crates/kv-index/src/entry.rs 'pub struct Entry {' | code | grep -P '\b(seq|version|next|prev|expire\w*|ttl|ptr|pointer|key|digest)\s*:' || true)
report LC-13 "Entry has no version, chain, expiry, pointer or key field" "$out"

out=$(grep -B3 -n '^pub struct Entry {' crates/kv-index/src/entry.rs | grep -q 'derive([^)]*Copy' && true || echo "crates/kv-index/src/entry.rs: Entry does not derive Copy")
report LC-20 "Entry is Copy" "$out"

out=$(printf '%s\n' "$RS" | g 'default[-_]ttl|db[-_]ttl|namespace[-_]ttl' -i | code)
report LC-13 "no default-TTL configuration" "$out"

# ---------------------------------------------------------------------------------------------
# LC-06 / LC-22 / LC-31: hashing and GPU crates; std SipHash maps
if [ -f Cargo.lock ]; then
  out=$(grep -nE '^name = "(sha1|sha1_smol|sha2|sha3|md-5|md5|blake2|blake3|ripemd[a-z0-9_-]*|siphasher)"' Cargo.lock | sed 's/^/Cargo.lock:/' || true)
  report LC-06 "no cryptographic hash crates in Cargo.lock" "$out"
  out=$(grep -nE '^name = "(cuda[a-z0-9_-]*|cudarc|cust|cust_core|cust_raw|wgpu[a-z0-9_-]*|vulkano[a-z0-9_-]*|opencl[a-z0-9_-]*|ocl|ocl-core|bumpalo|typed-arena)"' Cargo.lock | sed 's/^/Cargo.lock:/' || true)
  report LC-22 "no GPU or pooled-allocator crates in Cargo.lock" "$out"
else
  report LC-06 "Cargo.lock present" "Cargo.lock missing"
fi

out=$(printf '%s\n' "$RS" | g 'std::collections::(\{[^}]*\b)?Hash(Map|Set)\b' | code)
report LC-06 "no std HashMap/HashSet (hashbrown + foldhash)" "$out"

# ---------------------------------------------------------------------------------------------
# LC-08 / LC-21: device chosen by the allocator only
out=$(printf '%s\n' "$RS" | without "^$STORAGE/(alloc|format|recovery)\.rs$" | g 'PageId::new\s*\(' | code)
report LC-08 "PageId::new only in alloc.rs, format.rs, recovery.rs" "$out"

# ---------------------------------------------------------------------------------------------
# LC-10: expiry comparisons only in the EXPIRE-family handlers
out=$(printf '%s\n' "$RS" | without '^crates/kv-node/src/cmd/expire\.rs$' |
  g '\b\w*expire\w*\s*(<=|>=|<|>)\s*&?\w*expire\w*' | code)
report LC-10 "comparisons of two expiry values only in kv-node/src/cmd/expire.rs" "$out"

# ---------------------------------------------------------------------------------------------
# LC-14: no priorities or watermarks on command paths; no clocks in dispatch
out=$(printf '%s\n' "$RS" | grep -E '^crates/(kv-node/src/(net|dispatch|runtime|cluster)|kv-repl|kv-coord)/' |
  without '^crates/kv-coord/src/(failover|admin)\.rs$' | g "$(tok priority high_water low_water watermark)" | code)
report LC-14 "no priority/watermark identifiers in net, dispatch, runtime, cluster, kv-repl, kv-coord" "$out"

out=$(files 'crates/kv-node/src/dispatch' | g 'Instant::now|SystemTime|tokio::time|thread::sleep|\bsleep\s*\(|IORING_OP_TIMEOUT|\bTimeout\b' | code)
report LC-14 "no clocks or timers in dispatch" "$out"

# ---------------------------------------------------------------------------------------------
# LC-15: foreground scrubs and txn credits never wait on pacing or queues
out=$(files "$STORAGE/stale.rs" "$STORAGE/writer.rs" | g 'TokenBucket|LatencyController' | code)
report LC-15 "foreground scrub path does not reference pacing" "$out"

out=$(files "$STORAGE/intent.rs" crates/kv-node/src/net/budget.rs | g 'VecDeque|LinkedList|BinaryHeap|wait_?list|WaitList|waiters?\b' -i | code)
report LC-15 "no waiter queues in intent.rs and net/budget.rs" "$out"

# ---------------------------------------------------------------------------------------------
# LC-16: pacing imports nothing but std/core
out=$(files crates/kv-common/src/pace.rs | g '^\s*(pub\s+)?use\s+(?!(std|core)::)' | code)
out2=$(files crates/kv-common/src/pace.rs | g "$(tok queue_depth qd conn conns connection inflight in_flight lag backlog)" | code)
report LC-16 "pace.rs imports only std/core and reads no queue or connection state" "$out$out2"

# ---------------------------------------------------------------------------------------------
# LC-17: no query machinery; no scripting/sort/module commands in the table
out=$( (files 'crates' 'tests/crash' | grep -iE '(secondary|planner|aggregate|map_reduce)[^/]*$' || true;
        printf '%s\n' "$RS" | g '\bmod\s+(secondary|planner|aggregate|map_reduce)\b' | code) )
report LC-17 "no secondary-index, planner, aggregate or map_reduce modules" "$out"

out=$( (files 'crates/kv-cmd/src' | g '"(EVAL|EVALSHA|EVAL_RO|EVALSHA_RO|FCALL|FCALL_RO|FUNCTION|SCRIPT|SORT|SORT_RO)"|"(FT\.|WASM)' -i | code;
        files crates/kv-cmd/src/spec.rs | g '^\s*(Eval|Evalsha|EvalRo|EvalshaRo|Fcall|FcallRo|Function|Script|Sort|SortRo)\b' | code) )
report LC-17 "command table has no scripting, function, sort or module commands" "$out"

# ---------------------------------------------------------------------------------------------
# LC-18: SCAN-family walks emit page references only
out=$(printf '%s\n' "$RS" | grep -E '^crates/(kv-node/src/cmd/(scan|cluster_cmds)\.rs|kv-repl/)' |
  g 'Vec<\s*Entry\s*>|Vec<\s*\(\s*Entry\s*,|Vec<\s*KeyHash\s*>|\b\w*(fp|hash|digest)\w*\s*:\s*(Small)?Vec<\s*\[?\s*u(32|64)\b|let\s+(mut\s+)?\w*(fp|hash|digest)\w*\s*(:\s*Vec<\s*u(32|64)\s*>\s*)?=\s*(Vec::|vec!)' -i | code)
report LC-18 "no entry, hash or fingerprint lists in scan.rs, cluster_cmds.rs, kv-repl" "$out"

out=$(printf '%s\n' "$RS" | without "^$STORAGE/snapshot\.rs$" | g '\.snapshot_iter\s*\(' | code)
report LC-18 "SlotTable::snapshot_iter called only from kv-storage/src/snapshot.rs" "$out"

out=$(printf '%s\n' "$RS" | without "^$STORAGE/devprobe\.rs$" | g '/sys/block|/sys/dev/block|/sys/class/nvme|nvme_id|NVME_ADMIN' | code)
report LC-18 "/sys/block and NVMe identify data read only in devprobe.rs" "$out"

out=$(block "$STORAGE/devprobe.rs" 'pub struct DevProbe {' | code | grep -P "$(tok nr_requests queue_depth queue_count nr_queues nr_hw_queues qd)" || true)
report LC-18 "DevProbe has no queue-count, nr_requests or queue-depth field" "$out"

defs=$(printf '%s\n' "$RS" | g '\bSCAN_STEP_MAX_PAGES\s*:' | code)
if [ "$(printf '%s\n' "$defs" | grep -c 'crates/kv-index/src/table.rs:[0-9]*:pub const SCAN_STEP_MAX_PAGES: usize = 32;')" -eq 1 ] &&
   [ "$(printf '%s' "$defs" | grep -c .)" -eq 1 ]; then out=""; else out="${defs:-SCAN_STEP_MAX_PAGES not defined}"; fi
report LC-18 "SCAN_STEP_MAX_PAGES is the literal constant 32, defined once" "$out"

# ---------------------------------------------------------------------------------------------
# LC-19: private memory only
out=$( (printf '%s\n' "$RS" | g 'shmget|shm_open|memfd_create|MAP_SHARED' | code; files 'crates/*.c' 'crates/*.h' | g 'shmget|shm_open|memfd_create|MAP_SHARED') )
report LC-19 "no shared-memory primitives" "$out"

# ---------------------------------------------------------------------------------------------
# LC-23: backlogs never reach disk; shipped units lock memory and disable swap and core files
out=$( (block crates/kv-repl/src/backlog.rs 'pub struct Backlog {' | code | grep -P '\b(File|RawFd|OwnedFd|BorrowedFd|fd|fds|file|path)\b' || true;
        files crates/kv-repl/src/backlog.rs | g 'std::fs\b|\bFile::|OpenOptions|\bopen\s*\(|\bwrite_all\b|\bpwrite\b' | code) )
report LC-23 "Backlog has no file or descriptor" "$out"

out=""
for u in dist/kvd.service dist/kvc.service; do
  if [ ! -f "$u" ]; then out="$out$u missing"$'\n'; continue; fi
  for k in 'LimitMEMLOCK=infinity' 'MemorySwapMax=0' 'LimitCORE=0' 'LimitNOFILE=1048576'; do
    grep -qx "$k" "$u" || out="$out$u: missing $k"$'\n'
  done
done
report LC-23 "systemd units: LimitMEMLOCK=infinity, MemorySwapMax=0, LimitCORE=0, LimitNOFILE" "${out%$'\n'}"

# ---------------------------------------------------------------------------------------------
# LC-24: handlers never read ownership fields
out=$(files 'crates/kv-node/src/cmd' | g '\.(owned|shard_slots|members|term|isr_len|sealed_out|pending_seal|min_isr|coord_addrs)\b' | code)
report LC-24 "kv-node/src/cmd/* uses only gate() and slot_filter()" "$out"

# ---------------------------------------------------------------------------------------------
# LC-25: no automatic membership changes; static failover candidates
out=$(files 'crates' 'tests/crash' | g 'auto_full|auto_resync|auto_rebalance|auto_migrat' -i)
report LC-25 "no automatic resync, rebalance or migration identifiers" "$out"

out=$(block crates/kv-coord/src/failover.rs 'pub struct Candidate {' | code | grep -P '\b(applied|position|pos|score|reply|replies|seq|lag|offset)\s*:' || true)
report LC-25 "Candidate holds static operator data only" "$out"

# ---------------------------------------------------------------------------------------------
# LC-26: replies carry no topology; no full map in node messages; no two-phase message types
out=$(awk '/pub enum NodeToCoord/ { on = 1 } on && /node-initiated/ { exit } on { print FILENAME ":" NR ":" $0 }' crates/kv-ctrl/src/msg.rs |
  code | grep -P '\b(role|shard|slot|slots|owned|keys|key_count|term)\s*:' || true)
report LC-26 "kv-ctrl reply variants carry no role/shard/slot/key-count/term field" "$out"

out=$(files 'crates/kv-ctrl/src' | g '\[u16;\s*16384\]|\bClusterState\b|\bShardRecord\b' | code)
report LC-26 "no full slot map type in kv-ctrl" "$out"

out=$(files crates/kv-ctrl/src/msg.rs crates/kv-repl/src/frame.rs | g "$(tok prepare precommit abort vote votes)" | code)
report LC-26 "control and replication messages have no prepare/abort/vote types" "$out"

# ---------------------------------------------------------------------------------------------
# LC-27: ids are not ordered
out=""
for t in NodeId CoordId; do
  grep -qE "assert_not_impl_any!\($t: PartialOrd, Ord\)" crates/kv-ctrl/src/ids.rs || out="$out crates/kv-ctrl/src/ids.rs: missing assert_not_impl_any!($t: PartialOrd, Ord)"
done
report LC-27 "NodeId and CoordId statically asserted not ordered" "$out"

# ---------------------------------------------------------------------------------------------
# LC-28: wall clock only in ttl/expire/clock/metrics/log modules
out=$(printf '%s\n' "$RS" | without '/(ttl|expire|clock|metrics|log)\.rs$' | g 'SystemTime|UNIX_EPOCH|now_unix_ms' | code)
report LC-28 "wall clock read only in ttl/expire/clock/metrics/log modules" "$out"

# ---------------------------------------------------------------------------------------------
# LC-29: full payloads, in-process only
out=$(files 'crates/kv-node' | g 'mkfifo|process::Command\b' | code)
report LC-29 "kv-node spawns no processes and creates no named pipes" "$out"

out=$(files "$STORAGE/effect.rs" crates/kv-repl/src/codec.rs crates/kv-repl/src/frame.rs | g "$(tok digest digests)" | code)
report LC-29 "replication ops have no digest form" "$out"

# ---------------------------------------------------------------------------------------------
# LC-30: health reports carry no storage-maintenance state
out=$(block crates/kv-ctrl/src/msg.rs 'pub struct Health {' | code | grep -P "$(tok scrub compaction compact defrag maintenance reclaim sweep warm warming)" || true)
report LC-30 "Health has no storage-maintenance field" "$out"

# ---------------------------------------------------------------------------------------------
# LC-32: no derived Debug on types holding user bytes (kv-node, kv-storage, kv-repl, kv-coord)
out=$(printf '%s\n' "$RS" | grep -E '^crates/(kv-node|kv-storage|kv-repl|kv-coord)/' | while read -r f; do
  awk -v f="$f" '
    /^[[:space:]]*\/\// { next }
    /#\[derive\([^)]*Debug/ { pend = 1; next }
    pend && /^[[:space:]]*#\[/ { next }
    pend && /(struct|enum)[[:space:]]+[A-Za-z0-9_]+/ {
      pend = 0; name = $0; on = 1; depth = 0; started = 0; hit = ""
    }
    pend && !/^[[:space:]]*$/ { pend = 0 }
    on {
      line = $0
      if (line ~ /(^|[^A-Za-z0-9_])(key|field|value|val|data)[[:space:]]*:/ || line ~ /&\[u8\]|Box<\[u8\]>|Vec<u8>/) hit = hit " L" NR
      n = gsub(/\{/, "{", line); m = gsub(/\}/, "}", line); depth += n - m; if (n > 0) started = 1
      if ((started && depth <= 0) || (!started && $0 ~ /;[[:space:]]*$/)) {
        if (hit != "") print f ": derive(Debug) on a type with user bytes (" hit " ): " name
        on = 0
      }
    }' "$f"
done)
report LC-32 "no derive(Debug) on types with key/field/value/data or byte-buffer fields" "$out"

out=$(grep -q 'pub struct Redacted' crates/kv-common/src/redact.rs 2>/dev/null && echo "" || echo "crates/kv-common/src/redact.rs: Redacted missing")
report LC-32 "kv_common::redact::Redacted exists" "$out"

# ---------------------------------------------------------------------------------------------
# LC-33: test tooling never records write-by-write traces
LC33_RE='(?<![A-Za-z0-9])(log-writes|blktrace|btrace|dm-log)(?![A-Za-z0-9])'
out=$(files 'tests' 'ci' | g "$LC33_RE" | grep -v '^ci/invariant_lint\.sh:[0-9]*:LC33_RE=' || true)
report LC-33 "no write-trace tooling in tests/ and ci/" "$out"

out=$(outside_fn tests/crash/faultdev/src/main.rs write_final_image | code | grep -P 'File::create|OpenOptions|fs::write|fs::File|File::options' || true)
report LC-33 "faultdev creates files only in write_final_image" "$out"

out=$( (for f in $(files 'crates/kv-sim/src'); do outside_fn "$f" write_artifact; done) | code | grep -P 'File::create|OpenOptions|fs::write|fs::File|File::options' || true)
sig=$(grep -cE 'pub fn write_artifact\(dir: &Path, seed: u64, image: &\[u8\]\)' crates/kv-sim/src/lib.rs || true)
[ "$sig" = 1 ] || out="$out crates/kv-sim/src/lib.rs: write_artifact(dir, seed, image) signature missing"
report LC-33 "kv-sim writes only (seed, image) artifacts" "$out"

# ---------------------------------------------------------------------------------------------
# LC-34: space pre-allocation only at format
out=$(printf '%s\n' "$RS" | without "^$STORAGE/format\.rs$" | g '\b(posix_)?fallocate\b' | code)
report LC-34 "fallocate only in kv-storage/src/format.rs" "$out"

# ---------------------------------------------------------------------------------------------
# LC-35: vocabulary (docs, README, Rust comments and CLI help, units)
VOCAB='log-structured|append-only|write-ahead|journal|group.commit'
out=$( (files 'docs' 'README.md' 'crates' 'tests/crash' 'dist' | without "^($STORAGE/devprobe\.rs|docs/DESIGN\.md)$" | g "$VOCAB" -i;
        # DESIGN.md: §3 (invariants) and §28 (open questions) may name the excluded structures
        if [ -n "$(files docs/DESIGN.md)" ]; then
          awk '/^## / { skip = ($0 ~ /^## (3|28)\./) } !skip { print FILENAME ":" NR ":" $0 }' docs/DESIGN.md | grep -iP "^[^:]+:\d+:.*($VOCAB)" || true
        fi) )
report LC-35 "no log vocabulary for product structures" "$out"

# ---------------------------------------------------------------------------------------------
# LC-36: background jobs advance by position barriers, never by load-vs-constant tests
LOAD='\w*(lag|backlog|outstanding|inflight|in_flight|pending_groups|unacked_count)\w*'
CONST='(\d[\d_]*|\w*(cfg|conf|config|params?|limit|threshold)\w*(\.\w+)*|self\.(cfg|config|params)\.\w+)'
out=$(printf '%s\n' "$RS" | grep -E '^crates/(kv-repl|kv-coord|kv-node/src/cluster)/' |
  without '^crates/kv-repl/src/(backlog|codec|frame)\.rs$' |
  g "\b$LOAD(\(\))?\s*(<=|>=|==|!=|<|>)\s*$CONST\b|\b$CONST\s*(<=|>=|==|!=|<|>)\s*$LOAD\b" -i | code)
report LC-36 "no comparison of lag/backlog/in-flight sizes against constants or config" "$out"

# ---------------------------------------------------------------------------------------------
# LC-37: fan-out and concurrency never derived from devices
out=$(files crates/kv-node/src/runtime/xmsg.rs crates/kv-node/src/dispatch/xtxn.rs | g '\bKeyHash\b' | code)
report LC-37 "XMsg / XTxnMsg carry key bytes only (no KeyHash)" "$out"

out=$(files crates/kv-node/src/config.rs | g '(per[-_]device|concurrency|[-_]streams\b)|(?<!recovery)[-_]qd\b' -i | code)
report LC-37 "config schema has no per-device, concurrency, stream-count or queue-depth key (except recovery-qd)" "$out"

out=$(printf '%s\n' "$RS" | g '\bnum_cpus\b|_SC_NPROCESSORS|get_nprocs' | code)
report LC-37 "threads default only from available_parallelism" "$out"

# ---------------------------------------------------------------------------------------------
# LC-38: coordinators are a topology service
out=$(block crates/kv-topo/src/state.rs 'pub struct ClusterState {' | code | grep -P '\b(CoordRecord|CoordEndpoint|CoordId|coordinators|coords|peers|peer_list)\b' || true)
out2=$(files 'crates/kv-topo/src' | g '\bCoordRecord\b' | code)
report LC-38 "ClusterState holds no coordinator list" "$out$out2"

out=$(files crates/kv-coord/src/clientsvc.rs | g '\b(static|let|fn)\s+STUB_|\bconst\s+STUB_\w+\s*:(?!\s*&\[u8\])' | code)
out2=$(files 'crates/kv-coord' | g '\b(Keyspace|PartitionStore|kv_storage|kv_index|StorageEngine)\b' | code)
report LC-38 "coordinator stubs are const &[u8] literals; no keyspace or storage types" "$out$out2"

out=$(files 'crates/kv-coord' | g 'master node|database node|database server' -i)
report LC-38 "coordinator text never says master node / database node" "$out"

# ---------------------------------------------------------------------------------------------
# LC-39: replica sync never reclaims or relocates
out=$(files 'crates/kv-repl' | g '\b(reset_allocator|release_all_ags|set_flush_cutoff|move_record|bulk_release)\b' | code)
report LC-39 "kv-repl calls no allocator reset, bulk release or record move" "$out"

out=$(files 'crates/kvadm' | g 'compact|defrag|reclaim' -i)
report LC-39 "kvadm text never presents resync/failover/maintenance as space reclamation" "$out"

# ---------------------------------------------------------------------------------------------
# §24: `unsafe` only in the five allowed files, each use with a SAFETY comment
UNSAFE_OK='^(crates/kv-index/src/sysbuf\.rs|crates/kv-storage/src/io\.rs|crates/kv-storage/src/cache\.rs|crates/kv-node/src/runtime/ring\.rs|crates/kv-common/src/ldouble\.rs)$'
out=$(printf '%s\n' "$RS" | without "$UNSAFE_OK" | g '\bunsafe\b' | code)
report U-01 "unsafe only in sysbuf.rs, io.rs, cache.rs, ring.rs, ldouble.rs (§24)" "$out"

out=$(printf '%s\n' "$RS" | grep -E "$UNSAFE_OK" | while read -r f; do
  awk -v f="$f" '
    { hist[NR] = $0 }
    $0 !~ /^[[:space:]]*\/\// && $0 ~ /(^|[^A-Za-z0-9_])unsafe([^A-Za-z0-9_]|$)/ {
      ok = 0
      for (i = NR - 1; i >= NR - 8 && i > 0; i--) if (hist[i] ~ /SAFETY:/) { ok = 1; break }
      if (!ok) print f ":" NR ": unsafe without a preceding // SAFETY: comment"
    }' "$f"
done)
report U-02 "every unsafe block/impl/extern has a SAFETY: comment" "$out"

out=$(files 'crates/*/src/lib.rs' 'crates/*/src/main.rs' 'tests/crash/*/src/main.rs' | while read -r f; do
  grep -q '^#!\[forbid(unsafe_op_in_unsafe_fn)\]' "$f" || echo "$f: missing #![forbid(unsafe_op_in_unsafe_fn)]"
done)
report U-03 "every crate root forbids unsafe_op_in_unsafe_fn" "$out"

# ---------------------------------------------------------------------------------------------
cat <<'EOF'
note  covered by named tests rather than this lint: LC-01 placement properties, LC-07, LC-11,
      LC-12, LC-17 golden replies, LC-23 smaps check, LC-24 golden replies, LC-25 property
      tests, LC-27 epochs, LC-28 golden replies, LC-32 log scan, LC-36/37 tests, LC-38 model,
      LC-39 full_sync_keeps_layout (DESIGN §25).
EOF

if [ "$fail" -ne 0 ]; then
  echo "invariant_lint: FAILED ($n_checks checks)"
  exit 1
fi
echo "invariant_lint: all $n_checks checks passed"
