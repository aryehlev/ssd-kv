#!/usr/bin/env bash
# Crate dependency graph check (DESIGN §24.2; LC-24, LC-26, LC-30, LC-37, LC-38).
#
# 1. The workspace contains exactly the crates of §24.1.
# 2. Each crate's direct workspace dependencies (normal + build) are within the §24.2 graph.
#    A crate may also name directly a crate that is already reachable through its listed
#    dependencies (e.g. kv-repl -> kv-common). Documented addition: kv-topo -> kv-resp, required
#    by the normative `render` signatures of §24.10 (kv_resp::ReplyWriter).
# 3. Forbidden reachability, transitive, including dev-dependencies of the crate itself:
#    kv-storage never reaches kv-ctrl/kv-topo/kv-node (LC-30); kv-node never reaches
#    kv-topo/kv-coord (LC-24); kv-coord never reaches kv-index/kv-storage/kv-repl/kv-node nor
#    any crate containing `impl StorageEngine` (LC-38); kvadm never reaches a storage crate
#    (LC-37). Shipped binaries never reach the test-only crates (kv-sim, faultdev) and never
#    enable kv-storage's "sim" feature; kv-node links no RESP client crate (LC-26).
set -euo pipefail
cd "$(dirname "$0")/.."

CARGO=${CARGO:-cargo}
TREE=("$CARGO" tree --locked --target all --prefix none --format '{p}')
fail=0
say() { printf '%-6s %s\n' "$1" "$2"; }

# §24.2, as allowed direct workspace dependencies (normal + build edges).
declare -A ALLOWED=(
  [kv-common]=""
  [kv-resp]="kv-common"
  [kv-cmd]="kv-common kv-resp"
  [kv-index]="kv-common"
  [kv-storage]="kv-common kv-index"
  [kv-repl]="kv-storage kv-common kv-index"
  [kv-ctrl]="kv-common"
  [kv-topo]="kv-ctrl kv-common kv-resp"
  [kv-node]="kv-common kv-resp kv-cmd kv-index kv-storage kv-repl kv-ctrl"
  [kv-coord]="kv-common kv-resp kv-cmd kv-ctrl kv-topo"
  [kvadm]="kv-resp kv-ctrl kv-common"
  [kv-sim]="kv-storage kv-index kv-repl kv-topo kv-coord kv-common kv-ctrl kv-resp kv-cmd"
  [kvbench]="kv-resp kv-common"
  [faultdev]="kv-storage kv-common kv-index"
)

# Forbidden transitive reachability (normal + build + the crate's own dev edges).
declare -A FORBIDDEN=(
  [kv-storage]="kv-ctrl kv-topo kv-node kv-coord kv-repl"
  [kv-index]="kv-storage kv-ctrl kv-topo kv-node kv-coord kv-repl"
  [kv-node]="kv-topo kv-coord kv-sim faultdev"
  [kv-coord]="kv-index kv-storage kv-repl kv-node kv-sim faultdev"
  [kvadm]="kv-index kv-storage kv-repl kv-node kv-coord kv-topo kv-sim faultdev"
  [kvbench]="kv-index kv-storage kv-repl kv-node kv-coord kv-sim faultdev"
  [kv-ctrl]="kv-storage kv-index kv-topo kv-node kv-coord"
  [kv-topo]="kv-storage kv-index kv-repl kv-node kv-coord"
)

# 1. workspace members
mapfile -t MEMBERS < <("${TREE[@]}" --workspace --depth 0 -e normal | awk 'NF {print $1}' | sort -u)
expected=$(printf '%s\n' "${!ALLOWED[@]}" | sort | tr '\n' ' ')
actual=$(printf '%s\n' "${MEMBERS[@]}" | sort | tr '\n' ' ')
if [ "$expected" = "$actual" ]; then
  say ok "workspace members: $actual"
else
  say FAIL "workspace members differ from §24.1: expected [$expected] got [$actual]"
  fail=1
fi
is_member() { local m; for m in "${MEMBERS[@]}"; do [ "$m" = "$1" ] && return 0; done; return 1; }

# 2. direct edges
for c in "${MEMBERS[@]}"; do
  [ -n "${ALLOWED[$c]+x}" ] || continue
  bad=""
  while read -r dep; do
    [ -z "$dep" ] && continue
    is_member "$dep" || continue
    case " ${ALLOWED[$c]} " in *" $dep "*) ;; *) bad="$bad $dep" ;; esac
  done < <("${TREE[@]}" -p "$c" -e normal,build --depth 1 | awk 'NR > 1 && NF {print $1}' | sort -u)
  if [ -z "$bad" ]; then
    say ok "$c: direct workspace deps within §24.2"
  else
    say FAIL "$c: direct dependency not in §24.2:$bad"
    fail=1
  fi
done

# crates whose sources contain an `impl StorageEngine for` (LC-38)
engine_crates=""
for c in "${MEMBERS[@]}"; do
  dir="crates/$c"
  [ "$c" = faultdev ] && dir="tests/crash/faultdev"
  if [ -d "$dir/src" ] && grep -rqE 'impl[[:space:]]+StorageEngine[[:space:]]+for' "$dir/src"; then
    engine_crates="$engine_crates $c"
  fi
done
FORBIDDEN[kv-coord]="${FORBIDDEN[kv-coord]}$engine_crates"
say info "crates containing impl StorageEngine:${engine_crates:- none}"

# 3. forbidden reachability
for c in "${!FORBIDDEN[@]}"; do
  is_member "$c" || continue
  reach=$("${TREE[@]}" -p "$c" -e normal,build,dev | awk 'NR > 1 && NF {print $1}' | sort -u)
  bad=""
  for f in ${FORBIDDEN[$c]}; do
    grep -qx -- "$f" <<<"$reach" && bad="$bad $f"
  done
  if [ -z "$bad" ]; then
    say ok "$c: reaches none of: ${FORBIDDEN[$c]}"
  else
    say FAIL "$c: reaches forbidden crate(s):$bad"
    fail=1
  fi
done

# shipped binaries never enable the simulator feature of kv-storage
for c in kv-node; do
  if "$CARGO" tree --locked --target all -p "$c" -e features --prefix none | grep -q 'kv-storage feature "sim"'; then
    say FAIL "$c: enables kv-storage feature \"sim\" (test-only)"
    fail=1
  else
    say ok "$c: kv-storage feature \"sim\" not enabled"
  fi
done

# LC-26: the data node links no RESP client library
clients=$("${TREE[@]}" -p kv-node -e normal,build | awk 'NR > 1 && NF {print $1}' | grep -xE 'redis|redis-[a-z-]+|fred|deadpool-redis|bb8-redis|mobc-redis' || true)
if [ -z "$clients" ]; then
  say ok "kv-node: links no RESP client crate (LC-26)"
else
  say FAIL "kv-node: links RESP client crate(s): $clients"
  fail=1
fi

if [ "$fail" -ne 0 ]; then
  echo "check_graph: FAILED"
  exit 1
fi
echo "check_graph: all checks passed"
