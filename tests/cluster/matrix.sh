#!/usr/bin/env bash
# Cluster client matrix (DESIGN §25.6): 3 kvc coordinators + 6 kvd data nodes on localhost,
# each data node on its own pre-allocated data file, driven by real Redis-Cluster client
# libraries: redis-py (RESP2, RESP3), Jedis, Lettuce (RESP2, RESP3), go-redis (RESP2, RESP3)
# and ioredis, each with its documented settings (§23.5).
#
# Topology: shard 1 = n0 (primary) + n1, shard 2 = n2 (primary) + n3, shard 3 = n4; n5 is
# started but registered only in the node-add scenario. Coordinator lines style r1 (default)
# or r2 (KV_MATRIX_STYLE).
#
# Scenarios, in order, while every client keeps writing (a verifier writer per client):
#   static      bootstrap, pipelines, MULTI/EXEC on one slot, server CROSSSLOT error
#   kill-primary   kill -9 the primary of shard 2 (failover <= 3 s client-visible), then the
#                  old primary restarts and returns as a member
#   kill-leader    kill -9 the coordinator leader; a new leader takes over; it restarts
#   migrate        `kvadm migrate` of a slot range from shard 1 to shard 3 under the load
#   node-add       `kvadm node add` of n5 + `replica add 3 n5` (learner merge full sync)
#   readonly       after the writers stop: READONLY replica reads through each library
#   verify         every client re-reads its keys: zero acknowledged writes lost
#
# Clients whose toolchain or library is missing are reported as SKIP. Every wait has a
# deadline, every client runs under `timeout`, and a watchdog ends the whole run (and every
# process it started) after KV_MATRIX_LIMIT_S seconds: a hang fails the run, it never blocks.
#
# Environment (all optional):
#   KVD_BIN KVC_BIN KVADM_BIN   binaries (default: $CARGO_TARGET_DIR or target, /debug/)
#   KV_MATRIX_DIR               work directory (default: $TMPDIR/kv-matrix-<pid>); kept on failure
#   KV_MATRIX_BASE_PORT         first port of the 30 ports used (default 29000)
#   KV_MATRIX_CLIENTS           subset, e.g. "py2 py3 jedis lettuce2 lettuce3 go2 go3 io"
#   KV_MATRIX_STYLE             r1 | r2 (coordinator CLUSTER NODES lines, default r1)
#   KV_MATRIX_PYTHON            python with redis-py (default python3)
#   KV_MATRIX_JAVA_CP           classpath with Jedis + Lettuce jars (default: mvn dependency:build-classpath)
#   KV_MATRIX_GOMODCACHE        module cache holding go-redis (default: go env GOMODCACHE)
#   KV_MATRIX_NODE_MODULES      node_modules holding ioredis (default: tests/cluster/node/node_modules)
#   KV_MATRIX_LEASE_MS          coordinator lease_ms (default: the cluster default)
#   KV_MATRIX_FAILOVER_MAX_S    client-visible failover bound (default 3)
#   KV_MATRIX_LIMIT_S           watchdog limit for the whole run (default 1500)
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
TGT=${CARGO_TARGET_DIR:-$ROOT/target}
KVD=${KVD_BIN:-$TGT/debug/kvd}
KVC=${KVC_BIN:-$TGT/debug/kvc}
KVADM=${KVADM_BIN:-$TGT/debug/kvadm}
W=${KV_MATRIX_DIR:-${TMPDIR:-/tmp}/kv-matrix-$$}
BASE=${KV_MATRIX_BASE_PORT:-29000}
STYLE=${KV_MATRIX_STYLE:-r1}
PY=${KV_MATRIX_PYTHON:-python3}
CLIENTS=${KV_MATRIX_CLIENTS:-"py2 py3 jedis lettuce2 lettuce3 go2 go3 io"}
FAILOVER_MAX=${KV_MATRIX_FAILOVER_MAX_S:-3}
LIMIT=${KV_MATRIX_LIMIT_S:-1500}
TOKEN=matrix-cluster-token
ADMIN=matrix-admin-token
NODES=6
PARTS=16

mkdir -p "$W" || exit 1
CTRL=$W/ctrl
rm -rf "$CTRL" && mkdir -p "$CTRL"
PHASES=$W/phases
: > "$PHASES"
LOG=$W/matrix.log
: > "$LOG"

say() { printf '%s %s\n' "$(date +%H:%M:%S)" "$*" | tee -a "$LOG"; }
die() { say "FAIL: $*"; exit 1; }
now() { date +%s.%N; }

for b in "$KVD" "$KVC" "$KVADM"; do
  [ -x "$b" ] || die "$b not found (cargo build -p kv-node -p kv-coord -p kvadm, or set KVD_BIN/KVC_BIN/KVADM_BIN)"
done

# ---------------------------------------------------------------------------------------------
# process bookkeeping, cleanup and the watchdog

declare -a CPID NPID CLPID
CLNAME=()
cleanup() {
  local code=$?
  trap - EXIT INT TERM
  exec 2>/dev/null
  for p in "${CLPID[@]:-}"; do
    [ -n "$p" ] && kill -9 -- "-$p" 2>/dev/null
  done
  for p in "${NPID[@]:-}" "${CPID[@]:-}"; do
    [ -n "$p" ] && kill -9 "$p" 2>/dev/null
  done
  touch "$DONE_FLAG" 2>/dev/null
  wait 2>/dev/null
  if [ "$code" = 0 ] && [ -z "${KV_MATRIX_KEEP:-}" ]; then
    rm -rf "$W"
  else
    # keep logs and results, drop the large data files
    rm -rf "$W"/n[0-9] 2>/dev/null
    echo "logs kept in $W"
  fi
  exit "$code"
}
trap cleanup EXIT
trap 'say "interrupted"; exit 130' INT TERM

MAIN=$$
DONE_FLAG=$W/.done
rm -f "$DONE_FLAG"
(
  for ((t = 0; t < LIMIT; t++)); do
    sleep 1
    { [ -e "$DONE_FLAG" ] || ! kill -0 "$MAIN" 2>/dev/null; } && exit 0
  done
  echo "watchdog: matrix exceeded ${LIMIT}s" >> "$LOG"
  kill -TERM "$MAIN" 2>/dev/null; sleep 10; kill -9 "$MAIN" 2>/dev/null
) &
WATCHDOG=$!

# wait_for SECS DESCRIPTION CMD...: polls CMD every 0.2 s until it succeeds or SECS pass
wait_for() {
  local secs=$1 what=$2; shift 2
  local end=$(( $(date +%s) + secs ))
  until "$@" >/dev/null 2>&1; do
    [ "$(date +%s)" -ge "$end" ] && { say "timed out after ${secs}s waiting for $what"; return 1; }
    sleep 0.2
  done
}

# ---------------------------------------------------------------------------------------------
# ports and processes

cport() { echo $(( BASE + 1 + $1 * 3 )); }       # client (RESP) port of coordinator i
pport() { echo $(( BASE + 2 + $1 * 3 )); }       # peer port
aport() { echo $(( BASE + 3 + $1 * 3 )); }       # admin port
nport() { echo $(( BASE + 100 + $1 * 3 )); }     # node RESP port
ncport() { echo $(( BASE + 101 + $1 * 3 )); }    # node control port
nrport() { echo $(( BASE + 102 + $1 * 3 )); }    # node replication port

SEEDS=$(for i in 0 1 2; do printf '127.0.0.1:%s,' "$(cport $i)"; done); SEEDS=${SEEDS%,}
ADMINS=(); for i in 0 1 2; do ADMINS+=(--coord "127.0.0.1:$(aport $i)"); done
adm() { timeout 30 "$KVADM" "${ADMINS[@]}" --token "$ADMIN" "$@"; }
# adm_retry SECS ARGS...: an admin command retried while the cluster settles (a node added a
# moment ago is flagged failed until its first lease renewal)
adm_retry() {
  local secs=$1; shift
  local end=$(( $(date +%s) + secs )) out
  until out=$(adm "$@" 2>&1); do
    [ "$(date +%s)" -ge "$end" ] && { echo "$out"; return 1; }
    sleep 0.3
  done
  echo "$out"
}

start_coord() {
  local i=$1
  "$KVC" --dir "$W/c$i" --cluster-auth-token "$TOKEN" --admin-token "$ADMIN" \
    --coord-line-style "$STYLE" --logfile "$W/c$i.log" --loglevel verbose \
    < /dev/null > /dev/null 2>> "$W/c$i.err" &
  CPID[$i]=$!
}

start_node() {
  local i=$1
  "$KVD" --bind 127.0.0.1 --port "$(nport "$i")" --data-dir "$W/n$i" --data-size 640mb \
    --format-partitions "$PARTS" --cluster-enabled yes --cluster-auth-token "$TOKEN" \
    --cluster-control-port "$(ncport "$i")" --cluster-repl-port "$(nrport "$i")" \
    --threads 2 --repl-backlog-size 4mb --cache-size 64mb --snapshot-region 0 \
    --logfile "$W/n$i.log" --loglevel verbose < /dev/null > /dev/null 2>> "$W/n$i.err" &
  NPID[$i]=$!
}

kill9() { [ -n "${1:-}" ] && kill -9 "$1" 2>/dev/null; wait "$1" 2>/dev/null; true; }

# A RESP round trip with bash's /dev/tcp (PING -> +PONG); bounded by timeout.
ping_port() { timeout 2 bash -c "exec 3<>/dev/tcp/127.0.0.1/$1; printf 'PING\r\n' >&3; head -c 5 <&3" 2>/dev/null | grep -q PONG; }

status() { adm status 2>/dev/null; }
leader_index() {
  local s; s=$(status) || return 1
  for i in 0 1 2; do echo "$s" | grep -q "leader c$i\b" && { echo "$i"; return 0; }; done
  return 1
}
has_leader() { leader_index >/dev/null; }
member_line() { status | grep -E "^\s+${NID[$1]} (primary|replica)" | head -1; }
is_primary() { member_line "$1" | grep -q " primary .*isr"; }
in_sync() { member_line "$1" | grep -q "in-sync isr"; }
phase_begin() { PH_NAME=$1; PH_T0=$(now); say "== phase $1"; }
phase_end() { echo "$PH_NAME $PH_T0 $(now)" >> "$PHASES"; }

# ---------------------------------------------------------------------------------------------
# client toolchains

build_clients() {
  local avail=()
  for c in $CLIENTS; do
    case $c in
      py2|py3)
        if "$PY" -c 'import redis, redis.cluster' 2>/dev/null; then avail+=("$c"); else say "SKIP $c: redis-py not importable by $PY"; fi ;;
      jedis|lettuce2|lettuce3)
        if [ -z "${JAVA_OK:-}" ]; then
          JAVA_OK=no
          if command -v javac >/dev/null && command -v java >/dev/null; then
            local cp=${KV_MATRIX_JAVA_CP:-}
            if [ -z "$cp" ] && command -v mvn >/dev/null; then
              (cd "$HERE/java" && timeout 600 mvn -q dependency:build-classpath -Dmdep.outputFile="$W/java.cp" >> "$W/build.log" 2>&1) && cp=$(cat "$W/java.cp")
            fi
            if [ -n "$cp" ] && mkdir -p "$W/java" && timeout 300 javac -d "$W/java" -cp "$cp" "$HERE/java/src/main/java/MatrixClient.java" >> "$W/build.log" 2>&1; then
              JAVA_CP="$W/java:$cp"; JAVA_OK=yes
            fi
          fi
        fi
        if [ "$JAVA_OK" = yes ]; then avail+=("$c"); else say "SKIP $c: no JDK, Maven or Jedis/Lettuce classpath (see $W/build.log)"; fi ;;
      go2|go3)
        if [ -z "${GO_OK:-}" ]; then
          GO_OK=no
          if command -v go >/dev/null; then
            local env=(GOFLAGS=-mod=mod GOTOOLCHAIN=local)
            [ -n "${KV_MATRIX_GOMODCACHE:-}" ] && env+=(GOMODCACHE="$KV_MATRIX_GOMODCACHE" GOPROXY=off)
            (cd "$HERE/go" && env "${env[@]}" GOCACHE="${GOCACHE:-$W/gocache}" timeout 600 go build -o "$W/goclient" . >> "$W/build.log" 2>&1) && GO_OK=yes
          fi
        fi
        if [ "$GO_OK" = yes ]; then avail+=("$c"); else say "SKIP $c: go toolchain or go-redis module missing (see $W/build.log)"; fi ;;
      io)
        local nm=${KV_MATRIX_NODE_MODULES:-$HERE/node/node_modules}
        if command -v node >/dev/null && [ -d "$nm/ioredis" ]; then NODE_MODS=$nm; avail+=("$c"); else say "SKIP io: node or ioredis missing ($nm)"; fi ;;
      *) say "unknown client $c" ;;
    esac
  done
  RUN_CLIENTS=("${avail[@]}")
}

start_client() {
  local c=$1 cmd
  case $c in
    py2) cmd=("$PY" "$HERE/py/client.py" --name py2 --proto 2) ;;
    py3) cmd=("$PY" "$HERE/py/client.py" --name py3 --proto 3) ;;
    jedis|lettuce2|lettuce3) cmd=(java -cp "$JAVA_CP" MatrixClient --name "$c") ;;
    go2) cmd=("$W/goclient" --name go2 --proto 2) ;;
    go3) cmd=("$W/goclient" --name go3 --proto 3) ;;
    io) cmd=(env NODE_PATH="$NODE_MODS" node "$HERE/node/client.js" --name io) ;;
  esac
  # its own process group (setsid), so that cleanup ends the client together with `timeout`
  setsid timeout -k 5 "$((LIMIT - 60))" "${cmd[@]}" --ctrl "$CTRL" --seeds "$SEEDS" > "$W/client-$c.out" 2>&1 &
  CLPID+=($!)
  CLNAME+=("$c")
}

# ---------------------------------------------------------------------------------------------
# bring-up

say "work dir $W, coordinator style $STYLE, binaries $(dirname "$KVD")"
LIST=""
for i in 0 1 2; do LIST+="c$i=127.0.0.1:$(pport $i)/$(cport $i)/$(aport $i),"; done; LIST=${LIST%,}
for i in 0 1 2; do
  rm -rf "$W/c$i"
  "$KVC" init --dir "$W/c$i" --id "c$i" --peers "$LIST" --half-bytes 1048576 > /dev/null || die "kvc init c$i"
  start_coord $i
done
for i in $(seq 0 $((NODES - 1))); do rm -rf "$W/n$i"; mkdir -p "$W/n$i"; start_node "$i"; done
wait_for 30 "a coordinator leader" has_leader || die "no coordinator leader"
if [ -n "${KV_MATRIX_LEASE_MS:-}" ]; then adm cluster params set lease_ms "$KV_MATRIX_LEASE_MS" > /dev/null || die "params"; fi
adm cluster params set partitions "$PARTS" > /dev/null || die "params partitions"
for i in $(seq 0 $((NODES - 1))); do
  wait_for 90 "node $i to listen" ping_port "$(nport "$i")" || die "node $i did not start: $(tail -3 "$W/n$i.err")"
done
declare -a NID
register() {
  local out
  out=$(adm node add "127.0.0.1:$(ncport "$1")" --client "127.0.0.1:$(nport "$1")" --repl "127.0.0.1:$(nrport "$1")" --zone "z$1") || return 1
  NID[$1]=$(echo "$out" | awk '{print $3}')
  [ -n "${NID[$1]}" ]
}
for i in 0 1 2 3 4; do wait_for 30 "node add n$i" register "$i" || die "node add n$i"; done
adm_retry 30 shard create 1 --primary "${NID[0]}" > /dev/null || die "shard create 1"
adm_retry 30 shard create 2 --primary "${NID[2]}" > /dev/null || die "shard create 2"
adm_retry 30 shard create 3 --primary "${NID[4]}" > /dev/null || die "shard create 3"
adm_retry 30 slots assign 0-8191 1 > /dev/null || die "slots assign 1"
adm_retry 30 slots assign 8192-16383 2 > /dev/null || die "slots assign 2"
for i in 0 2 4; do wait_for 60 "n$i primary" is_primary "$i" || die "n$i is not primary"; done
adm_retry 30 replica add 1 "${NID[1]}" > /dev/null || die "replica add 1"
adm_retry 30 replica add 2 "${NID[3]}" > /dev/null || die "replica add 2"
for i in 1 3; do wait_for 90 "n$i in sync" in_sync "$i" || die "n$i not in sync"; done
say "cluster up: $(status | grep -c ' isr$') ISR members"
status >> "$LOG"

build_clients
[ "${#RUN_CLIENTS[@]}" -gt 0 ] || die "no client toolchain available"
say "clients: ${RUN_CLIENTS[*]}"

# ---------------------------------------------------------------------------------------------
# scenarios

phase_begin static
for c in "${RUN_CLIENTS[@]}"; do start_client "$c"; done
all_static() {
  local n=0
  for c in "${RUN_CLIENTS[@]}"; do [ -f "$CTRL/$c.static" ] && n=$((n + 1)); done
  [ "$n" = "${#RUN_CLIENTS[@]}" ]
}
any_dead() {
  local i
  for i in "${!CLPID[@]}"; do kill -0 "${CLPID[$i]}" 2>/dev/null || return 0; done
  return 1
}
static_or_dead() { all_static || any_dead; }
wait_for 240 "every client's static checks" static_or_dead
for i in "${!CLPID[@]}"; do
  c=${CLNAME[$i]}
  if [ ! -f "$CTRL/$c.static" ]; then die "$c failed its static checks: $(tail -3 "$W/client-$c.out")"; fi
done
phase_end
sleep 3

phase_begin kill-primary
kill9 "${NPID[2]}"
T_KILL=$(now)
wait_for 60 "a promotion in shard 2" is_primary 3 || die "shard 2 was not failed over"
say "promotion committed $(awk "BEGIN{printf \"%.2f\", $(now) - $T_KILL}") s after the kill"
sleep 5
start_node 2
wait_for 90 "n2 to listen again" ping_port "$(nport 2)" || die "n2 did not restart"
wait_for 120 "n2 to return in sync" in_sync 2 || die "n2 did not return in sync"
phase_end
sleep 2

phase_begin kill-leader
L=$(leader_index) || die "no leader"
kill9 "${CPID[$L]}"
T_KILL=$(now)
new_leader() { local l; l=$(leader_index) && [ "$l" != "$L" ]; }
wait_for 30 "a new coordinator leader" new_leader || die "no new leader after killing c$L"
say "new leader c$(leader_index) $(awk "BEGIN{printf \"%.2f\", $(now) - $T_KILL}") s after killing c$L"
sleep 5
start_coord "$L"
sleep 3
phase_end

phase_begin migrate
out=$(adm_retry 30 migrate 0-4095 --to 3) || die "migrate: $out"
MID=$(echo "$out" | awk '{print $3}')
mig_done() { adm migrations | grep -E "^$MID " | grep -q "phase=done"; }
wait_for 240 "migration $MID to finish" mig_done || { adm migrations >> "$LOG"; die "migration did not finish"; }
sleep 3
phase_end

phase_begin node-add
wait_for 30 "node add n5" register 5 || die "node add n5"
adm_retry 30 replica add 3 "${NID[5]}" > /dev/null || die "replica add 3"
wait_for 180 "n5 in sync" in_sync 5 || die "n5 not in sync"
sleep 3
phase_end
status >> "$LOG"

phase_begin readonly-verify
touch "$CTRL/stop"
fails=0
for i in "${!CLPID[@]}"; do
  if ! wait "${CLPID[$i]}"; then fails=$((fails + 1)); fi
done
CLPID=()
phase_end

# ---------------------------------------------------------------------------------------------
# report

"$PY" - "$CTRL" "$PHASES" "$FAILOVER_MAX" "${RUN_CLIENTS[@]}" <<'EOF' | tee -a "$LOG"
import json, os, sys
ctrl, phases, fmax, clients = sys.argv[1], sys.argv[2], float(sys.argv[3]), sys.argv[4:]
ph = []
for line in open(phases):
    n, a, b = line.split()
    ph.append((n, float(a), float(b)))
bad = 0
print("%-9s %-6s %8s %8s %5s  %s" % ("client", "result", "acked", "unacked", "lost", "max outage per phase (s)"))
for c in clients:
    p = os.path.join(ctrl, c + ".result")
    if not os.path.exists(p):
        print("%-9s %-6s (no result file)" % (c, "FAIL")); bad += 1; continue
    r = json.load(open(p))
    per = []
    fail_ok = True
    for n, a, b in ph:
        if n in ("static", "readonly-verify"):
            continue
        m = 0.0
        for o in r.get("outages", []):
            s, e = o[0], o[1]
            if e >= a and s <= b:
                m = max(m, e - s)
        per.append("%s=%.2f" % (n, m))
        if n == "kill-primary" and m > fmax:
            fail_ok = False
    ok = r.get("ok") and r.get("lost", 1) == 0 and fail_ok
    bad += 0 if ok else 1
    print("%-9s %-6s %8d %8d %5d  %s" % (c, "ok" if ok else "FAIL", r.get("acked", 0), r.get("unacked", 0), r.get("lost", -1), " ".join(per)))
    for k, v in r.get("checks", {}).items():
        print("            %-10s %s" % (k, v))
    if r.get("lost_examples"):
        print("            lost: %s" % r["lost_examples"])
    if not fail_ok:
        print("            client-visible failover above %.1f s" % fmax)
sys.exit(1 if bad else 0)
EOF
rep=$?
for c in "${RUN_CLIENTS[@]}"; do
  [ -f "$CTRL/$c.result" ] || say "$c output: $(tail -5 "$W/client-$c.out" | tr '\n' ' ')"
done
if [ "$rep" != 0 ] || [ "$fails" != 0 ]; then die "client matrix: $fails client(s) exited with an error"; fi
say "client matrix passed: ${RUN_CLIENTS[*]} (style $STYLE)"
exit 0
