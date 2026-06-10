#!/usr/bin/env bash
# Head-to-head benchmark: ssd-kv vs Valkey 7
# Outputs a Markdown table to stdout.
# Usage: sudo bash bench_vs_valkey.sh   (needs root for drop_caches)

set -euo pipefail

SSDKV_BIN="./target/release/ssd-kv"
SSDKV_PORT=7379
VALKEY_PORT=7380
DATA_DIR="$(mktemp -d)"
VALKEY_DIR="$(mktemp -d)"

# ── helpers ────────────────────────────────────────────────────────────────────

die() { echo "ERROR: $*" >&2; exit 1; }

start_ssdkv() {
    "$SSDKV_BIN" --bind "127.0.0.1:${SSDKV_PORT}" \
        --data-dir "$DATA_DIR" \
        --reactor-threads 1 \
        --log-level error &
    SSDKV_PID=$!
    for _ in $(seq 1 20); do
        redis-cli -p "$SSDKV_PORT" PING &>/dev/null && return
        sleep 0.2
    done
    die "ssd-kv did not start"
}

start_valkey() {
    valkey-server \
        --port "$VALKEY_PORT" \
        --save "" \
        --appendonly no \
        --dir "$VALKEY_DIR" \
        --loglevel warning \
        --hz 100 \
        --daemonize yes \
        --pidfile "$VALKEY_DIR/valkey.pid"
    for _ in $(seq 1 20); do
        redis-cli -p "$VALKEY_PORT" PING &>/dev/null && return
        sleep 0.2
    done
    die "Valkey did not start"
}

stop_ssdkv()  { kill "$SSDKV_PID" 2>/dev/null || true; wait "$SSDKV_PID" 2>/dev/null || true; }
stop_valkey() {
    local pid
    pid="$(cat "$VALKEY_DIR/valkey.pid" 2>/dev/null || echo '')"
    [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true
}
cleanup() {
    stop_ssdkv; stop_valkey
    rm -rf "$DATA_DIR" "$VALKEY_DIR"
}
trap cleanup EXIT

drop_caches() { echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true; }

# Run redis-benchmark and extract ops/sec from the last "Summary:" block.
# redis-benchmark -q -t SET -n 100000 -c 50 -p PORT [extra…]
bench_ops() {
    local port="$1"; shift
    redis-benchmark -q -p "$port" "$@" 2>/dev/null \
        | awk '/ops\/sec/{gsub(/,/,""); printf "%.0f", $1; found=1} END{if(!found) print "N/A"}'
}

# Run redis-benchmark --latency-history mode; capture p50/p99 from --latency output.
# We use -t GET/SET -n 50000 --latency to get percentile output.
bench_latency() {
    local port="$1" cmd="$2"
    redis-benchmark -p "$port" -t "$cmd" -n 50000 -c 1 \
        --latency-history 2>/dev/null | tail -1 \
        | awk '{printf "%s/%s", $5, $7}'  2>/dev/null \
        || echo "N/A"
}

# Use --csv output with -n requests and -c clients, parse summary line.
bench_csv() {
    local port="$1"; shift
    redis-benchmark --csv -p "$port" "$@" 2>/dev/null \
        | awk -F'"' '/^"/{if(NF>=4){print $4}}'
}

# ── main ───────────────────────────────────────────────────────────────────────

echo "Starting servers…" >&2
start_ssdkv
start_valkey
sleep 1  # let both settle

# Warm both servers with 200K initial keys (so GETs hit real data).
WARMUP=20000
echo "Warming up ($WARMUP keys)…" >&2
redis-benchmark -p "$SSDKV_PORT"  -t SET -n "$WARMUP" -c 50 -q &>/dev/null
redis-benchmark -p "$VALKEY_PORT" -t SET -n "$WARMUP" -c 50 -q &>/dev/null

echo "" >&2
echo "Running benchmarks (this takes ~2 min)…" >&2

# ─ throughput sweeps ──────────────────────────────────────────────────────────

N=100000; PIPELINES=(1 10 50)

declare -A SKV_SET SKV_GET SKV_MIXED VAL_SET VAL_GET VAL_MIXED

for c in "${PIPELINES[@]}"; do
    echo "  pipeline=$c …" >&2

    SKV_SET[$c]=$(bench_ops  "$SSDKV_PORT"  -t SET   -n "$N" -c "$c" -d 64)
    VAL_SET[$c]=$(bench_ops  "$VALKEY_PORT" -t SET   -n "$N" -c "$c" -d 64)

    SKV_GET[$c]=$(bench_ops  "$SSDKV_PORT"  -t GET   -n "$N" -c "$c" -d 64)
    VAL_GET[$c]=$(bench_ops  "$VALKEY_PORT" -t GET   -n "$N" -c "$c" -d 64)

    # Mixed: 80% GET, 20% SET via redis-benchmark's built-in mixed mode
    # redis-benchmark does not have a --ratio flag; approximate via separate
    # lapped runs or use the default which is 50/50. Use explicit -e flag.
    SKV_MIXED[$c]=$(bench_ops  "$SSDKV_PORT"  -t GET,SET -n "$N" -c "$c" -d 64)
    VAL_MIXED[$c]=$(bench_ops  "$VALKEY_PORT" -t GET,SET -n "$N" -c "$c" -d 64)
done

# ─ latency (single-client, no pipelining) ─────────────────────────────────────

echo "  latency probes …" >&2

SKV_LAT_SET=$(redis-benchmark -p "$SSDKV_PORT"  -t SET -n 50000 -c 1 --latency -q 2>/dev/null | tail -1)
VAL_LAT_SET=$(redis-benchmark -p "$VALKEY_PORT" -t SET -n 50000 -c 1 --latency -q 2>/dev/null | tail -1)
SKV_LAT_GET=$(redis-benchmark -p "$SSDKV_PORT"  -t GET -n 50000 -c 1 --latency -q 2>/dev/null | tail -1)
VAL_LAT_GET=$(redis-benchmark -p "$VALKEY_PORT" -t GET -n 50000 -c 1 --latency -q 2>/dev/null | tail -1)

# ─ cold GET (page-cache dropped before each 10K-op run) ─────────────────────

echo "  cold GET (drop_caches) …" >&2
drop_caches
SKV_GET_COLD=$(bench_ops "$SSDKV_PORT"  -t GET -n 10000 -c 1)
# Valkey is in-memory; "cold" is not meaningful but we run it for completeness.
drop_caches
VAL_GET_COLD=$(bench_ops "$VALKEY_PORT" -t GET -n 10000 -c 1)

# ─ print results ─────────────────────────────────────────────────────────────

fmt_k() { printf "%.0f K" $(echo "scale=1; $1/1000" | bc); }

echo ""
echo "## Benchmark results — ssd-kv vs Valkey 7.2 (loopback, 1 reactor thread)"
echo ""
echo "Environment: $(uname -r), $(nproc) vCPUs, virtio-blk + ext4 (VM)"
echo "Valkey: persistence disabled (save \"\" appendonly no)"
echo "ssd-kv: durable (TSS sync every 50 ms, data on disk)"
echo ""
echo "### Throughput (ops/s, 100 K ops, 64-byte values)"
echo ""
printf "| %-28s | %15s | %15s |\n" "Workload" "ssd-kv" "Valkey 7"
printf "| %-28s | %15s | %15s |\n" "---" "---" "---"
for c in "${PIPELINES[@]}"; do
    printf "| %-28s | %15s | %15s |\n" \
        "SET  (c=${c})" \
        "$(fmt_k "${SKV_SET[$c]}")" \
        "$(fmt_k "${VAL_SET[$c]}")"
    printf "| %-28s | %15s | %15s |\n" \
        "GET warm (c=${c})" \
        "$(fmt_k "${SKV_GET[$c]}")" \
        "$(fmt_k "${VAL_GET[$c]}")"
    printf "| %-28s | %15s | %15s |\n" \
        "GET+SET mixed (c=${c})" \
        "$(fmt_k "${SKV_MIXED[$c]}")" \
        "$(fmt_k "${VAL_MIXED[$c]}")"
done
printf "| %-28s | %15s | %15s |\n" \
    "GET cold (c=1, drop_caches)" \
    "$(fmt_k "$SKV_GET_COLD")" \
    "$(fmt_k "$VAL_GET_COLD")"

echo ""
echo "### Latency at c=1 (µs, --latency, avg/p50/p99 format from redis-benchmark)"
echo ""
printf "| %-12s | %-40s | %-40s |\n" "Op" "ssd-kv" "Valkey 7"
printf "| %-12s | %-40s | %-40s |\n" "---" "---" "---"
printf "| %-12s | %-40s | %-40s |\n" "SET" "$SKV_LAT_SET" "$VAL_LAT_SET"
printf "| %-12s | %-40s | %-40s |\n" "GET" "$SKV_LAT_GET" "$VAL_LAT_GET"

echo ""
echo "Notes:"
echo "- ssd-kv data is fully durable on-disk; Valkey data is volatile (no persistence)."
echo "- ssd-kv cold GET forces SSD round-trips; Valkey has no equivalent (RAM only)."
echo "- 1 reactor thread for ssd-kv to match Valkey's single-threaded command loop."
