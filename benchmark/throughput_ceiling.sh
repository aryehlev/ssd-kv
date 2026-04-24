#!/bin/bash
# throughput_ceiling.sh
# ----
# Measure ssd-kv's sustained write throughput by running many
# redis-benchmark processes in parallel and summing their RPS.
#
# `redis-benchmark` maxes out around ~200k rps from a single invocation
# on this class of VM because it's event-loop-bound inside the benchmark
# tool itself, not on the server. To push the server past 1M rps we run
# N benchmark instances against it concurrently and aggregate.
#
# Intended use cases:
# 1. Sanity-check the parallel WAL shard architecture: on multi-device
#    NVMe with `--reactor-threads N --wal-dir /nvme0/... --wal-dir /nvme1/...`
#    you should see near-linear scaling up to the disk IOPS × N ceiling.
# 2. Demonstrate that our group-commit model exceeds Aerospike's single-
#    device ceiling — Aerospike caps at device-IOPS, we keep going via
#    per-fsync batching across shards.
# 3. Reproduce latency-vs-concurrency curves for p99 claims.
#
# Usage:
#   throughput_ceiling.sh [PORT] [NUM_CLIENTS] [OPS_PER_CLIENT] [VALUE_SIZE] [PIPELINE]
#
# Defaults tuned for "push 8 benchmark processes at 16-connection each,
# pipeline depth 16, 100-byte values, 30k ops each" → ~4M ops total,
# enough to smooth out timer noise.

set -u

PORT=${1:-7777}
NUM_CLIENTS=${2:-8}
OPS_PER_CLIENT=${3:-50000}
VALUE_SIZE=${4:-100}
PIPELINE=${5:-16}
CONNS_PER_CLIENT=${CONNS_PER_CLIENT:-16}
KEYSPACE=${KEYSPACE:-100000}

HOST=${HOST:-127.0.0.1}
TMP=${TMP:-/tmp/ssdkv-bench-harness}
mkdir -p "$TMP"

# Warm up: populate the keyspace so GETs don't all miss.
echo "[warmup] populating $KEYSPACE keys..."
redis-benchmark -h "$HOST" -p "$PORT" -t set -n "$KEYSPACE" -c 16 -P 16 \
    -d "$VALUE_SIZE" -r "$KEYSPACE" -q > "$TMP/warmup.log" 2>&1
tail -c 200 "$TMP/warmup.log" | tr '\r' '\n' | grep "per second" | tail -1

echo ""
echo "=== SET throughput ceiling ($NUM_CLIENTS parallel benchmarks, \
$CONNS_PER_CLIENT conns each, P=$PIPELINE, d=$VALUE_SIZE, n=$OPS_PER_CLIENT/client) ==="

pids=()
for i in $(seq 1 "$NUM_CLIENTS"); do
  # Seed keyspace range per client to avoid all clients hammering the same
  # subset. -r $KEYSPACE gives redis-benchmark a random uniform spread.
  redis-benchmark \
    -h "$HOST" -p "$PORT" \
    -t set -n "$OPS_PER_CLIENT" \
    -c "$CONNS_PER_CLIENT" -P "$PIPELINE" \
    -d "$VALUE_SIZE" -r "$KEYSPACE" \
    -q > "$TMP/client_$i.log" 2>&1 &
  pids+=($!)
done

# Wait for all benches. `wait` with no args waits for all background.
start=$(date +%s%N)
wait
end=$(date +%s%N)

elapsed_ms=$(( (end - start) / 1000000 ))
elapsed_sec=$(awk "BEGIN { printf \"%.3f\", $elapsed_ms / 1000.0 }")

echo ""
echo "=== per-client results ==="
total_ops=0
for i in $(seq 1 "$NUM_CLIENTS"); do
  rps_line=$(tail -c 200 "$TMP/client_$i.log" | tr '\r' '\n' | grep "per second" | tail -1)
  echo "  client $i: $rps_line"
  total_ops=$(( total_ops + OPS_PER_CLIENT ))
done

aggregate_rps=$(awk "BEGIN { printf \"%.0f\", $total_ops / $elapsed_sec }")

echo ""
echo "=== AGGREGATE ==="
echo "  wall clock: ${elapsed_sec}s"
echo "  total ops:  $total_ops"
echo "  rps:        $aggregate_rps"
echo ""

# GET phase — usually higher because no fsync on the critical path.
echo "=== GET throughput ceiling ==="
pids=()
for i in $(seq 1 "$NUM_CLIENTS"); do
  redis-benchmark \
    -h "$HOST" -p "$PORT" \
    -t get -n "$OPS_PER_CLIENT" \
    -c "$CONNS_PER_CLIENT" -P "$PIPELINE" \
    -d "$VALUE_SIZE" -r "$KEYSPACE" \
    -q > "$TMP/get_client_$i.log" 2>&1 &
  pids+=($!)
done

start=$(date +%s%N)
wait
end=$(date +%s%N)
elapsed_ms=$(( (end - start) / 1000000 ))
elapsed_sec=$(awk "BEGIN { printf \"%.3f\", $elapsed_ms / 1000.0 }")

total_ops=0
for i in $(seq 1 "$NUM_CLIENTS"); do
  rps_line=$(tail -c 200 "$TMP/get_client_$i.log" | tr '\r' '\n' | grep "per second" | tail -1)
  echo "  client $i: $rps_line"
  total_ops=$(( total_ops + OPS_PER_CLIENT ))
done

aggregate_rps=$(awk "BEGIN { printf \"%.0f\", $total_ops / $elapsed_sec }")
echo ""
echo "  wall clock: ${elapsed_sec}s"
echo "  total ops:  $total_ops"
echo "  rps:        $aggregate_rps"
