#!/usr/bin/env bash
# Run a battery of throughput / latency benchmarks against ssd-kv using
# redis-benchmark (standard, apples-to-apples with Redis/Valkey). For an
# Aerospike comparison, run the same workload shapes against Aerospike
# using asbench or YCSB — see the notes at the end.
#
# Requirements:
#   - redis-benchmark on PATH (ships with redis-tools / valkey-tools)
#   - ssd-kv already running on $HOST:$PORT
#
# Usage:
#   ./benchmark/run_comparison.sh                  # defaults below
#   HOST=other PORT=7777 ./benchmark/run_comparison.sh
#   WORKLOADS="get,set" ./benchmark/run_comparison.sh

set -euo pipefail

HOST=${HOST:-127.0.0.1}
PORT=${PORT:-7777}
KEYSPACE=${KEYSPACE:-1000000}    # -r: random-key range
REQUESTS=${REQUESTS:-2000000}    # -n: total requests per workload
CLIENTS=${CLIENTS:-64}           # -c: concurrent clients
PIPELINE=${PIPELINE:-16}         # -P: pipeline depth
VALUE_SIZE=${VALUE_SIZE:-100}    # -d: SET value size in bytes

# Comma-separated list; valid entries: set, get, incr, del, mset, ping_mbulk
WORKLOADS=${WORKLOADS:-set,get,incr,del,mset}

if ! command -v redis-benchmark >/dev/null 2>&1; then
  echo "error: redis-benchmark not found on PATH" >&2
  echo "       install redis-tools (debian/ubuntu) or valkey-tools" >&2
  exit 1
fi

if ! redis-cli -h "$HOST" -p "$PORT" PING >/dev/null 2>&1; then
  echo "error: no ssd-kv responding at $HOST:$PORT" >&2
  echo "       start it first:  ./target/release/ssd-kv --bind 0.0.0.0:$PORT" >&2
  exit 1
fi

echo "============================================================"
echo "ssd-kv benchmark (target: $HOST:$PORT)"
echo "  keyspace=$KEYSPACE  requests=$REQUESTS  clients=$CLIENTS"
echo "  pipeline=$PIPELINE  value_size=$VALUE_SIZE B"
echo "============================================================"

# Warm up. Also pre-populates keys so GET workloads are realistic.
echo ""
echo "-- warmup (SET $KEYSPACE keys) --"
redis-benchmark -h "$HOST" -p "$PORT" \
  -t set -n "$KEYSPACE" -c "$CLIENTS" -P "$PIPELINE" \
  -d "$VALUE_SIZE" -r "$KEYSPACE" -q

# Run each workload with throughput (-q) and latency (--latency-history gets
# noisy; use --csv for a single summary row).
for w in ${WORKLOADS//,/ }; do
  echo ""
  echo "-- $w --"
  redis-benchmark -h "$HOST" -p "$PORT" \
    -t "$w" -n "$REQUESTS" -c "$CLIENTS" -P "$PIPELINE" \
    -d "$VALUE_SIZE" -r "$KEYSPACE" -q
done

cat <<'EOF'

============================================================
Running the same workload against Aerospike or Valkey
============================================================

Valkey / Redis (RESP, same redis-benchmark):
  redis-benchmark -h <host> -p 6379 -t set,get,incr,del,mset \
                  -n 2000000 -c 64 -P 16 -d 100 -r 1000000 -q

Aerospike (native, asbench):
  asbench -h <host>:3000 -n test -s testset \
          -K 0 -k 1000000 -o B:100 \
          -z 64 --throughput 0 \
          -w RU,50      # mixed 50/50 reads/writes

YCSB (cross-store, most honest apples-to-apples):
  # Core workloads a-f cover everything from read-heavy to update-heavy.
  bin/ycsb load aerospike -P workloads/workloada -threads 64
  bin/ycsb run  aerospike -P workloads/workloada -threads 64
  bin/ycsb load redis     -P workloads/workloada -threads 64 \
                          -p redis.host=<host> -p redis.port=7777
  bin/ycsb run  redis     -P workloads/workloada -threads 64 \
                          -p redis.host=<host> -p redis.port=7777

Compare throughput + p50/p99/p99.9 latency.
EOF
