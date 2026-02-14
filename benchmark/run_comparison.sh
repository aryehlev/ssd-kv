#!/bin/bash
# Run comparison benchmark between SSD-KV and Redis

set -e

echo "================================================"
echo "SSD-KV vs Redis Performance Comparison"
echo "================================================"
echo ""

# Configuration
NUM_KEYS=${NUM_KEYS:-100000}
NUM_THREADS=${NUM_THREADS:-4}
VALUE_SIZE=${VALUE_SIZE:-100}

echo "Configuration:"
echo "  Keys: $NUM_KEYS"
echo "  Threads: $NUM_THREADS"
echo "  Value size: $VALUE_SIZE bytes"
echo ""

# Build the benchmark client
echo "Building benchmark client..."
cargo build --release --bin bench_client

# Start containers if not running
echo ""
echo "Starting containers..."
docker-compose up -d

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 5

# Run SSD-KV benchmark
echo ""
echo "================================================"
echo "Benchmarking SSD-KV (port 7777)"
echo "================================================"
./target/release/bench_client 127.0.0.1:7777 $NUM_KEYS $NUM_THREADS $VALUE_SIZE

# Run Redis benchmark for comparison
echo ""
echo "================================================"
echo "Benchmarking Redis (port 6379)"
echo "================================================"
redis-benchmark -h 127.0.0.1 -p 6379 -t set,get -n $NUM_KEYS -c $NUM_THREADS -d $VALUE_SIZE -q

# Optional: Stop containers
# docker-compose down
