#!/bin/bash
# Run comparison benchmark between SSD-KV and Aerospike

set -e

echo "================================================"
echo "SSD-KV vs Aerospike Performance Comparison"
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

# For Aerospike comparison, we'd need a separate client that speaks Aerospike protocol
# Here we just show how the benchmark would be structured

echo ""
echo "================================================"
echo "Notes on Aerospike Comparison"
echo "================================================"
echo ""
echo "To compare with Aerospike, install the Aerospike tools and run:"
echo "  asbench -h 127.0.0.1:3000 -n test -s testset -k $NUM_KEYS -o I1 -w RU,50"
echo ""
echo "Or use the YCSB benchmark framework for a fair comparison."
echo ""

# Optional: Stop containers
# docker-compose down
