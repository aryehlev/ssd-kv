# SSD-KV Performance Optimizations

This document tracks all performance optimizations implemented and planned for SSD-KV, based on research from ScyllaDB, Aerospike, and other high-performance databases.

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Kernel-Level Optimizations](#kernel-level-optimizations)
3. [Engine Optimizations](#engine-optimizations)
4. [Storage Optimizations](#storage-optimizations)
5. [Network Optimizations](#network-optimizations)
6. [Integration Status](#integration-status)
7. [Benchmarking](#benchmarking)
8. [Remaining Work](#remaining-work)

---

## Architecture Overview

### Current Architecture
```
┌─────────────────────────────────────────────────────────────┐
│                      Client Requests                         │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    TCP Server (tokio)                        │
│                    - Native Protocol (:7777)                 │
│                    - Redis Protocol (:7778)                  │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      Request Handler                         │
│                    - RwLock<HashMap> index                   │
│                    - Bloom filter                            │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Write Buffer + Storage                    │
└─────────────────────────────────────────────────────────────┘
```

### Target Architecture (Optimized)
```
┌─────────────────────────────────────────────────────────────┐
│                      Client Requests                         │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              io_uring / AF_XDP Network Layer                 │
│              - Zero-copy receive                             │
│              - Batched syscalls                              │
│              - Kernel bypass (AF_XDP)                        │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              Shard-Per-Core Engine (ScyllaDB-style)          │
│              - Each CPU owns its shard                       │
│              - No locks on hot path                          │
│              - Message passing for cross-core                │
└────────┬────────────────────┴────────────────────┬──────────┘
         │                                          │
         ▼                                          ▼
┌─────────────────┐                      ┌─────────────────────┐
│  Lock-Free      │                      │  Write-Ahead Log    │
│  Index          │                      │  (Sequential I/O)   │
│  - Open addr    │                      │  - Fast durability  │
│  - SIMD compare │                      │  - Async flush      │
│  - Inline vals  │                      │  - CRC validation   │
└─────────────────┘                      └─────────────────────┘
```

---

## Kernel-Level Optimizations

### 1. io_uring Networking (`src/io/uring_net.rs`)

**Status**: ✅ Implemented

**What it does**:
- Batches multiple syscalls into single kernel transition
- Supports SQPOLL mode (kernel-side polling, no syscalls for submission)
- Zero-copy receive with registered buffers

**Key structures**:
```rust
pub struct UringNet {
    ring: IoUring,
    buffers: Vec<Vec<u8>>,
    pending_ops: HashMap<u64, NetOperation>,
    next_token: u64,
}

pub enum NetOperation {
    Accept { listener_fd: RawFd },
    Recv { fd: RawFd, buffer_idx: usize },
    Send { fd: RawFd },
}
```

**Configuration** (optimal settings):
```rust
let ring = IoUring::builder()
    .setup_sqpoll(2000)     // 2ms polling timeout
    .setup_sqpoll_cpu(0)    // Pin to CPU 0
    .build(4096)?;          // 4K queue depth
```

**Expected improvement**: 30-50% reduction in syscall overhead

---

### 2. AF_XDP Zero-Copy (`src/perf/af_xdp.rs`)

**Status**: ✅ Implemented

**What it does**:
- Bypasses kernel network stack entirely
- Zero-copy packet I/O to/from user space
- Direct NIC access via UMEM shared memory

**Key structures**:
```rust
pub struct AfXdpSocket {
    socket: RawFd,
    umem: *mut u8,
    fill_ring: XskRing,
    comp_ring: XskRing,
    rx_ring: XskRing,
    tx_ring: XskRing,
    frame_size: usize,
    num_frames: usize,
}

pub struct XdpDesc {
    pub addr: u64,    // Offset in UMEM
    pub len: u32,     // Packet length
    pub options: u32,
}
```

**Requirements**:
- Linux 5.4+ kernel
- CAP_NET_ADMIN or root
- Network driver with XDP support

**Expected improvement**: 2-5x throughput for small packets

---

### 3. Huge Pages (`src/perf/huge_pages.rs`)

**Status**: ✅ Implemented

**What it does**:
- Uses 2MB or 1GB pages instead of 4KB
- Reduces TLB misses dramatically
- Critical for large data structures (index, buffers)

**Key structures**:
```rust
pub struct HugePageAlloc {
    ptr: *mut u8,
    size: usize,
    page_size: HugePageSize,
}

pub enum HugePageSize {
    Default,    // 4KB
    Huge2MB,    // 2MB
    Huge1GB,    // 1GB
}
```

**Usage**:
```rust
// Allocate index on huge pages
let alloc = HugePageAlloc::new(index_size, HugePageSize::Huge2MB)?;

// Get typed slice
let buckets: &mut [Bucket] = alloc.as_slice_mut();
```

**System setup** (required):
```bash
# Reserve 256 huge pages (512MB)
echo 256 | sudo tee /proc/sys/vm/nr_hugepages

# Or for 1GB pages (boot parameter)
hugepagesz=1G hugepages=4
```

**Expected improvement**: 10-20% for index-heavy workloads

---

### 4. Busy Polling (`src/perf/busy_poll.rs`)

**Status**: ✅ Implemented

**What it does**:
- Continuously polls for work instead of blocking
- Eliminates sleep/wake latency
- Combined with CPU isolation for dedicated cores

**Key structures**:
```rust
pub struct BusyPoller<T> {
    receiver: Receiver<T>,
    batch_size: usize,
    stats: Arc<BusyPollStats>,
}

pub struct CpuIsolation {
    cpu_id: usize,
}

pub struct AdaptiveSpinner {
    spin_count: u32,
    max_spins: u32,
    yield_threshold: u32,
}
```

**Usage**:
```rust
// Pin thread to CPU and busy poll
let isolation = CpuIsolation::new(cpu_id)?;
let poller = BusyPoller::new(receiver, 64);

loop {
    let batch = poller.poll_batch();
    for item in batch {
        process(item);
    }
}
```

**Expected improvement**: 50% latency reduction at p99

---

## Engine Optimizations

### 5. Shard-Per-Core Architecture (`src/engine/shard_per_core.rs`)

**Status**: ✅ Implemented

**What it does** (ScyllaDB-inspired):
- Each CPU core owns its data exclusively
- No locks on the hot path
- Explicit message passing for cross-core communication
- Linear scalability with core count

**Key structures**:
```rust
pub struct ShardPerCoreEngine {
    senders: Vec<Sender<ShardMessage>>,
    workers: Vec<JoinHandle<()>>,
    num_shards: usize,
}

pub enum ShardMessage {
    Get { key: Vec<u8>, respond: Sender<Option<Vec<u8>>> },
    Put { key: Vec<u8>, value: Vec<u8>, ttl: u32, respond: Sender<io::Result<()>> },
    Delete { key: Vec<u8>, respond: Sender<bool> },
    Shutdown,
}

pub struct Shard {
    id: usize,
    index: HashMap<u64, ShardEntry>,  // NO LOCKS - single-threaded!
    write_buffer: Vec<u8>,
    generation: u32,
    stats: ShardStats,
}
```

**Key insight**: Single-threaded access means HashMap operations are cache-optimal and lock-free.

**Expected improvement**: 2-4x throughput on multi-core systems

---

### 6. Lock-Free Index (`src/engine/lockfree_index.rs`)

**Status**: ✅ Implemented

**What it does** (Aerospike SPRIGS-inspired):
- Open addressing with linear probing (better cache locality)
- Lock-free reads using atomic operations
- Per-bucket locks only for writes
- SIMD-accelerated key comparison
- Inline storage for small keys/values

**Key structures**:
```rust
#[repr(C, align(64))]  // Cache-line aligned
pub struct Bucket {
    state: AtomicU8,              // empty/occupied/deleted/locked
    key_len: u8,
    value_len: u8,
    flags: u8,
    generation: AtomicU32,
    key_hash: u64,
    inline_key: [u8; 24],         // Inline key storage
    data: [u8; 64],               // Inline value OR disk location
}

pub struct LockFreeIndex {
    buckets: *mut Bucket,
    capacity: usize,
    mask: usize,                  // capacity - 1 for fast modulo
    count: AtomicU64,
    layout: Layout,
}
```

**Lock-free GET**:
```rust
pub fn get(&self, key: &[u8]) -> Option<GetResult> {
    let key_hash = hash_key(key);

    if let Some(index) = self.find_bucket(key, key_hash) {
        let bucket = self.bucket(index);

        // Optimistic read with generation check
        let gen1 = bucket.generation.load(Ordering::Acquire);
        let result = bucket.get_inline_value();
        let gen2 = bucket.generation.load(Ordering::Acquire);

        if gen1 != gen2 {
            return self.get(key); // Retry on concurrent modification
        }

        return result;
    }
    None
}
```

**Expected improvement**: Lock-free reads provide consistent sub-microsecond latency

---

## Storage Optimizations

### 7. Write-Ahead Log (`src/storage/wal.rs`)

**Status**: ✅ Implemented

**What it does** (Aerospike streaming write buffer inspired):
- Fast sequential writes (no random I/O)
- Durability guarantees via fsync
- Crash recovery via replay
- Background flush to main storage

**Key structures**:
```rust
#[repr(C, packed)]
pub struct WalEntryHeader {
    magic: u32,           // 0x57414C21 ("WAL!")
    entry_type: u8,       // PUT=1, DELETE=2
    flags: u8,
    key_len: u16,
    value_len: u32,
    generation: u32,
    ttl: u32,
    crc: u32,             // CRC32 of key + value
}

pub struct WriteAheadLog {
    config: WalConfig,
    writer: Mutex<BufWriter<File>>,
    file_seq: AtomicU64,
    position: AtomicU64,
    stats: Arc<WalStats>,
    sync_thread: Option<JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
}
```

**Write path**:
1. Append to WAL (sequential, fast)
2. Update in-memory index
3. Return success to client
4. Background: flush WAL to main storage

**Recovery**:
```rust
wal.replay(|header, key, value| {
    if header.is_put() {
        index.put(&key, Some(&value), None, header.generation)?;
    } else {
        index.delete(&key);
    }
    Ok(())
})?;
```

**Configuration**:
```rust
pub struct WalConfig {
    dir: PathBuf,
    max_file_size: u64,        // 64MB default
    sync_interval: Duration,    // 10ms default
    buffer_size: usize,         // 1MB default
    fsync: bool,                // true for durability
}
```

**Expected improvement**: 5-10x write throughput (sequential vs random I/O)

---

## Network Optimizations

### 8. io_uring TCP Server (`src/server/uring_server.rs`)

**Status**: ✅ Implemented

**What it does**:
- Uses io_uring for all network I/O
- Batched accept/recv/send operations
- Zero-copy where possible

**Integration with protocol**:
```rust
fn handle_request(&mut self, fd: RawFd, data: &[u8]) -> io::Result<()> {
    let header = Header::from_bytes(&data[..HEADER_SIZE])?;

    let response = match header.op_code {
        OpCode::Get => self.handle_get(&data[HEADER_SIZE..]),
        OpCode::Put => self.handle_put(&data[HEADER_SIZE..]),
        OpCode::Delete => self.handle_delete(&data[HEADER_SIZE..]),
        _ => Response::error(ErrorCode::InvalidRequest),
    };

    let response_data = response.serialize();
    self.uring.queue_send(fd, response_data)?;
    Ok(())
}
```

---

## Integration Status

| Component | File | Status | Integrated |
|-----------|------|--------|------------|
| io_uring networking | `src/io/uring_net.rs` | ✅ Done | ❌ Not yet |
| io_uring server | `src/server/uring_server.rs` | ✅ Done | ❌ Not yet |
| AF_XDP zero-copy | `src/perf/af_xdp.rs` | ✅ Done | ❌ Not yet |
| Huge pages | `src/perf/huge_pages.rs` | ✅ Done | ❌ Not yet |
| Busy polling | `src/perf/busy_poll.rs` | ✅ Done | ❌ Not yet |
| Shard-per-core | `src/engine/shard_per_core.rs` | ✅ Done | ❌ Not yet |
| Lock-free index | `src/engine/lockfree_index.rs` | ✅ Done | ❌ Not yet |
| Write-ahead log | `src/storage/wal.rs` | ✅ Done | ❌ Not yet |
| Defragmentation | `src/storage/defrag.rs` | ✅ Done | ❌ Not yet |

### Module Exports Needed

**`src/engine/mod.rs`** - add:
```rust
pub mod shard_per_core;
pub mod lockfree_index;
```

**`src/storage/mod.rs`** - add:
```rust
pub mod wal;
```

**`src/perf/mod.rs`** - add:
```rust
pub mod af_xdp;
pub mod huge_pages;
pub mod busy_poll;
```

**`src/io/mod.rs`** - add:
```rust
pub mod uring_net;
```

**`src/server/mod.rs`** - add:
```rust
pub mod uring_server;
```

---

## Benchmarking

### GCP Deployment (`scripts/gcp-deploy.sh`)

**Architecture** (fair comparison):
```
                    ┌─────────────────┐
                    │  Benchmark      │
                    │  Client         │
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
              ▼              │              ▼
    ┌─────────────────┐      │    ┌─────────────────┐
    │  SSD-KV         │      │    │  Aerospike      │
    │  10.x.x.x:7777  │      │    │  10.x.x.x:3000  │
    └─────────────────┘      │    └─────────────────┘
```

**Commands**:
```bash
# Create instances
./scripts/gcp-deploy.sh create

# Setup and deploy
./scripts/gcp-deploy.sh setup

# Run benchmark
./scripts/gcp-deploy.sh benchmark

# Clean up
./scripts/gcp-deploy.sh destroy
```

### GitHub Actions (`/.github/workflows/benchmark.yml`)

Automated benchmarking on every commit:
- Runs on ubuntu-latest
- Compares against Aerospike in Docker
- Configurable: keys, threads, value_size

### Current Benchmark Results (macOS, same machine)

```
SSD-KV Performance:
  PUT: 156,000 ops/sec
  GET: 312,000 ops/sec

Aerospike Performance (Docker):
  PUT: 45,000 ops/sec
  GET: 89,000 ops/sec
```

**Note**: These are not fair comparisons due to Docker overhead. Use GCP deployment for accurate comparison.

---

## Remaining Work

### High Priority

1. **Module Integration**
   - Export new modules in mod.rs files
   - Wire up WAL to main Handler
   - Switch to shard-per-core engine

2. **Enable GCP Billing**
   - Required for deployment script to work
   - Project: `propane-will-305216`

3. **Linux Testing**
   - io_uring requires Linux 5.1+
   - AF_XDP requires Linux 5.4+
   - Test all optimizations on real Linux

### Medium Priority

4. **Value Compression**
   - LZ4 for values > 256 bytes
   - Transparent compression/decompression

5. **Connection Batching**
   - Batch multiple requests per connection
   - Reduce per-request overhead

6. **Parallel Recovery**
   - Replay WAL files in parallel
   - Faster startup after crash

### Low Priority

7. **Adaptive Write Buffer**
   - Dynamic sizing based on write rate
   - Flush on size OR time threshold

8. **Read-ahead for Scans**
   - Prefetch adjacent blocks
   - Better range query performance

9. **NUMA Awareness**
   - Allocate memory on same NUMA node as CPU
   - Reduce cross-node latency

---

## Performance Targets

| Metric | Current | Target | Aerospike |
|--------|---------|--------|-----------|
| GET latency p50 | ~50μs | <10μs | ~50μs |
| GET latency p99 | ~500μs | <100μs | ~500μs |
| PUT latency p50 | ~100μs | <20μs | ~100μs |
| PUT throughput | 156K/s | >500K/s | ~300K/s |
| GET throughput | 312K/s | >1M/s | ~500K/s |

---

## References

1. **ScyllaDB Architecture**
   - Shard-per-core design: https://www.scylladb.com/product/technology/shard-per-core-architecture/
   - No locks, message passing between cores

2. **Aerospike Architecture**
   - SPRIGS index: https://aerospike.com/docs/architecture/primary-index
   - Defragmentation: https://aerospike.com/docs/operations/manage/storage/defragmentation

3. **io_uring**
   - Efficient I/O: https://kernel.dk/io_uring.pdf
   - SQPOLL mode for zero-syscall submission

4. **AF_XDP**
   - Zero-copy networking: https://www.kernel.org/doc/html/latest/networking/af_xdp.html
   - Kernel bypass without DPDK

---

## Quick Start (After Integration)

```bash
# Build with optimizations
cargo build --release --features "uring,xdp,hugepages"

# Configure system
sudo sysctl -w vm.nr_hugepages=256
sudo sysctl -w net.core.busy_poll=50

# Run with optimizations
./target/release/ssd-kv \
    --data-dir /data/ssd-kv \
    --bind 0.0.0.0:7777 \
    --workers 4 \
    --uring \
    --hugepages
```

---

*Last updated: 2024*
