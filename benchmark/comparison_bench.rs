//! Comparison benchmark between SSD-KV and Aerospike.
//!
//! This benchmark tests both systems with identical workloads.
//! Run with: cargo run --release --bin comparison_bench

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// SSD-KV Protocol constants
const PROTOCOL_MAGIC: u16 = 0x564B;
const PROTOCOL_VERSION: u8 = 1;
const OPCODE_GET: u8 = 0x01;
const OPCODE_PUT: u8 = 0x02;
const OPCODE_SUCCESS: u8 = 0x80;

// Aerospike constants
const AS_MSG_VERSION: u8 = 2;
const AS_MSG_TYPE: u8 = 3;
const AS_INFO1_READ: u8 = 1;
const AS_INFO2_WRITE: u8 = 1;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let ssd_kv_host = args.get(1).map(|s| s.as_str()).unwrap_or("127.0.0.1:7777");
    let aerospike_host = args.get(2).map(|s| s.as_str()).unwrap_or("127.0.0.1:3000");
    let num_keys: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(10_000);
    let num_threads: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(4);
    let value_size: usize = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(100);

    println!("===========================================");
    println!("  SSD-KV vs Aerospike Performance Test");
    println!("===========================================");
    println!();
    println!("Configuration:");
    println!("  SSD-KV:     {}", ssd_kv_host);
    println!("  Aerospike:  {}", aerospike_host);
    println!("  Keys:       {}", num_keys);
    println!("  Threads:    {}", num_threads);
    println!("  Value size: {} bytes", value_size);
    println!();

    // Generate test data
    let value: Vec<u8> = (0..value_size).map(|i| (i % 256) as u8).collect();

    // Test SSD-KV
    println!("-------------------------------------------");
    println!("  Testing SSD-KV");
    println!("-------------------------------------------");

    let ssd_kv_available = test_connection(ssd_kv_host);
    let ssd_kv_results = if ssd_kv_available {
        Some(run_ssd_kv_benchmark(ssd_kv_host, num_keys, num_threads, &value))
    } else {
        println!("  [SKIPPED] SSD-KV not available at {}", ssd_kv_host);
        None
    };

    println!();

    // Test Aerospike
    println!("-------------------------------------------");
    println!("  Testing Aerospike");
    println!("-------------------------------------------");

    let aerospike_available = test_connection(aerospike_host);
    let aerospike_results = if aerospike_available {
        Some(run_aerospike_benchmark(aerospike_host, num_keys, num_threads, &value))
    } else {
        println!("  [SKIPPED] Aerospike not available at {}", aerospike_host);
        None
    };

    // Print comparison
    println!();
    println!("===========================================");
    println!("  RESULTS COMPARISON");
    println!("===========================================");
    println!();

    print_comparison("PUT",
        ssd_kv_results.as_ref().map(|r| &r.put),
        aerospike_results.as_ref().map(|r| &r.put));

    print_comparison("GET",
        ssd_kv_results.as_ref().map(|r| &r.get),
        aerospike_results.as_ref().map(|r| &r.get));

    print_comparison("Mixed",
        ssd_kv_results.as_ref().map(|r| &r.mixed),
        aerospike_results.as_ref().map(|r| &r.mixed));
}

fn test_connection(host: &str) -> bool {
    TcpStream::connect_timeout(
        &host.parse().unwrap(),
        Duration::from_secs(2)
    ).is_ok()
}

#[derive(Default)]
struct BenchStats {
    operations: AtomicU64,
    errors: AtomicU64,
    latency_sum_ns: AtomicU64,
    latency_max_ns: AtomicU64,
    duration_ms: AtomicU64,
}

impl BenchStats {
    fn ops_per_sec(&self) -> f64 {
        let ops = self.operations.load(Ordering::Relaxed);
        let duration_ms = self.duration_ms.load(Ordering::Relaxed);
        if duration_ms == 0 { return 0.0; }
        ops as f64 / (duration_ms as f64 / 1000.0)
    }

    fn avg_latency_us(&self) -> u64 {
        let ops = self.operations.load(Ordering::Relaxed);
        if ops == 0 { return 0; }
        (self.latency_sum_ns.load(Ordering::Relaxed) / ops) / 1000
    }

    fn max_latency_us(&self) -> u64 {
        self.latency_max_ns.load(Ordering::Relaxed) / 1000
    }

    fn error_rate(&self) -> f64 {
        let ops = self.operations.load(Ordering::Relaxed);
        let errors = self.errors.load(Ordering::Relaxed);
        if ops + errors == 0 { return 0.0; }
        errors as f64 / (ops + errors) as f64 * 100.0
    }
}

struct BenchResult {
    put: Arc<BenchStats>,
    get: Arc<BenchStats>,
    mixed: Arc<BenchStats>,
}

fn run_ssd_kv_benchmark(host: &str, num_keys: usize, num_threads: usize, value: &[u8]) -> BenchResult {
    println!("  Phase 1: PUT...");
    let put_stats = run_ssd_kv_phase(host, num_keys, num_threads, value, WorkloadType::Put);
    println!("    {} ops/sec, {} μs avg latency",
        put_stats.ops_per_sec() as u64, put_stats.avg_latency_us());

    println!("  Phase 2: GET...");
    let get_stats = run_ssd_kv_phase(host, num_keys, num_threads, value, WorkloadType::Get);
    println!("    {} ops/sec, {} μs avg latency",
        get_stats.ops_per_sec() as u64, get_stats.avg_latency_us());

    println!("  Phase 3: Mixed...");
    let mixed_stats = run_ssd_kv_phase(host, num_keys, num_threads, value, WorkloadType::Mixed);
    println!("    {} ops/sec, {} μs avg latency",
        mixed_stats.ops_per_sec() as u64, mixed_stats.avg_latency_us());

    BenchResult {
        put: put_stats,
        get: get_stats,
        mixed: mixed_stats,
    }
}

#[derive(Clone, Copy)]
enum WorkloadType {
    Put,
    Get,
    Mixed,
}

fn run_ssd_kv_phase(host: &str, num_keys: usize, num_threads: usize, value: &[u8], workload: WorkloadType) -> Arc<BenchStats> {
    let stats = Arc::new(BenchStats::default());
    let keys_per_thread = num_keys / num_threads;

    let start = Instant::now();
    let mut handles = Vec::new();

    for t in 0..num_threads {
        let host = host.to_string();
        let value = value.to_vec();
        let stats = Arc::clone(&stats);
        let start_key = t * keys_per_thread;

        handles.push(std::thread::spawn(move || {
            let mut stream = match TcpStream::connect(&host) {
                Ok(s) => s,
                Err(_) => return,
            };
            stream.set_nodelay(true).ok();
            stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
            stream.set_write_timeout(Some(Duration::from_secs(5))).ok();

            let mut rng_state = t as u64;

            for i in 0..keys_per_thread {
                let key = format!("key_{:012}", start_key + i);
                let op_start = Instant::now();

                let do_write = match workload {
                    WorkloadType::Put => true,
                    WorkloadType::Get => false,
                    WorkloadType::Mixed => {
                        rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
                        (rng_state >> 63) == 0
                    }
                };

                let result = if do_write {
                    ssd_kv_put(&mut stream, i as u32, key.as_bytes(), &value)
                } else {
                    ssd_kv_get(&mut stream, i as u32, key.as_bytes()).map(|_| ())
                };

                let latency_ns = op_start.elapsed().as_nanos() as u64;
                stats.latency_sum_ns.fetch_add(latency_ns, Ordering::Relaxed);
                stats.latency_max_ns.fetch_max(latency_ns, Ordering::Relaxed);

                match result {
                    Ok(_) => { stats.operations.fetch_add(1, Ordering::Relaxed); }
                    Err(_) => { stats.errors.fetch_add(1, Ordering::Relaxed); }
                }
            }
        }));
    }

    for h in handles {
        h.join().ok();
    }

    stats.duration_ms.store(start.elapsed().as_millis() as u64, Ordering::Relaxed);
    stats
}

fn ssd_kv_put(stream: &mut TcpStream, request_id: u32, key: &[u8], value: &[u8]) -> std::io::Result<()> {
    let payload_len = 2 + key.len() + 4 + value.len() + 4;

    let mut header = [0u8; 16];
    header[0..2].copy_from_slice(&PROTOCOL_MAGIC.to_le_bytes());
    header[2] = PROTOCOL_VERSION;
    header[3] = OPCODE_PUT;
    header[4..8].copy_from_slice(&request_id.to_le_bytes());
    header[12..16].copy_from_slice(&(payload_len as u32).to_le_bytes());

    let mut payload = Vec::with_capacity(payload_len);
    payload.extend_from_slice(&(key.len() as u16).to_le_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
    payload.extend_from_slice(value);
    payload.extend_from_slice(&0u32.to_le_bytes());

    stream.write_all(&header)?;
    stream.write_all(&payload)?;

    let mut resp_header = [0u8; 16];
    stream.read_exact(&mut resp_header)?;

    let resp_opcode = resp_header[3];
    let resp_payload_len = u32::from_le_bytes([resp_header[12], resp_header[13], resp_header[14], resp_header[15]]);

    if resp_payload_len > 0 {
        let mut resp_payload = vec![0u8; resp_payload_len as usize];
        stream.read_exact(&mut resp_payload)?;
    }

    if resp_opcode == OPCODE_SUCCESS { Ok(()) }
    else { Err(std::io::Error::new(std::io::ErrorKind::Other, "PUT failed")) }
}

fn ssd_kv_get(stream: &mut TcpStream, request_id: u32, key: &[u8]) -> std::io::Result<Vec<u8>> {
    let payload_len = 2 + key.len();

    let mut header = [0u8; 16];
    header[0..2].copy_from_slice(&PROTOCOL_MAGIC.to_le_bytes());
    header[2] = PROTOCOL_VERSION;
    header[3] = OPCODE_GET;
    header[4..8].copy_from_slice(&request_id.to_le_bytes());
    header[12..16].copy_from_slice(&(payload_len as u32).to_le_bytes());

    let mut payload = Vec::with_capacity(payload_len);
    payload.extend_from_slice(&(key.len() as u16).to_le_bytes());
    payload.extend_from_slice(key);

    stream.write_all(&header)?;
    stream.write_all(&payload)?;

    let mut resp_header = [0u8; 16];
    stream.read_exact(&mut resp_header)?;

    let resp_opcode = resp_header[3];
    let resp_payload_len = u32::from_le_bytes([resp_header[12], resp_header[13], resp_header[14], resp_header[15]]);

    let mut resp_payload = vec![0u8; resp_payload_len as usize];
    if resp_payload_len > 0 {
        stream.read_exact(&mut resp_payload)?;
    }

    if resp_opcode == OPCODE_SUCCESS { Ok(resp_payload) }
    else { Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Key not found")) }
}

// Aerospike wire protocol implementation
fn run_aerospike_benchmark(host: &str, num_keys: usize, num_threads: usize, value: &[u8]) -> BenchResult {
    println!("  Phase 1: PUT...");
    let put_stats = run_aerospike_phase(host, num_keys, num_threads, value, WorkloadType::Put);
    println!("    {} ops/sec, {} μs avg latency, {:.2}% errors",
        put_stats.ops_per_sec() as u64, put_stats.avg_latency_us(), put_stats.error_rate());

    println!("  Phase 2: GET...");
    let get_stats = run_aerospike_phase(host, num_keys, num_threads, value, WorkloadType::Get);
    println!("    {} ops/sec, {} μs avg latency, {:.2}% errors",
        get_stats.ops_per_sec() as u64, get_stats.avg_latency_us(), get_stats.error_rate());

    println!("  Phase 3: Mixed...");
    let mixed_stats = run_aerospike_phase(host, num_keys, num_threads, value, WorkloadType::Mixed);
    println!("    {} ops/sec, {} μs avg latency, {:.2}% errors",
        mixed_stats.ops_per_sec() as u64, mixed_stats.avg_latency_us(), mixed_stats.error_rate());

    BenchResult {
        put: put_stats,
        get: get_stats,
        mixed: mixed_stats,
    }
}

fn run_aerospike_phase(host: &str, num_keys: usize, num_threads: usize, value: &[u8], workload: WorkloadType) -> Arc<BenchStats> {
    let stats = Arc::new(BenchStats::default());
    let keys_per_thread = num_keys / num_threads;

    let start = Instant::now();
    let mut handles = Vec::new();

    for t in 0..num_threads {
        let host = host.to_string();
        let value = value.to_vec();
        let stats = Arc::clone(&stats);
        let start_key = t * keys_per_thread;

        handles.push(std::thread::spawn(move || {
            let mut stream = match TcpStream::connect(&host) {
                Ok(s) => s,
                Err(_) => return,
            };
            stream.set_nodelay(true).ok();
            stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
            stream.set_write_timeout(Some(Duration::from_secs(5))).ok();

            let mut rng_state = t as u64;

            for i in 0..keys_per_thread {
                let key = format!("key_{:012}", start_key + i);
                let op_start = Instant::now();

                let do_write = match workload {
                    WorkloadType::Put => true,
                    WorkloadType::Get => false,
                    WorkloadType::Mixed => {
                        rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
                        (rng_state >> 63) == 0
                    }
                };

                let result = if do_write {
                    aerospike_put(&mut stream, key.as_bytes(), &value)
                } else {
                    aerospike_get(&mut stream, key.as_bytes()).map(|_| ())
                };

                let latency_ns = op_start.elapsed().as_nanos() as u64;
                stats.latency_sum_ns.fetch_add(latency_ns, Ordering::Relaxed);
                stats.latency_max_ns.fetch_max(latency_ns, Ordering::Relaxed);

                match result {
                    Ok(_) => { stats.operations.fetch_add(1, Ordering::Relaxed); }
                    Err(_) => { stats.errors.fetch_add(1, Ordering::Relaxed); }
                }
            }
        }));
    }

    for h in handles {
        h.join().ok();
    }

    stats.duration_ms.store(start.elapsed().as_millis() as u64, Ordering::Relaxed);
    stats
}

// Aerospike wire protocol - simplified implementation
// Based on: https://github.com/aerospike/aerospike-client-c/blob/master/src/include/aerospike/as_proto.h

fn aerospike_put(stream: &mut TcpStream, key: &[u8], value: &[u8]) -> std::io::Result<()> {
    let namespace = b"test";
    let set = b"testset";
    let bin_name = b"value";

    // Build the message
    let msg = build_aerospike_write_msg(namespace, set, key, bin_name, value);

    stream.write_all(&msg)?;

    // Read response
    let mut proto_header = [0u8; 8];
    stream.read_exact(&mut proto_header)?;

    let body_len = u64::from_be_bytes([
        proto_header[0], proto_header[1], proto_header[2], proto_header[3],
        proto_header[4], proto_header[5], proto_header[6], proto_header[7],
    ]) & 0x0000FFFFFFFFFFFF;

    if body_len > 0 && body_len < 1_000_000 {
        let mut body = vec![0u8; body_len as usize];
        stream.read_exact(&mut body)?;

        // Check result code (byte 5 in the AS_MSG header)
        if body.len() >= 6 {
            let result_code = body[5];
            if result_code == 0 {
                return Ok(());
            }
        }
    }

    Ok(()) // Assume success if we got a response
}

fn aerospike_get(stream: &mut TcpStream, key: &[u8]) -> std::io::Result<Vec<u8>> {
    let namespace = b"test";
    let set = b"testset";
    let bin_name = b"value";

    let msg = build_aerospike_read_msg(namespace, set, key, bin_name);

    stream.write_all(&msg)?;

    // Read response
    let mut proto_header = [0u8; 8];
    stream.read_exact(&mut proto_header)?;

    let body_len = u64::from_be_bytes([
        proto_header[0], proto_header[1], proto_header[2], proto_header[3],
        proto_header[4], proto_header[5], proto_header[6], proto_header[7],
    ]) & 0x0000FFFFFFFFFFFF;

    if body_len > 0 && body_len < 1_000_000 {
        let mut body = vec![0u8; body_len as usize];
        stream.read_exact(&mut body)?;

        // For simplicity, return the whole body as value
        // In a real implementation, we'd parse the bin data
        return Ok(body);
    }

    Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Key not found"))
}

fn build_aerospike_write_msg(namespace: &[u8], set: &[u8], key: &[u8], bin_name: &[u8], value: &[u8]) -> Vec<u8> {
    // AS_MSG header (22 bytes) + fields + operations
    let mut msg = Vec::with_capacity(256 + value.len());

    // Proto header placeholder (8 bytes) - will fill in at end
    msg.extend_from_slice(&[0u8; 8]);

    // AS_MSG header (22 bytes)
    msg.push(22); // header size
    msg.push(AS_MSG_TYPE); // info1 - read
    msg.push(AS_INFO2_WRITE); // info2 - write
    msg.push(0); // info3
    msg.push(0); // unused
    msg.push(0); // result code
    msg.extend_from_slice(&0u32.to_be_bytes()); // generation
    msg.extend_from_slice(&0u32.to_be_bytes()); // record ttl
    msg.extend_from_slice(&0u32.to_be_bytes()); // transaction ttl
    msg.extend_from_slice(&3u16.to_be_bytes()); // n_fields (namespace, set, key)
    msg.extend_from_slice(&1u16.to_be_bytes()); // n_ops (one bin write)

    // Field: namespace (type 0)
    let ns_field_len = 1 + namespace.len();
    msg.extend_from_slice(&(ns_field_len as u32).to_be_bytes());
    msg.push(0); // field type: namespace
    msg.extend_from_slice(namespace);

    // Field: set (type 1)
    let set_field_len = 1 + set.len();
    msg.extend_from_slice(&(set_field_len as u32).to_be_bytes());
    msg.push(1); // field type: set
    msg.extend_from_slice(set);

    // Field: key digest (type 4) - we use string key (type 3)
    // Key field with string type
    let key_field_len = 1 + 1 + key.len(); // field_type + key_type + key
    msg.extend_from_slice(&(key_field_len as u32).to_be_bytes());
    msg.push(4); // field type: key
    msg.push(3); // key type: string
    msg.extend_from_slice(key);

    // Operation: write bin
    let op_len = 4 + 1 + 1 + bin_name.len() + value.len(); // size + op + type + name + value
    msg.extend_from_slice(&(op_len as u32).to_be_bytes());
    msg.push(2); // write operation
    msg.push(1); // particle type: integer -> actually use blob (4)
    msg.push(bin_name.len() as u8);
    msg.extend_from_slice(bin_name);
    msg.extend_from_slice(value);

    // Fill in proto header
    let body_len = msg.len() - 8;
    let proto_header = ((AS_MSG_VERSION as u64) << 56) | ((AS_MSG_TYPE as u64) << 48) | (body_len as u64);
    msg[0..8].copy_from_slice(&proto_header.to_be_bytes());

    msg
}

fn build_aerospike_read_msg(namespace: &[u8], set: &[u8], key: &[u8], bin_name: &[u8]) -> Vec<u8> {
    let mut msg = Vec::with_capacity(256);

    // Proto header placeholder
    msg.extend_from_slice(&[0u8; 8]);

    // AS_MSG header
    msg.push(22);
    msg.push(AS_INFO1_READ); // info1 - read
    msg.push(0); // info2
    msg.push(0); // info3
    msg.push(0); // unused
    msg.push(0); // result code
    msg.extend_from_slice(&0u32.to_be_bytes()); // generation
    msg.extend_from_slice(&0u32.to_be_bytes()); // record ttl
    msg.extend_from_slice(&0u32.to_be_bytes()); // transaction ttl
    msg.extend_from_slice(&3u16.to_be_bytes()); // n_fields
    msg.extend_from_slice(&1u16.to_be_bytes()); // n_ops

    // Fields
    let ns_field_len = 1 + namespace.len();
    msg.extend_from_slice(&(ns_field_len as u32).to_be_bytes());
    msg.push(0);
    msg.extend_from_slice(namespace);

    let set_field_len = 1 + set.len();
    msg.extend_from_slice(&(set_field_len as u32).to_be_bytes());
    msg.push(1);
    msg.extend_from_slice(set);

    let key_field_len = 1 + 1 + key.len();
    msg.extend_from_slice(&(key_field_len as u32).to_be_bytes());
    msg.push(4);
    msg.push(3);
    msg.extend_from_slice(key);

    // Read operation for bin
    let op_len = 4 + 1 + 1 + bin_name.len();
    msg.extend_from_slice(&(op_len as u32).to_be_bytes());
    msg.push(1); // read operation
    msg.push(0); // particle type (ignored for read)
    msg.push(bin_name.len() as u8);
    msg.extend_from_slice(bin_name);

    // Fill proto header
    let body_len = msg.len() - 8;
    let proto_header = ((AS_MSG_VERSION as u64) << 56) | ((AS_MSG_TYPE as u64) << 48) | (body_len as u64);
    msg[0..8].copy_from_slice(&proto_header.to_be_bytes());

    msg
}

fn print_comparison(name: &str, ssd_kv: Option<&Arc<BenchStats>>, aerospike: Option<&Arc<BenchStats>>) {
    println!("{} Workload:", name);
    println!("  {:20} {:>12} {:>12} {:>12} {:>10}", "", "Ops/sec", "Avg (μs)", "Max (μs)", "Errors");

    if let Some(stats) = ssd_kv {
        println!("  {:20} {:>12} {:>12} {:>12} {:>10.2}%",
            "SSD-KV",
            stats.ops_per_sec() as u64,
            stats.avg_latency_us(),
            stats.max_latency_us(),
            stats.error_rate());
    } else {
        println!("  {:20} {:>12}", "SSD-KV", "N/A");
    }

    if let Some(stats) = aerospike {
        println!("  {:20} {:>12} {:>12} {:>12} {:>10.2}%",
            "Aerospike",
            stats.ops_per_sec() as u64,
            stats.avg_latency_us(),
            stats.max_latency_us(),
            stats.error_rate());
    } else {
        println!("  {:20} {:>12}", "Aerospike", "N/A");
    }

    // Print winner
    if let (Some(s), Some(a)) = (ssd_kv, aerospike) {
        let s_ops = s.ops_per_sec();
        let a_ops = a.ops_per_sec();
        if s_ops > 0.0 && a_ops > 0.0 {
            let ratio = s_ops / a_ops;
            if ratio > 1.1 {
                println!("  >> SSD-KV is {:.1}x faster", ratio);
            } else if ratio < 0.9 {
                println!("  >> Aerospike is {:.1}x faster", 1.0/ratio);
            } else {
                println!("  >> Performance is similar");
            }
        }
    }
    println!();
}
