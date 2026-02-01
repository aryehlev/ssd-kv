//! Benchmark client for comparing SSD-KV and Aerospike performance.
//!
//! Run with: cargo run --release --bin bench_client

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// Protocol constants
const PROTOCOL_MAGIC: u16 = 0x564B;
const PROTOCOL_VERSION: u8 = 1;
const OPCODE_GET: u8 = 0x01;
const OPCODE_PUT: u8 = 0x02;
const OPCODE_SUCCESS: u8 = 0x80;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let host = args.get(1).map(|s| s.as_str()).unwrap_or("127.0.0.1:7777");
    let num_keys: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(100_000);
    let num_threads: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(4);
    let value_size: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(100);

    println!("SSD-KV Benchmark");
    println!("================");
    println!("Host: {}", host);
    println!("Keys: {}", num_keys);
    println!("Threads: {}", num_threads);
    println!("Value size: {} bytes", value_size);
    println!();

    // Generate test data
    let value: Vec<u8> = (0..value_size).map(|i| (i % 256) as u8).collect();

    // Phase 1: Write benchmark
    println!("Phase 1: PUT benchmark");
    let write_stats = run_benchmark(host, num_keys, num_threads, &value, true);
    print_stats("PUT", &write_stats);

    // Phase 2: Read benchmark
    println!("\nPhase 2: GET benchmark");
    let read_stats = run_benchmark(host, num_keys, num_threads, &value, false);
    print_stats("GET", &read_stats);

    // Phase 3: Mixed workload (50% read, 50% write)
    println!("\nPhase 3: Mixed workload (50/50)");
    let mixed_stats = run_mixed_benchmark(host, num_keys, num_threads, &value);
    print_stats("Mixed", &mixed_stats);
}

#[derive(Default)]
struct BenchStats {
    operations: AtomicU64,
    errors: AtomicU64,
    latency_sum_ns: AtomicU64,
    latency_max_ns: AtomicU64,
}

fn run_benchmark(host: &str, num_keys: usize, num_threads: usize, value: &[u8], is_write: bool) -> Arc<BenchStats> {
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
                Err(e) => {
                    eprintln!("Connection failed: {}", e);
                    return;
                }
            };
            stream.set_nodelay(true).ok();

            for i in 0..keys_per_thread {
                let key = format!("key_{:012}", start_key + i);
                let op_start = Instant::now();

                let result = if is_write {
                    send_put(&mut stream, (start_key + i) as u32, key.as_bytes(), &value)
                } else {
                    send_get(&mut stream, (start_key + i) as u32, key.as_bytes()).map(|_| ())
                };

                let latency_ns = op_start.elapsed().as_nanos() as u64;
                stats.latency_sum_ns.fetch_add(latency_ns, Ordering::Relaxed);
                stats.latency_max_ns.fetch_max(latency_ns, Ordering::Relaxed);

                match result {
                    Ok(_) => {
                        stats.operations.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        stats.errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    for h in handles {
        h.join().ok();
    }

    let elapsed = start.elapsed();
    let ops = stats.operations.load(Ordering::Relaxed);
    let ops_per_sec = ops as f64 / elapsed.as_secs_f64();

    println!("  Total time: {:.2}s", elapsed.as_secs_f64());
    println!("  Throughput: {:.0} ops/sec", ops_per_sec);

    stats
}

fn run_mixed_benchmark(host: &str, num_keys: usize, num_threads: usize, value: &[u8]) -> Arc<BenchStats> {
    let stats = Arc::new(BenchStats::default());
    let ops_per_thread = num_keys / num_threads;

    let start = Instant::now();
    let mut handles = Vec::new();

    for t in 0..num_threads {
        let host = host.to_string();
        let value = value.to_vec();
        let stats = Arc::clone(&stats);

        handles.push(std::thread::spawn(move || {
            let mut stream = match TcpStream::connect(&host) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("Connection failed: {}", e);
                    return;
                }
            };
            stream.set_nodelay(true).ok();

            let mut rng_state = t as u64;

            for i in 0..ops_per_thread {
                // Simple LCG for random-ish behavior
                rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let is_write = (rng_state >> 63) == 0;
                let key_idx = (rng_state >> 32) as usize % (num_keys / 2);
                let key = format!("key_{:012}", key_idx);

                let op_start = Instant::now();

                let result = if is_write {
                    send_put(&mut stream, i as u32, key.as_bytes(), &value)
                } else {
                    send_get(&mut stream, i as u32, key.as_bytes()).map(|_| ())
                };

                let latency_ns = op_start.elapsed().as_nanos() as u64;
                stats.latency_sum_ns.fetch_add(latency_ns, Ordering::Relaxed);
                stats.latency_max_ns.fetch_max(latency_ns, Ordering::Relaxed);

                match result {
                    Ok(_) => {
                        stats.operations.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        stats.errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    for h in handles {
        h.join().ok();
    }

    let elapsed = start.elapsed();
    let ops = stats.operations.load(Ordering::Relaxed);
    let ops_per_sec = ops as f64 / elapsed.as_secs_f64();

    println!("  Total time: {:.2}s", elapsed.as_secs_f64());
    println!("  Throughput: {:.0} ops/sec", ops_per_sec);

    stats
}

fn print_stats(name: &str, stats: &BenchStats) {
    let ops = stats.operations.load(Ordering::Relaxed);
    let errors = stats.errors.load(Ordering::Relaxed);
    let avg_latency_us = if ops > 0 {
        (stats.latency_sum_ns.load(Ordering::Relaxed) / ops) / 1000
    } else {
        0
    };
    let max_latency_us = stats.latency_max_ns.load(Ordering::Relaxed) / 1000;

    println!("  {} Results:", name);
    println!("    Operations: {}", ops);
    println!("    Errors: {}", errors);
    println!("    Avg latency: {} μs", avg_latency_us);
    println!("    Max latency: {} μs", max_latency_us);
}

fn send_put(stream: &mut TcpStream, request_id: u32, key: &[u8], value: &[u8]) -> std::io::Result<()> {
    // Build payload: key_len(2) + key + value_len(4) + value + ttl(4)
    let payload_len = 2 + key.len() + 4 + value.len() + 4;

    // Build header
    let mut header = [0u8; 16];
    header[0..2].copy_from_slice(&PROTOCOL_MAGIC.to_le_bytes());
    header[2] = PROTOCOL_VERSION;
    header[3] = OPCODE_PUT;
    header[4..8].copy_from_slice(&request_id.to_le_bytes());
    header[12..16].copy_from_slice(&(payload_len as u32).to_le_bytes());

    // Build payload
    let mut payload = Vec::with_capacity(payload_len);
    payload.extend_from_slice(&(key.len() as u16).to_le_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
    payload.extend_from_slice(value);
    payload.extend_from_slice(&0u32.to_le_bytes()); // TTL

    // Send
    stream.write_all(&header)?;
    stream.write_all(&payload)?;

    // Read response
    let mut resp_header = [0u8; 16];
    stream.read_exact(&mut resp_header)?;

    let resp_opcode = resp_header[3];
    let resp_payload_len = u32::from_le_bytes([resp_header[12], resp_header[13], resp_header[14], resp_header[15]]);

    if resp_payload_len > 0 {
        let mut resp_payload = vec![0u8; resp_payload_len as usize];
        stream.read_exact(&mut resp_payload)?;
    }

    if resp_opcode == OPCODE_SUCCESS {
        Ok(())
    } else {
        Err(std::io::Error::new(std::io::ErrorKind::Other, "PUT failed"))
    }
}

fn send_get(stream: &mut TcpStream, request_id: u32, key: &[u8]) -> std::io::Result<Vec<u8>> {
    // Build payload: key_len(2) + key
    let payload_len = 2 + key.len();

    // Build header
    let mut header = [0u8; 16];
    header[0..2].copy_from_slice(&PROTOCOL_MAGIC.to_le_bytes());
    header[2] = PROTOCOL_VERSION;
    header[3] = OPCODE_GET;
    header[4..8].copy_from_slice(&request_id.to_le_bytes());
    header[12..16].copy_from_slice(&(payload_len as u32).to_le_bytes());

    // Build payload
    let mut payload = Vec::with_capacity(payload_len);
    payload.extend_from_slice(&(key.len() as u16).to_le_bytes());
    payload.extend_from_slice(key);

    // Send
    stream.write_all(&header)?;
    stream.write_all(&payload)?;

    // Read response
    let mut resp_header = [0u8; 16];
    stream.read_exact(&mut resp_header)?;

    let resp_opcode = resp_header[3];
    let resp_payload_len = u32::from_le_bytes([resp_header[12], resp_header[13], resp_header[14], resp_header[15]]);

    let mut resp_payload = vec![0u8; resp_payload_len as usize];
    if resp_payload_len > 0 {
        stream.read_exact(&mut resp_payload)?;
    }

    if resp_opcode == OPCODE_SUCCESS {
        Ok(resp_payload)
    } else {
        Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Key not found"))
    }
}
