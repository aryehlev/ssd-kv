//! UDP vs TCP benchmark for SSD-KV

use std::io::{Read, Write};
use std::net::{TcpStream, UdpSocket};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const PROTOCOL_MAGIC: u16 = 0x564B;
const TCP_PORT: u16 = 7777;
const UDP_PORT: u16 = 7778;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let num_keys: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(10000);
    let threads: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(4);

    println!("===========================================");
    println!("  SSD-KV: TCP vs UDP Performance Test");
    println!("===========================================\n");
    println!("Keys: {}, Threads: {}\n", num_keys, threads);

    // First, populate data via TCP
    println!("Populating {} keys via TCP...", num_keys);
    populate_keys(num_keys);
    println!("Done.\n");

    // Benchmark TCP reads
    println!("--- TCP GET Benchmark ---");
    let tcp_result = benchmark_tcp_gets(num_keys, threads);
    println!(
        "  {} ops/sec, {} μs avg latency\n",
        tcp_result.0, tcp_result.1
    );

    // Benchmark UDP reads
    println!("--- UDP GET Benchmark ---");
    let udp_result = benchmark_udp_gets(num_keys, threads);
    println!(
        "  {} ops/sec, {} μs avg latency\n",
        udp_result.0, udp_result.1
    );

    // Comparison
    let speedup = udp_result.0 as f64 / tcp_result.0 as f64;
    println!("===========================================");
    println!("  UDP is {:.1}x faster than TCP", speedup);
    println!("===========================================");
}

fn populate_keys(num_keys: usize) {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", TCP_PORT)).unwrap();
    stream.set_nodelay(true).unwrap();

    for i in 0..num_keys {
        let key = format!("bench_key_{:08}", i);
        let value = format!("bench_value_{:08}", i);
        let _ = tcp_put(&mut stream, i as u32, key.as_bytes(), value.as_bytes());
    }
}

fn benchmark_tcp_gets(num_keys: usize, threads: usize) -> (u64, u64) {
    let ops = Arc::new(AtomicU64::new(0));
    let total_latency = Arc::new(AtomicU64::new(0));
    let keys_per_thread = num_keys / threads;

    let start = Instant::now();

    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let ops = Arc::clone(&ops);
            let total_latency = Arc::clone(&total_latency);
            std::thread::spawn(move || {
                let mut stream = TcpStream::connect(format!("127.0.0.1:{}", TCP_PORT)).unwrap();
                stream.set_nodelay(true).unwrap();

                for i in 0..keys_per_thread {
                    let key_idx = t * keys_per_thread + i;
                    let key = format!("bench_key_{:08}", key_idx);

                    let op_start = Instant::now();
                    let _ = tcp_get(&mut stream, key_idx as u32, key.as_bytes());
                    let latency = op_start.elapsed().as_micros() as u64;

                    ops.fetch_add(1, Ordering::Relaxed);
                    total_latency.fetch_add(latency, Ordering::Relaxed);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start.elapsed().as_secs_f64();
    let total_ops = ops.load(Ordering::Relaxed);
    let avg_latency = total_latency.load(Ordering::Relaxed) / total_ops.max(1);

    ((total_ops as f64 / elapsed) as u64, avg_latency)
}

fn benchmark_udp_gets(num_keys: usize, threads: usize) -> (u64, u64) {
    let ops = Arc::new(AtomicU64::new(0));
    let total_latency = Arc::new(AtomicU64::new(0));
    let keys_per_thread = num_keys / threads;

    let start = Instant::now();

    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let ops = Arc::clone(&ops);
            let total_latency = Arc::clone(&total_latency);
            std::thread::spawn(move || {
                let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
                socket.connect(format!("127.0.0.1:{}", UDP_PORT)).unwrap();
                socket.set_read_timeout(Some(Duration::from_millis(100))).unwrap();

                let mut send_buf = [0u8; 256];
                let mut recv_buf = [0u8; 1024];

                for i in 0..keys_per_thread {
                    let key_idx = t * keys_per_thread + i;
                    let key = format!("bench_key_{:08}", key_idx);
                    let key_bytes = key.as_bytes();

                    // Build UDP GET packet
                    send_buf[0..2].copy_from_slice(&PROTOCOL_MAGIC.to_le_bytes());
                    send_buf[2] = 0x01; // GET opcode
                    send_buf[3] = key_bytes.len() as u8;
                    send_buf[4..8].copy_from_slice(&(key_idx as u32).to_le_bytes());
                    send_buf[8..8 + key_bytes.len()].copy_from_slice(key_bytes);

                    let op_start = Instant::now();

                    socket.send(&send_buf[..8 + key_bytes.len()]).unwrap();

                    // Receive response (with retry on timeout)
                    match socket.recv(&mut recv_buf) {
                        Ok(_) => {
                            let latency = op_start.elapsed().as_micros() as u64;
                            ops.fetch_add(1, Ordering::Relaxed);
                            total_latency.fetch_add(latency, Ordering::Relaxed);
                        }
                        Err(_) => {
                            // UDP packet lost, skip
                        }
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start.elapsed().as_secs_f64();
    let total_ops = ops.load(Ordering::Relaxed);
    let avg_latency = total_latency.load(Ordering::Relaxed) / total_ops.max(1);

    ((total_ops as f64 / elapsed) as u64, avg_latency)
}

fn tcp_put(stream: &mut TcpStream, request_id: u32, key: &[u8], value: &[u8]) -> bool {
    let payload_len = 4 + key.len() + 4 + value.len();
    let mut header = [0u8; 16];
    header[0..2].copy_from_slice(&PROTOCOL_MAGIC.to_le_bytes());
    header[2] = 0x01; // version
    header[3] = 0x02; // PUT opcode
    header[4..8].copy_from_slice(&request_id.to_le_bytes());
    header[12..16].copy_from_slice(&(payload_len as u32).to_le_bytes());

    let mut payload = Vec::with_capacity(payload_len);
    payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
    payload.extend_from_slice(value);

    stream.write_all(&header).is_ok() && stream.write_all(&payload).is_ok() && {
        let mut resp = [0u8; 16];
        stream.read_exact(&mut resp).is_ok()
    }
}

fn tcp_get(stream: &mut TcpStream, request_id: u32, key: &[u8]) -> Option<Vec<u8>> {
    let payload_len = 4 + key.len();
    let mut header = [0u8; 16];
    header[0..2].copy_from_slice(&PROTOCOL_MAGIC.to_le_bytes());
    header[2] = 0x01;
    header[3] = 0x01; // GET opcode
    header[4..8].copy_from_slice(&request_id.to_le_bytes());
    header[12..16].copy_from_slice(&(payload_len as u32).to_le_bytes());

    let mut payload = Vec::with_capacity(payload_len);
    payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
    payload.extend_from_slice(key);

    stream.write_all(&header).ok()?;
    stream.write_all(&payload).ok()?;

    let mut resp_header = [0u8; 16];
    stream.read_exact(&mut resp_header).ok()?;

    let resp_len = u32::from_le_bytes([resp_header[12], resp_header[13], resp_header[14], resp_header[15]]) as usize;
    if resp_len > 0 {
        let mut value = vec![0u8; resp_len];
        stream.read_exact(&mut value).ok()?;
        Some(value)
    } else {
        Some(vec![])
    }
}
